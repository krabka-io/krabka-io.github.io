#!/usr/bin/env node
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { VENDORS, aggregateSample, directIoCheck, ombMeasurementWindow, parseDiskstats, resourceTimeSeries, windowMetrics } from './benchmark-results.mjs';
import { OMB, OMB_BACKLOG_RETENTION_RATIO, ombCases, ombWorkload, ombDriver, ombRetentionBytes, ombTimeoutMs, prepareOmb, retriableCommitFailures, validateOmbResult, writeOmbReport } from './benchmark-openmessaging.mjs';
import { runLoggedCommand } from './benchmark-command.mjs';

const ROOT = fileURLToPath(new URL('../', import.meta.url));
const GIB = 1024 ** 3;
// How long a memory check waits for MemAvailable to come back, and how often
// it looks.
const MEMORY_WAIT_MS = 5 * 60_000;
const MEMORY_POLL_MS = 5_000;
const DEFAULT_IMAGES = {
  krabka: 'ghcr.io/krabka-io/krabka-broker:v1.0.5',
  kafka: 'apache/kafka:4.3.1',
  redpanda: 'docker.redpanda.com/redpandadata/redpanda:v26.2.2',
};
const { values: options } = parseArgs({ options: {
  'krabka-image': { type: 'string', default: DEFAULT_IMAGES.krabka },
  'redpanda-image': { type: 'string', default: DEFAULT_IMAGES.redpanda },
  'suite': { type: 'string', default: 'openmessaging' },
  'workloads': { type: 'string', default: 'all' },
  'replication-factors': { type: 'string', default: '1,3' },
  'repetitions': { type: 'string' },
  'vendors': { type: 'string', default: VENDORS.join(',') },
  'krabka-preallocate': { type: 'boolean', default: false },
  'krabka-io-uring': { type: 'boolean', default: false },
  'test-duration-minutes': { type: 'string' },
  'krabka-env': { type: 'string', multiple: true, default: [] },
  'no-fstrim': { type: 'boolean', default: false },
  'smoke': { type: 'boolean', default: false },
  'dry-run': { type: 'boolean', default: false },
  'help': { type: 'boolean', short: 'h' },
} });
if (options.help) {
  console.log(`Usage: npm run benchmark -- [--suite openmessaging] [--smoke] [--dry-run] [--krabka-image REF] [--redpanda-image REF]
OpenMessaging: upstream Kafka driver/catalog; --workloads all|NAME,NAME; --replication-factors 1,3; --repetitions 1..3.
--vendors krabka,kafka,redpanda runs a subset of the brokers; such a run cannot be published.
--krabka-preallocate sets preallocate=true on krabka's benchmark topics, which writes
their active segments with O_DIRECT where the kernel and filesystem support it.
--krabka-env KRABKA_NAME=VALUE (repeatable) passes a setting to every krabka broker.
Before every trial the runner trims the filesystems that hold the checkout and Docker's data
with sudo -n fstrim, so each trial starts on SSDs with the same free blocks; --no-fstrim skips it.
Smoke: shortened workloads; never publishes.
Dry run: validates the host and resolves images; starts no containers.
Requires native Linux/amd64 Docker with cgroup v2, Node >=22.12, JDK >=17,
14 available logical CPUs, 34 GiB available RAM, and 150 GiB free disk
(540 GiB with backlog workloads; 4 GiB for smoke).
See benchmarks/README.md for methodology and retained artifacts.`);
} else {
  await main().catch(error => { console.error(`benchmark: ${error.message}`); process.exitCode = 1; });
}

async function main() {
  assert.equal(options.suite, 'openmessaging', 'only the OpenMessaging suite is supported');
  const replicationFactors = options['replication-factors'].split(',').map(Number);
  assert.ok(replicationFactors.length && new Set(replicationFactors).size === replicationFactors.length
    && replicationFactors.every(rf => [1, 3].includes(rf)), 'replication-factors must be 1, 3, or 1,3');
  const runId = `${new Date().toISOString().replace(/[:.]/g, '-').replace(/-\d{3}Z$/, 'Z')}-${randomUUID().slice(0, 8)}`;
  const artifacts = path.join(ROOT, '.benchmarks', runId);
  await fs.mkdir(artifacts, { recursive: true });
  const controller = new AbortController();
  let interrupted = false;
  const onSignal = signal => { interrupted = true; controller.abort(new Error(`interrupted by ${signal}`)); };
  const onInt = () => onSignal('SIGINT');
  const onTerm = () => onSignal('SIGTERM');
  process.on('SIGINT', onInt);
  process.on('SIGTERM', onTerm);
  const containers = new Map();
  const volumes = new Set();
  let network;
  let lock;
  let resourceCreationStarted = false;
  const trials = [];
  const failures = [];
  const repetitions = options.smoke ? 1 : Number(options.repetitions ?? 1);
  assert.ok(Number.isInteger(repetitions) && repetitions >= 1 && repetitions <= 3, 'repetitions must be 1..3');
  const cases = ombCases(options.workloads);
  // A shorter measurement for a workload that cannot finish on the host, for
  // example one whose unlimited retention fills the disk before its upstream
  // duration ends. Recorded in the contract; the publisher refuses such runs.
  const testDuration = options['test-duration-minutes'] === undefined ? undefined : Number(options['test-duration-minutes']);
  assert.ok(testDuration === undefined || (Number.isInteger(testDuration) && testDuration >= 1),
    '--test-duration-minutes must be a positive whole number');
  for (const setting of options['krabka-env']) {
    assert.match(setting, /^KRABKA_[A-Z0-9_]+=[^\s]*$/, `invalid --krabka-env ${setting}`);
    // The settings are copied into provenance.json and the command log, so a
    // secret must not be one of them.
    assert.doesNotMatch(setting.split('=')[0], /SECRET|PASSWORD|PASSWD|TOKEN|CREDENTIAL|_KEY(_|$)|PRIVATE/,
      `--krabka-env ${setting.split('=')[0]} looks like a secret; it would be recorded in the run's provenance`);
  }
  const vendors = options.vendors.split(',');
  assert.ok(vendors.length > 0 && vendors.every((v, i) => VENDORS.includes(v) && vendors.indexOf(v) === i), 'invalid --vendors');
  const provenance = {
    schema_version: 1, suite: options.suite, run_id: runId, started_at: new Date().toISOString(),
    mode: options.smoke ? 'smoke' : 'full', status: 'running', repetitions,
    images: {}, cases, vendors, workload_source: OMB,
    contract: { partitions: 'from upstream workload', acks: 'all', idempotence: true, batch_bytes: 1048576, linger_ms: 1,
      broker_cpus: 4, broker_memory_bytes: 10 * GIB, swap_bytes: 0, nofile: 131072,
      kafka_heap_bytes: GIB, redpanda_shards: 4, redpanda_memory_bytes: 8 * GIB,
      redpanda_reserve_bytes: GIB, redpanda_write_caching: true,
      redpanda_network_iocbs_per_shard: 1024,
      krabka_preallocate: options['krabka-preallocate'],
      // Docker's default seccomp profile refuses io_uring_setup, so a broker
      // that submits direct writes through io_uring falls back to its write
      // threads. --krabka-io-uring starts krabka's brokers under that profile
      // plus the three io_uring syscalls; see benchmarks/seccomp/.
      ...(options['krabka-io-uring'] ? { krabka_seccomp: 'docker-29.8.1-default+io_uring' } : {}),
      ...(options['krabka-env'].length ? { krabka_env: options['krabka-env'] } : {}),
      ...(testDuration !== undefined ? { test_duration_minutes: testDuration } : {}),
      disk_trim_before_trial: !options['no-fstrim'],
      client_memory_bytes: 4 * GIB, client_heap_bytes: 2 * GIB,
      resource_sampling_ms: 250,
      durability: 'buffered writes; no identical crash-durability claim',
    },
  };
  provenance.replication_factors = replicationFactors;
  Object.assign(provenance.contract, {
    compression: 'none', max_in_flight_requests: 1,
    retention: { default: 'unlimited', backlog_topics: `${OMB_BACKLOG_RETENTION_RATIO}x the backlog, split across partitions` },
    delivery_verification: 'OMB rates/latencies only; no sequence or exact delivery check',
    warmup_minutes: options.smoke ? 0 : 1,
    smoke_overrides: options.smoke ? { minutes: 1, rate: 5000, backlog_gb: 0 } : null,
  });
  const save = () => fs.writeFile(path.join(artifacts, 'provenance.json'), `${JSON.stringify(provenance, null, 2)}\n`);

  async function command(executable, args, { timeout = 120_000, output, streamOutput = false, ignoreAbort = false } = {}) {
    if (!ignoreAbort) controller.signal.throwIfAborted();
    await fs.appendFile(path.join(artifacts, 'commands.jsonl'),
      `${JSON.stringify({ at: new Date().toISOString(), executable, args, timeout })}\n`);
    if (streamOutput) return runLoggedCommand(executable, args, { output, timeout,
      ...(ignoreAbort ? {} : { signal: controller.signal }) });
    return new Promise((resolve, reject) => {
      execFile(executable, args, { timeout, killSignal: 'SIGKILL', maxBuffer: 32 * 1024 ** 2,
        ...(ignoreAbort ? {} : { signal: controller.signal }) }, async (error, stdout, stderr) => {
        try {
          if (output) {
            await fs.writeFile(`${output}.stdout`, stdout);
            await fs.writeFile(`${output}.stderr`, stderr);
          }
          if (error) {
            error.message = `${executable} ${args.slice(0, 4).join(' ')}: ${error.message}\n${stderr.slice(-3000)}${stdout.slice(-1000)}`;
            reject(error);
          } else resolve(stdout.trim());
        } catch (writeError) { reject(writeError); }
      });
    });
  }
  const docker = (args, settings) => command('docker', args, settings);

  // Trim every filesystem in provenance.disk_trim.mounts that has not
  // refused. A mount that refuses (no passwordless sudo for fstrim, or a
  // filesystem without discard) is recorded and printed once, and is not
  // asked again; the other mounts are still trimmed. The contract then says
  // the run was not trimmed before every trial, so the shard merge does not
  // combine it with a run that was.
  async function trimDisks(directory) {
    const trim = provenance.disk_trim;
    if (!trim.requested) return;
    let complete = true;
    for (const [index, entry] of trim.mounts.entries()) {
      if (entry.unavailable) {
        complete = false;
        continue;
      }
      try {
        const output = await command('sudo', ['-n', 'fstrim', '--verbose', entry.mount],
          { timeout: 600_000, output: path.join(directory, `fstrim-${index}`) });
        entry.trimmed_bytes += Number(output.match(/\((\d+) bytes\) trimmed/)?.[1] ?? 0);
        entry.trials += 1;
      } catch (error) {
        if (controller.signal.aborted) throw error;
        entry.unavailable = error.message.split('\n')[0];
        complete = false;
        console.warn(`benchmark: ${entry.mount} runs untrimmed, fstrim unavailable: ${entry.unavailable}`);
      }
    }
    if (complete) trim.trials += 1;
    else provenance.contract.disk_trim_before_trial = false;
    await save();
  }
  // The mount, filesystem, block devices and their model and firmware behind
  // each location, and the devices whose /proc/diskstats counters a trial
  // samples. A RAID device lists its member devices too.
  async function storageLayout(locations) {
    const read = file => fs.readFile(file, 'utf8').then(text => text.trim(), () => null);
    const mounts = [];
    const devices = new Set();
    for (const location of locations) {
      const [mount] = JSON.parse(await command('findmnt', ['--json', '--output', 'TARGET,SOURCE,FSTYPE,OPTIONS', '--target', location])).filesystems;
      if (mounts.some(m => m.target === mount.target)) continue;
      const name = path.basename(mount.source);
      const members = await fs.readdir(`/sys/block/${name}/slaves`).catch(() => []);
      const disks = [];
      for (const member of members.length ? members : [name]) {
        const disk = path.basename(path.dirname(await fs.realpath(`/sys/class/block/${member}`)));
        const base = (await fs.access(`/sys/block/${member}`).then(() => true, () => false)) ? member : disk;
        disks.push({ device: member, disk: base, model: await read(`/sys/block/${base}/device/model`),
          firmware: await read(`/sys/block/${base}/device/firmware_rev`),
          size_bytes: Number(await read(`/sys/block/${base}/size`)) * 512 });
        devices.add(member);
      }
      devices.add(name);
      mounts.push({ ...mount, device: name, raid_level: await read(`/sys/block/${name}/md/level`), members: disks });
    }
    return { mounts, sampled_devices: [...devices].sort() };
  }
  async function removeContainer(id) {
    await docker(['rm', '-f', '-v', id], { ignoreAbort: true, timeout: 30_000 });
    containers.delete(id);
  }
  async function createContainer(args, label) {
    resourceCreationStarted = true;
    const id = await docker(['create', '--platform', 'linux/amd64', '--label', `krabka.benchmark=${runId}`, ...args]);
    containers.set(id, label);
    return id;
  }
  async function oneShot(args, label, settings = {}) {
    const id = await createContainer(args, label);
    try { return await docker(['start', '-a', id], settings); }
    finally { await removeContainer(id); }
  }
  // MemAvailable can stay short for a while after the previous trial's
  // containers are gone. Run 40452124698 lost its two RF3 lanes to that: on
  // two runners the check failed within a minute of a 20-minute max-rate
  // shard, and every later shard of the lane failed at once. Wait up to
  // MEMORY_WAIT_MS for the memory to come back; when it does not, record
  // where it is before failing, so the retained artifacts say what held it.
  async function awaitAvailableMemory(needed, purpose) {
    const read = async () => Number((await fs.readFile('/proc/meminfo', 'utf8')).match(/^MemAvailable:\s+(\d+)/m)[1]) * 1024;
    const started = Date.now();
    let available = await read();
    let polls = 0;
    while (available < needed && Date.now() - started < MEMORY_WAIT_MS) {
      await delay(MEMORY_POLL_MS, undefined, { signal: controller.signal });
      available = await read();
      polls++;
    }
    const waited = Math.round((Date.now() - started) / 1000);
    if (polls > 0) console.log(`benchmark: waited ${waited} s for ${needed / GIB} GiB available RAM; found ${(available / GIB).toFixed(1)} GiB`);
    if (available >= needed) return available;
    const meminfo = await fs.readFile('/proc/meminfo', 'utf8');
    const breakdown = ['MemFree', 'Cached', 'AnonPages', 'Shmem', 'Slab', 'SUnreclaim', 'Dirty', 'Writeback', 'Unevictable', 'AnonHugePages']
      .map(field => `${field} ${(Number(meminfo.match(new RegExp(`^${field}:\\s+(\\d+)`, 'm'))?.[1] ?? 0) * 1024 / GIB).toFixed(1)} GiB`)
      .join(', ');
    const processes = await command('ps', ['-eo', 'pid,rss,etime,comm', '--sort=-rss'])
      .then(listing => listing.split('\n').slice(0, 16).join('\n'), error => error.message);
    const running = await docker(['ps', '-a', '--format', '{{.Names}} {{.Status}} {{.Image}}'], { timeout: 30_000 })
      .catch(error => error.message);
    await fs.appendFile(path.join(artifacts, 'memory-shortfall.txt'),
      `${new Date().toISOString()} ${purpose}: ${breakdown}\n${meminfo}\n${processes}\n${running}\n\n`);
    throw new Error(`${purpose} needs ${needed / GIB} GiB available RAM; found ${(available / GIB).toFixed(1)} GiB after ${waited} s `
      + `(${breakdown}); memory-shortfall.txt lists the processes and containers`);
  }
  async function cleanup() {
    const errors = [];
    if (resourceCreationStarted) {
      // A signal can interrupt the CLI after the daemon created a resource but
      // before its ID reached us. Recover only this run's exact unique label.
      const filter = `label=krabka.benchmark=${runId}`;
      const ownedContainers = await docker(['ps', '-aq', '--no-trunc', '--filter', filter], { ignoreAbort: true, timeout: 30_000 });
      for (const id of ownedContainers.split('\n').filter(Boolean)) {
        if (!containers.has(id)) containers.set(id, `recovered-${id.slice(0, 12)}`);
      }
      const ownedVolumes = await docker(['volume', 'ls', '-q', '--filter', filter], { ignoreAbort: true, timeout: 30_000 });
      for (const volume of ownedVolumes.split('\n').filter(Boolean)) volumes.add(volume);
    }
    for (const [id, label] of containers) {
      try {
        await fs.writeFile(path.join(artifacts, `${label}.container-state.json`),
          `${await docker(['inspect', '--format', '{{json .State}}', id], { ignoreAbort: true, timeout: 30_000 })}\n`);
      } catch { /* Diagnostics must not prevent removal of this run's resources. */ }
      try {
        await docker(['logs', id], { output: path.join(artifacts, `${label}.container`), streamOutput: true,
          ignoreAbort: true, timeout: 30_000 });
      } catch { /* A created container may never have started. */ }
      try { await removeContainer(id); } catch (error) { errors.push(error.message); }
    }
    for (const volume of volumes) {
      try { await docker(['volume', 'rm', volume], { ignoreAbort: true, timeout: 30_000 }); volumes.delete(volume); }
      catch (error) { errors.push(error.message); }
    }
    if (network) {
      try { await docker(['network', 'rm', network], { ignoreAbort: true, timeout: 30_000 }); network = undefined; }
      catch (error) { errors.push(error.message); }
    }
    if (resourceCreationStarted) {
      const ownedNetworks = await docker(['network', 'ls', '-q', '--filter', `label=krabka.benchmark=${runId}`], { ignoreAbort: true, timeout: 30_000 });
      for (const id of ownedNetworks.split('\n').filter(Boolean)) {
        try { await docker(['network', 'rm', id], { ignoreAbort: true, timeout: 30_000 }); }
        catch (error) { errors.push(error.message); }
      }
    }
    if (errors.length) throw new Error(`cleanup failed: ${errors.join('\n')}`);
  }

  try {
    // Avoid overlapping runs in the same checkout. A dead owner's lock can be
    // inspected and removed manually, never by guessing whether a process died.
    const lockPath = path.join(ROOT, '.benchmarks', 'runner.lock');
    lock = await fs.open(lockPath, 'wx').catch(error => {
      if (error.code === 'EEXIST') throw new Error(`another benchmark owns ${lockPath}; inspect that file before removing a stale lock`);
      throw error;
    });
    await lock.writeFile(`${JSON.stringify({ pid: process.pid, run_id: runId })}\n`);
    assert.ok(process.platform === 'linux' && process.arch === 'x64', 'native Linux/amd64 is required');
    const [nodeMajor, nodeMinor] = process.versions.node.split('.').map(Number);
    assert.ok(nodeMajor > 22 || (nodeMajor === 22 && nodeMinor >= 12), 'Node >=22.12 is required');
    const info = JSON.parse(await docker(['info', '--format', '{{json .}}']));
    assert.ok(info.OSType === 'linux' && ['x86_64', 'amd64'].includes(info.Architecture), 'Docker must be native Linux/amd64');
    assert.equal(info.CgroupVersion, '2', 'Docker cgroup v2 is required');
    assert.equal(info.KernelVersion, os.release(), 'Docker must run on this host, not a VM or remote daemon');
    for (const capability of ['MemoryLimit', 'SwapLimit', 'CPUSet', 'CpuCfsQuota']) {
      assert.ok(info[capability], `Docker ${capability} is required`);
    }
    const javac = await command('javac', ['--version']);
    const java = await command('java', ['--version']);
    assert.ok(Number(javac.match(/javac (\d+)/)?.[1]) >= 17, 'JDK 17+ is required');
    const aioMax = Number(await fs.readFile('/proc/sys/fs/aio-max-nr', 'utf8'));
    const aioUsed = Number(await fs.readFile('/proc/sys/fs/aio-nr', 'utf8'));
    // Four shards per node: 1024 storage + 1024 networking + 2 preemption
    // contexts per shard, plus startup headroom. Never change a host sysctl.
    assert.ok(aioMax - aioUsed >= 3 * 4 * 2050 + 128, 'insufficient available Linux AIO slots for Redpanda RF3');
    const memoryAvailable = await awaitAvailableMemory(34 * GIB, 'the run');
    const disk = await fs.statfs(ROOT);
    const freeDisk = disk.bavail * disk.bsize;
    // OMB backlog topics retain 120 GiB, three times over at RF3. Allow up to
    // five minutes of writes between retention checks (about 84 GiB at RF3),
    // segment granularity, record overhead and the 20 GiB abort reserve.
    const ombBacklog = cases.some(c => c.id.startsWith('backlog-'));
    const requiredDisk = (options.smoke ? 4 : ombBacklog ? 540 : 150) * GIB;
    assert.ok(freeDisk >= requiredDisk, `insufficient disk: ${(freeDisk / GIB).toFixed(1)} GiB free`);
    const dockerDisk = await fs.statfs(info.DockerRootDir);
    const dockerFreeDisk = dockerDisk.bavail * dockerDisk.bsize;
    assert.ok(dockerFreeDisk >= requiredDisk,
      `insufficient Docker storage: ${(dockerFreeDisk / GIB).toFixed(1)} GiB free`);
    const ownCgroup = (await fs.readFile('/proc/self/cgroup', 'utf8')).match(/^0::(.+)$/m)?.[1];
    assert.ok(ownCgroup, 'host cgroup v2 is required');
    for (const counter of ['cpu.stat', 'memory.stat', 'memory.current', 'memory.events', 'cgroup.procs']) {
      await fs.readFile(path.join('/sys/fs/cgroup', ownCgroup, counter));
    }
    const allowed = (await fs.readFile('/proc/self/status', 'utf8')).match(/^Cpus_allowed_list:\s+(.+)$/m)[1];
    const cpus = allowed.split(',').flatMap(range => {
      const [start, end = start] = range.split('-').map(Number);
      return Array.from({ length: end - start + 1 }, (_, i) => start + i);
    });
    assert.ok(cpus.length >= 14 && info.NCPU >= 14, 'RF3 needs 14 available logical CPUs');
    const topology = [];
    for (const cpu of cpus) {
      const root = `/sys/devices/system/cpu/cpu${cpu}/topology`;
      const [core, socket] = await Promise.all([fs.readFile(`${root}/core_id`, 'utf8'), fs.readFile(`${root}/physical_package_id`, 'utf8')]);
      topology.push({ cpu, core: Number(core), socket: Number(socket) });
    }
    // Keep SMT siblings together where possible rather than interleaving every
    // broker with another broker on the same physical core.
    topology.sort((a, b) => a.socket - b.socket || a.core - b.core || a.cpu - b.cpu);
    const ordered = topology.map(t => t.cpu);
    provenance.cpu_sets = { brokers: [ordered.slice(0, 4), ordered.slice(4, 8), ordered.slice(8, 12)], client: ordered.slice(12) };
    provenance.host = { cpu_model: os.cpus()[0].model, logical_cpus: info.NCPU,
      kernel: os.release(), memory_total_bytes: info.MemTotal, available_memory_bytes: memoryAvailable,
      free_disk_bytes: freeDisk, docker_free_disk_bytes: dockerFreeDisk, topology,
      load_average: os.loadavg(), docker_version: info.ServerVersion,
      aio_max_nr: aioMax, aio_nr: aioUsed,
      javac, java, filesystem: await command('df', ['-T', ROOT]),
      storage: await storageLayout([ROOT, info.DockerRootDir]),
    };
    provenance.runner = {
      repository_commit: await command('git', ['-C', ROOT, 'rev-parse', 'HEAD']),
      source_hashes: Object.fromEntries(await Promise.all(['scripts/benchmark.mjs', 'scripts/benchmark-results.mjs',
        'scripts/benchmark-openmessaging.mjs', 'scripts/benchmark-command.mjs',
        'benchmarks/OpenMessagingMain.java', 'benchmarks/omb-kafka-coalesce-commits.patch',
        'benchmarks/BenchmarkAdmin.java', 'benchmarks/seccomp/krabka-io-uring.json'].map(async name =>
        [name, createHash('sha256').update(await fs.readFile(path.join(ROOT, name))).digest('hex')]))),
    };
    console.log(`benchmark ${runId}: ${provenance.mode}; artifacts ${artifacts}`);
    for (const vendor of VENDORS) {
      const requested = vendor === 'kafka' ? DEFAULT_IMAGES.kafka : options[`${vendor}-image`];
      assert.ok(requested && !requested.startsWith('-') && !/\s/.test(requested), `invalid ${vendor} image`);
      console.log(`Resolving ${vendor}: ${requested}`);
      await docker(['pull', '--platform', 'linux/amd64', requested], { timeout: 600_000, output: path.join(artifacts, `${vendor}.pull`) });
      const inspected = JSON.parse(await docker(['image', 'inspect', requested]))[0];
      assert.ok(inspected.Architecture === 'amd64' && inspected.Os === 'linux', 'image must be native Linux/amd64');
      const repository = requested.split('@')[0].replace(/:[^/:]+$/, '');
      const reference = requested.includes('@sha256:') ? requested
        : inspected.RepoDigests.find(d => d.startsWith(`${repository}@`)) ?? inspected.RepoDigests[0];
      assert.match(reference ?? '', /@sha256:[a-f0-9]{64}$/, 'registry image digest unavailable');
      const pinned = JSON.parse(await docker(['image', 'inspect', reference]))[0];
      assert.equal(pinned.Id, inspected.Id, 'image changed during resolution');
      provenance.images[vendor] = { requested, reference, image_id: inspected.Id, labels: inspected.Config.Labels };
    }
    // An SSD that has not been told which blocks are free slows down as it
    // fills with deleted data, so trials that run hours apart do not start
    // from the same disk. Trimming before every trial gives each trial the
    // same starting state. See "SSD state" in benchmarks/README.md.
    provenance.disk_trim = { requested: !options['no-fstrim'], mounts: [], trials: 0, unavailable: null };
    if (provenance.disk_trim.requested) {
      try {
        const mounts = await Promise.all([ROOT, info.DockerRootDir].map(location =>
          command('findmnt', ['--noheadings', '--output', 'TARGET', '--target', location])));
        provenance.disk_trim.mounts = [...new Set(mounts)]
          .map(mount => ({ mount, trials: 0, trimmed_bytes: 0, unavailable: null }));
      } catch (error) {
        provenance.disk_trim.unavailable = error.message.split('\n')[0];
        provenance.contract.disk_trim_before_trial = false;
        console.warn(`benchmark: running untrimmed, cannot find the mounts to trim: ${provenance.disk_trim.unavailable}`);
      }
    }
    await save();
    if (options['dry-run']) {
      provenance.status = 'dry-run';
      await save();
      console.log(JSON.stringify(provenance, null, 2));
      return;
    }

    await fs.copyFile(path.join(ROOT, 'benchmarks', 'BenchmarkAdmin.java'), path.join(artifacts, 'BenchmarkAdmin.java'));
    const kafkaImage = provenance.images.kafka.reference;
    const jarContainer = await createContainer([kafkaImage], 'client-jars');
    await docker(['cp', `${jarContainer}:/opt/kafka/libs`, path.join(artifacts, 'libs')]);
    await removeContainer(jarContainer);
    const classes = path.join(artifacts, 'classes');
    await fs.mkdir(classes);
    await command('javac', ['--release', '17', '-cp', `${artifacts}/libs/*`, '-d', classes,
      `${artifacts}/BenchmarkAdmin.java`],
    { output: path.join(artifacts, 'compile') });
    const jars = (await fs.readdir(path.join(artifacts, 'libs'))).filter(f => f.endsWith('.jar')).sort();
    provenance.client_jars = Object.fromEntries(await Promise.all(jars.map(async name =>
      [name, createHash('sha256').update(await fs.readFile(path.join(artifacts, 'libs', name))).digest('hex')])));
    provenance.client_java = await oneShot(['--entrypoint', 'java', kafkaImage, '--version'], 'client-version');
    await save();

    let ombRuntime;
    {
      console.log(`Building OpenMessaging ${OMB.commit}`);
      ombRuntime = await prepareOmb(artifacts, command, oneShot);
      provenance.openmessaging = ombRuntime.provenance;
      for (const workload of cases) {
        const source = await fs.readFile(path.join(ombRuntime.source, workload.upstream_file), 'utf8');
        const effective = ombWorkload(source, options.smoke, testDuration);
        Object.assign(workload, { config: effective.config, source_sha256: effective.sha256,
          timeout_ms: ombTimeoutMs(effective.config, options.smoke) });
        await fs.writeFile(path.join(artifacts, `${workload.id}.yaml`), effective.yaml);
      }
      await save();
    }

    const clientArgs = (extra = []) => [
      '--network', network, '--cpuset-cpus', provenance.cpu_sets.client.join(','),
      '--cpus', String(provenance.cpu_sets.client.length), '--memory', '4g', '--memory-swap', '4g',
      '--ulimit', 'nofile=131072:131072', '--volume', `${classes}:/bench/classes:ro`,
      ...extra,
      '--entrypoint', 'java', kafkaImage, '-Xms256m', '-Xmx2g', '-cp', '/bench/classes:/opt/kafka/libs/*',
    ];
    const runClient = (args, label, output, timeout = 120_000) => oneShot([...clientArgs(), ...args], label, { output, timeout });

    async function launchCluster(vendor, rf, repetition, directory, budget = {
      broker_memory_bytes: 10 * GIB, kafka_heap_bytes: GIB, redpanda_memory_bytes: 8 * GIB,
      redpanda_reserve_bytes: GIB,
    }) {
      await awaitAvailableMemory(rf * budget.broker_memory_bytes + 4 * GIB, `RF${rf} cluster startup`);
      resourceCreationStarted = true;
      network = await docker(['network', 'create', '--label', `krabka.benchmark=${runId}`, `krabka-bench-${runId}-${vendor}-rf${rf}-${repetition}`]);
      const names = Array.from({ length: rf }, (_, i) => `broker-${i}`);
      const image = provenance.images[vendor].reference;
      const clusterId = randomUUID();
      const directories = names.map(() => randomUUID());
      const initialControllers = names.map((name, i) => `${i}@${name}:9093:${directories[i]}`).join(',');
      const voters = names.map((name, i) => `${i}@${name}:9093`).join(',');
      const brokers = [];
      for (let i = 0; i < rf; i++) {
        const volume = `krabka-bench-${runId}-${vendor}-rf${rf}-${repetition}-${i}`;
        await docker(['volume', 'create', '--label', `krabka.benchmark=${runId}`, volume]);
        volumes.add(volume);
        const uid = { krabka: 65532, kafka: 1000, redpanda: 101 }[vendor];
        await oneShot(['--user', '0', '--volume', `${volume}:/data`, '--entrypoint', '/bin/bash', kafkaImage,
          '-c', 'chown "$1:$1" /data', '--', String(uid)], 'volume-permissions');
        const dataPath = { krabka: '/data', kafka: '/var/lib/kafka/data', redpanda: '/var/lib/redpanda/data' }[vendor];
        const common = ['--hostname', names[i], '--network', network, '--network-alias', names[i],
          '--cpuset-cpus', provenance.cpu_sets.brokers[i].join(','), '--cpus', '4',
          '--memory', String(budget.broker_memory_bytes), '--memory-swap', String(budget.broker_memory_bytes), '--ulimit', 'nofile=131072:131072',
          '--volume', `${volume}:${dataPath}`];
        let args;
        if (vendor === 'krabka') {
          await oneShot(['--network', network, '--volume', `${volume}:/data`, '--entrypoint', '/usr/bin/krabka-format', image,
            '--log-dir=/data', `--cluster-id=${clusterId}`, `--node-id=${i}`, `--initial-controllers=${initialControllers}`],
          `rf${rf}-${vendor}-${repetition}-format-${i}`, { output: path.join(directory, `format-${i}`) });
          args = [...common, '--env', 'RUST_LOG=warn', '--env', 'OTEL_SDK_DISABLED=true',
            '--env', `KRABKA_CONTROLLER_QUORUM_VOTERS=${voters}`, '--env', 'KRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY=1ms',
            ...options['krabka-env'].flatMap(setting => ['--env', setting]),
            ...(options['krabka-io-uring']
              ? ['--security-opt', `seccomp=${path.join(ROOT, 'benchmarks/seccomp/krabka-io-uring.json')}`] : []),
            image,
            '--log-dir=/data', `--broker-id=${i}`, `--cluster-id=${clusterId}`, '--process-roles=controller,broker',
            '--listen-addr=0.0.0.0:9092', `--advertised-listener=${names[i]}:9092`,
            `--offsets-topic-replication-factor=${rf}`, `--transaction-state-replication-factor=${rf}`,
            `--transaction-state-min-isr=${rf === 1 ? 1 : 2}`];
        } else if (vendor === 'kafka') {
          const env = { KAFKA_NODE_ID: i, KAFKA_PROCESS_ROLES: 'broker,controller', KAFKA_CONTROLLER_QUORUM_VOTERS: voters,
            KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093', KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://${names[i]}:9092`,
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
            KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER', KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: rf, KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: rf,
            KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: rf === 1 ? 1 : 2, KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0,
            KAFKA_HEAP_OPTS: `-Xms${budget.kafka_heap_bytes / 1024 ** 2}m -Xmx${budget.kafka_heap_bytes / 1024 ** 2}m`, KAFKA_LOG_DIRS: '/var/lib/kafka/data',
            CLUSTER_ID: Buffer.from(clusterId.replaceAll('-', ''), 'hex').toString('base64url'),
          };
          args = [...common, ...Object.entries(env).flatMap(([key, value]) => ['--env', `${key}=${value}`]), image];
        } else {
          // Supply an explicit config: the shipped Docker config enables
          // developer_mode, while dev-container also bypasses fsync entirely.
          const config = path.join(directory, `redpanda-${i}.yaml`);
          await fs.writeFile(config, `redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: ${i}
  developer_mode: false
  seed_servers: [${names.map(n => `{host: {address: ${n}, port: 33145}}`).join(', ')}]
  rpc_server: {address: 0.0.0.0, port: 33145}
  advertised_rpc_api: {address: ${names[i]}, port: 33145}
  kafka_api: [{address: 0.0.0.0, port: 9092}]
  advertised_kafka_api: [{address: ${names[i]}, port: 9092}]
  admin: [{address: 0.0.0.0, port: 9644}]
rpk:
  enable_memory_locking: false
`);
          // rpk rewrites its config atomically and preserves its owner. Keep
          // the config in the disposable volume, owned by the Redpanda user.
          await oneShot(['--user', '0', '--volume', `${volume}:/data`, '--volume', `${config}:/input.yaml:ro`,
            '--entrypoint', '/bin/bash', kafkaImage, '-c',
            'cp /input.yaml /data/redpanda.yaml && chown 101:101 /data/redpanda.yaml'], 'redpanda-config');
          args = [...common, image, 'redpanda', 'start', '--config=/var/lib/redpanda/data/redpanda.yaml',
            '--smp=4', `--memory=${budget.redpanda_memory_bytes / 1024 ** 2}M`,
            `--reserve-memory=${budget.redpanda_reserve_bytes / 1024 ** 2}M`, '--overprovisioned', '--check=false',
            '--max-networking-io-control-blocks=1024', '--default-log-level=warn'];
        }
        const id = await createContainer(args, `rf${rf}-${vendor}-${repetition}-broker-${i}`);
        brokers.push({ id, name: names[i] });
        await docker(['start', id]);
      }
      await fs.writeFile(path.join(directory, 'containers.json'), `${await docker(['inspect', ...brokers.map(b => b.id)])}\n`);
      for (const broker of brokers) {
        const state = JSON.parse(await docker(['inspect', broker.id]))[0].State;
        if (!state.Running || state.OOMKilled) {
          throw new Error(`${vendor} broker failed startup: ${await docker(['logs', broker.id]).catch(() => 'see container logs')}`);
        }
      }
      await runClient(['BenchmarkAdmin', 'ready', 'broker-0:9092', String(rf)], 'ready', path.join(directory, 'ready'));
      for (const broker of brokers) {
        const inspected = JSON.parse(await docker(['inspect', broker.id]))[0];
        assert.ok(inspected.State.Running && !inspected.State.OOMKilled, `${vendor} broker failed startup`);
        const pid = inspected.State.Pid;
        const cgroup = (await fs.readFile(`/proc/${pid}/cgroup`, 'utf8')).match(/^0::(.+)$/m)?.[1];
        assert.ok(cgroup, 'broker cgroup is inaccessible; Docker must be local');
        broker.cgroup = path.join('/sys/fs/cgroup', cgroup);
        // Docker sets the limit at creation, but systemd applies a scope's
        // properties moments after it starts: one Kafka trial on the cloud
        // runners read `max` here with the 10 GiB swap limit recorded in the
        // container's config, while the other vendors' trials of the same
        // shard read `0`. Read again for a few seconds before failing the trial.
        let swapLimit;
        for (let attempt = 0; ; attempt++) {
          swapLimit = (await fs.readFile(path.join(broker.cgroup, 'memory.swap.max'), 'utf8')).trim();
          if (swapLimit === '0' || attempt === 9) break;
          await delay(500);
        }
        assert.equal(swapLimit, '0', 'broker swap must be disabled');
        await readBroker(broker); // Verify access to every required counter.
      }
      return brokers;
    }

    // During the OMB measurement, read each krabka broker's open files from a
    // sidecar that shares its PID namespace and user, and check that every
    // benchmark partition replica holds its newest segment open with O_DIRECT.
    // The topic setting or a successful command alone does not prove the mode:
    // an unsupported kernel or filesystem falls back to buffered writes.
    async function verifyDirectIo(brokers, directory, workload, rf, signal) {
      const stdout = path.join(directory, 'workload.stdout');
      while (!(await fs.readFile(stdout, 'utf8').catch(() => '')).includes('----- Starting benchmark traffic')) {
        await delay(2_000, undefined, { signal });
      }
      // Let the producers reach every partition before the first listing:
      // 30 s into a full workload, a sixth of a shorter one. From here the
      // check runs to its end whether or not the measurement is still going;
      // the brokers stay up until it returns, and a descriptor's flags do not
      // change, so a listing taken after the measurement is the same
      // evidence. The smoke run 40436211985 lost the 16- and 100-partition
      // cells to the measurement ending inside this check.
      const durationMs = workload.config.testDurationMinutes * 60_000;
      await delay(Math.min(30_000, Math.max(5_000, Math.floor(durationMs / 6))), undefined, { signal });
      const user = JSON.parse(await docker(['image', 'inspect', provenance.images.krabka.reference]))[0].Config.User || '0';
      // One `ls` and shell built-ins: a process per descriptor took about a
      // second for 150 descriptors, longer than a segment lives at a maximum
      // rate, so the listing mixed the descriptors of consecutive segments.
      const script = 'ls -l /proc/1/fd 2>/dev/null | while read -r _ _ _ _ _ _ _ _ fd _ t; do case $t in /data/*.log) '
        + 'f=; while IFS=$\'\\t\' read -r k v; do [ "$k" = flags: ] && f=$v; done < /proc/1/fdinfo/$fd 2>/dev/null; '
        + 's=$(stat -L -c %s /proc/1/fd/$fd 2>/dev/null); echo "$t $f ${s:-?}";; esac; done';
      // At a high rate a segment can fill in about a second, and a listing
      // taken just after a roll shows the new segment before its O_DIRECT
      // descriptor opens. Each partition replica passes once one of up to ten
      // listings, 3 s apart, shows its newest segment open with O_DIRECT, or
      // is left unwritten when the last listing shows no bytes in it (see
      // directIoCheck).
      const brokersChecked = brokers.map(broker => ({ broker: broker.name, partitions: 0, verified: new Set(), unwritten: [], attempts: 0 }));
      const settled = checked => checked.verified.size + checked.unwritten.length === checked.partitions;
      for (let attempt = 1; attempt <= 10; attempt++) {
        for (const [i, broker] of brokers.entries()) {
          const checked = brokersChecked[i];
          if (attempt > 1 && settled(checked)) continue;
          const listing = await oneShot(['--pid', `container:${broker.id}`, '--user', user, '--network', 'none',
            '--cpuset-cpus', provenance.cpu_sets.client.join(','), '--entrypoint', '/bin/bash',
            provenance.images.kafka.reference, '-c', script], `direct-io-${i}`);
          await fs.appendFile(path.join(directory, `direct-io-broker-${i}.txt`), `# attempt ${attempt}\n${listing}\n`);
          const result = directIoCheck(listing, os.arch());
          checked.partitions = Math.max(checked.partitions, result.partitions);
          for (const partition of result.verified) checked.verified.add(partition);
          checked.unwritten = result.unwritten.filter(partition => !checked.verified.has(partition));
          checked.attempts = attempt;
        }
        if (brokersChecked.every(settled)) break;
        await delay(3_000);
      }
      // Decide before the sets become the arrays the report carries.
      const allSettled = brokersChecked.every(settled);
      for (const checked of brokersChecked) {
        checked.direct = checked.verified.size;
        checked.verified = [...checked.verified].sort();
      }
      const expected = workload.config.topics * workload.config.partitionsPerTopic * rf;
      const found = brokersChecked.reduce((n, b) => n + b.partitions, 0);
      return { checked_at: new Date().toISOString(), arch: os.arch(), expected_partition_replicas: expected,
        found_partition_replicas: found, brokers: brokersChecked, verified: found === expected && allSettled };
    }

    async function readBroker(broker) {
      const files = ['cpu.stat', 'memory.current', 'memory.stat', 'memory.events', 'cgroup.procs'];
      const [cpu, memory, stats, events, procs] = await Promise.all(files.map(file => fs.readFile(path.join(broker.cgroup, file), 'utf8')));
      // Pressure stall information is missing on kernels built without PSI,
      // and some memory.stat counters arrived with later kernels; those
      // fields are null rather than a failed sample.
      const [memoryPressure, ioPressure, cpuPressure] = await Promise.all(['memory.pressure', 'io.pressure', 'cpu.pressure']
        .map(file => fs.readFile(path.join(broker.cgroup, file), 'utf8').catch(() => '')));
      const counter = (text, key) => {
        const match = text.match(new RegExp(`^${key} (\\d+)$`, 'm'));
        assert.ok(match, `resource counter unavailable: ${key}`);
        return Number(match[1]);
      };
      const optional = (text, key) => {
        const match = text.match(new RegExp(`^${key} (\\d+)$`, 'm'));
        return match ? Number(match[1]) : null;
      };
      const stall = (text, kind) => {
        const match = text.match(new RegExp(`^${kind} .*total=(\\d+)$`, 'm'));
        return match ? Number(match[1]) : null;
      };
      const pids = procs.trim().split(/\s+/).filter(Boolean);
      assert.ok(pids.length > 0, 'broker exited while sampling');
      let rss = 0;
      for (const pid of pids) {
        const status = await fs.readFile(`/proc/${pid}/status`, 'utf8');
        const match = status.match(/^VmRSS:\s+(\d+) kB$/m);
        assert.ok(match, `process RSS unavailable: ${pid}`);
        rss += Number(match[1]) * 1024;
      }
      return { id: broker.id, cpu_usage_us: counter(cpu, 'usage_usec'), rss_bytes: rss,
        cpu_user_us: counter(cpu, 'user_usec'), cpu_system_us: counter(cpu, 'system_usec'),
        anon_bytes: counter(stats, 'anon'), memory_current_bytes: Number(memory.trim()),
        inactive_file_bytes: counter(stats, 'inactive_file'), oom_kill: counter(events, 'oom_kill'),
        // Page-cache reclaim inside the broker's cgroup: the file pages the
        // kernel holds for it, the pages it scanned and freed to stay under
        // the limit, how often the limit was reached, and the time its tasks
        // stalled on memory, I/O and CPU (PSI "some" and "full" totals, in
        // microseconds).
        active_file_bytes: optional(stats, 'active_file'), file_dirty_bytes: optional(stats, 'file_dirty'),
        file_writeback_bytes: optional(stats, 'file_writeback'), pgscan: optional(stats, 'pgscan'), pgsteal: optional(stats, 'pgsteal'),
        memory_max_events: optional(events, 'max'), memory_high_events: optional(events, 'high'),
        memory_stall_us: stall(memoryPressure, 'some'), memory_stall_full_us: stall(memoryPressure, 'full'),
        io_stall_us: stall(ioPressure, 'some'), cpu_stall_us: stall(cpuPressure, 'some') };
    }

    async function measureResources(brokers, filename, action, poll = async () => {}) {
      const samples = [];
      const startedAt = new Date().toISOString();
      const start = performance.now();
      // Retain the clock anchor even when the client or sampler fails.
      await fs.writeFile(`${filename}.metadata.json`, `${JSON.stringify({
        schema_version: 1, started_at: startedAt, sampling_interval_ms: 250,
      })}\n`);
      const sampleFile = await fs.open(filename, 'w');
      // Block device counters for the storage under the brokers, sampled with
      // the cgroups, so device use can be read over the OMB measurement window.
      const deviceNames = provenance.host.storage.sampled_devices;
      const deviceSamples = [];
      const hostSamples = [];
      const deviceFile = await fs.open(path.join(path.dirname(filename), 'devices.jsonl'), 'w');
      // Host memory and reclaim counters, sampled with the cgroups, in
      // host.jsonl: a broker whose page cache fills its cgroup stalls in the
      // kernel's reclaim, and the cgroup counters alone cannot tell that
      // from host-wide memory pressure. Pressure stall information is null on
      // kernels built without PSI. Anonymous, shared and unreclaimable slab
      // memory, the virtio balloon counters and stolen CPU time tell a
      // consumer outside every cgroup, or the hypervisor itself, from the
      // brokers when free memory disappears mid-trial.
      const hostFile = await fs.open(path.join(path.dirname(filename), 'host.jsonl'), 'w');
      async function readHost() {
        const [meminfo, vmstat, stat, memoryPressure, ioPressure] = await Promise.all([
          fs.readFile('/proc/meminfo', 'utf8'), fs.readFile('/proc/vmstat', 'utf8'), fs.readFile('/proc/stat', 'utf8'),
          fs.readFile('/proc/pressure/memory', 'utf8').catch(() => ''), fs.readFile('/proc/pressure/io', 'utf8').catch(() => '')]);
        const bytes = key => {
          const match = meminfo.match(new RegExp(`^${key.replace(/[()]/g, '\\$&')}:\\s+(\\d+) kB$`, 'm'));
          return match ? Number(match[1]) * 1024 : null;
        };
        const count = key => {
          const match = vmstat.match(new RegExp(`^${key} (\\d+)$`, 'm'));
          return match ? Number(match[1]) : null;
        };
        const stall = (text, kind) => {
          const match = text.match(new RegExp(`^${kind} .*total=(\\d+)$`, 'm'));
          return match ? Number(match[1]) : null;
        };
        // The eighth field of the aggregate cpu line counts ticks the hypervisor
        // ran another guest while this one was runnable.
        const steal = stat.match(/^cpu\s+(?:\d+\s+){7}(\d+)/m);
        return { mem_free_bytes: bytes('MemFree'), mem_available_bytes: bytes('MemAvailable'), cached_bytes: bytes('Cached'),
          dirty_bytes: bytes('Dirty'), writeback_bytes: bytes('Writeback'),
          active_file_bytes: bytes('Active(file)'), inactive_file_bytes: bytes('Inactive(file)'),
          anon_bytes: bytes('AnonPages'), shmem_bytes: bytes('Shmem'), slab_unreclaimable_bytes: bytes('SUnreclaim'),
          pgscan_kswapd: count('pgscan_kswapd'), pgscan_direct: count('pgscan_direct'),
          pgsteal_kswapd: count('pgsteal_kswapd'), pgsteal_direct: count('pgsteal_direct'),
          allocstall: [...vmstat.matchAll(/^allocstall_\w+ (\d+)$/gm)].reduce((sum, match) => sum + Number(match[1]), 0),
          balloon_inflate: count('balloon_inflate'), balloon_deflate: count('balloon_deflate'),
          cpu_steal_ticks: steal ? Number(steal[1]) : null,
          memory_stall_us: stall(memoryPressure, 'some'), memory_stall_full_us: stall(memoryPressure, 'full'),
          io_stall_us: stall(ioPressure, 'some') };
      }
      const samplingController = new AbortController();
      let sampleError;
      async function sample() {
        const [brokersSampled, diskstats, host] = await Promise.all([Promise.all(brokers.map(readBroker)), fs.readFile('/proc/diskstats', 'utf8'), readHost()]);
        aggregateSample(brokersSampled);
        const elapsed = performance.now() - start;
        const current = { elapsed_ms: elapsed, brokers: brokersSampled };
        const device = { elapsed_ms: elapsed, devices: parseDiskstats(diskstats, deviceNames) };
        const hostSample = { elapsed_ms: elapsed, host };
        samples.push(current);
        deviceSamples.push(device);
        hostSamples.push(hostSample);
        await sampleFile.write(`${JSON.stringify(current)}\n`);
        await deviceFile.write(`${JSON.stringify(device)}\n`);
        await hostFile.write(`${JSON.stringify(hostSample)}\n`);
      }
      const closeFiles = () => Promise.all([sampleFile.close(), deviceFile.close(), hostFile.close()]);
      try { await sample(); }
      catch (error) { await closeFiles(); throw error; }
      const sampling = (async () => {
        while (!samplingController.signal.aborted) {
          await delay(250, undefined, { signal: samplingController.signal }).catch(error => {
            if (error.name !== 'AbortError') throw error;
          });
          if (!samplingController.signal.aborted) {
            await sample();
            await poll();
          }
        }
      })().catch(error => { sampleError = error; controller.abort(error); });
      let output;
      try {
        output = await action();
      } finally {
        samplingController.abort();
        await sampling;
        try { if (!sampleError) await sample(); }
        finally { await closeFiles(); }
      }
      if (sampleError) throw sampleError;
      return { output, samples, deviceSamples, hostSamples, startedAt };
    }

    // Run each OMB workload on fresh clusters.
    {
      for (const rf of replicationFactors) for (let repetition = 1; repetition <= repetitions; repetition++) {
        const offset = (repetition - 1) % vendors.length;
        const order = [...vendors.slice(offset), ...vendors.slice(0, offset)];
        for (const workload of cases) for (const vendor of order) {
          const directory = path.join(artifacts, `rf${rf}-${vendor}-${repetition}-${workload.id}`);
          await fs.mkdir(directory);
          console.log(`OMB RF${rf} ${vendor} ${workload.id} round ${repetition}/${repetitions}`);
          try {
            await trimDisks(directory);
            const brokers = await launchCluster(vendor, rf, `${repetition}-${workload.id}`, directory);
            await fs.copyFile(path.join(artifacts, `${workload.id}.yaml`), path.join(directory, 'workload.yaml'));
            await fs.copyFile(path.join(ombRuntime.source, workload.upstream_file), path.join(directory, 'upstream-workload.yaml'));
            // OMB pauses inside the message callback during backlog fill, so
            // Kafka's poll loop must survive that pause without leaving its group.
            await fs.writeFile(path.join(directory, 'driver.yaml'), ombDriver(vendor, rf,
              workload.config.consumerBacklogSizeGB > 0 ? workload.timeout_ms : undefined, ombRetentionBytes(workload.config),
              vendor === 'krabka' && options['krabka-preallocate']));
            const timeout = workload.timeout_ms;
            const directAbort = new AbortController();
            const directIo = vendor === 'krabka' && options['krabka-preallocate']
              ? verifyDirectIo(brokers, directory, workload, rf, directAbort.signal).catch(error => ({ verified: false,
                error: error.name === 'AbortError' ? 'the OMB measurement ended before traffic started' : error.message }))
              : null;
            const measurement = await measureResources(brokers, path.join(directory, 'resources.jsonl'),
              () => oneShot(['--network', network, '--user', `${process.getuid()}:${process.getgid()}`,
                '--cpuset-cpus', provenance.cpu_sets.client.join(','), '--cpus', String(provenance.cpu_sets.client.length),
                '--memory', '4g', '--memory-swap', '4g', '--ulimit', 'nofile=131072:131072',
                '--volume', `${ombRuntime.source}:/src:ro`, '--volume', `${ombRuntime.m2}:/m2:ro`,
                '--volume', `${path.join(ROOT, 'benchmarks/OpenMessagingMain.java')}:/bench/OpenMessagingMain.java:ro`,
                '--volume', `${directory}:/output`, '--workdir', '/src', '--entrypoint', 'java', OMB.build_image,
                '-Xms256m', '-Xmx2g', '-cp', ombRuntime.classpath, '/bench/OpenMessagingMain.java',
                '--drivers', '/output/driver.yaml', '--output', '/output/result.json', '/output/workload.yaml'],
              `omb-${vendor}-${rf}-${workload.id}`, { output: path.join(directory, 'workload'), timeout, streamOutput: true }),
            async () => {
              // Duration-based maximum-rate workloads have no record ceiling.
              // Abort before filling a shared disk, including Docker storage.
              for (const location of [ROOT, info.DockerRootDir]) {
                const disk = await fs.statfs(location);
                assert.ok(disk.bavail * disk.bsize > (options.smoke ? 1 : 20) * GIB,
                  `OMB disk reserve exhausted at ${location}`);
              }
            }).finally(() => directAbort.abort());
            const directIoResult = directIo && await directIo;
            if (directIoResult) {
              await fs.writeFile(path.join(directory, 'direct-io.json'), `${JSON.stringify(directIoResult, null, 2)}\n`);
              assert.ok(directIoResult.verified, `O_DIRECT not verified: ${directIoResult.error
                ?? `${directIoResult.found_partition_replicas} of ${directIoResult.expected_partition_replicas} partition replicas found, `
                  + `${directIoResult.brokers.reduce((n, b) => n + b.direct, 0)} with their newest segment open with O_DIRECT, `
                  + `${directIoResult.brokers.reduce((n, b) => n + b.unwritten.length, 0)} with no bytes in it yet`}`);
            }
            const result = JSON.parse(await fs.readFile(path.join(directory, 'result.json'), 'utf8'));
            const logs = await Promise.all(['stdout', 'stderr'].map(stream => fs.readFile(path.join(directory, `workload.${stream}`), 'utf8')));
            validateOmbResult(result, vendor, rf, workload.config, logs.join('\n'));
            const timeline = resourceTimeSeries(measurement.samples, measurement.startedAt);
            const metrics = { cpu_seconds: timeline.samples.at(-1).cluster.cpu_seconds,
              rss_peak_bytes: Math.max(...timeline.samples.map(s => s.cluster.rss_bytes)),
              working_set_peak_bytes: Math.max(...timeline.samples.map(s => s.cluster.working_set_bytes)),
              // CPU, messages and device use over the OMB measurement window
              // only; the totals above cover the full capture.
              measurement_window: windowMetrics({ samples: measurement.samples, deviceSamples: measurement.deviceSamples,
                hostSamples: measurement.hostSamples, startedAt: measurement.startedAt,
                window: ombMeasurementWindow(logs[0], measurement.startedAt), result }) };
            const trial = { vendor, rf, repetition, case: workload, omb: result, metrics, time_series: timeline,
              retriable_commit_failures: retriableCommitFailures(logs.join('\n')),
              ...(directIoResult ? { direct_io: directIoResult } : {}) };
            await fs.writeFile(path.join(directory, 'trial.json'), `${JSON.stringify(trial, null, 2)}\n`);
            trials.push(trial);
            console.log(`OMB passed ${vendor} RF${rf} ${workload.id}: ${result.aggregatedEndToEndLatency99pct.toFixed(2)} ms end-to-end p99`);
          } catch (error) {
            controller.signal.throwIfAborted();
            const failure = { vendor, rf, repetition, case: workload, error: error.message, artifacts: path.basename(directory) };
            failures.push(failure);
            await fs.writeFile(path.join(directory, 'failure.json'), `${JSON.stringify(failure, null, 2)}\n`);
            console.error(`OMB failed ${vendor} RF${rf} ${workload.id}: ${error.message}`);
          }
          await cleanup();
          provenance.completed_trials = trials.length;
          provenance.failed_trials = failures.length;
          await save();
        }
      }
    }
    controller.signal.throwIfAborted();
    if (failures.length) {
      await fs.writeFile(path.join(artifacts, 'collection.json'), `${JSON.stringify({
        run_id: runId, status: 'failed', attempted_trials: trials.length + failures.length,
        successful_trials: trials.length, failures, trials }, null, 2)}\n`);
      throw new Error(`Completed ${trials.length + failures.length} benchmark attempts: ${trials.length} passed, ${failures.length} failed. Diagnostics and valid captures retained in ${artifacts}; results not published.`);
    }
    provenance.status = 'complete';
    provenance.completed_at = new Date().toISOString();
    provenance.host.load_average_at_end = os.loadavg();
    await save();
    await writeOmbReport(artifacts, provenance, trials);
    console.log(`OpenMessaging passed: ${trials.length} trials; ${artifacts}/summary.md`);
  } catch (error) {
    provenance.status = interrupted ? 'interrupted' : 'failed';
    provenance.error = error.message;
    provenance.completed_trials = trials.length;
    await save();
    throw error;
  } finally {
    try { await cleanup(); }
    finally {
      process.off('SIGINT', onInt);
      process.off('SIGTERM', onTerm);
      if (lock) {
        await lock.close();
        await fs.rm(path.join(ROOT, '.benchmarks', 'runner.lock'));
      }
    }
  }
}
