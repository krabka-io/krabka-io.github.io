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
import { CASES, VENDORS, aggregateSample, resourceSummary, resourceTimeSeries, validateDelivery, publishResults } from './benchmark-results.mjs';
import { curveCases, curveBudget, curveSummary } from './benchmark-curves.mjs';
import { OMB, ombCases, ombWorkload, ombDriver, prepareOmb, validateOmbResult, writeOmbReport } from './benchmark-openmessaging.mjs';
import { runLoggedCommand } from './benchmark-command.mjs';

const ROOT = fileURLToPath(new URL('../', import.meta.url));
const GIB = 1024 ** 3;
// Apache-2.0, Copyright The Krabka Authors. Download an unmodified, immutable
// upstream workload rather than maintaining another producer/consumer driver.
const WORKLOAD = {
  url: 'https://raw.githubusercontent.com/krabka-io/krabka-broker/c27ed4f7e2c9ba4bf48376ed674bc26b166353f4/packaging/performance/BrokerPerformanceWorkload.java',
  sha256: '159c33cc5be5dcceb233a71250437469a23c82e408f5077f2c5452cc86ea8e89',
  license: 'Apache-2.0',
};
const DEFAULT_IMAGES = {
  krabka: 'ghcr.io/krabka-io/krabka-broker:v0.7.0',
  kafka: 'apache/kafka:4.3.1',
  redpanda: 'docker.redpanda.com/redpandadata/redpanda:v26.2.2',
};
const { values: options } = parseArgs({ options: {
  'krabka-image': { type: 'string', default: DEFAULT_IMAGES.krabka },
  'redpanda-image': { type: 'string', default: DEFAULT_IMAGES.redpanda },
  'suite': { type: 'string', default: 'throughput' },
  'workloads': { type: 'string', default: 'all' },
  'replication-factors': { type: 'string', default: '1,3' },
  'repetitions': { type: 'string' },
  'smoke': { type: 'boolean', default: false },
  'dry-run': { type: 'boolean', default: false },
  'help': { type: 'boolean', short: 'h' },
} });
if (options.help) {
  console.log(`Usage: npm run benchmark -- [--suite throughput|curves|openmessaging] [--smoke] [--dry-run] [--krabka-image REF] [--redpanda-image REF]
Full: 108 measured trials, three repetitions, RF1/RF3, all three vendors.
Curves: 72 trials; RF1 load/memory sweeps and RF3 leader pause/resume; max 16 GiB RAM including client.
OpenMessaging: upstream Kafka driver/catalog; --workloads all|NAME,NAME; --replication-factors 1,3; --repetitions 1..3.
Smoke: all cases once, short durations/counts; never publishes.
Dry run: validates the host and resolves images; starts no containers and publishes nothing.
Requires native Linux/amd64 Docker with cgroup v2, Node >=22.12, JDK >=17,
14 available logical CPUs and 150 GiB free disk for full runs.
Throughput: 34 GiB available RAM, 4 GiB free disk for smoke.
Curves: 20 GiB available RAM, 24 GiB free disk for smoke.
See benchmarks/README.md for methodology and retained artifacts.`);
} else {
  await main().catch(error => { console.error(`benchmark: ${error.message}`); process.exitCode = 1; });
}

async function main() {
  assert.ok(['throughput', 'curves', 'openmessaging'].includes(options.suite), 'suite must be throughput, curves, or openmessaging');
  const omb = options.suite === 'openmessaging';
  assert.ok(omb || (options.workloads === 'all' && options['replication-factors'] === '1,3' && options.repetitions === undefined),
    '--workloads, --replication-factors and --repetitions require --suite openmessaging');
  const replicationFactors = options['replication-factors'].split(',').map(Number);
  assert.ok(replicationFactors.length && new Set(replicationFactors).size === replicationFactors.length
    && replicationFactors.every(rf => [1, 3].includes(rf)), 'replication-factors must be 1, 3, or 1,3');
  const curves = options.suite === 'curves';
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
  const repetitions = options.smoke ? 1 : Number(options.repetitions ?? (omb ? 1 : 3));
  assert.ok(Number.isInteger(repetitions) && repetitions >= 1 && repetitions <= 3, 'repetitions must be 1..3');
  const cases = omb ? ombCases(options.workloads) : curves ? curveCases(options.smoke)
    : CASES.map(c => options.smoke ? { ...c, records: c.bytes > 1024 ? 200 : 2000 } : { ...c });
  const provenance = {
    schema_version: 1, suite: options.suite, run_id: runId, started_at: new Date().toISOString(),
    mode: options.smoke ? 'smoke' : 'full', status: 'running', repetitions,
    images: {}, cases, workload_source: WORKLOAD,
    contract: { partitions: 12, acks: 'all', idempotence: true, batch_bytes: 65536, linger_ms: 5,
      broker_cpus: 4, broker_memory_bytes: 10 * GIB, swap_bytes: 0, nofile: 131072,
      kafka_heap_bytes: GIB, redpanda_shards: 4, redpanda_memory_bytes: 8 * GIB,
      redpanda_reserve_bytes: GIB, redpanda_write_caching: true,
      redpanda_network_iocbs_per_shard: 1024,
      client_memory_bytes: 4 * GIB, client_heap_bytes: 2 * GIB,
      warmup_records: options.smoke ? { 1: 2000, 3: 2000 } : { 1: 3_000_000, 3: 1_000_000 },
      resource_sampling_ms: 250, workload_timeout_seconds: 900,
      durability: 'buffered writes; no identical crash-durability claim',
    },
  };
  if (omb) {
    provenance.workload_source = OMB;
    provenance.replication_factors = replicationFactors;
    Object.assign(provenance.contract, { partitions: 'from upstream workload', batch_bytes: 1048576, linger_ms: 1,
      compression: 'none', max_in_flight_requests: 1, client_heap_bytes: 2 * GIB,
      delivery_verification: 'OMB rates/latencies only; no sequence or exact delivery check',
      warmup_minutes: options.smoke ? 0 : 1, smoke_overrides: options.smoke ? { minutes: 1, rate: 5000, backlog_gb: 0 } : null });
    delete provenance.contract.warmup_records;
    delete provenance.contract.workload_timeout_seconds;
  }
  if (curves) {
    provenance.workload_source = { path: 'benchmarks/BenchmarkTimeline.java', license: 'Apache-2.0',
      sha256: createHash('sha256').update(await fs.readFile(path.join(ROOT, 'benchmarks/BenchmarkTimeline.java'))).digest('hex') };
    delete provenance.contract.warmup_records;
    delete provenance.contract.workload_timeout_seconds;
    Object.assign(provenance.contract, { broker_memory_bytes: 4 * GIB,
      redpanda_memory_bytes: 3 * GIB, redpanda_reserve_bytes: GIB / 2,
      required_available_memory_bytes: 20 * GIB, maximum_cluster_and_client_memory_bytes: 16 * GIB,
      workload_sampling_ms: 1000, workload_max_records: options.smoke ? 16_000_000 : 64_000_000,
      warmup_seconds: options.smoke ? 2 : 5, drain_timeout_seconds: 60, workload_deadline_extra_seconds: 120,
      producer_max_block_ms: 30000, producer_request_timeout_ms: 10000, producer_delivery_timeout_ms: 45000,
      budget_source: 'per-trial budget overrides base broker allocations',
      lag_definition: 'max(0, acknowledged - consumed); offered backlog includes unsent scheduled records',
      fault: 'pause/resume broker leading partition 0; same containers and volumes',
      latency: 'HDR 3 significant digits, microseconds; scheduled send to ack/consume; measured cohort includes drain',
    });
  }
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
    const meminfo = await fs.readFile('/proc/meminfo', 'utf8');
    const memoryAvailable = Number(meminfo.match(/^MemAvailable:\s+(\d+)/m)[1]) * 1024;
    const requiredMemory = (curves ? 20 : 34) * GIB;
    assert.ok(memoryAvailable >= requiredMemory, `need ${requiredMemory / GIB} GiB available RAM; found ${(memoryAvailable / GIB).toFixed(1)} GiB`);
    const disk = await fs.statfs(ROOT);
    const freeDisk = disk.bavail * disk.bsize;
    // OMB backlog files retain 100 GB logical data with RF3. Allow replica
    // storage, headers and drain traffic; fresh volumes bound accumulation.
    const ombBacklog = omb && cases.some(c => c.id.startsWith('backlog-'));
    const requiredDisk = (options.smoke ? (curves ? 24 : 4) : ombBacklog ? 450 : 150) * GIB;
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
    };
    provenance.runner = {
      repository_commit: await command('git', ['-C', ROOT, 'rev-parse', 'HEAD']),
      source_hashes: Object.fromEntries(await Promise.all(['scripts/benchmark.mjs', 'scripts/benchmark-results.mjs',
        'scripts/benchmark-curves.mjs', 'scripts/benchmark-openmessaging.mjs', 'scripts/benchmark-command.mjs',
        'benchmarks/OpenMessagingMain.java',
        'benchmarks/BenchmarkAdmin.java', 'benchmarks/BenchmarkTimeline.java'].map(async name =>
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
    await save();
    if (options['dry-run']) {
      provenance.status = 'dry-run';
      await save();
      console.log(JSON.stringify(provenance, null, 2));
      return;
    }

    const sourceResponse = await fetch(WORKLOAD.url, { signal: AbortSignal.any([controller.signal, AbortSignal.timeout(30_000)]) });
    assert.ok(sourceResponse.ok, `workload download failed: HTTP ${sourceResponse.status}`);
    const source = Buffer.from(await sourceResponse.arrayBuffer());
    assert.equal(createHash('sha256').update(source).digest('hex'), WORKLOAD.sha256, 'upstream workload hash differs');
    await fs.writeFile(path.join(artifacts, 'BrokerPerformanceWorkload.java'), source);
    await fs.copyFile(path.join(ROOT, 'benchmarks', 'BenchmarkAdmin.java'), path.join(artifacts, 'BenchmarkAdmin.java'));
    await fs.copyFile(path.join(ROOT, 'benchmarks', 'BenchmarkTimeline.java'), path.join(artifacts, 'BenchmarkTimeline.java'));
    const kafkaImage = provenance.images.kafka.reference;
    const jarContainer = await createContainer([kafkaImage], 'client-jars');
    await docker(['cp', `${jarContainer}:/opt/kafka/libs`, path.join(artifacts, 'libs')]);
    await removeContainer(jarContainer);
    const classes = path.join(artifacts, 'classes');
    await fs.mkdir(classes);
    await command('javac', ['--release', '17', '-cp', `${artifacts}/libs/*`, '-d', classes,
      `${artifacts}/BrokerPerformanceWorkload.java`, `${artifacts}/BenchmarkAdmin.java`, `${artifacts}/BenchmarkTimeline.java`],
    { output: path.join(artifacts, 'compile') });
    const jars = (await fs.readdir(path.join(artifacts, 'libs'))).filter(f => f.endsWith('.jar')).sort();
    provenance.client_jars = Object.fromEntries(await Promise.all(jars.map(async name =>
      [name, createHash('sha256').update(await fs.readFile(path.join(artifacts, 'libs', name))).digest('hex')])));
    provenance.client_java = await oneShot(['--entrypoint', 'java', kafkaImage, '--version'], 'client-version');
    await save();

    let ombRuntime;
    if (omb) {
      console.log(`Building OpenMessaging ${OMB.commit}`);
      ombRuntime = await prepareOmb(artifacts, command, oneShot);
      provenance.openmessaging = ombRuntime.provenance;
      for (const workload of cases) {
        const source = await fs.readFile(path.join(ombRuntime.source, workload.upstream_file), 'utf8');
        const effective = ombWorkload(source, options.smoke);
        Object.assign(workload, { config: effective.config, source_sha256: effective.sha256 });
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
      const available = Number((await fs.readFile('/proc/meminfo', 'utf8')).match(/^MemAvailable:\s+(\d+)/m)[1]) * 1024;
      const needed = rf * budget.broker_memory_bytes + 4 * GIB + (curves ? 4 * GIB : 0);
      assert.ok(available >= needed, `RF${rf} needs ${needed / GIB} GiB available RAM before cluster startup`);
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
            '--env', `KRABKA_CONTROLLER_QUORUM_VOTERS=${voters}`, '--env', 'KRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY=1ms', image,
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
        const swapLimit = (await fs.readFile(path.join(broker.cgroup, 'memory.swap.max'), 'utf8')).trim();
        assert.equal(swapLimit, '0', 'broker swap must be disabled');
        await readBroker(broker); // Verify access to every required counter.
      }
      return brokers;
    }

    async function readBroker(broker) {
      const files = ['cpu.stat', 'memory.current', 'memory.stat', 'memory.events', 'cgroup.procs'];
      const [cpu, memory, stats, events, procs] = await Promise.all(files.map(file => fs.readFile(path.join(broker.cgroup, file), 'utf8')));
      const counter = (text, key) => {
        const match = text.match(new RegExp(`^${key} (\\d+)$`, 'm'));
        assert.ok(match, `resource counter unavailable: ${key}`);
        return Number(match[1]);
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
        anon_bytes: counter(stats, 'anon'), memory_current_bytes: Number(memory.trim()),
        inactive_file_bytes: counter(stats, 'inactive_file'), oom_kill: counter(events, 'oom_kill') };
    }

    async function measureResources(brokers, filename, action, poll = async () => {}) {
      const samples = [];
      const startedAt = new Date().toISOString();
      const start = performance.now();
      const sampleFile = await fs.open(filename, 'w');
      const samplingController = new AbortController();
      let sampleError;
      async function sample() {
        const brokersSampled = await Promise.all(brokers.map(readBroker));
        aggregateSample(brokersSampled);
        const current = { elapsed_ms: performance.now() - start, brokers: brokersSampled };
        samples.push(current);
        await sampleFile.write(`${JSON.stringify(current)}\n`);
      }
      try { await sample(); }
      catch (error) { await sampleFile.close(); throw error; }
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
        finally { await sampleFile.close(); }
      }
      if (sampleError) throw sampleError;
      return { output, samples, startedAt };
    }

    async function trial(vendor, rf, repetition, workload, brokers, directory, warmup = false) {
      controller.signal.throwIfAborted();
      const prefix = warmup ? 'warmup' : workload.id;
      const topic = `bench-${prefix}`;
      const topicSettings = await runClient(['BenchmarkAdmin', 'topic', 'broker-0:9092', topic, String(rf),
        String(rf === 1 ? 1 : 2), vendor], 'topic', path.join(directory, `${prefix}.topic`));
      await fs.writeFile(path.join(directory, `${prefix}.topic.json`), `${topicSettings}\n`);
      const { output, samples, startedAt } = await measureResources(brokers,
        path.join(directory, `${prefix}.resources.jsonl`), () => runClient([
          'BrokerPerformanceWorkload', 'broker-0:9092', topic, `${topic}-group`,
          String(workload.records), String(workload.bytes), String(workload.rate), '900',
          workload.compression, workload.payload], 'workload', path.join(directory, `${prefix}.workload`), 960_000));
      const measured = JSON.parse(output);
      validateDelivery(measured, workload.records);
      const metrics = { ...measured, ...resourceSummary(samples, measured.sent) };
      const result = { vendor, rf, min_isr: vendor === 'redpanda' ? null : (rf === 1 ? 1 : 2),
        acknowledgment: vendor === 'redpanda' ? `Raft majority (${rf === 1 ? 1 : 2})` : 'all in-sync replicas', repetition, case: workload,
        workload: measured, metrics, topic: JSON.parse(topicSettings),
        time_series: resourceTimeSeries(samples, startedAt) };
      await fs.writeFile(path.join(directory, `${prefix}.json`), `${JSON.stringify(result, null, 2)}\n`);
      if (!warmup) trials.push(result);
      console.log(`${warmup ? 'Warm-up' : `Trial ${trials.length}`}: RF${rf} ${vendor} round ${repetition} ${prefix}: ${measured.records_per_second.toFixed(0)} records/s, ${metrics.cpu_us_per_record.toFixed(2)} CPU µs/record`);
    }

    async function curveTrial(vendor, repetition, workload, brokers, directory, budget) {
      const rf = workload.rf;
      const topic = `bench-${workload.id}`;
      const bootstrap = brokers.map(b => `${b.name}:9092`).join(',');
      const topicSettings = JSON.parse(await runClient(['BenchmarkAdmin', 'topic', bootstrap, topic,
        String(rf), String(rf === 1 ? 1 : 2), vendor], 'topic', path.join(directory, 'topic')));
      let leader;
      if (workload.kind === 'recovery') {
        leader = JSON.parse(await runClient(['BenchmarkAdmin', 'leader', bootstrap, topic], 'leader', path.join(directory, 'leader')));
        assert.ok(brokers[leader.broker_id], 'leader is outside this run');
      }
      const events = [];
      let workloadStart;
      let paused = false;
      const event = async action => {
        const broker = brokers[leader.broker_id];
        await docker([action, broker.id]);
        paused = action === 'pause';
        events.push({ action, broker_id: leader.broker_id, container_id: broker.id,
          led_partitions: leader.led_partitions, at: new Date().toISOString(),
          elapsed_ms: performance.now() - workloadStart });
        await fs.writeFile(path.join(directory, 'events.json'), `${JSON.stringify(events, null, 2)}\n`);
      };
      const poll = async () => {
        if (!leader) return;
        if (workloadStart === undefined) {
          const header = await fs.readFile(path.join(directory, 'started.json'), 'utf8').catch(error => {
            if (error.code === 'ENOENT') return null;
            throw error;
          });
          if (!header) return;
          workloadStart = performance.now() - (Date.now() - Date.parse(JSON.parse(header).started_at));
        }
        const elapsed = performance.now() - workloadStart;
        if (events.length === 0 && elapsed >= (workload.warmup_seconds + workload.pause_after_seconds) * 1000) {
          await event('pause');
        } else if (paused && elapsed >= events[0].elapsed_ms + workload.pause_seconds * 1000) {
          await event('unpause');
        }
      };
      let measurement;
      try {
        measurement = await measureResources(brokers, path.join(directory, 'resources.jsonl'),
          () => oneShot([...clientArgs(['--user', String(process.getuid()), '--volume', `${directory}:/bench/output`]),
            'BenchmarkTimeline', bootstrap, topic, String(workload.rate), String(workload.warmup_seconds),
            String(workload.seconds), '/bench/output', String(provenance.contract.workload_max_records)], 'timeline',
          { output: path.join(directory, 'workload'), timeout: (workload.warmup_seconds + workload.seconds + 120) * 1000 }), poll);
      } finally {
        if (paused) await docker(['unpause', brokers[leader.broker_id].id], { ignoreAbort: true, timeout: 30_000 });
      }
      const measured = JSON.parse(measurement.output);
      const workloadTimeline = { schema_version: 1,
        ...JSON.parse(await fs.readFile(path.join(directory, 'started.json'), 'utf8')),
        sampling_interval_ms: 1000,
        samples: (await fs.readFile(path.join(directory, 'workload.jsonl'), 'utf8')).trim().split('\n').map(JSON.parse) };
      const result = { vendor, rf, repetition, case: workload, budget, topic: topicSettings,
        workload: measured, workload_time_series: workloadTimeline, events,
        time_series: resourceTimeSeries(measurement.samples, measurement.startedAt),
        metrics: resourceSummary(measurement.samples, measured.sent),
        curve: curveSummary(workload, measured, workloadTimeline, events) };
      if (leader) {
        result.recovered_topic = JSON.parse(await runClient(['BenchmarkAdmin', 'isr', bootstrap, topic,
          String(rf), '2', vendor], 'recovered-isr', path.join(directory, 'recovered-isr')));
      }
      await fs.writeFile(path.join(directory, 'trial.json'), `${JSON.stringify(result, null, 2)}\n`);
      trials.push(result);
      console.log(`Curve ${trials.length}: ${vendor} ${workload.id}, ${workload.memory_gib} GiB/broker: ${result.curve.records_per_second.toFixed(0)} ack records/s, ${result.curve.latency_ms_p99.toFixed(2)} ms p99`);
    }

    if (omb) {
      for (const rf of replicationFactors) for (let repetition = 1; repetition <= repetitions; repetition++) {
        const offset = (repetition - 1) % VENDORS.length;
        const order = [...VENDORS.slice(offset), ...VENDORS.slice(0, offset)];
        for (const workload of cases) for (const vendor of order) {
          const directory = path.join(artifacts, `rf${rf}-${vendor}-${repetition}-${workload.id}`);
          await fs.mkdir(directory);
          console.log(`OMB RF${rf} ${vendor} ${workload.id} round ${repetition}/${repetitions}`);
          try {
            const brokers = await launchCluster(vendor, rf, `${repetition}-${workload.id}`, directory);
            await fs.copyFile(path.join(artifacts, `${workload.id}.yaml`), path.join(directory, 'workload.yaml'));
            await fs.copyFile(path.join(ombRuntime.source, workload.upstream_file), path.join(directory, 'upstream-workload.yaml'));
            await fs.writeFile(path.join(directory, 'driver.yaml'), ombDriver(vendor, rf));
            const timeout = (workload.config.testDurationMinutes + (options.smoke ? 5 : 30)) * 60_000;
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
            });
            const result = JSON.parse(await fs.readFile(path.join(directory, 'result.json'), 'utf8'));
            const logs = await Promise.all(['stdout', 'stderr'].map(stream => fs.readFile(path.join(directory, `workload.${stream}`), 'utf8')));
            validateOmbResult(result, vendor, rf, workload.config, logs.join('\n'));
            const timeline = resourceTimeSeries(measurement.samples, measurement.startedAt);
            const metrics = { cpu_seconds: timeline.samples.at(-1).cluster.cpu_seconds,
              rss_peak_bytes: Math.max(...timeline.samples.map(s => s.cluster.rss_bytes)),
              working_set_peak_bytes: Math.max(...timeline.samples.map(s => s.cluster.working_set_bytes)) };
            const trial = { vendor, rf, repetition, case: workload, omb: result, metrics, time_series: timeline };
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
    } else if (curves) {
      for (let repetition = 1; repetition <= repetitions; repetition++) {
        const offset = (repetition - 1) % VENDORS.length;
        const order = [...VENDORS.slice(offset), ...VENDORS.slice(0, offset)];
        for (const workload of cases) for (const vendor of order) {
          const directory = path.join(artifacts, `rf${workload.rf}-${vendor}-${repetition}-${workload.id}`);
          await fs.mkdir(directory);
          const budget = curveBudget(workload, vendor);
          console.log(`Starting ${vendor} ${workload.id} round ${repetition}/${repetitions}`);
          try {
            const brokers = await launchCluster(vendor, workload.rf, repetition, directory, budget);
            await curveTrial(vendor, repetition, workload, brokers, directory, budget);
          } catch (error) {
            controller.signal.throwIfAborted();
            const failure = { vendor, rf: workload.rf, repetition, case: workload, budget,
              status: 'failed', error: error.message, artifacts: path.basename(directory) };
            await fs.writeFile(path.join(directory, 'failure.json'), `${JSON.stringify(failure, null, 2)}\n`);
            failures.push(failure);
            console.error(`Failed curve ${vendor} ${workload.id} round ${repetition}: ${error.message}`);
          }
          await cleanup();
          provenance.completed_trials = trials.length;
          provenance.attempted_trials = trials.length + failures.length;
          provenance.failed_trials = failures.length;
          await save();
        }
      }
    } else for (const rf of [1, 3]) {
      for (let repetition = 1; repetition <= repetitions; repetition++) {
        const offset = (repetition - 1) % VENDORS.length;
        const order = [...VENDORS.slice(offset), ...VENDORS.slice(0, offset)];
        for (const vendor of order) {
          const directory = path.join(artifacts, `rf${rf}-${vendor}-${repetition}`);
          await fs.mkdir(directory);
          console.log(`Starting RF${rf} ${vendor} round ${repetition}/${repetitions}`);
          const brokers = await launchCluster(vendor, rf, repetition, directory);
          await trial(vendor, rf, repetition, { ...CASES[0], records: provenance.contract.warmup_records[rf] }, brokers, directory, true);
          for (const workload of cases) await trial(vendor, rf, repetition, workload, brokers, directory);
          await cleanup();
          provenance.completed_trials = trials.length;
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
    if (omb) {
      await writeOmbReport(artifacts, provenance, trials);
      console.log(`OpenMessaging passed: ${trials.length} trials; ${artifacts}/summary.md`);
    } else if (options.smoke) console.log(`Smoke passed: ${trials.length} trials. Results retained only in ${artifacts}`);
    else {
      await publishResults(ROOT, provenance, trials, controller.signal);
      console.log(`Published ${trials.length} trials to benchmarks/results/${runId}/ and benchmarks/latest${curves ? '-curves' : ''}.md`);
    }
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
