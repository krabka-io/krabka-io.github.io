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
import { CASES, VENDORS, aggregateSample, resourceSummary, validateDelivery, publishResults } from './benchmark-results.mjs';

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
  krabka: 'ghcr.io/krabka-io/krabka-broker:v0.6.1',
  kafka: 'apache/kafka:4.3.1',
  redpanda: 'docker.redpanda.com/redpandadata/redpanda:v26.2.2',
};
const { values: options } = parseArgs({ options: {
  'krabka-image': { type: 'string', default: DEFAULT_IMAGES.krabka },
  'redpanda-image': { type: 'string', default: DEFAULT_IMAGES.redpanda },
  'smoke': { type: 'boolean', default: false },
  'dry-run': { type: 'boolean', default: false },
  'help': { type: 'boolean', short: 'h' },
} });
if (options.help) {
  console.log(`Usage: npm run benchmark -- [--smoke] [--dry-run] [--krabka-image REF] [--redpanda-image REF]
Full: 108 measured trials, three repetitions, RF1/RF3, all three vendors.
Smoke: same cases and resource limits, one repetition, small record counts; never publishes.
Dry run: validates the host and resolves images; starts no containers and publishes nothing.
Requires native Linux/amd64 Docker with cgroup v2, Node >=22.12, JDK >=17,
14 available logical CPUs, 34 GiB available RAM, and 150 GiB free disk (4 GiB for smoke).
See benchmarks/README.md for methodology and retained artifacts.`);
} else {
  await main().catch(error => { console.error(`benchmark: ${error.message}`); process.exitCode = 1; });
}

async function main() {
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
  const repetitions = options.smoke ? 1 : 3;
  const cases = CASES.map(c => options.smoke ? { ...c, records: c.bytes > 1024 ? 200 : 2000 } : { ...c });
  const provenance = {
    schema_version: 1, run_id: runId, started_at: new Date().toISOString(),
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
  const save = () => fs.writeFile(path.join(artifacts, 'provenance.json'), `${JSON.stringify(provenance, null, 2)}\n`);

  async function command(executable, args, { timeout = 120_000, output, ignoreAbort = false } = {}) {
    if (!ignoreAbort) controller.signal.throwIfAborted();
    await fs.appendFile(path.join(artifacts, 'commands.jsonl'),
      `${JSON.stringify({ at: new Date().toISOString(), executable, args, timeout })}\n`);
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
        await docker(['logs', id], { output: path.join(artifacts, `${label}.container`), ignoreAbort: true, timeout: 30_000 });
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
    assert.ok(memoryAvailable >= 34 * GIB, `need 34 GiB available RAM (three 10 GiB brokers + 4 GiB client); found ${(memoryAvailable / GIB).toFixed(1)} GiB`);
    const disk = await fs.statfs(ROOT);
    const freeDisk = disk.bavail * disk.bsize;
    assert.ok(freeDisk >= (options.smoke ? 4 : 150) * GIB, `insufficient disk: ${(freeDisk / GIB).toFixed(1)} GiB free`);
    const dockerDisk = await fs.statfs(info.DockerRootDir);
    const dockerFreeDisk = dockerDisk.bavail * dockerDisk.bsize;
    assert.ok(dockerFreeDisk >= (options.smoke ? 4 : 150) * GIB,
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
      source_hashes: Object.fromEntries(await Promise.all(['scripts/benchmark.mjs', 'scripts/benchmark-results.mjs', 'benchmarks/BenchmarkAdmin.java'].map(async name =>
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
    const kafkaImage = provenance.images.kafka.reference;
    const jarContainer = await createContainer([kafkaImage], 'client-jars');
    await docker(['cp', `${jarContainer}:/opt/kafka/libs`, path.join(artifacts, 'libs')]);
    await removeContainer(jarContainer);
    const classes = path.join(artifacts, 'classes');
    await fs.mkdir(classes);
    await command('javac', ['--release', '17', '-cp', `${artifacts}/libs/*`, '-d', classes,
      `${artifacts}/BrokerPerformanceWorkload.java`, `${artifacts}/BenchmarkAdmin.java`],
    { output: path.join(artifacts, 'compile') });
    const jars = (await fs.readdir(path.join(artifacts, 'libs'))).filter(f => f.endsWith('.jar')).sort();
    provenance.client_jars = Object.fromEntries(await Promise.all(jars.map(async name =>
      [name, createHash('sha256').update(await fs.readFile(path.join(artifacts, 'libs', name))).digest('hex')])));
    provenance.client_java = await oneShot(['--entrypoint', 'java', kafkaImage, '--version'], 'client-version');
    await save();

    const clientArgs = () => [
      '--network', network, '--cpuset-cpus', provenance.cpu_sets.client.join(','),
      '--cpus', String(provenance.cpu_sets.client.length), '--memory', '4g', '--memory-swap', '4g',
      '--ulimit', 'nofile=131072:131072', '--volume', `${classes}:/bench/classes:ro`,
      '--entrypoint', 'java', kafkaImage, '-Xms256m', '-Xmx2g', '-cp', '/bench/classes:/opt/kafka/libs/*',
    ];
    const runClient = (args, label, output, timeout = 120_000) => oneShot([...clientArgs(), ...args], label, { output, timeout });

    async function launchCluster(vendor, rf, repetition, directory) {
      const available = Number((await fs.readFile('/proc/meminfo', 'utf8')).match(/^MemAvailable:\s+(\d+)/m)[1]) * 1024;
      assert.ok(available >= (rf * 10 + 4) * GIB, `RF${rf} needs ${rf * 10 + 4} GiB available RAM before cluster startup`);
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
          '--memory', '10g', '--memory-swap', '10g', '--ulimit', 'nofile=131072:131072',
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
            KAFKA_HEAP_OPTS: '-Xms1g -Xmx1g', KAFKA_LOG_DIRS: '/var/lib/kafka/data',
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
            '--smp=4', '--memory=8G', '--reserve-memory=1G', '--overprovisioned', '--check=false',
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

    async function trial(vendor, rf, repetition, workload, brokers, directory, warmup = false) {
      controller.signal.throwIfAborted();
      const prefix = warmup ? 'warmup' : workload.id;
      const topic = `bench-${prefix}`;
      const topicSettings = await runClient(['BenchmarkAdmin', 'topic', 'broker-0:9092', topic, String(rf),
        String(rf === 1 ? 1 : 2), vendor], 'topic', path.join(directory, `${prefix}.topic`));
      await fs.writeFile(path.join(directory, `${prefix}.topic.json`), `${topicSettings}\n`);
      const samples = [];
      const start = performance.now();
      const sampleFile = await fs.open(path.join(directory, `${prefix}.resources.jsonl`), 'w');
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
          if (!samplingController.signal.aborted) await sample();
        }
      })().catch(error => { sampleError = error; controller.abort(error); });
      let output;
      try {
        output = await runClient(['BrokerPerformanceWorkload', 'broker-0:9092', topic, `${topic}-group`,
          String(workload.records), String(workload.bytes), String(workload.rate), '900',
          workload.compression, workload.payload], 'workload', path.join(directory, `${prefix}.workload`), 960_000);
      } finally {
        samplingController.abort();
        await sampling;
        try { if (!sampleError) await sample(); }
        finally { await sampleFile.close(); }
      }
      if (sampleError) throw sampleError;
      const measured = JSON.parse(output);
      validateDelivery(measured, workload.records);
      const metrics = { ...measured, ...resourceSummary(samples, measured.sent) };
      const result = { vendor, rf, min_isr: vendor === 'redpanda' ? null : (rf === 1 ? 1 : 2),
        acknowledgment: vendor === 'redpanda' ? `Raft majority (${rf === 1 ? 1 : 2})` : 'all in-sync replicas', repetition, case: workload,
        workload: measured, metrics, topic: JSON.parse(topicSettings) };
      await fs.writeFile(path.join(directory, `${prefix}.json`), `${JSON.stringify(result, null, 2)}\n`);
      if (!warmup) trials.push(result);
      console.log(`${warmup ? 'Warm-up' : `Trial ${trials.length}`}: RF${rf} ${vendor} round ${repetition} ${prefix}: ${measured.records_per_second.toFixed(0)} records/s, ${metrics.cpu_us_per_record.toFixed(2)} CPU µs/record`);
    }

    for (const rf of [1, 3]) {
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
    provenance.status = 'complete';
    provenance.completed_at = new Date().toISOString();
    provenance.host.load_average_at_end = os.loadavg();
    await save();
    if (options.smoke) console.log(`Smoke passed: ${trials.length} trials. Results retained only in ${artifacts}`);
    else {
      await publishResults(ROOT, provenance, trials, controller.signal);
      console.log(`Published ${trials.length} trials to benchmarks/results/${runId}/ and benchmarks/latest.md`);
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
