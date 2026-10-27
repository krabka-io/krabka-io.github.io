import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import { setTimeout as delay } from 'node:timers/promises';
import { median, aggregateSample, directIoCheck, ombMeasurementWindow, parseDiskstats, resourceSummary, resourceTimeSeries, windowMetrics } from './benchmark-results.mjs';
import { ombCases, ombWorkload, ombDriver, ombRetentionBytes, ombTimeoutMs, retriableCommitFailures, validateOmbResult, writeOmbReport } from './benchmark-openmessaging.mjs';
import { runLoggedCommand } from './benchmark-command.mjs';

test('actual cleanup retains final exit state before removal and tolerates inspect failure', async () => {
  const source = await fs.readFile(new URL('./benchmark.mjs', import.meta.url), 'utf8');
  const cleanupSource = source.slice(source.indexOf('async function cleanup()'), source.indexOf('\n\n  try {'));
  const artifacts = await fs.mkdtemp(path.join(os.tmpdir(), 'benchmark-exit-'));
  const state = { Status: 'exited', Running: false, OOMKilled: true, ExitCode: 137,
    Error: '', FinishedAt: '2026-10-06T05:42:15Z' };
  try {
    for (const unavailable of [false, true]) {
      const filename = path.join(artifacts, 'broker.container-state.json');
      await fs.rm(filename, { force: true });
      const containers = new Map([['owned-broker', 'broker']]);
      let inspected = false, removed = false;
      const cleanup = runInNewContext(`(${cleanupSource.trim()})`, {
        fs, path, artifacts, containers, volumes: new Set(), network: undefined,
        resourceCreationStarted: false,
        docker: async (args, options) => {
          assert.equal(args.at(-1), 'owned-broker');
          assert.equal(options.ignoreAbort, true);
          if (args[0] === 'inspect') {
            assert.deepEqual(Array.from(args), ['inspect', '--format', '{{json .State}}', 'owned-broker']);
            inspected = true;
            if (unavailable) throw new Error('container unavailable');
            return JSON.stringify(state);
          }
          assert.equal(args[0], 'logs');
        },
        removeContainer: async id => {
          assert.ok(inspected);
          if (!unavailable) assert.deepEqual(JSON.parse(await fs.readFile(filename, 'utf8')), state);
          containers.delete(id); removed = true;
        },
      });
      await cleanup();
      assert.ok(removed);
      assert.equal(containers.size, 0);
    }
    const workflow = await fs.readFile(new URL('../.github/workflows/openmessaging.yml', import.meta.url), 'utf8');
    assert.ok(workflow.includes('.benchmarks/*/*.container-state.json'));
  } finally { await fs.rm(artifacts, { recursive: true, force: true }); }
});

test('actual memory check waits for MemAvailable and records the shortfall before failing', async () => {
  const source = await fs.readFile(new URL('./benchmark.mjs', import.meta.url), 'utf8');
  const checkSource = source.slice(source.indexOf('async function awaitAvailableMemory('), source.indexOf('async function cleanup()'));
  const GIB = 1024 ** 3;
  const meminfo = available => ['MemTotal:       67416620 kB', `MemAvailable:   ${available / 1024} kB`, 'MemFree:        12582912 kB',
    'Cached:          2097152 kB', 'AnonPages:      31457280 kB', 'Shmem:             51080 kB', 'Slab:             460752 kB',
    'SUnreclaim:       174528 kB', 'Dirty:            281820 kB', 'Writeback:             0 kB', 'Unevictable:       27208 kB',
    'AnonHugePages:         0 kB', ''].join('\n');
  const artifacts = await fs.mkdtemp(path.join(os.tmpdir(), 'benchmark-memory-'));
  try {
    const check = readings => {
      const queue = [...readings];
      const trace = { waits: [], commands: [], logs: [] };
      const run = runInNewContext(`(${checkSource.trim()})`, {
        fs: { readFile: async file => { assert.equal(file, '/proc/meminfo'); return meminfo((queue.length > 1 ? queue.shift() : queue[0]) * GIB); },
          appendFile: fs.appendFile },
        path, artifacts, GIB, MEMORY_WAIT_MS: 200, MEMORY_POLL_MS: 5, controller: new AbortController(),
        console: { log: message => trace.logs.push(message) },
        delay: async ms => { trace.waits.push(ms); await delay(1); },
        command: async (executable, args) => { trace.commands.push([executable, ...args]); return 'PID RSS ELAPSED COMMAND\n1 100 00:01 init'; },
        docker: async args => { trace.commands.push(['docker', ...args]); return 'broker-0 Up 2 minutes image'; },
      });
      return { run, trace };
    };
    const recovered = check([49, 49, 61]);
    assert.equal(await recovered.run(50 * GIB, 'RF3 cluster startup'), 61 * GIB);
    assert.deepEqual(recovered.trace.waits, [5, 5]);
    assert.deepEqual(recovered.trace.commands, []);
    assert.match(recovered.trace.logs.join('\n'), /waited \d+ s for 50 GiB available RAM; found 61\.0 GiB/);
    const prompt = check([61]);
    assert.equal(await prompt.run(34 * GIB, 'the run'), 61 * GIB);
    assert.deepEqual(prompt.trace.waits, []);
    assert.deepEqual(prompt.trace.logs, []);
    const shortfall = check([28]);
    await assert.rejects(shortfall.run(34 * GIB, 'RF3 cluster startup'),
      /^Error: RF3 cluster startup needs 34 GiB available RAM; found 28\.0 GiB after \d+ s \(MemFree 12\.0 GiB, Cached 2\.0 GiB, AnonPages 30\.0 GiB, .*\); memory-shortfall\.txt lists/);
    assert.ok(shortfall.trace.waits.length >= 2);
    assert.deepEqual(shortfall.trace.commands, [['ps', '-eo', 'pid,rss,etime,comm', '--sort=-rss'],
      ['docker', 'ps', '-a', '--format', '{{.Names}} {{.Status}} {{.Image}}']]);
    const recorded = await fs.readFile(path.join(artifacts, 'memory-shortfall.txt'), 'utf8');
    assert.match(recorded, /RF3 cluster startup: MemFree 12\.0 GiB/);
    assert.match(recorded, /MemAvailable:   29360128 kB/);
    assert.match(recorded, /1 100 00:01 init\nbroker-0 Up 2 minutes image\n/);
    const workflow = await fs.readFile(new URL('../.github/workflows/openmessaging.yml', import.meta.url), 'utf8');
    assert.ok(workflow.includes('.benchmarks/*/memory-shortfall.txt'));
  } finally { await fs.rm(artifacts, { recursive: true, force: true }); }
});

const firstDevice = (await fs.readFile('/proc/diskstats', 'utf8')).trim().split('\n')[0].trim().split(/\s+/)[2];
const sampledHost = { host: { storage: { sampled_devices: [firstDevice] } } };

test('actual resource reader retains user and system CPU through the sampler', async () => {
  const source = await fs.readFile(new URL('./benchmark.mjs', import.meta.url), 'utf8');
  const reader = source.slice(source.indexOf('async function readBroker('),
    source.indexOf('\n    async function measureResources('));
  const sampler = source.slice(source.indexOf('async function measureResources('),
    source.indexOf('\n    // Run each OMB workload'));
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'benchmark-cpu-'));
  const cpu = 'usage_usec 900000\nuser_usec 200000\nsystem_usec 700000\n';
  try {
    for (const [name, data] of Object.entries({ 'cpu.stat': cpu, 'memory.current': '8192',
      'memory.stat': 'anon 4096\ninactive_file 1024\n', 'memory.events': 'oom_kill 0\n',
      'cgroup.procs': `${process.pid}\n` })) await fs.writeFile(path.join(directory, name), data);
    const readBroker = runInNewContext(`(${reader.trim()})`, { fs, path, assert });
    const measure = runInNewContext(`(${sampler.trim()})`, {
      fs, path, performance, AbortController, Date, delay, aggregateSample, parseDiskstats, provenance: sampledHost,
      controller: new AbortController(), readBroker,
    });
    const filename = path.join(directory, 'resources.jsonl');
    const result = await measure([{ id: 'broker', cgroup: directory }], filename, async () => {});
    assert.ok(result.samples.length >= 2);
    for (const sample of (await fs.readFile(filename, 'utf8')).trim().split('\n').map(JSON.parse)) {
      const row = sample.brokers[0];
      assert.deepEqual([row.cpu_usage_us, row.cpu_user_us, row.cpu_system_us], [900000, 200000, 700000]);
    }
    for (const counter of ['user_usec', 'system_usec']) {
      await fs.writeFile(path.join(directory, 'cpu.stat'), cpu.replace(new RegExp(`^${counter}.*\\n`, 'm'), ''));
      await assert.rejects(readBroker({ id: 'broker', cgroup: directory }), /resource counter unavailable/);
    }
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

test('actual resource sampler retains its clock anchor when the client fails', async () => {
  const source = await fs.readFile(new URL('./benchmark.mjs', import.meta.url), 'utf8');
  const sampler = source.slice(source.indexOf('async function measureResources('),
    source.indexOf('\n    // Run each OMB workload'));
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'benchmark-clock-'));
  try {
    const measure = runInNewContext(`(${sampler.trim()})`, {
      fs, path, performance, AbortController, Date, delay, aggregateSample, parseDiskstats, provenance: sampledHost,
      controller: new AbortController(), readBroker: async () => broker('a', 100, 200, 300, 50),
    });
    for (const failed of [false, true]) {
      const filename = path.join(directory, `${failed}.resources.jsonl`);
      const before = Date.now();
      let anchor;
      const action = async () => {
        anchor = JSON.parse(await fs.readFile(`${filename}.metadata.json`, 'utf8'));
        assert.ok(Date.parse(anchor.started_at) >= before && Date.parse(anchor.started_at) <= Date.now());
        if (failed) throw new Error('client failed');
        return 'client output';
      };
      if (failed) await assert.rejects(measure([{}], filename, action), /client failed/);
      else {
        const result = await measure([{}], filename, action);
        assert.equal(result.startedAt, anchor.started_at);
        assert.equal(result.output, 'client output');
      }
      assert.deepEqual(anchor, { schema_version: 1, started_at: anchor.started_at, sampling_interval_ms: 250 });
      assert.deepEqual(JSON.parse(await fs.readFile(`${filename}.metadata.json`, 'utf8')), anchor);
      const samples = (await fs.readFile(filename, 'utf8')).trim().split('\n').map(JSON.parse);
      assert.equal(samples.length, 2);
      assert.ok(samples[1].elapsed_ms > samples[0].elapsed_ms);
    }
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

test('OpenMessaging lanes run every shard, balance image order, preserve arguments and fail after a failed shard', async () => {
  const workflow = await fs.readFile(new URL('../.github/workflows/openmessaging.yml', import.meta.url), 'utf8');
  const step = workflow.split("      - name: Run this lane's shards against all three brokers\n")[1]
    .split('      - name: Add report to job summary\n')[0];
  const script = step.split('        run: |\n')[1].replace(/^          /gm, '');
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'omb-workflow-'));
  try {
    await fs.mkdir(path.join(directory, 'scripts'));
    await fs.writeFile(path.join(directory, 'scripts/benchmark.mjs'), `
      import fs from 'node:fs';
      fs.appendFileSync('calls.jsonl', JSON.stringify(process.argv.slice(2)) + '\\n');
      if (fs.readFileSync('calls.jsonl', 'utf8').trim().split('\\n').length === Number(process.env.FAIL_AT)) process.exit(7);
    `);
    const candidate = 'candidate;touch unexpected';
    const control = 'control image';
    const shards = [['simple-workload', '1'], ['backlog-1-topic-1-partition-1kb', '3']];
    const each = images => shards.flatMap(shard => images.map(image => [shard, image]));
    for (const scenario of [
      { comparison: '', mode: 'full', calls: each([candidate]), status: 0 },
      { comparison: control, mode: 'smoke', calls: each([control, candidate, candidate, control]), status: 0 },
      // Direct storage adds krabka's flags to every call and nothing else.
      { comparison: '', mode: 'full', storage: 'direct', calls: each([candidate]), status: 0 },
      { comparison: control, mode: 'smoke', storage: 'direct-io-uring', calls: each([control, candidate, candidate, control]), status: 0 },
      { comparison: '', mode: 'full', storage: 'invalid', calls: [], status: 1 },
      // The first shard's second image fails: its remaining images are
      // skipped, the next shard still runs, and the lane fails at the end.
      { comparison: control, mode: 'dry-run', failAt: '2',
        calls: [[shards[0], control], [shards[0], candidate], ...each([control, candidate, candidate, control]).slice(4)], status: 1 },
      { comparison: control, mode: 'invalid', calls: [], status: 1 },
    ]) {
      await fs.rm(path.join(directory, 'calls.jsonl'), { force: true });
      const result = spawnSync('bash', ['-e', '-o', 'pipefail', '-c', script], {
        cwd: directory, timeout: 10_000, encoding: 'utf8', env: { ...process.env,
          BENCHMARK_MODE: scenario.mode, LANE_SHARDS: shards.map(([w, rf]) => `rf${rf}-${w}`).join(' '),
          LANE_WORKLOADS: shards.map(([w]) => w).join(' '), LANE_RFS: shards.map(([, rf]) => rf).join(' '),
          BENCHMARK_REPETITIONS: '2', KRABKA_IMAGE: candidate,
          KRABKA_COMPARISON_IMAGE: scenario.comparison, KRABKA_STORAGE: scenario.storage ?? 'page-cache',
          REDPANDA_IMAGE: 'redpanda image', FAIL_AT: scenario.failAt ?? '',
        },
      });
      assert.ifError(result.error);
      assert.equal(result.status, scenario.status, result.stderr);
      const calls = await fs.readFile(path.join(directory, 'calls.jsonl'), 'utf8').catch(error => {
        if (error.code === 'ENOENT') return '';
        throw error;
      });
      const mode = { smoke: ['--smoke'], 'dry-run': ['--dry-run'] }[scenario.mode] ?? [];
      const storage = { direct: ['--krabka-preallocate'], 'direct-io-uring': ['--krabka-preallocate', '--krabka-io-uring'] }[scenario.storage] ?? [];
      assert.deepEqual(calls.trim() ? calls.trim().split('\n').map(JSON.parse) : [],
        scenario.calls.map(([[workload, rf], image]) => ['--suite', 'openmessaging', '--workloads', workload,
          '--replication-factors', rf, '--repetitions', '2', '--redpanda-image', 'redpanda image', ...mode, ...storage, '--krabka-image', image]));
    }
    await assert.rejects(fs.stat(path.join(directory, 'unexpected')), { code: 'ENOENT' });
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

test('long benchmark logs are retained beyond execFile limits and failures remain bounded', async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'benchmark-logs-'));
  const output = path.join(directory, 'workload');
  try {
    await runLoggedCommand(process.execPath, ['-e', 'process.stdout.write("x".repeat(34 * 1024 ** 2)); process.stderr.write("diagnostic");'],
      { output, timeout: 30_000 });
    assert.equal((await fs.stat(`${output}.stdout`)).size, 34 * 1024 ** 2);
    assert.equal(await fs.readFile(`${output}.stderr`, 'utf8'), 'diagnostic');
    await assert.rejects(runLoggedCommand(process.execPath, ['-e',
      'process.stderr.write("x".repeat(10000) + "failure tail"); process.exitCode = 7;'], { output, timeout: 30_000 }), error => {
      assert.match(error.message, /exit 7/);
      assert.match(error.message, /failure tail/);
      assert.ok(error.message.length < 4000);
      return true;
    });
    await assert.rejects(runLoggedCommand(process.execPath, ['-e', 'setInterval(() => {}, 1000)'],
      { output, timeout: 50 }), /SIGKILL/);
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});

test('OpenMessaging rejects failed or truncated upstream captures and incomplete matrices', async () => {
  const config = { topics: 1, partitionsPerTopic: 16, messageSize: 1024, testDurationMinutes: 1 };
  const capture = { driver: 'krabka-rf3', topics: 1, partitions: 16, messageSize: 1024,
    publishRate: Array(6).fill(5000), consumeRate: Array(6).fill(5000),
    publishErrorRate: Array(6).fill(0), backlog: Array(6).fill(0),
    aggregatedPublishLatency99pct: 1, aggregatedEndToEndLatency99pct: 2 };
  validateOmbResult(capture, 'krabka', 3, config);
  validateOmbResult({ ...capture, aggregatedEndToEndLatency99pct: 0 }, 'krabka', 3, config);
  assert.throws(() => validateOmbResult(capture, 'krabka', 3, config, '20:00:00 [consumer] ERROR ConsumerCoordinator - Offset commit failed'), /logged an error/);
  const shutdown = '19:29:44.105 [local-worker-1-1] ERROR LocalWorker - Got error\norg.apache.kafka.common.KafkaException: Producer closed while allocating memory\n\tat org.apache.kafka.clients.producer.internals.BufferPool.allocate(BufferPool.java:161)';
  validateOmbResult(capture, 'krabka', 3, config, shutdown);
  validateOmbResult(capture, 'krabka', 3, config, '01:44:14.136 [local-worker-1-1] ERROR LocalWorker - Got error\njava.lang.IllegalStateException: Cannot perform operation after producer has been closed\n\tat org.apache.kafka.clients.producer.KafkaProducer.throwIfProducerClosed(KafkaProducer.java:954)');
  const commit = '17:31:08.701 [pool-3-thread-1] ERROR KafkaBenchmarkConsumer - Offset commit with offsets {t-7=OffsetAndMetadata{offset=42695, leaderEpoch=null, metadata=\'\'}} failed\norg.apache.kafka.clients.consumer.RetriableCommitFailedException: Offset commit failed with a retriable exception.\nCaused by: org.apache.kafka.common.errors.NotCoordinatorException: This is not the correct coordinator.';
  validateOmbResult(capture, 'krabka', 3, config, `${commit}\n${commit}`);
  assert.equal(retriableCommitFailures(`${commit}\n${commit}`), 2);
  assert.throws(() => validateOmbResult(capture, 'krabka', 3, config, '17:31:08 [pool-3-thread-1] ERROR KafkaBenchmarkConsumer - Offset commit with offsets {t-7=x} failed\norg.apache.kafka.common.errors.GroupAuthorizationException: denied'), /logged an error/);
  assert.throws(() => validateOmbResult(capture, 'krabka', 3, config, '20:00:00 [local-worker-1-1] ERROR LocalWorker - Got error\norg.apache.kafka.common.errors.TimeoutException: Expiring 1 record(s)'), /logged an error/);
  assert.throws(() => validateOmbResult(capture, 'krabka', 3, config, `${shutdown}\n20:00:00 [consumer] ERROR ConsumerCoordinator - Offset commit failed`), /logged an error/);
  assert.throws(() => validateOmbResult({}, 'krabka', 3, config));
  assert.throws(() => validateOmbResult({ ...capture, publishErrorRate: [0, 0, 0, 0, 0, 1] }, 'krabka', 3, config), /publish errors/);
  assert.throws(() => validateOmbResult({ ...capture, consumeRate: Array(6).fill(0) }, 'krabka', 3, config), /consumed no/);
  assert.throws(() => validateOmbResult({ ...capture, publishRate: [5000] }, 'krabka', 3, config), /truncated/);
  // One sample fewer than six per minute is a normal upstream run.
  const short = { publishRate: Array(5).fill(5000), consumeRate: Array(5).fill(5000), publishErrorRate: Array(5).fill(0), backlog: Array(5).fill(0) };
  validateOmbResult({ ...capture, ...short }, 'krabka', 3, config);
  assert.throws(() => validateOmbResult({ ...capture, ...short, publishRate: Array(4).fill(5000) }, 'krabka', 3, config), /truncated/);
  assert.throws(() => validateOmbResult({ ...capture, driver: 'kafka-rf3' }, 'krabka', 3, config), /wrong OMB driver/);
  await assert.rejects(writeOmbReport('/unused', { cases: [{}], replication_factors: [1, 3], repetitions: 1 }, []), /incomplete/);
});

test('OpenMessaging catalog and smoke configuration keep comparison settings consistent', () => {
  assert.equal(ombCases().length, 13);
  assert.deepEqual(ombCases('1m-10-topics-1-partition-100b'), [{
    id: '1m-10-topics-1-partition-100b', upstream_file: 'workloads/1m-10-topics-1-partition-100b.yaml',
  }]);
  assert.throws(() => ombCases('1m-10-topics-1-partition-100b,1m-10-topics-1-partition-100b'), /duplicate/);
  assert.throws(() => ombCases('../outside'), /unknown/);
  assert.throws(() => ombCases('simple-workload,simple-workload'), /duplicate/);
  const source = 'topics: 1\npartitionsPerTopic: 16\nmessageSize: 1024\npayloadFile: "payload/payload-1Kb.data"\nproducerRate: 100000\nconsumerBacklogSizeGB: 100\ntestDurationMinutes: 15\n';
  const full = ombWorkload(source, false);
  assert.equal(full.config.consumerBacklogSizeGB, 100);
  assert.equal(full.config.testDurationMinutes, 15);
  assert.equal(ombTimeoutMs(full.config, false), 105 * 60_000);
  assert.match(full.yaml, /payloadFile: "\/src\/payload\/payload-1Kb.data"/);
  const smoke = ombWorkload(source, true);
  assert.equal(smoke.config.consumerBacklogSizeGB, 0);
  assert.equal(smoke.config.producerRate, 5000);
  assert.equal(smoke.config.testDurationMinutes, 1);
  assert.equal(ombTimeoutMs(smoke.config, true), 6 * 60_000);
  assert.equal(ombTimeoutMs({ ...full.config, consumerBacklogSizeGB: 0 }, false), 45 * 60_000);
  assert.equal(full.sha256, smoke.sha256);
  for (const vendor of ['krabka', 'kafka', 'redpanda']) {
    const driver = ombDriver(vendor, 3);
    assert.doesNotMatch(driver, /max.poll.interval.ms/);
    const backlogDriver = ombDriver(vendor, 3, ombTimeoutMs(full.config, false));
    assert.match(backlogDriver, /^  max.poll.interval.ms=6300000$/m);
    assert.match(driver, /acks=all\n  enable.idempotence=true/);
    assert.match(driver, /replicationFactor: 3/);
    assert.match(driver, /batch.size=1048576/);
    assert.match(driver, vendor === 'redpanda' ? /write.caching=true/ : /min.insync.replicas=2/);
  }
  assert.throws(() => ombDriver('krabka', 3, NaN), /invalid poll interval/);
});

test('OMB backlog topics retain 1.2x the backlog per partition and other topics retain everything', () => {
  const config = { topics: 1, partitionsPerTopic: 1, messageSize: 1024, testDurationMinutes: 5, producerRate: 100000 };
  const cases = [
    [{ ...config, consumerBacklogSizeGB: 0 }, -1],
    [{ ...config, consumerBacklogSizeGB: 100 }, Math.ceil(1.2 * 100 * 1024 ** 3)],
    [{ ...config, partitionsPerTopic: 16, consumerBacklogSizeGB: 100 }, Math.ceil(1.2 * 100 * 1024 ** 3 / 16)],
    [{ ...config, topics: 2, partitionsPerTopic: 8, consumerBacklogSizeGB: 100 }, Math.ceil(1.2 * 100 * 1024 ** 3 / 16)],
  ];
  for (const [workload, expected] of cases) {
    const bytes = ombRetentionBytes(workload);
    assert.equal(bytes, expected);
    for (const vendor of ['krabka', 'kafka', 'redpanda']) {
      const driver = ombDriver(vendor, 3, undefined, bytes);
      assert.match(driver, new RegExp(`^  retention.ms=-1\n  retention.bytes=${bytes}$`, 'm'));
    }
  }
  assert.match(ombDriver('kafka', 1), /^  retention.bytes=-1$/m);
  for (const bad of [0, -2, 1.5, NaN]) assert.throws(() => ombDriver('kafka', 1, undefined, bad), /invalid retention.bytes/);
});

test('the OMB driver sets preallocate only when asked', () => {
  assert.doesNotMatch(ombDriver('krabka', 1), /preallocate/);
  assert.match(ombDriver('krabka', 1, undefined, -1, true), /^  retention.bytes=-1\n  preallocate=true\ncommonConfig:/m);
});

const broker = (id, cpu, rss, current, inactive = 0) => ({ id, cpu_usage_us: cpu, rss_bytes: rss,
  memory_current_bytes: current, inactive_file_bytes: inactive, anon_bytes: rss / 2, oom_kill: 0 });
test('medians use numeric ordering, handle even samples, and preserve raw values', () => {
  const values = [100, 2, 10];
  assert.equal(median(values), 10);
  assert.deepEqual(values, [100, 2, 10]);
  assert.equal(median([1, 3, 5, 9]), 4);
  assert.throws(() => median([]));
  assert.throws(() => median([NaN]));
});

test('RF3 CPU deltas and memory peaks aggregate simultaneous broker samples', () => {
  const samples = [
    { elapsed_ms: 0, brokers: [broker('a', 1e6, 100, 200, 50), broker('b', 2e6, 400, 500, 100), broker('c', 3e6, 50, 100, 10)] },
    { elapsed_ms: 250, brokers: [broker('a', 2e6, 400, 600, 100), broker('b', 3e6, 100, 200, 50), broker('c', 4e6, 70, 100, 10)] },
  ];
  const summary = resourceSummary(samples, 1e6);
  assert.equal(summary.cpu_seconds, 3);
  assert.equal(summary.cpu_us_per_record, 3);
  assert.equal(summary.rss_peak_bytes, 570); // independent peaks would incorrectly yield 870
  assert.equal(summary.anon_peak_bytes, 285);
  assert.equal(summary.working_set_peak_bytes, 740);
  assert.equal(summary.max_sample_gap_ms, 250);
  assert.equal(aggregateSample([broker('a', 1, 1, 10, 20)]).working_set_bytes, 0);
});

test('missing counters, OOM, disappearing brokers, and counter resets fail measurement', () => {
  const a = broker('a', 100, 100, 200);
  assert.throws(() => aggregateSample([{ ...a, rss_bytes: undefined }]));
  assert.throws(() => aggregateSample([{ ...a, oom_kill: 1 }]));
  assert.throws(() => resourceSummary([{ elapsed_ms: 0, brokers: [a] }, { elapsed_ms: 250, brokers: [] }], 10));
  assert.throws(() => resourceSummary([{ elapsed_ms: 0, brokers: [a] }, { elapsed_ms: 250, brokers: [{ ...a, cpu_usage_us: 99 }] }], 10));
  assert.throws(() => resourceSummary([{ elapsed_ms: 0, brokers: [a] }, { elapsed_ms: 250, brokers: [{ ...a, id: 'replacement', cpu_usage_us: 101 }] }], 10));
});

test('time series preserves raw samples and uses actual gaps for cluster CPU and memory', () => {
  const samples = [
    { elapsed_ms: 10, brokers: [broker('a', 1e6, 100, 200, 50), broker('b', 2e6, 400, 500, 100)] },
    { elapsed_ms: 510, brokers: [broker('a', 2e6, 400, 600, 100), broker('b', 3e6, 100, 200, 50)] },
    { elapsed_ms: 1510, brokers: [broker('a', 3e6, 300, 600, 200), broker('b', 4e6, 150, 300, 50)] },
  ];
  const startedAt = '2026-10-03T04:00:00.000Z';
  const series = resourceTimeSeries(samples, startedAt);
  assert.equal(series.schema_version, 1);
  assert.equal(series.started_at, startedAt);
  assert.equal(series.sampling_interval_ms, 250);
  assert.deepEqual(series.samples.map(({ cluster, ...sample }) => sample), samples);
  assert.deepEqual(series.samples.map(s => s.cluster.cpu_cores), [null, 4, 2]);
  assert.deepEqual(series.samples.map(s => s.cluster.cpu_seconds), [0, 2, 4]);
  assert.deepEqual(series.samples.map(s => s.cluster.working_set_bytes), [550, 650, 650]);
  const summary = resourceSummary(samples, 100);
  assert.equal(summary.cpu_seconds, series.samples.at(-1).cluster.cpu_seconds);
  assert.equal(summary.rss_peak_bytes, Math.max(...series.samples.map(s => s.cluster.rss_bytes)));
  assert.equal(summary.max_sample_gap_ms, 1000);
  assert.throws(() => resourceTimeSeries(samples, 'invalid'), /measurement start/);
  for (const elapsed_ms of [-1, NaN, 10]) {
    assert.throws(() => resourceTimeSeries([samples[0], { ...samples[1], elapsed_ms }], startedAt));
  }
});

test('diskstats counters parse in bytes and refuse a missing device', () => {
  const text = ' 259  3 nvme0n1p3 10 0 80 5 20 0 160 9 0 30 40 0 0 0 0\n   9  3 md3 4 0 16 0 8 0 64 0 0 0 0 0 0 0 0\n';
  assert.deepEqual(parseDiskstats(text, ['md3', 'nvme0n1p3']), {
    md3: { reads: 4, read_bytes: 8192, writes: 8, written_bytes: 32768, io_ms: 0, weighted_io_ms: 0 },
    nvme0n1p3: { reads: 10, read_bytes: 40960, writes: 20, written_bytes: 81920, io_ms: 30, weighted_io_ms: 40 },
  });
  assert.throws(() => parseDiskstats(text, ['sda']), /device counters unavailable: sda/);
});

test('the OMB measurement window runs from benchmark traffic to its aggregate, across midnight', () => {
  const log = ['23:58:00.000 [main] INFO WorkloadGenerator - ----- Starting warm-up traffic (1m) ------',
    '23:59:00.100 [main] INFO WorkloadGenerator - ----- Aggregated Pub Latency (ms) avg: 1',
    '23:59:00.200 [main] INFO WorkloadGenerator - ----- Starting benchmark traffic (5m)------',
    '00:04:00.700 [main] INFO WorkloadGenerator - ----- Aggregated Pub Latency (ms) avg: 2'].join('\n');
  assert.deepEqual(ombMeasurementWindow(log, '2026-10-14T23:57:55.000Z'),
    { started_at: '2026-10-14T23:59:00.200Z', ended_at: '2026-10-15T00:04:00.700Z', seconds: 300.5 });
  assert.throws(() => ombMeasurementWindow(log.split('\n').slice(0, 2).join('\n'), '2026-10-14T23:57:55.000Z'), /start not found/);
});

test('window metrics interpolate CPU and device counters at the window edges', () => {
  const at = (elapsed_ms, cpu) => ({ elapsed_ms, brokers: [broker('a', cpu, 1, 1, 1)] });
  const samples = [at(0, 0), at(10_000, 4_000_000), at(20_000, 8_000_000)];
  const deviceSamples = [0, 10_000, 20_000].map(elapsed_ms => ({ elapsed_ms,
    devices: { md3: { reads: 0, read_bytes: 0, writes: elapsed_ms, written_bytes: elapsed_ms * 100, io_ms: elapsed_ms / 2, weighted_io_ms: 0 } } }));
  const window = { started_at: '2026-10-14T00:00:05.000Z', ended_at: '2026-10-14T00:00:15.000Z', seconds: 10 };
  const metrics = windowMetrics({ samples, deviceSamples, startedAt: '2026-10-14T00:00:00.000Z', window,
    result: { publishRate: [100, 300] } });
  assert.equal(metrics.cpu_seconds, 4);
  assert.equal(metrics.published_messages, 2000);
  assert.equal(metrics.messages_per_cpu_second, 500);
  assert.deepEqual(metrics.devices.md3, { utilization: 0.5, read_bytes: 0, written_bytes: 1_000_000, reads: 0, writes: 10_000 });
  assert.throws(() => windowMetrics({ samples, deviceSamples, startedAt: '2026-10-14T00:00:10.000Z', window,
    result: { publishRate: [1] } }), /outside the samples/);
});

test('the O_DIRECT check needs the newest segment of every benchmark partition open with O_DIRECT', () => {
  const listing = ['/data/__consumer_offsets-0/00000000000000000000.log 02100002',
    '/data/test-topic-a-0/00000000000000000000.log 02100002',
    '/data/test-topic-a-0/00000000000000000000.log 02140002',
    '/data/test-topic-a-1/00000000000000000000.log 02140002',
    '/data/test-topic-a-1/00000000000000001000.log 02100002'].join('\n');
  assert.deepEqual(directIoCheck(listing, 'x64'), { partitions: 2, direct: 1, missing: ['test-topic-a-1'], verified: ['test-topic-a-0'] });
  assert.deepEqual(directIoCheck(listing.replaceAll('02140002', '02100002'), 'x64').direct, 0);
  assert.deepEqual(directIoCheck(listing.replace(/02100002$/, '02140002'), 'x64'), { partitions: 2, direct: 2, missing: [], verified: ['test-topic-a-0', 'test-topic-a-1'] });
  assert.equal(directIoCheck('/data/test-topic-a-0/00000000000000000000.log 0200002', 'arm64').direct, 1);
  assert.throws(() => directIoCheck('', 'ppc64'), /O_DIRECT flag unknown/);
});

test('--test-duration-minutes shortens only the measurement and refuses a bad value', () => {
  const source = 'name: x\ntopics: 1\npartitionsPerTopic: 3\nmessageSize: 100\npayloadFile: "payload/payload-100b.data"\ntestDurationMinutes: 15\nproducerRate: 0\nconsumerBacklogSizeGB: 0\n';
  const shortened = ombWorkload(source, false, 5);
  assert.equal(shortened.config.testDurationMinutes, 5);
  assert.equal(shortened.config.partitionsPerTopic, 3);
  assert.equal(ombWorkload(source, false).config.testDurationMinutes, 15);
  assert.equal(shortened.sha256, ombWorkload(source, false).sha256, 'the upstream hash stays the source file\'s');
  assert.throws(() => ombWorkload(source, false, 0), /invalid test duration/);
});
