import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

export const OMB = {
  repository: 'https://github.com/openmessaging/benchmark.git',
  commit: '5b1fa70951a323da26bd587174b58bb2c65b0b5c',
  build_image: 'maven@sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc',
  kafka_client_version: '3.6.1',
  // Applied to the checkout before the build. Upstream's Kafka consumer sends
  // an async offset commit after every poll without waiting for the last one,
  // so a fast drain queues commits in the client until they expire with
  // "Failed to send request after 30000 ms" (openmessaging/benchmark#270), for
  // every broker. The patch sends at most one commit at a time, as
  // redpanda-data/openmessaging-benchmark#37 does.
  patches: ['benchmarks/omb-kafka-coalesce-commits.patch'],
};

// Catalog from https://openmessaging.cloud/docs/benchmarks/; two max-rate
// single-partition files were renamed upstream to include producer/consumer counts.
export const OMB_WORKLOADS = [
  'simple-workload', '1-topic-1-partition-1kb', '1-topic-1-partition-100b',
  '1-topic-16-partitions-1kb', 'backlog-1-topic-1-partition-1kb',
  'backlog-1-topic-16-partitions-1kb', 'max-rate-1-topic-1-partition-1p-1c-1kb',
  'max-rate-1-topic-1-partition-1p-1c-100b', '1-topic-3-partition-100b-3producers',
  'max-rate-1-topic-16-partitions-1kb', 'max-rate-1-topic-16-partitions-100b',
  'max-rate-1-topic-100-partitions-1kb', 'max-rate-1-topic-100-partitions-100b',
];

export function ombCases(selection = 'all') {
  const names = selection === 'all' ? OMB_WORKLOADS : selection.split(',');
  assert.ok(names.length && new Set(names).size === names.length, 'empty or duplicate OMB workloads');
  for (const name of names) assert.ok(OMB_WORKLOADS.includes(name)
    || name === '1m-10-topics-1-partition-100b', `unknown OMB workload: ${name}`);
  return names.map(id => ({ id, upstream_file: `workloads/${id}.yaml` }));
}

export function ombWorkload(source, smoke, durationMinutes) {
  // Keep the upstream fields/payload. Only shorten duration/rate/backlog in
  // smoke, or the measurement with --test-duration-minutes.
  let effective = source.replace(/^(payloadFile:\s*)["']?([^"'\n]+)["']?\s*$/m, '$1"/src/$2"');
  if (durationMinutes !== undefined) {
    assert.ok(Number.isInteger(durationMinutes) && durationMinutes >= 1, 'invalid test duration');
    effective = effective.replace(/^testDurationMinutes:.*$/m, `testDurationMinutes: ${durationMinutes}`);
  }
  if (smoke) {
    effective = effective.replace(/^testDurationMinutes:.*$/m, 'testDurationMinutes: 1')
      .replace(/^producerRate:.*$/m, 'producerRate: 5000')
      .replace(/^consumerBacklogSizeGB:.*$/m, 'consumerBacklogSizeGB: 0');
    effective += '\nwarmupDurationMinutes: 0\n';
  }
  const value = key => Number(effective.match(new RegExp(`^${key}:\\s*(\\d+)`, 'm'))?.[1]);
  const config = Object.fromEntries(['topics', 'partitionsPerTopic', 'messageSize', 'testDurationMinutes',
    'producerRate', 'consumerBacklogSizeGB'].map(key => [key, value(key)]));
  assert.ok(Object.values(config).every(Number.isFinite), 'missing OMB workload field');
  return { yaml: effective, config, sha256: createHash('sha256').update(source).digest('hex') };
}

export function ombTimeoutMs(config, smoke) {
  return (config.testDurationMinutes + (smoke ? 5 : config.consumerBacklogSizeGB > 0 ? 90 : 30)) * 60_000;
}

// Headroom over the requested backlog that a backlog topic retains.
export const OMB_BACKLOG_RETENTION_RATIO = 1.2;

// Per-partition retention.bytes for a workload, or -1 (unlimited) without a
// backlog. OMB keeps producing while the backlog drains, so with unlimited
// retention an RF3 backlog run stores every byte it ever published three
// times on one disk. Retaining 1.2x the backlog keeps every unconsumed record
// (the backlog only shrinks once filled) and lets the brokers delete what the
// consumer has already read.
export function ombRetentionBytes(config) {
  if (!(config.consumerBacklogSizeGB > 0)) return -1;
  const partitions = config.topics * config.partitionsPerTopic;
  assert.ok(Number.isInteger(partitions) && partitions > 0, 'invalid OMB partition count');
  return Math.ceil(OMB_BACKLOG_RETENTION_RATIO * config.consumerBacklogSizeGB * 1024 ** 3 / partitions);
}

// Trials a run leaves out on purpose. provenance.omitted lists each one as
// { vendor, rf, workload, reason }, for every round of it. Only a trial that
// never completed can be omitted (see split in benchmark-openmessaging-shards.mjs),
// and at least two brokers stay in every comparison.
export function ombOmission(provenance, vendor, rf, workload) {
  return (provenance.omitted ?? []).find(o => o.vendor === vendor && o.rf === rf && o.workload === workload);
}

export function checkOmbOmissions(provenance, vendors = ['krabka', 'kafka', 'redpanda']) {
  const omitted = provenance.omitted ?? [];
  assert.ok(Array.isArray(omitted), 'provenance.omitted must be a list');
  const keys = new Set();
  for (const o of omitted) {
    assert.ok(vendors.includes(o.vendor), `omitted trial names unknown broker ${o.vendor}`);
    assert.ok(provenance.replication_factors.includes(o.rf), 'omitted trial is outside the run\'s replication factors');
    assert.ok(provenance.cases.some(c => c.id === o.workload), `omitted trial names unknown workload ${o.workload}`);
    assert.ok(typeof o.reason === 'string' && o.reason.trim(), `omitted ${o.vendor} ${o.workload} RF${o.rf} needs a reason`);
    const key = `${o.vendor}/${o.rf}/${o.workload}`;
    assert.ok(!keys.has(key), `${key} is omitted twice`);
    keys.add(key);
  }
  for (const workload of provenance.cases) for (const rf of provenance.replication_factors) {
    // A run of one broker (--vendors) compares nothing and omits nothing.
    const kept = vendors.filter(vendor => !keys.has(`${vendor}/${rf}/${workload.id}`));
    if (kept.length < vendors.length) assert.ok(kept.length >= 2, `${workload.id} at RF${rf} needs at least two measured brokers`);
  }
  return omitted;
}

export function ombDriver(vendor, rf, maxPollIntervalMs, retentionBytes = -1, preallocate = false) {
  assert.ok(['krabka', 'kafka', 'redpanda'].includes(vendor) && [1, 3].includes(rf));
  assert.ok(maxPollIntervalMs === undefined || (Number.isInteger(maxPollIntervalMs)
    && maxPollIntervalMs > 0 && maxPollIntervalMs <= 2 ** 31 - 1), 'invalid poll interval');
  assert.ok(retentionBytes === -1 || (Number.isSafeInteger(retentionBytes) && retentionBytes > 0), 'invalid retention.bytes');
  return `name: ${vendor}-rf${rf}
driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver
replicationFactor: ${rf}
topicConfig: |
  ${vendor === 'redpanda' ? 'write.caching=true' : `min.insync.replicas=${rf === 1 ? 1 : 2}`}
  retention.ms=-1
  retention.bytes=${retentionBytes}${preallocate ? '\n  preallocate=true' : ''}
commonConfig: |
  bootstrap.servers=broker-0:9092
  request.timeout.ms=30000
  default.api.timeout.ms=90000
producerConfig: |
  acks=all
  enable.idempotence=true
  max.in.flight.requests.per.connection=1
  linger.ms=1
  batch.size=1048576
  compression.type=none
consumerConfig: |
  auto.offset.reset=earliest
  enable.auto.commit=false
  max.partition.fetch.bytes=10485760
${maxPollIntervalMs === undefined ? '' : `  max.poll.interval.ms=${maxPollIntervalMs}\n`}`;
}

// Builds OMB from its pinned commit into `directory/openmessaging`, with the
// Maven repository under `cacheRoot` shared by every run on this host that
// builds the same commit with the same build image. A fresh repository per
// run asked Maven Central for every dependency again, and the lanes of run
// 40427360965, each building a shard every few minutes, got 429 Too Many
// Requests and lost every shard after their first. The repository's jars are
// hashed into the provenance either way, so shards still compare their
// builds byte for byte. Runs on one host build one at a time, so the shared
// repository sees no concurrent writers.
export async function prepareOmb(directory, command, oneShot,
  cacheRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '.benchmarks', 'm2-cache')) {
  const source = path.join(directory, 'openmessaging');
  await fs.mkdir(source);
  await command('git', ['-c', 'init.templateDir=', 'init', source]);
  await command('git', ['-C', source, 'remote', 'add', 'origin', OMB.repository]);
  await command('git', ['-C', source, 'fetch', '--depth=1', 'origin', OMB.commit]);
  await command('git', ['-C', source, 'checkout', '--detach', 'FETCH_HEAD']);
  assert.equal((await command('git', ['-C', source, 'rev-parse', 'HEAD'])).trim(), OMB.commit);
  const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
  const patches = {};
  for (const patch of OMB.patches) {
    const file = path.join(root, patch);
    patches[patch] = createHash('sha256').update(await fs.readFile(file)).digest('hex');
    await command('git', ['-C', source, 'apply', '--whitespace=nowarn', file]);
  }
  await command('docker', ['pull', '--platform', 'linux/amd64', OMB.build_image], { timeout: 600_000 });
  const image = JSON.parse(await command('docker', ['image', 'inspect', OMB.build_image]))[0];
  const m2 = path.join(cacheRoot, `${OMB.commit.slice(0, 12)}-${image.Id.replace(/^sha256:/, '').slice(0, 12)}`);
  await fs.mkdir(m2, { recursive: true });
  // Maven's resolver retries a 429 or 503 (its `serviceUnavailable` default)
  // this many times, this long apart, before it fails the build.
  await oneShot(['--user', `${process.getuid()}:${process.getgid()}`, '--cpus', '4', '--memory', '4g', '--memory-swap', '4g',
    '--env', 'MAVEN_CONFIG=/m2', '--volume', `${source}:/src`, '--volume', `${m2}:/m2`, '--workdir', '/src',
    OMB.build_image, 'mvn', '-B', '-ntp', '-Dmaven.repo.local=/m2', '-Dmaven.test.skip=true',
    '-Daether.connector.http.retryHandler.count=8', '-Daether.connector.http.retryHandler.requestInterval=5000',
    '-Dcheckstyle.skip', '-Dspotless.check.skip=true', '-Dlicense.skip=true', '-Djacoco.skip', '-Dspotbugs.skip=true',
    '-pl', 'benchmark-framework', '-am', 'install'], 'omb-build',
  { timeout: 1_800_000, output: path.join(directory, 'omb-build') });
  const classpath = (await fs.readFile(path.join(source, 'benchmark-framework/target/classpath.txt'), 'utf8')).trim();
  const jars = Object.fromEntries(await Promise.all(classpath.split(':').map(async jar => {
    assert.ok(jar.startsWith('/m2/') || jar.startsWith('/src/'), `unexpected OMB classpath entry: ${jar}`);
    const local = jar.startsWith('/m2/') ? path.join(m2, jar.slice(4)) : path.join(source, jar.slice(5));
    return [jar, createHash('sha256').update(await fs.readFile(local)).digest('hex')];
  })));
  return { source, m2, classpath: `/src/benchmark-framework/target/classes:${classpath}`,
    provenance: { ...OMB, patches, build_image_id: image.Id, jars } };
}

// A retriable offset-commit failure from the patched consumer (see
// omb-kafka-coalesce-commits.patch): a leader or coordinator move fails one
// commit, the consumer puts its offsets back and the next commit carries them.
// Redpanda's fork logs these as warnings. They are counted, not failed.
const RETRIABLE_COMMIT_FAILURE = /\]\s+ERROR\s+KafkaBenchmarkConsumer - Offset commit with offsets \{[^\n]*\} failed\r?\norg\.apache\.kafka\.clients\.consumer\.RetriableCommitFailedException\b/g;

export function retriableCommitFailures(logs = '') {
  return (logs.match(RETRIABLE_COMMIT_FAILURE) ?? []).length;
}

export function validateOmbResult(result, vendor, rf, config, logs = '') {
  // Upstream catches workload exceptions and can exit zero without a result.
  // A process exit status alone is never enough to mark a trial successful.
  // One exception: at the end of a maximum-rate run, upstream closes the
  // producer while its send loop is still running. A send blocked waiting for
  // buffer memory fails with a KafkaException, and one that starts after the
  // close fails with an IllegalStateException; LocalWorker logs either. Both
  // follow the final aggregated results, so they say nothing about the
  // measurement.
  const errors = logs.replace(/\]\s+ERROR\s+LocalWorker - Got error\r?\n(?:org\.apache\.kafka\.common\.KafkaException: Producer closed while allocating memory|java\.lang\.IllegalStateException: Cannot perform operation after producer has been closed)\b/g, '')
    .replace(RETRIABLE_COMMIT_FAILURE, '');
  assert.ok(!/\]\s+ERROR\s/.test(errors), 'OMB logged an error; inspect workload logs');
  assert.equal(result.driver, `${vendor}-rf${rf}`, 'wrong OMB driver');
  for (const [key, expected] of [['topics', config.topics], ['partitions', config.partitionsPerTopic], ['messageSize', config.messageSize]]) {
    assert.equal(result[key], expected, `wrong OMB ${key}`);
  }
  // Upstream sleeps ten seconds and then collects a sample, so its intervals
  // run a little over ten seconds, and a run can report one sample fewer than
  // six per minute: Kafka's 15-minute 3-producer RF3 trial in run 40452124698
  // reported 89 of 90 and was good data.
  const minimumSamples = config.testDurationMinutes * 6 - 1;
  for (const key of ['publishRate', 'consumeRate', 'publishErrorRate', 'backlog']) {
    assert.ok(Array.isArray(result[key]) && result[key].length >= minimumSamples, `missing/truncated OMB ${key}`);
    assert.ok(result[key].every(value => Number.isFinite(value) && value >= 0), `invalid OMB ${key}`);
    assert.equal(result[key].length, result.publishRate.length, 'inconsistent OMB series');
  }
  assert.ok(result.publishRate.some(value => value > 0), 'OMB published no messages');
  assert.ok(result.consumeRate.some(value => value > 0), 'OMB consumed no messages');
  assert.ok(result.publishErrorRate.every(value => value === 0), 'OMB publish errors');
  for (const key of ['aggregatedPublishLatency99pct', 'aggregatedEndToEndLatency99pct']) {
    assert.ok(Number.isFinite(result[key]) && result[key] >= 0, `missing OMB ${key}`);
  }
}

export async function writeOmbReport(directory, provenance, trials) {
  const vendors = provenance.vendors ?? ['krabka', 'kafka', 'redpanda'];
  const omitted = checkOmbOmissions(provenance, vendors);
  assert.equal(trials.length,
    (provenance.cases.length * provenance.replication_factors.length * vendors.length - omitted.length) * provenance.repetitions,
    'incomplete OMB matrix');
  const actual = new Set();
  for (const trial of trials) {
    validateOmbResult(trial.omb, trial.vendor, trial.rf, trial.case.config);
    actual.add(`${trial.vendor}/${trial.rf}/${trial.repetition}/${trial.case.id}`);
  }
  assert.equal(actual.size, trials.length, 'duplicate OMB trials');
  for (const workload of provenance.cases) for (const rf of provenance.replication_factors) {
    for (const vendor of vendors) for (let repetition = 1; repetition <= provenance.repetitions; repetition++) {
      const key = `${vendor}/${rf}/${repetition}/${workload.id}`;
      if (ombOmission(provenance, vendor, rf, workload.id)) assert.ok(!actual.has(key), `omitted OMB trial ${key} is present`);
      else assert.ok(actual.has(key), 'missing OMB trial');
    }
  }
  const mean = values => values.reduce((a, b) => a + b, 0) / values.length;
  const lines = ['# OpenMessaging broker comparison', '',
    `Run: ${provenance.run_id}. Mode: ${provenance.mode}. Status: complete.`, '',
    `Upstream: ${OMB.repository} at ${OMB.commit}; Kafka client ${OMB.kafka_client_version} for every broker.`, '',
    'Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.',
    'OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.', '',
    ...Object.entries(provenance.images).map(([vendor, image]) => `- ${vendor}: ${image.reference}`), '',
    '| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |',
    '|---|---:|---|---:|---:|---:|---:|---:|---:|---:|'];
  for (const trial of trials) {
    const result = trial.omb;
    lines.push(`| ${trial.case.id} | ${trial.rf} | ${trial.vendor} | ${trial.repetition} | ${mean(result.publishRate).toFixed(0)} | ${mean(result.consumeRate).toFixed(0)} | ${result.aggregatedPublishLatency99pct.toFixed(2)} | ${result.aggregatedEndToEndLatency99pct.toFixed(2)} | ${trial.metrics.cpu_seconds.toFixed(2)} | ${(trial.metrics.rss_peak_bytes / 1024 ** 2).toFixed(2)} |`);
  }
  if (omitted.length) {
    lines.push('', '## Not measured', '');
    for (const o of omitted) lines.push(`- ${o.workload}, RF${o.rf}, ${o.vendor}: ${o.reason}`);
  }
  await fs.writeFile(path.join(directory, 'summary.md'), `${lines.join('\n')}\n`);
}
