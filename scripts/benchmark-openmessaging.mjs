import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

export const OMB = {
  repository: 'https://github.com/openmessaging/benchmark.git',
  commit: '5b1fa70951a323da26bd587174b58bb2c65b0b5c',
  build_image: 'maven@sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc',
  kafka_client_version: '3.6.1',
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

export function ombWorkload(source, smoke) {
  // Keep the upstream fields/payload. Only shorten duration/rate/backlog in smoke.
  let effective = source.replace(/^(payloadFile:\s*)["']?([^"'\n]+)["']?\s*$/m, '$1"/src/$2"');
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

export function ombDriver(vendor, rf, maxPollIntervalMs, retentionBytes = -1) {
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
  retention.bytes=${retentionBytes}
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

export async function prepareOmb(directory, command, oneShot) {
  const source = path.join(directory, 'openmessaging');
  const m2 = path.join(directory, 'm2');
  await fs.mkdir(source);
  await fs.mkdir(m2);
  await command('git', ['-c', 'init.templateDir=', 'init', source]);
  await command('git', ['-C', source, 'remote', 'add', 'origin', OMB.repository]);
  await command('git', ['-C', source, 'fetch', '--depth=1', 'origin', OMB.commit]);
  await command('git', ['-C', source, 'checkout', '--detach', 'FETCH_HEAD']);
  assert.equal((await command('git', ['-C', source, 'rev-parse', 'HEAD'])).trim(), OMB.commit);
  await command('docker', ['pull', '--platform', 'linux/amd64', OMB.build_image], { timeout: 600_000 });
  const image = JSON.parse(await command('docker', ['image', 'inspect', OMB.build_image]))[0];
  await oneShot(['--user', `${process.getuid()}:${process.getgid()}`, '--cpus', '4', '--memory', '4g', '--memory-swap', '4g',
    '--env', 'MAVEN_CONFIG=/m2', '--volume', `${source}:/src`, '--volume', `${m2}:/m2`, '--workdir', '/src',
    OMB.build_image, 'mvn', '-B', '-ntp', '-Dmaven.repo.local=/m2', '-Dmaven.test.skip=true',
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
    provenance: { ...OMB, build_image_id: image.Id, jars } };
}

export function validateOmbResult(result, vendor, rf, config, logs = '') {
  // Upstream catches workload exceptions and can exit zero without a result.
  // A process exit status alone is never enough to mark a trial successful.
  // One exception: at the end of a maximum-rate run, upstream closes the
  // producer while its send loop is still blocked waiting for buffer memory,
  // and LocalWorker logs the resulting KafkaException. It follows the final
  // aggregated results, so it says nothing about the measurement.
  const errors = logs.replace(/\]\s+ERROR\s+LocalWorker - Got error\r?\norg\.apache\.kafka\.common\.KafkaException: Producer closed while allocating memory\b/g, '');
  assert.ok(!/\]\s+ERROR\s/.test(errors), 'OMB logged an error; inspect workload logs');
  assert.equal(result.driver, `${vendor}-rf${rf}`, 'wrong OMB driver');
  for (const [key, expected] of [['topics', config.topics], ['partitions', config.partitionsPerTopic], ['messageSize', config.messageSize]]) {
    assert.equal(result[key], expected, `wrong OMB ${key}`);
  }
  const minimumSamples = config.testDurationMinutes * 6;
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
  assert.equal(trials.length, provenance.cases.length * provenance.replication_factors.length * 3 * provenance.repetitions,
    'incomplete OMB matrix');
  const actual = new Set();
  for (const trial of trials) {
    validateOmbResult(trial.omb, trial.vendor, trial.rf, trial.case.config);
    actual.add(`${trial.vendor}/${trial.rf}/${trial.repetition}/${trial.case.id}`);
  }
  assert.equal(actual.size, trials.length, 'duplicate OMB trials');
  for (const workload of provenance.cases) for (const rf of provenance.replication_factors) {
    for (const vendor of ['krabka', 'kafka', 'redpanda']) for (let repetition = 1; repetition <= provenance.repetitions; repetition++) {
      assert.ok(actual.has(`${vendor}/${rf}/${repetition}/${workload.id}`), 'missing OMB trial');
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
  await fs.writeFile(path.join(directory, 'summary.md'), `${lines.join('\n')}\n`);
}
