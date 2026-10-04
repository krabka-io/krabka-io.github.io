import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { gunzipSync } from 'node:zlib';
import { CASES, VENDORS, median, aggregateSample, resourceSummary, resourceTimeSeries, validateDelivery, validateComplete, publishResults } from './benchmark-results.mjs';
import { curveCases, curveBudget, curveSummary, validateTimeline } from './benchmark-curves.mjs';
import { ombCases, ombWorkload, ombDriver, validateOmbResult, writeOmbReport } from './benchmark-openmessaging.mjs';

test('OpenMessaging rejects failed or truncated upstream captures and incomplete matrices', async () => {
  const config = { topics: 1, partitionsPerTopic: 16, messageSize: 1024, testDurationMinutes: 1 };
  const capture = { driver: 'krabka-rf3', topics: 1, partitions: 16, messageSize: 1024,
    publishRate: Array(6).fill(5000), consumeRate: Array(6).fill(5000),
    publishErrorRate: Array(6).fill(0), backlog: Array(6).fill(0),
    aggregatedPublishLatency99pct: 1, aggregatedEndToEndLatency99pct: 2 };
  validateOmbResult(capture, 'krabka', 3, config);
  validateOmbResult({ ...capture, aggregatedEndToEndLatency99pct: 0 }, 'krabka', 3, config);
  assert.throws(() => validateOmbResult(capture, 'krabka', 3, config, '20:00:00 [consumer] ERROR ConsumerCoordinator - Offset commit failed'), /logged an error/);
  assert.throws(() => validateOmbResult({}, 'krabka', 3, config));
  assert.throws(() => validateOmbResult({ ...capture, publishErrorRate: [0, 0, 0, 0, 0, 1] }, 'krabka', 3, config), /publish errors/);
  assert.throws(() => validateOmbResult({ ...capture, consumeRate: Array(6).fill(0) }, 'krabka', 3, config), /consumed no/);
  assert.throws(() => validateOmbResult({ ...capture, publishRate: [5000] }, 'krabka', 3, config), /truncated/);
  assert.throws(() => validateOmbResult({ ...capture, driver: 'kafka-rf3' }, 'krabka', 3, config), /wrong OMB driver/);
  await assert.rejects(writeOmbReport('/unused', { cases: [{}], replication_factors: [1, 3], repetitions: 1 }, []), /incomplete/);
});

test('OpenMessaging catalog and smoke configuration keep comparison settings consistent', () => {
  assert.equal(ombCases().length, 13);
  assert.throws(() => ombCases('../outside'), /unknown/);
  assert.throws(() => ombCases('simple-workload,simple-workload'), /duplicate/);
  const source = 'topics: 1\npartitionsPerTopic: 16\nmessageSize: 1024\npayloadFile: "payload/payload-1Kb.data"\nproducerRate: 100000\nconsumerBacklogSizeGB: 100\ntestDurationMinutes: 15\n';
  const full = ombWorkload(source, false);
  assert.equal(full.config.consumerBacklogSizeGB, 100);
  assert.equal(full.config.testDurationMinutes, 15);
  assert.match(full.yaml, /payloadFile: "\/src\/payload\/payload-1Kb.data"/);
  const smoke = ombWorkload(source, true);
  assert.equal(smoke.config.consumerBacklogSizeGB, 0);
  assert.equal(smoke.config.producerRate, 5000);
  assert.equal(smoke.config.testDurationMinutes, 1);
  assert.equal(full.sha256, smoke.sha256);
  for (const vendor of ['krabka', 'kafka', 'redpanda']) {
    const driver = ombDriver(vendor, 3);
    assert.match(driver, /acks=all\n  enable.idempotence=true/);
    assert.match(driver, /replicationFactor: 3/);
    assert.match(driver, /batch.size=1048576/);
    assert.match(driver, vendor === 'redpanda' ? /write.caching=true/ : /min.insync.replicas=2/);
  }
});

const broker = (id, cpu, rss, current, inactive = 0) => ({ id, cpu_usage_us: cpu, rss_bytes: rss,
  memory_current_bytes: current, inactive_file_bytes: inactive, anon_bytes: rss / 2, oom_kill: 0 });
const delivery = records => ({ sent: records, consumed: records, duplicates: 0, errors: 0,
  seconds: 10, records_per_second: records / 10, mib_per_second: 1,
  latency_ms_p50: 1, latency_ms_p95: 2, latency_ms_p99: 3 });

function completeRun() {
  const provenance = { mode: 'full', status: 'complete', repetitions: 3,
    run_id: '2026-10-03T04-00-00Z-12345678', completed_at: '2026-10-03T04:00:00Z',
    images: Object.fromEntries(VENDORS.map(v => [v, { requested: `${v}:version`, reference: `${v}@sha256:${'a'.repeat(64)}` }])),
    host: { cpu_model: 'test host', logical_cpus: 16, kernel: 'Linux', memory_total_bytes: 64 * 1024 ** 3 },
    cpu_sets: { brokers: [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]], client: [12, 13] },
  };
  const trials = [];
  for (const rf of [1, 3]) for (const vendor of VENDORS) for (let repetition = 1; repetition <= 3; repetition++) {
    for (const workload of CASES) {
      const measured = delivery(workload.records);
      const samples = [0, 250].map((elapsed_ms, i) => ({ elapsed_ms,
        brokers: Array.from({ length: rf }, (_, n) => broker(`broker-${n}`, (i + 1) * 1e6, 100, 200)) }));
      trials.push({ rf, vendor, repetition, case: { ...workload }, workload: measured,
        metrics: { ...measured, ...resourceSummary(samples, workload.records) },
        time_series: resourceTimeSeries(samples, provenance.completed_at) });
    }
  }
  return { provenance, trials };
}

test('medians use numeric ordering, handle even samples, and preserve raw values', () => {
  const values = [100, 2, 10];
  assert.equal(median(values), 10);
  assert.deepEqual(values, [100, 2, 10]);
  assert.equal(median([1, 3, 5, 9]), 4);
  assert.throws(() => median([]));
  assert.throws(() => median([NaN]));
});

function curveRun() {
  const { provenance } = completeRun();
  provenance.suite = 'curves';
  provenance.cases = curveCases();
  const trials = [];
  for (const workload of provenance.cases) for (const vendor of VENDORS) for (let repetition = 1; repetition <= 3; repetition++) {
    const samples = [];
    let acknowledged = 0, consumed = 0;
    const duration = workload.warmup_seconds + workload.seconds;
    for (let i = 1; i <= duration + 1; i++) {
      const ackDelta = i <= duration ? 100 : 0;
      const consumeDelta = i <= duration ? 90 : acknowledged - consumed;
      acknowledged += ackDelta;
      consumed += consumeDelta;
      const offered = workload.rate > 0 ? Math.min(i, duration) * workload.rate : acknowledged;
      samples.push({ elapsed_ms: i * 1000, interval_ms: 1000,
        phase: i <= workload.warmup_seconds ? 'warmup' : i <= duration ? 'measure' : 'drain',
        offered_records_per_second: workload.rate, offered_records: offered,
        submitted: acknowledged, acknowledged, consumed, errors: 0,
        ack_records_per_second: ackDelta, consume_records_per_second: consumeDelta,
        consumer_lag_records: acknowledged - consumed, offered_backlog_records: Math.max(0, offered - consumed),
        ack_latency_ms_p99: ackDelta ? 1 : null, latency_ms_p99: 2, latency_samples: consumeDelta,
        client_cpu_seconds: i / 10, client_cpu_cores: 0.1, client_heap_used_bytes: 1000 });
    }
    const timeline = { schema_version: 1, started_at: provenance.completed_at, sampling_interval_ms: 1000, samples };
    const measured = { sent: acknowledged, consumed, errors: 0, duplicates: 0,
      seconds: duration + 1, latency_ms_p50: 1, latency_ms_p95: 1.5, latency_ms_p99: 2,
      ack_latency_ms_p99: 1, measured_latency_samples: workload.seconds * 100 };
    const events = workload.kind === 'recovery' ? [
      { action: 'pause', broker_id: 0, elapsed_ms: 20_000, at: provenance.completed_at },
      { action: 'unpause', broker_id: 0, elapsed_ms: 30_000, at: provenance.completed_at },
    ] : [];
    const resourceSamples = [0, (duration + 1) * 1000].map((elapsed_ms, i) => ({ elapsed_ms,
      brokers: Array.from({ length: workload.rf }, (_, n) => broker(`broker-${n}`, (i + 1) * 1e6, 100, 200)) }));
    trials.push({ vendor, repetition, rf: workload.rf, case: workload, budget: curveBudget(workload, vendor),
      workload: measured, workload_time_series: timeline, events, recovered_topic: { full_isr: true },
      time_series: resourceTimeSeries(resourceSamples, provenance.completed_at),
      metrics: resourceSummary(resourceSamples, measured.sent),
      curve: curveSummary(workload, measured, timeline, events) });
  }
  return { provenance, trials };
}

test('curve plan fits the local RF3 envelope and tunes allocations within each memory budget', () => {
  const cases = curveCases();
  assert.equal(cases.length, 8);
  assert.deepEqual(cases.filter(c => c.kind === 'memory').map(c => c.memory_gib), [2, 4, 8]);
  for (const workload of cases) {
    assert.ok(workload.rf * workload.memory_gib + 4 <= 16);
    for (const vendor of VENDORS) {
      const budget = curveBudget(workload, vendor);
      assert.ok((budget.kafka_heap_bytes ?? 0) < budget.broker_memory_bytes);
      assert.ok((budget.redpanda_memory_bytes ?? 0) + (budget.redpanda_reserve_bytes ?? 0) < budget.broker_memory_bytes);
    }
  }
});

test('curve summaries use achieved interval rates and retain overload and recovery evidence', () => {
  const { provenance, trials } = curveRun();
  validateComplete(provenance, trials);
  assert.equal(trials[0].curve.records_per_second, 100);
  assert.equal(trials[0].curve.consumer_lag_peak_records, 350);
  assert.ok(trials[0].curve.offered_backlog_at_measurement_end > 0);
  assert.equal(trials.find(t => t.case.kind === 'recovery').curve.recovery_seconds, null);
  const recovered = structuredClone(trials.find(t => t.case.kind === 'recovery'));
  let ack = 0, consumed = 0;
  for (const sample of recovered.workload_time_series.samples) {
    const drained = sample.phase === 'drain';
    const resumed = sample.elapsed_ms > 30_000;
    const ackDelta = drained ? 0 : resumed ? recovered.case.rate : 100;
    const consumeDelta = drained ? ack - consumed : resumed ? ack + ackDelta - consumed : 90;
    ack += ackDelta;
    consumed += consumeDelta;
    Object.assign(sample, { submitted: ack, acknowledged: ack, consumed,
      ack_records_per_second: ackDelta, consume_records_per_second: consumeDelta,
      consumer_lag_records: ack - consumed, offered_backlog_records: sample.offered_records - consumed,
      latency_samples: consumeDelta, latency_ms_p99: consumeDelta ? 2 : null });
  }
  Object.assign(recovered.workload, { sent: ack, consumed });
  assert.equal(curveSummary(recovered.case, recovered.workload, recovered.workload_time_series, recovered.events).recovery_seconds, 3);
  for (const corrupt of [
    t => { t.workload_time_series.samples[0].interval_ms = 500; },
    t => { t.workload_time_series.samples[1].acknowledged = 0; },
    t => { t.workload_time_series.samples[0].consumer_lag_records = 0; },
    t => { t.workload_time_series.samples[0].latency_ms_p99 = null; },
    t => { t.curve.records_per_second = 200; },
    t => { t.budget.broker_memory_bytes++; },
  ]) {
    const invalid = structuredClone(trials);
    corrupt(invalid[0]);
    assert.throws(() => validateComplete(provenance, invalid));
  }
  const recovery = trials.find(t => t.case.kind === 'recovery');
  assert.throws(() => validateTimeline(recovery.case, recovery.workload, recovery.workload_time_series, []));
  const missingIsr = structuredClone(trials);
  delete missingIsr.find(t => t.case.kind === 'recovery').recovered_topic;
  assert.throws(() => validateComplete(provenance, missingIsr), /replicas did not recover/);
});

test('curve publication retains all three chart datasets and keeps the throughput report', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'benchmark-curves-'));
  try {
    await fs.mkdir(path.join(root, 'benchmarks'));
    await fs.writeFile(path.join(root, 'benchmarks', 'latest.md'), 'original throughput report');
    const { provenance, trials } = curveRun();
    await publishResults(root, provenance, trials);
    const charts = JSON.parse(await fs.readFile(path.join(root, 'benchmarks', 'results', provenance.run_id, 'charts.json'), 'utf8'));
    assert.equal(charts.latency_vs_offered_throughput.length, 36);
    assert.equal(charts.throughput_vs_memory_budget.length, 27);
    assert.equal(charts.recovery.length, 9);
    assert.deepEqual(charts.recovery[0].timeline, trials.find(t => t.case.kind === 'recovery').workload_time_series);
    assert.equal(await fs.readFile(path.join(root, 'benchmarks', 'latest.md'), 'utf8'), 'original throughput report');
    assert.match(await fs.readFile(path.join(root, 'benchmarks', 'latest-curves.md'), 'utf8'), /End-to-end p99/);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});

test('archived full curve collection preserves the failed recovery outcome and cannot publish', async () => {
  const directory = new URL('../benchmarks/diagnostics/2026-10-03T07-52-36Z-230b0a75/', import.meta.url);
  const read = async name => JSON.parse(await fs.readFile(new URL(name, directory), 'utf8'));
  const compressed = async name => JSON.parse(gunzipSync(await fs.readFile(new URL(name, directory))));
  const provenance = await read('provenance.json');
  const { trials, failures, attempted_trials } = await compressed('collection.json.gz');
  const charts = await compressed('charts.json.gz');
  assert.equal(attempted_trials, 72);
  assert.equal(trials.length, 71);
  assert.equal(failures.length, 1);
  const outcomes = [...trials, ...failures];
  for (const c of provenance.cases) for (const vendor of VENDORS) for (let repetition = 1; repetition <= 3; repetition++) {
    assert.equal(outcomes.filter(t => t.case.id === c.id && t.vendor === vendor && t.repetition === repetition).length, 1);
  }
  for (const trial of trials) {
    assert.deepEqual(trial.curve, curveSummary(trial.case, trial.workload, trial.workload_time_series, trial.events));
  }
  assert.equal(charts.status, 'failed');
  assert.equal(charts.latency_vs_offered_throughput.length, 36);
  assert.equal(charts.throughput_vs_memory_budget.length, 27);
  assert.equal(charts.recovery.length, 9);
  const failed = charts.recovery.filter(t => t.status === 'failed');
  assert.equal(failed.length, 1);
  assert.equal(failed[0].vendor, 'krabka');
  assert.equal(failed[0].delivery.duplicates, 59);
  assert.equal(failed[0].curve, null);
  assert.throws(() => validateComplete(provenance, trials), /run is incomplete/);
  assert.throws(() => validateComplete({ ...provenance, status: 'complete' }, trials), /matrix is incomplete/);
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

test('delivery requires exact counts, unique sequences, no errors, and ordered quantiles', () => {
  validateDelivery(delivery(100), 100);
  for (const change of [{ sent: 99 }, { consumed: 99 }, { duplicates: 1 }, { errors: 1 },
    { seconds: 0 }, { records_per_second: Infinity }, { latency_ms_p95: 4 }]) {
    assert.throws(() => validateDelivery({ ...delivery(100), ...change }, 100));
  }
});

test('publication requires every vendor, topology, case, and three repetitions', () => {
  const { provenance, trials } = completeRun();
  validateComplete(provenance, trials);
  assert.throws(() => validateComplete(provenance, trials.slice(1)));
  const duplicated = structuredClone(trials);
  duplicated[0] = structuredClone(duplicated[1]);
  assert.throws(() => validateComplete(provenance, duplicated));
  const modified = structuredClone(trials);
  modified[0].case.rate = 1000;
  assert.throws(() => validateComplete(provenance, modified));
  assert.throws(() => validateComplete({ ...provenance, status: 'interrupted' }, trials));
  assert.throws(() => validateComplete({ ...provenance, mode: 'smoke' }, trials));
  for (const corrupt of [
    t => { delete t.time_series; },
    t => { t.time_series.samples.pop(); },
    t => { t.time_series.samples[1].cluster.cpu_cores = 0; },
    t => { t.metrics.rss_peak_bytes++; },
  ]) {
    const invalid = structuredClone(trials);
    corrupt(invalid[0]);
    assert.throws(() => validateComplete(provenance, invalid));
  }
});

test('failed and smoke publication preserve existing history and latest report', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'benchmark-publication-'));
  try {
    await fs.mkdir(path.join(root, 'benchmarks'));
    await fs.writeFile(path.join(root, 'benchmarks', 'latest.md'), 'previous report');
    const { provenance, trials } = completeRun();
    for (const status of ['failed', 'interrupted']) {
      await assert.rejects(publishResults(root, { ...provenance, status }, trials));
    }
    await assert.rejects(publishResults(root, { ...provenance, mode: 'smoke' }, trials));
    await assert.rejects(publishResults(root, provenance, trials, AbortSignal.abort(new Error('interrupted'))));
    await assert.rejects(publishResults(root, provenance, trials.slice(1)));
    const missingSeries = structuredClone(trials);
    delete missingSeries[0].time_series;
    await assert.rejects(publishResults(root, provenance, missingSeries), /time series missing/);
    assert.equal(await fs.readFile(path.join(root, 'benchmarks', 'latest.md'), 'utf8'), 'previous report');
    assert.deepEqual(await fs.readdir(path.join(root, 'benchmarks')), ['latest.md']);
    await publishResults(root, provenance, trials);
    const dated = path.join(root, 'benchmarks', 'results', provenance.run_id);
    assert.equal((await fs.readdir(path.join(dated, 'trials'))).length, 108);
    const retained = JSON.parse(await fs.readFile(path.join(dated, 'trials', 'rf3-krabka-1-1k-random-lz4.json'), 'utf8'));
    assert.deepEqual(retained.time_series, trials.find(t => t.rf === 3 && t.vendor === 'krabka'
      && t.repetition === 1 && t.case.id === '1k-random-lz4').time_series);
    const latest = await fs.readFile(path.join(root, 'benchmarks', 'latest.md'), 'utf8');
    assert.match(latest, /RF1/);
    assert.match(latest, /RF3/);
    assert.match(latest, /redpanda/);
    assert.match(latest, /minimum–maximum range/);
    await assert.rejects(publishResults(root, provenance, trials), /already published/);
    assert.equal(await fs.readFile(path.join(root, 'benchmarks', 'latest.md'), 'utf8'), latest);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});
