import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { curveCases, curveBudget, curveSummary, curveReport } from './benchmark-curves.mjs';

export const VENDORS = ['krabka', 'kafka', 'redpanda'];
export const CASES = [
  { id: '1k-random-lz4', bytes: 1024, payload: 'random', compression: 'lz4', rate: -1, records: 10_000_000 },
  { id: '100b-random-lz4', bytes: 100, payload: 'random', compression: 'lz4', rate: -1, records: 10_000_000 },
  { id: '1k-zeros-lz4', bytes: 1024, payload: 'zeros', compression: 'lz4', rate: -1, records: 10_000_000 },
  { id: '1k-random-none', bytes: 1024, payload: 'random', compression: 'none', rate: -1, records: 5_000_000 },
  { id: '100k-random-lz4', bytes: 102400, payload: 'random', compression: 'lz4', rate: -1, records: 60_000 },
  { id: '1k-random-20k', bytes: 1024, payload: 'random', compression: 'lz4', rate: 20_000, records: 600_000 },
];
export const METRICS = [
  'records_per_second', 'mib_per_second', 'latency_ms_p50', 'latency_ms_p95', 'latency_ms_p99',
  'cpu_seconds', 'cpu_us_per_record', 'rss_peak_bytes', 'anon_peak_bytes', 'working_set_peak_bytes',
];

export function median(values) {
  assert.ok(values.length > 0 && values.every(Number.isFinite), 'expected finite measurements');
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

export function validateDelivery(workload, records) {
  assert.equal(workload.sent, records, 'acknowledged count differs');
  assert.equal(workload.consumed, records, 'consumed count differs');
  assert.equal(workload.duplicates, 0, 'duplicate sequences');
  assert.equal(workload.errors, 0, 'producer errors');
  for (const key of ['seconds', 'records_per_second', 'mib_per_second']) {
    assert.ok(Number.isFinite(workload[key]) && workload[key] > 0, `invalid ${key}`);
  }
  for (const key of ['latency_ms_p50', 'latency_ms_p95', 'latency_ms_p99']) {
    assert.ok(Number.isFinite(workload[key]) && workload[key] >= 0, `invalid ${key}`);
  }
  assert.ok(workload.latency_ms_p50 <= workload.latency_ms_p95
    && workload.latency_ms_p95 <= workload.latency_ms_p99, 'latency quantiles out of order');
}

// Sum simultaneous cluster samples before taking peaks; summing independent
// per-broker peaks would overstate a cluster peak that never actually occurred.
export function aggregateSample(brokers) {
  assert.ok(brokers.length > 0, 'no brokers sampled');
  const total = { cpu_usage_us: 0, rss_bytes: 0, anon_bytes: 0, working_set_bytes: 0 };
  for (const broker of brokers) {
    for (const key of ['cpu_usage_us', 'rss_bytes', 'anon_bytes', 'memory_current_bytes', 'inactive_file_bytes', 'oom_kill']) {
      assert.ok(Number.isFinite(broker[key]) && broker[key] >= 0, `missing or invalid counter: ${key}`);
    }
    assert.ok(broker.rss_bytes > 0, 'broker RSS unavailable');
    assert.equal(broker.oom_kill, 0, 'broker OOM kill');
    total.cpu_usage_us += broker.cpu_usage_us;
    total.rss_bytes += broker.rss_bytes;
    total.anon_bytes += broker.anon_bytes;
    total.working_set_bytes += Math.max(0, broker.memory_current_bytes - broker.inactive_file_bytes);
  }
  return total;
}

function resourceTotals(samples) {
  assert.ok(samples.length >= 2, 'at least two resource samples required');
  const count = samples[0].brokers.length;
  assert.ok(samples.every(s => s.brokers.length === count), 'incomplete cluster samples');
  assert.ok(samples.every(s => Number.isFinite(s.elapsed_ms) && s.elapsed_ms >= 0), 'invalid sample time');
  const totals = samples.map(s => aggregateSample(s.brokers));
  for (let i = 1; i < samples.length; i++) {
    assert.ok(samples[i].elapsed_ms > samples[i - 1].elapsed_ms, 'sample times must increase');
    for (let broker = 0; broker < count; broker++) {
      assert.equal(samples[i].brokers[broker].id, samples[0].brokers[broker].id, 'broker identity changed');
      assert.ok(samples[i].brokers[broker].cpu_usage_us >= samples[i - 1].brokers[broker].cpu_usage_us,
        'CPU counter reset');
    }
  }
  return totals;
}

export function resourceTimeSeries(samples, startedAt) {
  assert.ok(Number.isFinite(Date.parse(startedAt)), 'invalid measurement start');
  const totals = resourceTotals(samples);
  return {
    schema_version: 1,
    started_at: startedAt,
    sampling_interval_ms: 250,
    samples: samples.map((sample, i) => ({
      ...sample,
      cluster: {
        ...totals[i],
        cpu_seconds: (totals[i].cpu_usage_us - totals[0].cpu_usage_us) / 1e6,
        // Average CPU cores used over the actual preceding interval. The first
        // sample is a baseline, not a measured zero-CPU interval.
        cpu_cores: i === 0 ? null : (totals[i].cpu_usage_us - totals[i - 1].cpu_usage_us)
          / ((sample.elapsed_ms - samples[i - 1].elapsed_ms) * 1000),
      },
    })),
  };
}

export function resourceSummary(samples, acknowledged) {
  const totals = resourceTotals(samples);
  const cpuUs = totals.at(-1).cpu_usage_us - totals[0].cpu_usage_us;
  assert.ok(cpuUs > 0 && acknowledged > 0, 'invalid CPU or record count');
  return {
    cpu_seconds: cpuUs / 1e6,
    cpu_us_per_record: cpuUs / acknowledged,
    rss_peak_bytes: Math.max(...totals.map(t => t.rss_bytes)),
    anon_peak_bytes: Math.max(...totals.map(t => t.anon_bytes)),
    working_set_peak_bytes: Math.max(...totals.map(t => t.working_set_bytes)),
    resource_samples: samples.length,
    sampling_interval_ms: 250,
    max_sample_gap_ms: Math.max(...samples.slice(1).map((s, i) => s.elapsed_ms - samples[i].elapsed_ms)),
    measurement_wall_seconds: (samples.at(-1).elapsed_ms - samples[0].elapsed_ms) / 1000,
  };
}

export function validateComplete(provenance, trials) {
  assert.equal(provenance.mode, 'full', 'smoke runs cannot publish');
  assert.equal(provenance.status, 'complete', 'run is incomplete');
  assert.equal(provenance.repetitions, 3, 'full results require three repetitions');
  assert.ok(/^\d{4}-\d{2}-\d{2}T[\d-]+Z-[a-f0-9]+$/.test(provenance.run_id), 'invalid run id');
  for (const vendor of VENDORS) {
    assert.match(provenance.images[vendor].reference, /@sha256:[a-f0-9]{64}$/, 'image must be immutable');
  }
  if (provenance.suite === 'curves') {
    const cases = curveCases();
    assert.deepEqual(provenance.cases, cases, 'curve plan differs');
    assert.equal(trials.length, cases.length * VENDORS.length * 3, 'curve matrix is incomplete');
    for (const workload of cases) for (const vendor of VENDORS) for (let repetition = 1; repetition <= 3; repetition++) {
      const selected = trials.filter(t => t.case.id === workload.id && t.vendor === vendor && t.repetition === repetition);
      assert.equal(selected.length, 1, 'missing or duplicate curve trial');
      const trial = selected[0];
      assert.deepEqual(trial.case, workload);
      assert.equal(trial.rf, workload.rf);
      assert.deepEqual(trial.budget, curveBudget(workload, vendor), 'curve resource budget differs');
      validateResourceEvidence(trial);
      assert.deepEqual(trial.curve, curveSummary(workload, trial.workload, trial.workload_time_series, trial.events));
      if (workload.kind === 'recovery') assert.equal(trial.recovered_topic?.full_isr, true, 'replicas did not recover');
    }
    return;
  }
  assert.equal(trials.length, 2 * 3 * VENDORS.length * CASES.length, 'matrix is incomplete');
  for (const rf of [1, 3]) {
    for (const vendor of VENDORS) {
      for (let repetition = 1; repetition <= 3; repetition++) {
        for (const workload of CASES) {
          const matches = trials.filter(t => t.rf === rf && t.vendor === vendor
            && t.repetition === repetition && t.case.id === workload.id);
          assert.equal(matches.length, 1, `missing or duplicate trial: ${rf}/${vendor}/${repetition}/${workload.id}`);
          const trial = matches[0];
          assert.deepEqual(trial.case, workload, 'workload contract differs');
          validateDelivery(trial.workload, workload.records);
          for (const key of METRICS) {
            assert.ok(Number.isFinite(trial.metrics[key]) && trial.metrics[key] >= 0, `invalid ${key}`);
          }
          validateResourceEvidence(trial);
        }
      }
    }
  }
}

function validateResourceEvidence(trial) {
  assert.ok(trial.metrics.resource_samples >= 2, 'resource evidence missing');
  assert.ok(trial.time_series, 'resource time series missing');
  assert.ok(trial.time_series.samples.every(s => s.brokers.length === trial.rf), 'time series topology differs');
  assert.deepEqual(trial.time_series,
    resourceTimeSeries(trial.time_series.samples, trial.time_series.started_at), 'invalid resource time series');
  for (const [key, value] of Object.entries(resourceSummary(trial.time_series.samples, trial.workload.sent))) {
    assert.equal(trial.metrics[key], value, `time series and summary differ: ${key}`);
  }
}

export function report(provenance, trials) {
  if (provenance.suite === 'curves') return curveReport(provenance, trials);
  const lines = [
    '# Krabka, Kafka 4.3.1, and Redpanda — local benchmark', '',
    `Run: ${provenance.run_id}. Completed: ${provenance.completed_at}.`, '',
    'These are buffered-write, local end-to-end throughput measurements. Each row reports the median of three independent repetitions, followed by the observed minimum–maximum range. Comparisons apply only to these images, this host, and this workload.', '',
    '## Images and host', '',
    ...VENDORS.map(v => `- ${v}: \`${provenance.images[v].requested}\` → \`${provenance.images[v].reference}\`.`),
    `- Host: ${provenance.host.cpu_model}; ${provenance.host.logical_cpus} logical CPUs; ${provenance.host.kernel}; ${provenance.host.memory_total_bytes} bytes RAM.`,
    `- Broker CPU sets: ${provenance.cpu_sets.brokers.map(c => c.join(',')).join(' / ')}. Client CPUs: ${provenance.cpu_sets.client.join(',')}. Logical CPUs can be SMT siblings; topology is retained in provenance.`,
    '- Per broker: 4 logical CPUs, 4 CPU quota, 10 GiB memory, no swap, 131,072 open files. Kafka heap: 1 GiB. Redpanda: 4 shards, 8 GiB application memory, 1 GiB reserve.',
    '- Redpanda networking AIO control blocks: 1,024 per shard, explicitly fixed so three nodes fit the shared host AIO budget. Host sysctls are not changed.',
    '- Same Kafka 4.3.1 client jars for all vendors. Fresh cluster/storage per repetition; vendor order rotates. Warm-up: 3 million records for RF1, 1 million for RF3. Case order matches the tables.',
    '- Twelve partitions, RF1/minISR1 or RF3/minISR2, full ISR before each workload, acks=all, idempotence, 65,536-byte batches, 5 ms linger. All acknowledged records consumed exactly once, with no producer errors.',
    '- Kafka/Krabka acknowledge their in-sync replication contract; Redpanda uses Raft majority acknowledgment with write.caching=true. This does not establish identical failure or crash-durability guarantees.', '',
    '## Measurement boundaries', '',
    'Throughput and latency come from the Java producer/consumer workload. Broker CPU is the cgroup cpu.stat usage delta; its window includes client startup, initialization, and shutdown. CPU per record divides aggregate broker CPU by acknowledged records, not replicas. Memory is sampled every 250 ms, and RF3 peaks use the simultaneous sum across brokers. Working set is memory.current minus inactive_file; RSS excludes most disk page cache. Sampling gaps are retained per trial. Warm-up is excluded; brokers retain warm-up and earlier-case data until the repetition ends.', '',
    'Each per-trial JSON retains the CPU and memory time series with its UTC start, actual elapsed sample times, raw per-broker counters, simultaneous cluster totals, and interval CPU cores used. Throughput and latency remain whole-workload summaries.', '',
    'No TLS, authentication, tiered storage, compaction, restart, failure injection, or forced per-append fsync is tested. Redpanda write caching is explicitly enabled, without dev-container mode or unsafe-bypass-fsync. Results include shared-host and client bottlenecks; they are not universal performance rankings or production qualification.', '',
  ];
  function range(selected, key, scale = 1, digits = 2) {
    const values = selected.map(t => t.metrics[key] / scale);
    return `${median(values).toFixed(digits)} (${Math.min(...values).toFixed(digits)}–${Math.max(...values).toFixed(digits)})`;
  }
  for (const rf of [1, 3]) {
    lines.push(`## RF${rf}`, '',
      '| Workload | Broker | Records/s | Logical MiB/s | CPU µs/record | p50 ms | p95 ms | p99 ms |',
      '|---|---|---:|---:|---:|---:|---:|---:|');
    for (const workload of CASES) {
      for (const vendor of VENDORS) {
        const selected = trials.filter(t => t.rf === rf && t.vendor === vendor && t.case.id === workload.id);
        lines.push(`| ${workload.id} | ${vendor} | ${range(selected, 'records_per_second', 1, 0)} | ${range(selected, 'mib_per_second')} | ${range(selected, 'cpu_us_per_record')} | ${range(selected, 'latency_ms_p50')} | ${range(selected, 'latency_ms_p95')} | ${range(selected, 'latency_ms_p99')} |`);
      }
    }
    lines.push('', '| Workload | Broker | CPU seconds | Peak RSS MiB | Peak anonymous MiB | Peak working set MiB |',
      '|---|---|---:|---:|---:|---:|');
    for (const workload of CASES) {
      for (const vendor of VENDORS) {
        const selected = trials.filter(t => t.rf === rf && t.vendor === vendor && t.case.id === workload.id);
        lines.push(`| ${workload.id} | ${vendor} | ${range(selected, 'cpu_seconds')} | ${range(selected, 'rss_peak_bytes', 1048576)} | ${range(selected, 'anon_peak_bytes', 1048576)} | ${range(selected, 'working_set_peak_bytes', 1048576)} |`);
      }
    }
    lines.push('');
  }
  return `${lines.join('\n')}\n`;
}

export async function publishResults(root, provenance, trials, signal) {
  // Validate before touching either history or the latest report.
  validateComplete(provenance, trials);
  signal?.throwIfAborted();
  const base = path.join(root, 'benchmarks');
  const latestName = provenance.suite === 'curves' ? 'latest-curves.md' : 'latest.md';
  const destination = path.join(base, 'results', provenance.run_id);
  const temporary = `${destination}.tmp`;
  const latestTemporary = path.join(base, `${latestName}.${provenance.run_id}.tmp`);
  await fs.mkdir(path.dirname(destination), { recursive: true });
  assert.equal(await fs.stat(destination).then(() => true, () => false), false, 'run already published');
  await fs.mkdir(temporary);
  let historyPublished = false;
  let latestPublished = false;
  try {
    await fs.mkdir(path.join(temporary, 'trials'));
    for (const trial of trials) {
      const name = `rf${trial.rf}-${trial.vendor}-${trial.repetition}-${trial.case.id}.json`;
      await fs.writeFile(path.join(temporary, 'trials', name), `${JSON.stringify(trial, null, 2)}\n`);
    }
    await fs.writeFile(path.join(temporary, 'provenance.json'), `${JSON.stringify(provenance, null, 2)}\n`);
    if (provenance.suite === 'curves') {
      const charts = { schema_version: 1, run_id: provenance.run_id,
        latency_vs_offered_throughput: [], throughput_vs_memory_budget: [], recovery: [] };
      for (const trial of trials) {
        const point = { vendor: trial.vendor, repetition: trial.repetition, rf: trial.rf,
          case_id: trial.case.id, ...trial.curve };
        if (trial.case.kind === 'latency') charts.latency_vs_offered_throughput.push({ ...point,
          offered_records_per_second: trial.case.rate });
        if (trial.case.kind === 'memory') charts.throughput_vs_memory_budget.push({ ...point,
          memory_budget_bytes: trial.budget.broker_memory_bytes });
        if (trial.case.kind === 'recovery') charts.recovery.push({ ...point,
          timeline: trial.workload_time_series, events: trial.events });
      }
      await fs.writeFile(path.join(temporary, 'charts.json'), `${JSON.stringify(charts, null, 2)}\n`);
    }
    const markdown = report(provenance, trials);
    await fs.writeFile(path.join(temporary, 'summary.md'), markdown);
    await fs.writeFile(latestTemporary,
      `[Dated report and per-trial results](results/${provenance.run_id}/summary.md)\n\n${markdown}`);
    signal?.throwIfAborted();
    await fs.rename(temporary, destination);
    historyPublished = true;
    signal?.throwIfAborted();
    await fs.rename(latestTemporary, path.join(base, latestName));
    latestPublished = true;
  } finally {
    if (historyPublished && !latestPublished) await fs.rm(destination, { recursive: true, force: true });
    await fs.rm(temporary, { recursive: true, force: true });
    await fs.rm(latestTemporary, { force: true });
  }
}
