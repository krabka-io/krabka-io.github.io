import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { CASES, VENDORS, METRICS, median, validateComplete } from '../../scripts/benchmark-results.mjs';

const root = path.resolve('benchmarks');

// Follow the published pointers, rather than picking a possibly incomplete directory.
function loadReport(pointer) {
  const latest = fs.readFileSync(path.join(root, pointer), 'utf8');
  const match = latest.match(/^\[Dated report and per-trial results\]\(results\/([\w-]+)\/summary\.md\)/);
  assert.ok(match, 'latest benchmark report must link to a dated run');
  const directory = path.join(root, 'results', match[1]);
  const provenance = JSON.parse(fs.readFileSync(path.join(directory, 'provenance.json'), 'utf8'));
  assert.equal(provenance.run_id, match[1], 'benchmark provenance must match the latest report');
  const trials = fs.readdirSync(path.join(directory, 'trials')).filter(name => name.endsWith('.json'))
    .map(name => JSON.parse(fs.readFileSync(path.join(directory, 'trials', name), 'utf8')));
  validateComplete(provenance, trials);
  return { provenance, trials, sourceURL: 'https://github.com/krabka-io/krabka-io.github.io/tree/main/benchmarks/results/' + match[1] };
}
const throughput = loadReport('latest.md');
const curves = loadReport('latest-curves.md');
export const provenance = throughput.provenance;
export const curveProvenance = curves.provenance;

const names = { krabka: 'krabka', kafka: 'Apache Kafka', redpanda: 'Redpanda' };
export const vendors = VENDORS.map(id => ({
  id,
  name: names[id],
  version: provenance.images[id].requested.split(':').at(-1).replace(/^v/, ''),
  ...provenance.images[id],
}));
export const sourceURL = throughput.sourceURL;
export const curveSourceURL = curves.sourceURL;
export const methodologyURL = 'https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md';

export const results = [1, 3].map(rf => ({
  rf,
  cases: CASES.map(workload => ({
    ...workload,
    label: `${workload.bytes === 100 ? '100 B' : workload.bytes === 1024 ? '1 KiB' : '100 KiB'} ${workload.payload === 'zeros' ? 'zeros' : 'random'} · ${workload.compression === 'lz4' ? 'LZ4' : 'uncompressed'}${workload.rate > 0 ? ' · 20,000 records/s limit' : ''}`,
    rows: vendors.map(vendor => {
      const selected = throughput.trials.filter(t => t.rf === rf && t.vendor === vendor.id && t.case.id === workload.id)
        .sort((a, b) => a.repetition - b.repetition);
      return {
        ...vendor,
        metrics: Object.fromEntries(METRICS.map(key => {
          const values = selected.map(t => t.metrics[key]);
          return [key, { median: median(values), min: Math.min(...values), max: Math.max(...values) }];
        })),
      };
    }),
  })),
}));

const curveMetricKeys = ['records_per_second', 'latency_ms_p99', 'ack_latency_ms_p99',
  'offered_backlog_at_measurement_end', 'recovery_seconds'];
export const viewerCases = [
  ...curveProvenance.cases.map(workload => ({
    ...workload, suite: 'curves', category: workload.kind === 'latency' ? 'load' : workload.kind,
    key: 'curves-' + workload.id,
    label: workload.kind === 'latency' ? format(workload.rate) + ' offered records/s'
      : workload.kind === 'memory' ? workload.memory_gib + ' GiB per broker' : 'Leader pause and resume',
  })),
  ...results.flatMap(topology => topology.cases.map(workload => ({
    ...CASES.find(c => c.id === workload.id), suite: 'throughput', category: 'throughput',
    rf: topology.rf, key: 'throughput-rf' + topology.rf + '-' + workload.id,
    label: 'RF' + topology.rf + ' · ' + workload.label,
  }))),
].map(workload => {
  const report = workload.suite === 'curves' ? curves : throughput;
  return { ...workload, run_id: report.provenance.run_id, sourceURL: report.sourceURL,
    summaries: vendors.map(vendor => {
      const selected = report.trials.filter(t => t.vendor === vendor.id && t.rf === workload.rf && t.case.id === workload.id);
      const keys = workload.suite === 'curves' ? [...curveMetricKeys, 'working_set_peak_bytes'] : METRICS;
      return { id: vendor.id, name: vendor.name, metrics: Object.fromEntries(keys.map(key => {
        const values = selected.map(t => key in (t.curve ?? {}) ? t.curve[key] : t.metrics[key]);
        const valid = values.filter(Number.isFinite);
        return [key, { median: valid.length === values.length ? median(valid) : null,
          min: valid.length ? Math.min(...valid) : null, max: valid.length ? Math.max(...valid) : null }];
      })) };
    }),
  };
});

// Serve one case/repetition at a time. Keep every sample, but omit raw container/topic metadata.
export function viewerData(key, repetition) {
  const workload = viewerCases.find(c => c.key === key);
  assert.ok(workload && [1, 2, 3].includes(repetition), 'unknown benchmark selection');
  const report = workload.suite === 'curves' ? curves : throughput;
  return { case: workload, repetition, trials: vendors.map(vendor => {
    const trial = report.trials.find(t => t.vendor === vendor.id && t.rf === workload.rf
      && t.case.id === workload.id && t.repetition === repetition);
    const resourceKeys = ['cpu_cores', 'rss_bytes', 'anon_bytes', 'working_set_bytes'];
    return { id: vendor.id, name: vendor.name, version: report.provenance.images[vendor.id].requested.split(':').at(-1).replace(/^v/, ''), metrics: trial.metrics, curve: trial.curve,
      budget: trial.budget, events: (trial.events ?? []).map(({ action, broker_id, elapsed_ms }) => ({ action, broker_id, elapsed_ms })),
      workload_time_series: trial.workload_time_series,
      time_series: { started_at: trial.time_series.started_at, sampling_interval_ms: trial.time_series.sampling_interval_ms,
        samples: trial.time_series.samples.map(sample => ({ elapsed_ms: sample.elapsed_ms,
          cluster: Object.fromEntries(resourceKeys.map(k => [k, sample.cluster[k]])) })) },
      sourceURL: report.sourceURL + '/trials/rf' + workload.rf + '-' + vendor.id + '-' + repetition + '-' + workload.id + '.json',
    };
  }) };
}

export function format(value, digits = 0) { return value.toLocaleString('en-US', {
  minimumFractionDigits: digits,
  maximumFractionDigits: digits,
}); }
