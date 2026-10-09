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
  const sourceURL = 'https://github.com/krabka-io/krabka-io.github.io/tree/main/benchmarks/results/' + match[1];
  return { provenance, trials, sourceURL, fileBaseURL: sourceURL.replace('/tree/', '/blob/') };
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
export const provenanceURL = throughput.fileBaseURL + '/provenance.json';
export const curveProvenanceURL = curves.fileBaseURL + '/provenance.json';
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
      sourceURL: report.fileBaseURL + '/trials/rf' + workload.rf + '-' + vendor.id + '-' + repetition + '-' + workload.id + '.json',
    };
  }) };
}

export function format(value, digits = 0) { return value.toLocaleString('en-US', {
  minimumFractionDigits: digits,
  maximumFractionDigits: digits,
}); }

// OpenMessaging on Google Cloud, published from a complete workflow artifact by
// scripts/publish-openmessaging.mjs. Absent until a run is published.
const ombImageLabels = {
  'sha256:117df778e3e8af143d8bc3681233c0151dc97d51cfafd3844c4daa09afaf9690': 'krabka-broker 1.0.0 (ada8e3ad, CI delivery image)',
  // The signed v1.0.1 release index and its amd64 image, which the runner pulls.
  'sha256:d0a383b12176a55eb771c870d819ccb4f60ce0bb6b5356b5ec44285c01d7fd93': 'krabka-broker 1.0.1 (6c64d9a, signed release)',
  'sha256:299ff81dba7d4cbb96a4d2a7fca341c003929610e5be628ba0083587bd488c4d': 'krabka-broker 1.0.1 (6c64d9a, signed release)',
};
function loadOpenMessaging() {
  const pointer = path.join(root, 'latest-openmessaging.md');
  if (!fs.existsSync(pointer)) return null;
  const match = fs.readFileSync(pointer, 'utf8').match(/^\[Dated report and per-trial results\]\(openmessaging\/([\w-]+)\/summary\.md\)/);
  assert.ok(match, 'latest OpenMessaging report must link to a dated run');
  const directory = path.join(root, 'openmessaging', match[1]);
  const provenance = JSON.parse(fs.readFileSync(path.join(directory, 'provenance.json'), 'utf8'));
  assert.equal(provenance.run_id, match[1], 'OpenMessaging provenance must match the latest report');
  assert.equal(provenance.status, 'complete', 'only complete OpenMessaging runs are published');
  const trials = JSON.parse(fs.readFileSync(path.join(directory, 'trials.json'), 'utf8'));
  assert.equal(trials.length, provenance.cases.length * provenance.replication_factors.length * VENDORS.length * provenance.repetitions,
    'incomplete OpenMessaging matrix');
  const sourceURL = 'https://github.com/krabka-io/krabka-io.github.io/tree/main/benchmarks/openmessaging/' + match[1];
  const images = VENDORS.map(id => {
    const image = provenance.images[id];
    const digest = image.reference.split('@').at(-1);
    return { id, name: names[id], requested: image.requested, reference: image.reference,
      label: ombImageLabels[digest] ?? `${names[id]} ${image.requested.split(':').at(-1).replace(/^v/, '')}` };
  });
  const pick = (list, key) => list.length ? median(list.map(t => key(t))) : null;
  const rows = provenance.replication_factors.map(rf => ({
    rf,
    cases: provenance.cases.map(workload => ({
      id: workload.id,
      config: workload.config,
      vendors: images.map(image => {
        const selected = trials.filter(t => t.rf === rf && t.vendor === image.id && t.case.id === workload.id);
        return { id: image.id, name: image.name,
          publish_rate: pick(selected, t => t.publish_rate),
          end_to_end_p99_ms: pick(selected, t => t.end_to_end_latency_ms.p99),
          publish_p99_ms: pick(selected, t => t.publish_latency_ms.p99),
          cpu_seconds: pick(selected, t => t.cpu_seconds),
          rss_peak_mib: pick(selected, t => t.rss_peak_bytes / 1048576) };
      }),
    })),
  }));
  return { provenance, images, rows, sourceURL,
    summaryURL: sourceURL.replace('/tree/', '/blob/') + '/summary.md',
    provenanceURL: sourceURL.replace('/tree/', '/blob/') + '/provenance.json' };
}
export const openmessaging = loadOpenMessaging();
