import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { VENDORS, median } from '../../scripts/benchmark-results.mjs';

const root = path.resolve('benchmarks');
const names = { krabka: 'krabka', kafka: 'Apache Kafka', redpanda: 'Redpanda' };
export const methodologyURL = 'https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md';
export function format(value, digits = 0) {
  return value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

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
  assert.equal(provenance.suite, 'openmessaging', 'only OpenMessaging results can drive the website');
  assert.equal(provenance.mode, 'full', 'smoke measurements cannot drive the website');
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
        assert.equal(selected.length, provenance.repetitions, 'missing or duplicate OMB trials');
        assert.deepEqual(selected.map(t => t.repetition).sort((a, b) => a - b),
          Array.from({ length: provenance.repetitions }, (_, i) => i + 1), 'invalid OMB repetitions');
        return { id: image.id, name: image.name,
          publish_rate: pick(selected, t => t.publish_rate),
          end_to_end_p99_ms: pick(selected, t => t.end_to_end_latency_ms.p99),
          publish_p99_ms: pick(selected, t => t.publish_latency_ms.p99) };
      }),
    })),
  }));
  return { provenance, images, rows, trials, sourceURL,
    archiveURL: fs.existsSync(path.join(root, 'archive', provenance.run_id + '.tar.gz'))
      ? 'https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/archive/README.md' : null,
    intervalsURL: fs.existsSync(path.join(directory, 'intervals.json'))
      ? sourceURL.replace('/tree/', '/blob/') + '/intervals.json' : null,
    summaryURL: sourceURL.replace('/tree/', '/blob/') + '/summary.md',
    provenanceURL: sourceURL.replace('/tree/', '/blob/') + '/provenance.json' };
}
export const openmessaging = loadOpenMessaging();
