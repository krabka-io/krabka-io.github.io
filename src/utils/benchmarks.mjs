import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { VENDORS, median } from '../../scripts/benchmark-results.mjs';

const root = path.resolve('benchmarks');
const names = { krabka: 'krabka', kafka: 'Apache Kafka', redpanda: 'Redpanda' };
export const methodologyURL =
  'https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md';
export function format(value, digits = 0) {
  return value.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

const ombImageLabels = {
  'sha256:117df778e3e8af143d8bc3681233c0151dc97d51cfafd3844c4daa09afaf9690':
    'krabka-broker 1.0.0 (ada8e3ad, CI delivery image)',
  // The signed v1.0.1 release index and its amd64 image, which the runner pulls.
  'sha256:d0a383b12176a55eb771c870d819ccb4f60ce0bb6b5356b5ec44285c01d7fd93':
    'krabka-broker 1.0.1 (6c64d9a, signed release)',
  'sha256:299ff81dba7d4cbb96a4d2a7fca341c003929610e5be628ba0083587bd488c4d':
    'krabka-broker 1.0.1 (6c64d9a, signed release)',
  // The signed v1.0.3 release index and its amd64 image.
  'sha256:ee5b77a5e83474ae051e2ea99f5d70a92fae352255297846e82b89e9ced60103':
    'krabka-broker 1.0.3 (2de4008, signed release)',
  'sha256:22890f0b03a7adcc0341c92936c1c694841b4b8066af3b0ad5b8fd09a2ab87cb':
    'krabka-broker 1.0.3 (2de4008, signed release)',
  // The signed v1.0.5 release index and its amd64 image.
  'sha256:2a81d1ba38de4e8ffb032fedf67952845185158a7607ac76ba38fa162dc37e50':
    'krabka-broker 1.0.5 (ac9deff, signed release)',
  'sha256:8c515d335e89455525fb1ad4d9986769c9dd18e44fb259250919c6e7cb53bd84':
    'krabka-broker 1.0.5 (ac9deff, signed release)',
};
function latestRunId() {
  const pointer = path.join(root, 'latest-openmessaging.md');
  if (!fs.existsSync(pointer)) return null;
  const match = fs
    .readFileSync(pointer, 'utf8')
    .match(/^\[Dated report and per-trial results\]\(openmessaging\/([\w-]+)\/summary\.md\)/);
  assert.ok(match, 'latest OpenMessaging report must link to a dated run');
  return match[1];
}
export function loadOpenMessaging(id) {
  if (!id) return null;
  const directory = path.join(root, 'openmessaging', id);
  const provenance = JSON.parse(fs.readFileSync(path.join(directory, 'provenance.json'), 'utf8'));
  assert.equal(provenance.run_id, id, 'OpenMessaging provenance must match the report');
  assert.equal(
    provenance.suite,
    'openmessaging',
    'only OpenMessaging results can drive the website',
  );
  assert.equal(provenance.mode, 'full', 'smoke measurements cannot drive the website');
  assert.equal(provenance.status, 'complete', 'only complete OpenMessaging runs are published');
  const trials = JSON.parse(fs.readFileSync(path.join(directory, 'trials.json'), 'utf8'));
  // Trials the run left out on purpose, each with its reason. The publisher
  // checks the list; the page shows each one as not measured.
  const omitted = provenance.omitted ?? [];
  assert.ok(
    omitted.every(
      (o) => VENDORS.includes(o.vendor) && typeof o.reason === 'string' && o.reason.trim(),
    ),
    'every omitted OpenMessaging trial needs a broker and a reason',
  );
  assert.equal(
    trials.length,
    (provenance.cases.length * provenance.replication_factors.length * VENDORS.length -
      omitted.length) *
      provenance.repetitions,
    'incomplete OpenMessaging matrix',
  );
  const sourceURL =
    'https://github.com/krabka-io/krabka-io.github.io/tree/main/benchmarks/openmessaging/' + id;
  const images = VENDORS.map((id) => {
    const image = provenance.images[id];
    const digest = image.reference.split('@').at(-1);
    return {
      id,
      name: names[id],
      requested: image.requested,
      reference: image.reference,
      label:
        ombImageLabels[digest] ??
        `${names[id]} ${image.requested.split(':').at(-1).replace(/^v/, '')}`,
    };
  });
  const pick = (list, key) => (list.length ? median(list.map((t) => key(t))) : null);
  const rows = provenance.replication_factors.map((rf) => ({
    rf,
    cases: provenance.cases.map((workload) => ({
      id: workload.id,
      config: workload.config,
      vendors: images.map((image) => {
        const selected = trials.filter(
          (t) => t.rf === rf && t.vendor === image.id && t.case.id === workload.id,
        );
        const omission = omitted.find(
          (o) => o.rf === rf && o.vendor === image.id && o.workload === workload.id,
        );
        if (omission) {
          assert.equal(selected.length, 0, 'an omitted OMB trial has results');
          return {
            id: image.id,
            name: image.name,
            omitted: omission.reason,
            publish_rate: null,
            end_to_end_p99_ms: null,
            publish_p99_ms: null,
            rss_peak_bytes: null,
            cpu_seconds: null,
          };
        }
        assert.equal(selected.length, provenance.repetitions, 'missing or duplicate OMB trials');
        assert.deepEqual(
          selected.map((t) => t.repetition).sort((a, b) => a - b),
          Array.from({ length: provenance.repetitions }, (_, i) => i + 1),
          'invalid OMB repetitions',
        );
        return {
          id: image.id,
          name: image.name,
          publish_rate: pick(selected, (t) => t.publish_rate),
          end_to_end_p99_ms: pick(selected, (t) => t.end_to_end_latency_ms.p99),
          publish_p99_ms: pick(selected, (t) => t.publish_latency_ms.p99),
          rss_peak_bytes: pick(selected, (t) => t.rss_peak_bytes),
          cpu_seconds: pick(selected, (t) => t.cpu_seconds),
        };
      }),
    })),
  }));
  return {
    provenance,
    images,
    rows,
    trials,
    omitted,
    sourceURL,
    archiveURL: fs.existsSync(path.join(root, 'archive', provenance.run_id + '.tar.gz'))
      ? 'https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/archive/README.md'
      : null,
    intervalsURL: fs.existsSync(path.join(directory, 'intervals.json'))
      ? sourceURL.replace('/tree/', '/blob/') + '/intervals.json'
      : null,
    summaryURL: sourceURL.replace('/tree/', '/blob/') + '/summary.md',
    provenanceURL: sourceURL.replace('/tree/', '/blob/') + '/provenance.json',
  };
}
export const openmessaging = loadOpenMessaging(latestRunId());
export const benchmarkHistory = fs
  .readdirSync(path.join(root, 'openmessaging'))
  .filter((id) => fs.existsSync(path.join(root, 'openmessaging', id, 'trials.json')))
  .map(loadOpenMessaging)
  .sort((a, b) => a.provenance.completed_at.localeCompare(b.provenance.completed_at));
