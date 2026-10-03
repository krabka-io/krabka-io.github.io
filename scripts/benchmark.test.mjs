import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { CASES, VENDORS, median, aggregateSample, resourceSummary, validateDelivery, validateComplete, publishResults } from './benchmark-results.mjs';

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
      trials.push({ rf, vendor, repetition, case: { ...workload }, workload: measured,
        metrics: { ...measured, cpu_seconds: 1, cpu_us_per_record: 1e6 / workload.records,
          rss_peak_bytes: 100, anon_peak_bytes: 50, working_set_peak_bytes: 200, resource_samples: 2 } });
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
    assert.equal(await fs.readFile(path.join(root, 'benchmarks', 'latest.md'), 'utf8'), 'previous report');
    assert.deepEqual(await fs.readdir(path.join(root, 'benchmarks')), ['latest.md']);
    await publishResults(root, provenance, trials);
    const dated = path.join(root, 'benchmarks', 'results', provenance.run_id);
    assert.equal((await fs.readdir(path.join(dated, 'trials'))).length, 108);
    const latest = await fs.readFile(path.join(root, 'benchmarks', 'latest.md'), 'utf8');
    assert.match(latest, /RF1/);
    assert.match(latest, /RF3/);
    assert.match(latest, /redpanda/);
    assert.match(latest, /minimum–maximum range/);
    await assert.rejects(publishResults(root, provenance, trials), /already published/);
    assert.equal(await fs.readFile(path.join(root, 'benchmarks', 'latest.md'), 'utf8'), latest);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});
