// Publishes a complete OpenMessaging run from its Actions artifact.
//
//   node scripts/publish-openmessaging.mjs <artifact>/<run-id>
//
// The OMB suite never commits results itself. This script takes the run
// directory from a downloaded `openmessaging-<run>-<attempt>` artifact,
// rejects anything but a complete full-mode matrix, and writes
// benchmarks/openmessaging/<run-id>/ (provenance.json, summary.md and a
// compact trials.json, intervals.json and gzip-compressed raw OMB results) plus
// benchmarks/latest-openmessaging.md, which the website follows.

import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { validateOmbResult } from './benchmark-openmessaging.mjs';

const VENDORS = ['krabka', 'kafka', 'redpanda'];
const mean = values => values.reduce((a, b) => a + b, 0) / values.length;

// The figures the website shows for one trial, taken from OMB's own result.
export function compactTrial(trial) {
  const o = trial.omb;
  return {
    vendor: trial.vendor,
    rf: trial.rf,
    repetition: trial.repetition,
    case: { id: trial.case.id, config: trial.case.config },
    publish_rate: mean(o.publishRate),
    consume_rate: mean(o.consumeRate),
    backlog_max: Math.max(...o.backlog),
    publish_latency_ms: { p50: o.aggregatedPublishLatency50pct, p99: o.aggregatedPublishLatency99pct, p999: o.aggregatedPublishLatency999pct },
    end_to_end_latency_ms: { p50: o.aggregatedEndToEndLatency50pct, p99: o.aggregatedEndToEndLatency99pct, p999: o.aggregatedEndToEndLatency999pct },
    cpu_seconds: trial.metrics.cpu_seconds,
    rss_peak_bytes: trial.metrics.rss_peak_bytes,
    working_set_peak_bytes: trial.metrics.working_set_peak_bytes,
    // Counted by the runner; a trial from before it counted them ran when any
    // commit failure failed the trial, so it had none.
    retriable_commit_failures: trial.retriable_commit_failures ?? 0,
  };
}

export async function loadRun(directory) {
  const provenance = JSON.parse(await fs.readFile(path.join(directory, 'provenance.json'), 'utf8'));
  assert.equal(provenance.suite, 'openmessaging', 'not an OpenMessaging run');
  assert.equal(provenance.mode, 'full', 'only full runs are published; smoke is a wiring check');
  assert.equal(provenance.status, 'complete', 'run is not complete');
  // The published run is the site's standard comparison, so krabka must run
  // as it ships: no broker settings passed with --krabka-env and no
  // preallocate override. Those runs are experiments, kept but not published.
  assert.ok(!provenance.contract?.krabka_env?.length,
    'a run with --krabka-env broker settings is an experiment and is not published');
  assert.ok(!provenance.contract?.krabka_preallocate,
    'a run with --krabka-preallocate is an experiment and is not published');
  await fs.access(path.join(directory, 'summary.md'));
  const trials = [];
  const rawResults = [];
  for (const rf of provenance.replication_factors) for (const vendor of VENDORS) {
    for (let repetition = 1; repetition <= provenance.repetitions; repetition++) for (const workload of provenance.cases) {
      const file = path.join(directory, `rf${rf}-${vendor}-${repetition}-${workload.id}`, 'trial.json');
      const trial = JSON.parse(await fs.readFile(file, 'utf8'));
      assert.deepEqual([trial.vendor, trial.rf, trial.repetition, trial.case.id], [vendor, rf, repetition, workload.id], `mismatched ${file}`);
      validateOmbResult(trial.omb, vendor, rf, trial.case.config);
      trials.push(compactTrial(trial));
      // Keep the complete OMB object: interval rates, backlog, all latency
      // percentiles/histograms and any future upstream fields. No resampling
      // or reconstructed timestamps. Cgroup telemetry stays in the artifact.
      const filename = `omb-results/rf${rf}-${vendor}-${repetition}-${workload.id}.json.gz`;
      const contents = gzipSync(Buffer.from(`${JSON.stringify(trial.omb)}\n`));
      rawResults.push({ vendor, rf, repetition, workload: workload.id,
        file: filename, samples: trial.omb.publishRate.length,
        sha256: createHash('sha256').update(contents).digest('hex'), contents });
    }
  }
  return { provenance, trials, rawResults };
}

export async function publish(directory, root = process.cwd()) {
  const { provenance, trials, rawResults } = await loadRun(directory);
  const destination = path.join(root, 'benchmarks', 'openmessaging', provenance.run_id);
  assert.equal(await fs.stat(destination).then(() => true, () => false), false, 'run already published');
  await fs.mkdir(destination, { recursive: true });
  await fs.mkdir(path.join(destination, 'omb-results'));
  for (const result of rawResults) await fs.writeFile(path.join(destination, result.file), result.contents);
  await fs.writeFile(path.join(destination, 'intervals.json'), `${JSON.stringify({
    schema_version: 1, run_id: provenance.run_id,
    format: 'gzip-compressed upstream OMB result JSON; samples retain upstream order and units',
    trials: rawResults.map(({ contents, ...metadata }) => metadata),
  }, null, 2)}\n`);
  await fs.copyFile(path.join(directory, 'provenance.json'), path.join(destination, 'provenance.json'));
  const summary = await fs.readFile(path.join(directory, 'summary.md'), 'utf8');
  await fs.writeFile(path.join(destination, 'summary.md'), summary);
  await fs.writeFile(path.join(destination, 'trials.json'), `${JSON.stringify(trials, null, 2)}\n`);
  await fs.writeFile(path.join(root, 'benchmarks', 'latest-openmessaging.md'),
    `[Dated report and per-trial results](openmessaging/${provenance.run_id}/summary.md)\n\n${summary}`);
  return { run_id: provenance.run_id, trials: trials.length };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const directory = process.argv[2];
  assert.ok(directory, 'usage: node scripts/publish-openmessaging.mjs <artifact>/<run-id>');
  const { run_id, trials } = await publish(path.resolve(directory));
  console.log(`Published OpenMessaging run ${run_id}: ${trials} trials.`);
}
