// Publishes a complete OpenMessaging run from its Actions artifact.
//
//   node scripts/publish-openmessaging.mjs <artifact>/<run-id>
//
// The OMB suite never commits results itself. This script takes the run
// directory from a downloaded `openmessaging-<run>-<attempt>` artifact,
// rejects anything but a complete full-mode matrix (less any trials the
// provenance lists as omitted, with a reason), and writes
// benchmarks/openmessaging/<run-id>/ (provenance.json, summary.md and a
// compact trials.json, intervals.json and gzip-compressed raw OMB results) plus
// benchmarks/latest-openmessaging.md, which the website follows.
//
// A run in which krabka wrote its segments with preallocate=true and O_DIRECT
// (the workflow's krabka_storage=direct) is the site's comparison with
// Redpanda, which does its own direct I/O. It goes to
// benchmarks/openmessaging-direct/<run-id>/ and
// benchmarks/latest-openmessaging-direct.md and leaves the page-cache run's
// files alone.

import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { checkOmbOmissions, ombOmission, validateOmbResult } from './benchmark-openmessaging.mjs';
import { OMB_STORAGE as STORAGE, hostInterfered } from './benchmark-results.mjs';

const VENDORS = ['krabka', 'kafka', 'redpanda'];

const mean = (values) => values.reduce((a, b) => a + b, 0) / values.length;

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
    publish_latency_ms: {
      p50: o.aggregatedPublishLatency50pct,
      p99: o.aggregatedPublishLatency99pct,
      p999: o.aggregatedPublishLatency999pct,
    },
    end_to_end_latency_ms: {
      p50: o.aggregatedEndToEndLatency50pct,
      p99: o.aggregatedEndToEndLatency99pct,
      p999: o.aggregatedEndToEndLatency999pct,
    },
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
  // krabka must run as it ships, except for one published variant: segments
  // preallocated and written with O_DIRECT (--krabka-preallocate), the mode in
  // which krabka is compared with Redpanda. Broker settings passed with
  // --krabka-env and the io_uring seccomp profile stay experiments, kept but
  // not published.
  assert.ok(
    !provenance.contract?.krabka_env?.length,
    'a run with --krabka-env broker settings is an experiment and is not published',
  );
  assert.ok(
    !provenance.contract?.krabka_seccomp,
    "a run with --krabka-io-uring changes krabka's container security profile and is not published",
  );
  assert.ok(
    [undefined, true, false].includes(provenance.contract?.krabka_preallocate),
    'krabka_preallocate must be a boolean',
  );
  const storage = provenance.contract?.krabka_preallocate ? 'direct' : 'page-cache';
  assert.ok(
    provenance.contract?.test_duration_minutes === undefined,
    'a run with --test-duration-minutes departs from the upstream workloads and is not published',
  );
  await fs.access(path.join(directory, 'summary.md'));
  checkOmbOmissions(provenance, VENDORS);
  const trials = [];
  const rawResults = [];
  const interfered = [];
  for (const rf of provenance.replication_factors)
    for (const vendor of VENDORS) {
      for (let repetition = 1; repetition <= provenance.repetitions; repetition++)
        for (const workload of provenance.cases) {
          if (ombOmission(provenance, vendor, rf, workload.id)) continue;
          const file = path.join(
            directory,
            `rf${rf}-${vendor}-${repetition}-${workload.id}`,
            'trial.json',
          );
          const trial = JSON.parse(await fs.readFile(file, 'utf8'));
          assert.deepEqual(
            [trial.vendor, trial.rf, trial.repetition, trial.case.id],
            [vendor, rf, repetition, workload.id],
            `mismatched ${file}`,
          );
          validateOmbResult(trial.omb, vendor, rf, trial.case.config);
          // The runner fails a direct-mode trial whose brokers it could not
          // see holding their active segments open with O_DIRECT; check the
          // record anyway, so a fallback to buffered writes is never
          // published as direct I/O.
          if (storage === 'direct' && vendor === 'krabka')
            assert.equal(
              trial.direct_io?.verified,
              true,
              `${workload.id} RF${rf} krabka round ${repetition}: O_DIRECT not verified`,
            );
          if (hostInterfered(trial))
            interfered.push(`${workload.id} RF${rf} ${vendor} round ${repetition}`);
          trials.push(compactTrial(trial));
          // Keep the complete OMB object: interval rates, backlog, all latency
          // percentiles/histograms and any future upstream fields. No resampling
          // or reconstructed timestamps. Cgroup telemetry stays in the artifact.
          const filename = `omb-results/rf${rf}-${vendor}-${repetition}-${workload.id}.json.gz`;
          const contents = gzipSync(Buffer.from(`${JSON.stringify(trial.omb)}\n`));
          rawResults.push({
            vendor,
            rf,
            repetition,
            workload: workload.id,
            file: filename,
            samples: trial.omb.publishRate.length,
            sha256: createHash('sha256').update(contents).digest('hex'),
            contents,
          });
        }
    }
  // A comparison with a cell the host shaped is not a comparison. The
  // summary marks such trials with †; the run is kept and not published.
  assert.ok(
    interfered.length === 0,
    `the host interfered with ${interfered.join(', ')} (balloon inflation or direct reclaim during measurement); repeat the run; not published`,
  );
  return { provenance, storage, trials, rawResults };
}

export async function publish(directory, root = process.cwd()) {
  const { provenance, storage, trials, rawResults } = await loadRun(directory);
  const { directory: published, pointer } = STORAGE[storage];
  const destination = path.join(root, 'benchmarks', published, provenance.run_id);
  for (const { directory: other } of Object.values(STORAGE))
    assert.equal(
      await fs.stat(path.join(root, 'benchmarks', other, provenance.run_id)).then(
        () => true,
        () => false,
      ),
      false,
      'run already published',
    );
  await fs.mkdir(destination, { recursive: true });
  await fs.mkdir(path.join(destination, 'omb-results'));
  for (const result of rawResults)
    await fs.writeFile(path.join(destination, result.file), result.contents);
  await fs.writeFile(
    path.join(destination, 'intervals.json'),
    `${JSON.stringify(
      {
        schema_version: 1,
        run_id: provenance.run_id,
        format: 'gzip-compressed upstream OMB result JSON; samples retain upstream order and units',
        trials: rawResults.map(({ contents, ...metadata }) => metadata),
      },
      null,
      2,
    )}\n`,
  );
  await fs.copyFile(
    path.join(directory, 'provenance.json'),
    path.join(destination, 'provenance.json'),
  );
  const summary = await fs.readFile(path.join(directory, 'summary.md'), 'utf8');
  await fs.writeFile(path.join(destination, 'summary.md'), summary);
  await fs.writeFile(path.join(destination, 'trials.json'), `${JSON.stringify(trials, null, 2)}\n`);
  await fs.writeFile(
    path.join(root, 'benchmarks', pointer),
    `[Dated report and per-trial results](${published}/${provenance.run_id}/summary.md)\n\n${summary}`,
  );
  return { run_id: provenance.run_id, storage, trials: trials.length };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const directory = process.argv[2];
  assert.ok(directory, 'usage: node scripts/publish-openmessaging.mjs <artifact>/<run-id>');
  const { run_id, storage, trials } = await publish(path.resolve(directory));
  console.log(
    `Published OpenMessaging run ${run_id} (krabka storage: ${storage}): ${trials} trials.`,
  );
}
