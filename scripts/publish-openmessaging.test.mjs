import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { publish } from './publish-openmessaging.mjs';

const VENDORS = ['krabka', 'kafka', 'redpanda'];
const config = {
  topics: 1,
  partitionsPerTopic: 1,
  messageSize: 1024,
  testDurationMinutes: 1,
  producerRate: 5000,
  consumerBacklogSizeGB: 0,
};
const series = (value) => Array(6).fill(value);

async function fakeRun(overrides = {}, { directIo } = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'omb-publish-'));
  const run = path.join(root, 'artifact', 'run-1');
  await fs.mkdir(run, { recursive: true });
  const provenance = {
    suite: 'openmessaging',
    run_id: 'run-1',
    mode: 'full',
    status: 'complete',
    repetitions: 1,
    replication_factors: [1],
    cases: [{ id: 'simple-workload', config }],
    ...overrides,
  };
  await fs.writeFile(path.join(run, 'provenance.json'), JSON.stringify(provenance));
  await fs.writeFile(path.join(run, 'summary.md'), '# OpenMessaging broker comparison\n');
  for (const vendor of VENDORS) {
    const directory = path.join(run, `rf1-${vendor}-1-simple-workload`);
    await fs.mkdir(directory);
    await fs.writeFile(
      path.join(directory, 'trial.json'),
      JSON.stringify({
        vendor,
        rf: 1,
        repetition: 1,
        case: { id: 'simple-workload', config },
        omb: {
          driver: `${vendor}-rf1`,
          topics: 1,
          partitions: 1,
          messageSize: 1024,
          publishRate: series(5000),
          consumeRate: series(5000),
          publishErrorRate: series(0),
          backlog: series(0),
          aggregatedPublishLatency50pct: 1,
          aggregatedPublishLatency99pct: 2,
          aggregatedPublishLatency999pct: 3,
          aggregatedEndToEndLatency50pct: 2,
          aggregatedEndToEndLatency99pct: 4,
          aggregatedEndToEndLatency999pct: 6,
        },
        metrics: { cpu_seconds: 10, rss_peak_bytes: 1048576, working_set_peak_bytes: 2097152 },
        time_series: { samples: [] },
        ...(vendor === 'krabka' && directIo ? { direct_io: directIo } : {}),
      }),
    );
  }
  await fs.mkdir(path.join(root, 'benchmarks'));
  return { root, run };
}

test('a complete full run is published with a compact trial list and a latest pointer', async () => {
  const { root, run } = await fakeRun();
  assert.deepEqual(await publish(run, root), { run_id: 'run-1', storage: 'page-cache', trials: 3 });
  const trials = JSON.parse(
    await fs.readFile(
      path.join(root, 'benchmarks', 'openmessaging', 'run-1', 'trials.json'),
      'utf8',
    ),
  );
  assert.equal(trials.length, 3);
  assert.equal(trials[0].end_to_end_latency_ms.p99, 4);
  assert.equal('time_series' in trials[0], false);
  const latest = await fs.readFile(
    path.join(root, 'benchmarks', 'latest-openmessaging.md'),
    'utf8',
  );
  assert.match(
    latest,
    /^\[Dated report and per-trial results\]\(openmessaging\/run-1\/summary\.md\)/,
  );
  await assert.rejects(publish(run, root), /already published/);
});

test('smoke and incomplete runs are refused', async () => {
  for (const overrides of [{ mode: 'smoke' }, { status: 'failed' }]) {
    const { root, run } = await fakeRun(overrides);
    await assert.rejects(publish(run, root));
    await assert.rejects(fs.access(path.join(root, 'benchmarks', 'latest-openmessaging.md')));
  }
  const { root, run } = await fakeRun();
  await fs.rm(path.join(run, 'rf1-redpanda-1-simple-workload'), { recursive: true });
  await assert.rejects(publish(run, root));
});

test('a run that omits a trial with a reason is published without it', async () => {
  const omitted = [
    { vendor: 'redpanda', rf: 1, workload: 'simple-workload', reason: 'disk reserve reached' },
  ];
  const { root, run } = await fakeRun({ omitted });
  await fs.rm(path.join(run, 'rf1-redpanda-1-simple-workload'), { recursive: true });
  assert.deepEqual(await publish(run, root), { run_id: 'run-1', storage: 'page-cache', trials: 2 });
  const provenance = JSON.parse(
    await fs.readFile(
      path.join(root, 'benchmarks', 'openmessaging', 'run-1', 'provenance.json'),
      'utf8',
    ),
  );
  assert.deepEqual(provenance.omitted, omitted);
  for (const bad of [
    [{ ...omitted[0], reason: '' }],
    [omitted[0], { ...omitted[0], vendor: 'kafka' }],
  ]) {
    const attempt = await fakeRun({ omitted: bad });
    await assert.rejects(publish(attempt.run, attempt.root));
  }
});

test('runs with broker settings are experiments and are refused', async () => {
  for (const contract of [
    { krabka_env: ['KRABKA_LOG_TAIL_CACHE_SIZE=32MiB'] },
    { krabka_env: ['KRABKA_LOG_TAIL_CACHE_SIZE=32MiB'], krabka_preallocate: true },
  ]) {
    const { root, run } = await fakeRun({ contract }, { directIo: { verified: true } });
    await assert.rejects(publish(run, root), /--krabka-env/);
    await assert.rejects(fs.access(path.join(root, 'benchmarks', 'latest-openmessaging.md')));
    await assert.rejects(
      fs.access(path.join(root, 'benchmarks', 'latest-openmessaging-direct.md')),
    );
  }
  const { root, run } = await fakeRun({ contract: { krabka_env: [], krabka_preallocate: false } });
  assert.deepEqual(await publish(run, root), {
    run_id: 'run-1',
    storage: 'page-cache',
    trials: 3,
  });
});

test('a direct-storage run is published to its own directory and pointer', async () => {
  const { root, run } = await fakeRun(
    { contract: { krabka_preallocate: true } },
    { directIo: { verified: true } },
  );
  // The page-cache run already on the site stays as it is.
  const pageCache = path.join(root, 'benchmarks', 'latest-openmessaging.md');
  await fs.writeFile(pageCache, 'page-cache pointer\n');
  assert.deepEqual(await publish(run, root), { run_id: 'run-1', storage: 'direct', trials: 3 });
  assert.equal(await fs.readFile(pageCache, 'utf8'), 'page-cache pointer\n');
  await assert.rejects(fs.access(path.join(root, 'benchmarks', 'openmessaging', 'run-1')));
  const published = path.join(root, 'benchmarks', 'openmessaging-direct', 'run-1');
  for (const file of ['provenance.json', 'summary.md', 'trials.json', 'intervals.json'])
    await fs.access(path.join(published, file));
  assert.match(
    await fs.readFile(path.join(root, 'benchmarks', 'latest-openmessaging-direct.md'), 'utf8'),
    /^\[Dated report and per-trial results\]\(openmessaging-direct\/run-1\/summary\.md\)/,
  );
  await assert.rejects(publish(run, root), /already published/);
});

test('a run id is published once across storage modes', async () => {
  const { root, run } = await fakeRun(
    { contract: { krabka_preallocate: true } },
    { directIo: { verified: true } },
  );
  await fs.mkdir(path.join(root, 'benchmarks', 'openmessaging', 'run-1'), { recursive: true });
  await assert.rejects(publish(run, root), /already published/);
});

test('a direct-storage run without verified O_DIRECT on every krabka trial is refused', async () => {
  for (const directIo of [undefined, { verified: false, error: 'not seen' }]) {
    const { root, run } = await fakeRun({ contract: { krabka_preallocate: true } }, { directIo });
    await assert.rejects(
      publish(run, root),
      /simple-workload RF1 krabka round 1: O_DIRECT not verified/,
    );
    await assert.rejects(
      fs.access(path.join(root, 'benchmarks', 'latest-openmessaging-direct.md')),
    );
  }
});

test('publication preserves every raw OMB field and interval in compressed results with verified checksums', async () => {
  const { root, run } = await fakeRun();
  const source = path.join(run, 'rf1-krabka-1-simple-workload', 'trial.json');
  const trial = JSON.parse(await fs.readFile(source, 'utf8'));
  // Deliberately uneven samples and upstream fields the compact view does not use.
  trial.omb.publishRate = [1000, 3000, 8000, 5000, 7000, 6000];
  trial.omb.publishLatency99pct = [1, 2, 30, 4, 50, 6];
  trial.omb.endToEndLatency99pct = [2, 4, 60, 8, 100, 12];
  trial.omb.aggregatedPublishLatencyBuckets = { 1.25: 3, 50.5: 17 };
  trial.omb.futureUpstreamField = { samples: [0, null, 1.23456789] };
  await fs.writeFile(source, JSON.stringify(trial));
  await publish(run, root);
  const published = path.join(root, 'benchmarks/openmessaging/run-1');
  const index = JSON.parse(await fs.readFile(path.join(published, 'intervals.json'), 'utf8'));
  assert.equal(index.schema_version, 1);
  assert.equal(index.run_id, 'run-1');
  assert.equal(index.trials.length, 3);
  for (const entry of index.trials) {
    const original = JSON.parse(
      await fs.readFile(
        path.join(
          run,
          `rf${entry.rf}-${entry.vendor}-${entry.repetition}-${entry.workload}/trial.json`,
        ),
        'utf8',
      ),
    );
    const compressed = await fs.readFile(path.join(published, entry.file));
    assert.equal(createHash('sha256').update(compressed).digest('hex'), entry.sha256);
    assert.deepEqual(JSON.parse(gunzipSync(compressed)), original.omb);
    assert.equal(entry.samples, original.omb.publishRate.length);
  }
  const compact = JSON.parse(await fs.readFile(path.join(published, 'trials.json'), 'utf8'));
  assert.equal(compact[0].publish_rate, trial.omb.publishRate.reduce((a, b) => a + b) / 6);
  assert.equal('omb' in compact[0], false);
  await fs.rm(root, { recursive: true, force: true });
});

test('a run with a shortened measurement is refused', async () => {
  const { root, run } = await fakeRun({ contract: { test_duration_minutes: 5 } });
  await assert.rejects(publish(run, root), /departs from the upstream workloads/);
});

test('a run in which the host shaped a trial is refused', async () => {
  const { root, run } = await fakeRun();
  const file = path.join(run, 'rf1-kafka-1-simple-workload', 'trial.json');
  const trial = JSON.parse(await fs.readFile(file, 'utf8'));
  trial.metrics.measurement_window = {
    host: {
      balloon_inflated_pages: 1048576,
      available_min_bytes: 2 ** 30,
      pgsteal_kswapd: 0,
      pgsteal_direct: 0,
      allocstall: 0,
    },
  };
  await fs.writeFile(file, JSON.stringify(trial));
  await assert.rejects(
    publish(run, root),
    /host interfered with simple-workload RF1 kafka round 1 \(balloon inflation or direct reclaim during measurement\); repeat the run; not published/,
  );
  await assert.rejects(fs.access(path.join(root, 'benchmarks', 'latest-openmessaging.md')));
  // A balloon-free window with no direct reclaim publishes.
  trial.metrics.measurement_window.host = {
    balloon_inflated_pages: null,
    available_min_bytes: 2 ** 35,
    pgsteal_kswapd: 4096,
    pgsteal_direct: 0,
    allocstall: 0,
  };
  await fs.writeFile(file, JSON.stringify(trial));
  await publish(run, root);
  await fs.access(path.join(root, 'benchmarks', 'latest-openmessaging.md'));
});

test('a run with --krabka-io-uring is refused', async () => {
  // As the workflow's direct-io-uring storage records it: preallocate plus the
  // io_uring seccomp profile.
  const { root, run } = await fakeRun(
    {
      contract: { krabka_preallocate: true, krabka_seccomp: 'docker-29.8.1-default+io_uring' },
    },
    { directIo: { verified: true } },
  );
  await assert.rejects(publish(run, root), /container security profile/);
  await assert.rejects(fs.access(path.join(root, 'benchmarks', 'latest-openmessaging-direct.md')));
  await assert.rejects(fs.access(path.join(root, 'benchmarks', 'openmessaging-direct')));
});
