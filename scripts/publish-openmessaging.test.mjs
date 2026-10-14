import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { publish } from './publish-openmessaging.mjs';

const VENDORS = ['krabka', 'kafka', 'redpanda'];
const config = { topics: 1, partitionsPerTopic: 1, messageSize: 1024, testDurationMinutes: 1, producerRate: 5000, consumerBacklogSizeGB: 0 };
const series = value => Array(6).fill(value);

async function fakeRun(overrides = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'omb-publish-'));
  const run = path.join(root, 'artifact', 'run-1');
  await fs.mkdir(run, { recursive: true });
  const provenance = { suite: 'openmessaging', run_id: 'run-1', mode: 'full', status: 'complete', repetitions: 1,
    replication_factors: [1], cases: [{ id: 'simple-workload', config }], ...overrides };
  await fs.writeFile(path.join(run, 'provenance.json'), JSON.stringify(provenance));
  await fs.writeFile(path.join(run, 'summary.md'), '# OpenMessaging broker comparison\n');
  for (const vendor of VENDORS) {
    const directory = path.join(run, `rf1-${vendor}-1-simple-workload`);
    await fs.mkdir(directory);
    await fs.writeFile(path.join(directory, 'trial.json'), JSON.stringify({
      vendor, rf: 1, repetition: 1, case: { id: 'simple-workload', config },
      omb: { driver: `${vendor}-rf1`, topics: 1, partitions: 1, messageSize: 1024,
        publishRate: series(5000), consumeRate: series(5000), publishErrorRate: series(0), backlog: series(0),
        aggregatedPublishLatency50pct: 1, aggregatedPublishLatency99pct: 2, aggregatedPublishLatency999pct: 3,
        aggregatedEndToEndLatency50pct: 2, aggregatedEndToEndLatency99pct: 4, aggregatedEndToEndLatency999pct: 6 },
      metrics: { cpu_seconds: 10, rss_peak_bytes: 1048576, working_set_peak_bytes: 2097152 },
      time_series: { samples: [] },
    }));
  }
  await fs.mkdir(path.join(root, 'benchmarks'));
  return { root, run };
}

test('a complete full run is published with a compact trial list and a latest pointer', async () => {
  const { root, run } = await fakeRun();
  assert.deepEqual(await publish(run, root), { run_id: 'run-1', trials: 3 });
  const trials = JSON.parse(await fs.readFile(path.join(root, 'benchmarks', 'openmessaging', 'run-1', 'trials.json'), 'utf8'));
  assert.equal(trials.length, 3);
  assert.equal(trials[0].end_to_end_latency_ms.p99, 4);
  assert.equal('time_series' in trials[0], false);
  const latest = await fs.readFile(path.join(root, 'benchmarks', 'latest-openmessaging.md'), 'utf8');
  assert.match(latest, /^\[Dated report and per-trial results\]\(openmessaging\/run-1\/summary\.md\)/);
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


test('runs with broker settings or a preallocate override are experiments and are refused', async () => {
  for (const [contract, message] of [
    [{ krabka_env: ['KRABKA_LOG_TAIL_CACHE_SIZE=32MiB'] }, /--krabka-env/],
    [{ krabka_preallocate: true }, /--krabka-preallocate/],
  ]) {
    const { root, run } = await fakeRun({ contract });
    await assert.rejects(publish(run, root), message);
    await assert.rejects(fs.access(path.join(root, 'benchmarks', 'latest-openmessaging.md')));
  }
  const { root, run } = await fakeRun({ contract: { krabka_env: [], krabka_preallocate: false } });
  assert.deepEqual(await publish(run, root), { run_id: 'run-1', trials: 3 });
});

test('publication preserves every raw OMB field and interval in compressed results with verified checksums', async () => {
  const { root, run } = await fakeRun();
  const source = path.join(run, 'rf1-krabka-1-simple-workload', 'trial.json');
  const trial = JSON.parse(await fs.readFile(source, 'utf8'));
  // Deliberately uneven samples and upstream fields the compact view does not use.
  trial.omb.publishRate = [1000, 3000, 8000, 5000, 7000, 6000];
  trial.omb.publishLatency99pct = [1, 2, 30, 4, 50, 6];
  trial.omb.endToEndLatency99pct = [2, 4, 60, 8, 100, 12];
  trial.omb.aggregatedPublishLatencyBuckets = { '1.25': 3, '50.5': 17 };
  trial.omb.futureUpstreamField = { samples: [0, null, 1.23456789] };
  await fs.writeFile(source, JSON.stringify(trial));
  await publish(run, root);
  const published = path.join(root, 'benchmarks/openmessaging/run-1');
  const index = JSON.parse(await fs.readFile(path.join(published, 'intervals.json'), 'utf8'));
  assert.equal(index.schema_version, 1);
  assert.equal(index.run_id, 'run-1');
  assert.equal(index.trials.length, 3);
  for (const entry of index.trials) {
    const original = JSON.parse(await fs.readFile(path.join(run, `rf${entry.rf}-${entry.vendor}-${entry.repetition}-${entry.workload}/trial.json`), 'utf8'));
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
