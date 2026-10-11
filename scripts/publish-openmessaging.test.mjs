import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
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
