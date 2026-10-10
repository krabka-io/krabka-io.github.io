import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { OMB_WORKLOADS } from './benchmark-openmessaging.mjs';
import { lanes, merge, plan, split } from './benchmark-openmessaging-shards.mjs';

const VENDORS = ['krabka', 'kafka', 'redpanda'];
const config = { topics: 1, partitionsPerTopic: 1, messageSize: 1024, testDurationMinutes: 1, producerRate: 5000, consumerBacklogSizeGB: 0 };
const images = {
  krabka: { requested: 'ghcr.io/krabka-io/krabka-broker@sha256:aa', reference: 'ghcr.io/krabka-io/krabka-broker@sha256:aa' },
  kafka: { requested: 'apache/kafka:4.3.1', reference: 'apache/kafka@sha256:bb' },
  redpanda: { requested: 'docker.redpanda.com/redpandadata/redpanda:v26.2.2', reference: 'docker.redpanda.com/redpandadata/redpanda@sha256:cc' },
};
const series = value => Array(6).fill(value);

async function shard(root, { workload, rf, repetitions = 1, status = 'complete', overrides = {}, host = 'vm-a' }) {
  const runId = `shard-${workload}-rf${rf}-${host}`;
  const directory = path.join(root, runId);
  await fs.mkdir(directory, { recursive: true });
  const provenance = { schema_version: 1, suite: 'openmessaging', run_id: runId, mode: 'full', status, repetitions,
    started_at: `2026-10-09T0${rf}:00:00Z`, completed_at: `2026-10-09T0${rf}:30:00Z`,
    images, cases: [{ id: workload, upstream_file: `workloads/${workload}.yaml`, config }],
    workload_source: { commit: '5b1fa709' }, contract: { broker_cpus: 4 }, client_jars: { a: '1' }, client_java: '17',
    openmessaging: { commit: '5b1fa709', patches: { p: '3' }, build_image_id: 'sha256:ee',
      jars: { '/m2/dep.jar': '2', '/src/driver-kafka/target/driver-kafka.jar': host } }, replication_factors: [rf], host: { name: host }, runner: { label: host }, cpu_sets: {},
    ...overrides };
  await fs.writeFile(path.join(directory, 'provenance.json'), JSON.stringify(provenance));
  for (const vendor of VENDORS) for (let repetition = 1; repetition <= repetitions; repetition++) {
    const trialDirectory = path.join(directory, `rf${rf}-${vendor}-${repetition}-${workload}`);
    await fs.mkdir(trialDirectory);
    await fs.writeFile(path.join(trialDirectory, 'workload.stdout'), 'log\n');
    await fs.writeFile(path.join(trialDirectory, 'trial.json'), JSON.stringify({
      vendor, rf, repetition, case: { id: workload, config },
      omb: { driver: `${vendor}-rf${rf}`, topics: 1, partitions: 1, messageSize: 1024,
        publishRate: series(5000), consumeRate: series(5000), publishErrorRate: series(0), backlog: series(0),
        aggregatedPublishLatency99pct: 2, aggregatedEndToEndLatency99pct: 4 },
      metrics: { cpu_seconds: 10, rss_peak_bytes: 1048576, working_set_peak_bytes: 2097152 },
    }));
  }
  return directory;
}

const scratch = () => fs.mkdtemp(path.join(os.tmpdir(), 'omb-shards-'));

test('the plan has one shard per workload and replication factor, in catalog order', () => {
  const all = plan('all', '1,3');
  assert.equal(all.length, OMB_WORKLOADS.length * 2);
  assert.equal(new Set(all.map(s => s.name)).size, all.length);
  assert.deepEqual(all.slice(0, 2), [
    { name: 'rf1-simple-workload', workload: 'simple-workload', rf: 1 },
    { name: 'rf3-simple-workload', workload: 'simple-workload', rf: 3 },
  ]);
  // Each backlog workload is a shard of its own.
  for (const id of ['backlog-1-topic-1-partition-1kb', 'backlog-1-topic-16-partitions-1kb']) {
    assert.deepEqual(all.filter(s => s.workload === id).map(s => s.name), [`rf1-${id}`, `rf3-${id}`]);
  }
  assert.deepEqual(plan('simple-workload,1-topic-1-partition-1kb', '3').map(s => s.name),
    ['rf3-simple-workload', 'rf3-1-topic-1-partition-1kb']);
  assert.throws(() => plan('all', '2'), /replication factors/);
  assert.throws(() => plan('not-a-workload', '1'), /unknown OMB workload/);
});

test('lanes split the shards evenly by weight and run every shard exactly once', () => {
  const all = plan('all', '1,3');
  for (const count of [1, 2, 4]) {
    const result = lanes(all, count);
    assert.deepEqual(result.map(l => l.name), Array.from({ length: count }, (_, i) => `lane-${i + 1}`));
    const names = result.flatMap(l => l.shards.split(' '));
    assert.deepEqual([...names].sort(), all.map(s => s.name).sort());
    for (const lane of result) {
      const shards = lane.shards.split(' ');
      assert.deepEqual(lane.workloads.split(' '), shards.map(n => all.find(s => s.name === n).workload));
      assert.deepEqual(lane.rfs.split(' ').map(Number), shards.map(n => all.find(s => s.name === n).rf));
    }
    // 26 shards, four of them backlog (weight 3): 34 units over the lanes.
    const weights = result.map(l => l.workloads.split(' ').reduce((sum, w) => sum + (w.startsWith('backlog-') ? 3 : 1), 0));
    assert.ok(Math.max(...weights) - Math.min(...weights) <= 1, `uneven lanes ${weights}`);
  }
  // The four backlog shards land on four different lanes.
  assert.deepEqual(lanes(all, 4).map(l => l.workloads.split(' ').filter(w => w.startsWith('backlog-')).length), [1, 1, 1, 1]);
  assert.deepEqual(lanes(plan('simple-workload', '1'), 4), [{ name: 'lane-1', shards: 'rf1-simple-workload', workloads: 'simple-workload', rfs: '1' }]);
  assert.throws(() => lanes(all, 0), /lane count/);
});

test('complete shards merge into one run in the single-runner layout', async () => {
  const root = await scratch();
  const shards = [];
  for (const workload of ['simple-workload', '1-topic-1-partition-1kb']) for (const rf of [1, 3]) {
    shards.push(await shard(root, { workload, rf, repetitions: 2, host: `vm-${workload}-${rf}` }));
  }
  const out = path.join(root, 'merged', 'gh-1-1');
  await fs.mkdir(path.dirname(out));
  assert.deepEqual(await merge(shards.reverse(), out), { run_id: 'gh-1-1', shards: 4, trials: 24 });
  const provenance = JSON.parse(await fs.readFile(path.join(out, 'provenance.json'), 'utf8'));
  assert.deepEqual([provenance.run_id, provenance.status, provenance.completed_trials], ['gh-1-1', 'complete', 24]);
  assert.deepEqual(provenance.cases.map(c => c.id), ['simple-workload', '1-topic-1-partition-1kb']);
  assert.deepEqual(provenance.replication_factors, [1, 3]);
  assert.deepEqual([provenance.started_at, provenance.completed_at], ['2026-10-09T01:00:00Z', '2026-10-09T03:30:00Z']);
  assert.equal(provenance.shards.length, 4);
  assert.equal(new Set(provenance.shards.map(s => s.host.name)).size, 4);
  await fs.access(path.join(out, 'rf3-redpanda-2-1-topic-1-partition-1kb', 'trial.json'));
  await fs.access(path.join(out, 'rf3-redpanda-2-1-topic-1-partition-1kb', 'workload.stdout'));
  const summary = await fs.readFile(path.join(out, 'summary.md'), 'utf8');
  assert.match(summary, /Run: gh-1-1\. Mode: full\. Status: complete\./);
  assert.equal(summary.split('\n').filter(line => line.startsWith('| simple-workload')).length, 12);
});

test('merging refuses shards that do not form one complete comparison', async () => {
  const cases = [
    ['an incomplete shard', [{ workload: 'simple-workload', rf: 1, status: 'failed' }], /is failed/],
    ['a duplicated shard', [{ workload: 'simple-workload', rf: 1 }, { workload: 'simple-workload', rf: 1, host: 'vm-b' }], /more than one shard/],
    ['a missing workload/RF pair', [{ workload: 'simple-workload', rf: 1 }, { workload: '1-topic-1-partition-1kb', rf: 3 }], /no shard ran/],
    ['a different image', [{ workload: 'simple-workload', rf: 1 },
      { workload: 'simple-workload', rf: 3, overrides: { images: { ...images, krabka: { ...images.krabka, reference: 'other@sha256:dd' } } } }], /different krabka image/],
    ['different rounds', [{ workload: 'simple-workload', rf: 1 }, { workload: 'simple-workload', rf: 3, repetitions: 2 }], /differs in repetitions/],
    ['a different dependency jar', [{ workload: 'simple-workload', rf: 1 }, { workload: 'simple-workload', rf: 3,
      overrides: { openmessaging: { commit: '5b1fa709', patches: { p: '3' }, build_image_id: 'sha256:ee', jars: { '/m2/dep.jar': '9' } } } }], /differs in the OMB build/],
    ['a different OMB patch', [{ workload: 'simple-workload', rf: 1 }, { workload: 'simple-workload', rf: 3,
      overrides: { openmessaging: { commit: '5b1fa709', patches: { p: '4' }, build_image_id: 'sha256:ee', jars: { '/m2/dep.jar': '2' } } } }], /differs in the OMB build/],
    ['a smoke shard among full ones', [{ workload: 'simple-workload', rf: 1 }, { workload: 'simple-workload', rf: 3, overrides: { mode: 'smoke' } }], /differs in mode/],
  ];
  for (const [label, specs, error] of cases) {
    const root = await scratch();
    const shards = [];
    for (const spec of specs) shards.push(await shard(root, spec));
    const out = path.join(root, 'merged');
    await assert.rejects(merge(shards, out), error, label);
  }
});

// A run interrupted part-way through RF3: every RF1 trial completed, RF3 has
// one completed trial and one failure.
async function interrupted(root, { omit = null, fail = null } = {}) {
  const directory = await shard(root, { workload: 'simple-workload', rf: 1, status: 'running',
    overrides: { run_id: 'killed-run', replication_factors: [1, 3] } });
  const rf3 = path.join(directory, 'rf3-krabka-1-simple-workload');
  await fs.mkdir(rf3);
  await fs.writeFile(path.join(rf3, 'failure.json'), '{}');
  if (omit) await fs.rm(path.join(directory, omit), { recursive: true });
  if (fail) await fs.writeFile(path.join(directory, fail, 'failure.json'), '{}');
  return directory;
}

test('a completed replication factor of an interrupted run splits into a mergeable shard', async () => {
  const root = await scratch();
  const source = await interrupted(root);
  const out = path.join(root, 'rf1-shard');
  const shard1 = await split(source, 1, out, 'runner killed during RF3');
  assert.equal(shard1.trials.length, 3);
  assert.deepEqual([shard1.provenance.run_id, shard1.provenance.status, shard1.provenance.replication_factors],
    ['killed-run-rf1', 'complete', [1]]);
  assert.deepEqual(shard1.provenance.derived_from,
    { run_id: 'killed-run', status: 'running', replication_factor: 1, reason: 'runner killed during RF3' });
  await assert.rejects(fs.access(path.join(out, 'rf3-krabka-1-simple-workload')));
  const rf3 = await shard(root, { workload: 'simple-workload', rf: 3, host: 'vm-rerun' });
  const merged = path.join(root, 'merged');
  assert.deepEqual(await merge([out, rf3], merged), { run_id: 'merged', shards: 2, trials: 6 });
  const provenance = JSON.parse(await fs.readFile(path.join(merged, 'provenance.json'), 'utf8'));
  assert.deepEqual(provenance.shards.map(s => s.derived_from?.run_id ?? null), ['killed-run', null]);
});

test('splitting refuses a replication factor with a missing or failed trial', async () => {
  const cases = [
    ['the incomplete RF', {}, 3, /failed in run killed-run/],
    ['a missing trial', { omit: 'rf1-kafka-1-simple-workload' }, 1, /has no completed rf1-kafka-1-simple-workload/],
    ['a failed trial', { fail: 'rf1-redpanda-1-simple-workload' }, 1, /rf1-redpanda-1-simple-workload failed/],
  ];
  for (const [label, damage, rf, error] of cases) {
    const root = await scratch();
    const source = await interrupted(root, damage);
    await assert.rejects(split(source, rf, path.join(root, 'out'), 'test'), error, label);
  }
  const root = await scratch();
  const complete = await shard(root, { workload: 'simple-workload', rf: 1 });
  await assert.rejects(split(complete, 1, path.join(root, 'out'), 'test'), /is complete/);
});
