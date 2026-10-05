// Checks the Cluster Lab's observation modules without a browser: the live
// invariant checker (invariants.js), the chart sampler (charts.js) and the
// record tracer (trace.js).
//
// The checker and the sampler run on synthetic snapshots in the shapes of the
// admin's cluster observer and the producer and consumer snapshots. The tracer
// runs on the real frames of scripts/fixtures/lab-analyzer.json: its Produce
// exchange, then copies of a Fetch and an OffsetCommit from the same run with
// their topic, partition and offsets patched to follow that record.
//
// Usage: npm run check-lab-observe

import assert from 'node:assert/strict';
import fs from 'node:fs';
import { setSchemas, decodeFrame } from '../public/playground/lab/kafka-decode.js';
import { Capture } from '../public/playground/lab/capture.js';
import { InvariantChecker, STALL_MS } from '../public/playground/lab/invariants.js';
import { Sampler, rtts, faultMarkers } from '../public/playground/lab/charts.js';
import { traceRecord } from '../public/playground/lab/trace.js';

let passed = 0;
const check = async (name, fn) => {
  try {
    await fn();
    passed++;
  } catch (err) {
    console.error(`  FAIL ${name}: ${err.stack || err.message}`);
    process.exitCode = 1;
  }
};

setSchemas(JSON.parse(fs.readFileSync('public/playground/lab/kafka-schemas.json', 'utf8')));
const fixture = JSON.parse(fs.readFileSync('scripts/fixtures/lab-analyzer.json', 'utf8'));

// ---- synthetic snapshots ----

const part = (partition, leader, epoch, isr, hwm, replicas = [1, 2, 3]) => ({ partition, leader, leader_epoch: epoch, replicas, isr, offline: [], hwm, log_start: 0 });
const cluster = (at, partitions, quorum = { leader: 1, epoch: 3, high_watermark: 10, voters: [] }) => ({
  at, cluster_id: 'c', brokers: [{ id: 1 }, { id: 2 }, { id: 3 }], controller: quorum?.leader ?? null, quorum,
  topics: [{ name: 'orders', internal: false, partitions }], groups: [], errors: [],
});
const snap = (now, { c = null, acked = null, acks = -1, consumer = {} } = {}) => ({
  now,
  nodes: [
    { id: 7, kind: 'admin', alive: true, state: c ? { topics: [], cluster: c } : { topics: [] } },
    { id: 4, kind: 'producer', alive: true, state: { acks, acked: 0, ...(acked ? { acked_upto: acked } : {}) } },
    { id: 5, kind: 'consumer', alive: true, state: { group: 'billing', lag: 0, processed: 0, ...consumer } },
  ],
});

await check('an old module without the new fields checks nothing and does not throw', () => {
  const k = new InvariantChecker();
  assert.deepEqual(k.observe(snap(1000)), []);
  assert.deepEqual(k.observe({ now: 5, nodes: [{ id: 1, kind: 'producer', state: null }] }), []);
  assert.deepEqual(k.observe(null), []);
  assert.equal(k.ok, true);
});

await check('acked records: a high watermark at or below an acked offset for STALL_MS is a loss', () => {
  const k = new InvariantChecker();
  k.observe(snap(1000, { acked: { 'orders-0': 10 }, c: cluster(900, [part(0, 1, 1, [1, 2, 3], 5)]) }));
  // The poll completed before the ack was seen: not compared.
  assert.equal(k.ok, true);
  k.observe(snap(1600, { acked: { 'orders-0': 10 }, c: cluster(1500, [part(0, 1, 1, [1, 2, 3], 11)]) }));
  // A new leader's high watermark stands low while its ISR catches up or
  // shrinks: polls within STALL_MS of the first low one are not a loss.
  k.observe(snap(2600, { acked: { 'orders-0': 10 }, c: cluster(2500, [part(0, 2, 2, [2], 6)]) }));
  k.observe(snap(30_600, { acked: { 'orders-0': 10 }, c: cluster(30_500, [part(0, 2, 2, [2], 6)]) }));
  assert.equal(k.ok, true);
  const end = 2500 + STALL_MS;
  const v = k.observe(snap(end + 100, { acked: { 'orders-0': 10 }, c: cluster(end, [part(0, 2, 2, [2], 6)]) }));
  assert.equal(v.length, 1);
  assert.equal(v[0].check, 'acked_lost');
  assert.equal(v[0].at, end);
  assert.equal(v[0].data.acked, 10);
  // Reported once, not on every later poll.
  assert.equal(k.observe(snap(end + 1100, { acked: { 'orders-0': 10 }, c: cluster(end + 1000, [part(0, 2, 2, [2], 6)]) })).length, 0);
});

await check('acked records: a one-poll dip that recovers, and acks=1, are not losses', () => {
  const k = new InvariantChecker();
  k.observe(snap(1000, { acked: { 'orders-0': 10 }, c: cluster(1000, [part(0, 1, 1, [1, 2, 3], 11)]) }));
  k.observe(snap(2000, { acked: { 'orders-0': 10 }, c: cluster(2000, [part(0, 2, 2, [2, 3], 9)]) }));
  k.observe(snap(3000, { acked: { 'orders-0': 10 }, c: cluster(3000, [part(0, 2, 2, [2, 3], 11)]) }));
  k.observe(snap(4000, { acked: { 'orders-0': 10 }, c: cluster(4000, [part(0, 2, 2, [2, 3], 9)]) }));
  assert.equal(k.ok, true);
  const one = new InvariantChecker();
  for (const t of [1000, 2000, 3000]) one.observe(snap(t, { acks: 1, acked: { 'orders-0': 10 }, c: cluster(t, [part(0, 2, 2, [2], 3)]) }));
  assert.equal(one.ok, true);
});

await check('acked records: a partition gone from two clean polls lost them; a failed poll says nothing', () => {
  const k = new InvariantChecker();
  k.observe(snap(1000, { acked: { 'orders-0': 10 }, c: cluster(1000, [part(0, 1, 1, [1], 11)]) }));
  const failed = { ...cluster(2000, []), errors: ['Metadata: timed out'] };
  k.observe(snap(2000, { acked: { 'orders-0': 10 }, c: failed }));
  k.observe(snap(3000, { acked: { 'orders-0': 10 }, c: { ...failed, at: 3000 } }));
  assert.equal(k.ok, true);
  k.observe(snap(4000, { acked: { 'orders-0': 10 }, c: cluster(4000, []) }));
  const v = k.observe(snap(5000, { acked: { 'orders-0': 10 }, c: cluster(5000, []) }));
  assert.deepEqual(v.map((x) => x.check), ['acked_lost']);
  assert.match(v[0].text, /gone from the cluster/);
});

await check('one leader per partition and leader epoch', () => {
  const k = new InvariantChecker();
  k.observe(snap(1000, { c: cluster(1000, [part(0, 1, 4, [1, 2, 3], 1)]) }));
  k.observe(snap(2000, { c: cluster(2000, [part(0, 2, 5, [2, 3], 1)]) }));
  assert.equal(k.ok, true);
  const v = k.observe(snap(3000, { c: cluster(3000, [part(0, 3, 5, [3], 1)]) }));
  assert.deepEqual(v.map((x) => x.check), ['split_leader']);
  assert.deepEqual(v[0].data.leaders, [2, 3]);
});

await check('one KRaft quorum leader per epoch', () => {
  const k = new InvariantChecker();
  k.observe(snap(1000, { c: cluster(1000, [], { leader: 1, epoch: 3, voters: [] }) }));
  k.observe(snap(2000, { c: cluster(2000, [], { leader: 2, epoch: 4, voters: [] }) }));
  assert.equal(k.ok, true);
  const v = k.observe(snap(3000, { c: cluster(3000, [], { leader: 3, epoch: 4, voters: [] }) }));
  assert.deepEqual(v.map((x) => x.check), ['split_quorum']);
});

await check('offsets go backwards only after a reset_offsets for the group', () => {
  const r = { tp: 'orders-0', from: 10, to: 4, at: 5000 };
  const k = new InvariantChecker();
  const v = k.observe(snap(5100, { consumer: { offset_regressions: [r] } }));
  assert.deepEqual(v.map((x) => x.check), ['offset_regression']);
  // The same entry in the next snapshot is not new.
  assert.equal(k.observe(snap(5200, { consumer: { offset_regressions: [r] } })).length, 0);
  const ok = new InvariantChecker();
  ok.observe(snap(4900), [{ index: 1, at: 4800, node: 7, kind: 'admin_done', detail: { cmd: 'reset_offsets', group: 'billing' } }]);
  assert.equal(ok.observe(snap(5100, { consumer: { offset_regressions: [r] } })).length, 0);
  const other = new InvariantChecker();
  other.observe(snap(4900), [{ index: 1, at: 4800, node: 7, kind: 'admin_done', detail: { cmd: 'reset_offsets', group: 'audit' } }]);
  assert.equal(other.observe(snap(5100, { consumer: { offset_regressions: [r] } })).length, 1);
});

await check('a read_committed consumer that sees aborted records', () => {
  const scenario = { nodes: [{ id: 5, kind: 'consumer', config: { isolation_level: 'read_committed' } }] };
  const k = new InvariantChecker();
  assert.equal(k.observe(snap(1000, { consumer: { aborted_seen: 0 } }), [], scenario).length, 0);
  const v = k.observe(snap(2000, { consumer: { aborted_seen: 2 } }), [], scenario);
  assert.deepEqual(v.map((x) => x.check), ['aborted_read']);
  assert.equal(k.observe(snap(3000, { consumer: { aborted_seen: 2 } }), [], scenario).length, 0);
  const loose = new InvariantChecker();
  assert.equal(loose.observe(snap(2000, { consumer: { aborted_seen: 2, isolation_level: 'read_uncommitted' } })).length, 0);
  k.reset();
  assert.equal(k.violations.length, 0);
});

// ---- the chart sampler ----

await check('the sampler takes one row per lab second with rates, lag, RTTs and ISR', () => {
  const s = new Sampler();
  const ex = (apiKey, at, rtt) => ({ apiKey, req: { at }, resp: { deliverAt: at + rtt }, rtt });
  const capture = { exchanges: [ex(0, 1100, 4), ex(0, 1200, 8), ex(1, 1300, 500), ex(0, 1400, 6), ex(1, 2500, 20)] };
  const at = (now, acked, processed, lag, c) => ({
    now,
    nodes: [
      { id: 4, kind: 'producer', state: { acked } },
      { id: 8, kind: 'producer', state: { acked: acked * 2 } },
      { id: 5, kind: 'consumer', state: { processed, lag } },
      { id: 7, kind: 'admin', state: c ? { cluster: c } : {} },
    ],
  });
  assert.ok(s.sample(at(1000, 10, 5, 3), capture));
  assert.equal(s.sample(at(1500, 12, 6, 3), capture), null);
  const row = s.sample(at(2000, 20, 15, 7, cluster(1900, [part(0, 1, 1, [1, 2, 3], 1), part(1, 2, 1, [2], 1)])), capture);
  assert.equal(row.produce, 30); // (20 + 40) - (10 + 20) over one second
  assert.equal(row.consume, 10);
  assert.equal(row.lag, 7);
  assert.equal(row.produce_p50, 6);
  assert.equal(row.produce_p99, 8);
  assert.equal(row.fetch_p50, 500);
  assert.equal(row.min_isr, 1);
  assert.equal(row.urp, 1);
  const third = s.sample(at(3200, 15, 15, 7), capture);
  assert.equal(third.produce, 0); // a restarted producer counts from zero: no negative rate
  assert.equal(third.fetch_p99, 20);
  assert.equal(third.min_isr, null);
  // One producer restarts while the other keeps going: the drop of one does
  // not cancel the progress of the other.
  const fourth = s.sample({ now: 4200, nodes: [{ id: 4, kind: 'producer', state: { acked: 0 } }, { id: 8, kind: 'producer', state: { acked: 80 } }] }, capture);
  assert.equal(fourth.produce, 50);
  assert.equal(s.rows.length, 4);
  assert.deepEqual(rtts(null, 0, 1), { produce_p50: null, produce_p99: null, fetch_p50: null, fetch_p99: null });
});

await check('fault markers name the fault and its nodes', () => {
  const m = faultMarkers([
    { at: 15000, kind: 'fault', detail: { kind: 'kill', node: 3 } },
    { at: 16000, kind: 'fault', detail: { kind: 'partition', a: 1, b: 4 } },
    { at: 17000, kind: 'partitions_assigned', detail: {} },
  ], (id) => `n${id}`);
  assert.deepEqual(m, [{ t: 15000, kind: 'fault', label: 'kill n3' }, { t: 16000, kind: 'fault', label: 'partition n1–n4' }]);
});

// ---- the tracer, on real frames ----

const b64 = (u8) => Buffer.from(u8).toString('base64');
const fromB64 = (s) => Uint8Array.from(Buffer.from(s, 'base64'));
const base = new Capture();
base.add(fixture.frames);
const exOf = (api) => base.exchanges.find((e) => e.apiKey === api && e.resp);
const produce = exOf(0);
const offsetCommit = exOf(8);
// The Fetch response that carries records: a replica fetch of offsets 19–21.
const fetches = base.exchanges.filter((e) => e.apiKey === 1 && e.resp);
const walk = (n, f) => {
  f(n);
  for (const c of n.children || []) walk(c, f);
};
const find = (root, label) => {
  let hit = null;
  walk(root, (n) => (hit ??= n.label === label ? n : null));
  return hit;
};
let fetch = null;
for (const ex of fetches) {
  const d = await decodeFrame(ex.resp.bytes, { size: ex.resp.size, request: false, answers: ex });
  if (find(d.root, 'Records')?.children?.some((c) => c.kind === 'batch')) fetch = { ex, d };
}
const raw = (f, patch = {}) => ({ at: f.at, deliver_at: f.deliverAt, src: f.src, dst: f.dst, conn: f.conn, kind: f.kind, size: f.size, label: f.label, bytes: b64(f.bytes), ...patch });

await check('the fixture has the frames the tracer check needs', () => {
  assert.ok(produce && offsetCommit && fetch, 'a Produce, an OffsetCommit and a Fetch response with records');
});

// Topic ids as the cluster observer reports them.
const topicIds = new Map([['7Z63YIhORBiEY1TEtB4T6w', 'orders']]);
const orders = fromB64('7Z63YIhORBiEY1TEtB4T6w');
const rec = { producer: produce.client.node, topic: 'orders', partition: 1, offset: 18, key: 'customer-6', seq: 36 };

await check('trace: the Produce exchange is found by its base offset and key; the rest is not yet', async () => {
  const c = new Capture();
  c.add(fixture.frames);
  const r = await traceRecord(c, rec, { topicIds, brokers: new Set([1, 2, 3]), replicas: [2, 3, 1] });
  const steps = Object.fromEntries(r.steps.map((s) => [`${s.step}${s.to ?? ''}`, s]));
  assert.equal(r.steps[0].step, 'produce');
  assert.equal(r.steps[0].found, true);
  assert.equal(r.steps[0].ex, c.exchanges.find((e) => e.id === produce.id));
  assert.match(r.steps[0].detail, /base offset 18, record 0, key "customer-6"/);
  assert.doesNotMatch(r.steps[0].detail, /differs/);
  assert.equal(r.steps.filter((s) => s.step === 'replicate').length, 2, 'one row per follower');
  for (const s of r.steps.slice(1)) {
    assert.ok(!s.found, s.step);
    assert.match(s.why, /not happened yet/);
  }
  assert.ok(steps.hwm && steps.deliver && steps.commit);
});

await check('trace: unacked records and offsets outside the capture say why', async () => {
  const c = new Capture();
  c.add(fixture.frames);
  const r = await traceRecord(c, { ...rec, offset: null }, { topicIds });
  assert.match(r.steps[0].why, /not acknowledged yet/);
  const later = await traceRecord(c, { ...rec, offset: 99 }, { topicIds });
  assert.equal(later.steps.length, 1);
  assert.match(later.steps[0].why, /No Produce request for offset 99/);
  c.evicted = 3;
  c.frames[0].at = 1;
  const gone = await traceRecord(c, { ...rec, offset: 99 }, { topicIds });
  assert.match(gone.steps[0].why, /Outside the capture window/);
});

await check('trace: follows the record to a follower, the high watermark, a consumer and a commit', async () => {
  // Patch a copy of the Fetch response to carry orders-1 offsets 18–20 with a
  // high watermark of 19, and the OffsetCommit to commit orders-1 at 19.
  const fetchResp = Uint8Array.from(fetch.ex.resp.bytes);
  const dv = new DataView(fetchResp.buffer);
  const resp = fetch.d.root;
  fetchResp.set(orders, find(resp, 'TopicId').start);
  dv.setInt32(find(resp, 'PartitionIndex').start, 1);
  dv.setBigInt64(find(resp, 'HighWatermark').start, 19n);
  dv.setBigInt64(find(resp, 'Records').children.find((n) => n.kind === 'batch').start, 18n);
  const commitReq = Uint8Array.from(offsetCommit.req.bytes);
  const cd = await decodeFrame(commitReq, { request: true });
  const cv = new DataView(commitReq.buffer);
  cv.setInt32(find(cd.root, 'PartitionIndex').start, 1);
  cv.setBigInt64(find(cd.root, 'CommittedOffset').start, 19n);
  const t0 = produce.resp.deliverAt;
  const follower = fetch.ex.client.node;
  const consumer = offsetCommit.client.node;
  const frames = [
    // The follower's fetch long-polls: sent before the Produce, answered after it.
    raw(fetch.ex.req, { at: produce.req.at - 100, deliver_at: produce.req.at - 95 }),
    raw(produce.req), raw(produce.resp),
    // Then the same response to the consumer on its own connection.
    raw(fetch.ex.resp, { at: t0 + 3, deliver_at: t0 + 4, bytes: b64(fetchResp) }),
    raw(fetch.ex.req, { at: t0 + 5, deliver_at: t0 + 6, src: { node: consumer, port: 0 }, conn: 99 }),
    raw(fetch.ex.resp, { at: t0 + 7, deliver_at: t0 + 8, dst: { node: consumer, port: 0 }, conn: 99, bytes: b64(fetchResp) }),
    raw(offsetCommit.req, { at: t0 + 9, deliver_at: t0 + 10, bytes: b64(commitReq) }), raw(offsetCommit.resp, { at: t0 + 11, deliver_at: t0 + 12 }),
  ];
  const c = new Capture();
  c.add(frames);
  const r = await traceRecord(c, rec, { topicIds, brokers: new Set([1, 2, 3]), replicas: [produce.server.node, follower] });
  const by = (step) => r.steps.filter((s) => s.step === step);
  assert.deepEqual(r.steps.map((s) => [s.step, Boolean(s.found)]), [['produce', true], ['replicate', true], ['hwm', true], ['deliver', true], ['commit', true]]);
  assert.equal(by('replicate')[0].to, follower);
  assert.equal(by('replicate')[0].at, t0 + 4);
  assert.match(by('hwm')[0].detail, /high watermark 19 > 18/);
  assert.equal(by('deliver')[0].to, consumer);
  assert.equal(by('deliver')[0].at, t0 + 8);
  assert.match(by('commit')[0].detail, /group billing committed 19/);
  assert.equal(by('commit')[0].at, t0 + 9);
  // A record past the high watermark is copied and delivered, not committed past.
  const r2 = await traceRecord(c, { ...rec, offset: 19 }, { topicIds, replicas: [produce.server.node, follower], brokers: new Set([1, 2, 3]) });
  assert.equal(r2.steps[0].found, undefined, 'offset 19 is not in that Produce batch');
});

console.log(`check-lab-observe: ${passed} checks passed${process.exitCode ? ', some FAILED' : ''}`);
