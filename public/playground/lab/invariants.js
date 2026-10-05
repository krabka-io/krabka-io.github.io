// The live invariant checker: what a Kafka cluster promises, checked against
// every snapshot the lab takes.
//
// A pure module (no DOM), so the page, the experiment runner and the plain-node
// check all use it the same way:
//
//   import { InvariantChecker } from "./invariants.js";
//   const checker = new InvariantChecker();
//   const fresh = checker.observe(world.snapshot(), newEvents, world.scenario());
//   // fresh: the violations this call found; checker.violations: all of them
//   checker.ok          // true while nothing was violated (`invariants_hold`)
//   checker.reset()     // a new run of the scenario
//
// `snapshot` is `world.snapshot()`; `events` the world events since the last
// call (only `admin_done` with `cmd: "reset_offsets"` is read); `scenario`
// (optional) gives the consumers' configured `isolation_level` when their
// snapshot does not carry it. Every field is optional: a module without the
// cluster observer or the producer and consumer additions checks less, it
// does not fail.
//
// A violation is `{ id, at, check, node, text, data }`, `at` in lab ms:
//
//   acked_lost         a producer with acks=all saw an offset acknowledged, and
//                      the partition's high watermark then stayed at or below
//                      it for STALL_MS, or the partition was gone on two
//                      cluster polls in a row. A new leader's high watermark
//                      can stand below the old one until its ISR catches up or
//                      shrinks (replica.lag.time.max.ms, 30 s by default), so
//                      a short dip is not a loss.
//                      ponytail: a loss that new writes cover within STALL_MS
//                      (an unclean election with producers still writing) goes
//                      unseen; comparing the records at the acked offsets
//                      through the capture would catch it.
//   offset_regression  a consumer's position went backwards with no
//                      reset_offsets for its group at or before that moment
//   split_leader       two brokers led one partition in the same leader epoch
//   split_quorum       two KRaft quorum leaders in the same epoch
//   aborted_read       a read_committed consumer received a record of an
//                      aborted transaction

export const CHECKS = {
  acked_lost: "No acknowledged record lost",
  offset_regression: "Consumer offsets only move forward",
  split_leader: "One leader per partition and leader epoch",
  split_quorum: "One KRaft quorum leader per epoch",
  aborted_read: "read_committed never sees aborted records",
};

// How long a high watermark may stand at or below an acked offset before the
// record counts as lost: past replica.lag.time.max.ms and a broker session.
export const STALL_MS = 60_000;

// How long a reset_offsets excuses a regression after it completed.
const RESET_GRACE_MS = 30_000;

const isAll = (acks) => acks === -1 || acks === "-1" || acks === "all";

export class InvariantChecker {
  constructor() {
    this.reset();
  }

  reset() {
    this.violations = [];
    this.seq = 0;
    // tp → [{ t, acked }] increasing in both: the acked offsets seen, by lab time.
    this.acked = new Map();
    // tp → the cluster poll time of the last low high watermark.
    this.lowHwm = new Map();
    this.seen = new Set(); // tps some cluster poll reported
    this.leaders = new Map(); // "topic-p@epoch" → broker
    this.quorum = new Map(); // epoch → leader
    this.polls = new Map(); // admin node → last cluster.at
    this.regressions = new Set();
    this.resets = []; // { group, at }
    this.aborted = new Map(); // consumer → aborted_seen
    this.flagged = new Set(); // keys already reported once
  }

  get ok() {
    return this.violations.length === 0;
  }

  observe(snapshot, events = [], scenario = null) {
    const fresh = [];
    if (!snapshot?.nodes) return fresh;
    const now = snapshot.now ?? 0;
    const add = (v) => {
      const out = { id: ++this.seq, ...v };
      this.violations.push(out);
      fresh.push(out);
    };
    for (const e of events || []) {
      if (e.kind === "admin_done" && e.detail?.cmd === "reset_offsets") this.resets.push({ group: e.detail.group ?? null, at: e.at ?? now });
    }
    const config = (id) => scenario?.nodes?.find((n) => n.id === id)?.config || {};

    // Producers first: an ack seen in this snapshot is checked against the
    // polls that completed after it.
    for (const n of snapshot.nodes) {
      const s = n.state;
      if (n.kind !== "producer" || !s || !isAll(s.acks ?? config(n.id).acks) || !s.acked_upto) continue;
      for (const [tp, off] of Object.entries(s.acked_upto)) {
        if (typeof off !== "number") continue;
        let list = this.acked.get(tp);
        if (!list) this.acked.set(tp, (list = []));
        const last = list[list.length - 1];
        if (!last || off > last.acked) list.push({ t: now, acked: off, node: n.id });
      }
    }

    for (const n of snapshot.nodes) {
      const c = n.state?.cluster;
      if (!c || c.at == null || this.polls.get(n.id) === c.at) continue;
      this.polls.set(n.id, c.at);
      this.checkCluster(c, add);
    }

    for (const n of snapshot.nodes) {
      const s = n.state;
      if (n.kind !== "consumer" || !s) continue;
      const group = s.group ?? config(n.id).group ?? null;
      for (const r of s.offset_regressions || []) {
        const key = `${n.id}:${r.tp}:${r.from}:${r.to}:${r.at}`;
        if (this.regressions.has(key)) continue;
        this.regressions.add(key);
        const at = r.at ?? now;
        const excused = this.resets.some((x) => (x.group == null || x.group === group) && x.at <= at && at - x.at <= RESET_GRACE_MS);
        if (!excused) add({ at, check: "offset_regression", node: n.id, text: `position of ${r.tp} went back from ${r.from} to ${r.to} without a reset_offsets`, data: r });
      }
      const iso = s.isolation_level ?? config(n.id).isolation_level;
      const seen = s.aborted_seen ?? 0;
      const before = this.aborted.get(n.id) ?? 0;
      this.aborted.set(n.id, seen);
      if (iso === "read_committed" && seen > before) {
        add({ at: now, check: "aborted_read", node: n.id, text: `received ${seen - before} record${seen - before === 1 ? "" : "s"} of aborted transactions under read_committed (${seen} in all)`, data: { aborted_seen: seen } });
      }
    }
    return fresh;
  }

  checkCluster(c, add) {
    const at = c.at;
    const q = c.quorum;
    if (q && q.leader != null && q.leader >= 0 && q.epoch != null) {
      const prev = this.quorum.get(q.epoch);
      if (prev == null) this.quorum.set(q.epoch, q.leader);
      else if (prev !== q.leader && this.once(`q${q.epoch}`)) {
        add({ at, check: "split_quorum", node: null, text: `KRaft epoch ${q.epoch} had two leaders: ${prev} and ${q.leader}`, data: { epoch: q.epoch, leaders: [prev, q.leader] } });
      }
    }
    const present = new Set();
    for (const t of c.topics || []) {
      for (const p of t.partitions || []) {
        const tp = `${t.name}-${p.partition}`;
        present.add(tp);
        this.seen.add(tp);
        if (p.leader != null && p.leader >= 0 && p.leader_epoch != null) {
          const key = `${tp}@${p.leader_epoch}`;
          const prev = this.leaders.get(key);
          if (prev == null) this.leaders.set(key, p.leader);
          else if (prev !== p.leader && this.once(`l${key}`)) {
            add({ at, check: "split_leader", node: null, text: `${tp} had two leaders in epoch ${p.leader_epoch}: ${prev} and ${p.leader}`, data: { tp, epoch: p.leader_epoch, leaders: [prev, p.leader] } });
          }
        }
        if (p.hwm != null) this.checkAcked(tp, p.hwm, at, add, p);
      }
    }
    // A partition that held acked records and is gone from a clean poll (a
    // wiped single replica, a deleted topic) lost them all.
    if (!c.errors?.length) for (const tp of this.acked.keys()) if (this.seen.has(tp) && !present.has(tp)) this.checkAcked(tp, null, at, add, {});
  }

  // `hwm` null: the partition is gone.
  checkAcked(tp, hwm, at, add, p) {
    const list = this.acked.get(tp);
    if (!list?.length) return;
    // The highest offset acked before this poll completed.
    while (list.length > 1 && list[1].t <= at) list.shift();
    const ack = list[0].t <= at ? list[0] : null;
    if (!ack) return;
    if (hwm != null && hwm > ack.acked) {
      this.lowHwm.delete(tp);
      return;
    }
    const first = this.lowHwm.get(tp);
    if (first == null) {
      this.lowHwm.set(tp, at);
    } else if (first !== at && (hwm == null || at - first >= STALL_MS) && this.once(`a${tp}@${ack.acked}`)) {
      const now = hwm == null ? "the partition is gone from the cluster" : `the high watermark has stood at ${hwm} or below for ${Math.round((at - first) / 1000)} s`;
      add({ at, check: "acked_lost", node: ack.node, text: `${tp}: offset ${ack.acked} was acknowledged under acks=all, ${now}`, data: { tp, acked: ack.acked, hwm, leader: p.leader, leader_epoch: p.leader_epoch } });
    }
  }

  // True the first time `key` is seen: one violation per broken fact, not per poll.
  once(key) {
    if (this.flagged.has(key)) return false;
    this.flagged.add(key);
    return true;
  }
}
