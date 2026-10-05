// Trace a record: follow one produced record through the network capture.
//
// From a producer's "Last records" row (topic, partition, offset, key) the
// matcher walks the capture (capture.js) and decodes the exchanges it needs
// (kafka-decode.js):
//
//   produce    the Produce request whose batch holds the offset: the response
//              gives the batch's base offset, the request its records, and the
//              record at that index must carry the row's key
//   replicate  each follower's Fetch response whose batches cover the offset
//   hwm        the first Fetch response whose high watermark passed it
//   deliver    a consumer's Fetch response whose batches cover it
//   commit     the first OffsetCommit for the partition past it
//
// Produce and Fetch name topics by id from v13: the ids come from the
// Metadata responses in the capture and from the cluster observer. A step that
// is missing says why: not happened yet, outside the capture window, or the
// decode budget ran out.

import { el, button, fmtMs } from "./dom.js";
import { decodeFrame } from "./kafka-decode.js";

const PRODUCE = 0;
const FETCH = 1;
const OFFSET_COMMIT = 8;
const METADATA = 3;
const MAX_DECODES = 800;
// Longer than any fetch's max wait.
const LONG_POLL_MS = 5000;

// Decoded frames, kept while their frame lives.
const decoded = new WeakMap();
async function decode(f, request, ex) {
  let d = decoded.get(f);
  if (!d) {
    d = await decodeFrame(f.bytes, { size: f.size, request, answers: ex });
    decoded.set(f, d);
  }
  return d;
}

// ---- walking the decoded tree ----

const kid = (n, ...labels) => n?.children?.find((c) => labels.includes(c.label));
const items = (n) => (n?.children || []).filter((c) => c.label.startsWith("["));
const num = (n) => (n == null || n.value == null ? null : Number(n.value));
// The message body: the root's struct after the header.
const body = (d) => d.root.children.find((c) => c.kind === "struct" && !/header/i.test(c.label));
// Topic ids as the decoder prints them: URL-safe base64, no padding.
export const normId = (id) => String(id || "").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

// Each topic entry of `list` with its name (by name or by id) and its partitions.
function* topicsIn(list, topicIds) {
  for (const t of items(list)) {
    const name = kid(t, "Name", "Topic", "TopicName")?.value ?? topicIds.get(normId(kid(t, "TopicId")?.value)) ?? null;
    yield { name, node: t };
  }
}

// Batches under a Records field: the decoder's batch summaries.
const batchesOf = (records) => (records?.children || []).filter((c) => c.kind === "batch").map((c) => c.batch);
const covers = (b, offset) => !b.control && Number(b.baseOffset) <= offset && offset <= Number(b.lastOffset);

// Topic id → name from the Metadata responses in the capture, newest first,
// until `topic` has its id.
async function metadataIds(capture, topicIds, topic) {
  for (let i = capture.exchanges.length - 1; i >= 0; i--) {
    if ([...topicIds.values()].includes(topic)) return;
    const ex = capture.exchanges[i];
    if (ex.apiKey !== METADATA || !ex.resp?.bytes) continue;
    const d = await decode(ex.resp, false, ex);
    for (const t of items(kid(body(d), "Topics"))) {
      const id = kid(t, "TopicId")?.value;
      const name = kid(t, "Name")?.value;
      if (id && name) topicIds.set(normId(id), name);
    }
  }
}

// `rec`: { producer, topic, partition, offset, key }. `opts`: brokers (Set of
// broker node ids), replicas ([broker ids] of the partition, when known),
// topicIds (Map id → name), maxDecodes. Resolves { steps, decodes, note }.
export async function traceRecord(capture, rec, opts = {}) {
  const brokers = opts.brokers || new Set();
  const topicIds = new Map([...(opts.topicIds || [])].map(([k, v]) => [normId(k), v]));
  const budget = { left: opts.maxDecodes ?? MAX_DECODES };
  const steps = [];
  const span = capture.span();
  const end = span ? span[1] : null;
  const lost = capture.evicted || capture.dropped || capture.ignored;
  const lostWhy = [capture.evicted && "evicted frames to stay within its budget", capture.dropped && "dropped frames the page read too late", capture.ignored && "was paused"].filter(Boolean).join(", ");
  if (rec.offset == null) {
    return { steps: [{ step: "produce", label: "Produce request", why: "The record is not acknowledged yet, so its offset is unknown. Refresh once the row shows an offset." }], decodes: 0 };
  }
  await metadataIds(capture, topicIds, rec.topic);
  const want = (name, part) => name === rec.topic && Number(part) === Number(rec.partition);

  // 1. The Produce request whose batch holds the offset.
  let produce = null;
  for (let i = capture.exchanges.length - 1; i >= 0 && budget.left > 0; i--) {
    const ex = capture.exchanges[i];
    if (ex.apiKey !== PRODUCE || !ex.resp?.bytes || (rec.producer != null && ex.client.node !== rec.producer)) continue;
    budget.left--;
    const resp = await decode(ex.resp, false, ex);
    let base = null;
    for (const t of topicsIn(kid(body(resp), "Responses"), topicIds)) {
      for (const p of items(kid(t.node, "PartitionResponses"))) if (want(t.name, num(kid(p, "Index")))) base = num(kid(p, "BaseOffset"));
    }
    if (base == null || base < 0 || base > rec.offset) continue;
    const req = await decode(ex.req, true, ex);
    for (const t of topicsIn(kid(body(req), "TopicData"), topicIds)) {
      for (const p of items(kid(t.node, "PartitionData"))) {
        if (!want(t.name, num(kid(p, "Index")))) continue;
        const records = batchesOf(kid(p, "Records")).flatMap((b) => b.records);
        const r = records[rec.offset - base];
        if (r) produce = { ex, record: r, base };
      }
    }
    if (produce) break;
    // An earlier batch of this partition cannot hold a later offset.
    break;
  }
  const outside = (what) => (lost && span && (span[0] > 0 || capture.evicted) ? `Outside the capture window: the capture ${lostWhy}, and keeps ${fmtMs(span[0])}–${fmtMs(end)}.` : what);
  if (!produce) {
    steps.push({ step: "produce", label: "Produce request", why: outside(span ? `No Produce request for offset ${rec.offset} in the capture (${fmtMs(span[0])}–${fmtMs(end)}).` : "The capture is empty.") });
    return { steps, decodes: (opts.maxDecodes ?? MAX_DECODES) - budget.left };
  }
  const keyOk = rec.key == null || produce.record.key === JSON.stringify(String(rec.key));
  const pex = produce.ex;
  const leader = pex.server.node;
  steps.push({
    step: "produce", label: "Produce request", found: true, at: pex.req.at, from: pex.client.node, to: leader, ex: pex,
    detail: `batch base offset ${produce.base}, record ${rec.offset - produce.base}${produce.record.key != null ? `, key ${produce.record.key}` : ""}${keyOk ? "" : " (the key differs from the row's)"}; acked ${fmtMs(pex.resp.deliverAt)}`,
  });

  // 2–5. Forward from the produce: Fetch responses and OffsetCommits. A
  // fetch long-polls, so one sent shortly before the Produce can answer with
  // the record: the scan starts a long-poll window earlier, skips responses
  // sent before the record reached the leader, and keeps the earliest answer.
  const followers = (opts.replicas || []).filter((r) => r !== leader);
  const copied = new Map();
  const delivered = new Map();
  let hwm = null;
  let commit = null;
  const arrived = (ex) => ex.resp.deliverAt;
  const earlier = (map, key, ex) => (!map.has(key) || arrived(ex) < arrived(map.get(key))) && map.set(key, ex);
  const done = () => hwm && commit && delivered.size && (opts.replicas ? followers.every((f) => copied.has(f)) : false);
  let start = capture.exchanges.indexOf(pex);
  while (start > 0 && capture.exchanges[start - 1].req.at >= pex.req.at - LONG_POLL_MS) start--;
  let doneAt = null;
  for (let i = start; i < capture.exchanges.length && budget.left > 0; i++) {
    const ex = capture.exchanges[i];
    if (ex === pex) continue;
    // Everything found, and later requests cannot be answered earlier.
    if (doneAt == null && done()) doneAt = Math.max(...[hwm.ex, ...copied.values(), ...delivered.values()].map(arrived), commit.ex.req.at);
    if (doneAt != null && ex.req.at > doneAt) break;
    if (ex.apiKey === FETCH && ex.resp?.bytes && ex.resp.at >= pex.req.deliverAt) {
      const replica = brokers.has(ex.client.node);
      // A follower that has the record already, or a consumer that got it, needs nothing more.
      const got = replica ? copied.get(ex.client.node) : delivered.get(ex.client.node);
      if (hwm && got && ex.req.at > arrived(got)) continue;
      budget.left--;
      const d = await decode(ex.resp, false, ex);
      for (const t of topicsIn(kid(body(d), "Responses"), topicIds)) {
        for (const p of items(kid(t.node, "Partitions"))) {
          if (!want(t.name, num(kid(p, "PartitionIndex")))) continue;
          const mark = num(kid(p, "HighWatermark"));
          if (mark != null && mark > rec.offset && (!hwm || arrived(ex) < arrived(hwm.ex))) hwm = { ex, mark };
          if (batchesOf(kid(p, "Records")).some((b) => covers(b, rec.offset))) earlier(replica ? copied : delivered, ex.client.node, ex);
        }
      }
    } else if (ex.apiKey === OFFSET_COMMIT && ex.req?.bytes && !commit && ex.req.at >= pex.req.at) {
      budget.left--;
      const d = await decode(ex.req, true, ex);
      for (const t of topicsIn(kid(body(d), "Topics"), topicIds)) {
        for (const p of items(kid(t.node, "Partitions"))) {
          const off = num(kid(p, "CommittedOffset"));
          if (want(t.name, num(kid(p, "PartitionIndex"))) && off > rec.offset) commit = { ex, off, group: kid(body(d), "GroupId")?.value };
        }
      }
    }
  }
  const ranOut = budget.left <= 0;
  const notYet = (what) => (ranOut ? `Stopped after decoding ${opts.maxDecodes ?? MAX_DECODES} exchanges; Refresh to look further.` : `${what} in the capture up to ${fmtMs(end)}: not happened yet, or not at all.`);
  const exStep = (step, label, ex, detail, extra = {}) => ({ step, label, found: true, at: ex.resp?.deliverAt ?? ex.req.at, from: ex.server.node, to: ex.client.node, ex, detail, ...extra });

  if (opts.replicas && !followers.length) steps.push({ step: "replicate", label: "Replication", why: "The partition has one replica: no follower copies it." });
  for (const f of opts.replicas ? followers : [...copied.keys()]) {
    const ex = copied.get(f);
    if (ex) steps.push(exStep("replicate", "Copied to a follower", ex, `the follower's Fetch response carries offset ${rec.offset}`));
    else steps.push({ step: "replicate", label: "Copied to a follower", to: f, why: notYet(`No Fetch response to broker ${f} with this offset`) });
  }
  if (!opts.replicas && !copied.size) steps.push({ step: "replicate", label: "Copied to a follower", why: notYet("No follower Fetch response with this offset") });
  steps.push(hwm ? exStep("hwm", "High watermark passed it", hwm.ex, `high watermark ${hwm.mark} > ${rec.offset}: committed to the ISR, readable by consumers`) : { step: "hwm", label: "High watermark passed it", why: notYet("No Fetch response with a high watermark past this offset") });
  if (delivered.size) for (const ex of delivered.values()) steps.push(exStep("deliver", "Delivered to a consumer", ex, `the consumer's Fetch response carries offset ${rec.offset}`));
  else steps.push({ step: "deliver", label: "Delivered to a consumer", why: notYet("No consumer Fetch response with this offset") });
  steps.push(commit
    ? { step: "commit", label: "Offset committed past it", found: true, at: commit.ex.req.at, from: commit.ex.client.node, to: commit.ex.server.node, ex: commit.ex, detail: `${commit.group ? `group ${commit.group} ` : ""}committed ${commit.off}${commit.ex.errors?.length ? `, answered ${commit.ex.errors.join(" ")}` : ""}` }
    : { step: "commit", label: "Offset committed past it", why: notYet("No OffsetCommit past this offset") });
  return { steps, decodes: (opts.maxDecodes ?? MAX_DECODES) - budget.left };
}

// ---- the panel ----

export class TracePanel {
  // hooks: capture, nodeName(id), context() → { brokers, replicas(topic, p), topicIds }, showExchange(ex)
  constructor(container, hooks) {
    this.hooks = hooks;
    this.rec = null;
    this.root = el("div", "lab-trace");
    this.head = el("div", "lab-charts-bar");
    this.body = el("div", "lab-trace-body");
    this.root.append(this.head, this.body);
    container.appendChild(this.root);
    this.renderEmpty();
  }

  renderEmpty() {
    this.head.replaceChildren(el("span", "lab-muted lab-small", "Select a producer and press Trace on one of its Last records to follow that record through the network capture."));
    this.body.replaceChildren();
  }

  async trace(rec) {
    this.rec = rec;
    const name = this.hooks.nodeName;
    const refresh = button("Refresh", "lab-btn-sm", () => this.trace(this.rec), { title: "Look through the capture again" });
    this.head.replaceChildren(el("strong", null, `${rec.topic}-${rec.partition} offset ${rec.offset ?? "unacked"}`), el("span", "lab-muted lab-small", `seq ${rec.seq} from ${name(rec.producer)}${rec.key != null ? ` · key ${rec.key}` : ""}`), refresh);
    this.body.replaceChildren(el("p", "lab-muted lab-small", "Looking through the capture…"));
    const ctx = this.hooks.context();
    const result = await traceRecord(this.hooks.capture, rec, { brokers: ctx.brokers, replicas: ctx.replicas(rec.topic, rec.partition), topicIds: ctx.topicIds });
    if (this.rec !== rec) return;
    const list = el("ol", "lab-trace-steps");
    for (const s of result.steps) {
      const li = el("li", s.found ? "lab-trace-found" : "lab-trace-missing");
      li.dataset.step = s.step;
      const nodes = s.from != null && s.to != null ? `${name(s.from)} → ${name(s.to)}` : s.to != null ? name(s.to) : "";
      li.append(el("span", "lab-trace-at", s.found ? fmtMs(s.at) : "–"), el("strong", null, s.label), el("span", "lab-muted", nodes));
      if (s.ex) li.appendChild(button(`Exchange ${s.ex.id}`, "lab-btn-sm", () => this.hooks.showExchange(s.ex), { title: "Select this exchange in the Network tab" }));
      li.appendChild(el("span", "lab-trace-detail", s.found ? s.detail : s.why));
      list.appendChild(li);
    }
    this.body.replaceChildren(list, el("p", "lab-muted lab-small", `Decoded ${result.decodes} exchange${result.decodes === 1 ? "" : "s"}.`));
  }
}
