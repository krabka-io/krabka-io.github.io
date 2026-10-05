// Decoders for the files a broker keeps in its log directory, with checks.
//
// analyzeFile(path, bytes, { sibling }) returns
//   { kind, root, summary: [[label, value]], checks: [{ ok, text }], batches }
// where `root` is a tree for the bytes view (kafka-decode.js node shape) and
// `sibling(name)` reads another file of the same directory, for the checks
// that compare an index with its segment.
//
// Formats, as Kafka writes them and krabka keeps them:
//   NNN.log        RecordBatch v2, back to back
//   NNN.index      8-byte entries: offset − NNN (int32), byte position (int32)
//   NNN.timeindex  12-byte entries: max timestamp (int64), offset − NNN (int32)
//   NNN.snapshot   producer state: version, CRC-32C of the rest, entries
//   *checkpoint    text: version, count, one entry per line
//   quorum-state, *.json, meta.properties: text

import { crc32c } from "./codecs.js";
import { decodeBatches, loadSchemas, Reader, fmtInt } from "./kafka-decode.js";

const node = (label, start, end, extra = {}) => ({ label, start, end, buf: "main", ...extra });
const latin = new TextDecoder("latin1");

export function baseName(path) {
  return path.slice(path.lastIndexOf("/") + 1);
}

// The topic a partition directory holds: "orders-0" → "orders".
export function topicOf(path) {
  if (/__cluster_metadata|bootstrap\.records/.test(path)) return "__cluster_metadata";
  const dir = path.split("/").slice(-2, -1)[0] || "";
  return dir.replace(/-\d+$/, "");
}

function segmentBase(path) {
  const m = /^(\d{20})\./.exec(baseName(path));
  return m ? BigInt(m[1]) : null;
}

const fmtTime = (ms) => (typeof ms === "bigint" ? ms : BigInt(ms)).toString();

async function analyzeLog(path, bytes) {
  const schemas = await loadSchemas();
  const buffers = new Map();
  const { nodes, summaries } = await decodeBatches(bytes, 0, bytes.length, "main", schemas, { topic: topicOf(path), buffers });
  const root = node(baseName(path), 0, bytes.length, { kind: "struct", value: `${summaries.length} batches`, children: nodes });
  const checks = [];
  const base = segmentBase(path);
  if (summaries.length) {
    const first = summaries[0];
    const last = summaries[summaries.length - 1];
    if (base != null) checks.push({ ok: first.baseOffset === base, text: `first batch starts at offset ${first.baseOffset}; the file name says ${base}` });
    const bad = summaries.filter((b) => !b.crcOk);
    checks.push({ ok: !bad.length, text: bad.length ? `${bad.length} of ${summaries.length} batches fail their CRC-32C` : `all ${summaries.length} batch CRC-32Cs verify` });
    const gaps = [];
    const backwards = [];
    for (let i = 1; i < summaries.length; i++) {
      const prev = summaries[i - 1];
      const next = summaries[i];
      if (next.baseOffset <= prev.lastOffset) backwards.push(`${prev.lastOffset} → ${next.baseOffset}`);
      else if (next.baseOffset !== prev.lastOffset + 1n) gaps.push(`${prev.lastOffset + 1n}–${next.baseOffset - 1n}`);
      if (next.epoch < prev.epoch) backwards.push(`leader epoch ${prev.epoch} → ${next.epoch}`);
    }
    checks.push({ ok: !backwards.length, text: backwards.length ? `offsets or epochs go backwards: ${backwards.slice(0, 4).join(", ")}` : "offsets and leader epochs never go backwards" });
    checks.push({ ok: true, warn: gaps.length > 0, text: gaps.length ? `offset gaps (compaction or aborted data would explain them): ${gaps.slice(0, 4).join(", ")}${gaps.length > 4 ? " …" : ""}` : "offsets are contiguous" });
    const end = last.pos + last.size;
    checks.push({ ok: end === bytes.length, text: end === bytes.length ? "the batches fill the file exactly" : `${bytes.length - end} bytes after the last whole batch` });
    const problems = summaries.flatMap((b) => b.problems.filter((p) => p !== "CRC mismatch").map((p) => `offset ${b.baseOffset}: ${p}`));
    if (problems.length) checks.push({ ok: false, text: problems.slice(0, 3).join("; ") });
  } else checks.push({ ok: bytes.length === 0, text: bytes.length ? "no RecordBatch could be read" : "empty segment" });
  const records = summaries.reduce((n, b) => n + b.count, 0);
  const codecs = [...new Set(summaries.map((b) => ["none", "gzip", "snappy", "lz4", "zstd"][b.codec]))];
  const producers = new Set(summaries.filter((b) => b.producerId >= 0n).map((b) => String(b.producerId)));
  const summary = summaries.length ? [
    ["batches", summaries.length], ["records", records],
    ["offsets", `${summaries[0].baseOffset}–${summaries[summaries.length - 1].lastOffset}`],
    ["leader epochs", [...new Set(summaries.map((b) => b.epoch))].join(", ")],
    ["timestamps", `${fmtTime(summaries[0].baseTimestamp)}–${fmtTime(summaries[summaries.length - 1].maxTimestamp)} ms`],
    ["compression", codecs.join(", ")],
    ["control batches", summaries.filter((b) => b.control).length],
    ["producer ids", producers.size ? [...producers].slice(0, 6).join(", ") : "none (not idempotent)"],
  ] : [["batches", 0]];
  return { kind: "segment", root, summary, checks, batches: summaries, buffers };
}

async function segmentBatches(sibling, path) {
  const name = baseName(path).replace(/\.(index|timeindex|snapshot)$/, ".log");
  const log = await sibling(name);
  if (!log) return null;
  const schemas = await loadSchemas();
  return (await decodeBatches(log, 0, log.length, "main", schemas, { topic: topicOf(path) })).summaries;
}

async function analyzeIndex(path, bytes, sibling, time) {
  const width = time ? 12 : 8;
  const base = segmentBase(path) ?? 0n;
  const v = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const children = [];
  const entries = [];
  for (let at = 0; at + width <= bytes.length; at += width) {
    const ts = time ? v.getBigInt64(at) : null;
    const rel = v.getInt32(at + (time ? 8 : 0));
    const pos = time ? null : v.getInt32(at + 4);
    // Kafka preallocates index files and pads them with zeros: a zero entry after the first ends them.
    if (at > 0 && rel === 0 && (time ? ts === 0n : pos === 0)) break;
    const offset = base + BigInt(rel);
    entries.push({ at, ts, rel, pos, offset });
    const kids = time
      ? [node("Timestamp", at, at + 8, { value: fmtInt(ts), kind: "ts" }), node("Relative offset", at + 8, at + 12, { value: rel, kind: "int", note: `offset ${offset}` })]
      : [node("Relative offset", at, at + 4, { value: rel, kind: "int", note: `offset ${offset}` }), node("Position", at + 4, at + 8, { value: pos, kind: "len" })];
    children.push(node(`Entry ${entries.length - 1}`, at, at + width, { kind: "struct", value: time ? `${ts} ms → offset ${offset}` : `offset ${offset} → byte ${pos}`, children: kids }));
  }
  if (entries.length * width < bytes.length) children.push(node("Zero padding", entries.length * width, bytes.length, { kind: "bytes", value: `${bytes.length - entries.length * width} bytes` }));
  const checks = [{ ok: bytes.length % width === 0, text: `${bytes.length} bytes: ${bytes.length % width === 0 ? "whole" : "not whole"} ${width}-byte entries` }];
  const mono = entries.every((e, i) => i === 0 || (e.offset > entries[i - 1].offset && (time ? e.ts >= entries[i - 1].ts : e.pos > entries[i - 1].pos)));
  checks.push({ ok: mono, text: mono ? "entries increase" : "entries are out of order" });
  const batches = await segmentBatches(sibling, path);
  if (!batches) checks.push({ ok: true, warn: true, text: "no segment file beside it to check the entries against" });
  else {
    entries.forEach((e, i) => {
      const batch = time ? batches.find((b) => e.offset >= b.baseOffset && e.offset <= b.lastOffset) : batches.find((b) => b.pos === e.pos);
      let ok;
      let why;
      if (!batch) {
        ok = false;
        why = time ? `offset ${e.offset} is in no batch of the segment` : `byte ${e.pos} is not where a batch starts`;
      } else if (time) {
        ok = batch.maxTimestamp === e.ts;
        why = ok ? `the batch holding offset ${e.offset} has max timestamp ${e.ts}` : `the batch holding offset ${e.offset} has max timestamp ${batch.maxTimestamp}, not ${e.ts}`;
      } else {
        ok = e.offset >= batch.baseOffset && e.offset <= batch.lastOffset;
        why = ok ? `byte ${e.pos} starts the batch of offsets ${batch.baseOffset}–${batch.lastOffset}` : `byte ${e.pos} starts offsets ${batch.baseOffset}–${batch.lastOffset}, not ${e.offset}`;
      }
      Object.assign(children[i], { status: ok ? "ok" : "bad", note: why });
    });
    const bad = children.filter((c) => c.status === "bad").length;
    checks.push({ ok: !bad, text: bad ? `${bad} of ${entries.length} entries disagree with the segment` : `all ${entries.length} entries agree with the segment's ${batches.length} batches` });
  }
  const root = node(baseName(path), 0, bytes.length, { kind: "struct", value: `${entries.length} entries`, children });
  return { kind: time ? "timeindex" : "index", root, checks, summary: [["entries", entries.length], ["base offset", String(base)]] };
}

// Kafka's ProducerStateManager snapshot, version 1.
function analyzeSnapshot(path, bytes) {
  const r = new Reader(bytes);
  const children = [];
  const checks = [];
  const f = (label, read, extra = {}) => {
    const s = r.pos;
    const value = read();
    children.push(node(label, s, r.pos, { value: fmtInt(value), kind: "int", ...extra }));
    return value;
  };
  try {
    const version = f("Version", () => r.i16());
    const stored = new DataView(bytes.buffer, bytes.byteOffset).getUint32(2);
    const computed = crc32c(bytes.subarray(6));
    f("CRC-32C", () => r.u32(), { kind: "crc", value: `0x${stored.toString(16).padStart(8, "0")}`, status: stored === computed ? "ok" : "bad", note: stored === computed ? "matches the bytes after it" : `computed 0x${computed.toString(16)}` });
    checks.push({ ok: stored === computed, text: stored === computed ? "CRC-32C verifies" : "CRC-32C mismatch" });
    const count = f("Producer entries", () => r.i32(), { kind: "len" });
    for (let i = 0; i < count; i++) {
      const s = r.pos;
      const kids = [];
      const g = (label, read, extra = {}) => {
        const a = r.pos;
        const value = read();
        kids.push(node(label, a, r.pos, { value: fmtInt(value), kind: "int", ...extra }));
        return value;
      };
      const pid = g("Producer id", () => r.i64());
      g("Producer epoch", () => r.i16());
      g("Last sequence", () => r.i32());
      g("Last offset", () => r.i64());
      g("Offset delta", () => r.i32());
      g("Timestamp", () => r.i64(), { kind: "ts" });
      g("Coordinator epoch", () => r.i32());
      g("Current txn first offset", () => r.i64());
      children.push(node(`Producer ${pid}`, s, r.pos, { kind: "struct", children: kids }));
    }
    checks.push({ ok: r.pos === bytes.length, text: r.pos === bytes.length ? `version ${version}, ${count} producers, nothing left over` : `${bytes.length - r.pos} bytes left over` });
  } catch (err) {
    children.push(node("Not decoded", r.pos, bytes.length, { kind: "bytes", status: "bad", value: err.message }));
  }
  return { kind: "snapshot", root: node(baseName(path), 0, bytes.length, { kind: "struct", children }), checks, summary: [] };
}

function analyzeText(path, bytes) {
  const text = latin.decode(bytes);
  const children = [];
  let at = 0;
  for (const line of text.split("\n")) {
    if (line.length) children.push(node(`Line ${children.length + 1}`, at, at + line.length, { kind: "str", value: JSON.stringify(line) }));
    at += line.length + 1;
  }
  const checks = [];
  const summary = [];
  const lines = text.trim().split("\n");
  // Kafka's checkpoint files: version, count, then the entries. A one-line
  // checkpoint is krabka's own: the value alone.
  if (/checkpoint$/.test(path) && lines.length === 1) summary.push(["value", lines[0]]);
  else if (/checkpoint$/.test(path)) {
    const count = Number(lines[1]);
    checks.push({ ok: lines.length - 2 === count, text: `version ${lines[0]}, ${count} entries declared, ${lines.length - 2} present` });
    if (/leader-epoch/.test(path)) for (const l of lines.slice(2)) summary.push([`epoch ${l.split(" ")[0]}`, `starts at offset ${l.split(" ")[1]}`]);
    else for (const l of lines.slice(2)) summary.push([l.split(" ").slice(0, -1).join(" ") || "value", l.split(" ").pop()]);
  }
  let pretty = null;
  try {
    pretty = JSON.stringify(JSON.parse(text), null, 2);
    checks.push({ ok: true, text: "valid JSON" });
  } catch { /* not JSON */ }
  return { kind: "text", root: node(baseName(path), 0, bytes.length, { kind: "struct", children }), checks, summary, text: pretty ?? text };
}

export async function analyzeFile(path, bytes, { sibling = async () => null } = {}) {
  const name = baseName(path);
  if (name.endsWith(".log")) return analyzeLog(path, bytes);
  if (name.endsWith(".timeindex")) return analyzeIndex(path, bytes, sibling, true);
  if (name.endsWith(".index")) return analyzeIndex(path, bytes, sibling, false);
  if (name.endsWith(".snapshot")) return analyzeSnapshot(path, bytes);
  const printable = bytes.every((b) => b === 9 || b === 10 || b === 13 || (b >= 32 && b < 127));
  if (printable) return analyzeText(path, bytes);
  return { kind: "binary", root: node(name, 0, bytes.length, { kind: "bytes", value: `${bytes.length} bytes` }), checks: [], summary: [] };
}

// A whole partition: every segment decoded, then the checks that span them.
// `read(path)` returns a file's bytes. Returns { segments, checks, epochs }.
export async function analyzePartition(part, read) {
  const schemas = await loadSchemas();
  const segments = [];
  for (const s of part.segments) {
    const bytes = await read(s.path);
    if (!bytes) continue;
    const { summaries } = await decodeBatches(bytes, 0, bytes.length, "main", schemas, { topic: part.topic });
    const crcBad = summaries.filter((b) => !b.crcOk).length;
    segments.push({
      path: s.path, base: s.base, size: bytes.length, batches: summaries.length, crcBad,
      records: summaries.reduce((n, b) => n + b.count, 0),
      first: summaries[0]?.baseOffset ?? null, last: summaries[summaries.length - 1]?.lastOffset ?? null,
      epochs: [...new Set(summaries.map((b) => b.epoch))],
      firstByEpoch: summaries.reduce((m, b) => (m.has(b.epoch) ? m : m.set(b.epoch, b.baseOffset)), new Map()),
      control: summaries.filter((b) => b.control).length,
    });
  }
  const checks = [];
  const bad = segments.reduce((n, s) => n + s.crcBad, 0);
  checks.push({ ok: !bad, text: bad ? `${bad} batches fail their CRC-32C` : `every batch CRC-32C verifies across ${segments.length} segments` });
  const names = segments.filter((s) => s.first != null && s.first !== s.base);
  checks.push({ ok: !names.length, text: names.length ? `segments whose first offset is not their name: ${names.map((s) => baseName(s.path)).join(", ")}` : "every segment starts at the offset its name says" });
  const joins = [];
  for (let i = 1; i < segments.length; i++) {
    const prev = segments[i - 1];
    const next = segments[i];
    if (prev.last != null && next.first != null && next.first !== prev.last + 1n) joins.push(`${prev.last} → ${next.first}`);
  }
  checks.push({ ok: true, warn: joins.length > 0, text: joins.length ? `offsets jump between segments: ${joins.slice(0, 4).join(", ")}` : "each segment continues where the previous one ends" });
  const tiny = segments.filter((s) => s.batches === 1).length;
  if (segments.length > 3 && tiny > segments.length / 2) checks.push({ ok: true, warn: true, text: `${tiny} of ${segments.length} segments hold a single batch: the log rolls a segment per append` });
  // The leader-epoch checkpoint names the first offset of each epoch; the log must agree.
  const epochFile = part.files.find((f) => baseName(f.path) === "leader-epoch-checkpoint");
  const epochs = [];
  if (epochFile) {
    const text = new TextDecoder().decode((await read(epochFile.path)) || new Uint8Array());
    for (const line of text.trim().split("\n").slice(2)) {
      const [epoch, start] = line.split(" ").map((x) => x && BigInt(x));
      if (epoch == null || start == null) continue;
      const firstSeen = segments.map((s) => s.firstByEpoch.get(Number(epoch))).find((o) => o != null);
      epochs.push({ epoch: Number(epoch), start, firstSeen });
    }
    const off = epochs.filter((e) => e.firstSeen != null && e.firstSeen !== e.start);
    checks.push({ ok: !off.length, text: !epochs.length ? "leader-epoch-checkpoint is empty" : off.length ? `leader epochs whose first batch is not where the checkpoint says: ${off.map((e) => `${e.epoch} at ${e.firstSeen}, not ${e.start}`).join("; ")}` : `leader-epoch-checkpoint agrees with the log for ${epochs.length} epoch${epochs.length === 1 ? "" : "s"}` });
  }
  return { segments, checks, epochs };
}

// A partition directory at a glance, from its file list: segment bases and sizes.
export function partitionsOf(files) {
  const parts = new Map();
  for (const f of files) {
    const dir = f.path.slice(0, f.path.lastIndexOf("/"));
    const name = baseName(f.path);
    if (!/\.(log|index|timeindex|snapshot)$|checkpoint$/.test(name)) continue;
    let p = parts.get(dir);
    if (!p) parts.set(dir, (p = { dir, topic: topicOf(f.path), files: [], segments: [], bytes: 0 }));
    p.files.push(f);
    p.bytes += f.size;
    const base = segmentBase(name);
    if (base != null && name.endsWith(".log")) p.segments.push({ base, size: f.size, path: f.path });
  }
  for (const p of parts.values()) p.segments.sort((a, b) => (a.base < b.base ? -1 : 1));
  return [...parts.values()].sort((a, b) => a.dir.localeCompare(b.dir, undefined, { numeric: true }));
}
