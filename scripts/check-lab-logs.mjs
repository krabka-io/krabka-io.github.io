// Checks the pure side of the Cluster Lab's Logs tab: line parsing, filtering,
// level directives and the bounded store. Runs with plain node:
//   node scripts/check-lab-logs.mjs

import assert from "node:assert/strict";
import { LEVELS, levelRank, normalizeLevel, parseLine, parseDirective, parseQuery, matchQuery, filterEntries, toNdjson, DIRECTIVE_EXAMPLES } from "../public/playground/lab/logparse.js";
import { LogStore, LogLevels } from "../public/playground/lab/logstore.js";

let passed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
  } catch (err) {
    console.error(`FAIL ${name}\n  ${err.message}`);
    process.exitCode = 1;
  }
}

// ---- parseLine ------------------------------------------------------------------------------------

check("json line: the four keys and the flattened fields", () => {
  const raw = '{"ts":12.345,"level":"WARN","target":"krabka_broker::raft","message":"lost the leader","term":7,"peer":{"id":2}}';
  const r = parseLine(raw);
  assert.equal(r.format, "json");
  assert.equal(r.level, "WARN");
  assert.equal(r.target, "krabka_broker::raft");
  assert.equal(r.message, "lost the leader");
  assert.equal(r.ts, 12.345);
  assert.deepEqual(r.fields, { term: 7, peer: { id: 2 } });
  assert.equal(r.record.term, 7);
  assert.equal(r.raw, raw);
});

check("json line: a missing or odd level is INFO, a string ts is read, a nested message is found", () => {
  assert.equal(parseLine('{"message":"x"}').level, "INFO");
  assert.equal(parseLine('{"level":"verbose","message":"x"}').level, "INFO");
  assert.equal(parseLine('{"level":"warning","message":"x"}').level, "WARN");
  assert.equal(parseLine('{"ts":"3.5","message":"x"}').ts, 3.5);
  assert.equal(parseLine('{"level":"INFO","fields":{"message":"nested"}}').message, "nested");
});

check("compact text line of the first broker build", () => {
  const r = parseLine("  12.345s  INFO krabka_broker::x: message k=v count=3");
  assert.equal(r.format, "text");
  assert.equal(r.level, "INFO");
  assert.equal(r.target, "krabka_broker::x");
  assert.equal(r.message, "message");
  assert.equal(r.ts, 12.345);
  assert.deepEqual(r.fields, { k: "v", count: "3" });
  assert.equal(r.record.level, "INFO");
  const plain = parseLine("   0.001s ERROR krabka_broker: the broker did not start: address in use");
  assert.equal(plain.level, "ERROR");
  assert.equal(plain.message, "the broker did not start: address in use");
  assert.deepEqual(plain.fields, {});
});

check("a panic or any other line is raw, with a guessed level", () => {
  const panic = parseLine("thread 'main' panicked at src/main.rs:12:5:\r");
  assert.equal(panic.format, "raw");
  assert.equal(panic.level, "ERROR");
  assert.equal(panic.raw, "thread 'main' panicked at src/main.rs:12:5:");
  assert.equal(parseLine("[guest] heartbeat 3").level, "INFO");
  assert.equal(parseLine("{ not json }").format, "raw");
  assert.equal(parseLine("[1,2]").format, "raw");
  assert.equal(parseLine("   "), null);
  assert.equal(parseLine(""), null);
});

check("level ordering", () => {
  assert.deepEqual(LEVELS, ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"]);
  for (let i = 1; i < LEVELS.length; i++) assert.ok(levelRank(LEVELS[i]) > levelRank(LEVELS[i - 1]));
  assert.equal(normalizeLevel(" warn "), "WARN");
  assert.equal(normalizeLevel("nope"), null);
});

// ---- directives -----------------------------------------------------------------------------------

check("directives from the spec are valid and normalised", () => {
  for (const d of ["info", "debug", "warn,krabka_broker=debug", "info,krabka_broker::request=debug", "trace", "off", "error"]) {
    const r = parseDirective(d);
    assert.ok(r.ok, `${d}: ${r.error}`);
    assert.equal(r.directive, d);
  }
  for (const d of DIRECTIVE_EXAMPLES) assert.ok(parseDirective(d).ok, d);
  assert.equal(parseDirective(" WARN , krabka_broker = Debug ").directive, "warn,krabka_broker=debug");
  assert.deepEqual(parseDirective("warn,krabka_broker::raft=trace").entries, [{ target: null, level: "warn" }, { target: "krabka_broker::raft", level: "trace" }]);
});

check("an empty directive is valid and means the broker's default", () => {
  assert.deepEqual(parseDirective(""), { ok: true, directive: "", entries: [] });
  assert.equal(parseDirective("   ").ok, true);
});

check("bad directives say why", () => {
  assert.match(parseDirective("loud").error, /not a level/);
  assert.match(parseDirective("krabka_broker=loud").error, /not a level/);
  assert.match(parseDirective("=debug").error, /not a target/);
  assert.match(parseDirective("bad-target=debug").error, /not a target/);
  assert.match(parseDirective("info,,debug").error, /empty/);
  assert.match(parseDirective("info,debug").error, /only one bare level/);
  assert.equal(parseDirective("info,").ok, false);
});

// ---- filtering ------------------------------------------------------------------------------------

const store = new LogStore();
const names = { 1: "node-1", 2: "node-2" };
const nodeName = (id) => names[id] ?? `#${id}`;
store.add(1, "stderr", '{"ts":1.0,"level":"INFO","target":"krabka_broker::raft","message":"elected leader","term":3}', 1000, 0);
store.add(1, "stderr", '{"ts":2.0,"level":"WARN","target":"krabka_broker::raft","message":"slow heartbeat","duration_ms":250}', 2000, 0);
store.add(2, "stderr", '{"ts":2.5,"level":"ERROR","target":"krabka_broker::log","message":"segment rolled back","duration_ms":40}', 2500, 0);
store.add(2, "stderr", '{"ts":3.0,"level":"DEBUG","target":"krabka_broker::network::dispatch","message":"Produce v9","duration_ms":3}', 3000, 0);
store.add(2, "stderr", "thread 'main' panicked at src/main.rs:1:1", 3100);
store.mark(2, "killed", "process killed", { level: "WARN", now: 3200 });
const all = store.entries();

const show = (filter) => filterEntries(all, { nodes: new Set(), targets: new Set(), ...filter }, nodeName).map((e) => e.message);

check("entries carry the lab time: base + ts, or now", () => {
  assert.equal(all[0].at, 1000);
  assert.equal(all[2].at, 2500);
  assert.equal(all[4].at, 3100);
});

check("minimum level keeps the line and above; markers always stay", () => {
  assert.equal(show({ minLevel: "ERROR" }).length, 3);
  assert.deepEqual(show({ minLevel: "WARN" }), ["slow heartbeat", "segment rolled back", "thread 'main' panicked at src/main.rs:1:1", "process killed"]);
  assert.equal(show({ minLevel: "TRACE" }).length, 6);
});

check("node and target filters", () => {
  assert.equal(show({ nodes: new Set([1]) }).length, 2);
  assert.deepEqual(show({ targets: new Set(["krabka_broker::log"]) }), ["segment rolled back", "process killed"]);
  assert.equal(show({ prefix: "krabka_broker::raft" }).length, 3, "two raft lines and the marker");
  assert.equal(show({ prefix: "krabka_broker::" }).filter((m) => m !== "process killed").length, 4);
});

check("free text searches the message, target and fields", () => {
  assert.deepEqual(show({ query: parseQuery("heartbeat") }), ["slow heartbeat"]);
  assert.deepEqual(show({ query: parseQuery("SEGMENT rolled") }), ["segment rolled back"]);
  assert.deepEqual(show({ query: parseQuery('"elected leader"') }), ["elected leader"]);
  assert.deepEqual(show({ query: parseQuery("term=3") }), ["elected leader"]);
  assert.equal(show({ query: parseQuery("level") }).length, 0, "the key names of the JSON are not searchable text");
});

check("field:value equality", () => {
  assert.deepEqual(show({ query: parseQuery("node:2 level:warn") }), ["process killed"]);
  assert.deepEqual(show({ query: parseQuery("level:error") }), ["segment rolled back", "thread 'main' panicked at src/main.rs:1:1"]);
  assert.deepEqual(show({ query: parseQuery("target:raft") }), ["elected leader", "slow heartbeat"]);
  assert.deepEqual(show({ query: parseQuery("node:node-1") }), ["elected leader", "slow heartbeat"]);
  assert.deepEqual(show({ query: parseQuery("term:3") }), ["elected leader"]);
  assert.deepEqual(show({ query: parseQuery("duration_ms:40") }), ["segment rolled back"]);
  assert.deepEqual(show({ query: parseQuery("node:1 slow") }), ["slow heartbeat"]);
  assert.equal(show({ query: parseQuery("nosuchfield:1") }).length, 0);
});

check("field:>value comparisons (optional extra)", () => {
  assert.deepEqual(show({ query: parseQuery("duration_ms:>100") }), ["slow heartbeat"]);
  assert.deepEqual(show({ query: parseQuery("duration_ms:<=40 node:2") }), ["segment rolled back", "Produce v9"]);
  assert.deepEqual(show({ query: parseQuery("level:>=warn node:1") }), ["slow heartbeat"]);
});

check("matchQuery on one entry", () => {
  assert.equal(matchQuery(all[1], parseQuery("node:1 level:warn target:raft duration_ms:>100"), nodeName), true);
  assert.equal(matchQuery(all[1], parseQuery("node:2"), nodeName), false);
});

check("NDJSON export is the raw lines as shown", () => {
  const shown = filterEntries(all, { minLevel: "ERROR", nodes: new Set(), targets: new Set() }, nodeName);
  const text = toNdjson(shown);
  const lines = text.split("\n");
  assert.equal(lines.at(-1), "");
  assert.equal(lines[0], '{"ts":2.5,"level":"ERROR","target":"krabka_broker::log","message":"segment rolled back","duration_ms":40}');
  assert.equal(lines[1], "thread 'main' panicked at src/main.rs:1:1");
  assert.equal(JSON.parse(lines[2]).kind, "killed", "a marker is a JSON line of its own");
  assert.equal(toNdjson([]), "");
});

// ---- the store ------------------------------------------------------------------------------------

check("counts per level and target follow the lines", () => {
  assert.deepEqual(store.levels, { TRACE: 0, DEBUG: 1, INFO: 1, WARN: 1, ERROR: 2 }, "markers are not counted");
  assert.equal(store.targets.get("krabka_broker::raft"), 2);
  assert.deepEqual(store.attention, { warn: 1, error: 2 });
});

check("per node history is bounded and the oldest line goes first", () => {
  const s = new LogStore({ perNode: 3, total: 100 });
  for (let i = 1; i <= 5; i++) s.add(1, "stderr", `{"level":"INFO","target":"t","message":"m${i}"}`, i);
  s.add(2, "stderr", '{"level":"ERROR","target":"u","message":"other"}', 6);
  assert.deepEqual(s.entries().map((e) => e.message), ["m3", "m4", "m5", "other"]);
  assert.equal(s.levels.INFO, 3);
  assert.equal(s.targets.get("t"), 3);
});

check("total history is bounded across nodes", () => {
  const s = new LogStore({ perNode: 10, total: 6 });
  for (let i = 1; i <= 5; i++) {
    s.add(1, "stderr", `{"level":"INFO","target":"a","message":"a${i}"}`, i);
    s.add(2, "stderr", `{"level":"WARN","target":"b","message":"b${i}"}`, i);
  }
  const held = s.entries().map((e) => e.message);
  assert.equal(held.length, 6);
  assert.deepEqual(held, ["a3", "b3", "a4", "b4", "a5", "b5"]);
  assert.equal(s.levels.INFO + s.levels.WARN, 6);
});

check("a store at the spec's limits keeps 20,000 lines and stays quick", () => {
  const s = new LogStore();
  const t0 = performance.now();
  for (let i = 0; i < 30_000; i++) s.add((i % 5) + 1, "stderr", `{"ts":${i / 1000},"level":"INFO","target":"t${i % 7}","message":"line ${i}"}`, i, 0);
  assert.equal(s.size, 20_000);
  assert.equal(s.entries().length, 20_000);
  assert.ok(performance.now() - t0 < 3000, "30,000 lines in under 3 s");
  const t1 = performance.now();
  const hits = filterEntries(s.entries(), { query: parseQuery("line 1999 node:3") }, nodeName);
  assert.ok(hits.length >= 0 && performance.now() - t1 < 500, "a search over 20,000 lines in under half a second");
});

check("subscribers hear every change; since() finds what is new; seq survives Clear", () => {
  const s = new LogStore();
  let heard = 0;
  const off = s.subscribe(() => heard++);
  s.add(1, "stderr", '{"level":"ERROR","message":"a"}', 1);
  const mark = s.seq;
  s.add(1, "stderr", '{"level":"INFO","message":"b"}', 2);
  s.add(1, "stderr", '{"level":"ERROR","message":"c"}', 3);
  assert.deepEqual(s.since(mark, "ERROR").map((e) => e.message), ["c"]);
  s.clear();
  assert.equal(s.size, 0);
  assert.ok(s.seq >= 3);
  off();
  s.add(1, "stderr", '{"level":"INFO","message":"d"}', 4);
  assert.equal(heard, 4);
});

// ---- level settings -------------------------------------------------------------------------------

check("level settings are kept per scenario, with a node overriding the default", () => {
  const backing = new Map();
  const storage = { getItem: (k) => backing.get(k) ?? null, setItem: (k, v) => backing.set(k, v) };
  const levels = new LogLevels(storage);
  assert.deepEqual(levels.get("s1"), { default: "", nodes: {} });
  levels.set("s1", "all", "info");
  levels.set("s1", 2, "debug");
  levels.set("s2", "all", "warn");
  assert.equal(levels.directive("s1", 1), "info");
  assert.equal(levels.directive("s1", 2), "debug");
  assert.equal(levels.directive("s2", 2), "warn");
  assert.deepEqual(JSON.parse(backing.get("krabka-lab.loglevels")).s1, { default: "info", nodes: { 2: "debug" } });
  levels.set("s1", "all", "trace");
  assert.equal(levels.directive("s1", 2), "trace", "All brokers replaces single-broker choices");
  backing.set("krabka-lab.loglevels", "{not json");
  assert.deepEqual(new LogLevels(storage).get("s1"), { default: "", nodes: {} }, "damaged storage reads as the default");
});

check("level settings survive a storage that refuses writes", () => {
  const levels = new LogLevels({ getItem: () => null, setItem: () => { throw new Error("quota"); } });
  levels.set("s", "all", "debug");
  assert.equal(levels.directive("s", 1), "debug", "the restart that follows still sees the choice");
});

if (process.exitCode) console.error("check-lab-logs: failed");
else console.log(`check-lab-logs: ${passed} checks passed`);
