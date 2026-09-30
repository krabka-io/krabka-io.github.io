// The lines real brokers logged, kept in this tab for the Logs panel.
//
// `LogStore` holds the last 5,000 lines of each node and at most 20,000 in
// all, oldest first out, with counts per level, node and target kept up to
// date as lines come and go. `LogLevels` keeps the level the reader chose for
// each scenario's brokers (`KRABKA_LOG` of their process).

import { parseLine, levelRank } from "./logparse.js";

export const PER_NODE = 5000;
export const TOTAL = 20_000;
const LEVELS_KEY = "krabka-lab.loglevels";
// Dead lines (trimmed from a node's history) stay in `all` until this many pile up or someone reads it.
const COMPACT_AT = 2000;

export class LogStore {
  constructor({ perNode = PER_NODE, total = TOTAL } = {}) {
    this.perNode = perNode;
    this.total = total;
    this.listeners = new Set();
    this.seq = 0; // never starts over: a reader of `since(seq)` would miss the lines after a Clear
    this.reset();
  }

  reset() {
    this.all = [];
    this.head = 0;
    this.dead = 0;
    this.queues = new Map(); // node id → its live entries, oldest first
    this.levels = { TRACE: 0, DEBUG: 0, INFO: 0, WARN: 0, ERROR: 0 }; // lines only, not markers
    this.targets = new Map(); // target → lines
    this.version = 0;
  }

  /** Calls `fn()` after the store changed; returns the function that stops it. */
  subscribe(fn) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  notify() {
    this.version++;
    for (const fn of this.listeners) fn();
  }

  /**
   * One line a process wrote. `now` is the lab's clock in ms; `base` the lab
   * time the process started at: a line that carries its own `ts` is placed at
   * `base + ts`, anything else at `now`. Returns the entry, or null for an empty line.
   */
  add(node, stream, text, now, base = null) {
    const parsed = parseLine(text);
    if (!parsed) return null;
    const at = parsed.ts != null && base != null ? Math.min(base + parsed.ts * 1000, now) : now;
    return this.push({ node, stream, marker: null, at, ...parsed });
  }

  /**
   * A process lifecycle row (`kind`: started, restarted, killed, exited, ...).
   * It is a JSON line of its own, so it survives Download.
   */
  mark(node, kind, message, { level = "INFO", now, detail = {} } = {}) {
    const fields = { node, kind, ...detail };
    const record = { ts: now / 1000, level, target: "lab::process", message, ...fields };
    return this.push({ node, stream: "lab", marker: kind, at: now, raw: JSON.stringify(record), format: "json", level, target: "lab::process", message, ts: record.ts, fields, record });
  }

  push(entry) {
    entry.seq = ++this.seq;
    this.all.push(entry);
    let queue = this.queues.get(entry.node);
    if (!queue) this.queues.set(entry.node, (queue = []));
    queue.push(entry);
    this.count(entry, 1);
    if (queue.length > this.perNode) this.drop(queue.shift());
    if (this.all.length - this.dead > this.total) {
      // Evict from a moving head, not with shift(): reindexing 20,000 lines
      // per new line would be quadratic. The array is compacted in batches.
      while (this.all[this.head].dead) this.head++;
      const oldest = this.all[this.head++];
      oldest.dead = true;
      this.dead++;
      this.queues.get(oldest.node).shift();
      this.count(oldest, -1);
      if (this.dead > COMPACT_AT) this.compact();
    }
    this.notify();
    return entry;
  }

  drop(entry) {
    entry.dead = true;
    this.dead++;
    this.count(entry, -1);
    if (this.dead > COMPACT_AT) this.compact();
  }

  count(entry, by) {
    if (entry.marker) return;
    this.levels[entry.level] += by;
    if (!entry.target) return;
    const n = (this.targets.get(entry.target) ?? 0) + by;
    if (n > 0) this.targets.set(entry.target, n);
    else this.targets.delete(entry.target);
  }

  compact() {
    if (!this.dead) return;
    this.all = this.all.filter((e) => !e.dead);
    this.dead = 0;
    this.head = 0;
  }

  /** Every live entry, oldest first. The array is the store's own: do not change it. */
  entries() {
    this.compact();
    return this.all;
  }

  get size() {
    return this.all.length - this.dead;
  }

  /** WARN and ERROR lines held, for the tab's badge. */
  get attention() {
    return { warn: this.levels.WARN, error: this.levels.ERROR };
  }

  /** The lines of one level or above that a node wrote since `seq`. */
  since(seq, minLevel = "WARN") {
    const min = levelRank(minLevel);
    const out = [];
    for (let i = this.all.length - 1; i >= 0 && this.all[i].seq > seq; i--) {
      const e = this.all[i];
      if (!e.dead && !e.marker && levelRank(e.level) >= min) out.push(e);
    }
    return out.reverse();
  }

  clear() {
    this.reset();
    this.notify();
  }
}

// ---- the level the reader chose --------------------------------------------------------------------

/**
 * `{ default, nodes: { <node id>: <directive> } }` per scenario id, in
 * localStorage. A directive is a `KRABKA_LOG` value; "" is the broker's own
 * default. The settings are not part of a node's config: editing the config
 * wipes the node's disk, and a log level should not.
 */
export class LogLevels {
  constructor(storage = globalThis.localStorage) {
    this.storage = storage;
    // What the last `set` wrote, for a browser that refuses storage: the restart that follows must still see it.
    this.cache = {};
  }

  read() {
    if (this.unsaved) return this.cache;
    try {
      const all = JSON.parse(this.storage?.getItem(LEVELS_KEY) ?? "{}");
      return all && typeof all === "object" && !Array.isArray(all) ? all : {};
    } catch {
      return this.cache;
    }
  }

  get(scenarioId) {
    const s = this.read()[scenarioId];
    const nodes = s && typeof s.nodes === "object" && s.nodes && !Array.isArray(s.nodes) ? s.nodes : {};
    return { default: typeof s?.default === "string" ? s.default : "", nodes: { ...nodes } };
  }

  /** The directive node `nodeId` of the scenario starts with. */
  directive(scenarioId, nodeId) {
    const s = this.get(scenarioId);
    return typeof s.nodes[nodeId] === "string" ? s.nodes[nodeId] : s.default;
  }

  /** `scope` "all" sets every broker (and forgets single-broker choices); a node id sets that broker. */
  set(scenarioId, scope, directive) {
    const s = this.get(scenarioId);
    if (scope === "all") {
      s.default = directive;
      s.nodes = {};
    } else {
      s.nodes[scope] = directive;
    }
    const all = this.read();
    all[scenarioId] = s;
    this.cache = all;
    try {
      this.storage?.setItem(LEVELS_KEY, JSON.stringify(all));
    } catch {
      // Storage refused: the cache carries the choice until the page closes.
      this.unsaved = true;
    }
    return s;
  }
}
