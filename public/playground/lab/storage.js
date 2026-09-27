// Durable state in the browser: the IndexedDB database behind the lab.
//
// A node records every change to its durable state (a broker's partition
// logs, the controller log, a registry's schemas, the echo node's frame
// counter) as a `DurableOp`; the world hands them to the page through
// `drainDurable()` after every step. This module keeps two copies:
//
// - the **mirror**, an in-memory image per node, folded from the images the
//   world was loaded with and every op since, whatever the persistence
//   setting. It is always what the live nodes would restore from.
// - the **store**, IndexedDB, written in op order, all the writes of one
//   animation frame in one transaction.
//
// While "Persist to this browser" is off the ops fold into the mirror and are
// not written. Turning it back on replaces the scenario's stored records with
// the mirror in one transaction, clear then write, queued ahead of every
// later op, so the store never has the gaps the skipped ops would leave. A
// node whose stored data was forgotten is not written again until it
// restarts from nothing (its next `clear_all`) or the store is replaced, for
// the same reason: its next op alone would be a partial image.
//
// The fold follows `DurableImage::apply` in `playground/src/lab/net.rs`.
//
// Database `krabka-lab`, version 1:
//   logs      keyPath [scenario, node, store, index]  { bytes: Uint8Array }
//   kv        keyPath [scenario, node, store, key]    { value: Uint8Array }
//   scenarios keyPath id                              { name, json, updated }
//
// Every record is keyed by the scenario id, so two scenarios never share
// state and forgetting one is a range delete. Nothing here leaves the
// browser; there is no server.

export const DB_NAME = "krabka-lab";
export const DB_VERSION = 1;
const PERSIST_KEY = "krabka-lab.persist";

// Array keys compare element-wise and a shorter prefix sorts first; numbers
// sort before strings and arrays sort after everything else, so these two
// bounds bracket every value in one key position.
const LOWEST = -Infinity;
const HIGHEST = [];

export function storageAvailable() {
  try {
    return typeof indexedDB !== "undefined" && indexedDB != null;
  } catch {
    return false;
  }
}

export function bytesToBase64(bytes) {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  return btoa(binary);
}

export function base64ToBytes(text) {
  const binary = atob(text || "");
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

// ---- the mirror ---------------------------------------------------------------------------

// A drained op with its bytes decoded once, for both the mirror and the store.
function decodeOp(op) {
  const out = { ...op, node: Number(op.node) };
  if (op.op === "append") {
    out.index = Number(op.index);
    out.bytes = base64ToBytes(op.bytes);
  } else if (op.op === "put") {
    out.key = String(op.key);
    out.value = base64ToBytes(op.value);
  } else if (op.op === "truncate_before" || op.op === "truncate_from") {
    out.index = Number(op.index);
  } else if (op.op === "delete") {
    out.key = String(op.key);
  }
  return out;
}

// The first position in `log` (ascending by index) whose index is at least `index`.
function lowerBound(log, index) {
  let lo = 0;
  let hi = log.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (log[mid].index < index) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

// One `DurableImage` per node: `logs` maps a store to its entries in
// ascending index order, `kv` maps a store to its keys. Bytes are Uint8Arrays
// and never mutated, only replaced, so a snapshot can share them.
export class DurableMirror {
  constructor() {
    this.nodes = new Map();
  }

  // From images as `loadImages` returns them and `loadScenarioWithState`
  // takes them: `{"<node>": {logs: {store: [{index, bytes}]}, kv: {store: {key: value}}}}`, base64 bytes.
  static fromImages(images) {
    const m = new DurableMirror();
    for (const [node, image] of Object.entries(images || {})) {
      const img = m.image(Number(node));
      for (const [store, entries] of Object.entries(image?.logs || {})) {
        const log = entries.map((e) => ({ index: Number(e.index), bytes: base64ToBytes(e.bytes) }));
        log.sort((a, b) => a.index - b.index);
        img.logs.set(store, log);
      }
      for (const [store, kv] of Object.entries(image?.kv || {})) {
        img.kv.set(store, new Map(Object.entries(kv).map(([k, v]) => [k, base64ToBytes(v)])));
      }
    }
    return m;
  }

  image(node) {
    let img = this.nodes.get(node);
    if (!img) {
      img = { logs: new Map(), kv: new Map() };
      this.nodes.set(node, img);
    }
    return img;
  }

  // Fold one decoded op, the way `DurableImage::apply` does.
  apply(op) {
    const img = this.image(op.node);
    switch (op.op) {
      case "append": {
        let log = img.logs.get(op.store);
        if (!log) {
          log = [];
          img.logs.set(op.store, log);
        }
        const entry = { index: op.index, bytes: op.bytes };
        if (!log.length || log[log.length - 1].index < op.index) log.push(entry);
        else {
          const at = lowerBound(log, op.index);
          if (at < log.length && log[at].index === op.index) log[at] = entry;
          else log.splice(at, 0, entry);
        }
        break;
      }
      case "truncate_before": {
        const log = img.logs.get(op.store);
        if (log) log.splice(0, lowerBound(log, op.index));
        break;
      }
      case "truncate_from": {
        const log = img.logs.get(op.store);
        if (log) log.length = lowerBound(log, op.index);
        break;
      }
      case "put": {
        let kv = img.kv.get(op.store);
        if (!kv) {
          kv = new Map();
          img.kv.set(op.store, kv);
        }
        kv.set(op.key, op.value);
        break;
      }
      case "delete":
        img.kv.get(op.store)?.delete(op.key);
        break;
      case "clear":
        img.logs.delete(op.store);
        img.kv.delete(op.store);
        break;
      case "clear_all":
        img.logs.clear();
        img.kv.clear();
        break;
      default:
        // An op this page does not know changes nothing it can mirror.
        break;
    }
  }

  // The IndexedDB records of the nodes `nodes` selects (null: every node).
  records(scenario, nodes) {
    const logs = [];
    const kv = [];
    for (const [node, img] of this.nodes) {
      if (nodes && !nodes.has(node)) continue;
      for (const [store, log] of img.logs) for (const e of log) logs.push({ scenario, node, store, index: e.index, bytes: e.bytes });
      for (const [store, map] of img.kv) for (const [key, value] of map) kv.push({ scenario, node, store, key, value });
    }
    return { logs, kv };
  }

  // Back to the base64 image form, for `loadScenarioWithState`.
  toImages(nodes) {
    const out = {};
    for (const [node, img] of this.nodes) {
      if (nodes && !nodes.has(node)) continue;
      const logs = {};
      for (const [store, log] of img.logs) logs[store] = log.map((e) => ({ index: e.index, bytes: bytesToBase64(e.bytes) }));
      const kv = {};
      for (const [store, map] of img.kv) {
        const obj = {};
        for (const [key, value] of map) obj[key] = bytesToBase64(value);
        kv[store] = obj;
      }
      out[String(node)] = { logs, kv };
    }
    return out;
  }
}

// ---- IndexedDB helpers -------------------------------------------------------------------

function request(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error("IndexedDB request failed"));
  });
}

function done(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error || new Error("IndexedDB transaction failed"));
    tx.onabort = () => reject(tx.error || new Error("IndexedDB transaction aborted"));
  });
}

// Walk a cursor over `range`, calling `fn(value)` for each record.
function each(store, range, fn) {
  return new Promise((resolve, reject) => {
    const req = store.openCursor(range);
    req.onsuccess = () => {
      const cursor = req.result;
      if (!cursor) {
        resolve();
        return;
      }
      fn(cursor.value);
      cursor.continue();
    };
    req.onerror = () => reject(req.error || new Error("cursor failed"));
  });
}

const scenarioRange = (s) => IDBKeyRange.bound([s, LOWEST], [s, HIGHEST]);
const nodeRange = (s, n) => IDBKeyRange.bound([s, n, LOWEST], [s, n, HIGHEST]);
const storeRange = (s, n, store) => IDBKeyRange.bound([s, n, store, LOWEST], [s, n, store, HIGHEST]);

// Delete a scenario's records: every node's (`nodes` null) or the listed ones'.
function deleteScope(logs, kv, scenario, nodes) {
  if (!nodes) {
    logs.delete(scenarioRange(scenario));
    kv.delete(scenarioRange(scenario));
    return;
  }
  for (const n of nodes) {
    logs.delete(nodeRange(scenario, n));
    kv.delete(nodeRange(scenario, n));
  }
}

// ---- the store ------------------------------------------------------------------------------

export class LabStorage {
  // hooks: onError(err, context)
  constructor(hooks) {
    this.hooks = hooks;
    this.db = null;
    this.opening = null;
    this.available = storageAvailable();
    this.mirror = new DurableMirror();
    // Nodes whose stored data was forgotten while they kept running.
    this.forgotten = new Set();
    // Jobs not yet written, in order: `ops`, `replace` and `delete`.
    this.pending = [];
    this.scheduled = false;
    this.chain = Promise.resolve();
    this.dropped = 0;
    this.writes = 0;
    let persist = true;
    try {
      persist = localStorage.getItem(PERSIST_KEY) !== "0";
    } catch {
      // No localStorage: keep the default.
    }
    this.persist = this.available && persist;
  }

  // A new world was built from `images` (base64, as `loadImages` returns
  // them); the mirror starts from exactly that.
  resetMirror(images) {
    this.mirror = DurableMirror.fromImages(images);
    this.forgotten.clear();
  }

  // The mirror as `loadScenarioWithState` takes it: the live nodes' durable
  // state, whatever the persistence setting.
  mirrorImages() {
    return this.mirror.toImages();
  }

  // Turn persistence on or off. Turning it on stores the mirror for
  // `scenarioId` (see `syncFromMirror`), so the ops skipped while it was off
  // leave no gap. `nodes`: null for every node, or the ids this tab hosts.
  setPersist(on, scenarioId = "", nodes = null) {
    const was = this.persist;
    this.persist = Boolean(on) && this.available;
    try {
      localStorage.setItem(PERSIST_KEY, this.persist ? "1" : "0");
    } catch {
      // Not remembered; the toggle still applies to this page.
    }
    if (this.persist && !was && scenarioId) this.syncFromMirror(scenarioId, nodes);
  }

  // Replace what the store holds for `scenarioId` with the mirror, in one
  // transaction queued ahead of every later op. `nodes`: null replaces the
  // whole scenario; a list replaces only those nodes' records (another tab in
  // this browser may be storing the rest).
  syncFromMirror(scenarioId, nodes = null) {
    if (!scenarioId || !this.persist) return;
    const scope = nodes ? new Set(nodes.map(Number)) : null;
    // A snapshot now: ops drained after this point are written after it.
    const records = this.mirror.records(scenarioId, scope);
    this.pending.push({ kind: "replace", scenarioId, nodes: scope ? [...scope] : null, records });
    if (scope) for (const n of scope) this.forgotten.delete(n);
    else this.forgotten.clear();
    this.schedule();
  }

  // Open the database once; resolves to null when IndexedDB is unavailable.
  open() {
    if (this.db) return Promise.resolve(this.db);
    if (!this.available) return Promise.resolve(null);
    if (this.opening) return this.opening;
    this.opening = new Promise((resolve) => {
      let req;
      try {
        req = indexedDB.open(DB_NAME, DB_VERSION);
      } catch (err) {
        this.available = false;
        this.hooks.onError(err, "open storage");
        resolve(null);
        return;
      }
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains("logs")) db.createObjectStore("logs", { keyPath: ["scenario", "node", "store", "index"] });
        if (!db.objectStoreNames.contains("kv")) db.createObjectStore("kv", { keyPath: ["scenario", "node", "store", "key"] });
        if (!db.objectStoreNames.contains("scenarios")) db.createObjectStore("scenarios", { keyPath: "id" });
      };
      req.onsuccess = () => {
        this.db = req.result;
        this.db.onversionchange = () => {
          this.db.close();
          this.db = null;
          this.opening = null;
        };
        resolve(this.db);
      };
      req.onerror = () => {
        this.available = false;
        this.hooks.onError(req.error || new Error("IndexedDB refused to open"), "open storage");
        resolve(null);
      };
      req.onblocked = () => resolve(null);
    });
    return this.opening;
  }

  // ---- writing ops --------------------------------------------------------------------------

  // Fold the drained ops into the mirror and queue the ones to store. All the
  // ops of one animation frame land in one transaction, in order, without
  // blocking the frame.
  queueOps(scenarioId, ops) {
    if (!ops.length) return;
    const writable = [];
    for (const raw of ops) {
      const op = decodeOp(raw);
      this.mirror.apply(op);
      const store = this.persist && scenarioId;
      if (op.op === "clear_all") {
        // A node that restarts from nothing is consistent again from here on.
        this.forgotten.delete(op.node);
        if (store) writable.push(op);
      } else if (store && !this.forgotten.has(op.node)) {
        writable.push(op);
      } else {
        this.dropped += 1;
      }
    }
    if (!writable.length) return;
    this.pending.push({ kind: "ops", scenarioId, ops: writable });
    this.schedule();
  }

  schedule() {
    if (this.scheduled) return;
    this.scheduled = true;
    setTimeout(() => this.commitPending(), 0);
  }

  commitPending() {
    this.scheduled = false;
    const jobs = this.pending;
    this.pending = [];
    if (!jobs.length) return;
    this.chain = this.chain.then(() => this.writeJobs(jobs)).catch((err) => this.hooks.onError(err, "write durable state"));
  }

  async writeJobs(jobs) {
    const db = await this.open();
    if (!db) return;
    const tx = db.transaction(["logs", "kv"], "readwrite");
    const logs = tx.objectStore("logs");
    const kv = tx.objectStore("kv");
    for (const job of jobs) {
      const s = job.scenarioId;
      if (job.kind === "replace") {
        deleteScope(logs, kv, s, job.nodes);
        for (const r of job.records.logs) logs.put(r);
        for (const r of job.records.kv) kv.put(r);
        this.writes += job.records.logs.length + job.records.kv.length;
        continue;
      }
      if (job.kind === "delete") {
        deleteScope(logs, kv, s, job.nodes);
        continue;
      }
      for (const op of job.ops) {
        const n = op.node;
        switch (op.op) {
          case "append":
            logs.put({ scenario: s, node: n, store: op.store, index: op.index, bytes: op.bytes });
            break;
          case "truncate_before":
            logs.delete(IDBKeyRange.bound([s, n, op.store, LOWEST], [s, n, op.store, op.index], false, true));
            break;
          case "truncate_from":
            logs.delete(IDBKeyRange.bound([s, n, op.store, op.index], [s, n, op.store, HIGHEST], false, true));
            break;
          case "put":
            kv.put({ scenario: s, node: n, store: op.store, key: op.key, value: op.value });
            break;
          case "delete":
            kv.delete([s, n, op.store, op.key]);
            break;
          case "clear":
            logs.delete(storeRange(s, n, op.store));
            kv.delete(storeRange(s, n, op.store));
            break;
          case "clear_all":
            logs.delete(nodeRange(s, n));
            kv.delete(nodeRange(s, n));
            break;
          default:
            // An op this page does not know is skipped, not fatal.
            break;
        }
        this.writes += 1;
      }
    }
    await done(tx);
  }

  // Wait for every queued write to land.
  async flush() {
    if (this.pending.length) this.commitPending();
    await this.chain;
  }

  // ---- reading images --------------------------------------------------------------------------

  // Fold every stored record of a scenario into `{ "<node>": DurableImage }`.
  async loadImages(scenarioId) {
    const images = {};
    if (!scenarioId) return images;
    await this.flush();
    const db = await this.open();
    if (!db) return images;
    const tx = db.transaction(["logs", "kv"], "readonly");
    const range = scenarioRange(scenarioId);
    const image = (node) => {
      const key = String(node);
      if (!images[key]) images[key] = { logs: {}, kv: {} };
      return images[key];
    };
    // The cursor yields keys in order, so log entries arrive sorted by index.
    await each(tx.objectStore("logs"), range, (r) => {
      const img = image(r.node);
      (img.logs[r.store] ||= []).push({ index: r.index, bytes: bytesToBase64(r.bytes) });
    });
    await each(tx.objectStore("kv"), range, (r) => {
      const img = image(r.node);
      (img.kv[r.store] ||= {})[r.key] = bytesToBase64(r.value);
    });
    await done(tx);
    return images;
  }

  // Bytes and entry counts per node and in total.
  async usage(scenarioId) {
    const out = { total: { bytes: 0, logEntries: 0, kvEntries: 0 }, nodes: {} };
    if (!scenarioId) return out;
    const db = await this.open();
    if (!db) return out;
    const tx = db.transaction(["logs", "kv"], "readonly");
    const range = scenarioRange(scenarioId);
    const node = (id) => (out.nodes[id] ||= { bytes: 0, logEntries: 0, kvEntries: 0, stores: new Set() });
    await each(tx.objectStore("logs"), range, (r) => {
      const n = node(r.node);
      const size = r.bytes ? r.bytes.byteLength : 0;
      n.bytes += size;
      n.logEntries += 1;
      n.stores.add(r.store);
      out.total.bytes += size;
      out.total.logEntries += 1;
    });
    await each(tx.objectStore("kv"), range, (r) => {
      const n = node(r.node);
      const size = r.value ? r.value.byteLength : 0;
      n.bytes += size;
      n.kvEntries += 1;
      n.stores.add(r.store);
      out.total.bytes += size;
      out.total.kvEntries += 1;
    });
    await done(tx);
    for (const n of Object.values(out.nodes)) n.stores = n.stores.size;
    return out;
  }

  // Drop a node's stored data. The node keeps running, so it is not written
  // again until it restarts from nothing or the store is replaced; its next
  // op alone would be a partial image.
  async forgetNode(scenarioId, nodeId) {
    if (!scenarioId) return;
    const n = Number(nodeId);
    this.forgotten.add(n);
    this.pending.push({ kind: "delete", scenarioId, nodes: [n] });
    await this.flush();
  }

  // Drop the stored data of the scenario's nodes: `nodes` lists them (they
  // stop being written, as in `forgetNode`); `whole` deletes every record of
  // the scenario rather than only the listed nodes'.
  async forgetScenario(scenarioId, nodes = [], whole = true) {
    if (!scenarioId) return;
    for (const n of nodes) this.forgotten.add(Number(n));
    this.pending.push({ kind: "delete", scenarioId, nodes: whole ? null : nodes.map(Number) });
    await this.flush();
  }

  // ---- saved scenarios ----------------------------------------------------------------------------

  async saveScenario(doc) {
    const db = await this.open();
    if (!db || !doc.id) return false;
    const tx = db.transaction("scenarios", "readwrite");
    tx.objectStore("scenarios").put({ id: doc.id, name: doc.name || "", json: JSON.stringify(doc), updated: Date.now() });
    await done(tx);
    return true;
  }

  async listScenarios() {
    const db = await this.open();
    if (!db) return [];
    const tx = db.transaction("scenarios", "readonly");
    const rows = await request(tx.objectStore("scenarios").getAll());
    await done(tx);
    return rows.map((r) => ({ id: r.id, name: r.name, updated: r.updated })).sort((a, b) => b.updated - a.updated);
  }

  async loadScenario(id) {
    const db = await this.open();
    if (!db) return null;
    const tx = db.transaction("scenarios", "readonly");
    const row = await request(tx.objectStore("scenarios").get(id));
    await done(tx);
    return row ? JSON.parse(row.json) : null;
  }

  // Delete a saved scenario: its document and every stored record.
  async deleteScenario(id) {
    if (!id) return;
    this.pending.push({ kind: "delete", scenarioId: id, nodes: null });
    await this.flush();
    const db = await this.open();
    if (!db) return;
    const tx = db.transaction("scenarios", "readwrite");
    tx.objectStore("scenarios").delete(id);
    await done(tx);
  }
}
