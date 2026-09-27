// Durable state in the browser: the IndexedDB database behind the lab.
//
// A node records every change to its durable state (a broker's partition
// logs, the controller log, a registry's schemas, the echo node's frame
// counter) as a `DurableOp`; the world hands them to the page through
// `drainDurable()` after every step. This module writes them to IndexedDB in
// order, one transaction per animation frame, and folds them back into the
// `DurableImage` per node that `loadScenarioWithState` hands to the nodes
// when a scenario is reopened after a reload. The fold follows
// `DurableImage::apply` in `playground/src/lab/net.rs`.
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

export class LabStorage {
  // hooks: onError(err, context)
  constructor(hooks) {
    this.hooks = hooks;
    this.db = null;
    this.opening = null;
    this.available = storageAvailable();
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

  setPersist(on) {
    this.persist = Boolean(on) && this.available;
    try {
      localStorage.setItem(PERSIST_KEY, this.persist ? "1" : "0");
    } catch {
      // Not remembered; the toggle still applies to this page.
    }
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

  // Queue the drained ops of `scenarioId`. They are written in order, all the
  // ops of one animation frame in one transaction, without blocking the
  // frame. With persistence off the ops are dropped.
  queueOps(scenarioId, ops) {
    if (!ops.length) return;
    if (!this.persist || !scenarioId) {
      this.dropped += ops.length;
      return;
    }
    this.pending.push({ scenarioId, ops });
    if (!this.scheduled) {
      this.scheduled = true;
      setTimeout(() => this.commitPending(), 0);
    }
  }

  commitPending() {
    this.scheduled = false;
    const batch = this.pending;
    this.pending = [];
    if (!batch.length) return;
    this.chain = this.chain
      .then(() => this.writeBatch(batch))
      .catch((err) => this.hooks.onError(err, "write durable state"));
  }

  async writeBatch(batch) {
    const db = await this.open();
    if (!db) return;
    const tx = db.transaction(["logs", "kv"], "readwrite");
    const logs = tx.objectStore("logs");
    const kv = tx.objectStore("kv");
    for (const { scenarioId: s, ops } of batch) {
      for (const op of ops) {
        const n = Number(op.node);
        switch (op.op) {
          case "append":
            logs.put({ scenario: s, node: n, store: op.store, index: Number(op.index), bytes: base64ToBytes(op.bytes) });
            break;
          case "truncate_before":
            logs.delete(IDBKeyRange.bound([s, n, op.store, LOWEST], [s, n, op.store, Number(op.index)], false, true));
            break;
          case "truncate_from":
            logs.delete(IDBKeyRange.bound([s, n, op.store, Number(op.index)], [s, n, op.store, HIGHEST], false, true));
            break;
          case "put":
            kv.put({ scenario: s, node: n, store: op.store, key: String(op.key), value: base64ToBytes(op.value) });
            break;
          case "delete":
            kv.delete([s, n, op.store, String(op.key)]);
            break;
          case "clear":
            logs.delete(IDBKeyRange.bound([s, n, op.store, LOWEST], [s, n, op.store, HIGHEST]));
            kv.delete(IDBKeyRange.bound([s, n, op.store, LOWEST], [s, n, op.store, HIGHEST]));
            break;
          case "clear_all":
            logs.delete(IDBKeyRange.bound([s, n, LOWEST], [s, n, HIGHEST]));
            kv.delete(IDBKeyRange.bound([s, n, LOWEST], [s, n, HIGHEST]));
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
    const db = await this.open();
    if (!db) return images;
    const tx = db.transaction(["logs", "kv"], "readonly");
    const range = IDBKeyRange.bound([scenarioId, LOWEST], [scenarioId, HIGHEST]);
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
    const range = IDBKeyRange.bound([scenarioId, LOWEST], [scenarioId, HIGHEST]);
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

  async forgetNode(scenarioId, nodeId) {
    const db = await this.open();
    if (!db || !scenarioId) return;
    await this.flush();
    const tx = db.transaction(["logs", "kv"], "readwrite");
    const n = Number(nodeId);
    tx.objectStore("logs").delete(IDBKeyRange.bound([scenarioId, n, LOWEST], [scenarioId, n, HIGHEST]));
    tx.objectStore("kv").delete(IDBKeyRange.bound([scenarioId, n, LOWEST], [scenarioId, n, HIGHEST]));
    await done(tx);
  }

  // Drop every record of the scenario's nodes; the saved document stays
  // unless `andDocument` is set.
  async forgetScenario(scenarioId, andDocument = false) {
    const db = await this.open();
    if (!db || !scenarioId) return;
    await this.flush();
    const stores = andDocument ? ["logs", "kv", "scenarios"] : ["logs", "kv"];
    const tx = db.transaction(stores, "readwrite");
    const range = IDBKeyRange.bound([scenarioId, LOWEST], [scenarioId, HIGHEST]);
    tx.objectStore("logs").delete(range);
    tx.objectStore("kv").delete(range);
    if (andDocument) tx.objectStore("scenarios").delete(scenarioId);
    await done(tx);
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

  async deleteScenario(id) {
    await this.forgetScenario(id, true);
  }
}
