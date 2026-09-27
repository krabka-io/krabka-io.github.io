// Volumes: guest file systems persisted to IndexedDB (database `krabka-wasi`,
// separate from the lab's own `krabka-lab`).
//
// Stores (all keys out of line):
//   nodes    [volume, path]        -> { type: "dir"|"file", ino, size, atime, mtime }  (times: ns as decimal strings)
//   chunks   [volume, ino, index]  -> Uint8Array (bytes [index * PAGE_SIZE, ...), at most PAGE_SIZE)
//   volumes  volume                -> { nextIno, created, updated }
//
// Paths are relative to the volume root ("node-1/topic-0/0000.log"); the root
// itself has no record. Times are nanoseconds since the Unix epoch, stored as
// decimal strings.
// Chunks are keyed by inode number, not path, so a rename moves node records
// only. A file may be sparse: missing chunks, and bytes past a short chunk,
// read as zeros.
//
// The worker journals mutations (fs.js); `VolumeWriter` applies each batch
// in order, in one readwrite transaction, coalescing batches that arrive while
// a transaction is running. Durability is write-behind: `fd_sync` returns at
// once, and a batch is durable when its transaction completes.
//
// A process holds a Web Lock on its volume while it runs, so two tabs cannot
// write the same volume; `forget` and `importVolume` refuse a volume in use.

import { PAGE_SIZE } from "./protocol.js";
import { decodeTar, encodeTar } from "./tar.js";

export const DB_NAME = "krabka-wasi";
export const DB_VERSION = 1;
const NODES = "nodes";
const CHUNKS = "chunks";
const VOLUMES = "volumes";
const LOCK_PREFIX = "krabka-wasi:volume:";

let opening = null;

export function storageAvailable() {
  try {
    return typeof indexedDB !== "undefined" && indexedDB !== null;
  } catch {
    return false;
  }
}

/** Opens (and on first use creates) the database. */
export function openDb() {
  if (!storageAvailable()) return Promise.reject(new Error("IndexedDB is unavailable in this browser context"));
  opening ??= new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      for (const name of [NODES, CHUNKS, VOLUMES]) if (!db.objectStoreNames.contains(name)) db.createObjectStore(name);
    };
    request.onsuccess = () => {
      const db = request.result;
      db.onversionchange = () => {
        db.close();
        opening = null;
      };
      resolve(db);
    };
    request.onerror = () => {
      opening = null;
      reject(request.error ?? new Error(`cannot open IndexedDB ${DB_NAME}`));
    };
    request.onblocked = () => reject(new Error(`IndexedDB ${DB_NAME} is blocked by another tab holding an older version`));
  });
  return opening;
}

function result(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function completion(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction failed"));
    tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
  });
}

function checkVolume(volume) {
  if (typeof volume !== "string" || volume.length === 0) throw new TypeError(`a volume id is a non-empty string, not ${JSON.stringify(volume)}`);
}

const volumeRange = (volume) => IDBKeyRange.bound([volume], [volume, []]);
const inodeRange = (volume, ino, from = 0) => IDBKeyRange.bound([volume, ino, from], [volume, ino, []]);
/** Every path strictly below directory `path` ("/" sorts right before "0"). */
const belowRange = (volume, path) => IDBKeyRange.bound([volume, `${path}/`], [volume, `${path}0`], false, true);

// ---- locks ----------------------------------------------------------------------------------

const heldInPage = new Set();

/**
 * Takes the volume's exclusive lock, across tabs where the Web Locks API
 * exists. Resolves to a release function; rejects when the volume is in use.
 */
export function lockVolume(volume) {
  checkVolume(volume);
  const name = LOCK_PREFIX + volume;
  if (typeof navigator === "undefined" || !navigator.locks) {
    if (heldInPage.has(name)) return Promise.reject(new Error(`volume ${volume} is in use`));
    heldInPage.add(name);
    return Promise.resolve(() => heldInPage.delete(name));
  }
  return new Promise((resolve, reject) => {
    navigator.locks
      .request(name, { ifAvailable: true }, (lock) => {
        if (!lock) {
          reject(new Error(`volume ${volume} is in use by a running process (in this tab or another)`));
          return undefined;
        }
        return new Promise((release) => resolve(() => release()));
      })
      .catch(reject);
  });
}

async function withVolumeFree(volume, fn) {
  const release = await lockVolume(volume);
  try {
    return await fn();
  } finally {
    release();
  }
}

// ---- reading ----------------------------------------------------------------------------------

/**
 * The stored image of a volume, as the worker loads it before `_start`:
 * `{ nextIno, nodes: [{path, type, ino, size, atime, mtime}], chunks: [{ino, index, bytes}] }`.
 */
export async function loadImage(volume) {
  checkVolume(volume);
  const db = await openDb();
  const tx = db.transaction([NODES, CHUNKS, VOLUMES], "readonly");
  const nodes = tx.objectStore(NODES);
  const chunks = tx.objectStore(CHUNKS);
  const range = volumeRange(volume);
  const [nodeKeys, nodeValues, chunkKeys, chunkValues, meta] = await Promise.all([
    result(nodes.getAllKeys(range)),
    result(nodes.getAll(range)),
    result(chunks.getAllKeys(range)),
    result(chunks.getAll(range)),
    result(tx.objectStore(VOLUMES).get(volume)),
  ]);
  return {
    nextIno: meta?.nextIno ?? 2,
    nodes: nodeKeys.map((key, i) => ({ path: key[1], ...nodeValues[i] })),
    chunks: chunkKeys.map((key, i) => ({ ino: key[1], index: key[2], bytes: chunkValues[i] })),
  };
}

/** Assembles a file from its chunks (holes as zeros). */
function assemble(size, pieces) {
  const out = new Uint8Array(size);
  for (const { index, bytes } of pieces) {
    const at = index * PAGE_SIZE;
    if (at >= size) continue;
    out.set(bytes.subarray(0, Math.min(bytes.length, size - at)), at);
  }
  return out;
}

/** A stored file's bytes (as last committed), or null when the path is missing or a directory. */
export async function readVolumeFile(volume, path) {
  checkVolume(volume);
  const key = path.replace(/^\/+|\/+$/g, "");
  const db = await openDb();
  const tx = db.transaction([NODES, CHUNKS], "readonly");
  const node = await result(tx.objectStore(NODES).get([volume, key]));
  if (!node || node.type !== "file") return null;
  const range = inodeRange(volume, node.ino);
  const chunks = tx.objectStore(CHUNKS);
  const [keys, values] = await Promise.all([result(chunks.getAllKeys(range)), result(chunks.getAll(range))]);
  return assemble(node.size, keys.map((k, i) => ({ index: k[2], bytes: values[i] })));
}

/** The volumes this origin stores: `[{ id, nextIno, created, updated }]`. */
export async function listVolumes() {
  const db = await openDb();
  const store = db.transaction(VOLUMES, "readonly").objectStore(VOLUMES);
  const [keys, values] = await Promise.all([result(store.getAllKeys()), result(store.getAll())]);
  return keys.map((id, i) => ({ id, ...values[i] }));
}

/**
 * What a volume holds: `{ files, dirs, bytes, storedBytes, chunks }`, where
 * `bytes` sums file sizes and `storedBytes` the chunks actually stored.
 */
export async function usage(volume) {
  checkVolume(volume);
  const db = await openDb();
  const tx = db.transaction([NODES, CHUNKS], "readonly");
  const nodes = await result(tx.objectStore(NODES).getAll(volumeRange(volume)));
  let storedBytes = 0;
  let chunks = 0;
  await new Promise((resolve, reject) => {
    const request = tx.objectStore(CHUNKS).openCursor(volumeRange(volume));
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) {
        resolve();
        return;
      }
      chunks++;
      storedBytes += cursor.value.byteLength;
      cursor.continue();
    };
    request.onerror = () => reject(request.error);
  });
  const files = nodes.filter((n) => n.type === "file");
  return {
    files: files.length,
    dirs: nodes.length - files.length,
    bytes: files.reduce((sum, n) => sum + n.size, 0),
    storedBytes,
    chunks,
  };
}

// ---- writing ----------------------------------------------------------------------------------

async function rename(nodes, chunks, volume, from, to) {
  const source = await result(nodes.get([volume, from]));
  if (!source) return;
  const target = await result(nodes.get([volume, to]));
  if (target) {
    if (target.type === "file") chunks.delete(inodeRange(volume, target.ino));
    nodes.delete([volume, to]);
  }
  if (source.type === "dir") {
    const below = belowRange(volume, from);
    const [keys, values] = await Promise.all([result(nodes.getAllKeys(below)), result(nodes.getAll(below))]);
    nodes.delete(below);
    keys.forEach((key, i) => nodes.put(values[i], [volume, to + key[1].slice(from.length)]));
  }
  nodes.delete([volume, from]);
  nodes.put(source, [volume, to]);
}

/** Applies journal ops (see fs.js) to one volume in one transaction. */
async function apply(db, volume, ops, nextIno) {
  const tx = db.transaction([NODES, CHUNKS, VOLUMES], "readwrite");
  const done = completion(tx);
  const nodes = tx.objectStore(NODES);
  const chunks = tx.objectStore(CHUNKS);
  const volumes = tx.objectStore(VOLUMES);
  try {
    const meta = await result(volumes.get(volume));
    for (const op of ops) {
      switch (op[0]) {
        case "mkdir":
          nodes.put({ type: "dir", ino: op[2], size: 0, atime: op[3], mtime: op[3] }, [volume, op[1]]);
          break;
        case "create":
          nodes.put({ type: "file", ino: op[2], size: 0, atime: op[3], mtime: op[3] }, [volume, op[1]]);
          break;
        case "unlink": {
          const node = await result(nodes.get([volume, op[1]]));
          if (node && node.type === "file") chunks.delete(inodeRange(volume, node.ino));
          nodes.delete([volume, op[1]]);
          break;
        }
        case "rmdir":
          nodes.delete([volume, op[1]]);
          break;
        case "rename":
          await rename(nodes, chunks, volume, op[1], op[2]);
          break;
        case "size":
          chunks.delete(inodeRange(volume, op[1], Math.ceil(op[2] / PAGE_SIZE)));
          break;
        case "page":
          if (op[3].length === 0) chunks.delete([volume, op[1], op[2]]);
          else chunks.put(op[3], [volume, op[1], op[2]]);
          break;
        case "attr":
          nodes.put({ type: op[2], ino: op[3], size: op[4], atime: op[5], mtime: op[6] }, [volume, op[1]]);
          break;
        default:
          throw new Error(`unknown journal op ${JSON.stringify(op[0])}`);
      }
    }
    const now = Date.now();
    volumes.put({ nextIno: Math.max(nextIno, meta?.nextIno ?? 2), created: meta?.created ?? now, updated: now }, volume);
  } catch (err) {
    try {
      tx.abort();
    } catch {
      // Already finished.
    }
    await done.catch(() => {});
    throw err;
  }
  await done;
}

/** Applies a volume's journal batches in order, coalescing whatever queues up during a transaction. */
export class VolumeWriter {
  constructor(volume) {
    checkVolume(volume);
    this.volume = volume;
    this.queue = [];
    this.running = null;
    this.stats = { batches: 0, transactions: 0, ops: 0, commitMs: 0, failures: 0 };
  }

  /** Queues one batch; resolves when it is committed. */
  enqueue(ops, nextIno) {
    return new Promise((resolve, reject) => {
      this.queue.push({ ops, nextIno, resolve, reject });
      this.stats.batches++;
      this.running ??= this.#drain();
    });
  }

  /** Resolves when every queued batch has been applied (or has failed). */
  async idle() {
    while (this.running) await this.running;
  }

  async #drain() {
    try {
      const db = await openDb();
      while (this.queue.length > 0) {
        const batches = this.queue.splice(0);
        const ops = batches.length === 1 ? batches[0].ops : batches.flatMap((b) => b.ops);
        const nextIno = Math.max(...batches.map((b) => b.nextIno ?? 2));
        const started = performance.now();
        try {
          await apply(db, this.volume, ops, nextIno);
          this.stats.transactions++;
          this.stats.ops += ops.length;
          this.stats.commitMs += performance.now() - started;
          for (const batch of batches) batch.resolve();
        } catch (err) {
          this.stats.failures++;
          for (const batch of batches) batch.reject(err);
        }
      }
    } catch (err) {
      for (const batch of this.queue.splice(0)) batch.reject(err);
    } finally {
      this.running = null;
    }
  }
}

/** Deletes a volume and everything in it. Refuses while a process runs on it. */
export async function forget(volume) {
  checkVolume(volume);
  await withVolumeFree(volume, async () => {
    const db = await openDb();
    const tx = db.transaction([NODES, CHUNKS, VOLUMES], "readwrite");
    const done = completion(tx);
    tx.objectStore(NODES).delete(volumeRange(volume));
    tx.objectStore(CHUNKS).delete(volumeRange(volume));
    tx.objectStore(VOLUMES).delete(volume);
    await done;
  });
}

/**
 * The volume as a POSIX tar archive (Uint8Array), from what IndexedDB holds:
 * flush a running process first (`process.flush()`) to include its latest writes.
 */
export async function exportVolume(volume) {
  const image = await loadImage(volume);
  const byIno = new Map();
  for (const chunk of image.chunks) {
    if (!byIno.has(chunk.ino)) byIno.set(chunk.ino, []);
    byIno.get(chunk.ino).push(chunk);
  }
  const nodes = [...image.nodes].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return encodeTar(
    nodes.map((node) => ({
      path: node.path,
      type: node.type,
      mtimeMs: Number(BigInt(node.mtime ?? 0) / 1_000_000n),
      bytes: node.type === "file" ? assemble(node.size, byIno.get(node.ino) ?? []) : undefined,
    })),
  );
}

/**
 * Replaces a volume's contents with a tar archive (from `exportVolume` or any
 * ustar/PAX tar). All-zero chunks are left out, so sparse files stay sparse.
 * Refuses while a process runs on the volume. Resolves to
 * `{ files, dirs, bytes, skipped }`.
 */
export async function importVolume(volume, archive) {
  checkVolume(volume);
  const bytes = archive instanceof Uint8Array ? archive : new Uint8Array(await new Response(archive).arrayBuffer());
  const { entries, skipped } = decodeTar(bytes);
  return withVolumeFree(volume, async () => {
    const db = await openDb();
    const tx = db.transaction([NODES, CHUNKS, VOLUMES], "readwrite");
    const done = completion(tx);
    const nodes = tx.objectStore(NODES);
    const chunks = tx.objectStore(CHUNKS);
    nodes.delete(volumeRange(volume));
    chunks.delete(volumeRange(volume));
    const dirs = new Set();
    let ino = 2;
    let files = 0;
    let total = 0;
    const put = (path, type, size, mtimeMs) => {
      const mtime = String(BigInt(Math.round(mtimeMs)) * 1_000_000n);
      nodes.put({ type, ino, size, atime: mtime, mtime }, [volume, path]);
      return ino++;
    };
    const ensureParents = (path) => {
      const parts = path.split("/");
      for (let i = 1; i < parts.length; i++) {
        const dir = parts.slice(0, i).join("/");
        if (!dirs.has(dir)) {
          dirs.add(dir);
          put(dir, "dir", 0, Date.now());
        }
      }
    };
    for (const entry of entries) {
      ensureParents(entry.path);
      if (entry.type === "dir") {
        if (dirs.has(entry.path)) continue;
        dirs.add(entry.path);
        put(entry.path, "dir", 0, entry.mtimeMs);
        continue;
      }
      const fileIno = put(entry.path, "file", entry.bytes.length, entry.mtimeMs);
      files++;
      total += entry.bytes.length;
      for (let index = 0; index * PAGE_SIZE < entry.bytes.length; index++) {
        const page = entry.bytes.subarray(index * PAGE_SIZE, Math.min(entry.bytes.length, (index + 1) * PAGE_SIZE));
        if (page.every((b) => b === 0)) continue;
        chunks.put(page.slice(), [volume, fileIno, index]);
      }
    }
    const now = Date.now();
    tx.objectStore(VOLUMES).put({ nextIno: ino, created: now, updated: now }, volume);
    await done;
    return { files, dirs: dirs.size, bytes: total, skipped };
  });
}
