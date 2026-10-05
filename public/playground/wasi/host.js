// The host API (main thread): run a `wasm32-wasip1` command in a dedicated
// worker, give it listeners and a virtual network, drive its clock, keep its
// file system in IndexedDB, and watch it. See README.md in this directory.
//
//   import { spawn } from "/playground/wasi/host.js";
//   const proc = await spawn({ module: "/broker.wasm", listeners: [9092], volume: "node-1" });
//   const conn = proc.connect(9092);
//   conn.on("data", (bytes) => ...);
//   conn.send(request);
//
// Needs cross-origin isolation (SharedArrayBuffer and `Atomics.wait` in the
// worker): COOP/COEP response headers, or the lab's service worker
// (`/docs/lab/coi.js`).

import { ERRNO } from "./abi.js";
import { WasiClock } from "./clock.js";
import { DEFAULTS, DISK, DISK_MODES, FAULT_BYTES, FAULT_DISK_MODE, FAULT_DISK_MS, FAULT_PAUSED, FAULT_SEQ, FAULT_SKEW, OUT, REC } from "./protocol.js";
import { createRing, RingWriter } from "./ring.js";
import { lockVolume, VolumeWriter, loadImage, readVolumeFile } from "./volumes.js";

export { WasiClock } from "./clock.js";
export {
  DB_NAME,
  exportVolume,
  forget,
  importVolume,
  listVolumeFiles,
  listVolumes,
  readVolumeFile,
  readVolumeFileRange,
  storageAvailable,
  usage,
} from "./volumes.js";

const WORKER_URL = new URL("./worker.js", import.meta.url).href;
const encoder = new TextEncoder();
const EMPTY = new Uint8Array(0);
const LOG_TAIL = 200;
const IDLE_REQUEST = encoder.encode(JSON.stringify({ op: "idle" }));

function u32(value) {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, value >>> 0, true);
  return out;
}

function toBytes(data) {
  if (typeof data === "string") return encoder.encode(data);
  if (data instanceof Uint8Array) return data.slice();
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice();
  if (data instanceof ArrayBuffer) return new Uint8Array(data.slice(0));
  throw new TypeError("send() takes a Uint8Array, an ArrayBuffer, a view or a string");
}

/** Why a process cannot run in this context, or null when it can. */
export function isolationProblem() {
  if (typeof Worker === "undefined") return "Web Workers are unavailable in this context.";
  if (typeof SharedArrayBuffer === "undefined" || !globalThis.crossOriginIsolated) {
    return (
      "the page is not cross-origin isolated, so SharedArrayBuffer and Atomics.wait are unavailable. " +
      "Serve it with Cross-Origin-Opener-Policy: same-origin and Cross-Origin-Embedder-Policy: credentialless " +
      "(or require-corp), or call ensureCrossOriginIsolation() from /docs/lab/coi.js first."
    );
  }
  return null;
}

// ---- events ---------------------------------------------------------------------------------

class Emitter {
  #handlers = new Map();

  /** Adds a handler; returns a function that removes it. */
  on(type, fn) {
    let set = this.#handlers.get(type);
    if (!set) this.#handlers.set(type, (set = new Set()));
    set.add(fn);
    this.listening(type);
    return () => this.off(type, fn);
  }

  off(type, fn) {
    this.#handlers.get(type)?.delete(fn);
  }

  /** Resolves with the next event of `type`. */
  once(type) {
    return new Promise((resolve) => {
      const off = this.on(type, (value) => {
        off();
        resolve(value);
      });
    });
  }

  listenerCount(type) {
    return this.#handlers.get(type)?.size ?? 0;
  }

  /** Called after a handler is added. */
  listening() {}

  emit(type, ...args) {
    const set = this.#handlers.get(type);
    if (!set || set.size === 0) return false;
    for (const fn of [...set]) {
      try {
        fn(...args);
      } catch (err) {
        console.error(`wasi: a ${type} handler threw`, err);
      }
    }
    return true;
  }
}

// ---- connections ----------------------------------------------------------------------------

/**
 * One virtual TCP connection between the host and the guest: inbound (the
 * host called `connect(port)`) or outbound (the guest dialed and the host
 * accepted in `ondial`).
 *
 * Events: "accept" (inbound: the guest accepted it), "open" (outbound: the
 * host accepted the dial), "data" (Uint8Array from the guest), "end" (the
 * guest shut down its write side), "drain" (the send buffer fell below the
 * high-water mark), "close" ({ reason }).
 */
export class Connection extends Emitter {
  constructor(process, id, { direction, port, host = null, state }) {
    super();
    this.process = process;
    this.id = id;
    this.direction = direction;
    this.port = port;
    this.host = host;
    this.state = state; // "pending" | "connecting" | "open" | "closed"
    this.createdAt = performance.now();
    this.acceptedAt = null;
    this.bytesSent = 0;
    this.bytesReceived = 0;
    this.paused = false;
    this.ended = false; // the host called end()
    this.remoteEnded = false; // the guest shut down its write side
    this.closeInfo = null;
    // Host to guest: records not yet in the ring, and bytes in flight.
    this.queue = [];
    this.queueHead = 0;
    this.buffered = 0;
    this.inflight = 0;
    this.needDrain = false;
    // Guest to host: what arrived while nobody could take it, in order.
    this.inbox = [];
  }

  /** Bytes queued on the host and not yet handed to the worker. */
  get bufferedAmount() {
    return this.buffered;
  }

  /** Whether "data" events flow: a handler is attached and the connection is not paused. */
  get flowing() {
    return !this.paused && this.listenerCount("data") > 0;
  }

  /** Queues bytes for the guest. Returns false when the buffer is past the high-water mark: wait for "drain". */
  send(data) {
    return this.process.send(this, data);
  }

  /** Like send(), but resolves once the buffer is below the high-water mark; rejects on a closed connection. */
  write(data) {
    if (this.state === "closed" || this.state === "closing" || this.ended) return Promise.reject(new Error(`connection ${this.id} is closed`));
    if (this.send(data)) return Promise.resolve();
    return new Promise((resolve) => {
      const offDrain = this.on("drain", done);
      const offClose = this.on("close", done);
      function done() {
        offDrain();
        offClose();
        resolve();
      }
    });
  }

  /** Half-closes: the guest reads the end of the stream after the queued bytes; it can still write. */
  end() {
    this.process.end(this);
  }

  /** Closes: the guest reads the end of the stream, its writes fail (EPIPE, or ECONNRESET with reset). */
  close(options) {
    this.process.close(this, options);
  }

  /** Stops "data" events. The guest can send up to its window more, then waits. */
  pause() {
    this.paused = true;
  }

  resume() {
    this.paused = false;
    this.process.deliver(this);
  }

  listening(type) {
    if (type === "data" && this.inbox.length > 0) queueMicrotask(() => this.process.deliver(this));
  }
}

/**
 * Joins two connections: bytes flow both ways with backpressure (a full
 * destination pauses the source until it drains), an end or a close on one
 * side ends or closes the other. Returns a function that undoes it.
 */
export function pipe(a, b) {
  const forward = (from, to) => {
    const offs = [
      from.on("data", (bytes) => {
        if (!to.send(bytes)) from.pause();
      }),
      to.on("drain", () => from.resume()),
      from.on("end", () => to.end()),
      from.on("close", (info) => to.close({ reset: Boolean(info && info.reset) })),
    ];
    return () => offs.forEach((off) => off());
  };
  const undo = [forward(a, b), forward(b, a)];
  return () => undo.forEach((fn) => fn());
}

// ---- processes ------------------------------------------------------------------------------

const moduleCache = new Map();

/**
 * Compiles (and caches, by URL) a module given as a URL, bytes or a
 * WebAssembly.Module. A failed fetch rejects with an error whose `status` is
 * the HTTP status, so a caller can tell a missing module (404) apart.
 */
export async function compileModule(source) {
  if (source instanceof WebAssembly.Module) return source;
  if (typeof source === "string" || source instanceof URL) {
    const url = new URL(String(source), globalThis.location?.href).href;
    if (!moduleCache.has(url)) {
      const compiling = (async () => {
        const response = await fetch(url);
        if (!response.ok) {
          const err = new Error(`cannot fetch ${url}: HTTP ${response.status}`);
          err.status = response.status;
          throw err;
        }
        try {
          return await WebAssembly.compileStreaming(response.clone());
        } catch {
          return WebAssembly.compile(await response.arrayBuffer());
        }
      })();
      moduleCache.set(url, compiling);
      compiling.catch(() => moduleCache.delete(url));
    }
    return moduleCache.get(url);
  }
  return WebAssembly.compile(toBytes(source));
}

function createWorker(name) {
  // A blob-URL worker inherits the page's policy container, so it is
  // cross-origin isolated whether the isolation came from response headers or
  // from a service worker whose scope does not cover this directory.
  const bootstrap = new Blob([`import ${JSON.stringify(WORKER_URL)};\n`], { type: "text/javascript" });
  const url = URL.createObjectURL(bootstrap);
  const worker = new Worker(url, { type: "module", name });
  return { worker, url };
}

let processCount = 0;

/**
 * A guest process: one worker, one ring, the process's connections, and
 * optionally a persistent volume. Create with `spawn(options)`.
 *
 * Events: "stdout" (line), "stderr" (line), "accept" (Connection),
 * "dial" ({ host, port, accepted, conn }), "exit" ({ reason, code, error? }),
 * "trap" ({ name, message, stack, stderr }), "restart" ({ incarnation }),
 * "persisted" ({ ops, bytes, ms }), "warn" (text), "error" (Error).
 */
export class WasiProcess extends Emitter {
  /** Same as `spawn(options)`. */
  static spawn(options) {
    return spawn(options);
  }

  constructor(options, module) {
    super();
    const o = { ...DEFAULTS, ...options };
    this.name = o.name ?? `guest-${++processCount}`;
    this.module = module;
    this.args = o.args ?? [this.name];
    this.env = Array.isArray(o.env) ? [...o.env] : Object.entries(o.env ?? {}).map(([k, v]) => `${k}=${v}`);
    this.listeners = [...(o.listeners ?? [])];
    if (new Set(this.listeners).size !== this.listeners.length) throw new Error(`duplicate listener ports: ${this.listeners}`);
    this.volume = o.volume ?? null;
    this.mountPath = o.mountPath;
    this.clock = o.clock instanceof WasiClock ? o.clock : new WasiClock(o.clock ?? {});
    this.ondial = o.ondial ?? null;
    this.options = o;
    this.state = "idle"; // "starting" | "running" | "exited" | "killed" | "trapped" | "failed"
    this.incarnation = 0;
    this.layout = null;
    this.exitInfo = null;
    this.lastStats = null;
    this.worker = null;
    this.writer = null;
    this.conns = new Map();
    this.active = new Set(); // connections with queued records
    this.ctrl = []; // records not tied to a connection's order
    this.acks = new Map(); // id -> guest bytes consumed, not yet acknowledged
    this.inboxDirty = new Set(); // connections with undelivered data or events
    this.requests = new Map();
    this.ringSeq = 0; // records written into the ring so far
    this.inputSeq = 0; // of those, the ones that can wake the guest (not acknowledgements or requests)
    this.barriers = []; // idle requests, written after every record queued before them
    this.nextConnId = 1;
    this.nextRequest = 1;
    this.pumpQueued = false;
    this.releaseVolume = null;
    this.store = this.volume === null ? null : new VolumeWriter(this.volume);
    this.logs = { stdout: [], stderr: [] };
    this.hostStats = { bytesToGuest: 0, bytesFromGuest: 0, connections: 0, dials: 0, dialsAccepted: 0, journalBatches: 0, journalBytes: 0 };
    this.exited = null; // a promise per incarnation, resolved with the exit info
    // Faults the host injects (pause, clock skew, disk mode): one buffer for
    // every incarnation, so skew and disk mode outlive a restart; a pause does not.
    this.faultBuffer = new SharedArrayBuffer(FAULT_BYTES);
    this.faultI32 = new Int32Array(this.faultBuffer, 0, 4);
    this.faultI64 = new BigInt64Array(this.faultBuffer, 16, 1);
  }

  // ---- lifecycle ------------------------------------------------------------------------------

  async start() {
    if (this.state === "starting" || this.state === "running") throw new Error(`${this.name} is already ${this.state}`);
    const problem = isolationProblem();
    if (problem) throw new Error(`cannot start ${this.name}: ${problem}`);
    if (this.volume !== null && !this.releaseVolume) this.releaseVolume = await lockVolume(this.volume);
    const t0 = performance.now();
    this.helloAt = null;
    Atomics.store(this.faultI32, FAULT_PAUSED, 0);
    this.state = "starting";
    const incarnation = ++this.incarnation;
    let resolveExit;
    this.exited = new Promise((resolve) => (resolveExit = resolve));
    this.resolveExit = resolveExit;
    try {
      const image = this.volume === null ? null : await loadImage(this.volume);
      const loaded = performance.now();
      const ring = createRing(this.options.ringBytes);
      this.writer = new RingWriter(ring);
      const { worker, url } = createWorker(`wasi:${this.name}`);
      this.worker = worker;
      const running = new Promise((resolve, reject) => {
        this.startWaiter = { resolve, reject };
      });
      worker.onmessage = (event) => this.#message(incarnation, event.data, url);
      worker.onerror = (event) => {
        event.preventDefault();
        this.#workerError(incarnation, new Error(`the WASI worker failed: ${event.message || "it did not load"}`));
      };
      worker.onmessageerror = () => this.emit("error", new Error("a message from the worker could not be deserialized"));
      const transfer = image ? [...new Set(image.chunks.map((c) => c.bytes.buffer))] : [];
      worker.postMessage(
        {
          t: "start",
          module: this.module,
          ring,
          clock: this.clock.buffer,
          image,
          config: {
            name: this.name,
            args: this.args,
            env: this.env,
            listeners: this.listeners,
            mountPath: this.mountPath,
            persistent: this.volume !== null,
            window: this.options.window,
            backlog: this.options.backlog,
            journalIntervalMs: this.options.journalIntervalMs,
            journalMaxInFlight: this.options.journalMaxInFlight,
            maxLineBytes: this.options.maxLineBytes,
            maxBytes: this.options.maxBytes,
            seed: this.options.seed,
            faults: this.faultBuffer,
          },
        },
        transfer,
      );
      this.unsubscribeClock = this.clock.subscribe(() => this.#wake());
      const info = await running;
      const now = performance.now();
      this.startTimings = {
        imageMs: loaded - t0,
        bootMs: (this.helloAt ?? now) - loaded,
        instantiateMs: info.instantiateMs,
        totalMs: now - t0,
        files: image ? image.nodes.length : 0,
        bytes: image ? image.chunks.reduce((sum, c) => sum + c.bytes.length, 0) : 0,
      };
      this.layout = info.layout;
      this.guestEnv = info.env;
      this.imports = info.imports;
      this.instantiateMs = info.instantiateMs;
      this.startedAt = performance.now();
      this.state = "running";
      this.#pump();
    } catch (err) {
      if (this.incarnation === incarnation && this.state === "starting") this.#teardown({ reason: "failed", code: null, error: { message: err.message } }, "failed");
      if (this.releaseVolume) {
        await this.store?.idle();
        this.releaseVolume();
        this.releaseVolume = null;
      }
      throw err;
    }
    return this;
  }

  /** Stops the guest at once, like pulling the plug: journal batches not yet posted by the worker are lost. */
  async kill() {
    if (this.worker) this.#teardown({ reason: "kill", code: null }, "killed");
    await this.#settleVolume();
  }

  /**
   * Recreates the worker on the same volume, clock and listeners. By default it
   * first asks the guest to flush its journal (graceful); `{ flush: false }`
   * restarts from what IndexedDB holds right now, like a crash.
   */
  async restart({ flush = true, flushTimeoutMs = 5000 } = {}) {
    if (this.state === "starting") throw new Error(`${this.name} is starting`);
    if (this.state === "running" && flush) {
      await Promise.race([this.flush().catch(() => {}), new Promise((resolve) => setTimeout(resolve, flushTimeoutMs))]);
    }
    if (this.worker) this.#teardown({ reason: "restart", code: null }, "killed");
    await this.store?.idle();
    await this.start();
    this.emit("restart", { incarnation: this.incarnation });
    return this;
  }

  async #settleVolume() {
    await this.store?.idle();
    if (this.releaseVolume && !this.worker) {
      this.releaseVolume();
      this.releaseVolume = null;
    }
  }

  #teardown(info, state) {
    if (this.worker) this.worker.terminate();
    this.worker = null;
    this.writer = null;
    this.unsubscribeClock?.();
    this.unsubscribeClock = null;
    this.startWaiter?.reject(new Error(`${this.name} ${info.reason === "failed" ? "failed to start" : `stopped (${info.reason})`}`));
    this.startWaiter = null;
    for (const [, request] of this.requests) request.reject(new Error(`${this.name} stopped (${info.reason})`));
    this.requests.clear();
    const conns = [...this.conns.values()];
    this.conns.clear();
    this.active.clear();
    this.ctrl = [];
    this.barriers = [];
    this.acks.clear();
    for (const conn of conns) this.#remoteClose(conn, { reason: info.reason === "exit" || info.reason === "return" ? "exit" : info.reason, reset: true });
    this.state = state;
    this.exitInfo = info;
    this.emit("exit", info);
    this.resolveExit?.(info);
  }

  #workerError(incarnation, err) {
    if (incarnation !== this.incarnation) return;
    if (this.state === "starting") {
      this.startWaiter?.reject(err);
      return;
    }
    this.emit("error", err);
    this.#teardown({ reason: "trap", code: null, error: { name: err.name, message: err.message, stack: err.stack } }, "trapped");
    this.#settleVolume();
  }

  // ---- messages from the worker ---------------------------------------------------------------

  #message(incarnation, message, url) {
    if (incarnation !== this.incarnation || !this.worker) return;
    switch (message.t) {
      case "hello":
        this.helloAt = performance.now();
        URL.revokeObjectURL(url);
        break;
      case "running":
        this.startWaiter?.resolve(message);
        this.startWaiter = null;
        break;
      case "failed":
        this.startWaiter?.reject(new Error(`${this.name} failed to start: ${message.error.message}`));
        this.startWaiter = null;
        break;
      case "batch":
        this.#batch(message.items);
        break;
      case "reply": {
        const request = this.requests.get(message.id);
        this.requests.delete(message.id);
        if (!request) break;
        if (!message.ok) request.reject(new Error(message.value));
        else if (request.op === "idle") request.resolve({ value: message.value, fresh: this.#covers(request, message.value) });
        else request.resolve(message.value);
        break;
      }
      case "exit":
        this.#exit(message);
        break;
      default:
        this.emit("warn", `unknown worker message ${JSON.stringify(message.t)}`);
    }
  }

  #batch(items) {
    for (const item of items) {
      switch (item[0]) {
        case OUT.DATA: {
          const conn = this.conns.get(item[1]);
          if (!conn) break;
          conn.bytesReceived += item[2].length;
          this.hostStats.bytesFromGuest += item[2].length;
          conn.inbox.push({ type: "data", bytes: item[2] });
          this.inboxDirty.add(conn);
          break;
        }
        case OUT.ACCEPT: {
          const conn = this.conns.get(item[1]);
          if (!conn) break;
          conn.acceptedAt = performance.now();
          if (conn.state === "pending") conn.state = "open";
          conn.emit("accept", conn);
          this.emit("accept", conn);
          break;
        }
        case OUT.END: {
          const conn = this.conns.get(item[1]);
          if (conn) {
            conn.inbox.push({ type: "end" });
            this.inboxDirty.add(conn);
          }
          break;
        }
        case OUT.CLOSE: {
          const conn = this.conns.get(item[1]);
          if (conn) this.#remoteClose(conn, { reason: item[2] === "closed" ? "guest" : item[2] === "backlog" ? "refused: backlog full" : "refused: no listener", reset: false });
          break;
        }
        case OUT.DIAL:
          this.#dial(item[1], item[2], item[3]);
          break;
        case OUT.RX: {
          const conn = this.conns.get(item[1]);
          if (conn) {
            conn.inflight = Math.max(0, conn.inflight - item[2]);
            if (conn.queueHead < conn.queue.length) this.active.add(conn);
          }
          break;
        }
        case OUT.LOG:
          this.#line(item[1] === 1 ? "stdout" : "stderr", item[2]);
          break;
        case OUT.JOURNAL:
          this.#journal(item[1], item[2], item[3]);
          break;
        case OUT.WARN:
          if (!this.emit("warn", item[1])) console.warn(`wasi ${this.name}: ${item[1]}`);
          break;
        case OUT.SPACE:
          break;
        default:
          this.emit("warn", `unknown outbox item ${item[0]}`);
      }
    }
    for (const conn of this.inboxDirty) this.deliver(conn);
    this.inboxDirty.clear();
    this.#pump();
  }

  /** Hands a connection's queued data and events to its handlers, in order. */
  deliver(conn) {
    while (conn.inbox.length > 0) {
      const item = conn.inbox[0];
      if (item.type === "data") {
        if (!conn.flowing) return;
        conn.inbox.shift();
        conn.emit("data", item.bytes);
        this.acks.set(conn.id, (this.acks.get(conn.id) ?? 0) + item.bytes.length);
      } else if (item.type === "end") {
        conn.inbox.shift();
        conn.remoteEnded = true;
        conn.emit("end");
      } else {
        conn.inbox.shift();
        conn.state = "closed";
        conn.emit("close", item.info);
      }
    }
    if (this.acks.size > 0) this.#schedulePump();
  }

  /** The guest closed or refused the connection, or the process ended. */
  #remoteClose(conn, info) {
    if (conn.closeInfo) return;
    conn.closeInfo = info;
    this.conns.delete(conn.id);
    this.active.delete(conn);
    conn.queue = [];
    conn.queueHead = 0;
    conn.buffered = 0;
    if (conn.state !== "closed") conn.state = "closing";
    conn.inbox.push({ type: "close", info });
    this.deliver(conn);
    if (conn.needDrain) {
      conn.needDrain = false;
      conn.emit("drain");
    }
  }

  #line(stream, line) {
    const tail = this.logs[stream];
    tail.push(line);
    if (tail.length > LOG_TAIL) tail.shift();
    this.emit(stream, line);
  }

  #journal(ops, bytes, nextIno) {
    if (!this.store) return;
    const incarnation = this.incarnation;
    const started = performance.now();
    this.hostStats.journalBatches++;
    this.hostStats.journalBytes += bytes;
    const ack = () => {
      if (incarnation === this.incarnation && this.writer) {
        this.ctrl.push([REC.JOURNAL_ACK, 0, u32(bytes)]);
        this.#schedulePump();
      }
    };
    this.store.enqueue(ops, nextIno).then(
      () => {
        ack();
        this.emit("persisted", { ops: ops.length, bytes, ms: performance.now() - started });
      },
      (err) => {
        ack();
        this.emit("error", new Error(`persisting ${this.volume} failed: ${err.message}`, { cause: err }));
      },
    );
  }

  #dial(id, host, port) {
    const conn = new Connection(this, id, { direction: "outbound", port, host, state: "connecting" });
    this.conns.set(id, conn);
    this.hostStats.dials++;
    let settled = false;
    const decide = (accepted, errno, reason) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      const live = this.conns.get(id) === conn && conn.state === "connecting";
      if (live && accepted) {
        conn.state = "open";
        this.hostStats.dialsAccepted++;
        this.hostStats.connections++;
        this.#enqueueFirst(conn, REC.DIAL_OK, EMPTY);
        conn.emit("open", conn);
      } else if (live) {
        const payload = new Uint8Array(2 + encoder.encode(reason).length);
        new DataView(payload.buffer).setUint16(0, errno, true);
        payload.set(encoder.encode(reason), 2);
        this.#enqueueFirst(conn, REC.DIAL_FAIL, payload);
        conn.state = "closed";
        this.conns.delete(id);
        conn.closeInfo = { reason: `refused: ${reason}`, reset: false };
      }
      this.emit("dial", { host, port, accepted: live && accepted, conn });
    };
    const dial = {
      host,
      port,
      accept: () => {
        decide(true);
        return conn;
      },
      refuse: (code = "ECONNREFUSED") => decide(false, ERRNO[String(code).replace(/^E/, "")] ?? ERRNO.CONNREFUSED, String(code)),
    };
    const timer = setTimeout(() => decide(false, ERRNO.TIMEDOUT, "ETIMEDOUT"), this.options.dialTimeoutMs);
    let answer;
    try {
      answer = this.ondial ? this.ondial(host, port, dial) : null;
    } catch (err) {
      this.emit("error", err);
      decide(false, ERRNO.CONNREFUSED, "ECONNREFUSED");
      return;
    }
    Promise.resolve(answer).then(
      (value) => (value === conn || value === true ? decide(true) : decide(false, ERRNO.CONNREFUSED, "ECONNREFUSED")),
      (err) => {
        this.emit("error", err);
        decide(false, ERRNO.CONNREFUSED, "ECONNREFUSED");
      },
    );
  }

  #exit(message) {
    this.lastStats = message.stats;
    const info = { reason: message.reason, code: message.code };
    if (message.error) info.error = message.error;
    if (message.reason === "trap") {
      this.emit("trap", { ...message.error, stderr: [...this.logs.stderr] });
      this.#teardown(info, "trapped");
    } else {
      this.#teardown(info, "exited");
    }
    this.#settleVolume();
  }

  // ---- the ring -------------------------------------------------------------------------------

  #wake() {
    if (!this.writer) return;
    this.writer.notify();
  }

  #schedulePump() {
    if (this.pumpQueued) return;
    this.pumpQueued = true;
    queueMicrotask(() => this.#pump());
  }

  /** Puts a record at the front of a connection's queue (dial answers precede anything the host sent early). */
  #enqueueFirst(conn, kind, payload) {
    conn.queue.splice(conn.queueHead, 0, { kind, payload });
    this.active.add(conn);
    this.#schedulePump();
  }

  #enqueue(conn, kind, payload) {
    conn.queue.push({ kind, payload });
    this.active.add(conn);
    this.#schedulePump();
  }

  /**
   * Moves queued records into the ring: control first, then one record per
   * connection per round, then the idle requests once no connection holds
   * records back.
   */
  #pump() {
    this.pumpQueued = false;
    const writer = this.writer;
    if (!writer || (this.state !== "running" && this.state !== "starting")) return;
    for (const [id, bytes] of this.acks) this.ctrl.push([REC.ACK, id, u32(bytes)]);
    this.acks.clear();
    let wrote = false;
    let full = false;
    while (this.ctrl.length > 0) {
      const [kind, id, payload] = this.ctrl[0];
      if (!writer.tryWrite(kind, id, payload)) {
        full = true;
        break;
      }
      this.ctrl.shift();
      this.ringSeq++;
      wrote = true;
    }
    const window = this.options.window;
    const chunk = this.options.chunkBytes;
    while (!full && this.active.size > 0) {
      let progress = false;
      for (const conn of this.active) {
        const item = conn.queue[conn.queueHead];
        if (!item) {
          this.active.delete(conn);
          continue;
        }
        if (item.kind === REC.DATA) {
          const room = window - conn.inflight;
          if (room <= 0) {
            this.active.delete(conn); // back when the guest reports what it read
            continue;
          }
          const space = writer.room();
          if (space <= 0) {
            writer.requestSpace();
            full = true;
            break;
          }
          const n = Math.min(item.bytes.length - item.offset, chunk, room, space);
          writer.tryWrite(REC.DATA, conn.id, item.bytes.subarray(item.offset, item.offset + n));
          this.ringSeq++;
          this.inputSeq++;
          item.offset += n;
          conn.inflight += n;
          conn.buffered -= n;
          this.hostStats.bytesToGuest += n;
          if (item.offset === item.bytes.length) conn.queueHead++;
        } else {
          if (!writer.tryWrite(item.kind, conn.id, item.payload)) {
            full = true;
            break;
          }
          this.ringSeq++;
          this.inputSeq++;
          conn.queueHead++;
        }
        wrote = true;
        progress = true;
        if (conn.queueHead > 256 && conn.queueHead * 2 > conn.queue.length) {
          conn.queue = conn.queue.slice(conn.queueHead);
          conn.queueHead = 0;
        }
        if (conn.queueHead >= conn.queue.length) {
          conn.queue = [];
          conn.queueHead = 0;
          this.active.delete(conn);
        }
        if (conn.needDrain && conn.buffered < this.options.highWaterMark) {
          conn.needDrain = false;
          conn.emit("drain");
        }
      }
      if (!progress) break;
    }
    // An idle request covers what was written before it, so it waits until
    // the connections' records are in.
    while (!full && this.barriers.length > 0 && !this.#inputPending()) {
      const id = this.barriers[0];
      const request = this.requests.get(id);
      if (!request) {
        this.barriers.shift();
        continue;
      }
      if (!writer.tryWrite(REC.REQUEST, id, IDLE_REQUEST)) {
        full = true;
        break;
      }
      this.barriers.shift();
      this.ringSeq++;
      request.seq = this.ringSeq;
      request.inputSeq = this.inputSeq;
      wrote = true;
    }
    if (wrote) writer.notify();
  }

  // ---- the network --------------------------------------------------------------------------

  /** Opens a connection to the guest's listener on `port`. The guest sees it on its next accept. */
  connect(port) {
    const index = this.listeners.indexOf(port);
    if (index < 0) throw new Error(`${this.name} has no listener on port ${port} (it has ${this.listeners.join(", ") || "none"})`);
    if (this.state !== "running" && this.state !== "starting") throw new Error(`${this.name} is ${this.state}`);
    let id = this.nextConnId;
    while (this.conns.has(id)) id = id >= 0x7fff_ffff ? 1 : id + 1;
    this.nextConnId = id >= 0x7fff_ffff ? 1 : id + 1;
    const conn = new Connection(this, id, { direction: "inbound", port, state: "pending" });
    this.conns.set(id, conn);
    this.hostStats.connections++;
    this.#enqueue(conn, REC.CONNECT, u32(index));
    return conn;
  }

  /** Queues bytes for the guest on `conn`; false means "past the high-water mark, wait for drain". */
  send(conn, data) {
    if (conn.process !== this) throw new Error("that connection belongs to another process");
    if (conn.state === "closed" || conn.state === "closing" || conn.ended) return false;
    const bytes = toBytes(data);
    if (bytes.length > 0) {
      conn.queue.push({ kind: REC.DATA, bytes, offset: 0 });
      conn.buffered += bytes.length;
      conn.bytesSent += bytes.length;
      this.active.add(conn);
      this.#schedulePump();
    }
    const ok = conn.buffered < this.options.highWaterMark;
    if (!ok) conn.needDrain = true;
    return ok;
  }

  /** Half-closes `conn` (see Connection.end). */
  end(conn) {
    if (conn.state === "closed" || conn.ended) return;
    conn.ended = true;
    this.#enqueue(conn, REC.END, EMPTY);
  }

  /** Closes `conn` (see Connection.close); `{ reset: true }` resets it instead. */
  close(conn, { reset = false } = {}) {
    if (conn.state === "closed") return;
    if (this.conns.get(conn.id) === conn && this.writer) this.#enqueue(conn, REC.CLOSE, Uint8Array.of(reset ? 1 : 0));
    this.conns.delete(conn.id);
    conn.inbox = [];
    conn.state = "closed";
    conn.closeInfo ??= { reason: "host", reset };
    conn.emit("close", conn.closeInfo);
  }

  // ---- injected faults ----------------------------------------------------------------------

  #setFault(slot, value) {
    Atomics.store(this.faultI32, slot, value);
    Atomics.add(this.faultI32, FAULT_SEQ, 1);
    this.#wake();
  }

  /**
   * Stops the guest the way SIGSTOP does: from its next `poll_oneoff` or
   * blocking call on it gets no input, no readiness and no timers until
   * `resume()`. Input still reaches its socket buffers (up to one window per
   * connection), requests are served, and `quiesce()` resolves with no deadline.
   */
  pause() {
    this.#setFault(FAULT_PAUSED, 1);
  }

  resume() {
    this.#setFault(FAULT_PAUSED, 0);
  }

  get paused() {
    return Atomics.load(this.faultI32, FAULT_PAUSED) !== 0;
  }

  /** Offsets this process's REALTIME clock by `ms` (MONOTONIC is untouched); 0 removes the skew. */
  setClockSkew(ms) {
    Atomics.store(this.faultI64, FAULT_SKEW, BigInt(Math.round(Number(ms) || 0)) * 1_000_000n);
    Atomics.add(this.faultI32, FAULT_SEQ, 1);
    this.#wake();
  }

  get clockSkewMs() {
    return Number(Atomics.load(this.faultI64, FAULT_SKEW) / 1_000_000n);
  }

  /**
   * The volume's health: "ok"; "slow", where each `fd_sync` / `fd_datasync`
   * takes `ms` of host time (the guest waits for the clock); "full", where
   * writes that grow the files fail with ENOSPC; "eio", where writes and
   * syncs fail with EIO.
   */
  setDisk(mode, ms = 0) {
    const code = DISK_MODES.indexOf(mode);
    if (code < 0) throw new TypeError(`disk mode must be one of ${DISK_MODES.join(", ")}, not ${mode}`);
    Atomics.store(this.faultI32, FAULT_DISK_MS, code === DISK.SLOW ? Math.max(0, Math.round(Number(ms) || 0)) : 0);
    this.#setFault(FAULT_DISK_MODE, code);
  }

  get disk() {
    return { mode: DISK_MODES[Atomics.load(this.faultI32, FAULT_DISK_MODE)], ms: Atomics.load(this.faultI32, FAULT_DISK_MS) };
  }

  // ---- requests and observability -----------------------------------------------------------

  #request(op, args = {}) {
    if (this.state !== "running") return Promise.reject(new Error(`${this.name} is ${this.state}`));
    const id = this.nextRequest++;
    return new Promise((resolve, reject) => {
      this.requests.set(id, { resolve, reject, op, seq: -1, inputSeq: -1 });
      if (op === "idle") this.barriers.push(id);
      else this.ctrl.push([REC.REQUEST, id, encoder.encode(JSON.stringify({ op, ...args }))]);
      this.#schedulePump();
    });
  }

  /**
   * Whether a connection holds records for the guest that it could take now.
   * Bytes held back by a full window are not: the guest has a window of
   * unread bytes on that connection, and whether it reads them is its call.
   */
  #inputPending() {
    const window = this.options.window;
    const pending = (conn) => {
      const item = conn.queue[conn.queueHead];
      return item !== undefined && !(item.kind === REC.DATA && conn.inflight >= window);
    };
    for (const conn of this.active) if (pending(conn)) return true;
    for (const conn of this.conns.values()) if (pending(conn)) return true;
    return false;
  }

  /**
   * Whether an idle answer still holds: no input reached the ring after the
   * request or waits to, and no acknowledgement did either while the guest
   * waited for one (anything else in the ring cannot wake it).
   */
  #covers(request, answer) {
    if (request.inputSeq !== this.inputSeq || this.#inputPending()) return false;
    if (!answer.writeBlocked) return true;
    return request.seq === this.ringSeq && this.acks.size === 0 && !this.ctrl.some(([kind]) => kind === REC.ACK);
  }

  /**
   * Resolves once the guest has taken in everything the host sent it (up to
   * a full window per connection, which it may leave unread), its output has
   * reached the host, and it is blocked waiting for input or a timer:
   * `{ hostMs, deadlineMs }`, the clock's host time then and the host
   * time of the guest's next timer (null when it waits on none). Resolves
   * null when the process stops first. A host that drives the clock waits for
   * this before it lets time move on, so the guest answers at the instant its
   * input arrived. A guest that never blocks never resolves it: race it with
   * a timeout.
   */
  async quiesce() {
    for (;;) {
      if (this.state !== "running") return null;
      let answer;
      try {
        answer = await this.#request("idle");
      } catch {
        return null;
      }
      // Anything written after the request (more input, acknowledgements
      // that let the guest write on) is not covered by the answer: ask again.
      if (answer.fresh) return answer.value;
    }
  }

  /**
   * Counters: `{ name, state, incarnation, guest, host }`. `guest` comes from
   * the worker (syscall counts, poll and blocked time, sockets, file system,
   * journal); after an exit it is the final snapshot.
   */
  async stats() {
    const guest = this.state === "running" ? await this.#request("stats") : this.lastStats;
    return {
      name: this.name,
      state: this.state,
      incarnation: this.incarnation,
      guest,
      host: { ...this.hostStats, connectionsOpen: this.conns.size, store: this.store ? { ...this.store.stats } : null },
    };
  }

  /** Asks the guest to post its journal now, then waits until IndexedDB has committed it. */
  async flush() {
    if (this.state === "running") await this.#request("flush");
    await this.store?.idle();
  }

  /** A file's bytes: live from the running guest, else as stored. Null when missing. */
  async readFile(path) {
    if (this.state === "running") return this.#request("readFile", { path });
    if (this.volume === null) return null;
    await this.store?.idle();
    const mount = this.mountPath.replace(/\/+$/, "");
    const relative = path.startsWith(`${mount}/`) ? path.slice(mount.length + 1) : path;
    return readVolumeFile(this.volume, relative);
  }

  /** A live directory listing `[{ name, type, size }]` from the running guest, or null. */
  listFiles(path) {
    return this.#request("list", { path });
  }

  /** The last lines of "stdout" or "stderr". */
  tail(stream = "stderr") {
    return [...this.logs[stream]];
  }
}

/**
 * Spawns a guest.
 *
 * @param {object} options
 * @param {string|URL|ArrayBuffer|Uint8Array|WebAssembly.Module} options.module The `wasm32-wasip1` command.
 * @param {string[]} [options.args] argv, program name first (default `[name]`).
 * @param {object|string[]} [options.env] Environment, as an object or `KEY=VALUE` strings.
 * @param {number[]} [options.listeners] Listener ports; the guest gets one preopened socket each.
 * @param {string|null} [options.volume] IndexedDB volume id for the file system; null (default) is ephemeral.
 * @param {string} [options.mountPath] Where the volume is mounted in the guest (default "/data").
 * @param {WasiClock|object} [options.clock] A shared WasiClock, or options for a private one.
 * @param {(host: string, port: number, dial: object) => any} [options.ondial] Decides guest dials.
 * @returns {Promise<WasiProcess>} once the module is instantiated and `_start` is about to run.
 */
export async function spawn(options) {
  if (!options || options.module === undefined) throw new TypeError("spawn() needs { module }");
  const problem = isolationProblem();
  if (problem) throw new Error(`cannot spawn: ${problem}`);
  const module = await compileModule(options.module);
  const proc = new WasiProcess(options, module);
  await proc.start();
  return proc;
}
