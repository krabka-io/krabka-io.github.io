// The WASI preview-1 implementation behind `wasi_snapshot_preview1`, for one
// guest in one dedicated worker (blocking strategy A: a SharedArrayBuffer
// ring and `Atomics.wait`).
//
// Descriptor layout (dirs first, the way wasmtime lays them out): 0 stdin,
// 1 stdout, 2 stderr, 3 the volume mounted at `mountPath`, then one listening
// socket per listener port in the order the host listed them, then the dialer.
// wasi-libc scans preopens from fd 3 until `fd_prestat_get` answers EBADF, which
// the first socket does. The guest learns the rest from `KRABKA_LISTEN_FDS`,
// `KRABKA_LISTEN_PORTS` and `KRABKA_DIAL_FD`.
//
// Every call the guest imports is implemented or answers ENOSYS; nothing traps
// except `proc_exit` (by design) and a shim failure, which is reported with
// the call's name. Guest pointers out of bounds answer EFAULT.

import {
  ADVICE_MAX,
  CLOCKID,
  DIRENT_SIZE,
  ERRNO as E,
  EVENTRWFLAGS_HANGUP,
  EVENTTYPE,
  EVENT_SIZE,
  FDFLAGS,
  FDSTAT_SIZE,
  FILESTAT_SIZE,
  FILETYPE,
  FSTFLAGS,
  OFLAGS,
  PREOPENTYPE_DIR,
  PREVIEW1_FUNCTIONS,
  RIFLAGS,
  RIGHTS,
  RIGHTS_ALL,
  SDFLAGS,
  SUBCLOCKFLAGS_ABSTIME,
  SUBSCRIPTION_SIZE,
  WHENCE,
} from "./abi.js";
import { ClockView } from "./clock.js";
import { MemFs } from "./fs.js";
import { Dialer, LogFd, StdinFd, VirtualNet } from "./net.js";
import { MONOTONIC_OFFSET_NS, OUT, REC } from "./protocol.js";
import { RingReader } from "./ring.js";

const encoder = new TextEncoder();
const pathDecoder = new TextDecoder("utf-8", { fatal: true });
const textDecoder = new TextDecoder("utf-8");

/** How soon after an fsync the journal goes out, in ms. */
const SYNC_FLUSH_MS = 5;
/** Journal size that forces a flush regardless of the interval. */
const JOURNAL_FLUSH_BYTES = 4 << 20;
/** Guest output held back at most this long, in ms, or this many bytes. */
const OUTPUT_FLUSH_MS = 4;
const OUTPUT_FLUSH_BYTES = 256 * 1024;

/** Thrown by `proc_exit` to unwind the guest. */
export class ProcExit extends Error {
  constructor(code) {
    super(`proc_exit(${code})`);
    this.name = "ProcExit";
    this.code = code;
  }
}

/** A failure inside the shim itself, named after the call it happened in. */
export class ShimError extends Error {
  constructor(call, cause) {
    super(`wasi shim failed in ${call}: ${cause && cause.message ? cause.message : cause}`, { cause });
    this.name = "ShimError";
  }
}

function u32(bytes, at = 0) {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(at, true);
}

function u64(value) {
  return BigInt.asUintN(64, BigInt(value));
}

/** A u64 argument (a BigInt from wasm) as a safe integer, or -1 when it is not one. */
function safeNumber(value) {
  const big = BigInt.asUintN(64, value);
  return big <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(big) : -1;
}

class FileFd {
  constructor(fs, inode, rightsBase, rightsInheriting, flags) {
    this.kind = "file";
    this.filetype = FILETYPE.REGULAR_FILE;
    this.fs = fs;
    this.inode = inode;
    this.rightsBase = rightsBase;
    this.rightsInheriting = rightsInheriting;
    this.flags = flags;
    this.pos = 0;
    inode.opens++;
  }

  get mayBlock() {
    return false;
  }

  canRead() {
    return (this.rightsBase & RIGHTS.FD_READ) !== 0n;
  }

  canWrite() {
    return (this.rightsBase & RIGHTS.FD_WRITE) !== 0n;
  }

  read(dst, at, len) {
    if (!this.canRead()) return -E.BADF;
    const n = this.fs.readAt(this.inode, this.pos, dst, at, len);
    this.pos += n;
    return n;
  }

  write(src, at, len) {
    if (!this.canWrite()) return -E.BADF;
    if (this.flags & FDFLAGS.APPEND) this.pos = this.inode.size;
    const n = this.fs.writeAt(this.inode, this.pos, src, at, len);
    if (n > 0) this.pos += n;
    return n;
  }

  pollRead() {
    return { nbytes: Math.max(0, this.inode.size - this.pos), hangup: false };
  }

  pollWrite() {
    return { nbytes: 0, hangup: false };
  }

  close() {
    this.fs.release(this.inode);
  }
}

class DirFd {
  constructor(inode, rightsBase, rightsInheriting, preopen = null) {
    this.kind = "dir";
    this.filetype = FILETYPE.DIRECTORY;
    this.inode = inode;
    this.rightsBase = rightsBase;
    this.rightsInheriting = rightsInheriting;
    this.flags = 0;
    this.preopen = preopen === null ? null : encoder.encode(preopen);
  }

  get mayBlock() {
    return false;
  }

  read() {
    return -E.ISDIR;
  }

  write() {
    return -E.BADF;
  }

  pollRead() {
    return { nbytes: 0, hangup: false };
  }

  pollWrite() {
    return { nbytes: 0, hangup: false };
  }

  close() {}
}

/** Worker-to-host batches: items in order, their transferable buffers alongside. */
class Outbox {
  constructor(post) {
    this.post = post;
    this.items = [];
    this.transfer = [];
    this.bytes = 0;
  }

  push(item, transfer) {
    this.items.push(item);
    if (transfer === undefined) return;
    if (Array.isArray(transfer)) {
      for (const buffer of transfer) this.transfer.push(buffer);
    } else {
      this.transfer.push(transfer);
    }
  }

  get pending() {
    return this.items.length > 0;
  }

  flush() {
    if (this.items.length === 0) return;
    const items = this.items;
    const transfer = this.transfer;
    this.items = [];
    this.transfer = [];
    this.bytes = 0;
    this.post({ t: "batch", items }, transfer);
  }
}

/** A seeded PRNG (sfc32) for `random_get` when the host asks for determinism. */
function seededBytes(seed) {
  let a = seed >>> 0;
  let b = 0x9e3779b9;
  let c = 0x243f6a88;
  let d = 0xb7e15162;
  const next = () => {
    const t = (((a + b) | 0) + d) | 0;
    d = (d + 1) | 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) | 0;
    c = (c << 21) | (c >>> 11);
    c = (c + t) | 0;
    return t >>> 0;
  };
  for (let i = 0; i < 12; i++) next();
  return (dst) => {
    for (let i = 0; i < dst.length; i += 4) {
      const word = next();
      for (let j = 0; j < 4 && i + j < dst.length; j++) dst[i + j] = (word >>> (8 * j)) & 0xff;
    }
  };
}

function encodeStrings(strings) {
  const encoded = strings.map((s) => encoder.encode(`${s}\0`));
  return { encoded, bytes: encoded.reduce((sum, e) => sum + e.length, 0) };
}

export class Wasi {
  /**
   * @param {object} options
   * @param {object} options.config Process configuration from the host (see host.js).
   * @param {SharedArrayBuffer} options.ring The host-to-worker ring.
   * @param {SharedArrayBuffer} options.clock The WasiClock buffer.
   * @param {object|null} options.image The stored volume, or null for an empty one.
   * @param {(message: object, transfer: Transferable[]) => void} options.post postMessage to the host.
   */
  constructor({ config, ring, clock, image, post }) {
    this.config = config;
    this.post = post;
    this.ring = new RingReader(ring);
    this.clock = new ClockView(clock);
    this.outbox = new Outbox(post);
    this.started = performance.now();
    this.lastResume = this.started;
    this.lastOutFlush = this.started;
    this.requests = [];
    this.serving = false;
    this.memory = null;
    this.buffer = null;
    this.u8 = null;
    this.dv = null;
    this.warned = new Set();

    this.stats = {
      calls: Object.fromEntries(PREVIEW1_FUNCTIONS.map((name) => [name, 0])),
      unknownImports: [],
      poll: { calls: 0, immediate: 0, waits: 0, wakeups: 0, timeouts: 0, blockedMs: 0, busyMs: 0, maxSubscriptions: 0 },
      fs: { opens: 0, reads: 0, writes: 0, bytesRead: 0, bytesWritten: 0, syncs: 0 },
      journal: { flushes: 0, ops: 0, bytes: 0, waits: 0 },
      faults: 0,
    };
    this.journal = { lastFlush: this.started, deadline: Infinity, unacked: 0 };

    this.fs = new MemFs({ now: () => this.clock.realtimeNs(), journal: Boolean(config.persistent), maxBytes: config.maxBytes ?? Infinity });
    if (image) this.fs.load(image, (text) => this.warn(text));
    this.net = new VirtualNet({ outbox: this.outbox, window: config.window, backlog: config.backlog });
    this.random = config.seed === undefined || config.seed === null ? null : seededBytes(config.seed);

    const log = (fd) => (line) => this.outbox.push([OUT.LOG, fd, line]);
    this.fds = [
      new StdinFd(),
      new LogFd(1, log(1), config.maxLineBytes),
      new LogFd(2, log(2), config.maxLineBytes),
      new DirFd(this.fs.root, RIGHTS_ALL, RIGHTS_ALL, config.mountPath),
    ];
    const listenFds = config.listeners.map((port, index) => {
      const listener = this.net.addListener(index, port);
      listener.fd = this.fds.push(listener) - 1;
      return listener.fd;
    });
    this.dialer = new Dialer(this.net, (sock) => this.allocFd(sock));
    this.dialer.fd = this.fds.push(this.dialer) - 1;
    this.layout = { mount: 3, listeners: listenFds, dial: this.dialer.fd };

    const env = [
      ...config.env.filter((entry) => !/^KRABKA_(LISTEN_FDS|LISTEN_PORTS|DIAL_FD)=/.test(entry)),
      `KRABKA_LISTEN_FDS=${listenFds.join(",")}`,
      `KRABKA_LISTEN_PORTS=${config.listeners.join(",")}`,
      `KRABKA_DIAL_FD=${this.dialer.fd}`,
    ];
    this.envList = env;
    this.args = encodeStrings(config.args);
    this.env = encodeStrings(env);
  }

  // ---- plumbing -----------------------------------------------------------------------------

  /** The import object for `WebAssembly.instantiate`, with ENOSYS stubs for anything unknown. */
  imports(module) {
    const table = {};
    for (const name of PREVIEW1_FUNCTIONS) table[name] = this.#wrap(name, this[`$${name}`]);
    const imports = { wasi_snapshot_preview1: table };
    for (const { module: from, name, kind } of WebAssembly.Module.imports(module)) {
      if (kind !== "function") {
        if (!(from === "wasi_snapshot_preview1" && name in table)) {
          throw new Error(`the module imports a ${kind} ${from}.${name}; only functions can be provided`);
        }
        continue;
      }
      imports[from] ??= {};
      if (imports[from][name]) continue;
      const qualified = `${from}.${name}`;
      this.stats.unknownImports.push(qualified);
      this.stats.calls[qualified] = 0;
      this.warn(`unknown import ${qualified}: it answers ENOSYS`);
      imports[from][name] = () => {
        this.stats.calls[qualified]++;
        return E.NOSYS;
      };
    }
    return imports;
  }

  #wrap(name, impl) {
    const calls = this.stats.calls;
    return (...args) => {
      calls[name]++;
      if (this.memory === null) throw new ShimError(name, new Error("called before the module's memory was attached (a start section?)"));
      const buffer = this.memory.buffer;
      if (buffer !== this.buffer) {
        this.buffer = buffer;
        this.u8 = new Uint8Array(buffer);
        this.dv = new DataView(buffer);
      }
      try {
        return impl.apply(this, args);
      } catch (err) {
        if (err instanceof ProcExit) throw err;
        if (err instanceof RangeError && !/call stack/i.test(err.message)) {
          this.stats.faults++;
          if (!this.warned.has(`fault:${name}`)) {
            this.warned.add(`fault:${name}`);
            this.warn(`${name}: a guest pointer is out of bounds (${err.message}); answering EFAULT`);
          }
          return E.FAULT;
        }
        throw new ShimError(name, err);
      }
    };
  }

  /** Binds the instance's memory; call before `_start`. */
  attach(instance) {
    this.memory = instance.exports.memory;
    if (!(this.memory instanceof WebAssembly.Memory)) throw new Error("the module exports no memory");
  }

  warn(text) {
    this.outbox.push([OUT.WARN, text]);
  }

  allocFd(desc) {
    for (let fd = 3; fd < this.fds.length; fd++) {
      if (this.fds[fd] === undefined) {
        this.fds[fd] = desc;
        return fd;
      }
    }
    return this.fds.push(desc) - 1;
  }

  /** Bytes of guest memory at [ptr, ptr+len), bounds-checked (a RangeError answers EFAULT). */
  #bytes(ptr, len) {
    if (ptr + len > this.u8.length) throw new RangeError(`[${ptr}, ${ptr + len}) is outside memory`);
    return this.u8.subarray(ptr, ptr + len);
  }

  /** Decodes a guest path; returns the string or an errno. */
  #path(ptr, len) {
    try {
      return pathDecoder.decode(this.#bytes(ptr, len));
    } catch (err) {
      if (err instanceof RangeError) throw err;
      return E.ILSEQ;
    }
  }

  /** The iovec array at `ptr` as `[buf, len]` pairs, bounds-checked. */
  #iovecs(ptr, count) {
    const list = new Array(count);
    for (let i = 0; i < count; i++) {
      const buf = this.dv.getUint32(ptr + 8 * i, true);
      const len = this.dv.getUint32(ptr + 8 * i + 4, true);
      if (buf + len > this.u8.length) throw new RangeError(`iovec [${buf}, ${buf + len}) is outside memory`);
      list[i] = [buf, len];
    }
    return list;
  }

  #dirAt(fd) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind !== "dir") return E.NOTDIR;
    return d;
  }

  // ---- the host channel ---------------------------------------------------------------------

  /** Applies every record the host queued; serves requests afterwards. Returns how many there were. */
  drain() {
    const count = this.ring.drain((kind, id, bytes) => this.#record(kind, id, bytes));
    if (this.ring.takeSpaceRequest()) this.outbox.push([OUT.SPACE]);
    if (this.requests.length > 0 && !this.serving) this.#serveRequests();
    return count;
  }

  #record(kind, id, bytes) {
    switch (kind) {
      case REC.CONNECT:
        this.net.onConnect(id, u32(bytes));
        break;
      case REC.DATA:
        this.net.onData(id, bytes);
        break;
      case REC.END:
        this.net.onEnd(id);
        break;
      case REC.CLOSE:
        this.net.onClose(id, bytes[0] === 1);
        break;
      case REC.DIAL_OK:
        this.net.onDialOk(id);
        break;
      case REC.DIAL_FAIL:
        this.net.onDialFail(id, new DataView(bytes.buffer).getUint16(0, true));
        break;
      case REC.ACK:
        this.net.onAck(id, u32(bytes));
        break;
      case REC.JOURNAL_ACK:
        this.journal.unacked = Math.max(0, this.journal.unacked - u32(bytes));
        break;
      case REC.REQUEST:
        this.requests.push([id, JSON.parse(textDecoder.decode(bytes))]);
        break;
      default:
        this.warn(`unknown ring record kind ${kind}`);
    }
  }

  #serveRequests() {
    this.serving = true;
    try {
      while (this.requests.length > 0) {
        const [id, request] = this.requests.shift();
        let ok = true;
        let value = null;
        let transfer = [];
        try {
          value = this.#answer(request);
          if (value instanceof Uint8Array) transfer = [value.buffer];
        } catch (err) {
          ok = false;
          value = String(err && err.message ? err.message : err);
        }
        this.flushOutput();
        this.post({ t: "reply", id, ok, value }, transfer);
      }
    } finally {
      this.serving = false;
    }
  }

  #answer(request) {
    switch (request.op) {
      case "stats":
        return this.snapshot();
      case "flush":
        this.flushJournal();
        return true;
      case "readFile": {
        const node = this.#guestPath(request.path);
        return node && !node.isDir ? this.fs.readAll(node) : null;
      }
      case "list": {
        const node = this.#guestPath(request.path);
        if (!node || !node.isDir) return null;
        return [...node.entries.values()].map((child) => ({ name: child.name, type: child.type, size: child.isDir ? 0 : child.size }));
      }
      default:
        throw new Error(`unknown request ${request.op}`);
    }
  }

  /** A guest path ("/data/x") or a volume-relative one ("x") to its inode. */
  #guestPath(path) {
    const mount = this.config.mountPath.replace(/\/+$/, "");
    if (path === mount || path === `${mount}/`) return this.fs.root;
    if (path.startsWith(`${mount}/`)) return this.fs.lookup(path.slice(mount.length + 1));
    return path.startsWith("/") ? null : this.fs.lookup(path);
  }

  /** Posts every pending item: socket output, consumption reports, events and log lines. */
  flushOutput() {
    this.net.flushAll();
    this.outbox.flush();
    this.lastOutFlush = performance.now();
  }

  #maybeFlushOutput() {
    if (!this.outbox.pending && this.net.dirtyOut.size === 0) return;
    if (performance.now() - this.lastOutFlush >= OUTPUT_FLUSH_MS || this.outbox.bytes + this.net.pendingBytes >= OUTPUT_FLUSH_BYTES) {
      this.flushOutput();
    }
  }

  /** Real milliseconds until the journal is due (Infinity when there is none). */
  #journalWaitMs() {
    if (!this.fs.journalOn || !this.fs.journalPending) return Infinity;
    const due = Math.min(this.journal.deadline, this.journal.lastFlush + this.config.journalIntervalMs);
    return Math.max(0, due - performance.now());
  }

  #maybeFlushJournal() {
    if (!this.fs.journalOn || !this.fs.journalPending) return;
    if (this.#journalWaitMs() === 0 || this.fs.journalBytes >= JOURNAL_FLUSH_BYTES) this.flushJournal();
  }

  /** Sends the pending journal now; waits while too much of it is uncommitted. */
  flushJournal() {
    const batch = this.fs.take();
    this.journal.lastFlush = performance.now();
    this.journal.deadline = Infinity;
    if (!batch) return;
    this.outbox.push([OUT.JOURNAL, batch.ops, batch.bytes, batch.nextIno], batch.transfer);
    this.outbox.bytes += batch.bytes;
    this.journal.unacked += batch.bytes;
    this.stats.journal.flushes++;
    this.stats.journal.ops += batch.ops.length;
    this.stats.journal.bytes += batch.bytes;
    this.flushOutput();
    while (this.journal.unacked > this.config.journalMaxInFlight) {
      this.stats.journal.waits++;
      const seen = this.ring.epoch();
      if (this.drain() === 0) this.#sleep(seen, 1000);
      this.flushOutput();
    }
  }

  /** Blocks in `Atomics.wait` until the ring's epoch moves or `ms` passes. */
  #sleep(seen, ms) {
    const poll = this.stats.poll;
    poll.waits++;
    const started = performance.now();
    const result = this.ring.wait(seen, ms);
    poll.blockedMs += performance.now() - started;
    if (result === "timed-out") poll.timeouts++;
    else poll.wakeups++;
  }

  /** A blocking call found nothing to do: publish output, then sleep until the host writes or the journal is due. */
  #block() {
    const seen = this.ring.epoch();
    if (this.drain() > 0) return;
    this.flushOutput();
    this.#maybeFlushJournal();
    this.#sleep(seen, this.#journalWaitMs());
    this.drain();
  }

  /** Flushes everything; called when the guest exits or traps. */
  finish() {
    for (const fd of [1, 2]) {
      const d = this.fds[fd];
      if (d && d.kind === "log") d.flushPartial();
    }
    if (this.fs.journalOn) {
      const batch = this.fs.take();
      if (batch) {
        this.outbox.push([OUT.JOURNAL, batch.ops, batch.bytes, batch.nextIno], batch.transfer);
        this.stats.journal.flushes++;
      }
    }
    this.flushOutput();
  }

  snapshot() {
    const now = performance.now();
    return {
      uptimeMs: now - this.started,
      calls: { ...this.stats.calls },
      unknownImports: [...this.stats.unknownImports],
      poll: { ...this.stats.poll },
      fs: { ...this.stats.fs, ...this.fs.usage() },
      journal: { ...this.stats.journal, unackedBytes: this.journal.unacked, pending: this.fs.journalPending },
      net: { ...this.net.stats, sockets: this.net.sockets.size },
      fds: this.fds.filter(Boolean).length,
      faults: this.stats.faults,
      clock: {
        hostMs: Number(this.clock.hostNs() / 1000n) / 1000,
        realtimeMs: Number(this.clock.realtimeNs() / 1_000_000n),
        hostDriven: this.clock.hostDriven,
      },
    };
  }

  // ---- args, environment, clocks, randomness, process ---------------------------------------

  $args_sizes_get(argcPtr, sizePtr) {
    this.dv.setUint32(argcPtr, this.args.encoded.length, true);
    this.dv.setUint32(sizePtr, this.args.bytes, true);
    return E.SUCCESS;
  }

  $args_get(argvPtr, bufPtr) {
    return this.#strings(this.args, argvPtr, bufPtr);
  }

  $environ_sizes_get(countPtr, sizePtr) {
    this.dv.setUint32(countPtr, this.env.encoded.length, true);
    this.dv.setUint32(sizePtr, this.env.bytes, true);
    return E.SUCCESS;
  }

  $environ_get(environPtr, bufPtr) {
    return this.#strings(this.env, environPtr, bufPtr);
  }

  #strings(list, pointers, buf) {
    let at = buf;
    list.encoded.forEach((bytes, i) => {
      this.dv.setUint32(pointers + 4 * i, at, true);
      this.#bytes(at, bytes.length).set(bytes);
      at += bytes.length;
    });
    return E.SUCCESS;
  }

  $clock_res_get(id, resultPtr) {
    if (id > CLOCKID.THREAD_CPUTIME_ID) return E.INVAL;
    this.dv.setBigUint64(resultPtr, 1000n, true);
    return E.SUCCESS;
  }

  $clock_time_get(id, _precision, resultPtr) {
    let ns;
    switch (id) {
      case CLOCKID.MONOTONIC:
        ns = this.clock.monotonicNs();
        break;
      case CLOCKID.REALTIME:
        ns = this.clock.realtimeNs();
        break;
      case CLOCKID.PROCESS_CPUTIME_ID:
      case CLOCKID.THREAD_CPUTIME_ID:
        ns = BigInt(Math.round((performance.now() - this.started - this.stats.poll.blockedMs) * 1e6));
        break;
      default:
        return E.INVAL;
    }
    this.dv.setBigUint64(resultPtr, ns, true);
    this.#maybeFlushOutput();
    return E.SUCCESS;
  }

  $random_get(ptr, len) {
    const dst = this.#bytes(ptr, len);
    if (this.random) {
      this.random(dst);
    } else {
      for (let at = 0; at < len; at += 65536) crypto.getRandomValues(dst.subarray(at, Math.min(len, at + 65536)));
    }
    return E.SUCCESS;
  }

  $proc_exit(code) {
    throw new ProcExit(code >>> 0);
  }

  $proc_raise() {
    return E.NOSYS;
  }

  $sched_yield() {
    this.drain();
    this.flushOutput();
    return E.SUCCESS;
  }

  // ---- descriptors --------------------------------------------------------------------------

  $fd_close(fd) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    this.fds[fd] = undefined;
    d.close();
    return E.SUCCESS;
  }

  $fd_renumber(from, to) {
    const source = this.fds[from];
    const target = this.fds[to];
    if (!source || !target) return E.BADF;
    if (from === to) return E.SUCCESS;
    target.close();
    this.fds[to] = source;
    this.fds[from] = undefined;
    if ("fd" in source) source.fd = to;
    return E.SUCCESS;
  }

  $fd_fdstat_get(fd, ptr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    this.#bytes(ptr, FDSTAT_SIZE).fill(0);
    this.dv.setUint8(ptr, d.filetype);
    this.dv.setUint16(ptr + 2, d.flags, true);
    this.dv.setBigUint64(ptr + 8, d.rightsBase, true);
    this.dv.setBigUint64(ptr + 16, d.rightsInheriting, true);
    return E.SUCCESS;
  }

  $fd_fdstat_set_flags(fd, flags) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (flags & ~(FDFLAGS.APPEND | FDFLAGS.DSYNC | FDFLAGS.NONBLOCK | FDFLAGS.RSYNC | FDFLAGS.SYNC)) return E.INVAL;
    d.flags = flags;
    return E.SUCCESS;
  }

  $fd_fdstat_set_rights(fd, base, inheriting) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    const b = u64(base);
    const i = u64(inheriting);
    if ((b & ~d.rightsBase) !== 0n || (i & ~d.rightsInheriting) !== 0n) return E.NOTCAPABLE;
    d.rightsBase = b;
    d.rightsInheriting = i;
    return E.SUCCESS;
  }

  $fd_prestat_get(fd, ptr) {
    const d = this.fds[fd];
    if (!d || d.kind !== "dir" || d.preopen === null) return E.BADF;
    this.#bytes(ptr, 8).fill(0);
    this.dv.setUint8(ptr, PREOPENTYPE_DIR);
    this.dv.setUint32(ptr + 4, d.preopen.length, true);
    return E.SUCCESS;
  }

  $fd_prestat_dir_name(fd, ptr, len) {
    const d = this.fds[fd];
    if (!d || d.kind !== "dir" || d.preopen === null) return E.BADF;
    if (len < d.preopen.length) return E.NAMETOOLONG;
    this.#bytes(ptr, d.preopen.length).set(d.preopen);
    return E.SUCCESS;
  }

  #writeFilestat(ptr, st) {
    this.#bytes(ptr, FILESTAT_SIZE).fill(0);
    this.dv.setBigUint64(ptr, st.dev, true);
    this.dv.setBigUint64(ptr + 8, st.ino, true);
    this.dv.setUint8(ptr + 16, st.filetype);
    this.dv.setBigUint64(ptr + 24, st.nlink, true);
    this.dv.setBigUint64(ptr + 32, st.size, true);
    this.dv.setBigUint64(ptr + 40, st.atim, true);
    this.dv.setBigUint64(ptr + 48, st.mtim, true);
    this.dv.setBigUint64(ptr + 56, st.ctim, true);
  }

  $fd_filestat_get(fd, ptr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.inode) {
      this.#writeFilestat(ptr, this.fs.stat(d.inode));
    } else {
      this.#writeFilestat(ptr, { dev: 0n, ino: 0n, filetype: d.filetype, nlink: 1n, size: 0n, atim: 0n, mtim: 0n, ctim: 0n });
    }
    return E.SUCCESS;
  }

  $fd_filestat_set_size(fd, size) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind === "dir") return E.ISDIR;
    if (d.kind !== "file") return E.INVAL;
    if ((d.rightsBase & (RIGHTS.FD_FILESTAT_SET_SIZE | RIGHTS.FD_WRITE)) === 0n) return E.BADF;
    const n = safeNumber(size);
    if (n < 0) return E.FBIG;
    return this.fs.truncate(d.inode, n);
  }

  #times(inode, atim, mtim, flags) {
    if ((flags & FSTFLAGS.ATIM && flags & FSTFLAGS.ATIM_NOW) || (flags & FSTFLAGS.MTIM && flags & FSTFLAGS.MTIM_NOW)) return E.INVAL;
    const now = this.clock.realtimeNs();
    const atime = flags & FSTFLAGS.ATIM_NOW ? now : flags & FSTFLAGS.ATIM ? u64(atim) : null;
    const mtime = flags & FSTFLAGS.MTIM_NOW ? now : flags & FSTFLAGS.MTIM ? u64(mtim) : null;
    this.fs.setTimes(inode, atime, mtime);
    return E.SUCCESS;
  }

  $fd_filestat_set_times(fd, atim, mtim, flags) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (!d.inode) return E.SUCCESS;
    return this.#times(d.inode, atim, mtim, flags);
  }

  // ---- reads and writes ---------------------------------------------------------------------

  /** Reads through `op(dst, at, len)` into the iovecs; blocks on a blocking socket with nothing to read. */
  #readInto(d, iovs, count, nreadPtr, op) {
    let total = 0;
    for (const [buf, len] of this.#iovecs(iovs, count)) {
      if (len === 0) continue;
      let n = op(this.u8, buf, len);
      while (n === -E.AGAIN && total === 0 && d.mayBlock) {
        this.#block();
        n = op(this.u8, buf, len);
      }
      if (n < 0) {
        if (total > 0) break;
        return -n;
      }
      total += n;
      if (n < len) break;
    }
    this.dv.setUint32(nreadPtr, total, true);
    return E.SUCCESS;
  }

  /** Writes the iovecs through `op(src, at, len)`; blocks on a blocking socket with no room. */
  #writeFrom(d, iovs, count, nwrittenPtr, op) {
    let total = 0;
    for (const [buf, len] of this.#iovecs(iovs, count)) {
      if (len === 0) continue;
      let n = op(this.u8, buf, len);
      while (n === -E.AGAIN && total === 0 && d.mayBlock) {
        this.#block();
        n = op(this.u8, buf, len);
      }
      if (n < 0) {
        if (total > 0) break;
        return -n;
      }
      total += n;
      if (n < len) break;
    }
    this.dv.setUint32(nwrittenPtr, total, true);
    return E.SUCCESS;
  }

  $fd_read(fd, iovs, count, nreadPtr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    const errno = this.#readInto(d, iovs, count, nreadPtr, (dst, at, len) => d.read(dst, at, len));
    if (d.kind === "file" && errno === E.SUCCESS) {
      this.stats.fs.reads++;
      this.stats.fs.bytesRead += this.dv.getUint32(nreadPtr, true);
    }
    return errno;
  }

  $fd_write(fd, iovs, count, nwrittenPtr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    const errno = this.#writeFrom(d, iovs, count, nwrittenPtr, (src, at, len) => d.write(src, at, len));
    if (d.kind === "file") {
      if (errno === E.SUCCESS) {
        this.stats.fs.writes++;
        this.stats.fs.bytesWritten += this.dv.getUint32(nwrittenPtr, true);
      }
      this.#maybeFlushJournal();
    } else {
      this.#maybeFlushOutput();
    }
    return errno;
  }

  $fd_pread(fd, iovs, count, offset, nreadPtr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind === "dir") return E.ISDIR;
    if (d.kind !== "file") return E.SPIPE;
    if (!d.canRead()) return E.BADF;
    let at = safeNumber(offset);
    if (at < 0) return E.INVAL;
    const errno = this.#readInto(d, iovs, count, nreadPtr, (dst, ptr, len) => {
      const n = this.fs.readAt(d.inode, at, dst, ptr, len);
      at += n;
      return n;
    });
    this.stats.fs.reads++;
    this.stats.fs.bytesRead += this.dv.getUint32(nreadPtr, true);
    return errno;
  }

  $fd_pwrite(fd, iovs, count, offset, nwrittenPtr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind === "dir") return E.ISDIR;
    if (d.kind !== "file") return E.SPIPE;
    if (!d.canWrite()) return E.BADF;
    let at = safeNumber(offset);
    if (at < 0) return E.INVAL;
    const errno = this.#writeFrom(d, iovs, count, nwrittenPtr, (src, ptr, len) => {
      const n = this.fs.writeAt(d.inode, at, src, ptr, len);
      if (n > 0) at += n;
      return n;
    });
    this.stats.fs.writes++;
    this.stats.fs.bytesWritten += this.dv.getUint32(nwrittenPtr, true);
    this.#maybeFlushJournal();
    return errno;
  }

  $fd_seek(fd, offset, whence, resultPtr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind === "dir") return E.ISDIR;
    if (d.kind !== "file") return E.SPIPE;
    const delta = Number(BigInt.asIntN(64, offset));
    let base;
    if (whence === WHENCE.SET) base = 0;
    else if (whence === WHENCE.CUR) base = d.pos;
    else if (whence === WHENCE.END) base = d.inode.size;
    else return E.INVAL;
    const next = base + delta;
    if (next < 0 || !Number.isSafeInteger(next)) return E.INVAL;
    d.pos = next;
    this.dv.setBigUint64(resultPtr, BigInt(next), true);
    return E.SUCCESS;
  }

  $fd_tell(fd, resultPtr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind === "dir") return E.ISDIR;
    if (d.kind !== "file") return E.SPIPE;
    this.dv.setBigUint64(resultPtr, BigInt(d.pos), true);
    return E.SUCCESS;
  }

  #sync(fd) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind !== "file" && d.kind !== "dir") return E.INVAL;
    this.stats.fs.syncs++;
    // Durability is write-behind: fsync returns at once and only hurries the next journal flush.
    this.journal.deadline = Math.min(this.journal.deadline, performance.now() + SYNC_FLUSH_MS);
    return E.SUCCESS;
  }

  $fd_sync(fd) {
    return this.#sync(fd);
  }

  $fd_datasync(fd) {
    return this.#sync(fd);
  }

  $fd_advise(fd, _offset, _len, advice) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind !== "file") return E.SPIPE;
    if (advice > ADVICE_MAX) return E.INVAL;
    return E.SUCCESS;
  }

  $fd_allocate(fd, offset, len) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind !== "file") return d.kind === "dir" ? E.ISDIR : E.SPIPE;
    if (!d.canWrite()) return E.BADF;
    const start = safeNumber(offset);
    const size = safeNumber(len);
    if (start < 0 || size < 0) return E.FBIG;
    const end = start + size;
    return end > d.inode.size ? this.fs.truncate(d.inode, end) : E.SUCCESS;
  }

  $fd_readdir(fd, buf, bufLen, cookie, usedPtr) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind !== "dir") return E.NOTDIR;
    const dir = d.inode;
    const start = safeNumber(cookie);
    const out = this.#bytes(buf, bufLen);
    let used = 0;
    const put = (next, ino, type, name) => {
      if (used >= bufLen) return false;
      const entry = new Uint8Array(DIRENT_SIZE + name.length);
      const view = new DataView(entry.buffer);
      view.setBigUint64(0, BigInt(next), true);
      view.setBigUint64(8, BigInt(ino), true);
      view.setUint32(16, name.length, true);
      view.setUint8(20, type);
      entry.set(name, DIRENT_SIZE);
      const take = Math.min(entry.length, bufLen - used);
      out.set(take === entry.length ? entry : entry.subarray(0, take), used);
      used += take;
      return used < bufLen;
    };
    if (dir.nlink > 0) {
      let more = true;
      if (start <= 0) more = put(1, dir.ino, FILETYPE.DIRECTORY, encoder.encode("."));
      if (more && start <= 1) more = put(2, dir === this.fs.root ? dir.ino : dir.parent.ino, FILETYPE.DIRECTORY, encoder.encode(".."));
      if (more) {
        for (const child of dir.entries.values()) {
          if (child.seq < start) continue;
          if (!put(child.seq + 1, child.ino, child.filetype, child.encodedName)) break;
        }
      }
    }
    this.dv.setUint32(usedPtr, used, true);
    return E.SUCCESS;
  }

  // ---- paths --------------------------------------------------------------------------------

  $path_open(dirfd, _lookupFlags, pathPtr, pathLen, oflags, rightsBase, rightsInheriting, fdflags, fdPtr) {
    const dir = this.#dirAt(dirfd);
    if (typeof dir === "number") return dir;
    const path = this.#path(pathPtr, pathLen);
    if (typeof path === "number") return path;
    const base = u64(rightsBase);
    const how = {
      create: (oflags & OFLAGS.CREAT) !== 0,
      excl: (oflags & OFLAGS.EXCL) !== 0,
      trunc: (oflags & OFLAGS.TRUNC) !== 0,
      directory: (oflags & OFLAGS.DIRECTORY) !== 0,
      write: (base & RIGHTS.FD_WRITE) !== 0n,
    };
    const inode = this.fs.open(dir.inode, path, how);
    if (typeof inode === "number") return inode;
    this.stats.fs.opens++;
    const inheriting = u64(rightsInheriting);
    const desc = inode.isDir ? new DirFd(inode, base, inheriting) : new FileFd(this.fs, inode, base, inheriting, fdflags);
    this.dv.setUint32(fdPtr, this.allocFd(desc), true);
    return E.SUCCESS;
  }

  $path_create_directory(dirfd, pathPtr, pathLen) {
    const dir = this.#dirAt(dirfd);
    if (typeof dir === "number") return dir;
    const path = this.#path(pathPtr, pathLen);
    if (typeof path === "number") return path;
    return this.fs.mkdir(dir.inode, path);
  }

  $path_remove_directory(dirfd, pathPtr, pathLen) {
    const dir = this.#dirAt(dirfd);
    if (typeof dir === "number") return dir;
    const path = this.#path(pathPtr, pathLen);
    if (typeof path === "number") return path;
    return this.fs.rmdir(dir.inode, path);
  }

  $path_unlink_file(dirfd, pathPtr, pathLen) {
    const dir = this.#dirAt(dirfd);
    if (typeof dir === "number") return dir;
    const path = this.#path(pathPtr, pathLen);
    if (typeof path === "number") return path;
    return this.fs.unlink(dir.inode, path);
  }

  $path_rename(fromFd, fromPtr, fromLen, toFd, toPtr, toLen) {
    const from = this.#dirAt(fromFd);
    if (typeof from === "number") return from;
    const to = this.#dirAt(toFd);
    if (typeof to === "number") return to;
    const fromPath = this.#path(fromPtr, fromLen);
    if (typeof fromPath === "number") return fromPath;
    const toPath = this.#path(toPtr, toLen);
    if (typeof toPath === "number") return toPath;
    return this.fs.rename(from.inode, fromPath, to.inode, toPath);
  }

  #lookup(dirfd, pathPtr, pathLen) {
    const dir = this.#dirAt(dirfd);
    if (typeof dir === "number") return dir;
    const path = this.#path(pathPtr, pathLen);
    if (typeof path === "number") return path;
    const r = this.fs.resolve(dir.inode, path);
    if (typeof r === "number") return r;
    return r.node ?? E.NOENT;
  }

  $path_filestat_get(dirfd, _lookupFlags, pathPtr, pathLen, ptr) {
    const node = this.#lookup(dirfd, pathPtr, pathLen);
    if (typeof node === "number") return node;
    this.#writeFilestat(ptr, this.fs.stat(node));
    return E.SUCCESS;
  }

  $path_filestat_set_times(dirfd, _lookupFlags, pathPtr, pathLen, atim, mtim, flags) {
    const node = this.#lookup(dirfd, pathPtr, pathLen);
    if (typeof node === "number") return node;
    return this.#times(node, atim, mtim, flags);
  }

  $path_link() {
    return E.NOTSUP;
  }

  $path_symlink() {
    return E.NOTSUP;
  }

  $path_readlink() {
    return E.NOTSUP;
  }

  // ---- sockets ------------------------------------------------------------------------------

  $sock_accept(fd, flags, fdPtr) {
    const listener = this.fds[fd];
    if (!listener) return E.BADF;
    if (listener.kind !== "listener") return E.NOTSOCK;
    let sock = listener.accept();
    while (!sock) {
      if (!listener.mayBlock || flags & FDFLAGS.NONBLOCK) return E.AGAIN;
      this.#block();
      sock = listener.accept();
    }
    sock.flags = flags & FDFLAGS.NONBLOCK;
    sock.fd = this.allocFd(sock);
    this.net.stats.accepted++;
    this.outbox.push([OUT.ACCEPT, sock.id, sock.fd]);
    this.dv.setUint32(fdPtr, sock.fd, true);
    return E.SUCCESS;
  }

  $sock_recv(fd, iovs, count, riFlags, lenPtr, roFlagsPtr) {
    const sock = this.fds[fd];
    if (!sock) return E.BADF;
    if (sock.kind !== "socket") return E.NOTSOCK;
    const list = this.#iovecs(iovs, count);
    const want = list.reduce((sum, [, len]) => sum + len, 0);
    const peek = (riFlags & RIFLAGS.PEEK) !== 0;
    const waitAll = (riFlags & RIFLAGS.WAITALL) !== 0;
    this.dv.setUint16(roFlagsPtr, 0, true);
    if (want === 0) {
      this.dv.setUint32(lenPtr, 0, true);
      return E.SUCCESS;
    }
    // A blocking socket waits for data (for all of it with WAITALL) or for its end.
    if (sock.mayBlock) {
      while (sock.state === "connecting" || (!sock.ended && sock.readable < (waitAll ? want : 1))) this.#block();
    }
    let total = 0;
    if (peek) {
      const scratch = new Uint8Array(want);
      const n = sock.read(scratch, 0, want, true);
      if (n < 0) return -n;
      for (const [buf, len] of list) {
        const take = Math.min(len, n - total);
        if (take <= 0) break;
        this.u8.set(scratch.subarray(total, total + take), buf);
        total += take;
      }
    } else {
      for (const [buf, len] of list) {
        if (len === 0) continue;
        const n = sock.read(this.u8, buf, len);
        if (n < 0) {
          if (total > 0) break;
          return -n;
        }
        total += n;
        if (n < len) break;
      }
    }
    this.dv.setUint32(lenPtr, total, true);
    return E.SUCCESS;
  }

  $sock_send(fd, iovs, count, _siFlags, lenPtr) {
    const sock = this.fds[fd];
    if (!sock) return E.BADF;
    if (sock.kind !== "socket") return E.NOTSOCK;
    const errno = this.#writeFrom(sock, iovs, count, lenPtr, (src, at, len) => sock.write(src, at, len));
    this.#maybeFlushOutput();
    return errno;
  }

  $sock_shutdown(fd, how) {
    const d = this.fds[fd];
    if (!d) return E.BADF;
    if (d.kind === "listener") return E.NOTCONN;
    if (d.kind !== "socket") return E.NOTSOCK;
    if (how === 0 || how & ~(SDFLAGS.RD | SDFLAGS.WR)) return E.INVAL;
    return d.shutdown(how);
  }

  // ---- poll_oneoff ----------------------------------------------------------------------------

  $poll_oneoff(inPtr, outPtr, nsubs, neventsPtr) {
    if (nsubs === 0) return E.INVAL;
    const poll = this.stats.poll;
    poll.calls++;
    poll.busyMs += performance.now() - this.lastResume;
    poll.maxSubscriptions = Math.max(poll.maxSubscriptions, nsubs);
    this.#bytes(inPtr, nsubs * SUBSCRIPTION_SIZE);
    this.#bytes(outPtr, nsubs * EVENT_SIZE);
    const dv = this.dv;
    const subs = new Array(nsubs);
    let hasClock = false;
    for (let i = 0; i < nsubs; i++) {
      const at = inPtr + i * SUBSCRIPTION_SIZE;
      const userdata = dv.getBigUint64(at, true);
      const tag = dv.getUint8(at + 8);
      if (tag === EVENTTYPE.CLOCK) {
        const clockId = dv.getUint32(at + 16, true);
        const timeout = dv.getBigUint64(at + 24, true);
        const flags = dv.getUint16(at + 40, true);
        const deadline = this.#deadline(clockId, timeout, flags);
        subs[i] = deadline === null ? { userdata, tag, error: E.INVAL } : { userdata, tag, deadline, error: 0 };
        hasClock ||= deadline !== null;
      } else if (tag === EVENTTYPE.FD_READ || tag === EVENTTYPE.FD_WRITE) {
        subs[i] = { userdata, tag, fd: dv.getUint32(at + 16, true), error: 0 };
      } else {
        subs[i] = { userdata, tag, error: E.INVAL };
      }
    }
    let waited = false;
    for (;;) {
      const seen = this.ring.epoch();
      this.drain();
      const count = this.#collect(subs, outPtr, hasClock);
      if (count > 0) {
        this.flushOutput();
        this.#maybeFlushJournal();
        dv.setUint32(neventsPtr, count, true);
        if (!waited) poll.immediate++;
        this.lastResume = performance.now();
        return E.SUCCESS;
      }
      this.flushOutput();
      this.#maybeFlushJournal();
      let waitMs = this.#journalWaitMs();
      if (hasClock) for (const sub of subs) if (sub.deadline !== undefined) waitMs = Math.min(waitMs, this.clock.msUntil(sub.deadline));
      waited = true;
      this.#sleep(seen, waitMs);
    }
  }

  /** A clock subscription's deadline in host nanoseconds, or null for an unknown clock. */
  #deadline(clockId, timeout, flags) {
    if (clockId !== CLOCKID.MONOTONIC && clockId !== CLOCKID.REALTIME) return null;
    if (flags & SUBCLOCKFLAGS_ABSTIME) {
      this.clock.refresh();
      return clockId === CLOCKID.MONOTONIC ? timeout - MONOTONIC_OFFSET_NS : timeout - this.clock.baseNs;
    }
    return this.clock.hostNs() + timeout;
  }

  /** Writes an event for every ready subscription; returns how many. */
  #collect(subs, outPtr, hasClock) {
    const now = hasClock ? this.clock.hostNs() : 0n;
    let count = 0;
    const event = (sub, error, nbytes, flags) => {
      const at = outPtr + count * EVENT_SIZE;
      this.u8.fill(0, at, at + EVENT_SIZE);
      this.dv.setBigUint64(at, sub.userdata, true);
      this.dv.setUint16(at + 8, error, true);
      this.dv.setUint8(at + 10, sub.tag);
      this.dv.setBigUint64(at + 16, BigInt(nbytes), true);
      this.dv.setUint16(at + 24, flags, true);
      count++;
    };
    for (const sub of subs) {
      if (sub.error) {
        event(sub, sub.error, 0, 0);
      } else if (sub.tag === EVENTTYPE.CLOCK) {
        if (now >= sub.deadline) event(sub, 0, 0, 0);
      } else {
        const d = this.fds[sub.fd];
        if (!d) {
          event(sub, E.BADF, 0, 0);
          continue;
        }
        const ready = sub.tag === EVENTTYPE.FD_READ ? d.pollRead() : d.pollWrite();
        if (ready) event(sub, 0, ready.nbytes, ready.hangup ? EVENTRWFLAGS_HANGUP : 0);
      }
    }
    return count;
  }
}
