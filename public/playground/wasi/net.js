// Virtual sockets, worker side: preopened listeners, the dialer, and the
// stream sockets behind accepted and dialed connections.
//
// Readiness is edge-triggered. mio's wasip1 backend hands every registration's
// read and write subscription to every `poll_oneoff`, and tokio keeps a
// readiness bit set until an operation returns EAGAIN. A level-triggered answer
// ("this socket is writable") would make `poll_oneoff` return at once forever
// and burn the worker. So each direction has an "armed" flag: `poll_oneoff`
// reports the direction once while it is armed and ready, and the socket
// re-arms when the guest meets EAGAIN or a short write (tokio clears its bit on
// both). Hangups (peer closed, dial refused, reset) are reported through the
// same edges, as events with the HANGUP flag and error 0: an event with a
// non-zero error makes mio fail the whole poll.
//
// Flow control, both directions bounded by `window` bytes per connection:
// - host to guest: the host keeps at most `window` bytes in flight; the worker
//   reports what the guest consumed (`OUT.RX`) so the host can send more;
// - guest to host: `fd_write` accepts at most `window` bytes the host has not
//   acknowledged (`REC.ACK`); beyond that it returns EAGAIN, and the socket
//   becomes writable again when an acknowledgement frees room.

import { ERRNO as E, FDFLAGS, FILETYPE, RIGHTS_CHARDEV, RIGHTS_SOCKET } from "./abi.js";
import { DIAL_ID_BASE, OUT } from "./protocol.js";

const encoder = new TextEncoder();
const MAX_DIAL_LINE = 1024;

/** Parses `host:port` or `[v6]:port`; null when malformed. */
export function parseAddress(text) {
  const t = text.trim();
  let host;
  let portText;
  if (t.startsWith("[")) {
    const close = t.indexOf("]");
    if (close < 0 || t[close + 1] !== ":") return null;
    host = t.slice(1, close);
    portText = t.slice(close + 2);
  } else {
    const colon = t.lastIndexOf(":");
    if (colon <= 0) return null;
    host = t.slice(0, colon);
    portText = t.slice(colon + 1);
  }
  if (!/^\d{1,5}$/.test(portText)) return null;
  const port = Number(portText);
  if (port < 1 || port > 65535 || host.length === 0) return null;
  return { host, port };
}

export class Listener {
  constructor(net, index, port) {
    this.kind = "listener";
    this.filetype = FILETYPE.SOCKET_STREAM;
    this.net = net;
    this.index = index;
    this.port = port;
    this.flags = 0;
    this.rightsBase = RIGHTS_SOCKET;
    this.rightsInheriting = RIGHTS_SOCKET;
    this.pending = []; // sockets waiting for sock_accept, oldest first
    this.readArmed = true;
    this.open = true;
    this.fd = -1;
  }

  get mayBlock() {
    return (this.flags & FDFLAGS.NONBLOCK) === 0;
  }

  pollRead() {
    if (!this.readArmed || this.pending.length === 0) return null;
    this.readArmed = false;
    return { nbytes: this.pending.length, hangup: false };
  }

  /** A listener is never writable. */
  pollWrite() {
    return null;
  }

  read() {
    return -E.NOTCONN;
  }

  write() {
    return -E.NOTCONN;
  }

  /** The next queued connection, or null (re-arming the read edge). */
  accept() {
    const sock = this.pending.shift();
    if (sock) return sock;
    this.readArmed = true;
    return null;
  }

  close() {
    this.open = false;
    for (const sock of this.pending) this.net.forget(sock, "no-listener");
    this.pending = [];
  }
}

export class Socket {
  constructor(net, id, state) {
    this.kind = "socket";
    this.filetype = FILETYPE.SOCKET_STREAM;
    this.net = net;
    this.id = id;
    this.state = state; // "connecting" | "open" | "failed" | "closed"
    this.flags = 0;
    this.rightsBase = RIGHTS_SOCKET;
    this.rightsInheriting = RIGHTS_SOCKET;
    this.fd = -1;
    this.rx = []; // received chunks; rx[rxIndex] starts at rxOffset
    this.rxIndex = 0;
    this.rxOffset = 0;
    this.rxBytes = 0;
    this.rxConsumed = 0; // consumed since the last report to the host
    this.peerEnd = false; // the host shut its side: EOF after rx
    this.peerClosed = false; // the host closed: writes fail with EPIPE
    this.peerReset = false; // the host reset: reads and writes fail with ECONNRESET
    this.resetReported = false;
    this.failErrno = 0; // dial refused or timed out
    this.failReported = false;
    this.readShut = false;
    this.writeShut = false;
    this.out = []; // guest output not yet posted
    this.outBytes = 0;
    this.txUnacked = 0; // posted, not yet acknowledged by the host
    this.readArmed = true;
    this.writeArmed = true;
  }

  get mayBlock() {
    return (this.flags & FDFLAGS.NONBLOCK) === 0;
  }

  get txRoom() {
    return this.net.window - this.txUnacked - this.outBytes;
  }

  /** Bytes the guest could read now, and whether the stream has ended (for sock_recv's wait-all). */
  get readable() {
    return this.rxBytes;
  }

  get ended() {
    return this.readShut || this.peerEnd || this.peerReset || this.state === "failed";
  }

  pollRead() {
    const hangup = this.peerEnd || this.peerReset || this.state === "failed";
    if (!this.readArmed || (this.rxBytes === 0 && !hangup && !this.readShut)) return null;
    this.readArmed = false;
    return { nbytes: this.rxBytes, hangup };
  }

  pollWrite() {
    if (this.state === "connecting") return null;
    const hangup = this.state === "failed" || this.peerClosed || this.peerReset;
    const room = this.txRoom;
    if (!this.writeArmed || (!hangup && (this.writeShut || room <= 0))) return null;
    this.writeArmed = false;
    return { nbytes: Math.max(0, room), hangup };
  }

  /** Copies up to `len` received bytes into `dst[at..]`; returns the count or a negated errno. */
  read(dst, at, len, peek = false) {
    if (this.state === "connecting") {
      this.readArmed = true;
      this.net.stats.readAgain++;
      return -E.AGAIN;
    }
    if (this.state === "failed") {
      if (this.failReported) return 0;
      this.failReported = true;
      return -this.failErrno;
    }
    if (this.readShut) return 0;
    if (this.rxBytes === 0) {
      if (this.peerReset && !this.resetReported) {
        this.resetReported = true;
        return -E.CONNRESET;
      }
      if (this.peerEnd || this.peerReset) return 0;
      this.readArmed = true;
      this.net.stats.readAgain++;
      return -E.AGAIN;
    }
    const n = Math.min(len, this.rxBytes);
    let done = 0;
    let index = this.rxIndex;
    let offset = this.rxOffset;
    while (done < n) {
      const chunk = this.rx[index];
      const take = Math.min(n - done, chunk.length - offset);
      dst.set(chunk.subarray(offset, offset + take), at + done);
      done += take;
      offset += take;
      if (offset === chunk.length) {
        index++;
        offset = 0;
      }
    }
    if (!peek) {
      for (let i = this.rxIndex; i < index; i++) this.rx[i] = undefined;
      this.rxIndex = index;
      this.rxOffset = offset;
      if (this.rxIndex > 64 && this.rxIndex * 2 > this.rx.length) {
        this.rx = this.rx.slice(this.rxIndex);
        this.rxIndex = 0;
      }
      this.rxBytes -= n;
      this.consumed(n);
    }
    return n;
  }

  /** Queues `len` bytes of guest output from `src[at..]`; returns the count or a negated errno. */
  write(src, at, len) {
    if (this.state === "connecting") {
      this.writeArmed = true;
      this.net.stats.writeAgain++;
      return -E.AGAIN;
    }
    if (this.state === "failed") return -this.failErrno;
    if (this.writeShut || this.state === "closed") return -E.PIPE;
    if (this.peerReset) return -E.CONNRESET;
    if (this.peerClosed) return -E.PIPE;
    const room = this.txRoom;
    if (room <= 0) {
      this.writeArmed = true;
      this.net.stats.writeAgain++;
      this.net.stats.writeBlocked++;
      return -E.AGAIN;
    }
    const n = Math.min(len, room);
    this.out.push(src.slice(at, at + n));
    this.outBytes += n;
    this.net.dirtyOut.add(this);
    if (n < len) {
      this.writeArmed = true; // tokio clears write readiness after a short write
      this.net.stats.shortWrites++;
    }
    return n;
  }

  /** Received bytes were consumed (read, or discarded after a read shutdown). */
  consumed(n) {
    this.rxConsumed += n;
    this.net.rxDirty.add(this);
  }

  shutdown(how) {
    if (this.state === "connecting" || this.state === "failed") return E.NOTCONN;
    if (how & 2 && !this.writeShut) {
      this.writeShut = true;
      this.net.flushSocket(this);
      this.net.outbox.push([OUT.END, this.id]);
    }
    if (how & 1 && !this.readShut) {
      this.readShut = true;
      const dropped = this.rxBytes;
      this.rx = [];
      this.rxIndex = 0;
      this.rxOffset = 0;
      this.rxBytes = 0;
      if (dropped > 0) this.consumed(dropped);
    }
    return E.SUCCESS;
  }

  close() {
    if (this.state === "closed") return;
    this.net.flushSocket(this);
    this.state = "closed";
    this.net.forget(this, "closed");
  }
}

/** The dialer: the guest writes `host:port\n` and reads back `<fd>\n` or `ERR <reason>\n`. */
export class Dialer {
  constructor(net, allocFd) {
    this.kind = "dialer";
    this.filetype = FILETYPE.CHARACTER_DEVICE;
    this.flags = 0;
    this.rightsBase = RIGHTS_CHARDEV;
    this.rightsInheriting = 0n;
    this.net = net;
    this.allocFd = allocFd;
    this.line = "";
    this.replies = [];
    this.fd = -1;
  }

  get mayBlock() {
    return false;
  }

  write(src, at, len) {
    for (let i = 0; i < len; i++) {
      const byte = src[at + i];
      if (byte !== 10) {
        if (this.line.length < MAX_DIAL_LINE) this.line += String.fromCharCode(byte);
        continue;
      }
      const line = this.line;
      this.line = "";
      this.replies.push(encoder.encode(`${this.#dial(line)}\n`));
    }
    return len;
  }

  #dial(line) {
    if (line.length >= MAX_DIAL_LINE) return "ERR address too long";
    const address = parseAddress(line);
    if (!address) return `ERR bad address ${JSON.stringify(line)}`;
    const sock = this.net.dial(address.host, address.port);
    sock.fd = this.allocFd(sock);
    return String(sock.fd);
  }

  read(dst, at, len) {
    const head = this.replies[0];
    if (!head) return -E.AGAIN;
    const n = Math.min(len, head.length);
    dst.set(head.subarray(0, n), at);
    if (n === head.length) this.replies.shift();
    else this.replies[0] = head.subarray(n);
    return n;
  }

  pollRead() {
    return this.replies.length > 0 ? { nbytes: this.replies[0].length, hangup: false } : null;
  }

  pollWrite() {
    return { nbytes: MAX_DIAL_LINE, hangup: false };
  }

  close() {}
}

/** stdin: always at its end. */
export class StdinFd {
  constructor() {
    this.kind = "stdin";
    this.filetype = FILETYPE.CHARACTER_DEVICE;
    this.flags = 0;
    this.rightsBase = RIGHTS_CHARDEV;
    this.rightsInheriting = 0n;
  }

  get mayBlock() {
    return false;
  }

  read() {
    return 0;
  }

  write() {
    return -E.BADF;
  }

  pollRead() {
    return { nbytes: 0, hangup: true };
  }

  pollWrite() {
    return null;
  }

  close() {}
}

/** stdout or stderr: UTF-8 split into lines, each handed to `emit(line)`. */
export class LogFd {
  constructor(fd, emit, maxLine) {
    this.kind = "log";
    this.fd = fd;
    this.filetype = FILETYPE.CHARACTER_DEVICE;
    this.flags = FDFLAGS.APPEND;
    this.rightsBase = RIGHTS_CHARDEV;
    this.rightsInheriting = 0n;
    this.emit = emit;
    this.maxLine = maxLine;
    this.decoder = new TextDecoder("utf-8");
    this.partial = "";
  }

  get mayBlock() {
    return false;
  }

  read() {
    return -E.BADF;
  }

  write(src, at, len) {
    this.partial += this.decoder.decode(src.subarray(at, at + len), { stream: true });
    let newline;
    while ((newline = this.partial.indexOf("\n")) >= 0) {
      const line = this.partial.slice(0, newline);
      this.partial = this.partial.slice(newline + 1);
      this.emit(line.endsWith("\r") ? line.slice(0, -1) : line);
    }
    while (this.partial.length > this.maxLine) {
      this.emit(this.partial.slice(0, this.maxLine));
      this.partial = this.partial.slice(this.maxLine);
    }
    return len;
  }

  /** Emits an unterminated last line (at exit). */
  flushPartial() {
    this.partial += this.decoder.decode();
    if (this.partial.length > 0) this.emit(this.partial);
    this.partial = "";
  }

  pollRead() {
    return null;
  }

  pollWrite() {
    return { nbytes: 65536, hangup: false };
  }

  close() {}
}

/** The worker's side of the virtual network: listeners, sockets, and the flow-control books. */
export class VirtualNet {
  constructor({ outbox, window, backlog }) {
    this.outbox = outbox;
    this.window = window;
    this.backlog = backlog;
    this.listeners = [];
    this.sockets = new Map(); // id -> Socket, accepted or not
    this.dirtyOut = new Set(); // sockets with unposted output
    this.rxDirty = new Set(); // sockets with unreported consumption
    this.nextDialId = DIAL_ID_BASE;
    this.stats = {
      bytesIn: 0,
      bytesOut: 0,
      accepted: 0,
      refusedConnects: 0,
      dials: 0,
      dialsRefused: 0,
      readAgain: 0,
      writeAgain: 0,
      writeBlocked: 0,
      shortWrites: 0,
      maxSockets: 0,
    };
  }

  addListener(index, port) {
    const listener = new Listener(this, index, port);
    this.listeners[index] = listener;
    return listener;
  }

  #track(sock) {
    this.sockets.set(sock.id, sock);
    this.stats.maxSockets = Math.max(this.stats.maxSockets, this.sockets.size);
  }

  /** Drops a socket from the books and tells the host why. */
  forget(sock, why) {
    this.sockets.delete(sock.id);
    this.dirtyOut.delete(sock);
    this.rxDirty.delete(sock);
    this.outbox.push([OUT.CLOSE, sock.id, why]);
  }

  // ---- records from the host ----------------------------------------------------------------

  onConnect(id, index) {
    const listener = this.listeners[index];
    if (!listener || !listener.open || listener.pending.length >= this.backlog) {
      this.stats.refusedConnects++;
      this.outbox.push([OUT.CLOSE, id, listener && listener.open ? "backlog" : "no-listener"]);
      return;
    }
    const sock = new Socket(this, id, "open");
    this.#track(sock);
    listener.pending.push(sock);
  }

  onData(id, bytes) {
    const sock = this.sockets.get(id);
    if (!sock) return;
    this.stats.bytesIn += bytes.length;
    if (sock.readShut) {
      sock.consumed(bytes.length);
      return;
    }
    sock.rx.push(bytes);
    sock.rxBytes += bytes.length;
  }

  onEnd(id) {
    const sock = this.sockets.get(id);
    if (sock) sock.peerEnd = true;
  }

  onClose(id, reset) {
    const sock = this.sockets.get(id);
    if (!sock) return;
    sock.peerEnd = true;
    sock.peerClosed = true;
    if (reset) sock.peerReset = true;
  }

  onDialOk(id) {
    const sock = this.sockets.get(id);
    if (sock && sock.state === "connecting") sock.state = "open";
  }

  onDialFail(id, errno) {
    const sock = this.sockets.get(id);
    if (!sock || sock.state !== "connecting") return;
    sock.state = "failed";
    sock.failErrno = errno || E.CONNREFUSED;
    this.stats.dialsRefused++;
  }

  onAck(id, n) {
    const sock = this.sockets.get(id);
    if (sock) sock.txUnacked = Math.max(0, sock.txUnacked - n);
  }

  /** A new outbound socket in state "connecting"; the host decides its fate. */
  dial(host, port) {
    const sock = new Socket(this, this.nextDialId, "connecting");
    this.nextDialId = this.nextDialId >= 0xffff_fff0 ? DIAL_ID_BASE : this.nextDialId + 1;
    this.#track(sock);
    this.stats.dials++;
    this.outbox.push([OUT.DIAL, sock.id, host, port]);
    return sock;
  }

  // ---- output -------------------------------------------------------------------------------

  /** Bytes of guest output not yet handed to the outbox. */
  get pendingBytes() {
    let total = 0;
    for (const sock of this.dirtyOut) total += sock.outBytes;
    return total;
  }

  /** Moves one socket's output into the outbox. */
  flushSocket(sock) {
    this.dirtyOut.delete(sock);
    if (sock.outBytes === 0) return;
    let data;
    if (sock.out.length === 1) {
      data = sock.out[0];
    } else {
      data = new Uint8Array(sock.outBytes);
      let at = 0;
      for (const chunk of sock.out) {
        data.set(chunk, at);
        at += chunk.length;
      }
    }
    this.outbox.push([OUT.DATA, sock.id, data], data.buffer);
    this.outbox.bytes += data.length;
    sock.txUnacked += data.length;
    this.stats.bytesOut += data.length;
    sock.out = [];
    sock.outBytes = 0;
  }

  /**
   * Whether a socket waits for the host to acknowledge its output: its last
   * write met a full window. An acknowledgement wakes only such a guest.
   */
  writeBlocked() {
    for (const sock of this.sockets.values()) {
      if (sock.state === "open" && sock.writeArmed && sock.txRoom <= 0 && !sock.writeShut && !sock.peerClosed) return true;
    }
    return false;
  }

  /** Moves all pending output and consumption reports into the outbox. */
  flushAll() {
    for (const sock of this.dirtyOut) this.flushSocket(sock);
    for (const sock of this.rxDirty) {
      if (sock.rxConsumed > 0) this.outbox.push([OUT.RX, sock.id, sock.rxConsumed]);
      sock.rxConsumed = 0;
    }
    this.rxDirty.clear();
  }
}
