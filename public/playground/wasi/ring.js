// A single-producer (host) / single-consumer (worker) byte ring over a
// SharedArrayBuffer. Records are `[u32 kind][u32 id][u32 len][len bytes]` and
// wrap byte-wise at the end of the buffer. The host writes records, then bumps
// `RING_EPOCH` and notifies once per batch; the worker drains every record
// between polls and blocks with `Atomics.wait` on the epoch.
//
// The ring is bounded. When a record does not fit, the host keeps it queued,
// sets `RING_WANT_SPACE`, and the worker posts `{ t: "space" }` after draining.

import {
  RECORD_HEADER_BYTES,
  RING_CAPACITY,
  RING_EPOCH,
  RING_HEAD,
  RING_HEADER_BYTES,
  RING_TAIL,
  RING_WANT_SPACE,
} from "./protocol.js";

/** A new ring buffer with `capacity` data bytes. */
export function createRing(capacity) {
  const sab = new SharedArrayBuffer(RING_HEADER_BYTES + capacity);
  new Int32Array(sab, 0, RING_HEADER_BYTES / 4)[RING_CAPACITY] = capacity;
  return sab;
}

class RingBase {
  constructor(sab) {
    this.ctrl = new Int32Array(sab, 0, RING_HEADER_BYTES / 4);
    this.cap = this.ctrl[RING_CAPACITY];
    this.data = new Uint8Array(sab, RING_HEADER_BYTES, this.cap);
    this.header = new Uint8Array(RECORD_HEADER_BYTES);
    this.headerView = new DataView(this.header.buffer);
  }

  /** Copies `bytes` in at `offset`, wrapping; returns the offset after them. */
  put(offset, bytes) {
    const first = Math.min(bytes.length, this.cap - offset);
    this.data.set(first === bytes.length ? bytes : bytes.subarray(0, first), offset);
    if (first < bytes.length) this.data.set(bytes.subarray(first), 0);
    return (offset + bytes.length) % this.cap;
  }

  /** Copies `len` bytes out from `offset` into `out`, wrapping; returns the offset after them. */
  get(offset, len, out) {
    const first = Math.min(len, this.cap - offset);
    out.set(this.data.subarray(offset, offset + first), 0);
    if (first < len) out.set(this.data.subarray(0, len - first), first);
    return (offset + len) % this.cap;
  }
}

export class RingWriter extends RingBase {
  /** Payload bytes the next record can carry (0 when not even a header fits). */
  room() {
    const head = this.ctrl[RING_HEAD];
    const tail = Atomics.load(this.ctrl, RING_TAIL);
    const used = (head - tail + this.cap) % this.cap;
    return Math.max(0, this.cap - 1 - used - RECORD_HEADER_BYTES);
  }

  /** Writes one record if it fits; otherwise asks the worker for a space notice and returns false. */
  tryWrite(kind, id, payload) {
    if (payload.length > this.room()) {
      Atomics.store(this.ctrl, RING_WANT_SPACE, 1);
      return false;
    }
    this.headerView.setUint32(0, kind, true);
    this.headerView.setUint32(4, id >>> 0, true);
    this.headerView.setUint32(8, payload.length, true);
    let offset = this.put(this.ctrl[RING_HEAD], this.header);
    if (payload.length > 0) offset = this.put(offset, payload);
    Atomics.store(this.ctrl, RING_HEAD, offset);
    return true;
  }

  /** Asks the worker to post a space notice after its next drain. */
  requestSpace() {
    Atomics.store(this.ctrl, RING_WANT_SPACE, 1);
  }

  /** Wakes the worker. */
  notify() {
    Atomics.add(this.ctrl, RING_EPOCH, 1);
    Atomics.notify(this.ctrl, RING_EPOCH);
  }
}

export class RingReader extends RingBase {
  epoch() {
    return Atomics.load(this.ctrl, RING_EPOCH);
  }

  /** Calls `handle(kind, id, bytes)` for every queued record; returns how many there were. */
  drain(handle) {
    const head = Atomics.load(this.ctrl, RING_HEAD);
    let tail = this.ctrl[RING_TAIL];
    let count = 0;
    while (tail !== head) {
      tail = this.get(tail, RECORD_HEADER_BYTES, this.header);
      const kind = this.headerView.getUint32(0, true);
      const id = this.headerView.getUint32(4, true);
      const len = this.headerView.getUint32(8, true);
      const bytes = new Uint8Array(len);
      if (len > 0) tail = this.get(tail, len, bytes);
      Atomics.store(this.ctrl, RING_TAIL, tail);
      count++;
      handle(kind, id, bytes);
    }
    return count;
  }

  /** True once per host request for a space notice. */
  takeSpaceRequest() {
    return Atomics.exchange(this.ctrl, RING_WANT_SPACE, 0) === 1;
  }

  /** Blocks until the epoch moves past `seen` or `timeoutMs` passes. */
  wait(seen, timeoutMs) {
    return Atomics.wait(this.ctrl, RING_EPOCH, seen, timeoutMs);
  }
}
