// The virtual clock guests read through `clock_time_get` and `poll_oneoff`.
//
// A `WasiClock` lives on the main thread and owns a small SharedArrayBuffer;
// every worker attached to it reads the buffer. Two modes:
//
// - "real": host time runs at wall-clock speed from an anchor.
// - "host": host time moves only when the host calls `set()` or `advance()`.
//   Guest timers fire when it advances past their deadlines and stay silent
//   while it does not, so pausing the host's clock pauses the guest's timers.
//
// Guests see MONOTONIC = MONOTONIC_OFFSET_NS + host time and REALTIME =
// realtime base + host time. Several processes can share one clock (the lab
// drives a whole cluster with one); each change wakes every attached worker.

import {
  CLOCK_ANCHOR_NS,
  CLOCK_ANCHOR_WALL,
  CLOCK_BYTES,
  CLOCK_HOST,
  CLOCK_MODE,
  CLOCK_REAL,
  CLOCK_REALTIME_BASE,
  CLOCK_SEQ,
  MONOTONIC_OFFSET_NS,
} from "./protocol.js";

/** Wall time in whole microseconds since the Unix epoch, from the shared monotonic clock. */
export function wallMicros() {
  return BigInt(Math.round((performance.timeOrigin + performance.now()) * 1000));
}

/** Milliseconds (fractions allowed) to nanoseconds, exactly for whole milliseconds. */
export function msToNs(ms) {
  if (!Number.isFinite(ms)) throw new RangeError(`not a time: ${ms}`);
  const whole = Math.floor(ms);
  return BigInt(whole) * 1_000_000n + BigInt(Math.round((ms - whole) * 1e6));
}

/** Nanoseconds to milliseconds, to the microsecond. */
export function nsToMs(ns) {
  return Number(ns / 1000n) / 1000;
}

export class WasiClock {
  /**
   * @param {object} [options]
   * @param {"real"|"host"} [options.mode] "real" (default) runs at wall speed; "host" moves only when told.
   * @param {number} [options.timeMs] Host time to start from, in ms (default 0).
   * @param {number} [options.realtimeBaseMs] The guest's REALTIME at host time 0, in ms since the
   *   Unix epoch (default: now minus `timeMs`, so REALTIME starts at the wall clock).
   */
  constructor({ mode = "real", timeMs = 0, realtimeBaseMs } = {}) {
    if (mode !== "real" && mode !== "host") throw new TypeError(`clock mode must be "real" or "host", not ${mode}`);
    this.buffer = new SharedArrayBuffer(CLOCK_BYTES);
    this.i32 = new Int32Array(this.buffer, 0, 2);
    this.i64 = new BigInt64Array(this.buffer, 8, 3);
    this.subscribers = new Set();
    this.#publish(mode === "host" ? CLOCK_HOST : CLOCK_REAL, msToNs(timeMs), msToNs(realtimeBaseMs ?? Date.now() - timeMs));
  }

  /** "real" or "host". */
  get mode() {
    return Atomics.load(this.i32, CLOCK_MODE) === CLOCK_HOST ? "host" : "real";
  }

  /** Host time now, in milliseconds. */
  now() {
    return nsToMs(this.nowNs());
  }

  /** Host time now, in nanoseconds. */
  nowNs() {
    const anchor = Atomics.load(this.i64, CLOCK_ANCHOR_NS);
    if (Atomics.load(this.i32, CLOCK_MODE) === CLOCK_HOST) return anchor;
    return anchor + (wallMicros() - Atomics.load(this.i64, CLOCK_ANCHOR_WALL)) * 1000n;
  }

  /** The guest's REALTIME clock now, in ms since the Unix epoch. */
  realtime() {
    return nsToMs(Atomics.load(this.i64, CLOCK_REALTIME_BASE) + this.nowNs());
  }

  /** Moves host time to `ms`. Time never goes backwards. In "real" mode the clock keeps running from there. */
  set(ms) {
    const target = msToNs(ms);
    const now = this.nowNs();
    if (target < now) throw new RangeError(`the clock cannot go backwards (now ${nsToMs(now)} ms, asked for ${ms} ms)`);
    this.#publish(Atomics.load(this.i32, CLOCK_MODE), target);
  }

  /** Moves host time forward by `ms`. */
  advance(ms) {
    if (!(ms >= 0)) throw new RangeError(`advance needs a non-negative duration, not ${ms}`);
    this.#publish(Atomics.load(this.i32, CLOCK_MODE), this.nowNs() + msToNs(ms));
  }

  /** Switches to "host" mode at the current time: from now on only set() and advance() move it. */
  freeze() {
    this.#publish(CLOCK_HOST, this.nowNs());
  }

  /** Switches to "real" mode: host time runs at wall speed from its current value. */
  run() {
    this.#publish(CLOCK_REAL, this.nowNs());
  }

  /** Calls `fn()` after every change; returns the unsubscribe function. Processes use it to wake their workers. */
  subscribe(fn) {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }

  #publish(mode, anchorNs, realtimeBaseNs) {
    Atomics.add(this.i32, CLOCK_SEQ, 1); // odd: a write is in progress
    Atomics.store(this.i32, CLOCK_MODE, mode);
    Atomics.store(this.i64, CLOCK_ANCHOR_WALL, wallMicros());
    Atomics.store(this.i64, CLOCK_ANCHOR_NS, anchorNs);
    if (realtimeBaseNs !== undefined) Atomics.store(this.i64, CLOCK_REALTIME_BASE, realtimeBaseNs);
    Atomics.add(this.i32, CLOCK_SEQ, 1); // even again
    for (const fn of this.subscribers) fn();
  }
}

/** The worker's read-only view of a `WasiClock` buffer. */
export class ClockView {
  constructor(buffer) {
    this.i32 = new Int32Array(buffer, 0, 2);
    this.i64 = new BigInt64Array(buffer, 8, 3);
    this.seq = -1;
    this.refresh();
  }

  /** Re-reads the fields when the host changed them (a seqlock read). */
  refresh() {
    if (Atomics.load(this.i32, CLOCK_SEQ) === this.seq) return;
    for (;;) {
      const seq = Atomics.load(this.i32, CLOCK_SEQ);
      if (seq & 1) continue;
      const mode = Atomics.load(this.i32, CLOCK_MODE);
      const anchorWall = Atomics.load(this.i64, CLOCK_ANCHOR_WALL);
      const anchorNs = Atomics.load(this.i64, CLOCK_ANCHOR_NS);
      const baseNs = Atomics.load(this.i64, CLOCK_REALTIME_BASE);
      if (Atomics.load(this.i32, CLOCK_SEQ) !== seq) continue;
      this.seq = seq;
      this.hostDriven = mode === CLOCK_HOST;
      this.anchorWall = anchorWall;
      this.anchorNs = anchorNs;
      this.baseNs = baseNs;
      return;
    }
  }

  hostNs() {
    this.refresh();
    if (this.hostDriven) return this.anchorNs;
    return this.anchorNs + (wallMicros() - this.anchorWall) * 1000n;
  }

  monotonicNs() {
    return MONOTONIC_OFFSET_NS + this.hostNs();
  }

  realtimeNs() {
    const host = this.hostNs();
    return this.baseNs + host;
  }

  /** Real milliseconds until host time reaches `targetNs`: 0 when it has, Infinity while the host drives it. */
  msUntil(targetNs) {
    const now = this.hostNs();
    if (now >= targetNs) return 0;
    if (this.hostDriven) return Infinity;
    return Number(targetNs - now) / 1e6;
  }
}
