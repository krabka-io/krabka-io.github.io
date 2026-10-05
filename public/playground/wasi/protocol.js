// The protocol between the host (host.js, main thread) and the worker
// (worker.js): shared-buffer layouts, ring record kinds and outbox tags.
//
// Host to worker: once the guest runs, the worker sits in `Atomics.wait` or in
// guest code and never returns to its event loop, so everything the host says
// goes through a SharedArrayBuffer ring (ring.js). The host bumps `EPOCH` and
// notifies after writing; the worker drains the ring whenever the guest polls
// or blocks.
//
// Worker to host: `postMessage` works while the worker is blocked, so the
// worker batches its output (socket data, events, log lines, file-system
// journal) into `{ t: "batch", items }` messages.

/** Int32 slots at the start of a process's ring buffer. */
export const RING_HEAD = 0; // next byte the host writes (host-owned)
export const RING_TAIL = 1; // next byte the worker reads (worker-owned)
export const RING_EPOCH = 2; // bumped on every notify; the worker waits on it
export const RING_CAPACITY = 3; // data bytes after the header
export const RING_WANT_SPACE = 4; // set by the host when a record did not fit
export const RING_HEADER_BYTES = 64;
export const RECORD_HEADER_BYTES = 12; // [u32 kind][u32 id][u32 len], little-endian

/** Ring record kinds (host to worker). */
export const REC = Object.freeze({
  CONNECT: 1, // id = connection; payload u32 listener index
  DATA: 2, // id = connection; payload = bytes for the guest
  END: 3, // id = connection; the host shut its write side (the guest reads EOF)
  CLOSE: 4, // id = connection; payload u8: 0 = closed, 1 = reset
  DIAL_OK: 5, // id = connection; the host accepted the guest's dial
  DIAL_FAIL: 6, // id = connection; payload u16 errno, then a UTF-8 reason
  ACK: 7, // id = connection; payload u32: guest output the host consumed
  JOURNAL_ACK: 8, // payload u32: journal bytes the host committed
  REQUEST: 9, // id = request; payload = UTF-8 JSON { op, ... }
});

/** Outbox item tags (worker to host); each item is an array `[tag, ...fields]`. */
export const OUT = Object.freeze({
  DATA: 1, // [DATA, id, Uint8Array]
  ACCEPT: 2, // [ACCEPT, id, fd]
  END: 3, // [END, id]: the guest shut down its write side
  CLOSE: 4, // [CLOSE, id, why]: the guest closed the socket, or refused it ("backlog", "no-listener")
  DIAL: 5, // [DIAL, id, host, port]
  RX: 6, // [RX, id, bytes]: bytes the guest consumed from the connection
  LOG: 7, // [LOG, fd, line]
  JOURNAL: 8, // [JOURNAL, ops, bytes]
  WARN: 9, // [WARN, text]
  SPACE: 10, // [SPACE]: the worker drained the ring after the host found it full
});

/** Clock buffer layout: Int32 [SEQ, MODE], then BigInt64 [anchorWallMicros, anchorNs, realtimeBaseNs]. */
export const CLOCK_SEQ = 0;
export const CLOCK_MODE = 1;
export const CLOCK_ANCHOR_WALL = 0; // BigInt64 index: wall time (µs since the Unix epoch) at the anchor
export const CLOCK_ANCHOR_NS = 1; // BigInt64 index: host time (ns) at the anchor
export const CLOCK_REALTIME_BASE = 2; // BigInt64 index: REALTIME minus host time (ns)
export const CLOCK_BYTES = 32;
export const CLOCK_REAL = 0;
export const CLOCK_HOST = 1;

/**
 * Per-process fault buffer (host writes, worker reads): Int32 [SEQ, PAUSED,
 * DISK_MODE, DISK_MS], then BigInt64 [skewNs] at byte 16. Unlike the clock it
 * belongs to one process, so the lab can pause one broker, skew its wall
 * clock or break its disk while the others share the clock.
 */
export const FAULT_SEQ = 0;
export const FAULT_PAUSED = 1; // 1: poll_oneoff and blocking calls hand the guest nothing, like SIGSTOP
export const FAULT_DISK_MODE = 2; // DISK.*
export const FAULT_DISK_MS = 3; // host ms one fd_sync / fd_datasync takes in DISK.SLOW
export const FAULT_SKEW = 0; // BigInt64 index (byte 16): added to REALTIME, in ns
export const FAULT_BYTES = 24;
export const DISK = Object.freeze({ OK: 0, SLOW: 1, FULL: 2, EIO: 3 });
export const DISK_MODES = Object.freeze(["ok", "slow", "full", "eio"]);

/**
 * The guest's MONOTONIC clock reads host time plus this offset (10^8 s), so
 * `Instant` arithmetic that reaches into the past, such as "now minus seven
 * days", cannot underflow in a freshly started guest.
 */
export const MONOTONIC_OFFSET_NS = 100_000_000n * 1_000_000_000n;

/** Connection ids the worker allocates for dials start here; host ids stay below. */
export const DIAL_ID_BASE = 0x8000_0000;

/** File contents are paged in memory and stored in IndexedDB in chunks of this size. */
export const PAGE_SIZE = 64 * 1024;

export const DEFAULTS = Object.freeze({
  ringBytes: 1 << 20, // host-to-worker ring
  window: 256 * 1024, // per connection and direction: bytes in flight before the sender waits
  highWaterMark: 1 << 20, // host-side send buffer before send() returns false
  chunkBytes: 64 * 1024, // largest DATA record
  backlog: 128, // queued connections per listener
  journalIntervalMs: 50, // shortest gap between two journal flushes
  journalMaxInFlight: 64 << 20, // journal bytes posted but not committed before the guest waits
  dialTimeoutMs: 30_000, // an ondial promise that has not settled by then refuses the dial
  mountPath: "/data",
  maxLineBytes: 16 * 1024, // longer stdout/stderr lines are split
});
