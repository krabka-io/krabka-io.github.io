# Browser WASI runtime

Runs a `wasm32-wasip1` command, such as the Krabka broker, in a dedicated Web
Worker. The worker gives the guest virtual sockets, a dialer for outbound
connections, a virtual clock and a file system. The file system lives in the
worker's memory and persists to IndexedDB. The Cluster Lab uses this runtime
to run real brokers in the page.

The runtime implements all of WASI preview 1 (`wasi_snapshot_preview1`) that a
tokio server needs. It blocks with a SharedArrayBuffer ring and
`Atomics.wait`, so the page must be cross-origin isolated.

| File | Role |
| --- | --- |
| `host.js` | The host API (main thread): `spawn`, `WasiProcess`, `Connection`, `pipe`, plus the re-exports below. |
| `clock.js` | `WasiClock`: the virtual clock, in real-time or host-driven mode. |
| `volumes.js` | The IndexedDB store: image loading, the journal writer, `usage`, `forget`, export and import, and the volume locks. |
| `worker.js` | The worker entry point. `host.js` starts it from a blob URL. |
| `wasi.js` | The preview-1 implementation: descriptors, `poll_oneoff`, blocking calls, and the flush policy. |
| `net.js` | Worker-side sockets: listeners, streams, the dialer, stdio, and flow control. |
| `fs.js` | The in-memory file system and its journal. |
| `ring.js`, `protocol.js`, `abi.js` | The host-to-worker ring, the message layouts, and the WASI constants. |
| `tar.js` | The ustar/PAX codec for volume export and import. |
| `/docs/lab/coi.js`, `/docs/lab/coi-sw.js` | Cross-origin isolation for the lab page on GitHub Pages (see below). |

The runtime is plain ES modules with no build step. `scripts/check-wasi.mjs`
tests it end to end. See [Testing](#testing).

## Cross-origin isolation

`spawn()` needs `crossOriginIsolated`. When the server sends
`Cross-Origin-Opener-Policy: same-origin` and
`Cross-Origin-Embedder-Policy: credentialless` or `require-corp`, the page is
already isolated. GitHub Pages cannot send these headers, so the lab page
calls the helper first:

```js
import { ensureCrossOriginIsolation } from "/docs/lab/coi.js";

const coi = await ensureCrossOriginIsolation();
if (!coi.isolated && !coi.reloading) showWhyTheLabCannotRunBrokers(coi.reason);
```

The helper registers `/docs/lab/coi-sw.js` with scope `/docs/lab/`, so no
other page on the site is affected. It then reloads the page once. The
service worker adds COOP and COEP to the documents and worker scripts it
serves, and leaves every other request alone.

COEP starts as `credentialless`, so the site's cross-origin fonts and images
keep loading. Some browsers do not isolate a page under `credentialless`,
such as Safari. There the helper switches the service worker to
`require-corp`, reloads once more, and remembers the choice for that browser.

The helper returns `{ isolated, via, coep, reason, reloading }`:

- `via` is `"headers"`, `"service-worker"` or `null`.
- `reason` says in words why isolation is unavailable.
- `reloading` is true when the page is about to reload. The promise resolves
  before the reload happens.

The helper never reloads more than twice. `removeCrossOriginIsolation()`
unregisters the service worker.

The lab page's URL is `/docs/lab/`: Astro builds `src/pages/docs/lab.astro`
to `dist/docs/lab/index.html`. GitHub Pages redirects `/docs/lab` to
`/docs/lab/`, and the helper does the same when a server does not.

Workers start from a blob URL that imports `worker.js`. A blob-URL worker
inherits the page's COEP policy, so it is isolated even though
`/playground/wasi/` is outside the service worker's scope.

## Host API

```js
import { spawn, pipe, WasiClock, usage, forget, exportVolume, importVolume } from "/playground/wasi/host.js";
```

### `spawn(options) -> Promise<WasiProcess>`

Compiles the module and caches it by URL. Takes the volume's lock and loads
the volume's image from IndexedDB. Then it starts the worker. The promise
resolves once the module is instantiated and `_start` is about to run.

`compileModule(module)` is the compile step on its own. When the module URL
cannot be fetched, it rejects with an error whose `status` is the HTTP
status, so a missing module (404) can be told apart from a broken one.

| Option | Default | Meaning |
| --- | --- | --- |
| `module` | (required) | A URL, `ArrayBuffer`, `Uint8Array` or `WebAssembly.Module` of a WASI command (it must export `_start`). |
| `name` | `guest-N` | A label for logs, errors and the worker. |
| `args` | `[name]` | argv, with the program name first. |
| `env` | `{}` | The environment, as an object or as `"KEY=VALUE"` strings. The runtime adds the `KRABKA_*` variables (see below). |
| `listeners` | `[]` | Listener ports. The guest gets one preopened listening socket per port, in this order. |
| `volume` | `null` | The IndexedDB volume id for the file system. `null` gives an ephemeral file system. |
| `mountPath` | `"/data"` | Where the volume appears in the guest. `"/"` also works. |
| `clock` | `{ mode: "real" }` | A shared `WasiClock`, or options for a private one. |
| `ondial` | `null` (refuse all) | `(host, port, dial) => Connection \| null \| Promise<...>`. See [Dials](#dials). |
| `window` | 256 KiB | The per-connection, per-direction flow-control window. |
| `highWaterMark` | 1 MiB | The host-side send buffer size above which `send()` returns false. |
| `ringBytes` | 1 MiB | The host-to-worker ring (at least 4 KiB). |
| `backlog` | 128 | The number of connections that can wait per listener. Beyond it, `connect()` is refused. |
| `journalIntervalMs` | 50 | The shortest gap between two journal flushes. |
| `journalMaxInFlight` | 64 MiB | The number of journal bytes that can be posted but not yet committed before the guest's file calls wait. |
| `dialTimeoutMs` | 30000 | An `ondial` promise that has not settled by then refuses the dial with `ETIMEDOUT`. |
| `maxBytes` | unlimited | The total file size above which writes fail with `ENOSPC`. |
| `seed` | none | A number makes `random_get` a seeded PRNG instead of `crypto.getRandomValues`. |
| `maxLineBytes` | 16 KiB | The length above which stdout and stderr lines are split. |

`WasiProcess.spawn(options)` is the same function. It throws right away when
the page is not isolated. `isolationProblem()` returns the reason as a
string, or `null` when the page is isolated.

### `WasiProcess`

The network methods:

- `connect(port) -> Connection` opens a connection to the listener on
  `port`. The guest sees it on its next accept. Connections queue in the
  listener's backlog until then.
- `send(conn, data)`, `end(conn)` and `close(conn, { reset })` are the same
  as the `Connection` methods.
- `ondial` is a settable property with the same role as the option.

The lifecycle methods:

- `kill() -> Promise` stops the worker at once, like pulling the plug. Any
  journal the worker had not yet posted is lost.
- `restart({ flush = true, flushTimeoutMs = 5000 }) -> Promise` creates a
  new worker on the same volume, clock and listeners. By default it first
  asks the guest to flush its journal. `{ flush: false }` restarts from what
  IndexedDB holds now, like a crash.
- `start() -> Promise` starts a process again after it exited, trapped or
  was killed.
- `exited` is a promise for the current worker's exit info.

The file methods:

- `flush() -> Promise` asks the guest to post its journal, then waits until
  IndexedDB has committed it.
- `readFile(path) -> Promise<Uint8Array | null>` reads a file live from the
  guest's memory while it runs. Otherwise it reads what the volume stores.
- `listFiles(path) -> Promise<[{ name, type, size }] | null>` lists a
  directory live.

Waiting for the guest:

- `quiesce() -> Promise<{ hostMs, deadlineMs } | null>` resolves once the
  guest has taken in every record the host sent it before the call (and any
  sent while it waits), its output has reached the host, and it is blocked
  in `poll_oneoff` or a blocking call with nothing ready. Bytes that wait
  behind a full window do not hold it up: the guest has a window of unread
  bytes on that connection, and not reading them is its own backpressure. `hostMs` is the
  clock's host time then; `deadlineMs` is the host time of the earliest
  clock the guest waits on, or null when it waits on none. It resolves null
  when the process stops first, and never while the guest keeps running, so
  race it with a timeout. With a host-driven clock this is how a host keeps a
  guest in lockstep: advance the clock or hand over input, wait for
  `quiesce()`, and the guest's answers carry the instant the input arrived.

The observability methods:

- `stats() -> Promise<{ name, state, incarnation, guest, host }>` returns the
  counters. `guest` comes from the worker:
  - `calls`: syscall counts per import;
  - `poll`: calls, waits, `blockedMs` and `busyMs`;
  - `net`: bytes in and out, EAGAIN counts, accepts and dials;
  - `fs`: files, bytes and syncs;
  - `journal`: flushes, bytes and backpressure waits;
  - `clock`.

  After an exit, `guest` is the final snapshot. `host` holds the host-side
  byte and connection counts, plus the IndexedDB writer's stats.
- `tail("stdout" | "stderr") -> string[]` returns the last 200 lines.

Properties: `name`, `state` (`"starting"`, `"running"`, `"exited"`,
`"killed"`, `"trapped"` or `"failed"`), `incarnation` (which goes up on each
`restart()`), `listeners`, `volume`, `clock`,
`layout` (`{ mount, listeners, dial }`, the guest's descriptor numbers) and
`startTimings` (`{ imageMs, bootMs, instantiateMs, totalMs }`).

`on(type, fn)` returns an unsubscribe function. `once(type)` returns a
promise. The events:

| Event | Payload |
| --- | --- |
| `stdout`, `stderr` | One line, as UTF-8 text, without the newline. |
| `exit` | `{ reason, code, error? }`. `reason` is `"return"`, `"exit"` (`proc_exit`), `"trap"`, `"kill"`, `"restart"` or `"failed"`. |
| `trap` | `{ name, message, stack, stderr }`, for a wasm trap. Panics trap because guests build with `panic = "abort"`; the panic message is in `stderr`. |
| `accept` | The `Connection` the guest just accepted. |
| `dial` | `{ host, port, accepted, conn }`. |
| `persisted` | `{ ops, bytes, ms }`, once a journal batch is committed. |
| `restart` | `{ incarnation }`. |
| `warn` | Text, such as an unknown import (reported once, at instantiation) or a guest pointer out of bounds. Without a listener, warnings go to `console.warn`. |
| `error` | An `Error`, such as an IndexedDB failure or an `ondial` that threw. |

### `Connection`

`Connection` models one TCP connection. Its properties are `id`, `direction`
(`"inbound"` or `"outbound"`), `port`, `host` (for outbound connections),
`state` (`"pending"`, `"connecting"`, `"open"`, `"closing"` or `"closed"`),
`bufferedAmount`, `bytesSent`, `bytesReceived`, `createdAt` and `acceptedAt`.

The methods:

- `send(data) -> boolean` queues bytes for the guest. `data` is a
  `Uint8Array`, a view, an `ArrayBuffer` or a string, and it is copied.
  `send()` returns false once `bufferedAmount` passes the high-water mark:
  stop and wait for `drain`. On a closed connection it returns false and
  drops the data.
- `write(data) -> Promise` works like `send()`, but resolves once the buffer
  drops below the high-water mark. It rejects on a closed connection.
- `end()` half-closes the connection. The guest reads the end of the stream
  after the queued bytes, and it can still write.
- `close({ reset = false })` closes the connection. The guest reads the end
  of the stream, and its writes fail with `EPIPE`. With `reset`, reads and
  writes fail with `ECONNRESET` instead.
- `pause()` and `resume()` stop and restart `data` events.

The events:

- `data` delivers a `Uint8Array` from the guest.
- `end` means the guest shut down its write side.
- `close` delivers `{ reason, reset }`. The reason is `"guest"`, `"host"`,
  `"kill"`, `"exit"`, `"trap"`, `"restart"`, `"refused: backlog full"`,
  `"refused: no listener"` or `"refused: <errno>"`.
- `drain` means the send buffer fell below the high-water mark.
- `accept` (inbound) means the guest accepted the connection.
- `open` (outbound) means the host accepted the dial.

Data from the guest is delivered in order. When no `data` handler is
attached, or the connection is paused, the data waits. The guest is not
acknowledged until its data is delivered, so it stops after one window of
unread output: this is backpressure.

### `pipe(a, b) -> unpipe`

`pipe()` joins two connections, for example a guest's dial and another
guest's listener. Bytes flow both ways with backpressure: a full destination
pauses the source until it drains. An `end` or a `close` on one side is
passed to the other.

### Dials

The guest dials by writing `host:port` to its dialer (see the guest contract
below). The host sees `ondial(host, port, dial)`, where
`dial = { host, port, accept(), refuse(code) }`. To accept, return
`dial.accept()`, which is the new outbound `Connection`, from the callback or
from a promise. To refuse with `ECONNREFUSED`, return `null`. To refuse with
another code, call `dial.refuse("ETIMEDOUT")` (or `"EHOSTUNREACH"`, and so
on). A cluster routes one guest's dial to another guest like this:

```js
brokerA.ondial = (host, port, dial) => {
  const target = brokers.get(`${host}:${port}`);
  if (!target) return null;
  const conn = dial.accept();
  pipe(conn, target.process.connect(port));
  return conn;
};
```

### `WasiClock`

```js
const clock = new WasiClock({ mode: "host", timeMs: 0, realtimeBaseMs: Date.UTC(2030, 0, 1) });
const a = await spawn({ module, clock });
const b = await spawn({ module, clock }); // one clock for the whole cluster
clock.advance(100); // both guests' timers fire up to 100 ms
```

A `WasiClock` has two modes:

- In `"real"` mode (the default), host time runs at wall-clock speed.
- In `"host"` mode, host time moves only when the host calls `set(ms)` or
  `advance(ms)`. Guest timers fire when host time passes their deadlines, so
  a paused host clock pauses the guest's timers.

`freeze()` switches to host mode at the current time, and `run()` switches
back to real time. Neither mode lets time go backwards. `now()` returns the
host time in ms, and `realtime()` returns the guest's wall clock.

The guest sees:

- `MONOTONIC` = 10^8 s + host time. The offset keeps `Instant` arithmetic
  that reaches into the past from underflowing in a fresh guest.
- `REALTIME` = `realtimeBaseMs` + host time. The default base makes REALTIME
  start at the wall clock.

Every change wakes every attached worker at once.

### Volumes

| Function | Returns |
| --- | --- |
| `usage(volume)` | `{ files, dirs, bytes, storedBytes, chunks }` |
| `listVolumes()` | `[{ id, nextIno, created, updated }]` |
| `readVolumeFile(volume, path)` | The stored bytes of a volume-relative path, or `null`. |
| `exportVolume(volume)` | A POSIX tar (`Uint8Array`) of what IndexedDB holds. Call `process.flush()` first to include a running guest's latest writes. |
| `importVolume(volume, tar)` | `{ files, dirs, bytes, skipped }`. It replaces the volume's contents; all-zero chunks stay holes. |
| `forget(volume)` | Deletes the volume. |

`forget` and `importVolume` refuse a volume that a process is using.

## Guest contract

**Descriptors.** The layout puts directories first, the way wasmtime does:

- 0 is stdin (always at its end), 1 is stdout and 2 is stderr (both
  line-split).
- 3 is the volume, preopened at `mountPath`.
- The next descriptors are one listening socket per listener port, in the
  order the host listed them.
- The last one is the dialer.

wasi-libc scans preopens until `fd_prestat_get` answers `EBADF`, which the
first socket does. The runtime tells the guest the numbers:

- `KRABKA_LISTEN_FDS=<fd>,<fd>,...`, one per listener, in order;
- `KRABKA_LISTEN_PORTS=<port>,<port>,...`, the same order;
- `KRABKA_DIAL_FD=<fd>`.

**Listeners.** Adopt each listener with `std::net::TcpListener::from_raw_fd`,
`set_nonblocking(true)` and `tokio::net::TcpListener::from_std`. A listener
left in blocking mode blocks the whole guest in `sock_accept` until a
connection arrives. Accepted sockets start blocking, and mio sets them to
non-blocking.

Preview 1 has no `bind`, `connect`, `local_addr` or `peer_addr`. std answers
`peer_addr` with `Unsupported`, and `accept` returns `0.0.0.0:0`.

**Dialer protocol.** The guest writes `host:port\n` (or `[v6]:port\n`) to the
dialer and reads back one line:

- `<fd>\n` is a new socket in state *connecting*;
- `ERR <reason>\n` means the address is malformed.

The runtime allocates the socket at once and asks the host through `ondial`.
Until the host answers, writes return `EAGAIN` and no write readiness is
reported. When the host accepts, the socket reports write readiness. When
the host refuses, read and write subscriptions report `HANGUP` with error 0,
and the first read or write fails with `ECONNREFUSED` (or `ETIMEDOUT`, and so
on). Adopt the socket with `std::net::TcpStream::from_raw_fd`,
`set_nonblocking(true)` and `tokio::net::TcpStream::from_std`. See `dial` in
`playground/wasi-guest/src/net.rs`: it is the connector the broker's embedder
needs.

**Readiness.** Readiness is edge-triggered to match tokio. `poll_oneoff`
reports read readiness once per socket until a read returns `EAGAIN`. It
reports write readiness once, and again only after a write returned `EAGAIN`
or wrote short, or after the socket connected. A listener is never writable.
Hangups carry the `HANGUP` flag and error 0. `poll_oneoff` answers `EINVAL`
when there are no subscriptions. Read and write subscriptions on files are
always ready.

**Clocks.** Both `MONOTONIC` and `REALTIME` read the `WasiClock` (see above).
`clock_res_get` answers 1 µs. The two CPU clocks report the worker's
non-blocked time.

**File system.** The runtime implements the file system of preview 1:

- `path_open` with `O_CREAT`, `O_EXCL`, `O_TRUNC` and `O_DIRECTORY`, and the
  `APPEND` fdflag;
- read, write, `pread` and `pwrite`, seek and tell;
- `fd_filestat_*` and `path_filestat_*`, including set size and set times
  (times are nanosecond-exact);
- `fd_readdir`, with cookies that stay valid while entries are removed;
- mkdir, rmdir, unlink, and rename (within and across directories, over an
  existing file, and of directories);
- `fd_sync`, `fd_datasync`, `fd_advise`, `fd_allocate` and `fd_renumber`.

Files are sparse. An unlinked file stays readable through its open
descriptors. Rights are enforced as POSIX does: a write through a
descriptor without `FD_WRITE` fails with `EBADF`. Symlinks and hard links
answer `ENOTSUP`. `proc_raise` and any import the runtime does not know
answer `ENOSYS`, and are logged once.

**Not in preview 1.** There are no threads, so `spawn_blocking` and
`tokio::fs` cannot work. There is no `dup`, so `File::try_clone` fails with
`Unsupported`. There is no socket address. Relative paths resolve against
`/`, which has no preopen unless `mountPath` is `"/"`.

## Flow control

Every connection is bounded in both directions:

- **Host to guest.** At most `window` bytes are in flight: in the ring or in
  the guest's receive buffer. The worker reports what the guest consumed,
  and the host sends more. Beyond that, bytes wait in the connection's host
  buffer, and `send()` returns false past `highWaterMark`.
- **Guest to host.** `fd_write` accepts at most `window` bytes the host has
  not acknowledged. After that it answers `EAGAIN`. The host acknowledges
  bytes as it delivers them to a `data` handler, so a paused or absent
  handler stops the guest's writes.
- **The ring.** When it is full, records wait in the host's queues until the
  worker posts a space notice.
- **The journal.** Up to `journalMaxInFlight` bytes can be posted but
  uncommitted. Beyond that, the guest's file calls wait for IndexedDB.

## Persistence

The working file system lives in the worker's memory. The host loads the
volume's image from IndexedDB before `_start` runs. The worker journals every
mutation, and the host applies each batch to IndexedDB in one transaction, in
order. Batches that arrive while a transaction runs are coalesced into the
next one.

The journal is coalesced per `poll_oneoff` and per `journalIntervalMs`. An
fsync brings the next flush forward to within 5 ms.

The stored volume always equals the guest's file system at one of its flush
points. The data of a batch is never half applied, so after a reload or a
crash the guest sees a consistent earlier state. This is like a machine that
lost its last few milliseconds of writes, which is what the broker's log
recovery is built for.

**Durability is write-behind.** `fd_sync` and `fd_datasync` return at once.
Data is durable when its batch's transaction commits: by default within about
50 ms of the write, or 5 ms after an fsync. `process.flush()` waits for it.
`kill()` and a page reload lose whatever the worker had not yet posted.

The database is `krabka-wasi`, version 1, which is separate from the lab's
`krabka-lab`. All keys are out of line:

| Store | Key | Value |
| --- | --- | --- |
| `nodes` | `[volume, path]` | `{ type: "dir" \| "file", ino, size, atime, mtime }`. Times are nanoseconds since the epoch, as decimal strings. |
| `chunks` | `[volume, ino, index]` | A `Uint8Array` holding bytes `[index * 65536, ...)`, at most 64 KiB. |
| `volumes` | `volume` | `{ nextIno, created, updated }` |

Paths are relative to the volume root, such as `node-1/topic-0/0000.log`. The
root itself has no record.

Chunks are keyed by inode number rather than by path. A rename, such as the
log's `.swap` to `.log` or a partition directory renamed for deletion, then
rewrites only `nodes` records. A missing chunk, and the bytes after the end
of a short one, read as zeros. A dirty chunk is rewritten whole at each flush
(the price of fixed-size chunks), and flushes come at most every
`journalIntervalMs`.

A running process holds a Web Lock on `krabka-wasi:volume:<id>`, so two tabs
cannot write the same volume. Closing or reloading a tab releases its locks.

## Testing

```
node scripts/check-wasi.mjs            # or: npm run check-wasi
node scripts/check-wasi.mjs --only=headers --headed
```

The script builds `playground/wasi-guest` for `wasm32-wasip1`. It runs
`cargo` from inside the crate, so the crate's `.cargo/config.toml` applies,
including the `--cfg tokio_unstable` that tokio's `net` feature needs on
wasip1. It then runs `wasm-opt -Oz` and serves the result straight from the
crate's target directory: nothing is staged into `public/`.

It installs `playwright-core`, and `binaryen` when no `wasm-opt` is on
`PATH`, with `npm install --no-save`. It drives the Chromium in
`$PLAYWRIGHT_BROWSERS_PATH` (default `/opt/pw-browsers`). The site is served
three times:

- with COOP/COEP headers;
- without them, where the lab's service worker provides isolation;
- without them, with service workers blocked, where the runtime must refuse
  clearly.

The script prints spawn-to-accept time, RTT percentiles and throughput.
