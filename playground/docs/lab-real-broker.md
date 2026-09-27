# Real brokers in the Cluster Lab

How the lab page runs the real `krabka-broker`, compiled for `wasm32-wasip1`, as a node of the simulation, and the contract the broker's entry crate follows to be that process.

## Design Goals

The lab's simulated broker models the real one; it does not run it. A reader should also be able to put the real broker on the canvas and treat it like any node: connect clients to it, cut its links, kill it, restart it on the disk it had, and read what it does. Three properties follow, and they shaped everything below.

- **The real process is a node of the world.** The world keeps its slot (`lab::external::ExternalNode`), so links, faults, events and snapshots apply to it exactly as to a simulated node. Its frames cross the same link model, with the same latency, loss and cuts, and that includes the connections a broker opens to itself.
- **The real process runs on the lab's clock.** Pause stops it, speed scales it, and it answers a frame at the logical instant the frame arrived, as a simulated node does. Otherwise every latency measured through a real broker would include how fast this machine runs WebAssembly.
- **Nothing changes for readers who do not use it.** The lab page is not cross-origin isolated by default and loads none of the WASI runtime; the first real broker in a scenario turns both on.

## Architecture Overview

```
 world (wasm, krabka-playground)          page (lab/external.js)                   Web Worker (wasi/)
 ─────────────────────────────           ───────────────────────                  ──────────────────
 frame for node 4 held for its  ──drainExternal()──▶ Open → proc.connect(port)  ──ring──▶ listener fd 4/5
 link latency                                        Data → conn.send(bytes)             (the broker reads)
                                                     Close → conn.close()
 frame routed through the link  ◀─routeExternal()─── bytes the process wrote, cut ◀─post── socket output
 model as sent by node 4                             into Kafka frames
                                                     dial 10.0.0.2:9092 → Open  ◀─post── dialer fd 6
                                                     from (4, 0), fresh ConnId
 world clock                    ──every stop──────▶ WasiClock.set(now)         ──SAB───▶ MONOTONIC/REALTIME
                                ◀─quiesce()──────── the process blocked again, next timer at T
```

`public/playground/lab/external.js` (`ExternalHost`) owns one `WasiProcess` per `krabka-broker` node this tab hosts, on the browser WASI runtime in `public/playground/wasi/` (see its `README.md`). `public/playground/lab/world.js` drives the world; while this tab hosts a real broker it steps in lockstep with the processes (below). The crate side is `playground/src/lab/external.rs` and the "External nodes" section of `lab-design.md`. The process is `playground/broker-wasi`, the broker's entry crate (see [Integration](#integration)).

## The process contract

This is what the page gives the process and what it expects back. The broker's entry crate, `playground/broker-wasi`, implements the process side, and `scripts/check-real-broker.mjs` runs it. The WASI test guest (`playground/wasi-guest`, in lab mode when `KRABKA_NODE_ID` is set) follows the same contract and stands in for the broker in `scripts/check-lab-external.mjs`.

### Module

- The page loads `/playground/broker/krabka-broker.wasm`, resolved relative to the lab's scripts (`BROKER_MODULE_URL` in `external.js`), so it follows the site's base path. The site serves it from `public/playground/broker/`, where `npm run build:broker` stages it (it is git-ignored there); the deploy workflow runs that before the site build.
- It is a WASI command for `wasm32-wasip1` (it exports `_start`) that imports only `wasi_snapshot_preview1`; any other import answers `ENOSYS`. argv is `["krabka-broker"]`.
- The release module is about 13.6 MB, 4.6 MB gzipped on the wire. A tab downloads and compiles it once for all its real brokers, compiling while it streams in (`WebAssembly.compileStreaming`, so the site must serve it as `application/wasm`). Meanwhile the node's process state is `loading`, and its reason says how far it got: "downloading the broker: 3.1 MB of 13.6 MB" (only the bytes so far when the response is compressed, since its length then counts compressed bytes), then "compiling the broker".
- When the URL answers HTTP 404, the node's process state is `unavailable` with the reason "the real broker build is not on this site yet". The node stays up in the world, every connection to it is refused at once, and the page does not reload for isolation. Any other failure to load is `unavailable` with the error as the reason.

### Descriptors

| fd | What |
| --- | --- |
| 0 | stdin, always at its end |
| 1, 2 | stdout and stderr, split into lines; the inspector shows the last 40 of each |
| 3 | the node's volume, preopened at `/data` |
| 4 | the listening socket for port 9092 (Kafka) |
| 5 | the listening socket for port 9093 (the KRaft controller) |
| 6 | the dialer |

The directory comes before the sockets, so wasi-libc finds `/data` among the preopens on its own.

### Environment

The process gets exactly these variables:

| Variable | Value |
| --- | --- |
| `KRABKA_LISTEN_FDS` | `4,5` (set by the runtime) |
| `KRABKA_LISTEN_PORTS` | `9092,9093` (set by the runtime), in the order of `KRABKA_LISTEN_FDS` |
| `KRABKA_DIAL_FD` | `6` (set by the runtime) |
| `KRABKA_NODE_ID` | the scenario's node id, `n`; the broker's `node.id` |
| `KRABKA_HOST` | the node's virtual address, `10.0.(n >> 8).(n & 255)` (`net::node_ip`) |
| `KRABKA_VOTERS` | `id@10.0.x.y:9093` for every `krabka-broker` node of the scenario whose `voter` is true, in ascending id, joined by `,`: the same list on every node, and a node is a voter when its own id is in it. Empty when the scenario has no voter, and then no controller quorum can form |
| `KRABKA_CLUSTER_ID` | 22 characters of URL-safe base64 without padding: the first 16 bytes of SHA-256(`krabka-lab/cluster-id/<scenario id>`), with byte 6 set to `(b & 0x0f) \| 0x80` and byte 8 to `(b & 0x3f) \| 0x80` (a version-8 UUID); hashed again with `/1`, `/2`, ... appended while the text starts with `-`, as Kafka's `Uuid.randomUuid()` avoids a leading dash. The same on every node of the scenario |
| `KRABKA_CONFIG` | the JSON form of the broker's `broker.toml` (`krabka_broker::file_config::FileConfig`), built from the node's configuration below; `{}` when it sets nothing |

The environment is computed when a process starts: a change to the voter set reaches a running process when it next starts, which is how a static `controller.quorum.voters` behaves. The cluster id is derived from the scenario id so that a volume formatted by one run matches the scenario on every later run, across page reloads, without the page having to read the volume.

### Configuration

A real broker's configuration is a small set of known keys, checked by the page; an unknown key or a value of the wrong type puts the node in the `invalid` state and no process starts. Every key but `voter` lands at its place in `KRABKA_CONFIG`, and the keys come in the order of this table whatever order the scenario keeps them in, so one configuration is always the same string.

| Key | Value | In `KRABKA_CONFIG` | Kafka |
| --- | --- | --- | --- |
| `voter` | boolean, true by default | nothing: it puts the node in `KRABKA_VOTERS` | `process.roles` with `controller` |
| `rack` | non-empty string | `rack` | `broker.rack` |
| `num_partitions` | integer, 1 to 2^31 − 1 | `runtime.num_partitions` | `num.partitions` |
| `default_replication_factor` | integer, 1 to 32767 | `runtime.default_replication_factor` | `default.replication.factor` |
| `min_insync_replicas` | integer, 1 to 2^31 − 1 | `runtime.default_min_insync_replicas` | `min.insync.replicas` |
| `replica_lag_time_max_ms` | integer, 1 to 2^31 − 1 | `replica_lag_time_max`, as `"<n>ms"` | `replica.lag.time.max.ms` |

For example, `{ "rack": "a", "num_partitions": 3, "replica_lag_time_max_ms": 10000 }` becomes `{"rack":"a","runtime":{"num_partitions":3},"replica_lag_time_max":"10000ms"}`. The bounds fit the broker's field types (`i32`, `i16` and a duration), so a value the page takes is one the broker parses.

### Volume

- The volume is the node's disk: IndexedDB database `krabka-wasi`, volume `<scenario id>/<node id>`, mounted at `/data`. Everything the broker keeps goes under it.
- Durability is write-behind: a batch reaches IndexedDB about 50 ms after the write, 5 ms after an `fsync`, and a kill or a page reload loses what the worker had not posted. The stored volume is always the file system as of one flush point, so the broker's log recovery sees a consistent earlier state.
- The volume is kept whatever the lab's "Persist to this browser" setting says: restarting on it is the point. The Storage panel lists the scenario's volumes and forgets one that no process runs on.
- One process runs on a volume at a time (a Web Lock): the same scenario open in a second tab of the browser fails to start there, with the reason in the inspector.

### Network

**Inbound.** An `Open` from a lab client to the node's port 9092 or 9093 becomes a connection to that listener; an `Open` to any other port is refused (a `Close` answers it at once). `Data` becomes bytes on the connection and `Close` closes it: the process reads the end of the stream and its writes fail with `EPIPE`. What the process writes goes back as `Data` frames from `(node, port)` to the client; closing the socket, or shutting down its write side, sends a `Close` (the lab has no half-open connection).

The lab's nodes exchange whole messages: over a Kafka endpoint, one `Data` frame is one Kafka frame including its 4-byte length prefix. The process writes a byte stream, so the page cuts it: on a connection whose first message from the lab is exactly one Kafka frame, the process's output is cut into one `Data` frame per Kafka frame; on any other connection it travels in the chunks the process writes (the diagnostic `pinger` speaks that way). A length prefix that is negative or above 100 MiB (`socket.request.max.bytes`) ends the cutting for that connection. The broker therefore speaks only Kafka on both ports, as it does anyway.

**Outbound.** The process dials through the dialer: it writes `host:port\n` to fd 6 and reads back `<fd>\n`, the descriptor of a new socket in state *connecting*, at once. `ERR <reason>\n` answers only an address that is malformed or too long. The page then decides the dial, and its verdict arrives on the socket: until then writes return `EAGAIN`; when the page accepts, the socket becomes writable; when it refuses, read and write readiness report a hangup and the first read or write fails with the verdict's errno, which Rust's standard library reports as the `io::ErrorKind` in brackets.

- A host that is not the virtual address of a node of the scenario: `EHOSTUNREACH` (`HostUnreachable`).
- A node that is down: `ECONNREFUSED` (`ConnectionRefused`).
- A node behind a cut link, or either node isolated: the dial waits, like a SYN into a black hole. It opens when the link comes back, fails with `ECONNREFUSED` when the node goes down, and fails with `ETIMEDOUT` (`TimedOut`) after 30 s of lab time, Kafka's longest connection setup (`socket.connection.setup.timeout.max.ms`).
- Otherwise the dial is accepted at once: an `Open` goes to the node from `(node, 0)` with a connection id no live dial of the node uses, and bytes flow both ways. Dials to ports 9092 and 9093 are cut into Kafka frames; any other port travels in chunks.

A node dials its own address like any other. A combined broker and controller sends its `BrokerHeartbeat`, `AssignReplicasToDirs` and `AlterPartition` requests to the active controller over the network even when that is itself, as Kafka's combined mode does. The broker opens a new connection for each heartbeat, so every broker dials the active controller's `:9093` once per heartbeat interval (3 s of lab time). The world routes a node's connection to itself with no latency, whatever the node's links, and hands it back to the page, which connects it to the process's own listener; both ends are the process, and the page pumps both directions like any other pair.

There is no name resolution: the broker dials literal virtual addresses, which is what `KRABKA_VOTERS` and the advertised listeners carry.

Frames for the process wait in the page while its connection's send buffer is full (the runtime's 256 KiB window and 1 MiB high-water mark); none is dropped. Frames from the process are never held: the world queues them behind the link latency.

### Clock

All processes of the tab read one host-driven `WasiClock` that follows the world's clock: `MONOTONIC` is 10^8 s plus lab time, and `REALTIME` is the wall clock at the moment the clock was created plus lab time. While the tab hosts a real broker, the world advances in lockstep with the processes:

1. It steps to the next instant at which something is due: a frame or timer in the world, a frame held for another tab, the earliest timer a process reported, or the deadline of a dial waiting for a link.
2. When a process gets a frame there, or its timer falls due, the clock moves to that instant first, then the frames go to their processes.
3. The world waits, through `WasiProcess.quiesce()`, until each of those processes has taken in everything it was sent, its output has reached the page, and it is blocked again in `poll_oneoff`. The answer carries the deadline of its next timer. The process's frames are routed at that same instant.

So a process answers at the logical instant its input arrived, its timers fire exactly at their deadlines (tokio's timer wheel also wakes at slot boundaries, which are stops too), pause stops them and speed scales them. A process that has not blocked after 200 ms of wall time at one instant is marked lagging and runs free (its frames are routed when they come) until it blocks again, so one busy process cannot stall the lab. A process runs free from its start until it first blocks: that is its `booting` state.

### Lifecycle and faults

| Event | The process |
| --- | --- |
| the node is added, the scenario opens | a process starts on the node's volume |
| `kill` | killed; the world marks the node down and closes its connections |
| `restart` | a new process on the same volume |
| `wipe`, an edit of the node (its configuration or its name; the world clears an edited node's durable state too) | the volume is forgotten, then a fresh process starts |
| the node is removed | killed, and the volume is forgotten |
| the process exits or traps | the page kills the node in the world (mirrored to the session's peers) and records the reason |
| `isolate`, `partition`, `latency`, `loss` | nothing: the world applies them to the frames, and a dial waiting for a link takes another look |

The process states the inspector shows are `loading` (the module downloads or compiles, with the progress as the reason), `booting`, `running`, `unavailable` (the build is missing, or the page cannot be isolated, with the reason), `invalid` (the configuration), `waiting` (the scenario has no identity yet), `killed`, `exited` (with the code), `trapped` (with the message; a panic traps, since the broker builds with `panic = "abort"`) and `failed` (the process did not start, for example because another tab holds the volume). The page adds `process_start`, `process_ready`, `process_exit`, `process_trap`, `process_unavailable` and `process_failed` entries to the timeline.

What the page reports for the node through `applyRemoteSnapshot` (the inspector's state view):

```json
{
  "external": true,
  "process": { "state": "running", "reason": null, "lagging": false, "incarnation": 2,
               "address": "10.0.0.4", "volume": "<scenario id>/4", "module": "/playground/broker/krabka-broker.wasm",
               "started_at_ms": 5120, "exit": null, "notes": [] },
  "env": { "KRABKA_NODE_ID": "4", "KRABKA_HOST": "10.0.0.4", "KRABKA_VOTERS": "4@10.0.0.4:9093",
           "KRABKA_CLUSTER_ID": "…", "KRABKA_CONFIG": "{}" },
  "connections": { "inbound": 3, "outbound": 1, "waiting_dials": 0, "held_bytes": 0 },
  "stdout": ["…the last 40 lines…"],
  "stderr": ["…"],
  "runtime": { "uptime_ms": 5310, "clock_ms": 10420, "polls": 812, "blocked_ms": 5188, "busy_ms": 96,
               "bytes_in": 20480, "bytes_out": 30720, "sockets": 4, "accepted": 3, "dials": 1,
               "files": 12, "file_bytes": 81920, "syncs": 9, "journal_flushes": 30, "journal_bytes": 90112 }
}
```

`exit` is `{ "reason": "exit" | "return" | "trap", "code": 3 | null, "message": "exited with code 3" }` once the process ended by itself; `notes` holds the runtime's last five warnings and errors as `{ "level", "text" }`.

## Key Design Decisions

### Lockstep through `quiesce()`, not timestamps on frames

The first idea was to route the process's frames at the world time they arrive on the main thread. That time is whatever the world reached while the worker ran, so every round trip through a real broker would grow by up to a frame of lab time, more at higher speeds, and a burst of frames at one instant would coalesce into one chunk on a connection that is not length-framed. Timestamping the process's writes would need the world to route a frame in the past, which it cannot. Instead the world does not move on until the processes it just woke are blocked again. The runtime's `quiesce()` is a barrier with an exact meaning: it is answered only when the guest drained every record written before it and found nothing ready, and an answer goes stale when input (or, for a guest blocked on its send window, an acknowledgement) reached the ring after it. The cost is a few ring round trips per stop, well under a millisecond each, and stepping event by event (the world finds the next instant with `hasWorkBy`) while a real broker runs.

### Cutting the stream by the peer's framing

A lab node receives whole messages, a process writes bytes. For Kafka the message boundary is the length prefix, so the page cuts by it, which is exact whatever the process's write sizes. The cut applies where the peer speaks Kafka: dials to 9092 and 9093, and accepted connections whose first lab message is exactly one Kafka frame. That test is exact for every lab client (they send whole frames) and keeps the diagnostic `pinger`, whose pings are not Kafka frames, working against a real process.

### A dial's verdict arrives on its socket

The dialer is a character device the guest writes to and reads from synchronously, while the verdict on a dial belongs to the page: whether the target node is up, whether the link is cut, and, across a cut link, whether it heals within 30 s of lab time. Answering the dialer line only after that verdict would block the broker's one thread in the middle of a connect for as long as a black-holed dial waits, and a process blocked outside `poll_oneoff` also stops the lockstep. So the dialer answers every well-formed address at once with a connecting socket, and the verdict comes the way a real TCP connect reports it: through readiness and the errno of the socket's first read or write. The broker's client sees `ConnectionRefused` or `TimedOut` from the connection, as it would from a kernel socket, and handles it as any failed connection. `ERR` lines are left for addresses the guest should never have written.

### The broker's own configuration schema, not Kafka property names

The entry crate hands `KRABKA_CONFIG` to the broker's own parser for `broker.toml`, so the page sends that schema's JSON form rather than Kafka's dotted property names, and no mapping layer sits in the entry crate. The page offers only a handful of keys, each named after its Kafka property and checked against the broker's field types, so a configuration the page accepts is one the broker takes; the form's help text names where each key lands.

### Isolation only for readers who use it

The runtime blocks in `Atomics.wait` on a `SharedArrayBuffer`, which needs a cross-origin isolated page, and GitHub Pages cannot send the headers. The lab's service worker (`public/docs/lab/coi-sw.js`, scope `/docs/lab/`) adds them, at the cost of one reload. The page asks for it only when a scenario this tab runs contains a real broker, when one is added or a scenario with one is opened, and only when the broker build is on the site. Before the reload it saves the scenario with its identity as the last one and drops a `#s=` share code from the address, so the reload reopens the same scenario with its stored state.

### A real broker stays in its tab

A process and its volume live in one browser. Moving the node to another tab of a session would start it there on an empty disk, which is a wipe the reader did not ask for. So the session layer never moves a `krabka-broker` node off the hub, and a spoke's inspector says why.

## Integration

### The broker's entry crate

The broker's `wasm32-wasip1` entry point is `playground/broker-wasi`, a binary crate in its own workspace (like `playground/wasi-guest`) whose binary is `krabka-broker`. It owns the only `unsafe` code of the lab's Rust, the adoption of descriptors with `from_raw_fd` (its `fds` module): the crate denies `unsafe_code`, and each adoption site allows it and says why it is sound. It does the following.

1. **Builds** as a WASI command on `krabka-broker`, `krabka-format` and `krabka-raft` at the revision of krabka-io/krabka-broker#1153, `krabka-client-core`, `serde_json`, `thiserror`, `tokio` (`rt`, `net`, `time`, `sync`), `tracing`, `tracing-subscriber` and `uuid`, with the broker repository's `[patch.crates-io]` block for the sibling crates (the broker's own crates come with the git dependency at the same revision) and a `Cargo.lock` taken from the broker's. `.cargo/config.toml` sets `[target.wasm32-wasip1] rustflags = ["--cfg", "tokio_unstable"]`. The C code of ring, zstd and LZ4 compiles with clang and its `llvm-ar` against the wasi-sdk 25 sysroot (`CC_wasm32_wasip1`, `AR_wasm32_wasip1`, `CFLAGS_wasm32_wasip1=--sysroot=<wasi-sysroot>`). The release profile is for size: `opt-level = "s"`, fat LTO, one codegen unit, `panic = "abort"` and `strip`, which gives 13.6 MB, 4.6 MB gzipped, against 18.3 MB and 5.8 MB at `opt-level = 3`. `wasm-opt -Os` would take it to 12.1 MB but only to 4.53 MB gzipped, so the build does not run it. `npm run build:broker` (`playground/broker-wasi/build.sh`) builds the module and copies it to `public/playground/broker/krabka-broker.wasm`; it takes the C toolchain from the environment when the workflows set it, and otherwise finds clang's archiver and downloads the sysroot. The deploy workflow runs it before the site build, and the playground workflow runs the crate's unit tests and clippy, natively and for `wasm32-wasip1`.
2. **Reads the whole environment above** before it touches anything (`Contract::from_env`) and exits with code 2 and a line on stderr when it is malformed: the lab then shows the node as exited with the reason. `KRABKA_LISTEN_FDS` and `KRABKA_LISTEN_PORTS` zip into (fd, port) pairs, each port once, with 9092 among them and 9093 too on a voter; `KRABKA_VOTERS` splits on `,` into entries for `krabka_broker::file_config::parse_quorum_voter`; `KRABKA_CLUSTER_ID` decodes as a Kafka `Uuid`; `KRABKA_CONFIG` parses as the broker's `FileConfig`.
3. **Adopts the listeners and installs a connector over the dialer** in `main`, before the runtime starts: `krabka_client_core::transport::install_connector`. Its `connect(host, port)` writes `host:port\n` (an IPv6 host in brackets) to the dialer and reads the reply a byte at a time, which does not block since the dialer answers the line at once, then adopts the descriptor with `std::net::TcpStream::from_raw_fd`, `set_nonblocking(true)` and `tokio::net::TcpStream::from_std`. An `ERR <reason>` reply fails the dial with `InvalidInput`. The lab's verdict then arrives from the stream as `ConnectionRefused`, `TimedOut` or `HostUnreachable`, as described under Network. Every outbound connection goes through it, the node's own address included: raft traffic between controllers, heartbeats and forwarding to the active controller, replica fetchers, and transaction markers.
4. **Runs a current-thread tokio runtime** (`Builder::new_current_thread().enable_all()`), and takes all its time from `Instant`, `SystemTime` and tokio timers, which read the lab's clock. It waits only in `poll_oneoff`: no threads, no `spawn_blocking` (blocking work runs inline on `wasi`), no busy loop, or the lab marks it lagging.
5. **Formats its log directory in process** with `krabka_format::run_from_args(["krabka-format", "--log-dir", "/data/log", "--cluster-id", <the cluster id as a UUID>, "--node-id", $KRABKA_NODE_ID, "--ignore-formatted"])`, which returns 0 whether it formatted the directory or found it formatted, and whose own exit code the process takes when it is not 0. With no quorum flag the log carries no voter set of its own, and the node runs the static KIP-595 quorum it is configured with. It then reads `meta.properties` back with `krabka_broker::bootstrap::read_and_validate_meta_properties` and the cluster id, so a volume formatted for another cluster does not boot, and boots in `Rejoin` mode when `krabka_raft::metadata_log_nonempty` finds raft state in the metadata log, in `Bootstrap` mode otherwise, as the broker binary does.
6. **Builds its `BrokerConfig`** from the environment: the node id as `broker_id` and `node_id`; the roles `[Controller, Broker]` on a voter and `[Broker]` otherwise; `listen_addr` `$KRABKA_HOST:9092`, advertised as `$KRABKA_HOST:9092`; `controller_listen_addr` `$KRABKA_HOST:9093`; `controller_quorum_voters` from `KRABKA_VOTERS`; `log_dir` `/data/log`; the cluster id and directory id from `meta.properties`; and the boot mode. It keeps the default `heartbeat_timeout`, which works because the lab answers the node's dials to itself, and sets no metrics, OTLP, JWKS, OPA, schema registry or tiered storage over the topic-based RLMM, none of which runs on `wasm32-wasip1`. Then it applies `KRABKA_CONFIG` on top with `FileConfig::apply_to`, after adding an `[audit]` table with `enabled = false` when `KRABKA_CONFIG` has none: the broker reads a missing table as its secure default, audit on, and the lab runs without the audit log. A value the broker refuses exits with code 2.
7. **Hands over the listeners** it adopted with `std::net::TcpListener::from_raw_fd`, `set_nonblocking(true)` and `tokio::net::TcpListener::from_std`: port 9093 as the controller listener on a voter, and every other port, in order, as the data-plane listeners that `config.effective_listeners()` describes. A broker-only node closes its 9093 listener, so a connection to it is refused, as on a Kafka broker without the controller role. Preview 1 has no `local_addr`, so the addresses come from `KRABKA_HOST` and the ports.
8. **Starts** with `Broker::start_with_listeners(config, controller, data)` and serves until the broker stops on its own, which it does only when every log directory went offline (KIP-112); it then shuts the broker down and exits with code 1, as it does when the broker does not start. It relies on its log recovery after a kill or a reload, since the volume's durability is write-behind. It logs one event per line to stderr, stamped with the time since the process started, which the lab's clock drives, at `INFO` and above, but for `krabka_broker::network::dispatch`: that module logs every request it dispatches and every connection it accepts at `INFO`, where Kafka's default logging configuration keeps its request logger (`kafka.request.logger`) at `WARN` and logs accepted connections at `DEBUG`, so the process keeps it at `WARN` and the inspector's tail of the log stays readable.

### The world and the rest of the lab

A real broker is a node of the world like any other: simulated clients, the admin node and other real brokers reach it through links, and its snapshot is what the inspector shows. A session of several tabs keeps it on the hub; the other tabs see its snapshot and send it frames through the hub like any node's.

## Kafka / KIP Compliance

- **Framing.** Between a real broker and any lab node, one message is one Kafka frame with its length prefix, the unit of Kafka's protocol, and a prefix above `socket.request.max.bytes` (100 MiB) is not a Kafka frame.
- **Controller quorum.** `KRABKA_VOTERS` is a static KIP-595 `controller.quorum.voters`, identical on every node; a node is a controller when its id is in it, as `process.roles` makes it in Kafka. The KIP-853 dynamic quorum is not used.
- **Combined mode.** A node that is both broker and controller reaches its own controller listener over the network, as Kafka's combined mode does, so the lab routes a node's dials to itself instead of short-circuiting them.
- **Cluster id.** `KRABKA_CLUSTER_ID` has the form of Kafka's `Uuid.toString()`, 22 characters of URL-safe base64, and like `Uuid.randomUuid()` it never starts with `-`, so `kafka-storage format --cluster-id` would take it too.
- **Connection setup.** A dial across a cut link gives up after 30 s of lab time, the default of `socket.connection.setup.timeout.max.ms` ([KIP-601](https://cwiki.apache.org/confluence/display/KAFKA/KIP-601%3A+Configurable+socket+connection+timeout+in+NetworkClient)), the longest a Kafka client waits for a connection to be set up.
- **Configuration.** Each configuration key is a Kafka broker property in snake case, with Kafka's meaning, delivered through the broker's own schema.

## Testing

`npm run check-real-broker` runs the real broker. It builds the module with `npm run build:broker` (a no-op for cargo when the build is fresh; `--no-build` takes the staged module as it is), serves the built site with it, and in headless Chromium checks that one real broker boots, formats its volume and serves on its virtual address; that three voters form a quorum and serve, the lab's admin node creates a topic with three replicas on them, and the lab's own producer, a classic consumer group and a KIP-848 member produce and consume through them; that the inspector shows the brokers' metadata as the clients got it (brokers, controller, cluster id, partition leaders and ISR); that killing the leader of a partition moves the leadership while the consumer keeps consuming, and a restart brings the broker back on its volume, in `Rejoin` mode, and into every ISR; and that a page reload restores the brokers from their IndexedDB volumes, with every record still there for a new group and both groups resuming from their committed offsets. It takes about 40 s of browser time; the fault part runs the lab at 5x, which the processes keep up with.

`npm run check-lab-external` builds the WASI test guest and runs it as a `krabka-broker` node in headless Chromium. In Node it first checks the contract's pure parts on the `external.js` that ships: the virtual addresses both ways, the configuration's mapping onto `FileConfig` and its fixed key order, the refusal of bad values, the voter list, the environment, the cluster id, and the Kafka framing. In the browser it covers the missing-build state without a reload; the isolation reload on add and on open with the scenario kept; the download progress; the whole environment as the process sees it; the dials a node makes to itself at boot and on command; a pinger's round trip through the process over a 200 ms link (exactly the 400 ms a simulated echo gives); a dial through the world over a 300 ms link (600 ms, counted by the echo node) and 1.8 MB relayed back byte for byte, one lab frame per Kafka frame; unreachable and refused dials; a dial across a cut link that connects when the link heals and one that fails with `ETIMEDOUT` after exactly 30 s of lab time; kill, restart on the same volume, wipe, and a configuration change; pause; exit and trap; the Storage panel; and pinning in a session. `npm run check-wasi` covers the runtime, `quiesce()` included. `npm run check-lab` covers the lab without real brokers, unchanged.

The entry crate's unit tests (`cargo test` in `playground/broker-wasi`) run natively and cover the environment's checks, the configuration profile with `KRABKA_CONFIG` applied, and the dialer's line protocol.
