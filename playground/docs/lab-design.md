# Cluster Lab design

The Cluster Lab is the interactive distributed-systems playground on `/docs/lab`. It runs real `krabka-broker` processes (compiled for `wasm32-wasip1`, in Web Workers) joined by a virtual network to a Confluent-compatible schema registry, Kafka producers and consumers, and `krabka-client-streams` applications, which run as state machines inside one WebAssembly module. Several browser tabs, on one machine or across the internet, can each host a share of the clients and apps over WebRTC data channels; the brokers stay in the tab that started them.

This document is the contract between the modules. Every module owner codes against it. When an interface here has to change, change this document in the same commit.

## What is real and what is simulated

The brokers are the real thing: the `krabka-broker` binary, built for `wasm32-wasip1` and run as a process in a Web Worker (see [External nodes](#external-nodes-labexternal) and [`lab-real-broker.md`](lab-real-broker.md)). Its KRaft quorum, metadata, partition logs, replication, group coordinators and request handling are the broker's own code, unmodified; the page supplies its clock, network and disk.

Everything else in the lab is a **sans-IO simulation**. The echo and pinger probes, the schema registry, the producer, consumer and streams apps, and the Kafka client they share are synchronous state machines that receive frames and timer ticks and emit frames. Nothing in the crate opens a socket, reads a clock, spawns a thread, or touches a file. The host (the JavaScript page) owns the clock and the transport. Faults, latency and loss are applied to frames by the world, so they reach a broker process the same way they reach a simulated node, and the world, the clients and the apps replay under the scenario's seed; a broker process does not (it draws real randomness).

The following pieces are the real Krabka code:

| Piece | Crate | Runs in | Used for |
| --- | --- | --- | --- |
| The broker: request handlers, partition logs and replication, group coordinators, KRaft controller quorum, metadata image | `krabka-broker`, `krabka-raft`, `krabka-metadata` | a Web Worker, `wasm32-wasip1` | every broker node |
| Kafka wire codec, every request and response, byte-exact | `krabka-protocol` | the broker process and the lab module | every frame a broker or client handles |
| `RecordBatch` v2 codec with CRC-32C | `krabka-protocol::records` | the broker process and the lab module | every partition log, every produced or fetched batch |
| Streams topology, DSL, processors, state stores, changelogs | `krabka-client-streams` | the lab module, `wasm32-unknown-unknown` | every streams application node |
| KIP-1071 wire topology (`StreamsGroupHeartbeat.Topology`) | `krabka-client-streams::topology` | the lab module | the streams group join |
| Avro schema parsing and reader/writer compatibility | `apache-avro` (the engine `krabka-schema-registry` uses) | the lab module | the schema registry |
| JSON Schema and Protobuf parsing | `jsonschema`, `protox-parse`, `prost-reflect` | the lab module | the schema registry |

The following pieces are written for the lab, in this crate, and they model the behaviour of the real components rather than link them: the schema registry's REST surface and `_schemas` store, the JSON Schema and Protobuf compatibility rules (a documented subset), and the Kafka client used by the producer, consumer, streams and registry nodes. The real `krabka-schema-registry` and `krabka-client-*` crates are tokio programs over TCP and files, so they do not run in the lab module. Where the lab's model and Apache Kafka disagree, the lab is wrong: match Kafka.

## Module map and ownership

Everything lives under `playground/src/lab/`. One owner per directory; nobody edits another owner's directory in the same batch.

| Path | Owner | Holds |
| --- | --- | --- |
| `lab/mod.rs`, `lab/net.rs`, `lab/world.rs`, `lab/scenario.rs`, `lab/events.rs`, `lab/testing.rs` | core | the virtual network, the scheduler, the node trait, faults, the scenario format, the event log, and the test harness |
| `lab/codes.rs` | core | Kafka error codes as `i16` constants (the broker's `codes.rs` names) |
| `lab/external.rs` | core | `ExternalNode`, the world's stand-in for a real `krabka-broker` process that runs in a Web Worker |
| `lab/registry/**` | registry | the schema registry node: HTTP layer, REST routes, store, compatibility engines, `_schemas` client |
| `lab/client/**` | client | the sans-IO Kafka client: connections, metadata, produce, fetch, group membership, offsets |
| `lab/apps/**` | apps | the producer, consumer and streams application nodes, and the topology compiler |
| `lab/wasm.rs` | core | the `wasm-bindgen` surface |

Everything is `pub` inside `lab` where another module needs it, and `pub(crate)` never appears inside a private module (see the code style guide). The `lab` module is `pub` from `lib.rs`, so tests outside the module tree can drive it.

## Core types (`lab::net`)

```rust
/// A node in the scenario. Ids are stable for the life of the world.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct NodeId(pub u32);

/// A listener on a node. A broker listens on `KAFKA_PORT` (9092); a schema
/// registry listens on `HTTP_PORT` (8081). A client connects to an endpoint.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct Endpoint { pub node: NodeId, pub port: u16 }

pub const KAFKA_PORT: u16 = 9092;
pub const HTTP_PORT: u16 = 8081;

/// A logical connection. The client side allocates the id; it is unique per
/// `(client node, id)`. A server keys its per-connection state by the pair
/// `(Endpoint of the peer, ConnId)`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct ConnId(pub u32);

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Payload {
    /// The client opens a connection to `dst`. The server allocates its state.
    Open,
    /// One complete message. Over a Kafka endpoint this is one Kafka frame INCLUDING its
    /// 4-byte big-endian length prefix, so the bytes are exactly what TCP would carry.
    /// Over an HTTP endpoint this is one complete HTTP/1.1 request or response message.
    Data(Bytes),
    /// Either side closes. The other side drops its state; in-flight frames are lost.
    Close,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Frame {
    pub src: Endpoint,   // sender endpoint (for a client, port = the client's ephemeral port, see below)
    pub dst: Endpoint,
    pub conn: ConnId,
    pub payload: Payload,
}
```

A client never listens, so its endpoint port is `CLIENT_PORT` (0). The server replies to `frame.src` with the same `conn`. A connection is identified end to end by `(client endpoint, conn)`.

Ordering: frames on one connection are delivered in send order (FIFO per connection), like TCP. Frames on different connections may interleave. A partition or a `Kill` drops in-flight frames and closes every connection that crosses the cut; both sides observe a `Close` (the world synthesizes it) so a client can reconnect.

## The node trait (`lab::net`)

```rust
pub type Millis = u64;

pub trait Node {
    /// Node kind, one of "echo", "pinger", "schema-registry", "producer", "consumer",
    /// "streams", "krabka-broker" (an `ExternalNode`, see below).
    fn kind(&self) -> &'static str;
    /// Called once when the node starts or restarts. `ctx.now()` is the start time.
    fn start(&mut self, ctx: &mut Ctx<'_>);
    /// A frame arrived for one of this node's endpoints.
    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame);
    /// A timer this node armed with `ctx.arm(at)` fired. Timers never fire late in
    /// logical time: the world advances the clock to the deadline before calling.
    fn on_timer(&mut self, ctx: &mut Ctx<'_>);
    /// A control command from the UI or a scenario step (JSON, node-kind specific).
    fn control(&mut self, ctx: &mut Ctx<'_>, command: serde_json::Value) -> Result<serde_json::Value, String>;
    /// Observable state for the inspector, as JSON. Cheap; called every frame.
    fn snapshot(&self) -> serde_json::Value;
}

pub struct Ctx<'a> { /* private */ }
impl Ctx<'_> {
    pub fn now(&self) -> Millis;
    pub fn me(&self) -> NodeId;
    /// Queue a frame. Delivery time is `now + link latency`. Dropped when the link is cut.
    pub fn send(&mut self, frame: Frame);
    /// Arm the node's single timer at absolute time `at`. Re-arming replaces the earlier deadline;
    /// nodes that need several timers keep their own min-heap and re-arm for the earliest.
    pub fn arm(&mut self, at: Millis);
    /// Record an event for the timeline: `kind` is a short machine tag ("produce", "elect", ...),
    /// `detail` is the JSON the UI renders.
    pub fn event(&mut self, kind: &'static str, detail: serde_json::Value);
    /// Deterministic pseudo-random number in `0..n` (xorshift seeded from the world seed and the node id).
    pub fn rand(&mut self, n: u64) -> u64;
}
```

`Ctx` is the only way a node talks to the world. A node must not keep state that depends on wall-clock time or on `HashMap` iteration order that reaches the wire; use `BTreeMap` wherever order is observable.

## The world (`lab::world`)

```rust
pub struct World { /* private */ }
impl World {
    pub fn new(seed: u64) -> Self;
    pub fn from_scenario(scenario: &Scenario) -> Result<Self, LabError>;
    pub fn add_node(&mut self, spec: NodeSpec) -> Result<NodeId, LabError>;
    pub fn remove_node(&mut self, id: NodeId);
    pub fn now(&self) -> Millis;
    /// Run every deliverable frame and every due timer up to and including `until`, in
    /// deterministic order: (time, sequence number). Returns the number of steps taken.
    pub fn step_until(&mut self, until: Millis) -> usize;
    /// Run at most one step (one frame delivery or one timer). Returns false when idle up to `until`.
    pub fn step_once(&mut self, until: Millis) -> bool;
    pub fn fault(&mut self, fault: Fault);
    pub fn control(&mut self, node: NodeId, command: serde_json::Value) -> Result<serde_json::Value, String>;
    pub fn snapshot(&self) -> WorldSnapshot;       // serializable; the UI polls it
    pub fn events_since(&self, index: usize) -> &[Event];
    // ---- distributed hosting (see below) ----
    pub fn set_hosted(&mut self, nodes: &[NodeId]);   // which nodes THIS world instance runs
    pub fn drain_egress(&mut self) -> Vec<TimedFrame>; // frames for nodes hosted elsewhere
    pub fn push_ingress(&mut self, frames: Vec<Frame>); // frames that arrived from elsewhere
    pub fn apply_remote_snapshot(&mut self, node: NodeId, snapshot: serde_json::Value); // shadow state of a remote node
}

pub enum Fault {
    Kill(NodeId),                       // node stops; state kept (disk survives), connections closed;
                                        // a new connection to it is refused at once (a reset)
    Restart(NodeId),                    // node starts again from its kept state
    Wipe(NodeId),                       // like Restart but from empty state (disk lost)
    Partition { a: NodeId, b: NodeId }, // cut the link both ways
    Heal { a: NodeId, b: NodeId },
    Isolate(NodeId),                    // cut every link of the node
    Reconnect(NodeId),
    Latency { a: NodeId, b: NodeId, ms: Millis },  // one direction is enough; links are symmetric
    Loss { a: NodeId, b: NodeId, permille: u32 },  // drop probability per frame
    CutOneWay { from: NodeId, to: NodeId },        // drop frames from -> to only; no close: the
    HealOneWay { from: NodeId, to: NodeId },       // connection is half dead until a peer times out
    Pause(NodeId),                      // no timers, frames wait (SIGSTOP, a long GC pause)
    Resume(NodeId),                     // held frames first, in order, then a timer that came due
    ClockSkew { node: NodeId, ms: i64 },           // recorded; the page offsets a real broker's REALTIME
    Disk { node: NodeId, mode: DiskMode, ms },     // recorded; the page makes a real broker's volume
                                                   // ok | slow (syncs take ms) | full (ENOSPC) | eio
}
```

Link model: every unordered pair has `latency_ms` (default from the scenario, 5 ms), `loss_permille` (default 0) and a `cut` flag; a separate set of ordered pairs holds the one-way cuts. Loss is deterministic under the seed. A paused node refuses control commands; a paused real broker is stopped by the page (the world still hands its frames to the page, which holds them). The world snapshot reports `paused`, `skew_ms` and `disk` per node and `one_way_cuts`. One-way cuts are not saved in the scenario.

A scenario may carry an `experiment` (timed faults and commands, and checks), which the crate keeps as opaque JSON and returns from `scenario()`; the page runs it.

The world has one **event log**: `Vec<Event>` where `Event { index, at: Millis, node: NodeId, kind: &'static str, detail: Value }`. Nodes append through `Ctx::event`; the world appends its own (`"fault"`, `"deliver"` is NOT logged per frame: too many). A node may set `detail["level"]` to `"info" | "warn" | "error"`.

### Distributed hosting

A world can be **partial**: it holds the full scenario (all `NodeSpec`s) but runs only the hosted nodes. A frame whose destination node is not hosted goes to the **egress queue** as `TimedFrame { deliver_at: Millis, frame }` instead of the delivery queue; the host page ships it over WebRTC to the tab that hosts the destination, which calls `push_ingress`. Latency for such a frame is real network latency; the simulated link latency still applies at the sender (`deliver_at`), and the receiver delivers it at `max(local now, deliver_at)`... no: the receiver delivers it immediately on its next step, because clocks are not synchronized across tabs. Remote nodes appear in `snapshot()` with `hosted: false` and the last snapshot received through `apply_remote_snapshot`.

Faults on a link between a local and a remote node are applied locally on the sending side (a `cut` link drops frames before they reach egress) and mirrored by the page to the other tab, which applies the same fault.

## External nodes (`lab::external`)

A real `krabka-broker`, compiled for `wasm32-wasip1`, runs in a Web Worker behind the browser WASI runtime (`public/playground/wasi/`), not inside this crate. It is the only broker the lab has. Its scenario node has kind `krabka-broker`; the world keeps an `ExternalNode` stand-in for it (`Node::external()` is true), so links, faults, events and snapshots treat it like any other node. A scenario saved by an earlier version of the lab with kind `broker` (a model of the broker that the lab no longer has) is converted to `krabka-broker` by the page when it loads; this crate rejects the old kind as unknown.

- The world holds a frame for an external node for its link latency like any frame, then hands it to the page through `drainExternal()` (`[{deliver_at, frame}]`, each due now) instead of calling a `Node` method. The page gives it to the process: an `Open` to port 9092 or 9093 becomes a connection to that listener, `Data` bytes on it, `Close` its end.
- Frames the process sends (bytes on an accepted connection, an outbound dial and what follows on it) come back through `routeExternal(frames)`, which routes each as sent by its node, through the link model, at the current time. Frames from a node that is not an external node this world hosts, or that is down, are dropped.
- A killed external node refuses new connections at once, like a local one, and the page kills or restarts its process when it applies the fault. The page reports the process's state with `applyRemoteSnapshot`; until it does, the snapshot is `{"external": true}`.
- Addresses: a process sees the lab network as IPv4, node `n` at `10.0.(n >> 8).(n & 255)` (`net::node_ip`, `net::node_for_ip`): Kafka on 9092, the KRaft controller on 9093, a registry on 8081. A broker advertises `127.0.0.1:(9091 + n)` in its metadata, so the local kafkactl bridge can expose it on every OS, and the lab client maps that address back to the node.

The page side (the bridge between the world and the processes, the lab clock the processes run on, faults, volumes, logs and cross-origin isolation), the process contract, and what the broker's entry crate must do are in [`lab-real-broker.md`](lab-real-broker.md).

**Logs.** The dock's Logs tab is the page's view of what the broker processes write to stderr: one JSON object per line, stamped with the lab clock (the record format and the `KRABKA_LOG` level variable are in [`lab-real-broker.md`](lab-real-broker.md#logs)). The tab lists the lines of every broker of the scenario in lab-time order, in a virtualized list, with columns for time, level, node, target and message. A row opens into the whole record as a collapsible JSON tree. Filters cover the minimum level, the nodes, the targets, and a text search that also takes `field:value` terms; the stream can follow the newest line or pause when the reader scrolls up, be cleared, and be downloaded as NDJSON, the raw lines currently shown. Lines that are not JSON, such as a panic message, stay visible as raw lines with a guessed level. Process starts, exits, kills and restarts appear in the stream as marker rows. The crate has no part in it: the page keeps the lines (up to 5,000 per node, 20,000 in all) and never sends them to the world.

The tab also sets the log level: a preset (Quiet, Normal, Verbose, Trace) or a directive, for all brokers or for one. A level is a setting of the page, not of the node's `config` (editing a config wipes the node's disk), so it lives in the browser's `localStorage` per scenario. Changing it restarts the affected processes on their own disks, after a confirmation, because the level is an environment variable read when the process starts.

## Durable state (`lab::net::DurableOp`, `lab::net::DurableImage`)

The one node of the lab module that keeps state across a page reload is the echo node, whose counter survives, so the page keeps it in the browser's IndexedDB. The crate never touches storage itself: a node records every change through `Ctx::persist(DurableOp)`, the world collects the ops per node, and the page drains them with `drainDurable()` after every step and writes them to IndexedDB in order. When the page loads a scenario it read from storage, it folds the stored ops into one `DurableImage` per node (`DurableImage::apply` is the reference fold; the JavaScript store applies the same rules) and calls `loadScenarioWithState(scenario, hosted, images)`, which hands each image to `Node::load` before the node starts.

Two kinds of store, both named by the node: an append-only **log** whose entries the node numbers itself, with `Append`, `TruncateBefore` and `TruncateFrom`, and a **key-value** store with `Put` and `Delete`. `Clear` drops one store; the world emits `ClearAll` for a node on `Fault::Wipe`, on `update_node` and on `remove_node`, so the page drops what it kept. A node that keeps everything in memory ignores `load` and persists nothing.

Store names are conventions per node kind, documented on the node type. The echo node uses the key-value store `counters`. A real broker's disk is not part of this pipeline: it is a volume of the WASI runtime, in its own IndexedDB database (see "Volume" in [`lab-real-broker.md`](lab-real-broker.md)). The schema registry persists nothing: its state is the `_schemas` topic on the brokers, which it replays on every start.

## Scenario format (`lab::scenario`)

```json
{
  "version": 1,
  "id": "6f1c2a9e-…",
  "seed": 42,
  "name": "Three brokers, one producer, one consumer group",
  "links": { "default_latency_ms": 5 },
  "nodes": [
    { "id": 1, "kind": "krabka-broker", "name": "broker-1", "x": 120, "y": 80,
      "config": { "voter": true, "rack": "a" } },
    { "id": 4, "kind": "schema-registry", "name": "registry", "x": 400, "y": 80,
      "config": { "bootstrap": [1, 2, 3] } },
    { "id": 5, "kind": "producer", "name": "orders-producer", "x": 60, "y": 300,
      "config": { "bootstrap": [1], "topic": "orders", "rate_per_sec": 5, "acks": -1,
                  "key": { "pattern": "customer-{seq % 10}" },
                  "value": { "format": "json", "template": { "id": "{seq}", "total": "{rand 1 500}" } },
                  "serialization": { "registry": 4, "format": "avro", "schema": "{...avro record...}" } } },
    { "id": 6, "kind": "consumer", "name": "billing", "x": 700, "y": 300,
      "config": { "bootstrap": [1], "group": "billing", "topics": ["orders"], "protocol": "consumer",
                  "auto_offset_reset": "earliest", "process_ms": 2 } },
    { "id": 7, "kind": "streams", "name": "order-stats", "x": 400, "y": 420,
      "config": { "bootstrap": [1], "application_id": "order-stats",
                  "topology": { "source": "orders",
                                "ops": [ { "op": "filter", "field": "total", "gt": 100 },
                                         { "op": "count_by_key" } ],
                                "sink": "order-counts" } } }
  ],
  "topics": [ { "name": "orders", "partitions": 3, "replication_factor": 3 } ]
}
```

`x`/`y` are UI positions; the crate stores and echoes them but never reads them. `id` is the identity the page assigns when it first saves a scenario; the durable state in IndexedDB is keyed by it. `topics` are created exactly as `kafka-topics --create` would: an admin connection the world owns, bootstrapped at the scenario's brokers, sends a real `CreateTopics` to the broker the metadata names as controller once that broker serves, and the broker forwards it to the active controller in an `Envelope`.

Node config keys are owned by the node kind's module and documented in that module's rustdoc; a `krabka-broker` node's keys are checked by the page and listed in [`lab-real-broker.md`](lab-real-broker.md#configuration). Unknown keys are an error at load time, not ignored.

The controller quorum is static, as a KRaft quorum without KIP-853 is, and the page computes it: the voters are the `krabka-broker` nodes whose `voter` is not `false`, handed to every process as `KRABKA_VOTERS` when it starts (see `lab-real-broker.md`). A change to the voter set reaches a running process when it next starts, as a static `controller.quorum.voters` does.

## Schema registry (`lab::registry`)

`RegistryNode` implements `Node`. It listens on `HTTP_PORT`, and its state is the replay of the `_schemas` topic on the scenario's brokers, which `registry::kafkastore::KafkaStore` sets up, reads and writes as Confluent's `KafkaStore` does, through `lab::client` clients: an admin client, the idempotent `acks=-1` `Producer`, and the client's `Consumer`, without a group, assigned the topic's one partition and sought to its beginning. The registry keeps no durable state: every start, a `Wipe` included, replays `_schemas` and recovers every schema with its id and version.

Startup follows Confluent's `KafkaSchemaRegistry.init`. The store reads the cluster id (`DescribeCluster`) and runs `createOrVerifySchemaTopic`: a missing topic is created with `CreateTopics`, sent to `Metadata.controller_id` and forwarded to the active controller, with 1 partition, `cleanup.policy=compact` and a replication factor of `min(live brokers, kafkastore.topic.replication.factor)`, lowered with Confluent's warning and refused only when no broker is live; an existing topic must have exactly 1 partition and `cleanup.policy=compact`. The reader then looks the topic up and seeks to its beginning, and the store produces a `NOOP` record and waits until the reader has read it. The instance then joins the classic group `schema.registry.group.id` (protocol type `sr`, protocol `v0`, its identity `{"host":"node-<id>","port":8081,…}` as member metadata) and waits for its first assignment: the group's leader assigns every member the same primary, the leader-eligible member with the smallest URL. An instance that becomes the primary, at startup or after any rebalance, produces a `NOOP` and waits for the reader again before it takes a write (`setLeader`). Until its first assignment is applied the registry does not listen: it answers a new connection with a `Close`, and the `http` command fails. Each step has `kafkastore.init.timeout.ms`; a failed step, or a group not joined in time, leaves the registry refusing connections until it restarts.

Writes take Confluent's write lock, so they run one at a time in arrival order. On the primary a write first waits until the reader reaches the last written offset (producing a `NOOP` first when that offset is unknown), then decides on the state as it is then, and produces its records one at a time: the producer must acknowledge each within `kafkastore.timeout.ms`, and the reader must read it back within `kafkastore.timeout.ms` of the acknowledgement. Only then does the HTTP answer go out (read-your-writes). A write that fails answers its REST resource's Confluent error, such as `{"error_code":50002,"message":"Register operation timed out"}` with status 500, and its record may still land later, as in Confluent. A secondary forwards a write to the primary's REST listener and answers with the primary's answer, an error with `; error code: <code>` appended to its message as Confluent's `RestService` reports it; no answer within `leader.read.timeout.ms` answers `50003`, and a write while no primary is known (during a rebalance, or when no member may lead) answers `50004`. A registration of a schema the subject already holds, or of one that does not parse, is answered at once on any instance, and each connection's answers keep their order. The records are Confluent's, byte-exact (the `SCHEMA` key `{"keytype":"SCHEMA","subject":..,"version":..,"magic":1}` and its value, `CONFIG`, `MODE`, `DELETE_SUBJECT`, `NOOP` and tombstones), plus the lab's version high-water record (a `NOOP` key with a subject) before a permanent delete.

Config: `bootstrap` (required; Confluent's `kafkastore.bootstrap.servers`), `compatibility` (default `BACKWARD`), `mode` (default `READWRITE`), `kafkastore.topic` (`_schemas`), `kafkastore.timeout.ms` (500), `kafkastore.init.timeout.ms` (60000), `kafkastore.topic.replication.factor` (3), `leader.eligibility` (true), `schema.registry.group.id` (`schema-registry`), `kafkagroup.session.timeout.ms` (10000), `kafkagroup.heartbeat.interval.ms` (3000), `kafkagroup.rebalance.timeout.ms` (300000) and `leader.read.timeout.ms` (60000).

REST routes and error codes follow Confluent: `/subjects`, `/subjects/{s}` (POST lookup, DELETE), `/subjects/{s}/versions` (GET, POST register), `/subjects/{s}/versions/{v}` (GET, DELETE; `latest` and `-1`), `/subjects/{s}/versions/{v}/schema`, `/subjects/{s}/versions/{v}/referencedby`, `/schemas/ids/{id}`, `/schemas/ids/{id}/schema`, `/schemas/ids/{id}/versions`, `/schemas/ids/{id}/subjects`, `/schemas/types`, `/schemas`, `/config`, `/config/{s}`, `/mode`, `/mode/{s}`, `/compatibility/subjects/{s}/versions/{v}` (and `/versions` for all), with `40401` subject not found, `40402` version not found, `40403` schema not found, `42201` invalid schema, `42202` invalid version, `409` incompatible, `422` invalid compatibility level, `50001` store error, `50002` operation timed out, `50003` forwarding to the primary failed, `50004` no primary known, and the `?deleted=true`, `?permanent=true`, `?normalize=true` query parameters. Content type `application/vnd.schemaregistry.v1+json`.

Compatibility: Avro through `apache_avro::schema_compatibility::SchemaCompatibility` in both directions, the eight Confluent levels, transitive levels over every version. JSON Schema: `properties` added or removed against `additionalProperties`, `required` changes, type narrowing (documented subset). Protobuf: field removal, field number reuse, type change, required-ness (documented subset).

The registry also serves the producer's serializer: `apps` nodes register and look up schemas through the same HTTP frames, so a producer configured with a schema really does `POST /subjects/{topic}-value/versions` before its first record, and a consumer really does `GET /schemas/ids/{id}` on the first record it sees.

## Client (`lab::client`)

The sans-IO Kafka client the apps and the registry embed. It is a struct the node polls, not a `Node`:

```rust
pub struct KafkaClient { /* connections, metadata, pending requests */ }
impl KafkaClient {
    pub fn new(bootstrap: Vec<Endpoint>, client_id: &str, opts: ClientOptions) -> Self;
    /// Feed a frame that arrived for this client (the node forwards every frame whose conn it owns).
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> Vec<ClientEvent>;
    /// Drive timers and retries; returns the next deadline to arm.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<ClientEvent>, Option<Millis>);
    /// Send a request to the leader of a partition, to the coordinator of a group, to the
    /// active controller, to a specific broker, or to any bootstrap broker. Completion arrives
    /// as `ClientEvent::Response`, whose result is `Err(ClientError::Timeout { .. })` when the
    /// request timed out. `send` arms nothing: the node arms `next_deadline(now)` (or calls
    /// `on_tick`) after it.
    pub fn send<R: ProtocolRequest + 'static>(&mut self, ctx: &mut Ctx<'_>, target: Target, req: R) -> RequestId
    where R::Response: 'static;
    pub fn next_deadline(&self, now: Millis) -> Option<Millis>;
    pub fn metadata(&self) -> &MetadataCache;
    pub fn snapshot(&self) -> serde_json::Value;
}

pub enum Target { Any, Broker(i32), Controller, Leader { topic: String, partition: i32 }, Coordinator { key_type: CoordinatorType, key: String } }
```

It negotiates `ApiVersions` on every new connection (opening at version 4, as Kafka 4.x clients do: version 5 only adds ids this client leaves empty), picks the highest common version per api, keeps a metadata cache with leader/epoch per partition, refreshes on `NOT_LEADER_OR_FOLLOWER`/`UNKNOWN_TOPIC_OR_PARTITION`/`LEADER_NOT_AVAILABLE`, routes group requests through `FindCoordinator`, reconnects with backoff, times out a connection that never becomes ready after `connection_setup_timeout_ms` (10 s, doubling to 30 s, with jitter, as Kafka's `ClusterConnectionStates`), and times out requests (`request_timeout_ms`, default 30 s logical). A connection to a killed node is refused at once; one across a partition stays silent until the setup timeout, like a black-holed connection.

A request for any broker (`Target::Any`), the client's own metadata requests and its coordinator lookups go to the node that Kafka's `NetworkClient.leastLoadedNode` picks among the brokers of the metadata, visited from a random one: a ready connection with the fewest requests and room for another, else a connection being set up, else the node whose last connection attempt is oldest, a node never tried first. A node in reconnect backoff is never picked. While there is no metadata, or when none of its brokers can be picked and no connection to one is ready, the bootstrap brokers take their place, as Kafka's `rebootstrap` does. When a connection fails with requests queued on it, a caller's request is routed again, as `KafkaAdminClient` reassigns the calls of a failed node, and the client's own metadata request and lookup fail and pick again after their backoff. So a broker that accepts connections but never answers is left for another.

Metadata refreshes as Kafka's `Metadata` does: a refresh asked for while a request is out is kept for the next one, a tracked topic answered with an invalid-metadata error is asked for again, and the waits back off from `retry_backoff_ms` to `retry_backoff_max_ms` (100 ms to 1 s, doubling, with jitter). Each client draws its connection ids from its own range (`ClientOptions::conn_base`, `conn_base(lane)`, `KafkaClient::owns_conn`), so several clients can share one node.

On top of it: `Producer` (record accumulator per partition, `linger_ms`, `batch_size`, murmur2 default partitioner on the key like the JVM, sticky partitioner for null keys, idempotence with producer id and sequences, `acks` 0/1/-1, retries with the JVM's error classification, and a record for a topic or partition not yet in the metadata waiting up to `max_block_ms` as `KafkaProducer.waitOnMetadata` does, and a partition whose records wait for a leader asking for the metadata at every drain, as `Sender.sendProducerData` does; every codec, with pure-Rust lz4 (`lz4rip`) and zstd (`ruzstd`) through the patched copy of `krabka-compression` in `playground/vendor/`, since upstream binds C libraries that do not build for `wasm32-unknown-unknown`; and with a `transactional_id` Kafka's transactional producer at transaction version 1: `InitProducerId` at the transaction coordinator, `AddPartitionsToTxn` v3 before a partition's first batch, transactional batches in `Produce` v11 at most, `AddOffsetsToTxn` and `TxnOffsetCommit` v4 for `send_offsets_to_transaction`, `EndTxn` v4 once the records are acknowledged, coordinator errors retried, failed batches making the transaction abortable and fencing fatal) and `Consumer` (subscribe or manual `assign`, `seek`/`seek_to_beginning`/`seek_to_end`, classic `JoinGroup`/`SyncGroup` with the range assignor and the `ConsumerProtocolSubscription`/`Assignment` metadata bytes, the leader rejoining when its topics' partitions change, or KIP-848 `ConsumerGroupHeartbeat`, static membership with `group_instance_id` (KIP-345, KIP-814), fetch sessions off, `auto.offset.reset`, auto-commit in `poll_at` as Kafka's `poll` does, `max_poll_records`, `isolation_level` with `read_committed` dropping aborted transactions by the fetch's `aborted_transactions` and the abort markers as Kafka's `CompletedFetch` does, and the position moving past markers once a fetch is drained).

## Apps (`lab::apps`)

Each app node is a thin layer over `lab::client` (config, timers, templates, registry serde, commands, snapshots); its module rustdoc is the authoritative list of config keys, commands, snapshot fields and events.

- `ProducerNode` (`producer`): templated keys, values and headers (`{seq}`, `{seq % n}`, `{now}`, `{rand a b}`, `{pick a|b|c}`, `{uuid}`) at an exact rational `rate_per_sec`; the client `Producer` batches, partitions (murmur2 on the key, sticky for null keys) and retries. With `serialization` it registers `<topic>-value` (`POST /subjects/{s}/versions`) before its first record, retrying refusals with backoff, and frames every value `0x00 | id (4 bytes BE) | Avro datum or JSON text`. With `transactional_id` it writes `transaction_records` records per transaction, aborts every `abort_every`-th, marks each record with the header `lab-txn` = `"<n>:commit"` or `"<n>:abort"`, and ends a transaction only once its records are acknowledged.
- `ConsumerNode` (`consumer`): a classic or KIP-848 member that polls `max_poll_records` when its backlog is empty and processes one record per `process_ms`, committing in the client's `poll_at` (Kafka's `poll`) as the JVM does; `instance_id` makes it a static member (KIP-345), `seek` and `close` are Kafka's `seek` and `close`; lag per partition is `hwm − position + records polled and not yet processed`. With `deserialize` it decodes framed values through `GET /schemas/ids/{id}` (cached). Its snapshot counts `aborted_seen` (polled records whose `lab-txn` header says abort; 0 under `read_committed`) and lists `offset_regressions`.
- `StreamsNode` (`streams`): `apps::topology` compiles the spec (`filter`, `map`, `select_key`, `count_by_key`, `sum_by_key`, `window_count`) into a `krabka_client_streams` `Topology` with repartition topics `<app>-<store>-repartition` and changelogs `<app>-<store>-changelog`. The node joins its streams group with `StreamsGroupHeartbeat` (the byte-exact KIP-1071 topology at epoch 0, JVM field presence afterwards), runs one `EmbeddedTask` per active task (restore from the changelog partitions, fetch from the committed offsets, pipe, produce through the client's idempotent `Producer` (`acks=all`, its own connections) sink and repartition records by the default partitioner and changelog records to the task partition, commit with `OffsetCommit` v9 and the member epoch every `commit_interval_ms`), commits and closes a revoked task before reporting it, follows standby and warm-up changelogs, and restores after a restart. With `processing_guarantee` `exactly_once_v2` the producer is transactional (`<app>-<process id>-1`), every fetch is `read_committed`, a commit sends the offsets in the transaction and ends it with `EndTxn`, and an aborted transaction makes every active task restore from its changelogs and re-read its sources from the committed offsets. Commands: `pause`, `resume`, `query`.
- `AdminNode` (`admin`, hidden): the world keeps one whenever the scenario has a real broker (added with the first broker, rebuilt with `bootstrap` = every broker when brokers come or go, removed with the last; `world.scenario()` leaves it out). It creates the scenario's topics, and its cluster observer (`apps::admin::observer`, its own client on connection lane 1, client id `admin-<id>`, request timeout five periods) polls every `observe_ms` (default 1 000, 0 off). A round has three phases and a new round starts only once the last ended: `Metadata` (all topics), `DescribeCluster`, `DescribeQuorum` (broker listener first, which forwards to the active controller; on an error or a missing api, a voter's controller listener on 9093 through `Target::Endpoint`) and `ListPartitionReassignments`; then `ListOffsets` latest (-1) and earliest (-2) per leader and `ListGroups` per broker; then `OffsetFetch` and `DescribeGroups` per group at its coordinator. A failed question goes to `cluster.errors` and the values it would have replaced stay. Operator commands (`alter_config`, `describe_config`, `reassign`, `cancel_reassign`, `elect_leaders`, `reset_offsets`) send one request each and end in an `admin_done` or `admin_error` event.
- `RebalancerNode` (`rebalancer`): every `interval_ms` it reads `Metadata` and `ListPartitionReassignments`, plans greedily over the non-internal partitions (`replica_count`: move a replica from the fullest broker to the emptiest until they differ by at most one; `leader_count`: the same with the first replica of each list), and with `execute` sends the moves in one `AlterPartitionReassignments` when nothing is in progress, then, once nothing moves, a preferred `ElectLeaders` for the partitions whose leader is not their first replica. The `krabka-rebalancer` crate is not linked: its tokio multi-thread runtime, axum and reqwest do not build for `wasm32-unknown-unknown`.

## WebAssembly surface (`lab::wasm`)

```
class Lab {
  // Times and seeds are plain JavaScript numbers (32-bit): never a BigInt.
  constructor(seed: number)
  loadScenario(json: string): void
  scenario(): string                       // current scenario JSON (positions included)
  addNode(specJson: string): number        // returns node id
  removeNode(id: number): void
  updateNode(id: number, specJson: string): void   // config change; the node restarts
  now(): number
  stepUntil(untilMs: number): number
  fault(json: string): void
  control(id: number, json: string): string
  snapshot(): string                       // WorldSnapshot JSON
  eventsSince(index: number): string       // JSON array
  // external nodes (real brokers in Workers)
  drainExternal(): string                  // [{deliver_at, frame}] due at external nodes now
  routeExternal(framesJson: string): void  // frames an external process sent, routed as its node
  // durable state
  loadScenarioWithState(json: string, idsJson: string, imagesJson: string): void   // images: {"<node id>": DurableImage}
  drainDurable(): string                   // JSON array of {node, op: "append"|..., ...}; bytes base64
  // hosting
  setHosted(idsJson: string): void
  drainEgress(): string                    // JSON array of {deliver_at, frame}; payload bytes base64
  pushIngress(json: string): void
  applyRemoteSnapshot(id: number, json: string): void
}
```

Every string is JSON; bytes inside JSON are base64. The page never sees a Rust type.

## Testing

`lab::testing::TestWorld` builds a world from a scenario literal and offers `run_for(ms)`, `run_until(pred, max_ms)`, `frames_between(a, b)` counters and `node_snapshot(id)`. Module tests use it, with `client::fake_broker::FakeBroker`, a test fixture that answers the requests a client makes, in place of a broker. There is no broker in the crate's tests: cluster behaviour (a quorum forming, topics created through the admin node, producers and consumers through replicated partitions, leadership moving when a leader is killed, a schema registry replaying `_schemas`, a streams app counting into a sink topic) is covered in headless Chromium against real brokers by `npm run check-real-broker` and `npm run check-lab-clusters`, and `npm run check-lab` covers the page, persistence and WebRTC hosting without a broker.

Every assertion uses `assert2`. Wire-facing tests compare whole decoded structs. No test reads source text.
