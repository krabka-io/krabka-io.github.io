# Cluster Lab design

The Cluster Lab is the interactive distributed-systems playground on `/docs/lab`. It runs simulated Krabka brokers, a Confluent-compatible schema registry, Kafka producers and consumers, and `krabka-client-streams` applications inside one WebAssembly module, and it lets several browser tabs, on one machine or across the internet, each host a share of the nodes over WebRTC data channels.

This document is the contract between the modules. Every module owner codes against it. When an interface here has to change, change this document in the same commit.

## What is real and what is simulated

The lab is a **sans-IO simulation**. Every node is a synchronous state machine that receives frames and timer ticks and emits frames. Nothing in the crate opens a socket, reads a clock, spawns a thread, or touches a file. The host (the JavaScript page) owns the clock and the transport.

The following pieces are the real Krabka code, compiled unchanged for `wasm32-unknown-unknown`:

| Piece | Crate | Used for |
| --- | --- | --- |
| Kafka wire codec, every request and response, byte-exact | `krabka-protocol` | every frame a simulated broker or client handles |
| `RecordBatch` v2 codec with CRC-32C | `krabka-protocol::records` | every partition log |
| KRaft state machine (KIP-595, KIP-996) | `krabka-kraft-core` | the controller quorum between brokers |
| Metadata records and the immutable metadata image | `krabka-metadata` | the controller log and every broker's view of the cluster |
| Streams topology, DSL, processors, state stores, changelogs | `krabka-client-streams` | every streams application node |
| KIP-1071 wire topology (`StreamsGroupHeartbeat.Topology`) | `krabka-client-streams::topology` | the streams group join |
| Avro schema parsing and reader/writer compatibility | `apache-avro` (the engine `krabka-schema-registry` uses) | the schema registry |
| JSON Schema and Protobuf parsing | `jsonschema`, `protox-parse`, `prost-reflect` | the schema registry |

The following pieces are written for the lab, in this crate, and they model the behaviour of the real components rather than link them: the broker's request handlers, partition replication and ISR maintenance, the group coordinator (classic, KIP-848 and KIP-1071), the schema registry's REST surface and `_schemas` store, the JSON Schema and Protobuf compatibility rules (a documented subset), and the Kafka client used by the producer, consumer, streams and registry nodes. The real `krabka-broker`, `krabka-schema-registry` and `krabka-client-*` crates are tokio programs over TCP and files, so they cannot run in a browser. Where the lab's model and Apache Kafka disagree, the lab is wrong: match Kafka.

## Module map and ownership

Everything lives under `playground/src/lab/`. One owner per directory; nobody edits another owner's directory in the same batch.

| Path | Owner | Holds |
| --- | --- | --- |
| `lab/mod.rs`, `lab/net.rs`, `lab/world.rs`, `lab/clock.rs`, `lab/scenario.rs`, `lab/events.rs`, `lab/testing.rs` | core | the virtual network, the scheduler, the node trait, faults, the scenario format, the event log, and the test harness |
| `lab/codes.rs` | core | Kafka error codes as `i16` constants (the broker's `codes.rs` names) |
| `lab/broker/**` | broker | the simulated broker: connection pipeline, dispatch registry, handlers, partition logs, replication, coordinator |
| `lab/controller/**` | controller | the KRaft driver over `krabka-kraft-core`, the metadata log, the controller decisions (registration, topics, leader election, ISR) |
| `lab/registry/**` | registry | the schema registry node: HTTP layer, REST routes, store, compatibility engines, `_schemas` client |
| `lab/client/**` | client | the sans-IO Kafka client: connections, metadata, produce, fetch, group membership, offsets |
| `lab/apps/**` | apps | the producer, consumer and streams application nodes, and the topology compiler |
| `lab/wasm.rs` | core | the `wasm-bindgen` surface |

Everything is `pub` inside `lab` where another module needs it, and `pub(crate)` never appears inside a private module (see the code style guide). The `lab` module is `pub` from `lib.rs` so integration tests under `playground/tests/` can drive it.

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
    /// Node kind, one of "broker", "schema-registry", "producer", "consumer", "streams".
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
    Slow(NodeId, ms)                    // extra per-node processing delay before replies (optional)
}
```

Link model: every ordered pair has `latency_ms` (default from the scenario, 5 ms) and `loss_permille` (default 0) and a `cut` flag. Loss is deterministic under the seed.

The world has one **event log**: `Vec<Event>` where `Event { index, at: Millis, node: NodeId, kind: &'static str, detail: Value }`. Nodes append through `Ctx::event`; the world appends its own (`"fault"`, `"deliver"` is NOT logged per frame: too many). A node may set `detail["level"]` to `"info" | "warn" | "error"`.

### Distributed hosting

A world can be **partial**: it holds the full scenario (all `NodeSpec`s) but runs only the hosted nodes. A frame whose destination node is not hosted goes to the **egress queue** as `TimedFrame { deliver_at: Millis, frame }` instead of the delivery queue; the host page ships it over WebRTC to the tab that hosts the destination, which calls `push_ingress`. Latency for such a frame is real network latency; the simulated link latency still applies at the sender (`deliver_at`), and the receiver delivers it at `max(local now, deliver_at)`... no: the receiver delivers it immediately on its next step, because clocks are not synchronized across tabs. Remote nodes appear in `snapshot()` with `hosted: false` and the last snapshot received through `apply_remote_snapshot`.

Faults on a link between a local and a remote node are applied locally on the sending side (a `cut` link drops frames before they reach egress) and mirrored by the page to the other tab, which applies the same fault.

## External nodes (`lab::external`)

A real `krabka-broker`, compiled for `wasm32-wasip1`, runs in a Web Worker behind the browser WASI runtime (`public/playground/wasi/`), not inside this crate. Its scenario node has kind `krabka-broker`; the world keeps an `ExternalNode` stand-in for it (`Node::external()` is true), so links, faults, events and snapshots treat it like any other node.

- The world holds a frame for an external node for its link latency like any frame, then hands it to the page through `drainExternal()` (`[{deliver_at, frame}]`, each due now) instead of calling a `Node` method. The page gives it to the process: an `Open` to port 9092 or 9093 becomes a connection to that listener, `Data` bytes on it, `Close` its end.
- Frames the process sends (bytes on an accepted connection, an outbound dial and what follows on it) come back through `routeExternal(frames)`, which routes each as sent by its node, through the link model, at the current time. Frames from a node that is not an external node this world hosts, or that is down, are dropped.
- A killed external node refuses new connections at once, like a local one, and the page kills or restarts its process when it applies the fault. The page reports the process's state with `applyRemoteSnapshot`; until it does, the snapshot is `{"external": true}`.
- Addresses: a process sees the lab network as IPv4, node `n` at `10.0.(n >> 8).(n & 255)` (`net::node_ip`, `net::node_for_ip`): Kafka on 9092, the KRaft controller on 9093, a registry on 8081. A broker advertises its virtual address, and the lab client maps it back to the node.

The page side (the bridge between the world and the processes, the lab clock the processes run on, faults, volumes and cross-origin isolation), the process contract, and what the broker's entry crate must do are in [`lab-real-broker.md`](lab-real-broker.md).

## Durable state (`lab::net::DurableOp`, `lab::net::DurableImage`)

A node's durable state (a broker's partition logs and metadata, the controller's log) must survive a page reload, so the page keeps it in the browser's IndexedDB. The crate never touches storage itself: a node records every change through `Ctx::persist(DurableOp)`, the world collects the ops per node, and the page drains them with `drainDurable()` after every step and writes them to IndexedDB in order. When the page loads a scenario it read from storage, it folds the stored ops into one `DurableImage` per node (`DurableImage::apply` is the reference fold; the JavaScript store applies the same rules) and calls `loadScenarioWithState(scenario, hosted, images)`, which hands each image to `Node::load` before the node starts.

Two kinds of store, both named by the node: an append-only **log** whose entries the node numbers itself (a partition log uses the batch base offset, the controller its log offset) with `Append`, `TruncateBefore` and `TruncateFrom`, and a **key-value** store with `Put` and `Delete`. `Clear` drops one store; the world emits `ClearAll` for a node on `Fault::Wipe`, on `update_node` and on `remove_node`, so the page drops what it kept. A node that keeps everything in memory ignores `load` and persists nothing.

Store names are conventions per node kind, documented on the node type. The broker uses `log/<topic>/<partition>` for partition batches, `meta/<topic>/<partition>` for the partition's checkpoints (high watermark, leader epoch cache, producer state), the log store `kraft` for its copy of the metadata quorum's log, and the key-value store `kraft-state` for its quorum state (`quorum`) and high watermark (`hwm`); a reload rebuilds the metadata image by replaying the committed part of `kraft`. The schema registry persists nothing: its state is the `_schemas` topic on the brokers, which it replays on every start.

## Scenario format (`lab::scenario`)

```json
{
  "version": 1,
  "id": "6f1c2a9e-…",
  "seed": 42,
  "name": "Three brokers, one producer, one consumer group",
  "links": { "default_latency_ms": 5 },
  "nodes": [
    { "id": 1, "kind": "broker", "name": "broker-1", "x": 120, "y": 80,
      "config": { "broker_id": 1, "voter": true, "rack": "a" } },
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

`x`/`y` are UI positions; the crate stores and echoes them but never reads them. `id` is the identity the page assigns when it first saves a scenario; the durable state in IndexedDB is keyed by it. `topics` are created exactly as `kafka-topics --create` would: an admin connection the world owns sends a real `CreateTopics` to the broker the metadata names as controller once that broker serves, and the broker forwards it to the active controller in an `Envelope`.

Node config keys are owned by the node kind's module and documented in that module's rustdoc. Unknown keys are an error at load time, not ignored.

The controller quorum is static, as a KRaft quorum without KIP-853 is: when a scenario loads, its voters are the brokers whose `voter` is not `false`, and the world gives every broker that names no `controller_quorum_voters` that list (the scenario keeps what its author wrote). A broker added to a running scenario joins as an observer until the scenario loads again, except the first voter of a world with no quorum yet, which starts one; a voter of the loaded quorum cannot be turned into an observer.

## Broker (`lab::broker`)

`BrokerNode` implements `Node`. Its parts:

- **Connections.** Per `(peer endpoint, conn)`: a decode buffer is not needed (frames are whole), the negotiated api versions, the client id, and a FIFO of in-flight requests. Kafka serves the requests of one connection **in order**, one at a time; the broker may hold a request (a Fetch with `max_wait_ms`, a Produce with `acks=-1` waiting for the ISR) and it must still answer later requests on the same connection only after the held one. Model this with a per-connection queue and a "blocked" head.
- **Dispatch registry.** A macro lists every supported request type once: `(ApiKey, owned::FooRequest, handler)`. The registry derives `MIN_VERSION`, `MAX_VERSION`, `LATEST_STABLE_VERSION` and `FLEXIBLE_MIN` from `ProtocolRequest`, answers `ApiVersions` from the list, decodes the request header at header version 2 when `version >= FLEXIBLE_MIN` else 1 (`ControlledShutdown` v0 is header 0; the lab does not serve it), and encodes the response header at version 1 when the body is flexible else 0 (`ApiVersions` always 0). An unsupported api key or version gets `UNSUPPORTED_VERSION` (35) with an empty body of the right shape where Kafka does that, and a decode error closes the connection, as Kafka does.
- **Handlers.** `ApiVersions`, `Metadata`, `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `Produce`, `Fetch`, `ListOffsets`, `OffsetForLeaderEpoch`, `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`, `DescribeGroups`, `ListGroups`, `ConsumerGroupHeartbeat`, `ConsumerGroupDescribe`, `StreamsGroupHeartbeat`, `StreamsGroupDescribe`, `InitProducerId`, `DescribeCluster`, `DescribeConfigs`, `DescribeTopicPartitions`, `SaslHandshake` (returns the empty mechanism list: plaintext only), `DescribeQuorum` (forwarded to the active controller). The controller listener (9093) serves `ApiVersions`, `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `DescribeQuorum`, `AlterPartition`, `Envelope`, `BrokerRegistration`, `BrokerHeartbeat` and `AllocateProducerIds`. Each handler is `fn(&mut BrokerNode, &mut Ctx, &RequestCtx, FooRequest) -> HandlerOutcome` where the outcome is `Reply(FooResponse)`, `Hold(...)` (answer later from a timer or a state change) or `Close`.
- **Partition log.** `PartitionLog { batches: Vec<StoredBatch>, log_start: i64, hwm: i64, leader_epoch: i32, epoch_cache: Vec<(epoch, start_offset)> }`. Append assigns offsets, stamps `partition_leader_epoch`, re-encodes the batch (the CRC covers the assigned offsets? No: `base_offset` and `partition_leader_epoch` are outside the CRC; only the batch length/CRC-covered part is fixed) and stores the encoded bytes, so a Fetch returns bytes the way Kafka does. Idempotent producers: per `producer_id` the last 5 `(epoch, base_seq, last_seq)`; out-of-order sequence → `OUT_OF_ORDER_SEQUENCE_NUMBER`, duplicate → success with the original offset.
- **Replication.** A follower replica runs a fetch loop against the leader with `replica_id = my broker id`, appends returned batches verbatim, and reports its LEO on the next fetch. The leader tracks each follower's LEO and time of last fetch, advances the HWM to `min(LEO over ISR)`, and proposes ISR shrink (follower behind for `replica_lag_time_max_ms`, Kafka's 30 s of logical time) or expand (follower caught up to HWM) to the controller with `AlterPartition`. `acks=-1` produces complete when the HWM passes their last offset, else after `request_timeout` they fail with `REQUEST_TIMED_OUT`... Kafka returns `NOT_ENOUGH_REPLICAS` before append when `|ISR| < min.insync.replicas`, and `NOT_ENOUGH_REPLICAS_AFTER_APPEND` if the ISR shrinks after; match that.
- **Coordinator.** Group state per group id on the coordinator broker (`hash(group) % 50` mapped onto the `__consumer_offsets` partitions, which the lab creates as an internal topic with `replication_factor = min(3, brokers)`; the coordinator for a group is the leader of that partition, so a broker failure moves groups the way Kafka does). Group state and committed offsets are written as records to that partition (the real Kafka key/value formats are NOT required; a JSON value is fine, but write them, so that a coordinator failover on another broker loads the state from the replicated log). Classic protocol: `JoinGroup` with a rebalance timeout, leader/assignment through `SyncGroup`, generation ids, `Heartbeat` with session timeouts, `LeaveGroup`. KIP-848 `ConsumerGroupHeartbeat`: member epochs, server-side assignor (uniform, sticky-ish: keep an existing owner when possible), the reconciliation dance (`member_epoch`, revoked partitions must be acked before new ones are assigned). KIP-1071 `StreamsGroupHeartbeat`: same epoch mechanics; the topology is registered once (topology epoch), internal topics (repartition, changelog) are created from `TopicInfo`, `MISSING_SOURCE_TOPICS`/`MISSING_INTERNAL_TOPICS` statuses until they exist, active tasks are assigned per subtopology and partition, standby tasks when `num_standby_replicas > 0`.
- **Controller side.** Every broker embeds a `ControllerCore`, as a voter when its id is in `controller_quorum_voters` and as an observer otherwise (`process.roles=broker,controller`). The quorum leader becomes the active controller once it has applied its own leader-change record and makes every metadata decision with `ControllerDecisions`. Brokers register and heartbeat with the active controller's controller listener, stay fenced until they have caught up with the metadata log, and answer clients only once unfenced (connections are accepted earlier and wait). Controller requests sent to any broker are forwarded in an `Envelope`; leaders propose ISR changes with `AlterPartition`; producer-id blocks come from `AllocateProducerIds`. Every broker's image is the replay of the committed log.

Everything the UI shows for a broker comes from `snapshot()`: role in the quorum, epoch, controller id, registered brokers, topics with per-partition leader/ISR/replicas/LEO/HWM per replica, groups with members and lag, connection count, request counters per api key.

## Controller (`lab::controller`)

A driver over `krabka_kraft_core::QuorumStateMachine` shaped like `kraft_core::sim` but over `Ctx`: the quorum messages are `Event`s serialized with `serde_json` in `Payload::Data` on connection ids from 2^30 up (`RAFT_CONN_BASE`) between brokers (they are not Kafka frames; the lab does not claim wire fidelity for KIP-595 RPCs). Timers: election, fetch, heartbeat (300 ms), check-quorum, derived from the machine's `ResetTimer` actions and the node id stagger, as in `sim/node.rs`. A follower's `Fetch` carries its high watermark and the leader answers at once when its own is higher (KIP-1166).

The log is `MetadataLog { entries: Vec<(Epoch, MetadataBatch)> }` where a batch is `Vec<MetadataRecord>`; the leader appends, followers replicate on `Fetch`, the HWM from the machine marks commit, and committed batches are applied to `MetadataImage` on every node and handed out by `ControllerCore::take_committed` and applied by the broker.

Controller decisions (run by the active controller only, on every committed batch and on timers): broker registration (epoch, fenced/unfenced from heartbeats, 9 s session), topic creation with Kafka's striped placement, made deterministic (unfenced brokers first, the first replica leads), partition leader election when a leader is fenced (the first replica in replica order that is in the ISR and unfenced; if none, `NO_LEADER` and the partition is offline with its last ISR member kept; unclean election only where the topic enables it), producer-id blocks, `AlterPartition` validation (leader epoch and partition epoch checks), `__consumer_offsets` creation on first `FindCoordinator`, and topic deletion.

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

It negotiates `ApiVersions` on every new connection, picks the highest common version per api, keeps a metadata cache with leader/epoch per partition, refreshes on `NOT_LEADER_OR_FOLLOWER`/`UNKNOWN_TOPIC_OR_PARTITION`/`LEADER_NOT_AVAILABLE`, routes group requests through `FindCoordinator`, reconnects with backoff, times out a connection that never becomes ready after `connection_setup_timeout_ms` (10 s, doubling to 30 s, with jitter, as Kafka's `ClusterConnectionStates`), and times out requests (`request_timeout_ms`, default 30 s logical). A connection to a killed node is refused at once; one across a partition stays silent until the setup timeout, like a black-holed connection.

Metadata refreshes as Kafka's `Metadata` does: a refresh asked for while a request is out is kept for the next one, a tracked topic answered with an invalid-metadata error is asked for again, and the waits back off from `retry_backoff_ms` to `retry_backoff_max_ms` (100 ms to 1 s, doubling, with jitter). Each client draws its connection ids from its own range (`ClientOptions::conn_base`, `conn_base(lane)`, `KafkaClient::owns_conn`), so several clients can share one node.

On top of it: `Producer` (record accumulator per partition, `linger_ms`, `batch_size`, murmur2 default partitioner on the key like the JVM, sticky partitioner for null keys, idempotence with producer id and sequences, `acks` 0/1/-1, retries with the JVM's error classification, and a record for a topic or partition not yet in the metadata waiting up to `max_block_ms` as `KafkaProducer.waitOnMetadata` does) and `Consumer` (subscribe or manual `assign`, `seek`/`seek_to_beginning`/`seek_to_end`, classic `JoinGroup`/`SyncGroup` with the range assignor and the `ConsumerProtocolSubscription`/`Assignment` metadata bytes, the leader rejoining when its topics' partitions change, or KIP-848 `ConsumerGroupHeartbeat`, static membership with `group_instance_id` (KIP-345, KIP-814), fetch sessions off, `auto.offset.reset`, auto-commit in `poll_at` as Kafka's `poll` does, `max_poll_records`).

## Apps (`lab::apps`)

Each app node is a thin layer over `lab::client` (config, timers, templates, registry serde, commands, snapshots); its module rustdoc is the authoritative list of config keys, commands, snapshot fields and events.

- `ProducerNode` (`producer`): templated keys, values and headers (`{seq}`, `{seq % n}`, `{now}`, `{rand a b}`, `{pick a|b|c}`, `{uuid}`) at an exact rational `rate_per_sec`; the client `Producer` batches, partitions (murmur2 on the key, sticky for null keys) and retries. With `serialization` it registers `<topic>-value` (`POST /subjects/{s}/versions`) before its first record, retrying refusals with backoff, and frames every value `0x00 | id (4 bytes BE) | Avro datum or JSON text`.
- `ConsumerNode` (`consumer`): a classic or KIP-848 member that polls `max_poll_records` when its backlog is empty and processes one record per `process_ms`, committing in the client's `poll_at` (Kafka's `poll`) as the JVM does; `instance_id` makes it a static member (KIP-345), `seek` and `close` are Kafka's `seek` and `close`; lag per partition is `hwm − position + records polled and not yet processed`. With `deserialize` it decodes framed values through `GET /schemas/ids/{id}` (cached).
- `StreamsNode` (`streams`): `apps::topology` compiles the spec (`filter`, `map`, `select_key`, `count_by_key`, `sum_by_key`, `window_count`) into a `krabka_client_streams` `Topology` with repartition topics `<app>-<store>-repartition` and changelogs `<app>-<store>-changelog`. The node joins its streams group with `StreamsGroupHeartbeat` (the byte-exact KIP-1071 topology at epoch 0, JVM field presence afterwards), runs one `EmbeddedTask` per active task (restore from the changelog partitions, fetch from the committed offsets, pipe, produce through the client's idempotent `Producer` (`acks=all`, its own connections) sink and repartition records by the default partitioner and changelog records to the task partition, commit with `OffsetCommit` v9 and the member epoch every `commit_interval_ms`), commits and closes a revoked task before reporting it, follows standby and warm-up changelogs, and restores after a restart. Commands: `pause`, `resume`, `query`.

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

`lab::testing::TestWorld` builds a world from a scenario literal and offers `run_for(ms)`, `run_until(pred, max_ms)`, `frames_between(a, b)` counters and `node_snapshot(id)`. Module tests use it; integration tests in `playground/tests/lab_*.rs` run whole scenarios: a three-broker cluster elects a controller and creates topics; a producer's records reach a consumer through a replicated partition; killing the leader moves leadership and loses no acked record; a schema registered through REST survives a registry wipe; a streams app counts records into a sink topic.

Every assertion uses `assert2`. Wire-facing tests compare whole decoded structs. No test reads source text.
