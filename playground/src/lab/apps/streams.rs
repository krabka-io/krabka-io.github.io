//! The streams node: a `krabka-client-streams` application that joins its
//! streams group with KIP-1071 and runs each task it is assigned through the
//! crate's [`EmbeddedTask`](krabka_client_streams::EmbeddedTask).
//!
//! # Config
//!
//! ```json
//! { "bootstrap": [1, 2, 3], "application_id": "order-stats",
//!   "topology": { "source": "orders",
//!                 "ops": [ { "op": "filter", "field": "total", "gt": 100 },
//!                          { "op": "count_by_key" } ],
//!                 "sink": "order-counts" },
//!   "commit_interval_ms": 100, "num_standby_replicas": 0,
//!   "processing_guarantee": "exactly_once_v2",
//!   "deserialize": { "registry": 4 },
//!   "serialize": { "registry": 4, "format": "avro", "schema": "{...}", "subject": "order-counts-value" } }
//! ```
//!
//! - `bootstrap`, `application_id` (the group id and the prefix of the
//!   internal topics) and `topology` (see [`topology`](super::topology)) are
//!   required.
//! - `commit_interval_ms`: Kafka Streams' `commit.interval.ms`. Default: 100.
//! - `processing_guarantee`: Kafka Streams' `processing.guarantee`,
//!   `"at_least_once"` or `"exactly_once_v2"`. Default: `"at_least_once"`.
//! - `num_standby_replicas`: accepted and not used. With KIP-1071 the group
//!   decides the standbys (the broker's `group.streams.num.standby.replicas`),
//!   as a Kafka Streams client with `group.protocol=streams` ignores its own
//!   setting; a value other than 0 is reported as a warning.
//! - `deserialize`: `null`, or `{"registry": <node id>}` to decode source
//!   values in the Confluent wire format through that registry.
//! - `serialize`: `null`, or a schema the node registers (as the producer
//!   does) and frames the sink values with; repartition and changelog
//!   records stay JSON.
//!
//! # Behaviour
//!
//! The node joins the group `application_id` through the group coordinator
//! with `StreamsGroupHeartbeat`, sends the byte-exact KIP-1071 topology of
//! the compiled spec at epoch 0, and heartbeats on the interval the
//! coordinator names (see [`membership`]). While the group reports
//! `MISSING_SOURCE_TOPICS` or `MISSING_INTERNAL_TOPICS` it waits: the broker
//! creates the internal topics. For each active task `(subtopology,
//! partition)` it opens an embedded task, restores the task's stores from
//! their changelog partitions (from the start to the end offset at the start
//! of the restore, logging off), then fetches the task's source partitions
//! from the group's committed offsets (the start of the partition when none,
//! Kafka Streams' `auto.offset.reset=earliest`), pipes every record, and
//! produces what the task emits: sink and repartition records to the
//! partition the default partitioner picks for their key, changelog records
//! to the task's own partition. The node produces through the client's
//! idempotent [`Producer`] with `acks=all`, on connections of its own, as a
//! stream thread has a producer beside its consumers. The embedded stores
//! have no record cache, so every update of a count is emitted, as with
//! `statestore.cache.max.bytes=0`.
//!
//! Every `commit_interval_ms` the node waits until everything its tasks
//! emitted is acknowledged (Kafka Streams flushes its producer; the lab's
//! producer lingers 5 ms, where Kafka Streams sets `linger.ms` to 100 and
//! flushes), commits the offsets after the records it piped
//! with `OffsetCommit` for the group (member id and epoch), and fires the
//! tasks' wall-clock punctuators; nothing is piped while a commit is under
//! way, as a stream thread blocks in its commit. A revoked task commits and
//! closes before the member reports it gone; a standby keeps following its
//! changelogs, and a promoted standby keeps what it restored. A fenced
//! member drops its tasks without a commit and joins again. A restart starts
//! with empty stores, a new member id, and restores from the changelogs.
//!
//! With `exactly_once_v2` (KIP-447) the producer is transactional, with the
//! transactional id `<application_id>-<process id>-1`, so a restart fences
//! the producer of the run before; every fetch, the restores included, is
//! `read_committed`, and a changelog's end is its last stable offset. Each
//! commit flushes the producer, sends the consumed offsets with
//! `AddOffsetsToTxn` and `TxnOffsetCommit` (member id and epoch as the group
//! generation) and commits the transaction with `EndTxn`, so the outputs, the
//! changelog records and the offsets land together or not at all. A
//! transaction that fails aborts: nothing is piped until the abort is done,
//! then every active task drops its stores and restores them from the
//! changelogs and reads its sources from the committed offsets again, as
//! Kafka Streams wipes a corrupted task's state under EOS. A fenced
//! producer is closed and replaced; a fenced member aborts the open
//! transaction.
//!
//! A record whose value does not decode, or that the topology refuses, is
//! skipped with a `record_skipped` warning, as with Kafka Streams'
//! `LogAndContinueExceptionHandler`. Sink records wait while the `serialize`
//! schema is not registered; a sink value the schema refuses is dropped with
//! `serialization_failed`. Records for a topic the brokers do not have wait
//! for it up to `max.block.ms` (60 s), as Kafka's producer waits on
//! metadata, and hold up the next commit; then they fail with Kafka's text
//! (`produce_failed`).
//!
//! Each start of the node builds new clients whose connection ids come from
//! lanes of their own ([`conn_base`]), so an answer still on the way to the
//! previous run never reaches the new one.
//!
//! # Control commands
//!
//! - `{"cmd": "pause"}` and `{"cmd": "resume"}`: stop and start fetching and
//!   processing (KIP-834); the member keeps its heartbeats.
//! - `{"cmd": "query", "store": s, "key": k}`: the value of `k` in the local
//!   store `s`: `{"store", "key", "task", "value"}`, or `{"store", "key",
//!   "found": false}` when no local task holds the key.
//!
//! # Snapshot
//!
//! `state` (`joining`, `restoring`, `running`, `paused` or `error`),
//! `application_id`, `member_id`, `member_epoch`, `membership` (process id,
//! heartbeats, the group's status list, the owned tasks), `error`,
//! `topology` (`source`, `sink`, `subtopologies`, `repartition_topics`,
//! `stores` with their changelog topics), `tasks: [{"id", "role", "phase",
//! "partitions", "records_in", "records_out", "changelog_out", "skipped",
//! "restored", "lag", "buffered"}]`, `stores: [{"name", "task", "changelog",
//! "entries": [[key, value]]}]` (the first 20 entries per task; a window
//! store's value is `{"window_start", "window_end", "count"}`), `records_in`,
//! `records_out`, `commits`, `commit_interval_ms`, `processing_guarantee`,
//! `aborted_transactions`, `paused`, `last_outputs`
//! (the last ten sink records), `producer` (the client [`Producer`]'s
//! snapshot), `deserialize`, `serialize` and `client`.
//!
//! # Events
//!
//! `streams_joined`, `streams_status`, `tasks_assigned`, `task_restored`,
//! `tasks_revoked`, `streams_fenced` (warn), `streams_error` (error),
//! `commit_failed` (warn), `schema_registered`, `registry_error` (warn),
//! `streams_config` (warn, at start when `num_standby_replicas` is set),
//! `transaction_aborted` (warn, with the tasks that restore again),
//! `producer_restarted` (warn, after a fatal transaction error), and
//! at most one per five seconds of each of `produce_failed` (with Kafka's
//! text when the producer itself failed the record), `record_skipped` and
//! `serialization_failed` (warn).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::BufMut;
use krabka_client_streams::embedded::OutputRecord;
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
        offset_commit_request::{
            self, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::OffsetCommitResponse,
        offset_fetch_request::{
            self, OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        },
        offset_fetch_response::OffsetFetchResponse,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    primitives::uuid::Uuid,
    records::{RecordBatch, RecordsPayload},
};
use serde::Deserialize;
use serde_json::{Value, json};

use self::{
    membership::{Membership, MembershipEvent, Tasks},
    task::{Changelog, Decoded, Emitted, Position, RestoreEnd, Role, StreamTask, TaskId},
};
use super::{
    registry_client::{RegistrationEvent, SchemaCache, SchemaLookup, SchemaRegistration},
    serde::{SchemaFormat, unframe},
    topology::{CompiledTopology, StoreKind, TopologySpec},
};
use crate::lab::{
    LabError,
    client::{
        CONN_ID_LANES, ClientError, ClientEvent, ClientOptions, ConsumedRecord, CoordinatorType,
        GroupMetadata, KafkaClient, Producer, ProducerConfig, ProducerEvent, ProducerRecord,
        RequestId, Response, Target, conn_base, records_of,
    },
    codes,
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId},
    scenario::NodeSpec,
};

pub mod membership;
pub mod task;

/// How many sink records the snapshot lists.
const LAST_OUTPUTS: usize = 10;

/// `max.poll.interval.ms`, which Kafka Streams sends as the rebalance
/// timeout.
const REBALANCE_TIMEOUT_MS: i32 = 300_000;

/// `fetch.max.wait.ms`.
const FETCH_MAX_WAIT_MS: i32 = 500;

/// `max.partition.fetch.bytes`.
const PARTITION_FETCH_BYTES: i32 = 1 << 20;

/// A task fetches its sources again once it has fewer records waiting.
const MAX_BUFFERED: usize = 1_000;

/// The wait before a failed lookup or fetch goes again: `retry.backoff.ms`.
const RETRY_MS: Millis = 100;

/// How long a repeated warning stays out of the timeline.
const QUIET_MS: Millis = 5_000;

/// The last `OffsetCommit` and `OffsetFetch` version that names topics.
const TOPIC_NAME_OFFSET_VERSION: i16 = 9;

/// An `OffsetCommit` capped at the last version that names topics, as
/// Kafka's clients send it.
struct OffsetCommitByName(OffsetCommitRequest);

impl Encode for OffsetCommitByName {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for OffsetCommitByName {
    const API_KEY: i16 = offset_commit_request::API_KEY;
    const MIN_VERSION: i16 = offset_commit_request::MIN_VERSION;
    const MAX_VERSION: i16 = TOPIC_NAME_OFFSET_VERSION;
    const LATEST_STABLE_VERSION: i16 = TOPIC_NAME_OFFSET_VERSION;
    const FLEXIBLE_MIN: i16 = offset_commit_request::FLEXIBLE_MIN;
    type Response = OffsetCommitResponse;
}

/// An `OffsetFetch` capped at the last version that names topics.
struct OffsetFetchByName(OffsetFetchRequest);

impl Encode for OffsetFetchByName {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for OffsetFetchByName {
    const API_KEY: i16 = offset_fetch_request::API_KEY;
    const MIN_VERSION: i16 = offset_fetch_request::MIN_VERSION;
    const MAX_VERSION: i16 = TOPIC_NAME_OFFSET_VERSION;
    const LATEST_STABLE_VERSION: i16 = TOPIC_NAME_OFFSET_VERSION;
    const FLEXIBLE_MIN: i16 = offset_fetch_request::FLEXIBLE_MIN;
    type Response = OffsetFetchResponse;
}

/// The `ListOffsets` timestamps of the earliest and the latest offset.
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;

/// The position lookups one drive sends.
#[derive(Default)]
struct Lookups {
    /// The committed offsets of a task's source topics.
    committed: Vec<(TaskId, Vec<String>)>,
    /// A partition's start or end.
    offsets: Vec<(TaskId, String, Lookup)>,
}

/// What a `ListOffsets` looks up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lookup {
    /// The start of a source partition that has no committed offset.
    SourceStart,
    /// The start of a changelog partition whose restore began below it.
    ChangelogStart,
    /// The end of a changelog partition, where a restore stops.
    ChangelogEnd,
}

/// One partition of a fetch.
#[derive(Clone, PartialEq, Eq, Debug)]
struct FetchPart {
    task: TaskId,
    topic: String,
    changelog: bool,
    offset: i64,
}

/// A request the node waits for.
enum Pending {
    Heartbeat,
    Fetch {
        broker: i32,
        parts: Vec<FetchPart>,
    },
    OffsetFetch {
        task: TaskId,
        topics: Vec<String>,
    },
    ListOffsets {
        task: TaskId,
        topic: String,
        lookup: Lookup,
    },
    Commit {
        offsets: Vec<(TaskId, String, i64)>,
    },
}

/// Where the commit stands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Commit {
    Idle,
    /// Waiting for the emitted records' acknowledgements.
    Flushing,
    Sent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeserializeConfig {
    registry: NodeId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SerializeConfig {
    registry: NodeId,
    format: SchemaFormat,
    schema: Value,
    #[serde(default)]
    subject: Option<String>,
}

fn default_commit_interval_ms() -> Millis {
    100
}

/// Kafka Streams' `processing.guarantee`.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug, Default)]
#[serde(rename_all = "snake_case")]
enum ProcessingGuarantee {
    #[default]
    AtLeastOnce,
    ExactlyOnceV2,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    bootstrap: Vec<NodeId>,
    application_id: String,
    topology: Value,
    #[serde(default = "default_commit_interval_ms")]
    commit_interval_ms: Millis,
    #[serde(default)]
    num_standby_replicas: u32,
    #[serde(default)]
    processing_guarantee: ProcessingGuarantee,
    #[serde(default)]
    deserialize: Option<DeserializeConfig>,
    #[serde(default)]
    serialize: Option<SerializeConfig>,
}

/// The streams node. See the module documentation.
pub struct StreamsNode {
    bootstrap: Vec<Endpoint>,
    application_id: String,
    spec: TopologySpec,
    compiled: CompiledTopology,
    commit_interval_ms: Millis,
    num_standby_replicas: u32,
    /// `exactly_once_v2`: transactional output and offsets, `read_committed`
    /// fetches.
    eos: bool,
    /// The offsets the transaction being committed carries.
    txn_offsets: Option<Vec<(TaskId, String, i64)>>,
    aborted_transactions: u64,
    client: KafkaClient,
    membership: Membership,
    /// Kafka Streams keeps its process id in the state directory, so it
    /// outlives a restart.
    process_id: Option<String>,
    tasks: BTreeMap<TaskId, StreamTask>,
    /// The coordinator's last assignment.
    target: Tasks,
    /// The standby-role tasks that are warm-ups.
    warmups: BTreeSet<TaskId>,
    /// The stream thread's producer, on connections of its own.
    producer: Producer,
    /// How many times the node started: the connection-id lanes of its
    /// clients.
    starts: u32,
    pending: BTreeMap<RequestId, Pending>,
    /// Brokers with a fetch on the wire.
    fetching: BTreeSet<i32>,
    commit: Commit,
    next_commit_at: Millis,
    paused: bool,
    failed: Option<String>,
    records_in: u64,
    records_out: u64,
    commits: u64,
    last_outputs: VecDeque<Value>,
    /// Sink records that wait for the schema registration.
    held: VecDeque<OutputRecord>,
    schemas: Option<SchemaCache>,
    serializer: Option<SchemaRegistration>,
    /// The last warning of each kind and when it went out.
    quiet: BTreeMap<&'static str, Millis>,
}

impl StreamsNode {
    /// # Errors
    /// Returns a config error for an unknown or malformed key, a topology
    /// that does not parse or build, or a schema that does not parse.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let bad = |reason: String| LabError::config(spec, reason);
        let config: Config =
            serde_json::from_value(spec.config.clone()).map_err(|e| bad(format!("config: {e}")))?;
        if config.bootstrap.is_empty() {
            return Err(bad("`bootstrap` needs at least one broker".to_string()));
        }
        if config.application_id.is_empty() {
            return Err(bad("`application_id` is empty".to_string()));
        }
        let topology = TopologySpec::parse(&config.topology).map_err(bad)?;
        let compiled = CompiledTopology::new(&config.application_id, &topology).map_err(bad)?;
        let serializer = config
            .serialize
            .map(|s| {
                let text = match s.schema {
                    Value::String(text) => text,
                    doc => doc.to_string(),
                };
                SchemaRegistration::new(
                    s.registry,
                    s.subject
                        .unwrap_or_else(|| format!("{}-value", topology.sink)),
                    s.format,
                    text,
                )
                .map_err(|e| bad(format!("`serialize.schema`: {e}")))
            })
            .transpose()?;
        let bootstrap: Vec<Endpoint> = config
            .bootstrap
            .iter()
            .map(|n| Endpoint::kafka(*n))
            .collect();
        let client = KafkaClient::new(bootstrap.clone(), "streams", ClientOptions::default());
        let producer = build_producer(&bootstrap, "streams-producer", 1, None);
        let membership = Membership::new(
            &config.application_id,
            "",
            "",
            REBALANCE_TIMEOUT_MS,
            compiled.built.to_wire_request(),
            0,
        );
        Ok(Self {
            bootstrap,
            application_id: config.application_id,
            spec: topology,
            compiled,
            commit_interval_ms: config.commit_interval_ms.max(1),
            num_standby_replicas: config.num_standby_replicas,
            eos: config.processing_guarantee == ProcessingGuarantee::ExactlyOnceV2,
            txn_offsets: None,
            aborted_transactions: 0,
            client,
            membership,
            process_id: None,
            tasks: BTreeMap::new(),
            target: Tasks::default(),
            warmups: BTreeSet::new(),
            producer,
            starts: 0,
            pending: BTreeMap::new(),
            fetching: BTreeSet::new(),
            commit: Commit::Idle,
            next_commit_at: 0,
            paused: false,
            failed: None,
            records_in: 0,
            records_out: 0,
            commits: 0,
            last_outputs: VecDeque::new(),
            held: VecDeque::new(),
            schemas: config.deserialize.map(|d| SchemaCache::new(d.registry)),
            serializer,
            quiet: BTreeMap::new(),
        })
    }

    fn coordinator(&self) -> Target {
        Target::Coordinator {
            key_type: CoordinatorType::Group,
            key: self.application_id.clone(),
        }
    }

    /// Every topic the topology reads or writes, for the metadata.
    fn topics(&self) -> Vec<String> {
        let mut topics: BTreeSet<String> = BTreeSet::new();
        topics.insert(self.spec.source.clone());
        topics.insert(self.spec.sink.clone());
        topics.extend(self.compiled.plan.repartition_topics.iter().cloned());
        topics.extend(
            self.compiled
                .plan
                .stores
                .iter()
                .map(|s| s.changelog.clone()),
        );
        topics.into_iter().collect()
    }

    /// Emit a warning unless one of the same kind went out recently.
    fn warn(&mut self, ctx: &mut Ctx<'_>, kind: &'static str, detail: Value) {
        let now = ctx.now();
        if self.quiet.get(kind).is_some_and(|at| now < at + QUIET_MS) {
            return;
        }
        self.quiet.insert(kind, now);
        ctx.event(kind, detail);
    }

    // ---- responses ------------------------------------------------------------

    fn on_client_events(&mut self, ctx: &mut Ctx<'_>, events: Vec<ClientEvent>) {
        for event in events {
            let ClientEvent::Response { id, result } = event else {
                continue;
            };
            match self.pending.remove(&id) {
                Some(Pending::Heartbeat) => {
                    let response = result
                        .ok()
                        .and_then(Response::downcast::<StreamsGroupHeartbeatResponse>);
                    let events = self.membership.on_response(ctx.now(), response);
                    self.on_membership(ctx, events);
                }
                Some(Pending::Fetch { broker, parts }) => {
                    self.fetching.remove(&broker);
                    self.on_fetch(ctx, parts, result);
                }
                Some(Pending::OffsetFetch { task, topics }) => {
                    self.on_offset_fetch(ctx, &task, &topics, result);
                }
                Some(Pending::ListOffsets {
                    task,
                    topic,
                    lookup,
                }) => self.on_list_offsets(ctx, &task, &topic, lookup, result),
                Some(Pending::Commit { offsets }) => self.on_commit(ctx, &offsets, result),
                None => {}
            }
        }
    }

    fn on_membership(&mut self, ctx: &mut Ctx<'_>, events: Vec<MembershipEvent>) {
        for event in events {
            match event {
                MembershipEvent::Joined { member_id, epoch } => ctx.event(
                    "streams_joined",
                    json!({ "member_id": member_id, "epoch": epoch }),
                ),
                MembershipEvent::Status(status) => {
                    let level = if status.iter().any(|(code, _, _)| *code != 5) {
                        "warn"
                    } else {
                        "info"
                    };
                    let listed: Vec<Value> = status
                        .iter()
                        .map(|(code, name, detail)| {
                            json!({ "code": code, "name": name, "detail": detail })
                        })
                        .collect();
                    ctx.event(
                        "streams_status",
                        json!({ "status": listed, "level": level }),
                    );
                }
                MembershipEvent::Assigned(target) => {
                    self.target = target;
                    self.reconcile(ctx);
                }
                MembershipEvent::Fenced { code } => {
                    // The open transaction cannot commit for a member that
                    // is gone; its outputs abort.
                    if self.producer.transaction_open() && self.producer.abort_transaction().is_ok()
                    {
                        self.txn_offsets = None;
                    }
                    // The tasks are lost: closed without a commit.
                    let lost: Vec<String> = self.tasks.keys().map(ToString::to_string).collect();
                    self.tasks.clear();
                    self.target = Tasks::default();
                    self.warmups.clear();
                    self.commit = Commit::Idle;
                    self.held.clear();
                    ctx.event(
                        "streams_fenced",
                        json!({ "code": code, "lost": lost, "level": "warn" }),
                    );
                }
                MembershipEvent::Retry { code } => {
                    if code == codes::NETWORK_EXCEPTION {
                        self.client
                            .invalidate_coordinator(CoordinatorType::Group, &self.application_id);
                    } else {
                        let target = self.coordinator();
                        self.client.note_error(code, &target);
                    }
                }
                MembershipEvent::Failed { code, message } => {
                    ctx.event(
                        "streams_error",
                        json!({ "code": code, "message": message, "level": "error" }),
                    );
                    self.failed = Some(message);
                }
            }
        }
    }

    /// Move toward the coordinator's assignment: close the active tasks it
    /// took away (after their commit), drop the standbys it took away,
    /// promote or open the rest. A warm-up task runs as a standby, as Kafka
    /// Streams runs it, and is reported as a warm-up. An active task that
    /// became a standby opens as one once it closed.
    fn reconcile(&mut self, ctx: &mut Ctx<'_>) {
        let wanted = |set: &membership::TaskSet| -> BTreeSet<TaskId> {
            Tasks::list(set)
                .into_iter()
                .map(|(subtopology, partition)| TaskId {
                    subtopology,
                    partition,
                })
                .collect()
        };
        let active = wanted(&self.target.active);
        let warmup: BTreeSet<TaskId> = wanted(&self.target.warmup)
            .into_iter()
            .filter(|id| !active.contains(id))
            .collect();
        let standby: BTreeSet<TaskId> = wanted(&self.target.standby)
            .into_iter()
            .filter(|id| !active.contains(id))
            .chain(warmup.iter().cloned())
            .collect();
        let mut revoked = Vec::new();
        let mut dropped = Vec::new();
        for (id, task) in &mut self.tasks {
            match task.role {
                Role::Active if !active.contains(id) && !task.closing => {
                    task.closing = true;
                    revoked.push(id.to_string());
                }
                Role::Active if active.contains(id) => task.closing = false,
                Role::Standby if !standby.contains(id) && !active.contains(id) => {
                    dropped.push(id.clone());
                }
                _ => {}
            }
        }
        for id in &dropped {
            self.tasks.remove(id);
        }
        let mut opened = Vec::new();
        for id in &active {
            match self.tasks.get_mut(id) {
                Some(task) if task.role == Role::Standby => {
                    task.promote(&self.compiled);
                    opened.push(id.to_string());
                }
                Some(_) => {}
                None => match StreamTask::new(&self.compiled, id.clone(), Role::Active) {
                    Ok(task) => {
                        self.tasks.insert(id.clone(), task);
                        opened.push(id.to_string());
                    }
                    Err(e) => ctx.event(
                        "streams_error",
                        json!({ "task": id.to_string(), "message": e, "level": "error" }),
                    ),
                },
            }
        }
        for id in &standby {
            // A closing active task opens as a standby once it closed.
            if self.tasks.contains_key(id) {
                continue;
            }
            if let Ok(task) = StreamTask::new(&self.compiled, id.clone(), Role::Standby) {
                self.tasks.insert(id.clone(), task);
                let role = if warmup.contains(id) {
                    "warm-up"
                } else {
                    "standby"
                };
                opened.push(format!("{id} ({role})"));
            }
        }
        self.warmups = warmup;
        if !revoked.is_empty() {
            ctx.event("tasks_revoked", json!({ "tasks": revoked }));
        }
        if !opened.is_empty() {
            ctx.event("tasks_assigned", json!({ "tasks": opened }));
        }
        self.report_owned();
    }

    /// Report the tasks the member runs, once no revoked task is still
    /// committing.
    fn report_owned(&mut self) {
        if self.tasks.values().any(|t| t.closing) {
            return;
        }
        let mut owned = Tasks::default();
        for (id, task) in &self.tasks {
            let set = match task.role {
                Role::Active => &mut owned.active,
                Role::Standby if self.warmups.contains(id) => &mut owned.warmup,
                Role::Standby => &mut owned.standby,
            };
            set.entry(id.subtopology.clone())
                .or_default()
                .insert(id.partition);
        }
        self.membership.set_owned(owned);
    }

    fn on_fetch(
        &mut self,
        ctx: &mut Ctx<'_>,
        parts: Vec<FetchPart>,
        result: Result<Response, ClientError>,
    ) {
        let now = ctx.now();
        let response = result.ok().and_then(Response::downcast::<FetchResponse>);
        for part in parts {
            let Some(task) = self.tasks.get_mut(&part.task) else {
                continue;
            };
            let partition = part.task.partition;
            let row = response.as_ref().and_then(|r| {
                r.responses
                    .iter()
                    .find(|t| {
                        t.topic == part.topic
                            || self.client.metadata().topic_id(&part.topic) == Some(t.topic_id)
                    })
                    .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
            });
            let (code, records, next, hwm, leader) = match row {
                Some(row) if row.error_code == codes::NONE => {
                    let batches: &[RecordBatch] = row
                        .records
                        .as_ref()
                        .and_then(RecordsPayload::as_v2)
                        .unwrap_or(&[]);
                    let aborted = self
                        .eos
                        .then(|| row.aborted_transactions.as_deref().unwrap_or_default());
                    let (records, next) =
                        records_of(&part.topic, partition, batches, part.offset, aborted);
                    (codes::NONE, records, next, Some(row.high_watermark), None)
                }
                Some(row) => (
                    row.error_code,
                    Vec::new(),
                    None,
                    None,
                    Some((
                        row.current_leader.leader_id,
                        row.current_leader.leader_epoch,
                    )),
                ),
                None => (codes::NETWORK_EXCEPTION, Vec::new(), None, None, None),
            };
            if part.changelog {
                let Some(changelog) = task.changelogs.get_mut(&part.topic) else {
                    continue;
                };
                changelog.fetching = false;
                changelog_state(changelog).apply(code, next, hwm, now);
                task.restore(&part.topic, &records);
                if task.check_restored() {
                    ctx.event(
                        "task_restored",
                        json!({ "task": task.id.to_string(), "records": task.restored() }),
                    );
                }
            } else {
                let Some(source) = task.sources.get_mut(&part.topic) else {
                    continue;
                };
                source.fetching = false;
                FetchState {
                    position: &mut source.position,
                    hwm: &mut source.hwm,
                    retry_at: &mut source.retry_at,
                }
                .apply(code, next, hwm, now);
                task.buffer.extend(records);
            }
            if code != codes::NONE && code != codes::OFFSET_OUT_OF_RANGE {
                if let Some((leader, epoch)) = leader {
                    self.client
                        .update_leader(&part.topic, partition, leader, epoch);
                }
                let target = Target::Leader {
                    topic: part.topic.clone(),
                    partition,
                };
                self.client.note_error(code, &target);
            }
        }
    }

    fn on_offset_fetch(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: &TaskId,
        topics: &[String],
        result: Result<Response, ClientError>,
    ) {
        let now = ctx.now();
        let response = result
            .ok()
            .and_then(Response::downcast::<OffsetFetchResponse>);
        let Some(task) = self.tasks.get_mut(id) else {
            return;
        };
        let group = response.as_ref().and_then(|r| r.groups.first());
        let error = match (&response, group) {
            (None, _) => Some(codes::NETWORK_EXCEPTION),
            (Some(r), _) if r.error_code != codes::NONE => Some(r.error_code),
            (Some(_), Some(g)) if g.error_code != codes::NONE => Some(g.error_code),
            _ => None,
        };
        for topic in topics {
            let Some(source) = task.sources.get_mut(topic) else {
                continue;
            };
            let row = group
                .and_then(|g| g.topics.iter().find(|t| t.name == *topic))
                .and_then(|t| {
                    t.partitions
                        .iter()
                        .find(|p| p.partition_index == id.partition)
                });
            match (error, row) {
                (None, Some(row)) if row.error_code == codes::NONE && row.committed_offset >= 0 => {
                    source.position = Position::At(row.committed_offset);
                    source.processed = Some(row.committed_offset);
                    source.committed = Some(row.committed_offset);
                }
                (None, Some(row)) if row.error_code == codes::NONE => {
                    // Nothing committed: Kafka Streams' `auto.offset.reset`
                    // is `earliest`.
                    source.position = Position::Reset;
                }
                _ => {
                    source.position = Position::Unknown;
                    source.retry_at = now + RETRY_MS;
                }
            }
        }
        if let Some(code) = error {
            let target = self.coordinator();
            self.client.note_error(code, &target);
        }
    }

    fn on_list_offsets(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: &TaskId,
        topic: &str,
        lookup: Lookup,
        result: Result<Response, ClientError>,
    ) {
        let now = ctx.now();
        let offset = result
            .ok()
            .and_then(Response::downcast::<ListOffsetsResponse>)
            .and_then(|r| {
                r.topics
                    .iter()
                    .find(|t| t.name == topic)
                    .and_then(|t| {
                        t.partitions
                            .iter()
                            .find(|p| p.partition_index == id.partition)
                    })
                    .map(|p| (p.error_code, p.offset))
            });
        let Some(task) = self.tasks.get_mut(id) else {
            return;
        };
        let found = offset.and_then(|(code, o)| (code == codes::NONE && o >= 0).then_some(o));
        // A failed lookup goes again after the backoff.
        let retry_at = if found.is_some() { 0 } else { now + RETRY_MS };
        match lookup {
            Lookup::SourceStart => {
                // A partition the task has not processed from needs no
                // commit, as Kafka Streams commits only consumed offsets.
                if let Some(source) = task.sources.get_mut(topic) {
                    source.position = found.map_or(Position::Reset, Position::At);
                    source.retry_at = retry_at;
                }
            }
            Lookup::ChangelogStart => {
                if let Some(changelog) = task.changelogs.get_mut(topic) {
                    changelog.position = found.map_or(Position::Reset, Position::At);
                    changelog.retry_at = retry_at;
                }
            }
            Lookup::ChangelogEnd => {
                if let Some(changelog) = task.changelogs.get_mut(topic) {
                    changelog.end = found.map_or(RestoreEnd::Unknown, RestoreEnd::At);
                    changelog.retry_at = retry_at;
                }
                if task.check_restored() {
                    ctx.event(
                        "task_restored",
                        json!({ "task": task.id.to_string(), "records": task.restored() }),
                    );
                }
            }
        }
        if let Some((code, _)) = offset
            && code != codes::NONE
        {
            let target = Target::Leader {
                topic: topic.to_string(),
                partition: id.partition,
            };
            self.client.note_error(code, &target);
        }
    }

    fn on_commit(
        &mut self,
        ctx: &mut Ctx<'_>,
        offsets: &[(TaskId, String, i64)],
        result: Result<Response, ClientError>,
    ) {
        let response = result
            .ok()
            .and_then(Response::downcast::<OffsetCommitResponse>);
        let mut failed = None;
        for (id, topic, offset) in offsets {
            let code = response.as_ref().map_or(codes::NETWORK_EXCEPTION, |r| {
                r.topics
                    .iter()
                    .find(|t| t.name == *topic)
                    .and_then(|t| {
                        t.partitions
                            .iter()
                            .find(|p| p.partition_index == id.partition)
                    })
                    .map_or(codes::UNKNOWN_SERVER_ERROR, |p| p.error_code)
            });
            if code == codes::NONE {
                if let Some(source) = self
                    .tasks
                    .get_mut(id)
                    .and_then(|t| t.sources.get_mut(topic))
                {
                    source.committed = Some(*offset);
                }
            } else {
                failed = Some(code);
            }
        }
        if let Some(code) = failed {
            let target = self.coordinator();
            self.client.note_error(code, &target);
            ctx.event("commit_failed", json!({ "code": code, "level": "warn" }));
        } else {
            self.commits += 1;
        }
        self.finish_commit(ctx);
    }

    fn on_producer_events(&mut self, ctx: &mut Ctx<'_>, events: Vec<ProducerEvent>) {
        for event in events {
            match event {
                ProducerEvent::Failed {
                    topic,
                    partition,
                    code,
                    message,
                    ..
                } => self.warn(
                    ctx,
                    "produce_failed",
                    json!({
                        "topic": topic,
                        "partition": partition,
                        "code": code,
                        "message": message,
                        "level": "warn",
                    }),
                ),
                ProducerEvent::Acked { .. } => {}
                ProducerEvent::TransactionEnded { committed: true } => {
                    if let Some(offsets) = self.txn_offsets.take() {
                        self.mark_committed(&offsets);
                        self.commits += 1;
                    }
                    if self.commit == Commit::Sent {
                        self.finish_commit(ctx);
                    }
                }
                ProducerEvent::TransactionEnded { committed: false } => {
                    self.restore_after_abort(ctx);
                }
                ProducerEvent::TransactionError { fatal: false, .. } => {
                    // Abort, and pipe nothing until the abort is done.
                    self.txn_offsets = None;
                    self.commit = Commit::Sent;
                    if self.producer.abort_transaction().is_err() {
                        self.restore_after_abort(ctx);
                    }
                }
                ProducerEvent::TransactionError { fatal: true, .. } => {
                    ctx.event("producer_restarted", json!({ "level": "warn" }));
                    self.producer.close(ctx);
                    let lane = 2 * (self.starts % (CONN_ID_LANES / 2)) + 1;
                    self.starts = self.starts.wrapping_add(1);
                    let client_id = self.producer.client().client_id().to_string();
                    self.producer =
                        build_producer(&self.bootstrap, &client_id, lane, self.transactional_id());
                    self.restore_after_abort(ctx);
                }
            }
        }
    }

    /// Kafka Streams' EOS v2 transactional id: `<application id>-<process
    /// id>-<thread>`, stable across restarts so a new run fences the old.
    fn transactional_id(&self) -> Option<String> {
        self.eos.then(|| {
            format!(
                "{}-{}-1",
                self.application_id,
                self.process_id.as_deref().unwrap_or_default()
            )
        })
    }

    /// The committed offsets of the active tasks' sources.
    fn mark_committed(&mut self, offsets: &[(TaskId, String, i64)]) {
        for (id, topic, offset) in offsets {
            if let Some(source) = self
                .tasks
                .get_mut(id)
                .and_then(|t| t.sources.get_mut(topic))
            {
                source.committed = Some(*offset);
            }
        }
    }

    /// The transaction aborted: what the tasks did since the last commit is
    /// gone, so each active task drops its stores and restores them from the
    /// changelogs, and reads its sources from the committed offsets again.
    fn restore_after_abort(&mut self, ctx: &mut Ctx<'_>) {
        self.aborted_transactions += 1;
        self.txn_offsets = None;
        self.held.clear();
        let mut reset = Vec::new();
        let ids: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|(_, t)| t.role == Role::Active)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let closing = self.tasks.get(&id).is_some_and(|t| t.closing);
            if let Ok(mut task) = StreamTask::new(&self.compiled, id.clone(), Role::Active) {
                task.closing = closing;
                if let Some(old) = self.tasks.insert(id.clone(), task) {
                    old.embedded.close();
                }
                reset.push(id.to_string());
            }
        }
        ctx.event(
            "transaction_aborted",
            json!({ "tasks": reset, "level": "warn" }),
        );
        self.finish_commit(ctx);
    }

    // ---- driving --------------------------------------------------------------

    /// The position lookups due at `now`, marked as on the wire: the
    /// committed offsets of each task's sources, once the member holds an
    /// epoch, and the starts and ends the partitions need.
    fn due_lookups(&mut self, now: Millis) -> Lookups {
        let stable = self.membership.epoch() > 0;
        let mut due = Lookups::default();
        for (id, task) in &mut self.tasks {
            if task.closing {
                continue;
            }
            let mut unknown = Vec::new();
            for (topic, source) in &mut task.sources {
                if now < source.retry_at {
                    continue;
                }
                match source.position {
                    Position::Unknown if stable => {
                        source.position = Position::Looking;
                        unknown.push(topic.clone());
                    }
                    Position::Reset => {
                        source.position = Position::Looking;
                        due.offsets
                            .push((id.clone(), topic.clone(), Lookup::SourceStart));
                    }
                    _ => {}
                }
            }
            if !unknown.is_empty() {
                due.committed.push((id.clone(), unknown));
            }
            for (topic, changelog) in &mut task.changelogs {
                if now < changelog.retry_at {
                    continue;
                }
                if changelog.position == Position::Reset {
                    changelog.position = Position::Looking;
                    due.offsets
                        .push((id.clone(), topic.clone(), Lookup::ChangelogStart));
                }
                if changelog.end == RestoreEnd::Unknown {
                    changelog.end = RestoreEnd::Looking;
                    due.offsets
                        .push((id.clone(), topic.clone(), Lookup::ChangelogEnd));
                }
            }
        }
        due
    }

    /// Send the position lookups the tasks need: `OffsetFetch` to the group
    /// coordinator for committed offsets, `ListOffsets` to the leaders for
    /// starts and ends.
    fn request_positions(&mut self, ctx: &mut Ctx<'_>) {
        let due = self.due_lookups(ctx.now());
        for (task, topics) in due.committed {
            let request = OffsetFetchByName(OffsetFetchRequest {
                groups: vec![OffsetFetchRequestGroup {
                    group_id: self.application_id.clone(),
                    member_id: Some(self.membership.member_id().to_string()),
                    member_epoch: self.membership.epoch(),
                    topics: Some(
                        topics
                            .iter()
                            .map(|name| OffsetFetchRequestTopics {
                                name: name.clone(),
                                partition_indexes: vec![task.partition],
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ..Default::default()
                }],
                require_stable: true,
                ..Default::default()
            });
            let target = self.coordinator();
            let id = self.client.send(ctx, target, request);
            self.pending
                .insert(id, Pending::OffsetFetch { task, topics });
        }
        for (task, topic, lookup) in due.offsets {
            let target = Target::Leader {
                topic: topic.clone(),
                partition: task.partition,
            };
            let request = list_offsets(&topic, task.partition, lookup, self.eos);
            let id = self.client.send(ctx, target, request);
            self.pending.insert(
                id,
                Pending::ListOffsets {
                    task,
                    topic,
                    lookup,
                },
            );
        }
    }

    /// Pipe what the running tasks fetched, and hand what they emit to the
    /// producer.
    fn process(&mut self, ctx: &mut Ctx<'_>) {
        if self.paused || self.failed.is_some() || self.commit != Commit::Idle {
            return;
        }
        self.release_held(ctx);
        if !self.held.is_empty() {
            return;
        }
        let ids: Vec<TaskId> = self.tasks.keys().cloned().collect();
        for id in ids {
            let Some(task) = self.tasks.get_mut(&id) else {
                continue;
            };
            let schemas = &mut self.schemas;
            let emitted = task.run(|record| decode(ctx, schemas, record));
            self.emit(ctx, id.partition, emitted);
        }
    }

    /// Hand what a task emitted to the producer: its changelog records to
    /// the task's partition, its outputs through the sink serializer.
    fn emit(&mut self, ctx: &mut Ctx<'_>, partition: i32, emitted: Emitted) {
        self.records_in += emitted.piped;
        for (topic, offset, error) in emitted.skipped {
            self.warn(
                ctx,
                "record_skipped",
                json!({ "topic": topic, "offset": offset, "error": error, "level": "warn" }),
            );
        }
        let now = ctx.now();
        for record in emitted.changelogs {
            self.producer.send(
                now,
                ProducerRecord {
                    topic: record.topic,
                    partition: Some(partition),
                    key: Some(record.key),
                    value: record.value,
                    timestamp: record.timestamp,
                    ..ProducerRecord::default()
                },
            );
        }
        self.records_out += u64::try_from(emitted.outputs.len()).unwrap_or(u64::MAX);
        self.held.extend(emitted.outputs);
        self.release_held(ctx);
    }

    /// Hand the emitted records to the producer; sink records wait while the
    /// sink schema is not registered.
    fn release_held(&mut self, ctx: &mut Ctx<'_>) {
        while let Some(output) = self.held.front() {
            let is_sink = output.topic == self.spec.sink;
            let value = match (&mut self.serializer, is_sink) {
                (Some(registration), true) => {
                    let doc: Value = output
                        .value
                        .as_deref()
                        .and_then(|v| serde_json::from_slice(v).ok())
                        .unwrap_or(Value::Null);
                    match registration.serialize(&doc) {
                        Ok(Some(framed)) => Some(Some(framed)),
                        Ok(None) => break,
                        Err(e) => {
                            let detail = json!({ "topic": output.topic, "error": e.to_string(), "level": "warn" });
                            self.warn(ctx, "serialization_failed", detail);
                            None
                        }
                    }
                }
                _ => Some(output.value.clone()),
            };
            let Some(output) = self.held.pop_front() else {
                break;
            };
            let Some(value) = value else {
                continue;
            };
            if is_sink {
                if self.last_outputs.len() == LAST_OUTPUTS {
                    self.last_outputs.pop_front();
                }
                self.last_outputs.push_back(json!({
                    "topic": output.topic,
                    "key": output.key.as_deref().map(String::from_utf8_lossy),
                    "value": output.value.as_deref().map_or(Value::Null, super::serde::preview),
                    "timestamp": output.timestamp,
                }));
            }
            self.producer.send(
                ctx.now(),
                ProducerRecord {
                    topic: output.topic,
                    key: output.key,
                    value,
                    timestamp: Some(output.timestamp),
                    ..ProducerRecord::default()
                },
            );
        }
    }

    /// Start, flush and send the commit when it is due, or when a revoked
    /// task waits to close.
    fn commit_step(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self.commit == Commit::Idle {
            let closing = self.tasks.values().any(|t| t.closing);
            let active = self.tasks.values().any(|t| t.role == Role::Active);
            if closing || (active && now >= self.next_commit_at) {
                self.commit = Commit::Flushing;
            }
        }
        if self.commit != Commit::Flushing
            || self.producer.pending_records() > 0
            || !self.held.is_empty()
        {
            return;
        }
        let offsets: Vec<(TaskId, String, i64)> = self
            .tasks
            .iter()
            .filter(|(_, t)| t.role == Role::Active)
            .flat_map(|(id, t)| {
                t.sources
                    .iter()
                    .filter_map(move |(topic, s)| match s.processed {
                        Some(p) if s.committed != Some(p) => Some((id.clone(), topic.clone(), p)),
                        _ => None,
                    })
            })
            .collect();
        if self.eos {
            self.commit_transaction(ctx, offsets);
            return;
        }
        if offsets.is_empty() || self.membership.epoch() <= 0 {
            self.finish_commit(ctx);
            return;
        }
        let mut topics: BTreeMap<String, Vec<OffsetCommitRequestPartition>> = BTreeMap::new();
        for (id, topic, offset) in &offsets {
            topics
                .entry(topic.clone())
                .or_default()
                .push(OffsetCommitRequestPartition {
                    partition_index: id.partition,
                    committed_offset: *offset,
                    committed_leader_epoch: -1,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                });
        }
        let request = OffsetCommitByName(OffsetCommitRequest {
            group_id: self.application_id.clone(),
            generation_id_or_member_epoch: self.membership.epoch(),
            member_id: self.membership.member_id().to_string(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: topics
                .into_iter()
                .map(|(name, partitions)| OffsetCommitRequestTopic {
                    name,
                    partitions,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });
        let target = self.coordinator();
        let id = self.client.send(ctx, target, request);
        self.pending.insert(id, Pending::Commit { offsets });
        self.commit = Commit::Sent;
    }

    /// Commit the open transaction with `offsets`: the producer sends them
    /// with `AddOffsetsToTxn` and `TxnOffsetCommit`, then `EndTxn`, and the
    /// commit ends when [`ProducerEvent::TransactionEnded`] arrives.
    fn commit_transaction(&mut self, ctx: &mut Ctx<'_>, offsets: Vec<(TaskId, String, i64)>) {
        let open = self.producer.transaction_open();
        let epoch = self.membership.epoch();
        if (offsets.is_empty() && !open) || epoch <= 0 {
            self.finish_commit(ctx);
            return;
        }
        if !offsets.is_empty() {
            let group = GroupMetadata {
                group_id: self.application_id.clone(),
                member_id: self.membership.member_id().to_string(),
                generation: epoch,
            };
            let rows = offsets
                .iter()
                .map(|(id, topic, offset)| (topic.clone(), id.partition, *offset))
                .collect();
            if self
                .producer
                .send_offsets_to_transaction(group, rows)
                .is_err()
            {
                // The transaction already failed; its abort is under way.
                return;
            }
        }
        if self.producer.commit_transaction().is_ok() {
            self.txn_offsets = Some(offsets);
            self.commit = Commit::Sent;
        }
    }

    /// End a commit: fire the wall-clock punctuators, close the revoked
    /// tasks and report the tasks the member keeps.
    fn finish_commit(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        self.commit = Commit::Idle;
        self.next_commit_at = now + self.commit_interval_ms;
        let wall = i64::try_from(now).unwrap_or(i64::MAX);
        let ids: Vec<TaskId> = self.tasks.keys().cloned().collect();
        for id in ids {
            if let Some(task) = self.tasks.get_mut(&id) {
                let emitted = task.punctuate(wall);
                self.emit(ctx, id.partition, emitted);
            }
        }
        let closed: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|(_, t)| t.closing)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &closed {
            if let Some(task) = self.tasks.remove(id) {
                task.embedded.close();
            }
        }
        if !closed.is_empty() {
            self.reconcile(ctx);
        }
    }

    /// Send one fetch per broker for the partitions that may fetch.
    fn fetch(&mut self, ctx: &mut Ctx<'_>) {
        if self.paused || self.failed.is_some() {
            return;
        }
        let now = ctx.now();
        let mut per_broker: BTreeMap<i32, Vec<FetchPart>> = BTreeMap::new();
        let mut missing: BTreeSet<String> = BTreeSet::new();
        for (id, task) in &mut self.tasks {
            if task.closing {
                continue;
            }
            let mut parts: Vec<(String, bool, i64)> = Vec::new();
            if task.role == Role::Active && task.running && task.buffer.len() < MAX_BUFFERED {
                for (topic, s) in &task.sources {
                    if let (Position::At(offset), false) = (s.position, s.fetching)
                        && now >= s.retry_at
                    {
                        parts.push((topic.clone(), false, offset));
                    }
                }
            }
            for (topic, c) in &task.changelogs {
                let wanted = match (c.end, c.position) {
                    (RestoreEnd::Follow, Position::At(_)) => true,
                    (RestoreEnd::At(end), Position::At(p)) => p < end && !task.running,
                    _ => false,
                };
                if wanted
                    && !c.fetching
                    && now >= c.retry_at
                    && let Position::At(offset) = c.position
                {
                    parts.push((topic.clone(), true, offset));
                }
            }
            for (topic, changelog, offset) in parts {
                let Some(leader) = self.client.metadata().leader(&topic, id.partition) else {
                    missing.insert(topic);
                    continue;
                };
                if self.fetching.contains(&leader) {
                    continue;
                }
                per_broker.entry(leader).or_default().push(FetchPart {
                    task: id.clone(),
                    topic,
                    changelog,
                    offset,
                });
            }
        }
        if !missing.is_empty() {
            self.client.add_topics(missing.iter().map(String::as_str));
            self.client.request_metadata_refresh();
        }
        for (broker, parts) in per_broker {
            for part in &parts {
                if let Some(task) = self.tasks.get_mut(&part.task) {
                    if part.changelog {
                        if let Some(c) = task.changelogs.get_mut(&part.topic) {
                            c.fetching = true;
                        }
                    } else if let Some(s) = task.sources.get_mut(&part.topic) {
                        s.fetching = true;
                    }
                }
            }
            let request = self.fetch_request(&parts);
            let id = self.client.send(ctx, Target::Broker(broker), request);
            self.fetching.insert(broker);
            self.pending.insert(id, Pending::Fetch { broker, parts });
        }
    }

    fn fetch_request(&self, parts: &[FetchPart]) -> FetchRequest {
        let mut topics: BTreeMap<String, Vec<FetchPartition>> = BTreeMap::new();
        for part in parts {
            let epoch = self
                .client
                .metadata()
                .partition(&part.topic, part.task.partition)
                .map_or(-1, |p| p.leader_epoch);
            topics
                .entry(part.topic.clone())
                .or_default()
                .push(FetchPartition {
                    partition: part.task.partition,
                    current_leader_epoch: epoch,
                    fetch_offset: part.offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: PARTITION_FETCH_BYTES,
                    ..Default::default()
                });
        }
        FetchRequest {
            replica_id: -1,
            max_wait_ms: FETCH_MAX_WAIT_MS,
            min_bytes: 1,
            max_bytes: 52_428_800,
            isolation_level: i8::from(self.eos),
            session_id: 0,
            session_epoch: -1,
            topics: topics
                .into_iter()
                .map(|(topic, partitions)| FetchTopic {
                    topic_id: self
                        .client
                        .metadata()
                        .topic_id(&topic)
                        .unwrap_or(Uuid::ZERO),
                    topic,
                    partitions,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn heartbeat(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(request) = self.membership.poll(ctx.now()) {
            let target = self.coordinator();
            let id = self.client.send(ctx, target, request);
            self.pending.insert(id, Pending::Heartbeat);
        }
    }

    /// Move everything on, then arm the next deadline.
    fn drive(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(registration) = &mut self.serializer {
            registration.poll(ctx);
        }
        self.request_positions(ctx);
        self.process(ctx);
        self.commit_step(ctx);
        self.fetch(ctx);
        self.heartbeat(ctx);
        let now = ctx.now();
        let active = self.tasks.values().any(|t| t.role == Role::Active);
        let commit = (self.commit == Commit::Idle && active).then_some(self.next_commit_at);
        let retries = self.tasks.values().flat_map(|t| {
            t.sources
                .values()
                .map(|s| s.retry_at)
                .chain(t.changelogs.values().map(|c| c.retry_at))
                .filter(|at| *at > now)
        });
        let deadline = self
            .client
            .next_deadline(now)
            .into_iter()
            .chain(self.membership.next_deadline(now))
            .chain(self.producer.next_deadline(now))
            .chain(commit)
            .chain(retries)
            .chain(self.schemas.as_ref().and_then(SchemaCache::next_deadline))
            .chain(
                self.serializer
                    .as_ref()
                    .and_then(SchemaRegistration::next_deadline),
            )
            .min();
        if let Some(at) = deadline {
            ctx.arm(at.max(now));
        }
    }

    fn state(&self) -> &'static str {
        if self.failed.is_some() {
            "error"
        } else if self.paused {
            "paused"
        } else if self.membership.epoch() <= 0 {
            "joining"
        } else if self
            .tasks
            .values()
            .any(|t| t.role == Role::Active && !t.running)
        {
            "restoring"
        } else {
            "running"
        }
    }

    fn topology_snapshot(&self) -> Value {
        let wire = self.compiled.built.to_wire_request();
        let subtopologies: Vec<Value> = wire
            .subtopologies
            .iter()
            .map(|s| {
                json!({
                    "id": s.subtopology_id,
                    "source_topics": s.source_topics,
                    "repartition_source_topics": s.repartition_source_topics.iter().map(|t| &t.name).collect::<Vec<_>>(),
                    "repartition_sink_topics": s.repartition_sink_topics,
                    "changelog_topics": s.state_changelog_topics.iter().map(|t| &t.name).collect::<Vec<_>>(),
                })
            })
            .collect();
        let stores: Vec<Value> = self
            .compiled
            .plan
            .stores
            .iter()
            .map(|s| {
                let kind = match s.kind {
                    StoreKind::Count => "count",
                    StoreKind::Sum => "sum",
                    StoreKind::Window { .. } => "window",
                };
                json!({ "name": s.name, "kind": kind, "changelog": s.changelog })
            })
            .collect();
        json!({
            "source": self.spec.source,
            "sink": self.spec.sink,
            "subtopologies": subtopologies,
            "repartition_topics": self.compiled.plan.repartition_topics,
            "stores": stores,
        })
    }
}

/// The fields a fetch answer updates, for a source or a changelog.
struct FetchState<'a> {
    position: &'a mut Position,
    hwm: &'a mut Option<i64>,
    retry_at: &'a mut Millis,
}

fn changelog_state(changelog: &mut Changelog) -> FetchState<'_> {
    FetchState {
        position: &mut changelog.position,
        hwm: &mut changelog.hwm,
        retry_at: &mut changelog.retry_at,
    }
}

impl FetchState<'_> {
    /// Apply a fetch answer's position, high watermark and error.
    fn apply(self, code: i16, next: Option<i64>, hwm: Option<i64>, now: Millis) {
        match code {
            codes::NONE => {
                if let (Some(next), Position::At(at)) = (next, *self.position) {
                    *self.position = Position::At(at.max(next));
                }
                if hwm.is_some() {
                    *self.hwm = hwm;
                }
            }
            codes::OFFSET_OUT_OF_RANGE => *self.position = Position::Reset,
            _ => *self.retry_at = now + RETRY_MS,
        }
    }
}

/// A fetched value as a task pipes it: a value in the Confluent wire format
/// decoded through the registry to JSON when the node deserializes, else the
/// bytes as they are.
fn decode(
    ctx: &mut Ctx<'_>,
    schemas: &mut Option<SchemaCache>,
    record: &ConsumedRecord,
) -> Decoded {
    let raw = record.value.as_deref().unwrap_or_default();
    let (Some(cache), Some((schema_id, body))) = (schemas, unframe(raw)) else {
        return Decoded::Ready(raw.to_vec());
    };
    match cache.lookup(ctx, schema_id) {
        SchemaLookup::Pending => Decoded::Wait,
        SchemaLookup::Ready(schema) => match schema.decode(body) {
            Ok(doc) => Decoded::Ready(serde_json::to_vec(&doc).unwrap_or_default()),
            Err(e) => Decoded::Failed(e.to_string()),
        },
        SchemaLookup::Failed(reason) => Decoded::Failed(reason.to_string()),
    }
}

/// A `ListOffsets` of one partition: the earliest offset for a start, the
/// latest for an end, which is the last stable offset when `read_committed`.
fn list_offsets(
    topic: &str,
    partition: i32,
    lookup: Lookup,
    read_committed: bool,
) -> ListOffsetsRequest {
    let timestamp = match lookup {
        Lookup::SourceStart | Lookup::ChangelogStart => EARLIEST_TIMESTAMP,
        Lookup::ChangelogEnd => LATEST_TIMESTAMP,
    };
    ListOffsetsRequest {
        replica_id: -1,
        isolation_level: i8::from(read_committed),
        topics: vec![ListOffsetsTopic {
            name: topic.to_string(),
            partitions: vec![ListOffsetsPartition {
                partition_index: partition,
                current_leader_epoch: -1,
                timestamp,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The stream thread's producer: idempotent with `acks=all`, Kafka's
/// defaults, transactional under EOS, whose client numbers its connections
/// in `lane`.
fn build_producer(
    bootstrap: &[Endpoint],
    client_id: &str,
    lane: u32,
    transactional_id: Option<String>,
) -> Producer {
    let client = KafkaClient::new(
        bootstrap.to_vec(),
        client_id,
        ClientOptions {
            conn_base: conn_base(lane),
            ..ClientOptions::default()
        },
    );
    let config = ProducerConfig {
        transactional_id,
        ..ProducerConfig::default()
    };
    Producer::new(client, config, u64::from(lane))
}

/// A random id drawn from the node's generator: 16 bytes as Kafka's
/// `Uuid.toString()` (URL-safe base64) for a member id, or hyphenated as
/// `java.util.UUID` for a process id.
fn random_uuid(ctx: &mut Ctx<'_>) -> uuid::Uuid {
    let mut bytes = [0_u8; 16];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&ctx.rand(u64::MAX).to_be_bytes());
    }
    uuid::Builder::from_random_bytes(bytes).into_uuid()
}

impl Node for StreamsNode {
    fn kind(&self) -> &'static str {
        "streams"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self.process_id.is_none() {
            self.process_id = Some(random_uuid(ctx).hyphenated().to_string());
        }
        let process_id = self.process_id.clone().unwrap_or_default();
        let member_id = URL_SAFE_NO_PAD.encode(random_uuid(ctx).as_bytes());
        // Kafka Streams names its clients after the thread; each start takes
        // two lanes of connection ids, the consumer's and the producer's.
        let lane = 2 * (self.starts % (CONN_ID_LANES / 2));
        self.starts = self.starts.wrapping_add(1);
        let thread = format!("{}-{process_id}-StreamThread-1", self.application_id);
        self.client = KafkaClient::new(
            self.bootstrap.clone(),
            &format!("{thread}-consumer"),
            ClientOptions {
                conn_base: conn_base(lane),
                ..ClientOptions::default()
            },
        );
        self.producer = build_producer(
            &self.bootstrap,
            &format!("{thread}-producer"),
            lane + 1,
            self.transactional_id(),
        );
        let topics = self.topics();
        self.client.add_topics(topics.iter().map(String::as_str));
        self.membership = Membership::new(
            &self.application_id,
            &member_id,
            &process_id,
            REBALANCE_TIMEOUT_MS,
            self.compiled.built.to_wire_request(),
            now,
        );
        self.tasks.clear();
        self.target = Tasks::default();
        self.warmups.clear();
        self.pending.clear();
        self.fetching.clear();
        self.commit = Commit::Idle;
        self.next_commit_at = now + self.commit_interval_ms;
        self.txn_offsets = None;
        self.aborted_transactions = 0;
        self.failed = None;
        self.records_in = 0;
        self.records_out = 0;
        self.commits = 0;
        self.last_outputs.clear();
        self.held.clear();
        self.quiet.clear();
        if let Some(cache) = &mut self.schemas {
            cache.reset();
        }
        if let Some(registration) = &mut self.serializer {
            registration.restart(now);
        }
        if self.num_standby_replicas > 0 {
            ctx.event(
                "streams_config",
                json!({
                    "message": "num_standby_replicas is not used with the streams group protocol; the group's streams.num.standby.replicas decides",
                    "level": "warn",
                }),
            );
        }
        let (events, _) = self.client.on_tick(ctx);
        self.on_client_events(ctx, events);
        self.drive(ctx);
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if let Some(cache) = self.schemas.as_mut().filter(|c| c.owns(&frame)) {
            cache.on_frame(ctx, frame);
        } else if let Some(registration) = self.serializer.as_mut().filter(|r| r.owns(&frame)) {
            let events = registration.on_frame(ctx, frame);
            on_registration(ctx, events);
        } else if self.producer.client().owns_conn(frame.conn) {
            let (events, _) = self.producer.on_frame(ctx, frame);
            self.on_producer_events(ctx, events);
        } else {
            let events = self.client.on_frame(ctx, frame);
            self.on_client_events(ctx, events);
        }
        self.drive(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(cache) = &mut self.schemas {
            cache.on_tick(ctx);
        }
        if let Some(registration) = &mut self.serializer {
            let events = registration.on_tick(ctx);
            on_registration(ctx, events);
        }
        let (events, _) = self.client.on_tick(ctx);
        self.on_client_events(ctx, events);
        let (events, _) = self.producer.on_tick(ctx);
        self.on_producer_events(ctx, events);
        self.drive(ctx);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let answer = match command.get("cmd").and_then(Value::as_str) {
            Some("pause") => {
                self.paused = true;
                json!({ "paused": true })
            }
            Some("resume") => {
                self.paused = false;
                json!({ "paused": false })
            }
            Some("query") => {
                let store = command
                    .get("store")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "missing `store`".to_string())?;
                let key = command
                    .get("key")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "missing `key`".to_string())?;
                if self.compiled.plan.store(store).is_none() {
                    return Err(format!("the topology has no store `{store}`"));
                }
                let found = self
                    .tasks
                    .values()
                    .filter(|t| t.role == Role::Active && !t.closing)
                    .find_map(|t| t.query(store, key).map(|v| (t.id.to_string(), v)));
                match found {
                    Some((task, value)) => {
                        json!({ "store": store, "key": key, "task": task, "value": value })
                    }
                    None => json!({ "store": store, "key": key, "found": false }),
                }
            }
            other => return Err(format!("unknown streams command {other:?}")),
        };
        self.drive(ctx);
        Ok(answer)
    }

    fn snapshot(&self) -> Value {
        let tasks: Vec<Value> = self.tasks.values().map(StreamTask::snapshot).collect();
        let stores: Vec<Value> = self.tasks.values().flat_map(StreamTask::stores).collect();
        json!({
            "state": self.state(),
            "application_id": self.application_id,
            "member_id": self.membership.member_id(),
            "member_epoch": self.membership.epoch(),
            "membership": self.membership.snapshot(),
            "error": self.failed,
            "topology": self.topology_snapshot(),
            "tasks": tasks,
            "stores": stores,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "commits": self.commits,
            "commit_interval_ms": self.commit_interval_ms,
            "processing_guarantee": if self.eos { "exactly_once_v2" } else { "at_least_once" },
            "aborted_transactions": self.aborted_transactions,
            "paused": self.paused,
            "last_outputs": self.last_outputs,
            "producer": self.producer.snapshot(),
            "deserialize": self.schemas.as_ref().map_or(Value::Null, SchemaCache::snapshot),
            "serialize": self.serializer.as_ref().map_or(Value::Null, SchemaRegistration::snapshot),
            "client": self.client.snapshot(),
        })
    }
}

fn on_registration(ctx: &mut Ctx<'_>, events: Vec<RegistrationEvent>) {
    for event in events {
        match event {
            RegistrationEvent::Registered { subject, id } => {
                ctx.event("schema_registered", json!({ "subject": subject, "id": id }));
            }
            RegistrationEvent::Failed { subject, error } => ctx.event(
                "registry_error",
                json!({ "subject": subject, "error": error, "level": "warn" }),
            ),
        }
    }
}

#[cfg(test)]
mod tests;
