//! The simulated broker: a Kafka-speaking node with a metadata image, partition
//! logs, replication and a request pipeline that answers in order.
//!
//! [`BrokerNode`] listens on [`Endpoint::kafka`] of its node. Every request
//! reaches it as one frame, is decoded at the version the client asked for,
//! and is answered with a byte-exact Kafka response. Behaviour follows Apache
//! Kafka 4.3: KIP semantics, error codes and response shapes.
//!
//! # Configuration
//!
//! The `config` object of a `"broker"` node takes these keys; every other key
//! is an error.
//!
//! | key | type | default | meaning |
//! | --- | --- | --- | --- |
//! | `broker_id` | `i32` | required | the broker id: at least 1, and equal to the node id |
//! | `rack` | `string` | none | the rack the registration advertises |
//! | `voter` | `bool` | `true` | kept for the controller batch, unused here |
//! | `default_partitions` | `i32` | `1` | `num.partitions` for auto-created topics and `-1` requests |
//! | `default_replication_factor` | `i16` | `-1` | `default.replication.factor`; `-1` means every active broker |
//! | `min_insync_replicas` | `i32` | `1` | the broker default of `min.insync.replicas` |
//! | `log_retention_ms` | `i64` | `604800000` (7 days) | the broker default of `retention.ms`; `-1` keeps everything |
//! | `replica_lag_time_max_ms` | `u64` | `10000` | how long a follower may lag before it leaves the ISR |
//! | `request_timeout_ms` | `u64` | `30000` | the longest a held request waits (a `Produce` waits its own `timeout_ms` when shorter, a `Fetch` its `max_wait_ms`), and how long a follower waits for its leader's answer |
//!
//! A broker id and a node id are the same number in the lab: the client module
//! resolves the advertised host `node-<id>` back to [`Endpoint::kafka`] of
//! that node, so a spec whose `broker_id` differs from its `id` is rejected.
//! Broker ids start at 1 because the metadata image names a partition with no
//! leader by node id 0 (see [`NO_LEADER`]).
//!
//! # Control commands
//!
//! - `{"cmd": "apply_metadata", "records": [...]}` applies metadata records
//!   (the JSON form of [`MetadataRecord`]) as a controller would commit them.
//!   Tests feed several brokers one assignment with it; the feed carries the
//!   registration of every broker it names.
//! - `{"cmd": "pending_alter_partition"}` drains the ISR changes this broker
//!   applied on its own and would send the controller as `AlterPartition`.
//!
//! # Snapshot
//!
//! ```json
//! { "broker_id": 1, "cluster_id": "...", "controller_id": 1,
//!   "connections": 2, "held_requests": 0, "requests": {"Produce": 3},
//!   "brokers": [{"id": 1, "host": "node-1", "port": 9092, "rack": null, "fenced": false}],
//!   "topics": [{"name": "orders", "id": "...", "internal": false, "partitions": [
//!     {"index": 0, "leader": 1, "leader_epoch": 0, "replicas": [1, 2], "isr": [1, 2],
//!      "log_start": 0, "log_end": 3, "hwm": 3, "batches": 3, "size_bytes": 210,
//!      "fetch_state": "idle", "followers": [{"id": 2, "leo": 3, "lag_ms": 0}]}]}],
//!   "groups": [] }
//! ```
//!
//! `leader` is `null` for a leaderless partition. The log fields and
//! `fetch_state` are `null` for a partition this broker does not host, and
//! `followers` is filled only where this broker leads.
//!
//! # Events
//!
//! `topic_created`, `topic_deleted`, `leader_change`, `isr_change`,
//! `produce_error`, `fetch_error`, `replica_truncated`, `retention` and
//! `connection_closed_malformed`, each with a `level`.
//!
//! # Durable state
//!
//! The broker persists through [`Ctx::persist`] so a page reload restores it
//! through [`Node::load`]: every stored batch goes to the log store
//! `log/<topic>/<partition>` at its base offset, a truncation or a retention
//! pass trims that store, a deleted topic clears it; the partition's checkpoint
//! (`hwm`, `log_start`, `epochs`, `producers` as JSON) goes to the key-value
//! store `meta/<topic>/<partition>` whenever it changes; and every batch of
//! committed metadata records goes to the log store `metadata` as JSON, one
//! entry per [`BrokerNode::apply_metadata`], so a reload replays them in order.
//!
//! # Seams
//!
//! - The controller batch drives [`BrokerNode::apply_metadata`]; until then
//!   [`LocalController`] produces the records inside the broker, including
//!   the `ProducerIdsRecord` that claims a block of producer ids.
//! - ISR changes this broker decides are applied locally and queued for
//!   [`BrokerNode::pending_alter_partition`], where the controller batch will
//!   route them through `AlterPartition`.
//! - The group coordinator plugs into `handlers::groups`, which answers every
//!   group api with `COORDINATOR_NOT_AVAILABLE` until it lands, and appends to
//!   `__consumer_offsets` through [`BrokerNode::append_local`].

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, ProducerIdsRecord};
use serde_json::{Value, json};
use uuid::Uuid;

mod cluster;
mod conn;
mod dispatch;
mod handlers;
mod log;
mod replica;
/// Wire helpers for tests that talk to a broker.
pub mod test_support;

pub use self::{
    cluster::{
        CONSUMER_OFFSETS_PARTITIONS, CONSUMER_OFFSETS_TOPIC, LAB_CLUSTER_ID, LocalController,
        NO_LEADER, PlannedTopic, TopicPlan, TopicRefusal, active_brokers, cluster_id_string,
        group_partition, java_string_hash, partition_record, record_leader, registration_record,
    },
    conn::{Connection, FrameError, ParsedRequest, parse_request_frame, response_frame},
    dispatch::{
        DispatchError, HoldReason, Outcome, Reply, RequestCtx, Step, VersionRange,
        api_versions_table, versions,
    },
    handlers::api_versions::{finalized_features, supported_features},
    log::{
        AppendError, AppendInfo, AppendPolicy, Checkpoint, LogChange, PartitionLog, ProducerBatch,
        ProducerEntry, RecordError, StoredBatch,
    },
    replica::{AlterPartitionProposal, FetchState, FollowerState, Replica, ReplicaChange},
};
use self::{
    conn::QueuedRequest,
    dispatch::{ConnKey, Step as DispatchStep},
};
use super::{
    LabError, codes, config_field, config_field_or,
    net::{Ctx, DurableImage, DurableOp, Endpoint, Frame, Millis, Node, NodeId, Payload},
    scenario::NodeSpec,
};

/// The durable log store of the committed metadata records.
pub const METADATA_STORE: &str = "metadata";

/// How often the broker runs retention and ISR maintenance.
pub const TICK_MS: Millis = 1_000;

/// Kafka's default `retention.ms`: seven days.
pub const DEFAULT_RETENTION_MS: i64 = 604_800_000;

/// Kafka's `ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE`: how many producer ids
/// one allocation claims.
pub const PRODUCER_ID_BLOCK_SIZE: i64 = 1_000;

/// Where broker `n`'s blocks start, `(n - 1)` times this, while every broker
/// keeps its own image: brokers that claim blocks from separate images never
/// hand out the same id. A shared image moves every broker past the blocks
/// already claimed.
pub const PRODUCER_ID_BROKER_SPACING: i64 = 1_000_000;

/// A partition of a topic, the key of every replica map.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicPartition {
    /// The topic name.
    pub topic: String,
    /// The partition index.
    pub partition: i32,
}

impl TopicPartition {
    /// The key of `partition` of `topic`.
    #[must_use]
    pub fn new(topic: &str, partition: i32) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
        }
    }

    /// Kafka's `TopicPartition.toString`, `topic-partition`, which refusal
    /// messages name.
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }
}

/// The validated `config` of a broker node; the module docs describe each
/// key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerConfig {
    /// `broker_id`.
    pub broker_id: i32,
    /// `rack`.
    pub rack: Option<String>,
    /// `voter`.
    pub voter: bool,
    /// `default_partitions`.
    pub default_partitions: i32,
    /// `default_replication_factor`.
    pub default_replication_factor: i16,
    /// `min_insync_replicas`.
    pub min_insync_replicas: i32,
    /// `log_retention_ms`.
    pub log_retention_ms: i64,
    /// `replica_lag_time_max_ms`.
    pub replica_lag_time_max_ms: u64,
    /// `request_timeout_ms`.
    pub request_timeout_ms: u64,
}

const CONFIG_KEYS: &[&str] = &[
    "broker_id",
    "rack",
    "voter",
    "default_partitions",
    "default_replication_factor",
    "min_insync_replicas",
    "log_retention_ms",
    "replica_lag_time_max_ms",
    "request_timeout_ms",
];

impl BrokerConfig {
    /// Read and validate a spec's config.
    ///
    /// # Errors
    /// Returns a config error for an unknown key, a wrong type, a missing
    /// `broker_id`, a `broker_id` below 1 or other than the node id, or a
    /// default out of Kafka's range.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        if let Some(object) = spec.config.as_object() {
            if let Some(unknown) = object.keys().find(|k| !CONFIG_KEYS.contains(&k.as_str())) {
                return Err(LabError::config(
                    spec,
                    format!("unknown config field `{unknown}`"),
                ));
            }
        } else if !spec.config.is_null() {
            return Err(LabError::config(spec, "config must be an object"));
        }
        let broker_id: i32 = config_field(spec, "broker_id")?;
        if broker_id < 1 {
            return Err(LabError::config(
                spec,
                format!(
                    "broker_id {broker_id} must be at least 1: the metadata image names a leaderless partition by node 0"
                ),
            ));
        }
        if u64::try_from(broker_id).ok() != Some(u64::from(spec.id.0)) {
            return Err(LabError::config(
                spec,
                format!(
                    "broker_id {broker_id} must equal the node id {}: clients resolve `node-<id>` to the node",
                    spec.id
                ),
            ));
        }
        let config = Self {
            broker_id,
            rack: config_field_or(spec, "rack", None)?,
            voter: config_field_or(spec, "voter", true)?,
            default_partitions: config_field_or(spec, "default_partitions", 1)?,
            default_replication_factor: config_field_or(spec, "default_replication_factor", -1)?,
            min_insync_replicas: config_field_or(spec, "min_insync_replicas", 1)?,
            log_retention_ms: config_field_or(spec, "log_retention_ms", DEFAULT_RETENTION_MS)?,
            replica_lag_time_max_ms: config_field_or(spec, "replica_lag_time_max_ms", 10_000)?,
            request_timeout_ms: config_field_or(spec, "request_timeout_ms", 30_000)?,
        };
        if config.default_partitions < 1 {
            return Err(LabError::config(
                spec,
                "default_partitions must be at least 1",
            ));
        }
        if config.default_replication_factor == 0 || config.default_replication_factor < -1 {
            return Err(LabError::config(
                spec,
                "default_replication_factor must be positive or -1",
            ));
        }
        if config.min_insync_replicas < 1 {
            return Err(LabError::config(
                spec,
                "min_insync_replicas must be at least 1",
            ));
        }
        Ok(config)
    }
}

/// A broker node.
pub struct BrokerNode {
    id: NodeId,
    config: BrokerConfig,
    image: MetadataImage,
    controller: LocalController,
    incarnation_id: Uuid,
    replicas: BTreeMap<TopicPartition, Replica>,
    conns: BTreeMap<ConnKey, Connection>,
    links: BTreeMap<i32, replica::LeaderLink>,
    counters: BTreeMap<&'static str, u64>,
    /// The next id of the claimed producer-id block.
    next_producer_id: i64,
    /// The end of the claimed producer-id block, exclusive.
    producer_id_block_end: i64,
    next_tick: Millis,
    next_conn: u32,
    pending_alter: Vec<AlterPartitionProposal>,
    started: bool,
    /// The time of the last call the world made, for the snapshot.
    now: Millis,
    /// Logs a reload restored, taken by the replicas the image opens.
    restored_logs: BTreeMap<TopicPartition, PartitionLog>,
    /// The next index of the `metadata` durable store.
    metadata_index: u64,
    /// The checkpoints as last persisted, with the log's change counter at
    /// that moment, so an unchanged log costs no comparison.
    checkpoints: BTreeMap<TopicPartition, (u64, Checkpoint)>,
}

impl BrokerNode {
    /// Build a broker from its spec.
    ///
    /// # Errors
    /// Returns the config error of [`BrokerConfig::from_spec`].
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let config = BrokerConfig::from_spec(spec)?;
        Ok(Self {
            id: spec.id,
            image: MetadataImage::new(LAB_CLUSTER_ID),
            controller: LocalController::default(),
            incarnation_id: Uuid::nil(),
            replicas: BTreeMap::new(),
            conns: BTreeMap::new(),
            links: BTreeMap::new(),
            counters: BTreeMap::new(),
            next_producer_id: 0,
            producer_id_block_end: 0,
            next_tick: 0,
            next_conn: 0,
            pending_alter: Vec::new(),
            started: false,
            now: 0,
            restored_logs: BTreeMap::new(),
            metadata_index: 0,
            checkpoints: BTreeMap::new(),
            config,
        })
    }

    /// The broker id.
    #[must_use]
    pub fn broker_id(&self) -> i32 {
        self.config.broker_id
    }

    /// The node id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.id
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    /// The metadata image, the broker's view of the cluster.
    #[must_use]
    pub fn image(&self) -> &MetadataImage {
        &self.image
    }

    /// The controller id this broker advertises: itself, until the controller
    /// batch elects one.
    #[must_use]
    pub fn controller_id(&self) -> i32 {
        self.config.broker_id
    }

    /// The replica of a partition this broker hosts.
    #[must_use]
    pub fn replica(&self, topic: &str, partition: i32) -> Option<&Replica> {
        self.replicas.get(&TopicPartition::new(topic, partition))
    }

    /// The replica of a partition this broker hosts, for writing.
    pub fn replica_mut(&mut self, topic: &str, partition: i32) -> Option<&mut Replica> {
        self.replicas
            .get_mut(&TopicPartition::new(topic, partition))
    }

    /// Every hosted replica, by partition.
    #[must_use]
    pub fn replicas(&self) -> &BTreeMap<TopicPartition, Replica> {
        &self.replicas
    }

    /// The local controller that stands in for the quorum.
    pub fn local_controller(&mut self) -> &mut LocalController {
        &mut self.controller
    }

    /// The next producer id for `InitProducerId`. Ids come from a block of
    /// [`PRODUCER_ID_BLOCK_SIZE`] this broker claims by committing a
    /// `ProducerIdsRecord`, as Kafka's controller answers
    /// `AllocateProducerIds`; a block starts past every block the image
    /// knows, and past `(broker_id - 1) * PRODUCER_ID_BROKER_SPACING`.
    pub fn allocate_producer_id(&mut self, ctx: &mut Ctx<'_>) -> i64 {
        if self.next_producer_id >= self.producer_id_block_end {
            let me = cluster::meta_id(self.config.broker_id);
            let start = self
                .image
                .next_producer_id()
                .max(i64::from(self.config.broker_id - 1) * PRODUCER_ID_BROKER_SPACING);
            let end = start + PRODUCER_ID_BLOCK_SIZE;
            let claim = MetadataRecord::V1ProducerIds(ProducerIdsRecord {
                broker_id: me,
                broker_epoch: self.image.broker_epoch(me).unwrap_or(0),
                next_producer_id: end,
            });
            self.apply_metadata(ctx, &[claim]);
            self.next_producer_id = start;
            self.producer_id_block_end = end;
        }
        let id = self.next_producer_id;
        self.next_producer_id += 1;
        id
    }

    /// The ISR changes this broker applied on its own since the last drain,
    /// for the controller batch to send as `AlterPartition`.
    pub fn pending_alter_partition(&mut self) -> Vec<AlterPartitionProposal> {
        std::mem::take(&mut self.pending_alter)
    }

    /// The `min.insync.replicas` of a topic: its override, else the broker
    /// default.
    #[must_use]
    pub fn min_insync_replicas(&self, topic: &str) -> i32 {
        self.image
            .topic_config(topic)
            .and_then(|c| c.get("min.insync.replicas"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(self.config.min_insync_replicas)
    }

    /// Apply metadata records as a controller would commit them: the image
    /// changes, replicas open and close to match it, links follow the
    /// leaders, and events record what changed. This is the one way the
    /// image changes. Held requests the change unblocks are answered when the
    /// call the world made returns.
    pub fn apply_metadata(&mut self, ctx: &mut Ctx<'_>, records: &[MetadataRecord]) {
        for record in records {
            match record {
                MetadataRecord::V1Topic(topic) if self.image.topic(&topic.name).is_none() => {
                    ctx.event(
                        "topic_created",
                        json!({ "topic": topic.name, "level": "info" }),
                    );
                }
                MetadataRecord::V1DeleteTopic(topic) if self.image.topic(&topic.name).is_some() => {
                    ctx.event(
                        "topic_deleted",
                        json!({ "topic": topic.name, "level": "warn" }),
                    );
                }
                _ => {}
            }
            self.image.apply(record);
        }
        if !records.is_empty()
            && let Ok(bytes) = serde_json::to_vec(records)
        {
            ctx.persist(DurableOp::Append {
                store: METADATA_STORE.to_string(),
                index: self.metadata_index,
                bytes: Bytes::from(bytes),
            });
            self.metadata_index += 1;
        }
        self.reconcile_replicas(ctx);
        self.sync_links(ctx);
    }

    /// Open, update and close replicas so they match the image. A replica
    /// the image opens reports no leader or ISR change: the topic event
    /// already tells its story.
    fn reconcile_replicas(&mut self, ctx: &mut Ctx<'_>) {
        let me = self.config.broker_id;
        let now = ctx.now();
        let mut partitions: Vec<PartitionRecord> = self.image.all_partitions().cloned().collect();
        partitions.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        let mut keep = BTreeSet::new();
        for record in partitions {
            if !record.replicas.iter().any(|r| cluster::wire_id(*r) == me) {
                continue;
            }
            let key = TopicPartition::new(&record.topic, record.partition);
            keep.insert(key.clone());
            let change = if let Some(replica) = self.replicas.get_mut(&key) {
                replica.apply_record(&record, me, now)
            } else {
                let topic_id = self
                    .image
                    .topic(&record.topic)
                    .map_or_else(Uuid::nil, |t| t.topic_id);
                let mut replica = Replica::new(&record.topic, record.partition, topic_id);
                if let Some(log) = self.restored_logs.remove(&key) {
                    replica.log = log;
                }
                replica.apply_record(&record, me, now);
                self.replicas.insert(key.clone(), replica);
                ReplicaChange::default()
            };
            if change.leader_changed {
                ctx.event(
                    "leader_change",
                    json!({
                        "topic": key.topic, "partition": key.partition,
                        "leader": cluster::record_leader(&record),
                        "leader_epoch": record.leader_epoch.0,
                        "level": if cluster::record_leader(&record).is_some() { "info" } else { "warn" },
                    }),
                );
            }
            if change.isr_changed {
                let isr: Vec<i32> = record.isr.iter().map(|r| cluster::wire_id(*r)).collect();
                ctx.event(
                    "isr_change",
                    json!({
                        "topic": key.topic, "partition": key.partition, "isr": isr,
                        "level": if isr.len() < record.replicas.len() { "warn" } else { "info" },
                    }),
                );
            }
        }
        let gone: Vec<TopicPartition> = self
            .replicas
            .keys()
            .filter(|key| !keep.contains(*key))
            .cloned()
            .collect();
        for key in gone {
            self.replicas.remove(&key);
            self.checkpoints.remove(&key);
            ctx.persist(DurableOp::Clear {
                store: log_store(&key),
            });
            ctx.persist(DurableOp::Clear {
                store: meta_store(&key),
            });
        }
    }

    /// Record every batch change and checkpoint change since the last flush.
    fn flush_durable(&mut self, ctx: &mut Ctx<'_>) {
        for (key, replica) in &mut self.replicas {
            let store = log_store(key);
            for change in replica.log.take_journal() {
                let op = match change {
                    LogChange::Appended(base_offset) => {
                        let Some(batch) = replica.log.batch_at(base_offset) else {
                            continue;
                        };
                        DurableOp::Append {
                            store: store.clone(),
                            index: u64::try_from(base_offset).unwrap_or(0),
                            bytes: batch.bytes.clone(),
                        }
                    }
                    LogChange::TruncatedFrom(offset) => DurableOp::TruncateFrom {
                        store: store.clone(),
                        index: u64::try_from(offset).unwrap_or(0),
                    },
                    LogChange::TruncatedBefore(offset) => DurableOp::TruncateBefore {
                        store: store.clone(),
                        index: u64::try_from(offset).unwrap_or(0),
                    },
                    LogChange::Cleared => DurableOp::Clear {
                        store: store.clone(),
                    },
                };
                ctx.persist(op);
            }
            let changes = replica.log.changes();
            let previous = self.checkpoints.get(key);
            if previous.is_some_and(|(seen, _)| *seen == changes) {
                continue;
            }
            let checkpoint = replica.log.checkpoint();
            let before = previous.map(|(_, c)| checkpoint_fields(c));
            let meta = meta_store(key);
            for (index, (name, value)) in checkpoint_fields(&checkpoint).into_iter().enumerate() {
                if before.as_ref().is_none_or(|b| b[index].1 != value) {
                    ctx.persist(DurableOp::Put {
                        store: meta.clone(),
                        key: name.to_string(),
                        value: Bytes::from(value.to_string()),
                    });
                }
            }
            self.checkpoints.insert(key.clone(), (changes, checkpoint));
        }
    }

    /// Apply an ISR change this broker decided as leader: locally now, and
    /// queued for the controller batch to send as `AlterPartition`.
    fn change_isr(&mut self, ctx: &mut Ctx<'_>, key: &TopicPartition, new_isr: Vec<i32>) {
        let Some(record) = self.image.partition(&key.topic, key.partition).cloned() else {
            return;
        };
        let proposal = AlterPartitionProposal {
            topic: key.topic.clone(),
            partition: key.partition,
            new_isr: new_isr.clone(),
            leader_epoch: record.leader_epoch.0,
            partition_epoch: record.partition_epoch,
        };
        self.pending_alter.push(proposal);
        let changed = PartitionRecord {
            isr: new_isr.into_iter().map(cluster::meta_id).collect(),
            partition_epoch: record.partition_epoch + 1,
            ..record
        };
        self.apply_metadata(ctx, &[MetadataRecord::V1Partition(changed)]);
    }

    /// The periodic tick: retention on every hosted log of a topic whose
    /// `cleanup.policy` deletes, ISR shrink on every led partition.
    fn tick(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let me = self.config.broker_id;
        let lag = self.config.replica_lag_time_max_ms;
        let mut shrinks: Vec<(TopicPartition, Vec<i32>)> = Vec::new();
        let keys: Vec<TopicPartition> = self.replicas.keys().cloned().collect();
        for key in keys {
            let deletes = self
                .image
                .topic_config(&key.topic)
                .and_then(|c| c.get("cleanup.policy"))
                .is_none_or(|policy| policy.split(',').any(|p| p.trim() == "delete"));
            let retention_ms =
                self.topic_config_i64(&key.topic, "retention.ms", self.config.log_retention_ms);
            let retention_bytes = self.topic_config_i64(&key.topic, "retention.bytes", -1);
            let Some(replica) = self.replicas.get_mut(&key) else {
                continue;
            };
            let removed = if deletes {
                replica
                    .log
                    .apply_retention(now, retention_ms, retention_bytes)
            } else {
                0
            };
            if removed > 0 {
                ctx.event(
                    "retention",
                    json!({
                        "topic": key.topic, "partition": key.partition, "batches_deleted": removed,
                        "log_start": replica.log.log_start_offset(), "level": "info",
                    }),
                );
            }
            if replica.leads(me) {
                let out = replica.out_of_sync_followers(me, now, lag);
                if !out.is_empty() {
                    let isr: Vec<i32> = replica
                        .isr
                        .iter()
                        .copied()
                        .filter(|id| !out.contains(id))
                        .collect();
                    shrinks.push((key, isr));
                }
            }
        }
        for (key, isr) in shrinks {
            self.change_isr(ctx, &key, isr);
        }
    }

    /// A numeric topic config override, else `default`.
    fn topic_config_i64(&self, topic: &str, key: &str, default: i64) -> i64 {
        self.image
            .topic_config(topic)
            .and_then(|c| c.get(key))
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    /// The common tail of every call the world makes: run the tick when it
    /// is due, answer the held requests that can now complete, drive the
    /// replication links, persist what changed, and arm the timer.
    fn settle(&mut self, ctx: &mut Ctx<'_>) {
        self.now = ctx.now();
        if self.now >= self.next_tick {
            self.tick(ctx);
            self.next_tick = self.now + TICK_MS;
        }
        self.retry_all_held(ctx);
        self.poll_links(ctx);
        self.flush_durable(ctx);
        self.rearm(ctx);
    }

    // ---- connections ------------------------------------------------------------

    fn on_client_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        let key = frame.conn_key();
        match frame.payload {
            Payload::Open => {
                self.conns.insert(key, Connection::new(ctx.now()));
            }
            Payload::Close => {
                self.conns.remove(&key);
            }
            Payload::Data(bytes) => {
                if !self.conns.contains_key(&key) {
                    // A frame on a connection the broker never saw opened, or
                    // one it already closed, is answered like a reset peer.
                    ctx.send(Frame::close(Endpoint::kafka(self.id), key.0, key.1));
                    return;
                }
                match parse_request_frame(&bytes) {
                    Ok(parsed) => self.enqueue(ctx, key, parsed),
                    Err(error) => {
                        ctx.event(
                            "connection_closed_malformed",
                            json!({ "peer": key.0.node, "conn": key.1, "reason": error.to_string(), "level": "error" }),
                        );
                        self.close_connection(ctx, key);
                    }
                }
            }
        }
    }

    fn enqueue(&mut self, ctx: &mut Ctx<'_>, key: ConnKey, parsed: ParsedRequest) {
        let Some(conn) = self.conns.get_mut(&key) else {
            return;
        };
        conn.requests += 1;
        conn.client_id.clone_from(&parsed.client_id);
        conn.queue.push_back(QueuedRequest {
            api_key: parsed.api_key,
            version: parsed.version,
            correlation_id: parsed.correlation_id,
            client_id: parsed.client_id,
            body: parsed.body,
            received_at: ctx.now(),
            held: None,
        });
        self.pump(ctx, key);
    }

    /// Serve the queued requests of a connection in order until one is held
    /// or the queue is empty.
    fn pump(&mut self, ctx: &mut Ctx<'_>, key: ConnKey) {
        loop {
            let Some(conn) = self.conns.get_mut(&key) else {
                return;
            };
            if conn.is_blocked() {
                return;
            }
            let Some(request) = conn.queue.pop_front() else {
                return;
            };
            let req = RequestCtx {
                conn: key,
                api_key: request.api_key,
                version: request.version,
                correlation_id: request.correlation_id,
                client_id: request.client_id.clone(),
                held_until: None,
            };
            *self
                .counters
                .entry(<&'static str>::from(request.api_key))
                .or_insert(0) += 1;
            let step = dispatch::dispatch(self, ctx, &req, &request.body);
            if !self.finish_step(ctx, key, &req, request, step) {
                return;
            }
        }
    }

    /// Send, hold or close after a dispatch step. Returns whether the
    /// connection can serve its next request.
    fn finish_step(
        &mut self,
        ctx: &mut Ctx<'_>,
        key: ConnKey,
        req: &RequestCtx,
        mut request: QueuedRequest,
        step: Result<DispatchStep, DispatchError>,
    ) -> bool {
        match step {
            Ok(DispatchStep::Reply(reply)) => {
                self.send_response(ctx, key, req, &reply);
                true
            }
            Ok(DispatchStep::Silent) => true,
            Ok(DispatchStep::Hold(reason)) => {
                request.held = Some(reason);
                if let Some(conn) = self.conns.get_mut(&key) {
                    conn.queue.push_front(request);
                }
                false
            }
            Ok(DispatchStep::Close) => {
                self.close_connection(ctx, key);
                false
            }
            Err(error) => {
                ctx.event(
                    "connection_closed_malformed",
                    json!({ "peer": key.0.node, "conn": key.1, "api": <&'static str>::from(req.api_key), "reason": error.to_string(), "level": "error" }),
                );
                self.close_connection(ctx, key);
                false
            }
        }
    }

    fn send_response(&mut self, ctx: &mut Ctx<'_>, key: ConnKey, req: &RequestCtx, reply: &Reply) {
        let frame = response_frame(req.api_key, reply.version, req.correlation_id, &reply.body);
        ctx.send(Frame::data(Endpoint::kafka(self.id), key.0, key.1, frame));
    }

    fn close_connection(&mut self, ctx: &mut Ctx<'_>, key: ConnKey) {
        if self.conns.remove(&key).is_some() {
            ctx.send(Frame::close(Endpoint::kafka(self.id), key.0, key.1));
        }
    }

    /// Run every held request again until none can make progress. A request
    /// that completes lets the ones queued behind it run.
    fn retry_all_held(&mut self, ctx: &mut Ctx<'_>) {
        loop {
            let keys: Vec<ConnKey> = self
                .conns
                .iter()
                .filter(|(_, c)| c.is_blocked())
                .map(|(k, _)| *k)
                .collect();
            let mut progressed = false;
            for key in keys {
                let Some(mut request) = self.conns.get_mut(&key).and_then(|c| c.queue.pop_front())
                else {
                    continue;
                };
                let Some(held) = request.held.take() else {
                    continue;
                };
                let req = RequestCtx {
                    conn: key,
                    api_key: request.api_key,
                    version: request.version,
                    correlation_id: request.correlation_id,
                    client_id: request.client_id.clone(),
                    held_until: Some(held.deadline()),
                };
                let step = dispatch::retry(self, ctx, &req, &request.body, held);
                let still_held = matches!(step, Ok(DispatchStep::Hold(_)));
                if self.finish_step(ctx, key, &req, request, step) {
                    progressed = true;
                    self.pump(ctx, key);
                } else if !still_held {
                    progressed = true;
                }
            }
            if !progressed {
                return;
            }
        }
    }

    /// Arm the timer for the earliest deadline after now: the tick, a held
    /// request, or a replication link. [`BrokerNode::settle`] has handled
    /// everything due now, so the timer never fires twice in one instant.
    fn rearm(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let mut next = self.next_tick.max(now + 1);
        for conn in self.conns.values() {
            if let Some(deadline) = conn
                .queue
                .front()
                .and_then(|r| r.held.as_ref())
                .map(HoldReason::deadline)
            {
                next = next.min(deadline.max(now + 1));
            }
        }
        if let Some(at) = self.link_deadline(now) {
            next = next.min(at);
        }
        ctx.arm(next);
    }

    /// A fresh client-side connection id for a link this broker opens.
    fn next_conn_id(&mut self) -> super::net::ConnId {
        self.next_conn += 1;
        super::net::ConnId(self.next_conn)
    }
}

impl Node for BrokerNode {
    fn kind(&self) -> &'static str {
        "broker"
    }

    // The metadata log is replayed in order, then every partition log is
    // rebuilt beside its checkpoint; the image opens the replicas at start.
    fn load(&mut self, image: DurableImage) {
        if let Some(entries) = image.logs.get(METADATA_STORE) {
            for entry in entries {
                if let Ok(records) = serde_json::from_slice::<Vec<MetadataRecord>>(&entry.bytes) {
                    for record in &records {
                        self.image.apply(record);
                    }
                }
                self.metadata_index = entry.index + 1;
            }
        }
        let partitions: BTreeSet<TopicPartition> = image
            .logs
            .keys()
            .filter_map(|store| parse_store(store, "log/"))
            .chain(
                image
                    .kv
                    .keys()
                    .filter_map(|store| parse_store(store, "meta/")),
            )
            .collect();
        for key in partitions {
            let checkpoint = image.kv.get(&meta_store(&key)).map(|kv| Checkpoint {
                high_watermark: checkpoint_field(kv, "hwm").unwrap_or(0),
                log_start_offset: checkpoint_field(kv, "log_start").unwrap_or(0),
                epoch_cache: checkpoint_field(kv, "epochs").unwrap_or_default(),
                producers: checkpoint_field(kv, "producers").unwrap_or_default(),
            });
            let batches: Vec<Bytes> = image
                .logs
                .get(&log_store(&key))
                .map(|entries| entries.iter().map(|e| e.bytes.clone()).collect())
                .unwrap_or_default();
            let log = PartitionLog::restore(&batches, checkpoint);
            self.checkpoints
                .insert(key.clone(), (log.changes(), log.checkpoint()));
            self.restored_logs.insert(key, log);
        }
    }

    // A boot keeps the image and the logs and drops every connection: the
    // registration is re-applied, followers reconcile with their leaders
    // again, and the tick starts over.
    fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.started = true;
        self.conns.clear();
        self.links.clear();
        self.producer_id_block_end = self.next_producer_id;
        self.incarnation_id = Uuid::from_u64_pair(ctx.rand(u64::MAX), ctx.rand(u64::MAX));
        self.next_tick = ctx.now() + TICK_MS;
        let me = self.config.broker_id;
        let now = ctx.now();
        for replica in self.replicas.values_mut() {
            replica.on_restart(me, now);
        }
        let registration = MetadataRecord::V1BrokerRegistration(cluster::registration_record(
            me,
            self.config.rack.clone(),
            self.incarnation_id,
        ));
        self.apply_metadata(ctx, &[registration]);
        self.settle(ctx);
    }

    fn stop(&mut self) {
        self.started = false;
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if !self.started || frame.dst.node != self.id {
            return;
        }
        if frame.dst == Endpoint::kafka(self.id) {
            self.on_client_frame(ctx, frame);
        } else if frame.dst == Endpoint::client(self.id) {
            self.on_link_frame(ctx, frame);
        }
        self.settle(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        if !self.started {
            return;
        }
        self.settle(ctx);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let result = match command.get("cmd").and_then(Value::as_str) {
            Some("apply_metadata") => {
                let records: Vec<MetadataRecord> = serde_json::from_value(
                    command
                        .get("records")
                        .cloned()
                        .unwrap_or(Value::Array(Vec::new())),
                )
                .map_err(|e| format!("records: {e}"))?;
                self.apply_metadata(ctx, &records);
                Ok(json!({ "applied": records.len() }))
            }
            Some("pending_alter_partition") => {
                let pending = self.pending_alter_partition();
                serde_json::to_value(pending).map_err(|e| e.to_string())
            }
            other => Err(format!("unknown broker command {other:?}")),
        };
        if self.started {
            self.settle(ctx);
        } else {
            self.flush_durable(ctx);
        }
        result
    }

    fn snapshot(&self) -> Value {
        let me = self.config.broker_id;
        let mut brokers: Vec<Value> = self
            .image
            .brokers()
            .map(|b| {
                json!({
                    "id": cluster::wire_id(b.node_id), "host": b.host, "port": b.port,
                    "rack": b.rack, "fenced": b.fenced,
                })
            })
            .collect();
        brokers.sort_by_key(|b| b["id"].as_i64());
        let mut topics: Vec<&krabka_metadata::TopicRecord> = self.image.topics().collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        let topics: Vec<Value> = topics
            .into_iter()
            .map(|topic| {
                let partitions: Vec<Value> = self
                    .image
                    .partitions_of(&topic.name)
                    .map(|p| self.partition_snapshot(p, me))
                    .collect();
                json!({
                    "name": topic.name, "id": topic.topic_id.to_string(),
                    "internal": handlers::is_internal_topic(&topic.name),
                    "partitions": partitions,
                })
            })
            .collect();
        let held = self.conns.values().filter(|c| c.is_blocked()).count();
        json!({
            "broker_id": me,
            "cluster_id": cluster_id_string(self.image.cluster_id()),
            "controller_id": self.controller_id(),
            "connections": self.conns.len(),
            "held_requests": held,
            "requests": self.counters,
            "brokers": brokers,
            "topics": topics,
            "groups": Value::Array(Vec::new()),
        })
    }
}

impl BrokerNode {
    fn partition_snapshot(&self, record: &PartitionRecord, me: i32) -> Value {
        let replica = self
            .replicas
            .get(&TopicPartition::new(&record.topic, record.partition));
        let followers: Vec<Value> = replica
            .filter(|r| r.leads(me))
            .map(|r| {
                r.followers
                    .iter()
                    .map(|(id, f)| {
                        json!({ "id": id, "leo": f.log_end_offset, "lag_ms": r.follower_lag_ms(f, self.now) })
                    })
                    .collect()
            })
            .unwrap_or_default();
        json!({
            "index": record.partition,
            "leader": cluster::record_leader(record),
            "leader_epoch": record.leader_epoch.0,
            "replicas": record.replicas.iter().map(|r| cluster::wire_id(*r)).collect::<Vec<_>>(),
            "isr": record.isr.iter().map(|r| cluster::wire_id(*r)).collect::<Vec<_>>(),
            "log_start": replica.map(|r| r.log.log_start_offset()),
            "log_end": replica.map(|r| r.log.log_end_offset()),
            "hwm": replica.map(|r| r.log.high_watermark()),
            "batches": replica.map(|r| r.log.batches().len()),
            "size_bytes": replica.map(|r| r.log.size_bytes()),
            "fetch_state": replica.map(|r| r.fetch.name()),
            "followers": followers,
        })
    }

    /// Append records to a partition this broker leads, as the group
    /// coordinator does for `__consumer_offsets`. The records are validated
    /// like a client's; held requests the append unblocks are answered when
    /// the call the world made returns.
    ///
    /// # Errors
    /// Returns `NOT_LEADER_OR_FOLLOWER` for a partition this broker does not
    /// lead, else the append refusal's code.
    pub fn append_local(
        &mut self,
        ctx: &mut Ctx<'_>,
        topic: &str,
        partition: i32,
        records: &Bytes,
    ) -> Result<AppendInfo, i16> {
        let me = self.config.broker_id;
        let policy = handlers::append_policy(self, topic);
        let key = TopicPartition::new(topic, partition);
        let label = key.label();
        let replica = self
            .replicas
            .get_mut(&key)
            .filter(|r| r.leads(me))
            .ok_or(codes::NOT_LEADER_OR_FOLLOWER)?;
        let epoch = replica.leader_epoch;
        let info = replica
            .log
            .append(records, epoch, ctx.now(), policy, &label)
            .map_err(|e| e.code())?;
        replica.recompute_hwm(me);
        Ok(info)
    }
}

/// The durable log store of a partition.
#[must_use]
pub fn log_store(key: &TopicPartition) -> String {
    format!("log/{}/{}", key.topic, key.partition)
}

/// The durable key-value store of a partition's checkpoint.
#[must_use]
pub fn meta_store(key: &TopicPartition) -> String {
    format!("meta/{}/{}", key.topic, key.partition)
}

/// The persisted fields of a checkpoint, as the `meta` store keys them.
fn checkpoint_fields(checkpoint: &Checkpoint) -> [(&'static str, Value); 4] {
    [
        ("hwm", json!(checkpoint.high_watermark)),
        ("log_start", json!(checkpoint.log_start_offset)),
        ("epochs", json!(checkpoint.epoch_cache)),
        ("producers", json!(checkpoint.producers)),
    ]
}

/// One JSON field of a persisted checkpoint.
fn checkpoint_field<T: serde::de::DeserializeOwned>(
    kv: &BTreeMap<String, super::net::B64Bytes>,
    name: &str,
) -> Option<T> {
    kv.get(name).and_then(|v| serde_json::from_slice(&v.0).ok())
}

/// The partition a `<prefix><topic>/<partition>` store name names.
fn parse_store(store: &str, prefix: &str) -> Option<TopicPartition> {
    let rest = store.strip_prefix(prefix)?;
    let (topic, partition) = rest.rsplit_once('/')?;
    Some(TopicPartition::new(topic, partition.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn store_names_round_trip() {
        let key = TopicPartition::new("a/b", 3);
        assert!(log_store(&key) == "log/a/b/3");
        assert!(meta_store(&key) == "meta/a/b/3");
        assert!(key.label() == "a/b-3");
        assert!(parse_store(&log_store(&key), "log/") == Some(key.clone()));
        assert!(parse_store(&meta_store(&key), "meta/") == Some(key));
        assert!(parse_store("meta/x/0", "log/").is_none());
        assert!(parse_store("log/x/y", "log/").is_none());
    }

    #[test]
    fn config_rejects_unknown_keys_and_bad_ids() {
        let ok = NodeSpec::new(1, "broker", "b", json!({ "broker_id": 1, "rack": "a" }));
        let config = BrokerConfig::from_spec(&ok).unwrap();
        assert!(
            config
                == BrokerConfig {
                    broker_id: 1,
                    rack: Some("a".to_string()),
                    voter: true,
                    default_partitions: 1,
                    default_replication_factor: -1,
                    min_insync_replicas: 1,
                    log_retention_ms: DEFAULT_RETENTION_MS,
                    replica_lag_time_max_ms: 10_000,
                    request_timeout_ms: 30_000,
                }
        );
        for (id, config) in [
            (1, json!({ "broker_id": 2 })),
            (1, json!({ "broker_id": 1, "bogus": true })),
            (1, json!({ "rack": "a" })),
            (1, json!({ "broker_id": "one" })),
            (1, json!({ "broker_id": 1, "default_partitions": 0 })),
            (
                1,
                json!({ "broker_id": 1, "default_replication_factor": -2 }),
            ),
            (1, json!({ "broker_id": 1, "min_insync_replicas": 0 })),
            (1, json!(7)),
            (0, json!({ "broker_id": 0 })),
        ] {
            let spec = NodeSpec::new(id, "broker", "b", config.clone());
            assert!(BrokerConfig::from_spec(&spec).is_err(), "{config}");
        }
    }
}
