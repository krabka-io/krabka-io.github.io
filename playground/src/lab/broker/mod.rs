//! The simulated broker: a Kafka node in combined mode
//! (`process.roles=broker,controller`), with a share of the `KRaft`
//! metadata quorum, a metadata image, partition logs, replication, a group
//! coordinator and a request pipeline that answers in order.
//!
//! [`BrokerNode`] has two listeners, as a combined Kafka node does: the
//! client listener on [`Endpoint::kafka`](crate::lab::net::Endpoint::kafka) (`PLAINTEXT`, 9092) and the
//! controller listener on port 9093 (`CONTROLLER`), which also carries the
//! quorum's raft messages. Every request reaches it as one frame, is decoded
//! at the version the client asked for, and is answered with a byte-exact
//! Kafka response. Behaviour follows Apache Kafka 4.3: KIP semantics, error
//! codes and response shapes.
//!
//! # The cluster
//!
//! Every broker runs a [`ControllerCore`], as a voter when its id is in
//! `controller_quorum_voters` and as an observer otherwise, and applies every
//! committed batch of the metadata log to its image, so every broker's image
//! is the replay of the same log. The quorum leader becomes the active
//! controller once it has applied its own leader-change marker, and makes
//! every metadata decision with
//! [`ControllerDecisions`](crate::lab::controller::ControllerDecisions): broker registration
//! and fencing, topics, partitions, ISR changes, producer-id blocks, and the
//! leader elections a fenced broker forces.
//!
//! A broker registers with the active controller when it starts
//! (`BrokerRegistration` on the controller listener), heartbeats every
//! `broker_heartbeat_interval_ms`, and stays fenced until it has caught up
//! with the metadata log; its client listener answers only once the
//! controller unfenced it, as Kafka enables its request processing then. A
//! broker whose heartbeats stop for `broker_session_timeout_ms` is fenced,
//! and its partitions elect new leaders from their ISR. A client may send the
//! controller apis to any broker: the broker forwards them to the active
//! controller in an `Envelope` and relays the answer. A partition leader
//! proposes its ISR changes with `AlterPartition`. The group coordinator
//! serves the groups of the `__consumer_offsets` partitions the broker
//! leads, and keeps their state in those partitions.
//!
//! # Configuration
//!
//! The `config` object of a `"broker"` node takes these keys; every other key
//! is an error.
//!
//! | key | type | default | Kafka config | meaning |
//! | --- | --- | --- | --- | --- |
//! | `broker_id` | `i32` | required | `node.id` | the broker id: at least 1, and equal to the node id |
//! | `rack` | `string` | none | `broker.rack` | the rack the registration advertises |
//! | `voter` | `bool` | `true` | `process.roles` | whether the node is a quorum voter; it must agree with `controller_quorum_voters` |
//! | `controller_quorum_voters` | `[i32]` | `[broker_id]` for a voter, `[]` otherwise | `controller.quorum.voters` | the ids of the quorum's voters, the same on every broker |
//! | `default_partitions` | `i32` | `1` | `num.partitions` | the partition count of auto-created topics and `-1` requests |
//! | `default_replication_factor` | `i16` | `-1` | `default.replication.factor` | `-1` means every unfenced broker |
//! | `min_insync_replicas` | `i32` | `1` | `min.insync.replicas` | the broker default |
//! | `log_retention_ms` | `i64` | `604800000` (7 days) | `log.retention.ms` | the broker default of `retention.ms`; `-1` keeps everything |
//! | `replica_lag_time_max_ms` | `u64` | `10000` | `replica.lag.time.max.ms` | how long a follower may lag before it leaves the ISR |
//! | `request_timeout_ms` | `u64` | `30000` | `request.timeout.ms` | the longest a held request waits (a `Produce` waits its own `timeout_ms` when shorter, a `Fetch` its `max_wait_ms`), and how long a follower waits for its leader's answer |
//! | `broker_heartbeat_interval_ms` | `u64` | `2000` | `broker.heartbeat.interval.ms` | the pause between two heartbeats |
//! | `broker_session_timeout_ms` | `u64` | `9000` | `broker.session.timeout.ms` | how long the controller waits for a heartbeat before it fences the broker |
//! | `offsets_commit_timeout_ms` | `u64` | `5000` | `offsets.commit.timeout.ms` | how long a group coordinator write waits for its commit |
//! | `group_initial_rebalance_delay_ms` | `u64` | `3000` | `group.initial.rebalance.delay.ms` | how long a classic group that was empty waits for more members |
//! | `group_min_session_timeout_ms` | `u64` | `6000` | `group.min.session.timeout.ms` | the least session timeout a classic member may ask for |
//! | `group_max_session_timeout_ms` | `u64` | `1800000` | `group.max.session.timeout.ms` | the most session timeout a classic member may ask for |
//! | `group_consumer_session_timeout_ms` | `u64` | `45000` | `group.consumer.session.timeout.ms` | the session of a KIP-848 member |
//! | `group_consumer_heartbeat_interval_ms` | `i32` | `5000` | `group.consumer.heartbeat.interval.ms` | the heartbeat interval a KIP-848 member is told |
//! | `group_consumer_assignment_interval_ms` | `u64` | `1000` | `group.consumer.assignment.interval.ms` | the least time between two target assignments of a consumer group |
//! | `group_streams_session_timeout_ms` | `u64` | `45000` | `group.streams.session.timeout.ms` | the session of a KIP-1071 member |
//! | `group_streams_heartbeat_interval_ms` | `i32` | `5000` | `group.streams.heartbeat.interval.ms` | the heartbeat interval a KIP-1071 member is told |
//! | `group_streams_num_standby_replicas` | `i32` | `0` | `group.streams.num.standby.replicas` | the standby tasks of a streams group |
//! | `group_streams_initial_rebalance_delay_ms` | `u64` | `3000` | `group.streams.initial.rebalance.delay.ms` | how long the first assignment of a streams group waits |
//! | `group_streams_assignment_interval_ms` | `u64` | `1000` | `group.streams.assignment.interval.ms` | the least time between two target assignments of a streams group |
//!
//! A broker id and a node id are the same number in the lab: the client module
//! resolves the advertised host `node-<id>` back to [`Endpoint::kafka`](crate::lab::net::Endpoint::kafka) of
//! that node, so a spec whose `broker_id` differs from its `id` is rejected.
//! Broker ids start at 1 because the metadata image names a partition with no
//! leader by node id 0 (see [`NO_LEADER`]).
//!
//! # Control commands
//!
//! None: the broker changes only through the requests it serves and the
//! records its quorum commits.
//!
//! # Snapshot
//!
//! ```json
//! { "broker_id": 1, "cluster_id": "...", "controller_id": 1, "state": "RUNNING",
//!   "quorum": { "role": "Leader", "epoch": 1, "leader": 1, "hwm": 9, "leo": 9,
//!     "voters": [1, 2, 3], "voter": true, "log_len": 9, "applied": 9,
//!     "observers": [], "active": true, "metadata_offset": 8 },
//!   "lifecycle": { "state": "RUNNING", "registered": true, "broker_epoch": 3,
//!     "fenced": false, "incarnation_id": "..." },
//!   "channels": { "heartbeat": { "controller": 1, "connected": true, "queued": 0, "in_flight": null },
//!     "forwarding": {...}, "alter-partition": {...} },
//!   "isr_changes": { "queued": [], "in_flight": false },
//!   "producer_ids": { "next_id": null, "block_end": null, "next_block": null, "requesting": false },
//!   "connections": 2, "controller_connections": 2, "held_requests": 0, "requests": {"Produce": 3},
//!   "brokers": [{"id": 1, "host": "node-1", "port": 9092, "rack": null, "fenced": false}],
//!   "topics": [{"name": "orders", "id": "...", "internal": false, "partitions": [
//!     {"index": 0, "leader": 1, "leader_epoch": 0, "replicas": [1, 2], "isr": [1, 2],
//!      "pending_isr": null, "log_start": 0, "log_end": 3, "hwm": 3, "batches": 3,
//!      "size_bytes": 210, "fetch_state": "idle", "followers": [{"id": 2, "leo": 3, "lag_ms": 0}]}]}],
//!   "groups": { "broker_id": 1, "groups": {}, "offsets": {}, "loaded_partitions": [], ... } }
//! ```
//!
//! `controller_id` is the active controller this node knows, `null` when it
//! knows none; `state` is the lifecycle's Kafka `BrokerState`. `quorum` is
//! the controller core's view ([`ControllerCore::snapshot`]) plus whether
//! this node is the active controller and the offset of the last committed
//! batch it applied. `leader` is `null` for a leaderless partition; the log
//! fields and `fetch_state` are `null` for a partition this broker does not
//! host, `pending_isr` is the ISR this leader proposed and waits on, and
//! `followers` is filled only where this broker leads. `connections` counts
//! the client listener's connections, `controller_connections` the
//! controller listener's. `groups` is the group coordinator's snapshot
//! (`Coordinator::snapshot`) with the `__consumer_offsets` partitions it
//! loaded.
//!
//! # Events
//!
//! `topic_created`, `topic_deleted`, `leader_change`, `isr_change`,
//! `alter_partition_failed`, `produce_error`, `fetch_error`,
//! `replica_truncated`, `retention`, `connection_closed_malformed`,
//! `controller` (this node became or stopped being the active controller),
//! `broker_registered`, `broker_registration_failed`, `broker_heartbeat_failed`,
//! `broker_unfenced`, `broker_fenced`, `broker_fence_changed` (the active
//! controller fenced or unfenced a broker on its heartbeat),
//! `broker_session_expired`, `producer_ids_failed`, `topic_creation_failed`,
//! `coordinator_loaded`, `coordinator_unloaded`, and the controller core's
//! `quorum` and `raft`, each with a `level`.
//!
//! # Durable state
//!
//! The broker persists through [`Ctx::persist`] so a page reload restores it
//! through [`Node::load`]: every stored batch goes to the log store
//! `log/<topic>/<partition>` at its base offset, a truncation or a retention
//! pass trims that store, a deleted topic clears it; the partition's checkpoint
//! (`hwm`, `log_start`, `epochs`, `producers` as JSON) goes to the key-value
//! store `meta/<topic>/<partition>` whenever it changes. The controller core
//! keeps the metadata log in the log store `kraft` and its quorum state and
//! high watermark in the key-value store `kraft-state`; a reload rebuilds the
//! image by replaying the committed part of that log.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use serde_json::{Value, json};
use uuid::Uuid;

mod channel;
mod cluster;
mod conn;
pub mod coordinator;
mod dispatch;
mod forward;
mod groups;
mod handlers;
mod isr;
mod lifecycle;
mod log;
mod producer_ids;
mod quorum;
mod replica;
/// Wire helpers for tests that talk to a broker.
pub mod test_support;

use self::{
    channel::{ChannelEvent, ControllerChannel, FORWARDING_RETRY_TIMEOUT_MS, Purpose},
    conn::QueuedRequest,
    coordinator::CoordinatorConfig,
    dispatch::{ConnKey, Step as DispatchStep},
    forward::Forwarding,
    groups::{DEFAULT_OFFSETS_COMMIT_TIMEOUT_MS, Groups},
    isr::IsrManager,
    lifecycle::Lifecycle,
    producer_ids::ProducerIdManager,
    quorum::Quorum,
};
pub use self::{
    cluster::{
        CONSUMER_OFFSETS_PARTITIONS, CONSUMER_OFFSETS_TOPIC, LAB_CLUSTER_ID, NO_LEADER,
        active_brokers, cluster_id_string, group_partition, java_string_hash, parse_cluster_id,
        record_leader,
    },
    conn::{Connection, FrameError, ParsedRequest, parse_request_frame, response_frame},
    dispatch::{
        DispatchError, HoldReason, Listener, Outcome, Reply, RequestCtx, Step, VersionRange,
        broker_api_versions_table, broker_versions, controller_api_versions_table,
        controller_versions,
    },
    handlers::api_versions::{finalized_features, supported_features},
    lifecycle::{BrokerState, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_SESSION_TIMEOUT_MS},
    log::{
        AppendError, AppendInfo, AppendPolicy, Checkpoint, LogChange, PartitionLog, ProducerBatch,
        ProducerEntry, RecordError, StoredBatch,
    },
    replica::{FetchState, FollowerState, PendingIsr, Replica, ReplicaChange},
};
use super::{
    LabError, codes, config_field, config_field_or,
    controller::{ControllerCore, RAFT_CONN_BASE, RAFT_PORT},
    net::{
        CLIENT_PORT, ConnId, Ctx, DurableImage, DurableOp, Frame, KAFKA_PORT, Millis, Node, NodeId,
        Payload,
    },
    scenario::NodeSpec,
};

/// How often the broker runs retention and ISR maintenance.
pub const TICK_MS: Millis = 1_000;

/// Kafka's default `retention.ms`: seven days.
pub const DEFAULT_RETENTION_MS: i64 = 604_800_000;

/// How many rounds one call runs the quorum, the channels and the held
/// requests while they feed each other: a single voter commits a proposal
/// at once, so a request's write can complete within the call that made it.
const SETTLE_ROUNDS: usize = 8;

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
    /// `controller_quorum_voters`, ascending.
    pub controller_quorum_voters: Vec<i32>,
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
    /// `broker_heartbeat_interval_ms`.
    pub broker_heartbeat_interval_ms: u64,
    /// `broker_session_timeout_ms`.
    pub broker_session_timeout_ms: u64,
    /// `offsets_commit_timeout_ms`.
    pub offsets_commit_timeout_ms: u64,
    /// The `group_*` keys, as the group coordinator takes them.
    pub coordinator: CoordinatorConfig,
}

const CONFIG_KEYS: &[&str] = &[
    "broker_id",
    "rack",
    "voter",
    "controller_quorum_voters",
    "default_partitions",
    "default_replication_factor",
    "min_insync_replicas",
    "log_retention_ms",
    "replica_lag_time_max_ms",
    "request_timeout_ms",
    "broker_heartbeat_interval_ms",
    "broker_session_timeout_ms",
    "offsets_commit_timeout_ms",
    "group_initial_rebalance_delay_ms",
    "group_min_session_timeout_ms",
    "group_max_session_timeout_ms",
    "group_consumer_session_timeout_ms",
    "group_consumer_heartbeat_interval_ms",
    "group_consumer_assignment_interval_ms",
    "group_streams_session_timeout_ms",
    "group_streams_heartbeat_interval_ms",
    "group_streams_num_standby_replicas",
    "group_streams_initial_rebalance_delay_ms",
    "group_streams_assignment_interval_ms",
];

impl BrokerConfig {
    /// Read and validate a spec's config.
    ///
    /// # Errors
    /// Returns a config error for an unknown key, a wrong type, a missing
    /// `broker_id`, a `broker_id` below 1 or other than the node id, a
    /// `voter` flag that disagrees with `controller_quorum_voters`, or a
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
        let voter: bool = config_field_or(spec, "voter", true)?;
        let voters = quorum_voters(spec, broker_id, voter)?;
        let coordinator = coordinator_config(spec)?;
        let config = Self {
            broker_id,
            rack: config_field_or(spec, "rack", None)?,
            voter,
            controller_quorum_voters: voters,
            default_partitions: config_field_or(spec, "default_partitions", 1)?,
            default_replication_factor: config_field_or(spec, "default_replication_factor", -1)?,
            min_insync_replicas: config_field_or(spec, "min_insync_replicas", 1)?,
            log_retention_ms: config_field_or(spec, "log_retention_ms", DEFAULT_RETENTION_MS)?,
            replica_lag_time_max_ms: config_field_or(spec, "replica_lag_time_max_ms", 10_000)?,
            request_timeout_ms: config_field_or(spec, "request_timeout_ms", 30_000)?,
            broker_heartbeat_interval_ms: config_field_or(
                spec,
                "broker_heartbeat_interval_ms",
                DEFAULT_HEARTBEAT_INTERVAL_MS,
            )?,
            broker_session_timeout_ms: config_field_or(
                spec,
                "broker_session_timeout_ms",
                DEFAULT_SESSION_TIMEOUT_MS,
            )?,
            offsets_commit_timeout_ms: config_field_or(
                spec,
                "offsets_commit_timeout_ms",
                DEFAULT_OFFSETS_COMMIT_TIMEOUT_MS,
            )?,
            coordinator,
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
        if config.broker_heartbeat_interval_ms == 0 || config.broker_session_timeout_ms == 0 {
            return Err(LabError::config(
                spec,
                "broker_heartbeat_interval_ms and broker_session_timeout_ms must be positive",
            ));
        }
        Ok(config)
    }
}

/// The `controller_quorum_voters` of a broker's config, ascending: every id
/// at least 1, and the broker's own among them exactly when it is a voter.
fn quorum_voters(spec: &NodeSpec, broker_id: i32, voter: bool) -> Result<Vec<i32>, LabError> {
    let mut voters: Vec<i32> = config_field_or(
        spec,
        "controller_quorum_voters",
        if voter { vec![broker_id] } else { Vec::new() },
    )?;
    voters.sort_unstable();
    voters.dedup();
    if voters.iter().any(|&id| id < 1) {
        return Err(LabError::config(
            spec,
            "controller_quorum_voters must name broker ids of at least 1",
        ));
    }
    if voters.contains(&broker_id) != voter {
        return Err(LabError::config(
            spec,
            format!(
                "voter is {voter}, but controller_quorum_voters {} broker {broker_id}",
                if voter { "does not name" } else { "names" }
            ),
        ));
    }
    Ok(voters)
}

/// The `group_*` keys of a broker's config, as the group coordinator takes
/// them, each defaulting to Kafka's default.
fn coordinator_config(spec: &NodeSpec) -> Result<CoordinatorConfig, LabError> {
    let defaults = CoordinatorConfig::default();
    Ok(CoordinatorConfig {
        initial_rebalance_delay_ms: config_field_or(
            spec,
            "group_initial_rebalance_delay_ms",
            defaults.initial_rebalance_delay_ms,
        )?,
        classic_min_session_timeout_ms: config_field_or(
            spec,
            "group_min_session_timeout_ms",
            defaults.classic_min_session_timeout_ms,
        )?,
        classic_max_session_timeout_ms: config_field_or(
            spec,
            "group_max_session_timeout_ms",
            defaults.classic_max_session_timeout_ms,
        )?,
        consumer_session_timeout_ms: config_field_or(
            spec,
            "group_consumer_session_timeout_ms",
            defaults.consumer_session_timeout_ms,
        )?,
        consumer_heartbeat_interval_ms: config_field_or(
            spec,
            "group_consumer_heartbeat_interval_ms",
            defaults.consumer_heartbeat_interval_ms,
        )?,
        consumer_assignment_interval_ms: config_field_or(
            spec,
            "group_consumer_assignment_interval_ms",
            defaults.consumer_assignment_interval_ms,
        )?,
        streams_session_timeout_ms: config_field_or(
            spec,
            "group_streams_session_timeout_ms",
            defaults.streams_session_timeout_ms,
        )?,
        streams_heartbeat_interval_ms: config_field_or(
            spec,
            "group_streams_heartbeat_interval_ms",
            defaults.streams_heartbeat_interval_ms,
        )?,
        streams_num_standby_replicas: config_field_or(
            spec,
            "group_streams_num_standby_replicas",
            defaults.streams_num_standby_replicas,
        )?,
        streams_initial_rebalance_delay_ms: config_field_or(
            spec,
            "group_streams_initial_rebalance_delay_ms",
            defaults.streams_initial_rebalance_delay_ms,
        )?,
        streams_assignment_interval_ms: config_field_or(
            spec,
            "group_streams_assignment_interval_ms",
            defaults.streams_assignment_interval_ms,
        )?,
    })
}

/// A broker node.
pub struct BrokerNode {
    id: NodeId,
    config: BrokerConfig,
    image: MetadataImage,
    quorum: Quorum,
    lifecycle: Lifecycle,
    /// The channel of the registration and the heartbeats.
    heartbeat_channel: ControllerChannel,
    /// The channel of forwarded requests, topic creation and producer ids.
    forwarding_channel: ControllerChannel,
    /// The channel of `AlterPartition`.
    alter_partition_channel: ControllerChannel,
    forwarding: Forwarding,
    isr: IsrManager,
    producer_ids: ProducerIdManager,
    groups: Groups,
    replicas: BTreeMap<TopicPartition, Replica>,
    conns: BTreeMap<ConnKey, Connection>,
    links: BTreeMap<i32, replica::LeaderLink>,
    counters: BTreeMap<&'static str, u64>,
    next_tick: Millis,
    next_conn: u32,
    started: bool,
    /// The time of the last call the world made, for the snapshot.
    now: Millis,
    /// Logs a reload restored, taken by the replicas the image opens.
    restored_logs: BTreeMap<TopicPartition, PartitionLog>,
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
        let voters: Vec<NodeId> = config
            .controller_quorum_voters
            .iter()
            .filter_map(|&id| u32::try_from(id).ok().map(NodeId))
            .collect();
        let core = ControllerCore::new(spec.id, &voters, LAB_CLUSTER_ID);
        Ok(Self {
            id: spec.id,
            image: MetadataImage::new(LAB_CLUSTER_ID),
            quorum: Quorum::new(core),
            lifecycle: Lifecycle::new(),
            heartbeat_channel: ControllerChannel::new(config.broker_heartbeat_interval_ms),
            forwarding_channel: ControllerChannel::new(FORWARDING_RETRY_TIMEOUT_MS),
            alter_partition_channel: ControllerChannel::new(Millis::MAX),
            forwarding: Forwarding::default(),
            isr: IsrManager::default(),
            producer_ids: ProducerIdManager::default(),
            groups: Groups::new(config.broker_id, config.coordinator.clone()),
            replicas: BTreeMap::new(),
            conns: BTreeMap::new(),
            links: BTreeMap::new(),
            counters: BTreeMap::new(),
            next_tick: 0,
            next_conn: 0,
            started: false,
            now: 0,
            restored_logs: BTreeMap::new(),
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

    /// The metadata image, the broker's view of the cluster: the replay of
    /// the committed metadata log.
    #[must_use]
    pub fn image(&self) -> &MetadataImage {
        &self.image
    }

    /// The active controller this node knows, `-1` when it knows none.
    #[must_use]
    pub fn controller_id(&self) -> i32 {
        self.quorum
            .controller()
            .and_then(|id| i32::try_from(id.0).ok())
            .unwrap_or(-1)
    }

    /// Whether this node is the active controller.
    #[must_use]
    pub fn is_active_controller(&self) -> bool {
        self.quorum.active.is_some()
    }

    /// Where the broker's lifecycle is.
    #[must_use]
    pub fn broker_state(&self) -> BrokerState {
        self.lifecycle.state
    }

    /// The node's share of the metadata quorum.
    #[must_use]
    pub fn controller_core(&self) -> &ControllerCore {
        &self.quorum.core
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

    /// A random live broker, Kafka's `getRandomAliveBrokerId`, which
    /// `Metadata` and `DescribeCluster` report as the controller in `KRaft`;
    /// `-1` when no broker is live.
    fn random_alive_broker(&self, ctx: &mut Ctx<'_>) -> i32 {
        let mut alive: Vec<i32> = self
            .image
            .brokers()
            .filter(|b| !b.fenced)
            .map(|b| cluster::wire_id(b.node_id))
            .collect();
        alive.sort_unstable();
        if alive.is_empty() {
            return -1;
        }
        let pick = usize::try_from(ctx.rand(alive.len() as u64)).unwrap_or(0);
        alive[pick.min(alive.len() - 1)]
    }

    /// Apply committed metadata records: the image changes, replicas open and
    /// close to match it, links follow the leaders, the coordinator loads and
    /// unloads its partitions, and events record what changed. The quorum's
    /// committed batches are the one way the image changes. Held requests
    /// the change unblocks are answered when the call the world made returns.
    fn apply_metadata(&mut self, ctx: &mut Ctx<'_>, records: &[MetadataRecord]) {
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
        self.reconcile_replicas(ctx);
        self.sync_links(ctx);
        self.sync_coordinator(ctx);
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

    /// The periodic tick: retention on every hosted log of a topic whose
    /// `cleanup.policy` deletes, and an ISR shrink proposed for every led
    /// partition with a follower out of sync.
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
            self.propose_isr(&key, isr, None);
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

    /// The common tail of every call the world makes: fire the quorum's due
    /// timers, run the tick and the coordinator's deadlines, then drive the
    /// quorum, the lifecycle, the controller channels and the held requests
    /// until they settle, drive the replication links, persist what changed,
    /// and arm the timer.
    fn settle(&mut self, ctx: &mut Ctx<'_>) {
        self.now = ctx.now();
        if self
            .quorum
            .core
            .next_deadline()
            .is_some_and(|at| at <= self.now)
        {
            self.quorum.core.on_timer(ctx);
        }
        if self.now >= self.next_tick {
            self.tick(ctx);
            self.next_tick = self.now + TICK_MS;
        }
        self.coordinator_tick(ctx);
        for _ in 0..SETTLE_ROUNDS {
            let applied = self.quorum.applied;
            let active = self.quorum.active.is_some();
            let serving = self.lifecycle.serving();
            self.drive_quorum(ctx);
            self.poll_lifecycle(ctx);
            self.poll_isr(ctx);
            let events = self.poll_channels(ctx);
            let progressed = self.retry_all_held(ctx);
            if !serving && self.lifecycle.serving() {
                self.pump_all(ctx);
            }
            let settled = !events
                && !progressed
                && applied == self.quorum.applied
                && active == self.quorum.active.is_some()
                && serving == self.lifecycle.serving();
            if settled {
                break;
            }
        }
        self.prune_group_answers();
        self.poll_links(ctx);
        self.flush_durable(ctx);
        self.rearm(ctx);
    }

    /// Drive the three controller channels and route what finished. Returns
    /// whether anything did.
    fn poll_channels(&mut self, ctx: &mut Ctx<'_>) -> bool {
        let leader = self.quorum.controller();
        let next_conn = &mut self.next_conn;
        let mut new_conn = || {
            *next_conn += 1;
            ConnId(*next_conn)
        };
        let mut events = self.heartbeat_channel.poll(ctx, leader, &mut new_conn);
        events.extend(self.forwarding_channel.poll(ctx, leader, &mut new_conn));
        events.extend(
            self.alter_partition_channel
                .poll(ctx, leader, &mut new_conn),
        );
        let any = !events.is_empty();
        for event in events {
            self.on_channel_event(ctx, event);
        }
        any
    }

    /// Hand a finished channel request to whoever asked for it.
    fn on_channel_event(&mut self, ctx: &mut Ctx<'_>, event: ChannelEvent) {
        match event.purpose {
            Purpose::Lifecycle => self.on_lifecycle_outcome(ctx, event.outcome),
            Purpose::AlterPartition => self.on_isr_outcome(ctx, event.outcome),
            Purpose::ProducerIds => self.on_producer_ids_outcome(ctx, &event.outcome),
            Purpose::CreateTopics { topics } => {
                self.on_create_topics_outcome(ctx, &topics, &event.outcome);
            }
            Purpose::Forward { token } => self.on_forward_outcome(token, event.outcome),
        }
    }

    // ---- connections ------------------------------------------------------------

    /// A frame for one of the node's listeners.
    fn on_listener_frame(&mut self, ctx: &mut Ctx<'_>, listener: Listener, frame: Frame) {
        let key = frame.conn_key();
        match frame.payload {
            Payload::Open => {
                self.conns.insert(key, Connection::new(listener, ctx.now()));
            }
            Payload::Close => {
                self.conns.remove(&key);
            }
            Payload::Data(bytes) => {
                if self.conns.get(&key).is_none_or(|c| c.listener != listener) {
                    // A frame on a connection the broker never saw opened, or
                    // one it already closed, is answered like a reset peer.
                    ctx.send(Frame::close(listener.endpoint(self.id), key.0, key.1));
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

    /// A frame for the node's client socket: an answer of a controller
    /// listener to a channel, a raft frame of the core's links, or an answer
    /// of a leader to a replication link.
    fn on_client_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if frame.src.port == RAFT_PORT {
            if frame.conn.0 >= RAFT_CONN_BASE {
                self.quorum.core.on_frame(ctx, frame);
                return;
            }
            let mut events = self.heartbeat_channel.on_frame(ctx, &frame);
            events.extend(self.forwarding_channel.on_frame(ctx, &frame));
            events.extend(self.alter_partition_channel.on_frame(ctx, &frame));
            for event in events {
                self.on_channel_event(ctx, event);
            }
        } else {
            self.on_link_frame(ctx, frame);
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
            raw: parsed.raw,
            received_at: ctx.now(),
            held: None,
        });
        self.pump(ctx, key);
    }

    /// Serve the queued requests of a connection in order until one is held
    /// or the queue is empty. The client listener serves nothing until the
    /// broker was unfenced.
    fn pump(&mut self, ctx: &mut Ctx<'_>, key: ConnKey) {
        loop {
            let serving = self.lifecycle.serving();
            let Some(conn) = self.conns.get_mut(&key) else {
                return;
            };
            if conn.is_blocked() || (conn.listener == Listener::Broker && !serving) {
                return;
            }
            let listener = conn.listener;
            let Some(request) = conn.queue.pop_front() else {
                return;
            };
            let req = RequestCtx {
                conn: key,
                listener,
                api_key: request.api_key,
                version: request.version,
                correlation_id: request.correlation_id,
                client_id: request.client_id.clone(),
                raw: request.raw.clone(),
                held_until: None,
            };
            *self
                .counters
                .entry(<&'static str>::from(request.api_key))
                .or_insert(0) += 1;
            let step = listener.dispatch(self, ctx, &req, &request.body);
            if !self.finish_step(ctx, key, &req, request, step) {
                return;
            }
        }
    }

    /// Serve every connection that has requests waiting and nothing held,
    /// once the client listener opens.
    fn pump_all(&mut self, ctx: &mut Ctx<'_>) {
        let keys: Vec<ConnKey> = self
            .conns
            .iter()
            .filter(|(_, c)| !c.queue.is_empty() && !c.is_blocked())
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.pump(ctx, key);
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
        ctx.send(Frame::data(
            req.listener.endpoint(self.id),
            key.0,
            key.1,
            frame,
        ));
    }

    fn close_connection(&mut self, ctx: &mut Ctx<'_>, key: ConnKey) {
        if let Some(conn) = self.conns.remove(&key) {
            ctx.send(Frame::close(conn.listener.endpoint(self.id), key.0, key.1));
        }
    }

    /// Run every held request again until none can make progress. A request
    /// that completes lets the ones queued behind it run. Returns whether any
    /// held request moved.
    fn retry_all_held(&mut self, ctx: &mut Ctx<'_>) -> bool {
        let mut any = false;
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
                let Some(listener) = self.conns.get(&key).map(|c| c.listener) else {
                    continue;
                };
                let req = RequestCtx {
                    conn: key,
                    listener,
                    api_key: request.api_key,
                    version: request.version,
                    correlation_id: request.correlation_id,
                    client_id: request.client_id.clone(),
                    raw: request.raw.clone(),
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
                return any;
            }
            any = true;
        }
    }

    /// Arm the timer for the earliest deadline after now: the tick, a held
    /// request, a replication link, the quorum, the active controller's
    /// session check, the lifecycle, a controller channel, the ISR queue, or
    /// the group coordinator. [`BrokerNode::settle`] has handled everything
    /// due now, so the timer never fires twice in one instant.
    fn rearm(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let mut next = self.next_tick.max(now + 1);
        let mut note = |at: Option<Millis>| {
            if let Some(at) = at {
                next = next.min(at.max(now + 1));
            }
        };
        for conn in self.conns.values() {
            note(
                conn.queue
                    .front()
                    .and_then(|r| r.held.as_ref())
                    .map(HoldReason::deadline),
            );
        }
        note(self.link_deadline(now));
        note(self.quorum.core.next_deadline());
        note(self.controller_deadline());
        note(self.lifecycle.next_deadline());
        note(self.heartbeat_channel.next_deadline(now));
        note(self.forwarding_channel.next_deadline(now));
        note(self.alter_partition_channel.next_deadline(now));
        note(self.isr.next_deadline(now));
        note(self.coordinator_deadline());
        ctx.arm(next);
    }

    /// A fresh client-side connection id for a link this broker opens.
    fn next_conn_id(&mut self) -> ConnId {
        self.next_conn += 1;
        ConnId(self.next_conn)
    }
}

impl Node for BrokerNode {
    fn kind(&self) -> &'static str {
        "broker"
    }

    // The metadata log and the quorum state come back first, and the image
    // is rebuilt from the committed part of the log; then every partition
    // log is rebuilt beside its checkpoint, and the image opens the replicas
    // at start.
    fn load(&mut self, image: DurableImage) {
        self.quorum.core.load(&image);
        self.quorum.core.replay_committed();
        for batch in self.quorum.core.take_committed() {
            for record in &batch.records {
                self.image.apply(record);
            }
            self.quorum.applied = batch.offset;
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

    // A boot keeps the image, the metadata log and the partition logs, and
    // drops every connection and everything a process keeps in memory: the
    // quorum role, the channels, the ISR queue, the producer-id blocks and
    // the group coordinator. The broker registers again as a new
    // incarnation, followers reconcile with their leaders again, and the
    // coordinator reloads the partitions this broker leads.
    fn start(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        self.started = true;
        self.now = now;
        self.conns.clear();
        self.links.clear();
        self.heartbeat_channel.reset();
        self.forwarding_channel.reset();
        self.alter_partition_channel.reset();
        self.forwarding.reset();
        self.isr.reset();
        self.producer_ids.reset();
        self.groups = Groups::new(self.config.broker_id, self.coordinator_config());
        self.quorum.reset();
        self.quorum.core.start(ctx);
        // A new incarnation per start (KIP-631). A reload replays the node's
        // generator from the scenario seed, so the length of the metadata log
        // the node restored keeps the reloaded process a new incarnation too.
        let restored = u64::try_from(self.quorum.core.log_end_offset()).unwrap_or(0);
        let incarnation = Uuid::from_u64_pair(ctx.rand(u64::MAX), ctx.rand(u64::MAX) ^ restored);
        self.lifecycle.start(now, incarnation);
        self.next_tick = now + TICK_MS;
        let me = self.config.broker_id;
        for replica in self.replicas.values_mut() {
            replica.on_restart(me, now);
        }
        self.reconcile_replicas(ctx);
        self.sync_links(ctx);
        self.sync_coordinator(ctx);
        self.settle(ctx);
    }

    fn stop(&mut self) {
        self.started = false;
        self.quorum.core.stop();
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if !self.started || frame.dst.node != self.id {
            return;
        }
        match frame.dst.port {
            KAFKA_PORT => self.on_listener_frame(ctx, Listener::Broker, frame),
            RAFT_PORT if frame.conn.0 >= RAFT_CONN_BASE => self.quorum.core.on_frame(ctx, frame),
            RAFT_PORT => self.on_listener_frame(ctx, Listener::Controller, frame),
            CLIENT_PORT => self.on_client_frame(ctx, frame),
            _ => {}
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
        let result = Err(format!(
            "unknown broker command {:?}",
            command.get("cmd").and_then(Value::as_str)
        ));
        if self.started {
            self.settle(ctx);
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
        let client_connections = self
            .conns
            .values()
            .filter(|c| c.listener == Listener::Broker)
            .count();
        json!({
            "broker_id": me,
            "cluster_id": cluster_id_string(self.image.cluster_id()),
            "controller_id": self.quorum.controller(),
            "state": self.lifecycle.state.name(),
            "quorum": self.quorum.snapshot(),
            "lifecycle": self.lifecycle.snapshot(),
            "channels": {
                "heartbeat": self.heartbeat_channel.snapshot(),
                "forwarding": self.forwarding_channel.snapshot(),
                "alter-partition": self.alter_partition_channel.snapshot(),
            },
            "isr_changes": self.isr.snapshot(),
            "producer_ids": self.producer_ids.snapshot(),
            "connections": client_connections,
            "controller_connections": self.conns.len() - client_connections,
            "held_requests": held,
            "requests": self.counters,
            "brokers": brokers,
            "topics": topics,
            "groups": self.groups.snapshot(),
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
            "pending_isr": replica.and_then(|r| r.pending_isr.as_ref()).map(|p| p.proposed.clone()),
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
    fn config_takes_kafkas_defaults_and_the_group_keys() {
        let defaults = BrokerConfig {
            broker_id: 1,
            rack: Some("a".to_string()),
            voter: true,
            controller_quorum_voters: vec![1],
            default_partitions: 1,
            default_replication_factor: -1,
            min_insync_replicas: 1,
            log_retention_ms: DEFAULT_RETENTION_MS,
            replica_lag_time_max_ms: 10_000,
            request_timeout_ms: 30_000,
            broker_heartbeat_interval_ms: 2_000,
            broker_session_timeout_ms: 9_000,
            offsets_commit_timeout_ms: 5_000,
            coordinator: CoordinatorConfig::default(),
        };
        for (config, expected) in [
            (json!({ "broker_id": 1, "rack": "a" }), defaults.clone()),
            (
                json!({ "broker_id": 1, "rack": "a", "controller_quorum_voters": [3, 1, 2, 1],
                        "broker_heartbeat_interval_ms": 500, "group_initial_rebalance_delay_ms": 0,
                        "group_consumer_heartbeat_interval_ms": 1000 }),
                BrokerConfig {
                    controller_quorum_voters: vec![1, 2, 3],
                    broker_heartbeat_interval_ms: 500,
                    coordinator: CoordinatorConfig {
                        initial_rebalance_delay_ms: 0,
                        consumer_heartbeat_interval_ms: 1_000,
                        ..CoordinatorConfig::default()
                    },
                    ..defaults.clone()
                },
            ),
            (
                json!({ "broker_id": 1, "rack": "a", "voter": false, "controller_quorum_voters": [2] }),
                BrokerConfig {
                    voter: false,
                    controller_quorum_voters: vec![2],
                    ..defaults.clone()
                },
            ),
        ] {
            let spec = NodeSpec::new(1, "broker", "b", config.clone());
            assert!(
                BrokerConfig::from_spec(&spec).unwrap() == expected,
                "{config}"
            );
        }
    }

    #[test]
    fn config_rejects_unknown_keys_bad_ids_and_a_voter_outside_the_quorum() {
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
            (
                1,
                json!({ "broker_id": 1, "controller_quorum_voters": [2, 3] }),
            ),
            (
                1,
                json!({ "broker_id": 1, "voter": false, "controller_quorum_voters": [1, 2] }),
            ),
            (
                1,
                json!({ "broker_id": 1, "controller_quorum_voters": [0, 1] }),
            ),
            (
                1,
                json!({ "broker_id": 1, "broker_heartbeat_interval_ms": 0 }),
            ),
            (1, json!(7)),
            (0, json!({ "broker_id": 0 })),
        ] {
            let spec = NodeSpec::new(id, "broker", "b", config.clone());
            assert!(BrokerConfig::from_spec(&spec).is_err(), "{config}");
        }
    }
}
