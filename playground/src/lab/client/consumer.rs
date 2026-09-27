//! The consumer: group membership with the classic protocol or KIP-848,
//! positions from committed offsets or `auto.offset.reset`, a fetch loop per
//! leader, and offset commits.
//!
//! With the classic protocol the member joins with `JoinGroup`, carrying a
//! `ConsumerProtocolSubscription` for the `range` assignor; the leader runs
//! Kafka's `RangeAssignor` over every member's subscription and hands the
//! assignments to `SyncGroup`; a `Heartbeat` loop keeps the session, and
//! `REBALANCE_IN_PROGRESS`, `ILLEGAL_GENERATION` or `UNKNOWN_MEMBER_ID` make
//! the member join again. With KIP-848 the member sends
//! `ConsumerGroupHeartbeat` with epoch 0 and a member id of its own to join,
//! reconciles each assignment the coordinator returns (it stops fetching the
//! partitions it gives up, commits its positions when auto-commit is on,
//! revokes, then takes), and acknowledges it with the next heartbeat;
//! `FENCED_MEMBER_EPOCH` makes it give up its partitions as lost and join
//! again from epoch 0.
//!
//! Positions come from `OffsetFetch` at assignment, or from `ListOffsets`
//! with the reset policy when the group committed none. One `Fetch` per
//! leader carries every assigned partition the leader owns; the buffered
//! records leave through [`Consumer::poll`] and [`Consumer::poll_at`], which
//! advance the positions a commit sends.
//!
//! # Manual assignment and seeking
//!
//! [`Consumer::assign`] takes partitions without a group, as Kafka's
//! `KafkaConsumer.assign`: no membership, positions from the group's
//! committed offsets when the consumer has a `group.id`, else from the reset
//! policy, and commits with generation `-1` and no member id. It refuses a
//! consumer that subscribed, as Kafka's `SubscriptionState` does.
//! [`Consumer::seek`] sets a partition's position at once;
//! [`Consumer::seek_to_beginning`] and [`Consumer::seek_to_end`] ask for an
//! offset reset that the next `ListOffsets` resolves. Both drop the records
//! fetched for the old position, and a fetch answer for it is discarded,
//! as Kafka's `FetchCollector` discards a stale fetch. A seek on a partition
//! the consumer does not hold is refused with Kafka's `IllegalStateException`
//! text.
//!
//! # Static membership (KIP-345)
//!
//! With `group_instance_id` set, `JoinGroup`, `SyncGroup`, `Heartbeat`,
//! `OffsetCommit` and every KIP-848 heartbeat carry the instance id. A
//! restarted member joins with the same instance id and an empty member id,
//! and the coordinator gives it the place, and the assignment, of the member
//! it replaces; a returning leader that the coordinator tells to skip the
//! assignment (KIP-814) syncs with none. On close a classic static member
//! sends no `LeaveGroup`, as Kafka's `AbstractCoordinator.maybeLeaveGroup`
//! does, so the group keeps it until its session times out; a KIP-848
//! static member leaves with epoch `-2`, as `ConsumerMembershipManager`'s
//! `leaveGroupEpoch` does, and the coordinator keeps its assignment.
//!
//! # Auto-commit
//!
//! Kafka 4.3 commits the consumed positions on the interval only inside
//! `poll`, with either protocol: the classic consumer from
//! `ConsumerCoordinator.poll` (`maybeAutoCommitOffsetsAsync`), the KIP-848
//! consumer from the `AsyncPollEvent` of `poll`
//! (`CommitRequestManager.updateTimerAndMaybeCommit`). [`Consumer::poll_at`]
//! is that `poll`: it commits when `auto.commit.interval.ms` passed since the
//! last auto-commit, and then takes the records; [`Consumer::next_auto_commit`]
//! tells a node when a poll would commit. The interval starts over when an
//! assignment is installed, and a commit that fails with a retriable error
//! brings the next one forward to `retry.backoff.ms`. The consumer also
//! commits before it rejoins a classic group, before it reconciles a KIP-848
//! assignment, and when it closes. Ticks and frames never commit on the
//! interval.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::BufMut;
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions as AssignedPartitions,
        consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        heartbeat_request::HeartbeatRequest,
        heartbeat_response::HeartbeatResponse,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::JoinGroupResponse,
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
        offset_commit_request::{
            self, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::OffsetCommitResponse,
        offset_fetch_request::{
            self, OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
        offset_fetch_response::OffsetFetchResponse,
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid,
    records::{RecordBatch, RecordsPayload},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use super::{
    ClientError, ClientEvent, CoordinatorType, KafkaClient, RequestId, Target,
    assignor::{
        RANGE_PROTOCOL, decode_assignment, decode_subscription, encode_assignment,
        encode_subscription, range_assign,
    },
    batch::{ConsumedRecord, records_of},
    retry,
};
use crate::lab::{
    codes,
    net::{Ctx, Frame, Millis},
};

/// Kafka's `ConsumerGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH`.
const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;
/// Kafka's `ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
/// Kafka's `ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH`:
/// a static member leaves for a while and keeps its assignment.
const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;
/// The generation of an offset commit outside a group's generations:
/// Kafka's `OffsetCommitRequest.DEFAULT_GENERATION_ID`.
const NO_GENERATION: i32 = -1;
/// `ListOffsets` timestamp of the earliest offset.
const EARLIEST_TIMESTAMP: i64 = -2;
/// `ListOffsets` timestamp of the latest offset.
const LATEST_TIMESTAMP: i64 = -1;
/// The last `OffsetCommit` and `OffsetFetch` version that names topics; Kafka's
/// consumers cap there (`forTopicNames`).
const TOPIC_NAME_OFFSET_VERSION: i16 = 9;
/// `MEMBER_ID_REQUIRED` (KIP-394): the coordinator assigned a member id and
/// asks the member to join again with it.
const MEMBER_ID_REQUIRED: i16 = 79;
/// The protocol type of a consumer group.
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

/// The partitions one request covers, by topic name.
type Partitions = Vec<(String, i32)>;

/// The `(topic, partition, offset)` rows of one commit.
type Offsets = Vec<(String, i32, i64)>;

/// The partitions of one `ListOffsets`, each with the reset it resolves.
type Resets = Vec<((String, i32), AutoOffsetReset)>;

/// An `OffsetCommit` in flight.
struct PendingCommit {
    id: RequestId,
    offsets: Offsets,
    /// An auto-commit: one that fails with a retriable error brings the
    /// next auto-commit forward to `retry.backoff.ms`, as the callback of
    /// Kafka's `autoCommitOffsetsAsync` does.
    auto: bool,
}

/// The group protocol of a consumer: Kafka's `group.protocol`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupProtocol {
    /// `JoinGroup`, `SyncGroup` and `Heartbeat` with client-side assignment.
    #[default]
    Classic,
    /// `ConsumerGroupHeartbeat` with server-side assignment (KIP-848).
    Consumer,
}

/// Kafka's `auto.offset.reset`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoOffsetReset {
    Earliest,
    #[default]
    Latest,
}

impl AutoOffsetReset {
    const fn timestamp(self) -> i64 {
        match self {
            Self::Earliest => EARLIEST_TIMESTAMP,
            Self::Latest => LATEST_TIMESTAMP,
        }
    }
}

/// Kafka's `isolation.level`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
}

impl IsolationLevel {
    /// The wire value.
    #[must_use]
    pub const fn as_wire(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}

/// The settings of a consumer, with Kafka's defaults.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConsumerConfig {
    /// `group.id`. Empty means no group, Kafka's unset `group.id`: the
    /// consumer can only [`Consumer::assign`], and commits nothing.
    pub group_id: String,
    /// `group.instance.id`: the member is static (KIP-345). Default: none.
    pub group_instance_id: Option<String>,
    /// `group.protocol`. Default: classic.
    pub group_protocol: GroupProtocol,
    /// `auto.offset.reset`. Default: latest.
    pub auto_offset_reset: AutoOffsetReset,
    /// `session.timeout.ms`. Default: 45 000.
    pub session_timeout_ms: Millis,
    /// `heartbeat.interval.ms`. Default: 3 000.
    pub heartbeat_interval_ms: Millis,
    /// `max.poll.interval.ms`, the rebalance timeout. Default: 300 000.
    pub rebalance_timeout_ms: Millis,
    /// `max.poll.records`. Default: 500.
    pub max_poll_records: usize,
    /// `fetch.max.wait.ms`. Default: 500.
    pub fetch_max_wait_ms: Millis,
    /// `fetch.min.bytes`. Default: 1.
    pub fetch_min_bytes: i32,
    /// `fetch.max.bytes`. Default: 52 428 800.
    pub fetch_max_bytes: i32,
    /// `max.partition.fetch.bytes`. Default: 1 048 576.
    pub max_partition_fetch_bytes: i32,
    /// `enable.auto.commit`. Default: true.
    pub enable_auto_commit: bool,
    /// `auto.commit.interval.ms`. Default: 5 000.
    pub auto_commit_interval_ms: Millis,
    /// `isolation.level`. Default: read uncommitted.
    pub isolation_level: IsolationLevel,
    /// `retry.backoff.ms`. Default: 100.
    pub retry_backoff_ms: Millis,
    /// `client.rack`. Default: none.
    pub rack_id: Option<String>,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            group_instance_id: None,
            group_protocol: GroupProtocol::Classic,
            auto_offset_reset: AutoOffsetReset::Latest,
            session_timeout_ms: 45_000,
            heartbeat_interval_ms: 3_000,
            rebalance_timeout_ms: 300_000,
            max_poll_records: 500,
            fetch_max_wait_ms: 500,
            fetch_min_bytes: 1,
            fetch_max_bytes: 52_428_800,
            max_partition_fetch_bytes: 1_048_576,
            enable_auto_commit: true,
            auto_commit_interval_ms: 5_000,
            isolation_level: IsolationLevel::ReadUncommitted,
            retry_backoff_ms: 100,
            rack_id: None,
        }
    }
}

/// Where the member is in the group.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberState {
    /// No subscription.
    Unsubscribed,
    /// Joining: a `JoinGroup` or an epoch-0 heartbeat is due or in flight.
    Joining,
    /// Classic only: joined, waiting for the `SyncGroup` assignment.
    Syncing,
    /// A member with an assignment.
    Stable,
    /// The coordinator refused the member for good with this code.
    Failed(i16),
    /// The member left the group.
    Left,
}

impl MemberState {
    fn name(self) -> String {
        match self {
            Self::Unsubscribed => "unsubscribed".to_string(),
            Self::Joining => "joining".to_string(),
            Self::Syncing => "syncing".to_string(),
            Self::Stable => "stable".to_string(),
            Self::Failed(code) => format!("failed({code})"),
            Self::Left => "left".to_string(),
        }
    }
}

/// What the consumer reports.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ConsumerEvent {
    /// The member joined: the id and the generation or member epoch.
    Joined { member_id: String, generation: i32 },
    /// Partitions were added to the assignment.
    Assigned { partitions: Vec<(String, i32)> },
    /// Partitions were given up before a rebalance.
    Revoked { partitions: Vec<(String, i32)> },
    /// Partitions were lost: the coordinator fenced the member.
    Lost { partitions: Vec<(String, i32)> },
    /// Offsets were committed, as `(topic, partition, offset)`.
    Committed { offsets: Vec<(String, i32, i64)> },
    /// A request failed with a Kafka error code the member retries or
    /// ignores.
    Error { api: &'static str, code: i16 },
}

/// The consumer's counters.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ConsumerMetrics {
    /// Records fetched into the buffer.
    pub records: u64,
    /// Bytes of record batches fetched.
    pub bytes: u64,
    /// Records returned by `poll`.
    pub polled: u64,
    pub fetches: u64,
    pub commits: u64,
    pub rebalances: u64,
}

/// Why the consumer refused a call, with the text of the exception Kafka's
/// `KafkaConsumer` throws for it.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    /// The partition is not assigned to the consumer: Kafka's
    /// `SubscriptionState.assignedState`.
    #[error("No current assignment for partition {topic}-{partition}")]
    NotAssigned { topic: String, partition: i32 },
    /// A seek to a negative offset.
    #[error("seek offset must not be a negative number")]
    NegativeOffset,
    /// `assign` on a consumer that subscribed: Kafka's
    /// `SUBSCRIPTION_EXCEPTION_MESSAGE`.
    #[error("Subscription to topics, partitions and pattern are mutually exclusive")]
    Subscribed,
    /// `assign` with a partition of an empty or blank topic name.
    #[error("Topic partitions to assign to cannot have null or empty topic")]
    EmptyTopic,
    /// The consumer was closed.
    #[error("This consumer has already been closed.")]
    Closed,
}

/// How far a partition is in getting a position.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PositionState {
    /// The committed offset is not known yet.
    Init,
    /// `OffsetFetch` is in flight.
    FetchingCommitted,
    /// The group committed nothing, the position was out of range, or a
    /// seek asked for the beginning or the end: the strategy decides.
    NeedReset(AutoOffsetReset),
    /// `ListOffsets` for the strategy is in flight.
    Resetting(AutoOffsetReset),
    /// The position is known.
    Ready,
}

/// One assigned partition.
struct PartitionState {
    state: PositionState,
    /// The offset of the next record `poll` returns.
    position: i64,
    /// The offset the next fetch asks for: the position plus what is
    /// buffered.
    next_fetch: i64,
    buffered: VecDeque<ConsumedRecord>,
    high_watermark: i64,
    committed: Option<i64>,
    /// The leader epoch of the position: of the last record polled, or the
    /// one `ListOffsets` or `OffsetFetch` returned. It goes into the commit.
    leader_epoch: i32,
    fetch_in_flight: bool,
    /// The offset the fetch in flight asked for. Its answer counts only
    /// while the partition still fetches from there.
    fetch_offset: Option<i64>,
    /// KIP-848: the partition leaves with the reconciliation under way, so it
    /// neither fetches nor hands out records, as Kafka's
    /// `markPendingRevocationToPauseFetching` does.
    pending_revocation: bool,
    retry_at: Millis,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            state: PositionState::Init,
            position: 0,
            next_fetch: 0,
            buffered: VecDeque::new(),
            high_watermark: -1,
            committed: None,
            pending_revocation: false,
            leader_epoch: -1,
            fetch_in_flight: false,
            fetch_offset: None,
            retry_at: 0,
        }
    }

    fn lag(&self) -> Option<i64> {
        (self.high_watermark >= 0 && self.state == PositionState::Ready)
            .then(|| (self.high_watermark - self.position).max(0))
    }

    /// Kafka's `seekUnvalidated` with no leader epoch: the position is
    /// `offset` at once, and what was fetched for the old one is dropped.
    fn seek(&mut self, offset: i64) {
        self.state = PositionState::Ready;
        self.position = offset;
        self.next_fetch = offset;
        self.leader_epoch = -1;
        self.buffered.clear();
        self.retry_at = 0;
    }

    /// Kafka's `requestOffsetReset`: the position is gone until a
    /// `ListOffsets` for `strategy` answers.
    fn reset(&mut self, strategy: AutoOffsetReset) {
        self.state = PositionState::NeedReset(strategy);
        self.buffered.clear();
        self.retry_at = 0;
    }
}

/// KIP-848: an assignment the member reconciles once the commit before it
/// completed. Kafka's `AbstractMembershipManager.maybeReconcile` commits the
/// consumed positions first when auto-commit is on, then revokes and assigns.
struct Reconciling {
    target: BTreeSet<(String, i32)>,
    commit_sent: bool,
}

/// The leader of a classic group waits for the partition counts of every
/// subscribed topic before it assigns.
struct PendingLeader {
    generation: i32,
    members: Vec<(String, Vec<String>)>,
}

/// The fields the last KIP-848 heartbeat carried; a field goes out again
/// only when it changed. Kafka's `HeartbeatState.SentFields`.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
struct SentFields {
    rebalance_timeout_ms: Option<i32>,
    subscribed_topic_names: Option<Vec<String>>,
    topic_partitions: Option<BTreeMap<[u8; 16], Vec<i32>>>,
}

/// An `OffsetCommit` capped at the last version that names topics.
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

/// The consumer. See the module documentation.
pub struct Consumer {
    client: KafkaClient,
    config: ConsumerConfig,
    subscription: Vec<String>,
    state: MemberState,
    member_id: String,
    /// The classic generation, or the KIP-848 member epoch.
    generation: i32,
    /// When the next join attempt, or the next heartbeat after an error,
    /// may go out.
    rejoin_at: Millis,
    join: Option<RequestId>,
    sync: Option<RequestId>,
    heartbeat: Option<RequestId>,
    leave: Option<RequestId>,
    commit: Option<PendingCommit>,
    offset_fetch: Option<(RequestId, Partitions)>,
    list_offsets: BTreeMap<RequestId, Resets>,
    fetches: BTreeMap<RequestId, (i32, Partitions)>,
    fetch_brokers: BTreeSet<i32>,
    next_heartbeat_at: Millis,
    heartbeat_interval_ms: Millis,
    /// When a poll commits next: Kafka's auto-commit timer, which starts
    /// when the consumer first sees the clock.
    next_commit_at: Option<Millis>,
    pending_leader: Option<PendingLeader>,
    /// The partitions came from [`Consumer::assign`], outside any group:
    /// Kafka's `USER_ASSIGNED` subscription.
    manual: bool,
    assigned: BTreeMap<(String, i32), PartitionState>,
    sent_fields: SentFields,
    /// KIP-848: the last assignment the coordinator sent, reconciled again
    /// when metadata names its topics.
    target: Option<Vec<AssignedPartitions>>,
    /// KIP-848: a heartbeat must go out now to acknowledge the assignment.
    ack_pending: bool,
    /// KIP-848: the reconciliation waiting for its commit.
    reconciling: Option<Reconciling>,
    poll_cursor: usize,
    metrics: ConsumerMetrics,
    closed: bool,
}

impl Consumer {
    /// A consumer over `client`.
    #[must_use]
    pub fn new(client: KafkaClient, config: ConsumerConfig) -> Self {
        Self {
            heartbeat_interval_ms: config.heartbeat_interval_ms,
            client,
            config,
            subscription: Vec::new(),
            state: MemberState::Unsubscribed,
            member_id: String::new(),
            generation: -1,
            rejoin_at: 0,
            join: None,
            sync: None,
            heartbeat: None,
            leave: None,
            commit: None,
            offset_fetch: None,
            list_offsets: BTreeMap::new(),
            fetches: BTreeMap::new(),
            fetch_brokers: BTreeSet::new(),
            next_heartbeat_at: 0,
            next_commit_at: None,
            pending_leader: None,
            manual: false,
            assigned: BTreeMap::new(),
            sent_fields: SentFields::default(),
            target: None,
            ack_pending: false,
            reconciling: None,
            poll_cursor: 0,
            metrics: ConsumerMetrics::default(),
            closed: false,
        }
    }

    #[must_use]
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut KafkaClient {
        &mut self.client
    }

    #[must_use]
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    #[must_use]
    pub fn metrics(&self) -> &ConsumerMetrics {
        &self.metrics
    }

    #[must_use]
    pub fn state(&self) -> MemberState {
        self.state
    }

    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The classic generation, or the KIP-848 member epoch.
    #[must_use]
    pub fn generation(&self) -> i32 {
        self.generation
    }

    #[must_use]
    pub fn subscription(&self) -> &[String] {
        &self.subscription
    }

    /// The assigned partitions, sorted.
    #[must_use]
    pub fn assignment(&self) -> Vec<(String, i32)> {
        self.assigned.keys().cloned().collect()
    }

    /// The position of an assigned partition, once known.
    #[must_use]
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        self.assigned
            .get(&(topic.to_string(), partition))
            .filter(|p| p.state == PositionState::Ready)
            .map(|p| p.position)
    }

    /// The last committed offset of an assigned partition, once known.
    #[must_use]
    pub fn committed(&self, topic: &str, partition: i32) -> Option<i64> {
        self.assigned
            .get(&(topic.to_string(), partition))
            .and_then(|p| p.committed)
    }

    /// The lag of every assigned partition with a known high watermark.
    #[must_use]
    pub fn lag(&self) -> BTreeMap<(String, i32), i64> {
        self.assigned
            .iter()
            .filter_map(|(key, p)| p.lag().map(|lag| (key.clone(), lag)))
            .collect()
    }

    /// Records fetched and not yet polled.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.assigned.values().map(|p| p.buffered.len()).sum()
    }

    /// The high watermark the last fetch of an assigned partition reported:
    /// the end of the partition as the consumer sees it.
    #[must_use]
    pub fn high_watermark(&self, topic: &str, partition: i32) -> Option<i64> {
        self.assigned
            .get(&(topic.to_string(), partition))
            .map(|p| p.high_watermark)
            .filter(|hwm| *hwm >= 0)
    }

    /// Whether the partitions came from [`Consumer::assign`] rather than
    /// from a group.
    #[must_use]
    pub fn is_manually_assigned(&self) -> bool {
        self.manual
    }

    /// Subscribe to `topics` and join the group at the next tick. A change
    /// of subscription joins again.
    ///
    /// Kafka refuses `subscribe` on a consumer with a manual assignment
    /// until it unsubscribes; this `subscribe` has no error to give, so it
    /// drops the manual assignment and subscribes.
    pub fn subscribe(&mut self, topics: &[&str]) {
        let mut topics: Vec<String> = topics.iter().map(|t| (*t).to_string()).collect();
        topics.sort();
        topics.dedup();
        self.client.add_topics(topics.iter().map(String::as_str));
        self.subscription = topics;
        self.manual = false;
        self.assigned.clear();
        self.sent_fields = SentFields::default();
        self.state = MemberState::Joining;
        self.generation = match self.config.group_protocol {
            GroupProtocol::Classic => -1,
            GroupProtocol::Consumer => JOIN_GROUP_MEMBER_EPOCH,
        };
        self.rejoin_at = 0;
    }

    /// Take `partitions` by hand, outside any group: Kafka's
    /// `KafkaConsumer.assign`. A partition that stays keeps its position;
    /// the others lose what was fetched for them. An empty list gives up
    /// every partition, as Kafka's `assign` of an empty list unsubscribes.
    /// With auto-commit on, a commit due by the interval goes out first, as
    /// Kafka commits before the assignment changes.
    ///
    /// # Errors
    /// The checks run in Kafka's order: [`ConsumerError::Closed`] after
    /// [`Consumer::close`], [`ConsumerError::EmptyTopic`] for a topic name
    /// that Java's `trim` leaves empty, and [`ConsumerError::Subscribed`]
    /// when the consumer subscribed, after the auto-commit that is due. The
    /// assignment stays as it was. An empty list on a subscribed consumer is
    /// refused as well: Kafka's `unsubscribe` would take the member out of
    /// its group, and this consumer leaves a group only when it closes.
    pub fn assign(
        &mut self,
        ctx: &mut Ctx<'_>,
        partitions: &[(&str, i32)],
    ) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::Closed);
        }
        let subscribed = !self.manual && self.state != MemberState::Unsubscribed;
        if partitions.is_empty() {
            if subscribed {
                return Err(ConsumerError::Subscribed);
            }
            self.manual = false;
            self.assigned.clear();
            return Ok(());
        }
        // Kafka's `Utils.isBlank`: Java's `trim` drops every character up to
        // U+0020.
        if partitions
            .iter()
            .any(|(topic, _)| topic.chars().all(|c| c <= ' '))
        {
            return Err(ConsumerError::EmptyTopic);
        }
        self.maybe_auto_commit(ctx);
        if subscribed {
            return Err(ConsumerError::Subscribed);
        }
        let target: BTreeSet<(String, i32)> = partitions
            .iter()
            .map(|(topic, partition)| ((*topic).to_string(), *partition))
            .collect();
        self.assigned.retain(|key, _| target.contains(key));
        for key in target {
            self.assigned.entry(key).or_insert_with(PartitionState::new);
        }
        self.manual = true;
        let topics: BTreeSet<&str> = partitions.iter().map(|(topic, _)| *topic).collect();
        self.client.add_topics(topics);
        Ok(())
    }

    /// Fetch `partition` of `topic` from `offset` on: Kafka's
    /// `KafkaConsumer.seek`. The records fetched for the old position are
    /// dropped. Nothing is sent: the next fetch asks for `offset`.
    ///
    /// # Errors
    /// [`ConsumerError::NegativeOffset`] for an offset below 0,
    /// [`ConsumerError::Closed`] after [`Consumer::close`], and
    /// [`ConsumerError::NotAssigned`] for a partition the consumer does not
    /// hold.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<(), ConsumerError> {
        if offset < 0 {
            return Err(ConsumerError::NegativeOffset);
        }
        if self.closed {
            return Err(ConsumerError::Closed);
        }
        self.assigned
            .get_mut(&(topic.to_string(), partition))
            .ok_or_else(|| ConsumerError::NotAssigned {
                topic: topic.to_string(),
                partition,
            })?
            .seek(offset);
        Ok(())
    }

    /// Move `partitions`, or every assigned partition when the list is
    /// empty, to their first offset: Kafka's `seekToBeginning`. The offset
    /// is looked up with `ListOffsets` at the next tick, lazily, as in
    /// Kafka.
    ///
    /// # Errors
    /// [`ConsumerError::Closed`] after [`Consumer::close`], and
    /// [`ConsumerError::NotAssigned`] at the first partition the consumer
    /// does not hold; as in Kafka, the partitions before it are moved.
    pub fn seek_to_beginning(&mut self, partitions: &[(&str, i32)]) -> Result<(), ConsumerError> {
        self.seek_to(partitions, AutoOffsetReset::Earliest)
    }

    /// Move `partitions`, or every assigned partition when the list is
    /// empty, to the end of their log: Kafka's `seekToEnd`. The offset is
    /// looked up with `ListOffsets` at the next tick.
    ///
    /// # Errors
    /// As [`Consumer::seek_to_beginning`].
    pub fn seek_to_end(&mut self, partitions: &[(&str, i32)]) -> Result<(), ConsumerError> {
        self.seek_to(partitions, AutoOffsetReset::Latest)
    }

    fn seek_to(
        &mut self,
        partitions: &[(&str, i32)],
        strategy: AutoOffsetReset,
    ) -> Result<(), ConsumerError> {
        if self.closed {
            return Err(ConsumerError::Closed);
        }
        let keys: Vec<(String, i32)> = if partitions.is_empty() {
            self.assignment()
        } else {
            partitions
                .iter()
                .map(|(topic, partition)| ((*topic).to_string(), *partition))
                .collect()
        };
        for (topic, partition) in keys {
            let key = (topic, partition);
            let Some(state) = self.assigned.get_mut(&key) else {
                let (topic, partition) = key;
                return Err(ConsumerError::NotAssigned { topic, partition });
            };
            state.reset(strategy);
        }
        Ok(())
    }

    /// Take up to `max` buffered records, and never more than
    /// `max_poll_records`, advancing the positions the next commit sends.
    /// The partitions take turns, and a partition the member is giving up
    /// hands out none. It never commits; [`Consumer::poll_at`] is Kafka's
    /// `poll`, with its auto-commit.
    pub fn poll(&mut self, max: usize) -> Vec<ConsumedRecord> {
        let max = max.min(self.config.max_poll_records);
        let keys: Vec<(String, i32)> = self
            .assigned
            .iter()
            .filter(|(_, p)| !p.pending_revocation)
            .map(|(key, _)| key.clone())
            .collect();
        let mut out = Vec::new();
        if keys.is_empty() || max == 0 {
            return out;
        }
        let n = keys.len();
        for i in 0..n {
            let index = (self.poll_cursor + i) % n;
            let Some(state) = self.assigned.get_mut(&keys[index]) else {
                continue;
            };
            while out.len() < max {
                let Some(record) = state.buffered.pop_front() else {
                    break;
                };
                state.position = record.offset + 1;
                state.leader_epoch = record.leader_epoch;
                out.push(record);
            }
            if out.len() >= max {
                self.poll_cursor = (index + 1) % n;
                break;
            }
        }
        self.metrics.polled += u64::try_from(out.len()).unwrap_or(u64::MAX);
        out
    }

    /// Kafka's `KafkaConsumer.poll`: commit the consumed positions when
    /// `auto.commit.interval.ms` passed since the last auto-commit, take up
    /// to `max` records as [`Consumer::poll`] does, and send the fetches of
    /// the partitions whose records ran out, as Kafka sends the next fetches
    /// before `poll` returns. Kafka 4.3 auto-commits on the interval only
    /// here, with either group protocol.
    pub fn poll_at(&mut self, ctx: &mut Ctx<'_>, max: usize) -> Vec<ConsumedRecord> {
        self.maybe_auto_commit(ctx);
        let records = self.poll(max);
        if self.fetching() {
            self.maybe_fetch(ctx);
        }
        records
    }

    /// When a [`Consumer::poll_at`] would next commit on the interval:
    /// Kafka's `ConsumerCoordinator.timeToNextPoll`, which bounds how long a
    /// poll loop may wait. `None` while auto-commit is off, the consumer has
    /// no group, a commit is in flight (the next is due once it answers), or
    /// the consumer is closed. A tick does not commit, so a node that wants
    /// the commit polls at this time.
    #[must_use]
    pub fn next_auto_commit(&self) -> Option<Millis> {
        let possible = self.config.enable_auto_commit
            && self.has_group()
            && self.commit.is_none()
            && !self.closed;
        if possible { self.next_commit_at } else { None }
    }

    /// A frame arrived for this consumer's client.
    pub fn on_frame(
        &mut self,
        ctx: &mut Ctx<'_>,
        frame: Frame,
    ) -> (Vec<ConsumerEvent>, Option<Millis>) {
        let mut events = Vec::new();
        let client_events = self.client.on_frame(ctx, frame);
        self.handle_client_events(ctx, client_events, &mut events);
        self.step(ctx, &mut events);
        (events, self.next_deadline(ctx.now()))
    }

    /// Drive the membership, the positions, the fetches and the commits.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<ConsumerEvent>, Option<Millis>) {
        let mut events = Vec::new();
        let (client_events, _) = self.client.on_tick(ctx);
        self.handle_client_events(ctx, client_events, &mut events);
        self.step(ctx, &mut events);
        (events, self.next_deadline(ctx.now()))
    }

    /// Commit the current positions now, whatever `enable.auto.commit` says.
    /// A consumer without a group commits nothing.
    pub fn commit(&mut self, ctx: &mut Ctx<'_>) {
        if self.commit.is_none() {
            self.commit_positions(ctx, false);
        }
    }

    /// Commit, leave the group, and close the client. A classic static
    /// member sends no `LeaveGroup`, and a KIP-848 static member leaves with
    /// epoch `-2`, so the group keeps its assignment for the member that
    /// comes back with its instance id.
    pub fn close(&mut self, ctx: &mut Ctx<'_>) -> Vec<ConsumerEvent> {
        let mut events = Vec::new();
        if self.closed {
            return events;
        }
        self.closed = true;
        if self.config.enable_auto_commit && self.commit.is_none() {
            self.commit_positions(ctx, false);
        }
        let dynamic = self.config.group_instance_id.is_none();
        let leaves = match self.config.group_protocol {
            GroupProtocol::Classic => dynamic,
            GroupProtocol::Consumer => true,
        };
        if leaves
            && !self.member_id.is_empty()
            && matches!(self.state, MemberState::Stable | MemberState::Syncing)
        {
            self.send_leave(ctx);
        }
        let partitions = self.assignment();
        if !self.manual && !partitions.is_empty() {
            events.push(ConsumerEvent::Revoked { partitions });
        }
        self.assigned.clear();
        self.state = MemberState::Left;
        events
    }

    fn send_leave(&mut self, ctx: &mut Ctx<'_>) {
        match self.config.group_protocol {
            GroupProtocol::Classic => {
                let request = LeaveGroupRequest {
                    group_id: self.config.group_id.clone(),
                    member_id: self.member_id.clone(),
                    members: vec![MemberIdentity {
                        member_id: self.member_id.clone(),
                        group_instance_id: None,
                        reason: Some("the consumer is being closed".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                self.leave = Some(self.client.send(ctx, self.coordinator(), request));
            }
            GroupProtocol::Consumer => {
                // Kafka's `leaveGroup` unsubscribes and gives the assignment
                // up before the heartbeat is built, so the heartbeat reports
                // an empty subscription, and no partitions when it reported
                // some.
                let owned_reported = self
                    .sent_fields
                    .topic_partitions
                    .as_ref()
                    .is_some_and(|owned| !owned.is_empty());
                let subscribed_reported = self
                    .sent_fields
                    .subscribed_topic_names
                    .as_ref()
                    .is_none_or(|names| !names.is_empty());
                let member_epoch = if self.config.group_instance_id.is_some() {
                    LEAVE_GROUP_STATIC_MEMBER_EPOCH
                } else {
                    LEAVE_GROUP_MEMBER_EPOCH
                };
                let request = ConsumerGroupHeartbeatRequest {
                    group_id: self.config.group_id.clone(),
                    member_id: self.member_id.clone(),
                    member_epoch,
                    instance_id: self.config.group_instance_id.clone(),
                    rebalance_timeout_ms: -1,
                    subscribed_topic_names: subscribed_reported.then(Vec::new),
                    topic_partitions: owned_reported.then(Vec::new),
                    ..Default::default()
                };
                self.leave = Some(self.client.send(ctx, self.coordinator(), request));
            }
        }
    }

    /// Whether the consumer belongs to a group: Kafka's `group.id` is set.
    fn has_group(&self) -> bool {
        !self.config.group_id.is_empty()
    }

    /// Whether the consumer holds partitions it fetches: a member of its
    /// group, or a consumer with a manual assignment.
    fn fetching(&self) -> bool {
        !self.closed
            && (self.manual
                || matches!(
                    self.state,
                    MemberState::Joining | MemberState::Syncing | MemberState::Stable
                ))
    }

    /// Start the auto-commit timer the first time the consumer sees the
    /// clock, as Kafka starts it when the consumer is built.
    fn start_commit_timer(&mut self, now: Millis) {
        if self.next_commit_at.is_none() {
            self.next_commit_at = Some(now + self.config.auto_commit_interval_ms);
        }
    }

    /// Commit the consumed positions when the auto-commit interval passed:
    /// Kafka's `maybeAutoCommitOffsetsAsync`. The timer starts over whether
    /// or not there was something to commit, and a commit in flight holds
    /// the next one back until it answers.
    fn maybe_auto_commit(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        self.start_commit_timer(now);
        let due = self.next_auto_commit().is_some_and(|at| now >= at);
        if !due {
            return;
        }
        self.next_commit_at = Some(now + self.config.auto_commit_interval_ms);
        self.commit_positions(ctx, true);
    }

    fn coordinator(&self) -> Target {
        Target::Coordinator {
            key_type: CoordinatorType::Group,
            key: self.config.group_id.clone(),
        }
    }

    /// The next time the consumer needs a tick: `None` when only an answer
    /// or new metadata can move it on, which arrive as frames. An
    /// auto-commit is a poll's work, not a tick's; see
    /// [`Consumer::next_auto_commit`].
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let client = self.client.next_deadline(now);
        if !self.fetching() {
            return client.map(|at| at.max(now));
        }
        // Only what a tick can act on counts. A request in flight wakes the
        // member with its answer, and a partition without a leader waits for
        // the metadata the client refreshes.
        let membership = match (self.state, self.config.group_protocol) {
            _ if self.manual => None,
            (MemberState::Joining, GroupProtocol::Classic)
                if self.join.is_none() && self.sync.is_none() =>
            {
                Some(self.rejoin_at)
            }
            (MemberState::Joining, GroupProtocol::Consumer) if self.heartbeat.is_none() => {
                Some(self.rejoin_at)
            }
            (MemberState::Stable, _) if self.heartbeat.is_none() => Some(if self.ack_pending {
                now
            } else {
                self.next_heartbeat_at.max(self.rejoin_at)
            }),
            _ => None,
        };
        let partitions = self.assigned.iter().filter_map(|((topic, partition), p)| {
            let leader = self.client.metadata().leader(topic, *partition);
            let due = match p.state {
                PositionState::Init => {
                    self.offset_fetch.is_none()
                        && (self.manual || self.state == MemberState::Stable)
                }
                PositionState::NeedReset(_) => leader.is_some(),
                PositionState::Ready => {
                    !p.fetch_in_flight
                        && !p.pending_revocation
                        && p.buffered.is_empty()
                        && leader.is_some_and(|l| !self.fetch_brokers.contains(&l))
                }
                PositionState::FetchingCommitted | PositionState::Resetting(_) => false,
            };
            due.then_some(p.retry_at)
        });
        client
            .into_iter()
            .chain(membership)
            .chain(partitions)
            .min()
            .map(|at| at.max(now))
    }

    // ---- driving ----------------------------------------------------------------

    fn step(&mut self, ctx: &mut Ctx<'_>, events: &mut Vec<ConsumerEvent>) {
        self.start_commit_timer(ctx.now());
        if !self.fetching() {
            return;
        }
        if !self.manual {
            match self.config.group_protocol {
                GroupProtocol::Classic => {
                    self.maybe_join(ctx);
                    self.maybe_heartbeat(ctx);
                }
                GroupProtocol::Consumer => self.maybe_group_heartbeat(ctx),
            }
        }
        self.init_positions(ctx);
        self.maybe_fetch(ctx);
        let _ = events;
    }

    fn handle_client_events(
        &mut self,
        ctx: &mut Ctx<'_>,
        client_events: Vec<ClientEvent>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        for event in client_events {
            match event {
                ClientEvent::MetadataUpdated => {
                    self.try_leader_assignment(ctx);
                    if let Some(target) = self.target.take() {
                        self.reconcile(ctx, target, events);
                    }
                }
                ClientEvent::Response { id, result } => self.on_response(ctx, id, result, events),
            }
        }
    }

    fn on_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: RequestId,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        if self.join == Some(id) {
            self.join = None;
            self.on_join(ctx, result, events);
        } else if self.sync == Some(id) {
            self.sync = None;
            self.on_sync(ctx, result, events);
        } else if self.heartbeat == Some(id) {
            self.heartbeat = None;
            match self.config.group_protocol {
                GroupProtocol::Classic => self.on_heartbeat(ctx, result, events),
                GroupProtocol::Consumer => self.on_group_heartbeat(ctx, result, events),
            }
        } else if self.leave == Some(id) {
            self.leave = None;
            // The member is gone whatever the answer says.
            drop(result);
        } else if self.commit.as_ref().is_some_and(|c| c.id == id) {
            if let Some(commit) = self.commit.take() {
                self.on_commit(ctx, &commit, result, events);
            }
        } else if self.offset_fetch.as_ref().is_some_and(|(o, _)| *o == id) {
            if let Some((_, partitions)) = self.offset_fetch.take() {
                self.on_offset_fetch(ctx, &partitions, result, events);
            }
        } else if let Some(resets) = self.list_offsets.remove(&id) {
            self.on_list_offsets(ctx, &resets, result, events);
        } else if let Some((broker, partitions)) = self.fetches.remove(&id) {
            self.fetch_brokers.remove(&broker);
            self.on_fetch(ctx, &partitions, result, events);
        }
    }

    /// Give up every partition before a rebalance (the eager protocol) and
    /// join again.
    fn request_rejoin(
        &mut self,
        ctx: &mut Ctx<'_>,
        events: &mut Vec<ConsumerEvent>,
        keep_member_id: bool,
    ) {
        if !keep_member_id {
            self.member_id.clear();
        }
        // Kafka's `onJoinPrepare` auto-commits before the member joins again.
        if self.config.enable_auto_commit && self.commit.is_none() {
            self.commit_positions(ctx, true);
        }
        let partitions = self.assignment();
        if !partitions.is_empty() {
            events.push(ConsumerEvent::Revoked { partitions });
        }
        self.assigned.clear();
        self.fetch_brokers.clear();
        self.pending_leader = None;
        self.state = MemberState::Joining;
        self.rejoin_at = ctx.now();
        self.metrics.rebalances += 1;
    }

    fn backoff_after_error(&mut self, ctx: &mut Ctx<'_>) {
        self.rejoin_at = ctx.now() + self.config.retry_backoff_ms;
    }

    // ---- classic protocol -------------------------------------------------------

    fn maybe_join(&mut self, ctx: &mut Ctx<'_>) {
        if self.state != MemberState::Joining
            || self.join.is_some()
            || self.sync.is_some()
            || ctx.now() < self.rejoin_at
        {
            return;
        }
        let owned = self.assignment();
        let metadata = encode_subscription(
            &self.subscription,
            &owned,
            self.generation,
            self.config.rack_id.as_deref(),
        );
        let request = JoinGroupRequest {
            group_id: self.config.group_id.clone(),
            session_timeout_ms: millis_i32(self.config.session_timeout_ms),
            rebalance_timeout_ms: millis_i32(self.config.rebalance_timeout_ms),
            member_id: self.member_id.clone(),
            group_instance_id: self.config.group_instance_id.clone(),
            protocol_type: CONSUMER_PROTOCOL_TYPE.to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: RANGE_PROTOCOL.to_string(),
                metadata,
                ..Default::default()
            }],
            reason: Some(String::new()),
            ..Default::default()
        };
        self.join = Some(self.client.send(ctx, self.coordinator(), request));
    }

    fn on_join(
        &mut self,
        ctx: &mut Ctx<'_>,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let Some(response) = self.expect::<JoinGroupResponse>(ctx, "JoinGroup", result, events)
        else {
            return;
        };
        match response.error_code {
            codes::NONE => {
                self.generation = response.generation_id;
                self.member_id.clone_from(&response.member_id);
                events.push(ConsumerEvent::Joined {
                    member_id: self.member_id.clone(),
                    generation: self.generation,
                });
                self.state = MemberState::Syncing;
                if response.leader == response.member_id {
                    let members: Vec<(String, Vec<String>)> = response
                        .members
                        .iter()
                        .map(|m| {
                            (
                                m.member_id.clone(),
                                decode_subscription(&m.metadata)
                                    .map(|s| s.topics)
                                    .unwrap_or_default(),
                            )
                        })
                        .collect();
                    if response.skip_assignment {
                        // KIP-814: a static leader that came back keeps the
                        // assignment the group has; it follows the group's
                        // topics and syncs with no assignment, as Kafka's
                        // `onLeaderElected` does.
                        self.client.add_topics(
                            members
                                .iter()
                                .flat_map(|(_, topics)| topics.iter().map(String::as_str)),
                        );
                        self.send_sync(ctx, Vec::new());
                        return;
                    }
                    self.pending_leader = Some(PendingLeader {
                        generation: self.generation,
                        members,
                    });
                    self.try_leader_assignment(ctx);
                } else {
                    self.send_sync(ctx, Vec::new());
                }
            }
            MEMBER_ID_REQUIRED => {
                self.member_id.clone_from(&response.member_id);
                self.rejoin_at = ctx.now();
            }
            codes::UNKNOWN_MEMBER_ID => {
                self.member_id.clear();
                self.backoff_after_error(ctx);
            }
            codes::REBALANCE_IN_PROGRESS => self.rejoin_at = ctx.now(),
            code if retry::class(code).is_retriable() => {
                self.client.note_error(code, &self.coordinator());
                events.push(ConsumerEvent::Error {
                    api: "JoinGroup",
                    code,
                });
                self.backoff_after_error(ctx);
            }
            code => {
                events.push(ConsumerEvent::Error {
                    api: "JoinGroup",
                    code,
                });
                self.state = MemberState::Failed(code);
            }
        }
    }

    /// The leader assigns once metadata has the partition count of every
    /// topic any member subscribes to.
    fn try_leader_assignment(&mut self, ctx: &mut Ctx<'_>) {
        let Some(pending) = &self.pending_leader else {
            return;
        };
        if pending.generation != self.generation || self.state != MemberState::Syncing {
            self.pending_leader = None;
            return;
        }
        let topics: BTreeSet<&str> = pending
            .members
            .iter()
            .flat_map(|(_, topics)| topics.iter().map(String::as_str))
            .collect();
        let mut counts = BTreeMap::new();
        let mut missing = Vec::new();
        for topic in topics {
            match self.client.metadata().partition_count(topic) {
                Some(count) => {
                    counts.insert(topic.to_string(), count);
                }
                None if self.client.metadata().unknown_topics.contains(topic) => {
                    counts.insert(topic.to_string(), 0);
                }
                None => missing.push(topic.to_string()),
            }
        }
        if !missing.is_empty() {
            self.client.add_topics(missing.iter().map(String::as_str));
            self.client.request_metadata_refresh();
            return;
        }
        let Some(pending) = self.pending_leader.take() else {
            return;
        };
        let assignment = range_assign(&pending.members, &counts);
        let assignments = assignment
            .into_iter()
            .map(|(member_id, partitions)| SyncGroupRequestAssignment {
                member_id,
                assignment: encode_assignment(&partitions),
                ..Default::default()
            })
            .collect();
        self.send_sync(ctx, assignments);
    }

    fn send_sync(&mut self, ctx: &mut Ctx<'_>, assignments: Vec<SyncGroupRequestAssignment>) {
        let request = SyncGroupRequest {
            group_id: self.config.group_id.clone(),
            generation_id: self.generation,
            member_id: self.member_id.clone(),
            group_instance_id: self.config.group_instance_id.clone(),
            protocol_type: Some(CONSUMER_PROTOCOL_TYPE.to_string()),
            protocol_name: Some(RANGE_PROTOCOL.to_string()),
            assignments,
            ..Default::default()
        };
        self.sync = Some(self.client.send(ctx, self.coordinator(), request));
    }

    fn on_sync(
        &mut self,
        ctx: &mut Ctx<'_>,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let Some(response) = self.expect::<SyncGroupResponse>(ctx, "SyncGroup", result, events)
        else {
            return;
        };
        match response.error_code {
            codes::NONE => {
                let partitions = decode_assignment(&response.assignment).unwrap_or_default();
                self.install_assignment(partitions, events);
                self.state = MemberState::Stable;
                let now = ctx.now();
                self.next_heartbeat_at = now + self.config.heartbeat_interval_ms;
                // Kafka's `onJoinComplete` starts the auto-commit interval
                // over with the new assignment.
                self.next_commit_at = Some(now + self.config.auto_commit_interval_ms);
            }
            codes::REBALANCE_IN_PROGRESS | codes::ILLEGAL_GENERATION => {
                self.request_rejoin(ctx, events, true);
            }
            codes::UNKNOWN_MEMBER_ID => self.request_rejoin(ctx, events, false),
            code if retry::class(code).is_retriable() => {
                self.client.note_error(code, &self.coordinator());
                events.push(ConsumerEvent::Error {
                    api: "SyncGroup",
                    code,
                });
                self.state = MemberState::Joining;
                self.backoff_after_error(ctx);
            }
            code => {
                events.push(ConsumerEvent::Error {
                    api: "SyncGroup",
                    code,
                });
                self.state = MemberState::Failed(code);
            }
        }
    }

    /// Replace the assignment: partitions that stay keep their position.
    fn install_assignment(
        &mut self,
        partitions: Vec<(String, i32)>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let target: BTreeSet<(String, i32)> = partitions.into_iter().collect();
        let revoked: Vec<(String, i32)> = self
            .assigned
            .keys()
            .filter(|key| !target.contains(*key))
            .cloned()
            .collect();
        let added: Vec<(String, i32)> = target
            .iter()
            .filter(|key| !self.assigned.contains_key(*key))
            .cloned()
            .collect();
        for key in &revoked {
            self.assigned.remove(key);
        }
        for key in &added {
            self.assigned.insert(key.clone(), PartitionState::new());
        }
        if !revoked.is_empty() {
            events.push(ConsumerEvent::Revoked {
                partitions: revoked,
            });
        }
        if !added.is_empty() {
            events.push(ConsumerEvent::Assigned { partitions: added });
        }
    }

    fn maybe_heartbeat(&mut self, ctx: &mut Ctx<'_>) {
        if self.state != MemberState::Stable
            || self.heartbeat.is_some()
            || ctx.now() < self.next_heartbeat_at.max(self.rejoin_at)
        {
            return;
        }
        let request = HeartbeatRequest {
            group_id: self.config.group_id.clone(),
            generation_id: self.generation,
            member_id: self.member_id.clone(),
            group_instance_id: self.config.group_instance_id.clone(),
            ..Default::default()
        };
        self.heartbeat = Some(self.client.send(ctx, self.coordinator(), request));
    }

    fn on_heartbeat(
        &mut self,
        ctx: &mut Ctx<'_>,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        self.next_heartbeat_at = ctx.now() + self.config.heartbeat_interval_ms;
        let Some(response) = self.expect::<HeartbeatResponse>(ctx, "Heartbeat", result, events)
        else {
            return;
        };
        match response.error_code {
            codes::NONE => {}
            codes::REBALANCE_IN_PROGRESS | codes::ILLEGAL_GENERATION => {
                self.request_rejoin(ctx, events, true);
            }
            codes::UNKNOWN_MEMBER_ID => self.request_rejoin(ctx, events, false),
            codes::FENCED_INSTANCE_ID => {
                // A new process joined with the instance id: Kafka's
                // consumer fails with `FencedInstanceIdException`.
                events.push(ConsumerEvent::Error {
                    api: "Heartbeat",
                    code: codes::FENCED_INSTANCE_ID,
                });
                self.state = MemberState::Failed(codes::FENCED_INSTANCE_ID);
            }
            code => {
                self.client.note_error(code, &self.coordinator());
                events.push(ConsumerEvent::Error {
                    api: "Heartbeat",
                    code,
                });
            }
        }
    }

    // ---- KIP-848 ----------------------------------------------------------------

    fn maybe_group_heartbeat(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self.heartbeat.is_some() || now < self.rejoin_at {
            return;
        }
        let joining = self.state == MemberState::Joining;
        if !(joining || self.ack_pending || now >= self.next_heartbeat_at) {
            return;
        }
        if joining && self.member_id.is_empty() {
            self.member_id = member_id_from(ctx);
        }
        let request = self.build_group_heartbeat(joining);
        self.ack_pending = false;
        self.heartbeat = Some(self.client.send(ctx, self.coordinator(), request));
    }

    /// Kafka's `HeartbeatState.buildRequestData`: every field on a join, and
    /// afterwards only what changed since the last heartbeat.
    fn build_group_heartbeat(&mut self, joining: bool) -> ConsumerGroupHeartbeatRequest {
        if joining {
            self.sent_fields = SentFields::default();
        }
        let mut request = ConsumerGroupHeartbeatRequest {
            group_id: self.config.group_id.clone(),
            member_id: self.member_id.clone(),
            member_epoch: if joining {
                JOIN_GROUP_MEMBER_EPOCH
            } else {
                self.generation
            },
            // Every heartbeat names a static member's instance.
            instance_id: self.config.group_instance_id.clone(),
            rack_id: joining.then(|| self.config.rack_id.clone()).flatten(),
            rebalance_timeout_ms: -1,
            ..Default::default()
        };
        let rebalance_timeout = millis_i32(self.config.rebalance_timeout_ms);
        if self.sent_fields.rebalance_timeout_ms != Some(rebalance_timeout) {
            request.rebalance_timeout_ms = rebalance_timeout;
            self.sent_fields.rebalance_timeout_ms = Some(rebalance_timeout);
        }
        if self.sent_fields.subscribed_topic_names.as_ref() != Some(&self.subscription) {
            request.subscribed_topic_names = Some(self.subscription.clone());
            self.sent_fields.subscribed_topic_names = Some(self.subscription.clone());
        }
        let owned = self.owned_by_topic_id();
        if self.sent_fields.topic_partitions.as_ref() != Some(&owned) {
            request.topic_partitions = Some(
                owned
                    .iter()
                    .map(|(topic_id, partitions)| TopicPartitions {
                        topic_id: Uuid(*topic_id),
                        partitions: partitions.clone(),
                        ..Default::default()
                    })
                    .collect(),
            );
            self.sent_fields.topic_partitions = Some(owned);
        }
        request
    }

    fn owned_by_topic_id(&self) -> BTreeMap<[u8; 16], Vec<i32>> {
        let mut owned: BTreeMap<[u8; 16], Vec<i32>> = BTreeMap::new();
        for (topic, partition) in self.assigned.keys() {
            if let Some(id) = self.client.metadata().topic_id(topic) {
                owned.entry(id.0).or_default().push(*partition);
            }
        }
        owned
    }

    fn on_group_heartbeat(
        &mut self,
        ctx: &mut Ctx<'_>,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let now = ctx.now();
        let Ok(Some(response)) =
            result.map(super::Response::downcast::<ConsumerGroupHeartbeatResponse>)
        else {
            // A lost connection or a broker without the api: the next
            // heartbeat carries every field again, after the backoff.
            self.sent_fields = SentFields::default();
            self.client
                .invalidate_coordinator(CoordinatorType::Group, &self.config.group_id);
            self.backoff_after_error(ctx);
            return;
        };
        if response.heartbeat_interval_ms > 0 {
            self.heartbeat_interval_ms =
                Millis::try_from(response.heartbeat_interval_ms).unwrap_or(0);
        }
        self.next_heartbeat_at = now + self.heartbeat_interval_ms;
        match response.error_code {
            codes::NONE => {}
            codes::COORDINATOR_NOT_AVAILABLE | codes::NOT_COORDINATOR => {
                self.sent_fields = SentFields::default();
                self.client
                    .note_error(response.error_code, &self.coordinator());
                self.backoff_after_error(ctx);
                return;
            }
            codes::COORDINATOR_LOAD_IN_PROGRESS => {
                self.sent_fields = SentFields::default();
                self.backoff_after_error(ctx);
                return;
            }
            codes::FENCED_MEMBER_EPOCH | codes::UNKNOWN_MEMBER_ID => {
                let partitions = self.assignment();
                if !partitions.is_empty() {
                    events.push(ConsumerEvent::Lost { partitions });
                }
                self.assigned.clear();
                self.fetch_brokers.clear();
                self.target = None;
                self.generation = JOIN_GROUP_MEMBER_EPOCH;
                self.state = MemberState::Joining;
                self.sent_fields = SentFields::default();
                self.rejoin_at = now;
                self.metrics.rebalances += 1;
                return;
            }
            code => {
                events.push(ConsumerEvent::Error {
                    api: "ConsumerGroupHeartbeat",
                    code,
                });
                self.state = MemberState::Failed(code);
                return;
            }
        }
        if let Some(member_id) = response.member_id.filter(|id| !id.is_empty()) {
            self.member_id = member_id;
        }
        let joined = self.state == MemberState::Joining;
        self.generation = response.member_epoch;
        self.state = MemberState::Stable;
        if joined {
            events.push(ConsumerEvent::Joined {
                member_id: self.member_id.clone(),
                generation: self.generation,
            });
        }
        if let Some(assignment) = response.assignment {
            self.reconcile(ctx, assignment.topic_partitions, events);
        }
    }

    /// Start reconciling an assignment: Kafka's
    /// `AbstractMembershipManager.maybeReconcile`. An assignment that names a
    /// topic id the metadata does not know yet waits for a refresh. The
    /// partitions to give up stop fetching at once; the change itself waits
    /// for the commit of the consumed positions when auto-commit is on.
    fn reconcile(
        &mut self,
        ctx: &mut Ctx<'_>,
        assignment: Vec<AssignedPartitions>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let mut target = BTreeSet::new();
        for topic in &assignment {
            let Some(name) = self.client.metadata().topic_name(topic.topic_id) else {
                self.client.request_metadata_refresh();
                self.target = Some(assignment);
                return;
            };
            for partition in &topic.partitions {
                target.insert((name.to_string(), *partition));
            }
        }
        let current: BTreeSet<(String, i32)> = self.assigned.keys().cloned().collect();
        if target == current {
            self.reconciling = None;
            for p in self.assigned.values_mut() {
                p.pending_revocation = false;
            }
            return;
        }
        for (key, p) in &mut self.assigned {
            p.pending_revocation = !target.contains(key);
        }
        self.reconciling = Some(Reconciling {
            target,
            commit_sent: false,
        });
        self.advance_reconciliation(ctx, events);
    }

    /// Move the reconciliation on: send the commit before it, wait for that
    /// commit, then give up and take the partitions and acknowledge the
    /// assignment with the next heartbeat. A failed commit does not stop it,
    /// as in Kafka.
    fn advance_reconciliation(&mut self, ctx: &mut Ctx<'_>, events: &mut Vec<ConsumerEvent>) {
        let Some(reconciling) = &mut self.reconciling else {
            return;
        };
        if self.config.enable_auto_commit {
            if self.commit.is_some() {
                return;
            }
            if !reconciling.commit_sent {
                reconciling.commit_sent = true;
                if self.commit_positions(ctx, false) {
                    return;
                }
            }
        }
        let Some(Reconciling { target, .. }) = self.reconciling.take() else {
            return;
        };
        let revoked: Vec<(String, i32)> = self
            .assigned
            .keys()
            .filter(|key| !target.contains(*key))
            .cloned()
            .collect();
        let added: Vec<(String, i32)> = target
            .iter()
            .filter(|key| !self.assigned.contains_key(*key))
            .cloned()
            .collect();
        if !revoked.is_empty() {
            for key in &revoked {
                self.assigned.remove(key);
            }
            events.push(ConsumerEvent::Revoked {
                partitions: revoked,
            });
        }
        if !added.is_empty() {
            for key in &added {
                self.assigned.insert(key.clone(), PartitionState::new());
            }
            events.push(ConsumerEvent::Assigned { partitions: added });
        }
        self.metrics.rebalances += 1;
        self.ack_pending = true;
        // Kafka's `signalReconciliationCompleting` starts the auto-commit
        // interval over with the new assignment.
        self.next_commit_at = Some(ctx.now() + self.config.auto_commit_interval_ms);
    }

    // ---- commits ----------------------------------------------------------------

    /// Commit the position of every partition that has one, as Kafka's
    /// consumers commit `SubscriptionState.allConsumed()`. A member commits
    /// with its generation or epoch; a consumer with a manual assignment
    /// with generation `-1` and no member id, and a classic one without its
    /// instance id, as Kafka's `sendOffsetCommitRequest` does. `auto` marks
    /// an auto-commit. Returns whether a request went out: never without a
    /// group, nor for a member without a member id.
    fn commit_positions(&mut self, ctx: &mut Ctx<'_>, auto: bool) -> bool {
        if !self.has_group() || (!self.manual && self.member_id.is_empty()) {
            return false;
        }
        let offsets: Vec<(String, i32, i64, i32)> = self
            .assigned
            .iter()
            .filter(|(_, p)| p.state == PositionState::Ready)
            .map(|((topic, partition), p)| (topic.clone(), *partition, p.position, p.leader_epoch))
            .collect();
        if offsets.is_empty() {
            return false;
        }
        let mut topics: BTreeMap<String, Vec<OffsetCommitRequestPartition>> = BTreeMap::new();
        for (topic, partition, offset, epoch) in &offsets {
            topics
                .entry(topic.clone())
                .or_default()
                .push(OffsetCommitRequestPartition {
                    partition_index: *partition,
                    committed_offset: *offset,
                    committed_leader_epoch: *epoch,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                });
        }
        let (generation, member_id, group_instance_id) = match self.config.group_protocol {
            GroupProtocol::Classic if self.manual => (NO_GENERATION, String::new(), None),
            GroupProtocol::Consumer if self.manual => (
                NO_GENERATION,
                String::new(),
                self.config.group_instance_id.clone(),
            ),
            GroupProtocol::Classic | GroupProtocol::Consumer => (
                self.generation,
                self.member_id.clone(),
                self.config.group_instance_id.clone(),
            ),
        };
        let request = OffsetCommitByName(OffsetCommitRequest {
            group_id: self.config.group_id.clone(),
            generation_id_or_member_epoch: generation,
            member_id,
            group_instance_id,
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
        let id = self.client.send(ctx, self.coordinator(), request);
        self.commit = Some(PendingCommit {
            id,
            offsets: offsets
                .into_iter()
                .map(|(topic, partition, offset, _)| (topic, partition, offset))
                .collect(),
            auto,
        });
        self.metrics.commits += 1;
        true
    }

    /// A retriable failure of an auto-commit brings the next one forward to
    /// `retry.backoff.ms`, as Kafka's auto-commit callback resets its timer.
    fn back_off_auto_commit(&mut self, now: Millis, commit: &PendingCommit) {
        if commit.auto {
            self.next_commit_at = Some(now + self.config.retry_backoff_ms);
        }
    }

    fn on_commit(
        &mut self,
        ctx: &mut Ctx<'_>,
        commit: &PendingCommit,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let Some(response) =
            self.expect::<OffsetCommitResponse>(ctx, "OffsetCommit", result, events)
        else {
            self.back_off_auto_commit(ctx.now(), commit);
            self.advance_reconciliation(ctx, events);
            return;
        };
        let offsets = &commit.offsets;
        let mut committed = Vec::new();
        let mut retriable = false;
        for topic in &response.topics {
            for partition in &topic.partitions {
                let key = (topic.name.clone(), partition.partition_index);
                let Some((_, _, offset)) =
                    offsets.iter().find(|(t, p, _)| *t == key.0 && *p == key.1)
                else {
                    continue;
                };
                match partition.error_code {
                    codes::NONE => {
                        if let Some(p) = self.assigned.get_mut(&key) {
                            p.committed = Some(*offset);
                        }
                        committed.push((key.0, key.1, *offset));
                    }
                    codes::ILLEGAL_GENERATION
                    | codes::UNKNOWN_MEMBER_ID
                    | codes::REBALANCE_IN_PROGRESS
                        if self.config.group_protocol == GroupProtocol::Classic && !self.manual =>
                    {
                        events.push(ConsumerEvent::Error {
                            api: "OffsetCommit",
                            code: partition.error_code,
                        });
                        self.request_rejoin(
                            ctx,
                            events,
                            partition.error_code != codes::UNKNOWN_MEMBER_ID,
                        );
                        return;
                    }
                    codes::FENCED_INSTANCE_ID => {
                        // Another instance took the member's place: Kafka's
                        // consumer fails with `FencedInstanceIdException`.
                        events.push(ConsumerEvent::Error {
                            api: "OffsetCommit",
                            code: codes::FENCED_INSTANCE_ID,
                        });
                        self.state = MemberState::Failed(codes::FENCED_INSTANCE_ID);
                    }
                    code => {
                        if retry::class(code).is_retriable() {
                            retriable = true;
                        }
                        self.client.note_error(code, &self.coordinator());
                        events.push(ConsumerEvent::Error {
                            api: "OffsetCommit",
                            code,
                        });
                    }
                }
            }
        }
        if retriable {
            self.back_off_auto_commit(ctx.now(), commit);
        }
        if !committed.is_empty() {
            events.push(ConsumerEvent::Committed { offsets: committed });
        }
        self.advance_reconciliation(ctx, events);
    }

    // ---- positions --------------------------------------------------------------

    /// Give the new partitions a position: the committed offset through
    /// `OffsetFetch` when the consumer has a group, else the reset policy;
    /// then a `ListOffsets` per leader for the partitions a reset waits on.
    fn init_positions(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if !self.has_group() {
            // Nothing is committed without a group: the reset policy decides.
            let reset = self.config.auto_offset_reset;
            for p in self.assigned.values_mut() {
                if p.state == PositionState::Init {
                    p.state = PositionState::NeedReset(reset);
                }
            }
        } else if self.offset_fetch.is_none() && (self.manual || self.state == MemberState::Stable)
        {
            let need: Vec<(String, i32)> = self
                .assigned
                .iter()
                .filter(|(_, p)| p.state == PositionState::Init && now >= p.retry_at)
                .map(|(key, _)| key.clone())
                .collect();
            if !need.is_empty() {
                self.send_offset_fetch(ctx, need);
            }
        }
        let mut per_leader: BTreeMap<i32, Resets> = BTreeMap::new();
        let mut needs_metadata = false;
        for ((topic, partition), p) in &self.assigned {
            let PositionState::NeedReset(strategy) = p.state else {
                continue;
            };
            if now < p.retry_at {
                continue;
            }
            match self.client.metadata().leader(topic, *partition) {
                Some(leader) => per_leader
                    .entry(leader)
                    .or_default()
                    .push(((topic.clone(), *partition), strategy)),
                None => needs_metadata = true,
            }
        }
        if needs_metadata {
            self.client.request_metadata_refresh();
        }
        for (leader, resets) in per_leader {
            self.send_list_offsets(ctx, leader, resets);
        }
    }

    fn send_offset_fetch(&mut self, ctx: &mut Ctx<'_>, partitions: Vec<(String, i32)>) {
        let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for (topic, partition) in &partitions {
            by_topic.entry(topic.clone()).or_default().push(*partition);
        }
        // A KIP-848 member names itself; a manual assignment has no member.
        let member_id = (self.config.group_protocol == GroupProtocol::Consumer && !self.manual)
            .then(|| self.member_id.clone());
        let request = OffsetFetchByName(OffsetFetchRequest {
            group_id: self.config.group_id.clone(),
            topics: Some(
                by_topic
                    .iter()
                    .map(|(name, partition_indexes)| OffsetFetchRequestTopic {
                        name: name.clone(),
                        partition_indexes: partition_indexes.clone(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            groups: vec![OffsetFetchRequestGroup {
                group_id: self.config.group_id.clone(),
                member_epoch: if member_id.is_some() {
                    self.generation
                } else {
                    -1
                },
                member_id,
                topics: Some(
                    by_topic
                        .iter()
                        .map(|(name, partition_indexes)| OffsetFetchRequestTopics {
                            name: name.clone(),
                            partition_indexes: partition_indexes.clone(),
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            }],
            require_stable: true,
            ..Default::default()
        });
        for key in &partitions {
            if let Some(p) = self.assigned.get_mut(key) {
                p.state = PositionState::FetchingCommitted;
            }
        }
        let id = self.client.send(ctx, self.coordinator(), request);
        self.offset_fetch = Some((id, partitions));
    }

    fn on_offset_fetch(
        &mut self,
        ctx: &mut Ctx<'_>,
        partitions: &[(String, i32)],
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let now = ctx.now();
        let backoff = self.config.retry_backoff_ms;
        let Some(response) = self.expect::<OffsetFetchResponse>(ctx, "OffsetFetch", result, events)
        else {
            // A partition sought while the request was out keeps its
            // position; the others look their committed offset up again.
            for key in partitions {
                if let Some(p) = self
                    .assigned
                    .get_mut(key)
                    .filter(|p| p.state == PositionState::FetchingCommitted)
                {
                    p.state = PositionState::Init;
                    p.retry_at = now + backoff;
                }
            }
            return;
        };
        let group_error = std::iter::once(response.error_code)
            .chain(response.groups.iter().map(|g| g.error_code))
            .find(|code| *code != codes::NONE);
        let mut rows: BTreeMap<(String, i32), (i16, i64, i32)> = BTreeMap::new();
        for topic in &response.topics {
            for p in &topic.partitions {
                rows.insert(
                    (topic.name.clone(), p.partition_index),
                    (p.error_code, p.committed_offset, p.committed_leader_epoch),
                );
            }
        }
        for group in &response.groups {
            for topic in &group.topics {
                for p in &topic.partitions {
                    rows.insert(
                        (topic.name.clone(), p.partition_index),
                        (p.error_code, p.committed_offset, p.committed_leader_epoch),
                    );
                }
            }
        }
        if let Some(code) = group_error {
            self.client.note_error(code, &self.coordinator());
            events.push(ConsumerEvent::Error {
                api: "OffsetFetch",
                code,
            });
        }
        for key in partitions {
            let Some(p) = self.assigned.get_mut(key) else {
                continue;
            };
            if p.state != PositionState::FetchingCommitted {
                continue;
            }
            match (group_error, rows.get(key)) {
                (None, Some((codes::NONE, offset, epoch))) if *offset >= 0 => {
                    p.committed = Some(*offset);
                    p.position = *offset;
                    p.next_fetch = *offset;
                    p.leader_epoch = *epoch;
                    p.state = PositionState::Ready;
                }
                (None, Some((codes::NONE | codes::UNKNOWN_TOPIC_OR_PARTITION, _, _))) => {
                    p.state = PositionState::NeedReset(self.config.auto_offset_reset);
                }
                _ => {
                    p.state = PositionState::Init;
                    p.retry_at = now + backoff;
                }
            }
        }
    }

    /// Resolve the resets of `resets` with one `ListOffsets` to `leader`,
    /// each partition at the timestamp of its own strategy.
    fn send_list_offsets(&mut self, ctx: &mut Ctx<'_>, leader: i32, resets: Resets) {
        let mut by_topic: BTreeMap<String, Vec<ListOffsetsPartition>> = BTreeMap::new();
        for ((topic, partition), strategy) in &resets {
            let epoch = self
                .client
                .metadata()
                .partition(topic, *partition)
                .map_or(-1, |p| p.leader_epoch);
            by_topic
                .entry(topic.clone())
                .or_default()
                .push(ListOffsetsPartition {
                    partition_index: *partition,
                    current_leader_epoch: epoch,
                    timestamp: strategy.timestamp(),
                    ..Default::default()
                });
        }
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: self.config.isolation_level.as_wire(),
            topics: by_topic
                .into_iter()
                .map(|(name, partitions)| ListOffsetsTopic {
                    name,
                    partitions,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        for (key, strategy) in &resets {
            if let Some(p) = self.assigned.get_mut(key) {
                p.state = PositionState::Resetting(*strategy);
            }
        }
        let id = self.client.send(ctx, Target::Broker(leader), request);
        self.list_offsets.insert(id, resets);
    }

    /// Apply the offsets a `ListOffsets` found, to the partitions that still
    /// wait for that reset: a partition sought or reset otherwise since the
    /// request left keeps its new state, as Kafka's `maybeSeekUnvalidated`
    /// skips a reset that is no longer needed.
    fn on_list_offsets(
        &mut self,
        ctx: &mut Ctx<'_>,
        resets: &[((String, i32), AutoOffsetReset)],
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let now = ctx.now();
        let backoff = self.config.retry_backoff_ms;
        let response = self.expect::<ListOffsetsResponse>(ctx, "ListOffsets", result, events);
        for (key, strategy) in resets {
            let row = response.as_ref().and_then(|r| {
                r.topics
                    .iter()
                    .find(|t| t.name == key.0)
                    .and_then(|t| t.partitions.iter().find(|p| p.partition_index == key.1))
                    .map(|p| (p.error_code, p.offset, p.leader_epoch))
            });
            let Some(p) = self
                .assigned
                .get_mut(key)
                .filter(|p| p.state == PositionState::Resetting(*strategy))
            else {
                continue;
            };
            match row {
                Some((codes::NONE, offset, epoch)) if offset >= 0 => {
                    p.position = offset;
                    p.next_fetch = offset;
                    p.leader_epoch = epoch;
                    p.buffered.clear();
                    p.state = PositionState::Ready;
                    p.retry_at = now;
                }
                Some((code, _, _)) => {
                    let target = Target::Leader {
                        topic: key.0.clone(),
                        partition: key.1,
                    };
                    self.client.note_error(code, &target);
                    events.push(ConsumerEvent::Error {
                        api: "ListOffsets",
                        code,
                    });
                    p.state = PositionState::NeedReset(*strategy);
                    p.retry_at = now + backoff;
                }
                None => {
                    p.state = PositionState::NeedReset(*strategy);
                    p.retry_at = now + backoff;
                }
            }
        }
    }

    // ---- fetching ---------------------------------------------------------------

    fn maybe_fetch(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let mut per_leader: BTreeMap<i32, Vec<(String, i32)>> = BTreeMap::new();
        let mut needs_metadata = false;
        for ((topic, partition), p) in &self.assigned {
            let fetchable = p.state == PositionState::Ready
                && !p.fetch_in_flight
                && !p.pending_revocation
                && p.buffered.is_empty()
                && now >= p.retry_at;
            if !fetchable {
                continue;
            }
            match self.client.metadata().leader(topic, *partition) {
                Some(leader) if !self.fetch_brokers.contains(&leader) => per_leader
                    .entry(leader)
                    .or_default()
                    .push((topic.clone(), *partition)),
                Some(_) => {}
                None => needs_metadata = true,
            }
        }
        if needs_metadata {
            self.client.request_metadata_refresh();
        }
        for (leader, partitions) in per_leader {
            self.send_fetch(ctx, leader, partitions);
        }
    }

    fn send_fetch(&mut self, ctx: &mut Ctx<'_>, leader: i32, partitions: Vec<(String, i32)>) {
        let mut topics: BTreeMap<String, Vec<FetchPartition>> = BTreeMap::new();
        for (topic, partition) in &partitions {
            let Some(p) = self.assigned.get_mut(&(topic.clone(), *partition)) else {
                continue;
            };
            p.fetch_in_flight = true;
            p.fetch_offset = Some(p.next_fetch);
            let epoch = self
                .client
                .metadata()
                .partition(topic, *partition)
                .map_or(-1, |info| info.leader_epoch);
            topics
                .entry(topic.clone())
                .or_default()
                .push(FetchPartition {
                    partition: *partition,
                    current_leader_epoch: epoch,
                    fetch_offset: p.next_fetch,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: self.config.max_partition_fetch_bytes,
                    ..Default::default()
                });
        }
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: millis_i32(self.config.fetch_max_wait_ms),
            min_bytes: self.config.fetch_min_bytes,
            max_bytes: self.config.fetch_max_bytes,
            isolation_level: self.config.isolation_level.as_wire(),
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
            rack_id: self.config.rack_id.clone().unwrap_or_default(),
            ..Default::default()
        };
        let id = self.client.send(ctx, Target::Broker(leader), request);
        self.fetch_brokers.insert(leader);
        self.fetches.insert(id, (leader, partitions));
        self.metrics.fetches += 1;
    }

    fn on_fetch(
        &mut self,
        ctx: &mut Ctx<'_>,
        partitions: &[(String, i32)],
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let now = ctx.now();
        let backoff = self.config.retry_backoff_ms;
        let response = self.expect::<FetchResponse>(ctx, "Fetch", result, events);
        for key in partitions {
            let row = response.as_ref().and_then(|r| {
                let topic_id = self.client.metadata().topic_id(&key.0);
                r.responses
                    .iter()
                    .find(|t| {
                        t.topic == key.0
                            || topic_id.is_some_and(|id| id != Uuid::ZERO && id == t.topic_id)
                    })
                    .and_then(|t| t.partitions.iter().find(|p| p.partition_index == key.1))
            });
            let Some(p) = self.assigned.get_mut(key) else {
                continue;
            };
            p.fetch_in_flight = false;
            // Kafka's `FetchCollector` discards an answer for a position the
            // partition no longer has: a seek or a reset moved it.
            let sent_from = p.fetch_offset.take();
            if p.state != PositionState::Ready || sent_from != Some(p.next_fetch) {
                continue;
            }
            let Some(row) = row else {
                p.retry_at = now + backoff;
                continue;
            };
            match row.error_code {
                codes::NONE => {
                    let batches = row
                        .records
                        .as_ref()
                        .and_then(RecordsPayload::as_v2)
                        .unwrap_or(&[]);
                    let (records, next) = records_of(&key.0, key.1, batches, p.next_fetch);
                    let bytes: usize = batches.iter().map(RecordBatch::encoded_len).sum();
                    self.metrics.bytes += u64::try_from(bytes).unwrap_or(u64::MAX);
                    self.metrics.records += u64::try_from(records.len()).unwrap_or(u64::MAX);
                    if let Some(next) = next {
                        p.next_fetch = p.next_fetch.max(next);
                    }
                    p.high_watermark = row.high_watermark;
                    p.buffered.extend(records);
                    p.retry_at = now;
                }
                codes::OFFSET_OUT_OF_RANGE => {
                    events.push(ConsumerEvent::Error {
                        api: "Fetch",
                        code: codes::OFFSET_OUT_OF_RANGE,
                    });
                    p.state = PositionState::NeedReset(self.config.auto_offset_reset);
                    p.retry_at = now;
                }
                code => {
                    // KIP-951: the answer names the leader to fetch from.
                    let leader = (
                        row.current_leader.leader_id,
                        row.current_leader.leader_epoch,
                    );
                    let target = Target::Leader {
                        topic: key.0.clone(),
                        partition: key.1,
                    };
                    self.client.update_leader(&key.0, key.1, leader.0, leader.1);
                    self.client.note_error(code, &target);
                    events.push(ConsumerEvent::Error { api: "Fetch", code });
                    p.retry_at = now + backoff;
                }
            }
        }
    }

    /// The typed body of a response, or `None` after an error the caller
    /// retries: the coordinator is forgotten on a lost connection, as Kafka's
    /// `coordinatorDead` does.
    fn expect<T: 'static>(
        &mut self,
        ctx: &mut Ctx<'_>,
        api: &'static str,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) -> Option<T> {
        match result {
            Ok(response) => response.downcast::<T>(),
            Err(error) => {
                let code = match error {
                    ClientError::Timeout { .. } | ClientError::Disconnected { .. } => {
                        codes::NETWORK_EXCEPTION
                    }
                    ClientError::UnsupportedVersion { .. } => codes::UNSUPPORTED_VERSION,
                    _ => codes::UNKNOWN_SERVER_ERROR,
                };
                events.push(ConsumerEvent::Error { api, code });
                if matches!(
                    api,
                    "JoinGroup" | "SyncGroup" | "Heartbeat" | "OffsetCommit" | "OffsetFetch"
                ) {
                    self.client
                        .invalidate_coordinator(CoordinatorType::Group, &self.config.group_id);
                    self.backoff_after_error(ctx);
                    if matches!(self.state, MemberState::Syncing) {
                        self.state = MemberState::Joining;
                    }
                }
                None
            }
        }
    }

    /// The consumer for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let assignment: Vec<Value> = self
            .assigned
            .iter()
            .map(|((topic, partition), p)| {
                json!({
                    "topic": topic,
                    "partition": partition,
                    "position": (p.state == PositionState::Ready).then_some(p.position),
                    "committed": p.committed,
                    "high_watermark": (p.high_watermark >= 0).then_some(p.high_watermark),
                    "lag": p.lag(),
                    "buffered": p.buffered.len(),
                    "fetching": p.fetch_in_flight,
                })
            })
            .collect();
        let coordinator = self
            .client
            .coordinator(CoordinatorType::Group, &self.config.group_id)
            .map(|(id, _)| id);
        json!({
            "group": self.config.group_id,
            "protocol": match self.config.group_protocol {
                GroupProtocol::Classic => "classic",
                GroupProtocol::Consumer => "consumer",
            },
            "state": self.state.name(),
            "member_id": self.member_id,
            "group_instance_id": self.config.group_instance_id,
            "generation": self.generation,
            "coordinator": coordinator,
            "subscription": self.subscription,
            "manual_assignment": self.manual,
            "assignment": assignment,
            "records": self.metrics.records,
            "bytes": self.metrics.bytes,
            "polled": self.metrics.polled,
            "fetches": self.metrics.fetches,
            "commits": self.metrics.commits,
            "rebalances": self.metrics.rebalances,
            "client": self.client.snapshot(),
        })
    }
}

/// A KIP-848 member id. The client generates it (KIP-1082) as Kafka's
/// `Uuid.randomUuid().toString()`: a version 4 UUID in URL-safe base64
/// without padding, 22 characters, drawn from the node's deterministic
/// generator.
fn member_id_from(ctx: &mut Ctx<'_>) -> String {
    loop {
        let mut bytes = [0_u8; 16];
        for chunk in bytes.chunks_mut(4) {
            let word = u32::try_from(ctx.rand(1_u64 << 32)).unwrap_or(0);
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        let uuid = uuid::Builder::from_random_bytes(bytes).into_uuid();
        let id = URL_SAFE_NO_PAD.encode(uuid.as_bytes());
        // Kafka's `Uuid.randomUuid` draws again for an id that starts with
        // `-`, which a command line would read as an option.
        if !id.starts_with('-') {
            return id;
        }
    }
}

/// A logical duration as the `i32` milliseconds of a wire field.
fn millis_i32(ms: Millis) -> i32 {
    i32::try_from(ms).unwrap_or(i32::MAX)
}
