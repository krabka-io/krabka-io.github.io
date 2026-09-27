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
//! records leave through [`Consumer::poll`], which advances the positions
//! the auto-commit timer commits.

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
    /// `group.id`.
    pub group_id: String,
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

/// How far a partition is in getting a position.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PositionState {
    /// The committed offset is not known yet.
    Init,
    /// `OffsetFetch` is in flight.
    FetchingCommitted,
    /// The group committed nothing, or the position was out of range: the
    /// reset policy decides.
    NeedReset,
    /// `ListOffsets` is in flight.
    Resetting,
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
            retry_at: 0,
        }
    }

    fn lag(&self) -> Option<i64> {
        (self.high_watermark >= 0 && self.state == PositionState::Ready)
            .then(|| (self.high_watermark - self.position).max(0))
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
    commit: Option<(RequestId, Offsets)>,
    offset_fetch: Option<(RequestId, Partitions)>,
    list_offsets: BTreeMap<RequestId, Partitions>,
    fetches: BTreeMap<RequestId, (i32, Partitions)>,
    fetch_brokers: BTreeSet<i32>,
    next_heartbeat_at: Millis,
    heartbeat_interval_ms: Millis,
    next_commit_at: Millis,
    pending_leader: Option<PendingLeader>,
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
            next_commit_at: 0,
            pending_leader: None,
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

    /// Subscribe to `topics` and join the group at the next tick. A change
    /// of subscription joins again.
    pub fn subscribe(&mut self, topics: &[&str]) {
        let mut topics: Vec<String> = topics.iter().map(|t| (*t).to_string()).collect();
        topics.sort();
        topics.dedup();
        self.client.add_topics(topics.iter().map(String::as_str));
        self.subscription = topics;
        self.assigned.clear();
        self.sent_fields = SentFields::default();
        self.state = MemberState::Joining;
        self.generation = match self.config.group_protocol {
            GroupProtocol::Classic => -1,
            GroupProtocol::Consumer => JOIN_GROUP_MEMBER_EPOCH,
        };
        self.rejoin_at = 0;
    }

    /// Take up to `max` buffered records, and never more than
    /// `max_poll_records`, advancing the positions the next commit sends.
    /// The partitions take turns, and a partition the member is giving up
    /// hands out none.
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
    pub fn commit(&mut self, ctx: &mut Ctx<'_>) {
        if self.commit.is_none() {
            self.commit_positions(ctx);
        }
    }

    /// Commit, leave the group, and close the client.
    pub fn close(&mut self, ctx: &mut Ctx<'_>) -> Vec<ConsumerEvent> {
        let mut events = Vec::new();
        if self.closed {
            return events;
        }
        self.closed = true;
        if self.config.enable_auto_commit && self.commit.is_none() {
            self.commit_positions(ctx);
        }
        if !self.member_id.is_empty()
            && matches!(self.state, MemberState::Stable | MemberState::Syncing)
        {
            self.send_leave(ctx);
        }
        let partitions = self.assignment();
        if !partitions.is_empty() {
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
                // Kafka's `transitionToSendingLeaveGroup` gives the assignment
                // up first, so a member that reported partitions reports none.
                let owned_reported = self
                    .sent_fields
                    .topic_partitions
                    .as_ref()
                    .is_some_and(|owned| !owned.is_empty());
                let request = ConsumerGroupHeartbeatRequest {
                    group_id: self.config.group_id.clone(),
                    member_id: self.member_id.clone(),
                    member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                    rebalance_timeout_ms: -1,
                    topic_partitions: owned_reported.then(Vec::new),
                    ..Default::default()
                };
                self.leave = Some(self.client.send(ctx, self.coordinator(), request));
            }
        }
    }

    fn coordinator(&self) -> Target {
        Target::Coordinator {
            key_type: CoordinatorType::Group,
            key: self.config.group_id.clone(),
        }
    }

    /// The next time the consumer needs a tick: `None` when only an answer
    /// or new metadata can move it on, which arrive as frames.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let client = self.client.next_deadline(now);
        let active = !self.closed
            && matches!(
                self.state,
                MemberState::Joining | MemberState::Syncing | MemberState::Stable
            );
        if !active {
            return client.map(|at| at.max(now));
        }
        // Only what a tick can act on counts. A request in flight wakes the
        // member with its answer, and a partition without a leader waits for
        // the metadata the client refreshes.
        let membership = match (self.state, self.config.group_protocol) {
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
        let commit = (self.config.enable_auto_commit
            && self.state == MemberState::Stable
            && self.commit.is_none())
        .then_some(self.next_commit_at);
        let partitions = self.assigned.iter().filter_map(|((topic, partition), p)| {
            let leader = self.client.metadata().leader(topic, *partition);
            let due = match p.state {
                PositionState::Init => {
                    self.offset_fetch.is_none() && self.state == MemberState::Stable
                }
                PositionState::NeedReset => leader.is_some(),
                PositionState::Ready => {
                    !p.fetch_in_flight
                        && !p.pending_revocation
                        && p.buffered.is_empty()
                        && leader.is_some_and(|l| !self.fetch_brokers.contains(&l))
                }
                PositionState::FetchingCommitted | PositionState::Resetting => false,
            };
            due.then_some(p.retry_at)
        });
        client
            .into_iter()
            .chain(membership)
            .chain(commit)
            .chain(partitions)
            .min()
            .map(|at| at.max(now))
    }

    // ---- driving ----------------------------------------------------------------

    fn step(&mut self, ctx: &mut Ctx<'_>, events: &mut Vec<ConsumerEvent>) {
        if self.closed
            || matches!(
                self.state,
                MemberState::Unsubscribed | MemberState::Failed(_) | MemberState::Left
            )
        {
            return;
        }
        match self.config.group_protocol {
            GroupProtocol::Classic => {
                self.maybe_join(ctx);
                self.maybe_heartbeat(ctx);
            }
            GroupProtocol::Consumer => self.maybe_group_heartbeat(ctx),
        }
        self.maybe_commit(ctx);
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
        } else if self.commit.as_ref().is_some_and(|(c, _)| *c == id) {
            if let Some((_, offsets)) = self.commit.take() {
                self.on_commit(ctx, &offsets, result, events);
            }
        } else if self.offset_fetch.as_ref().is_some_and(|(o, _)| *o == id) {
            if let Some((_, partitions)) = self.offset_fetch.take() {
                self.on_offset_fetch(ctx, &partitions, result, events);
            }
        } else if let Some(partitions) = self.list_offsets.remove(&id) {
            self.on_list_offsets(ctx, &partitions, result, events);
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
        if self.config.enable_auto_commit && self.commit.is_none() {
            self.commit_positions(ctx);
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
            group_instance_id: None,
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
                    let members = response
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
            group_instance_id: None,
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
                self.next_commit_at = now + self.config.auto_commit_interval_ms;
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
            group_instance_id: None,
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
            self.next_commit_at = now + self.config.auto_commit_interval_ms;
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
                if self.commit_positions(ctx) {
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
    }

    // ---- commits ----------------------------------------------------------------

    fn maybe_commit(&mut self, ctx: &mut Ctx<'_>) {
        if !self.config.enable_auto_commit
            || self.state != MemberState::Stable
            || self.commit.is_some()
            || ctx.now() < self.next_commit_at
        {
            return;
        }
        self.next_commit_at = ctx.now() + self.config.auto_commit_interval_ms;
        self.commit_positions(ctx);
    }

    /// Commit every known position that changed since the last commit.
    /// Commit the position of every partition that has one, as Kafka's
    /// consumers commit `SubscriptionState.allConsumed()`. Returns whether a
    /// request went out.
    fn commit_positions(&mut self, ctx: &mut Ctx<'_>) -> bool {
        if self.member_id.is_empty() {
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
        let request = OffsetCommitByName(OffsetCommitRequest {
            group_id: self.config.group_id.clone(),
            generation_id_or_member_epoch: self.generation,
            member_id: self.member_id.clone(),
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
        let id = self.client.send(ctx, self.coordinator(), request);
        self.commit = Some((
            id,
            offsets
                .into_iter()
                .map(|(topic, partition, offset, _)| (topic, partition, offset))
                .collect(),
        ));
        self.metrics.commits += 1;
        true
    }

    fn on_commit(
        &mut self,
        ctx: &mut Ctx<'_>,
        offsets: &[(String, i32, i64)],
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let Some(response) =
            self.expect::<OffsetCommitResponse>(ctx, "OffsetCommit", result, events)
        else {
            self.advance_reconciliation(ctx, events);
            return;
        };
        let mut committed = Vec::new();
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
                        if self.config.group_protocol == GroupProtocol::Classic =>
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
                    code => {
                        self.client.note_error(code, &self.coordinator());
                        events.push(ConsumerEvent::Error {
                            api: "OffsetCommit",
                            code,
                        });
                    }
                }
            }
        }
        if !committed.is_empty() {
            events.push(ConsumerEvent::Committed { offsets: committed });
        }
        self.advance_reconciliation(ctx, events);
    }

    // ---- positions --------------------------------------------------------------

    fn init_positions(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self.offset_fetch.is_none() && self.state == MemberState::Stable {
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
        let mut per_leader: BTreeMap<i32, Vec<(String, i32)>> = BTreeMap::new();
        let mut needs_metadata = false;
        for ((topic, partition), p) in &self.assigned {
            if p.state != PositionState::NeedReset || now < p.retry_at {
                continue;
            }
            match self.client.metadata().leader(topic, *partition) {
                Some(leader) => per_leader
                    .entry(leader)
                    .or_default()
                    .push((topic.clone(), *partition)),
                None => needs_metadata = true,
            }
        }
        if needs_metadata {
            self.client.request_metadata_refresh();
        }
        for (leader, partitions) in per_leader {
            self.send_list_offsets(ctx, leader, partitions);
        }
    }

    fn send_offset_fetch(&mut self, ctx: &mut Ctx<'_>, partitions: Vec<(String, i32)>) {
        let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for (topic, partition) in &partitions {
            by_topic.entry(topic.clone()).or_default().push(*partition);
        }
        let member_id =
            (self.config.group_protocol == GroupProtocol::Consumer).then(|| self.member_id.clone());
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
            for key in partitions {
                if let Some(p) = self.assigned.get_mut(key) {
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
                    p.state = PositionState::NeedReset;
                }
                _ => {
                    p.state = PositionState::Init;
                    p.retry_at = now + backoff;
                }
            }
        }
    }

    fn send_list_offsets(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: i32,
        partitions: Vec<(String, i32)>,
    ) {
        let timestamp = self.config.auto_offset_reset.timestamp();
        let mut by_topic: BTreeMap<String, Vec<ListOffsetsPartition>> = BTreeMap::new();
        for (topic, partition) in &partitions {
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
                    timestamp,
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
        for key in &partitions {
            if let Some(p) = self.assigned.get_mut(key) {
                p.state = PositionState::Resetting;
            }
        }
        let id = self.client.send(ctx, Target::Broker(leader), request);
        self.list_offsets.insert(id, partitions);
    }

    fn on_list_offsets(
        &mut self,
        ctx: &mut Ctx<'_>,
        partitions: &[(String, i32)],
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ConsumerEvent>,
    ) {
        let now = ctx.now();
        let backoff = self.config.retry_backoff_ms;
        let response = self.expect::<ListOffsetsResponse>(ctx, "ListOffsets", result, events);
        for key in partitions {
            let row = response.as_ref().and_then(|r| {
                r.topics
                    .iter()
                    .find(|t| t.name == key.0)
                    .and_then(|t| t.partitions.iter().find(|p| p.partition_index == key.1))
                    .map(|p| (p.error_code, p.offset, p.leader_epoch))
            });
            let Some(p) = self.assigned.get_mut(key) else {
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
                    p.state = PositionState::NeedReset;
                    p.retry_at = now + backoff;
                }
                None => {
                    p.state = PositionState::NeedReset;
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
                    p.state = PositionState::NeedReset;
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
            "generation": self.generation,
            "coordinator": coordinator,
            "subscription": self.subscription,
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
