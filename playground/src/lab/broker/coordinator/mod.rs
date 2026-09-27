//! The group coordinator of one broker, as a pure state machine.
//!
//! A [`Coordinator`] holds every group whose `__consumer_offsets` partition
//! the broker leads: classic groups (`JoinGroup`, `SyncGroup`, `Heartbeat`,
//! `LeaveGroup`), KIP-848 consumer groups (`ConsumerGroupHeartbeat`),
//! KIP-1071 streams groups (`StreamsGroupHeartbeat`) and the committed
//! offsets of all of them. It does no I/O and reads no clock: every method
//! takes the logical time, and every effect the broker must carry out comes
//! back as a value.
//!
//! # How the broker drives it
//!
//! - A request handler calls the method of its api with the decoded request
//!   and gets the response, or a [`Pending::Held`] token for a `JoinGroup`
//!   or `SyncGroup` that Kafka answers later. After every call that can
//!   answer a held request (`join_group`, `sync_group`, `leave_group`,
//!   `on_tick`) the broker takes [`Coordinator::drain_completions`] and
//!   sends each response to the connection that holds the token.
//! - After every call the broker arms its timer at
//!   [`Coordinator::next_deadline`] and calls [`Coordinator::on_tick`] when it
//!   fires; expired sessions, rebalance and sync deadlines run there.
//! - After every call the broker takes [`Coordinator::drain_records`] and
//!   appends each record to the `__consumer_offsets` partition its key names
//!   ([`RecordKey::partition`], which is [`group_partition`] of its group).
//! - When the broker becomes the leader of a `__consumer_offsets` partition
//!   it replays that partition through [`Coordinator::load`]; when it stops
//!   leading one it calls [`Coordinator::unload`], which answers the held
//!   requests of the partition's groups with `NOT_COORDINATOR`.
//! - [`Coordinator::snapshot`] is the inspector's view.
//!
//! The group ids a coordinator holds are not checked against the partitions
//! the broker leads: the broker routes a request here only when it is the
//! coordinator of the group, and answers `NOT_COORDINATOR` itself otherwise.
//!
//! # Records
//!
//! Every record is a JSON key and an optional JSON value; a `null` value is a
//! tombstone that deletes the key. The keys are the [`RecordKey`] shapes:
//!
//! | Key | Value |
//! | --- | --- |
//! | `{"type":"offset","group":"g","topic":"t","partition":1}` | [`OffsetEntry`]: `{"offset":99,"leader_epoch":2,"metadata":"meta","commit_timestamp":7000}` |
//! | `{"type":"classic_group","group":"g"}` | [`ClassicGroupValue`]: `generation`, `protocol_type`, `protocol_name`, `leader`, and `members`, each with `id`, `instance_id`, `client_id`, `client_host`, `session_timeout_ms`, `rebalance_timeout_ms`, `protocols` (`name` and base64 `metadata`) and base64 `assignment` |
//! | `{"type":"consumer_group","group":"cg"}` | [`ConsumerGroupValue`]: `group_epoch`, `target_epoch`, `assignment_timestamp` (the time of the last target, or `null`), `members` ([`ConsumerMember`], whose `assigned` and `pending_revocation` map each partition to the epoch it was assigned at), `target` (member to partitions), `subscription_metadata` (topic name to `id` and `partitions`) and `topic_names` |
//! | `{"type":"streams_group","group":"app"}` | [`StreamsGroupValue`]: `group_epoch`, `target_epoch`, `assignment_timestamp`, `members` ([`StreamsMember`]), `topology` ([`StoredTopology`]), `target` (member to [`RoleTasks`]) and `metadata_signature` (topic name to partition count, `null` for a missing topic) |
//!
//! Partitions are keyed by topic id, written as 32 lowercase hexadecimal
//! characters; tasks are keyed by subtopology id. A group record holds the
//! whole group, and the coordinator writes one after every change it keeps,
//! so the latest record of a key is the group. Timers are not written:
//! [`Coordinator::load`] starts every loaded session at the load time.

use std::collections::BTreeMap;

use base64::Engine as _;
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_describe_response::ConsumerGroupDescribeResponse,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup},
        heartbeat_request::HeartbeatRequest,
        heartbeat_response::HeartbeatResponse,
        join_group_request::JoinGroupRequest,
        join_group_response::JoinGroupResponse,
        leave_group_request::LeaveGroupRequest,
        leave_group_response::LeaveGroupResponse,
        list_groups_request::ListGroupsRequest,
        list_groups_response::{ListGroupsResponse, ListedGroup},
        offset_commit_request::OffsetCommitRequest,
        offset_commit_response::OffsetCommitResponse,
        offset_fetch_request::OffsetFetchRequest,
        offset_fetch_response::OffsetFetchResponse,
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_describe_response::StreamsGroupDescribeResponse,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
        sync_group_request::SyncGroupRequest,
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid,
};
use serde::Serialize;
use serde_json::{Value, json};

use self::{
    classic::ClassicGroup,
    consumer::ConsumerGroup,
    streams::StreamsGroup,
    timers::{TimerKey, Timers},
};
use crate::lab::{codes, net::Millis};

mod classic;
mod consumer;
mod ids;
mod offsets;
mod persist;
mod streams;
mod streams_assignor;
mod streams_topology;
#[cfg(test)]
mod tests;
mod timers;
mod uniform;

pub use self::{
    classic::{ClassicGroupValue, ClassicMemberValue, ClassicState, ProtocolValue},
    consumer::{ConsumerGroupValue, ConsumerMember, ConsumerMemberState, TopicMeta},
    ids::{
        GroupId, HoldToken, MemberId, OFFSETS_TOPIC, OFFSETS_TOPIC_PARTITIONS, TopicId,
        group_partition, java_hash_code,
    },
    offsets::OffsetEntry,
    persist::RecordKey,
    streams::{RoleTasks, StreamsGroupValue, StreamsMember, StreamsMemberState, TaskMap},
    streams_topology::{InternalTopicToCreate, StoredSubtopology, StoredTopicInfo, StoredTopology},
    uniform::Partitions,
};

/// The configuration of a coordinator. Every field is a Kafka broker config
/// with Kafka's default.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CoordinatorConfig {
    /// `group.initial.rebalance.delay.ms`: how long a classic group that was
    /// empty waits for more members before its first rebalance completes.
    /// Default: `3000`.
    pub initial_rebalance_delay_ms: Millis,
    /// `group.min.session.timeout.ms` for classic members. Default: `6000`.
    pub classic_min_session_timeout_ms: Millis,
    /// `group.max.session.timeout.ms` for classic members. Default:
    /// `1800000`.
    pub classic_max_session_timeout_ms: Millis,
    /// `group.consumer.session.timeout.ms` (KIP-848). Default: `45000`.
    pub consumer_session_timeout_ms: Millis,
    /// `group.consumer.heartbeat.interval.ms` (KIP-848). Default: `5000`.
    pub consumer_heartbeat_interval_ms: i32,
    /// `group.consumer.assignment.interval.ms`: the least time between two
    /// target assignments of a consumer group; `0` does not wait. Default:
    /// `1000`.
    pub consumer_assignment_interval_ms: Millis,
    /// `group.streams.session.timeout.ms` (KIP-1071). Default: `45000`.
    pub streams_session_timeout_ms: Millis,
    /// `group.streams.heartbeat.interval.ms` (KIP-1071). Default: `5000`.
    pub streams_heartbeat_interval_ms: i32,
    /// `group.streams.num.standby.replicas` (KIP-1071). Default: `0`.
    pub streams_num_standby_replicas: i32,
    /// `group.streams.initial.rebalance.delay.ms`: how long the first
    /// assignment of a streams group waits for more members after a member
    /// joins it empty; `0` assigns at once. Default: `3000`.
    pub streams_initial_rebalance_delay_ms: Millis,
    /// `group.streams.assignment.interval.ms`: the least time between two
    /// target assignments of a streams group; `0` does not wait. Default:
    /// `1000`.
    pub streams_assignment_interval_ms: Millis,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            initial_rebalance_delay_ms: 3000,
            classic_min_session_timeout_ms: 6000,
            classic_max_session_timeout_ms: 1_800_000,
            consumer_session_timeout_ms: 45_000,
            consumer_heartbeat_interval_ms: 5000,
            consumer_assignment_interval_ms: 1000,
            streams_session_timeout_ms: 45_000,
            streams_heartbeat_interval_ms: 5000,
            streams_num_standby_replicas: 0,
            streams_initial_rebalance_delay_ms: 3000,
            streams_assignment_interval_ms: 1000,
        }
    }
}

/// The identity of the client behind a request: the `client_id` of the
/// request header and the peer address, which `DescribeGroups` reports as the
/// `client_host` and which Kafka prints as `/127.0.0.1`. The classic member
/// id is generated from the client id.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct MemberKey {
    pub client_id: String,
    pub client_host: String,
}

/// The answer to a request the coordinator may hold.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Pending<T> {
    /// The response, to send now.
    Ready(T),
    /// The request is held. Its response arrives as a [`Completion`] with
    /// this token, possibly before the call that returned it ends.
    Held(HoldToken),
}

/// The response of a request the coordinator held.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AnyResponse {
    JoinGroup(JoinGroupResponse),
    SyncGroup(SyncGroupResponse),
}

/// A held request and its response.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Completion {
    pub token: HoldToken,
    pub response: AnyResponse,
}

/// What the coordinator needs to know about topics. The broker implements it
/// over its metadata image, and each answer reflects the image at the call.
///
/// The coordinator reads it during a call to resolve subscriptions, topic
/// ids and topology sizes, and keeps no reference to it: a topic created or
/// resized later reaches a group at its next heartbeat.
pub trait TopicMetadata {
    /// The partition count of a topic that exists, or `None`.
    fn partitions(&self, topic: &str) -> Option<i32>;
    /// The id of a topic that exists, or `None`.
    fn topic_id(&self, topic: &str) -> Option<Uuid>;
    /// The name of the topic with `id`, or `None`.
    fn topic_name(&self, id: Uuid) -> Option<String>;
    /// The names of the existing topics a regular expression matches: a
    /// KIP-848 `SubscribedTopicRegex` or a KIP-1071 `SourceTopicRegex`, both
    /// Java RE2J patterns. A pattern the implementation cannot compile
    /// matches nothing.
    fn topics_matching(&self, regex: &str) -> Vec<String>;
}

/// Kafka's `throwIfEmptyString` test: Java's `String.trim` drops every
/// leading and trailing character at or below U+0020.
fn is_blank(value: &str) -> bool {
    value.trim_matches(|c: char| c <= ' ').is_empty()
}

/// Kafka's `canComputeNextTargetAssignment`: a group computes its next target
/// when it has none yet, when the interval is 0, or once the interval has
/// passed since the last one.
fn can_compute_next_target(last: Option<Millis>, interval: Millis, now: Millis) -> bool {
    last.is_none_or(|at| interval == 0 || now >= at.saturating_add(interval))
}

/// The facilities every group kind shares: the timers, the queued
/// completions and records, the hold tokens and the member id generator.
struct Shared {
    broker_id: i32,
    config: CoordinatorConfig,
    timers: Timers,
    completions: Vec<Completion>,
    records: Vec<(Bytes, Option<Bytes>)>,
    next_hold: u64,
    next_member_seq: u64,
}

impl Shared {
    fn hold(&mut self) -> HoldToken {
        self.next_hold += 1;
        HoldToken(self.next_hold)
    }

    fn complete(&mut self, token: HoldToken, response: AnyResponse) {
        self.completions.push(Completion { token, response });
    }

    fn persist<V: Serialize>(&mut self, key: &RecordKey, value: Option<&V>) {
        self.records.push(persist::encode(key, value));
    }

    /// A member id suffix that is unique for this broker: the broker id in
    /// the high bits and a counter in the low bits of a UUID, so two
    /// coordinators never mint the same id and a replay of the same
    /// coordinator ([`Shared::observe_member_id`]) continues the counter.
    fn next_uuid(&mut self) -> uuid::Uuid {
        self.next_member_seq += 1;
        let high = u128::from(self.broker_id.unsigned_abs()) << 64;
        uuid::Uuid::from_u128(high | u128::from(self.next_member_seq))
    }

    /// Kafka's `generateMemberId`: `<prefix>-<uuid>`.
    fn new_member_id(&mut self, prefix: &str) -> MemberId {
        MemberId(format!("{prefix}-{}", self.next_uuid()))
    }

    /// A KIP-848 member id the coordinator mints for a version 0
    /// `ConsumerGroupHeartbeat`, as Kafka's `Uuid.randomUuid().toString()`:
    /// the 16 bytes in URL-safe base64 without padding.
    fn new_raw_member_id(&mut self) -> MemberId {
        let bytes = self.next_uuid().into_bytes();
        MemberId(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    /// Move the member id counter past an id a replay restored, when this
    /// broker minted it: `<prefix>-<uuid>` or a base64 Kafka `Uuid`.
    fn observe_member_id(&mut self, member_id: &str) {
        let hyphenated = member_id
            .get(member_id.len().saturating_sub(36)..)
            .and_then(|suffix| uuid::Uuid::parse_str(suffix).ok());
        let raw = || {
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(member_id)
                .ok()?;
            uuid::Uuid::from_slice(&bytes).ok()
        };
        let Some(uuid) = hyphenated.or_else(raw) else {
            return;
        };
        let bits = uuid.as_u128();
        let high = u128::from(self.broker_id.unsigned_abs()) << 64;
        if bits & !u128::from(u64::MAX) == high {
            let seq = u64::try_from(bits & u128::from(u64::MAX)).unwrap_or(u64::MAX);
            self.next_member_seq = self.next_member_seq.max(seq);
        }
    }
}

/// One group, of one of the three kinds.
enum Group {
    Classic(ClassicGroup),
    Consumer(ConsumerGroup),
    Streams(StreamsGroup),
}

/// The group coordinator of one broker.
pub struct Coordinator {
    groups: BTreeMap<GroupId, Group>,
    offsets: BTreeMap<GroupId, offsets::GroupOffsets>,
    shared: Shared,
}

impl Coordinator {
    /// A coordinator with no groups.
    #[must_use]
    pub fn new(broker_id: i32, config: CoordinatorConfig) -> Self {
        Self {
            groups: BTreeMap::new(),
            offsets: BTreeMap::new(),
            shared: Shared {
                broker_id,
                config,
                timers: Timers::default(),
                completions: Vec::new(),
                records: Vec::new(),
                next_hold: 0,
                next_member_seq: 0,
            },
        }
    }

    #[must_use]
    pub fn broker_id(&self) -> i32 {
        self.shared.broker_id
    }

    #[must_use]
    pub fn config(&self) -> &CoordinatorConfig {
        &self.shared.config
    }

    /// The ids of every group the coordinator holds, in order.
    pub fn group_ids(&self) -> impl Iterator<Item = &GroupId> {
        self.groups.keys()
    }

    // ---- classic protocol -------------------------------------------------------

    /// `JoinGroup`. A held response completes when the rebalance round does.
    ///
    /// As Kafka 4.3's `classicGroupJoin`: a consumer or streams group with
    /// members answers `INCONSISTENT_GROUP_PROTOCOL` with the empty member
    /// id, which for a consumer group is Kafka's answer with
    /// `group.consumer.migration.policy=disabled`; an empty consumer or
    /// streams group gives way to a classic group; a member id for a group
    /// that does not exist then is `UNKNOWN_MEMBER_ID` and creates nothing.
    pub fn join_group(
        &mut self,
        now: Millis,
        client: &MemberKey,
        req: &JoinGroupRequest,
        version: i16,
    ) -> Pending<JoinGroupResponse> {
        let refuse = |error_code, member_id: &str| {
            Pending::Ready(JoinGroupResponse {
                error_code,
                member_id: member_id.to_string(),
                protocol_name: None,
                ..Default::default()
            })
        };
        // Kafka's `GroupCoordinatorService.joinGroup` checks the request before
        // it looks the group up.
        if req.group_id.is_empty() {
            return refuse(codes::INVALID_GROUP_ID, &req.member_id);
        }
        let session = u64::try_from(req.session_timeout_ms).unwrap_or(0);
        let config = &self.shared.config;
        if session < config.classic_min_session_timeout_ms
            || session > config.classic_max_session_timeout_ms
        {
            return refuse(codes::INVALID_SESSION_TIMEOUT, &req.member_id);
        }
        let group_id = GroupId::from(req.group_id.as_str());
        // Kafka's `classicGroupJoin`: a streams group with members refuses the
        // join. A consumer group with members refuses it too: Kafka hosts the
        // classic member there through the online migration, which the lab
        // does not model, and this is Kafka's answer, from the error path,
        // with `group.consumer.migration.policy=disabled`. An empty consumer
        // or streams group gives way to a classic group, and a member id
        // names a member of a group that exists, so a missing group is not
        // created for it.
        match self.groups.get(&group_id) {
            Some(Group::Consumer(g)) if !g.members.is_empty() => {
                return refuse(codes::INCONSISTENT_GROUP_PROTOCOL, "");
            }
            Some(Group::Streams(g)) if !g.members.is_empty() => {
                return refuse(codes::INCONSISTENT_GROUP_PROTOCOL, "");
            }
            Some(Group::Consumer(_) | Group::Streams(_)) => self.replace_empty_group(&group_id),
            None | Some(Group::Classic(_)) => {}
        }
        if !req.member_id.is_empty() && !self.groups.contains_key(&group_id) {
            return refuse(codes::UNKNOWN_MEMBER_ID, &req.member_id);
        }
        let group = self
            .groups
            .entry(group_id.clone())
            .or_insert_with(|| Group::Classic(ClassicGroup::new(group_id)));
        match group {
            Group::Classic(g) => classic::join(g, &mut self.shared, now, client, req, version),
            // `replace_empty_group` left no group of another kind.
            Group::Consumer(_) | Group::Streams(_) => {
                refuse(codes::INCONSISTENT_GROUP_PROTOCOL, &req.member_id)
            }
        }
    }

    /// `SyncGroup`. A follower's response is held until the leader's
    /// `SyncGroup` installs the assignments. A group that is not a classic
    /// group holds no classic member: `UNKNOWN_MEMBER_ID`.
    pub fn sync_group(
        &mut self,
        now: Millis,
        req: &SyncGroupRequest,
    ) -> Pending<SyncGroupResponse> {
        let error = |code| {
            Pending::Ready(SyncGroupResponse {
                error_code: code,
                ..Default::default()
            })
        };
        if req.group_id.is_empty() {
            return error(codes::INVALID_GROUP_ID);
        }
        match self.groups.get_mut(&GroupId::from(req.group_id.as_str())) {
            Some(Group::Classic(g)) => classic::sync(g, &mut self.shared, now, req),
            Some(Group::Consumer(_) | Group::Streams(_)) | None => error(codes::UNKNOWN_MEMBER_ID),
        }
    }

    /// `Heartbeat`. A group that is not a classic group holds no classic
    /// member: `UNKNOWN_MEMBER_ID`.
    pub fn heartbeat(&mut self, now: Millis, req: &HeartbeatRequest) -> HeartbeatResponse {
        let error_code = if req.group_id.is_empty() {
            codes::INVALID_GROUP_ID
        } else {
            match self.groups.get_mut(&GroupId::from(req.group_id.as_str())) {
                Some(Group::Classic(g)) => classic::heartbeat(g, &mut self.shared, now, req),
                Some(Group::Consumer(_) | Group::Streams(_)) | None => codes::UNKNOWN_MEMBER_ID,
            }
        };
        HeartbeatResponse {
            throttle_time_ms: 0,
            error_code,
            ..Default::default()
        }
    }

    /// `LeaveGroup`. On a consumer group every member it names is
    /// `UNKNOWN_MEMBER_ID`; on a streams group, or a group that does not
    /// exist, the whole request is.
    pub fn leave_group(
        &mut self,
        now: Millis,
        req: &LeaveGroupRequest,
        version: i16,
    ) -> LeaveGroupResponse {
        let error = |code| LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: code,
            ..Default::default()
        };
        if req.group_id.is_empty() {
            return error(codes::INVALID_GROUP_ID);
        }
        match self.groups.get_mut(&GroupId::from(req.group_id.as_str())) {
            Some(Group::Classic(g)) => classic::leave(g, &mut self.shared, now, req, version),
            // Kafka's `classicGroupLeaveToConsumerGroup` answers each member
            // it does not hold, and the lab hosts no classic member in a
            // consumer group.
            Some(Group::Consumer(_)) => classic::leave_unknown_members(req, version),
            Some(Group::Streams(_)) | None => error(codes::UNKNOWN_MEMBER_ID),
        }
    }

    // ---- offsets -----------------------------------------------------------------

    /// `OffsetCommit`, with the topic checks Kafka's `KafkaApis` makes before
    /// the group sees the commit: at v10 the topics come by id, resolved
    /// through `metadata`, and a topic or partition `metadata` does not have
    /// is refused. Only authorization is left to the broker.
    pub fn offset_commit(
        &mut self,
        now: Millis,
        req: &OffsetCommitRequest,
        version: i16,
        metadata: &dyn TopicMetadata,
    ) -> OffsetCommitResponse {
        offsets::commit(self, now, req, version, metadata)
    }

    /// `OffsetCommit` for a group this coordinator does not hold, refused
    /// with `error_code` behind the same topic checks as
    /// [`Coordinator::offset_commit`], as Kafka answers a commit its group
    /// coordinator service fails: the broker answers `NOT_COORDINATOR` for
    /// a group of a partition it does not lead with it.
    #[must_use]
    pub fn offset_commit_refused(
        req: &OffsetCommitRequest,
        version: i16,
        metadata: &dyn TopicMetadata,
        error_code: i16,
    ) -> OffsetCommitResponse {
        offsets::commit_refused(req, version, metadata, error_code)
    }

    /// `OffsetFetch`: the single-group shape below v8, the per-group shape
    /// from v8, topic ids at v10.
    #[must_use]
    pub fn offset_fetch(
        &self,
        req: &OffsetFetchRequest,
        version: i16,
        metadata: &dyn TopicMetadata,
    ) -> OffsetFetchResponse {
        offsets::fetch(self, req, version, metadata)
    }

    // ---- introspection -----------------------------------------------------------

    /// `DescribeGroups`. Only a classic group is described; any other id is
    /// answered in state `Dead`, with `GROUP_ID_NOT_FOUND` from v6.
    #[must_use]
    pub fn describe_groups(
        &self,
        req: &DescribeGroupsRequest,
        version: i16,
    ) -> DescribeGroupsResponse {
        const GROUP_ID_NOT_FOUND_MIN_VERSION: i16 = 6;
        let dead = |group_id: &str, message: String| {
            let not_found = version >= GROUP_ID_NOT_FOUND_MIN_VERSION;
            DescribedGroup {
                group_id: group_id.to_string(),
                group_state: "Dead".to_string(),
                error_code: if not_found {
                    codes::GROUP_ID_NOT_FOUND
                } else {
                    codes::NONE
                },
                error_message: not_found.then_some(message),
                ..Default::default()
            }
        };
        let groups = req
            .groups
            .iter()
            .map(|group_id| {
                // Kafka refuses only a null group id, which the wire cannot
                // carry here: the empty group id is described like any other.
                match self.groups.get(&GroupId::from(group_id.as_str())) {
                    Some(Group::Classic(g)) => g.describe(),
                    Some(Group::Consumer(_) | Group::Streams(_)) => dead(
                        group_id,
                        format!("Group {group_id} is not a classic group."),
                    ),
                    None => dead(group_id, format!("Group {group_id} not found.")),
                }
            })
            .collect();
        DescribeGroupsResponse {
            throttle_time_ms: 0,
            groups,
            ..Default::default()
        }
    }

    /// `ListGroups`, with the state and type filters of v4 and v5 (an empty
    /// filter matches every group).
    #[must_use]
    pub fn list_groups(&self, req: &ListGroupsRequest) -> ListGroupsResponse {
        let states: Vec<String> = req
            .states_filter
            .iter()
            .map(|s| s.trim().to_lowercase())
            .collect();
        let groups: Vec<ListedGroup> = self
            .groups
            .values()
            .map(|group| match group {
                Group::Classic(g) => g.listed(),
                Group::Consumer(g) => g.listed(),
                Group::Streams(g) => g.listed(),
            })
            .filter(|listed| {
                (states.is_empty() || states.contains(&listed.group_state.to_lowercase()))
                    && (req.types_filter.is_empty()
                        || req
                            .types_filter
                            .iter()
                            .any(|t| t.eq_ignore_ascii_case(&listed.group_type)))
            })
            .collect();
        ListGroupsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            groups,
            ..Default::default()
        }
    }

    // ---- KIP-848 ---------------------------------------------------------------

    /// `ConsumerGroupHeartbeat`.
    pub fn consumer_group_heartbeat(
        &mut self,
        now: Millis,
        client: &MemberKey,
        req: &ConsumerGroupHeartbeatRequest,
        version: i16,
        metadata: &dyn TopicMetadata,
    ) -> ConsumerGroupHeartbeatResponse {
        consumer::heartbeat(self, now, client, req, version, metadata)
    }

    /// The error code and message of the first check of Kafka's
    /// `throwIfConsumerGroupHeartbeatRequestIsInvalid` a request fails. Kafka
    /// makes these checks before it routes the request, so the broker
    /// answers them ahead of `NOT_COORDINATOR`.
    #[must_use]
    pub fn consumer_group_heartbeat_error(
        req: &ConsumerGroupHeartbeatRequest,
        version: i16,
    ) -> Option<(i16, String)> {
        consumer::request_error(req, version)
    }

    /// `ConsumerGroupDescribe`.
    #[must_use]
    pub fn consumer_group_describe(
        &self,
        req: &ConsumerGroupDescribeRequest,
    ) -> ConsumerGroupDescribeResponse {
        consumer::describe(self, req)
    }

    // ---- KIP-1071 --------------------------------------------------------------

    /// `StreamsGroupHeartbeat`, and the internal topics the topology needs
    /// that do not exist yet, for the broker to create.
    pub fn streams_group_heartbeat(
        &mut self,
        now: Millis,
        client: &MemberKey,
        req: &StreamsGroupHeartbeatRequest,
        metadata: &dyn TopicMetadata,
    ) -> (StreamsGroupHeartbeatResponse, Vec<InternalTopicToCreate>) {
        streams::heartbeat(self, now, client, req, metadata)
    }

    /// The error code and message of the first request check a
    /// `StreamsGroupHeartbeat` fails. Kafka makes these checks before it
    /// routes the request, so the broker answers them ahead of
    /// `NOT_COORDINATOR`.
    #[must_use]
    pub fn streams_group_heartbeat_error(
        req: &StreamsGroupHeartbeatRequest,
    ) -> Option<(i16, String)> {
        streams::request_error(req)
    }

    /// `StreamsGroupDescribe`.
    #[must_use]
    pub fn streams_group_describe(
        &self,
        req: &StreamsGroupDescribeRequest,
    ) -> StreamsGroupDescribeResponse {
        streams::describe(self, req)
    }

    // ---- time and completion ---------------------------------------------------------

    /// The earliest deadline; the broker arms its timer at it.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        self.shared.timers.next()
    }

    /// Run every deadline at or before `now`: session expiry, rebalance and
    /// sync deadlines, KIP-848 and KIP-1071 session and revocation timeouts,
    /// and the end of a streams group's initial rebalance delay. Returns the
    /// completions of the requests this answered.
    pub fn on_tick(&mut self, now: Millis) -> Vec<Completion> {
        for (at, key) in self.shared.timers.pop_due(now) {
            match key {
                TimerKey::ClassicSession { group, member } => {
                    if let Some(Group::Classic(g)) = self.groups.get_mut(&group) {
                        classic::session_expired(g, &mut self.shared, &member, at, now);
                    }
                }
                TimerKey::ClassicPending { group, member } => {
                    if let Some(Group::Classic(g)) = self.groups.get_mut(&group) {
                        classic::pending_expired(g, &mut self.shared, &member, at, now);
                    }
                }
                TimerKey::ClassicJoin { group } => {
                    if let Some(Group::Classic(g)) = self.groups.get_mut(&group) {
                        classic::join_deadline_fired(g, &mut self.shared, at, now);
                    }
                }
                TimerKey::ClassicSync { group, generation } => {
                    if let Some(Group::Classic(g)) = self.groups.get_mut(&group) {
                        classic::sync_deadline_fired(g, &mut self.shared, at, now, generation);
                    }
                }
                TimerKey::ConsumerSession { group, member } => {
                    if let Some(Group::Consumer(g)) = self.groups.get_mut(&group) {
                        g.session_expired(&mut self.shared, &member, at);
                    }
                }
                TimerKey::ConsumerRebalance { group, member } => {
                    if let Some(Group::Consumer(g)) = self.groups.get_mut(&group) {
                        g.rebalance_expired(&mut self.shared, &member, at);
                    }
                }
                TimerKey::StreamsSession { group, member } => {
                    if let Some(Group::Streams(g)) = self.groups.get_mut(&group) {
                        g.session_expired(&mut self.shared, &member, at);
                    }
                }
                TimerKey::StreamsRebalance { group, member } => {
                    if let Some(Group::Streams(g)) = self.groups.get_mut(&group) {
                        g.rebalance_expired(&mut self.shared, &member, at);
                    }
                }
                TimerKey::StreamsInitialRebalance { group } => {
                    if let Some(Group::Streams(g)) = self.groups.get_mut(&group) {
                        g.initial_delay_fired(&mut self.shared, at, now);
                    }
                }
            }
        }
        self.drain_completions()
    }

    /// Take the responses of the held requests answered since the last call.
    pub fn drain_completions(&mut self) -> Vec<Completion> {
        std::mem::take(&mut self.shared.completions)
    }

    /// The number of held requests answered but not yet drained.
    #[must_use]
    pub fn pending_completions(&self) -> usize {
        self.shared.completions.len()
    }

    // ---- persistence ------------------------------------------------------------

    /// Take the records written since the last call, in write order, for the
    /// broker to append to the group's `__consumer_offsets` partition.
    pub fn drain_records(&mut self) -> Vec<(Bytes, Option<Bytes>)> {
        std::mem::take(&mut self.shared.records)
    }

    /// Rebuild the state from the records of a `__consumer_offsets`
    /// partition, in log order; a later record for the same key wins. The
    /// sessions of the loaded members start at `now`. A record whose key is
    /// not one the coordinator writes is skipped. The offsets of a group with
    /// no group record belong to a simple classic group, as Kafka's replay
    /// creates one.
    pub fn load(&mut self, now: Millis, records: impl IntoIterator<Item = (Bytes, Option<Bytes>)>) {
        for (key, value) in records {
            let Some(key) = RecordKey::decode(&key) else {
                continue;
            };
            match key {
                RecordKey::Offset {
                    group,
                    topic,
                    partition,
                } => match value
                    .as_deref()
                    .and_then(persist::decode_value::<OffsetEntry>)
                {
                    Some(entry) => {
                        self.offsets
                            .entry(group)
                            .or_default()
                            .insert((topic, partition), entry);
                    }
                    None => {
                        if let Some(offsets) = self.offsets.get_mut(&group) {
                            offsets.remove(&(topic, partition));
                        }
                    }
                },
                RecordKey::ClassicGroup { group } => {
                    self.forget_group(&group);
                    if let Some(value) = value
                        .as_deref()
                        .and_then(persist::decode_value::<ClassicGroupValue>)
                    {
                        let loaded =
                            ClassicGroup::from_value(group.clone(), value, now, &mut self.shared);
                        self.groups.insert(group, Group::Classic(loaded));
                    }
                }
                RecordKey::ConsumerGroup { group } => {
                    self.forget_group(&group);
                    if let Some(value) = value
                        .as_deref()
                        .and_then(persist::decode_value::<ConsumerGroupValue>)
                    {
                        let loaded =
                            ConsumerGroup::from_value(group.clone(), value, now, &mut self.shared);
                        self.groups.insert(group, Group::Consumer(loaded));
                    }
                }
                RecordKey::StreamsGroup { group } => {
                    self.forget_group(&group);
                    if let Some(value) = value
                        .as_deref()
                        .and_then(persist::decode_value::<StreamsGroupValue>)
                    {
                        let loaded =
                            StreamsGroup::from_value(group.clone(), value, now, &mut self.shared);
                        self.groups.insert(group, Group::Streams(loaded));
                    }
                }
            }
        }
        // Offsets committed outside group management belong to a simple
        // classic group, as Kafka's load creates one.
        for group in self.offsets.keys() {
            if !self.groups.contains_key(group) {
                self.groups.insert(
                    group.clone(),
                    Group::Classic(ClassicGroup::new(group.clone())),
                );
            }
        }
    }

    /// Kafka's `onUnloaded`: the broker no longer leads `__consumer_offsets`
    /// partition `partition`, so every group on it goes, with its committed
    /// offsets and its timers. Each held `JoinGroup` and `SyncGroup` of those
    /// groups gets `NOT_COORDINATOR`, so its client looks the coordinator up
    /// again. Returns those completions.
    pub fn unload(&mut self, partition: i32) -> Vec<Completion> {
        let unloaded: Vec<GroupId> = self
            .groups
            .keys()
            .filter(|group| group_partition(group.as_str()) == partition)
            .cloned()
            .collect();
        for group_id in &unloaded {
            if let Some(Group::Classic(g)) = self.groups.get_mut(group_id) {
                classic::unload(g, &mut self.shared);
            }
            self.forget_group(group_id);
        }
        self.offsets
            .retain(|group, _| group_partition(group.as_str()) != partition);
        self.drain_completions()
    }

    /// Drop an empty group that makes way for a group of another kind, and
    /// write the tombstone of its record, as Kafka's
    /// `maybeDeleteEmptyClassicGroup` and `maybeDeleteEmptyConsumerGroup` do.
    /// The committed offsets stay with the group id.
    fn replace_empty_group(&mut self, group_id: &GroupId) {
        let group = group_id.clone();
        let key = match self.groups.get(group_id) {
            Some(Group::Classic(_)) => RecordKey::ClassicGroup { group },
            Some(Group::Consumer(_)) => RecordKey::ConsumerGroup { group },
            Some(Group::Streams(_)) => RecordKey::StreamsGroup { group },
            None => return,
        };
        self.forget_group(group_id);
        self.shared.persist::<()>(&key, None);
    }

    /// Drop a group and every timer it armed, ahead of its replacement.
    fn forget_group(&mut self, group_id: &GroupId) {
        match self.groups.remove(group_id) {
            Some(Group::Classic(g)) => g.cancel_timers(&mut self.shared),
            Some(Group::Consumer(g)) => g.cancel_timers(&mut self.shared),
            Some(Group::Streams(g)) => g.cancel_timers(&mut self.shared),
            None => {}
        }
    }

    // ---- inspector ---------------------------------------------------------------

    /// The coordinator for the inspector:
    ///
    /// ```json
    /// {
    ///   "broker_id": 1,
    ///   "groups": { "<group>": { "type": "classic" | "consumer" | "streams", ... } },
    ///   "offsets": { "<group>": [ { "topic", "partition", "offset", "leader_epoch", "metadata", "commit_timestamp" } ] },
    ///   "next_deadline": 12345,
    ///   "armed_timers": 3,
    ///   "idle": false,
    ///   "held_requests": 1
    /// }
    /// ```
    ///
    /// Every group has `type` and `state` (Kafka's state name). A classic
    /// group adds `protocol_type`, `protocol_name`, `generation`, `leader`,
    /// `pending_members`, `pending_sync`, `rebalance_deadline`,
    /// `sync_deadline` and `members` (`member_id`, `instance_id`,
    /// `client_id`, `client_host`, `session_timeout_ms`,
    /// `rebalance_timeout_ms`, `protocols`, `assignment_bytes`,
    /// `awaiting_join`, `awaiting_sync`, `session_deadline`). A consumer group
    /// adds `group_epoch`, `assignment_epoch`, `assignment_timestamp`,
    /// `topics` (name to partition count) and `members` (`member_id`,
    /// `instance_id`, `rack_id`, `client_id`, `client_host`, `member_epoch`,
    /// `previous_member_epoch`, `state`, `subscribed_topic_names`,
    /// `subscribed_topic_regex`, and `assigned`, `pending_revocation` and
    /// `target` as topic name to partitions, `session_deadline`,
    /// `rebalance_deadline`). A streams group adds `group_epoch`,
    /// `assignment_epoch`, `assignment_timestamp`,
    /// `initial_rebalance_deadline`, `topology_epoch`, `status`
    /// (`code` and `detail`, or `null`), `internal_topics_to_create`, `tasks`
    /// (subtopology to task count), `shutdown_requested_by`,
    /// `endpoint_information_epoch` and `members` (`member_id`, `instance_id`,
    /// `rack_id`, `client_id`, `client_host`, `process_id`, `user_endpoint`,
    /// `topology_epoch`, `member_epoch`, `previous_member_epoch`, `state`, and
    /// `tasks`, `pending_revocation` and `target` as `active`, `standby` and
    /// `warmup` task maps, `task_offsets`, `task_end_offsets`,
    /// `session_deadline`, `rebalance_deadline`).
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let groups: serde_json::Map<String, Value> = self
            .groups
            .iter()
            .map(|(id, group)| {
                let value = match group {
                    Group::Classic(g) => g.snapshot(),
                    Group::Consumer(g) => g.snapshot(),
                    Group::Streams(g) => g.snapshot(),
                };
                (id.as_str().to_string(), value)
            })
            .collect();
        let held: usize = self
            .groups
            .values()
            .map(|group| match group {
                Group::Classic(g) => g
                    .members
                    .values()
                    .map(|m| m.join_holds.len() + m.sync_holds.len())
                    .sum(),
                Group::Consumer(_) | Group::Streams(_) => 0,
            })
            .sum();
        json!({
            "broker_id": self.shared.broker_id,
            "groups": groups,
            "offsets": offsets::snapshot(&self.offsets),
            "next_deadline": self.shared.timers.next(),
            "armed_timers": self.shared.timers.len(),
            "idle": self.shared.timers.is_empty(),
            "held_requests": held,
        })
    }
}
