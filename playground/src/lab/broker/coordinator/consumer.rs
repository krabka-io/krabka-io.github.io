//! KIP-848 consumer groups: `ConsumerGroupHeartbeat` and
//! `ConsumerGroupDescribe`.
//!
//! A consumer group starts at group and target epoch 1, as Kafka 4.3's
//! `ModernGroup` does. The group epoch goes up when a member changes its
//! subscription, leaves or is fenced, and when the subscribed topics change.
//! A group epoch ahead of the target gets a new target from the
//! [`uniform`](super::uniform) assignor, at most once per
//! `group.consumer.assignment.interval.ms`. A member moves toward the target
//! in its own heartbeats, as Kafka 4.3's `CurrentAssignmentBuilder` does: a
//! member that must give up partitions it still reports keeps its epoch and
//! only the partitions it retains, until a heartbeat no longer reports the
//! revoked ones; it then takes the target epoch and every target partition
//! that no other member owns, and waits in `UnreleasedPartitions` for the
//! rest. Each assigned partition keeps the epoch it was assigned at, which
//! `OffsetCommit` checks a commit from an older member epoch against
//! (KIP-1251).
//!
//! The transitions follow Kafka 4.3's
//! `GroupMetadataManager.consumerGroupHeartbeat`, `consumerGroupLeave`, the
//! session and rebalance timeouts and the static membership rules of KIP-345
//! (`UNRELEASED_INSTANCE_ID`, `FENCED_INSTANCE_ID`). A group epoch bumped by
//! a leave or a fence gets its target from the next heartbeat, which is the
//! first call that sees the topic metadata.
//!
//! # Persisted value
//!
//! The record under `{"type":"consumer_group","group":<id>}` is a
//! [`ConsumerGroupValue`]: the group and target epochs, the time of the last
//! target, every member with its subscription, epochs, reconciliation state
//! and partitions (each with its assignment epoch), the target assignment,
//! and the subscribed topics with their ids and partition counts. Topic ids
//! are 32 hexadecimal characters. It is written after every heartbeat that
//! changed something.

use std::collections::{BTreeMap, BTreeSet};

use krabka_protocol::owned::{
    common::{
        consumer_group_describe_response::{
            assignment::Assignment as DescribedAssignment,
            topic_partitions::TopicPartitions as DescribedTopicPartitions,
        },
        consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
    },
    consumer_group_describe_request::ConsumerGroupDescribeRequest,
    consumer_group_describe_response::{
        ConsumerGroupDescribeResponse, DescribedGroup, Member as DescribedMember,
    },
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
    list_groups_response::ListedGroup,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    Coordinator, Group, MemberKey, Shared, TopicMetadata,
    classic::ClassicState,
    ids::{GroupId, MemberId, TopicId},
    is_blank,
    offsets::{CommitCheck, FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION},
    persist::RecordKey,
    timers::TimerKey,
    uniform::{self, MemberSpec, Partitions},
};
use crate::lab::{codes, net::Millis};

/// The member epoch of a heartbeat that leaves the group.
pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

/// The member epoch of a static member that leaves for a while and keeps
/// its assignment for a member with the same instance id.
pub const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// The first `ConsumerGroupHeartbeat` version that requires the member id the
/// consumer generated (KIP-1082).
const FIRST_CLIENT_MEMBER_ID_VERSION: i16 = 1;

/// The `group_type` and `protocol_type` of a consumer group.
pub const GROUP_TYPE: &str = "consumer";

/// The name of the assignor every group uses.
pub const ASSIGNOR_NAME: &str = "uniform";

/// Kafka's `group.consumer.assignors` default: the server assignors a member
/// may name. A member that names `range` still gets the uniform assignment.
pub const SUPPORTED_ASSIGNORS: [&str; 2] = ["uniform", "range"];

/// The group and target epoch of a new group: Kafka 4.3's `ModernGroup`
/// starts both at 1, so the first member's join takes the group to epoch 2.
pub const INITIAL_EPOCH: i32 = 1;

/// A member's partitions by topic, each with the epoch it was assigned at.
pub type PartitionEpochs = BTreeMap<TopicId, BTreeMap<i32, i32>>;

/// `ConsumerGroupDescribeResponse.Member.MemberType` of a consumer member.
const MEMBER_TYPE_CONSUMER: i8 = 1;

/// Where a member stands in its reconciliation toward the target.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ConsumerMemberState {
    /// The member holds its target assignment at the target epoch.
    Stable,
    /// The member must give up partitions and has not confirmed it yet.
    UnrevokedPartitions,
    /// The member waits for partitions another member still owns.
    UnreleasedPartitions,
}

impl ConsumerMemberState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "Stable",
            Self::UnrevokedPartitions => "UnrevokedPartitions",
            Self::UnreleasedPartitions => "UnreleasedPartitions",
        }
    }
}

/// One member of a consumer group.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ConsumerMember {
    pub id: MemberId,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: BTreeSet<String>,
    /// The subscribed regex; empty for none, as Kafka's member keeps it.
    pub subscribed_topic_regex: String,
    pub server_assignor: Option<String>,
    pub rebalance_timeout_ms: Millis,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: ConsumerMemberState,
    /// The partitions the member may own now.
    pub assigned: PartitionEpochs,
    /// The partitions the member must give up.
    pub pending_revocation: PartitionEpochs,
    #[serde(skip)]
    pub session_deadline: Millis,
    /// The deadline to revoke by, and the member epoch it was armed at.
    #[serde(skip)]
    pub rebalance_deadline: Option<(Millis, i32)>,
}

/// A subscribed topic that exists: its id and partition count.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct TopicMeta {
    pub id: TopicId,
    pub partitions: i32,
}

/// A consumer group.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConsumerGroup {
    pub group_id: GroupId,
    pub group_epoch: i32,
    /// The epoch the target assignment was computed for.
    pub target_epoch: i32,
    /// When the target was last computed; `None` before the first.
    pub assignment_timestamp: Option<Millis>,
    pub target: BTreeMap<MemberId, Partitions>,
    pub members: BTreeMap<MemberId, ConsumerMember>,
    pub instance_to_member: BTreeMap<String, MemberId>,
    /// The subscribed topics that exist, as of the last heartbeat.
    pub subscription_metadata: BTreeMap<String, TopicMeta>,
    /// The names of the topics that appear in assignments.
    pub topic_names: BTreeMap<TopicId, String>,
}

/// The persisted form of a consumer group.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ConsumerGroupValue {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub assignment_timestamp: Option<Millis>,
    pub members: Vec<ConsumerMember>,
    pub target: BTreeMap<MemberId, Partitions>,
    pub subscription_metadata: BTreeMap<String, TopicMeta>,
    pub topic_names: BTreeMap<TopicId, String>,
}

/// The member a regular heartbeat belongs to.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Resolved {
    New,
    Existing,
    /// A static member rejoins with a new member id in place of `previous`,
    /// which left with epoch -2.
    Replaces(MemberId),
}

/// An error code with the message Kafka puts on the response.
struct HeartbeatError {
    code: i16,
    message: String,
}

fn error_response(code: i16, message: Option<String>) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: code,
        error_message: message,
        ..Default::default()
    }
}

/// Kafka 4.3's `throwIfConsumerGroupHeartbeatRequestIsInvalid`, in its
/// order: the error code and message of the first check the request fails.
fn validate_request(
    req: &ConsumerGroupHeartbeatRequest,
    version: i16,
) -> Result<(), HeartbeatError> {
    let invalid = |message: &str| {
        Err(HeartbeatError {
            code: codes::INVALID_REQUEST,
            message: message.to_string(),
        })
    };
    let member_id_required = version >= FIRST_CLIENT_MEMBER_ID_VERSION
        || req.member_epoch > 0
        || req.member_epoch == LEAVE_GROUP_MEMBER_EPOCH;
    if member_id_required && is_blank(&req.member_id) {
        return invalid("MemberId can't be empty.");
    }
    if is_blank(&req.group_id) {
        return invalid("GroupId can't be empty.");
    }
    if req.instance_id.as_deref().is_some_and(is_blank) {
        return invalid("InstanceId can't be empty.");
    }
    if req.rack_id.as_deref().is_some_and(is_blank) {
        return invalid("RackId can't be empty.");
    }
    match req.member_epoch {
        0 => {
            if req.rebalance_timeout_ms == -1 {
                return invalid("RebalanceTimeoutMs must be provided in first request.");
            }
            if req
                .topic_partitions
                .as_ref()
                .is_none_or(|tp| !tp.is_empty())
            {
                return invalid("TopicPartitions must be empty when (re-)joining.");
            }
            // An empty list of names or an empty regex joins with no topics.
            if req.subscribed_topic_names.is_none() && req.subscribed_topic_regex.is_none() {
                return invalid(
                    "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when (re-)joining.",
                );
            }
        }
        LEAVE_GROUP_STATIC_MEMBER_EPOCH if req.instance_id.is_none() => {
            return invalid("InstanceId can't be null.");
        }
        epoch if epoch < LEAVE_GROUP_STATIC_MEMBER_EPOCH => {
            return invalid("MemberEpoch is invalid.");
        }
        _ => {}
    }
    if let Some(assignor) = &req.server_assignor
        && !SUPPORTED_ASSIGNORS.contains(&assignor.as_str())
    {
        return Err(HeartbeatError {
            code: codes::UNSUPPORTED_ASSIGNOR,
            message: format!(
                "ServerAssignor {assignor} is not supported. Supported assignors: {}.",
                SUPPORTED_ASSIGNORS.join(", ")
            ),
        });
    }
    Ok(())
}

/// The error code and message of the first check of Kafka's
/// `throwIfConsumerGroupHeartbeatRequestIsInvalid` a request fails, which
/// Kafka answers before it routes the request to a coordinator.
pub fn request_error(req: &ConsumerGroupHeartbeatRequest, version: i16) -> Option<(i16, String)> {
    validate_request(req, version)
        .err()
        .map(|error| (error.code, error.message))
}

/// `ConsumerGroupHeartbeat`.
pub fn heartbeat(
    coord: &mut Coordinator,
    now: Millis,
    client: &MemberKey,
    req: &ConsumerGroupHeartbeatRequest,
    version: i16,
    metadata: &dyn TopicMetadata,
) -> ConsumerGroupHeartbeatResponse {
    if let Err(error) = validate_request(req, version) {
        return error_response(error.code, Some(error.message));
    }
    let group_id = GroupId::from(req.group_id.as_str());
    let joining = req.member_epoch == 0;
    // Kafka's `getOrMaybeCreateConsumerGroup`: only a join creates a group,
    // and an empty classic group, one that only holds offsets, makes way. A
    // classic group with members would be upgraded online, which the lab does
    // not model: this is Kafka's answer with
    // `group.consumer.migration.policy=disabled`.
    match coord.groups.get(&group_id) {
        None if !joining => {
            return error_response(
                codes::GROUP_ID_NOT_FOUND,
                Some(format!("Consumer group {} not found.", req.group_id)),
            );
        }
        Some(Group::Classic(classic)) if joining && classic.state == ClassicState::Empty => {
            coord.replace_empty_group(&group_id);
        }
        Some(Group::Classic(_)) if joining => {
            return error_response(
                codes::GROUP_ID_NOT_FOUND,
                Some(format!(
                    "Cannot upgrade classic group {} to consumer group because online upgrade is disabled.",
                    req.group_id
                )),
            );
        }
        Some(Group::Classic(_) | Group::Streams(_)) => {
            return error_response(
                codes::GROUP_ID_NOT_FOUND,
                Some(format!("Group {} is not a consumer group.", req.group_id)),
            );
        }
        None | Some(Group::Consumer(_)) => {}
    }
    let member_id = if req.member_id.is_empty() {
        coord.shared.new_raw_member_id()
    } else {
        MemberId::from(req.member_id.as_str())
    };
    let group = coord
        .groups
        .entry(group_id.clone())
        .or_insert_with(|| Group::Consumer(ConsumerGroup::new(group_id)));
    let Group::Consumer(group) = group else {
        return error_response(codes::GROUP_ID_NOT_FOUND, None);
    };
    group.heartbeat(&mut coord.shared, now, client, req, &member_id, metadata)
}

/// `ConsumerGroupDescribe`.
pub fn describe(
    coord: &Coordinator,
    req: &ConsumerGroupDescribeRequest,
) -> ConsumerGroupDescribeResponse {
    let groups = req
        .group_ids
        .iter()
        .map(|group_id| {
            let not_found = |reason: &str| DescribedGroup {
                group_id: group_id.clone(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                error_message: Some(format!("Group {group_id} {reason}.")),
                ..Default::default()
            };
            match coord.groups.get(&GroupId::from(group_id.as_str())) {
                Some(Group::Consumer(g)) => g.describe(),
                Some(Group::Classic(_) | Group::Streams(_)) => not_found("is not a consumer group"),
                None => not_found("not found"),
            }
        })
        .collect();
    ConsumerGroupDescribeResponse {
        throttle_time_ms: 0,
        groups,
        ..Default::default()
    }
}

impl ConsumerGroup {
    /// An empty group at Kafka's initial epochs.
    #[must_use]
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            group_epoch: INITIAL_EPOCH,
            target_epoch: INITIAL_EPOCH,
            assignment_timestamp: None,
            target: BTreeMap::new(),
            members: BTreeMap::new(),
            instance_to_member: BTreeMap::new(),
            subscription_metadata: BTreeMap::new(),
            topic_names: BTreeMap::new(),
        }
    }

    fn session_key(&self, member_id: &MemberId) -> TimerKey {
        TimerKey::ConsumerSession {
            group: self.group_id.clone(),
            member: member_id.clone(),
        }
    }

    fn rebalance_key(&self, member_id: &MemberId) -> TimerKey {
        TimerKey::ConsumerRebalance {
            group: self.group_id.clone(),
            member: member_id.clone(),
        }
    }

    fn arm_session(&mut self, sh: &mut Shared, member_id: &MemberId, now: Millis) {
        let at = now.saturating_add(sh.config.consumer_session_timeout_ms);
        let key = self.session_key(member_id);
        if let Some(member) = self.members.get_mut(member_id) {
            sh.timers.rearm(Some(member.session_deadline), at, key);
            member.session_deadline = at;
        }
    }

    fn cancel_member_timers(&self, sh: &mut Shared, member: &ConsumerMember) {
        sh.timers
            .cancel(member.session_deadline, &self.session_key(&member.id));
        if let Some((at, _)) = member.rebalance_deadline {
            sh.timers.cancel(at, &self.rebalance_key(&member.id));
        }
    }

    /// Cancel every timer of the group, ahead of dropping it.
    pub fn cancel_timers(&self, sh: &mut Shared) {
        for member in self.members.values() {
            self.cancel_member_timers(sh, member);
        }
    }

    /// Kafka's `maybeReconcile` timer part: a member that entered
    /// `UnrevokedPartitions` must revoke within its rebalance timeout; any
    /// other state cancels the timeout.
    fn track_rebalance_timeout(&mut self, sh: &mut Shared, member_id: &MemberId, now: Millis) {
        let key = self.rebalance_key(member_id);
        let Some(member) = self.members.get_mut(member_id) else {
            return;
        };
        if let Some((at, _)) = member.rebalance_deadline.take() {
            sh.timers.cancel(at, &key);
        }
        if member.state == ConsumerMemberState::UnrevokedPartitions {
            let at = now.saturating_add(member.rebalance_timeout_ms);
            member.rebalance_deadline = Some((at, member.member_epoch));
            sh.timers.arm(at, key);
        }
    }

    fn persist(&self, sh: &mut Shared) {
        sh.persist(
            &RecordKey::ConsumerGroup {
                group: self.group_id.clone(),
            },
            Some(&self.value()),
        );
    }

    /// The persisted form of the group.
    #[must_use]
    pub fn value(&self) -> ConsumerGroupValue {
        ConsumerGroupValue {
            group_epoch: self.group_epoch,
            target_epoch: self.target_epoch,
            assignment_timestamp: self.assignment_timestamp,
            members: self.members.values().cloned().collect(),
            target: self.target.clone(),
            subscription_metadata: self.subscription_metadata.clone(),
            topic_names: self.topic_names.clone(),
        }
    }

    /// Rebuild a group from its persisted form at `now`: every session starts
    /// from `now`, and a member with partitions to revoke gets its rebalance
    /// timeout again, as Kafka's `onLoaded` does.
    pub fn from_value(
        group_id: GroupId,
        value: ConsumerGroupValue,
        now: Millis,
        sh: &mut Shared,
    ) -> Self {
        let mut group = Self::new(group_id);
        group.group_epoch = value.group_epoch;
        group.target_epoch = value.target_epoch;
        group.assignment_timestamp = value.assignment_timestamp;
        group.target = value.target;
        group.subscription_metadata = value.subscription_metadata;
        group.topic_names = value.topic_names;
        for member in value.members {
            sh.observe_member_id(member.id.as_str());
            let id = member.id.clone();
            if let Some(instance_id) = &member.instance_id {
                group
                    .instance_to_member
                    .insert(instance_id.clone(), id.clone());
            }
            group.members.insert(id.clone(), member);
            group.arm_session(sh, &id, now);
            group.track_rebalance_timeout(sh, &id, now);
        }
        group
    }

    /// Kafka's `ConsumerGroup.state`: `Empty` with no members, `Assigning`
    /// while the target is behind the group epoch, `Reconciling` while a
    /// member is not at the target, and `Stable` otherwise.
    #[must_use]
    pub fn state_name(&self) -> &'static str {
        if self.members.is_empty() {
            "Empty"
        } else if self.group_epoch > self.target_epoch {
            "Assigning"
        } else if self
            .members
            .values()
            .any(|m| m.state != ConsumerMemberState::Stable || m.member_epoch != self.target_epoch)
        {
            "Reconciling"
        } else {
            "Stable"
        }
    }

    /// The group as `ListGroups` lists it.
    #[must_use]
    pub fn listed(&self) -> ListedGroup {
        ListedGroup {
            group_id: self.group_id.as_str().to_string(),
            protocol_type: GROUP_TYPE.to_string(),
            group_state: self.state_name().to_string(),
            group_type: GROUP_TYPE.to_string(),
            ..Default::default()
        }
    }

    /// The group as `ConsumerGroupDescribe` describes it. A member's current
    /// assignment includes the partitions it has still to revoke, which it
    /// owns until it confirms the revocation.
    #[must_use]
    pub fn describe(&self) -> DescribedGroup {
        let assignment = |partitions: &Partitions| DescribedAssignment {
            topic_partitions: partitions
                .iter()
                .filter(|(_, ps)| !ps.is_empty())
                .map(|(topic, ps)| DescribedTopicPartitions {
                    topic_id: (*topic).into(),
                    topic_name: self.topic_names.get(topic).cloned().unwrap_or_default(),
                    partitions: ps.iter().copied().collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: self.group_id.as_str().to_string(),
            group_state: self.state_name().to_string(),
            group_epoch: self.group_epoch,
            assignment_epoch: self.target_epoch,
            assignor_name: ASSIGNOR_NAME.to_string(),
            members: self
                .members
                .values()
                .map(|m| {
                    let mut owned = partition_set(&m.assigned);
                    for (topic, ps) in &m.pending_revocation {
                        owned.entry(*topic).or_default().extend(ps.keys().copied());
                    }
                    DescribedMember {
                        member_id: m.id.as_str().to_string(),
                        instance_id: m.instance_id.clone(),
                        rack_id: m.rack_id.clone(),
                        member_epoch: m.member_epoch,
                        client_id: m.client_id.clone(),
                        client_host: m.client_host.clone(),
                        subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                        subscribed_topic_regex: Some(m.subscribed_topic_regex.clone()),
                        assignment: assignment(&owned),
                        target_assignment: assignment(
                            self.target.get(&m.id).unwrap_or(&Partitions::new()),
                        ),
                        member_type: MEMBER_TYPE_CONSUMER,
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The group for the inspector. Partitions are keyed by topic name.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let named = |partitions: &Partitions| -> Value {
            Value::Object(
                partitions
                    .iter()
                    .map(|(topic, ps)| {
                        let name = self
                            .topic_names
                            .get(topic)
                            .cloned()
                            .unwrap_or_else(|| topic.to_string());
                        (name, json!(ps.iter().copied().collect::<Vec<_>>()))
                    })
                    .collect(),
            )
        };
        json!({
            "type": GROUP_TYPE,
            "state": self.state_name(),
            "group_epoch": self.group_epoch,
            "assignment_epoch": self.target_epoch,
            "assignment_timestamp": self.assignment_timestamp,
            "topics": self.subscription_metadata.iter().map(|(name, meta)| (name.clone(), json!(meta.partitions))).collect::<serde_json::Map<_, _>>(),
            "members": self.members.values().map(|m| json!({
                "member_id": m.id,
                "instance_id": m.instance_id,
                "rack_id": m.rack_id,
                "client_id": m.client_id,
                "client_host": m.client_host,
                "member_epoch": m.member_epoch,
                "previous_member_epoch": m.previous_member_epoch,
                "state": m.state.as_str(),
                "subscribed_topic_names": m.subscribed_topic_names,
                "subscribed_topic_regex": m.subscribed_topic_regex,
                "assigned": named(&partition_set(&m.assigned)),
                "pending_revocation": named(&partition_set(&m.pending_revocation)),
                "target": named(self.target.get(&m.id).unwrap_or(&Partitions::new())),
                "session_deadline": m.session_deadline,
                "rebalance_deadline": m.rebalance_deadline.map(|(at, _)| at),
            })).collect::<Vec<_>>(),
        })
    }

    // ---- offsets ---------------------------------------------------------------

    /// Kafka 4.3's `ConsumerGroup.validateOffsetCommit`: a negative epoch
    /// commits on a group with no members; otherwise the member must exist
    /// and use v9 or later. A commit at the member's epoch passes, a newer
    /// epoch is `STALE_MEMBER_EPOCH`, and an older one (KIP-1251) is checked
    /// per partition against the epochs the member was assigned them at,
    /// among its assigned partitions and then those pending revocation.
    pub fn validate_offset_commit(
        &self,
        member_id: &str,
        member_epoch: i32,
        version: i16,
    ) -> Result<CommitCheck, i16> {
        if member_epoch < 0 && self.members.is_empty() {
            return Ok(CommitCheck::Any);
        }
        let member = self
            .members
            .get(member_id)
            .ok_or(codes::UNKNOWN_MEMBER_ID)?;
        if version < FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION {
            return Err(codes::UNSUPPORTED_VERSION);
        }
        if member_epoch == member.member_epoch {
            return Ok(CommitCheck::Any);
        }
        if member_epoch > member.member_epoch {
            return Err(codes::STALE_MEMBER_EPOCH);
        }
        let mut assigned = member.assigned.clone();
        for (topic, partitions) in &member.pending_revocation {
            let epochs = assigned.entry(*topic).or_default();
            for (partition, epoch) in partitions {
                epochs.entry(*partition).or_insert(*epoch);
            }
        }
        Ok(CommitCheck::AssignedBy {
            epoch: member_epoch,
            assigned,
        })
    }

    /// Kafka's `ConsumerGroup.validateOffsetFetch` for a fetch that names a
    /// member or an epoch: the member must exist at the epoch it names.
    pub fn validate_offset_fetch(
        &self,
        member_id: Option<&str>,
        member_epoch: i32,
    ) -> Result<(), i16> {
        let member = member_id
            .and_then(|id| self.members.get(id))
            .ok_or(codes::UNKNOWN_MEMBER_ID)?;
        if member_epoch == member.member_epoch {
            Ok(())
        } else {
            Err(codes::STALE_MEMBER_EPOCH)
        }
    }

    // ---- heartbeat ---------------------------------------------------------------

    /// Kafka 4.3's `consumerGroupHeartbeat` on this group.
    fn heartbeat(
        &mut self,
        sh: &mut Shared,
        now: Millis,
        client: &MemberKey,
        req: &ConsumerGroupHeartbeatRequest,
        member_id: &MemberId,
        metadata: &dyn TopicMetadata,
    ) -> ConsumerGroupHeartbeatResponse {
        if req.member_epoch < 0 {
            return self.leave(sh, req);
        }
        let resolved = match self.resolve_member(req, member_id) {
            Ok(resolved) => resolved,
            Err(error) => return error_response(error.code, Some(error.message)),
        };
        let mut changed = self.admit_member(sh, resolved, member_id, now);
        let assigned_before = self.members[member_id].assigned.clone();
        let update = self.update_member(member_id, client, req);
        changed |= update.changed;
        // The lab resolves a regex at once, so a changed regex bumps the
        // group epoch as Kafka's does for a regex that is already resolved.
        let subscription_changed = update.names_changed || update.regex_changed;
        let topics_changed = self.refresh_subscription_metadata(metadata);
        if subscription_changed || topics_changed {
            self.group_epoch = self.group_epoch.saturating_add(1);
            changed = true;
        }
        changed |= self.maybe_update_target(sh, now, metadata);
        let owned = req.topic_partitions.as_ref().map(|tps| {
            tps.iter()
                .map(|tp| {
                    (
                        TopicId::from(tp.topic_id),
                        tp.partitions.iter().copied().collect(),
                    )
                })
                .collect::<Partitions>()
        });
        if self.reconcile_member(member_id, owned.as_ref(), subscription_changed, metadata) {
            self.track_rebalance_timeout(sh, member_id, now);
            changed = true;
        }
        self.arm_session(sh, member_id, now);
        if changed {
            self.persist(sh);
        }
        self.accepted_response(sh, req, member_id, &assigned_before)
    }

    /// Admit the member a heartbeat resolved to: a new member joins at epoch
    /// 0 with the defaults of Kafka's `ConsumerGroupMember.Builder`, and a
    /// static replacement takes the released member's place. Joining does
    /// not bump the group epoch by itself: the new subscription does.
    /// Returns whether the group changed.
    fn admit_member(
        &mut self,
        sh: &mut Shared,
        resolved: Resolved,
        member_id: &MemberId,
        now: Millis,
    ) -> bool {
        match resolved {
            Resolved::New => {
                self.insert_member(
                    sh,
                    ConsumerMember {
                        id: member_id.clone(),
                        instance_id: None,
                        rack_id: None,
                        client_id: String::new(),
                        client_host: String::new(),
                        subscribed_topic_names: BTreeSet::new(),
                        subscribed_topic_regex: String::new(),
                        server_assignor: None,
                        rebalance_timeout_ms: 0,
                        member_epoch: 0,
                        previous_member_epoch: -1,
                        state: ConsumerMemberState::Stable,
                        assigned: PartitionEpochs::new(),
                        pending_revocation: PartitionEpochs::new(),
                        session_deadline: now,
                        rebalance_deadline: None,
                    },
                );
                true
            }
            Resolved::Replaces(previous) => {
                self.replace_static_member(sh, &previous, member_id);
                true
            }
            Resolved::Existing => false,
        }
    }

    /// Recompute the subscribed topics against the metadata. Returns whether
    /// they changed, which bumps the group epoch as a new metadata hash does
    /// in Kafka.
    fn refresh_subscription_metadata(&mut self, metadata: &dyn TopicMetadata) -> bool {
        let subscription = self.compute_subscription_metadata(metadata);
        if subscription == self.subscription_metadata {
            return false;
        }
        self.subscription_metadata = subscription;
        for (name, meta) in &self.subscription_metadata {
            self.topic_names.insert(meta.id, name.clone());
        }
        true
    }

    /// The response of an accepted heartbeat. Kafka sends the assignment on
    /// a join, on a full request, and when the member's assigned partitions
    /// changed.
    fn accepted_response(
        &self,
        sh: &Shared,
        req: &ConsumerGroupHeartbeatRequest,
        member_id: &MemberId,
        assigned_before: &PartitionEpochs,
    ) -> ConsumerGroupHeartbeatResponse {
        let member = &self.members[member_id];
        let full_request = req.rebalance_timeout_ms != -1
            && (req.subscribed_topic_names.is_some() || req.subscribed_topic_regex.is_some())
            && req.topic_partitions.is_some();
        let include_assignment =
            req.member_epoch == 0 || full_request || member.assigned != *assigned_before;
        ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            member_id: Some(member_id.as_str().to_string()),
            member_epoch: member.member_epoch,
            heartbeat_interval_ms: sh.config.consumer_heartbeat_interval_ms,
            assignment: include_assignment.then(|| Assignment {
                topic_partitions: member
                    .assigned
                    .iter()
                    .filter(|(_, ps)| !ps.is_empty())
                    .map(|(topic, ps)| TopicPartitions {
                        topic_id: (*topic).into(),
                        partitions: ps.keys().copied().collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Kafka's `getOrMaybeSubscribeDynamicConsumerGroupMember` and
    /// `getOrMaybeSubscribeStaticConsumerGroupMember`.
    fn resolve_member(
        &self,
        req: &ConsumerGroupHeartbeatRequest,
        member_id: &MemberId,
    ) -> Result<Resolved, HeartbeatError> {
        let joining = req.member_epoch == 0;
        let Some(instance_id) = req.instance_id.as_deref() else {
            return match self.members.get(member_id) {
                Some(member) => validate_epoch(member, req).map(|()| Resolved::Existing),
                None if joining => Ok(Resolved::New),
                None => Err(self.unknown_member(member_id)),
            };
        };
        let static_member = self
            .instance_to_member
            .get(instance_id)
            .and_then(|id| self.members.get(id));
        if joining {
            return match static_member {
                None if self.members.contains_key(member_id) => Ok(Resolved::Existing),
                None => Ok(Resolved::New),
                Some(existing) if existing.member_epoch != LEAVE_GROUP_STATIC_MEMBER_EPOCH => {
                    Err(HeartbeatError {
                        code: codes::UNRELEASED_INSTANCE_ID,
                        message: format!(
                            "Static member {member_id} with instance id {instance_id} cannot join the group because the instance id is owned by {} member.",
                            existing.id
                        ),
                    })
                }
                Some(existing) => Ok(Resolved::Replaces(existing.id.clone())),
            };
        }
        let existing = static_member.ok_or_else(|| HeartbeatError {
            code: codes::UNKNOWN_MEMBER_ID,
            message: format!("Instance id {instance_id} is unknown."),
        })?;
        if existing.id != *member_id {
            return Err(fenced_instance(member_id, instance_id, &existing.id));
        }
        validate_epoch(existing, req).map(|()| Resolved::Existing)
    }

    fn unknown_member(&self, member_id: &MemberId) -> HeartbeatError {
        HeartbeatError {
            code: codes::UNKNOWN_MEMBER_ID,
            message: format!(
                "Member {member_id} is not a member of group {}.",
                self.group_id
            ),
        }
    }

    fn insert_member(&mut self, sh: &mut Shared, member: ConsumerMember) {
        let id = member.id.clone();
        let deadline = member.session_deadline;
        sh.timers.arm(deadline, self.session_key(&id));
        self.members.insert(id, member);
    }

    /// Kafka's static replacement: the joining member takes the released
    /// member's subscription, target and assignment under its own id, at
    /// epoch 0. The group epoch does not change.
    fn replace_static_member(
        &mut self,
        sh: &mut Shared,
        previous: &MemberId,
        member_id: &MemberId,
    ) {
        let Some(mut member) = self.members.remove(previous) else {
            return;
        };
        self.cancel_member_timers(sh, &member);
        if let Some(old) = self.members.remove(member_id) {
            self.cancel_member_timers(sh, &old);
            if let Some(instance_id) = &old.instance_id
                && self.instance_to_member.get(instance_id) == Some(member_id)
            {
                self.instance_to_member.remove(instance_id);
            }
            self.target.remove(member_id);
        }
        member.id = member_id.clone();
        member.member_epoch = 0;
        member.previous_member_epoch = 0;
        member.rebalance_deadline = None;
        if let Some(instance_id) = &member.instance_id {
            self.instance_to_member
                .insert(instance_id.clone(), member_id.clone());
        }
        if let Some(target) = self.target.remove(previous) {
            self.target.insert(member_id.clone(), target);
        }
        self.insert_member(sh, member);
    }

    /// Apply the fields of a heartbeat to the member, as Kafka's
    /// `ConsumerGroupMember.Builder.maybeUpdate*` do: an absent field keeps
    /// the stored value.
    fn update_member(
        &mut self,
        member_id: &MemberId,
        client: &MemberKey,
        req: &ConsumerGroupHeartbeatRequest,
    ) -> MemberUpdate {
        let Some(member) = self.members.get_mut(member_id) else {
            return MemberUpdate::default();
        };
        let before = member.clone();
        member.client_id.clone_from(&client.client_id);
        member.client_host.clone_from(&client.client_host);
        if req.instance_id.is_some() {
            member.instance_id.clone_from(&req.instance_id);
        }
        if req.rack_id.is_some() {
            member.rack_id.clone_from(&req.rack_id);
        }
        if req.server_assignor.is_some() {
            member.server_assignor.clone_from(&req.server_assignor);
        }
        if let Ok(timeout) = u64::try_from(req.rebalance_timeout_ms) {
            member.rebalance_timeout_ms = timeout;
        }
        if let Some(names) = &req.subscribed_topic_names {
            member.subscribed_topic_names = names.iter().cloned().collect();
        }
        if let Some(regex) = &req.subscribed_topic_regex {
            member.subscribed_topic_regex.clone_from(regex);
        }
        let update = MemberUpdate {
            changed: before != *member,
            names_changed: before.subscribed_topic_names != member.subscribed_topic_names,
            regex_changed: before.subscribed_topic_regex != member.subscribed_topic_regex,
        };
        if let Some(instance_id) = &member.instance_id {
            let id = member.id.clone();
            self.instance_to_member.insert(instance_id.clone(), id);
        }
        update
    }

    /// Kafka's `computeSubscriptionMetadata`: every topic a member subscribes
    /// to, by name or through its regex, that exists.
    fn compute_subscription_metadata(
        &self,
        metadata: &dyn TopicMetadata,
    ) -> BTreeMap<String, TopicMeta> {
        self.subscribed_names(metadata)
            .into_iter()
            .filter_map(|name| {
                let id = metadata.topic_id(&name)?.into();
                let partitions = metadata.partitions(&name)?;
                Some((name, TopicMeta { id, partitions }))
            })
            .collect()
    }

    fn subscribed_names(&self, metadata: &dyn TopicMetadata) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        for member in self.members.values() {
            names.extend(member.subscribed_topic_names.iter().cloned());
            if !member.subscribed_topic_regex.is_empty() {
                names.extend(metadata.topics_matching(&member.subscribed_topic_regex));
            }
        }
        names
    }

    /// Kafka 4.3's `maybeUpdateTargetAssignment`: a target behind the group
    /// epoch is computed again, unless the last one is younger than
    /// `group.consumer.assignment.interval.ms`; the members then reconcile
    /// toward the last target. Returns whether a target was computed.
    fn maybe_update_target(
        &mut self,
        sh: &Shared,
        now: Millis,
        metadata: &dyn TopicMetadata,
    ) -> bool {
        if self.target_epoch >= self.group_epoch
            || !super::can_compute_next_target(
                self.assignment_timestamp,
                sh.config.consumer_assignment_interval_ms,
                now,
            )
        {
            return false;
        }
        self.compute_target(metadata);
        self.assignment_timestamp = Some(now);
        true
    }

    /// Run the assignor for the current group epoch.
    fn compute_target(&mut self, metadata: &dyn TopicMetadata) {
        let specs: Vec<MemberSpec> = self
            .members
            .values()
            .map(|m| MemberSpec {
                id: m.id.clone(),
                subscribed: subscribed_topic_ids(m, metadata)
                    .into_iter()
                    .filter(|id| self.topic_names.contains_key(id))
                    .collect(),
                current: self.target.get(&m.id).cloned().unwrap_or_default(),
            })
            .collect();
        let partitions: BTreeMap<TopicId, i32> = self
            .subscription_metadata
            .values()
            .map(|meta| (meta.id, meta.partitions))
            .collect();
        self.target = uniform::assign(&specs, &partitions);
        self.target_epoch = self.group_epoch;
    }

    /// Whether any member owns the partition, in its assignment or among the
    /// partitions it has still to revoke: Kafka's `currentPartitionEpoch` is
    /// not -1.
    fn is_owned(&self, topic: TopicId, partition: i32) -> bool {
        self.members.values().any(|m| {
            m.assigned
                .get(&topic)
                .is_some_and(|ps| ps.contains_key(&partition))
                || m.pending_revocation
                    .get(&topic)
                    .is_some_and(|ps| ps.contains_key(&partition))
        })
    }

    /// Kafka 4.3's `maybeReconcile` and `CurrentAssignmentBuilder.build`:
    /// move `member_id` toward the target. `owned` is what the heartbeat
    /// reports, `None` when it did not report. Returns whether the member
    /// changed.
    fn reconcile_member(
        &mut self,
        member_id: &MemberId,
        owned: Option<&Partitions>,
        subscription_changed: bool,
        metadata: &dyn TopicMetadata,
    ) -> bool {
        let Some(member) = self.members.get(member_id) else {
            return false;
        };
        let reconciled =
            member.state == ConsumerMemberState::Stable && member.member_epoch == self.target_epoch;
        if reconciled && !subscription_changed {
            return false;
        }
        let subscribed = subscribed_topic_ids(member, metadata);
        let next = match member.state {
            ConsumerMemberState::Stable if member.member_epoch == self.target_epoch => {
                update_current_assignment(member, owned, &subscribed)
            }
            ConsumerMemberState::UnrevokedPartitions
                if owns_revoked(owned, &member.pending_revocation) =>
            {
                if subscription_changed {
                    update_current_assignment(member, owned, &subscribed)
                } else {
                    None
                }
            }
            ConsumerMemberState::Stable
            | ConsumerMemberState::UnrevokedPartitions
            | ConsumerMemberState::UnreleasedPartitions => {
                Some(self.compute_next_assignment(member, owned, &subscribed))
            }
        };
        let Some(next) = next else {
            return false;
        };
        let changed = next != *member;
        self.members.insert(member_id.clone(), next);
        changed
    }

    /// Kafka 4.3's `CurrentAssignmentBuilder.computeNextAssignment`. A member
    /// that still reports a partition it must give up keeps its epoch and
    /// only the partitions it retains; otherwise it takes the target epoch
    /// and every target partition no other member owns, each assigned at the
    /// target epoch, and waits in `UnreleasedPartitions` for the rest. The
    /// target of a topic the member no longer subscribes to counts as empty.
    fn compute_next_assignment(
        &self,
        member: &ConsumerMember,
        owned: Option<&Partitions>,
        subscribed: &BTreeSet<TopicId>,
    ) -> ConsumerMember {
        let no_target = Partitions::new();
        let target = self.target.get(&member.id).unwrap_or(&no_target);
        let no_partitions = BTreeMap::new();
        let mut assigned = PartitionEpochs::new();
        let mut revoke = PartitionEpochs::new();
        let mut assign = Partitions::new();
        let mut unreleased = false;
        let topics: BTreeSet<TopicId> = target
            .keys()
            .chain(member.assigned.keys())
            .copied()
            .collect();
        for topic in topics {
            let wanted = if subscribed.contains(&topic) {
                target.get(&topic).cloned().unwrap_or_default()
            } else {
                BTreeSet::new()
            };
            let current = member.assigned.get(&topic).unwrap_or(&no_partitions);
            let (keep, drop): (BTreeMap<i32, i32>, BTreeMap<i32, i32>) =
                current.iter().partition(|(p, _)| wanted.contains(p));
            let mut take = BTreeSet::new();
            for partition in wanted.iter().filter(|p| !keep.contains_key(p)) {
                // A partition the member itself still has to revoke is not
                // held back.
                let revoking = member
                    .pending_revocation
                    .get(&topic)
                    .is_some_and(|ps| ps.contains_key(partition));
                if self.is_owned(topic, *partition) && !revoking {
                    unreleased = true;
                } else {
                    take.insert(*partition);
                }
            }
            if !keep.is_empty() {
                assigned.insert(topic, keep);
            }
            if !drop.is_empty() {
                revoke.insert(topic, drop);
            }
            if !take.is_empty() {
                assign.insert(topic, take);
            }
        }
        let mut next = member.clone();
        next.previous_member_epoch = member.member_epoch;
        if !revoke.is_empty() && owns_revoked(owned, &revoke) {
            next.state = ConsumerMemberState::UnrevokedPartitions;
            next.assigned = assigned;
            next.pending_revocation = revoke;
            return next;
        }
        for (topic, partitions) in assign {
            let epochs = assigned.entry(topic).or_default();
            for partition in partitions {
                epochs.insert(partition, self.target_epoch);
            }
        }
        next.state = if unreleased {
            ConsumerMemberState::UnreleasedPartitions
        } else {
            ConsumerMemberState::Stable
        };
        next.member_epoch = self.target_epoch;
        next.assigned = assigned;
        next.pending_revocation = PartitionEpochs::new();
        next
    }

    // ---- leave and fencing ---------------------------------------------------------

    /// Kafka's `consumerGroupLeave`: a dynamic member, or a static member that
    /// sends -1, is removed and the group epoch goes up. A static member that
    /// sends -2 stays at epoch -2 with its assignment, so a member with the
    /// same instance id can take its place; its session keeps running.
    fn leave(
        &mut self,
        sh: &mut Shared,
        req: &ConsumerGroupHeartbeatRequest,
    ) -> ConsumerGroupHeartbeatResponse {
        let member_id = MemberId::from(req.member_id.as_str());
        let resolved = match req.instance_id.as_deref() {
            None => self
                .members
                .get(&member_id)
                .map(|m| m.id.clone())
                .ok_or_else(|| self.unknown_member(&member_id)),
            Some(instance_id) => match self
                .instance_to_member
                .get(instance_id)
                .and_then(|id| self.members.get(id))
            {
                None => Err(HeartbeatError {
                    code: codes::UNKNOWN_MEMBER_ID,
                    message: format!("Instance id {instance_id} is unknown."),
                }),
                Some(existing) if existing.id != member_id => {
                    Err(fenced_instance(&member_id, instance_id, &existing.id))
                }
                Some(existing) => Ok(existing.id.clone()),
            },
        };
        let member_id = match resolved {
            Ok(id) => id,
            Err(error) => return error_response(error.code, Some(error.message)),
        };
        if req.instance_id.is_some() && req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
            let key = self.rebalance_key(&member_id);
            if let Some(member) = self.members.get_mut(&member_id) {
                if let Some((at, _)) = member.rebalance_deadline.take() {
                    sh.timers.cancel(at, &key);
                }
                member.member_epoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
                member.pending_revocation.clear();
                for epoch in member.assigned.values_mut().flat_map(|ps| ps.values_mut()) {
                    *epoch = 0;
                }
            }
            self.persist(sh);
            return ConsumerGroupHeartbeatResponse {
                member_id: Some(member_id.as_str().to_string()),
                member_epoch: LEAVE_GROUP_STATIC_MEMBER_EPOCH,
                ..Default::default()
            };
        }
        self.fence_member(sh, &member_id);
        ConsumerGroupHeartbeatResponse {
            member_id: Some(req.member_id.clone()),
            member_epoch: req.member_epoch,
            ..Default::default()
        }
    }

    /// Kafka's `consumerGroupFenceMember`: remove the member and bump the
    /// group epoch, so the next heartbeat computes a new target.
    fn fence_member(&mut self, sh: &mut Shared, member_id: &MemberId) {
        let Some(member) = self.members.remove(member_id) else {
            return;
        };
        self.cancel_member_timers(sh, &member);
        if let Some(instance_id) = &member.instance_id
            && self.instance_to_member.get(instance_id) == Some(member_id)
        {
            self.instance_to_member.remove(instance_id);
        }
        self.target.remove(member_id);
        self.group_epoch = self.group_epoch.saturating_add(1);
        self.persist(sh);
    }

    /// A member's session timer fired at `at`.
    pub fn session_expired(&mut self, sh: &mut Shared, member_id: &MemberId, at: Millis) {
        if self
            .members
            .get(member_id)
            .is_some_and(|m| m.session_deadline == at)
        {
            self.fence_member(sh, member_id);
        }
    }

    /// A member's rebalance timeout fired at `at`: it is fenced when it is
    /// still at the epoch the timeout was armed at, which means it has not
    /// revoked its partitions.
    pub fn rebalance_expired(&mut self, sh: &mut Shared, member_id: &MemberId, at: Millis) {
        let armed = self
            .members
            .get(member_id)
            .and_then(|m| m.rebalance_deadline)
            .is_some_and(|(deadline, epoch)| {
                deadline == at && self.members[member_id].member_epoch == epoch
            });
        if armed {
            self.fence_member(sh, member_id);
        }
    }
}

fn fenced_instance(member_id: &MemberId, instance_id: &str, owner: &MemberId) -> HeartbeatError {
    HeartbeatError {
        code: codes::FENCED_INSTANCE_ID,
        message: format!(
            "Static member {member_id} with instance id {instance_id} was fenced by member {owner}."
        ),
    }
}

/// Kafka's `throwIfConsumerGroupMemberEpochIsInvalid`: epoch 0 is a rejoin
/// and passes; a greater epoch is fenced; a smaller one is fenced unless
/// it is the previous epoch and the member owns only partitions of its
/// assignment, because the response that carried the new epoch may have
/// been lost.
fn validate_epoch(
    member: &ConsumerMember,
    req: &ConsumerGroupHeartbeatRequest,
) -> Result<(), HeartbeatError> {
    let received = req.member_epoch;
    if received == 0 || received == member.member_epoch {
        return Ok(());
    }
    let owns_subset = req.topic_partitions.as_ref().is_some_and(|owned| {
        owned.iter().all(|tp| {
            member
                .assigned
                .get(&TopicId::from(tp.topic_id))
                .is_some_and(|assigned| tp.partitions.iter().all(|p| assigned.contains_key(p)))
        })
    });
    if received < member.member_epoch && received == member.previous_member_epoch && owns_subset {
        return Ok(());
    }
    let relation = if received > member.member_epoch {
        "greater"
    } else {
        "smaller"
    };
    Err(HeartbeatError {
        code: codes::FENCED_MEMBER_EPOCH,
        message: format!(
            "The consumer group member has a {relation} member epoch ({received}) than the one known by the group coordinator ({}). The member must abandon all its partitions and rejoin.",
            member.member_epoch
        ),
    })
}

/// What a heartbeat changed on its member.
#[derive(Default)]
struct MemberUpdate {
    changed: bool,
    names_changed: bool,
    regex_changed: bool,
}

/// The partitions of `epochs`, without the epochs they were assigned at.
fn partition_set(epochs: &PartitionEpochs) -> Partitions {
    epochs
        .iter()
        .map(|(topic, partitions)| (*topic, partitions.keys().copied().collect()))
        .collect()
}

/// Kafka's `CurrentAssignmentBuilder.subscribedTopicIds`: the ids of the
/// topics the member names or its regex matches, that exist.
fn subscribed_topic_ids(
    member: &ConsumerMember,
    metadata: &dyn TopicMetadata,
) -> BTreeSet<TopicId> {
    let mut names = member.subscribed_topic_names.clone();
    if !member.subscribed_topic_regex.is_empty() {
        names.extend(metadata.topics_matching(&member.subscribed_topic_regex));
    }
    names
        .iter()
        .filter_map(|name| metadata.topic_id(name))
        .map(TopicId::from)
        .collect()
}

/// Kafka's `ownsRevokedPartitions`: whether the heartbeat still reports a
/// partition of `pending`. A heartbeat that reports nothing owns them all.
fn owns_revoked(owned: Option<&Partitions>, pending: &PartitionEpochs) -> bool {
    owned.is_none_or(|owned| {
        owned.iter().any(|(topic, partitions)| {
            pending
                .get(topic)
                .is_some_and(|revoked| partitions.iter().any(|p| revoked.contains_key(p)))
        })
    })
}

/// Kafka 4.3's `CurrentAssignmentBuilder.updateCurrentAssignment`: after a
/// subscription change, the partitions of the topics the member no longer
/// subscribes to go to revocation at once and the member keeps its epoch;
/// partitions it no longer reports are simply dropped. `None` when no
/// partition goes.
fn update_current_assignment(
    member: &ConsumerMember,
    owned: Option<&Partitions>,
    subscribed: &BTreeSet<TopicId>,
) -> Option<ConsumerMember> {
    let (assigned, pending) = if subscribed.is_empty() && member.pending_revocation.is_empty() {
        (PartitionEpochs::new(), member.assigned.clone())
    } else {
        let mut assigned = member.assigned.clone();
        let mut pending = member.pending_revocation.clone();
        for (topic, partitions) in &member.assigned {
            if !subscribed.contains(topic) {
                assigned.remove(topic);
                pending
                    .entry(*topic)
                    .or_default()
                    .extend(partitions.iter().map(|(p, e)| (*p, *e)));
            }
        }
        (assigned, pending)
    };
    if assigned == member.assigned {
        return None;
    }
    let mut next = member.clone();
    next.previous_member_epoch = member.member_epoch;
    next.assigned = assigned;
    if !pending.is_empty() && owns_revoked(owned, &pending) {
        next.state = ConsumerMemberState::UnrevokedPartitions;
        next.pending_revocation = pending;
    }
    Some(next)
}
