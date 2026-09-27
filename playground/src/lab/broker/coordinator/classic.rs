//! The classic group protocol: `JoinGroup`, `SyncGroup`, `Heartbeat` and
//! `LeaveGroup` over generations (KIP-62), static membership (KIP-345), the
//! member-id handshake (KIP-394) and the leader's skipped assignment
//! (KIP-814).
//!
//! The transitions follow Kafka's `GroupMetadataManager` for classic groups:
//! `classicGroupJoinToClassicGroup`, `completeClassicGroupJoin`,
//! `classicGroupSyncToClassicGroup`, `classicGroupHeartbeatToClassicGroup`,
//! `classicGroupLeaveToClassicGroup` and the session, join and sync
//! expirations. Where Kafka iterates a hash map, this module iterates in
//! member id order, so every response is deterministic.
//!
//! A `JoinGroup` or `SyncGroup` that Kafka parks on a future is held here
//! under a [`HoldToken`]; the round that answers it pushes a
//! [`Completion`](super::Completion) for the broker to deliver.
//!
//! # Persisted value
//!
//! The record under `{"type":"classic_group","group":<id>}` is a
//! [`ClassicGroupValue`]: the generation, the protocol type and name, the
//! leader, and every member with its timeouts, its `JoinGroup` protocols and
//! its assignment (base64). It is written when a generation stabilizes, when
//! the group becomes empty, and when a static member replaces its member id
//! without a rebalance, as Kafka writes its `GroupMetadata` record.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use krabka_protocol::owned::{
    describe_groups_response::{DescribedGroup, DescribedGroupMember},
    heartbeat_request::HeartbeatRequest,
    join_group_request::JoinGroupRequest,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    leave_group_request::LeaveGroupRequest,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
    list_groups_response::ListedGroup,
    sync_group_request::SyncGroupRequest,
    sync_group_response::SyncGroupResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    AnyResponse, MemberKey, Pending, Shared,
    ids::{GroupId, HoldToken, MemberId},
    persist::{RecordKey, base64_bytes},
    timers::TimerKey,
};
use crate::lab::{codes, net::Millis};

/// The first `JoinGroup` version that requires a known member id from a
/// dynamic member (KIP-394).
const FIRST_KNOWN_MEMBER_ID_VERSION: i16 = 4;

/// The first `JoinGroup` version with `SkipAssignment` (KIP-814).
const FIRST_SKIP_ASSIGNMENT_VERSION: i16 = 9;

/// The first `LeaveGroup` version with a member list.
const FIRST_MEMBER_LIST_LEAVE_VERSION: i16 = 3;

/// Kafka's `CLASSIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS`: how long a member that
/// joined for the first time may wait in `JoinGroup` before it expires.
pub const NEW_MEMBER_JOIN_TIMEOUT_MS: Millis = 5 * 60 * 1000;

/// The `group_type` of a classic group in `ListGroups`.
pub const GROUP_TYPE: &str = "classic";

/// The four states of a classic group.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClassicState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl ClassicState {
    /// The state string `DescribeGroups` and `ListGroups` report.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
        }
    }
}

/// Kafka's `InitialDelayedJoin`: the delay that runs now and the part of the
/// group rebalance timeout still left to extend into.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct InitialDelayedJoin {
    pub delay: Millis,
    pub remaining: Millis,
}

/// One member of a classic group.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClassicMember {
    pub id: MemberId,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout: Millis,
    pub rebalance_timeout: Millis,
    /// The protocols of the member's last `JoinGroup`, in its order of
    /// preference.
    pub protocols: Vec<(String, Bytes)>,
    /// The member's metadata for the protocol the group selected.
    pub protocol_metadata: Bytes,
    /// The assignment the leader's `SyncGroup` installed.
    pub assignment: Bytes,
    /// The member joined in the current round for the first time.
    pub is_new: bool,
    /// The deadline the member's session timer is armed at.
    pub session_deadline: Millis,
    /// The `JoinGroup` requests held for this member.
    pub join_holds: Vec<HoldToken>,
    /// The `SyncGroup` requests held for this member.
    pub sync_holds: Vec<HoldToken>,
}

impl ClassicMember {
    fn is_static(&self) -> bool {
        self.instance_id.is_some()
    }
}

/// A classic group.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClassicGroup {
    pub group_id: GroupId,
    pub state: ClassicState,
    pub protocol_type: Option<String>,
    pub generation_id: i32,
    pub leader: Option<MemberId>,
    pub protocol_name: Option<String>,
    pub members: BTreeMap<MemberId, ClassicMember>,
    /// KIP-345: `group.instance.id` to the member id that holds it.
    pub static_members: BTreeMap<String, MemberId>,
    /// The member ids a `JoinGroup` v4+ handed out with `MEMBER_ID_REQUIRED`
    /// and that have not joined with them yet, with their expiry.
    pub pending_members: BTreeMap<MemberId, Millis>,
    /// The members whose `JoinGroup` arrived since the round opened.
    pub joined_this_round: BTreeSet<MemberId>,
    /// The deadline of the current round.
    pub rebalance_deadline: Option<Millis>,
    /// The current round opened from `Empty`.
    pub rebalance_from_empty: bool,
    pub initial_join: Option<InitialDelayedJoin>,
    /// A new member joined during the initial delay, so it extends once more.
    pub new_member_added: bool,
    /// The members of the current generation that have not sent `SyncGroup`.
    pub pending_sync: BTreeSet<MemberId>,
    pub sync_deadline: Option<Millis>,
}

/// The persisted form of a classic group.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ClassicGroupValue {
    pub generation: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: Option<MemberId>,
    pub members: Vec<ClassicMemberValue>,
}

/// The persisted form of a classic member.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ClassicMemberValue {
    pub id: MemberId,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: Millis,
    pub rebalance_timeout_ms: Millis,
    pub protocols: Vec<ProtocolValue>,
    #[serde(with = "base64_bytes")]
    pub assignment: Bytes,
}

/// One `JoinGroup` protocol of a persisted member.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ProtocolValue {
    pub name: String,
    #[serde(with = "base64_bytes")]
    pub metadata: Bytes,
}

/// The request-independent inputs of one `JoinGroup`.
#[derive(Clone, Copy)]
struct JoinContext<'a> {
    now: Millis,
    client: &'a MemberKey,
    req: &'a JoinGroupRequest,
    version: i16,
}

impl JoinContext<'_> {
    fn session_timeout(&self) -> Millis {
        u64::try_from(self.req.session_timeout_ms).unwrap_or(0)
    }

    /// The rebalance timeout, or the session timeout when the request has no
    /// `RebalanceTimeoutMs` (v0).
    fn rebalance_timeout(&self) -> Millis {
        u64::try_from(self.req.rebalance_timeout_ms).unwrap_or_else(|_| self.session_timeout())
    }

    fn protocols(&self) -> Vec<(String, Bytes)> {
        self.req
            .protocols
            .iter()
            .map(|p| (p.name.clone(), p.metadata.clone()))
            .collect()
    }
}

// ---- state helpers -----------------------------------------------------------

impl ClassicGroup {
    /// An empty group.
    #[must_use]
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: ClassicState::Empty,
            protocol_type: None,
            generation_id: 0,
            leader: None,
            protocol_name: None,
            members: BTreeMap::new(),
            static_members: BTreeMap::new(),
            pending_members: BTreeMap::new(),
            joined_this_round: BTreeSet::new(),
            rebalance_deadline: None,
            rebalance_from_empty: false,
            initial_join: None,
            new_member_added: false,
            pending_sync: BTreeSet::new(),
            sync_deadline: None,
        }
    }

    /// Kafka's `ClassicGroup.validateMember`: an instance id nobody holds is
    /// `UNKNOWN_MEMBER_ID`, an instance id another member holds is
    /// `FENCED_INSTANCE_ID`, and a member id the group does not hold is
    /// `UNKNOWN_MEMBER_ID`.
    fn validate_member(&self, member_id: &str, instance_id: Option<&str>) -> Result<(), i16> {
        if let Some(instance_id) = instance_id {
            match self.static_members.get(instance_id) {
                None => return Err(codes::UNKNOWN_MEMBER_ID),
                Some(pinned) if pinned.as_str() != member_id => {
                    return Err(codes::FENCED_INSTANCE_ID);
                }
                Some(_) => {}
            }
        }
        if self.members.contains_key(member_id) {
            Ok(())
        } else {
            Err(codes::UNKNOWN_MEMBER_ID)
        }
    }

    /// Kafka's `ClassicGroup.supportsProtocols`.
    fn supports_protocols<'a>(
        &self,
        protocol_type: &str,
        mut names: impl Iterator<Item = &'a str>,
    ) -> bool {
        if self.state == ClassicState::Empty {
            return !protocol_type.is_empty() && names.next().is_some();
        }
        if self.protocol_type.as_deref() != Some(protocol_type) {
            return false;
        }
        let candidates = candidate_protocols(&self.members);
        names.any(|name| self.members.is_empty() || candidates.contains(name))
    }

    fn can_rebalance(&self) -> bool {
        matches!(
            self.state,
            ClassicState::Empty | ClassicState::Stable | ClassicState::CompletingRebalance
        )
    }

    /// Kafka's `ClassicGroup.rebalanceTimeoutMs`: the largest of the members'.
    fn rebalance_timeout(&self) -> Millis {
        self.members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or(0)
    }

    /// Kafka's `hasAllMembersJoined`: every member waits in `JoinGroup` and no
    /// `MEMBER_ID_REQUIRED` id is outstanding.
    fn has_all_members_joined(&self) -> bool {
        self.pending_members.is_empty()
            && self
                .members
                .keys()
                .all(|id| self.joined_this_round.contains(id))
    }

    /// Kafka's `maybeElectNewJoinedLeader`: the leader stays while it waits
    /// in `JoinGroup`; otherwise the smallest waiting member id takes over.
    fn maybe_elect_new_joined_leader(&mut self) -> bool {
        if let Some(leader) = &self.leader
            && self.members.contains_key(leader)
            && self.joined_this_round.contains(leader)
        {
            return true;
        }
        match self
            .members
            .keys()
            .find(|id| self.joined_this_round.contains(*id))
            .cloned()
        {
            Some(leader) => {
                self.leader = Some(leader);
                true
            }
            None => false,
        }
    }

    fn resolve_selected_protocol_metadata(&mut self, name: &str) {
        for member in self.members.values_mut() {
            if let Some((_, bytes)) = member.protocols.iter().find(|(n, _)| n == name) {
                member.protocol_metadata = bytes.clone();
            }
        }
    }

    /// Kafka's `ClassicGroup.add`, without the rebalance: pin the instance
    /// id, take the protocol type of the first member, make the member the
    /// leader when the group has none, and forget its pending id.
    fn insert_joining_member(&mut self, member: ClassicMember, protocol_type: &str) {
        if let Some(instance_id) = &member.instance_id {
            self.static_members
                .insert(instance_id.clone(), member.id.clone());
        }
        if self.members.is_empty() {
            self.protocol_type = Some(protocol_type.to_string());
        }
        if self.leader.is_none() {
            self.leader = Some(member.id.clone());
        }
        self.members.insert(member.id.clone(), member);
    }

    /// Remove a member from every index. The leader moves to the smallest
    /// remaining member id.
    fn remove_member_state(&mut self, member_id: &MemberId) -> Option<ClassicMember> {
        let member = self.members.remove(member_id)?;
        if let Some(instance_id) = &member.instance_id
            && self.static_members.get(instance_id) == Some(member_id)
        {
            self.static_members.remove(instance_id);
        }
        self.joined_this_round.remove(member_id);
        self.pending_sync.remove(member_id);
        if self.leader.as_ref() == Some(member_id) {
            self.leader = self.members.keys().next().cloned();
        }
        Some(member)
    }

    fn cancel_pending(&mut self, sh: &mut Shared, member_id: &MemberId) {
        if let Some(at) = self.pending_members.remove(member_id) {
            sh.timers.cancel(
                at,
                &TimerKey::ClassicPending {
                    group: self.group_id.clone(),
                    member: member_id.clone(),
                },
            );
        }
    }

    fn session_key(&self, member_id: &MemberId) -> TimerKey {
        TimerKey::ClassicSession {
            group: self.group_id.clone(),
            member: member_id.clone(),
        }
    }

    /// Arm a member's session timer at `at`, replacing the earlier deadline.
    fn arm_session(&mut self, sh: &mut Shared, member_id: &MemberId, at: Millis) {
        let key = self.session_key(member_id);
        if let Some(member) = self.members.get_mut(member_id) {
            sh.timers.rearm(Some(member.session_deadline), at, key);
            member.session_deadline = at;
        }
    }

    fn cancel_session(&self, sh: &mut Shared, member: &ClassicMember) {
        sh.timers
            .cancel(member.session_deadline, &self.session_key(&member.id));
    }

    fn set_rebalance_deadline(&mut self, sh: &mut Shared, at: Option<Millis>) {
        let key = TimerKey::ClassicJoin {
            group: self.group_id.clone(),
        };
        if let Some(old) = self.rebalance_deadline.take() {
            sh.timers.cancel(old, &key);
        }
        if let Some(at) = at {
            sh.timers.arm(at, key);
        }
        self.rebalance_deadline = at;
    }

    fn set_sync_deadline(&mut self, sh: &mut Shared, at: Option<Millis>) {
        let key = TimerKey::ClassicSync {
            group: self.group_id.clone(),
            generation: self.generation_id,
        };
        if let Some(old) = self.sync_deadline.take() {
            sh.timers.cancel(old, &key);
        }
        if let Some(at) = at {
            sh.timers.arm(at, key);
        }
        self.sync_deadline = at;
    }

    /// Cancel every timer of the group, ahead of dropping it.
    pub fn cancel_timers(&self, sh: &mut Shared) {
        for member in self.members.values() {
            self.cancel_session(sh, member);
        }
        for (member_id, at) in &self.pending_members {
            sh.timers.cancel(
                *at,
                &TimerKey::ClassicPending {
                    group: self.group_id.clone(),
                    member: member_id.clone(),
                },
            );
        }
        if let Some(at) = self.rebalance_deadline {
            sh.timers.cancel(
                at,
                &TimerKey::ClassicJoin {
                    group: self.group_id.clone(),
                },
            );
        }
        if let Some(at) = self.sync_deadline {
            sh.timers.cancel(
                at,
                &TimerKey::ClassicSync {
                    group: self.group_id.clone(),
                    generation: self.generation_id,
                },
            );
        }
    }

    /// The persisted form of the group.
    #[must_use]
    pub fn value(&self) -> ClassicGroupValue {
        ClassicGroupValue {
            generation: self.generation_id,
            protocol_type: self.protocol_type.clone(),
            protocol_name: self.protocol_name.clone(),
            leader: self.leader.clone(),
            members: self
                .members
                .values()
                .map(|m| ClassicMemberValue {
                    id: m.id.clone(),
                    instance_id: m.instance_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    session_timeout_ms: m.session_timeout,
                    rebalance_timeout_ms: m.rebalance_timeout,
                    protocols: m
                        .protocols
                        .iter()
                        .map(|(name, metadata)| ProtocolValue {
                            name: name.clone(),
                            metadata: metadata.clone(),
                        })
                        .collect(),
                    assignment: m.assignment.clone(),
                })
                .collect(),
        }
    }

    /// Rebuild a group from its persisted form at `now`. A group with members
    /// is `Stable`, as every persisted generation is; each member's session
    /// timer is armed from `now`, as Kafka's `onLoaded` does.
    pub fn from_value(
        group_id: GroupId,
        value: ClassicGroupValue,
        now: Millis,
        sh: &mut Shared,
    ) -> Self {
        let mut group = Self::new(group_id);
        group.generation_id = value.generation;
        group.protocol_type = value.protocol_type;
        group.protocol_name = value.protocol_name;
        group.leader = value.leader;
        for m in value.members {
            sh.observe_member_id(m.id.as_str());
            let protocols: Vec<(String, Bytes)> = m
                .protocols
                .into_iter()
                .map(|p| (p.name, p.metadata))
                .collect();
            let protocol_metadata = group
                .protocol_name
                .as_deref()
                .and_then(|name| protocols.iter().find(|(n, _)| n == name))
                .map(|(_, bytes)| bytes.clone())
                .unwrap_or_default();
            let session_deadline = now.saturating_add(m.session_timeout_ms);
            let member = ClassicMember {
                id: m.id.clone(),
                instance_id: m.instance_id.clone(),
                client_id: m.client_id,
                client_host: m.client_host,
                session_timeout: m.session_timeout_ms,
                rebalance_timeout: m.rebalance_timeout_ms,
                protocols,
                protocol_metadata,
                assignment: m.assignment,
                is_new: false,
                session_deadline,
                join_holds: Vec::new(),
                sync_holds: Vec::new(),
            };
            sh.timers.arm(session_deadline, group.session_key(&m.id));
            if let Some(instance_id) = m.instance_id {
                group.static_members.insert(instance_id, m.id.clone());
            }
            group.members.insert(m.id, member);
        }
        group.state = if group.members.is_empty() {
            ClassicState::Empty
        } else {
            ClassicState::Stable
        };
        group
    }

    /// The group as `ListGroups` lists it.
    #[must_use]
    pub fn listed(&self) -> ListedGroup {
        ListedGroup {
            group_id: self.group_id.as_str().to_string(),
            protocol_type: self.protocol_type.clone().unwrap_or_default(),
            group_state: self.state.as_str().to_string(),
            group_type: GROUP_TYPE.to_string(),
            ..Default::default()
        }
    }

    /// The group as `DescribeGroups` describes it: only a `Stable` group
    /// reports its protocol name and the members' metadata for it.
    #[must_use]
    pub fn describe(&self) -> DescribedGroup {
        let stable = self.state == ClassicState::Stable;
        DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: self.group_id.as_str().to_string(),
            group_state: self.state.as_str().to_string(),
            protocol_type: self.protocol_type.clone().unwrap_or_default(),
            protocol_data: if stable {
                self.protocol_name.clone().unwrap_or_default()
            } else {
                String::new()
            },
            members: self
                .members
                .values()
                .map(|m| DescribedGroupMember {
                    member_id: m.id.as_str().to_string(),
                    group_instance_id: m.instance_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    member_metadata: if stable {
                        m.protocol_metadata.clone()
                    } else {
                        Bytes::new()
                    },
                    member_assignment: m.assignment.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The group for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "type": GROUP_TYPE,
            "state": self.state.as_str(),
            "protocol_type": self.protocol_type,
            "protocol_name": self.protocol_name,
            "generation": self.generation_id,
            "leader": self.leader,
            "members": self.members.values().map(|m| json!({
                "member_id": m.id,
                "instance_id": m.instance_id,
                "client_id": m.client_id,
                "client_host": m.client_host,
                "session_timeout_ms": m.session_timeout,
                "rebalance_timeout_ms": m.rebalance_timeout,
                "protocols": m.protocols.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
                "assignment_bytes": m.assignment.len(),
                "awaiting_join": !m.join_holds.is_empty(),
                "awaiting_sync": !m.sync_holds.is_empty(),
                "session_deadline": m.session_deadline,
            })).collect::<Vec<_>>(),
            "pending_members": self.pending_members.keys().collect::<Vec<_>>(),
            "pending_sync": self.pending_sync.iter().collect::<Vec<_>>(),
            "rebalance_deadline": self.rebalance_deadline,
            "sync_deadline": self.sync_deadline,
        })
    }
}

/// The protocol names every member proposed.
fn candidate_protocols(members: &BTreeMap<MemberId, ClassicMember>) -> BTreeSet<&str> {
    let mut support: BTreeMap<&str, usize> = BTreeMap::new();
    for member in members.values() {
        let names: BTreeSet<&str> = member.protocols.iter().map(|(n, _)| n.as_str()).collect();
        for name in names {
            *support.entry(name).or_insert(0) += 1;
        }
    }
    support
        .into_iter()
        .filter(|&(_, count)| count == members.len())
        .map(|(name, _)| name)
        .collect()
}

/// Kafka's `ClassicGroup.selectProtocol`: each member votes for its most
/// preferred protocol among the names every member proposed, and the most
/// voted name wins; a tie goes to the smallest name.
fn select_protocol(members: &BTreeMap<MemberId, ClassicMember>) -> Option<String> {
    let candidates = candidate_protocols(members);
    let mut votes: BTreeMap<&str, usize> = BTreeMap::new();
    for member in members.values() {
        if let Some((name, _)) = member
            .protocols
            .iter()
            .find(|(name, _)| candidates.contains(name.as_str()))
        {
            *votes.entry(name.as_str()).or_insert(0) += 1;
        }
    }
    votes
        .into_iter()
        .max_by(|(a, va), (b, vb)| va.cmp(vb).then_with(|| b.cmp(a)))
        .map(|(name, _)| name.to_string())
}

fn persist(g: &ClassicGroup, sh: &mut Shared) {
    sh.persist(
        &RecordKey::ClassicGroup {
            group: g.group_id.clone(),
        },
        Some(&g.value()),
    );
}

/// A `JoinGroup` error as Kafka's coordinator builds it,
/// `new JoinGroupResponseData().setMemberId(..).setErrorCode(..)`: the
/// protocol name keeps the schema's default, the empty string.
fn error_response(error_code: i16, member_id: &str) -> JoinGroupResponse {
    JoinGroupResponse {
        error_code,
        member_id: member_id.to_string(),
        ..Default::default()
    }
}

/// A `JoinGroup` error whose protocol name Kafka sets to null: the answer
/// to a member that fails `validateMember`, and to the member a static
/// member replaced.
fn null_protocol_error(error_code: i16, member_id: &str) -> JoinGroupResponse {
    JoinGroupResponse {
        protocol_name: None,
        ..error_response(error_code, member_id)
    }
}

fn sync_error(error_code: i16) -> SyncGroupResponse {
    SyncGroupResponse {
        error_code,
        ..Default::default()
    }
}

/// The `JoinGroup` result of `member_id` for the current generation. The
/// leader gets the member list, in member id order; followers get none.
fn build_join_result(g: &ClassicGroup, member_id: &MemberId) -> JoinGroupResponse {
    let is_leader = g.leader.as_ref() == Some(member_id);
    let members = if is_leader {
        g.members
            .values()
            .map(|m| JoinGroupResponseMember {
                member_id: m.id.as_str().to_string(),
                group_instance_id: m.instance_id.clone(),
                metadata: m.protocol_metadata.clone(),
                ..Default::default()
            })
            .collect()
    } else {
        Vec::new()
    };
    JoinGroupResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        generation_id: g.generation_id,
        protocol_type: g.protocol_type.clone(),
        protocol_name: g.protocol_name.clone(),
        leader: g
            .leader
            .as_ref()
            .map(|l| l.as_str().to_string())
            .unwrap_or_default(),
        skip_assignment: false,
        member_id: member_id.as_str().to_string(),
        members,
        ..Default::default()
    }
}

/// One member's installed assignment with the group's protocol; outside
/// `Stable` it is `REBALANCE_IN_PROGRESS`.
fn read_current(g: &ClassicGroup, member_id: &MemberId) -> SyncGroupResponse {
    if g.state != ClassicState::Stable {
        return sync_error(codes::REBALANCE_IN_PROGRESS);
    }
    SyncGroupResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        protocol_type: g.protocol_type.clone(),
        protocol_name: g.protocol_name.clone(),
        assignment: g
            .members
            .get(member_id)
            .map(|m| m.assignment.clone())
            .unwrap_or_default(),
        ..Default::default()
    }
}

fn complete_join_holds(sh: &mut Shared, member: &mut ClassicMember, response: &JoinGroupResponse) {
    for token in member.join_holds.drain(..) {
        sh.complete(token, AnyResponse::JoinGroup(response.clone()));
    }
}

fn complete_sync_holds(sh: &mut Shared, member: &mut ClassicMember, response: &SyncGroupResponse) {
    for token in member.sync_holds.drain(..) {
        sh.complete(token, AnyResponse::SyncGroup(response.clone()));
    }
}

// ---- JoinGroup ---------------------------------------------------------------

/// Kafka's `classicGroupJoinToClassicGroup`. The caller has checked the
/// group id and the session timeout.
pub fn join(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    now: Millis,
    client: &MemberKey,
    req: &JoinGroupRequest,
    version: i16,
) -> Pending<JoinGroupResponse> {
    let ctx = JoinContext {
        now,
        client,
        req,
        version,
    };
    if req.member_id.is_empty() {
        join_new_member(g, sh, ctx)
    } else {
        join_existing_member(g, sh, ctx)
    }
}

/// Kafka's `classicGroupJoinNewMember`: a static member joins or replaces its
/// old member id at once; a dynamic member gets `MEMBER_ID_REQUIRED` at v4+.
fn join_new_member(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    ctx: JoinContext<'_>,
) -> Pending<JoinGroupResponse> {
    let req = ctx.req;
    if !g.supports_protocols(
        &req.protocol_type,
        req.protocols.iter().map(|p| p.name.as_str()),
    ) {
        return Pending::Ready(error_response(codes::INCONSISTENT_GROUP_PROTOCOL, ""));
    }
    // Kafka's `generateMemberId`: the instance id or the client id, a hyphen,
    // and a unique suffix. `kafka-consumer-groups --describe` prints it.
    let prefix = req
        .group_instance_id
        .as_deref()
        .unwrap_or(&ctx.client.client_id);
    let new_id = sh.new_member_id(prefix);
    if let Some(instance_id) = req.group_instance_id.as_deref() {
        if let Some(old_id) = g.static_members.get(instance_id).cloned() {
            return update_static_member(g, sh, ctx, (instance_id, old_id, new_id));
        }
        return add_member_then_rebalance(g, sh, ctx, &new_id);
    }
    if ctx.version >= FIRST_KNOWN_MEMBER_ID_VERSION {
        let expires = ctx.now.saturating_add(ctx.session_timeout());
        g.pending_members.insert(new_id.clone(), expires);
        sh.timers.arm(
            expires,
            TimerKey::ClassicPending {
                group: g.group_id.clone(),
                member: new_id.clone(),
            },
        );
        return Pending::Ready(error_response(codes::MEMBER_ID_REQUIRED, new_id.as_str()));
    }
    add_member_then_rebalance(g, sh, ctx, &new_id)
}

/// Kafka's `classicGroupJoinExistingMember`.
fn join_existing_member(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    ctx: JoinContext<'_>,
) -> Pending<JoinGroupResponse> {
    let req = ctx.req;
    let member_id = MemberId::from(req.member_id.as_str());
    if !g.supports_protocols(
        &req.protocol_type,
        req.protocols.iter().map(|p| p.name.as_str()),
    ) {
        return Pending::Ready(error_response(
            codes::INCONSISTENT_GROUP_PROTOCOL,
            &req.member_id,
        ));
    }
    if g.pending_members.contains_key(&member_id) {
        // A pending member is never static. Kafka's runtime answers the
        // `IllegalStateException` with `UNKNOWN_SERVER_ERROR` and no member
        // id.
        if req.group_instance_id.is_some() {
            return Pending::Ready(error_response(codes::UNKNOWN_SERVER_ERROR, ""));
        }
        g.cancel_pending(sh, &member_id);
        return add_member_then_rebalance(g, sh, ctx, &member_id);
    }
    if let Err(code) = g.validate_member(&req.member_id, req.group_instance_id.as_deref()) {
        return Pending::Ready(null_protocol_error(code, &req.member_id));
    }
    let unchanged = g
        .members
        .get(&member_id)
        .is_some_and(|m| m.protocols == ctx.protocols());
    let is_leader = g.leader.as_ref() == Some(&member_id);
    let rebalance = match g.state {
        ClassicState::PreparingRebalance => true,
        // A member that joins again with the same metadata, which it does
        // when it lost its `JoinGroup` response, gets the current generation.
        ClassicState::CompletingRebalance => !unchanged,
        // The leader's `JoinGroup` always rebalances, so it can react to
        // changes that do not show in member metadata, such as new topics.
        ClassicState::Stable => is_leader || !unchanged,
        ClassicState::Empty => {
            return Pending::Ready(error_response(codes::UNKNOWN_MEMBER_ID, &req.member_id));
        }
    };
    if rebalance {
        update_joining_member(g, ctx, &member_id);
        prepare_rebalance_or_complete_join(g, sh, ctx.now, &member_id)
    } else {
        Pending::Ready(build_join_result(g, &member_id))
    }
}

/// Kafka's `addMemberThenRebalanceOrCompleteJoin`. A new member cannot
/// heartbeat while its `JoinGroup` is parked, so its session timer is the
/// new-member join timeout.
fn add_member_then_rebalance(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    ctx: JoinContext<'_>,
    member_id: &MemberId,
) -> Pending<JoinGroupResponse> {
    let req = ctx.req;
    let protocols = ctx.protocols();
    let session_deadline = ctx.now.saturating_add(NEW_MEMBER_JOIN_TIMEOUT_MS);
    let member = ClassicMember {
        id: member_id.clone(),
        instance_id: req.group_instance_id.clone(),
        client_id: ctx.client.client_id.clone(),
        client_host: ctx.client.client_host.clone(),
        session_timeout: ctx.session_timeout(),
        rebalance_timeout: ctx.rebalance_timeout(),
        protocol_metadata: protocols
            .first()
            .map(|(_, bytes)| bytes.clone())
            .unwrap_or_default(),
        protocols,
        assignment: Bytes::new(),
        is_new: true,
        session_deadline,
        join_holds: Vec::new(),
        sync_holds: Vec::new(),
    };
    sh.timers.arm(session_deadline, g.session_key(member_id));
    // A new member during the initial delay extends it once more.
    if g.state == ClassicState::PreparingRebalance && g.rebalance_from_empty {
        g.new_member_added = true;
    }
    g.insert_joining_member(member, &req.protocol_type);
    prepare_rebalance_or_complete_join(g, sh, ctx.now, member_id)
}

/// Kafka's `ClassicGroup.updateMember`: the protocols and timeouts of the
/// member become the ones of its `JoinGroup`.
fn update_joining_member(g: &mut ClassicGroup, ctx: JoinContext<'_>, member_id: &MemberId) {
    if let Some(member) = g.members.get_mut(member_id) {
        let protocols = ctx.protocols();
        member.protocol_metadata = protocols
            .first()
            .map(|(_, bytes)| bytes.clone())
            .unwrap_or_default();
        member.protocols = protocols;
        member.rebalance_timeout = ctx.rebalance_timeout();
        member.session_timeout = ctx.session_timeout();
    }
}

/// Kafka's `updateStaticMemberThenRebalanceOrCompleteJoin`: the static member
/// takes `new_id` with everything else kept. In a `Stable` group whose
/// selected protocol does not change, the generation stays and the reply is
/// immediate; otherwise the group rebalances. Requests held under the old
/// member id are fenced.
fn update_static_member(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    ctx: JoinContext<'_>,
    ids: (&str, MemberId, MemberId),
) -> Pending<JoinGroupResponse> {
    let (instance_id, old_id, new_id) = ids;
    let current_leader = g.leader.clone();
    let Some(mut member) = g.members.remove(&old_id) else {
        return Pending::Ready(error_response(codes::UNKNOWN_SERVER_ERROR, ""));
    };
    g.cancel_session(sh, &member);
    complete_join_holds(
        sh,
        &mut member,
        &null_protocol_error(codes::FENCED_INSTANCE_ID, old_id.as_str()),
    );
    complete_sync_holds(sh, &mut member, &sync_error(codes::FENCED_INSTANCE_ID));
    g.joined_this_round.remove(&old_id);
    g.pending_sync.remove(&old_id);
    member.id = new_id.clone();
    member.is_new = false;
    member.session_deadline = ctx.now.saturating_add(member.session_timeout);
    sh.timers
        .arm(member.session_deadline, g.session_key(&new_id));
    g.members.insert(new_id.clone(), member);
    if g.leader.as_ref() == Some(&old_id) {
        g.leader = Some(new_id.clone());
    }
    g.static_members
        .insert(instance_id.to_string(), new_id.clone());
    update_joining_member(g, ctx, &new_id);
    match g.state {
        ClassicState::Stable if g.protocol_name == select_protocol(&g.members) => {
            persist(g, sh);
            let is_leader = g.leader.as_ref() == Some(&new_id);
            let mut result = build_join_result(g, &new_id);
            if ctx.version >= FIRST_SKIP_ASSIGNMENT_VERSION {
                // KIP-814: the leader gets the member list but must not
                // assign again.
                result.skip_assignment = is_leader;
            } else {
                result.members = Vec::new();
                result.leader = current_leader
                    .map(|l| l.as_str().to_string())
                    .unwrap_or_default();
            }
            Pending::Ready(result)
        }
        ClassicState::Stable | ClassicState::CompletingRebalance => {
            prepare_rebalance_or_complete_join(g, sh, ctx.now, &new_id)
        }
        ClassicState::PreparingRebalance => {
            let token = park_join(g, sh, &new_id);
            maybe_complete_join_phase(g, sh, ctx.now);
            Pending::Held(token)
        }
        // Kafka throws `IllegalStateException` here, which its runtime
        // answers with `UNKNOWN_SERVER_ERROR` and no member id.
        ClassicState::Empty => Pending::Ready(error_response(codes::UNKNOWN_SERVER_ERROR, "")),
    }
}

/// Hold the member's `JoinGroup` and count it as joined this round.
fn park_join(g: &mut ClassicGroup, sh: &mut Shared, member_id: &MemberId) -> HoldToken {
    let token = sh.hold();
    if let Some(member) = g.members.get_mut(member_id) {
        member.join_holds.push(token);
    }
    g.joined_this_round.insert(member_id.clone());
    token
}

/// Kafka's `maybePrepareRebalanceOrCompleteJoin`, with `member_id` waiting in
/// `JoinGroup` from here on. The `JoinGroup` is always held: a round that
/// completes at once pushes its completion before this returns.
fn prepare_rebalance_or_complete_join(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    now: Millis,
    member_id: &MemberId,
) -> Pending<JoinGroupResponse> {
    if g.can_rebalance() {
        let initial = g.state == ClassicState::Empty;
        prepare_rebalance(g, sh, now);
        let token = park_join(g, sh, member_id);
        // Kafka's `maybeCompleteJoinElseSchedule`: an initial round always
        // waits out its delay.
        if !initial && g.has_all_members_joined() {
            complete_join(g, sh, now);
        }
        Pending::Held(token)
    } else {
        let token = park_join(g, sh, member_id);
        maybe_complete_join_phase(g, sh, now);
        Pending::Held(token)
    }
}

/// Kafka's `maybeCompleteJoinPhase`: a round that did not open from `Empty`
/// completes as soon as every member waits in `JoinGroup`.
fn maybe_complete_join_phase(g: &mut ClassicGroup, sh: &mut Shared, now: Millis) {
    if g.state == ClassicState::PreparingRebalance
        && !g.rebalance_from_empty
        && g.has_all_members_joined()
    {
        complete_join(g, sh, now);
    }
}

/// Kafka's `prepareRebalance`: move to `PreparingRebalance` and arm the join
/// deadline. A round from `Empty` runs the initial delay; any other waits for
/// the group rebalance timeout. `SyncGroup` requests held from
/// `CompletingRebalance` get `REBALANCE_IN_PROGRESS` and the assignments are
/// reset.
fn prepare_rebalance(g: &mut ClassicGroup, sh: &mut Shared, now: Millis) {
    if g.state == ClassicState::CompletingRebalance {
        for member in g.members.values_mut() {
            member.assignment = Bytes::new();
            complete_sync_holds(sh, member, &sync_error(codes::REBALANCE_IN_PROGRESS));
        }
    }
    g.set_sync_deadline(sh, None);
    g.pending_sync.clear();
    let initial = g.state == ClassicState::Empty;
    let deadline = if initial {
        let delay = sh.config.initial_rebalance_delay_ms;
        g.initial_join = Some(InitialDelayedJoin {
            delay,
            remaining: g.rebalance_timeout().saturating_sub(delay),
        });
        now.saturating_add(delay)
    } else {
        g.initial_join = None;
        now.saturating_add(g.rebalance_timeout())
    };
    g.set_rebalance_deadline(sh, Some(deadline));
    g.new_member_added = false;
    g.rebalance_from_empty = initial;
    g.state = ClassicState::PreparingRebalance;
    g.joined_this_round.clear();
}

/// Kafka's `completeClassicGroupJoin`: drop the dynamic members that did not
/// join again, keep the leader while it joined, vote the protocol, and start
/// the next generation, answering every held `JoinGroup`.
fn complete_join(g: &mut ClassicGroup, sh: &mut Shared, now: Millis) {
    g.set_rebalance_deadline(sh, None);
    let removed: Vec<MemberId> = g
        .members
        .values()
        .filter(|m| !m.is_static() && !g.joined_this_round.contains(&m.id))
        .map(|m| m.id.clone())
        .collect();
    let rebalance_timeout = g.rebalance_timeout();
    for member_id in &removed {
        remove_member(g, sh, member_id);
    }
    if g.members.is_empty() {
        to_empty(g, sh);
        return;
    }
    if !g.maybe_elect_new_joined_leader() {
        // Nobody joined: wait another rebalance timeout, until the sessions
        // of the silent members expire.
        g.initial_join = None;
        g.set_rebalance_deadline(sh, Some(now.saturating_add(rebalance_timeout)));
        return;
    }
    let Some(chosen) = select_protocol(&g.members) else {
        // No protocol is common to every member, which the join gate keeps
        // a joined group out of. Answer the waiting members and start over.
        let response = error_response(codes::INCONSISTENT_GROUP_PROTOCOL, "");
        let ids: Vec<MemberId> = g.members.keys().cloned().collect();
        for member_id in &ids {
            if let Some(member) = g.members.get_mut(member_id) {
                complete_join_holds(sh, member, &response);
            }
            remove_member(g, sh, member_id);
        }
        to_empty(g, sh);
        return;
    };
    g.resolve_selected_protocol_metadata(&chosen);
    let leader = g
        .leader
        .clone()
        .filter(|l| g.members.contains_key(l))
        .or_else(|| g.members.keys().next().cloned());
    g.leader = leader;
    g.protocol_name = Some(chosen);
    g.generation_id = g.generation_id.saturating_add(1);
    g.state = ClassicState::CompletingRebalance;
    g.joined_this_round.clear();
    g.rebalance_from_empty = false;
    g.initial_join = None;
    g.new_member_added = false;
    let ids: Vec<MemberId> = g.members.keys().cloned().collect();
    for member_id in &ids {
        let response = build_join_result(g, member_id);
        let session_timeout = g.members[member_id].session_timeout;
        g.arm_session(sh, member_id, now.saturating_add(session_timeout));
        if let Some(member) = g.members.get_mut(member_id) {
            member.is_new = false;
            complete_join_holds(sh, member, &response);
        }
        g.pending_sync.insert(member_id.clone());
    }
    g.set_sync_deadline(sh, Some(now.saturating_add(rebalance_timeout)));
}

/// The group is empty: start the next generation in `Empty` and persist it,
/// as Kafka's `completeClassicGroupJoin` does for a group nobody is in.
fn to_empty(g: &mut ClassicGroup, sh: &mut Shared) {
    g.set_rebalance_deadline(sh, None);
    g.set_sync_deadline(sh, None);
    g.generation_id = g.generation_id.saturating_add(1);
    g.state = ClassicState::Empty;
    g.leader = None;
    g.protocol_name = None;
    g.joined_this_round.clear();
    g.pending_sync.clear();
    g.rebalance_from_empty = false;
    g.initial_join = None;
    g.new_member_added = false;
    persist(g, sh);
}

/// Remove a member: its held requests get `UNKNOWN_MEMBER_ID`, its session
/// timer is cancelled and every index forgets it. The group state does not
/// change; [`after_membership_change`] does that.
fn remove_member(g: &mut ClassicGroup, sh: &mut Shared, member_id: &MemberId) {
    let Some(mut member) = g.remove_member_state(member_id) else {
        return;
    };
    g.cancel_session(sh, &member);
    complete_join_holds(
        sh,
        &mut member,
        &error_response(codes::UNKNOWN_MEMBER_ID, ""),
    );
    complete_sync_holds(sh, &mut member, &sync_error(codes::UNKNOWN_MEMBER_ID));
}

/// The transition after members left, as Kafka's leave and expiration paths
/// run it: a live group rebalances, a round in progress may complete, and a
/// group nobody is in becomes `Empty`.
fn after_membership_change(g: &mut ClassicGroup, sh: &mut Shared, now: Millis) {
    if g.members.is_empty() {
        if g.state != ClassicState::Empty {
            to_empty(g, sh);
        }
        return;
    }
    match g.state {
        ClassicState::Stable | ClassicState::CompletingRebalance => prepare_rebalance(g, sh, now),
        ClassicState::PreparingRebalance => maybe_complete_join_phase(g, sh, now),
        ClassicState::Empty => {}
    }
}

// ---- SyncGroup ----------------------------------------------------------------

/// Kafka's `classicGroupSyncToClassicGroup`.
pub fn sync(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    now: Millis,
    req: &SyncGroupRequest,
) -> Pending<SyncGroupResponse> {
    if let Err(code) = validate_sync(g, req) {
        return Pending::Ready(sync_error(code));
    }
    let member_id = MemberId::from(req.member_id.as_str());
    match g.state {
        // Every `SyncGroup` in `PreparingRebalance` is answered at once, the
        // leader's included: a late leader of the previous round must not end
        // the round that gathers members now.
        ClassicState::PreparingRebalance | ClassicState::Empty => {
            Pending::Ready(sync_error(codes::REBALANCE_IN_PROGRESS))
        }
        ClassicState::CompletingRebalance => {
            g.pending_sync.remove(&member_id);
            if g.leader.as_ref() == Some(&member_id) {
                install_leader_assignments(g, sh, now, req);
                if g.pending_sync.is_empty() {
                    g.set_sync_deadline(sh, None);
                }
                Pending::Ready(read_current(g, &member_id))
            } else {
                let token = sh.hold();
                if let Some(member) = g.members.get_mut(&member_id) {
                    member.sync_holds.push(token);
                }
                Pending::Held(token)
            }
        }
        ClassicState::Stable => {
            g.pending_sync.remove(&member_id);
            if g.pending_sync.is_empty() {
                g.set_sync_deadline(sh, None);
            }
            Pending::Ready(read_current(g, &member_id))
        }
    }
}

/// Kafka's `validateSyncGroup`: the member and instance, the generation,
/// then the protocol type and name against the group's.
fn validate_sync(g: &ClassicGroup, req: &SyncGroupRequest) -> Result<(), i16> {
    g.validate_member(&req.member_id, req.group_instance_id.as_deref())?;
    if g.generation_id != req.generation_id {
        return Err(codes::ILLEGAL_GENERATION);
    }
    let inconsistent = |requested: Option<&str>, group: Option<&str>| matches!((requested, group), (Some(r), Some(g)) if r != g);
    if inconsistent(req.protocol_type.as_deref(), g.protocol_type.as_deref())
        || inconsistent(req.protocol_name.as_deref(), g.protocol_name.as_deref())
    {
        return Err(codes::INCONSISTENT_GROUP_PROTOCOL);
    }
    Ok(())
}

/// Install the leader's assignments over every member, a member the leader
/// omitted getting an empty one, persist the generation, and answer the held
/// followers. Every member's session timer restarts, as Kafka's
/// `propagateAssignment` does.
fn install_leader_assignments(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    now: Millis,
    req: &SyncGroupRequest,
) {
    let supplied: BTreeMap<&str, &Bytes> = req
        .assignments
        .iter()
        .map(|a| (a.member_id.as_str(), &a.assignment))
        .collect();
    for (member_id, member) in &mut g.members {
        member.assignment = supplied
            .get(member_id.as_str())
            .map_or_else(Bytes::new, |bytes| (*bytes).clone());
    }
    g.state = ClassicState::Stable;
    persist(g, sh);
    let ids: Vec<MemberId> = g.members.keys().cloned().collect();
    for member_id in &ids {
        let response = read_current(g, member_id);
        let session_timeout = g.members[member_id].session_timeout;
        g.arm_session(sh, member_id, now.saturating_add(session_timeout));
        if let Some(member) = g.members.get_mut(member_id) {
            complete_sync_holds(sh, member, &response);
        }
    }
}

// ---- Heartbeat and LeaveGroup ----------------------------------------------------

/// Kafka's `classicGroupHeartbeatToClassicGroup`: the error code, with the
/// member's session restarted in every state with members.
pub fn heartbeat(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    now: Millis,
    req: &HeartbeatRequest,
) -> i16 {
    if let Err(code) = g.validate_member(&req.member_id, req.group_instance_id.as_deref()) {
        return code;
    }
    if g.generation_id != req.generation_id {
        return codes::ILLEGAL_GENERATION;
    }
    let code = match g.state {
        ClassicState::Empty => return codes::UNKNOWN_MEMBER_ID,
        ClassicState::PreparingRebalance => codes::REBALANCE_IN_PROGRESS,
        ClassicState::CompletingRebalance | ClassicState::Stable => codes::NONE,
    };
    let member_id = MemberId::from(req.member_id.as_str());
    let session_timeout = g.members[&member_id].session_timeout;
    g.arm_session(sh, &member_id, now.saturating_add(session_timeout));
    code
}

/// Kafka's `classicGroupLeaveToClassicGroup`: each identity is resolved
/// through the instance index or the member index and removed, then the
/// group rebalances or completes its round. Below v3 the one member id of the
/// request is the only identity, and the response carries only the top-level
/// error.
pub fn leave(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    now: Millis,
    req: &LeaveGroupRequest,
    version: i16,
) -> LeaveGroupResponse {
    let identities = leave_identities(req, version);
    let mut members = Vec::with_capacity(identities.len());
    let mut any_removed = false;
    for (member_id, instance_id) in identities {
        let resolved: Result<Option<MemberId>, i16> =
            match (instance_id.as_deref(), member_id.as_str()) {
                (Some(instance), "") => g
                    .static_members
                    .get(instance)
                    .cloned()
                    .map(Some)
                    .ok_or(codes::UNKNOWN_MEMBER_ID),
                (Some(instance), id) => match g.static_members.get(instance) {
                    Some(pinned) if pinned.as_str() == id => Ok(Some(pinned.clone())),
                    Some(_) => Err(codes::FENCED_INSTANCE_ID),
                    None => Err(codes::UNKNOWN_MEMBER_ID),
                },
                (None, id) => {
                    let id = MemberId::from(id);
                    if g.pending_members.contains_key(&id) {
                        g.cancel_pending(sh, &id);
                        Ok(None)
                    } else if g.members.contains_key(&id) {
                        Ok(Some(id))
                    } else {
                        Err(codes::UNKNOWN_MEMBER_ID)
                    }
                }
            };
        let error_code = match resolved {
            Ok(Some(id)) => {
                remove_member(g, sh, &id);
                any_removed = true;
                codes::NONE
            }
            Ok(None) => {
                any_removed = true;
                codes::NONE
            }
            Err(code) => code,
        };
        members.push(MemberResponse {
            member_id,
            group_instance_id: instance_id,
            error_code,
            ..Default::default()
        });
    }
    if any_removed {
        after_membership_change(g, sh, now);
    }
    leave_response(members, version)
}

/// `LeaveGroup` on a group that holds none of the members it names: every
/// identity is `UNKNOWN_MEMBER_ID`.
#[must_use]
pub fn leave_unknown_members(req: &LeaveGroupRequest, version: i16) -> LeaveGroupResponse {
    let members = leave_identities(req, version)
        .into_iter()
        .map(|(member_id, group_instance_id)| MemberResponse {
            member_id,
            group_instance_id,
            error_code: codes::UNKNOWN_MEMBER_ID,
            ..Default::default()
        })
        .collect();
    leave_response(members, version)
}

/// The identities a `LeaveGroup` names: the member list from v3, the one
/// member id of the request before.
fn leave_identities(req: &LeaveGroupRequest, version: i16) -> Vec<(String, Option<String>)> {
    if version >= FIRST_MEMBER_LIST_LEAVE_VERSION {
        req.members
            .iter()
            .map(|m| (m.member_id.clone(), m.group_instance_id.clone()))
            .collect()
    } else {
        vec![(req.member_id.clone(), None)]
    }
}

/// The response over the per-member results, as Kafka's `LeaveGroupResponse`
/// shapes it: from v3 the member list under a top-level `NONE`; before v3 no
/// list, and the first member's error at the top level.
fn leave_response(members: Vec<MemberResponse>, version: i16) -> LeaveGroupResponse {
    if version >= FIRST_MEMBER_LIST_LEAVE_VERSION {
        LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            members,
            ..Default::default()
        }
    } else {
        LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: members.first().map_or(codes::NONE, |m| m.error_code),
            members: Vec::new(),
            ..Default::default()
        }
    }
}

// ---- offsets -------------------------------------------------------------------

/// Kafka's `ClassicGroup.validateOffsetCommit` for an `OffsetCommit`.
///
/// A negative generation commits on an `Empty` group: the admin client or a
/// consumer without group management. A member id, instance id or generation
/// at or above zero goes through the member check and must name the current
/// generation. A commit that names none of them on a group with members is
/// `UNKNOWN_MEMBER_ID`, and a valid member gets `REBALANCE_IN_PROGRESS` while
/// the group is `CompletingRebalance`.
pub fn validate_offset_commit(
    g: &ClassicGroup,
    member_id: &str,
    instance_id: Option<&str>,
    generation_id: i32,
) -> Result<(), i16> {
    let empty = g.state == ClassicState::Empty;
    if generation_id < 0 && empty {
        return Ok(());
    }
    if generation_id >= 0 || !member_id.is_empty() || instance_id.is_some() {
        g.validate_member(member_id, instance_id)?;
        if generation_id != g.generation_id {
            return Err(codes::ILLEGAL_GENERATION);
        }
    } else if !empty {
        return Err(codes::UNKNOWN_MEMBER_ID);
    }
    if g.state == ClassicState::CompletingRebalance {
        return Err(codes::REBALANCE_IN_PROGRESS);
    }
    Ok(())
}

/// A commit restarts the committer's session while the group is `Stable` or
/// `PreparingRebalance`, as Kafka's `OffsetMetadataManager` does.
pub fn refresh_committer_session(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    now: Millis,
    member_id: &str,
) {
    if !matches!(
        g.state,
        ClassicState::Stable | ClassicState::PreparingRebalance
    ) {
        return;
    }
    let member_id = MemberId::from(member_id);
    if let Some(session_timeout) = g.members.get(&member_id).map(|m| m.session_timeout) {
        g.arm_session(sh, &member_id, now.saturating_add(session_timeout));
    }
}

// ---- timers -------------------------------------------------------------------

/// Kafka's `onUnloaded` for a classic group: every held `JoinGroup` and
/// `SyncGroup` gets `NOT_COORDINATOR`.
pub fn unload(g: &mut ClassicGroup, sh: &mut Shared) {
    for member in g.members.values_mut() {
        let join = error_response(codes::NOT_COORDINATOR, member.id.as_str());
        complete_join_holds(sh, member, &join);
        complete_sync_holds(sh, member, &sync_error(codes::NOT_COORDINATOR));
    }
}

/// A member's session timer fired at `at`. A member that waits in `JoinGroup`
/// or `SyncGroup` cannot heartbeat and does not expire, unless it joined for
/// the first time in this round and its join timeout passed.
pub fn session_expired(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    member_id: &MemberId,
    at: Millis,
    now: Millis,
) {
    let Some(member) = g.members.get(member_id) else {
        return;
    };
    if member.session_deadline != at {
        return;
    }
    let satisfied =
        !member.is_new && (!member.join_holds.is_empty() || !member.sync_holds.is_empty());
    if satisfied {
        return;
    }
    remove_member(g, sh, member_id);
    after_membership_change(g, sh, now);
}

/// A `MEMBER_ID_REQUIRED` id that never joined expires.
pub fn pending_expired(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    member_id: &MemberId,
    at: Millis,
    now: Millis,
) {
    if g.pending_members.get(member_id) != Some(&at) {
        return;
    }
    g.pending_members.remove(member_id);
    maybe_complete_join_phase(g, sh, now);
}

/// The join deadline fired at `at`: extend the initial delay once more when a
/// new member joined during it, otherwise complete the round.
pub fn join_deadline_fired(g: &mut ClassicGroup, sh: &mut Shared, at: Millis, now: Millis) {
    if g.rebalance_deadline != Some(at) || g.state != ClassicState::PreparingRebalance {
        return;
    }
    if let Some(InitialDelayedJoin { delay, remaining }) = g.initial_join
        && g.new_member_added
        && remaining > 0
    {
        g.new_member_added = false;
        let next = sh.config.initial_rebalance_delay_ms.min(remaining);
        g.initial_join = Some(InitialDelayedJoin {
            delay: next,
            remaining: remaining.saturating_sub(delay),
        });
        g.set_rebalance_deadline(sh, Some(now.saturating_add(next)));
        return;
    }
    complete_join(g, sh, now);
}

/// The sync deadline of `generation` fired: the members that never sent
/// `SyncGroup` leave and the group rebalances.
pub fn sync_deadline_fired(
    g: &mut ClassicGroup,
    sh: &mut Shared,
    at: Millis,
    now: Millis,
    generation: i32,
) {
    if g.sync_deadline != Some(at) || g.generation_id != generation {
        return;
    }
    g.sync_deadline = None;
    if !matches!(
        g.state,
        ClassicState::CompletingRebalance | ClassicState::Stable
    ) || g.pending_sync.is_empty()
    {
        return;
    }
    let late: Vec<MemberId> = g.pending_sync.iter().cloned().collect();
    for member_id in &late {
        remove_member(g, sh, member_id);
    }
    after_membership_change(g, sh, now);
}
