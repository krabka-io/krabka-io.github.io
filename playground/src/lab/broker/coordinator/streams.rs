//! KIP-1071 streams groups: `StreamsGroupHeartbeat` and
//! `StreamsGroupDescribe`.
//!
//! A streams group runs the epoch mechanics of KIP-848 over tasks instead of
//! partitions: an active, a standby and a warmup role, each a set of
//! `(subtopology, partition)` tasks. The first join registers the topology
//! with its epoch. Every heartbeat configures the topology against the
//! cluster's topics ([`streams_topology`](super::streams_topology)); while a
//! source or internal topic is missing the group is `NotReady`, every
//! response carries the status, and the internal topics to create go back to
//! the broker. A ready topology gets a target from the sticky task assignor
//! ([`streams_assignor`](super::streams_assignor)), and each member
//! reconciles toward it in its own heartbeats as Kafka's streams
//! `CurrentAssignmentBuilder` does: tasks to revoke first, then the tasks
//! no other member (or no other member of the same process, for a standby)
//! still holds.
//!
//! The transitions follow Kafka 4.3's
//! `GroupMetadataManager.streamsGroupHeartbeat` and `streamsGroupLeave`, the
//! session and rebalance timeouts, static membership, the shutdown request
//! and the endpoint information for interactive queries. A member that joins
//! an empty group starts `group.streams.initial.rebalance.delay.ms`, during
//! which members reconcile toward an empty target; after it, a new target is
//! computed at most once per `group.streams.assignment.interval.ms`. Both
//! delays put `ASSIGNMENT_DELAYED` on the response.
//!
//! # Persisted value
//!
//! The record under `{"type":"streams_group","group":<id>}` is a
//! [`StreamsGroupValue`]: the group and target epochs, the time of the last
//! target, the topology, every member with its metadata, epochs,
//! reconciliation state, tasks and changelog offsets, the target assignment
//! and the partition counts the topology was last configured against. It is
//! written after every heartbeat that changed something. The initial
//! rebalance delay is a timer, which a load does not restore.

use std::collections::{BTreeMap, BTreeSet};

use krabka_protocol::owned::{
    common::{
        streams_group_describe_response::{
            assignment::Assignment as DescribedAssignment, endpoint::Endpoint as DescribedEndpoint,
            key_value::KeyValue as DescribedKeyValue, task_ids::TaskIds as DescribedTaskIds,
            task_offset::TaskOffset as DescribedTaskOffset,
        },
        streams_group_heartbeat_request::{
            task_ids::TaskIds as RequestTaskIds, task_offset::TaskOffset,
        },
        streams_group_heartbeat_response::{
            endpoint::Endpoint, status::Status, task_ids::TaskIds, topic_partition::TopicPartition,
        },
    },
    list_groups_response::ListedGroup,
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_describe_response::{
        DescribedGroup, Member as DescribedMember, StreamsGroupDescribeResponse,
    },
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::{EndpointToPartitions, StreamsGroupHeartbeatResponse},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    Coordinator, Group, MemberKey, Shared, TopicMetadata, can_compute_next_target,
    ids::{GroupId, MemberId},
    is_blank,
    offsets::FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION,
    persist::RecordKey,
    streams_assignor::{self, AssignorInput, AssignorMember, Task},
    streams_topology::{
        self, ASSIGNMENT_DELAYED, ConfiguredTopology, InternalTopicToCreate, SHUTDOWN_APPLICATION,
        STALE_TOPOLOGY, StoredTopology,
    },
    timers::TimerKey,
};
use crate::lab::{codes, net::Millis};

/// The member epoch of a heartbeat that leaves the group.
pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

/// The member epoch of a static member that leaves for a while.
pub const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// The group and target epoch of a new group, as Kafka's `StreamsGroup`
/// constructor sets them; the first join moves the group epoch past it.
pub const INITIAL_EPOCH: i32 = 1;

/// The `group_type` and `protocol_type` of a streams group.
pub const GROUP_TYPE: &str = "streams";

/// The tasks of one role: subtopology to partitions.
pub type TaskMap = BTreeMap<String, BTreeSet<i32>>;

/// The tasks of the three roles.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct RoleTasks {
    pub active: TaskMap,
    pub standby: TaskMap,
    pub warmup: TaskMap,
}

impl RoleTasks {
    fn is_empty(&self) -> bool {
        self.active.is_empty() && self.standby.is_empty() && self.warmup.is_empty()
    }

    /// Kafka's `TasksTuple.containsAny`: a task of any role is in both.
    fn contains_any(&self, other: &Self) -> bool {
        let overlap = |a: &TaskMap, b: &TaskMap| {
            a.iter()
                .any(|(s, ps)| b.get(s).is_some_and(|o| ps.iter().any(|p| o.contains(p))))
        };
        overlap(&self.active, &other.active)
            || overlap(&self.standby, &other.standby)
            || overlap(&self.warmup, &other.warmup)
    }
}

fn tasks_of(map: &TaskMap) -> impl Iterator<Item = Task> + '_ {
    map.iter()
        .flat_map(|(s, ps)| ps.iter().map(move |p| (s.clone(), *p)))
}

fn task_map(tasks: impl IntoIterator<Item = Task>) -> TaskMap {
    let mut map = TaskMap::new();
    for (s, p) in tasks {
        map.entry(s).or_default().insert(p);
    }
    map
}

fn task_ids_to_map(ids: &[RequestTaskIds]) -> TaskMap {
    let mut map = TaskMap::new();
    for t in ids {
        map.entry(t.subtopology_id.clone())
            .or_default()
            .extend(t.partitions.iter().copied());
    }
    map
}

fn map_to_task_ids(map: &TaskMap) -> Vec<TaskIds> {
    map.iter()
        .map(|(s, ps)| TaskIds {
            subtopology_id: s.clone(),
            partitions: ps.iter().copied().collect(),
            ..Default::default()
        })
        .collect()
}

fn map_to_described_task_ids(map: &TaskMap) -> Vec<DescribedTaskIds> {
    map.iter()
        .map(|(s, ps)| DescribedTaskIds {
            subtopology_id: s.clone(),
            partitions: ps.iter().copied().collect(),
            ..Default::default()
        })
        .collect()
}

/// Where a member stands in its reconciliation toward the target.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum StreamsMemberState {
    Stable,
    UnrevokedTasks,
    UnreleasedTasks,
}

impl StreamsMemberState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "Stable",
            Self::UnrevokedTasks => "UnrevokedTasks",
            Self::UnreleasedTasks => "UnreleasedTasks",
        }
    }
}

/// One member of a streams group.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StreamsMember {
    pub id: MemberId,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub process_id: String,
    pub user_endpoint: Option<(String, u16)>,
    pub client_tags: BTreeMap<String, String>,
    pub topology_epoch: i32,
    pub rebalance_timeout_ms: Millis,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: StreamsMemberState,
    pub tasks: RoleTasks,
    pub pending_revocation: RoleTasks,
    /// Changelog offsets by subtopology and partition.
    pub task_offsets: BTreeMap<String, BTreeMap<i32, i64>>,
    pub task_end_offsets: BTreeMap<String, BTreeMap<i32, i64>>,
    #[serde(skip)]
    pub session_deadline: Millis,
    #[serde(skip)]
    pub rebalance_deadline: Option<(Millis, i32)>,
}

/// A streams group.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StreamsGroup {
    pub group_id: GroupId,
    pub group_epoch: i32,
    pub target_epoch: i32,
    /// When the target was last computed; `None` before the first.
    pub assignment_timestamp: Option<Millis>,
    /// The end of the initial rebalance delay while it runs.
    pub initial_delay_deadline: Option<Millis>,
    pub members: BTreeMap<MemberId, StreamsMember>,
    pub instance_to_member: BTreeMap<String, MemberId>,
    pub topology: Option<StoredTopology>,
    pub target: BTreeMap<MemberId, RoleTasks>,
    /// The partition counts the topology was last configured against.
    pub metadata_signature: BTreeMap<String, Option<i32>>,
    pub configured: Option<ConfiguredTopology>,
    dirty: bool,
    pub shutdown_requested_by: Option<MemberId>,
    pub endpoint_information_epoch: i32,
}

/// The persisted form of a streams group.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StreamsGroupValue {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub assignment_timestamp: Option<Millis>,
    pub members: Vec<StreamsMember>,
    pub topology: Option<StoredTopology>,
    pub target: BTreeMap<MemberId, RoleTasks>,
    pub metadata_signature: BTreeMap<String, Option<i32>>,
}

/// The process ids that hold each task, over the tasks and the tasks pending
/// revocation of every member.
#[derive(Default)]
struct TaskOwners {
    active: BTreeMap<Task, String>,
    standby: BTreeMap<Task, BTreeSet<String>>,
    warmup: BTreeMap<Task, BTreeSet<String>>,
}

impl TaskOwners {
    fn of<'a>(members: impl Iterator<Item = &'a StreamsMember>) -> Self {
        let mut owners = Self::default();
        for m in members {
            for map in [&m.tasks.active, &m.pending_revocation.active] {
                for task in tasks_of(map) {
                    owners.active.insert(task, m.process_id.clone());
                }
            }
            for map in [&m.tasks.standby, &m.pending_revocation.standby] {
                for task in tasks_of(map) {
                    owners
                        .standby
                        .entry(task)
                        .or_default()
                        .insert(m.process_id.clone());
                }
            }
            for map in [&m.tasks.warmup, &m.pending_revocation.warmup] {
                for task in tasks_of(map) {
                    owners
                        .warmup
                        .entry(task)
                        .or_default()
                        .insert(m.process_id.clone());
                }
            }
        }
        owners
    }

    fn runs_elsewhere_in_process(&self, task: &Task, process: &str) -> bool {
        self.standby
            .get(task)
            .is_some_and(|ps| ps.contains(process))
            || self.warmup.get(task).is_some_and(|ps| ps.contains(process))
    }

    fn active_unreleased(&self, task: &Task, process: &str) -> bool {
        self.active.contains_key(task) || self.runs_elsewhere_in_process(task, process)
    }

    fn standby_unreleased(&self, task: &Task, process: &str) -> bool {
        self.active.get(task).is_some_and(|owner| owner == process)
            || self.runs_elsewhere_in_process(task, process)
    }
}

/// Kafka's `ASSIGNMENT_DELAYED` detail while the initial rebalance delay
/// holds the assignment back.
const INITIAL_DELAY_DETAIL: &str =
    "Assignment delayed due to the configured initial rebalance delay.";

/// Kafka's `ASSIGNMENT_DELAYED` detail while the assignment interval holds
/// the next assignment back.
const ASSIGNMENT_INTERVAL_DETAIL: &str =
    "Assignment delayed due to the configured assignment interval.";

/// What the assignment delays did to one heartbeat.
#[derive(Default)]
struct TargetStep {
    /// The initial rebalance delay runs: the member reconciles toward an
    /// empty target at the current target epoch.
    empty_target: bool,
    /// The `ASSIGNMENT_DELAYED` detail, when a delay held the target back.
    delayed: Option<&'static str>,
}

/// What the response of a heartbeat needs from before the heartbeat.
struct Before<'a> {
    /// The member's tasks before the heartbeat.
    tasks: &'a RoleTasks,
    /// Whether the group existed before the heartbeat.
    group_existed: bool,
    /// The `ASSIGNMENT_DELAYED` detail of the heartbeat.
    delayed: Option<&'static str>,
}

fn error_response(code: i16, message: Option<String>) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        error_code: code,
        error_message: message,
        status: Some(Vec::new()),
        ..Default::default()
    }
}

fn invalid_request(message: String) -> StreamsGroupHeartbeatResponse {
    error_response(codes::INVALID_REQUEST, Some(message))
}

/// Kafka 4.3's request checks, in its order: the source-topic regex check of
/// `throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures`, then
/// `throwIfStreamsGroupHeartbeatRequestIsInvalid` with `throwIfInvalidTopology`.
/// The error code and message of the first check the request fails.
fn validate_request(req: &StreamsGroupHeartbeatRequest) -> Result<(), (i16, String)> {
    let invalid = |message: &str| Err((codes::INVALID_REQUEST, message.to_string()));
    if req
        .topology
        .iter()
        .flat_map(|topology| topology.subtopologies.iter())
        .any(|subtopology| !subtopology.source_topic_regex.is_empty())
    {
        return invalid("Regular expressions for source topics are not supported yet.");
    }
    if is_blank(&req.member_id) {
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
    // Kafka's `throwIfNotEmptyCollection` refuses a null list too.
    let not_empty =
        |tasks: &Option<Vec<RequestTaskIds>>| tasks.as_ref().is_none_or(|t| !t.is_empty());
    match req.member_epoch {
        0 => {
            if req.rebalance_timeout_ms == -1 {
                return invalid("RebalanceTimeoutMs must be provided in first request.");
            }
            if not_empty(&req.active_tasks) {
                return invalid("ActiveTasks must be empty when (re-)joining.");
            }
            if not_empty(&req.standby_tasks) {
                return invalid("StandbyTasks must be empty when (re-)joining.");
            }
            if not_empty(&req.warmup_tasks) {
                return invalid("WarmupTasks must be empty when (re-)joining.");
            }
            let Some(topology) = &req.topology else {
                return invalid("Topology must be non-null when (re-)joining.");
            };
            if let Some(topic) = topology
                .subtopologies
                .iter()
                .flat_map(|subtopology| subtopology.state_changelog_topics.iter())
                .find(|topic| topic.partitions != 0)
            {
                return Err((
                    codes::STREAMS_INVALID_TOPOLOGY,
                    format!(
                        "Changelog topic {} must have an undefined partition count, but it is set to {}.",
                        topic.name, topic.partitions
                    ),
                ));
            }
        }
        LEAVE_GROUP_STATIC_MEMBER_EPOCH if req.instance_id.is_none() => {
            return invalid("InstanceId can't be null.");
        }
        epoch if epoch < LEAVE_GROUP_STATIC_MEMBER_EPOCH => {
            return invalid(&format!(
                "MemberEpoch is {epoch}, but must be greater than or equal to -2."
            ));
        }
        _ => {}
    }
    let given = [&req.active_tasks, &req.standby_tasks, &req.warmup_tasks];
    if given.iter().any(|t| t.is_some()) && given.iter().any(|t| t.is_none()) {
        return invalid("If one task-type is non-null, all must be non-null.");
    }
    if req.member_epoch != 0 && req.topology.is_some() {
        return invalid("Topology can only be provided when (re-)joining.");
    }
    Ok(())
}

/// The error code and message of the first request check a
/// `StreamsGroupHeartbeat` fails, which Kafka answers before it routes the
/// request to a coordinator.
pub fn request_error(req: &StreamsGroupHeartbeatRequest) -> Option<(i16, String)> {
    validate_request(req).err()
}

/// `StreamsGroupHeartbeat`, and the internal topics to create.
pub fn heartbeat(
    coord: &mut Coordinator,
    now: Millis,
    client: &MemberKey,
    req: &StreamsGroupHeartbeatRequest,
    metadata: &dyn TopicMetadata,
) -> (StreamsGroupHeartbeatResponse, Vec<InternalTopicToCreate>) {
    if let Err((code, message)) = validate_request(req) {
        return (error_response(code, Some(message)), Vec::new());
    }
    let group_id = GroupId::from(req.group_id.as_str());
    let joining = req.member_epoch == 0;
    let existed = coord.groups.contains_key(&group_id);
    match coord.groups.get(&group_id) {
        None if !joining => {
            return (
                error_response(
                    codes::GROUP_ID_NOT_FOUND,
                    Some(format!("Streams group {} not found.", req.group_id)),
                ),
                Vec::new(),
            );
        }
        Some(Group::Classic(classic)) if classic.members.is_empty() && joining => {
            coord.replace_empty_group(&group_id);
        }
        Some(Group::Classic(_) | Group::Consumer(_)) => {
            return (
                error_response(
                    codes::GROUP_ID_NOT_FOUND,
                    Some(format!("Group {} is not a streams group.", req.group_id)),
                ),
                Vec::new(),
            );
        }
        None | Some(Group::Streams(_)) => {}
    }
    let group = coord
        .groups
        .entry(group_id.clone())
        .or_insert_with(|| Group::Streams(StreamsGroup::new(group_id)));
    let Group::Streams(group) = group else {
        return (error_response(codes::GROUP_ID_NOT_FOUND, None), Vec::new());
    };
    let response = group.heartbeat(&mut coord.shared, now, client, req, metadata, existed);
    let to_create = if response.error_code == codes::NONE {
        group
            .configured
            .as_ref()
            .map(|c| c.internal_topics_to_create.values().cloned().collect())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    (response, to_create)
}

/// `StreamsGroupDescribe`.
pub fn describe(
    coord: &Coordinator,
    req: &StreamsGroupDescribeRequest,
) -> StreamsGroupDescribeResponse {
    let groups = req
        .group_ids
        .iter()
        .map(|group_id| {
            let not_found = |message: String| DescribedGroup {
                group_id: group_id.clone(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                error_message: Some(message),
                ..Default::default()
            };
            match coord.groups.get(&GroupId::from(group_id.as_str())) {
                Some(Group::Streams(g)) => g.describe(),
                Some(Group::Classic(_) | Group::Consumer(_)) => {
                    not_found(format!("Group {group_id} is not a streams group."))
                }
                None => not_found(format!("Group {group_id} not found.")),
            }
        })
        .collect();
    StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups,
        ..Default::default()
    }
}

impl StreamsGroup {
    /// An empty group at Kafka's initial epochs.
    #[must_use]
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            group_epoch: INITIAL_EPOCH,
            target_epoch: INITIAL_EPOCH,
            assignment_timestamp: None,
            initial_delay_deadline: None,
            members: BTreeMap::new(),
            instance_to_member: BTreeMap::new(),
            topology: None,
            target: BTreeMap::new(),
            metadata_signature: BTreeMap::new(),
            configured: None,
            dirty: false,
            shutdown_requested_by: None,
            endpoint_information_epoch: 0,
        }
    }

    fn session_key(&self, member_id: &MemberId) -> TimerKey {
        TimerKey::StreamsSession {
            group: self.group_id.clone(),
            member: member_id.clone(),
        }
    }

    fn rebalance_key(&self, member_id: &MemberId) -> TimerKey {
        TimerKey::StreamsRebalance {
            group: self.group_id.clone(),
            member: member_id.clone(),
        }
    }

    fn arm_session(&mut self, sh: &mut Shared, member_id: &MemberId, now: Millis) {
        let at = now.saturating_add(sh.config.streams_session_timeout_ms);
        let key = self.session_key(member_id);
        if let Some(member) = self.members.get_mut(member_id) {
            sh.timers.rearm(Some(member.session_deadline), at, key);
            member.session_deadline = at;
        }
    }

    fn cancel_member_timers(&self, sh: &mut Shared, member: &StreamsMember) {
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
        if let Some(at) = self.initial_delay_deadline {
            sh.timers.cancel(at, &self.initial_delay_key());
        }
    }

    fn initial_delay_key(&self) -> TimerKey {
        TimerKey::StreamsInitialRebalance {
            group: self.group_id.clone(),
        }
    }

    fn track_rebalance_timeout(&mut self, sh: &mut Shared, member_id: &MemberId, now: Millis) {
        let key = self.rebalance_key(member_id);
        let Some(member) = self.members.get_mut(member_id) else {
            return;
        };
        if let Some((at, _)) = member.rebalance_deadline.take() {
            sh.timers.cancel(at, &key);
        }
        if member.state == StreamsMemberState::UnrevokedTasks {
            let at = now.saturating_add(member.rebalance_timeout_ms);
            member.rebalance_deadline = Some((at, member.member_epoch));
            sh.timers.arm(at, key);
        }
    }

    fn insert_member(&mut self, sh: &mut Shared, member: StreamsMember) {
        let id = member.id.clone();
        sh.timers
            .arm(member.session_deadline, self.session_key(&id));
        if let Some(instance_id) = &member.instance_id {
            self.instance_to_member
                .insert(instance_id.clone(), id.clone());
        }
        self.members.insert(id, member);
    }

    fn persist(&self, sh: &mut Shared) {
        sh.persist(
            &RecordKey::StreamsGroup {
                group: self.group_id.clone(),
            },
            Some(&self.value()),
        );
    }

    /// The persisted form of the group.
    #[must_use]
    pub fn value(&self) -> StreamsGroupValue {
        StreamsGroupValue {
            group_epoch: self.group_epoch,
            target_epoch: self.target_epoch,
            assignment_timestamp: self.assignment_timestamp,
            members: self.members.values().cloned().collect(),
            topology: self.topology.clone(),
            target: self.target.clone(),
            metadata_signature: self.metadata_signature.clone(),
        }
    }

    /// Rebuild a group from its persisted form at `now`. The topology is
    /// configured again by the next heartbeat, which is the first call that
    /// sees the topics.
    pub fn from_value(
        group_id: GroupId,
        value: StreamsGroupValue,
        now: Millis,
        sh: &mut Shared,
    ) -> Self {
        let mut group = Self::new(group_id);
        group.group_epoch = value.group_epoch;
        group.target_epoch = value.target_epoch;
        group.assignment_timestamp = value.assignment_timestamp;
        group.topology = value.topology;
        group.target = value.target;
        group.metadata_signature = value.metadata_signature;
        for mut member in value.members {
            sh.observe_member_id(member.id.as_str());
            member.session_deadline = now.saturating_add(sh.config.streams_session_timeout_ms);
            let id = member.id.clone();
            group.insert_member(sh, member);
            group.track_rebalance_timeout(sh, &id, now);
        }
        group
    }

    /// Kafka's `StreamsGroup.state`: `Empty`, `NotReady` while the topology
    /// cannot be assigned, `Assigning` while the target is behind the group
    /// epoch, `Reconciling` while a member is not at the target, `Stable`
    /// otherwise.
    #[must_use]
    pub fn state_name(&self) -> &'static str {
        if self.members.is_empty() {
            "Empty"
        } else if !self
            .configured
            .as_ref()
            .is_some_and(ConfiguredTopology::is_ready)
        {
            "NotReady"
        } else if self.group_epoch > self.target_epoch {
            "Assigning"
        } else if self
            .members
            .values()
            .any(|m| m.state != StreamsMemberState::Stable || m.member_epoch != self.target_epoch)
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

    /// The group as `StreamsGroupDescribe` describes it. A ready topology is
    /// described with its decided partition counts; any other group describes
    /// the topology its members sent.
    #[must_use]
    pub fn describe(&self) -> DescribedGroup {
        let topology = match (&self.configured, &self.topology) {
            (Some(configured), _) if configured.is_ready() => Some(configured.describe()),
            (_, Some(stored)) => Some(stored.describe()),
            (_, None) => None,
        };
        let offsets = |offsets: &BTreeMap<String, BTreeMap<i32, i64>>| {
            offsets
                .iter()
                .flat_map(|(s, ps)| {
                    ps.iter().map(move |(p, o)| DescribedTaskOffset {
                        subtopology_id: s.clone(),
                        partition: *p,
                        offset: *o,
                        ..Default::default()
                    })
                })
                .collect()
        };
        let assignment = |tasks: &RoleTasks| DescribedAssignment {
            active_tasks: map_to_described_task_ids(&tasks.active),
            standby_tasks: map_to_described_task_ids(&tasks.standby),
            warmup_tasks: map_to_described_task_ids(&tasks.warmup),
            ..Default::default()
        };
        DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: self.group_id.as_str().to_string(),
            group_state: self.state_name().to_string(),
            group_epoch: self.group_epoch,
            assignment_epoch: self.target_epoch,
            topology,
            members: self
                .members
                .values()
                .map(|m| DescribedMember {
                    member_id: m.id.as_str().to_string(),
                    member_epoch: m.member_epoch,
                    instance_id: m.instance_id.clone(),
                    rack_id: m.rack_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    topology_epoch: m.topology_epoch,
                    process_id: m.process_id.clone(),
                    user_endpoint: m
                        .user_endpoint
                        .as_ref()
                        .map(|(host, port)| DescribedEndpoint {
                            host: host.clone(),
                            port: *port,
                            ..Default::default()
                        }),
                    client_tags: m
                        .client_tags
                        .iter()
                        .map(|(key, value)| DescribedKeyValue {
                            key: key.clone(),
                            value: value.clone(),
                            ..Default::default()
                        })
                        .collect(),
                    task_offsets: offsets(&m.task_offsets),
                    task_end_offsets: offsets(&m.task_end_offsets),
                    assignment: assignment(&m.tasks),
                    target_assignment: assignment(
                        self.target.get(&m.id).unwrap_or(&RoleTasks::default()),
                    ),
                    is_classic: false,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The group for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let role = |tasks: &RoleTasks| {
            json!({
                "active": tasks.active,
                "standby": tasks.standby,
                "warmup": tasks.warmup,
            })
        };
        json!({
            "type": GROUP_TYPE,
            "state": self.state_name(),
            "group_epoch": self.group_epoch,
            "assignment_epoch": self.target_epoch,
            "assignment_timestamp": self.assignment_timestamp,
            "initial_rebalance_deadline": self.initial_delay_deadline,
            "topology_epoch": self.topology.as_ref().map(|t| t.epoch),
            "status": self.configured.as_ref().and_then(|c| c.status.as_ref()).map(|(code, detail)| json!({"code": code, "detail": detail})),
            "internal_topics_to_create": self.configured.as_ref().map(|c| c.internal_topics_to_create.keys().collect::<Vec<_>>()).unwrap_or_default(),
            "tasks": self.configured.as_ref().map(ConfiguredTopology::number_of_tasks).unwrap_or_default(),
            "shutdown_requested_by": self.shutdown_requested_by,
            "endpoint_information_epoch": self.endpoint_information_epoch,
            "members": self.members.values().map(|m| json!({
                "member_id": m.id,
                "instance_id": m.instance_id,
                "rack_id": m.rack_id,
                "client_id": m.client_id,
                "client_host": m.client_host,
                "process_id": m.process_id,
                "user_endpoint": m.user_endpoint,
                "topology_epoch": m.topology_epoch,
                "member_epoch": m.member_epoch,
                "previous_member_epoch": m.previous_member_epoch,
                "state": m.state.as_str(),
                "tasks": role(&m.tasks),
                "pending_revocation": role(&m.pending_revocation),
                "target": role(self.target.get(&m.id).unwrap_or(&RoleTasks::default())),
                "task_offsets": m.task_offsets,
                "task_end_offsets": m.task_end_offsets,
                "session_deadline": m.session_deadline,
                "rebalance_deadline": m.rebalance_deadline.map(|(at, _)| at),
            })).collect::<Vec<_>>(),
        })
    }

    // ---- offsets ---------------------------------------------------------------

    /// Kafka's `StreamsGroup.validateOffsetCommit`, the rule of the consumer
    /// group.
    pub fn validate_offset_commit(
        &self,
        member_id: &str,
        member_epoch: i32,
        version: i16,
    ) -> Result<(), i16> {
        if member_epoch < 0 && self.members.is_empty() {
            return Ok(());
        }
        let member = self
            .members
            .get(member_id)
            .ok_or(codes::UNKNOWN_MEMBER_ID)?;
        if version < FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION {
            return Err(codes::UNSUPPORTED_VERSION);
        }
        if member_epoch == member.member_epoch {
            Ok(())
        } else {
            Err(codes::STALE_MEMBER_EPOCH)
        }
    }

    /// Kafka's `StreamsGroup.validateOffsetFetch` for a fetch that names a
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

    fn heartbeat(
        &mut self,
        sh: &mut Shared,
        now: Millis,
        client: &MemberKey,
        req: &StreamsGroupHeartbeatRequest,
        metadata: &dyn TopicMetadata,
        existed: bool,
    ) -> StreamsGroupHeartbeatResponse {
        if req.member_epoch == LEAVE_GROUP_MEMBER_EPOCH
            || req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH
        {
            return self.leave(sh, req);
        }
        let member_id = MemberId::from(req.member_id.as_str());
        let mut replaced = false;
        if let Some(instance_id) = req.instance_id.as_deref() {
            let existing = self.instance_to_member.get(instance_id).cloned();
            if let Some(response) = self.static_member_error(req, instance_id, existing.as_ref()) {
                return response;
            }
            if req.member_epoch == 0
                && let Some(previous) = existing
                && previous != member_id
            {
                if let Some(response) = self.topology_error(req, metadata) {
                    return response;
                }
                self.replace_static_member(sh, &previous, &member_id);
                replaced = true;
            }
        }
        let owned = owned_role_tasks(req);
        let Some(member) = self.members.get(&member_id) else {
            if req.member_epoch == 0 {
                return self.first_join(sh, now, client, req, metadata, existed);
            }
            return error_response(
                codes::UNKNOWN_MEMBER_ID,
                Some(format!(
                    "Member {member_id} is not a member of group {}.",
                    self.group_id
                )),
            );
        };
        if let Err(message) =
            Self::validate_heartbeat_epoch(member, req.member_epoch, owned.as_ref())
        {
            return error_response(codes::FENCED_MEMBER_EPOCH, Some(message));
        }
        let tasks_before = member.tasks.clone();
        if let Some(response) = self.topology_error(req, metadata) {
            return response;
        }
        let mut changed = self.update_member(&member_id, client, req);
        if let Some(topology) = &self.topology {
            let signature = streams_topology::metadata_signature(topology, metadata);
            if signature != self.metadata_signature {
                self.dirty = true;
            }
        }
        let epochs = (self.group_epoch, self.target_epoch);
        self.configure_and_bump(metadata);
        let step = self.maybe_update_target(sh, now);
        changed |= epochs != (self.group_epoch, self.target_epoch);
        if self.reconcile_member(&member_id, owned.as_ref(), &step) {
            self.track_rebalance_timeout(sh, &member_id, now);
            changed = true;
        }
        if req.shutdown_application && self.shutdown_requested_by.is_none() {
            self.shutdown_requested_by = Some(member_id.clone());
        }
        self.arm_session(sh, &member_id, now);
        if changed || replaced {
            self.persist(sh);
        }
        let before = Before {
            tasks: &tasks_before,
            group_existed: existed,
            delayed: step.delayed,
        };
        self.accepted_response(sh, req, &member_id, &before, metadata)
    }

    /// A join from a member the group does not hold.
    fn first_join(
        &mut self,
        sh: &mut Shared,
        now: Millis,
        client: &MemberKey,
        req: &StreamsGroupHeartbeatRequest,
        metadata: &dyn TopicMetadata,
        existed: bool,
    ) -> StreamsGroupHeartbeatResponse {
        if let Some(response) = self.topology_error(req, metadata) {
            return response;
        }
        let was_empty = self.members.is_empty();
        let member_id = MemberId::from(req.member_id.as_str());
        if self.topology.is_none()
            && let Some(topology) = &req.topology
        {
            self.topology = Some(StoredTopology::from_wire(topology));
        }
        let member = StreamsMember {
            id: member_id.clone(),
            instance_id: req.instance_id.clone(),
            rack_id: req.rack_id.clone(),
            client_id: client.client_id.clone(),
            client_host: client.client_host.clone(),
            process_id: req.process_id.clone().unwrap_or_default(),
            user_endpoint: req.user_endpoint.as_ref().map(|e| (e.host.clone(), e.port)),
            client_tags: req
                .client_tags
                .iter()
                .flatten()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect(),
            topology_epoch: req.topology.as_ref().map_or(0, |t| t.epoch),
            rebalance_timeout_ms: u64::try_from(req.rebalance_timeout_ms).unwrap_or(0),
            member_epoch: 0,
            previous_member_epoch: 0,
            state: StreamsMemberState::Stable,
            tasks: RoleTasks::default(),
            pending_revocation: RoleTasks::default(),
            task_offsets: req
                .task_offsets
                .as_deref()
                .map(offsets_to_map)
                .unwrap_or_default(),
            task_end_offsets: req
                .task_end_offsets
                .as_deref()
                .map(offsets_to_map)
                .unwrap_or_default(),
            session_deadline: now.saturating_add(sh.config.streams_session_timeout_ms),
            rebalance_deadline: None,
        };
        self.insert_member(sh, member);
        self.dirty = true;
        self.configure_and_bump(metadata);
        // Kafka schedules the initial rebalance delay when a member joins an
        // empty group, unless it is already scheduled.
        if was_empty {
            self.schedule_initial_delay(sh, now);
        }
        let step = self.maybe_update_target(sh, now);
        if req.shutdown_application && self.shutdown_requested_by.is_none() {
            self.shutdown_requested_by = Some(member_id.clone());
        }
        if self.reconcile_member(&member_id, Some(&RoleTasks::default()), &step) {
            self.track_rebalance_timeout(sh, &member_id, now);
        }
        self.persist(sh);
        let before = Before {
            tasks: &RoleTasks::default(),
            group_existed: existed,
            delayed: step.delayed,
        };
        self.accepted_response(sh, req, &member_id, &before, metadata)
    }

    /// Kafka's static member checks: a join may not take an instance id a
    /// member still holds (`UNRELEASED_INSTANCE_ID`), and any other heartbeat
    /// must come from the member that holds a known instance id
    /// (`UNKNOWN_MEMBER_ID`, `FENCED_INSTANCE_ID`).
    fn static_member_error(
        &self,
        req: &StreamsGroupHeartbeatRequest,
        instance_id: &str,
        existing: Option<&MemberId>,
    ) -> Option<StreamsGroupHeartbeatResponse> {
        if req.member_epoch == 0 {
            let existing = existing?;
            if *existing == MemberId::from(req.member_id.as_str()) {
                return None;
            }
            let released = self.members[existing].member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH;
            return (!released).then(|| {
                error_response(
                    codes::UNRELEASED_INSTANCE_ID,
                    Some(format!(
                        "Static member {} with instance id {instance_id} cannot join the group because the instance id is owned by {existing} member.",
                        req.member_id
                    )),
                )
            });
        }
        let Some(existing) = existing else {
            return Some(error_response(
                codes::UNKNOWN_MEMBER_ID,
                Some(format!("Instance id {instance_id} is unknown.")),
            ));
        };
        (existing.as_str() != req.member_id).then(|| {
            error_response(
                codes::FENCED_INSTANCE_ID,
                Some(format!(
                    "Static member {} with instance id {instance_id} was fenced by member {existing}.",
                    req.member_id
                )),
            )
        })
    }

    /// Kafka's static replacement: the joining member takes the released
    /// member's tasks and target under its own id, at epoch 0.
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
            self.target.remove(member_id);
        }
        member.id = member_id.clone();
        member.member_epoch = 0;
        member.previous_member_epoch = 0;
        member.rebalance_deadline = None;
        if let Some(target) = self.target.remove(previous) {
            self.target.insert(member_id.clone(), target);
        }
        self.insert_member(sh, member);
    }

    /// Kafka's `throwIfStreamsGroupMemberEpochIsInvalid` for a member the
    /// group holds: epoch 0 is a rejoin; the previous member epoch is
    /// accepted when the owned tasks of the request are all in the member's
    /// assignment, because the response that carried the new epoch may have
    /// been lost; any other epoch is `FENCED_MEMBER_EPOCH`, with Kafka's
    /// message.
    fn validate_heartbeat_epoch(
        member: &StreamsMember,
        requested: i32,
        owned: Option<&RoleTasks>,
    ) -> Result<(), String> {
        let contained = |owned: &TaskMap, assigned: &TaskMap| {
            owned.iter().all(|(s, ps)| {
                assigned
                    .get(s)
                    .is_some_and(|a| ps.iter().all(|p| a.contains(p)))
            })
        };
        let lost_bump = requested == member.previous_member_epoch
            && owned.is_some_and(|o| {
                contained(&o.active, &member.tasks.active)
                    && contained(&o.standby, &member.tasks.standby)
                    && contained(&o.warmup, &member.tasks.warmup)
            });
        if requested == 0 || requested == member.member_epoch || lost_bump {
            return Ok(());
        }
        let relation = if requested > member.member_epoch {
            "greater"
        } else {
            "smaller"
        };
        Err(format!(
            "The streams group member has a {relation} member epoch ({requested}) than the one known by the group coordinator ({}). The member must abandon all its partitions and rejoin.",
            member.member_epoch
        ))
    }

    /// The refusals of Kafka's `maybeUpdateTopology`, `configureTopics` and
    /// `throwIfRequestContainsInvalidTasks`: a topology ahead of the group's
    /// is `STREAMS_INVALID_TOPOLOGY_EPOCH`, a different topology at the same
    /// epoch is `INVALID_REQUEST`, a topology that cannot be sized is
    /// `STREAMS_INVALID_TOPOLOGY`, and an owned task outside a ready topology
    /// is `INVALID_REQUEST`.
    fn topology_error(
        &self,
        req: &StreamsGroupHeartbeatRequest,
        metadata: &dyn TopicMetadata,
    ) -> Option<StreamsGroupHeartbeatResponse> {
        let requested = req.topology.as_ref().map(StoredTopology::from_wire);
        if let (Some(group), Some(requested)) = (&self.topology, &requested) {
            if requested.epoch > group.epoch {
                return Some(error_response(
                    codes::STREAMS_INVALID_TOPOLOGY_EPOCH,
                    Some(format!(
                        "The member's topology epoch {} is ahead of the group's topology epoch {}.",
                        requested.epoch, group.epoch
                    )),
                ));
            }
            if requested.epoch == group.epoch && requested.subtopologies != group.subtopologies {
                return Some(invalid_request(
                    "Topology updates are not supported yet.".to_string(),
                ));
            }
        }
        let topology = self.topology.as_ref().or(requested.as_ref())?;
        let configured = match streams_topology::configure(topology, metadata) {
            Ok(configured) => configured,
            Err(error) => {
                return Some(error_response(
                    codes::STREAMS_INVALID_TOPOLOGY,
                    Some(error.0),
                ));
            }
        };
        if !configured.is_ready() {
            return None;
        }
        [&req.active_tasks, &req.standby_tasks, &req.warmup_tasks]
            .into_iter()
            .flatten()
            .flatten()
            .find_map(|task| {
                let Some(subtopology) = configured.subtopologies.get(&task.subtopology_id) else {
                    return Some(format!(
                        "Subtopology {} does not exist in the topology.",
                        task.subtopology_id
                    ));
                };
                let number_of_tasks = subtopology.number_of_tasks;
                task.partitions
                    .iter()
                    .find(|p| **p < 0 || **p >= number_of_tasks)
                    .map(|p| {
                        format!(
                            "Task {p} for subtopology {} is invalid. Number of tasks for this subtopology: {number_of_tasks}",
                            task.subtopology_id
                        )
                    })
            })
            .map(invalid_request)
    }

    /// Apply the fields of a steady-state heartbeat to the member, as Kafka's
    /// `StreamsGroupMember.Builder.maybeUpdate*` do. A dynamic member's
    /// change of any recorded field marks the group dirty, as Kafka's
    /// `hasStreamsMemberMetadataChanged` bumps the group epoch; a static
    /// member's only when it changes what the assignment reads (process,
    /// rack, tags, topology epoch). Returns whether anything changed.
    fn update_member(
        &mut self,
        member_id: &MemberId,
        client: &MemberKey,
        req: &StreamsGroupHeartbeatRequest,
    ) -> bool {
        let Some(member) = self.members.get_mut(member_id) else {
            return false;
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
        if let Ok(timeout) = u64::try_from(req.rebalance_timeout_ms) {
            member.rebalance_timeout_ms = timeout;
        }
        if let Some(topology) = &req.topology {
            member.topology_epoch = topology.epoch;
        }
        if let Some(process_id) = &req.process_id {
            member.process_id.clone_from(process_id);
        }
        let endpoint = req.user_endpoint.as_ref().map(|e| (e.host.clone(), e.port));
        if req.member_epoch == 0 || endpoint.is_some() {
            member.user_endpoint = endpoint;
        }
        if let Some(tags) = &req.client_tags {
            member.client_tags = tags
                .iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect();
        }
        if let Some(offsets) = &req.task_offsets {
            member.task_offsets = offsets_to_map(offsets);
        }
        if let Some(offsets) = &req.task_end_offsets {
            member.task_end_offsets = offsets_to_map(offsets);
        }
        let epoch_relevant = |m: &StreamsMember| {
            (
                m.topology_epoch,
                m.rack_id.clone(),
                m.client_tags.clone(),
                m.process_id.clone(),
            )
        };
        // Kafka's `hasStreamsMemberMetadataChanged`: any field of the member
        // record, which leaves out the changelog offsets.
        let metadata = |m: &StreamsMember| {
            (
                (m.client_id.clone(), m.client_host.clone()),
                m.instance_id.clone(),
                m.rack_id.clone(),
                m.rebalance_timeout_ms,
                m.topology_epoch,
                m.process_id.clone(),
                m.user_endpoint.clone(),
                m.client_tags.clone(),
            )
        };
        let metadata_changed = metadata(&before) != metadata(member);
        if metadata_changed
            && (req.instance_id.is_none() || epoch_relevant(&before) != epoch_relevant(member))
        {
            self.dirty = true;
        }
        if let Some(instance_id) = &member.instance_id {
            let id = member.id.clone();
            self.instance_to_member.insert(instance_id.clone(), id);
        }
        before != self.members[member_id]
    }

    /// Configure the topology when the group is dirty or was never
    /// configured, and bump the group epoch when dirty.
    fn configure_and_bump(&mut self, metadata: &dyn TopicMetadata) {
        if (self.dirty || self.configured.is_none())
            && let Some(topology) = &self.topology
        {
            self.configured = streams_topology::configure(topology, metadata).ok();
            self.metadata_signature = streams_topology::metadata_signature(topology, metadata);
        }
        if self.dirty {
            self.group_epoch = self.group_epoch.saturating_add(1);
            self.dirty = false;
        }
    }

    /// Kafka's `scheduleIfAbsent` of the initial rebalance delay.
    fn schedule_initial_delay(&mut self, sh: &mut Shared, now: Millis) {
        let delay = sh.config.streams_initial_rebalance_delay_ms;
        if delay == 0 || self.initial_delay_deadline.is_some() {
            return;
        }
        let at = now.saturating_add(delay);
        self.initial_delay_deadline = Some(at);
        sh.timers.arm(at, self.initial_delay_key());
    }

    /// Kafka 4.3's `maybeUpdateStreamsTargetAssignment`: while the initial
    /// rebalance delay runs, members reconcile toward an empty target at the
    /// current target epoch; a target behind the group epoch is computed
    /// again unless the last one is younger than the assignment interval, and
    /// the members then reconcile toward the last target. Either delay puts
    /// `ASSIGNMENT_DELAYED` on the response.
    fn maybe_update_target(&mut self, sh: &mut Shared, now: Millis) -> TargetStep {
        if let Some(deadline) = self.initial_delay_deadline {
            if now < deadline {
                return TargetStep {
                    empty_target: true,
                    delayed: Some(INITIAL_DELAY_DETAIL),
                };
            }
            // The delay ran out before its timer fired.
            sh.timers.cancel(deadline, &self.initial_delay_key());
            self.initial_delay_deadline = None;
        }
        if self.target_epoch >= self.group_epoch {
            return TargetStep::default();
        }
        if !can_compute_next_target(
            self.assignment_timestamp,
            sh.config.streams_assignment_interval_ms,
            now,
        ) {
            return TargetStep {
                empty_target: false,
                delayed: Some(ASSIGNMENT_INTERVAL_DETAIL),
            };
        }
        self.compute_target(sh);
        self.assignment_timestamp = Some(now);
        TargetStep::default()
    }

    /// The initial rebalance delay ended at `at`: Kafka's
    /// `computeDelayedTargetAssignment` computes the target of every member,
    /// unless the group is empty or its topology was never configured.
    pub fn initial_delay_fired(&mut self, sh: &mut Shared, at: Millis, now: Millis) {
        if self.initial_delay_deadline != Some(at) {
            return;
        }
        self.initial_delay_deadline = None;
        if self.members.is_empty() || self.configured.is_none() {
            return;
        }
        if self.target_epoch < self.group_epoch
            && can_compute_next_target(
                self.assignment_timestamp,
                sh.config.streams_assignment_interval_ms,
                now,
            )
        {
            self.compute_target(sh);
            self.assignment_timestamp = Some(now);
            self.persist(sh);
        }
    }

    /// Run the assignor for the group epoch. A topology that is not ready
    /// gets an empty target: the members advance their epoch with no tasks.
    fn compute_target(&mut self, sh: &Shared) {
        let ready = self.configured.as_ref().filter(|c| c.is_ready());
        self.target = match ready {
            Some(configured) => {
                let members: Vec<AssignorMember> = self
                    .members
                    .values()
                    .map(|m| AssignorMember {
                        id: m.id.clone(),
                        process_id: m.process_id.clone(),
                        current_active: tasks_of(&m.tasks.active).collect(),
                        current_standby: tasks_of(&m.tasks.standby).collect(),
                    })
                    .collect();
                let input = AssignorInput {
                    tasks: configured
                        .number_of_tasks()
                        .into_iter()
                        .flat_map(|(s, n)| (0..n.max(0)).map(move |p| (s.clone(), p)))
                        .collect(),
                    stateful: configured.stateful(),
                    num_standby_replicas: sh.config.streams_num_standby_replicas,
                };
                let assignment = streams_assignor::assign(&members, &input);
                self.members
                    .keys()
                    .map(|id| {
                        (
                            id.clone(),
                            RoleTasks {
                                active: assignment
                                    .active
                                    .get(id)
                                    .map(|t| task_map(t.iter().cloned()))
                                    .unwrap_or_default(),
                                standby: assignment
                                    .standby
                                    .get(id)
                                    .map(|t| task_map(t.iter().cloned()))
                                    .unwrap_or_default(),
                                warmup: TaskMap::new(),
                            },
                        )
                    })
                    .collect()
            }
            None => BTreeMap::new(),
        };
        self.target_epoch = self.group_epoch;
    }

    /// Kafka's streams `CurrentAssignmentBuilder.build`: move `member_id`
    /// toward the target. Returns whether the member changed.
    fn reconcile_member(
        &mut self,
        member_id: &MemberId,
        owned: Option<&RoleTasks>,
        step: &TargetStep,
    ) -> bool {
        let Some(member) = self.members.get(member_id) else {
            return false;
        };
        let epoch_if_unrevoked = match member.state {
            StreamsMemberState::Stable if member.member_epoch == self.target_epoch => return false,
            StreamsMemberState::Stable | StreamsMemberState::UnreleasedTasks => member.member_epoch,
            StreamsMemberState::UnrevokedTasks => match owned {
                Some(owned) if !owned.contains_any(&member.pending_revocation) => {
                    member.member_epoch.saturating_add(1)
                }
                _ => return false,
            },
        };
        let owners = TaskOwners::of(self.members.values());
        let target = if step.empty_target {
            RoleTasks::default()
        } else {
            self.target.get(member_id).cloned().unwrap_or_default()
        };
        let next = compute_next_assignment(
            member,
            epoch_if_unrevoked,
            self.target_epoch,
            &target,
            &owners,
            owned,
        );
        let changed = next != *member;
        self.members.insert(member_id.clone(), next);
        changed
    }

    /// The response of an accepted heartbeat: the status list, the tasks
    /// when the member joined or this heartbeat changed them, and the
    /// endpoint information of the group when the member's endpoint epoch is
    /// behind. Kafka's `hasAssignedTasksChanged(member, updatedMember)`
    /// compares the member's tasks before and after the heartbeat, and only a
    /// member's own heartbeat changes its tasks. The group's endpoint epoch
    /// goes up when a member with an endpoint joins or its tasks changed; a
    /// group this heartbeat created answers epoch 0 and keeps it, as Kafka's
    /// does. Kafka 4.3 sets neither the recovery lag nor the task offset
    /// interval, so both keep their defaults.
    fn accepted_response(
        &mut self,
        sh: &Shared,
        req: &StreamsGroupHeartbeatRequest,
        member_id: &MemberId,
        before: &Before<'_>,
        metadata: &dyn TopicMetadata,
    ) -> StreamsGroupHeartbeatResponse {
        let member = &self.members[member_id];
        let tasks_changed = *before.tasks != member.tasks;
        let mut endpoint_epoch = self.endpoint_information_epoch;
        if (req.member_epoch == 0 || tasks_changed) && member.user_endpoint.is_some() {
            endpoint_epoch = endpoint_epoch.saturating_add(1);
        }
        let partitions_by_user_endpoint = (endpoint_epoch != req.endpoint_information_epoch)
            .then(|| self.endpoint_to_partitions(member_id, metadata));
        if before.group_existed {
            self.endpoint_information_epoch = endpoint_epoch;
        }
        let mut status = Vec::new();
        if let Some(topology) = &self.topology
            && member.topology_epoch < topology.epoch
        {
            status.push(status_entry(
                STALE_TOPOLOGY,
                format!(
                    "The member's topology epoch {} is behind the group's topology epoch {}.",
                    member.topology_epoch, topology.epoch
                ),
            ));
        }
        if let Some(detail) = before.delayed {
            status.push(status_entry(ASSIGNMENT_DELAYED, detail.to_string()));
        }
        if let Some((code, detail)) = self.configured.as_ref().and_then(|c| c.status.as_ref()) {
            status.push(status_entry(*code, detail.clone()));
        }
        if let Some(requester) = &self.shutdown_requested_by {
            status.push(status_entry(
                SHUTDOWN_APPLICATION,
                format!(
                    "Streams group member {requester} encountered a fatal error and requested a shutdown for the entire application."
                ),
            ));
        }
        let send_tasks = req.member_epoch == 0 || tasks_changed;
        let tasks = &member.tasks;
        StreamsGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            member_id: member_id.as_str().to_string(),
            member_epoch: member.member_epoch,
            heartbeat_interval_ms: sh.config.streams_heartbeat_interval_ms,
            status: Some(status),
            active_tasks: send_tasks.then(|| map_to_task_ids(&tasks.active)),
            standby_tasks: send_tasks.then(|| map_to_task_ids(&tasks.standby)),
            warmup_tasks: send_tasks.then(|| map_to_task_ids(&tasks.warmup)),
            endpoint_information_epoch: self.endpoint_information_epoch,
            partitions_by_user_endpoint,
            ..Default::default()
        }
    }

    /// Kafka's `StreamsGroup.buildEndpointToPartitions`: one entry per member
    /// with a user endpoint, the other members in id order and `member_id`
    /// last, each listing the source partitions of its active tasks and of
    /// its standby and warmup tasks.
    fn endpoint_to_partitions(
        &self,
        member_id: &MemberId,
        metadata: &dyn TopicMetadata,
    ) -> Vec<EndpointToPartitions> {
        let mut members: Vec<&StreamsMember> = self
            .members
            .values()
            .filter(|m| m.id != *member_id)
            .collect();
        members.extend(self.members.get(member_id));
        members
            .into_iter()
            .filter_map(|m| {
                let (host, port) = m.user_endpoint.as_ref()?;
                let mut standby_and_warmup = m.tasks.standby.clone();
                for (s, ps) in &m.tasks.warmup {
                    standby_and_warmup
                        .entry(s.clone())
                        .or_default()
                        .extend(ps.iter().copied());
                }
                Some(EndpointToPartitions {
                    user_endpoint: Endpoint {
                        host: host.clone(),
                        port: *port,
                        ..Default::default()
                    },
                    active_partitions: self.topic_partitions(&m.tasks.active, metadata),
                    standby_partitions: self.topic_partitions(&standby_and_warmup, metadata),
                    ..Default::default()
                })
            })
            .collect()
    }

    /// Kafka's `EndpointToPartitionsManager.topicPartitions`: the source and
    /// repartition source partitions of the tasks, per topic.
    fn topic_partitions(
        &self,
        tasks: &TaskMap,
        metadata: &dyn TopicMetadata,
    ) -> Vec<TopicPartition> {
        let Some(configured) = &self.configured else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (subtopology_id, task_partitions) in tasks {
            let Some(subtopology) = configured.subtopologies.get(subtopology_id) else {
                continue;
            };
            let topics: BTreeSet<&str> = subtopology
                .source_topics
                .iter()
                .chain(subtopology.repartition_source_topics.keys())
                .map(String::as_str)
                .collect();
            for topic in topics {
                let Some(count) = metadata.partitions(topic) else {
                    continue;
                };
                let partitions: Vec<i32> = task_partitions
                    .iter()
                    .copied()
                    .filter(|p| *p < count)
                    .collect();
                if !partitions.is_empty() {
                    out.push(TopicPartition {
                        topic: topic.to_string(),
                        partitions,
                        ..Default::default()
                    });
                }
            }
        }
        out
    }

    // ---- leave and fencing ---------------------------------------------------------

    /// Kafka's `streamsGroupLeave`: a shutdown request is recorded first; a
    /// static member that sends -2 stays at epoch -2 with its tasks; any
    /// other member is removed and the group epoch goes up.
    fn leave(
        &mut self,
        sh: &mut Shared,
        req: &StreamsGroupHeartbeatRequest,
    ) -> StreamsGroupHeartbeatResponse {
        let member_id = MemberId::from(req.member_id.as_str());
        if req.shutdown_application && self.shutdown_requested_by.is_none() {
            self.shutdown_requested_by = Some(member_id.clone());
        }
        if let Some(instance_id) = req.instance_id.as_deref() {
            let existing = self.instance_to_member.get(instance_id).cloned();
            if let Some(response) = self.static_member_error(req, instance_id, existing.as_ref()) {
                return response;
            }
            if req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
                let key = self.rebalance_key(&member_id);
                if let Some(member) = self.members.get_mut(&member_id) {
                    if let Some((at, _)) = member.rebalance_deadline.take() {
                        sh.timers.cancel(at, &key);
                    }
                    member.previous_member_epoch = member.member_epoch;
                    member.member_epoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
                    member.pending_revocation = RoleTasks::default();
                }
                self.persist(sh);
                return StreamsGroupHeartbeatResponse {
                    member_id: req.member_id.clone(),
                    member_epoch: LEAVE_GROUP_STATIC_MEMBER_EPOCH,
                    status: Some(Vec::new()),
                    ..Default::default()
                };
            }
        }
        if !self.members.contains_key(&member_id) {
            return error_response(
                codes::UNKNOWN_MEMBER_ID,
                Some(format!(
                    "Member {member_id} is not a member of group {}.",
                    self.group_id
                )),
            );
        }
        self.fence_member(sh, &member_id);
        StreamsGroupHeartbeatResponse {
            member_id: req.member_id.clone(),
            member_epoch: req.member_epoch,
            status: Some(Vec::new()),
            ..Default::default()
        }
    }

    /// Kafka's `streamsGroupFenceMember`: remove the member and bump the
    /// group epoch. A group that becomes empty forgets its shutdown request.
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
        if self.members.is_empty() {
            self.shutdown_requested_by = None;
        }
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
    /// still at the epoch the timeout was armed at.
    pub fn rebalance_expired(&mut self, sh: &mut Shared, member_id: &MemberId, at: Millis) {
        let armed = self
            .members
            .get(member_id)
            .and_then(|m| m.rebalance_deadline.map(|d| (d, m.member_epoch)))
            .is_some_and(|((deadline, epoch), current)| deadline == at && epoch == current);
        if armed {
            self.fence_member(sh, member_id);
        }
    }
}

fn status_entry(status_code: i8, status_detail: String) -> Status {
    Status {
        status_code,
        status_detail,
        ..Default::default()
    }
}

/// The owned tasks of a heartbeat, when it reports all three roles.
fn owned_role_tasks(req: &StreamsGroupHeartbeatRequest) -> Option<RoleTasks> {
    match (&req.active_tasks, &req.standby_tasks, &req.warmup_tasks) {
        (Some(active), Some(standby), Some(warmup)) => Some(RoleTasks {
            active: task_ids_to_map(active),
            standby: task_ids_to_map(standby),
            warmup: task_ids_to_map(warmup),
        }),
        _ => None,
    }
}

fn offsets_to_map(offsets: &[TaskOffset]) -> BTreeMap<String, BTreeMap<i32, i64>> {
    let mut map: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
    for o in offsets {
        map.entry(o.subtopology_id.clone())
            .or_default()
            .insert(o.partition, o.offset);
    }
    map
}

/// Kafka's `computeAssignmentDifference` for one role: the tasks kept, the
/// tasks to revoke, the tasks to take, and whether a task was held back.
fn role_difference(
    current: &TaskMap,
    target: &TaskMap,
    unreleased: impl Fn(&Task) -> bool,
) -> (TaskMap, TaskMap, TaskMap, bool) {
    let mut keep = Vec::new();
    let mut revoke = Vec::new();
    let mut take = Vec::new();
    let mut held_back = false;
    for task in tasks_of(current) {
        if target.get(&task.0).is_some_and(|ps| ps.contains(&task.1)) {
            keep.push(task);
        } else {
            revoke.push(task);
        }
    }
    for task in tasks_of(target) {
        if current.get(&task.0).is_some_and(|ps| ps.contains(&task.1)) {
            continue;
        }
        if unreleased(&task) {
            held_back = true;
        } else {
            take.push(task);
        }
    }
    (task_map(keep), task_map(revoke), task_map(take), held_back)
}

/// Kafka's streams `computeNextAssignment` and `buildNewMember`.
fn compute_next_assignment(
    member: &StreamsMember,
    epoch_if_unrevoked: i32,
    target_epoch: i32,
    target: &RoleTasks,
    owners: &TaskOwners,
    owned: Option<&RoleTasks>,
) -> StreamsMember {
    let process = member.process_id.as_str();
    let (active, active_revoke, active_take, active_held) =
        role_difference(&member.tasks.active, &target.active, |t| {
            owners.active_unreleased(t, process)
        });
    let (standby, standby_revoke, standby_take, standby_held) =
        role_difference(&member.tasks.standby, &target.standby, |t| {
            owners.standby_unreleased(t, process)
        });
    let (warmup, warmup_revoke, warmup_take, warmup_held) =
        role_difference(&member.tasks.warmup, &target.warmup, |t| {
            owners.standby_unreleased(t, process)
        });
    let revoke = RoleTasks {
        active: active_revoke,
        standby: standby_revoke,
        warmup: warmup_revoke,
    };
    let mut next = member.clone();
    next.previous_member_epoch = member.member_epoch;
    let has_tasks_to_revoke = !revoke.is_empty() && owned.is_none_or(|o| o.contains_any(&revoke));
    if has_tasks_to_revoke {
        next.state = StreamsMemberState::UnrevokedTasks;
        next.member_epoch = epoch_if_unrevoked;
        next.tasks = RoleTasks {
            active,
            standby,
            warmup,
        };
        next.pending_revocation = revoke;
        return next;
    }
    let merge = |mut a: TaskMap, b: TaskMap| {
        for (s, ps) in b {
            a.entry(s).or_default().extend(ps);
        }
        a
    };
    next.state = if active_held || standby_held || warmup_held {
        StreamsMemberState::UnreleasedTasks
    } else {
        StreamsMemberState::Stable
    };
    next.member_epoch = target_epoch;
    next.tasks = RoleTasks {
        active: merge(active, active_take),
        standby: merge(standby, standby_take),
        warmup: merge(warmup, warmup_take),
    };
    next.pending_revocation = RoleTasks::default();
    next
}
