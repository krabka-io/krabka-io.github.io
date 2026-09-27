//! Behavioural tests of the coordinator, by protocol. The shared helpers
//! live here: a topic-metadata fake and the request builders.

use std::collections::BTreeMap;

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup, DescribedGroupMember},
        heartbeat_request::HeartbeatRequest,
        heartbeat_response::HeartbeatResponse,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        leave_group_response::{LeaveGroupResponse, MemberResponse},
        list_groups_request::ListGroupsRequest,
        list_groups_response::{ListGroupsResponse, ListedGroup},
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid,
};

use super::{
    AnyResponse, Completion, Coordinator, CoordinatorConfig, HoldToken, MemberKey, Pending,
    TopicMetadata, group_partition, java_hash_code,
};
use crate::lab::codes;

mod classic;
mod consumer;
mod offsets;
mod persist;
mod streams;

/// The member ids the coordinator of broker 1 mints, in order.
const M1: &str = "c1-00000000-0000-0001-0000-000000000001";
const M2: &str = "c2-00000000-0000-0001-0000-000000000002";

fn coord() -> Coordinator {
    Coordinator::new(1, CoordinatorConfig::default())
}

/// The configuration of a coordinator whose consumer and streams groups
/// assign at once: no assignment interval and no initial rebalance delay, so
/// every heartbeat sees the target of the latest group epoch.
fn undelayed() -> CoordinatorConfig {
    CoordinatorConfig {
        consumer_assignment_interval_ms: 0,
        streams_initial_rebalance_delay_ms: 0,
        streams_assignment_interval_ms: 0,
        ..CoordinatorConfig::default()
    }
}

fn client(id: &str) -> MemberKey {
    MemberKey {
        client_id: id.to_string(),
        client_host: format!("/{id}"),
    }
}

/// A topic-metadata fake. Topic ids are the topic's position in the list,
/// repeated over the 16 bytes. A regex is a `^prefix.*` pattern.
#[derive(Clone, Debug, Default)]
struct Topics(BTreeMap<String, (Uuid, i32)>);

impl Topics {
    fn new(topics: &[(&str, i32)]) -> Self {
        let mut out = Self::default();
        for (name, partitions) in topics {
            out.add(name, *partitions);
        }
        out
    }

    fn add(&mut self, name: &str, partitions: i32) {
        let next = u8::try_from(self.0.len() + 1).expect("few topics");
        self.0
            .entry(name.to_string())
            .or_insert((Uuid([next; 16]), partitions))
            .1 = partitions;
    }

    fn id(&self, name: &str) -> Uuid {
        self.0[name].0
    }
}

impl TopicMetadata for Topics {
    fn partitions(&self, topic: &str) -> Option<i32> {
        self.0.get(topic).map(|(_, p)| *p)
    }

    fn topic_id(&self, topic: &str) -> Option<Uuid> {
        self.0.get(topic).map(|(id, _)| *id)
    }

    fn topic_name(&self, id: Uuid) -> Option<String> {
        self.0
            .iter()
            .find(|(_, (topic_id, _))| *topic_id == id)
            .map(|(name, _)| name.clone())
    }

    fn topics_matching(&self, regex: &str) -> Vec<String> {
        let prefix = regex.trim_start_matches('^').trim_end_matches(".*");
        self.0
            .keys()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect()
    }
}

fn join_req(
    member_id: &str,
    instance_id: Option<&str>,
    metadata: &'static [u8],
) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: "g".to_string(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        group_instance_id: instance_id.map(str::to_string),
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: Bytes::from_static(metadata),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn join_member(
    member_id: &str,
    instance_id: Option<&str>,
    metadata: &'static [u8],
) -> JoinGroupResponseMember {
    JoinGroupResponseMember {
        member_id: member_id.to_string(),
        group_instance_id: instance_id.map(str::to_string),
        metadata: Bytes::from_static(metadata),
        ..Default::default()
    }
}

fn join_result(
    generation: i32,
    leader: &str,
    member_id: &str,
    members: Vec<JoinGroupResponseMember>,
) -> JoinGroupResponse {
    JoinGroupResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        generation_id: generation,
        protocol_type: Some("consumer".to_string()),
        protocol_name: Some("range".to_string()),
        leader: leader.to_string(),
        skip_assignment: false,
        member_id: member_id.to_string(),
        members,
        ..Default::default()
    }
}

fn join_error(error_code: i16, member_id: &str) -> Pending<JoinGroupResponse> {
    Pending::Ready(JoinGroupResponse {
        error_code,
        member_id: member_id.to_string(),
        protocol_name: None,
        ..Default::default()
    })
}

fn sync_req(
    member_id: &str,
    generation: i32,
    assignments: &[(&str, &'static [u8])],
) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id: "g".to_string(),
        generation_id: generation,
        member_id: member_id.to_string(),
        assignments: assignments
            .iter()
            .map(|(member, bytes)| SyncGroupRequestAssignment {
                member_id: (*member).to_string(),
                assignment: Bytes::from_static(bytes),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn sync_ok(assignment: &'static [u8]) -> SyncGroupResponse {
    SyncGroupResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        protocol_type: Some("consumer".to_string()),
        protocol_name: Some("range".to_string()),
        assignment: Bytes::from_static(assignment),
        ..Default::default()
    }
}

fn sync_error(error_code: i16) -> SyncGroupResponse {
    SyncGroupResponse {
        error_code,
        ..Default::default()
    }
}

fn hb(member_id: &str, generation: i32) -> HeartbeatRequest {
    HeartbeatRequest {
        group_id: "g".to_string(),
        generation_id: generation,
        member_id: member_id.to_string(),
        ..Default::default()
    }
}

fn hb_response(error_code: i16) -> HeartbeatResponse {
    HeartbeatResponse {
        error_code,
        ..Default::default()
    }
}

fn leave_req(members: &[(&str, Option<&str>)]) -> LeaveGroupRequest {
    LeaveGroupRequest {
        group_id: "g".to_string(),
        member_id: members
            .first()
            .map(|(m, _)| (*m).to_string())
            .unwrap_or_default(),
        members: members
            .iter()
            .map(|(member, instance)| MemberIdentity {
                member_id: (*member).to_string(),
                group_instance_id: instance.map(str::to_string),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn describe_req(groups: &[&str]) -> DescribeGroupsRequest {
    DescribeGroupsRequest {
        groups: groups.iter().map(|g| (*g).to_string()).collect(),
        ..Default::default()
    }
}

fn described_member(
    member_id: &str,
    instance_id: Option<&str>,
    client_id: &str,
    metadata: &'static [u8],
    assignment: &'static [u8],
) -> DescribedGroupMember {
    DescribedGroupMember {
        member_id: member_id.to_string(),
        group_instance_id: instance_id.map(str::to_string),
        client_id: client_id.to_string(),
        client_host: format!("/{client_id}"),
        member_metadata: Bytes::from_static(metadata),
        member_assignment: Bytes::from_static(assignment),
        ..Default::default()
    }
}

fn listed(group_id: &str, protocol_type: &str, state: &str, group_type: &str) -> ListedGroup {
    ListedGroup {
        group_id: group_id.to_string(),
        protocol_type: protocol_type.to_string(),
        group_state: state.to_string(),
        group_type: group_type.to_string(),
        ..Default::default()
    }
}

fn join_completion(token: u64, response: JoinGroupResponse) -> Completion {
    Completion {
        token: HoldToken(token),
        response: AnyResponse::JoinGroup(response),
    }
}

fn sync_completion(token: u64, response: SyncGroupResponse) -> Completion {
    Completion {
        token: HoldToken(token),
        response: AnyResponse::SyncGroup(response),
    }
}

/// Two members `M1` (leader, client `c1`) and `M2` (client `c2`) in a
/// `Stable` generation 1 with assignments `a1` and `a2`, at time 6200. Both
/// sessions are armed at 16200.
fn stable_two_member_group() -> Coordinator {
    let mut c = coord();
    assert!(
        c.join_group(0, &client("c1"), &join_req("", None, b"m1-meta"), 9)
            == join_error(codes::MEMBER_ID_REQUIRED, M1)
    );
    assert!(
        c.join_group(0, &client("c1"), &join_req(M1, None, b"m1-meta"), 9)
            == Pending::Held(HoldToken(1))
    );
    assert!(
        c.join_group(0, &client("c2"), &join_req("", None, b"m2-meta"), 9)
            == join_error(codes::MEMBER_ID_REQUIRED, M2)
    );
    assert!(
        c.join_group(0, &client("c2"), &join_req(M2, None, b"m2-meta"), 9)
            == Pending::Held(HoldToken(2))
    );
    assert!(c.on_tick(3000).is_empty());
    assert!(c.on_tick(6000).len() == 2);
    assert!(c.sync_group(6100, &sync_req(M2, 1, &[])) == Pending::Held(HoldToken(3)));
    assert!(
        c.sync_group(6200, &sync_req(M1, 1, &[(M1, b"a1"), (M2, b"a2")]))
            == Pending::Ready(sync_ok(b"a1"))
    );
    assert!(c.drain_completions() == vec![sync_completion(3, sync_ok(b"a2"))]);
    c
}

/// Java `String.hashCode` values and the `__consumer_offsets` partitions
/// Kafka's `Utils.abs(hashCode) % 50` gives for them.
#[test]
fn group_partition_matches_kafka() {
    for (group, hash, partition) in [
        ("", 0, 0),
        ("test", 3_556_498, 48),
        ("abc", 96_354, 4),
        ("my-group", -1_906_497_762, 12),
        ("consumer-group", -1_738_392_088, 38),
        ("polygenelubricants", i32::MIN, 0),
        ("\u{1F980}", 1_772_802, 2),
    ] {
        assert!(java_hash_code(group) == hash, "{group:?}");
        assert!(group_partition(group) == partition, "{group:?}");
    }
}

#[test]
fn timers_fire_in_order_and_cancel() {
    use super::{
        ids::{GroupId, MemberId},
        timers::{TimerKey, Timers},
    };
    let key = |member: &str| TimerKey::ClassicSession {
        group: GroupId::from("g"),
        member: MemberId::from(member),
    };
    let mut timers = Timers::default();
    timers.arm(30, key("b"));
    timers.arm(10, key("a"));
    timers.arm(10, key("c"));
    timers.rearm(Some(30), 20, key("b"));
    timers.cancel(10, &key("c"));
    assert!(timers.next() == Some(10));
    assert!(timers.len() == 2);
    assert!(timers.pop_due(15) == vec![(10, key("a"))]);
    assert!(timers.pop_due(15).is_empty());
    assert!(timers.pop_due(20) == vec![(20, key("b"))]);
    assert!(timers.is_empty());
}

#[test]
fn snapshot_lists_groups_offsets_and_timers() {
    let c = stable_two_member_group();
    let snapshot = c.snapshot();
    assert!(snapshot["broker_id"] == 1);
    assert!(snapshot["groups"]["g"]["type"] == "classic");
    assert!(snapshot["groups"]["g"]["state"] == "Stable");
    assert!(snapshot["groups"]["g"]["generation"] == 1);
    assert!(snapshot["groups"]["g"]["members"].as_array().map(Vec::len) == Some(2));
    assert!(snapshot["next_deadline"] == 16_200);
    assert!(snapshot["held_requests"] == 0);
    assert!(snapshot["offsets"] == serde_json::json!({}));
}
