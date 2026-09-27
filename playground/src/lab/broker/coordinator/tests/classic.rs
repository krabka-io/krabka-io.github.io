//! The classic protocol: join, sync, heartbeat, leave, expiry and static
//! membership.

use assert2::assert;
use krabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
};

use super::*;

/// KIP-394: a dynamic member gets `MEMBER_ID_REQUIRED` and joins again with
/// the id; the round from `Empty` waits out the initial delay, extended once
/// for the member that joined during it; the leader gets the member list;
/// the leader's `SyncGroup` carries every assignment to the held followers.
#[test]
fn two_members_join_sync_and_heartbeat() {
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
    assert!(c.drain_completions().is_empty());
    assert!(c.next_deadline() == Some(3000));
    assert!(c.on_tick(3000).is_empty());
    assert!(c.next_deadline() == Some(6000));

    let leader = join_result(
        1,
        M1,
        M1,
        vec![
            join_member(M1, None, b"m1-meta"),
            join_member(M2, None, b"m2-meta"),
        ],
    );
    let follower = join_result(1, M1, M2, vec![]);
    assert!(c.on_tick(6000) == vec![join_completion(1, leader), join_completion(2, follower)]);
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("g", "consumer", "CompletingRebalance", "classic")],
                ..Default::default()
            }
    );

    assert!(c.sync_group(6100, &sync_req(M2, 1, &[])) == Pending::Held(HoldToken(3)));
    assert!(
        c.sync_group(6200, &sync_req(M1, 1, &[(M1, b"a1"), (M2, b"a2")]))
            == Pending::Ready(sync_ok(b"a1"))
    );
    assert!(c.drain_completions() == vec![sync_completion(3, sync_ok(b"a2"))]);
    // A member that lost the response reads its assignment again.
    assert!(c.sync_group(6300, &sync_req(M2, 1, &[])) == Pending::Ready(sync_ok(b"a2")));

    for (member, generation, want) in [
        (M1, 1, codes::NONE),
        (M2, 1, codes::NONE),
        (M1, 2, codes::ILLEGAL_GENERATION),
        ("ghost", 1, codes::UNKNOWN_MEMBER_ID),
    ] {
        assert!(
            c.heartbeat(7000, &hb(member, generation)) == hb_response(want),
            "{member} at {generation}"
        );
    }
    assert!(
        c.describe_groups(&describe_req(&["g", "nope"]), 6)
            == DescribeGroupsResponse {
                groups: vec![
                    DescribedGroup {
                        group_id: "g".to_string(),
                        group_state: "Stable".to_string(),
                        protocol_type: "consumer".to_string(),
                        protocol_data: "range".to_string(),
                        members: vec![
                            described_member(M1, None, "c1", b"m1-meta", b"a1"),
                            described_member(M2, None, "c2", b"m2-meta", b"a2"),
                        ],
                        ..Default::default()
                    },
                    DescribedGroup {
                        group_id: "nope".to_string(),
                        group_state: "Dead".to_string(),
                        error_code: codes::GROUP_ID_NOT_FOUND,
                        error_message: Some("Group nope not found.".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("g", "consumer", "Stable", "classic")],
                ..Default::default()
            }
    );
}

/// Below v4 a member joins at once with the id the coordinator gives it, and
/// below v6 a described unknown group is `Dead` without an error.
#[test]
fn old_versions_join_without_the_member_id_handshake() {
    let mut c = coord();
    assert!(
        c.join_group(0, &client("c1"), &join_req("", None, b"m1-meta"), 3)
            == Pending::Held(HoldToken(1))
    );
    assert!(
        c.on_tick(3000)
            == vec![join_completion(
                1,
                join_result(1, M1, M1, vec![join_member(M1, None, b"m1-meta")])
            )]
    );
    assert!(
        c.describe_groups(&describe_req(&["nope"]), 5)
            == DescribeGroupsResponse {
                groups: vec![DescribedGroup {
                    group_id: "nope".to_string(),
                    group_state: "Dead".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
}

#[test]
fn join_is_refused_for_bad_inputs() {
    let mut c = stable_two_member_group();
    let short_session = JoinGroupRequest {
        session_timeout_ms: 100,
        ..join_req("", None, b"x")
    };
    let other_type = JoinGroupRequest {
        protocol_type: "connect".to_string(),
        ..join_req("", None, b"x")
    };
    let empty_group = JoinGroupRequest {
        group_id: String::new(),
        ..join_req(M1, None, b"x")
    };
    let long_session_for_a_missing_group = JoinGroupRequest {
        group_id: "nope".to_string(),
        session_timeout_ms: 1_800_001,
        ..join_req("ghost", None, b"x")
    };
    for (name, req, want) in [
        (
            "session timeout out of range",
            short_session,
            join_error(codes::INVALID_SESSION_TIMEOUT, ""),
        ),
        (
            "session timeout checked before the group lookup",
            long_session_for_a_missing_group,
            join_error(codes::INVALID_SESSION_TIMEOUT, "ghost"),
        ),
        (
            "another protocol type",
            other_type,
            join_error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
        ),
        (
            "unknown member id",
            join_req("ghost", None, b"x"),
            // Kafka nulls the protocol name of a member that fails
            // `validateMember`.
            Pending::Ready(JoinGroupResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                member_id: "ghost".to_string(),
                protocol_name: None,
                ..Default::default()
            }),
        ),
        (
            "empty group id",
            empty_group,
            join_error(codes::INVALID_GROUP_ID, M1),
        ),
    ] {
        assert!(c.join_group(7000, &client("c9"), &req, 9) == want, "{name}");
    }
}

/// A member whose session expires is removed and the group rebalances; the
/// survivor's heartbeat says so, and its `JoinGroup` completes the round at
/// once because every remaining member joined.
#[test]
fn session_timeout_fences_a_member_and_rebalances() {
    let mut c = stable_two_member_group();
    assert!(c.heartbeat(12_000, &hb(M1, 1)) == hb_response(codes::NONE));
    assert!(c.next_deadline() == Some(16_200));
    assert!(c.on_tick(16_200).is_empty());
    assert!(c.heartbeat(16_300, &hb(M2, 1)) == hb_response(codes::UNKNOWN_MEMBER_ID));
    assert!(c.heartbeat(16_300, &hb(M1, 1)) == hb_response(codes::REBALANCE_IN_PROGRESS));
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("g", "consumer", "PreparingRebalance", "classic")],
                ..Default::default()
            }
    );
    assert!(
        c.sync_group(16_300, &sync_req(M1, 1, &[]))
            == Pending::Ready(sync_error(codes::REBALANCE_IN_PROGRESS))
    );
    assert!(
        c.join_group(16_400, &client("c1"), &join_req(M1, None, b"m1-meta"), 9)
            == Pending::Held(HoldToken(4))
    );
    assert!(
        c.drain_completions()
            == vec![join_completion(
                4,
                join_result(2, M1, M1, vec![join_member(M1, None, b"m1-meta")])
            )]
    );
    assert!(
        c.sync_group(16_500, &sync_req(M1, 2, &[(M1, b"all")])) == Pending::Ready(sync_ok(b"all"))
    );
}

/// Kafka's `expireClassicGroupMemberHeartbeat`: a member that neither
/// heartbeats nor joins again expires at its session timeout, and because
/// every remaining member waits in `JoinGroup` the round completes then,
/// before its rebalance deadline. `expirePendingSync`: a member that keeps
/// heartbeating but never sends `SyncGroup` is removed one rebalance timeout
/// after the join completed, and the group that loses its last member starts
/// an empty generation.
#[test]
fn a_silent_member_expires_and_a_member_that_never_syncs_is_removed() {
    let mut c = coord();
    // Below v4 a member joins at once with the id the coordinator gives it.
    assert!(
        c.join_group(0, &client("c1"), &join_req("", None, b"m1-meta"), 3)
            == Pending::Held(HoldToken(1))
    );
    assert!(
        c.on_tick(3000)
            == vec![join_completion(
                1,
                join_result(1, M1, M1, vec![join_member(M1, None, b"m1-meta")])
            )]
    );
    assert!(c.sync_group(3100, &sync_req(M1, 1, &[(M1, b"a1")])) == Pending::Ready(sync_ok(b"a1")));
    // M2 opens a round whose deadline is the rebalance timeout, 33200. M1's
    // session, restarted by its `SyncGroup`, ends first.
    assert!(
        c.join_group(3200, &client("c2"), &join_req("", None, b"m2-meta"), 3)
            == Pending::Held(HoldToken(2))
    );
    assert!(c.next_deadline() == Some(13_100));
    assert!(
        c.on_tick(13_100)
            == vec![join_completion(
                2,
                join_result(2, M2, M2, vec![join_member(M2, None, b"m2-meta")])
            )]
    );
    assert!(c.heartbeat(13_200, &hb(M1, 1)) == hb_response(codes::UNKNOWN_MEMBER_ID));
    // M2 heartbeats while the group waits for its `SyncGroup`, which never
    // comes: the sync deadline is 13100 + 30000.
    for at in [20_000, 29_000, 38_000] {
        assert!(
            c.heartbeat(at, &hb(M2, 2)) == hb_response(codes::NONE),
            "{at}"
        );
    }
    assert!(c.next_deadline() == Some(43_100));
    assert!(c.on_tick(43_100).is_empty());
    assert!(c.heartbeat(43_200, &hb(M2, 2)) == hb_response(codes::UNKNOWN_MEMBER_ID));
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("g", "consumer", "Empty", "classic")],
                ..Default::default()
            }
    );
    assert!(
        c.describe_groups(&describe_req(&["g"]), 6)
            == DescribeGroupsResponse {
                groups: vec![DescribedGroup {
                    group_id: "g".to_string(),
                    group_state: "Empty".to_string(),
                    protocol_type: "consumer".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
}

/// Kafka's `CLASSIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS`: a member that joins
/// for the first time cannot heartbeat while its `JoinGroup` waits, so it
/// expires after five minutes, and its `JoinGroup` gets `UNKNOWN_MEMBER_ID`
/// with an empty member id. An existing member that heartbeats keeps its
/// place.
#[test]
fn a_new_member_waiting_in_join_group_expires_after_five_minutes() {
    let mut c = coord();
    let slow = |member_id: &str, metadata: &'static [u8]| JoinGroupRequest {
        rebalance_timeout_ms: 600_000,
        ..join_req(member_id, None, metadata)
    };
    assert!(
        c.join_group(0, &client("c1"), &slow("", b"m1-meta"), 3) == Pending::Held(HoldToken(1))
    );
    assert!(c.on_tick(3000).len() == 1);
    assert!(c.sync_group(3100, &sync_req(M1, 1, &[(M1, b"a1")])) == Pending::Ready(sync_ok(b"a1")));
    // M2's round waits up to M1's ten-minute rebalance timeout for M1.
    assert!(
        c.join_group(5000, &client("c2"), &slow("", b"m2-meta"), 3) == Pending::Held(HoldToken(2))
    );
    let mut at = 8000;
    while at < 305_000 {
        assert!(
            c.heartbeat(at, &hb(M1, 1)) == hb_response(codes::REBALANCE_IN_PROGRESS),
            "{at}"
        );
        at += 5000;
    }
    assert!(c.next_deadline() == Some(305_000));
    assert!(
        c.on_tick(305_000)
            == vec![join_completion(
                2,
                JoinGroupResponse {
                    error_code: codes::UNKNOWN_MEMBER_ID,
                    ..Default::default()
                }
            )]
    );
    assert!(c.heartbeat(305_100, &hb(M1, 1)) == hb_response(codes::REBALANCE_IN_PROGRESS));
    assert!(
        c.describe_groups(&describe_req(&["g"]), 6).groups[0].members
            == vec![described_member(M1, None, "c1", b"", b"a1")]
    );
}

/// KIP-345: a static member that joins again with an empty member id takes
/// a new id in place, keeps its assignment, and gets `skip_assignment` as
/// the leader (KIP-814); the old id is fenced.
#[test]
fn static_member_rejoin_keeps_the_assignment() {
    const S1: &str = "i1-00000000-0000-0001-0000-000000000001";
    const S1_AGAIN: &str = "i1-00000000-0000-0001-0000-000000000003";
    let mut c = coord();
    assert!(
        c.join_group(0, &client("c1"), &join_req("", Some("i1"), b"s1-meta"), 9)
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
    assert!(
        c.on_tick(6000)
            == vec![
                join_completion(2, join_result(1, S1, M2, vec![])),
                join_completion(
                    1,
                    join_result(
                        1,
                        S1,
                        S1,
                        vec![
                            join_member(M2, None, b"m2-meta"),
                            join_member(S1, Some("i1"), b"s1-meta")
                        ]
                    )
                ),
            ]
    );
    assert!(
        c.sync_group(6100, &sync_req(S1, 1, &[(S1, b"a1"), (M2, b"a2")]))
            == Pending::Ready(sync_ok(b"a1"))
    );
    assert!(c.sync_group(6100, &sync_req(M2, 1, &[])) == Pending::Ready(sync_ok(b"a2")));
    let records_before = c.drain_records().len();

    let rejoin = c.join_group(
        7000,
        &client("c1"),
        &join_req("", Some("i1"), b"s1-meta"),
        9,
    );
    assert!(
        rejoin
            == Pending::Ready(JoinGroupResponse {
                skip_assignment: true,
                ..join_result(
                    1,
                    S1_AGAIN,
                    S1_AGAIN,
                    vec![
                        join_member(M2, None, b"m2-meta"),
                        join_member(S1_AGAIN, Some("i1"), b"s1-meta")
                    ]
                )
            })
    );
    assert!(c.drain_records().len() == 1, "{records_before}");
    assert!(c.sync_group(7100, &sync_req(S1_AGAIN, 1, &[])) == Pending::Ready(sync_ok(b"a1")));
    assert!(
        c.heartbeat(
            7200,
            &HeartbeatRequest {
                group_instance_id: Some("i1".to_string()),
                ..hb(S1, 1)
            }
        ) == hb_response(codes::FENCED_INSTANCE_ID)
    );
    assert!(
        c.heartbeat(
            7200,
            &HeartbeatRequest {
                group_instance_id: Some("i1".to_string()),
                ..hb(S1_AGAIN, 1)
            }
        ) == hb_response(codes::NONE)
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("g", "consumer", "Stable", "classic")],
                ..Default::default()
            }
    );
}

#[test]
fn leave_group_removes_members_and_rebalances() {
    let mut c = stable_two_member_group();
    let response = c.leave_group(
        7000,
        &leave_req(&[(M2, None), ("ghost", None), ("", Some("i-unknown"))]),
        5,
    );
    assert!(
        response
            == LeaveGroupResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                members: vec![
                    MemberResponse {
                        member_id: M2.to_string(),
                        group_instance_id: None,
                        error_code: codes::NONE,
                        ..Default::default()
                    },
                    MemberResponse {
                        member_id: "ghost".to_string(),
                        group_instance_id: None,
                        error_code: codes::UNKNOWN_MEMBER_ID,
                        ..Default::default()
                    },
                    MemberResponse {
                        member_id: String::new(),
                        group_instance_id: Some("i-unknown".to_string()),
                        error_code: codes::UNKNOWN_MEMBER_ID,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
    );
    assert!(c.heartbeat(7100, &hb(M1, 1)) == hb_response(codes::REBALANCE_IN_PROGRESS));
    // Below v3 the one member id of the request leaves and the top-level
    // error is its error.
    let mut c = stable_two_member_group();
    assert!(
        c.leave_group(7000, &leave_req(&[("ghost", None)]), 2)
            == LeaveGroupResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            }
    );
    assert!(
        c.leave_group(7000, &leave_req(&[(M1, None), (M2, None)]), 2)
            == LeaveGroupResponse::default()
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("g", "consumer", "PreparingRebalance", "classic")],
                ..Default::default()
            }
    );
}

fn consumer_join(group: &str, member_id: &str) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: group.to_string(),
        member_id: member_id.to_string(),
        member_epoch: 0,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: Some(vec!["t".to_string()]),
        topic_partitions: Some(Vec::new()),
        ..Default::default()
    }
}

fn streams_join(group: &str, member_id: &str) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.to_string(),
        member_id: member_id.to_string(),
        member_epoch: 0,
        rebalance_timeout_ms: 60_000,
        topology: Some(Topology {
            epoch: 1,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".to_string(),
                source_topics: vec!["t".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        }),
        active_tasks: Some(Vec::new()),
        standby_tasks: Some(Vec::new()),
        warmup_tasks: Some(Vec::new()),
        process_id: Some("p1".to_string()),
        ..Default::default()
    }
}

fn join_to(group: &str, member_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group.to_string(),
        ..join_req(member_id, None, b"x")
    }
}

/// Kafka 4.3's `classicGroupJoin`, `classicGroupSync`,
/// `classicGroupHeartbeat` and `classicGroupLeave` against groups of the
/// other kinds: a consumer or streams group with members refuses a classic
/// member with `INCONSISTENT_GROUP_PROTOCOL` and the empty member id, and a
/// member id never creates a group. The streams group is still `Assigning`:
/// its initial rebalance delay runs.
#[test]
fn live_groups_of_other_kinds_refuse_classic_requests() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 2)]);
    let consumer =
        c.consumer_group_heartbeat(0, &client("c1"), &consumer_join("cg", "m1"), 1, &topics);
    assert!(consumer.error_code == codes::NONE);
    let (streams, _) =
        c.streams_group_heartbeat(0, &client("c2"), &streams_join("app", "s1"), &topics);
    assert!(streams.error_code == codes::NONE);
    let _ = c.drain_records();

    for (name, request, want) in [
        (
            "a member id for a missing group",
            join_to("nope", "ghost"),
            join_error(codes::UNKNOWN_MEMBER_ID, "ghost"),
        ),
        (
            "a consumer group with members",
            join_to("cg", ""),
            join_error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
        ),
        (
            "a member id for a consumer group with members",
            join_to("cg", "m1"),
            join_error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
        ),
        (
            "a streams group with members",
            join_to("app", ""),
            join_error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
        ),
    ] {
        assert!(
            c.join_group(1000, &client("c9"), &request, 9) == want,
            "{name}"
        );
    }
    let sync = SyncGroupRequest {
        group_id: "cg".to_string(),
        ..sync_req("m1", 1, &[])
    };
    assert!(c.sync_group(1000, &sync) == Pending::Ready(sync_error(codes::UNKNOWN_MEMBER_ID)));
    let heartbeat = HeartbeatRequest {
        group_id: "app".to_string(),
        ..hb("s1", 2)
    };
    assert!(c.heartbeat(1000, &heartbeat) == hb_response(codes::UNKNOWN_MEMBER_ID));
    let leave = LeaveGroupRequest {
        group_id: "cg".to_string(),
        ..leave_req(&[("m1", None), ("", Some("i1"))])
    };
    let unknown = |member_id: &str, group_instance_id: Option<&str>| MemberResponse {
        member_id: member_id.to_string(),
        group_instance_id: group_instance_id.map(str::to_string),
        error_code: codes::UNKNOWN_MEMBER_ID,
        ..Default::default()
    };
    assert!(
        c.leave_group(1000, &leave, 5)
            == LeaveGroupResponse {
                members: vec![unknown("m1", None), unknown("", Some("i1"))],
                ..Default::default()
            }
    );
    assert!(
        c.leave_group(1000, &leave, 2)
            == LeaveGroupResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            }
    );
    assert!(c.drain_records().is_empty());
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![
                    listed("app", "streams", "Assigning", "streams"),
                    listed("cg", "consumer", "Stable", "consumer"),
                ],
                ..Default::default()
            }
    );
}

/// Kafka's `maybeDeleteEmptyConsumerGroup`: a consumer group whose last
/// member left gives way to the classic group a `JoinGroup` creates, and its
/// record is deleted.
#[test]
fn an_empty_consumer_group_gives_way_to_a_classic_group() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 2)]);
    let consumer =
        c.consumer_group_heartbeat(0, &client("c1"), &consumer_join("cg", "m1"), 1, &topics);
    assert!(consumer.error_code == codes::NONE);
    let leave = ConsumerGroupHeartbeatRequest {
        member_epoch: -1,
        rebalance_timeout_ms: -1,
        subscribed_topic_names: None,
        topic_partitions: None,
        ..consumer_join("cg", "m1")
    };
    assert!(
        c.consumer_group_heartbeat(2000, &client("c1"), &leave, 1, &topics)
            .error_code
            == codes::NONE
    );
    let _ = c.drain_records();
    assert!(
        c.join_group(3000, &client("c9"), &join_to("cg", ""), 9)
            == join_error(
                codes::MEMBER_ID_REQUIRED,
                "c9-00000000-0000-0001-0000-000000000001"
            )
    );
    assert!(
        c.drain_records()
            == vec![(
                Bytes::from_static(br#"{"type":"consumer_group","group":"cg"}"#),
                None
            )]
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("cg", "", "Empty", "classic")],
                ..Default::default()
            }
    );
}
