//! KIP-848: `ConsumerGroupHeartbeat` and `ConsumerGroupDescribe`.

use assert2::assert;
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
        ConsumerGroupDescribeResponse, DescribedGroup as ConsumerDescribedGroup, Member,
    },
    consumer_group_heartbeat_request::{
        ConsumerGroupHeartbeatRequest, TopicPartitions as OwnedPartitions,
    },
    consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
};

use super::*;

fn join(member: &str, topics: &[&str]) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "cg".to_string(),
        member_id: member.to_string(),
        member_epoch: 0,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: Some(topics.iter().map(|t| (*t).to_string()).collect()),
        topic_partitions: Some(Vec::new()),
        ..Default::default()
    }
}

fn heartbeat(
    member: &str,
    epoch: i32,
    owned: Option<&[(Uuid, &[i32])]>,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "cg".to_string(),
        member_id: member.to_string(),
        member_epoch: epoch,
        topic_partitions: owned.map(|owned| {
            owned
                .iter()
                .map(|(topic_id, partitions)| OwnedPartitions {
                    topic_id: *topic_id,
                    partitions: partitions.to_vec(),
                    ..Default::default()
                })
                .collect()
        }),
        ..Default::default()
    }
}

fn accepted(
    member: &str,
    epoch: i32,
    assignment: Option<&[(Uuid, &[i32])]>,
) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        member_id: Some(member.to_string()),
        member_epoch: epoch,
        heartbeat_interval_ms: 5000,
        assignment: assignment.map(|assignment| Assignment {
            topic_partitions: assignment
                .iter()
                .map(|(topic_id, partitions)| TopicPartitions {
                    topic_id: *topic_id,
                    partitions: partitions.to_vec(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn refused(error_code: i16, message: &str) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code,
        error_message: Some(message.to_string()),
        ..Default::default()
    }
}

/// One heartbeat of a scenario: its name, the request and the whole
/// response it must get.
type Step = (
    &'static str,
    ConsumerGroupHeartbeatRequest,
    ConsumerGroupHeartbeatResponse,
);

/// Run the steps one second apart; member `mN` runs on client `cN`.
fn run_steps(c: &mut Coordinator, topics: &Topics, steps: Vec<Step>) {
    let timed = steps
        .into_iter()
        .enumerate()
        .map(|(i, step)| (u64::try_from(i).unwrap() * 1000, step))
        .collect();
    run_timed(c, topics, timed);
}

/// Run each step at its time; member `mN` runs on client `cN`.
fn run_timed(c: &mut Coordinator, topics: &Topics, steps: Vec<(u64, Step)>) {
    for (now, (name, request, want)) in steps {
        let client = client(&request.member_id.replace('m', "c"));
        let response = c.consumer_group_heartbeat(now, &client, &request, 1, topics);
        assert!(response == want, "{name} at {now}");
    }
}

fn described_assignment(topic_id: Uuid, partitions: &[i32]) -> DescribedAssignment {
    DescribedAssignment {
        topic_partitions: vec![DescribedTopicPartitions {
            topic_id,
            topic_name: "t".to_string(),
            partitions: partitions.to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Two members join one after the other. The second gets the target epoch
/// and no partitions while the first still owns them; the first keeps its
/// epoch and gets only the partitions it retains until it confirms the
/// revocation; then the second receives the released partitions. When the
/// first leaves, the second gets everything at the next epoch.
fn revocation_steps(t: Uuid) -> Vec<Step> {
    vec![
        (
            "m1 joins and gets every partition",
            join("m1", &["t"]),
            accepted("m1", 2, Some(&[(t, &[0, 1, 2, 3])])),
        ),
        (
            "m2 joins at the target epoch with nothing free",
            join("m2", &["t"]),
            accepted("m2", 3, Some(&[])),
        ),
        (
            "m1 learns what it must revoke, at its epoch",
            heartbeat("m1", 2, None),
            accepted("m1", 2, Some(&[(t, &[0, 1])])),
        ),
        (
            "m1 keeps everything: nothing changes",
            heartbeat("m1", 2, Some(&[(t, &[0, 1, 2, 3])])),
            accepted("m1", 2, None),
        ),
        (
            "m1 confirms the revocation and moves to the target epoch",
            heartbeat("m1", 2, Some(&[(t, &[0, 1])])),
            accepted("m1", 3, None),
        ),
        (
            "m2 receives the released partitions",
            heartbeat("m2", 3, None),
            accepted("m2", 3, Some(&[(t, &[2, 3])])),
        ),
        (
            "m2 is stable",
            heartbeat("m2", 3, None),
            accepted("m2", 3, None),
        ),
        (
            "m1 leaves",
            heartbeat("m1", -1, None),
            ConsumerGroupHeartbeatResponse {
                member_id: Some("m1".to_string()),
                member_epoch: -1,
                ..Default::default()
            },
        ),
        (
            "m2 gets every partition at the next epoch",
            heartbeat("m2", 3, None),
            accepted("m2", 4, Some(&[(t, &[0, 1, 2, 3])])),
        ),
        (
            "a stale epoch is fenced",
            heartbeat("m2", 3, None),
            refused(
                codes::FENCED_MEMBER_EPOCH,
                "The consumer group member has a smaller member epoch (3) than the one known by the group coordinator (4). The member must abandon all its partitions and rejoin.",
            ),
        ),
        (
            "a departed member is unknown",
            heartbeat("m1", 4, None),
            refused(
                codes::UNKNOWN_MEMBER_ID,
                "Member m1 is not a member of group cg.",
            ),
        ),
    ]
}

#[test]
fn two_members_reconcile_through_revocation() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 4)]);
    let t = topics.id("t");
    run_steps(&mut c, &topics, revocation_steps(t));
    let describe = ConsumerGroupDescribeRequest {
        group_ids: vec!["cg".to_string(), "g".to_string()],
        ..Default::default()
    };
    assert!(
        c.consumer_group_describe(&describe)
            == ConsumerGroupDescribeResponse {
                throttle_time_ms: 0,
                groups: vec![
                    ConsumerDescribedGroup {
                        group_id: "cg".to_string(),
                        group_state: "Stable".to_string(),
                        group_epoch: 4,
                        assignment_epoch: 4,
                        assignor_name: "uniform".to_string(),
                        members: vec![Member {
                            member_id: "m2".to_string(),
                            member_epoch: 4,
                            client_id: "c2".to_string(),
                            client_host: "/c2".to_string(),
                            subscribed_topic_names: vec!["t".to_string()],
                            // Kafka's member keeps the empty regex it starts with.
                            subscribed_topic_regex: Some(String::new()),
                            assignment: described_assignment(t, &[0, 1, 2, 3]),
                            target_assignment: described_assignment(t, &[0, 1, 2, 3]),
                            member_type: 1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ConsumerDescribedGroup {
                        group_id: "g".to_string(),
                        error_code: codes::GROUP_ID_NOT_FOUND,
                        error_message: Some("Group g not found.".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("cg", "consumer", "Stable", "consumer")],
                ..Default::default()
            }
    );
}

/// The members of a group appear in `ListGroups` as `Assigning` while a
/// departure left the target behind the group epoch, and a member that
/// stops heartbeating is fenced at the session timeout, which bumps the
/// epoch again.
#[test]
fn session_expiry_fences_a_silent_member() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 2)]);
    let t = topics.id("t");
    assert!(
        c.consumer_group_heartbeat(0, &client("c1"), &join("m1", &["t"]), 1, &topics)
            == accepted("m1", 2, Some(&[(t, &[0, 1])]))
    );
    assert!(
        c.consumer_group_heartbeat(1000, &client("c2"), &join("m2", &["t"]), 1, &topics)
            == accepted("m2", 3, Some(&[]))
    );
    assert!(c.next_deadline() == Some(45_000));
    // m2 heartbeats, m1 goes silent and is fenced at 45 s: its partitions
    // become free and m2 takes them at epoch 4.
    assert!(
        c.consumer_group_heartbeat(40_000, &client("c2"), &heartbeat("m2", 3, None), 1, &topics)
            == accepted("m2", 3, None)
    );
    assert!(c.on_tick(45_000).is_empty());
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("cg", "consumer", "Assigning", "consumer")],
                ..Default::default()
            }
    );
    assert!(
        c.consumer_group_heartbeat(46_000, &client("c2"), &heartbeat("m2", 3, None), 1, &topics)
            == accepted("m2", 4, Some(&[(t, &[0, 1])]))
    );
    assert!(
        c.consumer_group_heartbeat(47_000, &client("c1"), &heartbeat("m1", 2, None), 1, &topics)
            == refused(
                codes::UNKNOWN_MEMBER_ID,
                "Member m1 is not a member of group cg."
            )
    );
}

/// A member that does not confirm a revocation within its rebalance timeout
/// is fenced, and its partitions go to the member that waited for them.
#[test]
fn rebalance_timeout_fences_a_member_that_keeps_its_partitions() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 2)]);
    let t = topics.id("t");
    assert!(
        c.consumer_group_heartbeat(0, &client("c1"), &join("m1", &["t"]), 1, &topics)
            == accepted("m1", 2, Some(&[(t, &[0, 1])]))
    );
    assert!(
        c.consumer_group_heartbeat(1000, &client("c2"), &join("m2", &["t"]), 1, &topics)
            == accepted("m2", 3, Some(&[]))
    );
    assert!(
        c.consumer_group_heartbeat(
            2000,
            &client("c1"),
            &heartbeat("m1", 2, Some(&[(t, &[0, 1])])),
            1,
            &topics
        ) == accepted("m1", 2, Some(&[(t, &[0])]))
    );
    assert!(c.next_deadline() == Some(32_000));
    assert!(
        c.consumer_group_heartbeat(
            31_000,
            &client("c1"),
            &heartbeat("m1", 2, Some(&[(t, &[0, 1])])),
            1,
            &topics
        ) == accepted("m1", 2, None)
    );
    assert!(c.on_tick(32_000).is_empty());
    assert!(
        c.consumer_group_heartbeat(33_000, &client("c2"), &heartbeat("m2", 3, None), 1, &topics)
            == accepted("m2", 4, Some(&[(t, &[0, 1])]))
    );
}

/// KIP-848 v1: a regex subscription resolves against the topics that exist,
/// and a topic that appears later bumps the epoch and reaches the member.
#[test]
fn regex_subscription_follows_the_topics() {
    let mut c = coord();
    let mut topics = Topics::new(&[("orders-eu", 2), ("shipments", 3)]);
    let eu = topics.id("orders-eu");
    let request = ConsumerGroupHeartbeatRequest {
        subscribed_topic_names: None,
        subscribed_topic_regex: Some("^orders-.*".to_string()),
        ..join("m1", &[])
    };
    assert!(
        c.consumer_group_heartbeat(0, &client("c1"), &request, 1, &topics)
            == accepted("m1", 2, Some(&[(eu, &[0, 1])]))
    );
    topics.add("orders-us", 1);
    let us = topics.id("orders-us");
    assert!(
        c.consumer_group_heartbeat(1000, &client("c1"), &heartbeat("m1", 2, None), 1, &topics)
            == accepted("m1", 3, Some(&[(eu, &[0, 1]), (us, &[0])]))
    );
    let described = c.consumer_group_describe(&ConsumerGroupDescribeRequest {
        group_ids: vec!["cg".to_string()],
        ..Default::default()
    });
    assert!(
        described.groups[0].members[0].subscribed_topic_regex == Some("^orders-.*".to_string())
    );
    assert!(
        described.groups[0].members[0]
            .subscribed_topic_names
            .is_empty()
    );
}

/// KIP-345 for KIP-848: an instance id another member holds is
/// `UNRELEASED_INSTANCE_ID`; after that member leaves with -2 the new one
/// takes its assignment without a rebalance; a heartbeat with the old id is
/// fenced.
#[test]
fn static_members_hand_over_their_assignment() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 2)]);
    let t = topics.id("t");
    let static_join = |member: &str| ConsumerGroupHeartbeatRequest {
        instance_id: Some("i1".to_string()),
        ..join(member, &["t"])
    };
    assert!(
        c.consumer_group_heartbeat(0, &client("c1"), &static_join("m1"), 1, &topics)
            == accepted("m1", 2, Some(&[(t, &[0, 1])]))
    );
    assert!(
        c.consumer_group_heartbeat(1000, &client("c1"), &static_join("m1b"), 1, &topics)
            == refused(
                codes::UNRELEASED_INSTANCE_ID,
                "Static member m1b with instance id i1 cannot join the group because the instance id is owned by m1 member."
            )
    );
    let leave = ConsumerGroupHeartbeatRequest {
        instance_id: Some("i1".to_string()),
        ..heartbeat("m1", -2, None)
    };
    assert!(
        c.consumer_group_heartbeat(2000, &client("c1"), &leave, 1, &topics)
            == ConsumerGroupHeartbeatResponse {
                member_id: Some("m1".to_string()),
                member_epoch: -2,
                ..Default::default()
            }
    );
    assert!(
        c.consumer_group_heartbeat(3000, &client("c1"), &static_join("m1b"), 1, &topics)
            == accepted("m1b", 2, Some(&[(t, &[0, 1])]))
    );
    let stale = ConsumerGroupHeartbeatRequest {
        instance_id: Some("i1".to_string()),
        ..heartbeat("m1", 2, None)
    };
    assert!(
        c.consumer_group_heartbeat(4000, &client("c1"), &stale, 1, &topics)
            == refused(
                codes::FENCED_INSTANCE_ID,
                "Static member m1 with instance id i1 was fenced by member m1b."
            )
    );
}

/// The requests Kafka 4.3's `throwIfConsumerGroupHeartbeatRequestIsInvalid`
/// refuses, in its order, with the error code and message of each.
fn invalid_requests() -> [(
    &'static str,
    ConsumerGroupHeartbeatRequest,
    i16,
    &'static str,
); 11] {
    let invalid = codes::INVALID_REQUEST;
    [
        (
            "a blank member id, ahead of the group id",
            ConsumerGroupHeartbeatRequest {
                member_id: " ".to_string(),
                group_id: String::new(),
                ..join("m1", &["t"])
            },
            invalid,
            "MemberId can't be empty.",
        ),
        (
            "an empty member id from v1",
            join("", &["t"]),
            invalid,
            "MemberId can't be empty.",
        ),
        (
            "a blank group id",
            ConsumerGroupHeartbeatRequest {
                group_id: " ".to_string(),
                ..join("m1", &["t"])
            },
            invalid,
            "GroupId can't be empty.",
        ),
        (
            "an empty instance id",
            ConsumerGroupHeartbeatRequest {
                instance_id: Some(String::new()),
                ..join("m1", &["t"])
            },
            invalid,
            "InstanceId can't be empty.",
        ),
        (
            "a blank rack id",
            ConsumerGroupHeartbeatRequest {
                rack_id: Some("\t".to_string()),
                ..join("m1", &["t"])
            },
            invalid,
            "RackId can't be empty.",
        ),
        (
            "a join without a rebalance timeout",
            ConsumerGroupHeartbeatRequest {
                rebalance_timeout_ms: -1,
                ..join("m1", &["t"])
            },
            invalid,
            "RebalanceTimeoutMs must be provided in first request.",
        ),
        (
            "a join that owns partitions",
            ConsumerGroupHeartbeatRequest {
                topic_partitions: Some(vec![OwnedPartitions::default()]),
                ..join("m1", &["t"])
            },
            invalid,
            "TopicPartitions must be empty when (re-)joining.",
        ),
        (
            "a join without names or a regex",
            ConsumerGroupHeartbeatRequest {
                subscribed_topic_names: None,
                ..join("m1", &[])
            },
            invalid,
            "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when (re-)joining.",
        ),
        (
            "a static leave without an instance id",
            heartbeat("m1", -2, None),
            invalid,
            "InstanceId can't be null.",
        ),
        (
            "an epoch below -2",
            heartbeat("m1", -3, None),
            invalid,
            "MemberEpoch is invalid.",
        ),
        (
            "an assignor the group does not have",
            ConsumerGroupHeartbeatRequest {
                server_assignor: Some("sticky".to_string()),
                ..join("m1", &["t"])
            },
            codes::UNSUPPORTED_ASSIGNOR,
            "ServerAssignor sticky is not supported. Supported assignors: uniform, range.",
        ),
    ]
}

/// Kafka 4.3's `throwIfConsumerGroupHeartbeatRequestIsInvalid` refuses a
/// request, in its order, before any group changes; a heartbeat for a group
/// that does not exist is `GROUP_ID_NOT_FOUND`.
#[test]
fn invalid_requests_are_refused_before_the_group_changes() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 2)]);
    for (name, request, code, message) in invalid_requests() {
        assert!(
            c.consumer_group_heartbeat(0, &client("c1"), &request, 1, &topics)
                == refused(code, message),
            "{name}"
        );
    }
    assert!(
        c.consumer_group_heartbeat(0, &client("c1"), &heartbeat("m1", 4, None), 1, &topics)
            == refused(codes::GROUP_ID_NOT_FOUND, "Consumer group cg not found.")
    );
    assert!(c.list_groups(&ListGroupsRequest::default()) == ListGroupsResponse::default());
}

/// A join with an empty list of names is accepted, and joins at the group's
/// initial epoch 1 without a rebalance: its subscription did not change. At
/// version 0 an empty member id gets one the coordinator mints, in the form
/// of Kafka's `Uuid.toString`.
#[test]
fn members_join_without_topics_and_with_a_minted_id() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 2)]);
    assert!(
        c.consumer_group_heartbeat(0, &client("c1"), &join("m1", &[]), 1, &topics)
            == accepted("m1", 1, Some(&[]))
    );
    assert!(
        c.consumer_group_heartbeat(0, &client("c2"), &join("", &["t"]), 0, &topics)
            == accepted(
                "AAAAAAAAAAEAAAAAAAAAAQ",
                2,
                Some(&[(topics.id("t"), &[0, 1])])
            )
    );
}

/// Kafka 4.3's upgrade of a classic group is not modelled: a join to a
/// classic group with members gets Kafka's answer with
/// `group.consumer.migration.policy=disabled`, and any other heartbeat
/// finds no consumer group.
#[test]
fn a_classic_group_with_members_is_not_upgraded() {
    let mut c = stable_two_member_group();
    let topics = Topics::new(&[("t", 2)]);
    for (name, request, message) in [
        (
            "a join",
            join("m1", &["t"]),
            "Cannot upgrade classic group g to consumer group because online upgrade is disabled.",
        ),
        (
            "a heartbeat",
            heartbeat("m1", 3, None),
            "Group g is not a consumer group.",
        ),
    ] {
        let request = ConsumerGroupHeartbeatRequest {
            group_id: "g".to_string(),
            ..request
        };
        assert!(
            c.consumer_group_heartbeat(7000, &client("c1"), &request, 1, &topics)
                == refused(codes::GROUP_ID_NOT_FOUND, message),
            "{name}"
        );
    }
}

/// Kafka 4.3's `group.consumer.assignment.interval.ms`, a second by default:
/// a group epoch bumped within a second of the last target leaves the
/// members on that target, the group `Assigning`, and the first heartbeat
/// after the interval computes the new one.
#[test]
fn the_assignment_interval_holds_a_new_target_back() {
    let topics = Topics::new(&[("t", 2)]);
    let t = topics.id("t");
    let mut c = coord();
    run_timed(
        &mut c,
        &topics,
        vec![
            (
                0,
                (
                    "m1 joins and gets every partition",
                    join("m1", &["t"]),
                    accepted("m1", 2, Some(&[(t, &[0, 1])])),
                ),
            ),
            (
                200,
                (
                    "m2 joins on the target of epoch 2",
                    join("m2", &["t"]),
                    accepted("m2", 2, Some(&[])),
                ),
            ),
            (
                500,
                (
                    "m1 stays on the target of epoch 2",
                    heartbeat("m1", 2, None),
                    accepted("m1", 2, None),
                ),
            ),
        ],
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("cg", "consumer", "Assigning", "consumer")],
                ..Default::default()
            }
    );
    run_timed(
        &mut c,
        &topics,
        vec![
            (
                1000,
                (
                    "after the interval m1 must revoke",
                    heartbeat("m1", 2, None),
                    accepted("m1", 2, Some(&[(t, &[0])])),
                ),
            ),
            (
                1100,
                (
                    "m1 confirms the revocation",
                    heartbeat("m1", 2, Some(&[(t, &[0])])),
                    accepted("m1", 3, None),
                ),
            ),
            (
                1200,
                (
                    "m2 takes the released partition",
                    heartbeat("m2", 2, None),
                    accepted("m2", 3, Some(&[(t, &[1])])),
                ),
            ),
        ],
    );
}

/// A subscription change while the assignment interval holds the next target
/// back: Kafka 4.3's `updateCurrentAssignment` revokes the partitions of the
/// dropped topic at once, at the member's epoch, and the next target moves
/// the member on.
#[test]
fn a_dropped_topic_is_revoked_before_the_next_target() {
    let topics = Topics::new(&[("t", 2), ("u", 1)]);
    let (t, u) = (topics.id("t"), topics.id("u"));
    let mut c = coord();
    let drop_u = ConsumerGroupHeartbeatRequest {
        subscribed_topic_names: Some(vec!["t".to_string()]),
        ..heartbeat("m1", 2, Some(&[(t, &[0, 1]), (u, &[0])]))
    };
    run_timed(
        &mut c,
        &topics,
        vec![
            (
                0,
                (
                    "m1 joins t and u",
                    join("m1", &["t", "u"]),
                    accepted("m1", 2, Some(&[(t, &[0, 1]), (u, &[0])])),
                ),
            ),
            (
                300,
                (
                    "m1 drops u and must revoke it at epoch 2",
                    drop_u,
                    accepted("m1", 2, Some(&[(t, &[0, 1])])),
                ),
            ),
            (
                400,
                (
                    "m1 confirms and stays at epoch 2",
                    heartbeat("m1", 2, Some(&[(t, &[0, 1])])),
                    accepted("m1", 2, None),
                ),
            ),
            (
                1000,
                (
                    "the next target takes m1 to epoch 3",
                    heartbeat("m1", 2, Some(&[(t, &[0, 1])])),
                    accepted("m1", 3, None),
                ),
            ),
        ],
    );
}
