//! The consumer against the fake broker: the classic join, sync, positions,
//! fetch and commit; the range split and the rebalances when members come
//! and go; KIP-848 reconciliation; and a fetch that follows a new leader.

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        },
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
    },
    primitives::uuid::Uuid,
};

use super::{
    AutoOffsetReset, ConsumedRecord, Consumer, ConsumerConfig, ConsumerEvent, GroupProtocol,
    MemberState,
    assignor::{encode_assignment, encode_subscription},
    batch::BatchRecord,
    fake_broker::{CghAnswer, ClusterState, Seen},
    test_support::{Harness, Members, client, cluster},
};
use crate::lab::{
    codes,
    net::{Millis, NodeId},
};

fn config(protocol: GroupProtocol, reset: AutoOffsetReset) -> ConsumerConfig {
    ConsumerConfig {
        group_id: "billing".to_string(),
        group_protocol: protocol,
        auto_offset_reset: reset,
        ..Default::default()
    }
}

fn consumer(config: ConsumerConfig) -> Consumer {
    Consumer::new(client(&[1]), config)
}

/// Append one batch of `values` to a partition, each keyed `key-<value>`,
/// the first at timestamp 1 000 and each next one a millisecond later.
fn seed(state: &RefCell<ClusterState>, topic: &str, partition: i32, values: &[&str]) {
    let records: Vec<BatchRecord> = values
        .iter()
        .zip(1_000..)
        .map(|(value, timestamp)| BatchRecord {
            timestamp,
            key: Some(Bytes::from(format!("key-{value}"))),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Vec::new(),
        })
        .collect();
    state
        .borrow_mut()
        .append_records(topic, partition, &records);
}

/// `(partition, offset, value)` of each record.
fn summary(records: &[ConsumedRecord]) -> Vec<(i32, i64, String)> {
    records
        .iter()
        .map(|r| {
            let value = String::from_utf8_lossy(r.value.as_deref().unwrap_or_default());
            (r.partition, r.offset, value.into_owned())
        })
        .collect()
}

fn partitions(topic: &str, indexes: &[i32]) -> Vec<(String, i32)> {
    indexes.iter().map(|p| (topic.to_string(), *p)).collect()
}

fn decoded<R>(h: &Harness<impl super::test_support::Driven>) -> Vec<R>
where
    R: ProtocolRequest + for<'de> krabka_protocol::Decode<'de>,
{
    h.seen(R::API_KEY).iter().map(Seen::decode).collect()
}

/// The `JoinGroup` of a classic member of `billing` that subscribes to
/// `orders` and owns nothing yet.
fn join_request(member_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: "billing".to_string(),
        session_timeout_ms: 45_000,
        rebalance_timeout_ms: 300_000,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: encode_subscription(&["orders".to_string()], &[], -1, None),
            ..Default::default()
        }],
        reason: Some(String::new()),
        ..Default::default()
    }
}

/// The `SyncGroup` of the leader of `billing` that gives `member_id` the
/// partitions of `orders` at `indexes`.
fn leader_sync(generation_id: i32, member_id: &str, indexes: &[i32]) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id: "billing".to_string(),
        generation_id,
        member_id: member_id.to_string(),
        group_instance_id: None,
        protocol_type: Some("consumer".to_string()),
        protocol_name: Some("range".to_string()),
        assignments: vec![SyncGroupRequestAssignment {
            member_id: member_id.to_string(),
            assignment: encode_assignment(&partitions("orders", indexes)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The `ListOffsets` for the earliest offset of one partition of `orders`
/// at leader epoch 0.
fn earliest(partition: i32) -> ListOffsetsRequest {
    ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopic {
            name: "orders".to_string(),
            partitions: vec![ListOffsetsPartition {
                partition_index: partition,
                current_leader_epoch: 0,
                timestamp: -2,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A sessionless `Fetch` of one partition with the consumer defaults. Fetch
/// v13 and later name the topic by id only.
fn fetch_request(topic_id: Uuid, partition: i32, fetch_offset: i64) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 52_428_800,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics: vec![FetchTopic {
            topic: String::new(),
            topic_id,
            partitions: vec![FetchPartition {
                partition,
                current_leader_epoch: 0,
                fetch_offset,
                last_fetched_epoch: -1,
                log_start_offset: -1,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        rack_id: String::new(),
        ..Default::default()
    }
}

/// The `OffsetCommit` of `(partition, offset)` pairs of `orders`, at leader
/// epoch 0.
fn commit_request(generation: i32, member_id: &str, offsets: &[(i32, i64)]) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: "billing".to_string(),
        generation_id_or_member_epoch: generation,
        member_id: member_id.to_string(),
        group_instance_id: None,
        topics: vec![OffsetCommitRequestTopic {
            name: "orders".to_string(),
            partitions: offsets
                .iter()
                .map(|(partition, offset)| OffsetCommitRequestPartition {
                    partition_index: *partition,
                    committed_offset: *offset,
                    committed_leader_epoch: 0,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A record of `orders` as `seed` wrote it.
fn consumed(partition: i32, offset: i64, timestamp: i64, value: &str) -> ConsumedRecord {
    ConsumedRecord {
        topic: "orders".to_string(),
        partition,
        offset,
        timestamp,
        key: Some(Bytes::from(format!("key-{value}"))),
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: Vec::new(),
        leader_epoch: 0,
    }
}

#[test]
fn a_classic_member_joins_syncs_fetches_and_commits() {
    let state = cluster(&[("orders", 3)]);
    let topic_id = state.borrow().topics["orders"].id;
    seed(&state, "orders", 0, &["a0", "a1"]);
    seed(&state, "orders", 1, &["b0"]);
    let consumer = consumer(config(GroupProtocol::Classic, AutoOffsetReset::Earliest));
    let mut h = Harness::new(consumer, Rc::clone(&state));
    h.with_client(|c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.buffered() == 3, 2_000));
    let member = "member-00000001";
    assert!(h.client.state() == MemberState::Stable);
    assert!(h.client.member_id() == member);
    assert!(h.client.generation() == 1);
    // KIP-394: the first JoinGroup carries no member id and the coordinator
    // answers MEMBER_ID_REQUIRED with one; the second carries it. The leader
    // then assigns with the range assignor, here every partition to itself.
    assert!(decoded::<JoinGroupRequest>(&h) == vec![join_request(""), join_request(member)]);
    assert!(decoded::<SyncGroupRequest>(&h) == vec![leader_sync(1, member, &[0, 1, 2])]);
    // The group committed nothing, so `earliest` asks each leader.
    let fetched: Vec<OffsetFetchRequest> = decoded(&h);
    assert!(fetched.len() == 1);
    assert!(fetched[0].require_stable);
    assert!(
        fetched[0].groups
            == vec![OffsetFetchRequestGroup {
                group_id: "billing".to_string(),
                member_id: None,
                member_epoch: -1,
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "orders".to_string(),
                    partition_indexes: vec![0, 1, 2],
                    ..Default::default()
                }]),
                ..Default::default()
            }]
    );
    let listed: Vec<(NodeId, ListOffsetsRequest)> = h
        .seen(ListOffsetsRequest::API_KEY)
        .iter()
        .map(|s| (s.broker, s.decode()))
        .collect();
    let expected: Vec<(NodeId, ListOffsetsRequest)> = (0_i32..3)
        .map(|p| (NodeId(p.cast_unsigned() + 1), earliest(p)))
        .collect();
    assert!(listed == expected);
    // One sessionless Fetch per leader.
    let first_fetch: FetchRequest = h
        .seen(FetchRequest::API_KEY)
        .iter()
        .find(|s| s.broker == NodeId(1))
        .unwrap()
        .decode();
    assert!(first_fetch == fetch_request(topic_id, 0, 0));
    assert!(
        h.with_client(|c, _| c.poll(500))
            == vec![
                consumed(0, 0, 1_000, "a0"),
                consumed(0, 1, 1_001, "a1"),
                consumed(1, 0, 1_000, "b0"),
            ]
    );
    let positions: Vec<Option<i64>> = (0..3).map(|p| h.client.position("orders", p)).collect();
    assert!(positions == vec![Some(2), Some(1), Some(0)]);
    // `auto.commit.interval.ms` later every position goes to the
    // coordinator, with the member's generation.
    assert!(h.run_until(|h| !h.seen(OffsetCommitRequest::API_KEY).is_empty(), 6_000));
    assert!(
        decoded::<OffsetCommitRequest>(&h)[0]
            == commit_request(1, member, &[(0, 2), (1, 1), (2, 0)])
    );
    assert!(h.run_until(|h| h.client.committed("orders", 0).is_some(), 1_000));
    let committed = [("orders", 0, 2), ("orders", 1, 1), ("orders", 2, 0)];
    assert!(
        h.take_events()
            == vec![
                ConsumerEvent::Joined {
                    member_id: member.to_string(),
                    generation: 1,
                },
                ConsumerEvent::Assigned {
                    partitions: partitions("orders", &[0, 1, 2]),
                },
                ConsumerEvent::Committed {
                    offsets: committed
                        .iter()
                        .map(|(t, p, o)| ((*t).to_string(), *p, *o))
                        .collect(),
                },
            ]
    );
    // The heartbeats kept the member in its generation.
    assert!(!h.seen(12).is_empty());
}

#[test]
fn a_position_starts_at_the_committed_offset_or_by_the_reset_policy() {
    // The log holds r0 to r2 when the member joins; r3 comes after its
    // position is set. Rows: the offset the group committed, the reset
    // policy, the offsets polled, whether a ListOffsets asked a leader, and
    // whether a fetch met OFFSET_OUT_OF_RANGE.
    let rows = [
        (
            "a committed offset wins",
            Some(1),
            AutoOffsetReset::Latest,
            vec![1, 2, 3],
            false,
            false,
        ),
        (
            "earliest without a commit",
            None,
            AutoOffsetReset::Earliest,
            vec![0, 1, 2, 3],
            true,
            false,
        ),
        (
            "latest without a commit",
            None,
            AutoOffsetReset::Latest,
            vec![3],
            true,
            false,
        ),
        (
            "a commit past the log end resets",
            Some(10),
            AutoOffsetReset::Earliest,
            vec![0, 1, 2, 3],
            true,
            true,
        ),
    ];
    for (name, committed, reset, offsets, listed, out_of_range) in rows {
        let state = cluster(&[("orders", 1)]);
        seed(&state, "orders", 0, &["r0", "r1", "r2"]);
        if let Some(offset) = committed {
            state
                .borrow_mut()
                .groups
                .entry("billing".to_string())
                .or_default()
                .committed
                .insert(("orders".to_string(), 0), (offset, 0));
        }
        let mut h = Harness::new(
            consumer(config(GroupProtocol::Classic, reset)),
            Rc::clone(&state),
        );
        h.with_client(|c, _| c.subscribe(&["orders"]));
        assert!(
            h.run_until(|h| h.client.position("orders", 0).is_some(), 2_000),
            "{name}"
        );
        seed(&state, "orders", 0, &["r3"]);
        h.run_for(1_500);
        let polled: Vec<i64> = summary(&h.with_client(|c, _| c.poll(500)))
            .into_iter()
            .map(|(_, offset, _)| offset)
            .collect();
        assert!(polled == offsets, "{name}");
        assert!(
            h.seen(ListOffsetsRequest::API_KEY).is_empty() != listed,
            "{name}"
        );
        let met_out_of_range = h.take_events().contains(&ConsumerEvent::Error {
            api: "Fetch",
            code: codes::OFFSET_OUT_OF_RANGE,
        });
        assert!(met_out_of_range == out_of_range, "{name}");
    }
}

/// Run member `i` through `f`.
fn with_member<T>(
    h: &mut Harness<Members>,
    i: usize,
    f: impl FnOnce(&mut Consumer, &mut crate::lab::net::Ctx<'_>) -> T,
) -> T {
    h.with_client(|members, ctx| members.with(ctx, i, f))
}

/// Two classic members of `billing` on nodes 101 and 102, each subscribed
/// to `orders` in turn, run until each holds its range.
fn two_members(state: &Rc<RefCell<ClusterState>>, session_timeout_ms: Millis) -> Harness<Members> {
    let config = ConsumerConfig {
        session_timeout_ms,
        ..config(GroupProtocol::Classic, AutoOffsetReset::Earliest)
    };
    let members = Members::new(vec![
        (NodeId(101), consumer(config.clone())),
        (NodeId(102), consumer(config)),
    ]);
    let mut h = Harness::new(members, Rc::clone(state));
    with_member(&mut h, 0, |c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.get(0).assignment().len() == 3, 2_000));
    with_member(&mut h, 1, |c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(
        |h| {
            h.client.get(0).assignment() == partitions("orders", &[0, 1])
                && h.client.get(1).assignment() == partitions("orders", &[2])
                && h.client.get(0).state() == MemberState::Stable
                && h.client.get(1).state() == MemberState::Stable
        },
        10_000
    ));
    h
}

/// Poll each member until it read one record per partition it owns.
fn drain_members(h: &mut Harness<Members>) -> Vec<Vec<(i32, i64, String)>> {
    assert!(h.run_until(
        |h| h.client.get(0).buffered() == 2 && h.client.get(1).buffered() == 1,
        2_000
    ));
    (0..2)
        .map(|i| summary(&with_member(h, i, |c, _| c.poll(500))))
        .collect()
}

#[test]
fn members_split_the_partitions_by_range_and_a_leaving_member_hands_its_share_over() {
    let state = cluster(&[("orders", 3)]);
    for partition in 0..3 {
        seed(&state, "orders", partition, &[&format!("m{partition}")]);
    }
    let mut h = two_members(&state, 45_000);
    let (a, b) = ("member-00000001".to_string(), "member-00000002".to_string());
    // The rebalance the second member started: the leader split the
    // partitions by range over the sorted member ids.
    let syncs: Vec<SyncGroupRequest> = decoded(&h);
    let leader_sync = syncs
        .iter()
        .find(|s| s.generation_id == 2 && !s.assignments.is_empty())
        .unwrap();
    assert!(
        leader_sync.assignments
            == vec![
                SyncGroupRequestAssignment {
                    member_id: a.clone(),
                    assignment: encode_assignment(&partitions("orders", &[0, 1])),
                    ..Default::default()
                },
                SyncGroupRequestAssignment {
                    member_id: b.clone(),
                    assignment: encode_assignment(&partitions("orders", &[2])),
                    ..Default::default()
                },
            ]
    );
    assert!(
        drain_members(&mut h)
            == vec![
                vec![(0, 0, "m0".to_string()), (1, 0, "m1".to_string())],
                vec![(2, 0, "m2".to_string())],
            ]
    );
    // The eager protocol: the first member gave up everything before it
    // joined again, then took its range.
    let events = h.take_events();
    let of = |member: usize| -> Vec<ConsumerEvent> {
        events
            .iter()
            .filter(|(i, e)| *i == member && !matches!(e, ConsumerEvent::Committed { .. }))
            .map(|(_, e)| e.clone())
            .collect()
    };
    assert!(
        of(0)
            == vec![
                ConsumerEvent::Joined {
                    member_id: a.clone(),
                    generation: 1
                },
                ConsumerEvent::Assigned {
                    partitions: partitions("orders", &[0, 1, 2])
                },
                ConsumerEvent::Revoked {
                    partitions: partitions("orders", &[0, 1, 2])
                },
                ConsumerEvent::Joined {
                    member_id: a.clone(),
                    generation: 2
                },
                ConsumerEvent::Assigned {
                    partitions: partitions("orders", &[0, 1])
                },
            ]
    );
    assert!(
        of(1)
            == vec![
                ConsumerEvent::Joined {
                    member_id: b.clone(),
                    generation: 2
                },
                ConsumerEvent::Assigned {
                    partitions: partitions("orders", &[2])
                },
            ]
    );

    // The second member closes: it commits what it read and leaves, and the
    // first takes its partition from the committed offset.
    with_member(&mut h, 1, Consumer::close);
    assert!(h.run_until(|h| !h.seen(LeaveGroupRequest::API_KEY).is_empty(), 100));
    assert!(
        decoded::<LeaveGroupRequest>(&h)
            == vec![LeaveGroupRequest {
                group_id: "billing".to_string(),
                members: vec![MemberIdentity {
                    member_id: b.clone(),
                    group_instance_id: None,
                    reason: Some("the consumer is being closed".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }]
    );
    assert!(h.run_until(|h| h.client.get(0).assignment().len() == 3, 10_000));
    assert!(h.run_until(|h| h.client.get(0).position("orders", 2).is_some(), 2_000));
    assert!(h.client.get(0).position("orders", 2) == Some(1));
    h.run_for(1_000);
    assert!(with_member(&mut h, 0, |c, _| c.poll(500)).is_empty());
    assert!(h.client.get(0).generation() == 3);
}

#[test]
fn a_crashed_member_leaves_the_group_when_its_session_expires() {
    let state = cluster(&[("orders", 3)]);
    for partition in 0..3 {
        seed(&state, "orders", partition, &[&format!("m{partition}")]);
    }
    let mut h = two_members(&state, 10_000);
    h.with_client(|members, _| members.crash(1));
    let crashed_at = h.now();
    // The coordinator removes the silent member within `session.timeout.ms`
    // of its last heartbeat; the survivor learns of the rebalance at its next
    // heartbeat, at most `heartbeat.interval.ms` later.
    assert!(h.run_until(|h| h.client.get(0).assignment().len() == 3, 20_000));
    let took = h.now() - crashed_at;
    assert!((7_000..=13_500).contains(&took), "took {took} ms");
    assert!(h.client.get(0).generation() == 3);
    assert!(state.borrow().groups["billing"].members.len() == 1);
}

/// What the coordinator answers, heartbeat by heartbeat: the join gets both
/// partitions of `orders` at epoch 1; the acknowledgement nothing new; the
/// next heartbeat takes partition 1 away and keeps epoch 1 until the member
/// gives it up; the acknowledgement of that gets epoch 2; the next heartbeat
/// is fenced; the join after it gets both partitions at epoch 3.
fn fencing_script() -> VecDeque<CghAnswer> {
    let answer = |error_code: i16, member_epoch: i32, assignment: Option<&[i32]>| CghAnswer {
        error_code,
        member_epoch,
        assignment: assignment.map(|p| vec![("orders".to_string(), p.to_vec())]),
    };
    [
        answer(codes::NONE, 1, Some(&[0, 1])),
        answer(codes::NONE, 1, None),
        answer(codes::NONE, 1, Some(&[0])),
        answer(codes::NONE, 2, None),
        answer(codes::FENCED_MEMBER_EPOCH, 2, None),
        answer(codes::NONE, 3, Some(&[0, 1])),
    ]
    .into()
}

/// The heartbeats a member of `billing` sends through `fencing_script`: all
/// fields when it joins, then only the partitions it owns when they change.
fn fencing_heartbeats(member_id: &str, topic_id: Uuid) -> Vec<ConsumerGroupHeartbeatRequest> {
    let owned = |indexes: &[i32]| {
        Some(
            (!indexes.is_empty())
                .then(|| TopicPartitions {
                    topic_id,
                    partitions: indexes.to_vec(),
                    ..Default::default()
                })
                .into_iter()
                .collect(),
        )
    };
    let heartbeat = |member_epoch: i32| ConsumerGroupHeartbeatRequest {
        group_id: "billing".to_string(),
        member_id: member_id.to_string(),
        member_epoch,
        rebalance_timeout_ms: -1,
        ..Default::default()
    };
    let join = ConsumerGroupHeartbeatRequest {
        rebalance_timeout_ms: 300_000,
        subscribed_topic_names: Some(vec!["orders".to_string()]),
        topic_partitions: owned(&[]),
        ..heartbeat(0)
    };
    vec![
        join.clone(),
        ConsumerGroupHeartbeatRequest {
            topic_partitions: owned(&[0, 1]),
            ..heartbeat(1)
        },
        heartbeat(1),
        ConsumerGroupHeartbeatRequest {
            topic_partitions: owned(&[0]),
            ..heartbeat(1)
        },
        heartbeat(2),
        join,
        ConsumerGroupHeartbeatRequest {
            topic_partitions: owned(&[0, 1]),
            ..heartbeat(3)
        },
    ]
}

#[test]
fn a_kip848_member_commits_before_it_revokes_and_joins_again_when_fenced() {
    let state = cluster(&[("orders", 2)]);
    seed(&state, "orders", 0, &["x0"]);
    seed(&state, "orders", 1, &["y0"]);
    state.borrow_mut().knobs.cgh_script = fencing_script();
    let topic_id = state.borrow().topics["orders"].id;
    let consumer = consumer(config(GroupProtocol::Consumer, AutoOffsetReset::Earliest));
    let mut h = Harness::new(consumer, Rc::clone(&state));
    h.with_client(|c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.buffered() == 2, 2_000));
    assert!(
        summary(&h.with_client(|c, _| c.poll(500)))
            == vec![(0, 0, "x0".to_string()), (1, 0, "y0".to_string())]
    );
    assert!(h.run_until(
        |h| h.client.generation() == 3 && h.client.assignment().len() == 2,
        20_000
    ));
    // Let the acknowledgement of the last assignment reach the coordinator.
    h.run_for(100);
    let member_id = h.client.member_id().to_string();
    let heartbeats: Vec<ConsumerGroupHeartbeatRequest> = decoded(&h);
    assert!(heartbeats[..7] == fencing_heartbeats(&member_id, topic_id)[..]);
    // The commit of the consumed positions reached the coordinator after
    // the answer that took partition 1 away and before the acknowledgement.
    let order: Vec<i16> = state
        .borrow()
        .requests
        .iter()
        .map(|r| r.api_key)
        .filter(|key| *key == 8 || *key == 68)
        .collect();
    assert!(order[..5] == [68, 68, 68, 8, 68]);
    let commit: OffsetCommitRequest = h.seen(OffsetCommitRequest::API_KEY)[0].decode();
    assert!(commit.generation_id_or_member_epoch == 1);
    assert!(commit.member_id == member_id);
    let offsets: Vec<(i32, i64)> = commit.topics[0]
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.committed_offset))
        .collect();
    assert!(offsets == vec![(0, 1), (1, 1)]);
    let events: Vec<ConsumerEvent> = h
        .take_events()
        .into_iter()
        .filter(|e| !matches!(e, ConsumerEvent::Committed { .. }))
        .collect();
    let joined = |generation: i32| ConsumerEvent::Joined {
        member_id: member_id.clone(),
        generation,
    };
    assert!(
        events
            == vec![
                joined(1),
                ConsumerEvent::Assigned {
                    partitions: partitions("orders", &[0, 1])
                },
                ConsumerEvent::Revoked {
                    partitions: partitions("orders", &[1])
                },
                ConsumerEvent::Lost {
                    partitions: partitions("orders", &[0])
                },
                joined(3),
                ConsumerEvent::Assigned {
                    partitions: partitions("orders", &[0, 1])
                },
            ]
    );
}

#[test]
fn a_kip848_member_joins_with_a_kafka_member_id_and_leaves_with_epoch_minus_one() {
    let state = cluster(&[("orders", 2)]);
    seed(&state, "orders", 1, &["y0"]);
    let consumer = consumer(config(GroupProtocol::Consumer, AutoOffsetReset::Earliest));
    let mut h = Harness::new(consumer, Rc::clone(&state));
    h.with_client(|c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.buffered() == 1, 2_000));
    // Kafka's `Uuid.randomUuid().toString()`: 22 characters of URL-safe
    // base64.
    let member_id = h.client.member_id().to_string();
    assert!(member_id.len() == 22);
    assert!(
        member_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    );
    assert!(!member_id.starts_with('-'));
    assert!(h.client.assignment() == partitions("orders", &[0, 1]));
    assert!(summary(&h.with_client(|c, _| c.poll(500))) == vec![(1, 0, "y0".to_string())]);
    let closed = h.with_client(Consumer::close);
    assert!(
        closed
            == vec![ConsumerEvent::Revoked {
                partitions: partitions("orders", &[0, 1])
            }]
    );
    h.run_for(100);
    let heartbeats: Vec<ConsumerGroupHeartbeatRequest> = decoded(&h);
    assert!(
        heartbeats.last()
            == Some(&ConsumerGroupHeartbeatRequest {
                group_id: "billing".to_string(),
                member_id,
                member_epoch: -1,
                rebalance_timeout_ms: -1,
                topic_partitions: Some(Vec::new()),
                ..Default::default()
            })
    );
    // The close committed what the member read.
    assert!(
        state.borrow().groups["billing"]
            .committed
            .get(&("orders".to_string(), 1))
            == Some(&(1, 0))
    );
}

#[test]
fn a_fetch_that_meets_a_new_leader_follows_it() {
    let state = cluster(&[("orders", 1)]);
    seed(&state, "orders", 0, &["before"]);
    let consumer = consumer(config(GroupProtocol::Classic, AutoOffsetReset::Earliest));
    let mut h = Harness::new(consumer, Rc::clone(&state));
    h.with_client(|c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.buffered() == 1, 2_000));
    assert!(summary(&h.with_client(|c, _| c.poll(500))) == vec![(0, 0, "before".to_string())]);
    state.borrow_mut().set_leader("orders", 0, 2);
    seed(&state, "orders", 0, &["after"]);
    assert!(h.run_until(|h| h.client.buffered() == 1, 3_000));
    assert!(summary(&h.with_client(|c, _| c.poll(500))) == vec![(0, 1, "after".to_string())]);
    // Broker 1 answered NOT_LEADER_OR_FOLLOWER with the new leader in its
    // answer (KIP-951), and the fetches moved to broker 2.
    let brokers: Vec<NodeId> = h
        .seen(FetchRequest::API_KEY)
        .iter()
        .map(|s| s.broker)
        .collect();
    let moved = brokers.iter().position(|b| *b == NodeId(2)).unwrap();
    assert!(brokers[..moved].iter().all(|b| *b == NodeId(1)));
    assert!(brokers[moved..].iter().all(|b| *b == NodeId(2)));
    assert!(h.take_events().contains(&ConsumerEvent::Error {
        api: "Fetch",
        code: codes::NOT_LEADER_OR_FOLLOWER,
    }));
    assert!(
        h.client
            .client()
            .metadata()
            .partition("orders", 0)
            .unwrap()
            .leader_epoch
            == 1
    );
}
