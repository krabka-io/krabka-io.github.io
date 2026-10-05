//! The consumer against the fake broker: the classic join, sync, positions,
//! fetch and commit; the range split and the rebalances when members come
//! and go; KIP-848 reconciliation; a fetch that follows a new leader; manual
//! assignment and seeks; static membership; and when auto-commits go out.

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        find_coordinator_request::FindCoordinatorRequest,
        heartbeat_request::HeartbeatRequest,
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
    AutoOffsetReset, ClientOptions, ConsumedRecord, Consumer, ConsumerConfig, ConsumerError,
    ConsumerEvent, CoordinatorType, GroupProtocol, IsolationLevel, KafkaClient, MemberState,
    assignor::{encode_assignment, encode_subscription},
    batch::BatchRecord,
    conn_base,
    fake_broker::{CghAnswer, ClusterState, Seen},
    test_support::{CLIENT_NODE, Harness, Members, client, cluster},
};
use crate::lab::{
    codes,
    net::{Ctx, Endpoint, Millis, NodeId},
    testing::CtxBuffers,
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
    // Kafka 4.3 auto-commits only in `poll`: no tick commits, and the first
    // poll after `auto.commit.interval.ms` sends every position to the
    // coordinator, with the member's generation.
    h.run_for(6_000);
    assert!(h.seen(OffsetCommitRequest::API_KEY).is_empty());
    assert!(h.with_client(|c, ctx| c.poll_at(ctx, 500)).is_empty());
    assert!(h.run_until(|h| !h.seen(OffsetCommitRequest::API_KEY).is_empty(), 100));
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
    // Kafka's `leaveGroup` unsubscribes before the heartbeat is built, so it
    // reports an empty subscription as well as no partitions.
    let heartbeats: Vec<ConsumerGroupHeartbeatRequest> = decoded(&h);
    assert!(
        heartbeats.last()
            == Some(&ConsumerGroupHeartbeatRequest {
                group_id: "billing".to_string(),
                member_id,
                member_epoch: -1,
                rebalance_timeout_ms: -1,
                subscribed_topic_names: Some(Vec::new()),
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

/// How a consumer of these tests takes its partitions.
#[derive(Clone, Copy, Debug)]
enum Takes {
    /// `assign` without a group.
    Assign,
    /// `subscribe` in the group `billing` with a protocol.
    Subscribe(GroupProtocol),
}

impl Takes {
    fn consumer(self) -> Consumer {
        match self {
            Self::Assign => groupless(AutoOffsetReset::Earliest),
            Self::Subscribe(protocol) => consumer(config(protocol, AutoOffsetReset::Earliest)),
        }
    }

    fn take(self, h: &mut Harness<Consumer>) {
        match self {
            Self::Assign => {
                assert!(h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0)])) == Ok(()));
            }
            Self::Subscribe(_) => h.with_client(|c, _| c.subscribe(&["orders"])),
        }
    }
}

/// The ways a consumer takes partitions, each against a broker that
/// answers a partition without a leader with `LEADER_NOT_AVAILABLE`, as
/// Kafka's do, and with `NONE`, which asks for nothing by itself.
fn leaderless_rows() -> Vec<(Takes, i16)> {
    let takes = [
        Takes::Assign,
        Takes::Subscribe(GroupProtocol::Classic),
        Takes::Subscribe(GroupProtocol::Consumer),
    ];
    takes
        .into_iter()
        .flat_map(|t| [(t, codes::LEADER_NOT_AVAILABLE), (t, codes::NONE)])
        .collect()
}

#[test]
fn partitions_without_a_leader_at_the_first_metadata_answer_are_read_once_the_leader_is_back() {
    // As the producer's records do, the consumer's partitions that need a
    // leader ask for the metadata while they have none: Kafka's
    // `OffsetFetcher.groupListOffsetRequests` and
    // `AbstractFetch.maybeNodeForPosition` call `requestUpdate` for a
    // partition without a leader on every poll. The consumer's first
    // answer, to a request for every topic, shows `orders` without a leader
    // before the consumer takes it, as right after every broker restarted;
    // the leader comes back on broker 2 a second after the consumer took
    // the partition, and the consumer reads the partition within the most
    // the metadata backoff grows to (1 s) and a few round trips.
    for (takes, code) in leaderless_rows() {
        let name = format!("{takes:?}, {code}");
        let state = cluster(&[("orders", 1)]);
        seed(&state, "orders", 0, &["a", "b"]);
        {
            let mut s = state.borrow_mut();
            s.knobs.leaderless_error = code;
            s.set_leader("orders", 0, -1);
        }
        let mut h = Harness::new(takes.consumer(), Rc::clone(&state));
        assert!(
            h.run_until(|h| h.client.client().metadata().updated_at.is_some(), 1_000),
            "{name}"
        );
        h.run_for(1_000);
        takes.take(&mut h);
        h.run_for(1_000);
        assert!(
            h.client.assignment() == partitions("orders", &[0]),
            "{name}"
        );
        assert!(h.client.buffered() == 0, "{name}");
        state.borrow_mut().set_leader("orders", 0, 2);
        assert!(h.run_until(|h| h.client.buffered() == 2, 1_200), "{name}");
        let polled = h.with_client(|c, _| c.poll(500));
        assert!(
            polled == vec![consumed(0, 0, 1_000, "a"), consumed(0, 1, 1_001, "b")],
            "{name}"
        );
        let listed: Vec<NodeId> = h
            .seen(ListOffsetsRequest::API_KEY)
            .iter()
            .map(|s| s.broker)
            .collect();
        assert!(listed == vec![NodeId(2)], "{name}");
    }
}

#[test]
fn a_fetch_whose_partition_loses_its_leader_goes_on_once_the_leader_is_back() {
    // The partition loses its leader under a fetch: broker 1 answers
    // `NOT_LEADER_OR_FOLLOWER` without a new leader, and the consumer asks
    // for the metadata until one shows, as Kafka's
    // `AbstractFetch.maybeNodeForPosition` does on every poll. The leader
    // comes back on broker 2 a second later, and the next record is read
    // within the most the metadata backoff grows to (1 s) and a few round
    // trips.
    for (takes, code) in leaderless_rows() {
        let name = format!("{takes:?}, {code}");
        let state = cluster(&[("orders", 1)]);
        seed(&state, "orders", 0, &["before"]);
        state.borrow_mut().knobs.leaderless_error = code;
        let mut h = Harness::new(takes.consumer(), Rc::clone(&state));
        takes.take(&mut h);
        assert!(h.run_until(|h| h.client.buffered() == 1, 2_000), "{name}");
        let polled = h.with_client(|c, _| c.poll(500));
        assert!(polled == vec![consumed(0, 0, 1_000, "before")], "{name}");
        state.borrow_mut().set_leader("orders", 0, -1);
        seed(&state, "orders", 0, &["after"]);
        h.run_for(1_000);
        assert!(h.client.buffered() == 0, "{name}");
        state.borrow_mut().set_leader("orders", 0, 2);
        assert!(h.run_until(|h| h.client.buffered() == 1, 1_200), "{name}");
        let polled = h.with_client(|c, _| c.poll(500));
        assert!(
            polled
                == vec![ConsumedRecord {
                    leader_epoch: 1,
                    ..consumed(0, 1, 1_000, "after")
                }],
            "{name}"
        );
        let last_fetch = h.seen(FetchRequest::API_KEY).last().map(|s| s.broker);
        assert!(last_fetch == Some(NodeId(2)), "{name}");
    }
}

/// What a test does to the partitions of `orders`.
#[derive(Clone, Copy, Debug)]
enum Change {
    /// The topic is created with three partitions.
    Create,
    /// The topic grows from one partition to three.
    Grow,
}

#[test]
fn a_classic_leader_joins_again_when_the_partitions_of_its_topics_change() {
    // The leader assigns the partitions the metadata knows and keeps their
    // counts, Kafka's `assignmentSnapshot`. Once `orders` is created, or
    // gains partitions, the metadata no longer matches, and the leader
    // joins again (`ConsumerCoordinator.rejoinNeededOrPending`). The client
    // keeps asking for a topic it was told is unknown
    // (`Metadata.handleMetadataResponse`), so a created topic shows within
    // the metadata backoff; new partitions of a known topic show at the
    // next refresh, `metadata.max.age.ms` (5 min) later. Rows: the change,
    // the partitions held before it, and the time it may take to be
    // assigned.
    let rows = [
        (Change::Create, vec![], 5_000),
        (Change::Grow, vec![0], 310_000),
    ];
    for (change, before, within) in rows {
        let state = cluster(&[]);
        if let Change::Grow = change {
            state.borrow_mut().add_topic("orders", 1, 3);
        }
        let config = ConsumerConfig {
            enable_auto_commit: false,
            ..config(GroupProtocol::Classic, AutoOffsetReset::Earliest)
        };
        let mut h = Harness::new(consumer(config), Rc::clone(&state));
        h.with_client(|c, _| c.subscribe(&["orders"]));
        assert!(
            h.run_until(|h| h.client.state() == MemberState::Stable, 2_000),
            "{change:?}"
        );
        assert!(
            h.client.assignment() == partitions("orders", &before),
            "{change:?}"
        );
        let member_id = h.client.member_id().to_string();
        h.take_events();
        match change {
            Change::Create => state.borrow_mut().add_topic("orders", 3, 3),
            Change::Grow => state.borrow_mut().add_partitions("orders", 3),
        }
        assert!(
            h.run_until(|h| h.client.assignment().len() == 3, within),
            "{change:?}"
        );
        let revoked = (!before.is_empty()).then(|| ConsumerEvent::Revoked {
            partitions: partitions("orders", &before),
        });
        let expected: Vec<ConsumerEvent> = revoked
            .into_iter()
            .chain([
                ConsumerEvent::Joined {
                    member_id,
                    generation: 2,
                },
                ConsumerEvent::Assigned {
                    partitions: partitions("orders", &[0, 1, 2]),
                },
            ])
            .collect();
        assert!(h.take_events() == expected, "{change:?}");
    }
}

/// A consumer of the fake cluster without a group, Kafka's unset
/// `group.id`, with the reset policy `reset`.
fn groupless(reset: AutoOffsetReset) -> Consumer {
    consumer(ConsumerConfig {
        group_id: String::new(),
        auto_offset_reset: reset,
        ..ConsumerConfig::default()
    })
}

/// The offsets of `records`.
fn offsets(records: &[ConsumedRecord]) -> Vec<i64> {
    records.iter().map(|r| r.offset).collect()
}

/// The `ListOffsets` of partition 0 of `orders` at `timestamp`, leader
/// epoch 0.
fn list_offsets_at(timestamp: i64) -> ListOffsetsRequest {
    let mut request = earliest(0);
    request.topics[0].partitions[0].timestamp = timestamp;
    request
}

#[test]
fn a_consumer_without_a_group_reads_the_partitions_it_assigns_itself() {
    let state = cluster(&[("orders", 2)]);
    seed(&state, "orders", 0, &["a0", "a1"]);
    seed(&state, "orders", 1, &["b0"]);
    let mut h = Harness::new(groupless(AutoOffsetReset::Earliest), Rc::clone(&state));
    assert!(h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0), ("orders", 1)])) == Ok(()));
    assert!(h.client.is_manually_assigned());
    assert!(h.run_until(|h| h.client.buffered() == 3, 2_000));
    // Kafka's `assign` without a group: no coordinator, no membership and
    // no committed offsets, so `earliest` asks each leader.
    let group_apis = [
        FindCoordinatorRequest::API_KEY,
        JoinGroupRequest::API_KEY,
        HeartbeatRequest::API_KEY,
        ConsumerGroupHeartbeatRequest::API_KEY,
        OffsetFetchRequest::API_KEY,
    ];
    for api in group_apis {
        assert!(h.seen(api).is_empty(), "api {api}");
    }
    let listed: Vec<(NodeId, ListOffsetsRequest)> = h
        .seen(ListOffsetsRequest::API_KEY)
        .iter()
        .map(|s| (s.broker, s.decode()))
        .collect();
    assert!(listed == vec![(NodeId(1), earliest(0)), (NodeId(2), earliest(1))]);
    assert!(
        h.with_client(|c, ctx| c.poll_at(ctx, 500))
            == vec![
                consumed(0, 0, 1_000, "a0"),
                consumed(0, 1, 1_001, "a1"),
                consumed(1, 0, 1_000, "b0"),
            ]
    );
    // Without a group nothing is committed, and closing leaves no group
    // and revokes nothing.
    h.with_client(Consumer::commit);
    assert!(h.with_client(Consumer::close).is_empty());
    h.run_for(100);
    assert!(h.seen(OffsetCommitRequest::API_KEY).is_empty());
    assert!(h.seen(LeaveGroupRequest::API_KEY).is_empty());
    assert!(h.take_events().is_empty());
}

#[test]
fn a_manual_assignment_with_a_group_starts_from_the_commit_and_commits_outside_the_generations() {
    // Rows: the group protocol and the instance id its commit carries. The
    // classic consumer commits a manual assignment with Kafka's
    // `NO_GENERATION` and no instance id (`sendOffsetCommitRequest`); the
    // KIP-848 consumer names its configured instance
    // (`CommitRequestManager`). Neither joins the group.
    let rows = [
        (GroupProtocol::Classic, None),
        (GroupProtocol::Consumer, Some("i-1".to_string())),
    ];
    for (protocol, commit_instance) in rows {
        let state = cluster(&[("orders", 1)]);
        seed(&state, "orders", 0, &["r0", "r1", "r2"]);
        state
            .borrow_mut()
            .groups
            .entry("billing".to_string())
            .or_default()
            .committed
            .insert(("orders".to_string(), 0), (1, 0));
        let config = ConsumerConfig {
            group_instance_id: Some("i-1".to_string()),
            ..config(protocol, AutoOffsetReset::Earliest)
        };
        let mut h = Harness::new(consumer(config), Rc::clone(&state));
        assert!(
            h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0)])) == Ok(()),
            "{protocol:?}"
        );
        assert!(
            h.run_until(|h| h.client.buffered() == 2, 2_000),
            "{protocol:?}"
        );
        assert!(
            decoded::<OffsetFetchRequest>(&h)
                == vec![OffsetFetchRequest {
                    groups: vec![OffsetFetchRequestGroup {
                        group_id: "billing".to_string(),
                        member_id: None,
                        member_epoch: -1,
                        topics: Some(vec![OffsetFetchRequestTopics {
                            name: "orders".to_string(),
                            partition_indexes: vec![0],
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    require_stable: true,
                    ..Default::default()
                }],
            "{protocol:?}"
        );
        assert!(
            offsets(&h.with_client(|c, _| c.poll(500))) == vec![1, 2],
            "{protocol:?}"
        );
        h.with_client(Consumer::commit);
        assert!(
            h.run_until(|h| h.client.committed("orders", 0) == Some(3), 1_000),
            "{protocol:?}"
        );
        assert!(
            decoded::<OffsetCommitRequest>(&h)
                == vec![OffsetCommitRequest {
                    group_instance_id: commit_instance,
                    ..commit_request(-1, "", &[(0, 3)])
                }],
            "{protocol:?}"
        );
        let membership_apis = [
            JoinGroupRequest::API_KEY,
            HeartbeatRequest::API_KEY,
            ConsumerGroupHeartbeatRequest::API_KEY,
        ];
        for api in membership_apis {
            assert!(h.seen(api).is_empty(), "{protocol:?}: api {api}");
        }
    }
}

/// How a test moves a partition's position.
#[derive(Clone, Copy, Debug)]
enum Move {
    To(i64),
    Beginning,
    End,
}

#[test]
fn seeks_move_the_position_and_drop_what_was_fetched_for_the_old_one() {
    // The log holds r0 to r4 when the consumer fetches them all and hands
    // out r0 and r1; r5 arrives before the move. Rows: the move, the
    // offsets polled after it, and the `ListOffsets` it took: a seek needs
    // none, Kafka's `seekToBeginning` and `seekToEnd` resolve lazily with
    // one at -2 and at -1.
    let rows = [
        (Move::To(1), vec![1, 2, 3, 4, 5], vec![]),
        (Move::To(4), vec![4, 5], vec![]),
        (
            Move::Beginning,
            vec![0, 1, 2, 3, 4, 5],
            vec![list_offsets_at(-2)],
        ),
        (Move::End, vec![], vec![list_offsets_at(-1)]),
    ];
    for (movement, polled, listed) in rows {
        let state = cluster(&[("orders", 1)]);
        seed(&state, "orders", 0, &["r0", "r1", "r2", "r3", "r4"]);
        let mut h = Harness::new(groupless(AutoOffsetReset::Earliest), Rc::clone(&state));
        assert!(h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0)])) == Ok(()));
        assert!(
            h.run_until(|h| h.client.buffered() == 5, 2_000),
            "{movement:?}"
        );
        assert!(
            offsets(&h.with_client(|c, _| c.poll(2))) == vec![0, 1],
            "{movement:?}"
        );
        seed(&state, "orders", 0, &["r5"]);
        let before = h.seen(ListOffsetsRequest::API_KEY).len();
        let moved = h.with_client(|c, _| match movement {
            Move::To(offset) => c.seek("orders", 0, offset),
            Move::Beginning => c.seek_to_beginning(&[("orders", 0)]),
            Move::End => c.seek_to_end(&[]),
        });
        assert!(moved == Ok(()), "{movement:?}");
        h.run_for(1_000);
        assert!(
            offsets(&h.with_client(|c, _| c.poll(500))) == polled,
            "{movement:?}"
        );
        let taken: Vec<ListOffsetsRequest> = decoded::<ListOffsetsRequest>(&h)[before..].to_vec();
        assert!(taken == listed, "{movement:?}");
    }
}

/// What a test does to a consumer before the call it checks.
type Setup = fn(&mut Consumer, &mut Ctx<'_>);

/// The call a test checks.
type Call = fn(&mut Consumer, &mut Ctx<'_>) -> Result<(), ConsumerError>;

/// A refused call: its name, the setup, the call, the error with its text,
/// and the position of `orders-0` after the call.
type Refusal = (
    &'static str,
    Setup,
    Call,
    ConsumerError,
    &'static str,
    Option<i64>,
);

#[test]
fn a_call_kafka_refuses_is_refused_with_kafkas_text() {
    fn assigned(c: &mut Consumer, ctx: &mut Ctx<'_>) {
        assert!(c.assign(ctx, &[("orders", 0)]) == Ok(()));
        assert!(c.seek("orders", 0, 5) == Ok(()));
    }
    let not_assigned = ConsumerError::NotAssigned {
        topic: "orders".to_string(),
        partition: 1,
    };
    // Rows: the setup, the call, the error with its text, and the position
    // of `orders-0` after the call. As in Kafka, `seekToBeginning` moves the
    // partitions before the first one the consumer does not hold, and
    // `assign` checks the topic names before the subscription.
    let rows: [Refusal; 8] = [
        (
            "a seek on a partition the consumer does not hold",
            assigned,
            |c, _| c.seek("orders", 1, 0),
            not_assigned.clone(),
            "No current assignment for partition orders-1",
            Some(5),
        ),
        (
            "a seek to a negative offset",
            assigned,
            |c, _| c.seek("orders", 0, -1),
            ConsumerError::NegativeOffset,
            "seek offset must not be a negative number",
            Some(5),
        ),
        (
            "a seek to the beginning of a partition the consumer does not hold",
            assigned,
            |c, _| c.seek_to_beginning(&[("orders", 0), ("orders", 1)]),
            not_assigned,
            "No current assignment for partition orders-1",
            None,
        ),
        (
            "an assignment after a subscription",
            |c, _| c.subscribe(&["orders"]),
            |c, ctx| c.assign(ctx, &[("orders", 0)]),
            ConsumerError::Subscribed,
            "Subscription to topics, partitions and pattern are mutually exclusive",
            None,
        ),
        (
            "an empty assignment after a subscription",
            |c, _| c.subscribe(&["orders"]),
            |c, ctx| c.assign(ctx, &[]),
            ConsumerError::Subscribed,
            "Subscription to topics, partitions and pattern are mutually exclusive",
            None,
        ),
        (
            "an assignment of a blank topic",
            |_, _| {},
            |c, ctx| c.assign(ctx, &[(" \t", 0)]),
            ConsumerError::EmptyTopic,
            "Topic partitions to assign to cannot have null or empty topic",
            None,
        ),
        (
            "an assignment of a blank topic after a subscription",
            |c, _| c.subscribe(&["orders"]),
            |c, ctx| c.assign(ctx, &[("orders", 0), ("", 1)]),
            ConsumerError::EmptyTopic,
            "Topic partitions to assign to cannot have null or empty topic",
            None,
        ),
        (
            "a seek after close",
            |c, ctx| {
                assert!(c.assign(ctx, &[("orders", 0)]) == Ok(()));
                c.close(ctx);
            },
            |c, _| c.seek("orders", 0, 1),
            ConsumerError::Closed,
            "This consumer has already been closed.",
            None,
        ),
    ];
    for (name, setup, call, error, text, position) in rows {
        let mut c = consumer(config(GroupProtocol::Classic, AutoOffsetReset::Earliest));
        let mut bufs = CtxBuffers::new(CLIENT_NODE);
        let result = bufs.with(0, |ctx| {
            setup(&mut c, ctx);
            call(&mut c, ctx)
        });
        assert!(result == Err(error), "{name}");
        assert!(result.unwrap_err().to_string() == text, "{name}");
        assert!(c.position("orders", 0) == position, "{name}");
    }
}

#[test]
fn a_fetch_answer_for_a_position_the_consumer_left_is_discarded() {
    let state = cluster(&[("orders", 1)]);
    let topic_id = state.borrow().topics["orders"].id;
    seed(&state, "orders", 0, &["r0", "r1", "r2"]);
    let mut h = Harness::new(groupless(AutoOffsetReset::Earliest), Rc::clone(&state));
    assert!(h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0)])) == Ok(()));
    assert!(h.run_until(|h| h.client.buffered() == 3, 2_000));
    // The poll sends the next fetch, from offset 3, and the broker holds it
    // for `fetch.max.wait.ms`; r3 arrives while it waits.
    assert!(offsets(&h.with_client(|c, ctx| c.poll_at(ctx, 500))) == vec![0, 1, 2]);
    h.run_for(10);
    seed(&state, "orders", 0, &["r3"]);
    assert!(h.with_client(|c, _| c.seek("orders", 0, 1)) == Ok(()));
    // The held fetch answers with r3; the consumer, now at offset 1,
    // discards it as Kafka's `FetchCollector` discards a stale fetch, and
    // fetches from 1.
    assert!(h.run_until(|h| h.client.buffered() == 3, 2_000));
    assert!(offsets(&h.with_client(|c, _| c.poll(500))) == vec![1, 2, 3]);
    assert!(
        decoded::<FetchRequest>(&h)
            == vec![
                fetch_request(topic_id, 0, 0),
                fetch_request(topic_id, 0, 3),
                fetch_request(topic_id, 0, 1),
            ]
    );
}

#[test]
fn an_answer_to_a_reset_that_a_later_seek_replaced_is_ignored() {
    let state = cluster(&[("orders", 1)]);
    seed(&state, "orders", 0, &["r0", "r1", "r2"]);
    let mut h = Harness::new(groupless(AutoOffsetReset::Latest), Rc::clone(&state));
    assert!(h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0)])) == Ok(()));
    assert!(h.run_until(|h| h.client.position("orders", 0) == Some(3), 2_000));
    let before = h.seen(ListOffsetsRequest::API_KEY).len();
    // Both resets leave at once, the end first; the answers come back in
    // that order. Kafka's `maybeSeekUnvalidated` skips the end's answer,
    // because the partition waits for the beginning by then.
    assert!(h.with_client(|c, _| c.seek_to_end(&[])) == Ok(()));
    assert!(h.with_client(|c, _| c.seek_to_beginning(&[])) == Ok(()));
    assert!(h.run_until(|h| h.client.position("orders", 0).is_some(), 1_000));
    assert!(h.client.position("orders", 0) == Some(0));
    assert!(
        decoded::<ListOffsetsRequest>(&h)[before..] == [list_offsets_at(-1), list_offsets_at(-2)]
    );
    assert!(h.run_until(|h| h.client.buffered() == 3, 2_000));
    assert!(offsets(&h.with_client(|c, _| c.poll(500))) == vec![0, 1, 2]);
}

#[test]
fn a_partition_sought_while_its_committed_offset_is_looked_up_keeps_its_position() {
    // The answer to the `OffsetFetch` of a manual assignment is lost, and
    // the consumer seeks to 3 while it waits. When the request times out at
    // `request.timeout.ms` (30 s), the sought partition keeps its position:
    // Kafka looks committed offsets up only for the partitions that still
    // wait for one.
    let state = cluster(&[("orders", 1)]);
    seed(&state, "orders", 0, &["r0", "r1", "r2", "r3", "r4"]);
    state
        .borrow_mut()
        .groups
        .entry("billing".to_string())
        .or_default()
        .committed
        .insert(("orders".to_string(), 0), (1, 0));
    let config = config(GroupProtocol::Classic, AutoOffsetReset::Earliest);
    let mut h = Harness::new(consumer(config), Rc::clone(&state));
    assert!(h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0)])) == Ok(()));
    // The `OffsetFetch` leaves when the coordinator is known.
    assert!(h.run_until(
        |h| {
            h.client
                .client()
                .coordinator(CoordinatorType::Group, "billing")
                .is_some()
        },
        1_000
    ));
    state.borrow_mut().knobs.drop_responses = 1;
    assert!(h.with_client(|c, _| c.seek("orders", 0, 3)) == Ok(()));
    assert!(h.run_until(|h| h.client.buffered() == 2, 1_000));
    assert!(offsets(&h.with_client(|c, _| c.poll(500))) == vec![3, 4]);
    h.run_for(31_000);
    assert!(h.with_client(|c, _| c.poll(500)).is_empty());
    assert!(h.client.position("orders", 0) == Some(5));
    assert!(h.seen(OffsetFetchRequest::API_KEY).len() == 1);
}

/// A static member of `billing` with the instance id `instance` and a
/// session of 10 s.
fn static_member(protocol: GroupProtocol, instance: &str) -> ConsumerConfig {
    ConsumerConfig {
        group_instance_id: Some(instance.to_string()),
        session_timeout_ms: 10_000,
        ..config(protocol, AutoOffsetReset::Earliest)
    }
}

/// A consumer of a process that started again on the node of an earlier
/// one: its client draws connection ids from a lane of its own, so the
/// answers to the earlier process's requests never reach it.
fn restarted(config: ConsumerConfig) -> Consumer {
    let client = KafkaClient::new(
        vec![Endpoint::kafka(NodeId(1))],
        "test",
        ClientOptions {
            conn_base: conn_base(1),
            ..ClientOptions::default()
        },
    );
    Consumer::new(client, config)
}

#[test]
fn a_static_member_that_restarts_within_its_session_keeps_its_place() {
    // Two classic static members hold `orders` by range: `i-a` leads with
    // partitions 0 and 1, `i-b` holds 2. One closes, sending no
    // `LeaveGroup` (Kafka's `maybeLeaveGroup` for a static member), and a
    // new process with its instance id joins with no member id. The
    // coordinator gives it a new member id with the old member's place:
    // the generation stays, the other member sees no rebalance, and a
    // returning leader skips the assignment (KIP-814). Rows: which member
    // restarts, its instance id and its partitions.
    let rows = [(1, "i-b", vec![2]), (0, "i-a", vec![0, 1])];
    for (i, instance, held) in rows {
        let state = cluster(&[("orders", 3)]);
        let members = Members::new(vec![
            (
                NodeId(101),
                consumer(static_member(GroupProtocol::Classic, "i-a")),
            ),
            (
                NodeId(102),
                consumer(static_member(GroupProtocol::Classic, "i-b")),
            ),
        ]);
        let mut h = Harness::new(members, Rc::clone(&state));
        with_member(&mut h, 0, |c, _| c.subscribe(&["orders"]));
        assert!(h.run_until(|h| h.client.get(0).assignment().len() == 3, 2_000));
        with_member(&mut h, 1, |c, _| c.subscribe(&["orders"]));
        assert!(
            h.run_until(
                |h| h.client.get(0).assignment() == partitions("orders", &[0, 1])
                    && h.client.get(1).assignment() == partitions("orders", &[2])
                    && h.client.get(0).state() == MemberState::Stable
                    && h.client.get(1).state() == MemberState::Stable,
                10_000
            ),
            "{instance}"
        );
        with_member(&mut h, i, Consumer::close);
        h.run_for(100);
        h.take_events();
        let joins = h.seen(JoinGroupRequest::API_KEY).len();
        let syncs = h.seen(SyncGroupRequest::API_KEY).len();
        h.with_client(|members, _| {
            members.restart(
                i,
                restarted(static_member(GroupProtocol::Classic, instance)),
            );
        });
        with_member(&mut h, i, |c, _| c.subscribe(&["orders"]));
        assert!(
            h.run_until(|h| h.client.get(i).state() == MemberState::Stable, 2_000),
            "{instance}"
        );
        h.run_for(4_000);
        let new_id = format!("{instance}-{:08x}", 3);
        assert!(h.seen(LeaveGroupRequest::API_KEY).is_empty(), "{instance}");
        assert!(
            decoded::<JoinGroupRequest>(&h)[joins..]
                == [JoinGroupRequest {
                    session_timeout_ms: 10_000,
                    group_instance_id: Some(instance.to_string()),
                    ..join_request("")
                }],
            "{instance}"
        );
        assert!(
            decoded::<SyncGroupRequest>(&h)[syncs..]
                == [SyncGroupRequest {
                    group_id: "billing".to_string(),
                    generation_id: 2,
                    member_id: new_id.clone(),
                    group_instance_id: Some(instance.to_string()),
                    protocol_type: Some("consumer".to_string()),
                    protocol_name: Some("range".to_string()),
                    assignments: Vec::new(),
                    ..Default::default()
                }],
            "{instance}"
        );
        assert!(
            h.take_events()
                == vec![
                    (
                        i,
                        ConsumerEvent::Joined {
                            member_id: new_id,
                            generation: 2,
                        }
                    ),
                    (
                        i,
                        ConsumerEvent::Assigned {
                            partitions: partitions("orders", &held),
                        }
                    ),
                ],
            "{instance}"
        );
        let generations = [h.client.get(0).generation(), h.client.get(1).generation()];
        assert!(generations == [2, 2], "{instance}");
        assert!(
            state.borrow().groups["billing"].members.len() == 2,
            "{instance}"
        );
    }
}

#[test]
fn a_static_member_that_restarts_after_its_session_expired_joins_as_a_new_member() {
    // `i-b` closes without a `LeaveGroup` and stays away past its 10 s
    // session: the coordinator drops the member with its instance id and
    // the group rebalances to `i-a` alone. The new process of `i-b` then
    // joins as a new member, and the group rebalances again.
    let state = cluster(&[("orders", 3)]);
    let members = Members::new(vec![
        (
            NodeId(101),
            consumer(static_member(GroupProtocol::Classic, "i-a")),
        ),
        (
            NodeId(102),
            consumer(static_member(GroupProtocol::Classic, "i-b")),
        ),
    ]);
    let mut h = Harness::new(members, Rc::clone(&state));
    with_member(&mut h, 0, |c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.get(0).assignment().len() == 3, 2_000));
    with_member(&mut h, 1, |c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.get(1).state() == MemberState::Stable, 10_000));
    with_member(&mut h, 1, Consumer::close);
    assert!(h.run_until(
        |h| h.client.get(0).generation() == 3 && h.client.get(0).assignment().len() == 3,
        20_000
    ));
    assert!(state.borrow().groups["billing"].static_members.len() == 1);
    h.with_client(|members, _| {
        members.restart(1, restarted(static_member(GroupProtocol::Classic, "i-b")));
    });
    with_member(&mut h, 1, |c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(
        |h| {
            (0..2).all(|i| {
                h.client.get(i).generation() == 4 && h.client.get(i).state() == MemberState::Stable
            })
        },
        20_000
    ));
    let held: Vec<Vec<(String, i32)>> = (0..2).map(|i| h.client.get(i).assignment()).collect();
    assert!(held == vec![partitions("orders", &[0, 1]), partitions("orders", &[2])]);
    let new_id = h.client.get(1).member_id().to_string();
    assert!(new_id == "i-b-00000003");
    let group = &state.borrow().groups["billing"];
    assert!(
        group.static_members
            == [
                ("i-a".to_string(), "i-a-00000001".to_string()),
                ("i-b".to_string(), new_id),
            ]
            .into()
    );
}

#[test]
fn a_static_member_whose_instance_id_a_new_process_took_is_fenced() {
    // A second process joins with the instance id of a live member. The
    // coordinator gives it the member's place and assignment, and the old
    // member's next heartbeat answers FENCED_INSTANCE_ID (Kafka's
    // `validateMember`), on which Kafka's consumer fails with
    // `FencedInstanceIdException`.
    let state = cluster(&[("orders", 3)]);
    let members = Members::new(vec![
        (
            NodeId(101),
            consumer(static_member(GroupProtocol::Classic, "i-a")),
        ),
        (
            NodeId(102),
            consumer(static_member(GroupProtocol::Classic, "i-a")),
        ),
    ]);
    let mut h = Harness::new(members, Rc::clone(&state));
    with_member(&mut h, 0, |c, _| c.subscribe(&["orders"]));
    assert!(h.run_until(|h| h.client.get(0).assignment().len() == 3, 2_000));
    h.take_events();
    let heartbeats = h.seen(HeartbeatRequest::API_KEY).len();
    with_member(&mut h, 1, |c, _| c.subscribe(&["orders"]));
    let fenced = MemberState::Failed(codes::FENCED_INSTANCE_ID);
    assert!(h.run_until(|h| h.client.get(0).state() == fenced, 5_000));
    let (old_id, new_id) = ("i-a-00000001".to_string(), "i-a-00000002".to_string());
    assert!(
        h.take_events()
            == vec![
                (
                    1,
                    ConsumerEvent::Joined {
                        member_id: new_id.clone(),
                        generation: 1,
                    }
                ),
                (
                    1,
                    ConsumerEvent::Assigned {
                        partitions: partitions("orders", &[0, 1, 2]),
                    }
                ),
                (
                    0,
                    ConsumerEvent::Error {
                        api: "Heartbeat",
                        code: codes::FENCED_INSTANCE_ID,
                    }
                ),
            ]
    );
    assert!(
        decoded::<HeartbeatRequest>(&h)[heartbeats..]
            == [HeartbeatRequest {
                group_id: "billing".to_string(),
                generation_id: 1,
                member_id: old_id,
                group_instance_id: Some("i-a".to_string()),
                ..Default::default()
            }]
    );
    assert!(h.client.get(1).state() == MemberState::Stable);
    let group = &state.borrow().groups["billing"];
    assert!(group.members.keys().collect::<Vec<_>>() == [&new_id]);
    assert!(group.static_members == [("i-a".to_string(), new_id.clone())].into());
}

#[test]
fn a_classic_member_leaves_on_close_unless_it_is_static() {
    // Rows: the instance id, and the `LeaveGroup` requests the coordinator
    // sees: Kafka's `shouldSendLeaveGroupRequest` sends one for a dynamic
    // member only.
    let leave = |member_id: &str| LeaveGroupRequest {
        group_id: "billing".to_string(),
        members: vec![MemberIdentity {
            member_id: member_id.to_string(),
            group_instance_id: None,
            reason: Some("the consumer is being closed".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let rows = [
        (None, vec![leave("member-00000001")]),
        (Some("i-1"), vec![]),
    ];
    for (instance, leaves) in rows {
        let config = ConsumerConfig {
            group_instance_id: instance.map(str::to_string),
            ..config(GroupProtocol::Classic, AutoOffsetReset::Earliest)
        };
        let mut h = Harness::new(consumer(config), cluster(&[("orders", 1)]));
        h.with_client(|c, _| c.subscribe(&["orders"]));
        assert!(
            h.run_until(|h| h.client.state() == MemberState::Stable, 2_000),
            "{instance:?}"
        );
        h.with_client(Consumer::close);
        h.run_for(100);
        assert!(decoded::<LeaveGroupRequest>(&h) == leaves, "{instance:?}");
    }
}

#[test]
fn a_kip848_member_names_its_instance_in_every_heartbeat_and_a_static_one_leaves_with_epoch_minus_two()
 {
    // Rows: the instance id and the epoch of the leave heartbeat, Kafka's
    // `ConsumerMembershipManager.leaveGroupEpoch`.
    let rows = [(None, -1), (Some("i-1"), -2)];
    for (instance, leave_epoch) in rows {
        let instance_id = instance.map(str::to_string);
        let config = ConsumerConfig {
            group_instance_id: instance_id.clone(),
            ..config(GroupProtocol::Consumer, AutoOffsetReset::Earliest)
        };
        let state = cluster(&[("orders", 1)]);
        let topic_id = state.borrow().topics["orders"].id;
        let mut h = Harness::new(consumer(config), Rc::clone(&state));
        h.with_client(|c, _| c.subscribe(&["orders"]));
        assert!(
            h.run_until(|h| h.client.assignment().len() == 1, 2_000),
            "{instance:?}"
        );
        // Let the acknowledgement of the assignment go out.
        h.run_for(100);
        let member_id = h.client.member_id().to_string();
        h.with_client(Consumer::close);
        h.run_for(100);
        let base = ConsumerGroupHeartbeatRequest {
            group_id: "billing".to_string(),
            member_id: member_id.clone(),
            member_epoch: 1,
            instance_id: instance_id.clone(),
            rebalance_timeout_ms: -1,
            ..Default::default()
        };
        assert!(
            decoded::<ConsumerGroupHeartbeatRequest>(&h)
                == vec![
                    ConsumerGroupHeartbeatRequest {
                        member_epoch: 0,
                        rebalance_timeout_ms: 300_000,
                        subscribed_topic_names: Some(vec!["orders".to_string()]),
                        topic_partitions: Some(Vec::new()),
                        ..base.clone()
                    },
                    ConsumerGroupHeartbeatRequest {
                        topic_partitions: Some(vec![TopicPartitions {
                            topic_id,
                            partitions: vec![0],
                            ..Default::default()
                        }]),
                        ..base.clone()
                    },
                    ConsumerGroupHeartbeatRequest {
                        member_epoch: leave_epoch,
                        subscribed_topic_names: Some(Vec::new()),
                        topic_partitions: Some(Vec::new()),
                        ..base
                    },
                ],
            "{instance:?}"
        );
    }
}

/// When the next auto-commit is due, from when the poll that committed ran
/// and when the commit was answered.
type NextCommit = fn(Millis, Millis) -> Millis;

#[test]
fn auto_commits_go_out_in_poll_once_the_interval_passed() {
    // Kafka 4.3 auto-commits only in `poll`, with either protocol:
    // `ConsumerCoordinator.poll` and the KIP-848 consumer's `AsyncPollEvent`.
    // A tick never commits; a poll before the interval takes records and
    // commits nothing; the poll after it commits every position. The next
    // auto-commit is due `auto.commit.interval.ms` after that poll, or
    // `retry.backoff.ms` after a retriable failure. Rows: the protocol, the
    // errors the coordinator answers commits with, and when the next
    // auto-commit is due, from the time of the poll and of the answer.
    let after_the_interval: NextCommit = |polled_at, _| polled_at + 5_000;
    let after_the_backoff: NextCommit = |_, answered_at| answered_at + 100;
    let rows: [(GroupProtocol, &[i16], NextCommit); 4] = [
        (GroupProtocol::Classic, &[], after_the_interval),
        (GroupProtocol::Consumer, &[], after_the_interval),
        (
            GroupProtocol::Classic,
            &[codes::COORDINATOR_LOAD_IN_PROGRESS],
            after_the_backoff,
        ),
        (
            GroupProtocol::Consumer,
            &[codes::COORDINATOR_LOAD_IN_PROGRESS],
            after_the_backoff,
        ),
    ];
    for (protocol, errors, next) in rows {
        let state = cluster(&[("orders", 1)]);
        seed(&state, "orders", 0, &["r0", "r1"]);
        let mut h = Harness::new(
            consumer(config(protocol, AutoOffsetReset::Earliest)),
            Rc::clone(&state),
        );
        h.with_client(|c, _| c.subscribe(&["orders"]));
        assert!(
            h.run_until(|h| h.client.buffered() == 2, 2_000),
            "{protocol:?}"
        );
        let due = h.client.next_auto_commit().unwrap();
        assert!(
            offsets(&h.with_client(|c, ctx| c.poll_at(ctx, 500))) == vec![0, 1],
            "{protocol:?}"
        );
        h.run_for(due + 1_000 - h.now());
        assert!(
            h.seen(OffsetCommitRequest::API_KEY).is_empty(),
            "{protocol:?}"
        );
        state.borrow_mut().knobs.commit_errors = errors.iter().copied().collect();
        let polled_at = h.now();
        assert!(
            h.with_client(|c, ctx| c.poll_at(ctx, 500)).is_empty(),
            "{protocol:?}"
        );
        // The commit in flight holds the next one back until it answers.
        assert!(h.client.next_auto_commit().is_none(), "{protocol:?}");
        assert!(
            h.run_until(|h| h.client.next_auto_commit().is_some(), 100),
            "{protocol:?}"
        );
        let answered_at = h.now();
        assert!(
            decoded::<OffsetCommitRequest>(&h)
                == vec![commit_request(
                    h.client.generation(),
                    h.client.member_id(),
                    &[(0, 2)]
                )],
            "{protocol:?}"
        );
        assert!(
            h.client.next_auto_commit() == Some(next(polled_at, answered_at)),
            "{protocol:?}"
        );
    }
}

// ---- read_committed ---------------------------------------------------------------

#[test]
fn read_committed_skips_aborted_transactions_and_moves_past_the_markers() {
    let state = cluster(&[("orders", 1)]);
    let mut producer = Harness::new(super::producer_tests::transactional(), Rc::clone(&state));
    producer.run_until(|h| h.client.producer_id().is_some(), 1_000);
    super::producer_tests::transaction(&mut producer, &["a", "b"], true);
    super::producer_tests::transaction(&mut producer, &["c"], false);
    super::producer_tests::transaction(&mut producer, &["d"], true);
    // Rows: the isolation level and the values a poll hands out.
    let rows = [
        (IsolationLevel::ReadCommitted, vec!["a", "b", "d"]),
        (IsolationLevel::ReadUncommitted, vec!["a", "b", "c", "d"]),
    ];
    for (isolation_level, expected) in rows {
        let consumer = consumer(ConsumerConfig {
            group_id: String::new(),
            auto_offset_reset: AutoOffsetReset::Earliest,
            isolation_level,
            ..ConsumerConfig::default()
        });
        let mut h = Harness::new(consumer, Rc::clone(&state));
        assert!(h.with_client(|c, ctx| c.assign(ctx, &[("orders", 0)])) == Ok(()));
        assert!(h.run_until(|h| h.client.buffered() == expected.len(), 2_000));
        let polled = h.with_client(|c, ctx| c.poll_at(ctx, 500));
        let values: Vec<String> = summary(&polled).into_iter().map(|(_, _, v)| v).collect();
        assert!(values == expected, "{isolation_level:?}");
        // The cluster's request log holds the rows before too.
        let fetch: FetchRequest = h.seen(FetchRequest::API_KEY).last().unwrap().decode();
        assert!(fetch.isolation_level == isolation_level.as_wire());
        // Past the last record lies only the commit marker: the position
        // moves to the log end, so the lag is 0.
        assert!(
            h.client.position("orders", 0) == Some(7),
            "{isolation_level:?}"
        );
    }
}
