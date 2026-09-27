//! The records a coordinator writes rebuild the same state in another one.

use assert2::assert;
use krabka_protocol::owned::{
    consumer_group_describe_request::ConsumerGroupDescribeRequest,
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
    offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseGroup},
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
};

use super::{super::RecordKey, *};

fn consumer_join(member: &str) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "cg".to_string(),
        member_id: member.to_string(),
        member_epoch: 0,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: Some(vec!["t".to_string()]),
        topic_partitions: Some(Vec::new()),
        ..Default::default()
    }
}

fn streams_join(member: &str) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".to_string(),
        member_id: member.to_string(),
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

fn fetch_all(group: &str) -> OffsetFetchRequest {
    OffsetFetchRequest {
        groups: vec![OffsetFetchRequestGroup {
            group_id: group.to_string(),
            member_id: None,
            member_epoch: -1,
            topics: None,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A classic group with its assignments and offsets, a consumer group and a
/// streams group are rebuilt from the records; the loaded members' sessions
/// start at the load time.
#[test]
fn records_rebuild_the_coordinator() {
    let mut c = stable_two_member_group();
    let topics = Topics::new(&[("t", 2)]);
    let commit = OffsetCommitRequest {
        group_id: "g".to_string(),
        generation_id_or_member_epoch: 1,
        member_id: M1.to_string(),
        topics: vec![OffsetCommitRequestTopic {
            name: "t".to_string(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 1,
                committed_offset: 99,
                committed_leader_epoch: 2,
                committed_metadata: Some("meta".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        c.offset_commit(7000, &commit, 8, &topics).topics[0].partitions[0].error_code
            == codes::NONE
    );
    assert!(
        c.consumer_group_heartbeat(7000, &client("c3"), &consumer_join("m3"), 1, &topics)
            .error_code
            == codes::NONE
    );
    let (streams, _) = c.streams_group_heartbeat(7000, &client("c4"), &streams_join("m4"), &topics);
    assert!(streams.error_code == codes::NONE);

    let records = c.drain_records();
    let keys: Vec<RecordKey> = records
        .iter()
        .map(|(key, _)| serde_json::from_slice(key).expect("a record key"))
        .collect();
    assert!(keys.contains(&RecordKey::ClassicGroup { group: "g".into() }));
    assert!(keys.contains(&RecordKey::Offset {
        group: "g".into(),
        topic: "t".to_string(),
        partition: 1
    }));
    assert!(keys.contains(&RecordKey::ConsumerGroup { group: "cg".into() }));
    assert!(keys.contains(&RecordKey::StreamsGroup {
        group: "app".into()
    }));

    let mut loaded = Coordinator::new(2, CoordinatorConfig::default());
    loaded.load(100_000, records);
    let describe = describe_req(&["g", "cg", "app"]);
    assert!(loaded.describe_groups(&describe, 6) == c.describe_groups(&describe, 6));
    assert!(
        loaded.offset_fetch(&fetch_all("g"), 8, &topics)
            == c.offset_fetch(&fetch_all("g"), 8, &topics)
    );
    let consumer = ConsumerGroupDescribeRequest {
        group_ids: vec!["cg".to_string()],
        ..Default::default()
    };
    assert!(loaded.consumer_group_describe(&consumer) == c.consumer_group_describe(&consumer));
    // The streams group configures its topology again at its next heartbeat.
    let streams_hb = StreamsGroupHeartbeatRequest {
        group_id: "app".to_string(),
        member_id: "m4".to_string(),
        member_epoch: streams.member_epoch,
        ..Default::default()
    };
    let (original, _) = c.streams_group_heartbeat(100_100, &client("c4"), &streams_hb, &topics);
    let (replayed, _) =
        loaded.streams_group_heartbeat(100_100, &client("c4"), &streams_hb, &topics);
    assert!(replayed == original);
    let streams_describe = StreamsGroupDescribeRequest {
        group_ids: vec!["app".to_string()],
        ..Default::default()
    };
    assert!(
        loaded.streams_group_describe(&streams_describe)
            == c.streams_group_describe(&streams_describe)
    );
    assert!(
        loaded.list_groups(&ListGroupsRequest::default())
            == c.list_groups(&ListGroupsRequest::default())
    );

    // The loaded classic members heartbeat at generation 1, and expire from
    // the load time when they do not.
    assert!(loaded.heartbeat(100_200, &hb(M1, 1)) == hb_response(codes::NONE));
    assert!(loaded.next_deadline() == Some(110_000));
    assert!(loaded.on_tick(110_000).is_empty());
    assert!(loaded.heartbeat(110_100, &hb(M2, 1)) == hb_response(codes::UNKNOWN_MEMBER_ID));
    assert!(loaded.heartbeat(110_100, &hb(M1, 1)) == hb_response(codes::REBALANCE_IN_PROGRESS));
    // The member ids the replayed coordinator mints continue past the loaded
    // ones of its own broker only; broker 2 starts its own sequence.
    assert!(
        loaded.join_group(110_200, &client("c9"), &join_req("", None, b"x"), 9)
            == join_error(
                codes::MEMBER_ID_REQUIRED,
                "c9-00000000-0000-0002-0000-000000000001"
            )
    );
}

/// A replay of the same broker's records continues its member id sequence.
#[test]
fn a_replay_on_the_same_broker_does_not_reuse_member_ids() {
    let mut c = stable_two_member_group();
    let records = c.drain_records();
    let mut loaded = coord();
    loaded.load(0, records);
    assert!(
        loaded.join_group(0, &client("c9"), &join_req("", None, b"x"), 9)
            == join_error(
                codes::MEMBER_ID_REQUIRED,
                "c9-00000000-0000-0001-0000-000000000003"
            )
    );
}

/// A tombstone and an unknown key are handled: the group is dropped, the
/// record is skipped.
#[test]
fn tombstones_drop_groups_and_unknown_keys_are_skipped() {
    let mut c = stable_two_member_group();
    let mut records = c.drain_records();
    records.push((
        Bytes::from_static(br#"{"type":"classic_group","group":"g"}"#),
        None,
    ));
    records.push((
        Bytes::from_static(b"not json"),
        Some(Bytes::from_static(b"{}")),
    ));
    let mut loaded = coord();
    loaded.load(0, records);
    assert!(loaded.list_groups(&ListGroupsRequest::default()) == ListGroupsResponse::default());
    assert!(loaded.next_deadline().is_none());
}

/// Kafka's `onUnloaded` and `onLoaded`: the broker that stops leading the
/// group's partition drops the group, its offsets and its timers and answers
/// its held `JoinGroup` with `NOT_COORDINATOR`; the broker that takes the
/// partition over loads the records, which all route to that partition, and
/// serves the last persisted generation.
#[test]
fn a_partition_moves_to_another_coordinator() {
    let mut old = stable_two_member_group();
    let topics = Topics::new(&[("t", 2)]);
    let commit = OffsetCommitRequest {
        group_id: "g".to_string(),
        generation_id_or_member_epoch: 1,
        member_id: M1.to_string(),
        topics: vec![OffsetCommitRequestTopic {
            name: "t".to_string(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 1,
                committed_offset: 99,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        old.offset_commit(7000, &commit, 8, &topics).topics[0].partitions[0].error_code
            == codes::NONE
    );
    let offsets = old.offset_fetch(&fetch_all("g"), 8, &topics);
    // The leader joins again, and the round waits for the follower.
    assert!(
        old.join_group(7100, &client("c1"), &join_req(M1, None, b"m1-meta"), 9)
            == Pending::Held(HoldToken(4))
    );
    let records = old.drain_records();
    let partition = group_partition("g");
    assert!(
        records
            .iter()
            .all(|(key, _)| RecordKey::decode(key).map(|key| key.partition()) == Some(partition))
    );

    assert!(old.unload((partition + 1) % 50).is_empty());
    assert!(
        old.unload(partition)
            == vec![join_completion(
                4,
                JoinGroupResponse {
                    error_code: codes::NOT_COORDINATOR,
                    member_id: M1.to_string(),
                    ..Default::default()
                }
            )]
    );
    assert!(old.list_groups(&ListGroupsRequest::default()) == ListGroupsResponse::default());
    assert!(old.next_deadline().is_none());
    assert!(
        old.offset_fetch(&fetch_all("g"), 8, &topics)
            == OffsetFetchResponse {
                groups: vec![OffsetFetchResponseGroup {
                    group_id: "g".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }
    );

    let mut new = Coordinator::new(2, CoordinatorConfig::default());
    new.load(8000, records);
    assert!(new.offset_fetch(&fetch_all("g"), 8, &topics) == offsets);
    assert!(new.heartbeat(8100, &hb(M2, 1)) == hb_response(codes::NONE));
    assert!(
        new.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("g", "consumer", "Stable", "classic")],
                ..Default::default()
            }
    );
}
