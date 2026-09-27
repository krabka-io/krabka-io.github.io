use std::collections::BTreeMap;

use assert2::assert;
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, LeaderEpoch, MetadataImage, MetadataRecord,
    PartitionRecord, TopicConfigRecord, TopicRecord,
};
use serde_json::json;
use uuid::Uuid;

use super::{
    AlterPartition, AlteredPartition, CONSUMER_OFFSETS_TOPIC, ControllerConfig,
    ControllerDecisions, CreateTopicSpec, Heartbeat, HeartbeatOutcome, IsrMember, NO_LEADER,
    RegisterBroker, TopicError, unfenced_brokers,
};
use crate::lab::{codes, net::NodeId};

use krabka_metadata::NodeId as BrokerId;

const CLUSTER: Uuid = Uuid::from_u128(0xc1);
const SESSION_MS: u64 = 9_000;

/// A `CreatePartitions` row: its label, the topic, the count asked for, the
/// caller's assignments, and the expected error code.
type GrowthCase<'a> = (&'a str, &'a str, i32, Option<Vec<Vec<NodeId>>>, i16);

fn ids(brokers: &[u64]) -> Vec<BrokerId> {
    brokers.iter().copied().map(BrokerId).collect()
}

fn nodes(brokers: &[u32]) -> Vec<NodeId> {
    brokers.iter().copied().map(NodeId).collect()
}

/// A plaintext listener. The protocol type lives in `krabka-security`, which
/// the crate reaches through the record's serde form.
fn endpoint(host: &str) -> BrokerEndpoint {
    serde_json::from_value(json!({
        "name": "PLAINTEXT", "host": host, "port": 9092, "protocol": "Plaintext"
    }))
    .unwrap()
}

fn registration(id: u64, fenced: bool) -> BrokerRegistrationRecord {
    BrokerRegistrationRecord {
        node_id: BrokerId(id),
        broker_epoch: i64::try_from(id).unwrap() * 10,
        incarnation_id: Uuid::from_u128(u128::from(id)),
        host: format!("broker-{id}"),
        port: 9092,
        rack: None,
        endpoints: vec![endpoint(&format!("broker-{id}"))],
        log_dirs: vec![],
        fenced,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        features: BTreeMap::new(),
    }
}

/// An image with `brokers` registered and unfenced, at broker epoch
/// `10 * id`.
fn image_with(brokers: &[u64]) -> MetadataImage {
    let mut image = MetadataImage::new(CLUSTER);
    for &id in brokers {
        image.apply(&MetadataRecord::V1BrokerRegistration(registration(
            id, false,
        )));
    }
    image
}

fn apply_all(image: &mut MetadataImage, records: &[MetadataRecord]) {
    for record in records {
        image.apply(record);
    }
}

fn topic(image: &mut MetadataImage, name: &str, id: u128) {
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: Uuid::from_u128(id),
        partitions: 0,
        replication_factor: 0,
    }));
}

fn partition(
    topic: &str,
    index: i32,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
    leader_epoch: i32,
    partition_epoch: i32,
) -> PartitionRecord {
    PartitionRecord {
        topic: topic.into(),
        partition: index,
        leader: BrokerId(leader),
        replicas: ids(replicas),
        isr: ids(isr),
        leader_epoch: LeaderEpoch(leader_epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch,
    }
}

fn decisions() -> ControllerDecisions {
    ControllerDecisions::new(ControllerConfig::default(), 7)
}

fn heartbeat(broker: u32, epoch: i64) -> Heartbeat {
    Heartbeat {
        broker_id: NodeId(broker),
        broker_epoch: epoch,
        current_metadata_offset: epoch,
        want_fence: false,
        want_shutdown: false,
    }
}

#[test]
fn create_topics_stripes_replicas_over_the_unfenced_brokers() {
    let mut image = image_with(&[1, 2, 3, 4]);
    image.apply(&MetadataRecord::V1BrokerRegistration(registration(4, true)));
    let mut decisions = decisions();
    let results = decisions.create_topics(&image, &[CreateTopicSpec::new("orders", 3, 3)]);
    assert!(let [Ok(created)] = results.as_slice());
    assert!(created.name == "orders");
    assert!(created.partitions == 3);
    assert!(created.replication_factor == 3);
    assert!(!created.topic_id.is_nil());
    let expected_partitions: Vec<MetadataRecord> = [
        (0, &[1, 2, 3][..]),
        (1, &[2, 3, 1][..]),
        (2, &[3, 1, 2][..]),
    ]
    .into_iter()
    .map(|(index, replicas)| {
        MetadataRecord::V1Partition(partition(
            "orders",
            index,
            replicas[0],
            replicas,
            replicas,
            0,
            0,
        ))
    })
    .collect();
    let mut expected = vec![MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: created.topic_id,
        partitions: 3,
        replication_factor: 3,
    })];
    expected.extend(expected_partitions);
    assert!(created.records == expected);
    apply_all(&mut image, &created.records);
    let stored = image.topic("orders").unwrap();
    assert!(stored.partitions == 3);
    assert!(stored.replication_factor == 3);
    assert!(stored.topic_id == created.topic_id);
    assert!(
        image.partition("orders", 1)
            == Some(&partition("orders", 1, 2, &[2, 3, 1], &[2, 3, 1], 0, 0))
    );
    assert!(image.partition("orders", 3).is_none());
    // The next topic starts its stripe one broker further, and a config
    // override rides along as a config record, between the topic record and
    // the partition records as Kafka writes it.
    let mut spec = CreateTopicSpec::new("events", 2, 2);
    spec.configs.insert("retention.ms".into(), "1000".into());
    let results = decisions.create_topics(&image, &[spec]);
    assert!(let [Ok(created)] = results.as_slice());
    assert!(
        created.records[1..]
            == vec![
                MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: "events".into(),
                    overrides: BTreeMap::from([("retention.ms".to_string(), "1000".to_string())]),
                }),
                MetadataRecord::V1Partition(partition("events", 0, 2, &[2, 3], &[2, 3], 0, 0)),
                MetadataRecord::V1Partition(partition("events", 1, 3, &[3, 1], &[3, 1], 0, 0)),
            ]
    );
}

#[test]
fn defaults_and_manual_assignments_are_honoured() {
    let mut image = image_with(&[1, 2, 3]);
    image.apply(&MetadataRecord::V1BrokerRegistration(registration(3, true)));
    let config = ControllerConfig {
        default_partitions: 4,
        default_replication_factor: 2,
        ..ControllerConfig::default()
    };
    let mut decisions = ControllerDecisions::new(config, 1);
    let results = decisions.create_topics(&image, &[CreateTopicSpec::new("defaults", -1, -1)]);
    assert!(let [Ok(created)] = results.as_slice());
    assert!((created.partitions, created.replication_factor) == (4, 2));
    // A manual assignment keeps the listed order, may name a fenced broker,
    // and leaves it out of the ISR; the first active replica leads.
    let spec = CreateTopicSpec {
        assignments: vec![(0, nodes(&[3, 2, 1])), (1, nodes(&[1, 3, 2]))],
        ..CreateTopicSpec::new("manual", -1, -1)
    };
    let results = decisions.create_topics(&image, &[spec]);
    assert!(let [Ok(created)] = results.as_slice());
    assert!(
        created.records[1..]
            == vec![
                MetadataRecord::V1Partition(partition("manual", 0, 2, &[3, 2, 1], &[2, 1], 0, 0)),
                MetadataRecord::V1Partition(partition("manual", 1, 1, &[1, 3, 2], &[1, 2], 0, 0)),
            ]
    );
    // Every replica fenced: the row is refused.
    let spec = CreateTopicSpec {
        assignments: vec![(0, nodes(&[3]))],
        ..CreateTopicSpec::new("dark", -1, -1)
    };
    let results = decisions.create_topics(&image, &[spec]);
    assert!(let [Err(error)] = results.as_slice());
    assert!(error.code == codes::INVALID_REPLICA_ASSIGNMENT);
}

#[test]
fn create_partitions_continues_the_stripe_and_refuses_shrinking() {
    let mut image = image_with(&[1, 2, 3]);
    let mut decisions = decisions();
    let results = decisions.create_topics(&image, &[CreateTopicSpec::new("t", 2, 2)]);
    assert!(let [Ok(created)] = results.as_slice());
    apply_all(&mut image, &created.records);
    let records = decisions.create_partitions(&image, "t", 4, None).unwrap();
    assert!(
        records
            == vec![
                MetadataRecord::V1Partition(partition("t", 2, 3, &[3, 1], &[3, 1], 0, 0)),
                MetadataRecord::V1Partition(partition("t", 3, 1, &[1, 2], &[1, 2], 0, 0)),
            ]
    );
    apply_all(&mut image, &records);
    assert!(image.topic_partition_count("t") == 4);
    let manual = decisions
        .create_partitions(&image, "t", 5, Some(&[nodes(&[2, 3])]))
        .unwrap();
    assert!(
        manual
            == vec![MetadataRecord::V1Partition(partition(
                "t",
                4,
                2,
                &[2, 3],
                &[2, 3],
                0,
                0
            ))]
    );
    let cases: Vec<GrowthCase<'_>> = vec![
        (
            "unknown topic",
            "nope",
            5,
            None,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
        ),
        ("same count", "t", 4, None, codes::INVALID_PARTITIONS),
        ("fewer", "t", 1, None, codes::INVALID_PARTITIONS),
        (
            "wrong assignment count",
            "t",
            6,
            Some(vec![nodes(&[1, 2])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
        ),
        (
            "rf mismatch",
            "t",
            5,
            Some(vec![nodes(&[1])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
        ),
    ];
    for (label, name, count, assignments, code) in cases {
        let result = decisions.create_partitions(&image, name, count, assignments.as_deref());
        assert!(let Err(error) = result, "{label}");
        assert!(error.code == code, "{label}: {:?}", error.message);
    }
}

#[test]
fn delete_topics_answers_per_name() {
    let mut image = image_with(&[1]);
    topic(&mut image, "gone", 3);
    let results = decisions().delete_topics(&image, &["gone".to_string(), "missing".to_string()]);
    assert!(
        results
            == vec![
                Ok(vec![MetadataRecord::V1DeleteTopic(
                    krabka_metadata::DeleteTopicRecord {
                        name: "gone".into()
                    }
                )]),
                Err(TopicError {
                    code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message: Some("This server does not host this topic-partition.".into()),
                }),
            ]
    );
    apply_all(&mut image, results[0].as_ref().unwrap());
    assert!(image.topic("gone").is_none());
}

#[test]
fn fencing_elects_the_next_isr_member_and_bumps_the_epochs() {
    let mut image = image_with(&[1, 2, 3]);
    for name in ["t", "u"] {
        topic(&mut image, name, u128::from(name.as_bytes()[0]));
    }
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "u".into(),
        overrides: BTreeMap::from([(
            "unclean.leader.election.enable".to_string(),
            "true".to_string(),
        )]),
    }));
    apply_all(
        &mut image,
        &[
            // Led by 1 with a clean successor: 2 leads, 1 leaves the ISR.
            MetadataRecord::V1Partition(partition("t", 0, 1, &[1, 2, 3], &[1, 2, 3], 4, 7)),
            // Led by 2 with 1 in the ISR: the ISR shrinks, the leader stays.
            MetadataRecord::V1Partition(partition("t", 1, 2, &[2, 1, 3], &[2, 1], 1, 1)),
            // Led by 1 alone in the ISR, unclean election off: offline, with
            // 1 kept as the last ISR member.
            MetadataRecord::V1Partition(partition("t", 2, 1, &[1, 2, 3], &[1], 0, 0)),
            // Not involving broker 1 at all: untouched.
            MetadataRecord::V1Partition(partition("t", 3, 3, &[3, 2], &[3, 2], 0, 0)),
            // Led by 1 alone in the ISR, unclean election on: 3 leads alone,
            // as the first active replica in assignment order.
            MetadataRecord::V1Partition(partition("u", 0, 1, &[1, 3, 2], &[1], 2, 2)),
        ],
    );
    let decisions = decisions();
    let records = decisions.elect_leaders_after_fence(&image, NodeId(1));
    assert!(
        records
            == vec![
                MetadataRecord::V1Partition(partition("t", 0, 2, &[1, 2, 3], &[2, 3], 5, 8)),
                MetadataRecord::V1Partition(partition("t", 1, 2, &[2, 1, 3], &[2], 1, 2)),
                MetadataRecord::V1Partition(partition("t", 2, NO_LEADER.0, &[1, 2, 3], &[1], 1, 1)),
                MetadataRecord::V1Partition(partition("u", 0, 3, &[1, 3, 2], &[3], 3, 3)),
            ]
    );
    apply_all(&mut image, &records);
    assert!(image.partition("t", 0).unwrap().leader == BrokerId(2));
    assert!(image.partition("t", 2).unwrap().leader == NO_LEADER);
    assert!(image.partition("t", 3) == Some(&partition("t", 3, 3, &[3, 2], &[3, 2], 0, 0)));
    // A broker nobody depends on changes nothing.
    assert!(
        decisions
            .elect_leaders_after_fence(&image, NodeId(9))
            .is_empty()
    );
}

#[test]
fn registration_follows_the_cluster_control_manager() {
    let mut image = image_with(&[1, 2]);
    topic(&mut image, "t", 1);
    image.apply(&MetadataRecord::V1Partition(partition(
        "t",
        0,
        1,
        &[1, 2],
        &[1, 2],
        0,
        0,
    )));
    let mut decisions = decisions();
    decisions.activate(&image, 1_000);
    let request = |broker: u32, incarnation: u128, cluster: Uuid| RegisterBroker {
        broker_id: NodeId(broker),
        incarnation_id: Uuid::from_u128(incarnation),
        cluster_id: cluster,
        rack: Some("a".into()),
        endpoints: vec![endpoint("new-host")],
    };
    assert!(
        decisions.register_broker(&image, &request(3, 33, Uuid::from_u128(9)), 1_000, 40)
            == Err(codes::INCONSISTENT_CLUSTER_ID)
    );
    let no_listener = RegisterBroker {
        endpoints: vec![],
        ..request(3, 33, CLUSTER)
    };
    assert!(
        decisions.register_broker(&image, &no_listener, 1_000, 40)
            == Err(codes::INVALID_REGISTRATION)
    );
    // A new incarnation of broker 1 while its session is live is a duplicate;
    // once the session lapsed it registers fenced at the log offset, and its
    // leadership moves.
    assert!(
        decisions.register_broker(&image, &request(1, 11, CLUSTER), 2_000, 40)
            == Err(codes::DUPLICATE_BROKER_REGISTRATION)
    );
    let records = decisions
        .register_broker(&image, &request(1, 11, CLUSTER), 1_000 + SESSION_MS, 40)
        .unwrap();
    assert!(
        records
            == vec![
                MetadataRecord::V1Partition(partition("t", 0, 2, &[1, 2], &[2], 1, 1)),
                MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                    broker_epoch: 40,
                    incarnation_id: Uuid::from_u128(11),
                    host: "new-host".into(),
                    rack: Some("a".into()),
                    endpoints: vec![endpoint("new-host")],
                    fenced: true,
                    ..registration(1, true)
                }),
            ]
    );
    apply_all(&mut image, &records);
    assert!(image.broker_epoch(BrokerId(1)) == Some(40));
    assert!(decisions.last_heartbeat(NodeId(1)) == Some(1_000 + SESSION_MS));
    // The same incarnation registering again keeps its epoch and its fence.
    let again = decisions
        .register_broker(&image, &request(1, 11, CLUSTER), 20_000, 99)
        .unwrap();
    assert!(let [MetadataRecord::V1BrokerRegistration(record)] = again.as_slice());
    assert!(record.broker_epoch == 40);
    assert!(record.fenced);
    // A brand new broker registers fenced with no partition changes.
    let fresh = decisions
        .register_broker(&image, &request(3, 33, CLUSTER), 20_000, 41)
        .unwrap();
    assert!(let [MetadataRecord::V1BrokerRegistration(record)] = fresh.as_slice());
    assert!((record.node_id, record.broker_epoch, record.fenced) == (BrokerId(3), 41, true));
}

#[test]
fn heartbeats_drive_the_fence_state_machine() {
    let mut image = image_with(&[1, 2]);
    image.apply(&MetadataRecord::V1BrokerRegistration(registration(1, true)));
    topic(&mut image, "t", 1);
    image.apply(&MetadataRecord::V1Partition(partition(
        "t",
        0,
        NO_LEADER.0,
        &[1, 2],
        &[1],
        1,
        1,
    )));
    image.apply(&MetadataRecord::V1Partition(partition(
        "t",
        1,
        2,
        &[2, 1],
        &[2, 1],
        0,
        0,
    )));
    let mut decisions = decisions();
    decisions.activate(&image, 0);
    assert!(
        decisions.broker_heartbeat(&image, &heartbeat(1, 11), 100)
            == Err(codes::STALE_BROKER_EPOCH)
    );
    assert!(
        decisions.broker_heartbeat(&image, &heartbeat(9, 90), 100)
            == Err(codes::STALE_BROKER_EPOCH)
    );
    // Not caught up yet: still fenced.
    let behind = Heartbeat {
        current_metadata_offset: 3,
        ..heartbeat(1, 10)
    };
    let outcome = decisions.broker_heartbeat(&image, &behind, 100).unwrap();
    assert!(
        outcome
            == HeartbeatOutcome {
                records: vec![],
                is_caught_up: false,
                is_fenced: true,
                should_shut_down: false,
            }
    );
    // Caught up: unfenced, and the offline partition it can lead elects it.
    let outcome = decisions
        .broker_heartbeat(&image, &heartbeat(1, 10), 200)
        .unwrap();
    assert!(
        outcome
            == HeartbeatOutcome {
                records: vec![
                    MetadataRecord::V1Partition(partition("t", 0, 1, &[1, 2], &[1], 2, 2)),
                    MetadataRecord::V1BrokerRegistration(registration(1, false)),
                ],
                is_caught_up: true,
                is_fenced: false,
                should_shut_down: false,
            }
    );
    apply_all(&mut image, &outcome.records);
    assert!(!image.broker(BrokerId(1)).unwrap().fenced);
    assert!(decisions.last_heartbeat(NodeId(1)) == Some(200));
    // Nothing to do while it keeps heartbeating.
    let outcome = decisions
        .broker_heartbeat(&image, &heartbeat(1, 10), 300)
        .unwrap();
    assert!(outcome.records.is_empty());
    let outcome = decisions
        .broker_heartbeat(&image, &heartbeat(2, 20), 1_000)
        .unwrap();
    assert!(outcome.records.is_empty());
    // A session that lapses fences the broker, and its partition goes
    // offline: no other ISR member is left, and the broker stays in the ISR
    // as its last member. Broker 2 heartbeated later and keeps its session.
    assert!(
        decisions
            .expire_sessions(&image, 300 + SESSION_MS - 1)
            .is_empty()
    );
    let records = decisions.expire_sessions(&image, 300 + SESSION_MS);
    assert!(
        records
            == vec![
                MetadataRecord::V1Partition(partition("t", 0, NO_LEADER.0, &[1, 2], &[1], 3, 3)),
                MetadataRecord::V1Partition(partition("t", 1, 2, &[2, 1], &[2], 0, 1)),
                MetadataRecord::V1BrokerRegistration(registration(1, true)),
            ]
    );
    // Asking to be fenced fences at once.
    let want_fence = Heartbeat {
        want_fence: true,
        ..heartbeat(2, 20)
    };
    let outcome = decisions
        .broker_heartbeat(&image, &want_fence, 400)
        .unwrap();
    assert!(outcome.is_fenced);
    assert!(!outcome.should_shut_down);
    assert!(let [MetadataRecord::V1Partition(_), MetadataRecord::V1BrokerRegistration(record)] = outcome.records.as_slice());
    assert!(record.fenced);
}

#[test]
fn controlled_shutdown_hands_leaderships_over_before_the_broker_may_stop() {
    let mut image = image_with(&[1, 2]);
    topic(&mut image, "t", 1);
    image.apply(&MetadataRecord::V1Partition(partition(
        "t",
        0,
        1,
        &[1, 2],
        &[1, 2],
        0,
        0,
    )));
    let mut decisions = decisions();
    decisions.activate(&image, 0);
    let want_shutdown = Heartbeat {
        want_shutdown: true,
        ..heartbeat(1, 10)
    };
    let outcome = decisions
        .broker_heartbeat(&image, &want_shutdown, 100)
        .unwrap();
    assert!(
        outcome
            == HeartbeatOutcome {
                records: vec![
                    MetadataRecord::V1Partition(partition("t", 0, 2, &[1, 2], &[1, 2], 1, 1)),
                    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                        in_controlled_shutdown: true,
                        ..registration(1, false)
                    }),
                ],
                is_caught_up: true,
                is_fenced: false,
                should_shut_down: false,
            }
    );
    apply_all(&mut image, &outcome.records);
    assert!(image.broker(BrokerId(1)).unwrap().in_controlled_shutdown);
    assert!(image.partition("t", 0).unwrap().leader == BrokerId(2));
    // Leading nothing now: it may stop, and it is fenced on the way out.
    let outcome = decisions
        .broker_heartbeat(&image, &want_shutdown, 200)
        .unwrap();
    assert!(outcome.should_shut_down);
    assert!(outcome.is_fenced);
    assert!(
        outcome.records
            == vec![
                MetadataRecord::V1Partition(partition("t", 0, 2, &[1, 2], &[2], 1, 2)),
                MetadataRecord::V1BrokerRegistration(registration(1, true)),
            ]
    );
    // An unfenced broker that leads nothing may stop at once.
    let mut idle = image_with(&[1, 2]);
    idle.apply(&MetadataRecord::V1BrokerRegistration(registration(
        1, false,
    )));
    let outcome = decisions
        .broker_heartbeat(&idle, &want_shutdown, 300)
        .unwrap();
    assert!(outcome.should_shut_down);
    assert!(outcome.records == vec![MetadataRecord::V1BrokerRegistration(registration(1, true))]);
    // A leadership no other replica can take does not hold the broker: it
    // may stop, it is fenced, and the partition goes offline.
    let mut sole = image_with(&[1, 2]);
    topic(&mut sole, "t", 1);
    sole.apply(&MetadataRecord::V1Partition(partition(
        "t",
        0,
        1,
        &[1, 2],
        &[1],
        0,
        0,
    )));
    let outcome = decisions
        .broker_heartbeat(&sole, &want_shutdown, 400)
        .unwrap();
    assert!(
        outcome
            == HeartbeatOutcome {
                records: vec![
                    MetadataRecord::V1Partition(partition(
                        "t",
                        0,
                        NO_LEADER.0,
                        &[1, 2],
                        &[1],
                        1,
                        1
                    )),
                    MetadataRecord::V1BrokerRegistration(registration(1, true)),
                ],
                is_caught_up: true,
                is_fenced: true,
                should_shut_down: true,
            }
    );
}

#[test]
fn unregistering_drops_the_broker_after_its_partitions_move() {
    let mut image = image_with(&[1, 2]);
    topic(&mut image, "t", 1);
    image.apply(&MetadataRecord::V1Partition(partition(
        "t",
        0,
        1,
        &[1, 2],
        &[1, 2],
        0,
        0,
    )));
    let mut decisions = decisions();
    assert!(decisions.unregister_broker(&image, NodeId(5)) == Err(codes::BROKER_ID_NOT_REGISTERED));
    let records = decisions.unregister_broker(&image, NodeId(1)).unwrap();
    assert!(
        records
            == vec![
                MetadataRecord::V1Partition(partition("t", 0, 2, &[1, 2], &[2], 1, 1)),
                MetadataRecord::V1UnregisterBroker(krabka_metadata::UnregisterBrokerRecord {
                    node_id: BrokerId(1)
                }),
            ]
    );
    apply_all(&mut image, &records);
    assert!(image.broker(BrokerId(1)).is_none());
    assert!(unfenced_brokers(&image) == nodes(&[2]));
}

#[test]
fn the_offsets_topic_is_compacted_and_replicated_as_far_as_the_brokers_allow() {
    let decisions = decisions();
    let two = decisions.consumer_offsets_spec(&image_with(&[1, 2]));
    assert!(two.name == CONSUMER_OFFSETS_TOPIC);
    assert!((two.partitions, two.replication_factor) == (50, 2));
    assert!(two.configs.get("cleanup.policy").map(String::as_str) == Some("compact"));
    let five = decisions.consumer_offsets_spec(&image_with(&[1, 2, 3, 4, 5]));
    assert!(five.replication_factor == 3);
    let none = decisions.consumer_offsets_spec(&MetadataImage::new(CLUSTER));
    assert!(none.replication_factor == 1);
}

#[test]
fn topic_ids_are_deterministic_per_seed_and_never_reserved() {
    let image = image_with(&[1]);
    let mut first = ControllerDecisions::new(ControllerConfig::default(), 42);
    let mut same = ControllerDecisions::new(ControllerConfig::default(), 42);
    let mut other = ControllerDecisions::new(ControllerConfig::default(), 43);
    let id = |decisions: &mut ControllerDecisions| {
        let results = decisions.create_topics(&image, &[CreateTopicSpec::new("t", 1, 1)]);
        assert!(let [Ok(created)] = results.as_slice());
        created.topic_id
    };
    let (a, b, c) = (id(&mut first), id(&mut same), id(&mut other));
    assert!(a == b);
    assert!(a != c);
    assert!(!a.is_nil());
    assert!(a != Uuid::from_u128(1));
    assert!((a.as_u128() >> 122) != 62);
    assert!(id(&mut first) != a);
}

#[test]
fn a_controller_that_takes_over_never_reuses_a_topic_id() {
    let mut image = image_with(&[1]);
    let mut first = ControllerDecisions::new(ControllerConfig::default(), 42);
    first.activate(&image, 0);
    let mut taken = Vec::new();
    for name in ["a", "b", "c"] {
        let results = first.create_topics(&image, &[CreateTopicSpec::new(name, 1, 1)]);
        assert!(let [Ok(created)] = results.as_slice());
        for record in &created.records {
            image.apply(record);
        }
        taken.push(created.topic_id);
    }
    // The successor starts from the same seed. Activated later, it draws a
    // different sequence; not activated, it replays the predecessor's
    // sequence and must step past every id the image already holds.
    for activation in [Some(5_000), None] {
        let mut successor = ControllerDecisions::new(ControllerConfig::default(), 42);
        if let Some(now) = activation {
            successor.activate(&image, now);
        }
        let results = successor.create_topics(&image, &[CreateTopicSpec::new("d", 1, 1)]);
        assert!(let [Ok(created)] = results.as_slice());
        assert!(!taken.contains(&created.topic_id), "{activation:?}");
    }
}

/// The refusal of one topic spec against `image`.
fn refusal(image: &MetadataImage, spec: CreateTopicSpec) -> TopicError {
    let results = decisions().create_topics(image, &[spec]);
    assert!(let [Err(error)] = results.as_slice());
    error.clone()
}

#[test]
fn create_topics_refuses_bad_counts_and_names() {
    let mut image = image_with(&[1, 2, 3]);
    topic(&mut image, "taken", 5);
    topic(&mut image, "a_b", 6);
    let cases = [
        (
            "rf above the brokers",
            CreateTopicSpec::new("t", 1, 4),
            codes::INVALID_REPLICATION_FACTOR,
        ),
        (
            "rf zero",
            CreateTopicSpec::new("t", 1, 0),
            codes::INVALID_REPLICATION_FACTOR,
        ),
        (
            "rf below -1",
            CreateTopicSpec::new("t", 1, -2),
            codes::INVALID_REPLICATION_FACTOR,
        ),
        (
            "no partitions",
            CreateTopicSpec::new("t", 0, 1),
            codes::INVALID_PARTITIONS,
        ),
        (
            "existing",
            CreateTopicSpec::new("taken", 1, 1),
            codes::TOPIC_ALREADY_EXISTS,
        ),
        (
            "empty name",
            CreateTopicSpec::new("", 1, 1),
            codes::INVALID_TOPIC_EXCEPTION,
        ),
        (
            "dot",
            CreateTopicSpec::new(".", 1, 1),
            codes::INVALID_TOPIC_EXCEPTION,
        ),
        (
            "bad character",
            CreateTopicSpec::new("a/b", 1, 1),
            codes::INVALID_TOPIC_EXCEPTION,
        ),
        (
            "too long",
            CreateTopicSpec::new(&"x".repeat(250), 1, 1),
            codes::INVALID_TOPIC_EXCEPTION,
        ),
        (
            "collides on dot and underscore",
            CreateTopicSpec::new("a.b", 1, 1),
            codes::INVALID_TOPIC_EXCEPTION,
        ),
    ];
    for (label, spec, code) in cases {
        let error = refusal(&image, spec);
        assert!(error.code == code, "{label}: {:?}", error.message);
        assert!(error.message.is_some_and(|m| !m.is_empty()), "{label}");
    }
    // With every broker fenced nothing can be placed.
    let mut fenced = image_with(&[1]);
    fenced.apply(&MetadataRecord::V1BrokerRegistration(registration(1, true)));
    assert!(
        refusal(&fenced, CreateTopicSpec::new("t", 1, 1)).code == codes::INVALID_REPLICATION_FACTOR
    );
}

#[test]
fn create_topics_refuses_manual_assignments_kafka_refuses() {
    let image = image_with(&[1, 2, 3]);
    let manual = |assignments: Vec<Vec<NodeId>>, partitions: i32, rf: i16| CreateTopicSpec {
        assignments: (0..).zip(assignments).collect(),
        ..CreateTopicSpec::new("t", partitions, rf)
    };
    let cases = [
        (
            "with an rf",
            manual(vec![nodes(&[1])], -1, 1),
            codes::INVALID_REQUEST,
        ),
        (
            "with a count",
            manual(vec![nodes(&[1])], 1, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "naming an unregistered broker",
            manual(vec![nodes(&[1, 9])], -1, -1),
            codes::INVALID_REPLICA_ASSIGNMENT,
        ),
        (
            "naming a broker twice",
            manual(vec![nodes(&[1, 1])], -1, -1),
            codes::INVALID_REPLICA_ASSIGNMENT,
        ),
        (
            "with uneven partitions",
            manual(vec![nodes(&[1, 2]), nodes(&[3])], -1, -1),
            codes::INVALID_REPLICA_ASSIGNMENT,
        ),
    ];
    for (label, spec, code) in cases {
        let error = refusal(&image, spec);
        assert!(error.code == code, "{label}: {:?}", error.message);
    }
}

#[test]
fn duplicate_topic_names_are_refused_on_every_row() {
    let image = image_with(&[1, 2, 3]);
    // A name that appears twice is refused on both rows, and the other rows
    // are unaffected.
    let results = decisions().create_topics(
        &image,
        &[
            CreateTopicSpec::new("dup", 1, 1),
            CreateTopicSpec::new("fine", 1, 1),
            CreateTopicSpec::new("dup", 1, 1),
        ],
    );
    let codes: Vec<Result<&str, i16>> = results
        .iter()
        .map(|r| r.as_ref().map(|c| c.name.as_str()).map_err(|e| e.code))
        .collect();
    assert!(
        codes
            == vec![
                Err(codes::INVALID_REQUEST),
                Ok("fine"),
                Err(codes::INVALID_REQUEST)
            ]
    );
    // Two rows with one name are both duplicates, whatever their order.
    let results = decisions().create_topics(
        &image,
        &[
            CreateTopicSpec::new("twice", 1, 1),
            CreateTopicSpec::new("twice", 1, 1),
        ],
    );
    assert!(
        results
            .iter()
            .all(|r| r.as_ref().is_err_and(|e| e.code == codes::INVALID_REQUEST))
    );
}

/// Broker 1 leads partition 0 of `t` at leader epoch 5 and partition epoch
/// 10, with replicas [1, 2, 3] and ISR [1, 2]; broker 3 is fenced.
fn alter_fixture() -> MetadataImage {
    let mut image = image_with(&[1, 2, 3]);
    image.apply(&MetadataRecord::V1BrokerRegistration(registration(3, true)));
    topic(&mut image, "t", 1);
    image.apply(&MetadataRecord::V1Partition(partition(
        "t",
        0,
        1,
        &[1, 2, 3],
        &[1, 2],
        5,
        10,
    )));
    image
}

fn member(broker: u32, epoch: i64) -> IsrMember {
    IsrMember {
        broker: NodeId(broker),
        broker_epoch: epoch,
    }
}

fn alter_row(
    requester: u32,
    leader_epoch: i32,
    partition_epoch: i32,
    isr: Vec<IsrMember>,
) -> AlterPartition {
    AlterPartition {
        broker_id: NodeId(requester),
        topic: "t".into(),
        partition: 0,
        leader_epoch,
        partition_epoch,
        new_isr: isr,
    }
}

#[test]
fn alter_partition_rows_are_validated_in_kafkas_order() {
    let image = alter_fixture();
    let cases = [
        (
            "a leader epoch the controller has not seen",
            alter_row(1, 6, 10, vec![member(1, -1)]),
            codes::NOT_CONTROLLER,
        ),
        (
            "a partition epoch the controller has not seen",
            alter_row(1, 4, 11, vec![member(1, -1)]),
            codes::NOT_CONTROLLER,
        ),
        (
            "an older leader epoch",
            alter_row(2, 4, 9, vec![member(1, -1)]),
            codes::FENCED_LEADER_EPOCH,
        ),
        (
            "a requester that is not the leader",
            alter_row(2, 5, 9, vec![member(1, -1)]),
            codes::INVALID_REQUEST,
        ),
        (
            "an older partition epoch",
            alter_row(1, 5, 9, vec![member(1, -1)]),
            codes::INVALID_UPDATE_VERSION,
        ),
        (
            "an ISR without the leader",
            alter_row(1, 5, 10, vec![member(2, -1)]),
            codes::INVALID_REQUEST,
        ),
        (
            "an ISR naming a replica twice",
            alter_row(1, 5, 10, vec![member(1, -1), member(2, -1), member(2, -1)]),
            codes::INVALID_REQUEST,
        ),
        (
            "an ISR with a broker that is not a replica",
            alter_row(1, 5, 10, vec![member(1, -1), member(4, -1)]),
            codes::INVALID_REQUEST,
        ),
        (
            "a fenced member",
            alter_row(1, 5, 10, vec![member(1, -1), member(3, -1)]),
            codes::INELIGIBLE_REPLICA,
        ),
        (
            "a member at a stale broker epoch",
            alter_row(1, 5, 10, vec![member(1, 10), member(2, 19)]),
            codes::INELIGIBLE_REPLICA,
        ),
    ];
    let decisions = decisions();
    for (label, request, code) in cases {
        assert!(
            decisions.alter_partition(&image, &request) == Err(code),
            "{label}"
        );
    }
    let unknown = AlterPartition {
        partition: 7,
        ..alter_row(1, 5, 10, vec![member(1, -1)])
    };
    assert!(decisions.alter_partition(&image, &unknown) == Err(codes::UNKNOWN_TOPIC_OR_PARTITION));
}

#[test]
fn alter_partition_admits_the_leaders_proposal_and_bumps_the_partition_epoch() {
    let mut image = alter_fixture();
    let decisions = decisions();
    // The ISR the partition has: admitted, and nothing is written, as
    // Kafka's `PartitionChangeBuilder` builds no record for it.
    let unchanged = decisions
        .alter_partition(
            &image,
            &alter_row(1, 5, 10, vec![member(1, 10), member(2, -1)]),
        )
        .unwrap();
    assert!(
        unchanged
            == AlteredPartition {
                leader: NodeId(1),
                leader_epoch: 5,
                isr: nodes(&[1, 2]),
                partition_epoch: 10,
                records: Vec::new(),
            }
    );
    // Current epochs, the leader asking, a smaller ISR of eligible members
    // with their real epochs: admitted with the partition epoch bumped.
    let admitted = decisions
        .alter_partition(&image, &alter_row(1, 5, 10, vec![member(1, 10)]))
        .unwrap();
    assert!(
        admitted
            == AlteredPartition {
                leader: NodeId(1),
                leader_epoch: 5,
                isr: nodes(&[1]),
                partition_epoch: 11,
                records: vec![MetadataRecord::V1Partition(partition(
                    "t",
                    0,
                    1,
                    &[1, 2, 3],
                    &[1],
                    5,
                    11
                ))],
            }
    );
    apply_all(&mut image, &admitted.records);
    // The stale expand built at partition epoch 10 loses to the newer ISR.
    assert!(
        decisions.alter_partition(
            &image,
            &alter_row(1, 5, 10, vec![member(1, -1), member(2, -1)])
        ) == Err(codes::INVALID_UPDATE_VERSION)
    );
    assert!(image.partition("t", 0).unwrap().partition_epoch == 11);
}
