//! The simulated broker over the wire: a scripted client injects Kafka
//! frames into a world that hosts only the brokers and reads the replies
//! back from the egress queue.

use std::collections::BTreeMap;

use assert2::assert;
use bytes::{BufMut as _, Bytes, BytesMut};
use krabka_metadata::{MetadataRecord, TopicConfigRecord, TopicRecord};
use krabka_playground::lab::{
    Endpoint, Fault, NodeId, World,
    broker::{
        LAB_CLUSTER_ID, api_versions_table, cluster_id_string, partition_record,
        registration_record, supported_features,
        test_support::{TestClient, batch, decode_response, encode_batch, idempotent_batch},
    },
    codes,
    net::{DurableImage, Frame, Payload},
    scenario::Scenario,
};
use krabka_protocol::{
    Encode as _, ProtocolRequest,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        delete_topics_response::DeletableTopicResult,
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_topic_partitions_request::{Cursor, DescribeTopicPartitionsRequest, TopicRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
        join_group_request::JoinGroupRequest,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        offset_for_leader_epoch_request::{
            OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
        },
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{
            BatchIndexAndErrorMessage, LeaderIdAndEpoch, NodeEndpoint, PartitionProduceResponse,
            ProduceResponse,
        },
        request_header::RequestHeader,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};
use serde_json::{Value, json};
use uuid::Uuid;

const BROKER: NodeId = NodeId(1);
const OTHER: NodeId = NodeId(2);
const CLIENT: u32 = 99;

fn scenario(brokers: &[(u32, Value)]) -> Scenario {
    let nodes: Vec<Value> = brokers
        .iter()
        .map(|(id, config)| json!({ "id": id, "kind": "broker", "config": config }))
        .collect();
    serde_json::from_value(json!({
        "version": 1, "seed": 7, "links": { "default_latency_ms": 5 }, "nodes": nodes,
    }))
    .unwrap()
}

/// A world that hosts its brokers, and a scripted client outside it.
struct Lab {
    world: World,
    client: TestClient,
}

impl Lab {
    fn one_broker() -> Self {
        Self::from_scenario(&scenario(&[(1, json!({ "broker_id": 1 }))]), &[BROKER])
    }

    fn from_scenario(scenario: &Scenario, hosted: &[NodeId]) -> Self {
        Self::connect(World::from_scenario_hosted(scenario, hosted).unwrap())
    }

    fn from_scenario_with_state(
        scenario: &Scenario,
        hosted: &[NodeId],
        images: BTreeMap<NodeId, DurableImage>,
    ) -> Self {
        Self::connect(World::from_scenario_with_state(scenario, hosted, images).unwrap())
    }

    fn connect(world: World) -> Self {
        let mut lab = Self {
            world,
            client: TestClient::new(CLIENT, 1),
        };
        lab.world.push_ingress(vec![lab.client.open(BROKER)]);
        lab.run(10);
        lab.drain();
        lab
    }

    fn run(&mut self, ms: u64) {
        let until = self.world.now() + ms;
        self.world.step_until(until);
    }

    /// Every frame the brokers sent out of the world since the last drain.
    fn drain(&mut self) -> Vec<Frame> {
        self.world
            .drain_egress()
            .into_iter()
            .map(|t| t.frame)
            .collect()
    }

    /// The broker's replies to the main client, oldest first; replies to
    /// other clients are dropped.
    fn replies(&mut self) -> Vec<Frame> {
        let client = self.client.clone();
        replies_to(&self.drain(), &client)
    }

    /// Send a request on the main client and take the one reply.
    fn call<R: ProtocolRequest>(&mut self, version: i16, request: &R) -> R::Response {
        let frame = self.client.request(BROKER, version, request);
        self.world.push_ingress(vec![frame]);
        self.run(20);
        let replies = self.replies();
        assert!(replies.len() == 1, "expected one reply, got {replies:?}");
        let (correlation, response) = decode_response::<R>(&replies[0], version).unwrap();
        assert!(correlation == self.client.last_correlation());
        response
    }

    fn create_topic(&mut self, name: &str, partitions: i32, configs: &[(&str, &str)]) -> WireUuid {
        let response = self.call(
            7,
            &CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: name.to_string(),
                    num_partitions: partitions,
                    replication_factor: -1,
                    configs: configs
                        .iter()
                        .map(|(k, v)| CreatableTopicConfig {
                            name: (*k).to_string(),
                            value: Some((*v).to_string()),
                            ..CreatableTopicConfig::default()
                        })
                        .collect(),
                    ..CreatableTopic::default()
                }],
                ..CreateTopicsRequest::default()
            },
        );
        assert!(
            response.topics[0].error_code == codes::NONE,
            "{:?}",
            response.topics[0]
        );
        response.topics[0].topic_id
    }

    fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        acks: i16,
        batch: &RecordBatch,
    ) -> ProduceResponse {
        self.call(11, &produce_request(topic, partition, acks, 1_000, batch))
    }

    fn metadata(&mut self, topic: &str) -> MetadataResponse {
        self.call(
            12,
            &MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some(topic.to_string()),
                    ..MetadataRequestTopic::default()
                }]),
                allow_auto_topic_creation: false,
                ..MetadataRequest::default()
            },
        )
    }

    fn snapshot(&self, node: NodeId) -> Value {
        self.world.node_snapshot(node).unwrap()
    }

    fn partition_snapshot(&self, node: NodeId, topic: &str, index: i64) -> Value {
        self.snapshot(node)["topics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == topic)
            .unwrap()["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["index"] == index)
            .cloned()
            .unwrap()
    }

    fn events(&self, kind: &str) -> Vec<Value> {
        self.world
            .events()
            .filter(|e| e.kind == kind)
            .map(|e| e.detail.clone())
            .collect()
    }
}

/// The frames of `frames` addressed to `client`'s connection.
fn replies_to(frames: &[Frame], client: &TestClient) -> Vec<Frame> {
    frames
        .iter()
        .filter(|f| f.dst == client.endpoint() && f.conn == client.conn)
        .cloned()
        .collect()
}

fn produce_request(
    topic: &str,
    partition: i32,
    acks: i16,
    timeout_ms: i32,
    batch: &RecordBatch,
) -> ProduceRequest {
    ProduceRequest {
        transactional_id: None,
        acks,
        timeout_ms,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(RecordsPayload::Raw(encode_batch(batch))),
                ..PartitionProduceData::default()
            }],
            ..TopicProduceData::default()
        }],
        ..ProduceRequest::default()
    }
}

/// A request frame with an empty body at any version, header version
/// `header_version`, for versions the codec refuses to encode.
fn raw_request(client: &TestClient, api_key: i16, version: i16, header_version: i16) -> Frame {
    let header = RequestHeader {
        request_api_key: api_key,
        request_api_version: version,
        correlation_id: 4_242,
        client_id: Some("raw".into()),
        ..RequestHeader::default()
    };
    let mut frame = BytesMut::new();
    frame.put_i32(i32::try_from(header.encoded_len(header_version)).unwrap());
    header.encode(&mut frame, header_version).unwrap();
    Frame::data(
        client.endpoint(),
        Endpoint::kafka(BROKER),
        client.conn,
        frame.freeze(),
    )
}

fn fetch_request(
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
    max_wait_ms: i32,
) -> FetchRequest {
    FetchRequest {
        max_wait_ms,
        min_bytes: 1,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            topic_id,
            partitions: vec![FetchPartition {
                partition,
                fetch_offset: offset,
                partition_max_bytes: 1 << 20,
                ..FetchPartition::default()
            }],
            ..FetchTopic::default()
        }],
        ..FetchRequest::default()
    }
}

fn fetched_batches(response: &FetchResponse) -> Vec<RecordBatch> {
    match &response.responses[0].partitions[0].records {
        Some(RecordsPayload::V2(batches)) => batches.clone(),
        Some(RecordsPayload::Raw(bytes) | RecordsPayload::Legacy(bytes)) if bytes.is_empty() => {
            Vec::new()
        }
        other => panic!("unexpected records {other:?}"),
    }
}

/// The batch as the log stores it: offsets and epoch assigned.
fn stored(batch: &RecordBatch, base_offset: i64, epoch: i32) -> RecordBatch {
    RecordBatch {
        base_offset,
        partition_leader_epoch: epoch,
        ..batch.clone()
    }
}

const TOPIC_ID: Uuid = Uuid::from_u128(0xABCD);

/// Both brokers' registrations, and `topic`/0 on `replicas` with `isr`, the
/// first ISR member leading at `epoch`: one metadata batch every broker
/// applies.
fn assignment(topic: &str, replicas: &[i32], isr: &[i32], epoch: i32) -> Vec<MetadataRecord> {
    vec![
        MetadataRecord::V1BrokerRegistration(registration_record(1, None, Uuid::from_u128(1))),
        MetadataRecord::V1BrokerRegistration(registration_record(2, None, Uuid::from_u128(2))),
        MetadataRecord::V1Topic(TopicRecord {
            name: topic.to_string(),
            topic_id: TOPIC_ID,
            partitions: 1,
            replication_factor: 2,
        }),
        partition_record(topic, 0, replicas, isr, epoch),
    ]
}

fn apply_metadata(lab: &mut Lab, node: NodeId, records: &[MetadataRecord]) {
    lab.world
        .control(node, json!({ "cmd": "apply_metadata", "records": records }))
        .unwrap();
}

/// Two brokers that replicate `t`/0, led by broker 1, with a two-second
/// replica lag bound.
fn two_brokers() -> Lab {
    let scenario = scenario(&[
        (
            1,
            json!({ "broker_id": 1, "replica_lag_time_max_ms": 2_000 }),
        ),
        (
            2,
            json!({ "broker_id": 2, "replica_lag_time_max_ms": 2_000 }),
        ),
    ]);
    let mut lab = Lab::from_scenario(&scenario, &[BROKER, OTHER]);
    let records = assignment("t", &[1, 2], &[1, 2], 0);
    apply_metadata(&mut lab, BROKER, &records);
    apply_metadata(&mut lab, OTHER, &records);
    lab.run(100);
    lab
}

#[test]
fn api_versions_negotiates_and_answers_unsupported_versions_like_kafka() {
    let mut lab = Lab::one_broker();
    let response = lab.call(
        4,
        &ApiVersionsRequest {
            client_software_name: "krabka-test".into(),
            client_software_version: "1.0".into(),
            ..ApiVersionsRequest::default()
        },
    );
    let expected = ApiVersionsResponse {
        api_keys: api_versions_table(),
        supported_features: supported_features(4),
        finalized_features_epoch: -1,
        ..ApiVersionsResponse::default()
    };
    assert!(response == expected);
    let fetch = response.api_keys.iter().find(|a| a.api_key == 1).unwrap();
    assert!((fetch.min_version, fetch.max_version) == (4, 18));

    // An unsupported ApiVersions version is answered at v0 with the table.
    let frame = raw_request(&lab.client, ApiVersionsRequest::API_KEY, 99, 2);
    lab.world.push_ingress(vec![frame]);
    lab.run(20);
    let replies = lab.replies();
    let (_, v0) = decode_response::<ApiVersionsRequest>(&replies[0], 0).unwrap();
    assert!(
        v0 == ApiVersionsResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            api_keys: api_versions_table(),
            ..ApiVersionsResponse::default()
        }
    );

    // Any other api at an unsupported version closes the connection.
    let frame = raw_request(&lab.client, MetadataRequest::API_KEY, 99, 2);
    lab.world.push_ingress(vec![frame]);
    lab.run(20);
    let replies = lab.replies();
    assert!(replies.len() == 1 && replies[0].payload == Payload::Close);
    assert!(lab.snapshot(BROKER)["connections"] == 0);
}

#[test]
fn malformed_and_unknown_frames_close_the_connection() {
    let mut lab = Lab::one_broker();
    let bad = Frame::data(
        lab.client.endpoint(),
        Endpoint::kafka(BROKER),
        lab.client.conn,
        Bytes::from_static(&[0, 0, 0, 50, 0, 3, 0, 9, 0, 0, 0, 1]),
    );
    lab.world.push_ingress(vec![bad]);
    lab.run(20);
    let replies = lab.replies();
    assert!(replies.len() == 1 && replies[0].payload == Payload::Close);
    let events = lab.events("connection_closed_malformed");
    assert!(events.len() == 1 && events[0]["level"] == "error");

    let mut other = TestClient::new(CLIENT, 2);
    lab.world.push_ingress(vec![other.open(BROKER)]);
    // Api key 9999 does not exist.
    let mut frame = other.request(BROKER, 0, &MetadataRequest::default());
    if let Payload::Data(bytes) = &mut frame.payload {
        let mut raw = bytes.to_vec();
        raw[4] = 0x27;
        raw[5] = 0x0F;
        *bytes = Bytes::from(raw);
    }
    lab.world.push_ingress(vec![frame]);
    lab.run(20);
    let replies = replies_to(&lab.drain(), &other);
    assert!(replies.len() == 1 && replies[0].payload == Payload::Close);
}

#[test]
fn create_topic_then_metadata_shows_it_with_leader_and_isr() {
    let mut lab = Lab::one_broker();
    let topic_id = lab.create_topic("orders", 2, &[("retention.ms", "1000")]);
    assert!(topic_id != WireUuid::ZERO);
    let again = lab.call(
        7,
        &CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "orders".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..CreatableTopic::default()
            }],
            ..CreateTopicsRequest::default()
        },
    );
    assert!(again.topics[0].error_code == codes::TOPIC_ALREADY_EXISTS);
    assert!(again.topics[0].error_message.as_deref() == Some("Topic 'orders' already exists."));

    let expected = MetadataResponse {
        brokers: vec![MetadataResponseBroker {
            node_id: 1,
            host: "node-1".into(),
            port: 9092,
            rack: None,
            ..MetadataResponseBroker::default()
        }],
        cluster_id: Some(cluster_id_string(LAB_CLUSTER_ID)),
        controller_id: 1,
        topics: vec![MetadataResponseTopic {
            error_code: codes::NONE,
            name: Some("orders".into()),
            topic_id,
            is_internal: false,
            partitions: (0..2)
                .map(|index| MetadataResponsePartition {
                    error_code: codes::NONE,
                    partition_index: index,
                    leader_id: 1,
                    leader_epoch: 0,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: Vec::new(),
                    ..MetadataResponsePartition::default()
                })
                .collect(),
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponse::default()
    };
    assert!(lab.metadata("orders") == expected);
    let missing = lab.metadata("nope");
    assert!(
        missing.topics
            == vec![MetadataResponseTopic {
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                name: Some("nope".into()),
                ..MetadataResponseTopic::default()
            }]
    );
    let events = lab.events("topic_created");
    assert!(events.len() == 1 && events[0]["topic"] == "orders");
}

#[test]
fn produced_batches_fetch_back_byte_for_byte() {
    let mut lab = Lab::one_broker();
    let topic_id = lab.create_topic("t", 1, &[]);
    let batches = [
        batch(&["a", "b"], 100),
        batch(&["c"], 200),
        batch(&["d"], 300),
    ];
    for (b, base_offset) in batches.iter().zip([0, 2, 3]) {
        let response = lab.produce("t", 0, 1, b);
        assert!(
            response.responses[0].partition_responses
                == vec![PartitionProduceResponse {
                    index: 0,
                    error_code: codes::NONE,
                    base_offset,
                    log_append_time_ms: -1,
                    log_start_offset: 0,
                    ..PartitionProduceResponse::default()
                }]
        );
    }
    let fetched = lab.call(13, &fetch_request("t", topic_id, 0, 0, 0));
    let row = &fetched.responses[0].partitions[0];
    assert!(row.error_code == codes::NONE);
    assert!(
        (
            row.high_watermark,
            row.last_stable_offset,
            row.log_start_offset
        ) == (4, 4, 0)
    );
    let expected = vec![
        stored(&batches[0], 0, 0),
        stored(&batches[1], 2, 0),
        stored(&batches[2], 3, 0),
    ];
    assert!(fetched_batches(&fetched) == expected);
    let from_two = lab.call(13, &fetch_request("t", topic_id, 0, 2, 0));
    assert!(fetched_batches(&from_two) == expected[1..]);
    let at_end = lab.call(13, &fetch_request("t", topic_id, 0, 4, 0));
    assert!(fetched_batches(&at_end).is_empty());
    let beyond = lab.call(13, &fetch_request("t", topic_id, 0, 5, 0));
    assert!(beyond.responses[0].partitions[0].error_code == codes::OFFSET_OUT_OF_RANGE);
    let partition = lab.partition_snapshot(BROKER, "t", 0);
    assert!(partition["log_end"] == 4 && partition["hwm"] == 4 && partition["batches"] == 3);
}

#[test]
fn idempotent_producers_get_original_offsets_for_duplicates_and_errors_for_gaps() {
    let mut lab = Lab::one_broker();
    lab.create_topic("t", 1, &[]);
    let first = lab.produce("t", 0, 1, &idempotent_batch(&["a", "b"], 42, 0, 0));
    assert!(first.responses[0].partition_responses[0].base_offset == 0);
    let again = lab.produce("t", 0, 1, &idempotent_batch(&["a", "b"], 42, 0, 0));
    // Kafka answers a duplicate with the original offsets and the stored
    // batch's timestamp.
    assert!(
        again.responses[0].partition_responses
            == vec![PartitionProduceResponse {
                index: 0,
                error_code: codes::NONE,
                base_offset: 0,
                log_append_time_ms: 1,
                log_start_offset: 0,
                ..PartitionProduceResponse::default()
            }]
    );
    let gap = lab.produce("t", 0, 1, &idempotent_batch(&["c"], 42, 0, 7));
    assert!(
        gap.responses[0].partition_responses
            == vec![PartitionProduceResponse {
                index: 0,
                error_code: codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
                base_offset: -1,
                log_start_offset: 0,
                ..PartitionProduceResponse::default()
            }]
    );
    let next = lab.produce("t", 0, 1, &idempotent_batch(&["c"], 42, 0, 2));
    assert!(next.responses[0].partition_responses[0].base_offset == 2);
    assert!(lab.partition_snapshot(BROKER, "t", 0)["log_end"] == 3);
    assert!(lab.events("produce_error").len() == 1);
}

#[test]
fn produce_rows_are_refused_in_kafka_order() {
    let mut lab = Lab::one_broker();
    lab.create_topic("compact", 1, &[("cleanup.policy", "compact")]);
    lab.create_topic("strict", 1, &[("min.insync.replicas", "2")]);
    let keyless = RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Record::default()
        }],
        ..RecordBatch::default()
    };
    let refused = |error_code| PartitionProduceResponse {
        index: 0,
        error_code,
        base_offset: -1,
        ..PartitionProduceResponse::default()
    };
    let key_error =
        "Compacted topic cannot accept message without key in topic partition compact-0.";
    for (case, topic, acks, batch, expected) in [
        (
            "an unknown topic",
            "nope",
            1,
            batch(&["a"], 1),
            refused(codes::UNKNOWN_TOPIC_OR_PARTITION),
        ),
        (
            "an invalid acks",
            "strict",
            2,
            batch(&["a"], 1),
            refused(codes::INVALID_REQUIRED_ACKS),
        ),
        (
            "a keyless record on a compacted topic",
            "compact",
            1,
            keyless,
            PartitionProduceResponse {
                log_start_offset: 0,
                record_errors: vec![BatchIndexAndErrorMessage {
                    batch_index: 0,
                    batch_index_error_message: Some(key_error.to_string()),
                    ..BatchIndexAndErrorMessage::default()
                }],
                error_message: Some(format!(
                    "One or more records have been rejected due to 1 record errors in total, and only showing the first three errors at most: [RecordError(batchIndex=0, message='{key_error}')]"
                )),
                ..refused(codes::INVALID_RECORD)
            },
        ),
        (
            "min.insync.replicas above the replication factor",
            "strict",
            -1,
            batch(&["a"], 1),
            PartitionProduceResponse {
                error_code: codes::NONE,
                base_offset: 0,
                log_start_offset: 0,
                ..refused(codes::NONE)
            },
        ),
    ] {
        let response = lab.produce(topic, 0, acks, &batch);
        assert!(
            response.responses[0].partition_responses == vec![expected],
            "{case}"
        );
    }
    let empty = ProduceRequest {
        topic_data: vec![TopicProduceData {
            name: "strict".into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::Raw(Bytes::new())),
                ..PartitionProduceData::default()
            }],
            ..TopicProduceData::default()
        }],
        ..produce_request("strict", 0, 1, 1_000, &batch(&["a"], 1))
    };
    let response = lab.call(11, &empty);
    assert!(response.responses[0].partition_responses == vec![refused(codes::INVALID_RECORD)]);
}

#[test]
fn a_follower_answers_not_leader_with_the_kip_951_hint() {
    let mut lab = Lab::one_broker();
    apply_metadata(&mut lab, BROKER, &assignment("t", &[2, 1], &[2, 1], 3));
    let response = lab.produce("t", 0, 1, &batch(&["a"], 1));
    assert!(
        response.responses[0].partition_responses
            == vec![PartitionProduceResponse {
                index: 0,
                error_code: codes::NOT_LEADER_OR_FOLLOWER,
                base_offset: -1,
                current_leader: LeaderIdAndEpoch {
                    leader_id: 2,
                    leader_epoch: 3,
                    ..LeaderIdAndEpoch::default()
                },
                ..PartitionProduceResponse::default()
            }]
    );
    assert!(
        response.node_endpoints
            == vec![NodeEndpoint {
                node_id: 2,
                host: "node-2".into(),
                port: 9092,
                rack: None,
                ..NodeEndpoint::default()
            }]
    );
}

#[test]
fn acks_all_refuses_a_short_isr_before_writing() {
    let mut lab = Lab::one_broker();
    let mut records = assignment("s", &[1, 2], &[1], 0);
    records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "s".into(),
        overrides: BTreeMap::from([("min.insync.replicas".to_string(), "2".to_string())]),
    }));
    apply_metadata(&mut lab, BROKER, &records);
    let response = lab.produce("s", 0, -1, &batch(&["a"], 1));
    assert!(
        response.responses[0].partition_responses
            == vec![PartitionProduceResponse {
                index: 0,
                error_code: codes::NOT_ENOUGH_REPLICAS,
                base_offset: -1,
                log_start_offset: 0,
                ..PartitionProduceResponse::default()
            }]
    );
    assert!(lab.partition_snapshot(BROKER, "s", 0)["log_end"] == 0);
    let one = lab.produce("s", 0, 1, &batch(&["a"], 1));
    assert!(one.responses[0].partition_responses[0].error_code == codes::NONE);
}

#[test]
fn list_offsets_answers_earliest_latest_and_timestamps() {
    let mut lab = Lab::one_broker();
    lab.create_topic("t", 1, &[]);
    lab.produce("t", 0, 1, &batch(&["a", "b"], 100));
    lab.produce("t", 0, 1, &batch(&["c"], 300));
    let ask = |timestamp: i64| ListOffsetsRequest {
        replica_id: -1,
        topics: vec![ListOffsetsTopic {
            name: "t".into(),
            partitions: vec![ListOffsetsPartition {
                partition_index: 0,
                timestamp,
                ..ListOffsetsPartition::default()
            }],
            ..ListOffsetsTopic::default()
        }],
        ..ListOffsetsRequest::default()
    };
    for (timestamp, version, expected) in [
        (-2, 8, (codes::NONE, -1, 0, 0)),
        (-1, 8, (codes::NONE, -1, 3, 0)),
        (101, 8, (codes::NONE, 101, 1, 0)),
        (250, 8, (codes::NONE, 300, 2, 0)),
        (301, 8, (codes::NONE, -1, -1, -1)),
        (-3, 8, (codes::NONE, 300, 2, 0)),
        (-3, 6, (codes::UNSUPPORTED_VERSION, -1, -1, -1)),
        (-7, 8, (codes::UNSUPPORTED_VERSION, -1, -1, -1)),
        (-5, 9, (codes::NONE, -1, -1, -1)),
    ] {
        let response = lab.call(version, &ask(timestamp));
        let row = &response.topics[0].partitions[0];
        assert!(
            (row.error_code, row.timestamp, row.offset, row.leader_epoch) == expected,
            "{timestamp} at v{version}: {row:?}"
        );
    }
    let unknown = lab.call(
        8,
        &ListOffsetsRequest {
            topics: vec![ListOffsetsTopic {
                name: "nope".into(),
                partitions: vec![ListOffsetsPartition::default()],
                ..ListOffsetsTopic::default()
            }],
            ..ask(-1)
        },
    );
    assert!(unknown.topics[0].partitions[0].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    let epoch = lab.call(
        4,
        &OffsetForLeaderEpochRequest {
            topics: vec![OffsetForLeaderTopic {
                topic: "t".into(),
                partitions: vec![OffsetForLeaderPartition {
                    partition: 0,
                    leader_epoch: 0,
                    ..OffsetForLeaderPartition::default()
                }],
                ..OffsetForLeaderTopic::default()
            }],
            ..OffsetForLeaderEpochRequest::default()
        },
    );
    let row = &epoch.topics[0].partitions[0];
    assert!((row.error_code, row.leader_epoch, row.end_offset) == (codes::NONE, 0, 3));
}

#[test]
fn fetch_past_the_end_waits_for_max_wait_or_a_produce() {
    let mut lab = Lab::one_broker();
    let topic_id = lab.create_topic("t", 1, &[]);
    let frame = lab
        .client
        .request(BROKER, 13, &fetch_request("t", topic_id, 0, 0, 500));
    lab.world.push_ingress(vec![frame]);
    lab.run(400);
    assert!(lab.replies().is_empty());
    assert!(lab.snapshot(BROKER)["held_requests"] == 1);
    lab.run(200);
    let replies = lab.replies();
    assert!(replies.len() == 1);
    let (_, response) = decode_response::<FetchRequest>(&replies[0], 13).unwrap();
    assert!(fetched_batches(&response).is_empty());

    // A held fetch answers early when a produce lands, from another connection.
    let frame = lab
        .client
        .request(BROKER, 13, &fetch_request("t", topic_id, 0, 0, 5_000));
    lab.world.push_ingress(vec![frame]);
    lab.run(100);
    let mut producer = TestClient::new(CLIENT, 2);
    let produce = producer.request(
        BROKER,
        11,
        &produce_request("t", 0, 1, 1_000, &batch(&["x"], 1)),
    );
    lab.world.push_ingress(vec![producer.open(BROKER), produce]);
    lab.run(30);
    let frames = lab.drain();
    let replies = replies_to(&frames, &lab.client);
    assert!(replies.len() == 1);
    let (_, response) = decode_response::<FetchRequest>(&replies[0], 13).unwrap();
    assert!(fetched_batches(&response) == vec![stored(&batch(&["x"], 1), 0, 0)]);
    assert!(replies_to(&frames, &producer).len() == 1);
}

#[test]
fn acks_all_waits_for_the_high_watermark_and_times_out_without_the_follower() {
    let mut lab = Lab::one_broker();
    apply_metadata(&mut lab, BROKER, &assignment("t", &[1, 2], &[1, 2], 0));
    assert!(lab.partition_snapshot(BROKER, "t", 0)["isr"] == json!([1, 2]));
    let frame = lab.client.request(
        BROKER,
        11,
        &produce_request("t", 0, -1, 300, &batch(&["a"], 1)),
    );
    lab.world.push_ingress(vec![frame]);
    // A later request on the same connection waits behind the held one.
    let frame = lab.client.request(BROKER, 12, &MetadataRequest::default());
    lab.world.push_ingress(vec![frame]);
    lab.run(250);
    assert!(lab.replies().is_empty());
    assert!(lab.partition_snapshot(BROKER, "t", 0)["hwm"] == 0);
    lab.run(100);
    let replies = lab.replies();
    assert!(replies.len() == 2);
    let (_, produce) = decode_response::<ProduceRequest>(&replies[0], 11).unwrap();
    assert!(
        produce.responses[0].partition_responses
            == vec![PartitionProduceResponse {
                index: 0,
                error_code: codes::REQUEST_TIMED_OUT,
                base_offset: 0,
                log_start_offset: 0,
                ..PartitionProduceResponse::default()
            }]
    );
    let (_, metadata) = decode_response::<MetadataRequest>(&replies[1], 12).unwrap();
    assert!(metadata.brokers.len() == 2);
    // The log kept the batch; the record only waits for replication.
    assert!(lab.partition_snapshot(BROKER, "t", 0)["log_end"] == 1);
}

#[test]
fn two_brokers_replicate_and_the_leader_advances_the_hwm_on_follower_fetches() {
    let mut lab = two_brokers();
    let batches = [batch(&["a", "b"], 100), batch(&["c"], 200)];
    let frame = lab
        .client
        .request(BROKER, 11, &produce_request("t", 0, -1, 5_000, &batches[0]));
    lab.world.push_ingress(vec![frame]);
    lab.run(200);
    let replies = lab.replies();
    assert!(replies.len() == 1);
    let (_, produce) = decode_response::<ProduceRequest>(&replies[0], 11).unwrap();
    let row = &produce.responses[0].partition_responses[0];
    assert!(row.error_code == codes::NONE && row.base_offset == 0);
    let leader = lab.partition_snapshot(BROKER, "t", 0);
    assert!(leader["hwm"] == 2 && leader["log_end"] == 2 && leader["isr"] == json!([1, 2]));
    assert!(leader["followers"] == json!([{ "id": 2, "leo": 2, "lag_ms": 0 }]));
    let follower = lab.partition_snapshot(OTHER, "t", 0);
    assert!(follower["log_end"] == 2 && follower["fetch_state"] == "fetching");
    // The follower learns the high watermark from its next fetch response,
    // which the leader holds for replica.fetch.wait.max.ms.
    lab.run(600);
    assert!(lab.partition_snapshot(OTHER, "t", 0)["hwm"] == 2);

    // The follower serves a consumer fetch of the replicated bytes (v11+).
    let mut reader = TestClient::new(CLIENT, 3);
    lab.world.push_ingress(vec![reader.open(OTHER)]);
    let frame = reader.request(
        OTHER,
        13,
        &fetch_request("t", WireUuid(TOPIC_ID.into_bytes()), 0, 0, 0),
    );
    lab.world.push_ingress(vec![frame]);
    lab.run(30);
    let replies = replies_to(&lab.drain(), &reader);
    let (_, fetched) = decode_response::<FetchRequest>(&replies[0], 13).unwrap();
    assert!(fetched_batches(&fetched) == vec![stored(&batches[0], 0, 0)]);
    // No OffsetForLeaderEpoch round: the follower truncates on fetch.
    assert!(
        lab.snapshot(BROKER)["requests"]
            .get("OffsetForLeaderEpoch")
            .is_none()
    );
    assert!(lab.snapshot(BROKER)["requests"]["Fetch"].as_u64() > Some(1));
}

#[test]
fn a_dead_follower_leaves_the_isr_and_rejoins_after_catching_up() {
    let mut lab = two_brokers();
    lab.produce("t", 0, 1, &batch(&["a", "b"], 100));
    lab.run(600);
    lab.world.fault(Fault::Kill { node: OTHER });
    lab.produce("t", 0, 1, &batch(&["c"], 200));
    lab.run(1_000);
    assert!(lab.partition_snapshot(BROKER, "t", 0)["isr"] == json!([1, 2]));
    assert!(lab.partition_snapshot(BROKER, "t", 0)["hwm"] == 2);
    lab.run(2_500);
    let shrunk = lab.partition_snapshot(BROKER, "t", 0);
    assert!(shrunk["isr"] == json!([1]) && shrunk["hwm"] == 3);
    let changes = lab.events("isr_change");
    assert!(
        changes
            .iter()
            .any(|e| e["isr"] == json!([1]) && e["level"] == "warn")
    );
    let pending = lab
        .world
        .control(BROKER, json!({ "cmd": "pending_alter_partition" }))
        .unwrap();
    assert!(
        pending
            == json!([{ "topic": "t", "partition": 0, "new_isr": [1], "leader_epoch": 0, "partition_epoch": 0 }])
    );
    let quick = lab.produce("t", 0, -1, &batch(&["d"], 300));
    assert!(quick.responses[0].partition_responses[0].error_code == codes::NONE);

    lab.world.fault(Fault::Restart { node: OTHER });
    lab.run(3_000);
    let back = lab.partition_snapshot(BROKER, "t", 0);
    assert!(back["isr"] == json!([1, 2]), "{back}");
    assert!(lab.partition_snapshot(OTHER, "t", 0)["log_end"] == 4);
}

#[test]
fn a_diverging_follower_truncates_to_the_new_leaders_epoch() {
    let mut lab = two_brokers();
    let batches = [
        batch(&["a", "b"], 100),
        batch(&["b2"], 150),
        batch(&["c"], 200),
    ];
    lab.produce("t", 0, 1, &batches[0]);
    lab.run(600);
    // Broker 1 writes a batch broker 2 never sees, then broker 2 takes the
    // partition at epoch 1 without it.
    lab.world.fault(Fault::Kill { node: OTHER });
    lab.produce("t", 0, 1, &batches[1]);
    lab.world.fault(Fault::Restart { node: OTHER });
    let moved = assignment("t", &[1, 2], &[2, 1], 1);
    apply_metadata(&mut lab, BROKER, &moved);
    apply_metadata(&mut lab, OTHER, &moved);
    let mut writer = TestClient::new(CLIENT, 2);
    let frame = writer.request(OTHER, 11, &produce_request("t", 0, 1, 1_000, &batches[2]));
    lab.world.push_ingress(vec![writer.open(OTHER), frame]);
    lab.run(1_500);
    assert!(
        lab.events("replica_truncated")
            == vec![json!({ "topic": "t", "partition": 0, "from": 3, "to": 2, "level": "warn" })]
    );
    let old_leader = lab.partition_snapshot(BROKER, "t", 0);
    assert!(
        old_leader["log_end"] == 3 && old_leader["leader"] == 2,
        "{old_leader}"
    );
    let mut reader = TestClient::new(CLIENT, 3);
    lab.world.push_ingress(vec![reader.open(BROKER)]);
    let frame = reader.request(
        BROKER,
        13,
        &fetch_request("t", WireUuid(TOPIC_ID.into_bytes()), 0, 2, 0),
    );
    lab.world.push_ingress(vec![frame]);
    lab.run(30);
    let replies = replies_to(&lab.drain(), &reader);
    let (_, fetched) = decode_response::<FetchRequest>(&replies[0], 13).unwrap();
    assert!(fetched_batches(&fetched) == vec![stored(&batches[2], 2, 1)]);
}

#[test]
fn retention_deletes_old_batches_on_the_tick() {
    let mut lab = Lab::one_broker();
    let topic_id = lab.create_topic("t", 1, &[("retention.ms", "1000")]);
    lab.create_topic(
        "kept",
        1,
        &[("retention.ms", "1000"), ("cleanup.policy", "compact")],
    );
    lab.produce("t", 0, 1, &batch(&["a"], 10));
    lab.produce("t", 0, 1, &batch(&["b"], 20));
    lab.produce("kept", 0, 1, &batch(&["a"], 10));
    lab.run(1_500);
    // The tick at one second is inside the window of the batches stamped 10 and 20.
    assert!(lab.partition_snapshot(BROKER, "t", 0)["log_start"] == 0);
    lab.run(1_000);
    assert!(lab.partition_snapshot(BROKER, "t", 0)["log_start"] == 2);
    // A compacted topic without `delete` in its policy keeps everything.
    assert!(lab.partition_snapshot(BROKER, "kept", 0)["log_start"] == 0);
    let events = lab.events("retention");
    assert!(events.len() == 1 && events[0]["batches_deleted"] == 2);
    let fetched = lab.call(13, &fetch_request("t", topic_id, 0, 0, 0));
    assert!(fetched.responses[0].partitions[0].error_code == codes::OFFSET_OUT_OF_RANGE);
    let produce = lab.produce("t", 0, 1, &batch(&["c"], 3_000));
    let row = &produce.responses[0].partition_responses[0];
    assert!(row.base_offset == 2 && row.log_start_offset == 2);
}

#[test]
fn a_reloaded_broker_serves_what_it_stored() {
    let scenario = scenario(&[(1, json!({ "broker_id": 1 }))]);
    let mut lab = Lab::from_scenario(&scenario, &[BROKER]);
    let topic_id = lab.create_topic("t", 1, &[]);
    let batches = [batch(&["a", "b"], 100), batch(&["c"], 200)];
    for b in &batches {
        lab.produce("t", 0, 1, b);
    }
    let mut image = DurableImage::default();
    for (node, op) in lab.world.drain_durable() {
        assert!(node == BROKER);
        image.apply(op);
    }
    assert!(image.logs["log/t/0"].len() == 2);
    assert!(image.kv["meta/t/0"]["hwm"].0.as_ref() == b"3");
    assert!(!image.logs["metadata"].is_empty());

    let mut reloaded =
        Lab::from_scenario_with_state(&scenario, &[BROKER], BTreeMap::from([(BROKER, image)]));
    let metadata = reloaded.metadata("t");
    assert!(metadata.topics[0].topic_id == topic_id);
    let fetched = reloaded.call(13, &fetch_request("t", topic_id, 0, 0, 0));
    let row = &fetched.responses[0].partitions[0];
    assert!(row.error_code == codes::NONE && row.high_watermark == 3);
    assert!(
        fetched_batches(&fetched) == vec![stored(&batches[0], 0, 0), stored(&batches[1], 2, 0)]
    );
    let next = reloaded.produce("t", 0, 1, &batch(&["d"], 300));
    assert!(next.responses[0].partition_responses[0].base_offset == 3);
}

#[test]
fn find_coordinator_creates_the_offsets_topic_then_hashes_groups_like_kafka() {
    let mut lab = Lab::one_broker();
    let find = FindCoordinatorRequest {
        key_type: 0,
        coordinator_keys: vec!["consumer-group".into(), "abc".into()],
        ..FindCoordinatorRequest::default()
    };
    let row = |key: &str, error_code, node: Option<(i32, &str, i32)>| {
        let (node_id, host, port) = node.unwrap_or((-1, "", -1));
        Coordinator {
            key: key.into(),
            node_id,
            host: host.into(),
            port,
            error_code,
            ..Coordinator::default()
        }
    };
    // The request that finds no offsets topic creates it and answers
    // COORDINATOR_NOT_AVAILABLE, as Kafka's does.
    let first = lab.call(4, &find);
    assert!(
        first.coordinators
            == vec![
                row("consumer-group", codes::COORDINATOR_NOT_AVAILABLE, None),
                row("abc", codes::COORDINATOR_NOT_AVAILABLE, None),
            ]
    );
    let found = lab.call(4, &find);
    let here = Some((1, "node-1", 9092));
    assert!(
        found.coordinators
            == vec![
                row("consumer-group", codes::NONE, here),
                row("abc", codes::NONE, here)
            ]
    );
    let metadata = lab.metadata("__consumer_offsets");
    let topic = &metadata.topics[0];
    assert!(topic.is_internal && topic.partitions.len() == 50);
    assert!(lab.partition_snapshot(BROKER, "__consumer_offsets", 38)["leader"] == 1);
    let legacy = lab.call(
        1,
        &FindCoordinatorRequest {
            key: "consumer-group".into(),
            key_type: 0,
            ..FindCoordinatorRequest::default()
        },
    );
    assert!(
        legacy
            == FindCoordinatorResponse {
                error_message: Some("NONE".into()),
                node_id: 1,
                host: "node-1".into(),
                port: 9092,
                ..FindCoordinatorResponse::default()
            }
    );
    let txn = lab.call(
        4,
        &FindCoordinatorRequest {
            key_type: 1,
            coordinator_keys: vec!["tx".into()],
            ..FindCoordinatorRequest::default()
        },
    );
    assert!(txn.coordinators == vec![row("tx", codes::COORDINATOR_NOT_AVAILABLE, None)]);
    let join = lab.call(
        9,
        &JoinGroupRequest {
            group_id: "g".into(),
            ..JoinGroupRequest::default()
        },
    );
    assert!(join.error_code == codes::COORDINATOR_NOT_AVAILABLE);
}

#[test]
fn describe_configs_reads_the_image() {
    let mut lab = Lab::one_broker();
    lab.create_topic("t", 3, &[("retention.ms", "1000")]);
    let configs = lab.call(
        4,
        &DescribeConfigsRequest {
            resources: vec![
                DescribeConfigsResource {
                    resource_type: 2,
                    resource_name: "t".into(),
                    ..DescribeConfigsResource::default()
                },
                DescribeConfigsResource {
                    resource_type: 2,
                    resource_name: "nope".into(),
                    ..DescribeConfigsResource::default()
                },
                DescribeConfigsResource {
                    resource_type: 4,
                    resource_name: "1".into(),
                    configuration_keys: Some(vec!["broker.id".into()]),
                    ..DescribeConfigsResource::default()
                },
            ],
            include_synonyms: true,
            ..DescribeConfigsRequest::default()
        },
    );
    let topic = &configs.results[0];
    assert!(topic.error_code == codes::NONE && topic.configs.len() == 33);
    let retention = topic
        .configs
        .iter()
        .find(|c| c.name == "retention.ms")
        .unwrap();
    assert!(retention.value.as_deref() == Some("1000") && retention.config_source == 1);
    assert!(retention.synonyms.len() == 2 && retention.synonyms[1].name == "log.retention.ms");
    assert!(configs.results[1].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    let broker = &configs.results[2];
    assert!(
        broker.configs.len() == 1
            && broker.configs[0].read_only
            && broker.configs[0].value.as_deref() == Some("1")
    );
}

#[test]
fn describe_topic_partitions_pages_with_a_cursor() {
    let mut lab = Lab::one_broker();
    lab.create_topic("t", 3, &[]);
    let ask = |cursor: Option<Cursor>| DescribeTopicPartitionsRequest {
        topics: vec![TopicRequest {
            name: "t".into(),
            ..TopicRequest::default()
        }],
        response_partition_limit: 2,
        cursor,
        ..DescribeTopicPartitionsRequest::default()
    };
    let page = lab.call(0, &ask(None));
    let indexes = |response: &krabka_protocol::owned::describe_topic_partitions_response::DescribeTopicPartitionsResponse| -> Vec<i32> {
        response.topics[0].partitions.iter().map(|p| p.partition_index).collect()
    };
    assert!(indexes(&page) == vec![0, 1]);
    let cursor = page.next_cursor.clone().unwrap();
    assert!((cursor.topic_name.as_str(), cursor.partition_index) == ("t", 2));
    let rest = lab.call(
        0,
        &ask(Some(Cursor {
            topic_name: "t".into(),
            partition_index: 2,
            ..Cursor::default()
        })),
    );
    assert!(indexes(&rest) == vec![2] && rest.next_cursor.is_none());
    let foreign = lab.call(
        0,
        &ask(Some(Cursor {
            topic_name: "other".into(),
            partition_index: 0,
            ..Cursor::default()
        })),
    );
    assert!(foreign.topics[0].error_code == codes::INVALID_REQUEST);
}

#[test]
fn delete_topics_refuses_like_kafka_and_deletes_the_rest() {
    let mut lab = Lab::one_broker();
    let topic_id = lab.create_topic("t", 1, &[]);
    let kept_id = lab.create_topic("kept", 1, &[]);
    lab.produce("t", 0, 1, &batch(&["a"], 1));
    let by = |name: Option<&str>, topic_id| DeleteTopicState {
        name: name.map(str::to_owned),
        topic_id,
        ..DeleteTopicState::default()
    };
    let deleted = lab.call(
        6,
        &DeleteTopicsRequest {
            topics: vec![
                by(None, topic_id),
                by(None, WireUuid::ZERO),
                by(Some("kept"), kept_id),
                by(Some("nope"), WireUuid::ZERO),
                by(Some("dup"), WireUuid::ZERO),
                by(Some("dup"), WireUuid::ZERO),
            ],
            ..DeleteTopicsRequest::default()
        },
    );
    let row =
        |name: Option<&str>, topic_id, error_code, message: Option<&str>| DeletableTopicResult {
            name: name.map(str::to_owned),
            topic_id,
            error_code,
            error_message: message.map(str::to_owned),
            ..DeletableTopicResult::default()
        };
    assert!(
        deleted.responses
            == vec![
                row(
                    None,
                    WireUuid::ZERO,
                    codes::INVALID_REQUEST,
                    Some("Neither topic name nor id were specified.")
                ),
                row(
                    Some("kept"),
                    kept_id,
                    codes::INVALID_REQUEST,
                    Some("You may not specify both topic name and topic id.")
                ),
                row(
                    Some("dup"),
                    WireUuid::ZERO,
                    codes::INVALID_REQUEST,
                    Some("Duplicate topic name.")
                ),
                row(
                    Some("nope"),
                    WireUuid::ZERO,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    Some("This server does not host this topic-partition.")
                ),
                row(Some("t"), topic_id, codes::NONE, None),
            ]
    );
    assert!(lab.metadata("t").topics[0].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    assert!(lab.metadata("kept").topics[0].error_code == codes::NONE);
    assert!(lab.events("topic_deleted").len() == 1);
}

#[test]
fn metadata_auto_creates_missing_topics_after_the_existing_ones() {
    let mut lab = Lab::one_broker();
    lab.create_topic("t", 1, &[]);
    let names = |response: &MetadataResponse| -> Vec<(Option<String>, i16)> {
        response
            .topics
            .iter()
            .map(|t| (t.name.clone(), t.error_code))
            .collect()
    };
    let creating = lab.call(
        12,
        &MetadataRequest {
            topics: Some(
                ["auto", "bad/name", "t"]
                    .into_iter()
                    .map(|name| MetadataRequestTopic {
                        name: Some(name.into()),
                        ..MetadataRequestTopic::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: true,
            ..MetadataRequest::default()
        },
    );
    assert!(
        names(&creating)
            == vec![
                (Some("t".into()), codes::NONE),
                (Some("bad/name".into()), codes::INVALID_TOPIC_EXCEPTION),
                (Some("auto".into()), codes::LEADER_NOT_AVAILABLE),
            ]
    );
    let created = lab.metadata("auto");
    assert!(created.topics[0].error_code == codes::NONE && created.topics[0].partitions.len() == 1);
}
