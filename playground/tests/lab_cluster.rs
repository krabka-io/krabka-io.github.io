//! Three brokers as one `KRaft` cluster in combined mode, driven over the
//! Kafka wire by a scripted client outside the world: the quorum elects an
//! active controller and every broker registers and is unfenced, a topic
//! created through any broker is striped over the three, leadership and group
//! coordination move when a broker dies, and acknowledged records and
//! committed offsets survive the failover and a reload of the whole cluster.

use std::collections::{BTreeMap, BTreeSet};

use assert2::assert;
use bytes::Bytes;
use krabka_playground::lab::{
    Endpoint, Fault, Millis, NodeId, World,
    broker::{
        CONSUMER_OFFSETS_PARTITIONS, CONSUMER_OFFSETS_TOPIC, LAB_CLUSTER_ID,
        broker_api_versions_table, cluster_id_string, group_partition, supported_features,
        test_support::{TestClient, batch, decode_response, encode_batch},
    },
    codes,
    controller::{METADATA_TOPIC, RAFT_PORT},
    net::{DurableImage, Frame, Payload},
    scenario::Scenario,
    testing::TestWorld,
};
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        common::{
            consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
            describe_quorum_response::replica_state::ReplicaState,
        },
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
        create_topics_request::{CreatableReplicaAssignment, CreatableTopic, CreateTopicsRequest},
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_quorum_request::{self, DescribeQuorumRequest},
        describe_quorum_response::{self, DescribeQuorumResponse, Listener},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::Coordinator,
        heartbeat_request::HeartbeatRequest,
        heartbeat_response::HeartbeatResponse,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        leave_group_response::{LeaveGroupResponse, MemberResponse},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
        offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopics,
        },
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse},
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{RecordBatch, RecordsPayload},
};
use serde_json::{Value, json};

/// The node the scripted client speaks from. The world does not host it, so
/// its frames go in through `push_ingress` and the answers come out of the
/// egress queue.
const CLIENT: u32 = 99;

/// The brokers of every test cluster, all three voters of the quorum.
const BROKERS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

/// The longest a request waits for its answer, in logical milliseconds.
const ANSWER_WAIT_MS: Millis = 5_000;

/// Three brokers on 5 ms links, each told the quorum's voters.
fn scenario() -> Scenario {
    let nodes: Vec<Value> = BROKERS
        .iter()
        .map(|broker| {
            json!({
                "id": broker.0, "kind": "broker",
                "config": { "broker_id": broker.0, "controller_quorum_voters": [1, 2, 3] },
            })
        })
        .collect();
    serde_json::from_value(json!({
        "version": 1, "seed": 7, "links": { "default_latency_ms": 5 }, "nodes": nodes,
    }))
    .unwrap()
}

/// The node of a wire broker id.
fn node(id: i32) -> NodeId {
    NodeId(u32::try_from(id).unwrap())
}

/// One connection of the scripted client to one listener.
struct Conn {
    client: TestClient,
    listener: Endpoint,
}

/// A cluster in a world, and the frames its brokers sent the scripted client.
struct Cluster {
    world: World,
    /// The brokers a test killed and has not restarted.
    down: BTreeSet<NodeId>,
    /// The last connection id the client used; every connection takes a new
    /// one.
    last_conn: u32,
    /// The frames that reached the client and no receive has taken yet.
    inbox: Vec<Frame>,
}

impl Cluster {
    /// The scenario's cluster, built and started through [`TestWorld`], with
    /// the client outside it. Nothing has run yet.
    fn new() -> Self {
        let mut test_world = TestWorld::from_scenario(&scenario());
        test_world.world_mut().set_hosted(&BROKERS);
        Self::around(test_world.take())
    }

    /// The cluster rebuilt from the durable images a page keeps across a
    /// reload.
    fn reloaded(images: BTreeMap<NodeId, DurableImage>) -> Self {
        Self::around(World::from_scenario_with_state(&scenario(), &BROKERS, images).unwrap())
    }

    fn around(world: World) -> Self {
        Self {
            world,
            down: BTreeSet::new(),
            last_conn: 0,
            inbox: Vec::new(),
        }
    }

    /// A cluster whose brokers all serve their clients.
    fn serving() -> Self {
        let mut cluster = Self::new();
        cluster.wait_until_serving(5_000);
        cluster
    }

    /// Run until every live broker is `RUNNING` (registered, caught up with
    /// the metadata log and unfenced) and every live broker's image shows
    /// every live broker unfenced.
    fn wait_until_serving(&mut self, max_ms: Millis) {
        let serving = self.run_until(max_ms, |cluster| {
            let live: Vec<Value> = cluster.live().iter().map(|b| json!(b.0)).collect();
            cluster.live().iter().all(|broker| {
                let snapshot = cluster.snapshot(*broker);
                let unfenced: Vec<Value> = snapshot["brokers"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|b| b["fenced"] == false)
                    .map(|b| b["id"].clone())
                    .collect();
                snapshot["state"] == "RUNNING" && unfenced == live
            })
        });
        assert!(serving, "the brokers do not serve after {max_ms} ms");
    }

    /// Advance logical time by `ms`, keeping what reaches the client.
    fn run(&mut self, ms: Millis) {
        let until = self.world.now() + ms;
        self.world.step_until(until);
        self.collect();
    }

    fn collect(&mut self) {
        let frames = self.world.drain_egress().into_iter().map(|t| t.frame);
        self.inbox.extend(frames);
    }

    /// Run in 10 ms slices until `done` holds; whether it did within
    /// `max_ms`.
    fn run_until(&mut self, max_ms: Millis, mut done: impl FnMut(&Self) -> bool) -> bool {
        let deadline = self.world.now() + max_ms;
        while !done(self) {
            if self.world.now() >= deadline {
                return false;
            }
            self.run(10);
        }
        true
    }

    /// Run one delivery or timer at a time until `done` holds; whether it
    /// did within `max_ms`.
    fn step_until(&mut self, max_ms: Millis, mut done: impl FnMut(&Self) -> bool) -> bool {
        let deadline = self.world.now() + max_ms;
        let mut held = done(self);
        while !held && self.world.step_once(deadline) {
            held = done(self);
        }
        self.collect();
        held
    }

    fn snapshot(&self, broker: NodeId) -> Value {
        self.world.node_snapshot(broker).unwrap()
    }

    /// The brokers that run, ascending.
    fn live(&self) -> Vec<NodeId> {
        BROKERS
            .into_iter()
            .filter(|broker| !self.down.contains(broker))
            .collect()
    }

    /// The live broker that says it is the active controller.
    fn active_controller(&self) -> Option<NodeId> {
        self.live()
            .into_iter()
            .find(|broker| self.snapshot(*broker)["quorum"]["active"] == true)
    }

    /// A partition as `broker`'s snapshot shows it, or `null`.
    fn partition(&self, broker: NodeId, topic: &str, index: i32) -> Value {
        self.snapshot(broker)["topics"]
            .as_array()
            .and_then(|topics| topics.iter().find(|t| t["name"] == topic))
            .and_then(|topic| topic["partitions"].as_array())
            .and_then(|partitions| partitions.iter().find(|p| p["index"] == index))
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn kill(&mut self, broker: NodeId) {
        self.world.fault(Fault::Kill { node: broker });
        self.down.insert(broker);
    }

    fn restart(&mut self, broker: NodeId) {
        self.world.fault(Fault::Restart { node: broker });
        self.down.remove(&broker);
    }

    /// Every durable op the brokers recorded, folded into one image each.
    fn durable_images(&mut self) -> BTreeMap<NodeId, DurableImage> {
        let mut images: BTreeMap<NodeId, DurableImage> = BTreeMap::new();
        for (node, op) in self.world.drain_durable() {
            images.entry(node).or_default().apply(op);
        }
        images
    }

    /// A new connection to `broker`'s client listener.
    fn connect(&mut self, broker: NodeId) -> Conn {
        self.connect_to(Endpoint::kafka(broker))
    }

    fn connect_to(&mut self, listener: Endpoint) -> Conn {
        self.last_conn += 1;
        let client = TestClient::new(CLIENT, self.last_conn);
        self.world.push_ingress(vec![client.open_to(listener)]);
        Conn { client, listener }
    }

    /// Send a request on `conn`; returns its correlation id.
    fn send<R: ProtocolRequest>(&mut self, conn: &mut Conn, version: i16, request: &R) -> i32 {
        let frame = conn.client.request_to(conn.listener, version, request);
        self.world.push_ingress(vec![frame]);
        conn.client.last_correlation()
    }

    /// The answer to request `correlation` on `conn`, or `None` when none
    /// arrives within `max_ms`.
    fn receive<R: ProtocolRequest>(
        &mut self,
        conn: &Conn,
        version: i16,
        correlation: i32,
        max_ms: Millis,
    ) -> Option<R::Response> {
        let deadline = self.world.now() + max_ms;
        loop {
            let answer = self.inbox.iter().position(|frame| {
                frame.dst == conn.client.endpoint()
                    && frame.conn == conn.client.conn
                    && matches!(frame.payload, Payload::Data(_))
            });
            if let Some(at) = answer {
                let frame = self.inbox.remove(at);
                let (answered, response) = decode_response::<R>(&frame, version).unwrap();
                assert!(answered == correlation);
                return Some(response);
            }
            if self.world.now() >= deadline {
                return None;
            }
            self.run(5);
        }
    }

    /// Send a request and wait for its answer.
    fn call<R: ProtocolRequest>(
        &mut self,
        conn: &mut Conn,
        version: i16,
        request: &R,
    ) -> R::Response {
        let correlation = self.send(conn, version, request);
        self.receive::<R>(conn, version, correlation, ANSWER_WAIT_MS)
            .unwrap_or_else(|| panic!("no answer from {} to api key {}", conn.listener, R::API_KEY))
    }

    /// `Metadata` v12 for `topics`, from `broker`.
    fn metadata(&mut self, broker: NodeId, topics: &[&str]) -> MetadataResponse {
        let mut conn = self.connect(broker);
        let request = MetadataRequest {
            topics: Some(
                topics
                    .iter()
                    .map(|name| MetadataRequestTopic {
                        name: Some((*name).to_string()),
                        ..MetadataRequestTopic::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: false,
            ..MetadataRequest::default()
        };
        self.call(&mut conn, 12, &request)
    }

    /// Create a topic through `via`, wait until every live broker applied
    /// it, and return its id.
    fn create_topic(&mut self, via: NodeId, topic: CreatableTopic) -> WireUuid {
        let name = topic.name.clone();
        let partitions = if topic.assignments.is_empty() {
            topic.num_partitions
        } else {
            i32::try_from(topic.assignments.len()).unwrap()
        };
        let mut conn = self.connect(via);
        let request = CreateTopicsRequest {
            topics: vec![topic],
            timeout_ms: 30_000,
            ..CreateTopicsRequest::default()
        };
        let created = self.call(&mut conn, 4, &request);
        let expected = CreateTopicsResponse {
            topics: vec![CreatableTopicResult {
                name: name.clone(),
                error_message: None,
                ..CreatableTopicResult::default()
            }],
            ..CreateTopicsResponse::default()
        };
        assert!(created == expected);
        let applied = self.run_until(1_000, |cluster| {
            cluster.live().iter().all(|broker| {
                (0..partitions)
                    .all(|index| cluster.partition(*broker, &name, index)["leader"].is_i64())
            })
        });
        assert!(applied, "{name} never reached every broker");
        self.metadata(via, &[&name]).topics[0].topic_id
    }

    /// Create `orders`, three partitions of three replicas each, through
    /// `via`.
    fn create_orders(&mut self, via: NodeId) -> WireUuid {
        self.create_topic(
            via,
            CreatableTopic {
                name: "orders".into(),
                num_partitions: 3,
                replication_factor: 3,
                ..CreatableTopic::default()
            },
        )
    }

    /// Produce `records` with `acks=-1` to `broker` and wait for the answer.
    fn produce(
        &mut self,
        broker: NodeId,
        topic: &str,
        partition: i32,
        records: &RecordBatch,
    ) -> PartitionProduceResponse {
        let mut conn = self.connect(broker);
        let response: ProduceResponse =
            self.call(&mut conn, 11, &produce_request(topic, partition, records));
        response.responses[0].partition_responses[0].clone()
    }

    /// Every batch of a partition, fetched from its leader `broker` up to
    /// the high watermark.
    fn fetch_all(
        &mut self,
        broker: NodeId,
        topic_id: WireUuid,
        partition: i32,
    ) -> Vec<RecordBatch> {
        let mut conn = self.connect(broker);
        let request = FetchRequest {
            min_bytes: 1,
            topics: vec![FetchTopic {
                topic_id,
                partitions: vec![FetchPartition {
                    partition,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        };
        let response = self.call(&mut conn, 13, &request);
        let row = &response.responses[0].partitions[0];
        assert!(row.error_code == codes::NONE, "{row:?}");
        match &row.records {
            Some(RecordsPayload::V2(batches)) => batches.clone(),
            Some(RecordsPayload::Raw(bytes) | RecordsPayload::Legacy(bytes))
                if bytes.is_empty() =>
            {
                Vec::new()
            }
            other => panic!("unexpected records {other:?}"),
        }
    }

    /// The coordinator `broker` names for `group`, asked again while the
    /// offsets topic is created. Returns once the coordinator has loaded
    /// the group's partition, as a client finds after its retries.
    fn find_coordinator(&mut self, broker: NodeId, group: &str) -> Coordinator {
        let mut conn = self.connect(broker);
        let request = FindCoordinatorRequest {
            key_type: 0,
            coordinator_keys: vec![group.to_string()],
            ..FindCoordinatorRequest::default()
        };
        for _ in 0..100 {
            let mut response = self.call(&mut conn, 4, &request);
            let coordinator = response.coordinators.remove(0);
            if coordinator.error_code == codes::NONE {
                let at = node(coordinator.node_id);
                let partition = group_partition(group, CONSUMER_OFFSETS_PARTITIONS);
                let loaded = self.run_until(ANSWER_WAIT_MS, |cluster| {
                    cluster.snapshot(at)["groups"]["loaded_partitions"]
                        .as_array()
                        .is_some_and(|loaded| loaded.contains(&json!(partition)))
                });
                assert!(loaded, "broker {at} never loaded partition {partition}");
                return coordinator;
            }
            self.run(100);
        }
        panic!("broker {broker} never found the coordinator of {group}");
    }

    /// `OffsetFetch` v9 of every offset `group` committed, from `broker`.
    fn committed_offsets(&mut self, broker: NodeId, group: &str) -> OffsetFetchResponse {
        let mut conn = self.connect(broker);
        let request = OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: group.to_string(),
                member_id: None,
                member_epoch: -1,
                topics: None,
                ..OffsetFetchRequestGroup::default()
            }],
            ..OffsetFetchRequest::default()
        };
        self.call(&mut conn, 9, &request)
    }
}

fn produce_request(topic: &str, partition: i32, records: &RecordBatch) -> ProduceRequest {
    ProduceRequest {
        acks: -1,
        timeout_ms: 10_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(RecordsPayload::Raw(encode_batch(records))),
                ..PartitionProduceData::default()
            }],
            ..TopicProduceData::default()
        }],
        ..ProduceRequest::default()
    }
}

/// The answer to an acknowledged produce.
fn acked(index: i32, base_offset: i64) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset: 0,
        ..PartitionProduceResponse::default()
    }
}

/// A batch as the log keeps it: offsets and leader epoch assigned.
fn stored(records: &RecordBatch, base_offset: i64, leader_epoch: i32) -> RecordBatch {
    RecordBatch {
        base_offset,
        partition_leader_epoch: leader_epoch,
        ..records.clone()
    }
}

/// A `Metadata` partition row without an error.
fn partition_row(
    index: i32,
    leader: i32,
    leader_epoch: i32,
    replicas: &[i32],
    isr: &[i32],
    offline: &[i32],
) -> MetadataResponsePartition {
    MetadataResponsePartition {
        partition_index: index,
        leader_id: leader,
        leader_epoch,
        replica_nodes: replicas.to_vec(),
        isr_nodes: isr.to_vec(),
        offline_replicas: offline.to_vec(),
        ..MetadataResponsePartition::default()
    }
}

/// What `Metadata` v12 answers about `orders` while `live` are the unfenced
/// brokers. `controller_id` is a random live broker in `KRaft`, so it comes
/// from the answer.
fn orders_metadata(
    live: &[i32],
    controller_id: i32,
    topic_id: WireUuid,
    partitions: &[MetadataResponsePartition],
) -> MetadataResponse {
    MetadataResponse {
        brokers: live
            .iter()
            .map(|id| MetadataResponseBroker {
                node_id: *id,
                host: format!("node-{id}"),
                port: 9092,
                rack: None,
                ..MetadataResponseBroker::default()
            })
            .collect(),
        cluster_id: Some(cluster_id_string(LAB_CLUSTER_ID)),
        controller_id,
        topics: vec![MetadataResponseTopic {
            name: Some("orders".into()),
            topic_id,
            partitions: partitions.to_vec(),
            ..MetadataResponseTopic::default()
        }],
        ..MetadataResponse::default()
    }
}

/// `Metadata` from every broker in `live` agrees with `partitions`, and each
/// names one of `live` as its controller.
fn every_broker_agrees(
    cluster: &mut Cluster,
    live: &[i32],
    topic_id: WireUuid,
    partitions: &[MetadataResponsePartition],
) {
    for id in live {
        let broker = node(*id);
        let metadata = cluster.metadata(broker, &["orders"]);
        assert!(live.contains(&metadata.controller_id));
        let expected = orders_metadata(live, metadata.controller_id, topic_id, partitions);
        assert!(metadata == expected, "broker {broker}");
    }
}

#[test]
fn three_voters_elect_a_controller_and_every_broker_registers_unfenced() {
    let mut cluster = Cluster::new();
    cluster.wait_until_serving(5_000);
    // The lab staggers election timeouts by node id, so broker 1 wins the
    // first election, and every broker knows it.
    assert!(cluster.active_controller() == Some(NodeId(1)));
    for broker in BROKERS {
        let snapshot = cluster.snapshot(broker);
        assert!(snapshot["controller_id"] == 1, "broker {broker}");
        assert!(snapshot["quorum"]["leader"] == 1, "broker {broker}");
    }
    let unfenced: BTreeSet<Option<NodeId>> = cluster
        .world
        .events()
        .filter(|event| event.kind == "broker_unfenced")
        .map(|event| event.node)
        .collect();
    assert!(unfenced == BROKERS.into_iter().map(Some).collect());
    for broker in BROKERS {
        let mut conn = cluster.connect(broker);
        let request = DescribeClusterRequest {
            endpoint_type: 1,
            ..DescribeClusterRequest::default()
        };
        let described = cluster.call(&mut conn, 2, &request);
        assert!([1, 2, 3].contains(&described.controller_id));
        let expected = DescribeClusterResponse {
            endpoint_type: 1,
            cluster_id: cluster_id_string(LAB_CLUSTER_ID),
            controller_id: described.controller_id,
            brokers: (1..=3)
                .map(|id| DescribeClusterBroker {
                    broker_id: id,
                    host: format!("node-{id}"),
                    port: 9092,
                    rack: None,
                    is_fenced: false,
                    ..DescribeClusterBroker::default()
                })
                .collect(),
            cluster_authorized_operations: i32::MIN,
            ..DescribeClusterResponse::default()
        };
        assert!(described == expected, "broker {broker}");
    }
}

#[test]
fn a_client_that_connects_early_is_answered_once_its_broker_runs() {
    let mut cluster = Cluster::new();
    let mut conn = cluster.connect(NodeId(3));
    let request = ApiVersionsRequest {
        client_software_name: "krabka-test".into(),
        client_software_version: "1.0".into(),
        ..ApiVersionsRequest::default()
    };
    let correlation = cluster.send(&mut conn, 4, &request);
    // The broker accepts the connection at once but answers nothing while
    // it registers, catches up and waits to be unfenced, as Kafka binds its
    // socket server before it enables request processing: the answer leaves
    // in the step that makes the broker `RUNNING`.
    let deadline = cluster.world.now() + 5_000;
    let mut state_before = Value::Null;
    while cluster.inbox.is_empty() {
        state_before = cluster.snapshot(NodeId(3))["state"].clone();
        assert!(
            cluster.world.step_once(deadline),
            "no answer by {deadline} ms"
        );
        cluster.collect();
    }
    let snapshot = cluster.snapshot(NodeId(3));
    assert!(state_before == "RECOVERY" && snapshot["state"] == "RUNNING");
    assert!(snapshot["connections"] == 1);
    let answer = cluster.receive::<ApiVersionsRequest>(&conn, 4, correlation, 0);
    let expected = ApiVersionsResponse {
        api_keys: broker_api_versions_table(),
        supported_features: supported_features(4),
        finalized_features_epoch: -1,
        ..ApiVersionsResponse::default()
    };
    assert!(answer == Some(expected));
}

#[test]
fn a_topic_created_through_any_broker_is_striped_and_every_broker_agrees() {
    let mut cluster = Cluster::serving();
    // Broker 3 is not the controller: it forwards the request in an
    // `Envelope`.
    let topic_id = cluster.create_orders(NodeId(3));
    let striped = [
        partition_row(0, 1, 0, &[1, 2, 3], &[1, 2, 3], &[]),
        partition_row(1, 2, 0, &[2, 3, 1], &[2, 3, 1], &[]),
        partition_row(2, 3, 0, &[3, 1, 2], &[3, 1, 2], &[]),
    ];
    every_broker_agrees(&mut cluster, &[1, 2, 3], topic_id, &striped);
}

#[test]
fn a_killed_controller_hands_over_its_partitions_and_rejoins_as_a_follower() {
    let mut cluster = Cluster::serving();
    let topic_id = cluster.create_orders(NodeId(2));
    cluster.kill(NodeId(1));
    // Broker 2 times out on the dead leader first, but broker 3 has not
    // yet and refuses its pre-vote (KIP-996); broker 3 times out next and
    // wins with broker 2's vote.
    assert!(cluster.run_until(5_000, |c| c.active_controller() == Some(NodeId(3))));
    // Broker 1's session runs out on the new controller, which fences it,
    // elects a new leader from the ISR where it led, and takes it out of
    // every ISR.
    let moved = cluster.run_until(15_000, |c| {
        c.partition(NodeId(3), "orders", 0)["leader"] == 2
    });
    assert!(moved);
    let failed_over = [
        partition_row(0, 2, 1, &[1, 2, 3], &[2, 3], &[1]),
        partition_row(1, 2, 0, &[2, 3, 1], &[2, 3], &[1]),
        partition_row(2, 3, 0, &[3, 1, 2], &[3, 2], &[1]),
    ];
    every_broker_agrees(&mut cluster, &[2, 3], topic_id, &failed_over);
    let written = batch(&["while-1-was-down"], 10);
    assert!(cluster.produce(NodeId(2), "orders", 0, &written) == acked(0, 0));

    cluster.restart(NodeId(1));
    let expanded = cluster.run_until(10_000, |c| {
        c.partition(NodeId(2), "orders", 0)["isr"] == json!([2, 3, 1])
            && c.partition(NodeId(2), "orders", 1)["isr"] == json!([2, 3, 1])
            && c.partition(NodeId(3), "orders", 2)["isr"] == json!([3, 2, 1])
    });
    assert!(expanded);
    let rejoined = [
        partition_row(0, 2, 1, &[1, 2, 3], &[2, 3, 1], &[]),
        partition_row(1, 2, 0, &[2, 3, 1], &[2, 3, 1], &[]),
        partition_row(2, 3, 0, &[3, 1, 2], &[3, 2, 1], &[]),
    ];
    every_broker_agrees(&mut cluster, &[1, 2, 3], topic_id, &rejoined);
    // Broker 1 follows the new controller and caught up on the record it
    // missed.
    let snapshot = cluster.snapshot(NodeId(1));
    assert!(snapshot["quorum"]["role"] == "Follower" && snapshot["quorum"]["leader"] == 3);
    assert!(snapshot["controller_id"] == 3);
    let partition = cluster.partition(NodeId(1), "orders", 0);
    assert!(partition["log_end"] == 1 && partition["fetch_state"] == "fetching");
}

#[test]
fn acks_all_loses_no_acknowledged_record_across_leader_kills() {
    let mut cluster = Cluster::serving();
    // Broker 2 leads `ledger`; broker 1, the controller, stays up.
    let topic_id = cluster.create_topic(
        NodeId(1),
        CreatableTopic {
            name: "ledger".into(),
            num_partitions: -1,
            replication_factor: -1,
            assignments: vec![CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![2, 3, 1],
                ..CreatableReplicaAssignment::default()
            }],
            ..CreatableTopic::default()
        },
    );
    let batches = [
        batch(&["a", "b"], 100),
        batch(&["c"], 200),
        batch(&["never-acked"], 300),
        batch(&["d"], 400),
    ];
    assert!(cluster.produce(NodeId(2), "ledger", 0, &batches[0]) == acked(0, 0));
    assert!(cluster.produce(NodeId(2), "ledger", 0, &batches[1]) == acked(0, 2));
    // The leader appends a third batch and dies before any follower fetches
    // it, so nobody acknowledges it.
    let mut writer = cluster.connect(NodeId(2));
    cluster.send(&mut writer, 11, &produce_request("ledger", 0, &batches[2]));
    let appended = cluster.step_until(100, |c| c.partition(NodeId(2), "ledger", 0)["log_end"] == 4);
    assert!(appended);
    cluster.kill(NodeId(2));
    let moved = cluster.run_until(15_000, |c| {
        c.partition(NodeId(3), "ledger", 0)["leader"] == 3
    });
    assert!(moved);
    assert!(cluster.partition(NodeId(3), "ledger", 0)["isr"] == json!([3, 1]));
    assert!(cluster.produce(NodeId(3), "ledger", 0, &batches[3]) == acked(0, 3));

    // Back, broker 2 truncates the batch nobody acknowledged and catches up.
    cluster.restart(NodeId(2));
    let caught_up = cluster.run_until(10_000, |c| {
        c.partition(NodeId(3), "ledger", 0)["isr"] == json!([3, 1, 2])
    });
    assert!(caught_up);
    let truncated: Vec<Value> = cluster
        .world
        .events()
        .filter(|event| event.kind == "replica_truncated")
        .map(|event| event.detail.clone())
        .collect();
    assert!(
        truncated
            == vec![
                json!({ "topic": "ledger", "partition": 0, "from": 4, "to": 3, "level": "warn" })
            ]
    );
    let acknowledged = vec![
        stored(&batches[0], 0, 0),
        stored(&batches[1], 2, 0),
        stored(&batches[3], 3, 1),
    ];
    assert!(cluster.fetch_all(NodeId(3), topic_id, 0) == acknowledged);

    // Kill the second leader too: broker 2, first in replica order and back
    // in the ISR, leads again and serves the same records.
    cluster.kill(NodeId(3));
    let moved = cluster.run_until(15_000, |c| {
        c.partition(NodeId(2), "ledger", 0)["leader"] == 2
    });
    assert!(moved);
    assert!(cluster.fetch_all(NodeId(2), topic_id, 0) == acknowledged);
}

/// The request that commits `offsets` of `orders` for `group` as a client
/// outside group management: generation `-1` and no member id.
fn commit_request(group: &str, offsets: &[(i32, i64)]) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: group.to_string(),
        generation_id_or_member_epoch: -1,
        retention_time_ms: -1,
        topics: vec![OffsetCommitRequestTopic {
            name: "orders".into(),
            partitions: offsets
                .iter()
                .map(|(partition, offset)| OffsetCommitRequestPartition {
                    partition_index: *partition,
                    committed_offset: *offset,
                    committed_leader_epoch: -1,
                    committed_metadata: Some(String::new()),
                    ..OffsetCommitRequestPartition::default()
                })
                .collect(),
            ..OffsetCommitRequestTopic::default()
        }],
        ..OffsetCommitRequest::default()
    }
}

/// The `OffsetCommit` v9 answer that accepts every offset of `offsets`.
fn commit_accepted(offsets: &[(i32, i64)]) -> OffsetCommitResponse {
    OffsetCommitResponse {
        topics: vec![OffsetCommitResponseTopic {
            name: "orders".into(),
            partitions: offsets
                .iter()
                .map(|(partition, _)| OffsetCommitResponsePartition {
                    partition_index: *partition,
                    error_code: codes::NONE,
                    ..OffsetCommitResponsePartition::default()
                })
                .collect(),
            ..OffsetCommitResponseTopic::default()
        }],
        ..OffsetCommitResponse::default()
    }
}

/// The `OffsetFetch` v9 answer that lists `offsets` of `orders` for `group`.
fn fetched_offsets(group: &str, offsets: &[(i32, i64)]) -> OffsetFetchResponse {
    OffsetFetchResponse {
        groups: vec![OffsetFetchResponseGroup {
            group_id: group.to_string(),
            topics: vec![OffsetFetchResponseTopics {
                name: "orders".into(),
                partitions: offsets
                    .iter()
                    .map(|(partition, offset)| OffsetFetchResponsePartitions {
                        partition_index: *partition,
                        committed_offset: *offset,
                        committed_leader_epoch: -1,
                        metadata: Some(String::new()),
                        error_code: codes::NONE,
                        ..OffsetFetchResponsePartitions::default()
                    })
                    .collect(),
                ..OffsetFetchResponseTopics::default()
            }],
            error_code: codes::NONE,
            ..OffsetFetchResponseGroup::default()
        }],
        ..OffsetFetchResponse::default()
    }
}

#[test]
fn a_group_keeps_its_committed_offsets_when_its_coordinator_dies() {
    const GROUP: &str = "failover";
    let mut cluster = Cluster::serving();
    cluster.create_orders(NodeId(1));
    let coordinator = cluster.find_coordinator(NodeId(1), GROUP);
    let old = node(coordinator.node_id);
    // The coordinator leads the group's `__consumer_offsets` partition; the
    // first other replica in its replica order takes over.
    let offsets_partition = group_partition(GROUP, CONSUMER_OFFSETS_PARTITIONS);
    let metadata = cluster.metadata(old, &[CONSUMER_OFFSETS_TOPIC]);
    let replicas = metadata.topics[0].partitions[usize::try_from(offsets_partition).unwrap()]
        .replica_nodes
        .clone();
    assert!(replicas[0] == coordinator.node_id);
    let successor = replicas[1];

    let offsets = [(0, 5), (1, 7), (2, 0)];
    let mut consumer = cluster.connect(old);
    let committed = cluster.call(&mut consumer, 9, &commit_request(GROUP, &offsets));
    assert!(committed == commit_accepted(&offsets));
    assert!(cluster.committed_offsets(old, GROUP) == fetched_offsets(GROUP, &offsets));

    cluster.kill(old);
    let survivor = cluster.live()[0];
    let moved = cluster.run_until(15_000, |c| {
        c.partition(survivor, CONSUMER_OFFSETS_TOPIC, offsets_partition)["leader"] == successor
    });
    assert!(moved);
    let found = cluster.find_coordinator(survivor, GROUP);
    let expected = Coordinator {
        key: GROUP.into(),
        node_id: successor,
        host: format!("node-{successor}"),
        port: 9092,
        error_code: codes::NONE,
        ..Coordinator::default()
    };
    assert!(found == expected);
    let new = node(successor);
    assert!(cluster.committed_offsets(new, GROUP) == fetched_offsets(GROUP, &offsets));
}

fn join_request(group: &str, member_id: &str, metadata: &'static [u8]) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group.to_string(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 30_000,
        member_id: member_id.to_string(),
        protocol_type: "consumer".into(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: Bytes::from_static(metadata),
            ..JoinGroupRequestProtocol::default()
        }],
        ..JoinGroupRequest::default()
    }
}

fn sync_request(
    group: &str,
    generation_id: i32,
    member_id: &str,
    assignments: &[(&str, &'static [u8])],
) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id: group.to_string(),
        generation_id,
        member_id: member_id.to_string(),
        protocol_type: Some("consumer".into()),
        protocol_name: Some("range".into()),
        assignments: assignments
            .iter()
            .map(|(member, assignment)| SyncGroupRequestAssignment {
                member_id: (*member).to_string(),
                assignment: Bytes::from_static(assignment),
                ..SyncGroupRequestAssignment::default()
            })
            .collect(),
        ..SyncGroupRequest::default()
    }
}

/// A completed join: `member_id` in generation `generation_id` led by
/// `leader`, with the member list only the leader gets.
fn joined(
    generation_id: i32,
    leader: &str,
    member_id: &str,
    members: &[(&str, &'static [u8])],
) -> JoinGroupResponse {
    JoinGroupResponse {
        generation_id,
        protocol_type: Some("consumer".into()),
        protocol_name: Some("range".into()),
        leader: leader.to_string(),
        member_id: member_id.to_string(),
        members: members
            .iter()
            .map(|(member, metadata)| JoinGroupResponseMember {
                member_id: (*member).to_string(),
                group_instance_id: None,
                metadata: Bytes::from_static(metadata),
                ..JoinGroupResponseMember::default()
            })
            .collect(),
        ..JoinGroupResponse::default()
    }
}

fn synced(assignment: &'static [u8]) -> SyncGroupResponse {
    SyncGroupResponse {
        protocol_type: Some("consumer".into()),
        protocol_name: Some("range".into()),
        assignment: Bytes::from_static(assignment),
        ..SyncGroupResponse::default()
    }
}

/// Join `group` with an empty member id, and take the id the coordinator
/// requires.
fn member_id_for(
    cluster: &mut Cluster,
    conn: &mut Conn,
    group: &str,
    metadata: &'static [u8],
) -> String {
    let answer = cluster.call(conn, 9, &join_request(group, "", metadata));
    assert!(answer.member_id.starts_with("test-client-"));
    let expected = JoinGroupResponse {
        error_code: codes::MEMBER_ID_REQUIRED,
        member_id: answer.member_id.clone(),
        ..JoinGroupResponse::default()
    };
    assert!(answer == expected);
    answer.member_id
}

#[test]
fn a_classic_group_joins_syncs_and_rebalances_when_a_member_leaves() {
    const GROUP: &str = "classic";
    let mut cluster = Cluster::serving();
    let coordinator = cluster.find_coordinator(NodeId(2), GROUP);
    let at = node(coordinator.node_id);
    let (mut first, mut second) = (cluster.connect(at), cluster.connect(at));
    let a = member_id_for(&mut cluster, &mut first, GROUP, b"meta-a");
    let b = member_id_for(&mut cluster, &mut second, GROUP, b"meta-b");

    // Both join; the first to join leads, and the join completes once the
    // initial rebalance delay has passed.
    let join_a = cluster.send(&mut first, 9, &join_request(GROUP, &a, b"meta-a"));
    let join_b = cluster.send(&mut second, 9, &join_request(GROUP, &b, b"meta-b"));
    let joined_a = cluster.receive::<JoinGroupRequest>(&first, 9, join_a, 10_000);
    let joined_b = cluster.receive::<JoinGroupRequest>(&second, 9, join_b, 10_000);
    let members = [(a.as_str(), &b"meta-a"[..]), (b.as_str(), &b"meta-b"[..])];
    assert!(joined_a == Some(joined(1, &a, &a, &members)));
    assert!(joined_b == Some(joined(1, &a, &b, &[])));

    // The follower's sync waits for the leader's, which carries the
    // assignments.
    let sync_b = cluster.send(&mut second, 5, &sync_request(GROUP, 1, &b, &[]));
    let assignments = [(a.as_str(), &b"to-a"[..]), (b.as_str(), &b"to-b"[..])];
    let sync_a = cluster.send(&mut first, 5, &sync_request(GROUP, 1, &a, &assignments));
    let synced_a = cluster.receive::<SyncGroupRequest>(&first, 5, sync_a, ANSWER_WAIT_MS);
    let synced_b = cluster.receive::<SyncGroupRequest>(&second, 5, sync_b, ANSWER_WAIT_MS);
    assert!(synced_a == Some(synced(b"to-a")));
    assert!(synced_b == Some(synced(b"to-b")));

    // The second member leaves; the first learns of the rebalance from its
    // heartbeat and joins the next generation alone.
    let leave = LeaveGroupRequest {
        group_id: GROUP.into(),
        members: vec![MemberIdentity {
            member_id: b.clone(),
            ..MemberIdentity::default()
        }],
        ..LeaveGroupRequest::default()
    };
    let left = cluster.call(&mut second, 5, &leave);
    let expected = LeaveGroupResponse {
        members: vec![MemberResponse {
            member_id: b.clone(),
            // Kafka echoes the request's instance id.
            group_instance_id: None,
            ..MemberResponse::default()
        }],
        ..LeaveGroupResponse::default()
    };
    assert!(left == expected);
    let heartbeat = HeartbeatRequest {
        group_id: GROUP.into(),
        generation_id: 1,
        member_id: a.clone(),
        ..HeartbeatRequest::default()
    };
    let beat = cluster.call(&mut first, 4, &heartbeat);
    let rebalancing = HeartbeatResponse {
        error_code: codes::REBALANCE_IN_PROGRESS,
        ..HeartbeatResponse::default()
    };
    assert!(beat == rebalancing);
    let rejoined = cluster.call(&mut first, 9, &join_request(GROUP, &a, b"meta-a"));
    assert!(rejoined == joined(2, &a, &a, &[(a.as_str(), &b"meta-a"[..])]));
    let resynced = cluster.call(
        &mut first,
        5,
        &sync_request(GROUP, 2, &a, &[(a.as_str(), &b"all-to-a"[..])]),
    );
    assert!(resynced == synced(b"all-to-a"));
}

#[test]
fn a_kip_848_member_gets_an_assignment() {
    const GROUP: &str = "kip-848";
    let mut cluster = Cluster::serving();
    let topic_id = cluster.create_orders(NodeId(1));
    let coordinator = cluster.find_coordinator(NodeId(3), GROUP);
    let mut conn = cluster.connect(node(coordinator.node_id));
    let heartbeat = ConsumerGroupHeartbeatRequest {
        group_id: GROUP.into(),
        member_id: "kip-848-member".into(),
        member_epoch: 0,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: Some(vec!["orders".into()]),
        topic_partitions: Some(Vec::new()),
        ..ConsumerGroupHeartbeatRequest::default()
    };
    let answer = cluster.call(&mut conn, 1, &heartbeat);
    let expected = ConsumerGroupHeartbeatResponse {
        member_id: Some("kip-848-member".into()),
        // Kafka 4.3's `ModernGroup` starts at epoch 1, so the first join
        // takes the group, and its member, to epoch 2.
        member_epoch: 2,
        heartbeat_interval_ms: 5_000,
        assignment: Some(Assignment {
            topic_partitions: vec![TopicPartitions {
                topic_id,
                partitions: vec![0, 1, 2],
                ..TopicPartitions::default()
            }],
            ..Assignment::default()
        }),
        ..ConsumerGroupHeartbeatResponse::default()
    };
    assert!(answer == expected);
}

/// The `DescribeQuorum` request for the metadata partition.
fn describe_quorum_request() -> DescribeQuorumRequest {
    DescribeQuorumRequest {
        topics: vec![describe_quorum_request::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![describe_quorum_request::PartitionData {
                partition_index: 0,
                ..describe_quorum_request::PartitionData::default()
            }],
            ..describe_quorum_request::TopicData::default()
        }],
        ..DescribeQuorumRequest::default()
    }
}

/// The answer with every replica's fetch times zeroed, after checking each
/// is from the last second.
fn without_fetch_times(mut answer: DescribeQuorumResponse, now: Millis) -> DescribeQuorumResponse {
    let now = i64::try_from(now).unwrap();
    for partition in answer
        .topics
        .iter_mut()
        .flat_map(|t| t.partitions.iter_mut())
    {
        for replica in partition
            .current_voters
            .iter_mut()
            .chain(partition.observers.iter_mut())
        {
            let times = [
                replica.last_fetch_timestamp,
                replica.last_caught_up_timestamp,
            ];
            assert!(
                times.iter().all(|at| (now - 1_000..=now).contains(at)),
                "{replica:?} at {now}"
            );
            replica.last_fetch_timestamp = 0;
            replica.last_caught_up_timestamp = 0;
        }
    }
    answer
}

#[test]
fn describe_quorum_shows_the_voters_and_the_leader() {
    let mut cluster = Cluster::serving();
    cluster.create_orders(NodeId(1));
    cluster.run(1_000);
    let quorum = cluster.snapshot(NodeId(1))["quorum"].clone();
    let epoch = i32::try_from(quorum["epoch"].as_i64().unwrap()).unwrap();
    let high_watermark = quorum["hwm"].as_i64().unwrap();
    let described = DescribeQuorumResponse {
        topics: vec![describe_quorum_response::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![describe_quorum_response::PartitionData {
                partition_index: 0,
                leader_id: 1,
                leader_epoch: epoch,
                high_watermark,
                current_voters: (1..=3)
                    .map(|id| ReplicaState {
                        replica_id: id,
                        log_end_offset: high_watermark,
                        last_fetch_timestamp: 0,
                        last_caught_up_timestamp: 0,
                        ..ReplicaState::default()
                    })
                    .collect(),
                ..describe_quorum_response::PartitionData::default()
            }],
            ..describe_quorum_response::TopicData::default()
        }],
        nodes: (1..=3)
            .map(|id| describe_quorum_response::Node {
                node_id: id,
                listeners: vec![Listener {
                    name: "CONTROLLER".into(),
                    host: format!("node-{id}"),
                    port: RAFT_PORT,
                    ..Listener::default()
                }],
                ..describe_quorum_response::Node::default()
            })
            .collect(),
        ..DescribeQuorumResponse::default()
    };
    // A broker forwards the request to the active controller; the
    // controller listener of the quorum leader answers it itself (KIP-919).
    for listener in [
        Endpoint::kafka(NodeId(3)),
        Endpoint::new(NodeId(1), RAFT_PORT),
    ] {
        let mut conn = cluster.connect_to(listener);
        let answer = cluster.call(&mut conn, 2, &describe_quorum_request());
        let now = cluster.world.now();
        assert!(without_fetch_times(answer, now) == described, "{listener}");
    }
    // Any other controller is not the leader.
    let mut conn = cluster.connect_to(Endpoint::new(NodeId(2), RAFT_PORT));
    let answer = cluster.call(&mut conn, 2, &describe_quorum_request());
    let partition = &answer.topics[0].partitions[0];
    assert!(partition.error_code == codes::NOT_LEADER_OR_FOLLOWER);
}

#[test]
fn the_cluster_serves_the_same_records_and_offsets_after_a_reload() {
    const GROUP: &str = "reloaded";
    let mut cluster = Cluster::serving();
    let topic_id = cluster.create_orders(NodeId(1));
    let records = [
        batch(&["a", "b"], 100),
        batch(&["c"], 200),
        batch(&["d"], 300),
    ];
    // Partition `p` is led by broker `p + 1`.
    for (partition, leader) in [(0, NodeId(1)), (1, NodeId(2)), (2, NodeId(3))] {
        let index = usize::try_from(partition).unwrap();
        assert!(
            cluster.produce(leader, "orders", partition, &records[index]) == acked(partition, 0)
        );
    }
    let offsets = [(0, 2), (1, 1), (2, 1)];
    let coordinator = cluster.find_coordinator(NodeId(1), GROUP);
    let at = node(coordinator.node_id);
    let mut consumer = cluster.connect(at);
    let committed = cluster.call(&mut consumer, 9, &commit_request(GROUP, &offsets));
    assert!(committed == commit_accepted(&offsets));
    // The followers learn the high watermarks on their next fetches and
    // checkpoint them.
    cluster.run(1_000);

    let mut reloaded = Cluster::reloaded(cluster.durable_images());
    reloaded.wait_until_serving(30_000);
    let everywhere = reloaded.run_until(10_000, |c| {
        (0..3).all(|partition| {
            BROKERS.iter().all(|broker| {
                c.partition(*broker, "orders", partition)["isr"]
                    .as_array()
                    .map(Vec::len)
                    == Some(3)
            })
        })
    });
    assert!(everywhere);
    let metadata = reloaded.metadata(NodeId(1), &["orders"]);
    for (partition, written) in metadata.topics[0].partitions.iter().zip(&records) {
        let leader = node(partition.leader_id);
        let fetched = reloaded.fetch_all(leader, topic_id, partition.partition_index);
        assert!(
            fetched == vec![stored(written, 0, 0)],
            "partition {}",
            partition.partition_index
        );
    }
    let coordinator = reloaded.find_coordinator(NodeId(2), GROUP);
    let at = node(coordinator.node_id);
    assert!(reloaded.committed_offsets(at, GROUP) == fetched_offsets(GROUP, &offsets));
}
