//! The schema registry against simulated brokers: `_schemas` is a Kafka
//! topic, written with `acks=-1` and read back before a write is answered.
//!
//! A world hosts the brokers and the registry. The tests play two clients
//! from outside it: an HTTP client of the registry, and a Kafka client that
//! reads `_schemas` raw from the partition leader and asks the brokers for
//! metadata and configs. Their frames enter through `push_ingress` and the
//! answers leave through `drain_egress`.
//!
//! Brokers that run no `KRaft` controller each know only themselves. The
//! tests then play the controller's part where they need a cluster: they
//! register every broker with every other, copy the records of the topic
//! the registry created to the brokers that did not create it, and elect a
//! new leader when the old one dies. With brokers that run a controller,
//! the tests leave all of that to it.

use std::collections::{BTreeMap, BTreeSet};

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{MetadataRecord, TopicConfigRecord, TopicRecord};
use krabka_playground::lab::{
    Endpoint, Fault, NodeId, World,
    broker::{
        partition_record, registration_record,
        test_support::{TestClient, decode_response},
    },
    net::{ConnId, DurableOp, Frame, Millis, Payload},
    registry::http::{HttpRequest, HttpResponse},
    scenario::{NodeSpec, Scenario},
};
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResponse,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic},
    },
    records::RecordsPayload,
};
use serde_json::{Value, json};
use uuid::Uuid;

const REGISTRY: NodeId = NodeId(4);
const HTTP_CLIENT: NodeId = NodeId(99);
const KAFKA_CLIENT: u32 = 98;
const TOPIC: &str = "_schemas";

/// A record of `_schemas` as the leader stores it: offset, key, value.
type Stored = (i64, Option<Bytes>, Option<Bytes>);

fn av(name: &str) -> String {
    format!(
        "{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{{\"name\":\"{name}\",\"type\":\"int\",\"default\":0}}]}}"
    )
}

/// A stored record from its key and value texts.
fn stored(offset: i64, key: &str, value: Option<&str>) -> Stored {
    (
        offset,
        Some(Bytes::from(key.to_string())),
        value.map(|v| Bytes::from(v.to_string())),
    )
}

/// The noop Confluent's `KafkaStore` writes to learn the end of the topic.
fn noop(offset: i64) -> Stored {
    stored(offset, r#"{"keytype":"NOOP","magic":0}"#, None)
}

fn ok(body: &Value) -> HttpResponse {
    HttpResponse::ok(body)
}

/// The `SCHEMA` record of an Avro schema, as Confluent writes it.
fn schema_record(
    offset: i64,
    subject: &str,
    version: i32,
    id: i32,
    schema: &str,
    deleted: bool,
) -> Stored {
    let key =
        format!(r#"{{"keytype":"SCHEMA","subject":"{subject}","version":{version},"magic":1}}"#);
    let value = format!(
        r#"{{"subject":"{subject}","version":{version},"id":{id},"schema":{},"deleted":{deleted}}}"#,
        serde_json::to_string(schema).unwrap()
    );
    stored(offset, &key, Some(&value))
}

/// The reasons the store gave for the writes it failed, in order.
fn write_failures(lab: &Lab) -> Vec<String> {
    lab.world
        .events()
        .filter(|e| e.kind == "kafkastore" && e.detail["step"] == "write_failed")
        .map(|e| e.detail["message"].as_str().unwrap().to_string())
        .collect()
}

/// Confluent's `StoreTimeoutException` when no acknowledgement came.
const ACK_TIMEOUT: &str = "Put operation timed out while waiting for an ack from Kafka";

/// A world of brokers and a registry, and the test's two clients.
struct Lab {
    world: World,
    brokers: Vec<NodeId>,
    /// The brokers run no controller, so the test plays its part.
    relay: bool,
    kafka: BTreeMap<NodeId, TestClient>,
    next_http: u32,
    /// Frames that left the world for the test's clients, not read yet.
    inbox: Vec<Frame>,
}

impl Lab {
    /// `brokers` brokers, formed into a cluster, and then a registry with
    /// `config` added to it.
    fn new(brokers: u32, config: Value) -> Self {
        let lab = Self::brokers(brokers);
        lab.with_registry(config)
    }

    /// `count` brokers, formed into a cluster.
    fn brokers(count: u32) -> Self {
        let nodes: Vec<Value> = (1..=count)
            .map(|id| json!({ "id": id, "kind": "broker", "config": { "broker_id": id } }))
            .collect();
        let scenario: Scenario = serde_json::from_value(json!({
            "version": 1, "seed": 7, "links": { "default_latency_ms": 5 }, "nodes": nodes,
        }))
        .unwrap();
        let brokers: Vec<NodeId> = (1..=count).map(NodeId).collect();
        let mut hosted = brokers.clone();
        hosted.push(REGISTRY);
        let mut lab = Self {
            world: World::from_scenario_hosted(&scenario, &hosted).unwrap(),
            brokers,
            relay: false,
            kafka: BTreeMap::new(),
            next_http: 0,
            inbox: Vec::new(),
        };
        lab.run(5_000);
        let first = lab.brokers[0];
        let known = lab.metadata(first, None).brokers.len();
        lab.relay = known < lab.brokers.len();
        if lab.relay {
            let registrations: Vec<MetadataRecord> = lab
                .brokers
                .iter()
                .map(|b| {
                    MetadataRecord::V1BrokerRegistration(registration_record(
                        i32::try_from(b.0).unwrap(),
                        None,
                        Uuid::from_u128(u128::from(b.0)),
                    ))
                })
                .collect();
            for broker in lab.brokers.clone() {
                lab.apply_metadata(broker, &registrations);
            }
        }
        assert!(lab.metadata(first, None).brokers.len() == lab.brokers.len());
        lab
    }

    /// Add the registry with `config` (its `bootstrap` defaults to every
    /// broker) and run until it serves.
    fn with_registry(mut self, mut config: Value) -> Self {
        if config.get("bootstrap").is_none() {
            config["bootstrap"] = json!(self.brokers);
        }
        self.world
            .add_node(NodeSpec::new(
                REGISTRY.0,
                "schema-registry",
                "registry",
                config,
            ))
            .unwrap();
        self.await_ready();
        self
    }

    /// Run until the registry serves, relaying `_schemas` to every broker
    /// when the test plays the controller and the registry just created it.
    fn await_ready(&mut self) {
        if self.relay && self.topic_holder().is_none() {
            let created = self.run_until(|lab| lab.topic_holder().is_some(), 10_000);
            assert!(created, "{}", self.snapshot(REGISTRY));
            self.relay_topic();
        }
        let ready = self.run_until(|lab| lab.snapshot(REGISTRY)["state"] == "ready", 60_000);
        assert!(ready, "{}", self.snapshot(REGISTRY));
    }

    fn snapshot(&self, node: NodeId) -> Value {
        self.world.node_snapshot(node).unwrap()
    }

    fn run(&mut self, ms: Millis) {
        let until = self.world.now() + ms;
        self.world.step_until(until);
        self.collect();
    }

    fn collect(&mut self) {
        self.inbox
            .extend(self.world.drain_egress().into_iter().map(|t| t.frame));
    }

    /// Step until `pred` holds or `max_ms` passed; whether it held.
    fn run_until(&mut self, mut pred: impl FnMut(&mut Self) -> bool, max_ms: Millis) -> bool {
        let deadline = self.world.now() + max_ms;
        loop {
            self.collect();
            if pred(self) {
                return true;
            }
            if !self.world.step_once(deadline) {
                self.collect();
                return pred(self);
            }
        }
    }

    // ---- the Kafka client ---------------------------------------------------------

    /// Send `request` to `broker` and wait for its answer.
    fn kafka_call<R: ProtocolRequest>(
        &mut self,
        broker: NodeId,
        version: i16,
        request: &R,
    ) -> R::Response {
        if !self.kafka.contains_key(&broker) {
            let client = TestClient::new(KAFKA_CLIENT, broker.0);
            self.world.push_ingress(vec![client.open(broker)]);
            self.kafka.insert(broker, client);
        }
        let client = self.kafka.get_mut(&broker).unwrap();
        let frame = client.request(broker, version, request);
        let (me, conn) = (client.endpoint(), client.conn);
        self.world.push_ingress(vec![frame]);
        let is_answer =
            move |f: &Frame| f.dst == me && f.conn == conn && f.payload.data().is_some();
        let answered = self.run_until(|lab| lab.inbox.iter().any(is_answer), 5_000);
        assert!(answered, "{broker} did not answer");
        let at = self.inbox.iter().position(is_answer).unwrap();
        let frame = self.inbox.remove(at);
        decode_response::<R>(&frame, version).unwrap().1
    }

    /// `Metadata` v12 from `broker`, for `topic` or for every topic.
    fn metadata(&mut self, broker: NodeId, topic: Option<&str>) -> MetadataResponse {
        let request = MetadataRequest {
            topics: topic.map(|t| {
                vec![MetadataRequestTopic {
                    name: Some(t.to_string()),
                    ..MetadataRequestTopic::default()
                }]
            }),
            allow_auto_topic_creation: false,
            ..MetadataRequest::default()
        };
        self.kafka_call(broker, 12, &request)
    }

    /// The `_schemas` topic and its partition as `broker` describes them.
    fn schemas_partition(
        &mut self,
        broker: NodeId,
    ) -> (MetadataResponseTopic, MetadataResponsePartition) {
        let response = self.metadata(broker, Some(TOPIC));
        let topic = response.topics[0].clone();
        let partition = topic.partitions[0].clone();
        (topic, partition)
    }

    /// Every record of `_schemas`, fetched raw from the partition leader.
    fn schemas_records(&mut self) -> Vec<Stored> {
        let any = self.alive_broker();
        let (topic, partition) = self.schemas_partition(any);
        let leader = NodeId(u32::try_from(partition.leader_id).unwrap());
        let request = FetchRequest {
            max_wait_ms: 0,
            min_bytes: 1,
            topics: vec![FetchTopic {
                topic: TOPIC.to_string(),
                topic_id: topic.topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        };
        let response: FetchResponse = self.kafka_call(leader, 13, &request);
        let row = &response.responses[0].partitions[0];
        assert!(row.error_code == 0, "{row:?}");
        let Some(RecordsPayload::V2(batches)) = &row.records else {
            return Vec::new();
        };
        batches
            .iter()
            .flat_map(|b| {
                b.records.iter().map(move |r| {
                    (
                        b.base_offset + i64::from(r.offset_delta),
                        r.key.clone(),
                        r.value.clone(),
                    )
                })
            })
            .collect()
    }

    /// The leader of `_schemas` as a live broker names it.
    fn leader(&mut self) -> NodeId {
        let any = self.alive_broker();
        let (_, partition) = self.schemas_partition(any);
        NodeId(u32::try_from(partition.leader_id).unwrap())
    }

    fn alive_broker(&self) -> NodeId {
        let snapshot = self.world.snapshot();
        *self
            .brokers
            .iter()
            .find(|b| snapshot.nodes.iter().any(|n| n.id == **b && n.alive))
            .unwrap()
    }

    // ---- the controller's part ----------------------------------------------------

    fn apply_metadata(&mut self, broker: NodeId, records: &[MetadataRecord]) {
        self.world
            .control(
                broker,
                json!({ "cmd": "apply_metadata", "records": records }),
            )
            .unwrap();
    }

    /// The broker whose image holds `_schemas`, when one does.
    fn topic_holder(&self) -> Option<NodeId> {
        self.brokers.iter().copied().find(|b| {
            self.snapshot(*b)["topics"]
                .as_array()
                .is_some_and(|ts| ts.iter().any(|t| t["name"] == TOPIC))
        })
    }

    /// Copy the records of `_schemas` from the broker that created it to
    /// the others, as the controller's commit would reach them.
    fn relay_topic(&mut self) {
        let holder = self.topic_holder().unwrap();
        let (topic, partition) = self.schemas_partition(holder);
        let replicas = partition.replica_nodes.clone();
        let records = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: TOPIC.to_string(),
                topic_id: Uuid::from_bytes(topic.topic_id.0),
                partitions: 1,
                replication_factor: i16::try_from(replicas.len()).unwrap(),
            }),
            partition_record(
                TOPIC,
                0,
                &replicas,
                &partition.isr_nodes,
                partition.leader_epoch,
            ),
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.to_string(),
                overrides: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            }),
        ];
        for broker in self.brokers.clone() {
            if broker != holder {
                self.apply_metadata(broker, &records);
            }
        }
    }

    /// The controller's answer to the death of `dead`: fence it, and move
    /// the leadership of `_schemas` to the first live member of the ISR,
    /// with the next leader epoch.
    fn elect_without(&mut self, dead: NodeId) {
        let survivors: Vec<NodeId> = self
            .brokers
            .iter()
            .copied()
            .filter(|b| *b != dead)
            .collect();
        let (_, partition) = self.schemas_partition(survivors[0]);
        let dead_id = i32::try_from(dead.0).unwrap();
        let isr: Vec<i32> = partition
            .replica_nodes
            .iter()
            .copied()
            .filter(|r| *r != dead_id && partition.isr_nodes.contains(r))
            .collect();
        let mut fenced = registration_record(dead_id, None, Uuid::from_u128(u128::from(dead.0)));
        fenced.fenced = true;
        let records = vec![
            MetadataRecord::V1BrokerRegistration(fenced),
            partition_record(
                TOPIC,
                0,
                &partition.replica_nodes,
                &isr,
                partition.leader_epoch + 1,
            ),
        ];
        for broker in survivors {
            self.apply_metadata(broker, &records);
        }
    }

    // ---- the HTTP client ----------------------------------------------------------

    fn http_open(&mut self) -> ConnId {
        self.next_http += 1;
        let conn = ConnId(self.next_http);
        self.world.push_ingress(vec![Frame::open(
            Endpoint::client(HTTP_CLIENT),
            Endpoint::http(REGISTRY),
            conn,
        )]);
        conn
    }

    fn http_send(&mut self, conn: ConnId, request: &HttpRequest) {
        self.world.push_ingress(vec![Frame::data(
            Endpoint::client(HTTP_CLIENT),
            Endpoint::http(REGISTRY),
            conn,
            request.encode(),
        )]);
    }

    /// The payloads the registry sent on `conn`, taken out of the inbox.
    fn http_replies(&mut self, conn: ConnId) -> Vec<Payload> {
        let (mine, rest): (Vec<Frame>, Vec<Frame>) = std::mem::take(&mut self.inbox)
            .into_iter()
            .partition(|f| f.dst == Endpoint::client(HTTP_CLIENT) && f.conn == conn);
        self.inbox = rest;
        mine.into_iter().map(|f| f.payload).collect()
    }

    fn has_http_reply(&self, conn: ConnId) -> bool {
        self.inbox
            .iter()
            .any(|f| f.dst == Endpoint::client(HTTP_CLIENT) && f.conn == conn)
    }

    /// Send one request on a new connection, with `Connection: close`, and
    /// wait for the answer.
    fn call(&mut self, method: &str, path: &str, body: Option<Value>) -> HttpResponse {
        let conn = self.http_open();
        let mut request = request(method, path, body);
        request.close = true;
        self.http_send(conn, &request);
        let answered = self.run_until(|lab| lab.has_http_reply(conn), 60_000);
        assert!(answered, "{method} {path} was not answered");
        let replies = self.http_replies(conn);
        assert!(
            replies.len() == 2 && replies[1] == Payload::Close,
            "{replies:?}"
        );
        response(&replies[0])
    }

    fn register(&mut self, subject: &str, schema: &str) -> HttpResponse {
        self.call(
            "POST",
            &format!("/subjects/{subject}/versions"),
            Some(json!({ "schema": schema })),
        )
    }
}

fn request(method: &str, path: &str, body: Option<Value>) -> HttpRequest {
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let mut request = HttpRequest::new(method, path);
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap();
        request = request.with_query(k, v);
    }
    match body {
        Some(body) => request.with_json(&body),
        None => request,
    }
}

fn response(payload: &Payload) -> HttpResponse {
    let Payload::Data(bytes) = payload else {
        panic!("expected a response, got {payload:?}");
    };
    HttpResponse::parse(bytes).unwrap().0
}

#[test]
fn a_registry_creates_a_compacted_schemas_topic_on_three_replicas() {
    let mut lab = Lab::new(3, json!({}));
    let (topic, partition) = lab.schemas_partition(NodeId(1));
    let replicas: BTreeSet<i32> = partition.replica_nodes.iter().copied().collect();
    assert!(replicas == BTreeSet::from([1, 2, 3]));
    assert!(
        topic
            == MetadataResponseTopic {
                error_code: 0,
                name: Some(TOPIC.to_string()),
                is_internal: false,
                partitions: vec![MetadataResponsePartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: partition.replica_nodes[0],
                    leader_epoch: 0,
                    replica_nodes: partition.replica_nodes.clone(),
                    isr_nodes: partition.replica_nodes.clone(),
                    offline_replicas: Vec::new(),
                    ..MetadataResponsePartition::default()
                }],
                ..topic.clone()
            }
    );
    let configs: DescribeConfigsResponse = lab.kafka_call(
        NodeId(2),
        4,
        &DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: 2,
                resource_name: TOPIC.to_string(),
                configuration_keys: Some(vec!["cleanup.policy".to_string()]),
                ..DescribeConfigsResource::default()
            }],
            ..DescribeConfigsRequest::default()
        },
    );
    let policy: Vec<(String, Option<String>, i8)> = configs.results[0]
        .configs
        .iter()
        .map(|c| (c.name.clone(), c.value.clone(), c.config_source))
        .collect();
    // Source 1 is a dynamic topic config: the topic's own override.
    assert!(policy == vec![("cleanup.policy".to_string(), Some("compact".to_string()), 1)]);
    let store = lab.snapshot(REGISTRY)["store"].clone();
    assert!(
        store["topic"]
            == json!({
                "name": TOPIC,
                "partitions": 1,
                "replication_factor": 3,
                "cleanup_policy": "compact",
                "created": true,
            })
    );
    // Confluent's startup wrote two noops: one to learn the end of the
    // topic, one when the lone instance became the primary.
    assert!(lab.schemas_records() == vec![noop(0), noop(1)]);
    assert!(
        lab.world
            .events()
            .filter(|e| e.kind == "kafkastore")
            .all(|e| e.detail["level"] != "warn")
    );
}

#[test]
fn writes_land_on_schemas_as_confluent_records_in_order() {
    let mut lab = Lab::new(3, json!({}));
    let subject = "orders-value";
    assert!(lab.register(subject, &av("A")) == ok(&json!({ "id": 1 })));
    assert!(lab.register(subject, &av("B")) == ok(&json!({ "id": 2 })));
    // The same schema again is looked up, not written.
    assert!(lab.register(subject, &av("A")) == ok(&json!({ "id": 1 })));
    let lookup = lab.call(
        "POST",
        &format!("/subjects/{subject}"),
        Some(json!({ "schema": av("B") })),
    );
    assert!(lookup == ok(&json!({ "subject": subject, "version": 2, "id": 2, "schema": av("B") })));
    assert!(lab.call("GET", "/subjects", None) == ok(&json!([subject])));
    let versions = format!("/subjects/{subject}/versions");
    assert!(lab.call("GET", &versions, None) == ok(&json!([1, 2])));
    let config = format!("/config/{subject}");
    assert!(
        lab.call("PUT", &config, Some(json!({ "compatibility": "NONE" })))
            == ok(&json!({ "compatibility": "NONE" }))
    );
    assert!(lab.call("GET", &config, None) == ok(&json!({ "compatibilityLevel": "NONE" })));
    let first = format!("/subjects/{subject}/versions/1");
    assert!(lab.call("DELETE", &first, None) == ok(&json!(1)));
    assert!(lab.call("DELETE", &format!("{first}?permanent=true"), None) == ok(&json!(1)));
    assert!(lab.call("GET", &versions, None) == ok(&json!([2])));
    let whole = format!("/subjects/{subject}");
    assert!(lab.call("DELETE", &whole, None) == ok(&json!([2])));
    assert!(lab.call("DELETE", &format!("{whole}?permanent=true"), None) == ok(&json!([2])));
    assert!(lab.call("GET", "/subjects?deleted=true", None) == ok(&json!([])));

    let high_water = |offset: i64| {
        stored(
            offset,
            r#"{"keytype":"NOOP","subject":"orders-value","magic":0}"#,
            Some(r#"{"nextVersion":3}"#),
        )
    };
    let tombstone = |offset: i64, version: i32| {
        stored(
            offset,
            &format!(
                r#"{{"keytype":"SCHEMA","subject":"orders-value","version":{version},"magic":1}}"#
            ),
            None,
        )
    };
    let config_key = r#"{"keytype":"CONFIG","subject":"orders-value","magic":0}"#;
    assert!(
        lab.schemas_records()
            == vec![
                noop(0),
                noop(1),
                schema_record(2, subject, 1, 1, &av("A"), false),
                schema_record(3, subject, 2, 2, &av("B"), false),
                stored(4, config_key, Some(r#"{"compatibilityLevel":"NONE"}"#)),
                schema_record(5, subject, 1, 1, &av("A"), true),
                high_water(6),
                tombstone(7, 1),
                stored(
                    8,
                    r#"{"keytype":"DELETE_SUBJECT","subject":"orders-value","magic":0}"#,
                    Some(r#"{"subject":"orders-value","version":2}"#),
                ),
                high_water(9),
                tombstone(10, 2),
                stored(11, config_key, None),
            ]
    );
    let snapshot = lab.snapshot(REGISTRY);
    assert!(snapshot["records"] == 12);
    assert!(snapshot["applied"] == 12);
    assert!(snapshot["store"]["reader"]["offset"] == 11);
    assert!(snapshot["store"]["last_written_offset"] == 11);
    assert!(snapshot["writes"] == json!({ "queued": 0, "active": null }));
}

#[test]
fn a_write_is_answered_only_once_its_record_is_read_back() {
    let mut lab = Lab::new(3, json!({}));
    let conn = lab.http_open();
    lab.http_send(
        conn,
        &request(
            "POST",
            "/subjects/s/versions",
            Some(json!({ "schema": av("A") })),
        ),
    );
    // The record lands at offset 2, behind the startup's two noops. At every
    // step until the answer, the answer is out only if the reader has the
    // record and the state has the schema.
    let mut steps = 0;
    let answered = lab.run_until(
        |lab| {
            steps += 1;
            let snapshot = lab.snapshot(REGISTRY);
            let read_back = snapshot["store"]["reader"]["offset"].as_i64() >= Some(2);
            let applied = snapshot["subjects"]
                == json!([{
                    "subject": "s",
                    "versions": [{ "version": 1, "id": 1, "deleted": false }],
                    "compatibility": null,
                    "mode": null,
                }]);
            let replied = lab.has_http_reply(conn);
            assert!(!replied || (read_back && applied), "{snapshot}");
            replied
        },
        5_000,
    );
    assert!(answered);
    assert!(steps > 1);
    assert!(lab.http_replies(conn) == vec![Payload::Data(ok(&json!({ "id": 1 })).encode())]);
}

#[test]
fn a_write_the_reader_does_not_read_back_in_time_times_out_after_its_ack() {
    let mut lab = Lab::new(3, json!({}));
    let leader = lab.leader();
    let fetches = |lab: &Lab| lab.snapshot(REGISTRY)["store"]["reader"]["fetches"].as_u64();
    // The reader sends a fresh fetch, which waits at the leader for 500 ms.
    // Its empty answer then crosses a link that takes a second.
    let before = fetches(&lab);
    assert!(lab.run_until(|lab| fetches(lab) > before, 1_000));
    lab.world.fault(Fault::Latency {
        a: REGISTRY,
        b: leader,
        ms: 1_000,
    });
    lab.run(600);
    lab.world.fault(Fault::Latency {
        a: REGISTRY,
        b: leader,
        ms: 5,
    });
    // The record is acknowledged at once, but the reader waits for its slow
    // answer and cannot read the record back within `kafkastore.timeout.ms`.
    let answer = lab.register("s", &av("A"));
    assert!(answer == HttpResponse::error(500, 50002, "Register operation timed out"));
    assert!(lab.snapshot(REGISTRY)["store"]["producer"]["acked"] == 3);
    assert!(
        write_failures(&lab)
            == vec![
                "KafkaStoreReaderThread failed to reach target offset within the timeout interval. targetOffset: 2, offsetReached: 1, timeout(ms): 500"
                    .to_string()
            ]
    );
    // The record is on the topic all the same: once the reader reads it,
    // the schema is registered.
    assert!(lab.run_until(|lab| lab.snapshot(REGISTRY)["schemas"] == 1, 5_000));
    assert!(lab.register("s", &av("A")) == ok(&json!({ "id": 1 })));
}

#[test]
fn with_the_leaders_link_cut_a_registration_times_out_and_lands_after_the_heal() {
    let mut lab = Lab::new(3, json!({}));
    assert!(lab.register("s", &av("A")) == ok(&json!({ "id": 1 })));
    let leader = lab.leader();
    lab.world.fault(Fault::Partition {
        a: REGISTRY,
        b: leader,
    });
    let sent = lab.world.now();
    let answer = lab.register("s", &av("B"));
    assert!(answer == HttpResponse::error(500, 50002, "Register operation timed out"));
    // `ack.get(kafkastore.timeout.ms)`: the answer came 500 ms after the
    // request.
    assert!(lab.world.now() - sent == 500);
    assert!(write_failures(&lab) == vec![ACK_TIMEOUT.to_string()]);
    assert!(lab.snapshot(REGISTRY)["store"]["last_written_offset"] == Value::Null);
    // The producer keeps the record and retries it: once the link heals it
    // lands, and the reader applies it.
    lab.world.fault(Fault::Heal {
        a: REGISTRY,
        b: leader,
    });
    let landed = lab.run_until(|lab| lab.snapshot(REGISTRY)["schemas"] == 2, 60_000);
    assert!(landed, "{}", lab.snapshot(REGISTRY));
    assert!(lab.register("s", &av("B")) == ok(&json!({ "id": 2 })));
    // The next write first learns the end of the topic with a noop.
    assert!(lab.register("s", &av("C")) == ok(&json!({ "id": 3 })));
    assert!(
        lab.schemas_records()
            == vec![
                noop(0),
                noop(1),
                schema_record(2, "s", 1, 1, &av("A"), false),
                schema_record(3, "s", 2, 2, &av("B"), false),
                noop(4),
                schema_record(5, "s", 3, 3, &av("C"), false),
            ]
    );
}

#[test]
fn a_wiped_or_restarted_registry_replays_every_schema_from_the_brokers() {
    let wipe = [Fault::Wipe { node: REGISTRY }];
    let restart = [
        Fault::Kill { node: REGISTRY },
        Fault::Restart { node: REGISTRY },
    ];
    // The faults, the starts the registry counts after them, and what the
    // host's store saw of the registry: a wipe clears it, and the registry
    // itself never writes to it.
    let rows: [(&[Fault], u64, Vec<DurableOp>); 2] = [
        (&wipe, 1, vec![DurableOp::ClearAll]),
        (&restart, 2, Vec::new()),
    ];
    for (faults, started, durable) in rows {
        let mut lab = Lab::new(3, json!({}));
        assert!(lab.register("a", &av("A1")) == ok(&json!({ "id": 1 })));
        assert!(lab.register("a", &av("A2")) == ok(&json!({ "id": 2 })));
        assert!(lab.register("b", &av("A1")) == ok(&json!({ "id": 1 })));
        assert!(lab.register("b", &av("B2")) == ok(&json!({ "id": 3 })));
        assert!(
            lab.call("PUT", "/config/a", Some(json!({ "compatibility": "FULL" })))
                == ok(&json!({ "compatibility": "FULL" }))
        );
        assert!(lab.call("DELETE", "/subjects/b/versions/1", None) == ok(&json!(1)));
        let reads = [
            "/schemas?deleted=true",
            "/subjects?deleted=true",
            "/subjects/a/versions",
            "/subjects/b/versions?deleted=true",
            "/config/a",
            "/schemas/ids/3",
        ];
        let before: Vec<HttpResponse> = reads.iter().map(|p| lab.call("GET", p, None)).collect();
        assert!(before.iter().all(|r| r.status == 200));
        lab.world.drain_durable();

        for fault in faults {
            lab.world.fault(*fault);
        }
        // Loading: a new connection is refused at once.
        let conn = lab.http_open();
        assert!(lab.run_until(|lab| lab.has_http_reply(conn), 1_000));
        assert!(lab.http_replies(conn) == vec![Payload::Close]);
        assert!(lab.snapshot(REGISTRY)["state"] == "loading");
        lab.await_ready();
        let after: Vec<HttpResponse> = reads.iter().map(|p| lab.call("GET", p, None)).collect();
        assert!(after == before, "{faults:?}");
        // The replay read every record, and the startup's two new noops.
        let snapshot = lab.snapshot(REGISTRY);
        assert!(snapshot["records"] == 10);
        assert!(snapshot["refused"] == 1);
        assert!(snapshot["started"] == started);
        let ops: Vec<DurableOp> = lab
            .world
            .drain_durable()
            .into_iter()
            .filter(|(node, _)| *node == REGISTRY)
            .map(|(_, op)| op)
            .collect();
        assert!(ops == durable, "{faults:?}");
    }
}

#[test]
fn killing_the_leader_before_a_registration_loses_and_duplicates_nothing() {
    let mut lab = Lab::new(3, json!({}));
    assert!(lab.register("s", &av("A")) == ok(&json!({ "id": 1 })));
    let leader = lab.leader();
    lab.world.fault(Fault::Kill { node: leader });
    // No leader answers within `kafkastore.timeout.ms`.
    assert!(
        lab.register("s", &av("B"))
            == HttpResponse::error(500, 50002, "Register operation timed out")
    );
    assert!(write_failures(&lab) == vec![ACK_TIMEOUT.to_string()]);
    if lab.relay {
        lab.run(1_000);
        lab.elect_without(leader);
    }
    // The producer retries the record until the new leader takes it.
    let landed = lab.run_until(|lab| lab.snapshot(REGISTRY)["schemas"] == 2, 60_000);
    assert!(landed, "{}", lab.snapshot(REGISTRY));
    assert!(lab.leader() != leader);
    assert!(lab.register("s", &av("B")) == ok(&json!({ "id": 2 })));
    assert!(lab.register("s", &av("C")) == ok(&json!({ "id": 3 })));
    assert!(lab.call("GET", "/subjects/s/versions", None) == ok(&json!([1, 2, 3])));
    assert!(
        lab.schemas_records()
            == vec![
                noop(0),
                noop(1),
                schema_record(2, "s", 1, 1, &av("A"), false),
                schema_record(3, "s", 2, 2, &av("B"), false),
                noop(4),
                schema_record(5, "s", 3, 3, &av("C"), false),
            ]
    );
}

#[test]
fn killing_the_leader_before_it_acknowledges_a_committed_record_duplicates_nothing() {
    let mut lab = Lab::new(3, json!({}));
    let leader = lab.leader();
    let conn = lab.http_open();
    lab.http_send(
        conn,
        &request(
            "POST",
            "/subjects/s/versions",
            Some(json!({ "schema": av("A") })),
        ),
    );
    // Kill the leader the moment the record is committed: every replica has
    // it, and the acknowledgement is still on the wire.
    let committed = lab.run_until(
        |lab| {
            lab.snapshot(leader)["topics"]
                .as_array()
                .and_then(|ts| ts.iter().find(|t| t["name"] == TOPIC))
                .is_some_and(|t| t["partitions"][0]["hwm"] == 3)
        },
        5_000,
    );
    assert!(committed);
    lab.world.fault(Fault::Kill { node: leader });
    if lab.relay {
        lab.run(1_000);
        lab.elect_without(leader);
    }
    assert!(lab.run_until(|lab| lab.has_http_reply(conn), 60_000));
    let answer = response(&lab.http_replies(conn)[0]);
    assert!(answer == HttpResponse::error(500, 50002, "Register operation timed out"));
    assert!(write_failures(&lab) == vec![ACK_TIMEOUT.to_string()]);
    // The producer's retry is a duplicate the new leader recognises: the
    // record is there once.
    let landed = lab.run_until(|lab| lab.snapshot(REGISTRY)["schemas"] == 1, 60_000);
    assert!(landed, "{}", lab.snapshot(REGISTRY));
    assert!(lab.register("s", &av("A")) == ok(&json!({ "id": 1 })));
    assert!(lab.register("s", &av("B")) == ok(&json!({ "id": 2 })));
    assert!(
        lab.schemas_records()
            == vec![
                noop(0),
                noop(1),
                schema_record(2, "s", 1, 1, &av("A"), false),
                noop(3),
                schema_record(4, "s", 2, 2, &av("B"), false),
            ]
    );
    let producer = lab.snapshot(REGISTRY)["store"]["producer"].clone();
    assert!(producer["failed"] == 0);
    assert!(producer["retried"].as_u64() >= Some(1));
}

#[test]
fn a_connection_is_answered_in_order_and_a_write_holds_the_requests_behind_it() {
    let mut lab = Lab::new(1, json!({}));
    assert!(lab.register("s", &av("A")) == ok(&json!({ "id": 1 })));
    // One frame pipelines a registration and a read of what it registers.
    let conn = lab.http_open();
    let mut bytes = request(
        "POST",
        "/subjects/s/versions",
        Some(json!({ "schema": av("B") })),
    )
    .encode()
    .to_vec();
    bytes.extend_from_slice(&request("GET", "/subjects/s/versions", None).encode());
    lab.world.push_ingress(vec![Frame::data(
        Endpoint::client(HTTP_CLIENT),
        Endpoint::http(REGISTRY),
        conn,
        Bytes::from(bytes),
    )]);
    // Meanwhile another connection re-registers a schema the subject has:
    // no write, so no wait behind the first.
    let other = lab.http_open();
    lab.http_send(
        other,
        &request(
            "POST",
            "/subjects/s/versions",
            Some(json!({ "schema": av("A") })),
        ),
    );
    assert!(lab.run_until(|lab| lab.has_http_reply(other), 1_000));
    assert!(!lab.has_http_reply(conn));
    assert!(lab.http_replies(other) == vec![Payload::Data(ok(&json!({ "id": 1 })).encode())]);
    let both = lab.run_until(
        |lab| lab.inbox.iter().filter(|f| f.conn == conn).count() == 2,
        5_000,
    );
    assert!(both);
    let replies: Vec<HttpResponse> = lab.http_replies(conn).iter().map(response).collect();
    assert!(replies == vec![ok(&json!({ "id": 2 })), ok(&json!([1, 2]))]);
    // A request split over two frames is served once it is whole.
    let whole = request("GET", "/subjects", None).encode();
    let (head, tail) = whole.split_at(whole.len() - 5);
    for part in [head, tail] {
        lab.world.push_ingress(vec![Frame::data(
            Endpoint::client(HTTP_CLIENT),
            Endpoint::http(REGISTRY),
            conn,
            Bytes::copy_from_slice(part),
        )]);
        lab.run(20);
    }
    let replies: Vec<HttpResponse> = lab.http_replies(conn).iter().map(response).collect();
    assert!(replies == vec![ok(&json!(["s"]))]);
    // Garbage gets a 400 and the connection closes.
    lab.world.push_ingress(vec![Frame::data(
        Endpoint::client(HTTP_CLIENT),
        Endpoint::http(REGISTRY),
        conn,
        Bytes::from_static(b"\x00\x01 nope\r\n\r\n"),
    )]);
    assert!(lab.run_until(
        |lab| lab.inbox.iter().filter(|f| f.conn == conn).count() == 2,
        1_000
    ));
    let replies = lab.http_replies(conn);
    assert!(response(&replies[0]).status == 400);
    assert!(replies[1] == Payload::Close);
    assert!(lab.snapshot(REGISTRY)["connections"] == 1);
}

#[test]
fn http_commands_answer_reads_and_queue_writes() {
    let mut lab = Lab::new(1, json!({}));
    let read = lab
        .world
        .control(REGISTRY, json!({ "cmd": "http", "path": "/subjects" }))
        .unwrap();
    assert!(read == json!({ "status": 200, "body": [] }));
    let queued = lab
        .world
        .control(
            REGISTRY,
            json!({ "cmd": "http", "method": "post", "path": "/subjects/s/versions", "body": { "schema": av("A") } }),
        )
        .unwrap();
    assert!(queued == json!({ "queued": 2 }));
    let done = lab.run_until(|lab| lab.snapshot(REGISTRY)["schemas"] == 1, 5_000);
    assert!(done);
    let answers: Vec<Value> = lab
        .world
        .events()
        .filter(|e| e.kind == "registry")
        .map(|e| e.detail.clone())
        .collect();
    assert!(
        answers
            == vec![json!({
                "method": "POST",
                "path": "/subjects/s/versions",
                "status": 200,
                "request": 2,
                "result": { "id": 1 },
            })]
    );
    // A registration the subject has is answered at once.
    let again = lab
        .world
        .control(
            REGISTRY,
            json!({ "cmd": "http", "method": "POST", "path": "/subjects/s/versions", "body": { "schema": av("A") } }),
        )
        .unwrap();
    assert!(again == json!({ "status": 200, "body": { "id": 1 } }));
}

#[test]
fn a_lone_broker_gets_the_schemas_topic_on_one_replica_with_confluents_warning() {
    let mut lab = Lab::new(1, json!({}));
    let warnings: Vec<Value> = lab
        .world
        .events()
        .filter(|e| e.kind == "kafkastore" && e.detail["level"] == "warn")
        .map(|e| e.detail.clone())
        .collect();
    assert!(
        warnings
            == vec![json!({
                "step": "create_topic",
                "level": "warn",
                "message": "Creating the schema topic _schemas using a replication factor of 1, which is less than the desired one of 3. If this is a production environment, it's crucial to add more brokers and increase the replication factor of the topic.",
            })]
    );
    let (_, partition) = lab.schemas_partition(NodeId(1));
    assert!(partition.replica_nodes == vec![1]);
}

#[test]
fn a_schemas_topic_unfit_for_the_store_fails_the_startup() {
    use krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };
    for (partitions, policy, error) in [
        (
            3,
            "compact",
            "The schema topic _schemas should have only 1 partition but has 3",
        ),
        (
            1,
            "delete",
            "The retention policy of the schema topic _schemas is incorrect. Expected cleanup.policy to be 'compact' but it is delete",
        ),
    ] {
        let mut lab = Lab::brokers(1);
        let created: CreateTopicsResponse = lab.kafka_call(
            NodeId(1),
            7,
            &CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: TOPIC.to_string(),
                    num_partitions: partitions,
                    replication_factor: 1,
                    configs: vec![CreatableTopicConfig {
                        name: "cleanup.policy".to_string(),
                        value: Some(policy.to_string()),
                        ..CreatableTopicConfig::default()
                    }],
                    ..CreatableTopic::default()
                }],
                timeout_ms: 30_000,
                ..CreateTopicsRequest::default()
            },
        );
        assert!(created.topics[0].error_code == 0);
        lab.world
            .add_node(NodeSpec::new(
                REGISTRY.0,
                "schema-registry",
                "registry",
                json!({ "bootstrap": [1] }),
            ))
            .unwrap();
        let failed = lab.run_until(|lab| lab.snapshot(REGISTRY)["state"] == "failed", 5_000);
        assert!(failed, "{}", lab.snapshot(REGISTRY));
        assert!(lab.snapshot(REGISTRY)["store"]["error"] == error);
        let failures: Vec<Value> = lab
            .world
            .events()
            .filter(|e| e.kind == "kafkastore" && e.detail["level"] == "error")
            .map(|e| e.detail.clone())
            .collect();
        assert!(failures == vec![json!({ "step": "failed", "message": error, "level": "error" })]);
        // The registry does not listen.
        let conn = lab.http_open();
        assert!(lab.run_until(|lab| lab.has_http_reply(conn), 1_000));
        assert!(lab.http_replies(conn) == vec![Payload::Close]);
        assert!(
            lab.world
                .control(REGISTRY, json!({ "cmd": "http", "path": "/subjects" }))
                == Err(format!("the schema registry failed to start: {error}"))
        );
    }
}
