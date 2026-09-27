//! The application nodes end to end: producers, consumers and streams apps
//! against simulated brokers and a schema registry in one world.
//!
//! A scripted client outside the world (node 99, never hosted) reads the
//! partitions back with raw `Metadata` and `Fetch` requests, so the tests
//! check what the brokers stored, not what the nodes say they sent.

use std::collections::BTreeMap;

use assert2::assert;
use krabka_playground::lab::{
    NodeId, World,
    apps::serde::{SchemaFormat, ValueSchema, unframe},
    broker::test_support::{TestClient, decode_response},
    client::partition_for_key,
    net::Frame,
    scenario::Scenario,
};
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid,
    records::RecordsPayload,
};
use serde_json::{Value, json};

/// The node id of the scripted client.
const CLIENT: u32 = 99;

/// A record read back from a partition.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Stored {
    partition: i32,
    offset: i64,
    key: Option<String>,
    value: Vec<u8>,
}

/// A world that hosts every scenario node, and a scripted client outside it.
struct Lab {
    world: World,
    /// One client connection per broker, by broker node.
    clients: BTreeMap<NodeId, TestClient>,
}

impl Lab {
    fn new(scenario: &Value) -> Self {
        let scenario: Scenario = serde_json::from_value(scenario.clone()).unwrap();
        let hosted: Vec<NodeId> = scenario.nodes.iter().map(|n| n.id).collect();
        let world = World::from_scenario_hosted(&scenario, &hosted).unwrap();
        Self {
            world,
            clients: BTreeMap::new(),
        }
    }

    fn run_for(&mut self, ms: u64) {
        let until = self.world.now() + ms;
        self.world.step_until(until);
    }

    /// Step until `pred` holds on the node's snapshot, for at most `max_ms`.
    fn run_until(&mut self, node: u32, max_ms: u64, pred: impl Fn(&Value) -> bool) -> bool {
        let deadline = self.world.now() + max_ms;
        loop {
            if pred(&self.snapshot(node)) {
                return true;
            }
            if !self.world.step_once(deadline) {
                return pred(&self.snapshot(node));
            }
        }
    }

    /// Step until the scenario's admin reports every topic created. The
    /// producers start after it: a producer's first metadata request that
    /// misses its topic is not asked again until `metadata.max.age.ms` (see
    /// the batch report).
    fn wait_for_topics(&mut self) {
        let deadline = self.world.now() + 5_000;
        while !self.world.events().any(|e| e.kind == "topics_created") {
            assert!(
                self.world.step_once(deadline),
                "the topics were not created"
            );
        }
    }

    fn snapshot(&self, node: u32) -> Value {
        self.world.node_snapshot(NodeId(node)).unwrap()
    }

    fn control(&mut self, node: u32, command: Value) -> Value {
        self.world.control(NodeId(node), command).unwrap()
    }

    /// Send one request from the scripted client to `broker` and take its
    /// reply.
    fn call<R: ProtocolRequest>(
        &mut self,
        broker: NodeId,
        version: i16,
        request: &R,
    ) -> R::Response {
        if !self.clients.contains_key(&broker) {
            let client = TestClient::new(CLIENT, broker.0);
            self.world.push_ingress(vec![client.open(broker)]);
            self.clients.insert(broker, client);
        }
        let client = self.clients.get_mut(&broker).unwrap();
        let frame = client.request(broker, version, request);
        let correlation = client.last_correlation();
        let endpoint = client.endpoint();
        self.world.push_ingress(vec![frame]);
        self.run_for(50);
        let replies: Vec<Frame> = self
            .world
            .drain_egress()
            .into_iter()
            .map(|t| t.frame)
            .filter(|f| f.dst == endpoint && f.conn.0 == broker.0)
            .collect();
        assert!(replies.len() == 1, "expected one reply, got {replies:?}");
        let (got, response) = decode_response::<R>(&replies[0], version).unwrap();
        assert!(got == correlation);
        response
    }

    /// The leader node and the id of every partition of `topic`, from
    /// `broker`'s metadata.
    fn leaders(&mut self, broker: NodeId, topic: &str) -> (Uuid, BTreeMap<i32, NodeId>) {
        let response = self.call(
            broker,
            12,
            &MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some(topic.to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            },
        );
        let t = &response.topics[0];
        let leaders = t
            .partitions
            .iter()
            .map(|p| {
                (
                    p.partition_index,
                    NodeId(u32::try_from(p.leader_id).unwrap()),
                )
            })
            .collect();
        (t.topic_id, leaders)
    }

    /// Every record of `topic`, read from each partition's leader.
    fn read_topic(&mut self, broker: NodeId, topic: &str) -> Vec<Stored> {
        let (topic_id, leaders) = self.leaders(broker, topic);
        let mut out = Vec::new();
        for (partition, leader) in leaders {
            let response = self.call(
                leader,
                16,
                &FetchRequest {
                    max_wait_ms: 0,
                    min_bytes: 1,
                    topics: vec![FetchTopic {
                        topic: topic.to_string(),
                        topic_id,
                        partitions: vec![FetchPartition {
                            partition,
                            fetch_offset: 0,
                            partition_max_bytes: 1 << 20,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
            let row = &response.responses[0].partitions[0];
            assert!(row.error_code == 0, "{row:?}");
            let batches = match &row.records {
                Some(RecordsPayload::V2(batches)) => batches.clone(),
                _ => Vec::new(),
            };
            for batch in batches {
                for record in &batch.records {
                    out.push(Stored {
                        partition,
                        offset: batch.base_offset + i64::from(record.offset_delta),
                        key: record
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8(k.to_vec()).unwrap()),
                        value: record
                            .value
                            .as_ref()
                            .map(|v| v.to_vec())
                            .unwrap_or_default(),
                    });
                }
            }
        }
        out
    }
}

fn brokers() -> Vec<Value> {
    (1..=3)
        .map(|id| json!({ "id": id, "kind": "broker", "config": { "broker_id": id } }))
        .collect()
}

fn scenario(mut nodes: Vec<Value>, topics: &Value) -> Value {
    let mut all = brokers();
    all.append(&mut nodes);
    json!({
        "version": 1,
        "seed": 11,
        "links": { "default_latency_ms": 5 },
        "nodes": all,
        "topics": topics,
    })
}

#[test]
fn a_producer_writes_to_the_brokers_and_every_record_reads_back() {
    let producer = json!({ "id": 10, "kind": "producer", "config": {
        "bootstrap": [1],
        "topic": "orders",
        "rate_per_sec": 0,
        "key": { "pattern": "customer-{seq % 10}" },
        "value": { "format": "json", "template": { "id": "{seq}", "total": "{rand 1 500}" } },
    } });
    let mut lab = Lab::new(&scenario(
        vec![producer],
        &json!([{ "name": "orders", "partitions": 3 }]),
    ));
    lab.wait_for_topics();
    lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 200 }));
    assert!(lab.run_until(10, 10_000, |s| s["acked"].as_u64() >= Some(60)));
    lab.control(10, json!({ "cmd": "pause" }));
    // Whatever was generated before the pause gets its answer.
    assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
    let snapshot = lab.snapshot(10);
    let generated = snapshot["generated"].as_u64().unwrap();
    assert!(snapshot["acked"] == generated);
    assert!(snapshot["failed"] == 0);

    let stored = lab.read_topic(NodeId(1), "orders");
    assert!(stored.len() == usize::try_from(generated).unwrap());
    let mut ids: Vec<u64> = Vec::new();
    for record in &stored {
        let key = record.key.clone().unwrap();
        // The JVM's default partitioner placed the record by its key.
        assert!(partition_for_key(key.as_bytes(), 3) == Some(record.partition));
        let value: Value = serde_json::from_slice(&record.value).unwrap();
        let id = value["id"].as_u64().unwrap();
        assert!(key == format!("customer-{}", id % 10));
        assert!((1..=500).contains(&value["total"].as_u64().unwrap()));
        ids.push(id);
    }
    ids.sort_unstable();
    assert!(ids == (0..generated).collect::<Vec<_>>());
    // Offsets run from 0 in each partition, in the order the producer sent.
    for partition in 0..3 {
        let offsets: Vec<i64> = stored
            .iter()
            .filter(|r| r.partition == partition)
            .map(|r| r.offset)
            .collect();
        assert!(offsets == (0..i64::try_from(offsets.len()).unwrap()).collect::<Vec<_>>());
    }
    // The snapshot lists the last records with the offsets the brokers gave.
    let last = snapshot["last_records"].as_array().unwrap();
    assert!(last.len() == 10);
    for row in last {
        let partition = i32::try_from(row["partition"].as_i64().unwrap()).unwrap();
        let offset = row["offset"].as_i64().unwrap();
        let record = stored
            .iter()
            .find(|r| r.partition == partition && r.offset == offset)
            .unwrap();
        assert!(serde_json::from_slice::<Value>(&record.value).unwrap() == row["value_preview"]);
    }
}

const ORDER_SCHEMA: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"total","type":"double"}]}"#;

#[test]
fn an_avro_producer_registers_its_schema_and_frames_every_value() {
    let producer = json!({ "id": 10, "kind": "producer", "config": {
        "bootstrap": [1],
        "topic": "orders",
        "rate_per_sec": 0,
        "key": { "pattern": "c-{seq % 3}" },
        "value": { "format": "json", "template": { "id": "{seq}", "total": "{rand 1 500}" } },
        "serialization": { "registry": 4, "format": "avro", "schema": ORDER_SCHEMA },
    } });
    let registry = json!({ "id": 4, "kind": "schema-registry", "config": { "bootstrap": [1] } });
    let mut lab = Lab::new(&scenario(
        vec![registry, producer],
        &json!([{ "name": "orders", "partitions": 1 }]),
    ));
    lab.wait_for_topics();
    lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 50 }));
    assert!(lab.run_until(10, 10_000, |s| s["acked"].as_u64() >= Some(20)));
    let snapshot = lab.snapshot(10);
    let serialization = &snapshot["serialization"];
    assert!(serialization["state"] == "ready");
    assert!(serialization["subject"] == "orders-value");
    let schema_id = serialization["schema_id"].as_i64().unwrap();
    // The registry holds the schema under the subject, with that id.
    let registered = lab.control(
        4,
        json!({ "cmd": "http", "method": "GET", "path": "/subjects/orders-value/versions/latest" }),
    );
    assert!(registered["status"] == 200);
    assert!(registered["body"]["id"] == schema_id);
    let schema = ValueSchema::parse(
        SchemaFormat::Avro,
        registered["body"]["schema"].as_str().unwrap(),
    )
    .unwrap();

    lab.control(10, json!({ "cmd": "pause" }));
    assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
    let stored = lab.read_topic(NodeId(1), "orders");
    assert!(!stored.is_empty());
    for (seq, record) in stored.iter().enumerate() {
        let (id, body) = unframe(&record.value).unwrap();
        assert!(i64::from(id) == schema_id);
        let doc = schema.decode(body).unwrap();
        assert!(doc["id"] == json!(seq));
        assert!(
            doc["total"]
                .as_f64()
                .is_some_and(|t| (1.0..=500.0).contains(&t))
        );
    }
}
