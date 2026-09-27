//! The application nodes end to end: producers, consumers and streams apps
//! against three simulated brokers (one metadata quorum, a group coordinator in
//! each) and a schema registry in one world.
//!
//! A scripted client outside the world (node 99, never hosted) reads the
//! partitions back with raw `Metadata` and `Fetch` requests, and a group's
//! committed offsets with `FindCoordinator` and `OffsetFetch`, as an admin
//! client would, so the tests check what the brokers stored, not what the
//! nodes say they did.
//!
//! The lab's client does not yet ask again for a topic its first metadata
//! request missed, so a producer or a classic consumer that starts before
//! the scenario's topics exist would wait for `metadata.max.age.ms`. The
//! producers therefore start at no rate and get one once the topics exist,
//! and the consumers join then; the streams app looks its topics up again
//! itself and starts with the scenario.

use std::collections::{BTreeMap, BTreeSet};

use assert2::assert;
use krabka_playground::lab::{
    NodeId, World,
    apps::serde::{SchemaFormat, ValueSchema, unframe},
    broker::test_support::{TestClient, decode_response},
    client::partition_for_key,
    net::Frame,
    scenario::{NodeSpec, Scenario},
    world::Fault,
};
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        find_coordinator_request::FindCoordinatorRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
    },
    primitives::uuid::Uuid,
    records::RecordsPayload,
};
use serde_json::{Value, json};

/// The node id of the scripted client.
const CLIENT: u32 = 99;

/// How often, in logical time, a test checks what it waits for.
const CHECK_MS: u64 = 10;

/// A record read back from a partition.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Stored {
    partition: i32,
    offset: i64,
    timestamp: i64,
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

    /// Add a node and start it here, as the page adds one to a running
    /// world.
    fn add(&mut self, spec: &Value) {
        let spec: NodeSpec = serde_json::from_value(spec.clone()).unwrap();
        let mut hosted = self.world.hosted();
        hosted.push(spec.id);
        self.world.set_hosted(&hosted);
        self.world.add_node(spec).unwrap();
    }

    /// Advance the world `ms` of logical time.
    fn run_for(&mut self, ms: u64) {
        let until = self.world.now() + ms;
        self.world.step_until(until);
    }

    /// Step until `pred` holds on the node's snapshot, for at most `max_ms`.
    fn run_until(&mut self, node: u32, max_ms: u64, pred: impl Fn(&Value) -> bool) -> bool {
        self.run_until_all(max_ms, |lab| pred(&lab.snapshot(node)))
    }

    /// Step until the scenario's admin reports every topic created.
    fn wait_for_topics(&mut self) {
        let created = |lab: &Self| lab.world.events().any(|e| e.kind == "topics_created");
        assert!(
            self.run_until_all(5_000, created),
            "the topics were not created"
        );
    }

    /// Step until `pred` holds on the whole lab, for at most `max_ms`,
    /// checking it every `CHECK_MS` of logical time.
    fn run_until_all(&mut self, max_ms: u64, pred: impl Fn(&Self) -> bool) -> bool {
        let deadline = self.world.now() + max_ms;
        while self.world.now() < deadline {
            if pred(self) {
                return true;
            }
            let next = (self.world.now() + CHECK_MS).min(deadline);
            self.world.step_until(next);
        }
        pred(self)
    }

    /// A node's snapshot.
    fn snapshot(&self, node: u32) -> Value {
        self.world.node_snapshot(NodeId(node)).unwrap()
    }

    /// A consumer's assigned partitions.
    fn assigned(&self, node: u32) -> BTreeSet<i32> {
        self.snapshot(node)["assignment"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|row| row["partition"].as_i64())
            .map(|p| i32::try_from(p).unwrap())
            .collect()
    }

    /// The details of the events of `kind` a node recorded, with their time.
    fn events(&self, node: u32, kind: &str) -> Vec<(u64, Value)> {
        self.world
            .events()
            .filter(|e| e.node == Some(NodeId(node)) && e.kind == kind)
            .map(|e| (e.at, e.detail.clone()))
            .collect()
    }

    /// Send a node a control command and take its answer.
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

    /// The offsets `group` committed, by topic and partition, as an admin
    /// client reads them: `FindCoordinator`, then `OffsetFetch` of every
    /// topic from the coordinator. Empty while the group has no coordinator.
    fn committed(&mut self, group: &str) -> BTreeMap<(String, i32), i64> {
        let found = self.call(
            NodeId(1),
            4,
            &FindCoordinatorRequest {
                key_type: 0,
                coordinator_keys: vec![group.to_string()],
                ..Default::default()
            },
        );
        let coordinator = &found.coordinators[0];
        if coordinator.error_code != 0 {
            return BTreeMap::new();
        }
        let node = NodeId(u32::try_from(coordinator.node_id).unwrap());
        let fetched = self.call(
            node,
            9,
            &OffsetFetchRequest {
                groups: vec![OffsetFetchRequestGroup {
                    group_id: group.to_string(),
                    member_epoch: -1,
                    topics: None,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        fetched.groups[0]
            .topics
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .filter(|p| p.committed_offset >= 0)
                    .map(|p| ((t.name.clone(), p.partition_index), p.committed_offset))
            })
            .collect()
    }

    /// Run until `group` has committed the end of every partition of
    /// `topic`, checking every half second for at most `max_ms`.
    fn wait_committed(&mut self, group: &str, topic: &str, max_ms: u64) -> bool {
        let deadline = self.world.now() + max_ms;
        loop {
            let mut ends: BTreeMap<(String, i32), i64> = BTreeMap::new();
            for r in self.read_topic(NodeId(1), topic) {
                let end = ends.entry((topic.to_string(), r.partition)).or_default();
                *end = (*end).max(r.offset + 1);
            }
            let committed: BTreeMap<(String, i32), i64> = self
                .committed(group)
                .into_iter()
                .filter(|((t, _), _)| t == topic)
                .collect();
            if committed == ends {
                return true;
            }
            if self.world.now() >= deadline {
                return false;
            }
            self.run_for(500);
        }
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
                        timestamp: batch.base_timestamp + record.timestamp_delta,
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
    assert!(json!([snapshot["acked"], snapshot["failed"]]) == json!([generated, 0]));

    // Every record is stored once, keyed from its sequence number, on the
    // partition the JVM's default partitioner picks for the key.
    let stored = lab.read_topic(NodeId(1), "orders");
    let id_of = |r: &Stored| {
        let value: Value = serde_json::from_slice(&r.value).unwrap();
        value["id"].as_u64().unwrap()
    };
    let mut placed: Vec<(u64, Option<String>, i32)> = stored
        .iter()
        .map(|r| (id_of(r), r.key.clone(), r.partition))
        .collect();
    placed.sort();
    let expected: Vec<(u64, Option<String>, i32)> = (0..generated)
        .map(|id| {
            let key = format!("customer-{}", id % 10);
            let partition = partition_for_key(key.as_bytes(), 3).unwrap();
            (id, Some(key), partition)
        })
        .collect();
    assert!(placed == expected);
    assert!(stored.iter().all(|r| {
        let value: Value = serde_json::from_slice(&r.value).unwrap();
        value["total"]
            .as_u64()
            .is_some_and(|t| (1..=500).contains(&t))
    }));
    // Each partition holds its records from offset 0, in the order the
    // producer sent them.
    for partition in 0..3 {
        let mut held: Vec<(i64, u64)> = stored
            .iter()
            .filter(|r| r.partition == partition)
            .map(|r| (r.offset, id_of(r)))
            .collect();
        held.sort_unstable();
        let offsets: Vec<i64> = held.iter().map(|(offset, _)| *offset).collect();
        let ids: Vec<u64> = held.iter().map(|(_, id)| *id).collect();
        let mut in_order = ids.clone();
        in_order.sort_unstable();
        assert!(offsets == (0..i64::try_from(held.len()).unwrap()).collect::<Vec<_>>());
        assert!(ids == in_order);
    }
    // The snapshot lists the last ten records with the offsets the brokers
    // gave them.
    let mut last: Vec<&Stored> = stored
        .iter()
        .filter(|r| id_of(r) + 10 >= generated)
        .collect();
    last.sort_by_key(|r| id_of(r));
    let rows: Vec<Value> = last
        .iter()
        .map(|r| {
            json!({
                "seq": id_of(r),
                "partition": r.partition,
                "offset": r.offset,
                "key": r.key,
                "value_preview": serde_json::from_slice::<Value>(&r.value).unwrap(),
            })
        })
        .collect();
    assert!(snapshot["last_records"] == json!(rows));
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
    let mut serialization = lab.snapshot(10)["serialization"].clone();
    let schema_id = serialization["schema_id"].as_i64().unwrap();
    assert!(
        serialization
            .as_object_mut()
            .unwrap()
            .remove("client")
            .is_some()
    );
    assert!(
        serialization
            == json!({
                "registry": 4,
                "subject": "orders-value",
                "format": "avro",
                "state": "ready",
                "schema_id": schema_id,
                "failed": 0,
                "error": null,
            })
    );
    // The registry holds the schema under the subject, with that id.
    let registered = lab.control(
        4,
        json!({ "cmd": "http", "method": "GET", "path": "/subjects/orders-value/versions/latest" }),
    );
    assert!(
        registered
            == json!({
                "status": 200,
                "body": {
                    "subject": "orders-value",
                    "version": 1,
                    "id": schema_id,
                    "schema": ORDER_SCHEMA,
                },
            })
    );
    let schema = ValueSchema::parse(SchemaFormat::Avro, ORDER_SCHEMA).unwrap();

    lab.control(10, json!({ "cmd": "pause" }));
    assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
    // Every value is framed with the id and decodes with the registry's
    // schema to the document the producer generated.
    let mut stored = lab.read_topic(NodeId(1), "orders");
    stored.sort_by_key(|r| r.offset);
    let decoded: Vec<(i64, Value)> = stored
        .iter()
        .map(|r| {
            let (id, body) = unframe(&r.value).unwrap();
            (i64::from(id), schema.decode(body).unwrap())
        })
        .collect();
    let ids: Vec<(i64, Value)> = decoded
        .iter()
        .map(|(id, doc)| (*id, doc["id"].clone()))
        .collect();
    let expected: Vec<(i64, Value)> = (0..stored.len())
        .map(|seq| (schema_id, json!(seq)))
        .collect();
    assert!(!stored.is_empty());
    assert!(ids == expected);
    assert!(decoded.iter().all(|(_, doc)| {
        doc["total"]
            .as_f64()
            .is_some_and(|t| (1.0..=500.0).contains(&t))
    }));
}

/// A producer of JSON orders keyed by customer, at no rate until a test
/// starts it.
fn orders_producer(id: u32, extra: &Value) -> Value {
    let mut config = json!({
        "bootstrap": [1, 2, 3],
        "topic": "orders",
        "rate_per_sec": 0,
        "key": { "pattern": "customer-{seq % 10}" },
        "value": { "format": "json", "template": { "id": "{seq}", "total": "{rand 1 500}" } },
    });
    merge(&mut config, extra);
    json!({ "id": id, "kind": "producer", "config": config })
}

/// A member of the group `billing` on `orders`, reading from the start.
fn billing_consumer(id: u32, protocol: &str, extra: &Value) -> Value {
    let mut config = json!({
        "bootstrap": [1, 2, 3],
        "group": "billing",
        "topics": ["orders"],
        "protocol": protocol,
        "auto_offset_reset": "earliest",
    });
    merge(&mut config, extra);
    json!({ "id": id, "kind": "consumer", "config": config })
}

/// Set the keys of `extra` in `config`.
fn merge(config: &mut Value, extra: &Value) {
    for (key, value) in extra.as_object().into_iter().flatten() {
        config[key] = value.clone();
    }
}

/// The records a consumer processed.
fn processed(lab: &Lab, node: u32) -> u64 {
    lab.snapshot(node)["processed"].as_u64().unwrap_or(0)
}

#[test]
fn a_group_of_two_shares_three_partitions_and_both_consume() {
    for protocol in ["classic", "consumer"] {
        let mut lab = Lab::new(&scenario(
            vec![orders_producer(10, &json!({}))],
            &json!([{ "name": "orders", "partitions": 3 }]),
        ));
        lab.wait_for_topics();
        lab.add(&billing_consumer(21, protocol, &json!({})));
        lab.add(&billing_consumer(22, protocol, &json!({})));
        lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 100 }));
        // Each member takes a share of the three partitions and consumes it.
        let shared = |lab: &Lab| {
            let (a, b) = (lab.assigned(21), lab.assigned(22));
            !a.is_empty()
                && !b.is_empty()
                && a.is_disjoint(&b)
                && a.union(&b).copied().collect::<Vec<_>>() == vec![0, 1, 2]
                && processed(lab, 21) > 0
                && processed(lab, 22) > 0
        };
        assert!(
            lab.run_until_all(30_000, shared),
            "{protocol}: {}",
            lab.snapshot(21)
        );
        lab.control(10, json!({ "cmd": "pause" }));
        assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
        let acked = lab.snapshot(10)["acked"].as_u64().unwrap();
        // Every record is processed once, and the group commits up to the end.
        let done = |lab: &Lab| {
            processed(lab, 21) + processed(lab, 22) == acked
                && [21, 22].iter().all(|n| {
                    lab.snapshot(*n)["assignment"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|row| row["committed"] == row["hwm"] && row["lag"] == 0)
                })
        };
        assert!(
            lab.run_until_all(20_000, done),
            "{protocol}: {} {}",
            lab.snapshot(21),
            lab.snapshot(22)
        );
    }
}

#[test]
fn a_slow_consumer_shows_lag_that_drains_when_it_speeds_up() {
    let mut lab = Lab::new(&scenario(
        vec![orders_producer(10, &json!({}))],
        &json!([{ "name": "orders", "partitions": 3 }]),
    ));
    lab.wait_for_topics();
    // Ten records a second against fifty.
    lab.add(&billing_consumer(
        21,
        "consumer",
        &json!({ "process_ms": 100 }),
    ));
    lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 50 }));
    assert!(lab.run_until(21, 30_000, |s| s["lag"].as_i64() >= Some(200)));
    lab.control(10, json!({ "cmd": "pause" }));
    assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
    // With nothing new, the slow consumer still lags.
    lab.run_for(3_000);
    let lagging = lab.snapshot(21)["lag"].as_i64().unwrap();
    assert!(lagging >= 150, "{lagging}");
    assert!(lab.control(21, json!({ "cmd": "process_ms", "ms": 0 })) == json!({ "process_ms": 0 }));
    // Instant processing drains the lag, and the group commits the end.
    let acked = lab.snapshot(10)["acked"].as_u64().unwrap();
    let drained = |lab: &Lab| {
        let s = lab.snapshot(21);
        s["lag"] == 0
            && s["processed"] == acked
            && s["assignment"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["committed"] == row["hwm"])
    };
    assert!(lab.run_until_all(15_000, drained), "{}", lab.snapshot(21));
}

#[test]
fn a_killed_members_partitions_move_when_its_session_expires() {
    // The protocol, the session the group gives the member (the consumer's
    // `session.timeout.ms`, or the broker's
    // `group.consumer.session.timeout.ms`), and its heartbeat interval.
    for (protocol, session, heartbeat) in [("classic", 45_000, 3_000), ("consumer", 45_000, 5_000)]
    {
        let mut lab = Lab::new(&scenario(
            vec![orders_producer(10, &json!({}))],
            &json!([{ "name": "orders", "partitions": 3 }]),
        ));
        lab.wait_for_topics();
        lab.add(&billing_consumer(21, protocol, &json!({})));
        lab.add(&billing_consumer(22, protocol, &json!({})));
        lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 20 }));
        let shared = |lab: &Lab| {
            let (a, b) = (lab.assigned(21), lab.assigned(22));
            !a.is_empty() && !b.is_empty() && a.is_disjoint(&b) && a.len() + b.len() == 3
        };
        assert!(lab.run_until_all(30_000, shared), "{protocol}");
        lab.run_for(2_000);
        let killed_at = lab.world.now();
        lab.world.fault(Fault::Kill { node: NodeId(22) });
        // A crashed member cannot leave the group: its partitions move once
        // its session expires, to the member that is left.
        let all = |lab: &Lab| lab.assigned(21) == BTreeSet::from([0, 1, 2]);
        assert!(
            lab.run_until_all(session + heartbeat + 10_000, all),
            "{protocol}"
        );
        let moved_after = lab.world.now() - killed_at;
        assert!(
            moved_after >= session - heartbeat,
            "{protocol}: {moved_after} ms"
        );
        assert!(
            moved_after <= session + heartbeat + 5_000,
            "{protocol}: {moved_after} ms"
        );
        // The survivor reads the moved partitions on from the committed
        // offsets and catches up.
        lab.control(10, json!({ "cmd": "pause" }));
        let caught_up = |lab: &Lab| {
            lab.snapshot(10)["pending_records"] == 0
                && lab.snapshot(21)["lag"] == 0
                && lab.snapshot(21)["assignment"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|row| row["committed"] == row["hwm"])
        };
        assert!(
            lab.run_until_all(20_000, caught_up),
            "{protocol}: {}",
            lab.snapshot(21)
        );
    }
}

#[test]
fn a_consumer_decodes_what_an_avro_producer_framed() {
    let producer = json!({ "id": 10, "kind": "producer", "config": {
        "bootstrap": [1, 2, 3],
        "topic": "orders",
        "rate_per_sec": 0,
        "key": { "pattern": "c-{seq % 3}" },
        "value": { "format": "json", "template": { "id": "{seq}", "total": "{rand 1 500}" } },
        "serialization": { "registry": 4, "format": "avro", "schema": ORDER_SCHEMA },
    } });
    let registry =
        json!({ "id": 4, "kind": "schema-registry", "config": { "bootstrap": [1, 2, 3] } });
    let mut lab = Lab::new(&scenario(
        vec![registry, producer],
        &json!([{ "name": "orders", "partitions": 1 }]),
    ));
    lab.wait_for_topics();
    lab.add(&billing_consumer(
        21,
        "consumer",
        &json!({ "deserialize": { "registry": 4 } }),
    ));
    lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 20 }));
    assert!(lab.run_until(21, 20_000, |s| s["processed"].as_u64() >= Some(30)));
    lab.control(10, json!({ "cmd": "pause" }));
    assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
    let acked = lab.snapshot(10)["acked"].clone();
    assert!(lab.run_until(21, 10_000, |s| s["processed"] == acked));

    // The consumer looked the producer's schema up by the id in each frame
    // and shows the documents the producer serialized.
    let schema_id = lab.snapshot(10)["serialization"]["schema_id"].clone();
    let schema = ValueSchema::parse(SchemaFormat::Avro, ORDER_SCHEMA).unwrap();
    let stored = lab.read_topic(NodeId(1), "orders");
    let last: Vec<Value> = stored[stored.len() - 10..]
        .iter()
        .map(|r| {
            let (id, body) = unframe(&r.value).unwrap();
            assert!(json!(id) == schema_id);
            json!({
                "topic": "orders",
                "partition": r.partition,
                "offset": r.offset,
                "key": r.key,
                "value_preview": schema.decode(body).unwrap(),
                "schema_id": id,
            })
        })
        .collect();
    let consumer = lab.snapshot(21);
    assert!(consumer["last_records"] == json!(last));
    assert!(consumer["deserialize"]["schemas"] == json!({ schema_id.to_string(): "ready (avro)" }));
}

/// The counting app: orders over 250 counted per customer into
/// `order-counts`, with the store `counts`.
fn counting_app(id: u32) -> Value {
    json!({ "id": id, "kind": "streams", "config": {
        "bootstrap": [1, 2, 3],
        "application_id": "order-stats",
        "topology": {
            "source": "orders",
            "ops": [
                { "op": "filter", "field": "total", "gt": 250 },
                { "op": "count_by_key", "store": "counts" },
            ],
            "sink": "order-counts",
        },
    } })
}

fn counting_lab() -> Lab {
    let mut lab = Lab::new(&scenario(
        vec![orders_producer(10, &json!({})), counting_app(20)],
        &json!([
            { "name": "orders", "partitions": 3 },
            { "name": "order-counts", "partitions": 3 },
        ]),
    ));
    lab.wait_for_topics();
    lab
}

/// Send orders at 50 a second for `ms`, then wait for their answers.
fn send_orders(lab: &mut Lab, ms: u64) {
    lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 50 }));
    lab.run_for(ms);
    lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 0 }));
    assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
}

/// The counts of a count topic by key, in offset order: `{"key", "count"}`
/// JSON values for the sink, 8-byte big-endian longs for the changelog.
fn counts_by_key(records: &[Stored], changelog: bool) -> BTreeMap<String, Vec<i64>> {
    let mut sorted: Vec<&Stored> = records.iter().collect();
    sorted.sort_by_key(|r| (r.partition, r.offset));
    let mut out: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for r in sorted {
        let key = r.key.clone().unwrap();
        let count = if changelog {
            i64::from_be_bytes(r.value.as_slice().try_into().unwrap())
        } else {
            let value: Value = serde_json::from_slice(&r.value).unwrap();
            assert!(value["key"] == json!(key));
            value["count"].as_i64().unwrap()
        };
        out.entry(key).or_default().push(count);
    }
    out
}

/// One run of counts from 1 per customer, as long as the customer's orders
/// over 250.
fn expected_runs(orders: &[Stored]) -> BTreeMap<String, Vec<i64>> {
    let mut totals: BTreeMap<String, i64> = BTreeMap::new();
    for r in orders {
        let value: Value = serde_json::from_slice(&r.value).unwrap();
        if value["total"].as_i64().unwrap() > 250 {
            *totals.entry(r.key.clone().unwrap()).or_default() += 1;
        }
    }
    totals
        .into_iter()
        .map(|(key, n)| (key, (1..=n).collect()))
        .collect()
}

#[test]
fn a_streams_app_counts_into_its_sink_and_its_changelog() {
    let mut lab = counting_lab();
    send_orders(&mut lab, 5_000);
    assert!(
        lab.wait_committed("order-stats", "orders", 30_000),
        "{}",
        lab.snapshot(20)
    );
    let orders = lab.read_topic(NodeId(1), "orders");
    let runs = expected_runs(&orders);
    // Every update of a count reached the sink, in order.
    assert!(counts_by_key(&lab.read_topic(NodeId(1), "order-counts"), false) == runs);
    // And the store's changelog, which the broker created for the group, in
    // the partition of the task that counted it.
    let changelog = lab.read_topic(NodeId(1), "order-stats-counts-changelog");
    assert!(counts_by_key(&changelog, true) == runs);
    let partition_of: BTreeMap<String, i32> = orders
        .iter()
        .map(|r| (r.key.clone().unwrap(), r.partition))
        .collect();
    assert!(
        changelog
            .iter()
            .all(|r| partition_of[r.key.as_ref().unwrap()] == r.partition)
    );
    // The store answers queries with the last counts.
    for (key, counts) in &runs {
        let answer = lab.control(20, json!({ "cmd": "query", "store": "counts", "key": key }));
        assert!(
            answer
                == json!({
                    "store": "counts",
                    "key": key,
                    "task": format!("0_{}", partition_of[key]),
                    "value": counts.last(),
                })
        );
    }
}

#[test]
fn a_restarted_streams_app_restores_its_counts_and_goes_on() {
    let mut lab = counting_lab();
    send_orders(&mut lab, 3_000);
    assert!(lab.wait_committed("order-stats", "orders", 30_000));
    let changelog: Vec<Stored> = lab.read_topic(NodeId(1), "order-stats-counts-changelog");

    // The process crashes and comes back with empty stores and a new member
    // id; the group hands it the tasks once the old member's session
    // (`group.streams.session.timeout.ms`, 45 s) expired.
    let restarted_at = lab.world.now();
    lab.world.fault(Fault::Kill { node: NodeId(20) });
    lab.world.fault(Fault::Restart { node: NodeId(20) });
    let mark = lab.world.event_count();
    send_orders(&mut lab, 3_000);
    assert!(
        lab.wait_committed("order-stats", "orders", 90_000),
        "{}",
        lab.snapshot(20)
    );

    // The tasks came back once the old member's session had expired.
    let assigned = lab.events(20, "tasks_assigned");
    let (back_at, tasks) = assigned.last().unwrap();
    assert!(*tasks == json!({ "tasks": ["0_0", "0_1", "0_2"] }));
    assert!(
        back_at - restarted_at >= 40_000,
        "{back_at} - {restarted_at}"
    );
    // Each task restored its changelog partition before it went on.
    let mut restored: Vec<Value> = lab
        .world
        .events_since(mark)
        .into_iter()
        .filter(|e| e.node == Some(NodeId(20)) && e.kind == "task_restored")
        .map(|e| e.detail)
        .collect();
    restored.sort_by_key(|d| d["task"].as_str().unwrap().to_string());
    let expected: Vec<Value> = (0..3)
        .map(|p| {
            json!({
                "task": format!("0_{p}"),
                "records": changelog.iter().filter(|r| r.partition == p).count(),
            })
        })
        .collect();
    assert!(restored == expected);
    // So every customer's counts run on unbroken across the restart.
    let orders = lab.read_topic(NodeId(1), "orders");
    let runs = expected_runs(&orders);
    assert!(counts_by_key(&lab.read_topic(NodeId(1), "order-counts"), false) == runs);
    assert!(
        counts_by_key(
            &lab.read_topic(NodeId(1), "order-stats-counts-changelog"),
            true
        ) == runs
    );
}

#[test]
fn window_count_emits_a_count_per_key_and_window() {
    let producer = json!({ "id": 10, "kind": "producer", "config": {
        "bootstrap": [1, 2, 3],
        "topic": "clicks",
        "rate_per_sec": 0,
        "key": { "pattern": "user-{seq % 2}" },
        "value": { "format": "json", "template": { "page": "{pick home|cart|search}" } },
    } });
    let app = json!({ "id": 20, "kind": "streams", "config": {
        "bootstrap": [1, 2, 3],
        "application_id": "clicks-per-second",
        "topology": {
            "source": "clicks",
            "ops": [{ "op": "window_count", "size_ms": 1000, "store": "per-second" }],
            "sink": "click-counts",
        },
    } });
    let mut lab = Lab::new(&scenario(
        vec![producer, app],
        &json!([
            { "name": "clicks", "partitions": 1 },
            { "name": "click-counts", "partitions": 1 },
        ]),
    ));
    lab.wait_for_topics();
    lab.control(10, json!({ "cmd": "rate", "rate_per_sec": 10 }));
    lab.run_for(4_000);
    lab.control(10, json!({ "cmd": "pause" }));
    assert!(lab.run_until(10, 5_000, |s| s["pending_records"] == 0));
    assert!(
        lab.wait_committed("clicks-per-second", "clicks", 30_000),
        "{}",
        lab.snapshot(20)
    );

    // Each click counts in the one-second window its timestamp falls in,
    // and every update of a window's count reaches the sink.
    let mut clicks = lab.read_topic(NodeId(1), "clicks");
    clicks.sort_by_key(|r| r.offset);
    let mut windows: BTreeMap<(String, i64), i64> = BTreeMap::new();
    let expected: Vec<(String, Value)> = clicks
        .iter()
        .map(|r| {
            let key = r.key.clone().unwrap();
            let start = r.timestamp - r.timestamp.rem_euclid(1_000);
            let count = windows.entry((key.clone(), start)).or_default();
            *count += 1;
            let value = json!({
                "key": key,
                "window_start": start,
                "window_end": start + 1_000,
                "count": *count,
            });
            (key, value)
        })
        .collect();
    let mut emitted = lab.read_topic(NodeId(1), "click-counts");
    emitted.sort_by_key(|r| r.offset);
    let emitted: Vec<(String, Value)> = emitted
        .iter()
        .map(|r| {
            (
                r.key.clone().unwrap(),
                serde_json::from_slice(&r.value).unwrap(),
            )
        })
        .collect();
    assert!(emitted == expected);
    assert!(windows.len() >= 6);
    // The store answers with the windows of a key.
    let user_0: Vec<Value> = windows
        .iter()
        .filter(|((key, _), _)| key == "user-0")
        .map(|((_, start), count)| {
            json!({ "window_start": start, "window_end": start + 1_000, "count": count })
        })
        .collect();
    assert!(
        lab.control(
            20,
            json!({ "cmd": "query", "store": "per-second", "key": "user-0" })
        ) == json!({ "store": "per-second", "key": "user-0", "task": "0_0", "value": user_0 })
    );
}
