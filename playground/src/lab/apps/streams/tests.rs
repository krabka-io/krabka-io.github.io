//! The streams node against a scripted cluster: the client tests' fake
//! broker serves metadata, fetches, produces and offsets, a scripted
//! coordinator answers the streams group heartbeats, and a stub answers the
//! schema registry calls, all on a deterministic clock.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use assert2::assert;
use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode, ProtocolRequest,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        common::{
            streams_group_heartbeat_request::task_ids::TaskIds as RequestTaskIds,
            streams_group_heartbeat_response::task_ids::TaskIds as ResponseTaskIds,
        },
        fetch_request::FetchRequest,
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        list_offsets_request::ListOffsetsRequest,
        metadata_request::MetadataRequest,
        offset_commit_request::OffsetCommitRequest,
        offset_fetch_request::OffsetFetchRequest,
        produce_request::ProduceRequest,
        request_header::RequestHeader,
        response_header::ResponseHeader,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
};
use serde_json::{Value, json};

use super::StreamsNode;
use crate::lab::{
    LabError,
    apps::{
        serde::{SchemaFormat, ValueSchema, frame, unframe},
        topology::{CompiledTopology, TopologySpec},
    },
    client::{
        BatchRecord, ConsumedRecord,
        fake_broker::{ClusterState, FakeBroker, frame_response},
        partition_for_key, records_of,
    },
    codes,
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId, Payload},
    registry::http::{HttpRequest, HttpResponse},
    scenario::NodeSpec,
    testing::CtxBuffers,
};

const BROKER: NodeId = NodeId(1);
const REGISTRY: NodeId = NodeId(4);
const APP: NodeId = NodeId(9);

/// The one-way delay of every frame.
const LATENCY: Millis = 1;

fn node(config: Value) -> Result<StreamsNode, LabError> {
    StreamsNode::from_spec(&NodeSpec::new(APP.0, "streams", "s", config))
}

/// Tasks of one role: subtopology ids and their partitions.
type TaskList = Vec<(&'static str, Vec<i32>)>;

/// The scripted KIP-1071 coordinator: it answers every heartbeat with the
/// member epoch and the assignment the test set last, and records what the
/// member sent.
#[derive(Default)]
struct Coordinator {
    epoch: i32,
    /// The active tasks the next answer hands out; `None` leaves the task
    /// lists out.
    next: Option<TaskList>,
    /// The standby and warm-up tasks that go with `next`.
    next_standby: TaskList,
    next_warmup: TaskList,
    /// An error code the next answer carries instead.
    error: Option<i16>,
    /// Every heartbeat, with the time it arrived.
    seen: Vec<(Millis, StreamsGroupHeartbeatRequest)>,
}

impl Coordinator {
    fn answer(
        &mut self,
        now: Millis,
        request: StreamsGroupHeartbeatRequest,
        state: &mut ClusterState,
    ) -> StreamsGroupHeartbeatResponse {
        self.seen.push((now, request.clone()));
        if let Some(code) = self.error.take() {
            return StreamsGroupHeartbeatResponse {
                error_code: code,
                ..Default::default()
            };
        }
        let tasks = self.next.take();
        let standby = std::mem::take(&mut self.next_standby);
        let warmup = std::mem::take(&mut self.next_warmup);
        if request.member_epoch == 0 || tasks.is_some() {
            self.epoch += 1;
        }
        // The fake broker takes a commit from a member it knows.
        state
            .groups
            .entry(request.group_id.clone())
            .or_default()
            .epochs
            .insert(request.member_id.clone(), self.epoch);
        let ids = |tasks: TaskList| -> Vec<ResponseTaskIds> {
            tasks
                .into_iter()
                .map(|(subtopology, partitions)| ResponseTaskIds {
                    subtopology_id: subtopology.to_string(),
                    partitions,
                    ..Default::default()
                })
                .collect()
        };
        let lists = tasks.is_some();
        StreamsGroupHeartbeatResponse {
            member_id: request.member_id,
            member_epoch: self.epoch,
            heartbeat_interval_ms: 1_000,
            status: Some(Vec::new()),
            active_tasks: tasks.map(ids),
            standby_tasks: lists.then(|| ids(standby)),
            warmup_tasks: lists.then(|| ids(warmup)),
            ..Default::default()
        }
    }
}

/// A schema registry stub: schemas by id, and the id the next registration
/// gets.
#[derive(Default)]
struct Registry {
    schemas: BTreeMap<i32, Value>,
    next_id: i32,
    registered: Vec<(String, Value)>,
}

impl Registry {
    fn answer(&mut self, frame: &Frame) -> Option<Frame> {
        let (request, _) = HttpRequest::parse(frame.payload.data()?).ok()?;
        let response = match (request.method.as_str(), request.segments().as_slice()) {
            ("GET", [schemas, ids, id]) if schemas == "schemas" && ids == "ids" => {
                match id.parse().ok().and_then(|id: i32| self.schemas.get(&id)) {
                    Some(schema) => HttpResponse::ok(schema),
                    None => HttpResponse::error(404, 40403, "Schema not found"),
                }
            }
            ("POST", [subjects, subject, versions])
                if subjects == "subjects" && versions == "versions" =>
            {
                self.registered
                    .push((subject.clone(), request.body_json().unwrap_or(Value::Null)));
                HttpResponse::ok(&json!({ "id": self.next_id }))
            }
            _ => HttpResponse::error(404, 404, "no such path"),
        };
        Some(frame.reply(Payload::Data(response.encode())))
    }
}

/// The versions the scripted broker lists: the fake broker's apis the node
/// uses, and the streams group heartbeat the coordinator script answers.
fn api_versions() -> ApiVersionsResponse {
    fn row<R: ProtocolRequest>() -> ApiVersion {
        ApiVersion {
            api_key: R::API_KEY,
            min_version: R::MIN_VERSION,
            max_version: R::LATEST_STABLE_VERSION,
            ..Default::default()
        }
    }
    ApiVersionsResponse {
        api_keys: vec![
            row::<ProduceRequest>(),
            row::<FetchRequest>(),
            row::<ListOffsetsRequest>(),
            row::<MetadataRequest>(),
            row::<OffsetCommitRequest>(),
            row::<OffsetFetchRequest>(),
            row::<FindCoordinatorRequest>(),
            row::<InitProducerIdRequest>(),
            row::<ApiVersionsRequest>(),
            row::<StreamsGroupHeartbeatRequest>(),
        ],
        ..Default::default()
    }
}

/// A response of a flexible version: length, header v1, body.
fn flexible_response<T: Encode>(correlation_id: i32, version: i16, body: &T) -> Bytes {
    let header = ResponseHeader {
        correlation_id,
        ..Default::default()
    };
    let len = header.encoded_len(1) + body.encoded_len(version);
    let mut buf = BytesMut::with_capacity(4 + len);
    buf.put_i32(i32::try_from(len).unwrap());
    header.encode(&mut buf, 1).unwrap();
    body.encode(&mut buf, version).unwrap();
    buf.freeze()
}

/// The streams node, one broker, the coordinator script and the registry
/// stub.
struct Cluster {
    state: Rc<RefCell<ClusterState>>,
    broker: FakeBroker,
    broker_bufs: CtxBuffers,
    broker_timer: Option<Millis>,
    node: StreamsNode,
    node_bufs: CtxBuffers,
    node_timer: Option<Millis>,
    /// The connections the node opened, to close when it restarts.
    opened: Vec<Frame>,
    coordinator: Coordinator,
    registry: Registry,
    now: Millis,
    seq: u64,
    wire: Vec<(Millis, u64, Frame)>,
    events: Vec<(&'static str, Value)>,
    /// Steps taken at the current time, to catch a busy loop.
    spins: (Millis, u32),
}

impl Cluster {
    fn new(config: Value, topics: &[(&str, i32)]) -> Self {
        let state = ClusterState::new(&[(1, BROKER.0)]);
        for (topic, partitions) in topics {
            state.borrow_mut().add_topic(topic, *partitions, 1);
        }
        let mut cluster = Self {
            broker: FakeBroker::new(BROKER, 1, Rc::clone(&state)),
            state,
            broker_bufs: CtxBuffers::new(BROKER),
            broker_timer: None,
            node: node(config).unwrap(),
            node_bufs: CtxBuffers::new(APP),
            node_timer: None,
            opened: Vec::new(),
            coordinator: Coordinator::default(),
            registry: Registry::default(),
            now: 0,
            seq: 0,
            wire: Vec::new(),
            events: Vec::new(),
            spins: (0, 0),
        };
        cluster.call_broker(FakeBroker::start);
        cluster
    }

    fn start(&mut self) {
        self.call_node(StreamsNode::start);
    }

    /// Crash the node and start it again: what was on the wire to or from
    /// it is lost and its connections close, as the world does.
    fn restart(&mut self) {
        self.node.stop();
        self.node_timer = None;
        self.wire
            .retain(|(_, _, f)| f.src.node != APP && f.dst.node != APP);
        for open in std::mem::take(&mut self.opened) {
            self.deliver(Frame::close(open.src, open.dst, open.conn));
        }
        self.start();
    }

    fn call_node<T>(&mut self, f: impl FnOnce(&mut StreamsNode, &mut Ctx<'_>) -> T) -> T {
        let now = self.now;
        let node = &mut self.node;
        let result = self.node_bufs.with(now, |ctx| f(node, ctx));
        if let Some(at) = self.node_bufs.timer.take() {
            self.node_timer = Some(at);
        }
        self.events.append(&mut self.node_bufs.events);
        for frame in self.node_bufs.take_frames() {
            if frame.payload == Payload::Open {
                self.opened.push(frame.clone());
            }
            self.send(frame);
        }
        result
    }

    fn call_broker(&mut self, f: impl FnOnce(&mut FakeBroker, &mut Ctx<'_>)) {
        let now = self.now;
        let broker = &mut self.broker;
        self.broker_bufs.with(now, |ctx| f(broker, ctx));
        if let Some(at) = self.broker_bufs.timer.take() {
            self.broker_timer = Some(at);
        }
        for frame in self.broker_bufs.take_frames() {
            self.send(frame);
        }
    }

    fn send(&mut self, frame: Frame) {
        self.seq += 1;
        self.wire.push((self.now + LATENCY, self.seq, frame));
    }

    fn deliver(&mut self, frame: Frame) {
        if frame.dst.node == APP {
            self.call_node(|node, ctx| node.on_frame(ctx, frame));
        } else if frame.dst == Endpoint::kafka(BROKER) {
            match self.intercept(&frame) {
                Some(reply) => self.send(reply),
                None => self.call_broker(|broker, ctx| broker.on_frame(ctx, frame)),
            }
        } else if frame.dst == Endpoint::http(REGISTRY)
            && let Some(reply) = self.registry.answer(&frame)
        {
            self.send(reply);
        }
    }

    /// Answer `ApiVersions`, which must list the streams group heartbeat the
    /// fake broker does not serve, and the heartbeat itself.
    fn intercept(&mut self, frame: &Frame) -> Option<Frame> {
        let bytes = frame.payload.data()?;
        let mut cursor: &[u8] = bytes.get(4..)?;
        let api_key = i16::from_be_bytes(cursor.get(0..2)?.try_into().ok()?);
        let version = i16::from_be_bytes(cursor.get(2..4)?.try_into().ok()?);
        let reply = match api_key {
            ApiVersionsRequest::API_KEY => {
                let flexible = version >= ApiVersionsRequest::FLEXIBLE_MIN;
                let header =
                    RequestHeader::decode(&mut cursor, if flexible { 2 } else { 1 }).ok()?;
                frame_response(api_key, version, header.correlation_id, &api_versions())
            }
            StreamsGroupHeartbeatRequest::API_KEY => {
                let header = RequestHeader::decode(&mut cursor, 2).ok()?;
                let request = StreamsGroupHeartbeatRequest::decode(&mut cursor, version).ok()?;
                let answer =
                    self.coordinator
                        .answer(self.now, request, &mut self.state.borrow_mut());
                flexible_response(header.correlation_id, version, &answer)
            }
            _ => return None,
        };
        Some(frame.reply(Payload::Data(reply)))
    }

    /// Run the next frame or timer due by `until`; `false` when none is.
    fn step(&mut self, until: Millis) -> bool {
        let frame_at = self.wire.iter().map(|(at, seq, _)| (*at, *seq)).min();
        let next = [
            frame_at.map(|(at, _)| (at, 0)),
            self.broker_timer.map(|at| (at, 1)),
            self.node_timer.map(|at| (at, 2)),
        ]
        .into_iter()
        .flatten()
        .min()
        .filter(|(at, _)| *at <= until);
        let Some((at, which)) = next else {
            return false;
        };
        self.now = self.now.max(at);
        if self.spins.0 == self.now {
            self.spins.1 += 1;
            assert!(self.spins.1 < 100_000, "a busy loop at {} ms", self.now);
        } else {
            self.spins = (self.now, 0);
        }
        match which {
            0 => {
                let index = self
                    .wire
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, (at, seq, _))| (*at, *seq))
                    .map(|(i, _)| i)
                    .unwrap();
                let (_, _, frame) = self.wire.swap_remove(index);
                self.deliver(frame);
            }
            1 => {
                self.broker_timer = None;
                self.call_broker(FakeBroker::on_timer);
            }
            _ => {
                self.node_timer = None;
                self.call_node(StreamsNode::on_timer);
            }
        }
        true
    }

    fn run_for(&mut self, ms: Millis) {
        let until = self.now + ms;
        while self.step(until) {}
        self.now = until;
    }

    /// Step until `done` holds, for at most `max_ms`; whether it held.
    fn run_until(&mut self, max_ms: Millis, done: impl Fn(&Self) -> bool) -> bool {
        let until = self.now + max_ms;
        loop {
            if done(self) {
                return true;
            }
            if !self.step(until) {
                self.now = until;
                return done(self);
            }
        }
    }

    fn control(&mut self, command: Value) -> Result<Value, String> {
        self.call_node(|node, ctx| node.control(ctx, command))
    }

    /// Append JSON records with string keys, as a producer outside the test
    /// would.
    fn append(&self, topic: &str, partition: i32, records: &[(&str, Value, i64)]) {
        let batch: Vec<BatchRecord> = records
            .iter()
            .map(|(key, value, timestamp)| BatchRecord {
                timestamp: *timestamp,
                key: Some(Bytes::copy_from_slice(key.as_bytes())),
                value: Some(Bytes::from(serde_json::to_vec(value).unwrap())),
                headers: Vec::new(),
            })
            .collect();
        self.state
            .borrow_mut()
            .append_records(topic, partition, &batch);
    }

    fn read(&self, topic: &str, partition: i32) -> Vec<ConsumedRecord> {
        let batches = self.state.borrow().batches(topic, partition);
        records_of(topic, partition, &batches, 0).0
    }

    /// A partition's records as string keys and JSON values.
    fn json(&self, topic: &str, partition: i32) -> Vec<(String, Value)> {
        self.read(topic, partition)
            .iter()
            .map(|r| {
                (
                    key_of(r),
                    serde_json::from_slice(r.value.as_deref().unwrap_or(b"null")).unwrap(),
                )
            })
            .collect()
    }

    /// A count changelog partition: keys and 8-byte big-endian counts.
    fn counts(&self, topic: &str, partition: i32) -> Vec<(String, i64)> {
        self.read(topic, partition)
            .iter()
            .map(|r| {
                let value: [u8; 8] = r.value.as_deref().unwrap().try_into().unwrap();
                (key_of(r), i64::from_be_bytes(value))
            })
            .collect()
    }

    fn committed(&self, group: &str) -> BTreeMap<(String, i32), i64> {
        self.state
            .borrow()
            .groups
            .get(group)
            .map(|g| {
                g.committed
                    .iter()
                    .map(|(k, (offset, _))| (k.clone(), *offset))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The broker requests of one api.
    fn requests(&self, api_key: i16) -> usize {
        self.state.borrow().seen(api_key).len()
    }

    fn events_of(&self, kind: &str) -> Vec<Value> {
        self.events
            .iter()
            .filter(|(k, _)| *k == kind)
            .map(|(_, detail)| detail.clone())
            .collect()
    }
}

fn key_of(record: &ConsumedRecord) -> String {
    String::from_utf8_lossy(record.key.as_deref().unwrap_or_default()).into_owned()
}

fn owned(pairs: &[(&str, &[i32])]) -> Vec<RequestTaskIds> {
    pairs
        .iter()
        .map(|(s, ps)| RequestTaskIds {
            subtopology_id: (*s).to_string(),
            partitions: ps.to_vec(),
            ..Default::default()
        })
        .collect()
}

/// A heartbeat after the join that reports the owned tasks of each role.
fn report_roles(
    member_id: &str,
    epoch: i32,
    active: &[(&str, &[i32])],
    standby: &[(&str, &[i32])],
    warmup: &[(&str, &[i32])],
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".to_string(),
        member_id: member_id.to_string(),
        member_epoch: epoch,
        rebalance_timeout_ms: -1,
        active_tasks: Some(owned(active)),
        standby_tasks: Some(owned(standby)),
        warmup_tasks: Some(owned(warmup)),
        ..Default::default()
    }
}

/// A heartbeat after the join that reports the owned active tasks.
fn report(member_id: &str, epoch: i32, active: &[(&str, &[i32])]) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".to_string(),
        member_id: member_id.to_string(),
        member_epoch: epoch,
        rebalance_timeout_ms: -1,
        active_tasks: Some(owned(active)),
        standby_tasks: Some(Vec::new()),
        warmup_tasks: Some(Vec::new()),
        ..Default::default()
    }
}

fn counting_topology() -> Value {
    json!({
        "source": "orders",
        "ops": [
            { "op": "filter", "field": "total", "gt": 100 },
            { "op": "count_by_key", "store": "counts" },
        ],
        "sink": "order-counts",
    })
}

/// The counting app on `orders` (two partitions), with `extra` config.
fn counting_cluster(extra: &Value) -> Cluster {
    let mut config =
        json!({ "bootstrap": [1], "application_id": "app", "topology": counting_topology() });
    for (key, value) in extra.as_object().into_iter().flatten() {
        config[key] = value.clone();
    }
    Cluster::new(
        config,
        &[
            ("orders", 2),
            ("order-counts", 1),
            ("app-counts-changelog", 2),
        ],
    )
}

/// The settings and counters of the node's producer snapshot.
fn producer_counts(producer: &Value) -> Value {
    json!({
        "acks": producer["acks"],
        "idempotent": producer["idempotent"],
        "sent": producer["sent"],
        "acked": producer["acked"],
        "failed": producer["failed"],
        "pending_records": producer["pending_records"],
    })
}

fn count(key: &str, n: i64) -> (String, Value) {
    (key.to_string(), json!({ "key": key, "count": n }))
}

fn offsets(entries: &[(&str, i32, i64)]) -> BTreeMap<(String, i32), i64> {
    entries
        .iter()
        .map(|(topic, partition, offset)| (((*topic).to_string(), *partition), *offset))
        .collect()
}

fn task_ids(snapshot: &Value) -> Vec<Value> {
    snapshot["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|t| t["id"].clone())
        .collect()
}

#[test]
fn the_config_is_checked_at_load() {
    let topology = json!({ "source": "t", "ops": [], "sink": "u" });
    let with = |extra: Value| {
        let mut config = json!({ "bootstrap": [1], "application_id": "a", "topology": topology });
        for (key, value) in extra.as_object().into_iter().flatten() {
            config[key] = value.clone();
        }
        config
    };
    let cases = [
        (
            json!({ "application_id": "a", "topology": topology }),
            "config: missing field `bootstrap`",
        ),
        (
            with(json!({ "bogus": 1 })),
            "config: unknown field `bogus`, expected one of `bootstrap`, `application_id`, \
             `topology`, `commit_interval_ms`, `num_standby_replicas`, `deserialize`, `serialize`",
        ),
        (
            with(json!({ "bootstrap": [] })),
            "`bootstrap` needs at least one broker",
        ),
        (
            with(json!({ "application_id": "" })),
            "`application_id` is empty",
        ),
        (
            with(
                json!({ "topology": { "source": "t", "ops": [{ "op": "explode" }], "sink": "u" } }),
            ),
            "`topology.ops[0]`: unknown op `explode`",
        ),
        (
            with(json!({ "serialize": { "registry": 4, "format": "avro", "schema": "{" } })),
            "`serialize.schema`: the schema does not parse: Failed to parse schema from JSON",
        ),
    ];
    for (config, reason) in cases {
        let error = node(config.clone()).err().map(|e| e.to_string());
        assert!(
            error == Some(format!("node 9 (streams): {reason}")),
            "{config}"
        );
    }
    // The page's probe config takes the defaults.
    assert!(node(with(json!({}))).is_ok());
}

#[test]
fn a_fresh_node_describes_its_topology() {
    let node = node(json!({
        "bootstrap": [1],
        "application_id": "app",
        "topology": {
            "source": "orders",
            "ops": [{ "op": "select_key", "field": "customer" }, { "op": "count_by_key" }],
            "sink": "counts",
        },
    }))
    .unwrap();
    let mut snapshot = node.snapshot();
    assert!(snapshot.as_object_mut().unwrap().remove("client").is_some());
    let producer = snapshot
        .as_object_mut()
        .unwrap()
        .remove("producer")
        .unwrap();
    assert!(
        producer_counts(&producer)
            == json!({ "acks": -1, "idempotent": true, "sent": 0, "acked": 0, "failed": 0, "pending_records": 0 })
    );
    assert!(
        snapshot
            == json!({
                "state": "joining",
                "application_id": "app",
                "member_id": "",
                "member_epoch": 0,
                "membership": {
                    "member_id": "",
                    "process_id": "",
                    "member_epoch": 0,
                    "state": "joining",
                    "heartbeats": 0,
                    "heartbeat_interval_ms": 5_000,
                    "status": [],
                    "owned_active": [],
                    "owned_standby": [],
                },
                "error": null,
                "topology": {
                    "source": "orders",
                    "sink": "counts",
                    "subtopologies": [
                        {
                            "id": "0",
                            "source_topics": ["orders"],
                            "repartition_source_topics": [],
                            "repartition_sink_topics": ["app-count-by-key-1-repartition"],
                            "changelog_topics": [],
                        },
                        {
                            "id": "1",
                            "source_topics": [],
                            "repartition_source_topics": ["app-count-by-key-1-repartition"],
                            "repartition_sink_topics": [],
                            "changelog_topics": ["app-count-by-key-1-changelog"],
                        },
                    ],
                    "repartition_topics": ["app-count-by-key-1-repartition"],
                    "stores": [{
                        "name": "count-by-key-1",
                        "kind": "count",
                        "changelog": "app-count-by-key-1-changelog",
                    }],
                },
                "tasks": [],
                "stores": [],
                "records_in": 0,
                "records_out": 0,
                "commits": 0,
                "commit_interval_ms": 100,
                "paused": false,
                "last_outputs": [],
                "deserialize": null,
                "serialize": null,
            })
    );
}

#[test]
fn a_standby_setting_is_reported_as_unused() {
    let mut c = counting_cluster(&json!({ "num_standby_replicas": 1 }));
    c.start();
    assert!(
        c.events_of("streams_config")
            == vec![json!({
                "message": "num_standby_replicas is not used with the streams group protocol; \
                            the group's streams.num.standby.replicas decides",
                "level": "warn",
            })]
    );
}

/// The counting app after it counted a, b (filtered) and a on partition 0,
/// then c on partition 1, and committed both.
fn counted() -> Cluster {
    let mut c = counting_cluster(&json!({}));
    c.append(
        "orders",
        0,
        &[
            ("a", json!({ "total": 150 }), 1_000),
            ("b", json!({ "total": 50 }), 1_001),
            ("a", json!({ "total": 200 }), 1_002),
        ],
    );
    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    c.start();
    // Only a partition the task processed from is committed.
    assert!(c.run_until(5_000, |c| c.committed("app")
        == offsets(&[("orders", 0, 3)])));
    c.append("orders", 1, &[("c", json!({ "total": 500 }), 1_003)]);
    let done = offsets(&[("orders", 0, 3), ("orders", 1, 1)]);
    assert!(c.run_until(5_000, |c| c.committed("app") == done));
    // The last commit answer reaches the node.
    c.run_for(100);
    c
}

#[test]
fn the_node_joins_counts_and_commits() {
    let c = counted();
    // The filter drops b; the counts go to the sink and to the changelog
    // partition of each task.
    assert!(c.json("order-counts", 0) == vec![count("a", 1), count("a", 2), count("c", 1)]);
    assert!(
        c.counts("app-counts-changelog", 0) == vec![("a".to_string(), 1), ("a".to_string(), 2)]
    );
    assert!(c.counts("app-counts-changelog", 1) == vec![("c".to_string(), 1)]);

    // The join carries the topology; the next heartbeat reports the tasks.
    let snapshot = c.node.snapshot();
    let member_id = snapshot["member_id"].as_str().unwrap().to_string();
    let process_id = snapshot["membership"]["process_id"]
        .as_str()
        .unwrap()
        .to_string();
    let spec = TopologySpec::parse(&counting_topology()).unwrap();
    let wire = CompiledTopology::new("app", &spec)
        .unwrap()
        .built
        .to_wire_request();
    let heartbeats: Vec<StreamsGroupHeartbeatRequest> =
        c.coordinator.seen.iter().map(|(_, r)| r.clone()).collect();
    assert!(
        heartbeats[..2]
            == [
                StreamsGroupHeartbeatRequest {
                    group_id: "app".to_string(),
                    member_id: member_id.clone(),
                    member_epoch: 0,
                    process_id: Some(process_id.clone()),
                    rebalance_timeout_ms: 300_000,
                    topology: Some(wire),
                    client_tags: Some(Vec::new()),
                    active_tasks: Some(Vec::new()),
                    standby_tasks: Some(Vec::new()),
                    warmup_tasks: Some(Vec::new()),
                    ..Default::default()
                },
                report(&member_id, 1, &[("0", &[0, 1])]),
            ]
    );
    // Kafka Streams' member id is a URL-safe base64 UUID, its process id a
    // hyphenated one.
    assert!(member_id.len() == 22);
    assert!(process_id.len() == 36);
}

#[test]
fn the_snapshot_shows_the_member_its_tasks_and_their_stores() {
    let c = counted();
    let mut snapshot = c.node.snapshot();
    let member_id = snapshot["member_id"].clone();
    let process_id = snapshot["membership"]["process_id"].clone();
    let object = snapshot.as_object_mut().unwrap();
    assert!(object.remove("client").is_some());
    // Three counts and their changelog records, all acknowledged.
    let producer = object.remove("producer").unwrap();
    assert!(
        producer_counts(&producer)
            == json!({ "acks": -1, "idempotent": true, "sent": 6, "acked": 6, "failed": 0, "pending_records": 0 })
    );
    assert!(
        object.remove("membership")
            == Some(json!({
                "member_id": member_id,
                "process_id": process_id,
                "member_epoch": 1,
                "state": "stable",
                "heartbeats": c.coordinator.seen.len(),
                "heartbeat_interval_ms": 1_000,
                "status": [],
                "owned_active": ["0_0", "0_1"],
                "owned_standby": [],
            }))
    );
    let output = |key: &str, n: i64, timestamp: i64| {
        json!({
            "topic": "order-counts",
            "key": key,
            "value": { "key": key, "count": n },
            "timestamp": timestamp,
        })
    };
    let task = |id: &str, partition: i32, piped: u64, out: u64| {
        json!({
            "id": id,
            "role": "active",
            "phase": "running",
            "partitions": [format!("orders-{partition}"), format!("app-counts-changelog-{partition}")],
            "records_in": piped,
            "records_out": out,
            "changelog_out": out,
            "skipped": 0,
            "restored": 0,
            "lag": 0,
            "buffered": 0,
        })
    };
    let store = |id: &str, entries: Value| json!({ "name": "counts", "task": id, "changelog": "app-counts-changelog", "entries": entries });
    assert!(
        snapshot
            == json!({
                "state": "running",
                "application_id": "app",
                "member_id": member_id,
                "member_epoch": 1,
                "error": null,
                "topology": {
                    "source": "orders",
                    "sink": "order-counts",
                    "subtopologies": [{
                        "id": "0",
                        "source_topics": ["orders"],
                        "repartition_source_topics": [],
                        "repartition_sink_topics": [],
                        "changelog_topics": ["app-counts-changelog"],
                    }],
                    "repartition_topics": [],
                    "stores": [{ "name": "counts", "kind": "count", "changelog": "app-counts-changelog" }],
                },
                "tasks": [task("0_0", 0, 3, 2), task("0_1", 1, 1, 1)],
                "stores": [store("0_0", json!([["a", 2]])), store("0_1", json!([["c", 1]]))],
                "records_in": 4,
                "records_out": 3,
                "commits": c.requests(OffsetCommitRequest::API_KEY),
                "commit_interval_ms": 100,
                "paused": false,
                "last_outputs": [output("a", 1, 1_000), output("a", 2, 1_002), output("c", 1, 1_003)],
                "deserialize": null,
                "serialize": null,
            })
    );
}

#[test]
fn queries_read_the_local_stores() {
    let mut c = counted();
    let cases = [
        (
            json!({ "cmd": "query", "store": "counts", "key": "a" }),
            Ok(json!({ "store": "counts", "key": "a", "task": "0_0", "value": 2 })),
        ),
        (
            json!({ "cmd": "query", "store": "counts", "key": "zzz" }),
            Ok(json!({ "store": "counts", "key": "zzz", "found": false })),
        ),
        (
            json!({ "cmd": "query", "store": "nope", "key": "a" }),
            Err("the topology has no store `nope`".to_string()),
        ),
        (
            json!({ "cmd": "query", "key": "a" }),
            Err("missing `store`".to_string()),
        ),
        (
            json!({ "cmd": "seek" }),
            Err("unknown streams command Some(\"seek\")".to_string()),
        ),
    ];
    for (command, expected) in cases {
        assert!(c.control(command.clone()) == expected, "{command}");
    }
}

#[test]
fn a_paused_node_holds_its_records_until_it_resumes() {
    let mut c = counting_cluster(&json!({}));
    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    c.start();
    assert!(c.run_until(5_000, |c| c.node.snapshot()["state"] == "running"));
    assert!(c.control(json!({ "cmd": "pause" })) == Ok(json!({ "paused": true })));
    c.append("orders", 0, &[("a", json!({ "total": 150 }), 1)]);
    c.run_for(3_000);
    assert!(c.json("order-counts", 0).is_empty());
    assert!(c.node.snapshot()["state"] == "paused");
    // The member keeps its heartbeats while paused.
    assert!(c.coordinator.seen.len() >= 4);
    assert!(c.control(json!({ "cmd": "resume" })) == Ok(json!({ "paused": false })));
    assert!(c.run_until(5_000, |c| c.json("order-counts", 0) == vec![count("a", 1)]));
}

#[test]
fn a_revoked_task_commits_and_closes_before_the_member_reports_it() {
    // A long commit interval, so only the revocation commits.
    let mut c = counting_cluster(&json!({ "commit_interval_ms": 60_000 }));
    c.append("orders", 0, &[("a", json!({ "total": 150 }), 1)]);
    c.append(
        "orders",
        1,
        &[
            ("c", json!({ "total": 150 }), 2),
            ("c", json!({ "total": 300 }), 3),
        ],
    );
    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    c.start();
    assert!(c.run_until(5_000, |c| c.json("order-counts", 0).len() == 3));
    assert!(c.committed("app").is_empty());
    let member_id = c.node.snapshot()["member_id"].as_str().unwrap().to_string();

    c.coordinator.next = Some(vec![("0", vec![0])]);
    let revoked = report(&member_id, 2, &[("0", &[0])]);
    assert!(c.run_until(5_000, |c| {
        c.coordinator.seen.iter().any(|(_, r)| *r == revoked)
    }));
    // The member committed every task's progress, then closed the task, and
    // only then reported it gone.
    assert!(c.committed("app") == offsets(&[("orders", 0, 1), ("orders", 1, 2)]));
    let committed_at: Vec<Millis> = c
        .state
        .borrow()
        .seen(OffsetCommitRequest::API_KEY)
        .iter()
        .map(|s| s.at)
        .collect();
    let reported_at = c
        .coordinator
        .seen
        .iter()
        .find(|(_, r)| *r == revoked)
        .map(|(at, _)| *at)
        .unwrap();
    assert!(committed_at.len() == 1);
    assert!(committed_at[0] < reported_at);
    assert!(c.events_of("tasks_revoked") == vec![json!({ "tasks": ["0_1"] })]);
    assert!(task_ids(&c.node.snapshot()) == vec![json!("0_0")]);

    // The partition it gave up is no longer read.
    c.append("orders", 1, &[("c", json!({ "total": 999 }), 4)]);
    c.append("orders", 0, &[("a", json!({ "total": 999 }), 5)]);
    assert!(c.run_until(5_000, |c| c.json("order-counts", 0).len() == 4));
    c.run_for(2_000);
    assert!(
        c.json("order-counts", 0)
            == vec![count("a", 1), count("c", 1), count("c", 2), count("a", 2)]
    );
}

#[test]
fn a_fenced_member_loses_its_tasks_rejoins_and_restores_them() {
    let mut c = counting_cluster(&json!({}));
    c.append(
        "orders",
        0,
        &[
            ("a", json!({ "total": 150 }), 1),
            ("a", json!({ "total": 160 }), 2),
        ],
    );
    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    c.start();
    assert!(c.run_until(5_000, |c| c.committed("app")
        == offsets(&[("orders", 0, 2)])));
    let restored_before = c.events_of("task_restored").len();

    c.coordinator.error = Some(codes::FENCED_MEMBER_EPOCH);
    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    let joins = |c: &Cluster| {
        c.coordinator
            .seen
            .iter()
            .filter(|(_, r)| r.member_epoch == 0)
            .count()
    };
    assert!(c.run_until(5_000, |c| joins(c) == 2));
    assert!(
        c.events_of("streams_fenced")
            == vec![
                json!({ "code": codes::FENCED_MEMBER_EPOCH, "lost": ["0_0", "0_1"], "level": "warn" })
            ]
    );

    // Back in the group, the tasks restore their counts from the changelog
    // and count on from the committed offsets.
    c.append("orders", 0, &[("a", json!({ "total": 170 }), 3)]);
    assert!(c.run_until(5_000, |c| c.json("order-counts", 0).len() == 3));
    assert!(c.json("order-counts", 0) == vec![count("a", 1), count("a", 2), count("a", 3)]);
    assert!(
        c.events_of("task_restored")[restored_before..]
            == [
                json!({ "task": "0_1", "records": 0 }),
                json!({ "task": "0_0", "records": 2 }),
            ]
    );
}

#[test]
fn a_restarted_node_restores_its_counts_and_goes_on() {
    let mut c = counting_cluster(&json!({}));
    c.append(
        "orders",
        0,
        &[
            ("a", json!({ "total": 150 }), 1),
            ("a", json!({ "total": 150 }), 2),
        ],
    );
    c.append("orders", 1, &[("c", json!({ "total": 150 }), 3)]);
    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    c.start();
    let done = offsets(&[("orders", 0, 2), ("orders", 1, 1)]);
    assert!(c.run_until(5_000, |c| c.committed("app") == done));
    let before = c.node.snapshot();

    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    c.restart();
    let mark = c.events.len();
    c.append("orders", 0, &[("a", json!({ "total": 150 }), 4)]);
    c.append("orders", 1, &[("c", json!({ "total": 150 }), 5)]);
    assert!(c.run_until(5_000, |c| c.json("order-counts", 0).len() == 5));
    assert!(
        c.json("order-counts", 0)
            == vec![
                count("a", 1),
                count("a", 2),
                count("c", 1),
                count("a", 3),
                count("c", 2)
            ]
    );
    let after = c.node.snapshot();
    // A new member of the same process.
    assert!(after["member_id"] != before["member_id"]);
    assert!(after["membership"]["process_id"] == before["membership"]["process_id"]);
    let restored: Vec<Value> = c.events[mark..]
        .iter()
        .filter(|(kind, _)| *kind == "task_restored")
        .map(|(_, detail)| detail.clone())
        .collect();
    assert!(
        restored
            == vec![
                json!({ "task": "0_0", "records": 2 }),
                json!({ "task": "0_1", "records": 1 }),
            ]
    );
    assert!(
        after["stores"]
            == json!([
                { "name": "counts", "task": "0_0", "changelog": "app-counts-changelog", "entries": [["a", 3]] },
                { "name": "counts", "task": "0_1", "changelog": "app-counts-changelog", "entries": [["c", 2]] },
            ])
    );
}

#[test]
fn a_select_key_goes_through_the_repartition_topic() {
    let mut c = Cluster::new(
        json!({
            "bootstrap": [1],
            "application_id": "app",
            "topology": {
                "source": "orders",
                "ops": [
                    { "op": "select_key", "field": "customer" },
                    { "op": "count_by_key", "store": "per-customer" },
                ],
                "sink": "customer-counts",
            },
        }),
        &[
            ("orders", 2),
            ("customer-counts", 1),
            ("app-per-customer-repartition", 2),
            ("app-per-customer-changelog", 2),
        ],
    );
    let placed = [("o1", "alice", 0), ("o2", "bob", 0), ("o3", "alice", 1)];
    for (i, (order, customer, partition)) in placed.iter().enumerate() {
        let timestamp = i64::try_from(i).unwrap();
        c.append(
            "orders",
            *partition,
            &[(order, json!({ "customer": customer }), timestamp)],
        );
    }
    c.coordinator.next = Some(vec![("0", vec![0, 1]), ("1", vec![0, 1])]);
    c.start();
    assert!(c.run_until(10_000, |c| c.json("customer-counts", 0).len() == 3));

    // The re-keyed records go to the partition murmur2 picks for the new
    // key, and the task of that partition counts them into its changelog
    // partition.
    let mut repartition: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    let mut changelog: BTreeMap<i32, Vec<(String, i64)>> = BTreeMap::new();
    let mut counted: BTreeMap<&str, i64> = BTreeMap::new();
    for (_, customer, _) in placed {
        let partition = partition_for_key(customer.as_bytes(), 2).unwrap();
        repartition
            .entry(partition)
            .or_default()
            .push(customer.to_string());
        let n = counted.entry(customer).or_default();
        *n += 1;
        changelog
            .entry(partition)
            .or_default()
            .push((customer.to_string(), *n));
    }
    let read_keys = |c: &Cluster, topic: &str| -> BTreeMap<i32, Vec<String>> {
        (0..2)
            .map(|p| (p, c.read(topic, p).iter().map(key_of).collect::<Vec<_>>()))
            .filter(|(_, keys)| !keys.is_empty())
            .collect()
    };
    assert!(read_keys(&c, "app-per-customer-repartition") == repartition);
    let changelog_read: BTreeMap<i32, Vec<(String, i64)>> = (0..2)
        .map(|p| (p, c.counts("app-per-customer-changelog", p)))
        .filter(|(_, counts)| !counts.is_empty())
        .collect();
    assert!(changelog_read == changelog);
    // The tasks of the second subtopology write to the sink in task order,
    // so the order across their partitions is theirs.
    let mut sink = c.json("customer-counts", 0);
    sink.sort_by_key(|(key, value)| (key.clone(), value["count"].as_i64()));
    assert!(sink == vec![count("alice", 1), count("alice", 2), count("bob", 1)]);
    // Every source partition is committed, the repartition topic's too.
    let mut expected = offsets(&[("orders", 0, 2), ("orders", 1, 1)]);
    for (partition, keys) in &repartition {
        expected.insert(
            ("app-per-customer-repartition".to_string(), *partition),
            i64::try_from(keys.len()).unwrap(),
        );
    }
    assert!(c.run_until(5_000, |c| c.committed("app") == expected));
}

#[test]
fn values_decode_and_encode_through_the_registry() {
    let order_schema =
        r#"{"type":"record","name":"Order","fields":[{"name":"total","type":"long"}]}"#;
    let count_schema = r#"{"type":"record","name":"Count","fields":[{"name":"key","type":"string"},{"name":"count","type":"long"}]}"#;
    let mut c = Cluster::new(
        json!({
            "bootstrap": [1],
            "application_id": "app",
            "topology": counting_topology(),
            "deserialize": { "registry": 4 },
            "serialize": { "registry": 4, "format": "avro", "schema": count_schema },
        }),
        &[
            ("orders", 1),
            ("order-counts", 1),
            ("app-counts-changelog", 1),
        ],
    );
    c.registry
        .schemas
        .insert(1, json!({ "schema": order_schema }));
    c.registry.next_id = 2;
    let orders = ValueSchema::parse(SchemaFormat::Avro, order_schema).unwrap();
    let framed = |key: &str, total: i64| BatchRecord {
        timestamp: total,
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value: Some(frame(
            1,
            &orders.encode(&json!({ "total": total })).unwrap(),
        )),
        headers: Vec::new(),
    };
    c.state.borrow_mut().append_records(
        "orders",
        0,
        &[framed("a", 150), framed("a", 50), framed("b", 300)],
    );
    c.coordinator.next = Some(vec![("0", vec![0])]);
    c.start();
    assert!(c.run_until(5_000, |c| c.read("order-counts", 0).len() == 2));

    // The node registered the sink schema under the sink's value subject and
    // frames every sink value with the id it got.
    assert!(
        c.registry.registered
            == vec![(
                "order-counts-value".to_string(),
                json!({ "schema": count_schema })
            )]
    );
    let counts = ValueSchema::parse(SchemaFormat::Avro, count_schema).unwrap();
    let sink: Vec<(i32, String, Value)> = c
        .read("order-counts", 0)
        .iter()
        .map(|r| {
            let (id, body) = unframe(r.value.as_deref().unwrap()).unwrap();
            (id, key_of(r), counts.decode(body).unwrap())
        })
        .collect();
    assert!(
        sink == vec![
            (2, "a".to_string(), json!({ "key": "a", "count": 1 })),
            (2, "b".to_string(), json!({ "key": "b", "count": 1 })),
        ]
    );
    // Repartition and changelog records stay the node's own format.
    assert!(
        c.counts("app-counts-changelog", 0) == vec![("a".to_string(), 1), ("b".to_string(), 1)]
    );
    let snapshot = c.node.snapshot();
    let registry_client = json!({
        "registry": 4,
        "connected": true,
        "pending": 0,
        "requests": 1,
        "failures": 0,
        "last_error": null,
    });
    assert!(
        snapshot["deserialize"]
            == json!({ "registry": 4, "schemas": { "1": "ready (avro)" }, "client": registry_client })
    );
    assert!(
        snapshot["serialize"]
            == json!({
                "registry": 4,
                "subject": "order-counts-value",
                "format": "avro",
                "state": "ready",
                "schema_id": 2,
                "failed": 0,
                "error": null,
                "client": registry_client,
            })
    );
    assert!(
        c.events_of("schema_registered")
            == vec![json!({ "subject": "order-counts-value", "id": 2 })]
    );
}

#[test]
fn standbys_follow_their_changelogs_and_keep_what_they_restored() {
    let mut c = counting_cluster(&json!({}));
    // Another member's task 0_1 counted z up to 5 in the changelog.
    let logged = BatchRecord {
        timestamp: 1,
        key: Some(Bytes::from_static(b"z")),
        value: Some(Bytes::copy_from_slice(&5_i64.to_be_bytes())),
        headers: Vec::new(),
    };
    c.state
        .borrow_mut()
        .append_records("app-counts-changelog", 1, &[logged]);
    c.coordinator.next = Some(vec![("0", vec![0])]);
    c.coordinator.next_standby = vec![("0", vec![1])];
    c.start();
    let member_id = |c: &Cluster| c.node.snapshot()["member_id"].as_str().unwrap().to_string();
    let reported = |c: &Cluster, request: &StreamsGroupHeartbeatRequest| {
        c.coordinator.seen.iter().any(|(_, r)| r == request)
    };
    assert!(c.run_until(5_000, |c| reported(
        c,
        &report_roles(&member_id(c), 1, &[("0", &[0])], &[("0", &[1])], &[])
    )));
    // The standby follows its changelog partition and never processes.
    let standby_store = json!({
        "name": "counts", "task": "0_1", "changelog": "app-counts-changelog", "entries": [["z", 5]],
    });
    assert!(c.run_until(5_000, |c| c.node.snapshot()["stores"][1] == standby_store));
    assert!(c.node.snapshot()["tasks"][1]["phase"] == "standby");

    // Promoted, it counts on from what it restored.
    c.coordinator.next = Some(vec![("0", vec![0, 1])]);
    c.append("orders", 1, &[("z", json!({ "total": 150 }), 2)]);
    assert!(c.run_until(5_000, |c| c.json("order-counts", 0) == vec![count("z", 6)]));

    // Demoted, it closes as an active task and opens again as a standby.
    c.coordinator.next = Some(vec![("0", vec![0])]);
    c.coordinator.next_standby = vec![("0", vec![1])];
    assert!(c.run_until(5_000, |c| reported(
        c,
        &report_roles(&member_id(c), 3, &[("0", &[0])], &[("0", &[1])], &[])
    )));
    assert!(c.committed("app") == offsets(&[("orders", 1, 1)]));
    assert!(c.node.snapshot()["tasks"][1]["phase"] == "standby");

    // A warm-up runs as a standby and is reported as a warm-up.
    c.coordinator.next = Some(vec![("0", vec![0])]);
    c.coordinator.next_warmup = vec![("0", vec![1])];
    assert!(c.run_until(5_000, |c| reported(
        c,
        &report_roles(&member_id(c), 4, &[("0", &[0])], &[], &[("0", &[1])])
    )));
    assert!(
        c.events_of("tasks_assigned")
            == vec![
                json!({ "tasks": ["0_0", "0_1 (standby)"] }),
                json!({ "tasks": ["0_1"] }),
                json!({ "tasks": ["0_1 (standby)"] }),
            ]
    );
}
