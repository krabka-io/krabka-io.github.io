//! The scenario's admin client: a node that creates the scenario's topics
//! with real `CreateTopics` requests, and creates or deletes topics on
//! command from the page.
//!
//! Config: `{ "bootstrap": [node ids], "topics": [TopicSpec...] }`. The node
//! connects at start and sends one `CreateTopics` per topic to the controller
//! the metadata names, with `timeout_ms` 30 000, as `kafka-topics --create`
//! does. `NOT_CONTROLLER`, `COORDINATOR_NOT_AVAILABLE`, the other retriable
//! codes and a lost connection make it try again 500 ms later. A scenario
//! topic whose replica count fits its bootstrap brokers also retries an early
//! `INVALID_REPLICATION_FACTOR` while those brokers register.
//!
//! Commands: `{"cmd":"create_topic","name":..,"partitions":..,"replication_factor":..}`
//! and `{"cmd":"delete_topic","name":..}` queue the work and answer at once;
//! the snapshot shows the outcome.
//!
//! Snapshot: `{"topics":[{"name","partitions","replication_factor","status":"pending|created|exists|failed|deleting|deleted","error"}],"client":{..}}`.
//! Events: `topics_created` once every topic of the config exists,
//! `topic_created`, `topic_deleted` and `admin_error` per outcome.

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    create_topics_response::CreateTopicsResponse,
    delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    delete_topics_response::DeleteTopicsResponse,
};
use serde_json::{Value, json};

use crate::lab::{
    LabError,
    client::{ClientEvent, ClientOptions, KafkaClient, RequestId, Target, error_class},
    codes, config_field, config_field_or,
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId},
    scenario::{NodeSpec, TopicSpec},
};

/// The `timeout_ms` of the admin requests.
const ADMIN_TIMEOUT_MS: i32 = 30_000;
/// The wait before a failed request goes out again.
const RETRY_MS: Millis = 500;

/// Where a topic of the admin node is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Pending,
    Created,
    Exists,
    Failed,
    Deleting,
    Deleted,
}

impl Status {
    const fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Created => "created",
            Self::Exists => "exists",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }

    const fn is_done(self) -> bool {
        matches!(self, Self::Created | Self::Exists)
    }
}

/// One topic the node manages.
struct Managed {
    spec: TopicSpec,
    status: Status,
    error: Option<i16>,
    request: Option<RequestId>,
    retry_at: Millis,
    attempts: u32,
    /// The topic came from the config, so it counts for `topics_created`.
    from_config: bool,
}

/// The admin node. See the module documentation.
pub struct AdminNode {
    id: NodeId,
    bootstrap: Vec<NodeId>,
    client: KafkaClient,
    topics: BTreeMap<String, Managed>,
    /// `topics_created` was recorded.
    announced: bool,
}

impl AdminNode {
    /// # Errors
    /// Returns a config error when `bootstrap` is missing or empty, or a
    /// topic spec does not parse.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let bootstrap: Vec<NodeId> = config_field(spec, "bootstrap")?;
        if bootstrap.is_empty() {
            return Err(LabError::config(
                spec,
                "`bootstrap` needs at least one broker",
            ));
        }
        let topics: Vec<TopicSpec> = config_field_or(spec, "topics", Vec::new())?;
        for key in spec
            .config
            .as_object()
            .map(|o| o.keys())
            .into_iter()
            .flatten()
        {
            if key != "bootstrap" && key != "topics" {
                return Err(LabError::config(
                    spec,
                    format!("unknown config field `{key}`"),
                ));
            }
        }
        let client = Self::build_client(&bootstrap, spec.id);
        Ok(Self {
            id: spec.id,
            bootstrap,
            client,
            topics: topics
                .into_iter()
                .map(|t| {
                    (
                        t.name.clone(),
                        Managed {
                            spec: t,
                            status: Status::Pending,
                            error: None,
                            request: None,
                            retry_at: 0,
                            attempts: 0,
                            from_config: true,
                        },
                    )
                })
                .collect(),
            announced: false,
        })
    }

    fn build_client(bootstrap: &[NodeId], id: NodeId) -> KafkaClient {
        KafkaClient::new(
            bootstrap.iter().map(|n| Endpoint::kafka(*n)).collect(),
            &format!("admin-{id}"),
            ClientOptions::default(),
        )
    }

    fn drive(&mut self, ctx: &mut Ctx<'_>, events: Vec<ClientEvent>) {
        for event in events {
            if let ClientEvent::Response { id, result } = event {
                self.on_response(ctx, id, result);
            }
        }
        self.send_due(ctx);
        self.announce(ctx);
        let deadline = self
            .client
            .next_deadline(ctx.now())
            .into_iter()
            .chain(
                self.topics
                    .values()
                    .filter(|t| {
                        t.request.is_none()
                            && matches!(t.status, Status::Pending | Status::Deleting)
                    })
                    .map(|t| t.retry_at),
            )
            .min();
        if let Some(at) = deadline {
            ctx.arm(at.max(ctx.now()));
        }
    }

    fn send_due(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let due: Vec<String> = self
            .topics
            .iter()
            .filter(|(_, t)| {
                t.request.is_none()
                    && matches!(t.status, Status::Pending | Status::Deleting)
                    && now >= t.retry_at
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            let Some(topic) = self.topics.get_mut(&name) else {
                continue;
            };
            topic.attempts += 1;
            let id = if topic.status == Status::Deleting {
                let request = DeleteTopicsRequest {
                    topics: vec![DeleteTopicState {
                        name: Some(name.clone()),
                        ..Default::default()
                    }],
                    topic_names: vec![name.clone()],
                    timeout_ms: ADMIN_TIMEOUT_MS,
                    ..Default::default()
                };
                self.client.send(ctx, Target::Controller, request)
            } else {
                let request = CreateTopicsRequest {
                    topics: vec![CreatableTopic {
                        name: name.clone(),
                        num_partitions: topic.spec.partitions,
                        replication_factor: topic.spec.replication_factor,
                        assignments: Vec::new(),
                        configs: topic
                            .spec
                            .configs
                            .iter()
                            .map(|(k, v)| CreatableTopicConfig {
                                name: k.clone(),
                                value: Some(v.clone()),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    timeout_ms: ADMIN_TIMEOUT_MS,
                    validate_only: false,
                    ..Default::default()
                };
                self.client.send(ctx, Target::Controller, request)
            };
            if let Some(topic) = self.topics.get_mut(&name) {
                topic.request = Some(id);
            }
        }
    }

    fn on_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: RequestId,
        result: Result<crate::lab::client::Response, crate::lab::client::ClientError>,
    ) {
        let Some(name) = self
            .topics
            .iter()
            .find(|(_, t)| t.request == Some(id))
            .map(|(name, _)| name.clone())
        else {
            return;
        };
        let now = ctx.now();
        let Ok(response) = result else {
            // A lost connection or a timeout: try again after the backoff.
            self.client.request_metadata_refresh();
            if let Some(topic) = self.topics.get_mut(&name) {
                topic.request = None;
                topic.retry_at = now + RETRY_MS;
            }
            return;
        };
        let code = if response.is::<CreateTopicsResponse>() {
            response
                .downcast::<CreateTopicsResponse>()
                .and_then(|r| {
                    r.topics
                        .iter()
                        .find(|t| t.name == name)
                        .map(|t| t.error_code)
                })
                .unwrap_or(codes::UNKNOWN_SERVER_ERROR)
        } else {
            response
                .downcast::<DeleteTopicsResponse>()
                .and_then(|r| {
                    r.responses
                        .iter()
                        .find(|t| t.name.as_deref() == Some(name.as_str()))
                        .map(|t| t.error_code)
                })
                .unwrap_or(codes::UNKNOWN_SERVER_ERROR)
        };
        let Some(topic) = self.topics.get_mut(&name) else {
            return;
        };
        topic.request = None;
        let deleting = topic.status == Status::Deleting;
        match code {
            codes::NONE => {
                topic.status = if deleting {
                    Status::Deleted
                } else {
                    Status::Created
                };
                topic.error = None;
                ctx.event(
                    if deleting {
                        "topic_deleted"
                    } else {
                        "topic_created"
                    },
                    json!({ "topic": name, "partitions": topic.spec.partitions }),
                );
            }
            codes::TOPIC_ALREADY_EXISTS if !deleting => {
                topic.status = Status::Exists;
                topic.error = None;
                ctx.event("topic_created", json!({ "topic": name, "existed": true }));
            }
            codes::UNKNOWN_TOPIC_OR_PARTITION if deleting => {
                topic.status = Status::Deleted;
                topic.error = None;
                ctx.event("topic_deleted", json!({ "topic": name, "existed": false }));
            }
            code if error_class(code).is_retriable()
                || code == codes::NOT_CONTROLLER
                || (code == codes::INVALID_REPLICATION_FACTOR
                    && !deleting
                    && topic.from_config
                    && topic.spec.replication_factor > 0
                    && topic.spec.replication_factor as usize <= self.bootstrap.len()
                    && u64::from(topic.attempts) * RETRY_MS <= ADMIN_TIMEOUT_MS as u64) =>
            {
                self.client.note_error(code, &Target::Controller);
                topic.retry_at = now + RETRY_MS;
                topic.error = Some(code);
                ctx.event(
                    "admin_retry",
                    json!({ "topic": name, "code": code, "level": "warn" }),
                );
            }
            code => {
                topic.status = Status::Failed;
                topic.error = Some(code);
                ctx.event(
                    "admin_error",
                    json!({ "topic": name, "code": code, "level": "error" }),
                );
            }
        }
    }

    fn announce(&mut self, ctx: &mut Ctx<'_>) {
        if self.announced {
            return;
        }
        let from_config: Vec<&Managed> = self.topics.values().filter(|t| t.from_config).collect();
        if from_config.is_empty() || !from_config.iter().all(|t| t.status.is_done()) {
            return;
        }
        self.announced = true;
        let names: Vec<&str> = from_config.iter().map(|t| t.spec.name.as_str()).collect();
        ctx.event("topics_created", json!({ "topics": names }));
    }
}

impl Node for AdminNode {
    fn kind(&self) -> &'static str {
        "admin"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.client = Self::build_client(&self.bootstrap, self.id);
        for topic in self.topics.values_mut() {
            topic.request = None;
            topic.retry_at = 0;
            if matches!(topic.status, Status::Failed) {
                topic.status = Status::Pending;
            }
        }
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        let events = self.client.on_frame(ctx, frame);
        self.drive(ctx, events);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let name = command
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "missing `name`".to_string())?
            .to_string();
        match command.get("cmd").and_then(Value::as_str) {
            Some("create_topic") => {
                let partitions = command
                    .get("partitions")
                    .and_then(Value::as_i64)
                    .and_then(|p| i32::try_from(p).ok())
                    .unwrap_or(1);
                let replication_factor = command
                    .get("replication_factor")
                    .and_then(Value::as_i64)
                    .and_then(|r| i16::try_from(r).ok())
                    .unwrap_or(-1);
                self.topics.insert(
                    name.clone(),
                    Managed {
                        spec: TopicSpec {
                            name: name.clone(),
                            partitions,
                            replication_factor,
                            configs: BTreeMap::new(),
                        },
                        status: Status::Pending,
                        error: None,
                        request: None,
                        retry_at: 0,
                        attempts: 0,
                        from_config: false,
                    },
                );
                self.drive(ctx, Vec::new());
                Ok(json!({ "topic": name, "status": "pending" }))
            }
            Some("delete_topic") => {
                let topic = self.topics.entry(name.clone()).or_insert_with(|| Managed {
                    spec: TopicSpec {
                        name: name.clone(),
                        partitions: 1,
                        replication_factor: -1,
                        configs: BTreeMap::new(),
                    },
                    status: Status::Deleting,
                    error: None,
                    request: None,
                    retry_at: 0,
                    attempts: 0,
                    from_config: false,
                });
                topic.status = Status::Deleting;
                topic.request = None;
                topic.retry_at = 0;
                self.drive(ctx, Vec::new());
                Ok(json!({ "topic": name, "status": "deleting" }))
            }
            other => Err(format!("unknown admin command {other:?}")),
        }
    }

    fn snapshot(&self) -> Value {
        let topics: Vec<Value> = self
            .topics
            .values()
            .map(|t| {
                json!({
                    "name": t.spec.name,
                    "partitions": t.spec.partitions,
                    "replication_factor": t.spec.replication_factor,
                    "status": t.status.name(),
                    "error": t.error,
                    "attempts": t.attempts,
                })
            })
            .collect();
        json!({ "topics": topics, "client": self.client.snapshot() })
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    use assert2::assert;
    use krabka_protocol::{
        ProtocolRequest,
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        },
        primitives::uuid::Uuid,
    };
    use serde_json::{Value, json};

    use super::AdminNode;
    use crate::lab::{
        LabError,
        client::fake_broker::{ClusterState, FakeBroker, Seen},
        codes,
        net::{Frame, Millis, Node, NodeId, TimedFrame},
        scenario::{NodeSpec, Scenario},
        testing::CtxBuffers,
        world::World,
    };

    const ADMIN: NodeId = NodeId(10);

    /// The link latency between the admin and the brokers, each way.
    const LATENCY: Millis = 5;

    /// A world that hosts only the admin node, with three fake brokers
    /// outside it: frames for a broker leave through the egress, as they
    /// would for a broker another tab hosts, reach it at their `deliver_at`,
    /// and the answers come back through the ingress one link latency later.
    struct Remote {
        world: World,
        brokers: BTreeMap<NodeId, (FakeBroker, CtxBuffers)>,
        state: Rc<RefCell<ClusterState>>,
        /// Frames on their way to a broker.
        outbound: Vec<TimedFrame>,
        /// Answers on their way back.
        inbound: Vec<Answer>,
    }

    /// A broker's answer on its way back, with its arrival time.
    type Answer = (Millis, Frame);

    impl Remote {
        fn new(config: Value) -> Self {
            let state = ClusterState::new(&[(1, 1), (2, 2), (3, 3)]);
            let scenario = Scenario {
                nodes: vec![NodeSpec::new(ADMIN.0, "admin", "admin", config)],
                ..Scenario::empty(7)
            };
            let world = World::from_scenario_hosted(&scenario, &[ADMIN]).unwrap();
            let brokers = (1..=3)
                .map(|id| {
                    let node = NodeId(id);
                    let broker_id = i32::try_from(id).unwrap();
                    let mut broker = FakeBroker::new(node, broker_id, Rc::clone(&state));
                    let mut bufs = CtxBuffers::new(node);
                    bufs.with(0, |ctx| broker.start(ctx));
                    (node, (broker, bufs))
                })
                .collect();
            Self {
                world,
                brokers,
                state,
                outbound: Vec::new(),
                inbound: Vec::new(),
            }
        }

        /// Advance `ms` of logical time a millisecond at a time, carrying
        /// the frames both ways after each step.
        fn run_for(&mut self, ms: Millis) {
            let end = self.world.now() + ms;
            while self.world.now() < end {
                let next = self.world.now() + 1;
                self.world.step_until(next);
                self.exchange();
            }
        }

        /// Hand the brokers what reached them by now, and the world the
        /// answers that came back by now.
        fn exchange(&mut self) {
            let now = self.world.now();
            self.outbound.extend(self.world.drain_egress());
            let (due, later): (Vec<TimedFrame>, Vec<TimedFrame>) =
                std::mem::take(&mut self.outbound)
                    .into_iter()
                    .partition(|timed| timed.deliver_at <= now);
            self.outbound = later;
            for timed in due {
                let frame = timed.frame;
                if let Some((broker, bufs)) = self.brokers.get_mut(&frame.dst.node) {
                    bufs.with(now, |ctx| broker.on_frame(ctx, frame));
                    let answers = bufs.take_frames();
                    self.inbound
                        .extend(answers.into_iter().map(|frame| (now + LATENCY, frame)));
                }
            }
            let (arrived, travelling): (Vec<Answer>, Vec<Answer>) =
                std::mem::take(&mut self.inbound)
                    .into_iter()
                    .partition(|(at, _)| *at <= now);
            self.inbound = travelling;
            if !arrived.is_empty() {
                self.world
                    .push_ingress(arrived.into_iter().map(|(_, frame)| frame).collect());
                self.world.step_until(now);
            }
        }

        fn snapshot(&self) -> Value {
            self.world.node_snapshot(ADMIN).unwrap()
        }

        /// `(name, status, error, attempts)` of each topic of the snapshot.
        fn statuses(&self) -> Vec<(String, String, Value, u64)> {
            self.snapshot()["topics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| {
                    (
                        t["name"].as_str().unwrap().to_string(),
                        t["status"].as_str().unwrap().to_string(),
                        t["error"].clone(),
                        t["attempts"].as_u64().unwrap(),
                    )
                })
                .collect()
        }

        fn events(&self, kind: &str) -> Vec<Value> {
            self.world
                .events()
                .filter(|e| e.kind == kind)
                .map(|e| e.detail.clone())
                .collect()
        }

        fn requests<R>(&self) -> Vec<R>
        where
            R: ProtocolRequest + for<'de> krabka_protocol::Decode<'de>,
        {
            self.state
                .borrow()
                .seen(R::API_KEY)
                .iter()
                .map(Seen::decode)
                .collect()
        }
    }

    fn create(name: &str, partitions: i32, replication_factor: i16) -> CreateTopicsRequest {
        CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: partitions,
                replication_factor,
                ..Default::default()
            }],
            timeout_ms: 30_000,
            validate_only: false,
            ..Default::default()
        }
    }

    fn status(
        name: &str,
        status: &str,
        error: Value,
        attempts: u64,
    ) -> (String, String, Value, u64) {
        (name.to_string(), status.to_string(), error, attempts)
    }

    #[test]
    fn the_admin_creates_the_scenario_topics_through_the_controller() {
        let mut remote = Remote::new(json!({
            "bootstrap": [1],
            "topics": [
                { "name": "orders", "partitions": 3, "replication_factor": 3 },
                { "name": "audit" },
            ],
        }));
        remote.run_for(200);
        // One CreateTopics per topic, v7 with `timeout_ms` 30 000, to the
        // controller the metadata named. The broker picks the replication
        // factor of `audit`.
        assert!(
            remote.requests::<CreateTopicsRequest>()
                == vec![create("audit", 1, -1), create("orders", 3, 3)]
        );
        let seen = remote.state.borrow().seen(19);
        assert!(seen.iter().all(|s| s.version == 7 && s.broker == NodeId(1)));
        let layout: Vec<(String, usize, usize)> = remote
            .state
            .borrow()
            .topics
            .iter()
            .map(|(name, t)| {
                (
                    name.clone(),
                    t.partitions.len(),
                    t.partitions[&0].replicas.len(),
                )
            })
            .collect();
        assert!(layout == vec![("audit".to_string(), 1, 3), ("orders".to_string(), 3, 3)]);
        assert!(
            remote.statuses()
                == vec![
                    status("audit", "created", Value::Null, 1),
                    status("orders", "created", Value::Null, 1),
                ]
        );
        assert!(remote.events("topics_created") == vec![json!({ "topics": ["audit", "orders"] })]);
        assert!(
            remote.events("topic_created")
                == vec![
                    json!({ "topic": "audit", "partitions": 1 }),
                    json!({ "topic": "orders", "partitions": 3 }),
                ]
        );
        assert!(remote.snapshot()["client"]["connections"][0]["state"] == "ready");
    }

    #[test]
    fn the_admin_retries_what_can_succeed_and_reports_the_rest() {
        // `orders` exists already; the first answer for `audit` is
        // NOT_CONTROLLER, which Kafka's admin client retries; `bad` gets
        // INVALID_REPLICATION_FACTOR, which it does not.
        let mut remote = Remote::new(json!({
            "bootstrap": [2],
            "topics": [{ "name": "orders" }, { "name": "audit" }, { "name": "bad" }],
        }));
        {
            let mut state = remote.state.borrow_mut();
            state.add_topic("orders", 1, 3);
            state.knobs.create_topics_errors =
                [codes::NOT_CONTROLLER, codes::INVALID_REPLICATION_FACTOR].into();
        }
        remote.run_for(2_000);
        assert!(
            remote.statuses()
                == vec![
                    status("audit", "created", Value::Null, 2),
                    status("bad", "failed", json!(codes::INVALID_REPLICATION_FACTOR), 1),
                    status("orders", "exists", Value::Null, 1),
                ]
        );
        let times: Vec<Millis> = remote
            .state
            .borrow()
            .seen(19)
            .iter()
            .filter(|s| s.decode::<CreateTopicsRequest>().topics[0].name == "audit")
            .map(|s| s.at)
            .collect();
        // The retry waits 500 ms from the answer, so the two requests reach
        // the broker 500 ms and a round trip apart.
        assert!(times.len() == 2);
        assert!(times[1] - times[0] == 500 + 2 * LATENCY);
        assert!(
            remote.events("admin_retry")
                == vec![
                    json!({ "topic": "audit", "code": codes::NOT_CONTROLLER, "level": "warn" })
                ]
        );
        assert!(
            remote.events("admin_error")
                == vec![json!({
                    "topic": "bad",
                    "code": codes::INVALID_REPLICATION_FACTOR,
                    "level": "error",
                })]
        );
        // A topic failed for good, so the scenario's topics never all exist.
        assert!(remote.events("topics_created").is_empty());

        // A preset can ask for three replicas before all three real brokers
        // have registered. Retry that startup answer, then create the topic.
        let mut remote = Remote::new(json!({
            "bootstrap": [1, 2, 3],
            "topics": [{ "name": "orders", "partitions": 3, "replication_factor": 3 }],
        }));
        remote.state.borrow_mut().knobs.create_topics_errors =
            [codes::INVALID_REPLICATION_FACTOR].into();
        remote.run_for(2_000);
        assert!(remote.statuses() == vec![status("orders", "created", Value::Null, 2)]);
    }

    #[test]
    fn commands_create_and_delete_topics_with_real_requests() {
        let mut remote = Remote::new(json!({ "bootstrap": [1] }));
        remote.run_for(50);
        let answer = remote
            .world
            .control(
                ADMIN,
                json!({ "cmd": "create_topic", "name": "temp", "partitions": 2, "replication_factor": 1 }),
            )
            .unwrap();
        assert!(answer == json!({ "topic": "temp", "status": "pending" }));
        remote.run_for(100);
        assert!(remote.requests::<CreateTopicsRequest>() == vec![create("temp", 2, 1)]);
        assert!(remote.state.borrow().topics["temp"].partitions.len() == 2);
        assert!(remote.statuses() == vec![status("temp", "created", Value::Null, 1)]);

        for name in ["temp", "missing"] {
            let answer = remote
                .world
                .control(ADMIN, json!({ "cmd": "delete_topic", "name": name }))
                .unwrap();
            assert!(answer == json!({ "topic": name, "status": "deleting" }));
        }
        remote.run_for(100);
        let delete = |name: &str| DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(name.to_string()),
                topic_id: Uuid::ZERO,
                ..Default::default()
            }],
            timeout_ms: 30_000,
            ..Default::default()
        };
        let mut deletes = remote.requests::<DeleteTopicsRequest>();
        deletes.sort_by(|a, b| a.topics[0].name.cmp(&b.topics[0].name));
        assert!(deletes == vec![delete("missing"), delete("temp")]);
        assert!(!remote.state.borrow().topics.contains_key("temp"));
        assert!(
            remote.statuses()
                == vec![
                    status("missing", "deleted", Value::Null, 1),
                    status("temp", "deleted", Value::Null, 2),
                ]
        );
        // Each command drives the node at once, so `temp` went first.
        assert!(
            remote.events("topic_deleted")
                == vec![
                    json!({ "topic": "temp", "partitions": 2 }),
                    json!({ "topic": "missing", "existed": false }),
                ]
        );
        let refused = [
            (json!({ "cmd": "create_topic" }), "missing `name`"),
            (
                json!({ "cmd": "rename", "name": "x" }),
                "unknown admin command Some(\"rename\")",
            ),
        ];
        for (command, expected) in refused {
            assert!(remote.world.control(ADMIN, command).unwrap_err() == expected);
        }
    }

    #[test]
    fn the_config_is_checked_at_load() {
        let rows = [
            (json!({}), "missing config field `bootstrap`"),
            (
                json!({ "bootstrap": [] }),
                "`bootstrap` needs at least one broker",
            ),
            (
                json!({ "bootstrap": [1], "topic": [] }),
                "unknown config field `topic`",
            ),
            (
                json!({ "bootstrap": [1], "topics": [{ "name": "t", "partition": 2 }] }),
                "config field `topics`: unknown field `partition`",
            ),
        ];
        for (config, reason) in rows {
            let spec = NodeSpec::new(ADMIN.0, "admin", "admin", config.clone());
            let Err(LabError::Config { reason: actual, .. }) = AdminNode::from_spec(&spec) else {
                panic!("{config} was accepted");
            };
            assert!(actual.starts_with(reason), "{config}: {actual}");
        }
        let spec = NodeSpec::new(ADMIN.0, "admin", "admin", json!({ "bootstrap": [1, 2] }));
        let node = AdminNode::from_spec(&spec).unwrap();
        assert!(node.kind() == "admin");
        assert!(node.snapshot()["topics"] == json!([]));
    }
}
