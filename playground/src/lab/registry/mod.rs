//! The simulated schema registry: Confluent Schema Registry over the lab's
//! frames, with its state in the `_schemas` topic on the scenario's brokers.
//!
//! [`RegistryNode`] listens on [`HTTP_PORT`](super::net::HTTP_PORT) and serves the Confluent REST
//! API. Its state is the replay of `_schemas`, which the
//! [`KafkaStore`] sets up, reads and writes on the
//! brokers as Confluent's `KafkaStore` does. The node keeps nothing of its
//! own: every start, a wipe included, replays the topic from its beginning.
//!
//! # Startup
//!
//! On every start the store runs Confluent's startup (see [`kafkastore`]).
//! Until it is done the registry does not listen, as Confluent's REST server
//! starts only once the store has caught up: a new connection is refused with
//! a `Close`, and the `http` command fails. A startup that fails leaves the
//! registry refusing until it restarts.
//!
//! # Requests
//!
//! Reads are served from the replayed state at once. A request that may
//! write (a registration, a delete, a level, a mode) takes Confluent's write
//! lock: writes run one at a time, in arrival order. Each first waits until
//! the reader reaches the last written offset
//! (`waitUntilKafkaReaderReachesLastOffset`), then decides on the state as
//! it is then, then writes its records one `KafkaStore.put` at a time. Its
//! response goes out only once the reader has read its last record back; a
//! write that fails answers the operation's Confluent error instead (see
//! [`rest::WriteOp::failure`]). A registration of a schema the subject
//! already has is answered at once, since Confluent looks it up before it
//! takes the lock. The requests of one connection are answered in order: the
//! requests behind one that waits for the store wait with it.
//!
//! # Config
//!
//! ```json
//! { "bootstrap": [1, 2, 3], "compatibility": "BACKWARD", "mode": "READWRITE",
//!   "kafkastore.topic": "_schemas", "kafkastore.timeout.ms": 500,
//!   "kafkastore.init.timeout.ms": 60000,
//!   "kafkastore.topic.replication.factor": 3 }
//! ```
//!
//! `bootstrap` names the brokers, Confluent's `kafkastore.bootstrap.servers`;
//! it is required and names at least one. `compatibility` and `mode` are the
//! global defaults that apply until a `CONFIG` or `MODE` record sets them.
//! The `kafkastore.*` keys are Confluent's, with its defaults; see
//! [`StoreConfig`]. Any other key is an error.
//!
//! # Control commands
//!
//! `{"cmd": "http", "method": "POST", "path": "/subjects/s/versions",
//! "body": {...}?}` serves one REST request from the page. A read, and a
//! registration the subject already has, returns `{"status": 200, "body":
//! <JSON or text>}`. A request that may write joins the write queue and
//! returns `{"queued": <n>}`; its answer is the `registry` event that carries
//! `"request": <n>`. The command fails while the registry does not serve.
//!
//! # Snapshot
//!
//! ```json
//! { "state": "stopped" | "loading" | "ready" | "failed", "started": 1,
//!   "bootstrap": [1, 2, 3],
//!   "config": { "kafkastore.topic": "_schemas", "kafkastore.timeout.ms": 500,
//!               "kafkastore.init.timeout.ms": 60000,
//!               "kafkastore.topic.replication.factor": 3 },
//!   "compatibility": "BACKWARD", "mode": "READWRITE",
//!   "subjects": [{ "subject": "s", "versions": [{ "version": 1, "id": 1,
//!                  "deleted": false }], "compatibility": null, "mode": null }],
//!   "schemas": 1, "records": 3, "applied": 3,
//!   "unknown_records": 0, "undecodable_records": 0,
//!   "connections": 1, "refused": 0, "requests": 4, "errors": 0,
//!   "writes": { "queued": 0, "active": null },
//!   "store": { ...the store's snapshot... } }
//! ```
//!
//! `state` is the store's, or `stopped` while the node is down, when
//! `store` is `null`. `records` counts the records the reader applied and
//! `applied` is the offset after the last one. `writes.active` is
//! `{"op", "path", "stage": "catch_up" | "write"}` while a write holds the
//! lock. `store` is [`KafkaStore::snapshot`]: the startup step, the topic,
//! the reader's `offset` and `end_offset`, the running task, and the admin,
//! reader and producer clients with their connections.
//!
//! # Events
//!
//! `registry` for every write and every error answer; `kafkastore` for the
//! store's startup, its warnings and its failure, and for the reason of a
//! write the store failed (`{"step": "write_failed", "op", "path",
//! "message"}`).
//!
//! # Durable state
//!
//! None: like Confluent's, the registry's state lives in `_schemas`.

pub mod compat;
pub mod error;
pub mod format;
pub mod http;
pub mod ids;
pub mod kafkastore;
mod lane;
pub mod record;
pub mod rest;
pub mod service;
pub mod store;

use std::collections::{BTreeMap, VecDeque};

use bytes::BytesMut;
use serde_json::{Value, json};

use self::{
    http::{HttpError, HttpRequest, HttpResponse},
    ids::LogOffset,
    kafkastore::{KafkaStore, StoreConfig},
    record::RawRecord,
    rest::WriteOp,
    service::RegistryService,
};
use super::{
    LabError, config_field, config_field_or,
    net::{ConnId, Ctx, Endpoint, Frame, Node, NodeId, Payload},
    scenario::NodeSpec,
};

/// The config keys of a registry node.
const CONFIG_KEYS: [&str; 7] = [
    "bootstrap",
    "compatibility",
    "mode",
    "kafkastore.topic",
    "kafkastore.timeout.ms",
    "kafkastore.init.timeout.ms",
    "kafkastore.topic.replication.factor",
];

/// Where a request came from, and where its answer goes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Origin {
    /// An HTTP connection: the client endpoint and the connection.
    Http(Endpoint, ConnId),
    /// An `http` control command, by its number.
    Control(u64),
}

/// A request that may write, waiting for the write lock.
struct Pending {
    origin: Origin,
    request: HttpRequest,
    op: WriteOp,
}

/// The write that holds the lock.
enum Writer {
    Idle,
    /// The store catches up before the request is decided.
    CatchingUp(Pending),
    /// The request's records are being written; `response` goes out when
    /// they are read back.
    Writing {
        pending: Pending,
        response: HttpResponse,
    },
}

/// One open HTTP connection.
#[derive(Default)]
struct Connection {
    /// The bytes of the requests not served yet.
    buffer: BytesMut,
    /// A request of this connection waits for the store; the ones behind it
    /// wait too.
    waiting: bool,
}

/// The node's settings.
#[derive(Clone)]
struct Settings {
    bootstrap: Vec<NodeId>,
    compatibility: String,
    mode: String,
    store: StoreConfig,
}

/// A schema registry node. See the module documentation.
pub struct RegistryNode {
    id: NodeId,
    settings: Settings,
    service: RegistryService,
    store: Option<KafkaStore>,
    connections: BTreeMap<(Endpoint, ConnId), Connection>,
    queue: VecDeque<Pending>,
    writer: Writer,
    next_command: u64,
    started: u64,
    requests: u64,
    errors: u64,
    refused: u64,
}

impl RegistryNode {
    /// # Errors
    /// Returns a configuration error for an unknown key, a missing or empty
    /// `bootstrap`, a field of the wrong shape, an unknown level or mode, an
    /// empty topic name, or a replication factor below 1.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        if let Some(key) = spec
            .config
            .as_object()
            .into_iter()
            .flat_map(|o| o.keys())
            .find(|k| !CONFIG_KEYS.contains(&k.as_str()))
        {
            return Err(LabError::config(
                spec,
                format!("unknown config field `{key}`"),
            ));
        }
        let bootstrap: Vec<NodeId> = config_field(spec, "bootstrap")?;
        if bootstrap.is_empty() {
            return Err(LabError::config(
                spec,
                "`bootstrap` needs at least one broker",
            ));
        }
        let compatibility: String = config_field_or(spec, "compatibility", "BACKWARD".to_string())?;
        let level = compat::CompatibilityLevel::parse(&compatibility).ok_or_else(|| {
            LabError::config(
                spec,
                format!("unknown compatibility level `{compatibility}`"),
            )
        })?;
        let mode: String = config_field_or(spec, "mode", "READWRITE".to_string())?;
        let mode = mode.to_ascii_uppercase();
        if !service::MODES.contains(&mode.as_str()) {
            return Err(LabError::config(spec, format!("unknown mode `{mode}`")));
        }
        let defaults = StoreConfig::default();
        let store = StoreConfig {
            topic: config_field_or(spec, "kafkastore.topic", defaults.topic)?,
            timeout_ms: config_field_or(spec, "kafkastore.timeout.ms", defaults.timeout_ms)?,
            init_timeout_ms: config_field_or(
                spec,
                "kafkastore.init.timeout.ms",
                defaults.init_timeout_ms,
            )?,
            replication_factor: config_field_or(
                spec,
                "kafkastore.topic.replication.factor",
                defaults.replication_factor,
            )?,
        };
        if store.topic.is_empty() {
            return Err(LabError::config(spec, "`kafkastore.topic` is empty"));
        }
        if store.replication_factor < 1 {
            return Err(LabError::config(
                spec,
                "`kafkastore.topic.replication.factor` must be at least 1",
            ));
        }
        let settings = Settings {
            bootstrap,
            compatibility: level.as_str().to_string(),
            mode,
            store,
        };
        Ok(Self {
            id: spec.id,
            service: RegistryService::new(&settings.compatibility, &settings.mode),
            settings,
            store: None,
            connections: BTreeMap::new(),
            queue: VecDeque::new(),
            writer: Writer::Idle,
            next_command: 0,
            started: 0,
            requests: 0,
            errors: 0,
            refused: 0,
        })
    }

    /// The service behind the REST surface.
    #[must_use]
    pub fn service(&self) -> &RegistryService {
        &self.service
    }

    /// Whether the registry serves: its store finished the startup.
    #[must_use]
    pub fn serving(&self) -> bool {
        self.store.as_ref().is_some_and(KafkaStore::is_ready)
    }

    fn not_serving(&self) -> String {
        match self.store.as_ref().and_then(KafkaStore::failure) {
            Some(failure) => format!("the schema registry failed to start: {failure}"),
            None => "the schema registry is loading its store and does not serve yet".to_string(),
        }
    }

    /// Apply the records the reader read.
    fn apply(&mut self, read: Vec<(LogOffset, RawRecord)>) {
        for (offset, record) in read {
            self.service.apply(offset, &record);
        }
    }

    /// The tail of every call: run the writes that can run, and arm the
    /// timer for the store.
    fn settle(&mut self, ctx: &mut Ctx<'_>) {
        self.pump_writes(ctx);
        if let Some(at) = self
            .store
            .as_ref()
            .and_then(|store| store.next_deadline(ctx.now()))
        {
            ctx.arm(at);
        }
    }

    fn take_outcome(&mut self) -> Option<Result<(), kafkastore::StoreError>> {
        self.store.as_mut().and_then(KafkaStore::take_outcome)
    }

    /// Run the write queue as far as the store lets it: the lock's holder
    /// catches up, is decided, writes, and answers; then the next one.
    fn pump_writes(&mut self, ctx: &mut Ctx<'_>) {
        loop {
            match std::mem::replace(&mut self.writer, Writer::Idle) {
                Writer::Idle => {
                    let Some(store) = self.store.as_mut().filter(|s| s.is_ready()) else {
                        return;
                    };
                    let Some(pending) = self.queue.pop_front() else {
                        return;
                    };
                    store.begin_catch_up(ctx);
                    self.writer = Writer::CatchingUp(pending);
                }
                Writer::CatchingUp(pending) => match self.take_outcome() {
                    None => {
                        self.writer = Writer::CatchingUp(pending);
                        return;
                    }
                    Some(Err(error)) => {
                        Self::log_store_failure(ctx, &pending, &error);
                        let response = pending.op.failure(&error).to_response();
                        self.finish(ctx, &pending, &response);
                    }
                    Some(Ok(())) => {
                        let outcome = rest::handle(&self.service, &pending.request);
                        match (outcome.write, self.store.as_mut()) {
                            (Some(write), Some(store)) => {
                                store.begin_write(ctx, write.records);
                                self.writer = Writer::Writing {
                                    pending,
                                    response: outcome.response,
                                };
                            }
                            _ => self.finish(ctx, &pending, &outcome.response),
                        }
                    }
                },
                Writer::Writing { pending, response } => match self.take_outcome() {
                    None => {
                        self.writer = Writer::Writing { pending, response };
                        return;
                    }
                    Some(Ok(())) => self.finish(ctx, &pending, &response),
                    Some(Err(error)) => {
                        Self::log_store_failure(ctx, &pending, &error);
                        let response = pending.op.failure(&error).to_response();
                        self.finish(ctx, &pending, &response);
                    }
                },
            }
        }
    }

    /// The store's reason for a failed write, which the answer does not
    /// carry: Confluent logs it.
    fn log_store_failure(ctx: &mut Ctx<'_>, pending: &Pending, error: &kafkastore::StoreError) {
        ctx.event(
            "kafkastore",
            json!({
                "step": "write_failed",
                "op": pending.op.name(),
                "path": pending.request.path,
                "message": error.to_string(),
                "level": "warn",
            }),
        );
    }

    /// A write answered: send its response, and serve what its connection
    /// holds behind it.
    fn finish(&mut self, ctx: &mut Ctx<'_>, pending: &Pending, response: &HttpResponse) {
        self.answer(ctx, pending.origin, &pending.request, response);
        if let Origin::Http(peer, conn) = pending.origin
            && let Some(connection) = self.connections.get_mut(&(peer, conn))
        {
            connection.waiting = false;
            self.serve(ctx, peer, conn);
        }
    }

    /// Count, record and deliver an answer.
    fn answer(
        &mut self,
        ctx: &mut Ctx<'_>,
        origin: Origin,
        request: &HttpRequest,
        response: &HttpResponse,
    ) {
        if response.status >= 400 {
            self.errors += 1;
        }
        let command = match origin {
            Origin::Control(id) => Some(id),
            Origin::Http(..) => None,
        };
        Self::log_request(ctx, request, response, command);
        if let Origin::Http(peer, conn) = origin {
            self.respond(ctx, peer, conn, response, request.close);
        }
    }

    fn respond(
        &mut self,
        ctx: &mut Ctx<'_>,
        peer: Endpoint,
        conn: ConnId,
        response: &HttpResponse,
        close: bool,
    ) {
        if !self.connections.contains_key(&(peer, conn)) {
            // The client went away; its request still ran.
            return;
        }
        let me = Endpoint::http(self.id);
        ctx.send(Frame::data(me, peer, conn, response.encode()));
        if close {
            ctx.send(Frame::close(me, peer, conn));
            self.connections.remove(&(peer, conn));
        }
    }

    /// Serve the complete requests in a connection's buffer, in order,
    /// until one has to wait for the store.
    fn serve(&mut self, ctx: &mut Ctx<'_>, peer: Endpoint, conn: ConnId) {
        loop {
            let Some(connection) = self.connections.get_mut(&(peer, conn)) else {
                return;
            };
            if connection.waiting || connection.buffer.is_empty() {
                return;
            }
            let (request, used) = match HttpRequest::parse(&connection.buffer) {
                Ok(parsed) => parsed,
                Err(HttpError::Incomplete) => return,
                Err(error) => {
                    self.errors += 1;
                    let response = HttpResponse::error(400, 400, error.to_string());
                    self.respond(ctx, peer, conn, &response, true);
                    return;
                }
            };
            let _ = connection.buffer.split_to(used);
            self.requests += 1;
            let origin = Origin::Http(peer, conn);
            match self.admit(origin, request) {
                Admitted::Answered(request, response) => {
                    self.answer(ctx, origin, &request, &response);
                    if request.close {
                        return;
                    }
                }
                Admitted::Queued => {
                    if let Some(connection) = self.connections.get_mut(&(peer, conn)) {
                        connection.waiting = true;
                    }
                    return;
                }
            }
        }
    }

    /// Answer a request that writes nothing, or queue it for the write
    /// lock.
    fn admit(&mut self, origin: Origin, request: HttpRequest) -> Admitted {
        let Some(op) = rest::write_op(&request) else {
            let response = rest::handle(&self.service, &request).response;
            return Admitted::Answered(request, response);
        };
        if let Some(response) = rest::registered_already(&self.service, &request) {
            return Admitted::Answered(request, response);
        }
        self.queue.push_back(Pending {
            origin,
            request,
            op,
        });
        Admitted::Queued
    }

    /// A timeline event for a mutation or an error; `command` is the number
    /// of the `http` command that asked.
    fn log_request(
        ctx: &mut Ctx<'_>,
        request: &HttpRequest,
        response: &HttpResponse,
        command: Option<u64>,
    ) {
        if rest::write_op(request).is_none() && response.status < 400 {
            return;
        }
        let mut detail = json!({
            "method": request.method,
            "path": request.path,
            "status": response.status,
        });
        if let Some(command) = command {
            detail["request"] = command.into();
        }
        if response.status >= 400 {
            detail["level"] = "warn".into();
            if let Some(message) = response
                .body_json()
                .and_then(|b| b["message"].as_str().map(String::from))
            {
                detail["message"] = message.into();
            }
        } else if let Some(body) = response.body_json() {
            detail["result"] = body;
        }
        ctx.event("registry", detail);
    }

    fn on_http_frame(&mut self, ctx: &mut Ctx<'_>, frame: &Frame) {
        let (peer, conn) = (frame.src, frame.conn);
        match &frame.payload {
            Payload::Open => {
                if self.serving() {
                    self.connections.insert((peer, conn), Connection::default());
                } else {
                    // Not listening yet: the connection is refused.
                    self.refused += 1;
                    ctx.send(frame.reply(Payload::Close));
                }
            }
            Payload::Close => {
                self.connections.remove(&(peer, conn));
            }
            Payload::Data(bytes) => {
                let Some(connection) = self.connections.get_mut(&(peer, conn)) else {
                    ctx.send(frame.reply(Payload::Close));
                    return;
                };
                connection.buffer.extend_from_slice(bytes);
                self.serve(ctx, peer, conn);
            }
        }
    }

    fn run_http_command(&mut self, ctx: &mut Ctx<'_>, command: &Value) -> Result<Value, String> {
        if !self.serving() {
            return Err(self.not_serving());
        }
        let method = command
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("GET")
            .to_ascii_uppercase();
        let path = command
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "http command needs a path".to_string())?;
        let mut request = HttpRequest::new(&method, path);
        if let Some(body) = command.get("body").filter(|b| !b.is_null()) {
            request = request.with_json(body);
        }
        let (request, _) = HttpRequest::parse(&request.encode()).map_err(|e| e.to_string())?;
        self.requests += 1;
        self.next_command += 1;
        let origin = Origin::Control(self.next_command);
        match self.admit(origin, request) {
            Admitted::Answered(request, response) => {
                self.answer(ctx, origin, &request, &response);
                let body = response
                    .body_json()
                    .unwrap_or_else(|| Value::String(response.body.clone()));
                Ok(json!({ "status": response.status, "body": body }))
            }
            Admitted::Queued => Ok(json!({ "queued": self.next_command })),
        }
    }

    fn writes_snapshot(&self) -> Value {
        let active = match &self.writer {
            Writer::Idle => Value::Null,
            Writer::CatchingUp(pending) => json!({
                "op": pending.op.name(),
                "path": pending.request.path,
                "stage": "catch_up",
            }),
            Writer::Writing { pending, .. } => json!({
                "op": pending.op.name(),
                "path": pending.request.path,
                "stage": "write",
            }),
        };
        json!({ "queued": self.queue.len(), "active": active })
    }
}

/// What [`RegistryNode::admit`] did with a request.
enum Admitted {
    /// The request needed no write and has its answer.
    Answered(HttpRequest, HttpResponse),
    /// The request waits for the write lock.
    Queued,
}

impl Node for RegistryNode {
    fn kind(&self) -> &'static str {
        "schema-registry"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        // A restart of a live node resets what the process before it had
        // open.
        if let Some(mut store) = self.store.take() {
            store.close(ctx);
        }
        let me = Endpoint::http(self.id);
        for (peer, conn) in std::mem::take(&mut self.connections).into_keys() {
            ctx.send(Frame::close(me, peer, conn));
        }
        self.started += 1;
        self.service = RegistryService::new(&self.settings.compatibility, &self.settings.mode);
        self.queue.clear();
        self.writer = Writer::Idle;
        let generation = u32::try_from(self.started % u64::from(u32::MAX)).unwrap_or(0);
        self.store = Some(KafkaStore::start(
            self.settings.store.clone(),
            &self.settings.bootstrap,
            generation,
            ctx,
        ));
        self.settle(ctx);
    }

    fn stop(&mut self) {
        self.connections.clear();
        self.queue.clear();
        self.writer = Writer::Idle;
        self.store = None;
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if frame.dst == Endpoint::http(self.id) {
            self.on_http_frame(ctx, &frame);
        } else if frame.dst == Endpoint::client(self.id)
            && let Some(store) = &mut self.store
        {
            let read = store.on_frame(ctx, frame);
            self.apply(read);
        }
        self.settle(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(store) = &mut self.store {
            let read = store.on_tick(ctx);
            self.apply(read);
        }
        self.settle(ctx);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let answer = match command.get("cmd").and_then(Value::as_str) {
            Some("http") => self.run_http_command(ctx, &command),
            other => Err(format!("unknown registry command {other:?}")),
        };
        self.settle(ctx);
        answer
    }

    fn snapshot(&self) -> Value {
        let state = self.service.state();
        let subjects: Vec<Value> = state
            .subject_entries()
            .map(|(name, versions)| {
                json!({
                    "subject": name,
                    "versions": versions.iter().map(|v| json!({
                        "version": v.version,
                        "id": v.id,
                        "deleted": v.deleted,
                    })).collect::<Vec<_>>(),
                    "compatibility": state.subject_compat(name),
                    "mode": state.subject_mode(name),
                })
            })
            .collect();
        let store = &self.settings.store;
        json!({
            "state": self.store.as_ref().map_or("stopped", KafkaStore::state_name),
            "started": self.started,
            "bootstrap": self.settings.bootstrap,
            "config": {
                "kafkastore.topic": store.topic,
                "kafkastore.timeout.ms": store.timeout_ms,
                "kafkastore.init.timeout.ms": store.init_timeout_ms,
                "kafkastore.topic.replication.factor": store.replication_factor,
            },
            "compatibility": state.global_compat(),
            "mode": state.global_mode(),
            "subjects": subjects,
            "schemas": state.schema_count(),
            "records": self.service.record_count(),
            "applied": self.service.applied(),
            "unknown_records": self.service.unknown_records(),
            "undecodable_records": self.service.undecodable_records(),
            "connections": self.connections.len(),
            "refused": self.refused,
            "requests": self.requests,
            "errors": self.errors,
            "writes": self.writes_snapshot(),
            "store": self.store.as_ref().map(KafkaStore::snapshot),
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::lab::testing::CtxBuffers;

    fn spec(config: Value) -> NodeSpec {
        NodeSpec::new(4, "schema-registry", "registry", config)
    }

    #[test]
    fn config_is_validated() {
        let node = RegistryNode::from_spec(&spec(json!({ "bootstrap": [1] }))).unwrap();
        assert!(node.service().state().global_compat() == "BACKWARD");
        assert!(node.settings.store == StoreConfig::default());
        let custom = RegistryNode::from_spec(&spec(json!({
            "bootstrap": [1, 2],
            "compatibility": "full",
            "mode": "readonly",
            "kafkastore.topic": "_schemas_lab",
            "kafkastore.timeout.ms": 250,
            "kafkastore.init.timeout.ms": 5000,
            "kafkastore.topic.replication.factor": 1,
        })))
        .unwrap();
        assert!(custom.service().state().global_compat() == "FULL");
        assert!(custom.service().state().global_mode() == "READONLY");
        assert!(custom.settings.bootstrap == vec![NodeId(1), NodeId(2)]);
        assert!(
            custom.settings.store
                == StoreConfig {
                    topic: "_schemas_lab".to_string(),
                    timeout_ms: 250,
                    init_timeout_ms: 5000,
                    replication_factor: 1,
                }
        );
        let refused = [
            (json!({}), "missing config field `bootstrap`"),
            (
                json!({ "bootstrap": [] }),
                "`bootstrap` needs at least one broker",
            ),
            (
                json!({ "bootstrap": "one" }),
                "config field `bootstrap`: invalid type",
            ),
            (
                json!({ "bootstrap": [1], "compatibility": "SIDEWAYS" }),
                "unknown compatibility level `SIDEWAYS`",
            ),
            (
                json!({ "bootstrap": [1], "mode": "SIDEWAYS" }),
                "unknown mode `SIDEWAYS`",
            ),
            (
                json!({ "bootstrap": [1], "kafkastore.topic": "" }),
                "`kafkastore.topic` is empty",
            ),
            (
                json!({ "bootstrap": [1], "kafkastore.topic.replication.factor": 0 }),
                "`kafkastore.topic.replication.factor` must be at least 1",
            ),
            (
                json!({ "bootstrap": [1], "kafkastore.timeout.ms": -1 }),
                "config field `kafkastore.timeout.ms`: invalid value",
            ),
            (
                json!({ "bootstrap": [1], "kafkastore.connection.url": "x" }),
                "unknown config field `kafkastore.connection.url`",
            ),
        ];
        for (config, reason) in refused {
            let Err(LabError::Config { reason: actual, .. }) =
                RegistryNode::from_spec(&spec(config.clone()))
            else {
                panic!("{config} was accepted");
            };
            assert!(actual.starts_with(reason), "{config}: {actual}");
        }
    }

    #[test]
    fn a_loading_registry_refuses_connections_and_commands() {
        let mut node = RegistryNode::from_spec(&spec(json!({ "bootstrap": [1] }))).unwrap();
        let mut buffers = CtxBuffers::new(NodeId(4));
        buffers.with(0, |ctx| node.start(ctx));
        // The store's first request goes to the bootstrap broker; nothing
        // has answered, so the registry does not serve.
        let sent = buffers.take_frames();
        assert!(sent.iter().all(|f| f.dst == Endpoint::kafka(NodeId(1))));
        assert!(!node.serving());
        let client = Endpoint::client(NodeId(9));
        let open = Frame::open(client, Endpoint::http(NodeId(4)), ConnId(1));
        buffers.with(5, |ctx| node.on_frame(ctx, open.clone()));
        assert!(buffers.take_frames() == vec![open.reply(Payload::Close)]);
        let answer = buffers.with(5, |ctx| {
            node.control(ctx, json!({ "cmd": "http", "path": "/subjects" }))
        });
        assert!(
            answer
                == Err(
                    "the schema registry is loading its store and does not serve yet".to_string()
                )
        );
        let snapshot = node.snapshot();
        assert!(snapshot["state"] == "loading");
        assert!(snapshot["refused"] == 1);
        assert!(snapshot["connections"] == 0);
        assert!(snapshot["store"]["step"] == "cluster_id");
        assert!(
            buffers.with(5, |ctx| node.control(ctx, json!({ "cmd": "nope" })))
                == Err("unknown registry command Some(\"nope\")".to_string())
        );
    }
}
