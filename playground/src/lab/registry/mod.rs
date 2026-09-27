//! The simulated schema registry.
//!
//! [`RegistryNode`] listens on [`HTTP_PORT`](super::net::HTTP_PORT) and serves the Confluent REST
//! API over the lab's frames. Its state is the replay of a `_schemas` log: in
//! this batch the log is the in-memory [`SchemaLog`], mirrored record by
//! record into the node's durable store so registered schemas survive a page
//! reload; the Kafka-backed store that produces to the brokers is the next
//! batch, and then the durable log becomes the local cache of `_schemas`.
//!
//! # Config
//!
//! ```json
//! { "bootstrap": [1, 2, 3], "compatibility": "BACKWARD", "mode": "READWRITE" }
//! ```
//!
//! `bootstrap` names the brokers the Kafka-backed store will connect to; it
//! is accepted and kept for the snapshot. `compatibility` and `mode` are the
//! global defaults that apply until a `CONFIG` or `MODE` record sets them.
//!
//! # Durable state
//!
//! Every `_schemas` record is appended to the durable log store `schemas` at
//! its log offset, as the JSON document `{"key": <key text>, "value": <value
//! text or null>}`, both texts verbatim so a restored record is byte-exact.
//! On [`Node::load`] the entries are replayed into the log in index order,
//! exactly as a restart replays `_schemas`.
//!
//! # Control commands
//!
//! - `{"cmd": "http", "method": "POST", "path": "/subjects/s/versions",
//!   "body": {...}?}` serves one REST request from the page and returns
//!   `{"status": 200, "body": <JSON or text>}`.
//! - `{"cmd": "register", "subject": "s", "schema": "...", "schemaType"?:
//!   "AVRO", "references"?: [...]}` registers a schema and returns
//!   `{"id": n, "version": n}`.

pub mod compat;
pub mod error;
pub mod format;
pub mod http;
pub mod ids;
pub mod log;
pub mod record;
pub mod rest;
pub mod service;
pub mod store;

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use serde_json::{Value, json};

use self::{
    format::SchemaType,
    http::{HttpError, HttpRequest, HttpResponse},
    ids::LogOffset,
    log::SchemaLog,
    record::RawRecord,
    service::{RegisterRequest, RegistryService},
};
use super::{
    LabError, config_field_or,
    net::{ConnId, Ctx, DurableImage, DurableOp, Endpoint, Frame, Node, NodeId, Payload},
    scenario::NodeSpec,
};

/// The durable log store that mirrors `_schemas`.
pub const DURABLE_STORE: &str = "schemas";

/// A response held until the record it depends on has been read back.
struct HeldResponse {
    peer: Endpoint,
    conn: ConnId,
    wait_for: LogOffset,
    response: HttpResponse,
    close: bool,
}

/// One open HTTP connection: the bytes of a request not yet complete.
#[derive(Default)]
struct Connection {
    buffer: BytesMut,
}

/// A schema registry node.
pub struct RegistryNode {
    id: NodeId,
    bootstrap: Vec<NodeId>,
    service: RegistryService<SchemaLog>,
    connections: BTreeMap<(Endpoint, ConnId), Connection>,
    held: Vec<HeldResponse>,
    started: u64,
    requests: u64,
    errors: u64,
}

impl RegistryNode {
    /// # Errors
    /// Returns a configuration error when a field has the wrong shape or
    /// names an unknown level or mode.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let bootstrap: Vec<NodeId> = config_field_or(spec, "bootstrap", Vec::new())?;
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
        Ok(Self {
            id: spec.id,
            bootstrap,
            service: RegistryService::new(SchemaLog::new(), level.as_str(), &mode),
            connections: BTreeMap::new(),
            held: Vec::new(),
            started: 0,
            requests: 0,
            errors: 0,
        })
    }

    /// The service behind the REST surface.
    #[must_use]
    pub fn service(&self) -> &RegistryService<SchemaLog> {
        &self.service
    }

    /// The durable document of one record.
    fn durable_bytes(record: &RawRecord) -> Bytes {
        let doc = json!({
            "key": String::from_utf8_lossy(&record.key),
            "value": record.value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
        });
        Bytes::from(serde_json::to_vec(&doc).unwrap_or_default())
    }

    /// A record from its durable document, if it is one.
    fn record_from_durable(bytes: &[u8]) -> Option<RawRecord> {
        let doc: Value = serde_json::from_slice(bytes).ok()?;
        let key = doc.get("key")?.as_str()?;
        let value = match doc.get("value") {
            None | Some(Value::Null) => None,
            Some(v) => Some(Bytes::from(v.as_str()?.to_string())),
        };
        Some(RawRecord {
            key: Bytes::from(key.to_string()),
            value,
        })
    }

    /// Mirror the records the service appended into the durable log, then
    /// release every held response whose record has been applied.
    fn settle(&mut self, ctx: &mut Ctx<'_>) {
        for (offset, record) in self.service.take_appended() {
            ctx.persist(DurableOp::Append {
                store: DURABLE_STORE.to_string(),
                index: u64::try_from(offset.0).unwrap_or(0),
                bytes: Self::durable_bytes(&record),
            });
        }
        let applied = self.service.applied();
        let (ready, waiting): (Vec<HeldResponse>, Vec<HeldResponse>) =
            std::mem::take(&mut self.held)
                .into_iter()
                .partition(|h| h.wait_for < applied);
        self.held = waiting;
        for held in ready {
            self.respond(ctx, held.peer, held.conn, &held.response, held.close);
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
        let me = Endpoint::http(self.id);
        ctx.send(Frame::data(me, peer, conn, response.encode()));
        if close {
            ctx.send(Frame::close(me, peer, conn));
            self.connections.remove(&(peer, conn));
        }
    }

    /// Serve every complete request in a connection's buffer.
    fn serve(&mut self, ctx: &mut Ctx<'_>, peer: Endpoint, conn: ConnId) {
        loop {
            let Some(connection) = self.connections.get_mut(&(peer, conn)) else {
                return;
            };
            if connection.buffer.is_empty() {
                return;
            }
            let parsed = HttpRequest::parse(&connection.buffer);
            let (request, used) = match parsed {
                Ok(parsed) => parsed,
                Err(HttpError::Incomplete) => return,
                Err(error) => {
                    self.errors += 1;
                    let response = HttpResponse::error(400, 400, error.to_string());
                    self.connections.remove(&(peer, conn));
                    self.respond(ctx, peer, conn, &response, true);
                    return;
                }
            };
            let _ = connection.buffer.split_to(used);
            self.requests += 1;
            let outcome = rest::handle(&mut self.service, &request);
            if outcome.response.status >= 400 {
                self.errors += 1;
            }
            Self::log_request(ctx, &request, &outcome.response);
            match outcome.wait_for {
                Some(wait_for) if wait_for >= self.service.applied() => {
                    self.held.push(HeldResponse {
                        peer,
                        conn,
                        wait_for,
                        response: outcome.response,
                        close: request.close,
                    });
                }
                _ => self.respond(ctx, peer, conn, &outcome.response, request.close),
            }
            self.settle(ctx);
            if request.close {
                return;
            }
        }
    }

    /// A timeline event for a mutation or an error.
    fn log_request(ctx: &mut Ctx<'_>, request: &HttpRequest, response: &HttpResponse) {
        let mutation = matches!(request.method.as_str(), "POST" | "PUT" | "DELETE")
            && !request.path.starts_with("/compatibility")
            && !(request.method == "POST" && request.segments().len() == 2);
        if !mutation && response.status < 400 {
            return;
        }
        let mut detail = json!({
            "method": request.method,
            "path": request.path,
            "status": response.status,
        });
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

    fn run_http_command(&mut self, ctx: &mut Ctx<'_>, command: &Value) -> Result<Value, String> {
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
        let outcome = rest::handle(&mut self.service, &request);
        if outcome.response.status >= 400 {
            self.errors += 1;
        }
        Self::log_request(ctx, &request, &outcome.response);
        self.settle(ctx);
        let body = outcome
            .response
            .body_json()
            .unwrap_or_else(|| Value::String(outcome.response.body.clone()));
        Ok(json!({ "status": outcome.response.status, "body": body }))
    }

    fn run_register_command(
        &mut self,
        ctx: &mut Ctx<'_>,
        command: &Value,
    ) -> Result<Value, String> {
        let subject = command
            .get("subject")
            .and_then(Value::as_str)
            .ok_or_else(|| "register command needs a subject".to_string())?;
        let schema = command
            .get("schema")
            .and_then(Value::as_str)
            .ok_or_else(|| "register command needs a schema".to_string())?;
        let ty = SchemaType::from_wire(command.get("schemaType").and_then(Value::as_str))
            .ok_or_else(|| "unknown schemaType".to_string())?;
        let references: Vec<record::SchemaReference> = match command.get("references") {
            None | Some(Value::Null) => Vec::new(),
            Some(refs) => serde_json::from_value(refs.clone()).map_err(|e| e.to_string())?,
        };
        let written = self
            .service
            .register(RegisterRequest {
                subject,
                ty,
                schema,
                references: &references,
                import_id: None,
                import_version: None,
            })
            .map_err(|e| e.to_string())?;
        ctx.event(
            "registry",
            json!({ "method": "POST", "path": format!("/subjects/{subject}/versions"), "status": 200, "result": { "id": written.value.id } }),
        );
        self.settle(ctx);
        Ok(json!({ "id": written.value.id, "version": written.value.version }))
    }
}

impl Node for RegistryNode {
    fn kind(&self) -> &'static str {
        "schema-registry"
    }

    fn load(&mut self, image: DurableImage) {
        let records: Vec<RawRecord> = image
            .logs
            .get(DURABLE_STORE)
            .into_iter()
            .flatten()
            .filter_map(|entry| Self::record_from_durable(&entry.bytes))
            .collect();
        self.service.restore(records);
    }

    fn start(&mut self, _ctx: &mut Ctx<'_>) {
        self.started += 1;
        self.connections.clear();
        self.held.clear();
    }

    fn stop(&mut self) {
        self.connections.clear();
        self.held.clear();
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if frame.dst != Endpoint::http(self.id) {
            return;
        }
        let key = (frame.src, frame.conn);
        match frame.payload {
            Payload::Open => {
                self.connections.insert(key, Connection::default());
            }
            Payload::Close => {
                self.connections.remove(&key);
                self.held.retain(|h| (h.peer, h.conn) != key);
            }
            Payload::Data(bytes) => {
                let connection = self.connections.entry(key).or_default();
                connection.buffer.extend_from_slice(&bytes);
                self.serve(ctx, frame.src, frame.conn);
            }
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        self.service.poll();
        self.settle(ctx);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        match command.get("cmd").and_then(Value::as_str) {
            Some("http") => self.run_http_command(ctx, &command),
            Some("register") => self.run_register_command(ctx, &command),
            other => Err(format!("unknown registry command {other:?}")),
        }
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
        json!({
            "started": self.started,
            "bootstrap": self.bootstrap,
            "compatibility": state.global_compat(),
            "mode": state.global_mode(),
            "subjects": subjects,
            "schemas": state.schema_count(),
            "records": self.service.record_count(),
            "applied": self.service.applied(),
            "unknown_records": self.service.unknown_records(),
            "undecodable_records": self.service.undecodable_records(),
            "connections": self.connections.len(),
            "held": self.held.len(),
            "requests": self.requests,
            "errors": self.errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;
    use crate::lab::{
        scenario::Scenario,
        testing::{CtxBuffers, TestWorld},
        world::{Fault, World},
    };

    fn node(config: Value) -> RegistryNode {
        RegistryNode::from_spec(&NodeSpec::new(4, "schema-registry", "registry", config)).unwrap()
    }

    fn av(name: &str) -> String {
        format!(
            "{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{{\"name\":\"{name}\",\"type\":\"int\",\"default\":0}}]}}"
        )
    }

    /// Drive one request through a node with the test buffers; returns the
    /// response and the frames the node sent.
    fn request(
        node: &mut RegistryNode,
        buffers: &mut CtxBuffers,
        req: &HttpRequest,
    ) -> (HttpResponse, Vec<Frame>) {
        let client = Endpoint::client(NodeId(9));
        let server = Endpoint::http(NodeId(4));
        let conn = ConnId(1);
        buffers.with(0, |ctx| {
            node.on_frame(ctx, Frame::open(client, server, conn));
            node.on_frame(ctx, Frame::data(client, server, conn, req.encode()));
        });
        let frames = buffers.take_frames();
        let data = frames
            .iter()
            .find_map(|f| f.payload.data())
            .expect("a response frame");
        let (response, _) = HttpResponse::parse(data).unwrap();
        (response, frames)
    }

    #[test]
    fn config_is_validated() {
        assert!(node(json!({})).service().state().global_compat() == "BACKWARD");
        let custom =
            node(json!({ "bootstrap": [1, 2], "compatibility": "full", "mode": "readonly" }));
        assert!(custom.service().state().global_compat() == "FULL");
        assert!(custom.service().state().global_mode() == "READONLY");
        assert!(custom.bootstrap == vec![NodeId(1), NodeId(2)]);
        let spec = NodeSpec::new(
            4,
            "schema-registry",
            "r",
            json!({ "compatibility": "SIDEWAYS" }),
        );
        assert!(RegistryNode::from_spec(&spec).is_err());
        let spec = NodeSpec::new(4, "schema-registry", "r", json!({ "mode": "SIDEWAYS" }));
        assert!(RegistryNode::from_spec(&spec).is_err());
        let spec = NodeSpec::new(4, "schema-registry", "r", json!({ "bootstrap": "one" }));
        assert!(RegistryNode::from_spec(&spec).is_err());
    }

    #[test]
    fn requests_over_frames_are_answered_and_persisted() {
        let mut node = node(json!({}));
        let mut buffers = CtxBuffers::new(NodeId(4));
        buffers.with(0, |ctx| node.start(ctx));
        let register = HttpRequest::new("POST", "/subjects/orders-value/versions")
            .with_json(&json!({ "schema": av("Order") }));
        let (response, frames) = request(&mut node, &mut buffers, &register);
        assert!(response.status == 200);
        assert!(response.body == r#"{"id":1}"#);
        assert!(frames.len() == 1);
        assert!(frames[0].src == Endpoint::http(NodeId(4)));
        assert!(frames[0].dst == Endpoint::client(NodeId(9)));
        let ops = buffers.take_durable();
        let expected = json!({
            "key": r#"{"keytype":"SCHEMA","subject":"orders-value","version":1,"magic":1}"#,
            "value": format!(
                r#"{{"subject":"orders-value","version":1,"id":1,"schema":{},"deleted":false}}"#,
                serde_json::to_string(&av("Order")).unwrap()
            ),
        });
        assert!(
            ops == vec![DurableOp::Append {
                store: "schemas".into(),
                index: 0,
                bytes: Bytes::from(serde_json::to_vec(&expected).unwrap()),
            }]
        );
        // A read writes nothing, and `Connection: close` closes.
        let mut get = HttpRequest::new("GET", "/subjects/orders-value/versions/1");
        get.close = true;
        let (response, frames) = request(&mut node, &mut buffers, &get);
        assert!(response.body_json().unwrap()["id"] == 1);
        assert!(frames.len() == 2);
        assert!(frames[1].payload == Payload::Close);
        assert!(buffers.take_durable().is_empty());
        assert!(node.connections.is_empty());
        let snapshot = node.snapshot();
        assert!(snapshot["requests"] == 2);
        assert!(snapshot["errors"] == 0);
        assert!(snapshot["records"] == 1);
        assert!(snapshot["subjects"][0]["subject"] == "orders-value");
        assert!(
            snapshot["subjects"][0]["versions"]
                == json!([{ "version": 1, "id": 1, "deleted": false }])
        );
    }

    #[test]
    fn a_mutation_that_writes_nothing_is_answered_at_once() {
        let mut node = node(json!({}));
        let mut buffers = CtxBuffers::new(NodeId(4));
        buffers.with(0, |ctx| node.start(ctx));
        let register = HttpRequest::new("POST", "/subjects/orders-value/versions")
            .with_json(&json!({ "schema": av("Order") }));
        let (first, _) = request(&mut node, &mut buffers, &register);
        assert!(first.body == r#"{"id":1}"#);
        assert!(buffers.take_durable().len() == 1);
        // Registering the same schema again appends no record, so there is
        // nothing to wait for.
        let (again, _) = request(&mut node, &mut buffers, &register);
        assert!(again.status == 200);
        assert!(again.body == r#"{"id":1}"#);
        // Neither does clearing a subject mode that was never set.
        let clear = HttpRequest::new("DELETE", "/mode/orders-value");
        let (cleared, _) = request(&mut node, &mut buffers, &clear);
        assert!(cleared.status < 500);
        assert!(buffers.take_durable().is_empty());
        assert!(node.held.is_empty());
    }

    #[test]
    fn pipelined_and_split_requests_are_served_in_order() {
        let mut node = node(json!({}));
        let mut buffers = CtxBuffers::new(NodeId(4));
        let client = Endpoint::client(NodeId(9));
        let server = Endpoint::http(NodeId(4));
        let conn = ConnId(2);
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&HttpRequest::new("GET", "/subjects").encode());
        bytes.extend_from_slice(&HttpRequest::new("GET", "/schemas/types").encode());
        let whole = bytes.freeze();
        let (head, tail) = whole.split_at(whole.len() - 7);
        buffers.with(0, |ctx| {
            node.on_frame(ctx, Frame::open(client, server, conn));
            node.on_frame(
                ctx,
                Frame::data(client, server, conn, Bytes::copy_from_slice(head)),
            );
        });
        assert!(buffers.take_frames().len() == 1);
        buffers.with(0, |ctx| {
            node.on_frame(
                ctx,
                Frame::data(client, server, conn, Bytes::copy_from_slice(tail)),
            );
        });
        let frames = buffers.take_frames();
        assert!(frames.len() == 1);
        let (response, _) = HttpResponse::parse(frames[0].payload.data().unwrap()).unwrap();
        assert!(response.body == r#"["JSON","PROTOBUF","AVRO"]"#);
        // Garbage closes the connection with a 400.
        buffers.with(0, |ctx| {
            node.on_frame(
                ctx,
                Frame::data(
                    client,
                    server,
                    conn,
                    Bytes::from_static(b"\x00\x01 nope\r\n\r\n"),
                ),
            );
        });
        let frames = buffers.take_frames();
        assert!(frames.len() == 2);
        let (response, _) = HttpResponse::parse(frames[0].payload.data().unwrap()).unwrap();
        assert!(response.status == 400);
        assert!(frames[1].payload == Payload::Close);
        assert!(node.snapshot()["errors"] == 1);
    }

    #[test]
    fn control_commands_serve_requests_and_register() {
        let mut node = node(json!({}));
        let mut buffers = CtxBuffers::new(NodeId(4));
        let registered = buffers.with(0, |ctx| {
            node.control(
                ctx,
                json!({ "cmd": "register", "subject": "s", "schema": av("A") }),
            )
        });
        assert!(registered == Ok(json!({ "id": 1, "version": 1 })));
        let got = buffers.with(0, |ctx| {
            node.control(
                ctx,
                json!({ "cmd": "http", "method": "get", "path": "/subjects/s/versions/1" }),
            )
        });
        assert!(got.unwrap()["body"]["id"] == 1);
        let put = buffers.with(0, |ctx| {
            node.control(ctx, json!({ "cmd": "http", "method": "PUT", "path": "/config", "body": { "compatibility": "NONE" } }))
        });
        assert!(put == Ok(json!({ "status": 200, "body": { "compatibility": "NONE" } })));
        let raw = buffers.with(0, |ctx| {
            node.control(
                ctx,
                json!({ "cmd": "http", "path": "/subjects/s/versions/1/schema" }),
            )
        });
        assert!(raw.unwrap()["body"] == serde_json::from_str::<Value>(&av("A")).unwrap());
        assert!(buffers.take_durable().len() == 2);
        assert!(
            buffers
                .with(0, |ctx| node.control(ctx, json!({ "cmd": "nope" })))
                .is_err()
        );
        assert!(
            buffers
                .with(0, |ctx| node
                    .control(ctx, json!({ "cmd": "register", "subject": "s" })))
                .is_err()
        );
        assert!(
            buffers
                .with(0, |ctx| node.control(ctx, json!({ "cmd": "http" })))
                .is_err()
        );
        let bad = buffers.with(0, |ctx| {
            node.control(
                ctx,
                json!({ "cmd": "register", "subject": "s", "schema": "{" }),
            )
        });
        assert!(bad.is_err());
        let events: Vec<&str> = buffers.events.iter().map(|(kind, _)| *kind).collect();
        assert!(events == vec!["registry", "registry"]);
    }

    #[test]
    fn a_wiped_registry_starts_empty_and_a_reload_restores_the_schemas() {
        let scenario = r#"{"version":1,"nodes":[{"id":4,"kind":"schema-registry"}]}"#;
        let mut w = TestWorld::from_json(scenario);
        let register = |w: &mut TestWorld, subject: &str, schema: String| {
            w.world_mut()
                .control(
                    NodeId(4),
                    json!({ "cmd": "register", "subject": subject, "schema": schema }),
                )
                .unwrap()
        };
        assert!(register(&mut w, "a", av("A")) == json!({ "id": 1, "version": 1 }));
        assert!(register(&mut w, "b", av("B")) == json!({ "id": 2, "version": 1 }));
        w.world_mut()
            .control(NodeId(4), json!({ "cmd": "http", "method": "PUT", "path": "/config/a", "body": { "compatibility": "FULL" } }))
            .unwrap();
        let ops = w.world_mut().drain_durable();
        assert!(ops.len() == 3);
        assert!(ops.iter().all(|(node, _)| *node == NodeId(4)));
        let mut image = DurableImage::default();
        for (_, op) in ops {
            image.apply(op);
        }
        assert!(image.logs["schemas"].len() == 3);

        // A reload: the host folds the ops and hands the image to the new node.
        let parsed: Scenario = serde_json::from_str(scenario).unwrap();
        let mut reloaded =
            World::from_scenario_with_state(&parsed, &[], BTreeMap::from([(NodeId(4), image)]))
                .unwrap();
        let snapshot = reloaded.node_snapshot(NodeId(4)).unwrap();
        assert!(snapshot["schemas"] == 2);
        assert!(snapshot["records"] == 3);
        assert!(snapshot["subjects"][0]["compatibility"] == "FULL");
        let got = reloaded
            .control(
                NodeId(4),
                json!({ "cmd": "http", "path": "/subjects/b/versions/latest" }),
            )
            .unwrap();
        assert!(got["body"] == json!({ "subject": "b", "version": 1, "id": 2, "schema": av("B") }));
        // The restored records are not persisted again; a new one is, at the next offset.
        assert!(reloaded.drain_durable().is_empty());
        assert!(
            reloaded
                .control(
                    NodeId(4),
                    json!({ "cmd": "register", "subject": "b", "schema": av("B2") })
                )
                .unwrap()
                == json!({ "id": 3, "version": 2 })
        );
        let ops = reloaded.drain_durable();
        assert!(matches!(
            &ops[..],
            [(NodeId(4), DurableOp::Append { index: 3, .. })]
        ));

        // A wipe loses the in-memory log and clears the host's stores.
        reloaded.fault(Fault::Wipe { node: NodeId(4) });
        assert!(reloaded.node_snapshot(NodeId(4)).unwrap()["schemas"] == 0);
        assert!(
            reloaded
                .drain_durable()
                .iter()
                .any(|(node, op)| *node == NodeId(4) && *op == DurableOp::ClearAll)
        );
    }
}
