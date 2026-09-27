//! An HTTP/1.1 client of one schema registry, over one lab connection.
//!
//! The producer registers its schema and the consumer and the streams node
//! look schemas up through it, as Confluent's `CachedSchemaRegistryClient`
//! talks to the REST API. It sends one request at a time on a keep-alive
//! connection to [`Endpoint::http`] of the registry and answers each in
//! order. A refused or lost connection, and a request with no answer within
//! [`REQUEST_TIMEOUT_MS`], fail every request the client holds; the caller
//! retries with its own backoff, and the next request opens a new
//! connection.
//!
//! The connection ids come from the upper half of the id space, from
//! [`CONN_ID_BASE`] up, so they never meet the ids the node's Kafka client
//! numbers from 1.

use std::collections::{BTreeMap, VecDeque};

use derive_more::{Display, From, Into};
use serde_json::{Value, json};

use super::serde::{SchemaFormat, ValueSchema};
use crate::lab::{
    net::{ConnId, Ctx, Endpoint, Frame, Millis, NodeId, Payload},
    registry::http::{HttpRequest, HttpResponse},
};

/// The first connection id of a registry client.
pub const CONN_ID_BASE: u32 = 1 << 31;

/// How long a request may wait for its answer before the client closes the
/// connection and fails it.
pub const REQUEST_TIMEOUT_MS: Millis = 10_000;

/// The wait before retry number `attempts` of a failed registry request:
/// 500 ms, doubling up to 10 s.
#[must_use]
pub fn retry_backoff(attempts: u32) -> Millis {
    (500_u64 << attempts.min(5)).min(10_000)
}

/// The id of a request the client accepted.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Display, From, Into)]
pub struct RegistryRequestId(pub u64);

/// How a request ended.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RegistryReply {
    /// The registry answered.
    Response(HttpResponse),
    /// The connection was refused or lost, or the request timed out.
    Failed(String),
}

struct Request {
    id: RegistryRequestId,
    request: HttpRequest,
}

/// The client. See the module documentation.
pub struct RegistryClient {
    registry: NodeId,
    conn: Option<ConnId>,
    next_conn: u32,
    next_request: u64,
    queue: VecDeque<Request>,
    /// The request on the wire, with its deadline.
    in_flight: Option<(Request, Millis)>,
    sent: u64,
    failures: u64,
    last_error: Option<String>,
}

impl RegistryClient {
    /// A client of the registry node `registry`.
    #[must_use]
    pub fn new(registry: NodeId) -> Self {
        Self {
            registry,
            conn: None,
            next_conn: CONN_ID_BASE,
            next_request: 0,
            queue: VecDeque::new(),
            in_flight: None,
            sent: 0,
            failures: 0,
            last_error: None,
        }
    }

    /// The registry node.
    #[must_use]
    pub fn registry(&self) -> NodeId {
        self.registry
    }

    fn endpoint(&self) -> Endpoint {
        Endpoint::http(self.registry)
    }

    /// Whether `frame` belongs to this client's connection.
    #[must_use]
    pub fn owns(&self, frame: &Frame) -> bool {
        frame.src == self.endpoint() && self.conn == Some(frame.conn)
    }

    /// Whether a request waits or is on the wire.
    #[must_use]
    pub fn busy(&self) -> bool {
        self.in_flight.is_some() || !self.queue.is_empty()
    }

    /// Queue `request`; it goes out once the requests before it answered.
    pub fn send(&mut self, ctx: &mut Ctx<'_>, request: HttpRequest) -> RegistryRequestId {
        self.next_request += 1;
        let id = RegistryRequestId(self.next_request);
        self.queue.push_back(Request { id, request });
        self.pump(ctx);
        id
    }

    fn pump(&mut self, ctx: &mut Ctx<'_>) {
        if self.in_flight.is_some() {
            return;
        }
        let Some(next) = self.queue.pop_front() else {
            return;
        };
        let me = Endpoint::client(ctx.me());
        let conn = if let Some(conn) = self.conn {
            conn
        } else {
            let conn = ConnId(self.next_conn);
            self.next_conn = self.next_conn.wrapping_add(1).max(CONN_ID_BASE);
            self.conn = Some(conn);
            ctx.send(Frame::open(me, self.endpoint(), conn));
            conn
        };
        ctx.send(Frame::data(
            me,
            self.endpoint(),
            conn,
            next.request.encode(),
        ));
        self.sent += 1;
        self.in_flight = Some((next, ctx.now() + REQUEST_TIMEOUT_MS));
    }

    /// A frame of this client's connection arrived.
    pub fn on_frame(
        &mut self,
        ctx: &mut Ctx<'_>,
        frame: Frame,
    ) -> Vec<(RegistryRequestId, RegistryReply)> {
        if !self.owns(&frame) {
            return Vec::new();
        }
        let mut out = Vec::new();
        match frame.payload {
            Payload::Data(bytes) => {
                if let Some((request, _)) = self.in_flight.take() {
                    let reply = match HttpResponse::parse(&bytes) {
                        Ok((response, _)) => {
                            self.last_error = None;
                            RegistryReply::Response(response)
                        }
                        Err(e) => {
                            self.failures += 1;
                            RegistryReply::Failed(e.to_string())
                        }
                    };
                    out.push((request.id, reply));
                }
            }
            Payload::Close => {
                let reason = if self.in_flight.is_some() {
                    "the registry closed the connection"
                } else {
                    "the registry refused the connection"
                };
                self.conn = None;
                self.fail_all(reason, &mut out);
            }
            Payload::Open => {}
        }
        self.pump(ctx);
        out
    }

    /// Time out the request on the wire when its deadline passed.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> Vec<(RegistryRequestId, RegistryReply)> {
        let mut out = Vec::new();
        let expired = self
            .in_flight
            .as_ref()
            .is_some_and(|(_, deadline)| ctx.now() >= *deadline);
        if expired {
            if let Some(conn) = self.conn.take() {
                ctx.send(Frame::close(
                    Endpoint::client(ctx.me()),
                    self.endpoint(),
                    conn,
                ));
            }
            self.fail_all(
                &format!("the registry did not answer within {REQUEST_TIMEOUT_MS} ms"),
                &mut out,
            );
        }
        self.pump(ctx);
        out
    }

    fn fail_all(&mut self, reason: &str, out: &mut Vec<(RegistryRequestId, RegistryReply)>) {
        let failed = self
            .in_flight
            .take()
            .map(|(request, _)| request)
            .into_iter()
            .chain(self.queue.drain(..));
        for request in failed {
            self.failures += 1;
            out.push((request.id, RegistryReply::Failed(reason.to_string())));
        }
        self.last_error = Some(reason.to_string());
    }

    /// The deadline of the request on the wire.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        self.in_flight.as_ref().map(|(_, deadline)| *deadline)
    }

    /// Forget the connection and every request, as a restarted process has
    /// neither.
    pub fn reset(&mut self) {
        self.conn = None;
        self.queue.clear();
        self.in_flight = None;
        self.last_error = None;
    }

    /// The client for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "registry": self.registry,
            "connected": self.conn.is_some(),
            "pending": self.queue.len() + usize::from(self.in_flight.is_some()),
            "requests": self.sent,
            "failures": self.failures,
            "last_error": self.last_error,
        })
    }
}

/// Where the lookup of one schema id stands.
enum CacheEntry {
    Fetching {
        request: RegistryRequestId,
        attempts: u32,
    },
    Retry {
        at: Millis,
        attempts: u32,
    },
    Ready(ValueSchema),
    /// The registry does not know the id, or its schema is of a type the lab
    /// does not decode.
    Failed(String),
}

/// What a lookup found.
pub enum SchemaLookup<'a> {
    Ready(&'a ValueSchema),
    /// The lookup is on the wire or waits for its retry.
    Pending,
    Failed(&'a str),
}

/// Schemas by id, fetched from the registry with `GET /schemas/ids/{id}` on
/// first use and kept, as Confluent's deserializers cache them. A failed
/// lookup is retried with [`retry_backoff`]; an id the registry answers 404
/// for stays failed.
pub struct SchemaCache {
    client: RegistryClient,
    entries: BTreeMap<i32, CacheEntry>,
}

impl SchemaCache {
    /// A cache over the registry node `registry`.
    #[must_use]
    pub fn new(registry: NodeId) -> Self {
        Self {
            client: RegistryClient::new(registry),
            entries: BTreeMap::new(),
        }
    }

    /// Whether `frame` belongs to the cache's connection.
    #[must_use]
    pub fn owns(&self, frame: &Frame) -> bool {
        self.client.owns(frame)
    }

    /// The schema of `id`, starting its lookup when the cache has not seen
    /// the id.
    pub fn lookup(&mut self, ctx: &mut Ctx<'_>, id: i32) -> SchemaLookup<'_> {
        if !self.entries.contains_key(&id) {
            self.fetch(ctx, id, 0);
        }
        self.peek(id)
    }

    /// The schema of `id`, without starting a lookup.
    #[must_use]
    pub fn peek(&self, id: i32) -> SchemaLookup<'_> {
        match self.entries.get(&id) {
            Some(CacheEntry::Ready(schema)) => SchemaLookup::Ready(schema),
            Some(CacheEntry::Failed(reason)) => SchemaLookup::Failed(reason),
            Some(CacheEntry::Fetching { .. } | CacheEntry::Retry { .. }) | None => {
                SchemaLookup::Pending
            }
        }
    }

    fn fetch(&mut self, ctx: &mut Ctx<'_>, id: i32, attempts: u32) {
        let request = self
            .client
            .send(ctx, HttpRequest::new("GET", &format!("/schemas/ids/{id}")));
        self.entries
            .insert(id, CacheEntry::Fetching { request, attempts });
    }

    fn settle(&mut self, now: Millis, replies: Vec<(RegistryRequestId, RegistryReply)>) -> bool {
        let mut settled = false;
        for (request_id, reply) in replies {
            let Some((&id, attempts)) = self.entries.iter().find_map(|(id, e)| match e {
                CacheEntry::Fetching { request, attempts } if *request == request_id => {
                    Some((id, *attempts))
                }
                _ => None,
            }) else {
                continue;
            };
            let entry = match reply {
                RegistryReply::Response(response) if response.status == 200 => {
                    match parse_schema_response(&response) {
                        Ok(schema) => CacheEntry::Ready(schema),
                        Err(reason) => CacheEntry::Failed(reason),
                    }
                }
                RegistryReply::Response(response) if response.status == 404 => {
                    CacheEntry::Failed(format!("the registry has no schema {id}"))
                }
                RegistryReply::Response(_) | RegistryReply::Failed(_) => CacheEntry::Retry {
                    at: now + retry_backoff(attempts),
                    attempts: attempts.saturating_add(1),
                },
            };
            settled |= matches!(entry, CacheEntry::Ready(_) | CacheEntry::Failed(_));
            self.entries.insert(id, entry);
        }
        settled
    }

    /// A frame of the cache's connection arrived. Returns whether a lookup
    /// finished.
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> bool {
        let replies = self.client.on_frame(ctx, frame);
        self.settle(ctx.now(), replies)
    }

    /// Time out the request on the wire and send the retries that are due.
    /// Returns whether a lookup finished.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> bool {
        let replies = self.client.on_tick(ctx);
        let settled = self.settle(ctx.now(), replies);
        let due: Vec<(i32, u32)> = self
            .entries
            .iter()
            .filter_map(|(id, e)| match e {
                CacheEntry::Retry { at, attempts } if *at <= ctx.now() => Some((*id, *attempts)),
                _ => None,
            })
            .collect();
        for (id, attempts) in due {
            self.fetch(ctx, id, attempts);
        }
        settled
    }

    /// The next retry or request deadline.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        self.entries
            .values()
            .filter_map(|e| match e {
                CacheEntry::Retry { at, .. } => Some(*at),
                _ => None,
            })
            .chain(self.client.next_deadline())
            .min()
    }

    /// Forget every schema and the connection, as a restarted process has
    /// neither.
    pub fn reset(&mut self) {
        self.client.reset();
        self.entries.clear();
    }

    /// The cache for the inspector: `{"registry", "schemas": {"<id>":
    /// "ready"|"fetching"|"retrying"|<error>}, "client"}`.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let schemas: serde_json::Map<String, Value> = self
            .entries
            .iter()
            .map(|(id, e)| {
                let state = match e {
                    CacheEntry::Ready(schema) => format!("ready ({})", schema.format().name()),
                    CacheEntry::Fetching { .. } => "fetching".to_string(),
                    CacheEntry::Retry { .. } => "retrying".to_string(),
                    CacheEntry::Failed(reason) => reason.clone(),
                };
                (id.to_string(), Value::String(state))
            })
            .collect();
        json!({
            "registry": self.client.registry(),
            "schemas": schemas,
            "client": self.client.snapshot(),
        })
    }
}

/// The schema a `GET /schemas/ids/{id}` answer carries.
fn parse_schema_response(response: &HttpResponse) -> Result<ValueSchema, String> {
    let body = response
        .body_json()
        .ok_or_else(|| format!("the registry answered {}", response.body))?;
    let text = body
        .get("schema")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the registry answered {}", response.body))?;
    let format = SchemaFormat::from_registry_type(body.get("schemaType").and_then(Value::as_str))
        .map_err(|e| e.to_string())?;
    ValueSchema::parse(format, text).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::lab::testing::CtxBuffers;

    const ME: NodeId = NodeId(5);
    const REGISTRY: NodeId = NodeId(4);

    fn reply(frame: &Frame, status: u16, body: &Value) -> Frame {
        frame.reply(Payload::Data(HttpResponse::json(status, body).encode()))
    }

    #[test]
    fn requests_go_out_one_at_a_time_on_one_connection() {
        let mut client = RegistryClient::new(REGISTRY);
        let mut bufs = CtxBuffers::new(ME);
        let first = bufs.with(0, |ctx| {
            client.send(
                ctx,
                HttpRequest::new("POST", "/subjects/t-value/versions")
                    .with_json(&json!({"schema": "\"string\""})),
            )
        });
        let second = bufs.with(0, |ctx| {
            client.send(ctx, HttpRequest::new("GET", "/schemas/ids/1"))
        });
        let frames = bufs.take_frames();
        let conn = ConnId(CONN_ID_BASE);
        let to = Endpoint::http(REGISTRY);
        let from = Endpoint::client(ME);
        assert!(frames.len() == 2);
        assert!(frames[0] == Frame::open(from, to, conn));
        let (request, _) = HttpRequest::parse(frames[1].payload.data().unwrap()).unwrap();
        assert!(request.method == "POST");
        assert!(request.path == "/subjects/t-value/versions");
        assert!(client.next_deadline() == Some(REQUEST_TIMEOUT_MS));

        let answers = bufs.with(3, |ctx| {
            client.on_frame(ctx, reply(&frames[1], 200, &json!({"id": 1})))
        });
        assert!(
            answers
                == vec![(
                    first,
                    RegistryReply::Response(HttpResponse::json(200, &json!({"id": 1})))
                )]
        );
        // The second request follows on the same connection.
        let frames = bufs.take_frames();
        assert!(frames.len() == 1);
        assert!(frames[0].conn == conn);
        let answers = bufs.with(6, |ctx| {
            client.on_frame(ctx, reply(&frames[0], 404, &json!({})))
        });
        assert!(answers[0].0 == second);
        assert!(!client.busy());
    }

    #[test]
    fn a_refused_connection_fails_everything_and_the_next_request_reconnects() {
        let mut client = RegistryClient::new(REGISTRY);
        let mut bufs = CtxBuffers::new(ME);
        let a = bufs.with(0, |ctx| {
            client.send(ctx, HttpRequest::new("GET", "/subjects"))
        });
        let b = bufs.with(0, |ctx| {
            client.send(ctx, HttpRequest::new("GET", "/config"))
        });
        let frames = bufs.take_frames();
        let close = Frame::close(
            Endpoint::http(REGISTRY),
            Endpoint::client(ME),
            frames[0].conn,
        );
        let answers = bufs.with(5, |ctx| client.on_frame(ctx, close));
        let failed = RegistryReply::Failed("the registry closed the connection".to_string());
        assert!(answers == vec![(a, failed.clone()), (b, failed)]);
        assert!(client.snapshot()["last_error"] == "the registry closed the connection");
        bufs.with(6, |ctx| {
            client.send(ctx, HttpRequest::new("GET", "/subjects"))
        });
        let frames = bufs.take_frames();
        assert!(frames[0].payload == Payload::Open);
        assert!(frames[0].conn == ConnId(CONN_ID_BASE + 1));
    }

    #[test]
    fn a_silent_registry_times_out() {
        let mut client = RegistryClient::new(REGISTRY);
        let mut bufs = CtxBuffers::new(ME);
        let id = bufs.with(0, |ctx| {
            client.send(ctx, HttpRequest::new("GET", "/subjects"))
        });
        bufs.take_frames();
        assert!(
            bufs.with(REQUEST_TIMEOUT_MS - 1, |ctx| client.on_tick(ctx))
                .is_empty()
        );
        let answers = bufs.with(REQUEST_TIMEOUT_MS, |ctx| client.on_tick(ctx));
        assert!(
            answers
                == vec![(
                    id,
                    RegistryReply::Failed(
                        "the registry did not answer within 10000 ms".to_string()
                    )
                )]
        );
        let frames = bufs.take_frames();
        assert!(frames.len() == 1);
        assert!(frames[0].payload == Payload::Close);
    }

    #[test]
    fn frames_of_other_connections_are_not_the_clients() {
        let mut client = RegistryClient::new(REGISTRY);
        let mut bufs = CtxBuffers::new(ME);
        bufs.with(0, |ctx| {
            client.send(ctx, HttpRequest::new("GET", "/subjects"))
        });
        let stray = Frame::data(
            Endpoint::kafka(NodeId(1)),
            Endpoint::client(ME),
            ConnId(CONN_ID_BASE),
            Bytes::from_static(b"x"),
        );
        assert!(!client.owns(&stray));
        assert!(bufs.with(1, |ctx| client.on_frame(ctx, stray)).is_empty());
        assert!(client.busy());
    }
}
