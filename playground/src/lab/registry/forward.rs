//! The HTTP client a secondary forwards writes with: Confluent's
//! `leaderRestService`.
//!
//! A registry that is not the primary sends a write to the primary's REST
//! listener and answers with what comes back. [`Forwarder`] sends one
//! request at a time, each on a connection of its own that the request asks
//! to close (`Connection: close`). The answer ends the call; so do a refused
//! or closed connection and `leader.read.timeout.ms` without an answer,
//! which Confluent reports as an `IOException` and the lab as
//! [`Forwarded::Failed`].
//!
//! The forwarder is no Kafka client, but it sends from the node's client
//! endpoint as the registry's Kafka clients do, so it draws its connection
//! ids from a range of its own, one [`CONN_ID_RANGE`] from a base the node
//! gives it, as [`conn_base`](crate::lab::client::conn_base) spaces the
//! Kafka clients' ranges.
//!
//! [`relay`] turns the primary's answer into the secondary's: a success
//! passes through, and an error becomes Confluent's `RestException` of the
//! `RestClientException` the primary's body makes, whose message ends in
//! `; error code: <code>`.

use bytes::BytesMut;
use serde_json::{Value, json};

use crate::lab::{
    client::CONN_ID_RANGE,
    net::{ConnId, Ctx, Endpoint, Frame, Millis, NodeId, Payload},
    registry::http::{HttpError, HttpRequest, HttpResponse},
};

/// The error code of an error body that is not Confluent's JSON:
/// `RestService.JSON_PARSE_ERROR_CODE`.
const JSON_PARSE_ERROR_CODE: i32 = 50_005;

/// How a forwarded request ended.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Forwarded {
    /// The primary answered.
    Answered(HttpResponse),
    /// No answer came: why.
    Failed(String),
}

/// The request in flight.
struct Call {
    conn: ConnId,
    to: Endpoint,
    method: String,
    path: String,
    buffer: BytesMut,
    deadline: Millis,
}

/// The forwarding client. See the module documentation.
pub struct Forwarder {
    conn_base: u32,
    /// The connections opened so far.
    opened: u32,
    call: Option<Call>,
    forwarded: u64,
    failed: u64,
}

impl Forwarder {
    /// A forwarder that numbers its connections from `conn_base + 1` to
    /// `conn_base + CONN_ID_RANGE - 1`, then starts over.
    #[must_use]
    pub fn new(conn_base: u32) -> Self {
        Self {
            conn_base,
            opened: 0,
            call: None,
            forwarded: 0,
            failed: 0,
        }
    }

    /// Whether a connection id is in the forwarder's range.
    #[must_use]
    pub fn owns_conn(&self, conn: ConnId) -> bool {
        conn.0.wrapping_sub(self.conn_base).wrapping_sub(1) < CONN_ID_RANGE - 1
    }

    /// Send `request` to the registry on `to`. The outcome comes from
    /// [`Forwarder::on_frame`] or, after `timeout_ms`, from
    /// [`Forwarder::on_tick`].
    pub fn send(
        &mut self,
        ctx: &mut Ctx<'_>,
        to: NodeId,
        request: &HttpRequest,
        timeout_ms: Millis,
    ) {
        self.abort(ctx);
        let conn = ConnId(
            self.conn_base
                .wrapping_add(1 + self.opened % (CONN_ID_RANGE - 1)),
        );
        self.opened = self.opened.wrapping_add(1);
        let me = Endpoint::client(ctx.me());
        let target = Endpoint::http(to);
        let mut request = request.clone();
        request.close = true;
        ctx.send(Frame::open(me, target, conn));
        ctx.send(Frame::data(me, target, conn, request.encode()));
        self.call = Some(Call {
            conn,
            to: target,
            method: request.method,
            path: request.path,
            buffer: BytesMut::new(),
            deadline: ctx.now() + timeout_ms,
        });
    }

    /// A frame for the forwarding connection.
    pub fn on_frame(&mut self, frame: &Frame) -> Option<Forwarded> {
        let call = self
            .call
            .as_mut()
            .filter(|c| c.conn == frame.conn && c.to == frame.src)?;
        match &frame.payload {
            Payload::Open => None,
            Payload::Data(bytes) => {
                call.buffer.extend_from_slice(bytes);
                match HttpResponse::parse(&call.buffer) {
                    Ok((response, _)) => Some(self.end(Forwarded::Answered(response))),
                    Err(HttpError::Incomplete) => None,
                    Err(error) => Some(self.end(Forwarded::Failed(format!(
                        "the primary's answer is not HTTP: {error}"
                    )))),
                }
            }
            Payload::Close => {
                let reason = if call.buffer.is_empty() {
                    "the primary refused or closed the connection"
                } else {
                    "the primary closed the connection within its answer"
                };
                Some(self.end(Forwarded::Failed(reason.to_string())))
            }
        }
    }

    /// The timer fired: a call past its deadline fails.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> Option<Forwarded> {
        let deadline = self.call.as_ref()?.deadline;
        if ctx.now() < deadline {
            return None;
        }
        self.abort(ctx);
        Some(self.end(Forwarded::Failed("Read timed out".to_string())))
    }

    /// Close the connection of the call in flight, if any.
    pub fn abort(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(call) = &self.call {
            ctx.send(Frame::close(Endpoint::client(ctx.me()), call.to, call.conn));
        }
    }

    fn end(&mut self, outcome: Forwarded) -> Forwarded {
        self.call = None;
        match outcome {
            Forwarded::Answered(_) => self.forwarded += 1,
            Forwarded::Failed(_) => self.failed += 1,
        }
        outcome
    }

    /// The deadline of the call in flight.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        self.call.as_ref().map(|c| c.deadline)
    }

    /// The client for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "active": self.call.as_ref().map(|c| json!({
                "to": c.to.node.0,
                "method": c.method,
                "path": c.path,
                "deadline": c.deadline,
            })),
            "forwarded": self.forwarded,
            "failed": self.failed,
        })
    }
}

/// The secondary's answer to a request the primary answered with
/// `response`: a success as it came, an error as the `RestException` the
/// `RestClientException` of its body becomes.
#[must_use]
pub fn relay(response: HttpResponse) -> HttpResponse {
    if response.status < 400 {
        return response;
    }
    let body = response.body_json();
    let parsed = body.as_ref().and_then(|b| {
        let code = b.get("error_code")?.as_i64()?;
        let message = b.get("message")?.as_str()?;
        Some((i32::try_from(code).ok()?, message.to_string()))
    });
    let (code, message) = parsed.unwrap_or((JSON_PARSE_ERROR_CODE, response.body));
    HttpResponse::error(
        response.status,
        code,
        format!("{message}; error code: {code}"),
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::lab::{client::conn_base, testing::CtxBuffers};

    #[test]
    fn an_answer_relays_a_success_as_it_came_and_an_error_as_a_rest_client_exception() {
        let cases = [
            (
                HttpResponse::ok(&json!({ "id": 1 })),
                HttpResponse::ok(&json!({ "id": 1 })),
            ),
            (
                HttpResponse::error(409, 409, "Schema being registered is incompatible"),
                HttpResponse::error(
                    409,
                    409,
                    "Schema being registered is incompatible; error code: 409",
                ),
            ),
            (
                HttpResponse {
                    status: 502,
                    body: "Bad Gateway".to_string(),
                },
                HttpResponse::error(502, 50_005, "Bad Gateway; error code: 50005"),
            ),
        ];
        for (answer, relayed) in cases {
            assert!(relay(answer.clone()) == relayed, "{answer:?}");
        }
    }

    #[test]
    fn a_call_ends_with_its_answer_its_closed_connection_or_its_timeout() {
        let mut buffers = CtxBuffers::new(NodeId(5));
        let request = HttpRequest::new("POST", "/subjects/s/versions");
        let primary = Endpoint::http(NodeId(4));
        let me = Endpoint::client(NodeId(5));
        // The forwarder numbers its connections from its base on.
        let base = conn_base(4);
        let conn = |n: u32| ConnId(base + n);
        let data = |bytes: Bytes| Frame::data(primary, me, conn(1), bytes);

        let mut forwarder = Forwarder::new(base);
        buffers.with(0, |ctx| forwarder.send(ctx, NodeId(4), &request, 60_000));
        let mut sent = request.clone();
        sent.close = true;
        assert!(
            buffers.outbox
                == vec![
                    Frame::open(me, primary, conn(1)),
                    Frame::data(me, primary, conn(1), sent.encode()),
                ]
        );
        let answer = HttpResponse::ok(&json!({ "id": 1 })).encode();
        let (head, tail) = answer.split_at(10);
        assert!(
            forwarder
                .on_frame(&data(Bytes::copy_from_slice(head)))
                .is_none()
        );
        assert!(
            forwarder.on_frame(&data(Bytes::copy_from_slice(tail)))
                == Some(Forwarded::Answered(HttpResponse::ok(&json!({ "id": 1 }))))
        );
        // The primary's close after its answer belongs to no call.
        assert!(
            forwarder
                .on_frame(&Frame::close(primary, me, conn(1)))
                .is_none()
        );

        buffers.with(10, |ctx| forwarder.send(ctx, NodeId(4), &request, 60_000));
        assert!(
            forwarder.on_frame(&Frame::close(primary, me, conn(2)))
                == Some(Forwarded::Failed(
                    "the primary refused or closed the connection".to_string()
                ))
        );

        buffers.with(20, |ctx| forwarder.send(ctx, NodeId(4), &request, 60_000));
        assert!(forwarder.next_deadline() == Some(60_020));
        assert!(buffers.with(60_019, |ctx| forwarder.on_tick(ctx)).is_none());
        buffers.outbox.clear();
        assert!(
            buffers.with(60_020, |ctx| forwarder.on_tick(ctx))
                == Some(Forwarded::Failed("Read timed out".to_string()))
        );
        assert!(buffers.outbox == vec![Frame::close(me, primary, conn(3))]);
        assert!(forwarder.snapshot() == json!({ "active": null, "forwarded": 1, "failed": 2 }));
    }

    #[test]
    fn the_forwarder_owns_the_range_above_its_base_and_wraps_within_it() {
        let base = conn_base(9);
        let forwarder = Forwarder::new(base);
        let owned: Vec<bool> = [
            base,
            base + 1,
            base + CONN_ID_RANGE - 1,
            base + CONN_ID_RANGE,
            conn_base(8) + 1,
        ]
        .into_iter()
        .map(|id| forwarder.owns_conn(ConnId(id)))
        .collect();
        assert!(owned == vec![false, true, true, false, false]);
        // After the last id of the range, the next call opens the first.
        let mut buffers = CtxBuffers::new(NodeId(5));
        let mut forwarder = Forwarder::new(base);
        forwarder.opened = CONN_ID_RANGE - 2;
        let request = HttpRequest::new("DELETE", "/config");
        for expected in [base + CONN_ID_RANGE - 1, base + 1] {
            buffers.outbox.clear();
            buffers.with(0, |ctx| forwarder.send(ctx, NodeId(4), &request, 1_000));
            let opened: Vec<ConnId> = buffers
                .outbox
                .iter()
                .filter(|f| f.payload == Payload::Open)
                .map(|f| f.conn)
                .collect();
            assert!(opened == vec![ConnId(expected)]);
        }
    }
}
