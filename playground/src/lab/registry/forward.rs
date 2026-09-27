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
//! [`relay`] turns the primary's answer into the secondary's: a success
//! passes through, and an error becomes Confluent's `RestException` of the
//! `RestClientException` the primary's body makes, whose message ends in
//! `; error code: <code>`.

use bytes::BytesMut;
use serde_json::{Value, json};

use crate::lab::{
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
#[derive(Default)]
pub struct Forwarder {
    next_conn: u32,
    call: Option<Call>,
    forwarded: u64,
    failed: u64,
}

impl Forwarder {
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
        self.next_conn += 1;
        let conn = ConnId(self.next_conn);
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
    use crate::lab::testing::CtxBuffers;

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
        let data = |bytes: Bytes| Frame::data(primary, me, ConnId(1), bytes);

        let mut forwarder = Forwarder::default();
        buffers.with(0, |ctx| forwarder.send(ctx, NodeId(4), &request, 60_000));
        let mut sent = request.clone();
        sent.close = true;
        assert!(
            buffers.outbox
                == vec![
                    Frame::open(me, primary, ConnId(1)),
                    Frame::data(me, primary, ConnId(1), sent.encode()),
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
                .on_frame(&Frame::close(primary, me, ConnId(1)))
                .is_none()
        );

        buffers.with(10, |ctx| forwarder.send(ctx, NodeId(4), &request, 60_000));
        assert!(
            forwarder.on_frame(&Frame::close(primary, me, ConnId(2)))
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
        assert!(buffers.outbox == vec![Frame::close(me, primary, ConnId(3))]);
        assert!(forwarder.snapshot() == json!({ "active": null, "forwarded": 1, "failed": 2 }));
    }
}
