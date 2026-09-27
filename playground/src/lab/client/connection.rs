//! One connection to one broker: the `Open` frame, the `ApiVersions`
//! negotiation, the in-flight window, the queue behind it, and the reconnect
//! backoff.
//!
//! A connection follows Kafka's `NetworkClient` for one node: nothing is sent
//! before `ApiVersions` answered (KIP-35), at most `max_in_flight` requests
//! wait for an answer at a time, responses arrive in request order, and a
//! close fails everything in flight so the caller can retry elsewhere.

use std::{any::Any, collections::VecDeque};

use bytes::Bytes;
use krabka_protocol::{ProtocolRequest, owned::api_versions_request::ApiVersionsRequest};
use serde_json::{Value, json};

use super::{
    ClientError, ClientOptions,
    request::{self, ApiSpec, Outbound, VersionTable},
    retry,
};
use crate::lab::{
    codes,
    net::{ConnId, Ctx, Endpoint, Frame, Millis},
};

/// Where a connection is in its life.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    /// No connection. `retry_at` is when the next attempt may start.
    Closed { retry_at: Millis },
    /// `Open` went out and an `ApiVersions` request waits for its answer.
    Negotiating(Negotiation),
    /// Versions are known; requests flow.
    Ready,
}

/// The `ApiVersions` request a new connection waits on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Negotiation {
    pub correlation: i32,
    pub version: i16,
    /// When the attempt fails: the connection setup timeout after `Open`.
    pub deadline: Millis,
}

/// A request that waits for its answer.
pub struct InFlight {
    pub correlation: i32,
    pub version: i16,
    pub header_version: i16,
    /// When the request fails with a timeout: `request_timeout_ms` after it
    /// was sent, as Kafka counts it.
    pub deadline: Millis,
    pub request: Outbound,
}

/// A request that finished, well or not.
pub struct Completion {
    pub request: Outbound,
    pub version: i16,
    pub result: Result<Box<dyn Any>, ClientError>,
}

/// What a data frame did to the connection.
pub enum Received {
    /// `ApiVersions` answered; the connection is ready.
    Negotiated,
    /// A request completed.
    Completed(Completion),
    /// Nothing to do: a retry of the negotiation went out, or the frame was
    /// for a connection that is closed.
    Ignored,
    /// The broker refused the negotiation, or sent bytes that do not decode.
    /// The caller closes the connection.
    Broken(ClientError),
}

/// Counters for the inspector.
#[derive(Clone, Copy, Default, Debug)]
pub struct Stats {
    pub opened: u64,
    pub closed: u64,
    pub sent: u64,
    pub received: u64,
}

/// One connection to one broker endpoint.
pub struct Connection {
    endpoint: Endpoint,
    client: Endpoint,
    conn: ConnId,
    state: State,
    versions: VersionTable,
    next_correlation: i32,
    in_flight: VecDeque<InFlight>,
    queue: VecDeque<Outbound>,
    /// Closes since the connection was last ready: Kafka's
    /// `failedReconnectAttempts`, which drives the reconnect backoff.
    failures: u32,
    /// Attempts that closed before they were ready: Kafka's
    /// `failedConnectAttempts`, which drives the setup timeout.
    setup_failures: u32,
    /// When the last attempt started or the connection last closed, `None`
    /// before the first attempt: Kafka's `lastConnectAttemptMs`, which
    /// `leastLoadedNode` compares.
    last_attempt: Option<Millis>,
    /// The broker id metadata maps this endpoint to.
    broker_id: Option<i32>,
    stats: Stats,
}

impl Connection {
    /// A closed connection to `endpoint` from `client`.
    #[must_use]
    pub fn new(client: Endpoint, endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            client,
            conn: ConnId(0),
            state: State::Closed { retry_at: 0 },
            versions: VersionTable::default(),
            next_correlation: 0,
            in_flight: VecDeque::new(),
            queue: VecDeque::new(),
            failures: 0,
            setup_failures: 0,
            last_attempt: None,
            broker_id: None,
            stats: Stats::default(),
        }
    }

    #[must_use]
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    #[must_use]
    pub fn conn_id(&self) -> ConnId {
        self.conn
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.state, State::Ready)
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closed { .. })
    }

    #[must_use]
    pub fn is_negotiating(&self) -> bool {
        matches!(self.state, State::Negotiating(_))
    }

    /// When a closed connection may open again; `0` for an open one.
    #[must_use]
    pub fn retry_at(&self) -> Millis {
        match self.state {
            State::Closed { retry_at } => retry_at,
            State::Negotiating(_) | State::Ready => 0,
        }
    }

    #[must_use]
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    #[must_use]
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// When the last attempt started or the connection last closed, `None`
    /// before the first attempt: Kafka's
    /// `ClusterConnectionStates.lastConnectAttemptMs`, which `connecting`
    /// and `disconnected` set.
    #[must_use]
    pub fn last_attempt(&self) -> Option<Millis> {
        self.last_attempt
    }

    pub fn set_broker_id(&mut self, id: Option<i32>) {
        self.broker_id = id;
    }

    /// Queue a request behind the in-flight window.
    pub fn enqueue(&mut self, request: Outbound) {
        self.queue.push_back(request);
    }

    /// Take every queued request, so the caller can route it elsewhere.
    pub fn take_queue(&mut self) -> Vec<Outbound> {
        self.queue.drain(..).collect()
    }

    /// Take the queued requests whose deadline passed while they waited.
    pub fn expire_queue(&mut self, now: Millis) -> Vec<Outbound> {
        let (expired, kept): (Vec<Outbound>, Vec<Outbound>) =
            self.queue.drain(..).partition(|r| now >= r.deadline);
        self.queue = kept.into_iter().collect();
        expired
    }

    /// Open the connection as `conn`: send `Open`, then `ApiVersions` at the
    /// client's highest version, as Kafka's `NetworkClient` does. The
    /// negotiation must finish within the connection setup timeout, which
    /// doubles with each attempt that failed before it was ready.
    pub fn open(&mut self, ctx: &mut Ctx<'_>, conn: ConnId, opts: &ClientOptions, client_id: &str) {
        self.client = Endpoint::client(ctx.me());
        self.conn = conn;
        self.in_flight.clear();
        self.last_attempt = Some(ctx.now());
        self.stats.opened += 1;
        let setup_timeout = retry::exponential_backoff(
            opts.connection_setup_timeout_ms,
            opts.connection_setup_timeout_max_ms,
            self.setup_failures,
            ctx.rand(400),
        );
        let deadline = ctx.now() + setup_timeout;
        ctx.send(Frame::open(self.client, self.endpoint, conn));
        self.send_api_versions(
            ctx,
            ApiVersionsRequest::LATEST_STABLE_VERSION,
            deadline,
            opts,
            client_id,
        );
    }

    fn send_api_versions(
        &mut self,
        ctx: &mut Ctx<'_>,
        version: i16,
        deadline: Millis,
        opts: &ClientOptions,
        client_id: &str,
    ) {
        let request = ApiVersionsRequest {
            client_software_name: opts.software_name.clone(),
            client_software_version: opts.software_version.clone(),
            ..Default::default()
        };
        let correlation = self.next_correlation();
        let api = ApiSpec::of::<ApiVersionsRequest>();
        match request::frame_request(api, version, correlation, client_id, &request) {
            Ok(bytes) => {
                self.stats.sent += 1;
                ctx.send(Frame::data(self.client, self.endpoint, self.conn, bytes));
                self.state = State::Negotiating(Negotiation {
                    correlation,
                    version,
                    deadline,
                });
            }
            // `ApiVersions` encodes at every version it has; a failure here
            // is a codec defect, and the connection is closed without a wait
            // so the caller sees a stalled negotiation rather than a panic.
            Err(_) => {
                self.state = State::Closed {
                    retry_at: ctx.now(),
                };
            }
        }
    }

    fn next_correlation(&mut self) -> i32 {
        let id = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);
        id
    }

    /// A data frame arrived on this connection.
    pub fn on_data(
        &mut self,
        ctx: &mut Ctx<'_>,
        bytes: &Bytes,
        opts: &ClientOptions,
        client_id: &str,
    ) -> Received {
        match self.state {
            State::Negotiating(negotiation) => {
                self.on_api_versions(ctx, bytes, negotiation, opts, client_id)
            }
            State::Ready => self.on_response(bytes),
            State::Closed { .. } => Received::Ignored,
        }
    }

    fn on_api_versions(
        &mut self,
        ctx: &mut Ctx<'_>,
        bytes: &Bytes,
        negotiation: Negotiation,
        opts: &ClientOptions,
        client_id: &str,
    ) -> Received {
        let Negotiation {
            correlation,
            version,
            deadline,
        } = negotiation;
        self.stats.received += 1;
        let (header, body) = match request::response_body(bytes, 0) {
            Ok(parts) => parts,
            Err(e) => return Received::Broken(ClientError::Protocol(e)),
        };
        if header.correlation_id != correlation {
            return Received::Broken(ClientError::CorrelationMismatch {
                expected: correlation,
                got: header.correlation_id,
            });
        }
        let response = match request::decode_api_versions(body, version) {
            Ok(response) => response,
            Err(e) => return Received::Broken(ClientError::Protocol(e)),
        };
        if response.error_code == codes::UNSUPPORTED_VERSION && version > 0 {
            let retry = request::api_versions_retry_version(&response, version);
            self.send_api_versions(ctx, retry, deadline, opts, client_id);
            return Received::Ignored;
        }
        if response.error_code != codes::NONE {
            return Received::Broken(ClientError::Broker {
                api: "ApiVersions",
                code: response.error_code,
            });
        }
        self.versions = VersionTable::from_response(&response);
        self.state = State::Ready;
        // Kafka's `ClusterConnectionStates.ready` resets both backoffs.
        self.failures = 0;
        self.setup_failures = 0;
        Received::Negotiated
    }

    fn on_response(&mut self, bytes: &Bytes) -> Received {
        let Some(correlation) = request::correlation_id_of(bytes) else {
            return Received::Broken(ClientError::Protocol(
                krabka_protocol::ProtocolError::UnexpectedEof { needed: 8 },
            ));
        };
        let Some(position) = self
            .in_flight
            .iter()
            .position(|f| f.correlation == correlation)
        else {
            let expected = self.in_flight.front().map_or(-1, |f| f.correlation);
            return Received::Broken(ClientError::CorrelationMismatch {
                expected,
                got: correlation,
            });
        };
        let Some(in_flight) = self.in_flight.remove(position) else {
            return Received::Ignored;
        };
        self.stats.received += 1;
        let result = request::response_body(bytes, in_flight.header_version)
            .and_then(|(_, body)| (in_flight.request.decoder)(body, in_flight.version))
            .map_err(ClientError::Protocol);
        Received::Completed(Completion {
            request: in_flight.request,
            version: in_flight.version,
            result,
        })
    }

    /// The connection closed: the peer or the world sent `Close`, or the
    /// caller gave up on it. Everything in flight is returned so the caller
    /// can fail it, and the next attempt waits `backoff`. As in Kafka's
    /// `ClusterConnectionStates.disconnected`, the close counts as the last
    /// attempt, an attempt that closes before it was ready lengthens the
    /// next setup timeout, and any other close resets it.
    pub fn on_close(&mut self, now: Millis, backoff: Millis) -> Vec<InFlight> {
        self.setup_failures = if self.is_negotiating() {
            self.setup_failures.saturating_add(1)
        } else {
            0
        };
        self.last_attempt = Some(now);
        self.state = State::Closed {
            retry_at: now + backoff,
        };
        self.failures = self.failures.saturating_add(1);
        self.stats.closed += 1;
        self.in_flight.drain(..).collect()
    }

    /// Whether the oldest request in flight, or the negotiation, passed its
    /// deadline.
    #[must_use]
    pub fn timed_out(&self, now: Millis) -> bool {
        match self.state {
            State::Negotiating(negotiation) => now >= negotiation.deadline,
            State::Ready => self.in_flight.front().is_some_and(|f| now >= f.deadline),
            State::Closed { .. } => false,
        }
    }

    /// Send queued requests while the in-flight window has room.
    pub fn pump(
        &mut self,
        ctx: &mut Ctx<'_>,
        client_id: &str,
        opts: &ClientOptions,
        now: Millis,
    ) -> Vec<Completion> {
        let mut done = Vec::new();
        if !self.is_ready() {
            return done;
        }
        while self.in_flight.len() < opts.max_in_flight {
            let Some(request) = self.queue.pop_front() else {
                break;
            };
            let version = match self.versions.negotiate(request.api) {
                Ok(version) => version,
                Err(e) => {
                    done.push(Completion {
                        request,
                        version: -1,
                        result: Err(e),
                    });
                    continue;
                }
            };
            let correlation = self.next_correlation();
            let framed = request::frame_request(
                request.api,
                version,
                correlation,
                client_id,
                request.body.as_ref(),
            );
            match framed {
                Ok(bytes) => {
                    self.stats.sent += 1;
                    ctx.send(Frame::data(self.client, self.endpoint, self.conn, bytes));
                    if request.oneway {
                        done.push(Completion {
                            request,
                            version,
                            result: Ok(Box::new(())),
                        });
                    } else {
                        let header_version = request::response_header_version(
                            request.api.key,
                            request.api.flexible_min,
                            version,
                        );
                        self.in_flight.push_back(InFlight {
                            correlation,
                            version,
                            header_version,
                            deadline: now + opts.request_timeout_ms,
                            request,
                        });
                    }
                }
                Err(e) => done.push(Completion {
                    request,
                    version,
                    result: Err(ClientError::Protocol(e)),
                }),
            }
        }
        done
    }

    /// The earliest time this connection needs a tick: the deadline of the
    /// oldest request in flight or queued, the setup timeout of a
    /// negotiation, or the retry time of a closed connection with queued
    /// work.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        let queued = self.queue.iter().map(|r| r.deadline).min();
        let own = match self.state {
            State::Negotiating(negotiation) => Some(negotiation.deadline),
            State::Ready => self.in_flight.front().map(|f| f.deadline),
            State::Closed { retry_at } => (!self.queue.is_empty()).then_some(retry_at),
        };
        own.into_iter().chain(queued).min()
    }

    /// The connection for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let state = match self.state {
            State::Closed { .. } => "closed",
            State::Negotiating(_) => "connecting",
            State::Ready => "ready",
        };
        json!({
            "broker": self.endpoint.node,
            "broker_id": self.broker_id,
            "conn": self.conn,
            "state": state,
            "in_flight": self.in_flight.len(),
            "queued": self.queue.len(),
            "versions_negotiated": self.versions.len(),
            "failures": self.failures,
            "sent": self.stats.sent,
            "received": self.stats.received,
            "opened": self.stats.opened,
            "closed": self.stats.closed,
        })
    }
}
