//! The broker's channels to the active controller, Kafka's
//! `NodeToControllerChannelManager`.
//!
//! A [`ControllerChannel`] keeps one connection from the broker's client
//! socket to the controller listener of the node the local quorum names as
//! leader. It negotiates `ApiVersions` first, then sends the queued requests
//! one at a time (Kafka's channel allows one in flight). A request answered
//! with `NOT_CONTROLLER`, or lost with its connection, goes back to the head
//! of the queue and the channel looks the controller up again, as Kafka's
//! `NodeToControllerRequestThread.handleResponse` does. A request that waits
//! in the queue longer than the channel's retry timeout completes as
//! [`ChannelOutcome::TimedOut`], and one in flight longer than its request
//! timeout closes the connection and goes back to the queue.
//!
//! The broker keeps three channels, as Kafka's `BrokerServer` does:
//! `heartbeat` for the registration and the heartbeats (retry timeout
//! `broker.heartbeat.interval.ms`), `forwarding` for forwarded requests,
//! topic creation and producer-id blocks (60 s), and `alter-partition`,
//! which never gives up.

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use krabka_protocol::{
    ApiKey, Decode, ProtocolError, ProtocolRequest,
    owned::{
        allocate_producer_ids_request::AllocateProducerIdsRequest,
        allocate_producer_ids_response::AllocateProducerIdsResponse,
        alter_partition_request::AlterPartitionRequest,
        alter_partition_response::AlterPartitionResponse, api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_heartbeat_response::BrokerHeartbeatResponse,
        broker_registration_request::BrokerRegistrationRequest,
        broker_registration_response::BrokerRegistrationResponse,
        create_topics_request::CreateTopicsRequest, create_topics_response::CreateTopicsResponse,
        envelope_request::EnvelopeRequest, envelope_response::EnvelopeResponse,
    },
};
use serde_json::{Value, json};

use super::{
    conn::{parse_response_frame, request_frame},
    replica::{RECONNECT_BACKOFF_MAX_MS, RECONNECT_BACKOFF_MS, SOFTWARE_NAME, SOFTWARE_VERSION},
};
use crate::lab::{
    codes,
    controller::RAFT_PORT,
    net::{ConnId, Ctx, Endpoint, Frame, Millis, NodeId, Payload},
};

/// Kafka's `controller.socket.timeout.ms`: the request timeout of a channel,
/// unless its retry timeout is shorter.
pub const CONTROLLER_SOCKET_TIMEOUT_MS: Millis = 30_000;
/// The retry timeout of the `forwarding` channel.
pub const FORWARDING_RETRY_TIMEOUT_MS: Millis = 60_000;
/// How soon a channel with work and no known controller looks again, as
/// Kafka's request thread polls every 100 ms.
pub const CONTROLLER_LOOKUP_MS: Millis = 100;

/// A request a channel carries to the controller.
#[derive(Clone, Debug, PartialEq)]
pub enum ControllerRequest {
    /// The broker's registration.
    BrokerRegistration(BrokerRegistrationRequest),
    /// The broker's heartbeat.
    BrokerHeartbeat(BrokerHeartbeatRequest),
    /// ISR changes of the partitions the broker leads.
    AlterPartition(AlterPartitionRequest),
    /// A block of producer ids.
    AllocateProducerIds(AllocateProducerIdsRequest),
    /// Topics the broker creates on its own.
    CreateTopics(CreateTopicsRequest),
    /// A client request the broker forwards.
    Envelope(EnvelopeRequest),
}

impl ControllerRequest {
    fn api_key(&self) -> ApiKey {
        match self {
            Self::BrokerRegistration(_) => ApiKey::BrokerRegistration,
            Self::BrokerHeartbeat(_) => ApiKey::BrokerHeartbeat,
            Self::AlterPartition(_) => ApiKey::AlterPartition,
            Self::AllocateProducerIds(_) => ApiKey::AllocateProducerIds,
            Self::CreateTopics(_) => ApiKey::CreateTopics,
            Self::Envelope(_) => ApiKey::Envelope,
        }
    }

    /// The version range the broker can send.
    fn range(&self) -> (i16, i16) {
        fn of<R: ProtocolRequest>() -> (i16, i16) {
            (R::MIN_VERSION, R::LATEST_STABLE_VERSION)
        }
        match self {
            Self::BrokerRegistration(_) => of::<BrokerRegistrationRequest>(),
            Self::BrokerHeartbeat(_) => of::<BrokerHeartbeatRequest>(),
            Self::AlterPartition(_) => of::<AlterPartitionRequest>(),
            Self::AllocateProducerIds(_) => of::<AllocateProducerIdsRequest>(),
            Self::CreateTopics(_) => of::<CreateTopicsRequest>(),
            Self::Envelope(_) => of::<EnvelopeRequest>(),
        }
    }

    fn frame(
        &self,
        version: i16,
        correlation: i32,
        client_id: &str,
    ) -> Result<Bytes, ProtocolError> {
        match self {
            Self::BrokerRegistration(r) => request_frame(version, correlation, client_id, r),
            Self::BrokerHeartbeat(r) => request_frame(version, correlation, client_id, r),
            Self::AlterPartition(r) => request_frame(version, correlation, client_id, r),
            Self::AllocateProducerIds(r) => request_frame(version, correlation, client_id, r),
            Self::CreateTopics(r) => request_frame(version, correlation, client_id, r),
            Self::Envelope(r) => request_frame(version, correlation, client_id, r),
        }
    }
}

/// The controller's answer to a [`ControllerRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum ControllerResponse {
    /// The answer to a registration.
    BrokerRegistration(BrokerRegistrationResponse),
    /// The answer to a heartbeat.
    BrokerHeartbeat(BrokerHeartbeatResponse),
    /// The answer to ISR changes.
    AlterPartition(AlterPartitionResponse),
    /// A block of producer ids, or why there is none.
    AllocateProducerIds(AllocateProducerIdsResponse),
    /// The answer to a topic creation.
    CreateTopics(CreateTopicsResponse),
    /// The envelope around a forwarded request's answer.
    Envelope(EnvelopeResponse),
}

impl ControllerResponse {
    fn decode(api_key: ApiKey, version: i16, body: &[u8]) -> Result<Self, ProtocolError> {
        let mut cursor = body;
        Ok(match api_key {
            ApiKey::BrokerRegistration => {
                Self::BrokerRegistration(Decode::decode(&mut cursor, version)?)
            }
            ApiKey::BrokerHeartbeat => Self::BrokerHeartbeat(Decode::decode(&mut cursor, version)?),
            ApiKey::AlterPartition => Self::AlterPartition(Decode::decode(&mut cursor, version)?),
            ApiKey::AllocateProducerIds => {
                Self::AllocateProducerIds(Decode::decode(&mut cursor, version)?)
            }
            ApiKey::CreateTopics => Self::CreateTopics(Decode::decode(&mut cursor, version)?),
            ApiKey::Envelope => Self::Envelope(Decode::decode(&mut cursor, version)?),
            _ => return Err(ProtocolError::InvalidValue("not a controller api")),
        })
    }

    /// Kafka's `errorCounts().containsKey(NOT_CONTROLLER)`: whether the
    /// answer says the node is not the active controller, which sends the
    /// request back to the queue.
    fn is_not_controller(&self) -> bool {
        match self {
            Self::BrokerRegistration(r) => r.error_code == codes::NOT_CONTROLLER,
            Self::BrokerHeartbeat(r) => r.error_code == codes::NOT_CONTROLLER,
            Self::AlterPartition(r) => {
                r.error_code == codes::NOT_CONTROLLER
                    || r.topics
                        .iter()
                        .flat_map(|t| t.partitions.iter())
                        .any(|p| p.error_code == codes::NOT_CONTROLLER)
            }
            Self::AllocateProducerIds(r) => r.error_code == codes::NOT_CONTROLLER,
            Self::CreateTopics(r) => r
                .topics
                .iter()
                .any(|t| t.error_code == codes::NOT_CONTROLLER),
            Self::Envelope(r) => r.error_code == codes::NOT_CONTROLLER,
        }
    }
}

/// Who asked for a request, so its answer finds its way back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Purpose {
    /// The broker's registration or heartbeat.
    Lifecycle,
    /// A batch of ISR changes.
    AlterPartition,
    /// A block of producer ids.
    ProducerIds,
    /// Topics the broker creates on its own: auto-created topics,
    /// `__consumer_offsets` and a streams group's internal topics.
    CreateTopics {
        /// The names the request carries.
        topics: Vec<String>,
    },
    /// A client request the broker forwarded.
    Forward {
        /// The token its held request waits under.
        token: u64,
    },
}

/// How a channel request ended.
#[derive(Clone, Debug, PartialEq)]
pub enum ChannelOutcome {
    /// The controller answered.
    Response(ControllerResponse),
    /// The request waited for a controller longer than the retry timeout.
    TimedOut,
    /// The controller serves no version of the api the broker can send.
    VersionMismatch,
}

/// A finished channel request.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelEvent {
    /// Who asked for the request.
    pub purpose: Purpose,
    /// How it ended.
    pub outcome: ChannelOutcome,
}

/// A request waiting in a channel's queue.
#[derive(Clone, Debug, PartialEq)]
struct Queued {
    created_at: Millis,
    request: ControllerRequest,
    purpose: Purpose,
}

/// The request a channel's connection waits on.
#[derive(Clone, Debug, PartialEq)]
struct InFlight {
    /// The request, or `None` for the `ApiVersions` handshake.
    queued: Option<Queued>,
    api_key: ApiKey,
    version: i16,
    correlation: i32,
    sent_at: Millis,
}

/// The connection of a channel to one controller listener.
#[derive(Clone, Debug, PartialEq)]
struct ChannelConn {
    node: NodeId,
    id: ConnId,
    /// The controller's `ApiVersions` table, once the handshake answered.
    versions: Option<BTreeMap<i16, (i16, i16)>>,
    in_flight: Option<InFlight>,
    next_correlation: i32,
}

/// One channel to the active controller.
#[derive(Clone, Debug, PartialEq)]
pub struct ControllerChannel {
    retry_timeout_ms: Millis,
    request_timeout_ms: Millis,
    queue: VecDeque<Queued>,
    /// The controller the channel talks to, `None` until it looks one up.
    active: Option<NodeId>,
    conn: Option<ChannelConn>,
    /// No new connection opens before this time.
    reconnect_at: Millis,
    reconnect_backoff: Millis,
}

impl ControllerChannel {
    /// A channel whose queued requests time out after `retry_timeout_ms`.
    #[must_use]
    pub fn new(retry_timeout_ms: Millis) -> Self {
        Self {
            retry_timeout_ms,
            request_timeout_ms: CONTROLLER_SOCKET_TIMEOUT_MS.min(retry_timeout_ms),
            queue: VecDeque::new(),
            active: None,
            conn: None,
            reconnect_at: 0,
            reconnect_backoff: RECONNECT_BACKOFF_MS,
        }
    }

    /// Queue a request.
    pub fn enqueue(&mut self, now: Millis, request: ControllerRequest, purpose: Purpose) {
        self.queue.push_back(Queued {
            created_at: now,
            request,
            purpose,
        });
    }

    /// Forget every request and the connection, as a restart does.
    pub fn reset(&mut self) {
        self.queue.clear();
        self.active = None;
        self.conn = None;
        self.reconnect_at = 0;
        self.reconnect_backoff = RECONNECT_BACKOFF_MS;
    }

    /// Drive the channel: time out what waited too long, find the
    /// controller, connect, handshake, and send the next request.
    /// `leader` is the controller the local quorum names; `new_conn` hands
    /// out a fresh connection id of the broker's client socket.
    pub fn poll(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: Option<NodeId>,
        new_conn: &mut dyn FnMut() -> ConnId,
    ) -> Vec<ChannelEvent> {
        let now = ctx.now();
        let mut events = Vec::new();
        let timed_out = self.conn.as_ref().is_some_and(|conn| {
            conn.in_flight
                .as_ref()
                .is_some_and(|f| now >= f.sent_at.saturating_add(self.request_timeout_ms))
        });
        if timed_out {
            // Kafka's `NetworkClient` disconnects a request that timed out.
            self.disconnect(ctx, now, true);
        }
        let retry_timeout = self.retry_timeout_ms;
        let (expired, kept): (Vec<Queued>, Vec<Queued>) = self
            .queue
            .drain(..)
            .partition(|q| now.saturating_sub(q.created_at) >= retry_timeout);
        self.queue = kept.into();
        events.extend(expired.into_iter().map(|q| ChannelEvent {
            purpose: q.purpose,
            outcome: ChannelOutcome::TimedOut,
        }));
        if self.queue.is_empty() && self.conn.is_none() {
            return events;
        }
        if self.active.is_none() {
            self.active = leader;
        }
        let Some(target) = self.active else {
            return events;
        };
        if self.conn.as_ref().is_some_and(|c| c.node != target) {
            self.disconnect(ctx, now, true);
        }
        if self.conn.is_none() {
            if self.queue.is_empty() || now < self.reconnect_at {
                return events;
            }
            let id = new_conn();
            ctx.send(Frame::open(
                Endpoint::client(ctx.me()),
                Endpoint::new(target, RAFT_PORT),
                id,
            ));
            self.conn = Some(ChannelConn {
                node: target,
                id,
                versions: None,
                in_flight: None,
                next_correlation: 0,
            });
        }
        self.send_next(ctx, now, &mut events);
        events
    }

    /// Send the handshake or the next queued request when nothing is in
    /// flight.
    fn send_next(&mut self, ctx: &mut Ctx<'_>, now: Millis, events: &mut Vec<ChannelEvent>) {
        let client_id = ctx.me().to_string();
        let Some(conn) = self.conn.as_mut() else {
            return;
        };
        if conn.in_flight.is_some() {
            return;
        }
        let correlation = conn.next_correlation;
        let target = Endpoint::new(conn.node, RAFT_PORT);
        let Some(versions) = &conn.versions else {
            let request = ApiVersionsRequest {
                client_software_name: SOFTWARE_NAME.to_string(),
                client_software_version: SOFTWARE_VERSION.to_string(),
                ..ApiVersionsRequest::default()
            };
            let version = ApiVersionsRequest::LATEST_STABLE_VERSION;
            if let Ok(frame) = request_frame(version, correlation, &client_id, &request) {
                conn.next_correlation = conn.next_correlation.wrapping_add(1);
                conn.in_flight = Some(InFlight {
                    queued: None,
                    api_key: ApiKey::ApiVersions,
                    version,
                    correlation,
                    sent_at: now,
                });
                ctx.send(Frame::data(
                    Endpoint::client(ctx.me()),
                    target,
                    conn.id,
                    frame,
                ));
            }
            return;
        };
        let Some(queued) = self.queue.pop_front() else {
            return;
        };
        let api_key = queued.request.api_key();
        let (min, max) = queued.request.range();
        let version = versions
            .get(&(api_key as i16))
            .map(|&(their_min, their_max)| (min.max(their_min), max.min(their_max)))
            .filter(|(low, high)| low <= high)
            .map(|(_, high)| high);
        let Some(version) = version else {
            events.push(ChannelEvent {
                purpose: queued.purpose,
                outcome: ChannelOutcome::VersionMismatch,
            });
            return;
        };
        match queued.request.frame(version, correlation, &client_id) {
            Ok(frame) => {
                conn.next_correlation = conn.next_correlation.wrapping_add(1);
                conn.in_flight = Some(InFlight {
                    queued: Some(queued),
                    api_key,
                    version,
                    correlation,
                    sent_at: now,
                });
                ctx.send(Frame::data(
                    Endpoint::client(ctx.me()),
                    target,
                    conn.id,
                    frame,
                ));
            }
            Err(_) => events.push(ChannelEvent {
                purpose: queued.purpose,
                outcome: ChannelOutcome::VersionMismatch,
            }),
        }
    }

    /// Close the connection, put what it had in flight back at the head of
    /// the queue, and look the controller up again after the backoff.
    fn disconnect(&mut self, ctx: &mut Ctx<'_>, now: Millis, notify_peer: bool) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        if notify_peer {
            ctx.send(Frame::close(
                Endpoint::client(ctx.me()),
                Endpoint::new(conn.node, RAFT_PORT),
                conn.id,
            ));
        }
        if let Some(queued) = conn.in_flight.and_then(|f| f.queued) {
            self.queue.push_front(queued);
        }
        self.active = None;
        self.reconnect_at = now + self.reconnect_backoff;
        self.reconnect_backoff = (self.reconnect_backoff * 2).min(RECONNECT_BACKOFF_MAX_MS);
    }

    /// A frame for the broker's client socket from a controller listener.
    /// Frames of other connections are left alone.
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: &Frame) -> Vec<ChannelEvent> {
        let now = ctx.now();
        let ours = self
            .conn
            .as_ref()
            .is_some_and(|c| c.id == frame.conn && Endpoint::new(c.node, RAFT_PORT) == frame.src);
        if !ours {
            return Vec::new();
        }
        match &frame.payload {
            Payload::Open => Vec::new(),
            Payload::Close => {
                self.disconnect(ctx, now, false);
                Vec::new()
            }
            Payload::Data(bytes) => self.on_response(ctx, now, bytes),
        }
    }

    fn on_response(&mut self, ctx: &mut Ctx<'_>, now: Millis, bytes: &Bytes) -> Vec<ChannelEvent> {
        let Some(in_flight) = self.conn.as_mut().and_then(|c| c.in_flight.take()) else {
            return Vec::new();
        };
        let body = match parse_response_frame(bytes, in_flight.api_key, in_flight.version) {
            Ok((correlation, body)) if correlation == in_flight.correlation => body,
            _ => {
                if let Some(conn) = self.conn.as_mut() {
                    conn.in_flight = Some(in_flight);
                }
                self.disconnect(ctx, now, true);
                return Vec::new();
            }
        };
        if in_flight.api_key == ApiKey::ApiVersions {
            let mut cursor: &[u8] = &body;
            match ApiVersionsResponse::decode(&mut cursor, in_flight.version) {
                Ok(response) if response.error_code == codes::NONE => {
                    if let Some(conn) = self.conn.as_mut() {
                        conn.versions = Some(
                            response
                                .api_keys
                                .iter()
                                .map(|a| (a.api_key, (a.min_version, a.max_version)))
                                .collect(),
                        );
                    }
                    self.reconnect_backoff = RECONNECT_BACKOFF_MS;
                }
                _ => self.disconnect(ctx, now, true),
            }
            return Vec::new();
        }
        let Some(queued) = in_flight.queued else {
            return Vec::new();
        };
        match ControllerResponse::decode(in_flight.api_key, in_flight.version, &body) {
            Ok(response) if response.is_not_controller() => {
                // Kafka's request thread drops the connection to the stale
                // controller and sends the request again to the new one.
                self.queue.push_front(queued);
                self.disconnect(ctx, now, true);
                Vec::new()
            }
            Ok(response) => vec![ChannelEvent {
                purpose: queued.purpose,
                outcome: ChannelOutcome::Response(response),
            }],
            Err(_) => {
                self.queue.push_front(queued);
                self.disconnect(ctx, now, true);
                Vec::new()
            }
        }
    }

    /// The earliest time after `now` the channel needs a poll: a request
    /// timeout, a queued request's retry timeout, a reconnect, or the next
    /// controller lookup.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let mut next: Option<Millis> = None;
        let mut note = |at: Millis| {
            let at = at.max(now + 1);
            next = Some(next.map_or(at, |n| n.min(at)));
        };
        if let Some(f) = self.conn.as_ref().and_then(|c| c.in_flight.as_ref()) {
            note(f.sent_at.saturating_add(self.request_timeout_ms));
        }
        if let Some(q) = self.queue.front() {
            note(q.created_at.saturating_add(self.retry_timeout_ms));
            if self.conn.is_none() {
                note(if self.active.is_some() {
                    self.reconnect_at
                } else {
                    now + CONTROLLER_LOOKUP_MS
                });
            }
        }
        next
    }

    /// The channel for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "controller": self.active,
            "connected": self.conn.as_ref().is_some_and(|c| c.versions.is_some()),
            "queued": self.queue.len(),
            "in_flight": self
                .conn
                .as_ref()
                .and_then(|c| c.in_flight.as_ref())
                .map(|f| <&'static str>::from(f.api_key)),
        })
    }
}
