//! The sans-IO Kafka client the application nodes and the registry embed.
//!
//! [`KafkaClient`] is a struct a node polls, not a [`Node`](crate::lab::Node):
//! the node forwards every frame whose connection the client owns to
//! [`KafkaClient::on_frame`], calls [`KafkaClient::on_tick`] from its timer,
//! and arms the deadline the tick returns. Nothing here reads a clock or opens
//! a socket; every byte leaves through [`Ctx::send`] as one Kafka frame with
//! its length prefix.
//!
//! The client models Apache Kafka's `NetworkClient`, `ClusterConnectionStates`
//! and `Metadata`: one connection per broker, opened on demand and
//! negotiated with `ApiVersions` before anything else (KIP-35), within
//! `socket.connection.setup.timeout.ms`; requests framed with a
//! `RequestHeader` v1 or v2 by KIP-482 and decoded at the version they were
//! sent with; at most `max_in_flight` requests per connection with the rest
//! queued; `request_timeout_ms` per request, after which the connection
//! closes as Kafka's does; reconnection with the exponential backoff of
//! `reconnect.backoff.ms` and `reconnect.backoff.max.ms`, reset once a
//! connection is ready; a metadata cache refreshed on demand, on the errors
//! `InvalidMetadataException` covers, and every `metadata_max_age_ms`, which
//! also takes the leader an error answer names (KIP-951); and
//! `FindCoordinator` lookups started when a request needs one and cached per
//! key.
//!
//! The metadata refresh keeps Kafka's `Metadata` bookkeeping. A request for
//! a refresh (`requestUpdate`) is met by the next answer; a topic the client
//! starts to track (`requestUpdateForNewTopics`) goes out at once and stays
//! wanted until an answer to a request that named it arrives, so a topic
//! asked for while a request is out is looked up after it. A tracked topic
//! the answer reports with an error of the `InvalidMetadataException` class,
//! such as `UNKNOWN_TOPIC_OR_PARTITION`, asks again, as
//! `Metadata.handleMetadataResponse` does. Between requests the client waits
//! `retry.backoff.ms`, doubling up to `retry.backoff.max.ms` with 20 %
//! jitter for each answer in a row that moved no partition's leader epoch
//! (Kafka's equivalent responses) and for each request in a row that failed.
//!
//! # Connection ids
//!
//! Every client of a node sends from the node's one client endpoint, and a
//! broker tells connections apart by `(client endpoint, connection id)`, so
//! the clients that share a node must draw disjoint ids. A client draws from
//! the range [`ClientOptions::conn_base`] names; give each client of a node
//! its own lane with [`conn_base`], and route a frame to the client whose
//! range holds its id ([`KafkaClient::owns_conn`]).
//!
//! A node arms the deadline `on_tick` returns, or
//! [`KafkaClient::next_deadline`] after it handed the client work. The
//! deadline names only what a tick can act on, so a node never spins at one
//! logical time.
//!
//! # Typed requests
//!
//! [`KafkaClient::send`] takes any generated request type. The client keeps a
//! decoder for the matching response, and the completion arrives as
//! [`ClientEvent::Response`] whose [`Response::downcast`] gives the typed
//! `R::Response` back. The caller keeps the [`RequestId`] `send` returned to
//! know which request completed; a timeout or a lost connection completes the
//! same way with an `Err`.
//!
//! On top of the client sit the [`Producer`] and the [`Consumer`].

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use derive_more::{Display, From, Into};
use krabka_protocol::{
    ProtocolError, ProtocolRequest,
    owned::{
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::FindCoordinatorResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use self::{
    connection::{Completion, Connection, Received},
    request::{Outbound, Purpose},
};
use crate::lab::{
    codes,
    net::{ConnId, Ctx, Endpoint, Frame, Millis, Payload},
};

mod assignor;
mod batch;
mod connection;
mod consumer;
#[cfg(test)]
mod consumer_tests;
#[cfg(test)]
pub mod fake_broker;
mod metadata;
mod partitioner;
mod producer;
#[cfg(test)]
mod producer_tests;
mod request;
mod retry;
#[cfg(test)]
pub mod test_support;
#[cfg(test)]
mod tests;

pub use self::{
    assignor::{
        Subscription, decode_assignment, decode_subscription, encode_assignment,
        encode_subscription, range_assign,
    },
    batch::{BatchRecord, ConsumedRecord, ProducerStamp, build_batch, records_of},
    consumer::{
        AutoOffsetReset, Consumer, ConsumerConfig, ConsumerEvent, ConsumerMetrics, GroupProtocol,
        IsolationLevel, MemberState,
    },
    metadata::{BrokerInfo, MetadataCache, PartitionInfo, TopicInfo, endpoint_for_host, uuid_hex},
    partitioner::{
        StickyPartitioner, java_string_hash_code, murmur2, partition_for_key, to_positive,
    },
    producer::{
        Acks, Compression, Producer, ProducerConfig, ProducerEvent, ProducerMetrics,
        ProducerRecord, RttHistogram, SeqNo,
    },
    request::{ApiSpec, VersionTable, api_name},
    retry::{ErrorClass, class as error_class, exponential_backoff},
};

/// How many connection ids one client's range spans; see
/// [`ClientOptions::conn_base`]. The client draws the ids strictly between
/// its base and its base plus this span.
pub const CONN_ID_RANGE: u32 = 1 << 20;

/// How many clients one node can run on disjoint connection-id ranges below
/// `1 << 30`; see [`conn_base`].
pub const CONN_ID_LANES: u32 = 1 << 10;

/// The [`ClientOptions::conn_base`] of the client on `lane` of a node:
/// `lane * CONN_ID_RANGE`, with `lane` taken modulo [`CONN_ID_LANES`]. The
/// clients of one node on different lanes draw disjoint connection ids, all
/// below `1 << 30`, clear of the ids the node's other connections use: a
/// broker's quorum links number from `1 << 30`, and the schema registry
/// clients of an application node from `1 << 31`.
#[must_use]
pub const fn conn_base(lane: u32) -> u32 {
    (lane % CONN_ID_LANES) * CONN_ID_RANGE
}

/// The id of a request the client accepted. Unique per client.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct RequestId(pub u64);

/// The kind of coordinator a `FindCoordinator` lookup asks for, with the wire
/// value of `key_type`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatorType {
    Group,
    Transaction,
    Share,
}

impl CoordinatorType {
    /// The `key_type` of `FindCoordinator`.
    #[must_use]
    pub const fn as_wire(self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
            Self::Share => 2,
        }
    }
}

/// The cache key of a coordinator lookup.
pub type CoordinatorKey = (CoordinatorType, String);

/// Where a request goes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Target {
    /// The least loaded broker the client is connected to, or a bootstrap
    /// broker. Kafka's `leastLoadedNode`.
    Any,
    /// One broker by id, from the metadata cache.
    Broker(i32),
    /// The controller the metadata cache names, or any broker when it names
    /// none. In a `KRaft` cluster every broker forwards admin requests, and
    /// the metadata names a random live broker as the controller.
    Controller,
    /// The leader of a partition, from the metadata cache.
    Leader { topic: String, partition: i32 },
    /// The coordinator of a key, resolved through `FindCoordinator` and
    /// cached until an error invalidates it.
    Coordinator {
        key_type: CoordinatorType,
        key: String,
    },
}

/// What can go wrong with one request.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// No answer within `request_timeout_ms`. The connection closed.
    #[error("{api} timed out after {timeout_ms} ms")]
    Timeout {
        api: &'static str,
        timeout_ms: Millis,
    },
    /// The connection closed while the request was in flight.
    #[error("connection to {endpoint} closed with {api} in flight")]
    Disconnected {
        api: &'static str,
        endpoint: Endpoint,
    },
    /// The broker does not speak a version of the api the client can send.
    #[error(
        "{api}: broker supports v{broker_min}..=v{broker_max}, client sends v{client_min}..=v{client_max}"
    )]
    UnsupportedVersion {
        api: &'static str,
        broker_min: i16,
        broker_max: i16,
        client_min: i16,
        client_max: i16,
    },
    /// The request could not be encoded or the response could not be decoded.
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// A response carried a correlation id the client did not send.
    #[error("correlation id mismatch: expected {expected}, got {got}")]
    CorrelationMismatch { expected: i32, got: i32 },
    /// The broker answered an internal request of the client with an error.
    #[error("{api} failed with error code {code}")]
    Broker { api: &'static str, code: i16 },
    /// The client was closed with the request pending.
    #[error("client closed")]
    Closed,
}

/// A decoded response, typed behind `dyn Any`.
pub struct Response {
    pub api_key: i16,
    pub version: i16,
    /// The broker endpoint that answered.
    pub endpoint: Endpoint,
    body: Box<dyn Any>,
}

impl Response {
    /// The typed body: `R::Response` for the request type `R` the caller sent,
    /// or `()` for a one-way request.
    #[must_use]
    pub fn downcast<T: 'static>(self) -> Option<T> {
        self.body.downcast::<T>().ok().map(|b| *b)
    }

    /// Whether the body is a `T`.
    #[must_use]
    pub fn is<T: 'static>(&self) -> bool {
        self.body.is::<T>()
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Response")
            .field("api_key", &self.api_key)
            .field("version", &self.version)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

/// What the client reports to the node that polls it.
#[derive(Debug)]
pub enum ClientEvent {
    /// A request completed: with its decoded response, or with the error that
    /// ended it (a timeout, a lost connection, a version the broker lacks).
    Response {
        id: RequestId,
        result: Result<Response, ClientError>,
    },
    /// The metadata cache changed.
    MetadataUpdated,
}

/// The settings of a client, with Kafka's defaults.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClientOptions {
    /// `request.timeout.ms`. Default: 30 000.
    pub request_timeout_ms: Millis,
    /// `socket.connection.setup.timeout.ms`: how long a new connection may
    /// take to answer its `ApiVersions` before the client closes it. It
    /// doubles, with 20 % jitter, for each attempt that failed before it was
    /// ready. Default: 10 000.
    pub connection_setup_timeout_ms: Millis,
    /// `socket.connection.setup.timeout.max.ms`. Default: 30 000.
    pub connection_setup_timeout_max_ms: Millis,
    /// `max.in.flight.requests.per.connection`. Default: 5.
    pub max_in_flight: usize,
    /// `metadata.max.age.ms`. Default: 300 000.
    pub metadata_max_age_ms: Millis,
    /// `retry.backoff.ms`: the wait before a failed lookup or metadata
    /// refresh is sent again. Default: 100.
    pub retry_backoff_ms: Millis,
    /// `retry.backoff.max.ms`: the most the wait between metadata refreshes
    /// grows to, doubling from `retry_backoff_ms` for each answer in a row
    /// that brought nothing new and each request in a row that failed.
    /// Default: 1 000.
    pub retry_backoff_max_ms: Millis,
    /// `reconnect.backoff.ms`. Default: 50.
    pub reconnect_backoff_ms: Millis,
    /// `reconnect.backoff.max.ms`. Default: 1 000.
    pub reconnect_backoff_max_ms: Millis,
    /// The `client_software_name` of `ApiVersions` (KIP-511).
    pub software_name: String,
    /// The `client_software_version` of `ApiVersions`.
    pub software_version: String,
    /// The base of the connection ids the client draws. It numbers its
    /// connections `conn_base + 1` to `conn_base + CONN_ID_RANGE - 1` in
    /// turn, then starts over at `conn_base + 1`, skipping an id one of its
    /// open connections holds; [`KafkaClient::owns_conn`] tells whether an
    /// id is in the range.
    ///
    /// The clients of one node must use bases whose ranges do not overlap,
    /// since they all send from the node's client endpoint: take each from
    /// [`conn_base`] with a lane of its own, or use another multiple of
    /// [`CONN_ID_RANGE`] below `1 << 30`. The ids from `1 << 30` up belong
    /// to a node's other connections. Default: 0, so the ids start at 1.
    pub conn_base: u32,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            request_timeout_ms: 30_000,
            connection_setup_timeout_ms: 10_000,
            connection_setup_timeout_max_ms: 30_000,
            max_in_flight: 5,
            metadata_max_age_ms: 300_000,
            retry_backoff_ms: 100,
            retry_backoff_max_ms: 1_000,
            reconnect_backoff_ms: 50,
            reconnect_backoff_max_ms: 1_000,
            software_name: "krabka-lab".to_string(),
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            conn_base: 0,
        }
    }
}

/// A coordinator lookup that is in flight or waits for its retry.
#[derive(Clone, Copy, Debug)]
struct Lookup {
    in_flight: bool,
    retry_at: Millis,
}

/// The `Metadata` request that is out.
#[derive(Clone, Copy, Debug)]
struct Sent {
    /// It asked for every topic, so a topic its answer does not name is gone.
    full: bool,
    /// The `topics_version` it was built at: Kafka's request version.
    topics_version: u64,
}

/// Where the metadata refresh stands: Kafka's `Metadata` bookkeeping.
#[derive(Clone, Copy, Debug)]
struct Refresh {
    /// The `Metadata` request that is out, if one is.
    out: Option<Sent>,
    /// Something asked for a refresh: Kafka's `needFullUpdate`. The next
    /// answer clears it.
    needed: bool,
    /// Bumped each time the client starts to track a topic its cache lacks:
    /// Kafka's `requestVersion`.
    topics_version: u64,
    /// A tracked topic waits for an answer to a request that named it:
    /// Kafka's `needPartialUpdate`. An answer to a request built before the
    /// topic was tracked leaves it set.
    new_topics: bool,
    /// Requests in a row that got no answer: Kafka's `attempts`.
    failures: u32,
    /// Answers in a row that moved no partition's leader epoch: Kafka's
    /// `equivalentResponseCount`.
    equivalent: u32,
    /// The earliest time the next request may go out.
    next_at: Millis,
}

impl Refresh {
    /// Whether something waits for a refresh: Kafka's
    /// `Metadata.updateRequested`.
    const fn requested(self) -> bool {
        self.needed || self.new_topics
    }
}

/// How a target resolved.
enum Resolved {
    Endpoint(Endpoint),
    NeedMetadata,
    NeedCoordinator,
}

/// The sans-IO Kafka client. See the module documentation.
pub struct KafkaClient {
    client_id: String,
    opts: ClientOptions,
    bootstrap: Vec<Endpoint>,
    bootstrap_cursor: usize,
    conns: BTreeMap<Endpoint, Connection>,
    next_conn: u32,
    next_request: u64,
    metadata: MetadataCache,
    refresh: Refresh,
    /// The topics the client cares about; empty asks for every topic.
    topics: BTreeSet<String>,
    coordinators: BTreeMap<CoordinatorKey, (i32, Endpoint)>,
    lookups: BTreeMap<CoordinatorKey, Lookup>,
    /// Requests whose target is not known yet.
    waiting: Vec<Outbound>,
    /// Events produced outside `on_frame` and `on_tick`, delivered by the
    /// next of them.
    pending_events: Vec<ClientEvent>,
    closed: bool,
    timeouts: u64,
    disconnects: u64,
}

impl KafkaClient {
    /// A client that bootstraps through `bootstrap`.
    #[must_use]
    pub fn new(bootstrap: Vec<Endpoint>, client_id: &str, opts: ClientOptions) -> Self {
        Self {
            client_id: client_id.to_string(),
            opts,
            bootstrap,
            bootstrap_cursor: 0,
            conns: BTreeMap::new(),
            next_conn: 0,
            next_request: 0,
            metadata: MetadataCache::default(),
            refresh: Refresh {
                out: None,
                needed: true,
                topics_version: 0,
                new_topics: false,
                failures: 0,
                equivalent: 0,
                next_at: 0,
            },
            topics: BTreeSet::new(),
            coordinators: BTreeMap::new(),
            lookups: BTreeMap::new(),
            waiting: Vec::new(),
            pending_events: Vec::new(),
            closed: false,
            timeouts: 0,
            disconnects: 0,
        }
    }

    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    #[must_use]
    pub fn options(&self) -> &ClientOptions {
        &self.opts
    }

    /// The metadata cache.
    #[must_use]
    pub fn metadata(&self) -> &MetadataCache {
        &self.metadata
    }

    /// Whether `conn` belongs to this client, so a node with several clients
    /// can route a frame.
    #[must_use]
    pub fn owns(&self, endpoint: Endpoint, conn: ConnId) -> bool {
        self.conns
            .get(&endpoint)
            .is_some_and(|c| c.conn_id() == conn && !c.is_closed())
    }

    /// Whether `conn` is in the range of ids this client draws from (see
    /// [`ClientOptions::conn_base`]). Unlike [`KafkaClient::owns`] it also
    /// holds for a connection the client has closed, so a node with several
    /// clients routes every frame, a late one included, to the client that
    /// opened its connection.
    #[must_use]
    pub fn owns_conn(&self, conn: ConnId) -> bool {
        conn.0.wrapping_sub(self.opts.conn_base).wrapping_sub(1) < CONN_ID_RANGE - 1
    }

    /// Ask for the metadata of `topics` from now on, in addition to what the
    /// client already tracks. A topic the cache does not hold yet triggers a
    /// refresh.
    pub fn add_topics<'a>(&mut self, topics: impl IntoIterator<Item = &'a str>) {
        for topic in topics {
            self.track_topic(topic);
        }
    }

    /// Track `topic` from now on. A topic the cache does not hold yet asks
    /// for a refresh at once, without the backoff, and the refresh stays
    /// wanted until an answer to a request that named the topic arrives:
    /// Kafka's `requestUpdateForNewTopics`, which `ProducerMetadata.add`
    /// calls for a new topic.
    fn track_topic(&mut self, topic: &str) {
        if self.topics.insert(topic.to_string()) && !self.metadata.topics.contains_key(topic) {
            self.refresh.topics_version += 1;
            self.refresh.new_topics = true;
            self.refresh.equivalent = 0;
            self.refresh.next_at = 0;
        }
    }

    /// Adopt the leader and epoch an error answer names (KIP-951); see
    /// [`MetadataCache::update_leader`]. Returns whether the cache changed.
    pub fn update_leader(
        &mut self,
        topic: &str,
        partition: i32,
        leader: i32,
        leader_epoch: i32,
    ) -> bool {
        self.metadata
            .update_leader(topic, partition, leader, leader_epoch)
    }

    /// Refresh the metadata at the next tick.
    pub fn request_metadata_refresh(&mut self) {
        self.refresh.needed = true;
    }

    /// The cached coordinator of a key: its broker id and endpoint.
    #[must_use]
    pub fn coordinator(&self, key_type: CoordinatorType, key: &str) -> Option<(i32, Endpoint)> {
        self.coordinators.get(&(key_type, key.to_string())).copied()
    }

    /// Forget the coordinator of a key, so the next request looks it up
    /// again.
    pub fn invalidate_coordinator(&mut self, key_type: CoordinatorType, key: &str) {
        self.coordinators.remove(&(key_type, key.to_string()));
    }

    /// React to an error code a response carried for `target`, as Kafka's
    /// clients do: a code of the `InvalidMetadataException` class and
    /// `NOT_CONTROLLER` refresh the metadata; `NOT_COORDINATOR` and
    /// `COORDINATOR_NOT_AVAILABLE` forget the coordinator of the target.
    pub fn note_error(&mut self, code: i16, target: &Target) {
        if retry::class(code) == retry::ErrorClass::InvalidMetadata || code == codes::NOT_CONTROLLER
        {
            self.refresh.needed = true;
        }
        if matches!(
            code,
            codes::NOT_COORDINATOR | codes::COORDINATOR_NOT_AVAILABLE
        ) && let Target::Coordinator { key_type, key } = target
        {
            self.coordinators.remove(&(*key_type, key.clone()));
        }
    }

    /// Send a request. The completion arrives as [`ClientEvent::Response`]
    /// with the returned id, from a later `on_frame` or `on_tick`.
    pub fn send<R>(&mut self, ctx: &mut Ctx<'_>, target: Target, req: R) -> RequestId
    where
        R: ProtocolRequest + 'static,
        R::Response: 'static,
    {
        self.submit(ctx, target, req, false)
    }

    /// Send a request that expects no answer, such as a produce with
    /// `acks=0`. The completion carries an empty `()` body once the bytes
    /// left.
    pub fn send_oneway<R>(&mut self, ctx: &mut Ctx<'_>, target: Target, req: R) -> RequestId
    where
        R: ProtocolRequest + 'static,
        R::Response: 'static,
    {
        self.submit(ctx, target, req, true)
    }

    fn submit<R>(&mut self, ctx: &mut Ctx<'_>, target: Target, req: R, oneway: bool) -> RequestId
    where
        R: ProtocolRequest + 'static,
        R::Response: 'static,
    {
        let id = self.next_id();
        let now = ctx.now();
        let out = Outbound::new::<R>(
            id,
            Purpose::User,
            target,
            req,
            now + self.opts.request_timeout_ms,
            oneway,
        );
        if self.closed {
            self.pending_events.push(ClientEvent::Response {
                id,
                result: Err(ClientError::Closed),
            });
            return id;
        }
        self.route(ctx, out);
        let mut events = Vec::new();
        self.pump_all(ctx, &mut events);
        self.pending_events.extend(events);
        id
    }

    fn next_id(&mut self) -> RequestId {
        self.next_request += 1;
        RequestId(self.next_request)
    }

    /// The next connection id of the client's range, skipping the ids its
    /// open connections hold.
    fn next_conn_id(&mut self) -> ConnId {
        let held: BTreeSet<ConnId> = self
            .conns
            .values()
            .filter(|c| !c.is_closed())
            .map(Connection::conn_id)
            .collect();
        let (id, next) = draw_conn_id(self.opts.conn_base, self.next_conn, &held);
        self.next_conn = next;
        id
    }

    /// A frame arrived for this client.
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> Vec<ClientEvent> {
        let mut out = std::mem::take(&mut self.pending_events);
        let endpoint = frame.src;
        let known = self
            .conns
            .get(&endpoint)
            .is_some_and(|c| c.conn_id() == frame.conn);
        if !known {
            return out;
        }
        match frame.payload {
            Payload::Open => {}
            Payload::Data(bytes) => {
                let Some(conn) = self.conns.get_mut(&endpoint) else {
                    return out;
                };
                let received = conn.on_data(ctx, &bytes, &self.opts, &self.client_id);
                match received {
                    Received::Negotiated | Received::Ignored => {}
                    Received::Completed(completion) => {
                        self.complete(ctx, endpoint, completion, &mut out);
                    }
                    Received::Broken(error) => {
                        self.close_connection(ctx, endpoint, Some(&error), &mut out);
                    }
                }
            }
            Payload::Close => self.on_peer_close(ctx, endpoint, &mut out),
        }
        self.route_waiting(ctx);
        self.pump_all(ctx, &mut out);
        out
    }

    /// Drive timers, retries and the metadata refresh. Returns the events of
    /// this tick and the next time the caller must call again.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<ClientEvent>, Option<Millis>) {
        let mut out = std::mem::take(&mut self.pending_events);
        if self.closed {
            return (out, None);
        }
        let now = ctx.now();
        self.expire_connections(ctx, &mut out);
        self.expire_waiting(ctx, &mut out);
        self.refresh_metadata_if_due(ctx, now);
        self.run_lookups(ctx);
        self.route_waiting(ctx);
        self.pump_all(ctx, &mut out);
        (out, self.next_deadline(now))
    }

    /// Close every connection and fail every pending request with
    /// [`ClientError::Closed`].
    pub fn close(&mut self, ctx: &mut Ctx<'_>) {
        self.closed = true;
        let endpoints: Vec<Endpoint> = self.conns.keys().copied().collect();
        let mut out = Vec::new();
        for endpoint in endpoints {
            self.close_connection(ctx, endpoint, Some(&ClientError::Closed), &mut out);
            let queued = self
                .conns
                .get_mut(&endpoint)
                .map(Connection::take_queue)
                .unwrap_or_default();
            for request in queued {
                self.fail(ctx, request, ClientError::Closed, &mut out);
            }
        }
        for request in std::mem::take(&mut self.waiting) {
            self.fail(ctx, request, ClientError::Closed, &mut out);
        }
        self.pending_events.extend(out);
    }

    // ---- routing ----------------------------------------------------------------

    fn route(&mut self, ctx: &mut Ctx<'_>, request: Outbound) {
        if let Target::Leader { topic, .. } = &request.target {
            self.track_topic(topic);
        }
        match self.resolve(&request.target, ctx.now()) {
            Resolved::Endpoint(endpoint) => self.dispatch(ctx, endpoint, request),
            Resolved::NeedMetadata => {
                self.refresh.needed = true;
                self.waiting.push(request);
            }
            Resolved::NeedCoordinator => {
                let key = match &request.target {
                    Target::Coordinator { key_type, key } => Some((*key_type, key.clone())),
                    _ => None,
                };
                self.waiting.push(request);
                if let Some(key) = key {
                    self.start_lookup(ctx, key);
                }
            }
        }
    }

    fn resolve(&mut self, target: &Target, now: Millis) -> Resolved {
        match target {
            Target::Any => self
                .least_loaded(now)
                .map_or(Resolved::NeedMetadata, Resolved::Endpoint),
            Target::Broker(id) => self
                .metadata
                .broker_endpoint(*id)
                .map_or(Resolved::NeedMetadata, Resolved::Endpoint),
            Target::Controller => {
                let controller = self.metadata.controller_id;
                match self.metadata.broker_endpoint(controller) {
                    Some(endpoint) => Resolved::Endpoint(endpoint),
                    None if self.metadata.updated_at.is_some() => self
                        .least_loaded(now)
                        .map_or(Resolved::NeedMetadata, Resolved::Endpoint),
                    None => Resolved::NeedMetadata,
                }
            }
            Target::Leader { topic, partition } => self
                .metadata
                .leader(topic, *partition)
                .and_then(|leader| self.metadata.broker_endpoint(leader))
                .map_or(Resolved::NeedMetadata, Resolved::Endpoint),
            Target::Coordinator { key_type, key } => self
                .coordinators
                .get(&(*key_type, key.clone()))
                .map_or(Resolved::NeedCoordinator, |(_, endpoint)| {
                    Resolved::Endpoint(*endpoint)
                }),
        }
    }

    /// Kafka's `leastLoadedNode`: a ready connection with the fewest requests
    /// in flight, else one that is connecting, else a known broker or a
    /// bootstrap broker whose backoff passed, else the one whose backoff ends
    /// first.
    fn least_loaded(&mut self, now: Millis) -> Option<Endpoint> {
        if let Some(endpoint) = self
            .conns
            .iter()
            .filter(|(_, c)| c.is_ready())
            .min_by_key(|(_, c)| c.in_flight_len() + c.queue_len())
            .map(|(e, _)| *e)
        {
            return Some(endpoint);
        }
        if let Some(endpoint) = self
            .conns
            .iter()
            .find(|(_, c)| c.is_negotiating())
            .map(|(e, _)| *e)
        {
            return Some(endpoint);
        }
        let candidates: Vec<Endpoint> = self
            .metadata
            .brokers
            .values()
            .map(|b| b.endpoint)
            .chain(self.bootstrap.iter().copied())
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let start = self.bootstrap_cursor % candidates.len();
        for offset in 0..candidates.len() {
            let endpoint = candidates[(start + offset) % candidates.len()];
            let usable = self
                .conns
                .get(&endpoint)
                .is_none_or(|c| c.is_closed() && c.retry_at() <= now);
            if usable {
                self.bootstrap_cursor = start + offset + 1;
                return Some(endpoint);
            }
        }
        candidates
            .into_iter()
            .min_by_key(|e| self.conns.get(e).map_or(0, Connection::retry_at))
    }

    fn dispatch(&mut self, ctx: &mut Ctx<'_>, endpoint: Endpoint, request: Outbound) {
        let client = Endpoint::client(ctx.me());
        self.conns
            .entry(endpoint)
            .or_insert_with(|| Connection::new(client, endpoint))
            .enqueue(request);
    }

    fn route_waiting(&mut self, ctx: &mut Ctx<'_>) {
        let waiting = std::mem::take(&mut self.waiting);
        for request in waiting {
            self.route(ctx, request);
        }
    }

    /// Open closed connections that have work and whose backoff passed, and
    /// send what the in-flight windows allow.
    fn pump_all(&mut self, ctx: &mut Ctx<'_>, out: &mut Vec<ClientEvent>) {
        let now = ctx.now();
        let endpoints: Vec<Endpoint> = self.conns.keys().copied().collect();
        for endpoint in endpoints {
            let Some(conn) = self.conns.get_mut(&endpoint) else {
                continue;
            };
            if conn.is_closed() && conn.queue_len() > 0 && conn.retry_at() <= now {
                let id = self.next_conn_id();
                let Some(conn) = self.conns.get_mut(&endpoint) else {
                    continue;
                };
                conn.open(ctx, id, &self.opts, &self.client_id);
            }
            let Some(conn) = self.conns.get_mut(&endpoint) else {
                continue;
            };
            let completions = conn.pump(ctx, &self.client_id, &self.opts, now);
            for completion in completions {
                self.complete(ctx, endpoint, completion, out);
            }
        }
    }

    // ---- completion -------------------------------------------------------------

    fn complete(
        &mut self,
        ctx: &mut Ctx<'_>,
        endpoint: Endpoint,
        completion: Completion,
        out: &mut Vec<ClientEvent>,
    ) {
        let Completion {
            request,
            version,
            result,
        } = completion;
        match request.purpose {
            Purpose::User => out.push(ClientEvent::Response {
                id: request.id,
                result: result.map(|body| Response {
                    api_key: request.api.key,
                    version,
                    endpoint,
                    body,
                }),
            }),
            Purpose::Metadata => self.on_metadata(ctx, result, out),
            Purpose::FindCoordinator(key) => self.on_coordinator(ctx.now(), key, result),
        }
    }

    /// End a request that never got an answer. The caller's request reports
    /// the error; the client's own requests clear their in-flight state and
    /// wait for their retry.
    fn fail(
        &mut self,
        ctx: &mut Ctx<'_>,
        request: Outbound,
        error: ClientError,
        out: &mut Vec<ClientEvent>,
    ) {
        match request.purpose {
            Purpose::User => out.push(ClientEvent::Response {
                id: request.id,
                result: Err(error),
            }),
            Purpose::Metadata => self.on_metadata(ctx, Err(error), out),
            Purpose::FindCoordinator(key) => self.on_coordinator(ctx.now(), key, Err(error)),
        }
    }

    /// A `Metadata` request ended. An answer is Kafka's `Metadata.update`:
    /// it meets the refresh asked for (`needFullUpdate`), but a topic
    /// tracked after its request left stays wanted (`needPartialUpdate`),
    /// and a tracked topic it reports with an invalid-metadata error asks
    /// again. No answer is `failedUpdate`. Either way the next request waits
    /// the backoff of `retry.backoff.ms`, doubling up to
    /// `retry.backoff.max.ms` over the answers in a row that moved no leader
    /// epoch, or over the failures in a row.
    fn on_metadata(
        &mut self,
        ctx: &mut Ctx<'_>,
        result: Result<Box<dyn Any>, ClientError>,
        out: &mut Vec<ClientEvent>,
    ) {
        let now = ctx.now();
        let sent = self.refresh.out.take().unwrap_or(Sent {
            full: true,
            topics_version: self.refresh.topics_version,
        });
        let attempts = if let Some(response) = result
            .ok()
            .and_then(|b| b.downcast::<MetadataResponse>().ok())
        {
            let progressed = metadata::moves_epochs(&self.metadata, &response);
            self.metadata.apply(&response, sent.full, now);
            self.refresh.needed = self.asks_again(&response);
            self.refresh.new_topics = sent.topics_version < self.refresh.topics_version;
            self.refresh.failures = 0;
            self.refresh.equivalent = if progressed {
                0
            } else {
                self.refresh.equivalent.saturating_add(1)
            };
            for conn in self.conns.values_mut() {
                conn.set_broker_id(self.metadata.broker_at(conn.endpoint()));
            }
            out.push(ClientEvent::MetadataUpdated);
            self.refresh.equivalent.saturating_sub(1)
        } else {
            self.refresh.needed = true;
            self.refresh.failures = self.refresh.failures.saturating_add(1);
            self.refresh.equivalent = 0;
            self.refresh.failures - 1
        };
        self.refresh.next_at = now
            + retry::exponential_backoff(
                self.opts.retry_backoff_ms,
                self.opts.retry_backoff_max_ms,
                attempts,
                ctx.rand(400),
            );
    }

    /// Whether an answer asks for another refresh: a tracked topic, or a
    /// partition of one, carries an error of the `InvalidMetadataException`
    /// class, as in Kafka's `Metadata.handleMetadataResponse`. A client that
    /// tracks no topic asks for every topic, and keeps no topic the way a
    /// producer or a consumer does, so its answers never ask again.
    fn asks_again(&self, response: &MetadataResponse) -> bool {
        let invalid = |code: i16| retry::class(code) == retry::ErrorClass::InvalidMetadata;
        response.topics.iter().any(|topic| {
            topic
                .name
                .as_ref()
                .is_some_and(|name| self.topics.contains(name))
                && (invalid(topic.error_code)
                    || topic.partitions.iter().any(|p| invalid(p.error_code)))
        })
    }

    fn on_coordinator(
        &mut self,
        now: Millis,
        key: CoordinatorKey,
        result: Result<Box<dyn Any>, ClientError>,
    ) {
        let found = result
            .ok()
            .and_then(|b| b.downcast::<FindCoordinatorResponse>().ok())
            .and_then(|response| coordinator_of(&response, &key.1));
        match found {
            Some((node_id, endpoint)) => {
                self.coordinators.insert(key.clone(), (node_id, endpoint));
                self.lookups.remove(&key);
            }
            None => {
                self.lookups.insert(
                    key,
                    Lookup {
                        in_flight: false,
                        retry_at: now + self.opts.retry_backoff_ms,
                    },
                );
            }
        }
    }

    // ---- connection lifecycle ---------------------------------------------------

    fn on_peer_close(&mut self, ctx: &mut Ctx<'_>, endpoint: Endpoint, out: &mut Vec<ClientEvent>) {
        if self.conns.get(&endpoint).is_none_or(Connection::is_closed) {
            return;
        }
        self.disconnects += 1;
        let backoff = self.backoff_for(ctx, endpoint);
        let Some(conn) = self.conns.get_mut(&endpoint) else {
            return;
        };
        let failed = conn.on_close(ctx.now(), backoff);
        self.refresh.needed = true;
        for in_flight in failed {
            let api = in_flight.request.api.name;
            let completion = Completion {
                request: in_flight.request,
                version: in_flight.version,
                result: Err(ClientError::Disconnected { api, endpoint }),
            };
            self.complete(ctx, endpoint, completion, out);
        }
    }

    /// Close a connection from this side: send `Close`, fail what was in
    /// flight, and back off. `error` names why; `None` means a request timed
    /// out, and the requests past their deadline fail with a timeout.
    fn close_connection(
        &mut self,
        ctx: &mut Ctx<'_>,
        endpoint: Endpoint,
        error: Option<&ClientError>,
        out: &mut Vec<ClientEvent>,
    ) {
        let now = ctx.now();
        let backoff = self.backoff_for(ctx, endpoint);
        let Some(conn) = self.conns.get_mut(&endpoint) else {
            return;
        };
        if conn.is_closed() {
            return;
        }
        let client = Endpoint::client(ctx.me());
        ctx.send(Frame::close(client, endpoint, conn.conn_id()));
        let failed = conn.on_close(now, backoff);
        self.refresh.needed = true;
        for in_flight in failed {
            let api = in_flight.request.api.name;
            let reason = match error {
                Some(ClientError::Closed) => ClientError::Closed,
                Some(_) | None if now >= in_flight.deadline => {
                    self.timeouts += 1;
                    ClientError::Timeout {
                        api,
                        timeout_ms: self.opts.request_timeout_ms,
                    }
                }
                Some(_) | None => ClientError::Disconnected { api, endpoint },
            };
            let completion = Completion {
                request: in_flight.request,
                version: in_flight.version,
                result: Err(reason),
            };
            self.complete(ctx, endpoint, completion, out);
        }
    }

    fn backoff_for(&self, ctx: &mut Ctx<'_>, endpoint: Endpoint) -> Millis {
        let failures = self.conns.get(&endpoint).map_or(0, Connection::failures);
        retry::exponential_backoff(
            self.opts.reconnect_backoff_ms,
            self.opts.reconnect_backoff_max_ms,
            failures,
            ctx.rand(400),
        )
    }

    fn expire_connections(&mut self, ctx: &mut Ctx<'_>, out: &mut Vec<ClientEvent>) {
        let now = ctx.now();
        let expired: Vec<Endpoint> = self
            .conns
            .iter()
            .filter(|(_, c)| c.timed_out(now))
            .map(|(e, _)| *e)
            .collect();
        for endpoint in expired {
            self.close_connection(ctx, endpoint, None, out);
        }
        let timeout_ms = self.opts.request_timeout_ms;
        let mut expired_queued = Vec::new();
        for conn in self.conns.values_mut() {
            expired_queued.extend(conn.expire_queue(now));
        }
        for request in expired_queued {
            self.timeouts += 1;
            let api = request.api.name;
            self.fail(ctx, request, ClientError::Timeout { api, timeout_ms }, out);
        }
    }

    fn expire_waiting(&mut self, ctx: &mut Ctx<'_>, out: &mut Vec<ClientEvent>) {
        let now = ctx.now();
        let (expired, waiting): (Vec<Outbound>, Vec<Outbound>) = std::mem::take(&mut self.waiting)
            .into_iter()
            .partition(|r| now >= r.deadline);
        self.waiting = waiting;
        let timeout_ms = self.opts.request_timeout_ms;
        for request in expired {
            self.timeouts += 1;
            let api = request.api.name;
            self.fail(ctx, request, ClientError::Timeout { api, timeout_ms }, out);
        }
    }

    // ---- metadata and coordinators ---------------------------------------------

    /// Send a `Metadata` request when one is wanted and the backoff passed:
    /// Kafka's `Metadata.timeToNextUpdate`.
    fn refresh_metadata_if_due(&mut self, ctx: &mut Ctx<'_>, now: Millis) {
        if self.refresh.out.is_some() || now < self.refresh.next_at {
            return;
        }
        let stale = self
            .metadata
            .age(now)
            .is_none_or(|age| age >= self.opts.metadata_max_age_ms);
        if stale {
            // Kafka's `timeToAllowUpdate`: a refresh on expiry does not back
            // off over equivalent answers.
            self.refresh.equivalent = 0;
        }
        if !(self.refresh.requested() || stale) {
            return;
        }
        let full = self.topics.is_empty();
        let request = MetadataRequest {
            topics: (!full).then(|| {
                self.topics
                    .iter()
                    .map(|name| MetadataRequestTopic {
                        name: Some(name.clone()),
                        ..Default::default()
                    })
                    .collect()
            }),
            allow_auto_topic_creation: false,
            ..Default::default()
        };
        let id = self.next_id();
        let out = Outbound::new::<MetadataRequest>(
            id,
            Purpose::Metadata,
            Target::Any,
            request,
            now + self.opts.request_timeout_ms,
            false,
        );
        self.refresh.out = Some(Sent {
            full,
            topics_version: self.refresh.topics_version,
        });
        self.route(ctx, out);
    }

    /// Retry the coordinator lookups the waiting requests still need, and
    /// drop the ones nothing waits for.
    fn run_lookups(&mut self, ctx: &mut Ctx<'_>) {
        let needed: BTreeSet<CoordinatorKey> = self
            .waiting
            .iter()
            .filter_map(|r| match &r.target {
                Target::Coordinator { key_type, key } => Some((*key_type, key.clone())),
                _ => None,
            })
            .filter(|key| !self.coordinators.contains_key(key))
            .collect();
        self.lookups
            .retain(|key, lookup| lookup.in_flight || needed.contains(key));
        for key in needed {
            self.start_lookup(ctx, key);
        }
    }

    /// Send a `FindCoordinator` for `key` unless one is in flight or waits
    /// for its retry.
    fn start_lookup(&mut self, ctx: &mut Ctx<'_>, key: CoordinatorKey) {
        let now = ctx.now();
        let lookup = self.lookups.entry(key.clone()).or_insert(Lookup {
            in_flight: false,
            retry_at: 0,
        });
        if lookup.in_flight || now < lookup.retry_at {
            return;
        }
        lookup.in_flight = true;
        let request = FindCoordinatorRequest {
            key: key.1.clone(),
            key_type: key.0.as_wire(),
            coordinator_keys: vec![key.1.clone()],
            ..Default::default()
        };
        let id = self.next_id();
        let out = Outbound::new::<FindCoordinatorRequest>(
            id,
            Purpose::FindCoordinator(key),
            Target::Any,
            request,
            now + self.opts.request_timeout_ms,
            false,
        );
        self.route(ctx, out);
    }

    /// The next time the client needs [`KafkaClient::on_tick`]: a request
    /// deadline, a connection setup timeout, a reconnect, a metadata
    /// refresh, a coordinator lookup to retry, or events queued outside a
    /// tick.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let conns = self.conns.values().filter_map(Connection::next_deadline);
        let waiting = self.waiting.iter().map(|r| r.deadline);
        let metadata = if self.refresh.out.is_some() {
            None
        } else if self.refresh.requested() || self.metadata.updated_at.is_none() {
            Some(self.refresh.next_at)
        } else {
            self.metadata
                .updated_at
                .map(|at| at + self.opts.metadata_max_age_ms)
        };
        let lookups = self
            .lookups
            .values()
            .filter(|l| !l.in_flight)
            .map(|l| l.retry_at);
        // Events queued outside a tick go out with the next one.
        let pending = (!self.pending_events.is_empty()).then_some(now);
        conns
            .chain(waiting)
            .chain(metadata)
            .chain(lookups)
            .chain(pending)
            .min()
            .map(|at| at.max(now))
    }

    /// The client for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let connections: Vec<Value> = self.conns.values().map(Connection::snapshot).collect();
        let coordinators: serde_json::Map<String, Value> = self
            .coordinators
            .iter()
            .map(|((kind, key), (node, endpoint))| {
                (
                    format!("{}:{key}", format!("{kind:?}").to_lowercase()),
                    json!({ "broker_id": node, "node": endpoint.node }),
                )
            })
            .collect();
        json!({
            "client_id": self.client_id,
            "connections": connections,
            "metadata": self.metadata.snapshot(),
            "pending": self.waiting.len(),
            "coordinators": coordinators,
            "timeouts": self.timeouts,
            "disconnects": self.disconnects,
        })
    }
}

/// The connection id a client with `conn_base` draws after `drawn` earlier
/// draws, and the count of draws after it: ids run from `conn_base + 1` to
/// `conn_base + CONN_ID_RANGE - 1` in turn, start over after the last, and
/// skip the ids in `held`. A client holds one connection per broker, so a
/// free id turns up within a few draws.
#[must_use]
fn draw_conn_id(conn_base: u32, drawn: u32, held: &BTreeSet<ConnId>) -> (ConnId, u32) {
    let span = CONN_ID_RANGE - 1;
    let mut drawn = drawn;
    let mut id = ConnId(conn_base.wrapping_add(1 + drawn % span));
    for _ in 0..span {
        id = ConnId(conn_base.wrapping_add(1 + drawn % span));
        drawn = drawn.wrapping_add(1);
        if !held.contains(&id) {
            break;
        }
    }
    (id, drawn)
}

/// The coordinator a `FindCoordinator` response names for `key`: the row of
/// the batched form (v4 and later) or the top-level fields before it.
fn coordinator_of(response: &FindCoordinatorResponse, key: &str) -> Option<(i32, Endpoint)> {
    let row = response.coordinators.iter().find(|c| c.key == key);
    let (code, node_id, host) = match row {
        Some(row) => (row.error_code, row.node_id, row.host.as_str()),
        None => (
            response.error_code,
            response.node_id,
            response.host.as_str(),
        ),
    };
    if code != codes::NONE || node_id < 0 || host.is_empty() {
        return None;
    }
    Some((node_id, metadata::endpoint_for_host(host, node_id)))
}
