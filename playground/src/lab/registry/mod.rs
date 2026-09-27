//! The simulated schema registry: Confluent Schema Registry over the lab's
//! frames, with its state in the `_schemas` topic on the scenario's brokers.
//!
//! [`RegistryNode`] listens on [`HTTP_PORT`] and serves the Confluent REST
//! API. Its state is the replay of `_schemas`, which the [`KafkaStore`] sets
//! up, reads and writes on the brokers as Confluent's `KafkaStore` does. The
//! node keeps nothing of its own: every start, a wipe included, replays the
//! topic from its beginning.
//!
//! # Startup
//!
//! On every start the store runs Confluent's startup (see [`kafkastore`]).
//! The instance then joins the `schema-registry` group, whose members elect
//! the primary as Confluent's `KafkaGroupLeaderElector` does: the
//! leader-eligible member with the smallest URL (`http://node-<id>:8081`).
//! The instance that becomes the primary catches up once more
//! ([`KafkaStore::begin_leader_catch_up`]). Until the first assignment is
//! applied the registry does not listen, as Confluent's REST server starts
//! only after `init`: a new connection is refused with a `Close`, and the
//! `http` command fails. A store startup that fails, and a group that is
//! not joined within `kafkastore.init.timeout.ms`, leave the registry
//! refusing until it restarts.
//!
//! # Requests
//!
//! Reads are served from the replayed state at once, on every instance. A
//! request that may write (a registration, a delete, a level, a mode) takes
//! Confluent's write lock: writes run one at a time, in arrival order. On
//! the primary each first waits until the reader reaches the last written
//! offset (`waitUntilKafkaReaderReachesLastOffset`), then decides on the
//! state as it is then, then writes its records one `KafkaStore.put` at a
//! time. Its response goes out only once the reader has read its last
//! record back; a write that fails answers the operation's Confluent error
//! instead (see [`rest::WriteOp::failure`]). A secondary forwards the write
//! to the primary's REST listener and answers with the primary's answer, an
//! error with `; error code: <code>` appended to its message, as Confluent's
//! `RestService` does; no answer within `leader.read.timeout.ms` answers
//! 50003, and a write while no primary is known answers 50004 (see
//! [`rest::WriteOp::forwarding_failed`] and
//! [`rest::WriteOp::unknown_leader`]). A registration of a schema the subject
//! already has, or of one that does not parse, is answered at once on any
//! instance, since Confluent looks the schema up before it takes the lock
//! (see [`rest::before_lock`]). The requests of one connection are answered
//! in order: the requests behind one that waits for the store or the
//! primary wait with it.
//!
//! # Config
//!
//! ```json
//! { "bootstrap": [1, 2, 3], "compatibility": "BACKWARD", "mode": "READWRITE",
//!   "kafkastore.topic": "_schemas", "kafkastore.timeout.ms": 500,
//!   "kafkastore.init.timeout.ms": 60000,
//!   "kafkastore.topic.replication.factor": 3,
//!   "leader.eligibility": true, "schema.registry.group.id": "schema-registry",
//!   "kafkagroup.session.timeout.ms": 10000,
//!   "kafkagroup.heartbeat.interval.ms": 3000,
//!   "kafkagroup.rebalance.timeout.ms": 300000,
//!   "leader.read.timeout.ms": 60000 }
//! ```
//!
//! `bootstrap` names the brokers, Confluent's `kafkastore.bootstrap.servers`;
//! it is required and names at least one. `compatibility` and `mode` are the
//! global defaults that apply until a `CONFIG` or `MODE` record sets them.
//! The other keys are Confluent's, with its defaults; see [`StoreConfig`]
//! for the `kafkastore.*` ones. Any other key is an error.
//!
//! # Control commands
//!
//! `{"cmd": "http", "method": "POST", "path": "/subjects/s/versions",
//! "body": {...}?}` serves one REST request from the page. A read, and a
//! registration answered before the lock, returns `{"status": <code>,
//! "body": <JSON or text>}`. A request that may write joins the write queue and
//! returns `{"queued": <n>}`; its answer is the `registry` event that carries
//! `"request": <n>`. The command fails while the registry does not serve.
//!
//! # Snapshot
//!
//! ```json
//! { "state": "stopped" | "loading" | "ready" | "failed", "error": null,
//!   "started": 1, "bootstrap": [1, 2, 3],
//!   "config": { "kafkastore.topic": "_schemas", ...every key above but
//!               `bootstrap`, `compatibility` and `mode`... },
//!   "compatibility": "BACKWARD", "mode": "READWRITE",
//!   "subjects": [{ "subject": "s", "versions": [{ "version": 1, "id": 1,
//!                  "deleted": false }], "compatibility": null, "mode": null }],
//!   "schemas": 1, "records": 3, "applied": 3,
//!   "unknown_records": 0, "undecodable_records": 0,
//!   "connections": 1, "refused": 0, "requests": 4, "errors": 0,
//!   "writes": { "queued": 0, "active": null },
//!   "election": { "url": "http://node-4:8081", "eligible": true,
//!                 "joined": true, "leader": "http://node-4:8081",
//!                 "is_leader": true, "member": { ... } },
//!   "forwarder": { "active": null, "forwarded": 0, "failed": 0 },
//!   "store": { ...the store's snapshot... } }
//! ```
//!
//! `state` is `stopped` while the node is down, when `store`, `member` and
//! `forwarder` are `null`; `error` is why a startup failed. `records` counts
//! the records the reader applied and `applied` is the offset after the
//! last one. `writes.active` is `{"op", "path", "stage": "catch_up" |
//! "write" | "forward"}` while a write holds the lock, or `{"stage":
//! "leader_catch_up"}` while a new primary catches up. `election.leader` is
//! the primary the last assignment named, `null` while a rebalance runs or
//! when no member may lead; `member` is the group member: its state, member
//! id, generation, assignment and client. `store` is
//! [`KafkaStore::snapshot`]: the startup step, the topic, the reader's
//! `offset` and `end_offset`, the running task, and the admin, reader and
//! producer clients with their connections.
//!
//! # Events
//!
//! `registry` for every write and every error answer; `kafkastore` for the
//! store's startup, its warnings and its failure, and for the reason of a
//! write the store failed (`{"step": "write_failed", "op", "path",
//! "message"}`); `election` for each assignment (`{"step": "assigned",
//! "generation", "leader", "is_leader"}`), each rebalance that begins
//! (`revoked`), a group without an eligible member (`no_leader`), an
//! assignment Confluent rejects (`assignment_failed`), a write the primary
//! did not answer (`forward_failed`), a catch-up of a new primary that
//! failed (`set_leader_failed`), and a group not joined in time (`failed`).
//!
//! # Durable state
//!
//! None: like Confluent's, the registry's state lives in `_schemas`.

pub mod compat;
mod election;
pub mod error;
pub mod format;
mod forward;
pub mod http;
pub mod ids;
pub mod kafkastore;
pub mod record;
pub mod rest;
pub mod service;
pub mod store;

use std::collections::{BTreeMap, VecDeque};

use bytes::BytesMut;
use serde_json::{Value, json};

use self::{
    election::{ElectionEvent, Elector, ElectorConfig, Identity},
    forward::{Forwarded, Forwarder},
    http::{HttpError, HttpRequest, HttpResponse},
    ids::LogOffset,
    kafkastore::{KafkaStore, StoreConfig},
    record::RawRecord,
    rest::WriteOp,
    service::RegistryService,
};
use super::{
    LabError,
    client::{ClientOptions, KafkaClient, conn_base},
    config_field, config_field_or,
    net::{ConnId, Ctx, Endpoint, Frame, HTTP_PORT, Millis, Node, NodeId, Payload},
    scenario::NodeSpec,
};

/// The config keys of a registry node.
const CONFIG_KEYS: [&str; 13] = [
    "bootstrap",
    "compatibility",
    "mode",
    "kafkastore.topic",
    "kafkastore.timeout.ms",
    "kafkastore.init.timeout.ms",
    "kafkastore.topic.replication.factor",
    "leader.eligibility",
    "schema.registry.group.id",
    "kafkagroup.session.timeout.ms",
    "kafkagroup.heartbeat.interval.ms",
    "kafkagroup.rebalance.timeout.ms",
    "leader.read.timeout.ms",
];

/// The failure of a group that was not joined within
/// `kafkastore.init.timeout.ms`: Confluent's `SchemaRegistryTimeoutException`.
const JOIN_TIMEOUT: &str = "Timed out waiting for join group to complete";

/// A client of one start of the node, each on connection ids of its own:
/// Confluent's store runs an admin client, a reader and a producer, its
/// leader elector a group member, and a secondary an HTTP client to the
/// primary. All send from the node's client endpoint.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Admin,
    Reader,
    Producer,
    Elector,
    Forwarder,
}

impl Role {
    /// The clients of one start.
    const COUNT: u32 = 5;

    /// The connection-id base of this client in the node's
    /// `generation`-th start: each start takes the next [`Role::COUNT`]
    /// lanes of the client module's [`conn_base`], so a late answer to a
    /// client of an earlier start reaches none of this one's.
    fn conn_base(self, generation: u32) -> u32 {
        let lane = match self {
            Self::Admin => 0,
            Self::Reader => 1,
            Self::Producer => 2,
            Self::Elector => 3,
            Self::Forwarder => 4,
        };
        conn_base(generation.wrapping_mul(Self::COUNT).wrapping_add(lane))
    }
}

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

/// What holds the write lock.
enum Writer {
    Idle,
    /// A new primary catches up before it takes a write: Confluent's
    /// `setLeader`.
    LeaderCatchUp,
    /// The store catches up before the request is decided.
    CatchingUp(Pending),
    /// The request's records are being written; `response` goes out when
    /// they are read back.
    Writing {
        pending: Pending,
        response: HttpResponse,
    },
    /// A secondary waits for the primary's answer.
    Forwarding(Pending),
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
    /// `leader.eligibility`.
    eligible: bool,
    elector: ElectorConfig,
    /// `leader.read.timeout.ms`: how long a secondary waits for the
    /// primary's answer.
    leader_read_timeout_ms: Millis,
}

/// The election as the node applies it: Confluent's `leaderIdentity` and
/// `joinedLatch`.
#[derive(Default)]
struct Leadership {
    /// The primary the last assignment named.
    leader: Option<Identity>,
    /// The first assignment was applied, so the registry serves.
    joined: bool,
    /// When the group must be joined by.
    join_deadline: Option<Millis>,
    /// This instance just became the primary and catches up first.
    catch_up_due: bool,
}

/// A schema registry node. See the module documentation.
pub struct RegistryNode {
    id: NodeId,
    settings: Settings,
    service: RegistryService,
    store: Option<KafkaStore>,
    elector: Option<Elector>,
    forwarder: Option<Forwarder>,
    leadership: Leadership,
    /// Why the startup failed outside the store: the group was not joined.
    failure: Option<String>,
    generation: u32,
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
    /// empty topic or group name, or a replication factor below 1.
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
        let settings = Settings {
            bootstrap,
            compatibility: level.as_str().to_string(),
            mode,
            store: store_config(spec)?,
            eligible: config_field_or(spec, "leader.eligibility", true)?,
            elector: elector_config(spec)?,
            leader_read_timeout_ms: config_field_or(spec, "leader.read.timeout.ms", 60_000)?,
        };
        Ok(Self {
            id: spec.id,
            service: RegistryService::new(&settings.compatibility, &settings.mode),
            settings,
            store: None,
            elector: None,
            forwarder: None,
            leadership: Leadership::default(),
            failure: None,
            generation: 0,
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

    /// Whether the registry serves: its store finished the startup and the
    /// first assignment of the election was applied.
    #[must_use]
    pub fn serving(&self) -> bool {
        self.failure.is_none()
            && self.leadership.joined
            && self.store.as_ref().is_some_and(KafkaStore::is_ready)
    }

    /// Whether this instance is the primary.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.leadership.leader.as_ref() == Some(&self.identity())
    }

    /// How this instance advertises itself to the group.
    fn identity(&self) -> Identity {
        Identity::of_node(self.id, HTTP_PORT, self.settings.eligible)
    }

    /// Why the startup failed, once it did.
    fn startup_failure(&self) -> Option<&str> {
        self.failure
            .as_deref()
            .or_else(|| self.store.as_ref().and_then(KafkaStore::failure))
    }

    fn not_serving(&self) -> String {
        match self.startup_failure() {
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

    /// The tail of every call: join the group once the store is ready, fail
    /// a startup whose group was not joined in time, run the writes that
    /// can run, and arm the timer. A failed startup drives nothing more.
    fn settle(&mut self, ctx: &mut Ctx<'_>) {
        if self.failure.is_some() {
            return;
        }
        if self.elector.is_none() && self.store.as_ref().is_some_and(KafkaStore::is_ready) {
            self.join_group(ctx);
        }
        if !self.leadership.joined
            && self
                .leadership
                .join_deadline
                .is_some_and(|at| ctx.now() >= at)
        {
            self.fail_startup(ctx, JOIN_TIMEOUT.to_string());
            return;
        }
        self.pump_writes(ctx);
        let now = ctx.now();
        let deadlines = [
            self.store.as_ref().and_then(|s| s.next_deadline(now)),
            self.elector.as_ref().and_then(|e| e.next_deadline(now)),
            self.forwarder
                .as_ref()
                .and_then(Forwarder::next_deadline)
                .map(|at| at.max(now)),
            self.leadership
                .join_deadline
                .filter(|_| !self.leadership.joined)
                .map(|at| at.max(now)),
        ];
        if let Some(at) = deadlines.into_iter().flatten().min() {
            ctx.arm(at);
        }
    }

    /// Confluent's `electLeader`: the group member and the forwarding client
    /// start, and the startup waits for the first assignment.
    fn join_group(&mut self, ctx: &mut Ctx<'_>) {
        let endpoints = self
            .settings
            .bootstrap
            .iter()
            .map(|n| Endpoint::kafka(*n))
            .collect();
        let options = ClientOptions {
            conn_base: Role::Elector.conn_base(self.generation),
            ..ClientOptions::default()
        };
        let client = KafkaClient::new(endpoints, "sr-1", options);
        self.elector = Some(Elector::new(
            client,
            self.settings.elector.clone(),
            self.identity(),
        ));
        self.forwarder = Some(Forwarder::new(Role::Forwarder.conn_base(self.generation)));
        self.leadership.join_deadline = Some(ctx.now() + self.settings.store.init_timeout_ms);
    }

    /// The startup failed for good: every client closes, as Confluent's
    /// process exits.
    fn fail_startup(&mut self, ctx: &mut Ctx<'_>, message: String) {
        self.close_clients(ctx);
        ctx.event(
            "election",
            json!({ "step": "failed", "message": message, "level": "error" }),
        );
        self.failure = Some(message);
    }

    fn close_clients(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(store) = &mut self.store {
            store.close(ctx);
        }
        if let Some(elector) = &mut self.elector {
            elector.close(ctx);
        }
        if let Some(forwarder) = &mut self.forwarder {
            forwarder.abort(ctx);
        }
    }

    /// Apply what the group member reports: Confluent's `onRevoked` and
    /// `onAssigned`, which call `setLeader`.
    fn on_election(&mut self, ctx: &mut Ctx<'_>, events: Vec<ElectionEvent>) {
        for event in events {
            match event {
                ElectionEvent::Revoked => {
                    self.leadership.leader = None;
                    ctx.event("election", json!({ "step": "revoked", "level": "info" }));
                }
                ElectionEvent::Assigned {
                    generation,
                    assignment,
                } => {
                    if assignment.error != 0 {
                        // Confluent's `onAssigned` throws before `setLeader`.
                        ctx.event(
                            "election",
                            json!({
                                "step": "assignment_failed",
                                "error": assignment.error,
                                "message": "The schema registry group contained multiple members advertising the same URL.",
                                "level": "error",
                            }),
                        );
                        continue;
                    }
                    let changed = assignment.leader_identity.is_some()
                        && assignment.leader_identity != self.leadership.leader;
                    self.leadership.leader = assignment.leader_identity;
                    let is_leader = self.is_leader();
                    ctx.event(
                        "election",
                        json!({
                            "step": "assigned",
                            "generation": generation,
                            "leader": self.leadership.leader.as_ref().map(Identity::url),
                            "is_leader": is_leader,
                            "level": "info",
                        }),
                    );
                    if self.leadership.leader.is_none() {
                        ctx.event(
                            "election",
                            json!({
                                "step": "no_leader",
                                "message": "No leader eligible schema registry instances joined the schema registry group. Rebalancing was successful and this instance can serve reads, but no writes can be processed.",
                                "level": "warn",
                            }),
                        );
                    }
                    if changed && is_leader {
                        // `setLeader` catches up before `joinedLatch` counts
                        // down.
                        self.leadership.catch_up_due = true;
                    } else {
                        self.leadership.joined = true;
                    }
                }
            }
        }
    }

    /// The primary answered a forwarded write, or did not.
    fn on_forwarded(&mut self, ctx: &mut Ctx<'_>, outcome: Forwarded) {
        let writer = std::mem::replace(&mut self.writer, Writer::Idle);
        let Writer::Forwarding(pending) = writer else {
            self.writer = writer;
            return;
        };
        let response = match outcome {
            Forwarded::Answered(response) => forward::relay(response),
            Forwarded::Failed(reason) => {
                ctx.event(
                    "election",
                    json!({
                        "step": "forward_failed",
                        "op": pending.op.name(),
                        "path": pending.request.path,
                        "leader": self.leadership.leader.as_ref().map(Identity::url),
                        "message": reason,
                        "level": "warn",
                    }),
                );
                pending.op.forwarding_failed().to_response()
            }
        };
        self.finish(ctx, &pending, &response);
    }

    fn take_outcome(&mut self) -> Option<Result<(), kafkastore::StoreError>> {
        self.store.as_mut().and_then(KafkaStore::take_outcome)
    }

    /// Run the write queue as far as the store and the primary let it: a
    /// new primary catches up first; then the lock's holder catches up, is
    /// decided, writes and answers on the primary, or is forwarded on a
    /// secondary; then the next one.
    fn pump_writes(&mut self, ctx: &mut Ctx<'_>) {
        loop {
            match std::mem::replace(&mut self.writer, Writer::Idle) {
                Writer::Idle => {
                    if self.failure.is_some() {
                        return;
                    }
                    let Some(store) = self.store.as_mut().filter(|s| s.is_ready()) else {
                        return;
                    };
                    if self.leadership.catch_up_due {
                        self.leadership.catch_up_due = false;
                        store.begin_leader_catch_up(ctx);
                        self.writer = Writer::LeaderCatchUp;
                        continue;
                    }
                    let Some(pending) = self.queue.pop_front() else {
                        return;
                    };
                    self.begin(ctx, pending);
                }
                Writer::LeaderCatchUp => match self.take_outcome() {
                    None => {
                        self.writer = Writer::LeaderCatchUp;
                        return;
                    }
                    Some(Ok(())) => self.leadership.joined = true,
                    Some(Err(error)) => ctx.event(
                        "election",
                        json!({
                            "step": "set_leader_failed",
                            "message": format!("Exception getting latest offset: {error}"),
                            "level": "error",
                        }),
                    ),
                },
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
                Writer::Forwarding(pending) => {
                    self.writer = Writer::Forwarding(pending);
                    return;
                }
            }
        }
    }

    /// Take the lock for `pending`: catch up on the primary, forward on a
    /// secondary, or answer that no primary is known.
    fn begin(&mut self, ctx: &mut Ctx<'_>, pending: Pending) {
        if self.is_leader() {
            if let Some(store) = self.store.as_mut() {
                store.begin_catch_up(ctx);
            }
            self.writer = Writer::CatchingUp(pending);
            return;
        }
        let Some(leader) = &self.leadership.leader else {
            let response = pending.op.unknown_leader().to_response();
            self.finish(ctx, &pending, &response);
            return;
        };
        let timeout = self.settings.leader_read_timeout_ms;
        if let (Some(node), Some(forwarder)) = (leader.node(), self.forwarder.as_mut()) {
            forwarder.send(ctx, node, &pending.request, timeout);
            self.writer = Writer::Forwarding(pending);
        } else {
            // A primary whose host names no lab node cannot be reached.
            let response = pending.op.forwarding_failed().to_response();
            self.finish(ctx, &pending, &response);
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
        if let Some(response) = rest::before_lock(&self.service, &request) {
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

    /// A frame for one of the node's clients: the forwarder, the group
    /// member, or the store's.
    fn on_client_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if let Some(forwarder) = &mut self.forwarder
            && forwarder.owns_conn(frame.conn)
        {
            if let Some(outcome) = forwarder.on_frame(&frame) {
                self.on_forwarded(ctx, outcome);
            }
        } else if let Some(elector) = &mut self.elector
            && elector.owns_conn(frame.conn)
        {
            let events = elector.on_frame(ctx, frame);
            self.on_election(ctx, events);
        } else if let Some(store) = &mut self.store {
            let read = store.on_frame(ctx, frame);
            self.apply(read);
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
        let holder = |pending: &Pending, stage: &str| {
            json!({
                "op": pending.op.name(),
                "path": pending.request.path,
                "stage": stage,
            })
        };
        let active = match &self.writer {
            Writer::Idle => Value::Null,
            Writer::LeaderCatchUp => json!({ "stage": "leader_catch_up" }),
            Writer::CatchingUp(pending) => holder(pending, "catch_up"),
            Writer::Writing { pending, .. } => holder(pending, "write"),
            Writer::Forwarding(pending) => holder(pending, "forward"),
        };
        json!({ "queued": self.queue.len(), "active": active })
    }

    fn state_name(&self) -> &'static str {
        match &self.store {
            None => "stopped",
            Some(_) if self.startup_failure().is_some() => "failed",
            Some(_) if self.serving() => "ready",
            Some(_) => "loading",
        }
    }
}

/// The `kafkastore.*` keys of a registry's config.
fn store_config(spec: &NodeSpec) -> Result<StoreConfig, LabError> {
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
    Ok(store)
}

/// The group keys of a registry's config: `schema.registry.group.id` and
/// `kafkagroup.*`.
fn elector_config(spec: &NodeSpec) -> Result<ElectorConfig, LabError> {
    let defaults = ElectorConfig::default();
    let elector = ElectorConfig {
        group_id: config_field_or(spec, "schema.registry.group.id", defaults.group_id)?,
        session_timeout_ms: config_field_or(
            spec,
            "kafkagroup.session.timeout.ms",
            defaults.session_timeout_ms,
        )?,
        heartbeat_interval_ms: config_field_or(
            spec,
            "kafkagroup.heartbeat.interval.ms",
            defaults.heartbeat_interval_ms,
        )?,
        rebalance_timeout_ms: config_field_or(
            spec,
            "kafkagroup.rebalance.timeout.ms",
            defaults.rebalance_timeout_ms,
        )?,
    };
    if elector.group_id.is_empty() {
        return Err(LabError::config(
            spec,
            "`schema.registry.group.id` is empty",
        ));
    }
    Ok(elector)
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
        self.close_clients(ctx);
        let me = Endpoint::http(self.id);
        for (peer, conn) in std::mem::take(&mut self.connections).into_keys() {
            ctx.send(Frame::close(me, peer, conn));
        }
        self.started += 1;
        self.generation = u32::try_from(self.started % u64::from(u32::MAX)).unwrap_or(0);
        self.service = RegistryService::new(&self.settings.compatibility, &self.settings.mode);
        self.queue.clear();
        self.writer = Writer::Idle;
        self.elector = None;
        self.forwarder = None;
        self.leadership = Leadership::default();
        self.failure = None;
        self.store = Some(KafkaStore::start(
            self.settings.store.clone(),
            &self.settings.bootstrap,
            self.generation,
            ctx,
        ));
        self.settle(ctx);
    }

    fn stop(&mut self) {
        self.connections.clear();
        self.queue.clear();
        self.writer = Writer::Idle;
        self.store = None;
        self.elector = None;
        self.forwarder = None;
        self.leadership = Leadership::default();
        self.failure = None;
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if frame.dst == Endpoint::http(self.id) {
            self.on_http_frame(ctx, &frame);
        } else if frame.dst == Endpoint::client(self.id) {
            self.on_client_frame(ctx, frame);
        }
        self.settle(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(store) = &mut self.store {
            let read = store.on_tick(ctx);
            self.apply(read);
        }
        if let Some(elector) = &mut self.elector {
            let events = elector.on_tick(ctx);
            self.on_election(ctx, events);
        }
        if let Some(outcome) = self.forwarder.as_mut().and_then(|f| f.on_tick(ctx)) {
            self.on_forwarded(ctx, outcome);
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
        let settings = &self.settings;
        let store = &settings.store;
        let group = &settings.elector;
        let identity = self.identity();
        json!({
            "state": self.state_name(),
            "error": self.startup_failure(),
            "started": self.started,
            "bootstrap": settings.bootstrap,
            "config": {
                "kafkastore.topic": store.topic,
                "kafkastore.timeout.ms": store.timeout_ms,
                "kafkastore.init.timeout.ms": store.init_timeout_ms,
                "kafkastore.topic.replication.factor": store.replication_factor,
                "leader.eligibility": settings.eligible,
                "schema.registry.group.id": group.group_id,
                "kafkagroup.session.timeout.ms": group.session_timeout_ms,
                "kafkagroup.heartbeat.interval.ms": group.heartbeat_interval_ms,
                "kafkagroup.rebalance.timeout.ms": group.rebalance_timeout_ms,
                "leader.read.timeout.ms": settings.leader_read_timeout_ms,
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
            "election": {
                "url": identity.url(),
                "eligible": identity.eligible,
                "joined": self.leadership.joined,
                "leader": self.leadership.leader.as_ref().map(Identity::url),
                "is_leader": self.is_leader(),
                "member": self.elector.as_ref().map(Elector::snapshot),
            },
            "forwarder": self.forwarder.as_ref().map(Forwarder::snapshot),
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
        assert!(node.settings.elector == ElectorConfig::default());
        assert!(node.settings.eligible);
        assert!(node.settings.leader_read_timeout_ms == 60_000);
        let custom = RegistryNode::from_spec(&spec(json!({
            "bootstrap": [1, 2],
            "compatibility": "full",
            "mode": "readonly",
            "kafkastore.topic": "_schemas_lab",
            "kafkastore.timeout.ms": 250,
            "kafkastore.init.timeout.ms": 5000,
            "kafkastore.topic.replication.factor": 1,
            "leader.eligibility": false,
            "schema.registry.group.id": "registries",
            "kafkagroup.session.timeout.ms": 6000,
            "kafkagroup.heartbeat.interval.ms": 2000,
            "kafkagroup.rebalance.timeout.ms": 20000,
            "leader.read.timeout.ms": 1000,
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
        assert!(
            custom.settings.elector
                == ElectorConfig {
                    group_id: "registries".to_string(),
                    session_timeout_ms: 6000,
                    heartbeat_interval_ms: 2000,
                    rebalance_timeout_ms: 20000,
                }
        );
        assert!(!custom.settings.eligible);
        assert!(custom.settings.leader_read_timeout_ms == 1000);
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
                json!({ "bootstrap": [1], "schema.registry.group.id": "" }),
                "`schema.registry.group.id` is empty",
            ),
            (
                json!({ "bootstrap": [1], "leader.eligibility": "yes" }),
                "config field `leader.eligibility`: invalid type",
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
    fn each_start_draws_its_clients_connection_ids_from_fresh_lanes() {
        use crate::lab::client::{CONN_ID_LANES, CONN_ID_RANGE};
        let roles = [
            Role::Admin,
            Role::Reader,
            Role::Producer,
            Role::Elector,
            Role::Forwarder,
        ];
        let bases = |generation: u32| -> Vec<u32> {
            roles.iter().map(|r| r.conn_base(generation)).collect()
        };
        // Two starts: ten ranges, all apart, all below the ids from 1 << 30
        // that belong to the node's other connections.
        let mut both = bases(1);
        both.extend(bases(2));
        assert!(both == (5..15).map(|lane| lane * CONN_ID_RANGE).collect::<Vec<_>>());
        assert!(both.iter().all(|b| b + CONN_ID_RANGE <= 1 << 30));
        // The starts go round the client module's lanes: start 205 takes
        // lanes 1 to 5 again, and start 1024 is laid out as start 0.
        assert!(
            bases(205)
                == bases(0)
                    .into_iter()
                    .map(|b| b + CONN_ID_RANGE)
                    .collect::<Vec<_>>()
        );
        assert!(bases(CONN_ID_LANES) == bases(0));
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
            snapshot["election"]
                == json!({
                    "url": "http://node-4:8081",
                    "eligible": true,
                    "joined": false,
                    "leader": null,
                    "is_leader": false,
                    "member": null,
                })
        );
        assert!(
            buffers.with(5, |ctx| node.control(ctx, json!({ "cmd": "nope" })))
                == Err("unknown registry command Some(\"nope\")".to_string())
        );
    }
}
