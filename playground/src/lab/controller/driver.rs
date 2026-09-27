//! The quorum driver: one broker's share of the metadata quorum, built on the
//! real [`QuorumStateMachine`] and driven through the lab's [`Ctx`].
//!
//! The driver does what `krabka_kraft_core::sim` does over an in-process bus,
//! but over the virtual network: every [`Action`] the machine returns becomes
//! frames on the raft links, a log append or truncation, a high-watermark
//! advance, or a timer. Replication happens over the link too. A follower's
//! `Fetch` names the offset and epoch of its log tip, and the leader answers
//! with the entries after it, or with the point where the follower's log
//! diverges, exactly as KIP-595 replicates.
//!
//! Timers follow `sim/node.rs`: the election timeout is `1000 ms + 50 ms per
//! node id`, so voters do not arm their timers in lockstep, and a leader
//! re-announces its epoch every 300 ms. The leader also holds a fetch that
//! finds nothing new for a short while, as Kafka's `fetch.max.wait.ms` does,
//! so an idle follower polls at a steady pace and a proposal reaches the
//! followers as soon as it is appended. A fetch carries the follower's high
//! watermark, and a leader whose own is higher answers it at once (KIP-1166),
//! so a commit reaches every follower without waiting out a held fetch.

use std::collections::BTreeMap;

use bytes::Bytes;
use krabka_kraft_core::{
    Action, Event, QuorumStateMachine, TimerKind,
    event::{LogEnd, SuccessorRank},
    role::{ReplicaProgress, Role},
    types::{Epoch, LogView, QuorumState, ReplicaKey, SimInstant},
};
use krabka_protocol::owned::{
    common::describe_quorum_response::replica_state::ReplicaState,
    describe_quorum_response::{DescribeQuorumResponse, Listener, Node, PartitionData, TopicData},
};
use krabka_units::prelude::millis;
use krabka_voters::{KRaftVersionRange, Voter, VoterSet};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    CommittedBatch, NotLeader, ProposalId, RAFT_PORT, broker_id, lab_id,
    log::{Entry, MetadataLog},
    wire::{FetchResponse, LogPoint, RaftMessage},
};
use crate::lab::{
    codes,
    net::{
        CLIENT_PORT, ConnId, Ctx, DurableImage, DurableOp, Endpoint, Frame, Millis, NodeId, Payload,
    },
};

/// How often a leader re-announces its epoch, as `sim/node.rs` does.
pub const HEARTBEAT_MS: Millis = 300;
/// The election timeout of the voter with node id 0; every id adds
/// [`ELECTION_TIMEOUT_STAGGER_MS`] to it. It is also the fetch timeout of a
/// follower, as in the consensus core.
pub const BASE_ELECTION_TIMEOUT_MS: Millis = 1_000;
/// The stagger between the election timeouts of adjacent node ids.
pub const ELECTION_TIMEOUT_STAGGER_MS: Millis = 50;
/// How long a leader holds a fetch that finds nothing new before it answers
/// with an empty response. Kafka's `fetch.max.wait.ms` is a quarter of its
/// fetch timeout, and so is this.
pub const FETCH_MAX_WAIT_MS: Millis = 250;
/// How long a node waits after a link closed before it opens it again.
pub const RECONNECT_BACKOFF_MS: Millis = 250;
/// How soon an observer that was redirected to no leader tries the next
/// voter.
pub const DISCOVERY_RETRY_MS: Millis = 200;
/// The most entries one fetch response carries.
pub const MAX_FETCH_ENTRIES: usize = 256;
/// The name of the metadata partition `DescribeQuorum` describes.
pub const METADATA_TOPIC: &str = "__cluster_metadata";
/// The durable log store: one [`Entry`] as JSON per log offset.
pub const KRAFT_LOG_STORE: &str = "kraft";
/// The durable key-value store of the quorum state and the high watermark.
pub const KRAFT_STATE_STORE: &str = "kraft-state";
/// The key of the quorum state in [`KRAFT_STATE_STORE`]: a
/// [`DurableQuorumState`] as JSON.
pub const QUORUM_STATE_KEY: &str = "quorum";
/// The key of the high watermark in [`KRAFT_STATE_STORE`]: the offset as a
/// decimal string.
pub const HIGH_WATERMARK_KEY: &str = "hwm";

/// What Kafka's `quorum-state` file holds, as the controller persists it on
/// every `PersistQuorumState` action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableQuorumState {
    pub epoch: Epoch,
    /// The candidate this node granted its binding vote in `epoch`.
    pub voted_for: Option<NodeId>,
    pub leader: Option<NodeId>,
}
/// Raft connection ids start here, so they never collide with the Kafka
/// client connections a broker opens from the same client endpoint, which it
/// counts from one. A broker tells the raft frames on its controller
/// listener from the Kafka requests there by this bound.
pub const RAFT_CONN_BASE: u32 = 1 << 30;
/// Kafka's `Errors.NOT_LEADER_OR_FOLLOWER.message()`.
const NOT_LEADER_OR_FOLLOWER_MESSAGE: &str = "For requests intended only for the leader, this \
     error indicates that the broker is not the current leader. For requests intended for any \
     replica, this error indicates that the broker is not a replica of the topic partition.";

/// The election timeout of a voter, staggered by its node id.
#[must_use]
pub fn election_timeout_ms(node: NodeId) -> Millis {
    BASE_ELECTION_TIMEOUT_MS + ELECTION_TIMEOUT_STAGGER_MS * Millis::from(node.0)
}

/// The deadlines the driver keeps beside the machine's role.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Timers {
    election: Option<Millis>,
    fetch: Option<Millis>,
    check_quorum: Option<Millis>,
    heartbeat: Option<Millis>,
    purgatory: Option<Millis>,
}

/// One of the [`Timers`], in the order they fire when several are due at
/// once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Timer {
    Election,
    Fetch,
    CheckQuorum,
    Heartbeat,
    Purgatory,
}

impl Timers {
    const ALL: [Timer; 5] = [
        Timer::Election,
        Timer::Fetch,
        Timer::CheckQuorum,
        Timer::Heartbeat,
        Timer::Purgatory,
    ];

    fn slot(&mut self, timer: Timer) -> &mut Option<Millis> {
        match timer {
            Timer::Election => &mut self.election,
            Timer::Fetch => &mut self.fetch,
            Timer::CheckQuorum => &mut self.check_quorum,
            Timer::Heartbeat => &mut self.heartbeat,
            Timer::Purgatory => &mut self.purgatory,
        }
    }

    fn deadline(self, timer: Timer) -> Option<Millis> {
        match timer {
            Timer::Election => self.election,
            Timer::Fetch => self.fetch,
            Timer::CheckQuorum => self.check_quorum,
            Timer::Heartbeat => self.heartbeat,
            Timer::Purgatory => self.purgatory,
        }
    }

    /// The earliest deadline.
    fn next(self) -> Option<Millis> {
        Self::ALL.iter().filter_map(|&t| self.deadline(t)).min()
    }

    /// The earliest timer due at `now`; ties go to the earlier variant.
    fn due(self, now: Millis) -> Option<Timer> {
        Self::ALL
            .iter()
            .copied()
            .filter_map(|t| self.deadline(t).map(|at| (at, t)))
            .filter(|&(at, _)| at <= now)
            .min_by_key(|&(at, _)| at)
            .map(|(_, t)| t)
    }
}

/// The client connection to one peer's raft listener.
#[derive(Debug, Clone, Copy)]
struct Link {
    conn: ConnId,
    open: bool,
    closed_at: Option<Millis>,
}

/// The one fetch a follower or observer has on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InFlightFetch {
    correlation: u64,
    target: NodeId,
}

/// A fetch the leader holds until there is something to answer with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeldFetch {
    correlation: u64,
    fetch_offset: i64,
    deadline: Millis,
}

/// One broker's share of the metadata quorum.
///
/// The broker that embeds it forwards every frame whose `dst.port` or
/// `src.port` is [`RAFT_PORT`] to [`on_frame`](Self::on_frame), calls
/// [`on_timer`](Self::on_timer) when the deadline from
/// [`next_deadline`](Self::next_deadline) is reached, and re-reads that
/// deadline after every call, because a frame or a proposal can move it.
pub struct ControllerCore {
    me: NodeId,
    voters: Vec<NodeId>,
    cluster_id: Uuid,
    machine: QuorumStateMachine,
    log: MetadataLog,
    high_watermark: i64,
    /// Entries already handed out by [`take_committed`](Self::take_committed).
    delivered: usize,
    running: bool,
    /// The logical time of the last call, for the timestamps
    /// `DescribeQuorum` reports.
    now: Millis,
    timers: Timers,
    links: BTreeMap<NodeId, Link>,
    next_conn: u32,
    fetch: Option<InFlightFetch>,
    next_correlation: u64,
    /// Which voter an observer without a leader asks next.
    discovery_cursor: usize,
    /// Leader side: fetches waiting for new entries, per follower.
    held: BTreeMap<NodeId, HeldFetch>,
    /// Leader side: the progress of the observers that fetch from it.
    observers: BTreeMap<NodeId, ReplicaProgress>,
    /// Leader side: the offset of the leader-change marker that opened this
    /// node's leadership.
    epoch_start_offset: Option<i64>,
}

impl ControllerCore {
    /// A controller for node `me` in the quorum of `voters`. A node outside
    /// `voters` is an observer: it replicates the log but never votes.
    #[must_use]
    pub fn new(me: NodeId, voters: &[NodeId], cluster_id: Uuid) -> Self {
        let mut voters = voters.to_vec();
        voters.sort_unstable();
        voters.dedup();
        let machine = Self::machine(me, QuorumState::bootstrap(cluster_id, voter_set(&voters)));
        Self {
            me,
            voters,
            cluster_id,
            machine,
            log: MetadataLog::default(),
            high_watermark: 0,
            delivered: 0,
            running: false,
            now: 0,
            timers: Timers::default(),
            links: BTreeMap::new(),
            next_conn: 0,
            fetch: None,
            next_correlation: 0,
            discovery_cursor: 0,
            held: BTreeMap::new(),
            observers: BTreeMap::new(),
            epoch_start_offset: None,
        }
    }

    fn machine(me: NodeId, state: QuorumState) -> QuorumStateMachine {
        let timeout = u32::try_from(election_timeout_ms(me)).unwrap_or(u32::MAX);
        QuorumStateMachine::new(broker_id(me), state, millis(timeout))
    }

    /// Boot, or boot again after a restart. The epoch and the vote survive a
    /// restart, as Kafka's `quorum-state` file does; the role, the leader
    /// belief and every connection do not. A voter arms its election timer,
    /// due at once for the only voter of its quorum, as Kafka's
    /// `KafkaRaftClient.initialize` makes a lone voter a candidate at once;
    /// an observer starts looking for the leader.
    pub fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.now = ctx.now();
        self.running = true;
        let state = QuorumState {
            leader_id: None,
            ..self.machine.quorum_state().clone()
        };
        self.machine = Self::machine(self.me, state);
        self.links.clear();
        self.fetch = None;
        self.held.clear();
        self.observers.clear();
        self.epoch_start_offset = None;
        self.timers = Timers::default();
        let timeout = election_timeout_ms(self.me);
        if self.machine.is_voter() {
            let lone = self.voters.as_slice() == [self.me];
            self.timers.election = Some(if lone { self.now } else { self.now + timeout });
        } else {
            self.timers.fetch = Some(self.now + timeout);
            self.discover(ctx);
        }
        ctx.event(
            "quorum",
            json!({ "role": self.role_name(), "epoch": self.epoch(), "started": true }),
        );
    }

    /// Halt. No frame or timer arrives until [`start`](Self::start).
    pub fn stop(&mut self) {
        self.running = false;
        self.timers = Timers::default();
        self.links.clear();
        self.fetch = None;
        self.held.clear();
        self.observers.clear();
        self.epoch_start_offset = None;
    }

    /// Restore the log, the quorum state and the high watermark the host kept
    /// from an earlier run, folded into `image`. The broker calls it from its
    /// `Node::load`, before [`start`](Self::start). The log is read from
    /// offset 0 up to the first gap or unreadable entry. The entries below the
    /// restored high watermark count as handed out already, because the
    /// broker restored the image it applied them to;
    /// [`replay_committed`](Self::replay_committed) hands them out again.
    pub fn load(&mut self, image: &DurableImage) {
        self.log = MetadataLog::default();
        for durable in image.logs.get(KRAFT_LOG_STORE).into_iter().flatten() {
            let expected = u64::try_from(self.log.end_offset()).unwrap_or(u64::MAX);
            let Ok(entry) = serde_json::from_slice::<Entry>(&durable.bytes) else {
                break;
            };
            if durable.index != expected {
                break;
            }
            self.log.append(entry.epoch, entry.records);
        }
        let state = image.kv.get(KRAFT_STATE_STORE);
        if let Some(quorum) = state
            .and_then(|kv| kv.get(QUORUM_STATE_KEY))
            .and_then(|value| serde_json::from_slice::<DurableQuorumState>(&value.0).ok())
        {
            let voted_key = quorum.voted_for.map(|id| ReplicaKey {
                id: broker_id(id),
                directory_id: Uuid::nil(),
            });
            self.machine = Self::machine(
                self.me,
                QuorumState {
                    leader_epoch: quorum.epoch,
                    leader_id: quorum.leader.map(broker_id),
                    voted_key,
                    ..QuorumState::bootstrap(self.cluster_id, voter_set(&self.voters))
                },
            );
        }
        let high_watermark = state
            .and_then(|kv| kv.get(HIGH_WATERMARK_KEY))
            .and_then(|value| std::str::from_utf8(&value.0).ok()?.parse::<i64>().ok())
            .unwrap_or(0);
        self.high_watermark = high_watermark.clamp(0, self.log.end_offset());
        self.delivered = usize::try_from(self.high_watermark).unwrap_or(0);
    }

    /// Hand every committed entry out again on the next
    /// [`take_committed`](Self::take_committed), for a broker that rebuilds
    /// its image from the log after a reload.
    pub fn replay_committed(&mut self) {
        self.delivered = 0;
    }

    /// The durable quorum state: the epoch, the vote and the leader.
    #[must_use]
    pub fn quorum_state(&self) -> &QuorumState {
        self.machine.quorum_state()
    }

    /// The quorum state as it is persisted.
    #[must_use]
    pub fn durable_quorum_state(&self) -> DurableQuorumState {
        DurableQuorumState {
            epoch: self.epoch(),
            voted_for: self
                .machine
                .quorum_state()
                .voted_key
                .and_then(|key| lab_id(key.id)),
            leader: self.leader(),
        }
    }

    /// A frame for the raft listener, or for the client side of one of this
    /// node's raft links.
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        self.now = ctx.now();
        if !self.running {
            return;
        }
        match frame.payload {
            Payload::Open => {}
            Payload::Close => {
                if frame.dst.port == CLIENT_PORT
                    && frame.src.port == RAFT_PORT
                    && let Some(link) = self.links.get_mut(&frame.src.node)
                    && link.conn == frame.conn
                {
                    link.open = false;
                    link.closed_at = Some(self.now);
                }
            }
            Payload::Data(bytes) => {
                if frame.dst.port != RAFT_PORT {
                    return;
                }
                match RaftMessage::decode(&bytes) {
                    Ok(message) => self.handle(ctx, frame.src.node, message),
                    Err(error) => ctx.event(
                        "raft",
                        json!({ "level": "warn", "from": frame.src.node, "error": error.to_string() }),
                    ),
                }
            }
        }
    }

    /// Fire every deadline that is due. Safe to call when none is.
    pub fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        self.now = ctx.now();
        if !self.running {
            return;
        }
        // Every firing re-arms at a later time or clears the timer, so the
        // loop ends; the bound guards against a timer that fires at `now`.
        for _ in 0..16 {
            let Some(timer) = self.timers.due(self.now) else {
                break;
            };
            *self.timers.slot(timer) = None;
            match timer {
                Timer::Election => {
                    self.step(ctx, Event::ElectionTimeout);
                }
                Timer::Fetch => {
                    self.step(ctx, Event::FetchTimeout);
                    if self.is_discovering_observer() {
                        self.discover(ctx);
                    }
                }
                Timer::CheckQuorum => {
                    self.step(ctx, Event::CheckQuorumTimeout);
                }
                Timer::Heartbeat => {
                    if self.machine.role().is_leader() {
                        let epoch = self.epoch();
                        self.apply_action(ctx, Action::SendBeginQuorumEpoch { epoch });
                        self.timers.heartbeat = Some(self.now + HEARTBEAT_MS);
                    }
                }
                Timer::Purgatory => self.release_due_fetches(ctx),
            }
        }
    }

    /// The earliest deadline, for the broker to arm its timer at.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        if self.running {
            self.timers.next()
        } else {
            None
        }
    }

    /// Propose a batch of records. Only the active controller, the quorum
    /// leader, accepts; any other node answers the leader it knows so the
    /// broker can forward. The batch commits once a majority has replicated
    /// it, and then [`take_committed`](Self::take_committed) hands it out at
    /// the returned offset.
    ///
    /// # Errors
    /// Returns [`NotLeader`] when this node does not lead the quorum.
    pub fn propose(
        &mut self,
        ctx: &mut Ctx<'_>,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<ProposalId, NotLeader> {
        self.now = ctx.now();
        if !self.running || !self.machine.role().is_leader() {
            return Err(NotLeader {
                leader: self.leader(),
            });
        }
        let offset = self.log.end_offset();
        let entry = Entry {
            epoch: self.epoch(),
            records,
        };
        persist_entry(ctx, offset, &entry);
        self.log.append(entry.epoch, entry.records);
        self.after_local_append(ctx);
        Ok(ProposalId(offset))
    }

    /// The batches committed since the last call, in log order. Every node,
    /// leader or follower, yields every entry exactly once, so every broker
    /// applies the same records in the same order. A leader-change marker is
    /// a batch with no records.
    pub fn take_committed(&mut self) -> Vec<CommittedBatch> {
        let committed = usize::try_from(self.high_watermark.max(0))
            .unwrap_or(usize::MAX)
            .min(self.log.len());
        if committed <= self.delivered {
            return Vec::new();
        }
        let batches = self.log.entries()[self.delivered..committed]
            .iter()
            .enumerate()
            .map(|(index, entry)| CommittedBatch {
                offset: i64::try_from(self.delivered + index).unwrap_or(i64::MAX),
                epoch: entry.epoch,
                records: entry.records.clone(),
            })
            .collect();
        self.delivered = committed;
        batches
    }

    /// This node's id.
    #[must_use]
    pub fn me(&self) -> NodeId {
        self.me
    }

    /// The voters, in ascending id order.
    #[must_use]
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }

    /// Whether this node is a voter rather than an observer.
    #[must_use]
    pub fn is_voter(&self) -> bool {
        self.machine.is_voter()
    }

    /// Whether this node is the active controller.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.machine.role().is_leader()
    }

    /// The leader this node knows for the current epoch.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.machine.quorum_state().leader_id.and_then(lab_id)
    }

    /// The current leader epoch.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.machine.quorum_state().leader_epoch
    }

    /// The machine's role name: `Unattached`, `Voted`, `Follower`,
    /// `Prospective`, `Candidate`, `Leader`, `Resigned` or `Observer`.
    #[must_use]
    pub fn role_name(&self) -> &'static str {
        self.machine.role().name()
    }

    /// The offset below which entries are committed.
    #[must_use]
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// One past the last entry in this node's log.
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        self.log.end_offset()
    }

    /// The offset of the leader-change marker that opened this node's
    /// leadership, while it leads. Kafka's controller claims its leadership
    /// once everything up to that marker is committed and applied, so the
    /// state it decides on holds every committed record.
    #[must_use]
    pub fn epoch_start_offset(&self) -> Option<i64> {
        self.epoch_start_offset
    }

    /// Whether the node runs: started and not stopped.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// This node's copy of the log.
    #[must_use]
    pub fn log(&self) -> &MetadataLog {
        &self.log
    }

    /// The `DescribeQuorum` answer for the metadata partition, filled as
    /// Kafka's leader fills it: the epoch, the high watermark, every voter and
    /// observer with its log end offset and the times it last fetched and was
    /// last caught up, and the voters' listeners. A node that is not the
    /// leader answers `NOT_LEADER_OR_FOLLOWER` on the partition.
    #[must_use]
    pub fn describe_quorum(&self) -> DescribeQuorumResponse {
        let Role::Leader { replicas, .. } = self.machine.role() else {
            return DescribeQuorumResponse {
                topics: vec![TopicData {
                    topic_name: METADATA_TOPIC.into(),
                    partitions: vec![PartitionData {
                        partition_index: 0,
                        error_code: codes::NOT_LEADER_OR_FOLLOWER,
                        error_message: Some(NOT_LEADER_OR_FOLLOWER_MESSAGE.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
        };
        let own = ReplicaProgress {
            fetch_offset: self.log.end_offset(),
            last_fetch: SimInstant(self.now),
            last_caught_up: SimInstant(self.now),
        };
        let current_voters = self
            .voters
            .iter()
            .map(|&voter| {
                let progress = if voter == self.me {
                    Some(&own)
                } else {
                    replicas.get(&broker_id(voter))
                };
                replica_state(voter, progress)
            })
            .collect();
        let observers = self
            .observers
            .iter()
            .map(|(&observer, progress)| replica_state(observer, Some(progress)))
            .collect();
        DescribeQuorumResponse {
            topics: vec![TopicData {
                topic_name: METADATA_TOPIC.into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    leader_id: wire_id(self.me),
                    leader_epoch: i32::try_from(self.epoch()).unwrap_or(i32::MAX),
                    high_watermark: self.high_watermark,
                    current_voters,
                    observers,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            nodes: self
                .voters
                .iter()
                .map(|&voter| Node {
                    node_id: wire_id(voter),
                    listeners: vec![Listener {
                        name: "CONTROLLER".into(),
                        host: format!("node-{voter}"),
                        port: RAFT_PORT,
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The observable state for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "role": self.role_name(),
            "epoch": self.epoch(),
            "leader": self.leader(),
            "hwm": self.high_watermark,
            "leo": self.log.end_offset(),
            "voters": self.voters,
            "voter": self.is_voter(),
            "log_len": self.log.len(),
            "applied": self.delivered,
            "observers": self.observers.keys().collect::<Vec<_>>(),
        })
    }

    // ---- message handling -------------------------------------------------------

    fn handle(&mut self, ctx: &mut Ctx<'_>, from: NodeId, message: RaftMessage) {
        match message {
            RaftMessage::VoteRequest {
                cluster_id,
                voter_id,
                candidate_epoch,
                candidate,
                last_epoch,
                last_offset,
                pre_vote,
            } => {
                self.step(
                    ctx,
                    Event::ReceiveVoteRequest {
                        from: broker_id(from),
                        cluster_id: Some(cluster_id),
                        voter_id: broker_id(voter_id),
                        voter_directory_id: Uuid::nil(),
                        candidate_epoch,
                        candidate: broker_id(candidate),
                        candidate_directory_id: Uuid::nil(),
                        candidate_log_end: LogEnd {
                            last_epoch,
                            last_offset,
                        },
                        pre_vote,
                    },
                );
            }
            RaftMessage::VoteResponse { epoch, granted } => {
                self.step(
                    ctx,
                    Event::ReceiveVoteResponse {
                        from: broker_id(from),
                        epoch,
                        vote_granted: granted,
                    },
                );
            }
            RaftMessage::BeginQuorumEpoch { leader_epoch } => {
                self.step(
                    ctx,
                    Event::ReceiveBeginQuorumEpoch {
                        leader_id: broker_id(from),
                        leader_epoch,
                    },
                );
            }
            RaftMessage::EndQuorumEpoch {
                leader_epoch,
                preferred_successors,
            } => {
                self.step(
                    ctx,
                    Event::ReceiveEndQuorumEpoch {
                        leader_id: broker_id(from),
                        leader_epoch,
                        successor_rank: successor_rank(&preferred_successors, self.me),
                    },
                );
            }
            RaftMessage::Fetch {
                correlation,
                fetch_epoch,
                fetch_offset,
                high_watermark,
            } => self.serve_fetch(
                ctx,
                from,
                correlation,
                (fetch_epoch, fetch_offset),
                high_watermark,
            ),
            RaftMessage::FetchResponse(response) => self.on_fetch_response(ctx, from, response),
        }
    }

    /// Follower side of a `FetchResponse`: append what the leader sent, learn
    /// its high watermark, and let the machine fetch again; or follow the
    /// leader a redirect names.
    fn on_fetch_response(&mut self, ctx: &mut Ctx<'_>, from: NodeId, response: FetchResponse) {
        let FetchResponse {
            correlation,
            leader_id,
            leader_epoch,
            diverging,
            high_watermark,
            start_offset,
            entries,
        } = response;
        let in_flight = InFlightFetch {
            correlation,
            target: from,
        };
        if self.fetch != Some(in_flight) || leader_epoch < self.epoch() {
            return;
        }
        self.fetch = None;
        if leader_id == Some(from) {
            if diverging.is_none() {
                if self.log.append_from(start_offset, &entries) {
                    for (index, entry) in entries.iter().enumerate() {
                        let offset =
                            start_offset.saturating_add(i64::try_from(index).unwrap_or(i64::MAX));
                        persist_entry(ctx, offset, entry);
                    }
                }
                let hwm = high_watermark.min(self.log.end_offset());
                if hwm > self.high_watermark {
                    self.high_watermark = hwm;
                    self.persist_high_watermark(ctx);
                }
            }
            let attached = self.leader() == Some(from) && leader_epoch == self.epoch();
            if attached {
                self.step(
                    ctx,
                    Event::ReceiveFetchResponse {
                        leader_id: broker_id(from),
                        leader_epoch,
                        diverging: diverging.map(Into::into),
                    },
                );
            } else {
                // Kafka's follower attaches to the leader a fetch response
                // names; the machine's announcement path applies the same
                // guards and fetches again.
                self.step(
                    ctx,
                    Event::ReceiveBeginQuorumEpoch {
                        leader_id: broker_id(from),
                        leader_epoch,
                    },
                );
            }
        } else if let Some(leader) = leader_id.filter(|&leader| leader != self.me) {
            self.step(
                ctx,
                Event::ReceiveBeginQuorumEpoch {
                    leader_id: broker_id(leader),
                    leader_epoch,
                },
            );
        } else if self.is_discovering_observer() {
            let retry = self.now + DISCOVERY_RETRY_MS;
            self.timers.fetch = Some(self.timers.fetch.map_or(retry, |at| at.min(retry)));
        }
    }

    /// Leader side of a `Fetch` from the log point `(fetch_epoch,
    /// fetch_offset)`. A node that does not lead redirects the fetch to the
    /// leader it knows. The leader answers at once when it has entries, a
    /// divergence or a high watermark the fetcher lacks, and holds the fetch
    /// otherwise.
    fn serve_fetch(
        &mut self,
        ctx: &mut Ctx<'_>,
        from: NodeId,
        correlation: u64,
        (fetch_epoch, fetch_offset): (Epoch, i64),
        high_watermark: i64,
    ) {
        let actions = self.step(
            ctx,
            Event::ReceiveFetch {
                from: broker_id(from),
                fetch_epoch,
                fetch_offset,
            },
        );
        if !self.machine.role().is_leader() {
            let redirect = RaftMessage::FetchResponse(FetchResponse {
                correlation,
                leader_id: self.leader(),
                leader_epoch: self.epoch(),
                diverging: None,
                high_watermark: -1,
                start_offset: fetch_offset,
                entries: Vec::new(),
            });
            self.send_to(ctx, from, &redirect);
            return;
        }
        let diverging = actions.iter().find_map(|action| match action {
            Action::ReplyDivergingEpoch(point) => Some(LogPoint::from(*point)),
            _ => None,
        });
        let advanced = actions
            .iter()
            .any(|action| matches!(action, Action::AdvanceHighWatermark(_)));
        if !self.voters.contains(&from) && diverging.is_none() {
            let caught_up = fetch_offset >= self.log.end_offset();
            let progress = self.observers.entry(from).or_default();
            progress.fetch_offset = fetch_offset;
            progress.last_fetch = SimInstant(self.now);
            if caught_up {
                progress.last_caught_up = SimInstant(self.now);
            }
        }
        let has_data = diverging.is_none() && fetch_offset < self.log.end_offset();
        let stale = high_watermark < self.high_watermark;
        if diverging.is_some() || has_data || advanced || stale {
            self.answer_fetch(ctx, from, correlation, fetch_offset, diverging);
        } else {
            let deadline = self.now + FETCH_MAX_WAIT_MS;
            self.held.insert(
                from,
                HeldFetch {
                    correlation,
                    fetch_offset,
                    deadline,
                },
            );
            self.timers.purgatory = Some(
                self.timers
                    .purgatory
                    .map_or(deadline, |at| at.min(deadline)),
            );
        }
    }

    fn answer_fetch(
        &mut self,
        ctx: &mut Ctx<'_>,
        to: NodeId,
        correlation: u64,
        fetch_offset: i64,
        diverging: Option<LogPoint>,
    ) {
        let entries = if diverging.is_some() {
            Vec::new()
        } else {
            self.log
                .entries_from(fetch_offset, MAX_FETCH_ENTRIES)
                .to_vec()
        };
        let response = RaftMessage::FetchResponse(FetchResponse {
            correlation,
            leader_id: Some(self.me),
            leader_epoch: self.epoch(),
            diverging,
            high_watermark: self.high_watermark,
            start_offset: fetch_offset,
            entries,
        });
        self.send_to(ctx, to, &response);
    }

    /// Answer every held fetch: the log or the high watermark moved.
    fn release_held_fetches(&mut self, ctx: &mut Ctx<'_>) {
        let held = std::mem::take(&mut self.held);
        self.timers.purgatory = None;
        for (to, fetch) in held {
            self.answer_fetch(ctx, to, fetch.correlation, fetch.fetch_offset, None);
        }
    }

    /// Answer the held fetches whose wait is over.
    fn release_due_fetches(&mut self, ctx: &mut Ctx<'_>) {
        let now = self.now;
        let held = std::mem::take(&mut self.held);
        for (to, fetch) in held {
            if fetch.deadline <= now {
                self.answer_fetch(ctx, to, fetch.correlation, fetch.fetch_offset, None);
            } else {
                self.held.insert(to, fetch);
            }
        }
        self.timers.purgatory = self.held.values().map(|fetch| fetch.deadline).min();
    }

    /// Send a fetch for the log after this node's tip.
    fn fetch_from(&mut self, ctx: &mut Ctx<'_>, target: NodeId) {
        self.next_correlation += 1;
        let correlation = self.next_correlation;
        self.fetch = Some(InFlightFetch {
            correlation,
            target,
        });
        let fetch = RaftMessage::Fetch {
            correlation,
            fetch_epoch: self.log.last_epoch(),
            fetch_offset: self.log.end_offset(),
            high_watermark: self.high_watermark,
        };
        self.send_to(ctx, target, &fetch);
    }

    /// An observer without a leader asks the next voter, which redirects it.
    fn discover(&mut self, ctx: &mut Ctx<'_>) {
        if self.voters.is_empty() {
            return;
        }
        let target = self.voters[self.discovery_cursor % self.voters.len()];
        self.discovery_cursor = self.discovery_cursor.wrapping_add(1);
        self.fetch_from(ctx, target);
    }

    fn is_discovering_observer(&self) -> bool {
        matches!(
            self.machine.role(),
            Role::Observer {
                leader_id: None,
                ..
            }
        )
    }

    // ---- the machine and its actions -------------------------------------------

    /// Hand one event to the machine and execute what it returns.
    fn step(&mut self, ctx: &mut Ctx<'_>, event: Event) -> Vec<Action> {
        let now = SimInstant(self.now);
        let actions = self.machine.on_event(event, &self.log, now);
        for action in actions.clone() {
            self.apply_action(ctx, action);
        }
        self.reconcile();
        actions
    }

    /// Execute one action, as `sim/bus.rs` does over its bus.
    fn apply_action(&mut self, ctx: &mut Ctx<'_>, action: Action) {
        match action {
            Action::SendVoteRequest { epoch, pre_vote } => {
                self.broadcast_vote_request(ctx, epoch, pre_vote);
            }
            Action::ReplyVote { to, epoch, granted } => {
                if let Some(to) = lab_id(to) {
                    self.send_to(ctx, to, &RaftMessage::VoteResponse { epoch, granted });
                }
            }
            Action::SendBeginQuorumEpoch { epoch } => {
                self.broadcast_to_peers(
                    ctx,
                    &RaftMessage::BeginQuorumEpoch {
                        leader_epoch: epoch,
                    },
                );
            }
            Action::SendEndQuorumEpoch {
                epoch,
                preferred_successors,
            } => {
                self.broadcast_to_peers(
                    ctx,
                    &RaftMessage::EndQuorumEpoch {
                        leader_epoch: epoch,
                        preferred_successors: preferred_successors
                            .into_iter()
                            .filter_map(lab_id)
                            .collect(),
                    },
                );
            }
            Action::SendFetch { leader_id } => {
                if let Some(leader) = lab_id(leader_id) {
                    self.fetch_from(ctx, leader);
                }
            }
            Action::AppendLeaderChange { epoch } => {
                let entry = Entry {
                    epoch,
                    records: Vec::new(),
                };
                self.epoch_start_offset = Some(self.log.end_offset());
                persist_entry(ctx, self.log.end_offset(), &entry);
                self.log.append(entry.epoch, entry.records);
                self.after_local_append(ctx);
            }
            Action::AdvanceHighWatermark(hwm) => {
                if hwm > self.high_watermark {
                    self.high_watermark = hwm;
                    self.persist_high_watermark(ctx);
                    self.release_held_fetches(ctx);
                }
            }
            Action::TruncateTo(point) => self.truncate(ctx, point),
            Action::ResetTimer { kind, deadline } => {
                let timer = match kind {
                    TimerKind::Election => Timer::Election,
                    TimerKind::Fetch => Timer::Fetch,
                    TimerKind::CheckQuorum => Timer::CheckQuorum,
                };
                *self.timers.slot(timer) = Some(deadline.0);
            }
            Action::TransitionedTo(role) => {
                let kind = if role == "Leader" { "elect" } else { "quorum" };
                ctx.event(
                    kind,
                    json!({ "role": role, "epoch": self.epoch(), "leader": self.leader() }),
                );
            }
            Action::PersistQuorumState => {
                let state = self.durable_quorum_state();
                ctx.persist(DurableOp::Put {
                    store: KRAFT_STATE_STORE.to_string(),
                    key: QUORUM_STATE_KEY.to_string(),
                    value: Bytes::from(serde_json::to_vec(&state).unwrap_or_default()),
                });
            }
            // The diverging point is carried in the fetch response the leader
            // builds.
            Action::ReplyDivergingEpoch(_) => {}
        }
    }

    /// A vote request to every other voter, each addressed by name.
    fn broadcast_vote_request(&mut self, ctx: &mut Ctx<'_>, epoch: Epoch, pre_vote: bool) {
        let LogEnd {
            last_epoch,
            last_offset,
        } = self.log.log_end();
        for peer in self.voters.clone() {
            let request = RaftMessage::VoteRequest {
                cluster_id: self.cluster_id,
                voter_id: peer,
                candidate_epoch: epoch,
                candidate: self.me,
                last_epoch,
                last_offset,
                pre_vote,
            };
            self.send_to(ctx, peer, &request);
        }
    }

    /// The same message to every other voter and every known observer.
    fn broadcast_to_peers(&mut self, ctx: &mut Ctx<'_>, message: &RaftMessage) {
        for peer in self.peers() {
            self.send_to(ctx, peer, message);
        }
    }

    /// Truncate the log to `point`, as the leader asked. A committed entry is
    /// on a majority and can never be asked back, so a truncation below the
    /// high watermark would be a protocol violation: it is refused and
    /// reported.
    fn truncate(&mut self, ctx: &mut Ctx<'_>, point: krabka_kraft_core::types::LogOffsetMetadata) {
        let offset = point.offset.max(self.high_watermark);
        if point.offset < self.high_watermark {
            ctx.event(
                "truncate",
                json!({ "level": "error", "asked": point.offset, "hwm": self.high_watermark }),
            );
        }
        if offset < self.log.end_offset() {
            ctx.event(
                "truncate",
                json!({ "level": "warn", "from": self.log.end_offset(), "to": offset, "epoch": point.epoch }),
            );
            self.log.truncate_to(offset);
            ctx.persist(DurableOp::TruncateFrom {
                store: KRAFT_LOG_STORE.to_string(),
                index: u64::try_from(offset).unwrap_or(u64::MAX),
            });
        }
    }

    fn persist_high_watermark(&self, ctx: &mut Ctx<'_>) {
        ctx.persist(DurableOp::Put {
            store: KRAFT_STATE_STORE.to_string(),
            key: HIGH_WATERMARK_KEY.to_string(),
            value: Bytes::from(self.high_watermark.to_string()),
        });
    }

    /// The leader appended an entry: a lone voter, which no follower ever
    /// fetches from, commits it through the machine's own high-watermark
    /// rule by scoring its own log end, and then the held fetches are
    /// answered, carrying the new high watermark, as Kafka's
    /// `updateLeaderEndOffsetAndTimestamp` moves the high watermark before it
    /// completes the fetch purgatory.
    fn after_local_append(&mut self, ctx: &mut Ctx<'_>) {
        if self.voters.len() == 1 && self.machine.role().is_leader() {
            self.step(
                ctx,
                Event::ReceiveFetch {
                    from: broker_id(self.me),
                    fetch_epoch: self.log.last_epoch(),
                    fetch_offset: self.log.end_offset(),
                },
            );
        }
        self.release_held_fetches(ctx);
    }

    /// Keep only the timers and the state the current role uses, as
    /// `sim/scheduler.rs` does after every event.
    fn reconcile(&mut self) {
        match self.machine.role() {
            Role::Leader { .. } => {
                self.timers.election = None;
                self.timers.fetch = None;
                self.fetch = None;
                if self.timers.heartbeat.is_none() {
                    self.timers.heartbeat = Some(self.now + HEARTBEAT_MS);
                }
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                self.timers.election = None;
                self.timers.heartbeat = None;
                self.timers.check_quorum = None;
                self.timers.purgatory = None;
                self.held.clear();
                self.observers.clear();
                self.epoch_start_offset = None;
            }
            Role::Unattached { .. }
            | Role::Voted { .. }
            | Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Resigned => {
                self.timers.fetch = None;
                self.timers.heartbeat = None;
                self.timers.check_quorum = None;
                self.timers.purgatory = None;
                self.fetch = None;
                self.held.clear();
                self.observers.clear();
                self.epoch_start_offset = None;
            }
        }
    }

    // ---- links --------------------------------------------------------------------

    /// Every node a leader announces to: the other voters and the observers
    /// that fetched from it.
    fn peers(&self) -> Vec<NodeId> {
        let mut peers: Vec<NodeId> = self
            .voters
            .iter()
            .copied()
            .chain(self.observers.keys().copied())
            .filter(|&peer| peer != self.me)
            .collect();
        peers.sort_unstable();
        peers.dedup();
        peers
    }

    /// Send a message on the link to `peer`, opening it first when it is
    /// closed. A message sent during the reconnect backoff is dropped, as it
    /// would be while a TCP connection is down.
    fn send_to(&mut self, ctx: &mut Ctx<'_>, peer: NodeId, message: &RaftMessage) {
        if peer == self.me {
            return;
        }
        let listener = Endpoint::new(peer, RAFT_PORT);
        let client = Endpoint::client(self.me);
        let link = self.links.entry(peer).or_insert(Link {
            conn: ConnId(RAFT_CONN_BASE),
            open: false,
            closed_at: None,
        });
        if !link.open {
            if link
                .closed_at
                .is_some_and(|at| self.now < at + RECONNECT_BACKOFF_MS)
            {
                return;
            }
            self.next_conn = self.next_conn.wrapping_add(1);
            link.conn = ConnId(RAFT_CONN_BASE.wrapping_add(self.next_conn));
            link.open = true;
            link.closed_at = None;
            ctx.send(Frame::open(client, listener, link.conn));
        }
        ctx.send(Frame::data(client, listener, link.conn, message.encode()));
    }
}

/// Record one log entry at `offset` for the host to store.
fn persist_entry(ctx: &mut Ctx<'_>, offset: i64, entry: &Entry) {
    ctx.persist(DurableOp::Append {
        store: KRAFT_LOG_STORE.to_string(),
        index: u64::try_from(offset).unwrap_or(u64::MAX),
        // The entry is records the image already accepted, so the
        // serialization cannot fail.
        bytes: Bytes::from(serde_json::to_vec(entry).unwrap_or_default()),
    });
}

/// The voter set the machine runs with: every voter with a nil directory id
/// and no endpoints, as `sim/node.rs` builds it.
fn voter_set(voters: &[NodeId]) -> VoterSet {
    VoterSet::from_voters(voters.iter().map(|&id| Voter {
        id: broker_id(id),
        directory_id: Uuid::nil(),
        endpoints: Vec::new(),
        kraft_version: KRaftVersionRange::default(),
    }))
}

/// Where `me` stands in a resigning leader's preferred successors.
fn successor_rank(preferred: &[NodeId], me: NodeId) -> SuccessorRank {
    let position = preferred
        .iter()
        .position(|&node| node == me)
        .unwrap_or(preferred.len());
    SuccessorRank {
        position: u32::try_from(position).unwrap_or(u32::MAX),
        successors: u32::try_from(preferred.len()).unwrap_or(u32::MAX),
    }
}

/// A node id as `DescribeQuorum` carries it.
fn wire_id(node: NodeId) -> i32 {
    i32::try_from(node.0).unwrap_or(-1)
}

/// One replica row of `DescribeQuorum`: the progress the leader tracks, or
/// `-1` everywhere for a replica that never fetched.
fn replica_state(node: NodeId, progress: Option<&ReplicaProgress>) -> ReplicaState {
    let fetched = progress.filter(|progress| progress.last_fetch.0 > 0);
    let timestamp = |at: SimInstant| {
        if at.0 > 0 {
            i64::try_from(at.0).unwrap_or(i64::MAX)
        } else {
            -1
        }
    };
    ReplicaState {
        replica_id: wire_id(node),
        log_end_offset: fetched.map_or(-1, |progress| progress.fetch_offset),
        last_fetch_timestamp: fetched.map_or(-1, |progress| timestamp(progress.last_fetch)),
        last_caught_up_timestamp: fetched.map_or(-1, |progress| timestamp(progress.last_caught_up)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn election_timeouts_are_staggered_by_node_id() {
        let cases = [(NodeId(0), 1_000), (NodeId(1), 1_050), (NodeId(3), 1_150)];
        for (node, want) in cases {
            assert!(election_timeout_ms(node) == want, "node {node}");
        }
    }

    #[test]
    fn timers_fire_the_earliest_due_deadline_first() {
        let mut timers = Timers::default();
        assert!(timers.next().is_none());
        assert!(timers.due(100).is_none());
        timers.heartbeat = Some(300);
        timers.election = Some(500);
        timers.purgatory = Some(300);
        assert!(timers.next() == Some(300));
        assert!(timers.due(299).is_none());
        assert!(timers.due(300) == Some(Timer::Heartbeat));
        *timers.slot(Timer::Heartbeat) = None;
        assert!(timers.due(300) == Some(Timer::Purgatory));
        *timers.slot(Timer::Purgatory) = None;
        assert!(timers.due(300).is_none());
        assert!(timers.due(500) == Some(Timer::Election));
    }

    #[test]
    fn successor_rank_is_the_position_in_the_list_or_its_length() {
        let list = [NodeId(3), NodeId(1)];
        let cases = [(NodeId(3), 0), (NodeId(1), 1), (NodeId(2), 2)];
        for (me, position) in cases {
            assert!(
                successor_rank(&list, me)
                    == SuccessorRank {
                        position,
                        successors: 2
                    },
                "node {me}"
            );
        }
        assert!(successor_rank(&[], NodeId(1)) == SuccessorRank::default());
    }

    #[test]
    fn a_replica_that_never_fetched_reports_minus_one() {
        assert!(
            replica_state(NodeId(2), None)
                == ReplicaState {
                    replica_id: 2,
                    log_end_offset: -1,
                    last_fetch_timestamp: -1,
                    last_caught_up_timestamp: -1,
                    ..Default::default()
                }
        );
        let progress = ReplicaProgress {
            fetch_offset: 7,
            last_fetch: SimInstant(400),
            last_caught_up: SimInstant(0),
        };
        assert!(
            replica_state(NodeId(2), Some(&progress))
                == ReplicaState {
                    replica_id: 2,
                    log_end_offset: 7,
                    last_fetch_timestamp: 400,
                    last_caught_up_timestamp: -1,
                    ..Default::default()
                }
        );
    }
}
