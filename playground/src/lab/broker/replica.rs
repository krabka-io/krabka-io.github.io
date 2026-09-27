//! Replicas: the partitions this broker hosts, the follower tracking a leader
//! keeps, and the fetch loop a follower runs against its leader.
//!
//! A leader advances the high watermark to the smallest log end offset over
//! the ISR on every follower fetch, and completes the `acks=-1` produces the
//! advance covers. A follower runs Kafka's replica fetcher against the
//! leader's Kafka endpoint, over a connection of its own: an `ApiVersions`
//! handshake, then a loop of `Fetch` requests with its `replica_id`,
//! appending the leader's batches verbatim and adopting the leader's high
//! watermark and log start offset. On a leader or epoch change a log with
//! epoch history fetches at once with its `last_fetched_epoch`, and the
//! leader's diverging epoch truncates it (KIP-320, Kafka's truncation on
//! fetch); a log without one truncates to its high watermark first.
//! `OFFSET_OUT_OF_RANGE` resets the follower against the leader's log end and
//! log start with `ListOffsets`. A request unanswered after
//! `request_timeout_ms` closes the connection, and a closed connection
//! reconnects with backoff.
//!
//! ISR shrink and expand are decided here, as Kafka's `Partition` decides
//! them, and applied locally through [`BrokerNode::apply_metadata`]; each
//! decision is also queued for the controller batch as an
//! [`AlterPartitionProposal`].

use std::{collections::BTreeMap, ops::Range};

use bytes::Bytes;
use krabka_metadata::PartitionRecord;
use krabka_protocol::{
    ApiKey, Decode, DecodeBorrow, ProtocolError, ProtocolRequest,
    borrowed::fetch_response::FetchResponse,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{RecordBatchBorrowed, RecordsPayloadBorrowed},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use super::{
    BrokerNode, TopicPartition, cluster,
    conn::{parse_response_frame, request_frame},
    log::{EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, PartitionLog},
};
use crate::lab::{
    codes,
    net::{ConnId, Ctx, Endpoint, Frame, Millis, NodeId, Payload},
};

/// Kafka's `replica.fetch.wait.max.ms`.
pub const FETCH_MAX_WAIT_MS: i32 = 500;
/// Kafka's `replica.fetch.min.bytes`.
pub const FETCH_MIN_BYTES: i32 = 1;
/// Kafka's `replica.fetch.response.max.bytes`.
pub const FETCH_MAX_BYTES: i32 = 10_485_760;
/// Kafka's `replica.fetch.max.bytes`, per partition.
pub const PARTITION_MAX_BYTES: i32 = 1_048_576;
/// Kafka's `replica.fetch.backoff.ms`: the pause after a partition's fetch
/// failed.
pub const FETCH_BACKOFF_MS: Millis = 1_000;
/// Kafka's `reconnect.backoff.ms`.
pub const RECONNECT_BACKOFF_MS: Millis = 50;
/// Kafka's `reconnect.backoff.max.ms`.
pub const RECONNECT_BACKOFF_MAX_MS: Millis = 1_000;
/// The KIP-511 name a lab broker sends in its `ApiVersions` handshake.
pub const SOFTWARE_NAME: &str = "krabka-lab-broker";
/// The KIP-511 version a lab broker sends in its `ApiVersions` handshake.
pub const SOFTWARE_VERSION: &str = "0.2.0";

/// A log end offset the leader has not learnt yet.
pub const UNKNOWN_LOG_END: i64 = -1;

/// Kafka's `Records.LOG_OVERHEAD`: the base offset and the batch length
/// before the part of a batch that `batch_length` counts.
const LOG_OVERHEAD: usize = 12;

/// What a leader knows about one follower.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FollowerState {
    /// The follower's log end offset, its last fetch offset; `-1` before the
    /// first fetch.
    pub log_end_offset: i64,
    /// The follower's log start offset from its last fetch.
    pub log_start_offset: i64,
    /// When the follower last fetched.
    pub last_fetch_at: Millis,
    /// When the follower last read up to the leader's end.
    pub last_caught_up_at: Millis,
    /// The leader's log end when the follower last fetched, for Kafka's
    /// "caught up to the previous end" rule.
    pub leader_log_end_at_last_fetch: i64,
}

impl FollowerState {
    /// The state Kafka's `Replica.resetReplicaState` gives a follower under
    /// a new leader: nothing fetched yet, and caught up now when it is in the
    /// ISR, so it has the whole lag bound to fetch.
    fn fresh(now: Millis, in_sync: bool) -> Self {
        Self {
            log_end_offset: UNKNOWN_LOG_END,
            log_start_offset: UNKNOWN_LOG_END,
            last_fetch_at: 0,
            last_caught_up_at: if in_sync { now } else { 0 },
            leader_log_end_at_last_fetch: UNKNOWN_LOG_END,
        }
    }

    /// How long ago the follower was caught up.
    #[must_use]
    pub fn lag_ms(&self, now: Millis) -> Millis {
        now.saturating_sub(self.last_caught_up_at)
    }
}

/// Where a follower replica is in its fetch loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchState {
    /// Not a follower with a leader to fetch from.
    Idle,
    /// The leader or its epoch changed; the next poll reconciles the log
    /// before it fetches.
    Truncating,
    Fetching,
    /// The leader answered `OFFSET_OUT_OF_RANGE`: learn its log end.
    ResetLatest,
    /// The leader's log end is not behind this log's: learn its log start.
    ResetEarliest,
    /// The last fetch failed; the next one waits until `until`.
    Delayed {
        until: Millis,
    },
}

impl FetchState {
    /// The state's name, for the snapshot.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Truncating => "truncating",
            Self::Fetching => "fetching",
            Self::ResetLatest => "reset_latest",
            Self::ResetEarliest => "reset_earliest",
            Self::Delayed { .. } => "delayed",
        }
    }
}

/// What applying a partition record changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicaChange {
    /// The leader moved.
    pub leader_changed: bool,
    /// The leader epoch moved.
    pub epoch_changed: bool,
    /// The ISR changed.
    pub isr_changed: bool,
}

/// An ISR change this broker decided as leader, in the shape an
/// `AlterPartition` request carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterPartitionProposal {
    /// The topic.
    pub topic: String,
    /// The partition.
    pub partition: i32,
    /// The ISR the leader proposes.
    pub new_isr: Vec<i32>,
    /// The leader epoch the proposal was made under.
    pub leader_epoch: i32,
    /// The partition epoch the proposal replaces.
    pub partition_epoch: i32,
}

/// A partition this broker hosts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replica {
    /// The topic.
    pub topic: String,
    /// The partition.
    pub partition: i32,
    /// The topic's id, which fetches name it by.
    pub topic_id: Uuid,
    /// The partition's log on this broker.
    pub log: PartitionLog,
    /// The leader the image names, `None` for a leaderless partition.
    pub leader: Option<i32>,
    /// The current leader epoch.
    pub leader_epoch: i32,
    /// The current partition epoch.
    pub partition_epoch: i32,
    /// The assigned replicas, in assignment order.
    pub replicas: Vec<i32>,
    /// The in-sync replicas.
    pub isr: Vec<i32>,
    /// The followers a leader tracks.
    pub followers: BTreeMap<i32, FollowerState>,
    /// The log end when this broker took the lead at `leader_epoch`.
    pub epoch_start_offset: i64,
    /// Where the follower fetch loop is.
    pub fetch: FetchState,
}

impl Replica {
    /// A replica with an empty log; [`Replica::apply_record`] fills the rest.
    #[must_use]
    pub fn new(topic: &str, partition: i32, topic_id: Uuid) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
            topic_id,
            log: PartitionLog::new(),
            leader: None,
            leader_epoch: -1,
            partition_epoch: -1,
            replicas: Vec::new(),
            isr: Vec::new(),
            followers: BTreeMap::new(),
            epoch_start_offset: 0,
            fetch: FetchState::Idle,
        }
    }

    /// Whether broker `me` leads this partition.
    #[must_use]
    pub fn leads(&self, me: i32) -> bool {
        self.leader == Some(me)
    }

    /// Take the leader, ISR and epochs of a committed partition record. A
    /// new leadership assigns the epoch at the log end and resets the
    /// follower tracking; a new leader or epoch sends a follower back to
    /// truncation.
    pub fn apply_record(
        &mut self,
        record: &PartitionRecord,
        me: i32,
        now: Millis,
    ) -> ReplicaChange {
        let leader = cluster::record_leader(record);
        let epoch = record.leader_epoch.0;
        let replicas: Vec<i32> = record
            .replicas
            .iter()
            .map(|r| cluster::wire_id(*r))
            .collect();
        let isr: Vec<i32> = record.isr.iter().map(|r| cluster::wire_id(*r)).collect();
        let change = ReplicaChange {
            leader_changed: leader != self.leader,
            epoch_changed: epoch != self.leader_epoch,
            isr_changed: isr != self.isr,
        };
        self.leader = leader;
        self.leader_epoch = epoch;
        self.partition_epoch = record.partition_epoch;
        self.replicas = replicas;
        self.isr = isr;
        if self.leads(me) {
            if change.leader_changed || change.epoch_changed {
                self.become_leader(me, now);
            } else {
                for id in self.replicas.clone() {
                    if id != me {
                        let in_sync = self.isr.contains(&id);
                        self.followers
                            .entry(id)
                            .or_insert_with(|| FollowerState::fresh(now, in_sync));
                    }
                }
                self.followers.retain(|id, _| self.replicas.contains(id));
            }
            self.recompute_hwm(me);
        } else if change.leader_changed || change.epoch_changed {
            self.followers.clear();
            self.fetch = if self.leader.is_some() {
                FetchState::Truncating
            } else {
                FetchState::Idle
            };
        }
        change
    }

    fn become_leader(&mut self, me: i32, now: Millis) {
        let log_end = self.log.log_end_offset();
        self.log.assign_epoch(self.leader_epoch, log_end);
        self.epoch_start_offset = log_end;
        self.fetch = FetchState::Idle;
        self.followers = self
            .replicas
            .iter()
            .copied()
            .filter(|id| *id != me)
            .map(|id| (id, FollowerState::fresh(now, self.isr.contains(&id))))
            .collect();
    }

    /// Reset the in-memory state a boot loses: follower tracking on a leader,
    /// the fetch loop on a follower.
    pub fn on_restart(&mut self, me: i32, now: Millis) {
        if self.leads(me) {
            self.become_leader(me, now);
            self.recompute_hwm(me);
        } else if self.leader.is_some() {
            self.fetch = FetchState::Truncating;
        }
    }

    /// Note a follower's fetch at `fetch_offset`. Returns whether the
    /// follower now qualifies for the ISR: it is a replica outside the ISR
    /// that has caught up to the high watermark and to this leader's epoch
    /// start, Kafka's `Partition.isFollowerInSync`.
    pub fn record_follower_fetch(
        &mut self,
        follower: i32,
        fetch_offset: i64,
        log_start_offset: i64,
        now: Millis,
    ) -> bool {
        let leader_log_end = self.log.log_end_offset();
        let in_sync = self.isr.contains(&follower);
        let state = self
            .followers
            .entry(follower)
            .or_insert_with(|| FollowerState::fresh(now, in_sync));
        let previous_fetch_at = state.last_fetch_at;
        let previous_leader_log_end = state.leader_log_end_at_last_fetch;
        state.log_end_offset = fetch_offset;
        state.log_start_offset = log_start_offset;
        state.last_fetch_at = now;
        if fetch_offset >= leader_log_end {
            state.last_caught_up_at = state.last_caught_up_at.max(now);
        } else if fetch_offset >= previous_leader_log_end {
            state.last_caught_up_at = state.last_caught_up_at.max(previous_fetch_at);
        }
        state.leader_log_end_at_last_fetch = leader_log_end;
        !in_sync
            && self.replicas.contains(&follower)
            && fetch_offset >= self.log.high_watermark()
            && fetch_offset >= self.epoch_start_offset
    }

    /// Move the high watermark to the smallest log end over the ISR. A
    /// follower whose end this leader has not learnt holds it back. Returns
    /// whether it advanced.
    pub fn recompute_hwm(&mut self, me: i32) -> bool {
        if !self.leads(me) {
            return false;
        }
        let mut floor = self.log.log_end_offset();
        for id in &self.isr {
            if *id == me {
                continue;
            }
            match self.followers.get(id) {
                Some(f) if f.log_end_offset >= 0 => floor = floor.min(f.log_end_offset),
                _ => return false,
            }
        }
        self.log.set_high_watermark(floor)
    }

    /// The ISR followers that have not caught up for longer than `lag`
    /// while behind the log end, Kafka's `Partition.isFollowerOutOfSync`.
    #[must_use]
    pub fn out_of_sync_followers(&self, me: i32, now: Millis, lag: Millis) -> Vec<i32> {
        let log_end = self.log.log_end_offset();
        self.isr
            .iter()
            .copied()
            .filter(|id| *id != me)
            .filter(|id| match self.followers.get(id) {
                Some(f) => f.log_end_offset != log_end && f.lag_ms(now) > lag,
                None => true,
            })
            .collect()
    }

    /// How far a follower lags, as the inspector shows it: zero while it
    /// holds everything the leader holds.
    #[must_use]
    pub fn follower_lag_ms(&self, follower: &FollowerState, now: Millis) -> Millis {
        if follower.log_end_offset >= self.log.log_end_offset() {
            0
        } else {
            follower.lag_ms(now)
        }
    }
}

/// A request a link is waiting on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InFlight {
    /// The correlation id the answer must echo.
    pub correlation: i32,
    /// The api of the request.
    pub api_key: ApiKey,
    /// The version the request went out at.
    pub version: i16,
    /// When it went out, for the request timeout.
    pub sent_at: Millis,
}

/// The client connection a follower keeps to one leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderLink {
    /// The leader the link reaches.
    pub leader: i32,
    /// The current connection. A reconnect takes a fresh id, so a late frame
    /// of an earlier connection is never read as an answer.
    pub conn: ConnId,
    /// Whether the connection is open.
    pub open: bool,
    /// Whether the `ApiVersions` handshake answered.
    pub handshaken: bool,
    /// The request waiting for its answer.
    pub in_flight: Option<InFlight>,
    /// The correlation id of the next request.
    pub next_correlation: i32,
    /// When a closed link opens again.
    pub reconnect_at: Option<Millis>,
    /// The pause before the next reconnect, doubling up to the maximum.
    pub reconnect_backoff: Millis,
    /// The earliest time the next request may leave, so a leader that
    /// answers at once cannot make the loop spin inside one instant.
    pub earliest_send_at: Millis,
}

impl LeaderLink {
    fn new(leader: i32, conn: ConnId) -> Self {
        Self {
            leader,
            conn,
            open: false,
            handshaken: false,
            in_flight: None,
            next_correlation: 0,
            reconnect_at: None,
            reconnect_backoff: RECONNECT_BACKOFF_MS,
            earliest_send_at: 0,
        }
    }
}

/// What a link should send next, by partition.
#[derive(Default)]
struct LinkWork {
    reset_latest: Vec<(TopicPartition, i32)>,
    reset_earliest: Vec<(TopicPartition, i32)>,
    fetching: Vec<TopicPartition>,
}

/// One partition row of a fetch answer, with the batches as the bytes the
/// leader sent.
struct FetchedPartition {
    error_code: i16,
    high_watermark: i64,
    log_start_offset: i64,
    diverging_epoch: (i32, i64),
    records: Bytes,
}

/// The Kafka endpoint of broker `id`.
fn broker_endpoint(id: i32) -> Endpoint {
    Endpoint::kafka(NodeId(u32::try_from(id).unwrap_or(u32::MAX)))
}

/// The byte range of `batches` inside `buf`, the buffer the borrowed decoder
/// read them from. That decoder reinterprets every batch header in place, so
/// a header's address locates its batch, and a batch runs `LOG_OVERHEAD +
/// batch_length` bytes from there. The follower appends exactly these bytes.
fn batch_span(buf: &[u8], batches: &[RecordBatchBorrowed<'_>]) -> Option<Range<usize>> {
    let base = buf.as_ptr().addr();
    let first = batches.first()?;
    let last = batches.last()?;
    let start = std::ptr::from_ref(first.header())
        .addr()
        .checked_sub(base)?;
    let last_start = std::ptr::from_ref(last.header()).addr().checked_sub(base)?;
    let last_len = usize::try_from(last.header().batch_length.get())
        .ok()?
        .checked_add(LOG_OVERHEAD)?;
    let end = last_start.checked_add(last_len)?;
    (start <= end && end <= buf.len()).then_some(start..end)
}

/// Truncate a follower's log to `target`, recording a truncation that drops
/// records.
fn truncate_follower(ctx: &mut Ctx<'_>, key: &TopicPartition, replica: &mut Replica, target: i64) {
    let from = replica.log.log_end_offset();
    // Truncating at the log end still drops an epoch entry that starts there,
    // which is what lets the next truncation round ask about an older epoch.
    replica.log.truncate_to(target);
    if target < from {
        ctx.event(
            "replica_truncated",
            json!({ "topic": key.topic, "partition": key.partition, "from": from, "to": target, "level": "warn" }),
        );
    }
}

impl BrokerNode {
    /// Keep a link to every leader this broker follows, and close the ones it
    /// no longer needs.
    pub(super) fn sync_links(&mut self, ctx: &mut Ctx<'_>) {
        let me = self.config.broker_id;
        let leaders: Vec<i32> = self
            .replicas
            .values()
            .filter(|r| !r.leads(me))
            .filter_map(|r| r.leader)
            .collect();
        for leader in &leaders {
            if !self.links.contains_key(leader) {
                let conn = self.next_conn_id();
                self.links.insert(*leader, LeaderLink::new(*leader, conn));
            }
        }
        let stale: Vec<i32> = self
            .links
            .keys()
            .copied()
            .filter(|leader| !leaders.contains(leader))
            .collect();
        for leader in stale {
            if let Some(link) = self.links.remove(&leader)
                && link.open
            {
                ctx.send(Frame::close(
                    Endpoint::client(self.id),
                    broker_endpoint(leader),
                    link.conn,
                ));
            }
        }
    }

    /// The earliest time after `now` a link needs the timer: a reconnect, a
    /// request timeout, a paused partition, or a send the spin guard held
    /// back. Anything due at `now` was handled by [`BrokerNode::poll_links`]
    /// before this is asked.
    pub(super) fn link_deadline(&self, now: Millis) -> Option<Millis> {
        let timeout = self.config.request_timeout_ms;
        let mut next: Option<Millis> = None;
        let mut note = |at: Millis| {
            if at > now {
                next = Some(next.map_or(at, |n| n.min(at)));
            }
        };
        for link in self.links.values() {
            match (link.open, link.in_flight) {
                (false, _) => {
                    if let Some(at) = link.reconnect_at {
                        note(at);
                    }
                }
                (true, Some(in_flight)) => note(in_flight.sent_at + timeout),
                (true, None) => note(link.earliest_send_at),
            }
        }
        for replica in self.replicas.values() {
            if let FetchState::Delayed { until } = replica.fetch {
                note(until);
            }
        }
        next
    }

    /// Drive every link: open it, handshake, then send the request its
    /// partitions need.
    pub(super) fn poll_links(&mut self, ctx: &mut Ctx<'_>) {
        let leaders: Vec<i32> = self.links.keys().copied().collect();
        for leader in leaders {
            self.poll_link(ctx, leader);
        }
    }

    fn poll_link(&mut self, ctx: &mut Ctx<'_>, leader: i32) {
        let now = ctx.now();
        let timeout = self.config.request_timeout_ms;
        let timed_out = self.links.get(&leader).is_some_and(|link| {
            link.open && link.in_flight.is_some_and(|f| now >= f.sent_at + timeout)
        });
        if timed_out {
            // Kafka's `NetworkClient` disconnects a request that timed out.
            ctx.event(
                "fetch_error",
                json!({ "leader": leader, "reason": "request timed out; reconnecting", "level": "warn" }),
            );
            self.disconnect_link(ctx, leader, true);
        }
        let me = Endpoint::client(self.id);
        let Some(link) = self.links.get_mut(&leader) else {
            return;
        };
        if !link.open {
            if link.reconnect_at.is_some_and(|at| at > now) {
                return;
            }
            link.open = true;
            link.handshaken = false;
            link.in_flight = None;
            link.reconnect_at = None;
            ctx.send(Frame::open(me, broker_endpoint(leader), link.conn));
        }
        if link.in_flight.is_some() || link.earliest_send_at > now {
            return;
        }
        if !link.handshaken {
            let request = ApiVersionsRequest {
                client_software_name: SOFTWARE_NAME.to_string(),
                client_software_version: SOFTWARE_VERSION.to_string(),
                ..ApiVersionsRequest::default()
            };
            self.send_link_request(
                ctx,
                leader,
                ApiVersionsRequest::LATEST_STABLE_VERSION,
                &request,
            );
            return;
        }
        let work = self.classify_followers(ctx, leader);
        if !work.reset_latest.is_empty() {
            self.send_list_offsets(ctx, leader, &work.reset_latest, LATEST_TIMESTAMP);
        } else if !work.reset_earliest.is_empty() {
            self.send_list_offsets(ctx, leader, &work.reset_earliest, EARLIEST_TIMESTAMP);
        } else if !work.fetching.is_empty() {
            self.send_fetch(ctx, leader, &work.fetching);
        }
    }

    /// Close a link, closing the leader's side too when `notify_leader`, and
    /// schedule the reconnect on a fresh connection after the backoff.
    fn disconnect_link(&mut self, ctx: &mut Ctx<'_>, leader: i32, notify_leader: bool) {
        let conn = self.next_conn_id();
        let now = ctx.now();
        let me = Endpoint::client(self.id);
        let Some(link) = self.links.get_mut(&leader) else {
            return;
        };
        if notify_leader && link.open {
            ctx.send(Frame::close(me, broker_endpoint(leader), link.conn));
        }
        link.conn = conn;
        link.open = false;
        link.handshaken = false;
        link.in_flight = None;
        link.reconnect_at = Some(now + link.reconnect_backoff);
        link.reconnect_backoff = (link.reconnect_backoff * 2).min(RECONNECT_BACKOFF_MAX_MS);
    }

    /// Sort the partitions that follow `leader` by what they need next, and
    /// let a paused partition fetch again once its pause is over.
    ///
    /// A partition whose leader just changed reconciles here, as Kafka's
    /// fetcher does with truncation on fetch: a log with epoch history
    /// fetches at once and lets the leader's diverging epoch truncate it, and
    /// a log without one truncates to its high watermark
    /// (`truncateToHighWatermark`).
    fn classify_followers(&mut self, ctx: &mut Ctx<'_>, leader: i32) -> LinkWork {
        let me = self.config.broker_id;
        let now = ctx.now();
        let mut work = LinkWork::default();
        for (key, replica) in &mut self.replicas {
            if replica.leader != Some(leader) || replica.leads(me) {
                continue;
            }
            if let FetchState::Delayed { until } = replica.fetch {
                if until > now {
                    continue;
                }
                replica.fetch = FetchState::Fetching;
            }
            if replica.fetch == FetchState::Truncating {
                if replica.log.latest_epoch().is_none() {
                    let hwm = replica.log.high_watermark();
                    truncate_follower(ctx, key, replica, hwm);
                }
                replica.fetch = FetchState::Fetching;
            }
            match replica.fetch {
                FetchState::Fetching => work.fetching.push(key.clone()),
                FetchState::ResetLatest => {
                    work.reset_latest.push((key.clone(), replica.leader_epoch));
                }
                FetchState::ResetEarliest => {
                    work.reset_earliest
                        .push((key.clone(), replica.leader_epoch));
                }
                FetchState::Idle | FetchState::Truncating | FetchState::Delayed { .. } => {}
            }
        }
        work
    }

    fn send_link_request<R: ProtocolRequest>(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: i32,
        version: i16,
        request: &R,
    ) {
        let now = ctx.now();
        let me = Endpoint::client(self.id);
        let client_id = format!("broker-{}", self.config.broker_id);
        let (Some(api_key), Some(link)) =
            (ApiKey::from_i16(R::API_KEY), self.links.get_mut(&leader))
        else {
            return;
        };
        let correlation = link.next_correlation;
        link.next_correlation = link.next_correlation.wrapping_add(1);
        let Ok(frame) = request_frame(version, correlation, &client_id, request) else {
            return;
        };
        link.in_flight = Some(InFlight {
            correlation,
            api_key,
            version,
            sent_at: now,
        });
        ctx.send(Frame::data(me, broker_endpoint(leader), link.conn, frame));
    }

    fn send_list_offsets(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: i32,
        partitions: &[(TopicPartition, i32)],
        timestamp: i64,
    ) {
        let mut topics: BTreeMap<String, Vec<ListOffsetsPartition>> = BTreeMap::new();
        for (key, current_leader_epoch) in partitions {
            topics
                .entry(key.topic.clone())
                .or_default()
                .push(ListOffsetsPartition {
                    partition_index: key.partition,
                    current_leader_epoch: *current_leader_epoch,
                    timestamp,
                    ..ListOffsetsPartition::default()
                });
        }
        let request = ListOffsetsRequest {
            replica_id: self.config.broker_id,
            isolation_level: 0,
            topics: topics
                .into_iter()
                .map(|(name, partitions)| ListOffsetsTopic {
                    name,
                    partitions,
                    ..ListOffsetsTopic::default()
                })
                .collect(),
            ..ListOffsetsRequest::default()
        };
        self.send_link_request(
            ctx,
            leader,
            ListOffsetsRequest::LATEST_STABLE_VERSION,
            &request,
        );
    }

    fn send_fetch(&mut self, ctx: &mut Ctx<'_>, leader: i32, partitions: &[TopicPartition]) {
        let mut topics: BTreeMap<String, (Uuid, Vec<FetchPartition>)> = BTreeMap::new();
        for key in partitions {
            let Some(replica) = self.replicas.get(key) else {
                continue;
            };
            let entry = topics
                .entry(key.topic.clone())
                .or_insert_with(|| (replica.topic_id, Vec::new()));
            entry.1.push(FetchPartition {
                partition: key.partition,
                current_leader_epoch: replica.leader_epoch,
                fetch_offset: replica.log.log_end_offset(),
                last_fetched_epoch: replica.log.latest_epoch().unwrap_or(-1),
                log_start_offset: replica.log.log_start_offset(),
                partition_max_bytes: PARTITION_MAX_BYTES,
                high_watermark: replica.log.high_watermark(),
                ..FetchPartition::default()
            });
        }
        let broker_epoch = self
            .image
            .broker_epoch(cluster::meta_id(self.config.broker_id))
            .unwrap_or(-1);
        // A Kafka follower opens every fetch as a full one at session epoch
        // 0; a leader that keeps no sessions answers each in full.
        let request = FetchRequest {
            replica_id: self.config.broker_id,
            max_wait_ms: FETCH_MAX_WAIT_MS,
            min_bytes: FETCH_MIN_BYTES,
            max_bytes: FETCH_MAX_BYTES,
            session_id: 0,
            session_epoch: 0,
            topics: topics
                .into_iter()
                .map(|(topic, (topic_id, partitions))| FetchTopic {
                    topic,
                    topic_id: WireUuid(topic_id.into_bytes()),
                    partitions,
                    ..FetchTopic::default()
                })
                .collect(),
            replica_state: ReplicaState {
                replica_id: self.config.broker_id,
                replica_epoch: broker_epoch,
                ..ReplicaState::default()
            },
            ..FetchRequest::default()
        };
        self.send_link_request(ctx, leader, FetchRequest::LATEST_STABLE_VERSION, &request);
    }

    /// A frame on one of this broker's links.
    pub(super) fn on_link_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        let Some(leader) = self
            .links
            .values()
            .find(|l| l.conn == frame.conn && broker_endpoint(l.leader) == frame.src)
            .map(|l| l.leader)
        else {
            return;
        };
        match frame.payload {
            Payload::Open => {}
            Payload::Close => self.disconnect_link(ctx, leader, false),
            Payload::Data(bytes) => self.on_link_response(ctx, leader, &bytes),
        }
    }

    fn on_link_response(&mut self, ctx: &mut Ctx<'_>, leader: i32, bytes: &Bytes) {
        let now = ctx.now();
        let Some(in_flight) = self.links.get(&leader).and_then(|l| l.in_flight) else {
            return;
        };
        let body = match parse_response_frame(bytes, in_flight.api_key, in_flight.version) {
            Ok((correlation, body)) if correlation == in_flight.correlation => body,
            Ok(_) | Err(_) => {
                // Kafka's `NetworkClient` closes a connection whose answer does
                // not match the request it waits on.
                self.link_failed(ctx, leader, "the response does not match the request");
                return;
            }
        };
        if let Some(link) = self.links.get_mut(&leader) {
            link.in_flight = None;
            link.earliest_send_at = now + 1;
            link.reconnect_backoff = RECONNECT_BACKOFF_MS;
        }
        let mut cursor: &[u8] = &body;
        let handled: Result<(), ProtocolError> = match in_flight.api_key {
            ApiKey::ApiVersions => {
                ApiVersionsResponse::decode(&mut cursor, in_flight.version).map(|resp| {
                    if resp.error_code == codes::NONE {
                        if let Some(link) = self.links.get_mut(&leader) {
                            link.handshaken = true;
                        }
                    } else {
                        self.link_failed(ctx, leader, "the ApiVersions handshake failed");
                    }
                })
            }
            ApiKey::ListOffsets => ListOffsetsResponse::decode(&mut cursor, in_flight.version)
                .map(|resp| self.on_list_offsets_response(ctx, leader, &resp)),
            ApiKey::Fetch => self.on_fetch_response(ctx, leader, in_flight.version, &body),
            _ => Ok(()),
        };
        if let Err(error) = handled {
            self.link_failed(
                ctx,
                leader,
                &format!("the response does not decode: {error}"),
            );
        }
    }

    /// Record why a link failed and reconnect it.
    fn link_failed(&mut self, ctx: &mut Ctx<'_>, leader: i32, reason: &str) {
        ctx.event(
            "fetch_error",
            json!({ "leader": leader, "reason": format!("{reason}; reconnecting"), "level": "error" }),
        );
        self.disconnect_link(ctx, leader, true);
    }

    fn follower_mut(&mut self, key: &TopicPartition, leader: i32) -> Option<&mut Replica> {
        let me = self.config.broker_id;
        self.replicas
            .get_mut(key)
            .filter(|r| r.leader == Some(leader) && !r.leads(me))
    }

    /// The leader's log end or start after `OFFSET_OUT_OF_RANGE`, Kafka's
    /// `fetchOffsetAndTruncate`: a follower ahead of the leader truncates to
    /// the leader's end; one behind the leader's start restarts from that
    /// start; any other goes back to fetching where it is.
    fn on_list_offsets_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: i32,
        resp: &ListOffsetsResponse,
    ) {
        let now = ctx.now();
        for topic in &resp.topics {
            for row in &topic.partitions {
                let key = TopicPartition::new(&topic.name, row.partition_index);
                let Some(replica) = self.follower_mut(&key, leader) else {
                    continue;
                };
                if row.error_code != codes::NONE || row.offset < 0 {
                    replica.fetch = FetchState::Delayed {
                        until: now + FETCH_BACKOFF_MS,
                    };
                    continue;
                }
                let log_end = replica.log.log_end_offset();
                match replica.fetch {
                    FetchState::ResetLatest if row.offset < log_end => {
                        truncate_follower(ctx, &key, replica, row.offset);
                        replica.fetch = FetchState::Fetching;
                    }
                    FetchState::ResetLatest => replica.fetch = FetchState::ResetEarliest,
                    FetchState::ResetEarliest => {
                        if row.offset > log_end {
                            replica.log.truncate_fully_and_start_at(row.offset);
                            ctx.event(
                                "replica_truncated",
                                json!({ "topic": key.topic, "partition": key.partition, "from": log_end, "to": row.offset, "restart": true, "level": "warn" }),
                            );
                        }
                        replica.fetch = FetchState::Fetching;
                    }
                    _ => {}
                }
            }
        }
    }

    /// A fetch answer, decoded with the borrowed codec so each partition's
    /// batches stay the bytes the leader sent.
    fn on_fetch_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: i32,
        version: i16,
        body: &Bytes,
    ) -> Result<(), ProtocolError> {
        let now = ctx.now();
        let mut cursor: &[u8] = body;
        let resp = FetchResponse::decode_borrow(&mut cursor, version)?;
        if resp.error_code != codes::NONE {
            for replica in self
                .replicas
                .values_mut()
                .filter(|r| r.leader == Some(leader))
            {
                replica.fetch = FetchState::Delayed {
                    until: now + FETCH_BACKOFF_MS,
                };
            }
            ctx.event(
                "fetch_error",
                json!({ "leader": leader, "error_code": resp.error_code, "level": "warn" }),
            );
            return Ok(());
        }
        let mut rows = Vec::new();
        for topic in &resp.responses {
            let name = if topic.topic.is_empty() {
                self.image
                    .topic_name_by_id(&Uuid::from_bytes(topic.topic_id.0))
                    .map(str::to_owned)
            } else {
                Some(topic.topic.to_string())
            };
            let Some(name) = name else {
                continue;
            };
            for row in &topic.partitions {
                let records = match &row.records {
                    Some(RecordsPayloadBorrowed::V2(batches)) => {
                        batch_span(body, batches).map_or_else(Bytes::new, |span| body.slice(span))
                    }
                    _ => Bytes::new(),
                };
                rows.push((
                    TopicPartition::new(&name, row.partition_index),
                    FetchedPartition {
                        error_code: row.error_code,
                        high_watermark: row.high_watermark,
                        log_start_offset: row.log_start_offset,
                        diverging_epoch: (
                            row.diverging_epoch.epoch,
                            row.diverging_epoch.end_offset,
                        ),
                        records,
                    },
                ));
            }
        }
        for (key, row) in &rows {
            self.on_fetch_row(ctx, leader, key, row);
        }
        Ok(())
    }

    /// Kafka's `AbstractFetcherThread.processFetchRequest` on one partition's
    /// answer.
    fn on_fetch_row(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: i32,
        key: &TopicPartition,
        row: &FetchedPartition,
    ) {
        let now = ctx.now();
        let Some(replica) = self.follower_mut(key, leader) else {
            return;
        };
        if replica.fetch != FetchState::Fetching {
            return;
        }
        match row.error_code {
            codes::NONE => {}
            codes::OFFSET_OUT_OF_RANGE => {
                replica.fetch = FetchState::ResetLatest;
                return;
            }
            code => {
                replica.fetch = FetchState::Delayed {
                    until: now + FETCH_BACKOFF_MS,
                };
                ctx.event(
                    "fetch_error",
                    json!({ "topic": key.topic, "partition": key.partition, "api": "Fetch", "error_code": code, "level": "warn" }),
                );
                return;
            }
        }
        // Kafka's `truncateOnFetchResponse`: truncate to where the logs
        // agree. When the leader named an epoch this log never had, the next
        // fetch carries the older epoch the truncation left, and the leader
        // answers about that one.
        let (epoch, end_offset) = row.diverging_epoch;
        if epoch >= 0 || end_offset >= 0 {
            let (target, _) = replica.log.truncation_target(epoch, end_offset);
            truncate_follower(ctx, key, replica, target);
            return;
        }
        // Kafka's fetcher backs a partition off after batches it cannot
        // append; the batches before the bad one stay appended.
        if !row.records.is_empty()
            && let Err(error) = replica.log.append_replicated(&row.records)
        {
            ctx.event(
                "fetch_error",
                json!({ "topic": key.topic, "partition": key.partition, "reason": error.to_string(), "level": "error" }),
            );
            replica.fetch = FetchState::Delayed {
                until: now + FETCH_BACKOFF_MS,
            };
            return;
        }
        replica.log.set_high_watermark(row.high_watermark);
        if row.log_start_offset > replica.log.log_start_offset() {
            replica.log.increment_log_start_offset(row.log_start_offset);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use krabka_metadata::LeaderEpoch;
    use krabka_protocol::records::{Record, RecordBatch, TimestampType};

    use super::*;
    use crate::lab::broker::log::AppendPolicy;

    fn record(leader: i32, isr: &[i32], epoch: i32) -> PartitionRecord {
        PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: if leader < 0 {
                cluster::NO_LEADER
            } else {
                cluster::meta_id(leader)
            },
            replicas: vec![
                cluster::meta_id(1),
                cluster::meta_id(2),
                cluster::meta_id(3),
            ],
            isr: isr.iter().map(|id| cluster::meta_id(*id)).collect(),
            leader_epoch: LeaderEpoch(epoch),
            partition_epoch: epoch,
            ..PartitionRecord::default()
        }
    }

    fn one_record_batch() -> Bytes {
        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            value: Some(Bytes::from_static(b"v")),
            ..Record::default()
        });
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();
        buf.freeze()
    }

    #[test]
    fn leader_tracks_followers_and_moves_the_high_watermark_over_the_isr() {
        let mut replica = Replica::new("t", 0, Uuid::from_u128(1));
        let change = replica.apply_record(&record(1, &[1, 2, 3], 0), 1, 0);
        assert!(
            change
                == ReplicaChange {
                    leader_changed: true,
                    epoch_changed: true,
                    isr_changed: true
                }
        );
        assert!(replica.leads(1));
        assert!(replica.log.epoch_cache() == [(0, 0)]);
        assert!(replica.followers.len() == 2);
        assert!(!replica.recompute_hwm(1));
        assert!(!replica.record_follower_fetch(2, 0, 0, 10));
        assert!(!replica.record_follower_fetch(3, 0, 0, 10));
        assert!(replica.log.high_watermark() == 0);
        assert!(replica.out_of_sync_followers(1, 20_000, 10_000).is_empty());
    }

    #[test]
    fn a_follower_behind_the_end_leaves_the_isr_after_the_lag_and_rejoins_when_caught_up() {
        let mut replica = Replica::new("t", 0, Uuid::from_u128(1));
        replica.apply_record(&record(1, &[1, 2], 0), 1, 0);
        let policy = AppendPolicy {
            timestamp_type: TimestampType::CreateTime,
            max_message_bytes: 1 << 20,
            compacted: false,
            timestamp_before_max_ms: i64::MAX,
            timestamp_after_max_ms: i64::MAX,
        };
        replica
            .log
            .append(&one_record_batch(), 0, 0, policy, "t-0")
            .unwrap();
        replica.record_follower_fetch(2, 0, 0, 100);
        // The first fetch is behind the end, so the follower stays caught up
        // as of the election at time 0.
        assert!(replica.follower_lag_ms(&replica.followers[&2], 5_000) == 5_000);
        assert!(replica.out_of_sync_followers(1, 5_000, 10_000).is_empty());
        assert!(replica.out_of_sync_followers(1, 10_101, 10_000) == vec![2]);
        replica.apply_record(&record(1, &[1], 0), 1, 10_101);
        assert!(replica.log.high_watermark() == 1);
        assert!(replica.record_follower_fetch(2, 1, 0, 10_200));
        assert!(replica.follower_lag_ms(&replica.followers[&2], 10_300) == 0);
        replica.apply_record(&record(1, &[1, 2], 0), 1, 10_200);
        assert!(replica.isr == vec![1, 2]);
        assert!(replica.followers[&2].log_end_offset == 1);
    }

    #[test]
    fn a_new_leader_or_epoch_sends_a_follower_back_to_truncation() {
        let mut replica = Replica::new("t", 0, Uuid::from_u128(1));
        replica.apply_record(&record(1, &[1, 2, 3], 0), 2, 0);
        assert!(replica.fetch == FetchState::Truncating);
        replica.fetch = FetchState::Fetching;
        let same = replica.apply_record(&record(1, &[1, 2], 0), 2, 5);
        assert!(
            same == ReplicaChange {
                leader_changed: false,
                epoch_changed: false,
                isr_changed: true
            }
        );
        assert!(replica.fetch == FetchState::Fetching);
        let moved = replica.apply_record(&record(3, &[3, 2], 1), 2, 6);
        assert!(moved.leader_changed && moved.epoch_changed);
        assert!(replica.fetch == FetchState::Truncating);
        let leaderless = replica.apply_record(&record(-1, &[3, 2], 1), 2, 7);
        assert!(leaderless.leader_changed);
        assert!(replica.fetch == FetchState::Idle && replica.leader.is_none());
        replica.apply_record(&record(2, &[2], 2), 2, 8);
        assert!(replica.leads(2));
        assert!(replica.log.epoch_cache() == [(2, 0)]);
    }

    #[test]
    fn batch_spans_locate_the_bytes_the_borrowed_decoder_read() {
        let one = one_record_batch();
        let mut run = BytesMut::from(&b"prefix"[..]);
        run.extend_from_slice(&one);
        run.extend_from_slice(&one);
        let buf = run.freeze();
        let mut cursor: &[u8] = &buf[6..];
        let payload = RecordsPayloadBorrowed::decode_borrow(&mut cursor, 0).unwrap();
        let RecordsPayloadBorrowed::V2(batches) = payload else {
            panic!("a v2 payload");
        };
        assert!(batch_span(&buf, &batches) == Some(6..buf.len()));
        assert!(batch_span(&buf, &batches[..1]) == Some(6..6 + one.len()));
        assert!(batch_span(&buf, &[]).is_none());
    }
}
