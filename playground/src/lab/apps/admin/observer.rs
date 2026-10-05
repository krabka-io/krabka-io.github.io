//! The admin node's cluster observer: every `observe_ms` it asks the cluster
//! what an operator's tools would, and keeps the last good answer of each
//! question for the snapshot's `cluster` field.
//!
//! A round runs in three phases, and a new round starts only once the last
//! one finished, so a slow cluster skips rounds instead of piling requests
//! up. Phase one sends `Metadata` (every topic, internal ones included),
//! `DescribeCluster`, `DescribeQuorum` for `__cluster_metadata-0` and
//! `ListPartitionReassignments`. `DescribeQuorum` goes to the broker
//! listener first, which forwards it to the active controller; when the
//! broker answers an error or does not speak the api, the observer asks a
//! voter's controller listener (port 9093) directly, and keeps asking there
//! while that works. Phase two, with the brokers and partition leaders of
//! phase one, sends per leader one `ListOffsets` for the latest offsets
//! (timestamp -1, `read_uncommitted`: the high watermark) and one for the
//! earliest (-2: the log start), and one `ListGroups` per broker (each lists
//! only the groups it coordinates). Phase three sends per group an
//! `OffsetFetch` and a describe to its coordinator: `ConsumerGroupDescribe`
//! for a KIP-848 group, `DescribeGroups` for a classic one (a streams or
//! share group is not described, so its member count stays `null`).
//!
//! A question that fails lands in `errors` as short text; the values it would
//! have replaced stay as the last good round left them.
//!
//! The observer runs its own client on connection lane 1 with the admin's
//! client id, and a request timeout of five rounds (2 to 30 s), so a broker
//! that went silent holds a round for seconds, not for Kafka's 30 s.

use std::collections::BTreeMap;

use base64::Engine as _;
use krabka_protocol::{
    owned::{
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_describe_response::ConsumerGroupDescribeResponse,
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::DescribeClusterResponse,
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::DescribeGroupsResponse,
        describe_quorum_request::{self, DescribeQuorumRequest},
        describe_quorum_response::DescribeQuorumResponse,
        list_groups_request::ListGroupsRequest,
        list_groups_response::ListGroupsResponse,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
        list_partition_reassignments_request::ListPartitionReassignmentsRequest,
        list_partition_reassignments_response::ListPartitionReassignmentsResponse,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
        offset_fetch_response::OffsetFetchResponse,
    },
    primitives::uuid::Uuid,
};
use serde_json::{Value, json};

use crate::lab::{
    client::{
        ClientError, ClientEvent, ClientOptions, CoordinatorType, KafkaClient, OffsetFetchByName,
        RequestId, Response, Target, conn_base,
    },
    codes,
    net::{Ctx, Endpoint, Frame, Millis, NodeId},
};

/// The port of a `KRaft` voter's controller listener (`CONTROLLER_PORT` of
/// `external.js`).
pub const CONTROLLER_PORT: u16 = 9093;
/// The `KRaft` metadata log `DescribeQuorum` asks about.
const METADATA_TOPIC: &str = "__cluster_metadata";
/// The `timeout_ms` of the observer's requests that carry one.
const TIMEOUT_MS: i32 = 5_000;

/// What one in-flight request of a round asks.
#[derive(Clone, Debug)]
enum Ask {
    Metadata,
    Cluster,
    /// `via` is the controller listener asked, or `None` for a broker.
    Quorum {
        via: Option<Endpoint>,
    },
    Reassignments,
    Groups {
        broker: i32,
    },
    Offsets {
        broker: i32,
        latest: bool,
    },
    GroupOffsets(String),
    DescribeGroup(String),
    DescribeConsumerGroup(String),
}

/// One partition as the last good answers gave it.
#[derive(Clone, Debug, Default)]
struct PartitionView {
    leader: i32,
    leader_epoch: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    offline: Vec<i32>,
    hwm: Option<i64>,
    log_start: Option<i64>,
}

#[derive(Clone, Debug)]
struct TopicView {
    id: Uuid,
    internal: bool,
    partitions: BTreeMap<i32, PartitionView>,
}

#[derive(Clone, Debug, Default)]
struct GroupView {
    state: String,
    /// `group_type` of `ListGroups`: classic, consumer, streams or share.
    kind: String,
    members: Option<usize>,
    committed: BTreeMap<(String, i32), i64>,
}

/// One broker: id, rack, and whether it is fenced (`None` before
/// `DescribeCluster` v2).
type BrokerRow = (i32, Option<String>, Option<bool>);

/// A partition with its log start and high watermark, when known.
pub type PartitionOffsets = (i32, Option<i64>, Option<i64>);

/// The cluster as the last good answers describe it.
#[derive(Clone, Debug, Default)]
pub struct View {
    cluster_id: Option<String>,
    /// From `DescribeCluster`, which knows fenced brokers.
    described: Option<Vec<BrokerRow>>,
    /// From `Metadata`, the fallback.
    listed: Vec<BrokerRow>,
    quorum: Option<Value>,
    controller: Option<i32>,
    topics: BTreeMap<String, TopicView>,
    groups: BTreeMap<String, GroupView>,
    reassignments: Vec<Value>,
}

impl View {
    /// The partitions of `topic` with their `(log_start, hwm)`.
    #[must_use]
    pub fn offsets_of(&self, topic: &str) -> Option<Vec<PartitionOffsets>> {
        self.topics.get(topic).map(|t| {
            t.partitions
                .iter()
                .map(|(p, v)| (*p, v.log_start, v.hwm))
                .collect()
        })
    }

    /// The member count of `group` the last describe gave.
    #[must_use]
    pub fn members_of(&self, group: &str) -> Option<usize> {
        self.groups.get(group).and_then(|g| g.members)
    }

    /// The topic and partitions of the cluster, to reassign or elect from.
    #[must_use]
    pub fn partitions_of(&self, topic: &str) -> Option<Vec<i32>> {
        self.topics
            .get(topic)
            .map(|t| t.partitions.keys().copied().collect())
    }
}

/// The observer. See the module documentation.
pub struct Observer {
    client: KafkaClient,
    every: Millis,
    /// When the next round may start.
    next_at: Millis,
    /// When the round in flight started.
    started: Millis,
    phase: u8,
    out: BTreeMap<RequestId, Ask>,
    /// The groups this round's `ListGroups` answers named, with their state,
    /// and whether one of those requests failed.
    listed_groups: BTreeMap<String, (String, String)>,
    groups_partial: bool,
    errors: Vec<String>,
    last_errors: Vec<String>,
    at: Option<Millis>,
    /// `DescribeQuorum` goes straight to a controller listener: the broker
    /// listener could not answer it.
    quorum_direct: bool,
    /// Counts the controller listeners tried, to move on from one that fails.
    quorum_tries: usize,
    view: View,
    rounds: u64,
}

impl Observer {
    #[must_use]
    pub fn new(bootstrap: &[NodeId], client_id: &str, every: Millis) -> Self {
        Self {
            client: Self::build_client(bootstrap, client_id, every),
            every,
            next_at: 0,
            started: 0,
            phase: 0,
            out: BTreeMap::new(),
            listed_groups: BTreeMap::new(),
            groups_partial: false,
            errors: Vec::new(),
            last_errors: Vec::new(),
            at: None,
            quorum_direct: false,
            quorum_tries: 0,
            view: View::default(),
            rounds: 0,
        }
    }

    fn build_client(bootstrap: &[NodeId], client_id: &str, every: Millis) -> KafkaClient {
        KafkaClient::new(
            bootstrap.iter().map(|n| Endpoint::kafka(*n)).collect(),
            client_id,
            ClientOptions {
                request_timeout_ms: (every * 5).clamp(2_000, 30_000),
                conn_base: conn_base(1),
                ..ClientOptions::default()
            },
        )
    }

    /// Start over after the node restarts: a new client, no round in
    /// flight, the last good view kept.
    pub fn restart(&mut self, bootstrap: &[NodeId]) {
        let id = self.client.client_id().to_string();
        self.client = Self::build_client(bootstrap, &id, self.every);
        self.out.clear();
        self.phase = 0;
        self.next_at = 0;
    }

    #[must_use]
    pub fn view(&self) -> &View {
        &self.view
    }

    /// Whether `frame` belongs to the observer's client.
    #[must_use]
    pub fn owns(&self, frame: &Frame) -> bool {
        self.client.owns_conn(frame.conn)
    }

    /// Run the next round as soon as the one in flight, if any, ends: a
    /// command changed the cluster.
    pub fn poke(&mut self, now: Millis) {
        if self.every > 0 {
            self.next_at = self.next_at.min(now);
        }
    }

    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        let events = self.client.on_frame(ctx, frame);
        self.drive(ctx, events);
    }

    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) {
        if self.every == 0 {
            return;
        }
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }

    /// When the observer needs a tick next.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        if self.every == 0 {
            return None;
        }
        let round = self.out.is_empty().then_some(self.next_at.max(now));
        self.client
            .next_deadline(now)
            .into_iter()
            .chain(round)
            .min()
    }

    fn drive(&mut self, ctx: &mut Ctx<'_>, events: Vec<ClientEvent>) {
        for event in events {
            if let ClientEvent::Response { id, result } = event {
                self.on_response(ctx, id, result);
            }
        }
        if self.out.is_empty() && self.phase == 0 && ctx.now() >= self.next_at {
            self.start_round(ctx);
        }
    }

    fn send<R>(&mut self, ctx: &mut Ctx<'_>, target: Target, ask: Ask, request: R)
    where
        R: krabka_protocol::ProtocolRequest + 'static,
        R::Response: 'static,
    {
        let id = self.client.send(ctx, target, request);
        self.out.insert(id, ask);
    }

    fn start_round(&mut self, ctx: &mut Ctx<'_>) {
        self.started = ctx.now();
        self.phase = 1;
        self.errors.clear();
        self.listed_groups.clear();
        self.groups_partial = false;
        self.send(
            ctx,
            Target::Any,
            Ask::Metadata,
            MetadataRequest {
                topics: None,
                allow_auto_topic_creation: false,
                ..Default::default()
            },
        );
        self.send(
            ctx,
            Target::Any,
            Ask::Cluster,
            DescribeClusterRequest {
                include_fenced_brokers: true,
                endpoint_type: 1,
                ..Default::default()
            },
        );
        if self.quorum_direct {
            self.ask_controller(ctx);
        } else {
            self.send(
                ctx,
                Target::Any,
                Ask::Quorum { via: None },
                quorum_request(),
            );
        }
        self.send(
            ctx,
            Target::Controller,
            Ask::Reassignments,
            ListPartitionReassignmentsRequest {
                timeout_ms: TIMEOUT_MS,
                topics: None,
                ..Default::default()
            },
        );
    }

    /// Ask a voter's controller listener for `DescribeQuorum`: the quorum
    /// leader the last answer named, else the brokers in turn.
    fn ask_controller(&mut self, ctx: &mut Ctx<'_>) {
        let mut ids: Vec<i32> = self.client.metadata().brokers.keys().copied().collect();
        if ids.is_empty() {
            ids = self.view.listed.iter().map(|b| b.0).collect();
        }
        let pick = self
            .view
            .controller
            .filter(|_| self.quorum_tries == 0)
            .or_else(|| (!ids.is_empty()).then(|| ids[self.quorum_tries % ids.len()]));
        let Some(broker) = pick else {
            self.errors
                .push("DescribeQuorum: no broker known to ask".to_string());
            return;
        };
        let node = self
            .client
            .metadata()
            .broker_endpoint(broker)
            .map_or_else(|| NodeId(u32::try_from(broker).unwrap_or(0)), |e| e.node);
        let via = Endpoint::new(node, CONTROLLER_PORT);
        self.send(
            ctx,
            Target::Endpoint(via),
            Ask::Quorum { via: Some(via) },
            quorum_request(),
        );
    }

    fn on_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: RequestId,
        result: Result<Response, ClientError>,
    ) {
        let Some(ask) = self.out.remove(&id) else {
            return;
        };
        match result {
            Ok(response) => self.on_answer(ctx, ask, response),
            Err(error) => self.on_failure(ctx, ask, &error),
        }
        if self.out.is_empty() {
            self.end_phase(ctx);
        }
    }

    fn on_failure(&mut self, ctx: &mut Ctx<'_>, ask: Ask, error: &ClientError) {
        match ask {
            Ask::Quorum { via: None } => {
                self.errors
                    .push(format!("DescribeQuorum on the broker listener: {error}"));
                self.ask_controller(ctx);
            }
            Ask::Quorum { via: Some(via) } => {
                self.quorum_tries += 1;
                self.quorum_direct = false;
                self.errors
                    .push(format!("DescribeQuorum at {via}: {error}"));
            }
            Ask::Groups { .. } => {
                self.groups_partial = true;
                self.errors.push(format!("{}: {error}", ask_name(&ask)));
            }
            other => self.errors.push(format!("{}: {error}", ask_name(&other))),
        }
    }

    fn on_answer(&mut self, ctx: &mut Ctx<'_>, ask: Ask, response: Response) {
        let name = ask_name(&ask);
        let failed = match ask {
            Ask::Metadata => response
                .downcast::<MetadataResponse>()
                .map(|r| self.on_metadata(&r)),
            Ask::Cluster => response
                .downcast::<DescribeClusterResponse>()
                .map(|r| self.on_cluster(r)),
            Ask::Quorum { via } => response
                .downcast::<DescribeQuorumResponse>()
                .map(|r| self.on_quorum(ctx, via, &r)),
            Ask::Reassignments => response
                .downcast::<ListPartitionReassignmentsResponse>()
                .map(|r| self.on_reassignments(&r)),
            Ask::Groups { .. } => response
                .downcast::<ListGroupsResponse>()
                .map(|r| self.on_groups(r)),
            Ask::Offsets { latest, .. } => response
                .downcast::<ListOffsetsResponse>()
                .map(|r| self.on_offsets(latest, &r)),
            Ask::GroupOffsets(group) => response
                .downcast::<OffsetFetchResponse>()
                .map(|r| self.on_group_offsets(&group, r)),
            Ask::DescribeGroup(group) => response.downcast::<DescribeGroupsResponse>().map(|r| {
                let found = r.groups.into_iter().find(|g| g.group_id == group);
                self.on_described(
                    &group,
                    found.map(|g| (g.error_code, g.group_state, g.members.len())),
                )
            }),
            Ask::DescribeConsumerGroup(group) => response
                .downcast::<ConsumerGroupDescribeResponse>()
                .map(|r| {
                    let found = r.groups.into_iter().find(|g| g.group_id == group);
                    self.on_described(
                        &group,
                        found.map(|g| (g.error_code, g.group_state, g.members.len())),
                    )
                }),
        };
        match failed {
            Some(Some(problem)) => self.errors.push(format!("{name}: {problem}")),
            Some(None) => {}
            None => self.errors.push(format!("{name}: unexpected answer")),
        }
    }

    // ---- answers: each returns the problem it met, if any ---------------------

    fn on_metadata(&mut self, response: &MetadataResponse) -> Option<String> {
        if let Some(id) = &response.cluster_id {
            self.view.cluster_id = Some(id.clone());
        }
        self.view.listed = response
            .brokers
            .iter()
            .map(|b| (b.node_id, b.rack.clone(), None))
            .collect();
        let mut problems = Vec::new();
        let mut topics = BTreeMap::new();
        for topic in &response.topics {
            let Some(name) = topic.name.clone() else {
                continue;
            };
            if topic.error_code != codes::NONE {
                problems.push(format!("{name} error {}", topic.error_code));
                // Keep what the last good answer said of the topic.
                if let Some(old) = self.view.topics.remove(&name) {
                    topics.insert(name, old);
                }
                continue;
            }
            let old = self.view.topics.remove(&name);
            let partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    let before = old
                        .as_ref()
                        .and_then(|t| t.partitions.get(&p.partition_index));
                    (
                        p.partition_index,
                        PartitionView {
                            leader: p.leader_id,
                            leader_epoch: p.leader_epoch,
                            replicas: p.replica_nodes.clone(),
                            isr: p.isr_nodes.clone(),
                            offline: p.offline_replicas.clone(),
                            hwm: before.and_then(|b| b.hwm),
                            log_start: before.and_then(|b| b.log_start),
                        },
                    )
                })
                .collect();
            topics.insert(
                name,
                TopicView {
                    id: topic.topic_id,
                    internal: topic.is_internal,
                    partitions,
                },
            );
        }
        self.view.topics = topics;
        (!problems.is_empty()).then(|| problems.join(", "))
    }

    fn on_cluster(&mut self, response: DescribeClusterResponse) -> Option<String> {
        if response.error_code != codes::NONE {
            return Some(error_text(response.error_code, response.error_message));
        }
        self.view.cluster_id = Some(response.cluster_id);
        self.view.described = Some(
            response
                .brokers
                .into_iter()
                .map(|b| (b.broker_id, b.rack, Some(b.is_fenced)))
                .collect(),
        );
        None
    }

    fn on_quorum(
        &mut self,
        ctx: &mut Ctx<'_>,
        via: Option<Endpoint>,
        response: &DescribeQuorumResponse,
    ) -> Option<String> {
        let partition = response
            .topics
            .iter()
            .find(|t| t.topic_name == METADATA_TOPIC)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == 0));
        let code = match partition {
            _ if response.error_code != codes::NONE => response.error_code,
            Some(p) => p.error_code,
            None => codes::UNKNOWN_TOPIC_OR_PARTITION,
        };
        let Some(p) = partition.filter(|_| code == codes::NONE) else {
            let message = response
                .error_message
                .clone()
                .or_else(|| partition.and_then(|p| p.error_message.clone()));
            if via.is_none() {
                self.ask_controller(ctx);
            } else {
                self.quorum_tries += 1;
                self.quorum_direct = false;
            }
            return Some(error_text(code, message));
        };
        if via.is_some() {
            self.quorum_direct = true;
        }
        self.quorum_tries = 0;
        let leader_end = p
            .current_voters
            .iter()
            .find(|r| r.replica_id == p.leader_id)
            .map_or(p.high_watermark, |r| r.log_end_offset);
        let rows = |replicas: &[krabka_protocol::owned::common::describe_quorum_response::replica_state::ReplicaState]| -> Vec<Value> {
            replicas
                .iter()
                .map(|r| {
                    json!({
                        "id": r.replica_id,
                        "log_end_offset": r.log_end_offset,
                        "lag": (leader_end - r.log_end_offset).max(0),
                    })
                })
                .collect()
        };
        self.view.controller = (p.leader_id >= 0).then_some(p.leader_id);
        self.view.quorum = Some(json!({
            "leader": self.view.controller,
            "epoch": p.leader_epoch,
            "high_watermark": p.high_watermark,
            "voters": rows(&p.current_voters),
            "observers": rows(&p.observers),
        }));
        None
    }

    fn on_reassignments(
        &mut self,
        response: &ListPartitionReassignmentsResponse,
    ) -> Option<String> {
        if response.error_code != codes::NONE {
            return Some(error_text(
                response.error_code,
                response.error_message.clone(),
            ));
        }
        self.view.reassignments = response
            .topics
            .iter()
            .flat_map(|t| {
                t.partitions.iter().map(|p| {
                    json!({
                        "topic": t.name,
                        "partition": p.partition_index,
                        "replicas": p.replicas,
                        "adding": p.adding_replicas,
                        "removing": p.removing_replicas,
                    })
                })
            })
            .collect();
        None
    }

    fn on_groups(&mut self, response: ListGroupsResponse) -> Option<String> {
        if response.error_code != codes::NONE {
            self.groups_partial = true;
            return Some(error_text(response.error_code, None));
        }
        for group in response.groups {
            self.listed_groups
                .insert(group.group_id, (group.group_state, group.group_type));
        }
        None
    }

    fn on_offsets(&mut self, latest: bool, response: &ListOffsetsResponse) -> Option<String> {
        let mut problems = Vec::new();
        for topic in &response.topics {
            for p in &topic.partitions {
                if p.error_code != codes::NONE {
                    problems.push(format!(
                        "{}-{} error {}",
                        topic.name, p.partition_index, p.error_code
                    ));
                    continue;
                }
                if let Some(view) = self
                    .view
                    .topics
                    .get_mut(&topic.name)
                    .and_then(|t| t.partitions.get_mut(&p.partition_index))
                {
                    if latest {
                        view.hwm = Some(p.offset);
                    } else {
                        view.log_start = Some(p.offset);
                    }
                }
            }
        }
        (!problems.is_empty()).then(|| problems.join(", "))
    }

    fn on_group_offsets(&mut self, group: &str, response: OffsetFetchResponse) -> Option<String> {
        // The batched form (v8 and later) answers in `groups`, the older
        // one at the top level.
        let (code, rows): (i16, Vec<(String, i32, i64)>) =
            match response.groups.into_iter().find(|g| g.group_id == group) {
                Some(g) => (
                    g.error_code,
                    g.topics
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions
                                .into_iter()
                                .map(move |p| (name.clone(), p.partition_index, p.committed_offset))
                        })
                        .collect(),
                ),
                None => (
                    response.error_code,
                    response
                        .topics
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions
                                .into_iter()
                                .map(move |p| (name.clone(), p.partition_index, p.committed_offset))
                        })
                        .collect(),
                ),
            };
        if code != codes::NONE {
            return Some(format!("{group} error {code}"));
        }
        if let Some(view) = self.view.groups.get_mut(group) {
            view.committed = rows
                .into_iter()
                .filter(|(_, _, offset)| *offset >= 0)
                .map(|(topic, partition, offset)| ((topic, partition), offset))
                .collect();
        }
        None
    }

    /// A group's describe: its `(error, state, member count)`.
    fn on_described(
        &mut self,
        group: &str,
        described: Option<(i16, String, usize)>,
    ) -> Option<String> {
        let (code, state, members) = described?;
        if code != codes::NONE {
            return Some(format!("{group} error {code}"));
        }
        if let Some(view) = self.view.groups.get_mut(group) {
            view.members = Some(members);
            if !state.is_empty() {
                view.state = state;
            }
        }
        None
    }

    // ---- phases -------------------------------------------------------------------

    fn end_phase(&mut self, ctx: &mut Ctx<'_>) {
        loop {
            match self.phase {
                1 => {
                    self.phase = 2;
                    self.start_phase_two(ctx);
                }
                2 => {
                    self.phase = 3;
                    self.merge_groups();
                    self.start_phase_three(ctx);
                }
                _ => break,
            }
            if !self.out.is_empty() {
                return;
            }
        }
        // The round is over.
        let now = ctx.now();
        self.phase = 0;
        self.rounds += 1;
        self.at = Some(now);
        self.last_errors = std::mem::take(&mut self.errors);
        // A round that overran its period skips the rounds it overlapped.
        let periods = (now - self.started) / self.every.max(1) + 1;
        self.next_at = self.started + periods * self.every.max(1);
    }

    /// The groups of this round: the ones the `ListGroups` answers named,
    /// and, when one of those failed, the ones known before as well.
    fn merge_groups(&mut self) {
        let mut groups = BTreeMap::new();
        for (id, (state, kind)) in std::mem::take(&mut self.listed_groups) {
            let mut view = self.view.groups.remove(&id).unwrap_or_default();
            view.state = state;
            view.kind = kind;
            groups.insert(id, view);
        }
        if self.groups_partial {
            groups.append(&mut self.view.groups);
        }
        self.view.groups = groups;
    }

    fn start_phase_two(&mut self, ctx: &mut Ctx<'_>) {
        let mut by_leader: BTreeMap<i32, BTreeMap<String, Vec<(i32, i32)>>> = BTreeMap::new();
        for (name, topic) in &self.view.topics {
            for (index, p) in &topic.partitions {
                if p.leader >= 0 {
                    by_leader
                        .entry(p.leader)
                        .or_default()
                        .entry(name.clone())
                        .or_default()
                        .push((*index, p.leader_epoch));
                }
            }
        }
        for (broker, topics) in by_leader {
            for (latest, timestamp) in [(true, -1), (false, -2)] {
                let request = ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: 0,
                    timeout_ms: TIMEOUT_MS,
                    topics: topics
                        .iter()
                        .map(|(name, partitions)| ListOffsetsTopic {
                            name: name.clone(),
                            partitions: partitions
                                .iter()
                                .map(|(index, epoch)| ListOffsetsPartition {
                                    partition_index: *index,
                                    current_leader_epoch: *epoch,
                                    timestamp,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                self.send(
                    ctx,
                    Target::Broker(broker),
                    Ask::Offsets { broker, latest },
                    request,
                );
            }
        }
        // Each broker lists the groups it coordinates.
        let brokers: Vec<i32> = self.view.listed.iter().map(|b| b.0).collect();
        if brokers.is_empty() {
            self.groups_partial = true;
        }
        for broker in brokers {
            self.send(
                ctx,
                Target::Broker(broker),
                Ask::Groups { broker },
                ListGroupsRequest::default(),
            );
        }
    }

    fn start_phase_three(&mut self, ctx: &mut Ctx<'_>) {
        let partitions: Vec<(String, Vec<i32>)> = self
            .view
            .topics
            .iter()
            .filter(|(_, t)| !t.internal)
            .map(|(name, t)| (name.clone(), t.partitions.keys().copied().collect()))
            .collect();
        let groups: Vec<(String, String)> = self
            .view
            .groups
            .iter()
            .map(|(id, g)| (id.clone(), g.kind.clone()))
            .collect();
        for (group, kind) in groups {
            let target = Target::Coordinator {
                key_type: CoordinatorType::Group,
                key: group.clone(),
            };
            if !partitions.is_empty() {
                self.send(
                    ctx,
                    target.clone(),
                    Ask::GroupOffsets(group.clone()),
                    offset_fetch(&group, &partitions),
                );
            }
            match kind.as_str() {
                "consumer" => self.send(
                    ctx,
                    target,
                    Ask::DescribeConsumerGroup(group.clone()),
                    ConsumerGroupDescribeRequest {
                        group_ids: vec![group],
                        ..Default::default()
                    },
                ),
                "" | "classic" => self.send(
                    ctx,
                    target,
                    Ask::DescribeGroup(group.clone()),
                    DescribeGroupsRequest {
                        groups: vec![group],
                        ..Default::default()
                    },
                ),
                _ => {}
            }
        }
    }

    /// The snapshot's `cluster` field, or `null` before the first round.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let Some(at) = self.at else {
            return Value::Null;
        };
        let view = &self.view;
        let brokers: Vec<Value> = view
            .described
            .as_ref()
            .unwrap_or(&view.listed)
            .iter()
            .map(|(id, rack, fenced)| json!({ "id": id, "rack": rack, "fenced": fenced }))
            .collect();
        let topics: Vec<Value> = view
            .topics
            .iter()
            .map(|(name, t)| {
                let partitions: Vec<Value> = t
                    .partitions
                    .iter()
                    .map(|(index, p)| {
                        json!({
                            "partition": index,
                            "leader": p.leader,
                            "leader_epoch": p.leader_epoch,
                            "replicas": p.replicas,
                            "isr": p.isr,
                            "offline": p.offline,
                            "hwm": p.hwm,
                            "log_start": p.log_start,
                        })
                    })
                    .collect();
                json!({
                    "name": name,
                    "id": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(t.id.0),
                    "internal": t.internal,
                    "partitions": partitions,
                })
            })
            .collect();
        let groups: Vec<Value> = view
            .groups
            .iter()
            .map(|(id, g)| {
                let offsets: Vec<(Value, Option<i64>)> = g
                    .committed
                    .iter()
                    .map(|((topic, partition), committed)| {
                        let hwm = view
                            .topics
                            .get(topic)
                            .and_then(|t| t.partitions.get(partition))
                            .and_then(|p| p.hwm);
                        let lag = hwm.map(|hwm| (hwm - committed).max(0));
                        (
                            json!({
                                "topic": topic,
                                "partition": partition,
                                "committed": committed,
                                "lag": lag,
                            }),
                            lag,
                        )
                    })
                    .collect();
                let lag: Option<i64> = offsets.iter().map(|(_, lag)| *lag).sum();
                json!({
                    "id": id,
                    "state": g.state,
                    "members": g.members,
                    "lag": lag,
                    "offsets": offsets.into_iter().map(|(v, _)| v).collect::<Vec<_>>(),
                })
            })
            .collect();
        json!({
            "at": at,
            "cluster_id": view.cluster_id,
            "brokers": brokers,
            "controller": view.controller,
            "quorum": view.quorum,
            "topics": topics,
            "groups": groups,
            "errors": self.last_errors,
            "rounds": self.rounds,
        })
    }

    /// The snapshot's `reassignments` field.
    #[must_use]
    pub fn reassignments(&self) -> Value {
        Value::from(self.view.reassignments.clone())
    }
}

fn quorum_request() -> DescribeQuorumRequest {
    DescribeQuorumRequest {
        topics: vec![describe_quorum_request::TopicData {
            topic_name: METADATA_TOPIC.to_string(),
            partitions: vec![describe_quorum_request::PartitionData {
                partition_index: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// An `OffsetFetch` for every partition of `partitions`, in both the batched
/// form and the older top-level one, capped at the last version that names
/// topics.
fn offset_fetch(group: &str, partitions: &[(String, Vec<i32>)]) -> OffsetFetchByName {
    OffsetFetchByName(OffsetFetchRequest {
        group_id: group.to_string(),
        topics: Some(
            partitions
                .iter()
                .map(|(name, indexes)| OffsetFetchRequestTopic {
                    name: name.clone(),
                    partition_indexes: indexes.clone(),
                    ..Default::default()
                })
                .collect(),
        ),
        groups: vec![OffsetFetchRequestGroup {
            group_id: group.to_string(),
            member_epoch: -1,
            topics: Some(
                partitions
                    .iter()
                    .map(|(name, indexes)| OffsetFetchRequestTopics {
                        name: name.clone(),
                        partition_indexes: indexes.clone(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }],
        ..Default::default()
    })
}

fn ask_name(ask: &Ask) -> String {
    match ask {
        Ask::Metadata => "Metadata".to_string(),
        Ask::Cluster => "DescribeCluster".to_string(),
        Ask::Quorum { .. } => "DescribeQuorum".to_string(),
        Ask::Reassignments => "ListPartitionReassignments".to_string(),
        Ask::Groups { broker } => format!("ListGroups from broker {broker}"),
        Ask::Offsets { broker, latest } => format!(
            "ListOffsets {} from broker {broker}",
            if *latest { "latest" } else { "earliest" }
        ),
        Ask::GroupOffsets(g) => format!("OffsetFetch {g}"),
        Ask::DescribeGroup(g) => format!("DescribeGroups {g}"),
        Ask::DescribeConsumerGroup(g) => format!("ConsumerGroupDescribe {g}"),
    }
}

/// `error <code>`, with the broker's message when it gave one.
#[must_use]
pub fn error_text(code: i16, message: Option<String>) -> String {
    match message.filter(|m| !m.is_empty()) {
        Some(m) => format!("error {code}: {m}"),
        None => format!("error {code}"),
    }
}
