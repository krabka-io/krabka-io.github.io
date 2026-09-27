//! The primary election among registry instances: Confluent's
//! `KafkaGroupLeaderElector` over its `SchemaRegistryCoordinator`.
//!
//! Every instance is a member of the classic group
//! `schema.registry.group.id` (default `schema-registry`), with protocol
//! type `sr` and one protocol, `v0`, whose metadata is the instance's
//! [`Identity`] as Confluent's JSON. The [`Elector`] runs Kafka's
//! `AbstractCoordinator` for it over a lab client of its own:
//!
//! - `JoinGroup` with `kafkagroup.session.timeout.ms` and
//!   `kafkagroup.rebalance.timeout.ms`, again at once with the member id a
//!   `MEMBER_ID_REQUIRED` answer hands out;
//! - the member the coordinator makes the group's leader computes the
//!   assignment ([`assign`]): the leader-eligible member with the smallest
//!   URL is the primary, and every member receives the same [`Assignment`];
//! - `SyncGroup`, then a `Heartbeat` every `kafkagroup.heartbeat.interval.ms`
//!   whether or not the last one was answered; a session without a
//!   successful heartbeat answer forgets the coordinator, and
//!   `REBALANCE_IN_PROGRESS`, `ILLEGAL_GENERATION` and `UNKNOWN_MEMBER_ID`
//!   join again.
//!
//! Each completed generation is an [`ElectionEvent::Assigned`], Confluent's
//! `onAssigned`. Each later join starts with an [`ElectionEvent::Revoked`],
//! its `onRevoked`, after which the primary is unknown until the next
//! assignment: Confluent's default, `leader.election.sticky=false`.

use bytes::Bytes;
use krabka_protocol::owned::{
    heartbeat_request::HeartbeatRequest,
    heartbeat_response::HeartbeatResponse,
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    join_group_response::JoinGroupResponse,
    sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
    sync_group_response::SyncGroupResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::lab::{
    client::{
        ClientError, ClientEvent, CoordinatorType, KafkaClient, RequestId, Response, Target,
        exponential_backoff,
    },
    codes,
    net::{Ctx, Frame, Millis, NodeId, node_for_ip},
    registry::lane::Lane,
};

/// The protocol type of the group: `SchemaRegistryCoordinator.protocolType`.
pub const PROTOCOL_TYPE: &str = "sr";
/// The one protocol a member offers: `SR_SUBPROTOCOL_V0`.
pub const PROTOCOL: &str = "v0";
/// `SchemaRegistryIdentity.CURRENT_VERSION` and
/// `Assignment.CURRENT_VERSION`.
const VERSION: i32 = 1;
/// The assignment error of a group whose members share a URL.
pub const DUPLICATE_URLS: i16 = 1;
/// `retry.backoff.ms` of the member's client.
const RETRY_BACKOFF_MS: Millis = 100;
/// `retry.backoff.max.ms` of the member's client.
const RETRY_BACKOFF_MAX_MS: Millis = 1_000;

/// How an instance advertises itself to the group: Confluent's
/// `SchemaRegistryIdentity`. Two identities are the same instance when all
/// four fields agree.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Identity {
    /// `host.name`: `node-<id>` in the lab.
    pub host: String,
    /// The port of the REST listener.
    pub port: u16,
    /// `leader.eligibility`: whether the instance may be the primary.
    pub eligible: bool,
    /// The scheme of the REST listener.
    pub scheme: String,
}

/// The JSON of an identity, in the order Confluent's Jackson mapper writes
/// it: the creator's properties first, then the rest.
#[derive(Serialize, Deserialize)]
struct IdentityJson {
    host: String,
    port: u16,
    master_eligibility: bool,
    scheme: String,
    version: i32,
    leader: bool,
}

impl Identity {
    /// The identity of the lab registry on `node`.
    #[must_use]
    pub fn of_node(node: NodeId, port: u16, eligible: bool) -> Self {
        Self {
            host: format!("node-{}", node.0),
            port,
            eligible,
            scheme: "http".to_string(),
        }
    }

    /// `getUrl`: `scheme://host:port`, which orders the candidates.
    #[must_use]
    pub fn url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }

    /// The lab node the host names: `node-<id>`, or a virtual address.
    #[must_use]
    pub fn node(&self) -> Option<NodeId> {
        self.host
            .strip_prefix("node-")
            .and_then(|id| id.parse().ok())
            .map(NodeId)
            .or_else(|| self.host.parse().ok().and_then(node_for_ip))
    }

    fn to_json(&self) -> IdentityJson {
        IdentityJson {
            host: self.host.clone(),
            port: self.port,
            master_eligibility: self.eligible,
            scheme: self.scheme.clone(),
            version: VERSION,
            leader: false,
        }
    }

    fn from_json(json: IdentityJson) -> Self {
        Self {
            host: json.host,
            port: json.port,
            eligible: json.master_eligibility,
            scheme: json.scheme,
        }
    }

    /// The member metadata: `SchemaRegistryProtocol.serializeMetadata`.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(&self.to_json()).unwrap_or_default())
    }

    /// The identity in a member's metadata, if it is one.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok().map(Self::from_json)
    }
}

/// What every member of a generation receives: Confluent's
/// `SchemaRegistryProtocol.Assignment`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Assignment {
    /// `0`, or [`DUPLICATE_URLS`].
    pub error: i16,
    /// The member id of the primary, if any member may lead.
    pub leader: Option<String>,
    /// The identity of the primary.
    pub leader_identity: Option<Identity>,
}

/// The JSON of an assignment, in Jackson's order; the absent primary is
/// left out, as `@JsonInclude(NON_EMPTY)` does.
#[derive(Serialize, Deserialize)]
struct AssignmentJson {
    error: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    master: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    master_identity: Option<IdentityJson>,
    version: i32,
}

impl Assignment {
    /// The member assignment: `SchemaRegistryProtocol.serializeAssignment`.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let json = AssignmentJson {
            error: self.error,
            master: self.leader.clone(),
            master_identity: self.leader_identity.as_ref().map(Identity::to_json),
            version: VERSION,
        };
        Bytes::from(serde_json::to_vec(&json).unwrap_or_default())
    }

    /// The assignment a `SyncGroup` answer carries, if it is one.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let json: AssignmentJson = serde_json::from_slice(bytes).ok()?;
        Some(Self {
            error: json.error,
            leader: json.master,
            leader_identity: json.master_identity.map(Identity::from_json),
        })
    }
}

/// The assignment the group's leader computes from its members, each with
/// the identity its metadata holds: Confluent's `onLeaderElected`. The
/// leader-eligible member with the smallest URL is the primary; members
/// that share a URL make it [`DUPLICATE_URLS`].
#[must_use]
pub fn assign(members: &[(String, Option<Identity>)]) -> Assignment {
    let mut leader: Option<(&str, &Identity)> = None;
    let mut urls = std::collections::BTreeSet::new();
    let identified: Vec<(&str, &Identity)> = members
        .iter()
        .filter_map(|(member, identity)| identity.as_ref().map(|i| (member.as_str(), i)))
        .collect();
    for (member, identity) in &identified {
        urls.insert(identity.url());
        let smaller = leader.is_none_or(|(_, current)| identity.url() < current.url());
        if identity.eligible && smaller {
            leader = Some((member, identity));
        }
    }
    Assignment {
        error: if urls.len() == identified.len() {
            codes::NONE
        } else {
            DUPLICATE_URLS
        },
        leader: leader.map(|(member, _)| member.to_string()),
        leader_identity: leader.map(|(_, identity)| identity.clone()),
    }
}

/// The group member's settings: Confluent's `schema.registry.group.id` and
/// `kafkagroup.*`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ElectorConfig {
    /// `schema.registry.group.id`. Default: `schema-registry`.
    pub group_id: String,
    /// `kafkagroup.session.timeout.ms`. Default: 10 000.
    pub session_timeout_ms: Millis,
    /// `kafkagroup.heartbeat.interval.ms`. Default: 3 000.
    pub heartbeat_interval_ms: Millis,
    /// `kafkagroup.rebalance.timeout.ms`. Default: 300 000.
    pub rebalance_timeout_ms: Millis,
}

impl Default for ElectorConfig {
    fn default() -> Self {
        Self {
            group_id: "schema-registry".to_string(),
            session_timeout_ms: 10_000,
            heartbeat_interval_ms: 3_000,
            rebalance_timeout_ms: 300_000,
        }
    }
}

/// What the member tells the registry.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ElectionEvent {
    /// A rebalance began: Confluent's `onRevoked`. The primary is unknown
    /// until the next assignment.
    Revoked,
    /// A generation completed: Confluent's `onAssigned`.
    Assigned {
        /// The generation.
        generation: i32,
        /// What every member received.
        assignment: Assignment,
    },
}

/// Where the member is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    /// A `JoinGroup` is due or in flight.
    Joining,
    /// Joined; the `SyncGroup` is in flight.
    Syncing,
    /// Assigned; heartbeats keep the session.
    Stable,
}

impl Phase {
    const fn name(self) -> &'static str {
        match self {
            Self::Joining => "joining",
            Self::Syncing => "syncing",
            Self::Stable => "stable",
        }
    }
}

/// The member of the `schema-registry` group. See the module documentation.
pub struct Elector {
    lane: Lane<KafkaClient>,
    config: ElectorConfig,
    identity: Identity,
    phase: Phase,
    member_id: String,
    generation: i32,
    join: Option<RequestId>,
    sync: Option<RequestId>,
    heartbeats: Vec<RequestId>,
    rejoin_at: Millis,
    attempts: u32,
    next_heartbeat_at: Millis,
    /// When the last heartbeat succeeded, or the session began.
    session_from: Millis,
    /// Confluent's `assignmentSnapshot`.
    assignment: Option<Assignment>,
    /// A join after a completed one revokes first.
    needs_join_prepare: bool,
    joins: u64,
    last_error: Option<i16>,
}

impl Elector {
    /// A member for `identity` over the client on `lane`; it joins at its
    /// first tick.
    #[must_use]
    pub fn new(lane: Lane<KafkaClient>, config: ElectorConfig, identity: Identity) -> Self {
        Self {
            lane,
            config,
            identity,
            phase: Phase::Joining,
            member_id: String::new(),
            generation: -1,
            join: None,
            sync: None,
            heartbeats: Vec::new(),
            rejoin_at: 0,
            attempts: 0,
            next_heartbeat_at: 0,
            session_from: 0,
            assignment: None,
            needs_join_prepare: true,
            joins: 0,
            last_error: None,
        }
    }

    /// The lane of the member's client, for routing frames.
    #[must_use]
    pub fn lane(&self) -> &Lane<KafkaClient> {
        &self.lane
    }

    /// A frame for the member's client.
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> Vec<ElectionEvent> {
        let frame = self.lane.inbound(frame);
        let events = self
            .lane
            .run(ctx, |client, ctx| client.on_frame(ctx, frame));
        let mut out = Vec::new();
        self.on_client_events(ctx, events, &mut out);
        self.drive(ctx, &mut out);
        out
    }

    /// The timer fired: drive the client and the membership.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> Vec<ElectionEvent> {
        let (events, _) = self.lane.run(ctx, KafkaClient::on_tick);
        let mut out = Vec::new();
        self.on_client_events(ctx, events, &mut out);
        self.drive(ctx, &mut out);
        out
    }

    /// Close the client. The member does not leave: the coordinator expires
    /// it once its session runs out.
    pub fn close(&mut self, ctx: &mut Ctx<'_>) {
        self.lane.run(ctx, KafkaClient::close);
        self.join = None;
        self.sync = None;
        self.heartbeats.clear();
    }

    fn coordinator(&self) -> Target {
        Target::Coordinator {
            key_type: CoordinatorType::Group,
            key: self.config.group_id.clone(),
        }
    }

    fn send<R>(&mut self, ctx: &mut Ctx<'_>, request: R) -> RequestId
    where
        R: krabka_protocol::ProtocolRequest + 'static,
        R::Response: 'static,
    {
        let target = self.coordinator();
        self.lane
            .run(ctx, |client, ctx| client.send(ctx, target, request))
    }

    /// Send what is due: the join, or the heartbeat; and forget the
    /// coordinator when the session ran out without a heartbeat answer.
    fn drive(&mut self, ctx: &mut Ctx<'_>, out: &mut Vec<ElectionEvent>) {
        let now = ctx.now();
        match self.phase {
            Phase::Joining if self.join.is_none() && now >= self.rejoin_at => {
                if self.needs_join_prepare {
                    self.needs_join_prepare = false;
                    if self.assignment.is_some() {
                        out.push(ElectionEvent::Revoked);
                    }
                }
                self.send_join(ctx);
            }
            Phase::Stable => {
                if now >= self.session_from + self.config.session_timeout_ms {
                    self.forget_coordinator();
                    self.session_from = now;
                }
                if now >= self.next_heartbeat_at {
                    let request = HeartbeatRequest {
                        group_id: self.config.group_id.clone(),
                        generation_id: self.generation,
                        member_id: self.member_id.clone(),
                        group_instance_id: None,
                        ..Default::default()
                    };
                    let id = self.send(ctx, request);
                    self.heartbeats.push(id);
                    self.next_heartbeat_at = now + self.config.heartbeat_interval_ms;
                }
            }
            Phase::Joining | Phase::Syncing => {}
        }
    }

    fn send_join(&mut self, ctx: &mut Ctx<'_>) {
        let request = JoinGroupRequest {
            group_id: self.config.group_id.clone(),
            session_timeout_ms: millis_i32(self.config.session_timeout_ms),
            rebalance_timeout_ms: millis_i32(self.config.rebalance_timeout_ms),
            member_id: self.member_id.clone(),
            group_instance_id: None,
            protocol_type: PROTOCOL_TYPE.to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: PROTOCOL.to_string(),
                metadata: self.identity.encode(),
                ..Default::default()
            }],
            reason: Some(String::new()),
            ..Default::default()
        };
        self.joins += 1;
        self.join = Some(self.send(ctx, request));
    }

    fn forget_coordinator(&mut self) {
        self.lane
            .get_mut()
            .invalidate_coordinator(CoordinatorType::Group, &self.config.group_id);
    }

    /// Record an error code an answer carried, and let the client react to
    /// it: `NOT_COORDINATOR` and `COORDINATOR_NOT_AVAILABLE` forget the
    /// coordinator.
    fn note_error(&mut self, code: i16) {
        self.last_error = Some(code);
        let target = self.coordinator();
        self.lane.get_mut().note_error(code, &target);
    }

    /// Join again: at once after a rebalance or a reset member, after the
    /// backoff after any other failure.
    fn rejoin(&mut self, now: Millis, backoff: Option<u64>) {
        self.phase = Phase::Joining;
        self.join = None;
        self.sync = None;
        self.heartbeats.clear();
        self.rejoin_at = match backoff {
            None => now,
            Some(jitter) => {
                let wait = exponential_backoff(
                    RETRY_BACKOFF_MS,
                    RETRY_BACKOFF_MAX_MS,
                    self.attempts,
                    jitter,
                );
                self.attempts += 1;
                now + wait
            }
        };
    }

    /// Kafka's `resetStateAndGeneration` with the member id.
    fn reset_member(&mut self) {
        self.member_id.clear();
        self.generation = -1;
    }

    fn on_client_events(
        &mut self,
        ctx: &mut Ctx<'_>,
        events: Vec<ClientEvent>,
        out: &mut Vec<ElectionEvent>,
    ) {
        for event in events {
            let ClientEvent::Response { id, result } = event else {
                continue;
            };
            if self.join == Some(id) {
                self.join = None;
                self.on_join(ctx, result);
            } else if self.sync == Some(id) {
                self.sync = None;
                self.on_sync(ctx, result, out);
            } else if let Some(at) = self.heartbeats.iter().position(|h| *h == id) {
                self.heartbeats.remove(at);
                self.on_heartbeat(ctx, result);
            }
        }
    }

    /// The typed body of an answer, or `None` after a lost answer, which
    /// forgets the coordinator as Kafka's `coordinatorDead` does.
    fn expect<T: 'static>(&mut self, result: Result<Response, ClientError>) -> Option<T> {
        let Ok(response) = result else {
            self.last_error = Some(codes::NETWORK_EXCEPTION);
            self.forget_coordinator();
            return None;
        };
        response.downcast::<T>()
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_>, result: Result<Response, ClientError>) {
        let now = ctx.now();
        let Some(response) = self.expect::<JoinGroupResponse>(result) else {
            self.rejoin(now, Some(ctx.rand(400)));
            return;
        };
        match response.error_code {
            codes::NONE => {
                self.member_id = response.member_id.clone();
                self.generation = response.generation_id;
                self.phase = Phase::Syncing;
                let assignments = if response.leader == response.member_id {
                    let members: Vec<(String, Option<Identity>)> = response
                        .members
                        .iter()
                        .map(|m| (m.member_id.clone(), Identity::decode(&m.metadata)))
                        .collect();
                    let assignment = assign(&members).encode();
                    response
                        .members
                        .iter()
                        .map(|m| SyncGroupRequestAssignment {
                            member_id: m.member_id.clone(),
                            assignment: assignment.clone(),
                            ..Default::default()
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let request = SyncGroupRequest {
                    group_id: self.config.group_id.clone(),
                    generation_id: self.generation,
                    member_id: self.member_id.clone(),
                    group_instance_id: None,
                    protocol_type: Some(PROTOCOL_TYPE.to_string()),
                    protocol_name: Some(PROTOCOL.to_string()),
                    assignments,
                    ..Default::default()
                };
                self.sync = Some(self.send(ctx, request));
            }
            codes::MEMBER_ID_REQUIRED => {
                self.member_id = response.member_id;
                self.rejoin(now, None);
            }
            codes::UNKNOWN_MEMBER_ID => {
                self.reset_member();
                self.rejoin(now, None);
            }
            codes::REBALANCE_IN_PROGRESS => self.rejoin(now, None),
            code => {
                self.note_error(code);
                self.rejoin(now, Some(ctx.rand(400)));
            }
        }
    }

    fn on_sync(
        &mut self,
        ctx: &mut Ctx<'_>,
        result: Result<Response, ClientError>,
        out: &mut Vec<ElectionEvent>,
    ) {
        let now = ctx.now();
        let Some(response) = self.expect::<SyncGroupResponse>(result) else {
            self.rejoin(now, Some(ctx.rand(400)));
            return;
        };
        match response.error_code {
            codes::NONE => {
                let Some(assignment) = Assignment::decode(&response.assignment) else {
                    self.last_error = Some(codes::INCONSISTENT_GROUP_PROTOCOL);
                    self.rejoin(now, Some(ctx.rand(400)));
                    return;
                };
                self.phase = Phase::Stable;
                self.attempts = 0;
                self.session_from = now;
                self.next_heartbeat_at = now + self.config.heartbeat_interval_ms;
                self.needs_join_prepare = true;
                self.assignment = Some(assignment.clone());
                out.push(ElectionEvent::Assigned {
                    generation: self.generation,
                    assignment,
                });
            }
            codes::REBALANCE_IN_PROGRESS => self.rejoin(now, None),
            codes::UNKNOWN_MEMBER_ID | codes::ILLEGAL_GENERATION => {
                self.reset_member();
                self.rejoin(now, None);
            }
            code => {
                self.note_error(code);
                self.rejoin(now, Some(ctx.rand(400)));
            }
        }
    }

    fn on_heartbeat(&mut self, ctx: &mut Ctx<'_>, result: Result<Response, ClientError>) {
        let now = ctx.now();
        let Some(response) = self.expect::<HeartbeatResponse>(result) else {
            return;
        };
        if self.phase != Phase::Stable {
            return;
        }
        match response.error_code {
            codes::NONE => self.session_from = now,
            codes::REBALANCE_IN_PROGRESS => self.rejoin(now, None),
            codes::UNKNOWN_MEMBER_ID | codes::ILLEGAL_GENERATION => {
                self.reset_member();
                self.rejoin(now, None);
            }
            code => {
                self.note_error(code);
            }
        }
    }

    /// The next time the member needs a tick.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let own = match self.phase {
            Phase::Joining if self.join.is_none() => Some(self.rejoin_at.max(now)),
            Phase::Stable => Some(
                self.next_heartbeat_at
                    .min(self.session_from + self.config.session_timeout_ms)
                    .max(now),
            ),
            Phase::Joining | Phase::Syncing => None,
        };
        own.into_iter()
            .chain(self.lane.get().next_deadline(now))
            .min()
    }

    /// The member for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "group": self.config.group_id,
            "state": self.phase.name(),
            "member_id": self.member_id,
            "generation": self.generation,
            "joins": self.joins,
            "identity": { "url": self.identity.url(), "eligible": self.identity.eligible },
            "assignment": self.assignment.as_ref().map(|a| json!({
                "error": a.error,
                "leader": a.leader,
                "leader_url": a.leader_identity.as_ref().map(Identity::url),
            })),
            "last_error": self.last_error,
            "client": self.lane.get().snapshot(),
        })
    }
}

/// A timeout in milliseconds as the wire's `int32`.
fn millis_i32(ms: Millis) -> i32 {
    i32::try_from(ms).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn identity(node: u32, eligible: bool) -> Identity {
        Identity::of_node(NodeId(node), 8081, eligible)
    }

    #[test]
    fn an_identity_is_confluents_json_and_names_its_node() {
        let me = identity(4, true);
        assert!(
            me.encode()
                == Bytes::from_static(
                    br#"{"host":"node-4","port":8081,"master_eligibility":true,"scheme":"http","version":1,"leader":false}"#
                )
        );
        assert!(Identity::decode(&me.encode()) == Some(me.clone()));
        assert!(me.url() == "http://node-4:8081");
        assert!(me.node() == Some(NodeId(4)));
        let addressed = Identity {
            host: "10.0.1.2".to_string(),
            ..me
        };
        assert!(addressed.node() == Some(NodeId(258)));
        assert!(Identity::decode(b"{}").is_none());
    }

    #[test]
    fn an_assignment_is_confluents_json() {
        let with_leader = Assignment {
            error: 0,
            leader: Some("sr-1-a".to_string()),
            leader_identity: Some(identity(4, true)),
        };
        assert!(
            with_leader.encode()
                == Bytes::from(
                    r#"{"error":0,"master":"sr-1-a","master_identity":{"host":"node-4","port":8081,"master_eligibility":true,"scheme":"http","version":1,"leader":false},"version":1}"#
                )
        );
        assert!(Assignment::decode(&with_leader.encode()) == Some(with_leader));
        let without = Assignment {
            error: 0,
            leader: None,
            leader_identity: None,
        };
        assert!(without.encode() == Bytes::from_static(br#"{"error":0,"version":1}"#));
        assert!(Assignment::decode(&without.encode()) == Some(without));
    }

    #[test]
    fn the_eligible_member_with_the_smallest_url_leads() {
        let member = |id: &str, identity: Option<Identity>| (id.to_string(), identity);
        let cases = [
            (
                vec![
                    member("b", Some(identity(5, true))),
                    member("a", Some(identity(4, true))),
                ],
                Some(("a", identity(4, true))),
                0,
            ),
            // The comparison is on the URL text: node-10 sorts before node-4.
            (
                vec![
                    member("a", Some(identity(4, true))),
                    member("b", Some(identity(10, true))),
                ],
                Some(("b", identity(10, true))),
                0,
            ),
            (
                vec![
                    member("a", Some(identity(4, false))),
                    member("b", Some(identity(5, true))),
                ],
                Some(("b", identity(5, true))),
                0,
            ),
            (vec![member("a", Some(identity(4, false)))], None, 0),
            (
                vec![
                    member("a", Some(identity(4, true))),
                    member("b", Some(identity(4, true))),
                ],
                Some(("a", identity(4, true))),
                DUPLICATE_URLS,
            ),
            (
                vec![member("a", None), member("b", Some(identity(5, true)))],
                Some(("b", identity(5, true))),
                0,
            ),
        ];
        for (members, leader, error) in cases {
            let expected = Assignment {
                error,
                leader: leader.as_ref().map(|(id, _)| (*id).to_string()),
                leader_identity: leader.map(|(_, identity)| identity),
            };
            assert!(assign(&members) == expected, "{members:?}");
        }
    }
}
