//! A fake broker for the client tests: a [`Node`] that answers the apis the
//! client uses with correctly encoded responses over a shared cluster state,
//! with knobs to inject the errors the client must handle.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    rc::Rc,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode, ProtocolRequest,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
        create_topics_request::CreateTopicsRequest,
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
        fetch_request::FetchRequest,
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
        heartbeat_request::HeartbeatRequest,
        heartbeat_response::HeartbeatResponse,
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        join_group_request::JoinGroupRequest,
        join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
        leave_group_request::LeaveGroupRequest,
        leave_group_response::{LeaveGroupResponse, MemberResponse},
        list_offsets_request::ListOffsetsRequest,
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
        metadata_request::MetadataRequest,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        offset_commit_request::OffsetCommitRequest,
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
        offset_fetch_request::OffsetFetchRequest,
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
            OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        },
        produce_request::ProduceRequest,
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
        request_header::RequestHeader,
        response_header::ResponseHeader,
        sync_group_request::SyncGroupRequest,
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid,
    records::{RecordBatch, RecordsPayload, patch_base_offset_and_leader_epoch},
};
use serde_json::{Value, json};

use super::request;
use crate::lab::{
    codes,
    net::{ConnId, Ctx, Endpoint, Frame, KAFKA_PORT, Millis, Node, NodeId, Payload},
};

/// `MEMBER_ID_REQUIRED`.
const MEMBER_ID_REQUIRED: i16 = 79;

/// One partition of the fake cluster.
#[derive(Clone, Debug)]
pub struct FakePartition {
    pub leader: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    /// Encoded batches with their assigned base offsets.
    pub log: Vec<(i64, i64, Bytes)>,
    pub log_start: i64,
    pub log_end: i64,
}

impl FakePartition {
    fn new(leader: i32, replicas: Vec<i32>) -> Self {
        Self {
            leader,
            leader_epoch: 0,
            isr: replicas.clone(),
            replicas,
            log: Vec::new(),
            log_start: 0,
            log_end: 0,
        }
    }
}

/// One topic of the fake cluster.
#[derive(Clone, Debug)]
pub struct FakeTopic {
    pub id: Uuid,
    pub partitions: BTreeMap<i32, FakePartition>,
}

/// The classic group states the fake coordinator distinguishes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

/// A member of a classic group.
#[derive(Clone, Debug)]
pub struct FakeMember {
    pub protocols: Vec<(String, Bytes)>,
    /// The generation the member last joined.
    pub joined: i32,
    pub session_timeout_ms: Millis,
    /// When the member last joined, synced or heartbeat.
    pub last_seen: Millis,
    /// The `group.instance.id` of a static member (KIP-345).
    pub instance_id: Option<String>,
}

/// A `JoinGroup` or `SyncGroup` the coordinator holds until the rebalance
/// completes.
#[derive(Clone, Debug)]
struct Held {
    broker: NodeId,
    reply_to: Endpoint,
    conn: ConnId,
    correlation: i32,
    version: i16,
    member_id: String,
}

/// One consumer group.
#[derive(Clone, Debug, Default)]
pub struct FakeGroup {
    pub generation: i32,
    pub state: Option<GroupState>,
    pub members: BTreeMap<String, FakeMember>,
    pub leader: Option<String>,
    pub assignments: BTreeMap<String, Bytes>,
    pub committed: BTreeMap<(String, i32), (i64, i32)>,
    pub next_member: u32,
    /// The member id each static member's instance id holds.
    pub static_members: BTreeMap<String, String>,
    joins: Vec<Held>,
    syncs: Vec<Held>,
    /// KIP-848: the epoch of each member.
    pub epochs: BTreeMap<String, i32>,
    pub group_epoch: i32,
}

impl FakeGroup {
    /// Whether the coordinator holds a join or a sync of the member. Kafka
    /// keeps such a member alive without heartbeats.
    fn awaiting(&self, member_id: &str) -> bool {
        self.joins
            .iter()
            .chain(&self.syncs)
            .any(|held| held.member_id == member_id)
    }
}

/// A scripted `ConsumerGroupHeartbeat` answer.
#[derive(Clone, Debug)]
pub struct CghAnswer {
    pub error_code: i16,
    pub member_epoch: i32,
    /// `None` leaves the assignment out of the response.
    pub assignment: Option<Vec<(String, Vec<i32>)>>,
}

/// One request a fake broker saw.
#[derive(Clone, Debug)]
pub struct Seen {
    pub broker: NodeId,
    /// When the request reached the broker.
    pub at: Millis,
    pub api_key: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub body: Bytes,
}

impl Seen {
    /// The decoded request body.
    ///
    /// # Panics
    /// Panics when the body does not decode at the version it was sent at.
    #[must_use]
    pub fn decode<R: ProtocolRequest + for<'de> Decode<'de>>(&self) -> R {
        let mut cursor: &[u8] = &self.body;
        R::decode(&mut cursor, self.version).expect("request decodes at its version")
    }
}

/// The knobs a test turns to inject errors.
#[derive(Clone, Debug, Default)]
pub struct Knobs {
    /// Codes the next produce answers for a partition, in order.
    pub produce_errors: BTreeMap<(String, i32), VecDeque<i16>>,
    pub fetch_errors: BTreeMap<(String, i32), VecDeque<i16>>,
    pub heartbeat_errors: VecDeque<i16>,
    pub join_errors: VecDeque<i16>,
    pub sync_errors: VecDeque<i16>,
    pub commit_errors: VecDeque<i16>,
    pub find_coordinator_errors: VecDeque<i16>,
    pub create_topics_errors: VecDeque<i16>,
    pub cgh_script: VecDeque<CghAnswer>,
    /// How many `ApiVersions` requests get `UNSUPPORTED_VERSION` first.
    pub api_versions_unsupported: u32,
    /// Advertised maximum versions that differ from the broker's own.
    pub api_versions_max: BTreeMap<i16, i16>,
    /// The wait before an `acks=-1` produce is answered.
    pub produce_delay_ms: Millis,
    /// Do not answer any request.
    pub silent: bool,
    /// Brokers, by id, that take connections and requests but answer none,
    /// `ApiVersions` included, as a broker cut off from its controller
    /// never answers its clients.
    pub silent_brokers: BTreeSet<i32>,
    /// The error code a `Metadata` answer gives a partition without a
    /// leader: `LEADER_NOT_AVAILABLE`, as Kafka's
    /// `KRaftMetadataCache.getPartitionMetadata` answers, unless a test sets
    /// another.
    pub leaderless_error: i16,
    /// Handle this many requests but lose their answers, as a network that
    /// drops them after the broker acted.
    pub drop_responses: u32,
    /// Kafka's dedup and ordering checks on idempotent batches.
    pub sequence_checks: bool,
}

/// The idempotent state of one producer on one partition.
#[derive(Clone, Debug, Default)]
struct ProducerState {
    /// `None` until the partition saw the producer.
    epoch: Option<i16>,
    next_sequence: i32,
    /// The last five batches: base sequence and base offset.
    recent: VecDeque<(i32, i64)>,
}

/// What the sequence check decides for one idempotent batch.
enum SequenceCheck {
    Append,
    /// One of the last five batches again: success with its offset.
    Duplicate(i64),
    Reject(i16),
}

impl ProducerState {
    /// Kafka's `ProducerAppendInfo.checkSequence` and
    /// `checkProducerEpoch`: an older epoch is fenced, a newer epoch starts
    /// at sequence 0, a producer the partition never saw may start anywhere,
    /// and otherwise the batch must be the next sequence.
    fn check(&self, epoch: i16, base_sequence: i32) -> SequenceCheck {
        match self.epoch {
            None => SequenceCheck::Append,
            Some(current) if epoch < current => {
                SequenceCheck::Reject(codes::INVALID_PRODUCER_EPOCH)
            }
            Some(current) if epoch > current => {
                if base_sequence == 0 {
                    SequenceCheck::Append
                } else {
                    SequenceCheck::Reject(codes::OUT_OF_ORDER_SEQUENCE_NUMBER)
                }
            }
            Some(_) => {
                if let Some((_, offset)) =
                    self.recent.iter().find(|(base, _)| *base == base_sequence)
                {
                    SequenceCheck::Duplicate(*offset)
                } else if base_sequence == self.next_sequence {
                    SequenceCheck::Append
                } else {
                    SequenceCheck::Reject(codes::OUT_OF_ORDER_SEQUENCE_NUMBER)
                }
            }
        }
    }

    fn appended(&mut self, epoch: i16, base_sequence: i32, count: i32, base_offset: i64) {
        if self.epoch != Some(epoch) {
            self.recent.clear();
        }
        self.epoch = Some(epoch);
        self.next_sequence = base_sequence.wrapping_add(count) & i32::MAX;
        self.recent.push_back((base_sequence, base_offset));
        if self.recent.len() > 5 {
            self.recent.pop_front();
        }
    }
}

/// The state every fake broker of a cluster shares.
#[derive(Debug)]
pub struct ClusterState {
    /// Broker id to lab node.
    pub brokers: BTreeMap<i32, NodeId>,
    pub controller: i32,
    pub coordinator: i32,
    pub cluster_id: String,
    pub topics: BTreeMap<String, FakeTopic>,
    pub groups: BTreeMap<String, FakeGroup>,
    pub next_producer_id: i64,
    producer_epochs: BTreeMap<i64, i16>,
    producers: BTreeMap<(i64, String, i32), ProducerState>,
    pub knobs: Knobs,
    pub requests: Vec<Seen>,
    next_topic_id: u8,
}

impl ClusterState {
    /// A cluster of `brokers`, given as `(broker id, node id)`, with the
    /// first broker as controller and coordinator.
    #[must_use]
    pub fn new(brokers: &[(i32, u32)]) -> Rc<RefCell<Self>> {
        let first = brokers.first().map_or(-1, |(id, _)| *id);
        Rc::new(RefCell::new(Self {
            brokers: brokers
                .iter()
                .map(|(id, node)| (*id, NodeId(*node)))
                .collect(),
            controller: first,
            coordinator: first,
            cluster_id: "lab-cluster".to_string(),
            topics: BTreeMap::new(),
            groups: BTreeMap::new(),
            next_producer_id: 1_000,
            producer_epochs: BTreeMap::new(),
            producers: BTreeMap::new(),
            knobs: Knobs {
                sequence_checks: true,
                leaderless_error: codes::LEADER_NOT_AVAILABLE,
                ..Knobs::default()
            },
            requests: Vec::new(),
            next_topic_id: 1,
        }))
    }

    /// Create a topic with `partitions` partitions, leaders round-robin over
    /// the brokers.
    pub fn add_topic(&mut self, name: &str, partitions: i32, replication_factor: i16) {
        let rf = usize::try_from(replication_factor).unwrap_or(1);
        let map = (0..partitions.max(0))
            .map(|p| (p, self.placed(p, rf)))
            .collect();
        let mut id = [0_u8; 16];
        id[15] = self.next_topic_id;
        self.next_topic_id += 1;
        self.topics.insert(
            name.to_string(),
            FakeTopic {
                id: Uuid(id),
                partitions: map,
            },
        );
    }

    /// Grow `topic` to `partitions` partitions, as `CreatePartitions` does:
    /// the topic keeps its id and its partitions, and the new ones are
    /// placed as `add_topic` places them.
    pub fn add_partitions(&mut self, topic: &str, partitions: i32) {
        let Some(rf) = self
            .topics
            .get(topic)
            .map(|t| t.partitions.values().next().map_or(1, |p| p.replicas.len()))
        else {
            return;
        };
        let placed: Vec<(i32, FakePartition)> = (0..partitions.max(0))
            .filter(|p| !self.topics[topic].partitions.contains_key(p))
            .map(|p| (p, self.placed(p, rf)))
            .collect();
        if let Some(t) = self.topics.get_mut(topic) {
            t.partitions.extend(placed);
        }
    }

    /// Partition `p` of a new topic with `rf` replicas: the replicas start
    /// at broker `p` of the brokers in turn, and the first leads.
    fn placed(&self, p: i32, rf: usize) -> FakePartition {
        let brokers: Vec<i32> = self.brokers.keys().copied().collect();
        let rf = rf.clamp(1, brokers.len().max(1));
        let start = usize::try_from(p).unwrap_or(0) % brokers.len().max(1);
        let replicas: Vec<i32> = (0..rf)
            .map(|i| brokers[(start + i) % brokers.len()])
            .collect();
        let leader = replicas.first().copied().unwrap_or(-1);
        FakePartition::new(leader, replicas)
    }

    /// Move the leadership of a partition to `leader`, bumping the epoch.
    pub fn set_leader(&mut self, topic: &str, partition: i32, leader: i32) {
        if let Some(p) = self
            .topics
            .get_mut(topic)
            .and_then(|t| t.partitions.get_mut(&partition))
        {
            p.leader = leader;
            p.leader_epoch += 1;
        }
    }

    /// The decoded record batches of a partition's log.
    ///
    /// # Panics
    /// Panics when a stored batch does not decode.
    #[must_use]
    pub fn batches(&self, topic: &str, partition: i32) -> Vec<RecordBatch> {
        self.topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| {
                p.log
                    .iter()
                    .map(|(_, _, bytes)| {
                        let mut cursor: &[u8] = bytes;
                        RecordBatch::decode(&mut cursor).expect("stored batch decodes")
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Append one batch to a partition log as its leader: assign the next
    /// offset and stamp the leader epoch, as a broker does. Returns the base
    /// offset.
    ///
    /// # Panics
    /// Panics when the partition does not exist.
    pub fn append_batch(&mut self, topic: &str, partition: i32, batch: &RecordBatch) -> i64 {
        let p = self
            .topics
            .get_mut(topic)
            .and_then(|t| t.partitions.get_mut(&partition))
            .expect("the partition exists");
        let base_offset = p.log_end;
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).expect("batch encodes");
        patch_base_offset_and_leader_epoch(&mut buf, base_offset, p.leader_epoch);
        let count = i64::try_from(batch.records.len()).unwrap_or(1).max(1);
        let last = base_offset + count - 1;
        p.log.push((base_offset, last, buf.freeze()));
        p.log_end = last + 1;
        base_offset
    }

    /// Append one batch of plain records with `CreateTime` timestamps, as a
    /// producer outside the test would. Returns the base offset.
    pub fn append_records(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[super::batch::BatchRecord],
    ) -> i64 {
        let batch = super::batch::build_batch(records, None, 0);
        self.append_batch(topic, partition, &batch)
    }

    /// The requests seen for one api key.
    #[must_use]
    pub fn seen(&self, api_key: i16) -> Vec<Seen> {
        self.requests
            .iter()
            .filter(|s| s.api_key == api_key)
            .cloned()
            .collect()
    }

    fn topic_name_by_id(&self, id: Uuid) -> Option<String> {
        self.topics
            .iter()
            .find(|(_, t)| t.id == id)
            .map(|(name, _)| name.clone())
    }
}

/// What a handler answers with.
enum Reply {
    Now(Bytes),
    At(Millis, Bytes),
    Hold,
    Nothing,
}

/// A response the broker answers later.
struct Delayed {
    at: Millis,
    seq: u64,
    frame: Frame,
}

/// A fetch the broker holds for `max_wait_ms` or until data arrives.
struct HeldFetch {
    at: Millis,
    reply_to: Endpoint,
    conn: ConnId,
    correlation: i32,
    version: i16,
    request: FetchRequest,
}

/// The fake broker. See the module documentation.
pub struct FakeBroker {
    node: NodeId,
    broker_id: i32,
    state: Rc<RefCell<ClusterState>>,
    conns: BTreeSet<(Endpoint, ConnId)>,
    delayed: Vec<Delayed>,
    held: Vec<HeldFetch>,
    seq: u64,
    pub started: u32,
}

/// The apis the fake broker serves, with the versions of the crate.
fn supported() -> Vec<(i16, i16, i16)> {
    fn row<R: ProtocolRequest>() -> (i16, i16, i16) {
        (R::API_KEY, R::MIN_VERSION, R::LATEST_STABLE_VERSION)
    }
    vec![
        row::<ProduceRequest>(),
        row::<FetchRequest>(),
        row::<ListOffsetsRequest>(),
        row::<MetadataRequest>(),
        row::<OffsetCommitRequest>(),
        row::<OffsetFetchRequest>(),
        row::<FindCoordinatorRequest>(),
        row::<JoinGroupRequest>(),
        row::<HeartbeatRequest>(),
        row::<LeaveGroupRequest>(),
        row::<SyncGroupRequest>(),
        row::<ApiVersionsRequest>(),
        row::<CreateTopicsRequest>(),
        row::<DeleteTopicsRequest>(),
        row::<InitProducerIdRequest>(),
        row::<ConsumerGroupHeartbeatRequest>(),
    ]
}

fn flexible_min(api_key: i16) -> i16 {
    fn of<R: ProtocolRequest>() -> i16 {
        R::FLEXIBLE_MIN
    }
    match api_key {
        0 => of::<ProduceRequest>(),
        1 => of::<FetchRequest>(),
        2 => of::<ListOffsetsRequest>(),
        3 => of::<MetadataRequest>(),
        8 => of::<OffsetCommitRequest>(),
        9 => of::<OffsetFetchRequest>(),
        10 => of::<FindCoordinatorRequest>(),
        11 => of::<JoinGroupRequest>(),
        12 => of::<HeartbeatRequest>(),
        13 => of::<LeaveGroupRequest>(),
        14 => of::<SyncGroupRequest>(),
        18 => of::<ApiVersionsRequest>(),
        19 => of::<CreateTopicsRequest>(),
        20 => of::<DeleteTopicsRequest>(),
        22 => of::<InitProducerIdRequest>(),
        68 => of::<ConsumerGroupHeartbeatRequest>(),
        _ => i16::MAX,
    }
}

/// Frame a response: length, header at the version the api and body version
/// call for, body.
///
/// # Panics
/// Panics when the body does not encode at `version`.
pub fn frame_response<T: Encode>(
    api_key: i16,
    version: i16,
    correlation_id: i32,
    body: &T,
) -> Bytes {
    let header_version = request::response_header_version(api_key, flexible_min(api_key), version);
    let header = ResponseHeader {
        correlation_id,
        ..Default::default()
    };
    let len = header.encoded_len(header_version) + body.encoded_len(version);
    let mut buf = BytesMut::with_capacity(4 + len);
    buf.put_i32(i32::try_from(len).expect("response fits"));
    header
        .encode(&mut buf, header_version)
        .expect("response header encodes");
    body.encode(&mut buf, version).expect("response encodes");
    buf.freeze()
}

fn decode_body<R: for<'de> Decode<'de>>(body: &[u8], version: i16) -> Option<R> {
    let mut cursor = body;
    R::decode(&mut cursor, version).ok()
}

impl FakeBroker {
    pub fn new(node: NodeId, broker_id: i32, state: Rc<RefCell<ClusterState>>) -> Self {
        Self {
            node,
            broker_id,
            state,
            conns: BTreeSet::new(),
            delayed: Vec::new(),
            held: Vec::new(),
            seq: 0,
            started: 0,
        }
    }

    #[must_use]
    pub fn node(&self) -> NodeId {
        self.node
    }

    #[must_use]
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    fn endpoint(&self) -> Endpoint {
        Endpoint::kafka(self.node)
    }

    fn host_of(state: &ClusterState, broker: i32) -> String {
        state
            .brokers
            .get(&broker)
            .map_or_else(|| "unknown".to_string(), |node| format!("node-{node}"))
    }

    fn arm(&self, ctx: &mut Ctx<'_>) {
        let next = self
            .delayed
            .iter()
            .map(|d| d.at)
            .chain(self.held.iter().map(|h| h.at))
            .chain(self.next_session_expiry())
            .min();
        if let Some(at) = next {
            ctx.arm(at.max(ctx.now()));
        }
    }

    /// When the first classic member session of this coordinator expires.
    fn next_session_expiry(&self) -> Option<Millis> {
        let state = self.state.borrow();
        if state.coordinator != self.broker_id {
            return None;
        }
        state
            .groups
            .values()
            .flat_map(|g| {
                g.members
                    .iter()
                    .filter(|(id, _)| !g.awaiting(id))
                    .map(|(_, m)| m.last_seen + m.session_timeout_ms)
            })
            .min()
    }

    /// Remove the classic members whose session expired, as Kafka's
    /// `onExpireHeartbeat` does: the group rebalances without them, and a
    /// rebalance that only waited for them completes.
    fn expire_members(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let from = self.endpoint();
        let mut state = self.state.borrow_mut();
        if state.coordinator != self.broker_id {
            return;
        }
        let group_ids: Vec<String> = state.groups.keys().cloned().collect();
        for group_id in group_ids {
            let Some(group) = state.groups.get_mut(&group_id) else {
                continue;
            };
            let expired: Vec<String> = group
                .members
                .iter()
                .filter(|(id, m)| now >= m.last_seen + m.session_timeout_ms && !group.awaiting(id))
                .map(|(id, _)| id.clone())
                .collect();
            if expired.is_empty() {
                continue;
            }
            for id in &expired {
                group.members.remove(id);
                group.static_members.retain(|_, member| member != id);
            }
            if group.members.is_empty() {
                group.state = Some(GroupState::Empty);
                group.leader = None;
                continue;
            }
            group.state = Some(GroupState::PreparingRebalance);
            let next_generation = group.generation + 1;
            let complete = !group.joins.is_empty()
                && group.members.values().all(|m| m.joined == next_generation);
            if complete {
                Self::complete_join(&mut state, &group_id, ctx, from);
            }
        }
    }

    fn handle(&mut self, ctx: &mut Ctx<'_>, src: Endpoint, conn: ConnId, bytes: &Bytes) {
        let mut cursor: &[u8] = bytes;
        if cursor.remaining() < 8 {
            return;
        }
        let _len = cursor.get_i32();
        let api_key = i16::from_be_bytes([cursor[0], cursor[1]]);
        let version = i16::from_be_bytes([cursor[2], cursor[3]]);
        let header_version = request::request_header_version(flexible_min(api_key), version);
        let Ok(header) = RequestHeader::decode(&mut cursor, header_version) else {
            return;
        };
        let body = Bytes::copy_from_slice(cursor);
        self.state.borrow_mut().requests.push(Seen {
            broker: self.node,
            at: ctx.now(),
            api_key,
            version,
            correlation_id: header.correlation_id,
            client_id: header.client_id.clone(),
            body: body.clone(),
        });
        let silent = {
            let knobs = &self.state.borrow().knobs;
            knobs.silent || knobs.silent_brokers.contains(&self.broker_id)
        };
        if silent {
            return;
        }
        let reply = self.dispatch(ctx, src, conn, &header, version, &body);
        let lost = {
            let mut state = self.state.borrow_mut();
            let answered = matches!(reply, Reply::Now(_) | Reply::At(..));
            if answered && state.knobs.drop_responses > 0 {
                state.knobs.drop_responses -= 1;
                true
            } else {
                false
            }
        };
        if lost {
            self.arm(ctx);
            return;
        }
        match reply {
            Reply::Now(bytes) => ctx.send(Frame::data(self.endpoint(), src, conn, bytes)),
            Reply::At(at, bytes) => {
                self.seq += 1;
                self.delayed.push(Delayed {
                    at,
                    seq: self.seq,
                    frame: Frame::data(self.endpoint(), src, conn, bytes),
                });
                self.arm(ctx);
            }
            Reply::Hold | Reply::Nothing => {}
        }
        self.arm(ctx);
    }

    fn dispatch(
        &mut self,
        ctx: &mut Ctx<'_>,
        src: Endpoint,
        conn: ConnId,
        header: &RequestHeader,
        version: i16,
        body: &Bytes,
    ) -> Reply {
        let api_key = header.request_api_key;
        let correlation = header.correlation_id;
        let now = ctx.now();
        macro_rules! answer {
            ($req:ty, $handler:expr) => {{
                let Some(request) = decode_body::<$req>(body, version) else {
                    return Reply::Nothing;
                };
                let response = $handler(request);
                Reply::Now(frame_response(api_key, version, correlation, &response))
            }};
        }
        match api_key {
            18 => Reply::Now(frame_response(
                api_key,
                version,
                correlation,
                &self.api_versions(version),
            )),
            3 => answer!(MetadataRequest, |r| self.metadata(&r)),
            10 => answer!(FindCoordinatorRequest, |r| self
                .find_coordinator(&r, version)),
            22 => answer!(InitProducerIdRequest, |r| self.init_producer_id(&r)),
            19 => answer!(CreateTopicsRequest, |r| self.create_topics(&r)),
            20 => answer!(DeleteTopicsRequest, |r| self.delete_topics(&r)),
            2 => answer!(ListOffsetsRequest, |r| self.list_offsets(&r)),
            12 => answer!(HeartbeatRequest, |r| self.heartbeat(&r, now)),
            13 => answer!(LeaveGroupRequest, |r| self.leave_group(&r)),
            8 => answer!(OffsetCommitRequest, |r| self.offset_commit(&r)),
            9 => answer!(OffsetFetchRequest, |r| self.offset_fetch(&r)),
            68 => answer!(ConsumerGroupHeartbeatRequest, |r| self
                .consumer_group_heartbeat(&r)),
            0 => {
                let Some(request) = decode_body::<ProduceRequest>(body, version) else {
                    return Reply::Nothing;
                };
                let response = self.produce(&request);
                self.release_held(ctx);
                if request.acks == 0 {
                    return Reply::Nothing;
                }
                let bytes = frame_response(api_key, version, correlation, &response);
                let delay = self.state.borrow().knobs.produce_delay_ms;
                if request.acks == -1 && delay > 0 {
                    Reply::At(now + delay, bytes)
                } else {
                    Reply::Now(bytes)
                }
            }
            1 => {
                let Some(request) = decode_body::<FetchRequest>(body, version) else {
                    return Reply::Nothing;
                };
                let (response, empty) = self.fetch(&request);
                if empty && request.max_wait_ms > 0 {
                    self.held.push(HeldFetch {
                        at: now + Millis::try_from(request.max_wait_ms).unwrap_or(0),
                        reply_to: src,
                        conn,
                        correlation,
                        version,
                        request,
                    });
                    self.arm(ctx);
                    return Reply::Hold;
                }
                Reply::Now(frame_response(api_key, version, correlation, &response))
            }
            11 => {
                let Some(request) = decode_body::<JoinGroupRequest>(body, version) else {
                    return Reply::Nothing;
                };
                self.join_group(ctx, src, conn, correlation, version, &request)
            }
            14 => {
                let Some(request) = decode_body::<SyncGroupRequest>(body, version) else {
                    return Reply::Nothing;
                };
                self.sync_group(ctx, src, conn, correlation, version, &request)
            }
            _ => Reply::Nothing,
        }
    }

    /// Answer the held fetches whose partitions got data, or whose wait
    /// passed.
    fn release_held(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let held = std::mem::take(&mut self.held);
        for h in held {
            let (response, empty) = self.fetch(&h.request);
            if empty && now < h.at {
                self.held.push(h);
                continue;
            }
            let bytes = frame_response(1, h.version, h.correlation, &response);
            ctx.send(Frame::data(self.endpoint(), h.reply_to, h.conn, bytes));
        }
        self.arm(ctx);
    }

    // ---- handlers ---------------------------------------------------------------

    fn api_versions(&self, version: i16) -> ApiVersionsResponse {
        let mut state = self.state.borrow_mut();
        let overrides = state.knobs.api_versions_max.clone();
        let entry = |key: i16, min: i16, max: i16| ApiVersion {
            api_key: key,
            min_version: min,
            max_version: overrides.get(&key).copied().unwrap_or(max),
            ..Default::default()
        };
        if state.knobs.api_versions_unsupported > 0 && version > 0 {
            state.knobs.api_versions_unsupported -= 1;
            let own = entry(
                ApiVersionsRequest::API_KEY,
                ApiVersionsRequest::MIN_VERSION,
                ApiVersionsRequest::LATEST_STABLE_VERSION,
            );
            return ApiVersionsResponse {
                error_code: codes::UNSUPPORTED_VERSION,
                api_keys: vec![own],
                ..Default::default()
            };
        }
        ApiVersionsResponse {
            error_code: codes::NONE,
            api_keys: supported()
                .into_iter()
                .map(|(key, min, max)| entry(key, min, max))
                .collect(),
            ..Default::default()
        }
    }

    fn metadata(&self, request: &MetadataRequest) -> MetadataResponse {
        let state = self.state.borrow();
        let brokers = state
            .brokers
            .iter()
            .map(|(id, node)| MetadataResponseBroker {
                node_id: *id,
                host: format!("node-{node}"),
                port: i32::from(KAFKA_PORT),
                rack: None,
                ..Default::default()
            })
            .collect();
        let names: Vec<String> = match &request.topics {
            None => state.topics.keys().cloned().collect(),
            Some(list) => list
                .iter()
                .filter_map(|t| {
                    t.name
                        .clone()
                        .or_else(|| state.topic_name_by_id(t.topic_id))
                })
                .collect(),
        };
        let topics = names
            .into_iter()
            .map(|name| match state.topics.get(&name) {
                Some(topic) => MetadataResponseTopic {
                    error_code: codes::NONE,
                    name: Some(name.clone()),
                    topic_id: topic.id,
                    is_internal: name.starts_with("__"),
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|(index, p)| MetadataResponsePartition {
                            error_code: if p.leader < 0 {
                                state.knobs.leaderless_error
                            } else {
                                codes::NONE
                            },
                            partition_index: *index,
                            leader_id: p.leader,
                            leader_epoch: p.leader_epoch,
                            replica_nodes: p.replicas.clone(),
                            isr_nodes: p.isr.clone(),
                            offline_replicas: Vec::new(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                None => MetadataResponseTopic {
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    name: Some(name),
                    ..Default::default()
                },
            })
            .collect();
        MetadataResponse {
            brokers,
            cluster_id: Some(state.cluster_id.clone()),
            controller_id: state.controller,
            topics,
            ..Default::default()
        }
    }

    fn find_coordinator(
        &self,
        request: &FindCoordinatorRequest,
        version: i16,
    ) -> FindCoordinatorResponse {
        let mut state = self.state.borrow_mut();
        let code = state
            .knobs
            .find_coordinator_errors
            .pop_front()
            .unwrap_or(codes::NONE);
        let coordinator = state.coordinator;
        let host = Self::host_of(&state, coordinator);
        let keys: Vec<String> = if version >= 4 {
            request.coordinator_keys.clone()
        } else {
            vec![request.key.clone()]
        };
        let row = |key: &String| {
            if code == codes::NONE {
                Coordinator {
                    key: key.clone(),
                    node_id: coordinator,
                    host: host.clone(),
                    port: i32::from(KAFKA_PORT),
                    error_code: codes::NONE,
                    error_message: None,
                    ..Default::default()
                }
            } else {
                Coordinator {
                    key: key.clone(),
                    node_id: -1,
                    host: String::new(),
                    port: -1,
                    error_code: code,
                    error_message: None,
                    ..Default::default()
                }
            }
        };
        let top = row(&request.key);
        FindCoordinatorResponse {
            error_code: top.error_code,
            error_message: None,
            node_id: top.node_id,
            host: top.host.clone(),
            port: top.port,
            coordinators: keys.iter().map(row).collect(),
            ..Default::default()
        }
    }

    fn init_producer_id(&self, request: &InitProducerIdRequest) -> InitProducerIdResponse {
        let mut state = self.state.borrow_mut();
        let (producer_id, epoch) = if request.producer_id >= 0 {
            let epoch = state
                .producer_epochs
                .get(&request.producer_id)
                .copied()
                .unwrap_or(0)
                + 1;
            (request.producer_id, epoch)
        } else {
            let id = state.next_producer_id;
            state.next_producer_id += 1;
            (id, 0)
        };
        state.producer_epochs.insert(producer_id, epoch);
        InitProducerIdResponse {
            error_code: codes::NONE,
            producer_id,
            producer_epoch: epoch,
            ..Default::default()
        }
    }

    fn create_topics(&self, request: &CreateTopicsRequest) -> CreateTopicsResponse {
        let mut state = self.state.borrow_mut();
        let injected = state.knobs.create_topics_errors.pop_front();
        let topics = request
            .topics
            .iter()
            .map(|t| {
                let code = match injected {
                    Some(code) => code,
                    None if state.topics.contains_key(&t.name) => codes::TOPIC_ALREADY_EXISTS,
                    None => codes::NONE,
                };
                let partitions = if t.num_partitions < 0 {
                    1
                } else {
                    t.num_partitions
                };
                let rf = if t.replication_factor < 0 {
                    i16::try_from(state.brokers.len()).unwrap_or(1)
                } else {
                    t.replication_factor
                };
                if code == codes::NONE {
                    state.add_topic(&t.name, partitions, rf);
                }
                CreatableTopicResult {
                    name: t.name.clone(),
                    topic_id: state.topics.get(&t.name).map_or(Uuid::ZERO, |t| t.id),
                    error_code: code,
                    error_message: None,
                    num_partitions: partitions,
                    replication_factor: rf,
                    ..Default::default()
                }
            })
            .collect();
        CreateTopicsResponse {
            topics,
            ..Default::default()
        }
    }

    fn delete_topics(&self, request: &DeleteTopicsRequest) -> DeleteTopicsResponse {
        let mut state = self.state.borrow_mut();
        let names: Vec<String> = request
            .topic_names
            .iter()
            .cloned()
            .chain(request.topics.iter().filter_map(|t| t.name.clone()))
            .collect();
        let responses = names
            .into_iter()
            .map(|name| {
                let removed = state.topics.remove(&name);
                DeletableTopicResult {
                    name: Some(name),
                    topic_id: removed.as_ref().map_or(Uuid::ZERO, |t| t.id),
                    error_code: if removed.is_some() {
                        codes::NONE
                    } else {
                        codes::UNKNOWN_TOPIC_OR_PARTITION
                    },
                    error_message: None,
                    ..Default::default()
                }
            })
            .collect();
        DeleteTopicsResponse {
            responses,
            ..Default::default()
        }
    }

    fn produce(&self, request: &ProduceRequest) -> ProduceResponse {
        let mut state = self.state.borrow_mut();
        let broker_id = self.broker_id;
        let mut responses = Vec::new();
        for topic in &request.topic_data {
            let name = if topic.name.is_empty() {
                state.topic_name_by_id(topic.topic_id).unwrap_or_default()
            } else {
                topic.name.clone()
            };
            let mut partition_responses = Vec::new();
            for pd in &topic.partition_data {
                let batches: Vec<RecordBatch> = pd
                    .records
                    .as_ref()
                    .and_then(|r| r.as_v2().map(<[RecordBatch]>::to_vec))
                    .unwrap_or_default();
                let (code, base_offset, (leader, leader_epoch)) =
                    Self::append(&mut state, broker_id, &name, pd.index, &batches);
                partition_responses.push(PartitionProduceResponse {
                    index: pd.index,
                    error_code: code,
                    base_offset,
                    log_append_time_ms: -1,
                    log_start_offset: 0,
                    current_leader: krabka_protocol::owned::produce_response::LeaderIdAndEpoch {
                        leader_id: leader,
                        leader_epoch,
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
            responses.push(TopicProduceResponse {
                name,
                topic_id: topic.topic_id,
                partition_responses,
                ..Default::default()
            });
        }
        ProduceResponse {
            responses,
            ..Default::default()
        }
    }

    /// Append batches to a partition log. Returns the error code, the base
    /// offset and the current leader with its epoch. An injected code other
    /// than `NONE` answers instead of the append; an injected `NONE` lets the
    /// append through.
    fn append(
        state: &mut ClusterState,
        broker_id: i32,
        topic: &str,
        partition: i32,
        batches: &[RecordBatch],
    ) -> (i16, i64, (i32, i32)) {
        let (leader, leader_epoch) = state
            .topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map_or((-1, -1), |p| (p.leader, p.leader_epoch));
        let current = (leader, leader_epoch);
        let injected = state
            .knobs
            .produce_errors
            .get_mut(&(topic.to_string(), partition))
            .and_then(VecDeque::pop_front)
            .filter(|code| *code != codes::NONE);
        if let Some(code) = injected {
            return (code, -1, current);
        }
        if leader < 0 {
            return (codes::UNKNOWN_TOPIC_OR_PARTITION, -1, current);
        }
        if leader != broker_id {
            return (codes::NOT_LEADER_OR_FOLLOWER, -1, current);
        }
        let mut first_offset = None;
        for batch in batches {
            let count = i32::try_from(batch.records.len()).unwrap_or(0);
            let idempotent = state.knobs.sequence_checks && batch.producer_id >= 0;
            let key = (batch.producer_id, topic.to_string(), partition);
            if idempotent {
                let producer = state.producers.entry(key.clone()).or_default();
                match producer.check(batch.producer_epoch, batch.base_sequence) {
                    SequenceCheck::Append => {}
                    SequenceCheck::Duplicate(offset) => {
                        first_offset.get_or_insert(offset);
                        continue;
                    }
                    SequenceCheck::Reject(code) => return (code, -1, current),
                }
            }
            let base_offset = state.append_batch(topic, partition, batch);
            first_offset.get_or_insert(base_offset);
            if idempotent {
                state.producers.entry(key).or_default().appended(
                    batch.producer_epoch,
                    batch.base_sequence,
                    count,
                    base_offset,
                );
            }
        }
        (codes::NONE, first_offset.unwrap_or(0), current)
    }

    /// The fetch response, and whether every partition came back empty.
    fn fetch(&self, request: &FetchRequest) -> (FetchResponse, bool) {
        let mut state = self.state.borrow_mut();
        let mut empty = true;
        let mut responses = Vec::new();
        for topic in &request.topics {
            let name = if topic.topic.is_empty() {
                state.topic_name_by_id(topic.topic_id).unwrap_or_default()
            } else {
                topic.topic.clone()
            };
            let topic_id = state.topics.get(&name).map_or(Uuid::ZERO, |t| t.id);
            let mut partitions = Vec::new();
            for fp in &topic.partitions {
                let injected = state
                    .knobs
                    .fetch_errors
                    .get_mut(&(name.clone(), fp.partition))
                    .and_then(VecDeque::pop_front);
                let partition = state
                    .topics
                    .get(&name)
                    .and_then(|t| t.partitions.get(&fp.partition));
                let row = self.fetch_partition(fp, injected, partition);
                let has_records = row
                    .records
                    .as_ref()
                    .is_some_and(|records| records.payload_len() > 0);
                if row.error_code != codes::NONE || has_records {
                    empty = false;
                }
                partitions.push(row);
            }
            responses.push(FetchableTopicResponse {
                topic: name,
                topic_id,
                partitions,
                ..Default::default()
            });
        }
        (
            FetchResponse {
                responses,
                ..Default::default()
            },
            empty,
        )
    }

    /// The answer for one partition of a fetch: an injected error, the
    /// routing and range errors a broker gives, or the log from the fetch
    /// offset on.
    fn fetch_partition(
        &self,
        fp: &krabka_protocol::owned::fetch_request::FetchPartition,
        injected: Option<i16>,
        partition: Option<&FakePartition>,
    ) -> PartitionData {
        let error = |code: i16| PartitionData {
            partition_index: fp.partition,
            error_code: code,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: -1,
            ..Default::default()
        };
        match (injected, partition) {
            (Some(code), _) => error(code),
            (None, None) => error(codes::UNKNOWN_TOPIC_OR_PARTITION),
            (None, Some(p)) if p.leader != self.broker_id => PartitionData {
                current_leader: krabka_protocol::owned::fetch_response::LeaderIdAndEpoch {
                    leader_id: p.leader,
                    leader_epoch: p.leader_epoch,
                    ..Default::default()
                },
                ..error(codes::NOT_LEADER_OR_FOLLOWER)
            },
            (None, Some(p)) if fp.fetch_offset < p.log_start || fp.fetch_offset > p.log_end => {
                PartitionData {
                    high_watermark: p.log_end,
                    last_stable_offset: p.log_end,
                    log_start_offset: p.log_start,
                    ..error(codes::OFFSET_OUT_OF_RANGE)
                }
            }
            (None, Some(p)) => {
                let mut bytes = BytesMut::new();
                for (_, last, batch) in &p.log {
                    if *last >= fp.fetch_offset {
                        bytes.put_slice(batch);
                    }
                }
                PartitionData {
                    high_watermark: p.log_end,
                    last_stable_offset: p.log_end,
                    log_start_offset: p.log_start,
                    records: Some(RecordsPayload::Raw(bytes.freeze())),
                    ..error(codes::NONE)
                }
            }
        }
    }

    fn list_offsets(&self, request: &ListOffsetsRequest) -> ListOffsetsResponse {
        let state = self.state.borrow();
        let topics = request
            .topics
            .iter()
            .map(|t| ListOffsetsTopicResponse {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| {
                        let partition = state
                            .topics
                            .get(&t.name)
                            .and_then(|topic| topic.partitions.get(&p.partition_index));
                        match partition {
                            Some(fp) if fp.leader == self.broker_id => {
                                ListOffsetsPartitionResponse {
                                    partition_index: p.partition_index,
                                    error_code: codes::NONE,
                                    timestamp: -1,
                                    offset: if p.timestamp == -2 {
                                        fp.log_start
                                    } else {
                                        fp.log_end
                                    },
                                    leader_epoch: fp.leader_epoch,
                                    ..Default::default()
                                }
                            }
                            Some(_) => ListOffsetsPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: codes::NOT_LEADER_OR_FOLLOWER,
                                timestamp: -1,
                                offset: -1,
                                leader_epoch: -1,
                                ..Default::default()
                            },
                            None => ListOffsetsPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                timestamp: -1,
                                offset: -1,
                                leader_epoch: -1,
                                ..Default::default()
                            },
                        }
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        ListOffsetsResponse {
            topics,
            ..Default::default()
        }
    }

    // ---- the classic group coordinator ------------------------------------------

    fn join_group(
        &mut self,
        ctx: &mut Ctx<'_>,
        src: Endpoint,
        conn: ConnId,
        correlation: i32,
        version: i16,
        request: &JoinGroupRequest,
    ) -> Reply {
        let mut state = self.state.borrow_mut();
        if let Some(code) = state.knobs.join_errors.pop_front() {
            let response = JoinGroupResponse {
                error_code: code,
                generation_id: -1,
                member_id: request.member_id.clone(),
                ..Default::default()
            };
            return Reply::Now(frame_response(11, version, correlation, &response));
        }
        let group = state.groups.entry(request.group_id.clone()).or_default();
        let protocols: Vec<(String, Bytes)> = request
            .protocols
            .iter()
            .map(|p| (p.name.clone(), p.metadata.clone()))
            .collect();
        let member_id = match (&request.group_instance_id, request.member_id.is_empty()) {
            // KIP-345: a static member joins without the second round of
            // KIP-394, under a new id made from its instance id.
            (Some(instance), true) => {
                group.next_member += 1;
                let new_id = format!("{instance}-{:08x}", group.next_member);
                if let Some(response) =
                    Self::replace_static_member(group, instance, &new_id, &protocols, ctx.now())
                {
                    return Reply::Now(frame_response(11, version, correlation, &response));
                }
                new_id
            }
            (None, true) => {
                group.next_member += 1;
                let member_id = format!("{}-{:08x}", "member", group.next_member);
                let response = JoinGroupResponse {
                    error_code: MEMBER_ID_REQUIRED,
                    generation_id: -1,
                    protocol_type: Some(request.protocol_type.clone()),
                    member_id,
                    ..Default::default()
                };
                return Reply::Now(frame_response(11, version, correlation, &response));
            }
            (_, false) => request.member_id.clone(),
        };
        if let Some(instance) = &request.group_instance_id {
            group
                .static_members
                .insert(instance.clone(), member_id.clone());
        }
        let next_generation = group.generation + 1;
        group.members.insert(
            member_id.clone(),
            FakeMember {
                protocols,
                joined: next_generation,
                session_timeout_ms: Millis::try_from(request.session_timeout_ms).unwrap_or(0),
                last_seen: ctx.now(),
                instance_id: request.group_instance_id.clone(),
            },
        );
        group.state = Some(GroupState::PreparingRebalance);
        // A new rebalance ends the syncs of the last one, as Kafka answers
        // them with `REBALANCE_IN_PROGRESS`.
        for held in std::mem::take(&mut group.syncs) {
            let response = SyncGroupResponse {
                error_code: codes::REBALANCE_IN_PROGRESS,
                ..Default::default()
            };
            let bytes = frame_response(14, held.version, held.correlation, &response);
            ctx.send(Frame::data(
                self.endpoint(),
                held.reply_to,
                held.conn,
                bytes,
            ));
        }
        group.joins.push(Held {
            broker: self.node,
            reply_to: src,
            conn,
            correlation,
            version,
            member_id,
        });
        let all_joined = group.members.values().all(|m| m.joined == next_generation);
        if all_joined {
            Self::complete_join(&mut state, &request.group_id, ctx, self.endpoint());
        }
        Reply::Hold
    }

    /// Kafka's `updateStaticMemberThenRebalanceOrCompleteJoin` for a static
    /// member that comes back to a stable group: it takes `new_id` with its
    /// assignment, and the generation stays. The answer comes at once; a
    /// returning leader gets the members and must skip the assignment
    /// (KIP-814). In any other state the old member goes, and the member
    /// joins as a new one: `None`.
    fn replace_static_member(
        group: &mut FakeGroup,
        instance: &str,
        new_id: &str,
        protocols: &[(String, Bytes)],
        now: Millis,
    ) -> Option<JoinGroupResponse> {
        let old_id = group.static_members.get(instance)?.clone();
        let Some(mut member) = group.members.remove(&old_id) else {
            group.static_members.remove(instance);
            return None;
        };
        if group.state != Some(GroupState::Stable) {
            return None;
        }
        member.protocols = protocols.to_vec();
        member.last_seen = now;
        group.members.insert(new_id.to_string(), member);
        if let Some(assignment) = group.assignments.remove(&old_id) {
            group.assignments.insert(new_id.to_string(), assignment);
        }
        if group.leader.as_deref() == Some(old_id.as_str()) {
            group.leader = Some(new_id.to_string());
        }
        group
            .static_members
            .insert(instance.to_string(), new_id.to_string());
        let leader = group.leader.clone().unwrap_or_default();
        let is_leader = leader == new_id;
        Some(JoinGroupResponse {
            error_code: codes::NONE,
            generation_id: group.generation,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            leader,
            skip_assignment: is_leader,
            member_id: new_id.to_string(),
            members: if is_leader {
                Self::member_list(group)
            } else {
                Vec::new()
            },
            ..Default::default()
        })
    }

    /// The members of a group as `JoinGroup` lists them for the leader.
    fn member_list(group: &FakeGroup) -> Vec<JoinGroupResponseMember> {
        group
            .members
            .iter()
            .map(|(id, m)| JoinGroupResponseMember {
                member_id: id.clone(),
                group_instance_id: m.instance_id.clone(),
                metadata: m
                    .protocols
                    .iter()
                    .find(|(name, _)| name == "range")
                    .map(|(_, bytes)| bytes.clone())
                    .unwrap_or_default(),
                ..Default::default()
            })
            .collect()
    }

    /// Advance the generation and answer every held join: the leader gets the
    /// members with their metadata, the others an empty list.
    fn complete_join(state: &mut ClusterState, group_id: &str, ctx: &mut Ctx<'_>, from: Endpoint) {
        let Some(group) = state.groups.get_mut(group_id) else {
            return;
        };
        group.generation += 1;
        group.state = Some(GroupState::CompletingRebalance);
        group.assignments.clear();
        for member in group.members.values_mut() {
            member.last_seen = ctx.now();
        }
        let leader = group.members.keys().next().cloned().unwrap_or_default();
        group.leader = Some(leader.clone());
        let members = Self::member_list(group);
        let generation = group.generation;
        for held in std::mem::take(&mut group.joins) {
            let response = JoinGroupResponse {
                error_code: codes::NONE,
                generation_id: generation,
                protocol_type: Some("consumer".to_string()),
                protocol_name: Some("range".to_string()),
                leader: leader.clone(),
                skip_assignment: false,
                member_id: held.member_id.clone(),
                members: if held.member_id == leader {
                    members.clone()
                } else {
                    Vec::new()
                },
                ..Default::default()
            };
            let bytes = frame_response(11, held.version, held.correlation, &response);
            let _ = held.broker;
            ctx.send(Frame::data(from, held.reply_to, held.conn, bytes));
        }
    }

    fn sync_group(
        &mut self,
        ctx: &mut Ctx<'_>,
        src: Endpoint,
        conn: ConnId,
        correlation: i32,
        version: i16,
        request: &SyncGroupRequest,
    ) -> Reply {
        let mut state = self.state.borrow_mut();
        let injected = state.knobs.sync_errors.pop_front();
        let Some(group) = state.groups.get_mut(&request.group_id) else {
            let response = SyncGroupResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            };
            return Reply::Now(frame_response(14, version, correlation, &response));
        };
        let code = injected.unwrap_or(if !group.members.contains_key(&request.member_id) {
            codes::UNKNOWN_MEMBER_ID
        } else if request.generation_id != group.generation {
            codes::ILLEGAL_GENERATION
        } else if group.state == Some(GroupState::PreparingRebalance) {
            codes::REBALANCE_IN_PROGRESS
        } else {
            codes::NONE
        });
        if let Some(member) = group.members.get_mut(&request.member_id) {
            member.last_seen = ctx.now();
        }
        if code != codes::NONE {
            let response = SyncGroupResponse {
                error_code: code,
                ..Default::default()
            };
            return Reply::Now(frame_response(14, version, correlation, &response));
        }
        // The leader's assignment completes a rebalance. In a stable group a
        // sync, a returning static member's among them, gets the assignment
        // the group holds.
        if group.state == Some(GroupState::CompletingRebalance)
            && group.leader.as_deref() == Some(request.member_id.as_str())
        {
            group.assignments = request
                .assignments
                .iter()
                .map(|a| (a.member_id.clone(), a.assignment.clone()))
                .collect();
            group.state = Some(GroupState::Stable);
        }
        group.syncs.push(Held {
            broker: self.node,
            reply_to: src,
            conn,
            correlation,
            version,
            member_id: request.member_id.clone(),
        });
        if group.state == Some(GroupState::Stable) {
            for held in std::mem::take(&mut group.syncs) {
                let response = SyncGroupResponse {
                    error_code: codes::NONE,
                    protocol_type: Some("consumer".to_string()),
                    protocol_name: Some("range".to_string()),
                    assignment: group
                        .assignments
                        .get(&held.member_id)
                        .cloned()
                        .unwrap_or_default(),
                    ..Default::default()
                };
                let bytes = frame_response(14, held.version, held.correlation, &response);
                ctx.send(Frame::data(
                    self.endpoint(),
                    held.reply_to,
                    held.conn,
                    bytes,
                ));
            }
        }
        Reply::Hold
    }

    fn heartbeat(&self, request: &HeartbeatRequest, now: Millis) -> HeartbeatResponse {
        let mut state = self.state.borrow_mut();
        if let Some(code) = state.knobs.heartbeat_errors.pop_front() {
            return HeartbeatResponse {
                error_code: code,
                ..Default::default()
            };
        }
        let code = match state.groups.get(&request.group_id) {
            None => codes::UNKNOWN_MEMBER_ID,
            // Kafka's `validateMember`: another member holds the instance id.
            // An instance id nobody holds is an unknown member.
            Some(group)
                if request.group_instance_id.as_ref().is_some_and(|instance| {
                    group
                        .static_members
                        .get(instance)
                        .is_some_and(|holder| *holder != request.member_id)
                }) =>
            {
                codes::FENCED_INSTANCE_ID
            }
            Some(group)
                if request
                    .group_instance_id
                    .as_ref()
                    .is_some_and(|instance| !group.static_members.contains_key(instance)) =>
            {
                codes::UNKNOWN_MEMBER_ID
            }
            Some(group) if !group.members.contains_key(&request.member_id) => {
                codes::UNKNOWN_MEMBER_ID
            }
            Some(group) if group.generation != request.generation_id => codes::ILLEGAL_GENERATION,
            Some(group) if group.state == Some(GroupState::PreparingRebalance) => {
                codes::REBALANCE_IN_PROGRESS
            }
            Some(_) => codes::NONE,
        };
        if let Some(member) = state
            .groups
            .get_mut(&request.group_id)
            .and_then(|g| g.members.get_mut(&request.member_id))
        {
            member.last_seen = now;
        }
        HeartbeatResponse {
            error_code: code,
            ..Default::default()
        }
    }

    fn leave_group(&self, request: &LeaveGroupRequest) -> LeaveGroupResponse {
        let mut state = self.state.borrow_mut();
        let ids: Vec<String> = if request.members.is_empty() {
            vec![request.member_id.clone()]
        } else {
            request
                .members
                .iter()
                .map(|m| m.member_id.clone())
                .collect()
        };
        let mut members = Vec::new();
        if let Some(group) = state.groups.get_mut(&request.group_id) {
            for id in &ids {
                let known = group.members.remove(id).is_some();
                if known && !group.members.is_empty() {
                    group.state = Some(GroupState::PreparingRebalance);
                }
                members.push(MemberResponse {
                    member_id: id.clone(),
                    group_instance_id: None,
                    error_code: if known {
                        codes::NONE
                    } else {
                        codes::UNKNOWN_MEMBER_ID
                    },
                    ..Default::default()
                });
            }
        }
        LeaveGroupResponse {
            error_code: codes::NONE,
            members,
            ..Default::default()
        }
    }

    fn offset_commit(&self, request: &OffsetCommitRequest) -> OffsetCommitResponse {
        let mut state = self.state.borrow_mut();
        let injected = state.knobs.commit_errors.pop_front();
        let group = state.groups.entry(request.group_id.clone()).or_default();
        let member_error = if injected.is_some() {
            injected
        } else if !request.member_id.is_empty()
            && !group.members.contains_key(&request.member_id)
            && !group.epochs.contains_key(&request.member_id)
        {
            Some(codes::UNKNOWN_MEMBER_ID)
        } else if group.members.contains_key(&request.member_id)
            && request.generation_id_or_member_epoch != group.generation
        {
            Some(codes::ILLEGAL_GENERATION)
        } else {
            None
        };
        let topics = request
            .topics
            .iter()
            .map(|t| OffsetCommitResponseTopic {
                name: t.name.clone(),
                topic_id: t.topic_id,
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| {
                        let code = member_error.unwrap_or(codes::NONE);
                        if code == codes::NONE {
                            group.committed.insert(
                                (t.name.clone(), p.partition_index),
                                (p.committed_offset, p.committed_leader_epoch),
                            );
                        }
                        OffsetCommitResponsePartition {
                            partition_index: p.partition_index,
                            error_code: code,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        OffsetCommitResponse {
            topics,
            ..Default::default()
        }
    }

    fn offset_fetch(&self, request: &OffsetFetchRequest) -> OffsetFetchResponse {
        let state = self.state.borrow();
        let lookup = |group_id: &str, name: &str, partition: i32| {
            state
                .groups
                .get(group_id)
                .and_then(|g| g.committed.get(&(name.to_string(), partition)))
                .copied()
                .unwrap_or((-1, -1))
        };
        let topics = request
            .topics
            .iter()
            .flatten()
            .map(|t| OffsetFetchResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partition_indexes
                    .iter()
                    .map(|p| {
                        let (offset, epoch) = lookup(&request.group_id, &t.name, *p);
                        OffsetFetchResponsePartition {
                            partition_index: *p,
                            committed_offset: offset,
                            committed_leader_epoch: epoch,
                            metadata: Some(String::new()),
                            error_code: codes::NONE,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        let groups = request
            .groups
            .iter()
            .map(|g| OffsetFetchResponseGroup {
                group_id: g.group_id.clone(),
                topics: g
                    .topics
                    .iter()
                    .flatten()
                    .map(|t| OffsetFetchResponseTopics {
                        name: t.name.clone(),
                        topic_id: t.topic_id,
                        partitions: t
                            .partition_indexes
                            .iter()
                            .map(|p| {
                                let (offset, epoch) = lookup(&g.group_id, &t.name, *p);
                                OffsetFetchResponsePartitions {
                                    partition_index: *p,
                                    committed_offset: offset,
                                    committed_leader_epoch: epoch,
                                    metadata: Some(String::new()),
                                    error_code: codes::NONE,
                                    ..Default::default()
                                }
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                error_code: codes::NONE,
                ..Default::default()
            })
            .collect();
        OffsetFetchResponse {
            topics,
            error_code: codes::NONE,
            groups,
            ..Default::default()
        }
    }

    // ---- KIP-848 ----------------------------------------------------------------

    fn consumer_group_heartbeat(
        &self,
        request: &ConsumerGroupHeartbeatRequest,
    ) -> ConsumerGroupHeartbeatResponse {
        let mut state = self.state.borrow_mut();
        let scripted = state.knobs.cgh_script.pop_front();
        let assignment_of = |state: &ClusterState, topics: &[(String, Vec<i32>)]| {
            Assignment {
            topic_partitions: topics
                .iter()
                .filter_map(|(name, partitions)| {
                    state.topics.get(name).map(|t| {
                        krabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions {
                            topic_id: t.id,
                            partitions: partitions.clone(),
                            ..Default::default()
                        }
                    })
                })
                .collect(),
            ..Default::default()
        }
        };
        if let Some(answer) = scripted {
            let assignment = answer.assignment.as_ref().map(|a| assignment_of(&state, a));
            let group = state.groups.entry(request.group_id.clone()).or_default();
            group
                .epochs
                .insert(request.member_id.clone(), answer.member_epoch);
            return ConsumerGroupHeartbeatResponse {
                error_code: answer.error_code,
                member_id: Some(request.member_id.clone()),
                member_epoch: answer.member_epoch,
                heartbeat_interval_ms: 3_000,
                assignment,
                ..Default::default()
            };
        }
        if request.member_epoch < 0 {
            let group = state.groups.entry(request.group_id.clone()).or_default();
            group.epochs.remove(&request.member_id);
            return ConsumerGroupHeartbeatResponse {
                error_code: codes::NONE,
                member_id: Some(request.member_id.clone()),
                member_epoch: request.member_epoch,
                heartbeat_interval_ms: 3_000,
                ..Default::default()
            };
        }
        // The default assignor: a joining member takes every partition of
        // its subscribed topics at the next epoch.
        let subscribed: Vec<(String, Vec<i32>)> = request
            .subscribed_topic_names
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|name| {
                state
                    .topics
                    .get(&name)
                    .map(|t| (name.clone(), t.partitions.keys().copied().collect()))
            })
            .collect();
        let assignment = (request.member_epoch == 0).then(|| assignment_of(&state, &subscribed));
        let group = state.groups.entry(request.group_id.clone()).or_default();
        let epoch = if request.member_epoch == 0 {
            group.group_epoch += 1;
            group.group_epoch
        } else {
            group
                .epochs
                .get(&request.member_id)
                .copied()
                .unwrap_or(request.member_epoch)
        };
        group.epochs.insert(request.member_id.clone(), epoch);
        ConsumerGroupHeartbeatResponse {
            error_code: codes::NONE,
            member_id: Some(request.member_id.clone()),
            member_epoch: epoch,
            heartbeat_interval_ms: 3_000,
            assignment,
            ..Default::default()
        }
    }
}

impl Node for FakeBroker {
    fn kind(&self) -> &'static str {
        "fake-broker"
    }

    fn start(&mut self, _ctx: &mut Ctx<'_>) {
        self.started += 1;
        self.conns.clear();
        self.delayed.clear();
        self.held.clear();
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if frame.dst != self.endpoint() {
            return;
        }
        match &frame.payload {
            Payload::Open => {
                self.conns.insert((frame.src, frame.conn));
            }
            Payload::Close => {
                self.conns.remove(&(frame.src, frame.conn));
                self.held
                    .retain(|h| !(h.reply_to == frame.src && h.conn == frame.conn));
            }
            Payload::Data(bytes) => {
                if !self.conns.contains(&(frame.src, frame.conn)) {
                    return;
                }
                let bytes = bytes.clone();
                self.handle(ctx, frame.src, frame.conn, &bytes);
            }
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let mut due: Vec<Delayed> = Vec::new();
        self.delayed.retain(|d| {
            if d.at <= now {
                due.push(Delayed {
                    at: d.at,
                    seq: d.seq,
                    frame: d.frame.clone(),
                });
                false
            } else {
                true
            }
        });
        due.sort_by_key(|d| (d.at, d.seq));
        for d in due {
            ctx.send(d.frame);
        }
        self.expire_members(ctx);
        self.release_held(ctx);
    }

    fn control(&mut self, _ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        Err(format!("the fake broker has no commands: {command}"))
    }

    fn snapshot(&self) -> Value {
        json!({ "broker_id": self.broker_id, "connections": self.conns.len() })
    }
}
