//! A lab share-group member. The real broker assigns partitions, acquires
//! records, counts deliveries and persists Accept/Release/Reject acknowledgements.
//! One bounded fetch is processed at a time; heartbeat and per-broker share
//! session epochs are independent. Close releases unfinished work before leaving.
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use krabka_protocol::{
    owned::{
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch, ShareAcknowledgeRequest,
        },
        share_acknowledge_response::ShareAcknowledgeResponse,
        share_fetch_request::{FetchPartition, FetchTopic, ForgottenTopic, ShareFetchRequest},
        share_fetch_response::{AcquiredRecords, ShareFetchResponse},
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    primitives::uuid::Uuid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{Processing, serde::preview};
use crate::lab::{
    LabError,
    client::{
        ClientError, ClientEvent, ClientOptions, ConsumedRecord, CoordinatorType, KafkaClient,
        RequestId, Response, Target, records_of, uuid_hex,
    },
    codes,
    net::{Ctx, DurableImage, DurableOp, Endpoint, Frame, Millis, Node, NodeId},
    scenario::NodeSpec,
};

const RETRY_MS: Millis = 500;
const POLL_MS: Millis = 100;
const MAX_BYTES: i32 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Acknowledgement {
    #[default]
    Accept,
    Release,
    Reject,
}
impl Acknowledgement {
    fn wire(self) -> i8 {
        match self {
            Self::Accept => 1,
            Self::Release => 2,
            Self::Reject => 3,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    bootstrap: Vec<NodeId>,
    group: String,
    topics: Vec<String>,
    #[serde(default)]
    process_ms: Millis,
    #[serde(default = "default_max_records")]
    max_records: i32,
    #[serde(default)]
    acknowledgement: Acknowledgement,
}
fn default_max_records() -> i32 {
    10
}

type TopicPartition = ([u8; 16], i32);
#[derive(Default)]
struct Session {
    epoch: i32,
    partitions: BTreeSet<TopicPartition>,
}
impl Session {
    fn advance(&mut self) {
        self.epoch = if self.epoch == i32::MAX {
            1
        } else {
            self.epoch + 1
        };
    }
}

struct Delivery {
    record: ConsumedRecord,
    count: i16,
    acknowledgement: Option<Acknowledgement>,
}
struct PartitionBatch {
    topic: String,
    id: Uuid,
    partition: i32,
    ranges: Vec<AcquiredRecords>,
    records: Vec<Delivery>,
}
struct Batch {
    broker: i32,
    partitions: Vec<PartitionBatch>,
}
impl Batch {
    fn complete(&self) -> bool {
        self.partitions
            .iter()
            .flat_map(|p| &p.records)
            .all(|r| r.acknowledgement.is_some())
    }
    fn topics(&self) -> Vec<AcknowledgeTopic> {
        let mut topics: BTreeMap<[u8; 16], Vec<AcknowledgePartition>> = BTreeMap::new();
        for p in &self.partitions {
            // A single type covers a contiguous range. Missing/control offsets
            // are gaps (0); unprocessed records are released on close (2).
            let mut batches = Vec::new();
            for range in &p.ranges {
                let mut at = range.first_offset;
                for record in p.records.iter().filter(|r| {
                    range.first_offset <= r.record.offset && r.record.offset <= range.last_offset
                }) {
                    if at < record.record.offset {
                        batches.push(ack_batch(at, record.record.offset - 1, 0));
                    }
                    batches.push(ack_batch(
                        record.record.offset,
                        record.record.offset,
                        record
                            .acknowledgement
                            .unwrap_or(Acknowledgement::Release)
                            .wire(),
                    ));
                    at = record.record.offset + 1;
                }
                if at <= range.last_offset {
                    batches.push(ack_batch(at, range.last_offset, 0));
                }
            }
            topics
                .entry(p.id.0)
                .or_default()
                .push(AcknowledgePartition {
                    partition_index: p.partition,
                    acknowledgement_batches: batches,
                    ..Default::default()
                });
        }
        topics
            .into_iter()
            .map(|(topic_id, partitions)| AcknowledgeTopic {
                topic_id: Uuid(topic_id),
                partitions,
                ..Default::default()
            })
            .collect()
    }
}
fn ack_batch(first_offset: i64, last_offset: i64, kind: i8) -> AcknowledgementBatch {
    AcknowledgementBatch {
        first_offset,
        last_offset,
        acknowledge_types: vec![kind],
        ..Default::default()
    }
}

enum Step {
    Heartbeat,
    Fetch {
        broker: i32,
        partitions: BTreeSet<TopicPartition>,
    },
    Acknowledge {
        broker: i32,
        final_session: bool,
        topics: Vec<AcknowledgeTopic>,
    },
    Leave,
}
impl Step {
    fn broker(&self) -> Option<i32> {
        match self {
            Self::Fetch { broker, .. } | Self::Acknowledge { broker, .. } => Some(*broker),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Running,
    Closing,
    Closed,
    Failed,
}

/// Browser share consumer using the lab's existing framed Kafka client.
pub struct ShareConsumerNode {
    id: NodeId,
    config: Config,
    client: KafkaClient,
    member: String,
    incarnation: u64,
    epoch: i32,
    heartbeat_at: Millis,
    heartbeat_ms: Millis,
    assignment: BTreeSet<TopicPartition>,
    sessions: BTreeMap<i32, Session>,
    pending: BTreeMap<RequestId, Step>,
    batch: Option<Batch>,
    processing: Processing,
    next_poll: Millis,
    cursor: i32,
    paused: bool,
    phase: Phase,
    processed: u64,
    fetched: u64,
    redelivered: u64,
    acknowledged: [u64; 3],
    last_records: VecDeque<Value>,
    errors: VecDeque<String>,
    quiet: BTreeMap<String, Millis>,
}

impl ShareConsumerNode {
    /// # Errors
    /// Rejects missing/unknown configuration and invalid polling bounds.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let config: Config = serde_json::from_value(spec.config.clone())
            .map_err(|e| LabError::config(spec, e.to_string()))?;
        if config.bootstrap.is_empty()
            || config.group.is_empty()
            || config.topics.is_empty()
            || config.topics.iter().any(String::is_empty)
            || !(1..=1000).contains(&config.max_records)
        {
            return Err(LabError::config(
                spec,
                "bootstrap, group and topics must be nonempty; max_records must be 1 through 1000",
            ));
        }
        Ok(Self {
            id: spec.id,
            client: client(&config, spec.id),
            config,
            member: String::new(),
            incarnation: 0,
            epoch: 0,
            heartbeat_at: 0,
            heartbeat_ms: 5000,
            assignment: BTreeSet::new(),
            sessions: BTreeMap::new(),
            pending: BTreeMap::new(),
            batch: None,
            processing: Processing::default(),
            next_poll: 0,
            cursor: -1,
            paused: false,
            phase: Phase::Running,
            processed: 0,
            fetched: 0,
            redelivered: 0,
            acknowledged: [0; 3],
            last_records: VecDeque::new(),
            errors: VecDeque::new(),
            quiet: BTreeMap::new(),
        })
    }
    fn new_member(&mut self, ctx: &mut Ctx<'_>) {
        self.member = uuid::Uuid::from_u128(
            ((u128::from(ctx.rand(u64::MAX)) << 64) | u128::from(ctx.rand(u64::MAX)))
                ^ u128::from(self.incarnation),
        )
        .to_string();
        self.epoch = 0;
        self.assignment.clear();
        self.sessions.clear();
        self.pending.clear();
        self.batch = None;
        self.processing = Processing::default();
        self.heartbeat_at = ctx.now() + RETRY_MS;
    }
    fn coordinator(&self) -> Target {
        Target::Coordinator {
            key_type: CoordinatorType::Group,
            key: self.config.group.clone(),
        }
    }
    fn error(&mut self, ctx: &mut Ctx<'_>, api: &str, message: String, fatal: bool) {
        if fatal {
            self.phase = Phase::Failed;
        }
        let key = format!("{api}: {message}");
        if self
            .quiet
            .get(&key)
            .is_none_or(|at| ctx.now().saturating_sub(*at) >= 5000)
        {
            ctx.event(
                "share_consumer_error",
                json!({ "api": api, "message": Value::String(message), "level": "warn" }),
            );
            // Both displayed errors and deduplication keys are bounded.
            if self.quiet.len() >= 32 {
                self.quiet.clear();
            }
            self.quiet.insert(key.clone(), ctx.now());
            self.errors.push_back(key);
            while self.errors.len() > 10 {
                self.errors.pop_front();
            }
        }
    }
    fn broker_error(&mut self, ctx: &mut Ctx<'_>, api: &str, code: i16) {
        if matches!(
            code,
            codes::COORDINATOR_NOT_AVAILABLE | codes::NOT_COORDINATOR
        ) {
            self.client
                .invalidate_coordinator(CoordinatorType::Group, &self.config.group);
        }
        self.client.request_metadata_refresh();
        self.error(
            ctx,
            api,
            format!("broker error {code}"),
            matches!(
                code,
                codes::INCONSISTENT_GROUP_PROTOCOL
                    | codes::INVALID_GROUP_ID
                    | codes::TOPIC_AUTHORIZATION_FAILED
                    | codes::GROUP_AUTHORIZATION_FAILED
                    | codes::CLUSTER_AUTHORIZATION_FAILED
                    | codes::UNSUPPORTED_VERSION
            ),
        );
    }
    fn reset_session(&mut self, broker: i32) {
        self.sessions.remove(&broker);
        self.pending.retain(|_, s| s.broker() != Some(broker));
        if self.batch.as_ref().is_some_and(|b| b.broker == broker) {
            self.batch = None;
            self.processing = Processing::default();
        }
    }
    fn heartbeat(&mut self, ctx: &mut Ctx<'_>, leave: bool) {
        let id = self.client.send(
            ctx,
            self.coordinator(),
            ShareGroupHeartbeatRequest {
                group_id: self.config.group.clone(),
                member_id: self.member.clone(),
                member_epoch: if leave { -1 } else { self.epoch },
                subscribed_topic_names: (!leave && self.epoch == 0)
                    .then(|| self.config.topics.clone()),
                ..Default::default()
            },
        );
        self.pending
            .insert(id, if leave { Step::Leave } else { Step::Heartbeat });
        self.heartbeat_at = ctx.now() + self.heartbeat_ms;
    }
    fn acknowledge(&mut self, ctx: &mut Ctx<'_>, broker: i32, final_session: bool) {
        let topics = self
            .batch
            .as_ref()
            .filter(|b| b.broker == broker)
            .map_or_else(Vec::new, Batch::topics);
        let epoch = self.sessions.get(&broker).map_or(0, |s| s.epoch);
        let id = self.client.send(
            ctx,
            Target::Broker(broker),
            ShareAcknowledgeRequest {
                group_id: Some(self.config.group.clone()),
                member_id: Some(self.member.clone()),
                share_session_epoch: if final_session { -1 } else { epoch },
                topics: topics.clone(),
                ..Default::default()
            },
        );
        self.pending.insert(
            id,
            Step::Acknowledge {
                broker,
                final_session,
                topics,
            },
        );
    }
    fn fetch(&mut self, ctx: &mut Ctx<'_>) {
        self.next_poll = ctx.now() + POLL_MS;
        let mut by_broker: BTreeMap<i32, BTreeSet<TopicPartition>> = BTreeMap::new();
        for (id, partition) in &self.assignment {
            let info = self
                .client
                .metadata()
                .topics
                .values()
                .find(|t| t.topic_id.0 == *id)
                .and_then(|t| t.partitions.get(partition));
            if let Some(p) = info.filter(|p| p.error_code == 0 && p.leader >= 0) {
                by_broker
                    .entry(p.leader)
                    .or_default()
                    .insert((*id, *partition));
            } else {
                self.client.request_metadata_refresh();
            }
        }
        // Keep old sessions in the cycle so removed partitions are forgotten.
        for broker in self.sessions.keys() {
            by_broker.entry(*broker).or_default();
        }
        let Some(broker) = by_broker
            .keys()
            .find(|b| **b > self.cursor)
            .or_else(|| by_broker.keys().next())
            .copied()
        else {
            return;
        };
        self.cursor = broker;
        let partitions = by_broker.remove(&broker).unwrap();
        if partitions.is_empty() {
            if self.sessions.get(&broker).is_some_and(|s| s.epoch > 0) {
                self.acknowledge(ctx, broker, true);
            } else {
                self.sessions.remove(&broker);
            }
            return;
        }
        let session = self.sessions.entry(broker).or_default();
        let mut topics: BTreeMap<[u8; 16], Vec<FetchPartition>> = BTreeMap::new();
        for (id, partition) in &partitions {
            topics.entry(*id).or_default().push(FetchPartition {
                partition_index: *partition,
                partition_max_bytes: MAX_BYTES,
                ..Default::default()
            });
        }
        let mut forgotten: BTreeMap<[u8; 16], Vec<i32>> = BTreeMap::new();
        for (id, p) in session.partitions.difference(&partitions) {
            forgotten.entry(*id).or_default().push(*p);
        }
        let request = ShareFetchRequest {
            group_id: Some(self.config.group.clone()),
            member_id: Some(self.member.clone()),
            share_session_epoch: session.epoch,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: MAX_BYTES,
            max_records: self.config.max_records,
            batch_size: self.config.max_records,
            topics: topics
                .into_iter()
                .map(|(topic_id, partitions)| FetchTopic {
                    topic_id: Uuid(topic_id),
                    partitions,
                    ..Default::default()
                })
                .collect(),
            forgotten_topics_data: forgotten
                .into_iter()
                .map(|(topic_id, partitions)| ForgottenTopic {
                    topic_id: Uuid(topic_id),
                    partitions,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let id = self.client.send(ctx, Target::Broker(broker), request);
        self.pending.insert(id, Step::Fetch { broker, partitions });
    }
    fn response(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: RequestId,
        result: Result<Response, ClientError>,
    ) {
        let Some(step) = self.pending.remove(&id) else {
            return;
        };
        let response = match result {
            Ok(r) => r,
            Err(e) => {
                if let Some(broker) = step.broker() {
                    self.reset_session(broker);
                }
                self.heartbeat_at = ctx.now() + RETRY_MS;
                self.next_poll = ctx.now() + RETRY_MS;
                self.error(
                    ctx,
                    "request",
                    e.to_string(),
                    matches!(
                        e,
                        ClientError::UnsupportedVersion { .. } | ClientError::Protocol(_)
                    ),
                );
                return;
            }
        };
        match step {
            Step::Heartbeat => {
                if let Some(r) = response.downcast::<ShareGroupHeartbeatResponse>() {
                    self.on_heartbeat(ctx, r);
                }
            }
            Step::Fetch { broker, partitions } => {
                if let Some(r) = response.downcast::<ShareFetchResponse>() {
                    self.on_fetch(ctx, broker, partitions, r);
                }
            }
            Step::Acknowledge {
                broker,
                final_session,
                topics,
            } => {
                if let Some(r) = response.downcast::<ShareAcknowledgeResponse>() {
                    self.on_acknowledge(ctx, broker, final_session, &topics, &r);
                }
            }
            Step::Leave => {
                if let Some(r) = response.downcast::<ShareGroupHeartbeatResponse>() {
                    if matches!(
                        r.error_code,
                        codes::NONE
                            | codes::UNKNOWN_MEMBER_ID
                            | codes::FENCED_MEMBER_EPOCH
                            | codes::STALE_MEMBER_EPOCH
                    ) {
                        self.finish_close(ctx);
                    } else {
                        self.heartbeat_at = ctx.now() + RETRY_MS;
                        self.broker_error(ctx, "ShareGroupHeartbeat leave", r.error_code);
                    }
                }
            }
        }
    }
    fn on_heartbeat(&mut self, ctx: &mut Ctx<'_>, r: ShareGroupHeartbeatResponse) {
        if r.error_code != 0 {
            if matches!(
                r.error_code,
                codes::UNKNOWN_MEMBER_ID | codes::FENCED_MEMBER_EPOCH | codes::STALE_MEMBER_EPOCH
            ) {
                self.new_member(ctx);
            }
            self.heartbeat_at = ctx.now() + RETRY_MS;
            self.broker_error(ctx, "ShareGroupHeartbeat", r.error_code);
            return;
        }
        if r.member_epoch <= 0 {
            self.error(
                ctx,
                "ShareGroupHeartbeat",
                "nonpositive member epoch".into(),
                true,
            );
            return;
        }
        if self.epoch == 0 {
            ctx.event(
                "share_group_joined",
                json!({ "group": self.config.group, "member_id": self.member }),
            );
        }
        self.epoch = r.member_epoch;
        self.heartbeat_ms = u64::try_from(r.heartbeat_interval_ms)
            .unwrap_or(5000)
            .max(1);
        self.heartbeat_at = ctx.now() + self.heartbeat_ms;
        if let Some(a) = r.assignment {
            self.assignment = a
                .topic_partitions
                .into_iter()
                .flat_map(|t| t.partitions.into_iter().map(move |p| (t.topic_id.0, p)))
                .collect();
            ctx.event(
                "share_partitions_assigned",
                json!({ "partitions": self.assignment.len(), "member_epoch": self.epoch }),
            );
            self.client.request_metadata_refresh();
        }
    }
    fn on_fetch(
        &mut self,
        ctx: &mut Ctx<'_>,
        broker: i32,
        partitions: BTreeSet<TopicPartition>,
        r: ShareFetchResponse,
    ) {
        if r.error_code != 0 {
            self.reset_session(broker);
            self.next_poll = ctx.now() + RETRY_MS;
            self.broker_error(ctx, "ShareFetch", r.error_code);
            return;
        }
        let session = self.sessions.entry(broker).or_default();
        session.advance();
        session.partitions = partitions;
        let mut batch = Batch {
            broker,
            partitions: Vec::new(),
        };
        for topic in r.responses {
            let Some(name) = self
                .client
                .metadata()
                .topics
                .values()
                .find(|t| t.topic_id == topic.topic_id)
                .map(|t| t.name.clone())
            else {
                self.reset_session(broker);
                self.client.request_metadata_refresh();
                return;
            };
            for p in topic.partitions {
                if p.error_code != 0 {
                    self.broker_error(ctx, "ShareFetch", p.error_code);
                    continue;
                }
                if p.acquired_records.is_empty() {
                    continue;
                }
                if p.acquired_records.iter().any(|a| {
                    a.first_offset < 0
                        || a.last_offset < a.first_offset
                        || a.last_offset == i64::MAX
                        || a.delivery_count <= 0
                }) {
                    self.reset_session(broker);
                    self.error(ctx, "ShareFetch", "invalid acquired range".into(), true);
                    return;
                }
                let Some(raw) = p.records.as_ref().and_then(|r| r.as_v2()) else {
                    self.reset_session(broker);
                    self.error(
                        ctx,
                        "ShareFetch",
                        "acquired records without record batches".into(),
                        true,
                    );
                    return;
                };
                let (records, _) = records_of(&name, p.partition_index, raw, i64::MIN, None);
                let deliveries: Vec<_> = records
                    .into_iter()
                    .filter_map(|record| {
                        let range = p.acquired_records.iter().find(|a| {
                            a.first_offset <= record.offset && record.offset <= a.last_offset
                        })?;
                        Some(Delivery {
                            record,
                            count: range.delivery_count,
                            acknowledgement: None,
                        })
                    })
                    .collect();
                let total = batch
                    .partitions
                    .iter()
                    .map(|p| p.records.len())
                    .sum::<usize>()
                    + deliveries.len();
                if total > 1000 {
                    self.reset_session(broker);
                    self.error(
                        ctx,
                        "ShareFetch",
                        "broker exceeded the lab record bound".into(),
                        true,
                    );
                    return;
                }
                self.fetched += deliveries.len() as u64;
                self.redelivered += deliveries.iter().filter(|d| d.count > 1).count() as u64;
                self.processing
                    .accept(deliveries.iter().map(|d| d.record.clone()));
                batch.partitions.push(PartitionBatch {
                    topic: name.clone(),
                    id: topic.topic_id,
                    partition: p.partition_index,
                    ranges: p.acquired_records,
                    records: deliveries,
                });
            }
        }
        if !batch.partitions.is_empty() {
            self.batch = Some(batch);
        }
    }
    fn on_acknowledge(
        &mut self,
        ctx: &mut Ctx<'_>,
        broker: i32,
        final_session: bool,
        topics: &[AcknowledgeTopic],
        r: &ShareAcknowledgeResponse,
    ) {
        let expected: BTreeSet<_> = topics
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |p| (t.topic_id.0, p.partition_index))
            })
            .collect();
        let actual: BTreeSet<_> = r
            .responses
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .filter(|p| p.error_code == 0)
                    .map(move |p| (t.topic_id.0, p.partition_index))
            })
            .collect();
        let code = r
            .responses
            .iter()
            .flat_map(|t| &t.partitions)
            .find(|p| p.error_code != 0)
            .map_or(r.error_code, |p| p.error_code);
        if code != 0 || expected != actual {
            self.reset_session(broker);
            self.next_poll = ctx.now() + RETRY_MS;
            self.broker_error(ctx, "ShareAcknowledge", if code == 0 { -1 } else { code });
            return;
        }
        if let Some(batch) = self.batch.take() {
            for delivery in batch.partitions.iter().flat_map(|p| &p.records) {
                let kind = delivery.acknowledgement.unwrap_or(Acknowledgement::Release);
                self.acknowledged[usize::try_from(kind.wire() - 1)
                    .expect("acknowledgement type 1 through 3")] += 1;
            }
            ctx.event("share_acknowledged", json!({ "broker": broker, "accepted": self.acknowledged[0], "released": self.acknowledged[1], "rejected": self.acknowledged[2] }));
        }
        if final_session {
            self.sessions.remove(&broker);
        } else if let Some(s) = self.sessions.get_mut(&broker) {
            s.advance();
        }
    }
    fn finish_close(&mut self, ctx: &mut Ctx<'_>) {
        self.phase = Phase::Closed;
        self.assignment.clear();
        self.client.close(ctx);
        ctx.event("share_group_left", json!({ "group": self.config.group }));
    }
    fn drive(&mut self, ctx: &mut Ctx<'_>, events: Vec<ClientEvent>) {
        for event in events {
            if let ClientEvent::Response { id, result } = event {
                self.response(ctx, id, result);
            }
        }
        if matches!(self.phase, Phase::Closed | Phase::Failed) {
            return;
        }
        let now = ctx.now();
        if self.phase != Phase::Closing {
            let done = self
                .processing
                .advance(now, self.config.process_ms, !self.paused, |_| true);
            for record in done {
                if let Some(delivery) = self
                    .batch
                    .as_mut()
                    .and_then(|b| {
                        b.partitions
                            .iter_mut()
                            .find(|p| p.topic == record.topic && p.partition == record.partition)
                    })
                    .and_then(|p| {
                        p.records
                            .iter_mut()
                            .find(|r| r.record.offset == record.offset)
                    })
                {
                    delivery.acknowledgement = Some(self.config.acknowledgement);
                    self.processed += 1;
                    self.last_records.push_back(json!({ "topic": record.topic, "partition": record.partition, "offset": record.offset, "delivery_count": delivery.count, "acknowledgement": delivery.acknowledgement, "key": record.key.as_deref().map(preview), "value": record.value.as_deref().map(preview) }));
                    while self.last_records.len() > 10 {
                        self.last_records.pop_front();
                    }
                }
            }
        }
        let data_pending = self.pending.values().any(|s| s.broker().is_some());
        if !data_pending
            && let Some(b) = &self.batch
            && (self.phase == Phase::Closing || b.complete())
        {
            self.acknowledge(ctx, b.broker, self.phase == Phase::Closing);
        }
        if self.phase == Phase::Closing {
            if self.pending.is_empty() && self.batch.is_none() {
                let brokers: Vec<_> = self
                    .sessions
                    .iter()
                    .filter(|(_, s)| s.epoch > 0)
                    .map(|(b, _)| *b)
                    .collect();
                if !brokers.is_empty() {
                    for broker in brokers {
                        self.acknowledge(ctx, broker, true);
                    }
                } else if self.epoch > 0 {
                    if now >= self.heartbeat_at {
                        self.heartbeat(ctx, true);
                    }
                } else {
                    self.finish_close(ctx);
                    return;
                }
            }
        } else {
            if now >= self.heartbeat_at
                && !self.pending.values().any(|s| matches!(s, Step::Heartbeat))
            {
                self.heartbeat(ctx, false);
            }
            if self.epoch > 0
                && !self.paused
                && self.batch.is_none()
                && !data_pending
                && now >= self.next_poll
            {
                self.fetch(ctx);
            }
        }
        self.arm(ctx);
    }
    fn arm(&self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let heartbeat = ((self.phase != Phase::Closing
            || (self.batch.is_none() && self.sessions.is_empty()))
            && !self
                .pending
                .values()
                .any(|s| matches!(s, Step::Heartbeat | Step::Leave)))
        .then_some(self.heartbeat_at);
        let poll = (self.phase != Phase::Closing
            && !self.paused
            && self.epoch > 0
            && self.batch.is_none()
            && !self.pending.values().any(|s| s.broker().is_some()))
        .then_some(self.next_poll);
        let finish = self
            .processing
            .next_finish()
            .filter(|_| self.phase != Phase::Closing);
        if let Some(at) = [self.client.next_deadline(now), heartbeat, poll, finish]
            .into_iter()
            .flatten()
            .min()
        {
            ctx.arm(at.max(now));
        }
    }
}
fn client(config: &Config, id: NodeId) -> KafkaClient {
    let mut client = KafkaClient::new(
        config
            .bootstrap
            .iter()
            .map(|id| Endpoint::kafka(*id))
            .collect(),
        &format!("share-consumer-{id}"),
        ClientOptions::default(),
    );
    client.add_topics(config.topics.iter().map(String::as_str));
    client
}
impl Node for ShareConsumerNode {
    fn kind(&self) -> &'static str {
        "share-consumer"
    }
    fn load(&mut self, image: DurableImage) {
        self.incarnation = image
            .kv
            .get("share-consumer")
            .and_then(|s| s.get("incarnation"))
            .and_then(|v| v.0.as_ref().try_into().ok())
            .map_or(0, u64::from_be_bytes);
    }
    fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.phase = Phase::Running;
        let Some(incarnation) = self.incarnation.checked_add(1) else {
            self.error(ctx, "startup", "member incarnation exhausted".into(), true);
            return;
        };
        self.incarnation = incarnation;
        ctx.persist(DurableOp::Put {
            store: "share-consumer".into(),
            key: "incarnation".into(),
            value: bytes::Bytes::copy_from_slice(&incarnation.to_be_bytes()),
        });
        self.client = client(&self.config, self.id);
        self.new_member(ctx);
        self.heartbeat_at = ctx.now();
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }
    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        let events = self.client.on_frame(ctx, frame);
        self.drive(ctx, events);
    }
    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }
    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        match command.get("cmd").and_then(Value::as_str) {
            Some("pause") => self.paused = true,
            Some("resume") => self.paused = false,
            Some("process_ms") => {
                self.config.process_ms = command
                    .get("ms")
                    .and_then(Value::as_u64)
                    .ok_or("ms must be a nonnegative integer")?;
            }
            Some("acknowledgement") => {
                self.config.acknowledgement =
                    serde_json::from_value(command.get("type").cloned().unwrap_or(Value::Null))
                        .map_err(|e| e.to_string())?;
            }
            Some("close") => {
                if self.phase != Phase::Closed {
                    self.phase = Phase::Closing;
                }
                self.processing = Processing::default();
                self.heartbeat_at = ctx.now();
            }
            _ => return Err("unknown share consumer command".to_owned()),
        }
        self.drive(ctx, Vec::new());
        Ok(
            json!({ "paused": self.paused, "closing": self.phase == Phase::Closing, "closed": self.phase == Phase::Closed, "acknowledgement": self.config.acknowledgement }),
        )
    }
    fn snapshot(&self) -> Value {
        let assignment: Vec<_> = self.assignment.iter().map(|(id, p)| json!({ "topic": self.client.metadata().topics.values().find(|t| t.topic_id.0 == *id).map(|t| &t.name), "topic_id": uuid_hex(Uuid(*id)), "partition": p })).collect();
        let sessions: Vec<_> = self
            .sessions
            .iter()
            .map(|(broker, s)| json!({ "broker": broker, "epoch": s.epoch }))
            .collect();
        json!({ "state": if self.phase == Phase::Closed { "closed" } else if self.phase == Phase::Closing { "closing" } else if self.phase == Phase::Failed { "error" } else if self.epoch > 0 { "stable" } else { "joining" },
            "group": self.config.group, "member_id": self.member, "member_epoch": self.epoch, "subscription": self.config.topics, "assignment": assignment, "sessions": sessions,
            "paused": self.paused, "closed": self.phase == Phase::Closed, "process_ms": self.config.process_ms, "max_records": self.config.max_records, "acknowledgement": self.config.acknowledgement,
            "processed": self.processed, "records": self.fetched, "processing_backlog": self.processing.unprocessed(), "accepted": self.acknowledged[0], "released": self.acknowledged[1], "rejected": self.acknowledged[2], "redelivered": self.redelivered,
            "last_records": self.last_records, "errors": self.errors, "client": self.client.snapshot() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lab::testing::CtxBuffers;
    fn node() -> ShareConsumerNode {
        ShareConsumerNode::from_spec(&NodeSpec::new(
            3,
            "share-consumer",
            "",
            json!({"bootstrap":[1],"group":"workers","topics":["jobs"],"process_ms":100}),
        ))
        .unwrap()
    }
    fn delivery(offset: i64, acknowledgement: Option<Acknowledgement>) -> Delivery {
        Delivery {
            record: ConsumedRecord {
                topic: "jobs".into(),
                partition: 0,
                offset,
                timestamp: 0,
                key: None,
                value: Some(bytes::Bytes::from_static(b"job")),
                headers: Vec::new(),
                leader_epoch: 1,
            },
            count: 2,
            acknowledgement,
        }
    }
    fn batch() -> Batch {
        Batch {
            broker: 1,
            partitions: vec![PartitionBatch {
                topic: "jobs".into(),
                id: Uuid([1; 16]),
                partition: 0,
                ranges: vec![AcquiredRecords {
                    first_offset: 2,
                    last_offset: 8,
                    delivery_count: 2,
                    ..Default::default()
                }],
                records: vec![
                    delivery(3, Some(Acknowledgement::Accept)),
                    delivery(7, None),
                ],
            }],
        }
    }
    #[test]
    fn acknowledgements_preserve_gaps_completed_work_and_unfinished_work() {
        let mut b = batch();
        let topics = b.topics();
        let batches = &topics[0].partitions[0].acknowledgement_batches;
        let ledger: Vec<_> = batches
            .iter()
            .map(|a| (a.first_offset, a.last_offset, a.acknowledge_types.clone()))
            .collect();
        assert_eq!(
            ledger,
            vec![
                (2, 2, vec![0]),
                (3, 3, vec![1]),
                (4, 6, vec![0]),
                (7, 7, vec![2]),
                (8, 8, vec![0])
            ]
        );
        assert!(!b.complete());
        b.partitions[0].records[1].acknowledgement = Some(Acknowledgement::Reject);
        assert!(b.complete());
        assert_eq!(
            b.topics()[0].partitions[0].acknowledgement_batches[3].acknowledge_types,
            [3]
        );
    }
    #[test]
    fn only_finished_records_are_accepted_and_failed_acks_never_count_as_success() {
        let mut n = node();
        let mut ctx = CtxBuffers::new(NodeId(3));
        n.epoch = 1;
        n.heartbeat_at = 5000;
        n.next_poll = 5000;
        let mut b = batch();
        b.partitions[0].ranges[0].first_offset = 3;
        b.partitions[0].ranges[0].last_offset = 3;
        b.partitions[0].records = vec![delivery(3, None)];
        n.processing
            .accept(b.partitions[0].records.iter().map(|d| d.record.clone()));
        n.batch = Some(b);
        n.sessions.insert(
            1,
            Session {
                epoch: 1,
                ..Default::default()
            },
        );
        ctx.with(0, |ctx| n.drive(ctx, Vec::new()));
        ctx.with(99, |ctx| n.drive(ctx, Vec::new()));
        assert_eq!(n.processed, 0);
        assert!(n.pending.is_empty());
        ctx.with(100, |ctx| n.drive(ctx, Vec::new()));
        assert_eq!(n.processed, 1);
        assert_eq!(n.acknowledged, [0; 3]);
        let id = *n.pending.keys().next().unwrap();
        match &n.pending[&id] {
            Step::Acknowledge {
                topics,
                final_session,
                ..
            } => {
                assert!(!final_session);
                assert_eq!(
                    topics[0].partitions[0].acknowledgement_batches[0].acknowledge_types,
                    [1]
                );
            }
            _ => panic!("finished work must be acknowledged"),
        }
        ctx.with(101, |ctx| {
            n.response(
                ctx,
                id,
                Err(ClientError::Disconnected {
                    api: "ShareAcknowledge",
                    endpoint: Endpoint::kafka(NodeId(1)),
                }),
            );
        });
        assert_eq!(n.acknowledged, [0; 3]);
        assert!(n.batch.is_none());
        assert!(!n.sessions.contains_key(&1));
    }
    #[test]
    fn close_releases_unfinished_work_before_membership_leave() {
        let mut n = node();
        let mut ctx = CtxBuffers::new(NodeId(3));
        n.epoch = 1;
        n.batch = Some(batch());
        n.sessions.insert(
            1,
            Session {
                epoch: 4,
                ..Default::default()
            },
        );
        ctx.with(0, |ctx| n.control(ctx, json!({"cmd":"close"})))
            .unwrap();
        assert!(n.phase == Phase::Closing);
        assert!(n.phase != Phase::Closed);
        let step = n.pending.values().next().unwrap();
        assert!(matches!(
            step,
            Step::Acknowledge {
                final_session: true,
                ..
            }
        ));
        assert!(!n.pending.values().any(|s| matches!(s, Step::Leave)));
        if let Step::Acknowledge { topics, .. } = step {
            assert_eq!(
                topics[0].partitions[0].acknowledgement_batches[3].acknowledge_types,
                [2]
            );
        }
    }
    #[test]
    fn a_failed_leave_remains_closing_and_retries_after_backoff() {
        let mut n = node();
        let mut ctx = CtxBuffers::new(NodeId(3));
        n.phase = Phase::Closing;
        n.epoch = 1;
        n.pending.insert(RequestId(123), Step::Leave);
        ctx.with(0, |ctx| {
            n.response(
                ctx,
                RequestId(123),
                Err(ClientError::Disconnected {
                    api: "ShareGroupHeartbeat",
                    endpoint: Endpoint::kafka(NodeId(1)),
                }),
            );
        });
        assert!(n.phase == Phase::Closing);
        ctx.with(RETRY_MS - 1, |ctx| n.drive(ctx, Vec::new()));
        assert!(n.pending.is_empty());
        ctx.with(RETRY_MS, |ctx| n.drive(ctx, Vec::new()));
        assert!(n.pending.values().any(|s| matches!(s, Step::Leave)));
    }
    #[test]
    fn membership_reset_discards_old_acquisitions_and_late_completions() {
        let mut n = node();
        let mut ctx = CtxBuffers::new(NodeId(3));
        n.batch = Some(batch());
        n.member = "old-member".into();
        n.pending.insert(
            RequestId(123),
            Step::Acknowledge {
                broker: 1,
                final_session: false,
                topics: Vec::new(),
            },
        );
        ctx.with(0, |ctx| n.new_member(ctx));
        let member = n.member.clone();
        ctx.with(1, |ctx| {
            n.response(ctx, RequestId(123), Err(ClientError::Closed));
        });
        assert_ne!(member, "old-member");
        assert_eq!(n.member, member);
        assert_eq!(n.epoch, 0);
        assert_eq!(n.acknowledged, [0; 3]);
        assert!(n.batch.is_none());
        assert!(n.errors.is_empty());
    }
    #[test]
    fn share_configuration_rejects_unknown_fields_and_unbounded_fetches() {
        for extra in [
            json!({"max_records":0}),
            json!({"max_records":1001}),
            json!({"acknowledgement":"ignore"}),
            json!({"enable_auto_commit":true}),
            json!({"group":""}),
            json!({"topics":[]}),
        ] {
            let mut config = json!({"bootstrap":[1],"group":"workers","topics":["jobs"]});
            config
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(
                ShareConsumerNode::from_spec(&NodeSpec::new(3, "share-consumer", "", config))
                    .is_err()
            );
        }
        let mut session = Session {
            epoch: i32::MAX,
            ..Default::default()
        };
        session.advance();
        assert_eq!(session.epoch, 1);
    }
    #[test]
    fn reload_uses_a_new_member_even_when_the_scenario_rng_restarts() {
        let mut first = node();
        let mut ctx = CtxBuffers::new(NodeId(3));
        ctx.with(0, |ctx| first.start(ctx));
        let mut image = DurableImage::default();
        for op in ctx.take_durable() {
            image.apply(op);
        }
        let mut reloaded = node();
        reloaded.load(image);
        let mut restarted_rng = CtxBuffers::new(NodeId(3));
        restarted_rng.with(0, |ctx| reloaded.start(ctx));
        assert_ne!(first.member, reloaded.member);
        assert_eq!(reloaded.incarnation, 2);
    }
}
