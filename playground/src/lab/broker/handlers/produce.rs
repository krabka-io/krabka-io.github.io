//! `Produce` (api key 0): append batches to the partitions this broker leads.
//!
//! The request is read by its framing alone ([`ProduceBody`]), so a batch
//! that does not decode answers its own row instead of closing the
//! connection. Each row is decided in Kafka's order (`KafkaApis`, then
//! `ReplicaManager.appendRecords`, then `Partition.appendRecordsToLeader`):
//!
//! 1. an id no topic has is `UNKNOWN_TOPIC_ID` (v13+), a partition the image
//!    does not have `UNKNOWN_TOPIC_OR_PARTITION`;
//! 2. the records must be exactly one v2 batch (`INVALID_RECORD`, or
//!    `CORRUPT_MESSAGE` for a header the batch iterator refuses), without
//!    zstd before v7 (`UNSUPPORTED_COMPRESSION_TYPE`);
//! 3. an `acks` other than `0`, `1` and `-1` is `INVALID_REQUIRED_ACKS`;
//! 4. an internal topic is `INVALID_TOPIC_EXCEPTION` unless the client is
//!    `__admin_client`;
//! 5. a partition this broker does not lead is `NOT_LEADER_OR_FOLLOWER`,
//!    with the KIP-951 leader hint and the leader's endpoint from v10;
//! 6. an `acks=-1` append to a partition whose ISR is smaller than
//!    `min.insync.replicas` (capped at the replication factor) is
//!    `NOT_ENOUGH_REPLICAS`, before anything is written;
//! 7. the log validates and appends the batch ([`PartitionLog::append`]).
//!
//! With `acks=1` the request answers at once. With `acks=-1` it waits until
//! the high watermark covers every appended batch, then answers
//! `NOT_ENOUGH_REPLICAS_AFTER_APPEND` for a partition whose maximal ISR
//! shrank below the minimum meanwhile, `NOT_LEADER_OR_FOLLOWER` for one whose leadership
//! moved, and `REQUEST_TIMED_OUT` for one still short when the request's
//! `timeout_ms` runs out (bounded by the broker's `request_timeout_ms`).
//! `acks=0` answers nothing, and closes the connection when a row failed, as
//! Kafka does. The rows keep the request's order; Kafka's follow a hash map.
//! A transactional batch appends like any other: the lab has no transaction
//! coordinator.
//!
//! [`PartitionLog::append`]: super::super::PartitionLog::append

use bytes::{Buf, Bytes};
use krabka_protocol::{
    Decode, ProtocolError,
    owned::produce_response::{
        BatchIndexAndErrorMessage, LeaderIdAndEpoch, NodeEndpoint, PartitionProduceResponse,
        ProduceResponse, TopicProduceResponse,
    },
    records::{ProduceFraming, produce_framing},
};
use serde_json::json;

use super::{
    super::{
        BrokerNode, TopicPartition,
        dispatch::{HoldReason, Outcome, RequestCtx},
        log::{AppendError, single_batch},
    },
    append_policy, current_leader, is_internal_topic, unhosted_error, uuid_of,
};
use crate::lab::{
    codes,
    net::{Ctx, Millis},
};

/// Kafka's `acks=all`.
const ACKS_ALL: i16 = -1;
/// Kafka's `ProduceResponse.INVALID_OFFSET`.
const INVALID_OFFSET: i64 = -1;
/// The first version that names a topic by id.
const FIRST_TOPIC_ID_VERSION: i16 = 13;
/// The first version with the KIP-951 leader hint.
const LEADER_HINT_VERSION: i16 = 10;
/// The `client_id` Kafka lets append to an internal topic.
const ADMIN_CLIENT_ID: &str = "__admin_client";

/// A `Produce` request as the dispatcher decodes it: the framing, with every
/// partition's records still the bytes the client sent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProduceBody(pub ProduceFraming);

impl Decode<'_> for ProduceBody {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        produce_framing(buf.copy_to_bytes(buf.remaining()), version).map(Self)
    }
}

/// An `acks=-1` produce that waits for the high watermark.
#[derive(Debug)]
pub struct PendingProduce {
    /// The response with every row already decided but the waiting ones.
    pub response: ProduceResponse,
    /// The rows still waiting for the high watermark.
    pub waits: Vec<ProduceWait>,
    /// When the waiting rows answer `REQUEST_TIMED_OUT`.
    pub deadline: Millis,
}

/// One row of a pending produce and the offset its partition must commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProduceWait {
    /// The row's topic entry in the response.
    pub topic_row: usize,
    /// The row within that topic entry.
    pub partition_row: usize,
    /// The partition the row appended to.
    pub key: TopicPartition,
    /// The high watermark that covers the batch: its last offset plus one.
    pub required_offset: i64,
}

/// A refused row: Kafka's `PartitionResponse(error)`, every offset `-1`.
fn error_row(index: i32, error_code: i16) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index,
        error_code,
        base_offset: INVALID_OFFSET,
        ..PartitionProduceResponse::default()
    }
}

/// Serve a produce.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    ProduceBody(request): ProduceBody,
) -> Outcome<ProduceResponse> {
    let acks = request.acks;
    let timeout = u64::try_from(request.timeout_ms.max(0))
        .unwrap_or(0)
        .min(node.config().request_timeout_ms);
    let deadline = ctx.now() + timeout;
    let mut responses = Vec::with_capacity(request.topics.len());
    let mut waits = Vec::new();
    for topic in request.topics {
        let name = if req.version >= FIRST_TOPIC_ID_VERSION {
            node.image()
                .topic_name_by_id(&uuid_of(topic.topic_id))
                .map(str::to_owned)
        } else {
            Some(topic.name.clone())
        };
        let topic_row = responses.len();
        let mut rows = Vec::with_capacity(topic.partitions.len());
        for part in topic.partitions {
            let Some(name) = &name else {
                rows.push(error_row(part.partition, codes::UNKNOWN_TOPIC_ID));
                continue;
            };
            let key = TopicPartition::new(name, part.partition);
            let records = part.records.unwrap_or_default();
            let (row, required) = produce_one(node, ctx, req, &key, &records, acks);
            if let Some(required_offset) = required {
                waits.push(ProduceWait {
                    topic_row,
                    partition_row: rows.len(),
                    key,
                    required_offset,
                });
            }
            rows.push(row);
        }
        responses.push(TopicProduceResponse {
            name: topic.name,
            topic_id: topic.topic_id,
            partition_responses: rows,
            ..TopicProduceResponse::default()
        });
    }
    let response = ProduceResponse {
        responses,
        ..ProduceResponse::default()
    };
    if acks == 0 {
        let failed = response
            .responses
            .iter()
            .flat_map(|t| &t.partition_responses)
            .any(|p| p.error_code != codes::NONE);
        if failed {
            ctx.event(
                "produce_error",
                json!({ "reason": "an acks=0 produce failed; the connection closes", "level": "warn" }),
            );
            return Outcome::Close;
        }
        return Outcome::Silent;
    }
    if waits.is_empty() {
        return Outcome::Reply(with_leader_hints(node, req.version, response));
    }
    Outcome::Hold(HoldReason::Produce(Box::new(PendingProduce {
        response,
        waits,
        deadline,
    })))
}

/// Decide one partition's row. Returns the row and, for an `acks=-1`
/// append the high watermark does not cover yet, the offset it waits for.
fn produce_one(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    key: &TopicPartition,
    records: &Bytes,
    acks: i16,
) -> (PartitionProduceResponse, Option<i64>) {
    let index = key.partition;
    if node.image().partition(&key.topic, index).is_none() {
        return (error_row(index, codes::UNKNOWN_TOPIC_OR_PARTITION), None);
    }
    let length = match single_batch(records, req.version) {
        Ok(length) => length,
        Err(error) => return (refused(ctx, key, &error, None), None),
    };
    if !matches!(acks, 0 | 1 | ACKS_ALL) {
        return (error_row(index, codes::INVALID_REQUIRED_ACKS), None);
    }
    if is_internal_topic(&key.topic) && req.client_id.as_deref() != Some(ADMIN_CLIENT_ID) {
        return (error_row(index, codes::INVALID_TOPIC_EXCEPTION), None);
    }
    let me = node.broker_id();
    let now = ctx.now();
    let policy = append_policy(node, &key.topic);
    let min_isr = node.min_insync_replicas(&key.topic);
    let Some(replica) = node.replicas.get_mut(key).filter(|r| r.leads(me)) else {
        return (error_row(index, codes::NOT_LEADER_OR_FOLLOWER), None);
    };
    if acks == ACKS_ALL && replica.isr.len() < effective_min_isr(min_isr, replica.replicas.len()) {
        ctx.event(
            "produce_error",
            json!({ "topic": key.topic, "partition": index, "error_code": codes::NOT_ENOUGH_REPLICAS, "isr": replica.isr, "min_insync_replicas": min_isr, "level": "warn" }),
        );
        let mut row = error_row(index, codes::NOT_ENOUGH_REPLICAS);
        row.log_start_offset = replica.log.log_start_offset();
        return (row, None);
    }
    let epoch = replica.leader_epoch;
    match replica
        .log
        .append(&records.slice(..length), epoch, now, policy, &key.label())
    {
        Ok(info) => {
            replica.recompute_hwm(me);
            let row = PartitionProduceResponse {
                index,
                error_code: codes::NONE,
                base_offset: info.first_offset,
                log_append_time_ms: info.log_append_time_ms,
                log_start_offset: replica.log.log_start_offset(),
                ..PartitionProduceResponse::default()
            };
            let required = (acks == ACKS_ALL && replica.log.high_watermark() <= info.last_offset)
                .then_some(info.last_offset + 1);
            (row, required)
        }
        Err(error) => {
            let log_start = replica.log.log_start_offset();
            (refused(ctx, key, &error, Some(log_start)), None)
        }
    }
}

/// The row of a refused batch: Kafka's `LogAppendResult` keeps a message and
/// the record errors only for a record validation, and reports the log start
/// only for a refusal the log raised while validating.
fn refused(
    ctx: &mut Ctx<'_>,
    key: &TopicPartition,
    error: &AppendError,
    log_start: Option<i64>,
) -> PartitionProduceResponse {
    ctx.event(
        "produce_error",
        json!({ "topic": key.topic, "partition": key.partition, "error_code": error.code(), "reason": error.to_string(), "level": "warn" }),
    );
    let mut row = error_row(key.partition, error.code());
    row.error_message = error.wire_message();
    row.record_errors = error
        .record_errors()
        .iter()
        .map(|e| BatchIndexAndErrorMessage {
            batch_index: e.batch_index,
            batch_index_error_message: Some(e.message.clone()),
            ..BatchIndexAndErrorMessage::default()
        })
        .collect();
    if let Some(log_start) = log_start.filter(|_| error.reports_log_start()) {
        row.log_start_offset = log_start;
    }
    row
}

/// Kafka's `Partition.effectiveMinIsr`: `min.insync.replicas`, capped at the
/// replication factor.
fn effective_min_isr(min_insync_replicas: i32, replicas: usize) -> usize {
    usize::try_from(min_insync_replicas.max(0))
        .unwrap_or(usize::MAX)
        .min(replicas)
}

/// Add the KIP-951 leader hint to every `NOT_LEADER_OR_FOLLOWER` row, and
/// the endpoint of every live leader a hint names, from v10.
fn with_leader_hints(
    node: &BrokerNode,
    version: i16,
    mut response: ProduceResponse,
) -> ProduceResponse {
    if version < LEADER_HINT_VERSION {
        return response;
    }
    let mut endpoints: Vec<NodeEndpoint> = Vec::new();
    for topic in &mut response.responses {
        let name = if topic.name.is_empty() {
            node.image()
                .topic_name_by_id(&uuid_of(topic.topic_id))
                .map(str::to_owned)
                .unwrap_or_default()
        } else {
            topic.name.clone()
        };
        for row in &mut topic.partition_responses {
            if row.error_code != codes::NOT_LEADER_OR_FOLLOWER {
                continue;
            }
            let (leader_id, leader_epoch, alive) = current_leader(node, &name, row.index);
            row.current_leader = LeaderIdAndEpoch {
                leader_id,
                leader_epoch,
                ..LeaderIdAndEpoch::default()
            };
            if let Some(broker) = alive
                && !endpoints.iter().any(|e| e.node_id == leader_id)
            {
                endpoints.push(NodeEndpoint {
                    node_id: leader_id,
                    host: broker.host.clone(),
                    port: i32::from(broker.port),
                    rack: broker.rack.clone(),
                    ..NodeEndpoint::default()
                });
            }
        }
    }
    endpoints.sort_by_key(|e| e.node_id);
    response.node_endpoints = endpoints;
    response
}

/// Decide the rows of a held produce again: covered, failed, or still
/// waiting.
pub fn retry(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    mut pending: PendingProduce,
) -> Outcome<ProduceResponse> {
    let me = node.broker_id();
    let expired = ctx.now() >= pending.deadline;
    let mut still = Vec::new();
    for wait in pending.waits {
        let min_isr = node.min_insync_replicas(&wait.key.topic);
        let decided = match node.replicas.get(&wait.key) {
            None => Some(unhosted_error(
                node.image(),
                &wait.key.topic,
                wait.key.partition,
            )),
            Some(replica) if !replica.leads(me) => Some(codes::NOT_LEADER_OR_FOLLOWER),
            Some(replica) if replica.log.high_watermark() >= wait.required_offset => {
                // Kafka's `checkEnoughReplicasReachOffset` counts the maximal
                // ISR, a pending expansion included.
                if replica.maximal_isr().len() < effective_min_isr(min_isr, replica.replicas.len())
                {
                    Some(codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND)
                } else {
                    Some(codes::NONE)
                }
            }
            Some(_) if expired => Some(codes::REQUEST_TIMED_OUT),
            Some(_) => None,
        };
        match decided {
            Some(code) => {
                let row = &mut pending.response.responses[wait.topic_row].partition_responses
                    [wait.partition_row];
                row.error_code = code;
                if code != codes::NONE {
                    ctx.event(
                        "produce_error",
                        json!({ "topic": wait.key.topic, "partition": wait.key.partition, "error_code": code, "level": "warn" }),
                    );
                }
            }
            None => still.push(wait),
        }
    }
    if still.is_empty() {
        return Outcome::Reply(with_leader_hints(node, req.version, pending.response));
    }
    pending.waits = still;
    Outcome::Hold(HoldReason::Produce(Box::new(pending)))
}
