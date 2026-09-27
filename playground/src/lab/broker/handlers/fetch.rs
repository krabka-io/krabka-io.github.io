//! `Fetch` (api key 1): records from the partitions this broker hosts, for
//! consumers and for follower replicas.
//!
//! A partition the image does not have answers `UNKNOWN_TOPIC_OR_PARTITION`
//! (`UNKNOWN_TOPIC_ID` for an id no topic has, v13+); those rows come after
//! the readable ones, as Kafka's `KafkaApis.handleFetchRequest` appends them.
//! A readable row is refused in `Partition.fetchRecords` order: a partition
//! this broker does not host (`NOT_LEADER_OR_FOLLOWER`); the KIP-320 fence
//! of `current_leader_epoch` (`FENCED_LEADER_EPOCH` or `UNKNOWN_LEADER_EPOCH`);
//! `NOT_LEADER_OR_FOLLOWER` when only the leader may serve (every follower
//! fetch, and a consumer fetch below v11); a follower that is not a replica
//! (`UNKNOWN_LEADER_EPOCH` when it asserted an epoch, else
//! `NOT_LEADER_OR_FOLLOWER`); a `last_fetched_epoch` the log cannot place
//! (`OFFSET_OUT_OF_RANGE`) or that diverges (a `diverging_epoch` row,
//! KIP-320); and an offset below the log start or past the log end
//! (`OFFSET_OUT_OF_RANGE`). From v16 a `NOT_LEADER_OR_FOLLOWER` or
//! `FENCED_LEADER_EPOCH` row carries the KIP-951 leader hint, and the
//! response the leader's endpoint.
//!
//! A consumer reads up to the high watermark (`read_committed` up to the last
//! stable offset, which is the high watermark with no transactions), a
//! follower up to the log end. Reads return whole batches: the first
//! non-empty partition may exceed its budget by one batch (Kafka's
//! `minOneMessage`), and no batch is cut short, where Kafka's segment slice
//! may end in a partial one. Every row reports the high watermark, the last
//! stable offset and the log start as they were before the read.
//!
//! A follower fetch updates the leader's view of that follower, which may
//! move the high watermark and expand the ISR. A fetch that read fewer than
//! `min_bytes` waits up to `max_wait_ms` (bounded by `request_timeout_ms`)
//! unless a row failed or diverged, and is answered early when a partition
//! changes. Fetch sessions are not kept, as with Kafka's
//! `max.incremental.fetch.session.cache.slots=0`: a full request (session
//! epoch `0` or `-1`) is answered in full with `session_id` 0, and an
//! incremental one answers `FETCH_SESSION_ID_NOT_FOUND`.

use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::{
            EpochEndOffset, FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch, NodeEndpoint,
            PartitionData,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};
use serde_json::json;

use super::{
    super::{
        BrokerNode, TopicPartition,
        dispatch::{HoldReason, Outcome, RequestCtx},
    },
    EpochRule, current_leader, epoch_fence, resolve_topic, unhosted_error,
};
use crate::lab::{codes, net::Ctx};

/// The first version whose consumer fetch may be served by a follower
/// (KIP-392: the request carries client metadata).
const CLIENT_METADATA_VERSION: i16 = 11;
/// The first version that names a topic by id.
const FIRST_TOPIC_ID_VERSION: i16 = 13;
/// The first version whose replica id travels in `replica_state`.
const REPLICA_STATE_VERSION: i16 = 15;
/// The first version with the KIP-951 leader hint.
const LEADER_HINT_VERSION: i16 = 16;
/// Kafka's `FetchMetadata.INVALID_SESSION_ID`.
const INVALID_SESSION_ID: i32 = 0;
/// Kafka's `FetchMetadata.INITIAL_EPOCH` and `FINAL_EPOCH`: the session
/// epochs of a full request.
const FULL_REQUEST_EPOCHS: [i32; 2] = [0, -1];
/// Kafka's default `fetch.max.bytes`, the broker's cap on a response.
const FETCH_MAX_BYTES_CAP: i32 = 57_671_680;
/// `isolation_level` of a `read_committed` consumer.
const READ_COMMITTED: i8 = 1;

/// The row Kafka gives a read it refuses: every offset `-1`, empty records.
fn refused(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: None,
        records: Some(RecordsPayload::Raw(bytes::Bytes::new())),
        ..PartitionData::default()
    }
}

/// What the request says about the fetcher.
#[derive(Clone, Copy)]
struct Fetcher {
    replica_id: i32,
    read_committed: bool,
    version: i16,
    retry: bool,
}

impl Fetcher {
    fn is_follower(self) -> bool {
        self.replica_id >= 0
    }
}

/// One response row and the topic it belongs to, before rows are grouped.
struct Row {
    topic: String,
    topic_id: WireUuid,
    data: PartitionData,
}

/// Serve a fetch.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: FetchRequest,
) -> Outcome<FetchResponse> {
    if !FULL_REQUEST_EPOCHS.contains(&request.session_epoch) {
        return Outcome::Reply(FetchResponse {
            error_code: codes::FETCH_SESSION_ID_NOT_FOUND,
            session_id: INVALID_SESSION_ID,
            ..FetchResponse::default()
        });
    }
    let replica_id = if req.version >= REPLICA_STATE_VERSION {
        request.replica_state.replica_id
    } else {
        request.replica_id
    };
    let fetcher = Fetcher {
        replica_id,
        read_committed: replica_id < 0 && request.isolation_level == READ_COMMITTED,
        version: req.version,
        retry: req.is_retry(),
    };
    let max_bytes = usize::try_from(request.max_bytes.clamp(0, FETCH_MAX_BYTES_CAP)).unwrap_or(0);
    let min_bytes = usize::try_from(request.min_bytes.max(0))
        .unwrap_or(0)
        .min(max_bytes);
    let (readable, erroneous) = sort_partitions(node, req.version, request.topics);
    let mut remaining = max_bytes;
    let mut min_one_message = true;
    let mut bytes_read = 0usize;
    let mut respond_now = readable.is_empty();
    let mut rows = Vec::with_capacity(readable.len() + erroneous.len());
    for (mut row, key, part) in readable {
        let budget = usize::try_from(part.partition_max_bytes.max(0))
            .unwrap_or(0)
            .min(remaining);
        row.data = read_partition(node, ctx, &key, &part, fetcher, budget, min_one_message);
        let size = row
            .data
            .records
            .as_ref()
            .map_or(0, RecordsPayload::payload_len);
        if size > 0 {
            min_one_message = false;
        }
        bytes_read += size;
        remaining = remaining.saturating_sub(size);
        respond_now |= row.data.error_code != codes::NONE || row.data.diverging_epoch.epoch >= 0;
        if req.version >= LEADER_HINT_VERSION
            && matches!(
                row.data.error_code,
                codes::NOT_LEADER_OR_FOLLOWER | codes::FENCED_LEADER_EPOCH
            )
        {
            let (leader_id, leader_epoch, _) = current_leader(node, &key.topic, key.partition);
            row.data.current_leader = LeaderIdAndEpoch {
                leader_id,
                leader_epoch,
                ..LeaderIdAndEpoch::default()
            };
        }
        rows.push(row);
    }
    let wait = u64::try_from(request.max_wait_ms.max(0))
        .unwrap_or(0)
        .min(node.config().request_timeout_ms);
    let deadline = req.held_until.unwrap_or_else(|| ctx.now() + wait);
    if !respond_now && bytes_read < min_bytes && request.max_wait_ms > 0 && ctx.now() < deadline {
        return Outcome::Hold(HoldReason::Fetch { deadline });
    }
    let node_endpoints = if req.version >= LEADER_HINT_VERSION {
        leader_endpoints(node, &rows)
    } else {
        Vec::new()
    };
    rows.extend(erroneous);
    Outcome::Reply(FetchResponse {
        session_id: INVALID_SESSION_ID,
        responses: group_rows(rows),
        node_endpoints,
        ..FetchResponse::default()
    })
}

/// Kafka's metadata check before a read: the partitions the image has, to
/// read, and the refused rows of the rest.
fn sort_partitions(
    node: &BrokerNode,
    version: i16,
    topics: Vec<FetchTopic>,
) -> (Vec<(Row, TopicPartition, FetchPartition)>, Vec<Row>) {
    let mut readable = Vec::new();
    let mut erroneous = Vec::new();
    for topic in topics {
        let name = resolve_topic(node.image(), &topic.topic, topic.topic_id);
        for part in topic.partitions {
            let row = |data| Row {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                data,
            };
            match &name {
                Ok(name) if node.image().partition(name, part.partition).is_some() => {
                    let key = TopicPartition::new(name, part.partition);
                    readable.push((row(PartitionData::default()), key, part));
                }
                Ok(_) => erroneous.push(row(refused(
                    part.partition,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                ))),
                Err(code) => {
                    let code = if version < FIRST_TOPIC_ID_VERSION {
                        codes::UNKNOWN_TOPIC_OR_PARTITION
                    } else {
                        *code
                    };
                    erroneous.push(row(refused(part.partition, code)));
                }
            }
        }
    }
    (readable, erroneous)
}

/// Kafka's `FetchResponse.toMessage`: consecutive rows of one topic share a
/// topic entry, so the rows keep their order.
fn group_rows(rows: Vec<Row>) -> Vec<FetchableTopicResponse> {
    let mut topics: Vec<FetchableTopicResponse> = Vec::new();
    for row in rows {
        match topics.last_mut() {
            Some(last) if last.topic == row.topic && last.topic_id == row.topic_id => {
                last.partitions.push(row.data);
            }
            _ => topics.push(FetchableTopicResponse {
                topic: row.topic,
                topic_id: row.topic_id,
                partitions: vec![row.data],
                ..FetchableTopicResponse::default()
            }),
        }
    }
    topics
}

/// The endpoint of every live broker a leader hint names, ascending by id.
fn leader_endpoints(node: &BrokerNode, rows: &[Row]) -> Vec<NodeEndpoint> {
    let mut endpoints: Vec<NodeEndpoint> = Vec::new();
    for row in rows {
        let leader_id = row.data.current_leader.leader_id;
        if leader_id < 0 || endpoints.iter().any(|e| e.node_id == leader_id) {
            continue;
        }
        if let Some(broker) = super::alive_broker(node.image(), leader_id) {
            endpoints.push(NodeEndpoint {
                node_id: leader_id,
                host: broker.host.clone(),
                port: i32::from(broker.port),
                rack: broker.rack.clone(),
                ..NodeEndpoint::default()
            });
        }
    }
    endpoints.sort_by_key(|e| e.node_id);
    endpoints
}

/// Kafka's `Partition.fetchRecords` for one row.
fn read_partition(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    key: &TopicPartition,
    part: &FetchPartition,
    fetcher: Fetcher,
    budget: usize,
    min_one_message: bool,
) -> PartitionData {
    let me = node.broker_id();
    let now = ctx.now();
    let Some(replica) = node.replicas.get(key) else {
        return refused(
            part.partition,
            unhosted_error(node.image(), &key.topic, key.partition),
        );
    };
    if let Some(code) = epoch_fence(
        replica.leader_epoch,
        part.current_leader_epoch,
        EpochRule::Fetch,
    ) {
        return refused(part.partition, code);
    }
    let leader_only = fetcher.is_follower() || fetcher.version < CLIENT_METADATA_VERSION;
    if leader_only && !replica.leads(me) {
        return refused(part.partition, codes::NOT_LEADER_OR_FOLLOWER);
    }
    if fetcher.is_follower()
        && (fetcher.replica_id == me || !replica.replicas.contains(&fetcher.replica_id))
    {
        let code = if part.current_leader_epoch >= 0 {
            codes::UNKNOWN_LEADER_EPOCH
        } else {
            codes::NOT_LEADER_OR_FOLLOWER
        };
        return refused(part.partition, code);
    }
    let log = &replica.log;
    let (high_watermark, log_start, log_end) = (
        log.high_watermark(),
        log.log_start_offset(),
        log.log_end_offset(),
    );
    if part.last_fetched_epoch >= 0 {
        let (found_epoch, end_offset) = log.end_offset_for_epoch(part.last_fetched_epoch);
        if found_epoch < 0 || end_offset < 0 || part.fetch_offset < log_start {
            return out_of_range(ctx, key, part, log_start, log_end);
        }
        if found_epoch < part.last_fetched_epoch || end_offset < part.fetch_offset {
            return PartitionData {
                partition_index: part.partition,
                error_code: codes::NONE,
                high_watermark,
                last_stable_offset: high_watermark,
                log_start_offset: log_start,
                aborted_transactions: None,
                records: Some(RecordsPayload::Raw(bytes::Bytes::new())),
                diverging_epoch: EpochEndOffset {
                    epoch: found_epoch,
                    end_offset,
                    ..EpochEndOffset::default()
                },
                ..PartitionData::default()
            };
        }
    }
    if part.fetch_offset < log_start || part.fetch_offset > log_end {
        return out_of_range(ctx, key, part, log_start, log_end);
    }
    let upper = if fetcher.is_follower() {
        log_end
    } else {
        high_watermark
    };
    let records = if part.fetch_offset >= upper {
        bytes::Bytes::new()
    } else {
        let read = log.read(part.fetch_offset, budget, upper);
        // Past its budget only the first non-empty partition may take one
        // batch whole; any later one reads nothing.
        if read.len() > budget && !min_one_message {
            bytes::Bytes::new()
        } else {
            read
        }
    };
    let row = PartitionData {
        partition_index: part.partition,
        error_code: codes::NONE,
        high_watermark,
        last_stable_offset: high_watermark,
        log_start_offset: log_start,
        aborted_transactions: fetcher.read_committed.then(Vec::new),
        records: Some(RecordsPayload::Raw(records)),
        ..PartitionData::default()
    };
    if fetcher.is_follower() && !fetcher.retry {
        note_follower(node, ctx, key, fetcher.replica_id, part, now);
    }
    row
}

/// An `OFFSET_OUT_OF_RANGE` row, recorded as an event.
fn out_of_range(
    ctx: &mut Ctx<'_>,
    key: &TopicPartition,
    part: &FetchPartition,
    log_start: i64,
    log_end: i64,
) -> PartitionData {
    ctx.event(
        "fetch_error",
        json!({ "topic": key.topic, "partition": key.partition, "fetch_offset": part.fetch_offset, "log_start": log_start, "log_end": log_end, "error_code": codes::OFFSET_OUT_OF_RANGE, "level": "warn" }),
    );
    refused(part.partition, codes::OFFSET_OUT_OF_RANGE)
}

/// Track a follower's progress on the leader: its log end, the high
/// watermark it lets advance, and the ISR it may rejoin.
fn note_follower(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    key: &TopicPartition,
    follower: i32,
    part: &FetchPartition,
    now: u64,
) {
    let me = node.broker_id();
    let expand = {
        let Some(replica) = node.replicas.get_mut(key) else {
            return;
        };
        if !replica.leads(me) {
            return;
        }
        let expand =
            replica.record_follower_fetch(follower, part.fetch_offset, part.log_start_offset, now);
        replica.recompute_hwm(me);
        expand
    };
    if expand {
        let mut isr = node
            .replicas
            .get(key)
            .map(|r| r.isr.clone())
            .unwrap_or_default();
        isr.push(follower);
        node.change_isr(ctx, key, isr);
    }
}
