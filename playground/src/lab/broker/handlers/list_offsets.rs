//! `ListOffsets` (api key 2): the offset a timestamp names.
//!
//! Each partition row resolves against its log, bounded by the last
//! fetchable offset of the caller: the high watermark for a consumer
//! (`replica_id = -1`) and the log end for anyone else. `-1` is that bound
//! with the partition's leader epoch, `-2` the log start, `-3` the first
//! record with the largest timestamp (v7+), `-4` the earliest local offset
//! (v8+), and a timestamp the first record at or after it; a record at or
//! past the bound answers offset `-1`. The two tiered sentinels answer `-1`
//! in a lab without a remote tier. A negative timestamp that is no sentinel
//! of the request's version answers `UNSUPPORTED_VERSION`. From v4 the row's
//! `current_leader_epoch` is fenced (KIP-320; only `-1` is unasserted), a
//! partition this broker does not host answers `NOT_LEADER_OR_FOLLOWER`
//! when the image has it, and one it does not lead answers the same unless
//! the debugging replica id `-2` asks. A consumer that asks a leader whose
//! high watermark has not reached the start of its epoch answers
//! `OFFSET_NOT_AVAILABLE` from v5, `LEADER_NOT_AVAILABLE` before (KIP-207),
//! for the latest offset and for a lookup the bound cuts off. A partition
//! named twice answers `INVALID_REQUEST`. The rows keep the request's order;
//! Kafka's follow a hash map.

use std::collections::BTreeSet;

use krabka_protocol::owned::{
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest},
    list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
    },
};

use super::{
    super::{
        BrokerNode,
        dispatch::{Outcome, RequestCtx},
        log::{
            EARLIEST_LOCAL_TIMESTAMP, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP,
            NO_TIMESTAMP,
        },
    },
    EpochRule, epoch_fence, unhosted_error,
};
use crate::lab::{codes, net::Ctx};

/// Kafka's `ListOffsetsRequest.DEBUGGING_REPLICA_ID`, which any replica
/// may answer.
const DEBUGGING_REPLICA_ID: i32 = -2;
/// Kafka's `ListOffsetsRequest.CONSUMER_REPLICA_ID`.
const CONSUMER_REPLICA_ID: i32 = -1;
/// The KIP-1005 and KIP-1023 tiered sentinels.
const LATEST_TIERED_TIMESTAMP: i64 = -5;
const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;

fn error_row(partition_index: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index,
        error_code,
        ..ListOffsetsPartitionResponse::default()
    }
}

/// The first version that carries each sentinel, Kafka's
/// `timestampMinSupportedVersion`; `None` for a negative timestamp that is
/// no sentinel.
fn sentinel_min_version(timestamp: i64) -> Option<i16> {
    match timestamp {
        LATEST_TIMESTAMP | EARLIEST_TIMESTAMP => Some(1),
        MAX_TIMESTAMP => Some(7),
        EARLIEST_LOCAL_TIMESTAMP => Some(8),
        LATEST_TIERED_TIMESTAMP => Some(9),
        EARLIEST_PENDING_UPLOAD_TIMESTAMP => Some(10),
        _ => None,
    }
}

/// Serve a `ListOffsets`.
pub fn handle(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: ListOffsetsRequest,
) -> Outcome<ListOffsetsResponse> {
    let ListOffsetsRequest {
        replica_id, topics, ..
    } = request;
    let mut seen: BTreeSet<(&str, i32)> = BTreeSet::new();
    let mut duplicates: BTreeSet<(&str, i32)> = BTreeSet::new();
    for topic in &topics {
        for part in &topic.partitions {
            if !seen.insert((&topic.name, part.partition_index)) {
                duplicates.insert((&topic.name, part.partition_index));
            }
        }
    }
    let responses = topics
        .iter()
        .map(|topic| ListOffsetsTopicResponse {
            name: topic.name.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|part| {
                    if duplicates.contains(&(topic.name.as_str(), part.partition_index)) {
                        error_row(part.partition_index, codes::INVALID_REQUEST)
                    } else {
                        resolve_partition(node, &topic.name, part, req.version, replica_id)
                    }
                })
                .collect(),
            ..ListOffsetsTopicResponse::default()
        })
        .collect();
    Outcome::Reply(ListOffsetsResponse {
        topics: responses,
        ..ListOffsetsResponse::default()
    })
}

fn resolve_partition(
    node: &BrokerNode,
    topic: &str,
    part: &ListOffsetsPartition,
    version: i16,
    replica_id: i32,
) -> ListOffsetsPartitionResponse {
    let index = part.partition_index;
    if part.timestamp < 0 && sentinel_min_version(part.timestamp).is_none_or(|min| version < min) {
        return error_row(index, codes::UNSUPPORTED_VERSION);
    }
    let Some(replica) = node.replica(topic, index) else {
        return error_row(index, unhosted_error(node.image(), topic, index));
    };
    if version >= 4
        && let Some(code) = epoch_fence(
            replica.leader_epoch,
            part.current_leader_epoch,
            EpochRule::ListOffsets,
        )
    {
        return error_row(index, code);
    }
    if replica_id != DEBUGGING_REPLICA_ID && !replica.leads(node.broker_id()) {
        return error_row(index, codes::NOT_LEADER_OR_FOLLOWER);
    }
    if matches!(
        part.timestamp,
        LATEST_TIERED_TIMESTAMP | EARLIEST_PENDING_UPLOAD_TIMESTAMP
    ) {
        return ListOffsetsPartitionResponse {
            partition_index: index,
            error_code: codes::NONE,
            ..ListOffsetsPartitionResponse::default()
        };
    }
    let consumer = replica_id == CONSUMER_REPLICA_ID;
    let bound = if consumer {
        replica.log.high_watermark()
    } else {
        replica.log.log_end_offset()
    };
    // Kafka's `maybeOffsetsError`: a leader whose high watermark has not
    // reached the start of its epoch cannot tell a consumer where the end is.
    let lagging = consumer
        && replica.leads(node.broker_id())
        && replica.epoch_start_offset > replica.log.high_watermark();
    let not_available = || {
        error_row(
            index,
            if version >= 5 {
                codes::OFFSET_NOT_AVAILABLE
            } else {
                codes::LEADER_NOT_AVAILABLE
            },
        )
    };
    let (timestamp, offset, leader_epoch) = match part.timestamp {
        LATEST_TIMESTAMP if lagging => return not_available(),
        LATEST_TIMESTAMP => (NO_TIMESTAMP, bound, replica.leader_epoch),
        EARLIEST_TIMESTAMP | EARLIEST_LOCAL_TIMESTAMP => {
            replica.log.list_offset(part.timestamp, i64::MAX)
        }
        target => {
            let found = replica.log.list_offset(target, i64::MAX);
            if found.1 >= 0 && found.1 < bound {
                found
            } else if lagging {
                return not_available();
            } else {
                (NO_TIMESTAMP, -1, -1)
            }
        }
    };
    ListOffsetsPartitionResponse {
        partition_index: index,
        error_code: codes::NONE,
        timestamp,
        offset,
        leader_epoch,
        ..ListOffsetsPartitionResponse::default()
    }
}
