//! `OffsetForLeaderEpoch` (api key 23, KIP-101): where an epoch ends.
//!
//! For each requested `(partition, leader_epoch)` the leader answers the
//! largest epoch it knows that is not above the requested one and the
//! offset where the next epoch starts, or the log end for its latest epoch;
//! `(-1, -1)` when the cache cannot place the request. A partition this
//! broker does not host answers `NOT_LEADER_OR_FOLLOWER` when the image has
//! it, else `UNKNOWN_TOPIC_OR_PARTITION`; the row's `current_leader_epoch` is
//! fenced next (only `-1` is unasserted), then only the leader may answer.
//! Consumers ask it to validate their position after a leader change
//! (KIP-320); a follower learns the same from the diverging epoch of a
//! fetch.

use krabka_protocol::owned::{
    offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
    offset_for_leader_epoch_response::{
        EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
    },
};

use super::{
    super::{
        BrokerNode,
        dispatch::{Outcome, RequestCtx},
    },
    EpochRule, epoch_fence, unhosted_error,
};
use crate::lab::{codes, net::Ctx};

/// Serve an `OffsetForLeaderEpoch`.
pub fn handle(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    request: OffsetForLeaderEpochRequest,
) -> Outcome<OffsetForLeaderEpochResponse> {
    let me = node.broker_id();
    let topics = request
        .topics
        .into_iter()
        .map(|topic| OffsetForLeaderTopicResult {
            partitions: topic
                .partitions
                .iter()
                .map(|part| {
                    let mut row = EpochEndOffset {
                        partition: part.partition,
                        ..EpochEndOffset::default()
                    };
                    let Some(replica) = node.replica(&topic.topic, part.partition) else {
                        row.error_code = unhosted_error(node.image(), &topic.topic, part.partition);
                        return row;
                    };
                    if let Some(code) = epoch_fence(
                        replica.leader_epoch,
                        part.current_leader_epoch,
                        EpochRule::ListOffsets,
                    ) {
                        row.error_code = code;
                        return row;
                    }
                    if !replica.leads(me) {
                        row.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                        return row;
                    }
                    let (epoch, end_offset) = replica.log.end_offset_for_epoch(part.leader_epoch);
                    row.error_code = codes::NONE;
                    row.leader_epoch = epoch;
                    row.end_offset = end_offset;
                    row
                })
                .collect(),
            topic: topic.topic,
            ..OffsetForLeaderTopicResult::default()
        })
        .collect();
    Outcome::Reply(OffsetForLeaderEpochResponse {
        topics,
        ..OffsetForLeaderEpochResponse::default()
    })
}
