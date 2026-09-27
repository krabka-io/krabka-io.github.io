//! `DescribeTopicPartitions` (api key 75, KIP-966): topics and partitions in
//! pages.
//!
//! An empty `topics` lists every topic; named topics are deduplicated and
//! sorted. `response_partition_limit`, clamped to `[1, 2000]`, bounds the
//! partition rows; when it runs out the response names the next
//! `(topic, partition)` in `next_cursor`, and a request `cursor` resumes
//! there. A cursor that names a topic the request does not list, or a
//! negative partition, fails the request with `INVALID_REQUEST`. A topic that
//! does not exist answers `UNKNOWN_TOPIC_OR_PARTITION`, an invalid name
//! `INVALID_TOPIC_EXCEPTION`. A partition row follows
//! `KRaftMetadataCache.getPartitionMetadataForDescribeTopicResponse`: a
//! leader with no registration answers `LEADER_NOT_AVAILABLE` with leader
//! `-1`, and the KIP-966 ELR lists come from the image. Every topic row
//! carries the KIP-430 operations.

use krabka_metadata::{MetadataImage, NodeId, PartitionRecord};
use krabka_protocol::owned::{
    describe_topic_partitions_request::DescribeTopicPartitionsRequest,
    describe_topic_partitions_response::{
        Cursor, DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponsePartition,
        DescribeTopicPartitionsResponseTopic,
    },
};

use super::{
    super::{
        BrokerNode, cluster,
        dispatch::{Outcome, RequestCtx},
    },
    TOPIC_AUTHORIZED_OPERATIONS, is_internal_topic,
    metadata::{offline_replicas, reported_leader},
    wire_uuid,
};
use crate::lab::{codes, net::Ctx};

/// Kafka's `max.request.partition.size.limit`.
const MAX_PARTITION_LIMIT: i32 = 2_000;

/// One partition row, Kafka's
/// `KRaftMetadataCache.getPartitionMetadataForDescribeTopicResponse`.
fn partition_row(
    image: &MetadataImage,
    p: &PartitionRecord,
) -> DescribeTopicPartitionsResponsePartition {
    let leader_id = reported_leader(image, p);
    let (eligible, last_known) = image.partition_elr(&p.topic, p.partition);
    let ids =
        |nodes: &[NodeId]| -> Vec<i32> { nodes.iter().map(|r| cluster::wire_id(*r)).collect() };
    DescribeTopicPartitionsResponsePartition {
        error_code: if leader_id < 0 {
            codes::LEADER_NOT_AVAILABLE
        } else {
            codes::NONE
        },
        partition_index: p.partition,
        leader_id,
        leader_epoch: p.leader_epoch.0,
        replica_nodes: ids(&p.replicas),
        isr_nodes: ids(&p.isr),
        eligible_leader_replicas: Some(ids(eligible)),
        last_known_elr: Some(ids(last_known)),
        offline_replicas: offline_replicas(image, p),
        ..DescribeTopicPartitionsResponsePartition::default()
    }
}

/// Serve a `DescribeTopicPartitions`.
pub fn handle(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    request: DescribeTopicPartitionsRequest,
) -> Outcome<DescribeTopicPartitionsResponse> {
    let DescribeTopicPartitionsRequest {
        topics: requested,
        response_partition_limit,
        cursor,
        ..
    } = request;
    let listed = !requested.is_empty();
    if let Some(cursor) = &cursor
        && ((listed && !requested.iter().any(|t| t.name == cursor.topic_name))
            || cursor.partition_index < 0)
    {
        // Kafka's `DescribeTopicPartitionsRequest.getErrorResponse`.
        return Outcome::Reply(DescribeTopicPartitionsResponse {
            topics: requested
                .into_iter()
                .map(|t| DescribeTopicPartitionsResponseTopic {
                    error_code: codes::INVALID_REQUEST,
                    name: Some(t.name),
                    ..DescribeTopicPartitionsResponseTopic::default()
                })
                .collect(),
            ..DescribeTopicPartitionsResponse::default()
        });
    }
    let image = node.image();
    let mut topic_names: Vec<String> = if listed {
        requested.into_iter().map(|t| t.name).collect()
    } else {
        image.topics().map(|t| t.name.clone()).collect()
    };
    topic_names.sort();
    topic_names.dedup();
    let cursor = cursor.as_ref();
    if let Some(cursor) = cursor {
        topic_names.retain(|name| *name >= cursor.topic_name);
    }
    let limit = response_partition_limit.clamp(1, MAX_PARTITION_LIMIT);
    let mut emitted = 0;
    let mut topics = Vec::new();
    let mut next_cursor = None;
    for name in &topic_names {
        if emitted >= limit {
            next_cursor = Some(Cursor {
                topic_name: name.clone(),
                partition_index: 0,
                ..Cursor::default()
            });
            break;
        }
        let Some(record) = image.topic(name) else {
            let code = if cluster::validate_topic_name(name).is_ok() {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            } else {
                codes::INVALID_TOPIC_EXCEPTION
            };
            topics.push(DescribeTopicPartitionsResponseTopic {
                error_code: code,
                name: Some(name.clone()),
                is_internal: is_internal_topic(name),
                topic_authorized_operations: TOPIC_AUTHORIZED_OPERATIONS,
                ..DescribeTopicPartitionsResponseTopic::default()
            });
            continue;
        };
        let first = cursor
            .filter(|c| c.topic_name == *name)
            .map_or(0, |c| c.partition_index);
        let mut partitions = Vec::new();
        let mut truncated_at = None;
        for p in image.partitions_of(name).filter(|p| p.partition >= first) {
            if emitted >= limit {
                truncated_at = Some(p.partition);
                break;
            }
            partitions.push(partition_row(image, p));
            emitted += 1;
        }
        topics.push(DescribeTopicPartitionsResponseTopic {
            error_code: codes::NONE,
            name: Some(name.clone()),
            topic_id: wire_uuid(record.topic_id),
            is_internal: is_internal_topic(name),
            partitions,
            topic_authorized_operations: TOPIC_AUTHORIZED_OPERATIONS,
            ..DescribeTopicPartitionsResponseTopic::default()
        });
        if let Some(partition_index) = truncated_at {
            next_cursor = Some(Cursor {
                topic_name: name.clone(),
                partition_index,
                ..Cursor::default()
            });
            break;
        }
    }
    Outcome::Reply(DescribeTopicPartitionsResponse {
        topics,
        next_cursor,
        ..DescribeTopicPartitionsResponse::default()
    })
}
