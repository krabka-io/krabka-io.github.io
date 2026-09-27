//! `CreatePartitions` (api key 37): more partitions for a topic, placed
//! round-robin like a creation or as the request assigns them.
//!
//! As in Kafka's `ControllerApis.createPartitions`, every duplicated name
//! answers `INVALID_REQUEST` once, ahead of the other rows, and grows
//! nothing. The other rows follow `ReplicationControlManager.createPartitions`
//! in request order: an unknown topic answers `UNKNOWN_TOPIC_OR_PARTITION`
//! with no message, a count that does not grow the topic
//! `INVALID_PARTITIONS`, a bad assignment `INVALID_REPLICA_ASSIGNMENT`, and a
//! placement the active brokers cannot satisfy `INVALID_REPLICATION_FACTOR`.
//! The rows of one request commit together, and a `validate_only` request
//! commits nothing.

use krabka_protocol::owned::{
    create_partitions_request::CreatePartitionsRequest,
    create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
};

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{codes, net::Ctx};

/// Serve a `CreatePartitions`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    request: CreatePartitionsRequest,
) -> Outcome<CreatePartitionsResponse> {
    let CreatePartitionsRequest {
        topics,
        validate_only,
        ..
    } = request;
    let mut duplicates: Vec<String> = Vec::new();
    for topic in &topics {
        let repeated = topics.iter().filter(|t| t.name == topic.name).count() > 1;
        if repeated && !duplicates.contains(&topic.name) {
            duplicates.push(topic.name.clone());
        }
    }
    let mut results: Vec<CreatePartitionsTopicResult> = duplicates
        .iter()
        .map(|name| CreatePartitionsTopicResult {
            name: name.clone(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Duplicate topic name.".to_string()),
            ..CreatePartitionsTopicResult::default()
        })
        .collect();
    let image = node.image().clone();
    let mut records = Vec::new();
    for topic in topics {
        if duplicates.contains(&topic.name) {
            continue;
        }
        let assignments: Option<Vec<Vec<i32>>> = topic
            .assignments
            .map(|list| list.into_iter().map(|a| a.broker_ids).collect());
        let planned = node.controller.plan_partitions(
            &image,
            &topic.name,
            topic.count,
            assignments.as_deref(),
        );
        let (error_code, error_message) = match planned {
            Ok(planned) => {
                records.extend(planned);
                (codes::NONE, None)
            }
            Err(refusal) => (refusal.code, refusal.message),
        };
        results.push(CreatePartitionsTopicResult {
            name: topic.name,
            error_code,
            error_message,
            ..CreatePartitionsTopicResult::default()
        });
    }
    if !validate_only && !records.is_empty() {
        node.apply_metadata(ctx, &records);
    }
    Outcome::Reply(CreatePartitionsResponse {
        results,
        ..CreatePartitionsResponse::default()
    })
}
