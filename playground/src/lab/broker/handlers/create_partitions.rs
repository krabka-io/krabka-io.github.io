//! `CreatePartitions` (api key 37) on the controller listener: the active
//! controller grows topics.
//!
//! As in Kafka's `ControllerApis.createPartitions`, every duplicated name
//! answers `INVALID_REQUEST` once, ahead of the other rows, and grows
//! nothing. When other rows remain, a node that is not the active
//! controller answers `NOT_CONTROLLER` on every row of the request. The
//! other rows follow `ReplicationControlManager.createPartitions` in request
//! order ([`ControllerDecisions::create_partitions`]): an unknown topic
//! answers `UNKNOWN_TOPIC_OR_PARTITION` with no message, a count that does
//! not grow the topic `INVALID_PARTITIONS`, a bad assignment
//! `INVALID_REPLICA_ASSIGNMENT`, and a placement the brokers cannot satisfy
//! `INVALID_REPLICATION_FACTOR`. The rows of one request commit together,
//! and the answer waits for the commit; a `validate_only` request commits
//! nothing.
//!
//! [`ControllerDecisions::create_partitions`]: crate::lab::controller::ControllerDecisions::create_partitions

use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::{
    create_partitions_request::CreatePartitionsRequest,
    create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
};

use super::{
    super::{
        BrokerNode,
        dispatch::{Outcome, RequestCtx},
    },
    forwarded::create_partitions_error,
};
use crate::lab::{
    codes,
    net::{Ctx, NodeId},
};

/// Serve a `CreatePartitions` as the controller.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    CreatePartitionsRequest {
        topics,
        validate_only,
        ..
    }: CreatePartitionsRequest,
) -> Outcome<CreatePartitionsResponse> {
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
    let not_controller =
        |message: &str| create_partitions_error(&topics, codes::NOT_CONTROLLER, Some(message));
    let growing: Vec<_> = topics
        .iter()
        .filter(|t| !duplicates.contains(&t.name))
        .collect();
    if growing.is_empty() {
        return Outcome::Reply(CreatePartitionsResponse {
            results,
            ..CreatePartitionsResponse::default()
        });
    }
    let Some(active) = node.quorum.active.as_mut() else {
        return Outcome::Reply(not_controller(&node.quorum.not_controller_message()));
    };
    let mut records: Vec<MetadataRecord> = Vec::new();
    for topic in growing {
        let assignments: Option<Vec<Vec<NodeId>>> = topic.assignments.as_ref().map(|list| {
            list.iter()
                .map(|a| {
                    a.broker_ids
                        .iter()
                        .map(|&id| NodeId(u32::try_from(id).unwrap_or(u32::MAX)))
                        .collect()
                })
                .collect()
        });
        let decided = active.decisions.create_partitions(
            &active.image,
            &topic.name,
            topic.count,
            assignments.as_deref(),
        );
        let (error_code, error_message) = match decided {
            Ok(planned) => {
                records.extend(planned);
                (codes::NONE, None)
            }
            Err(refusal) => (refusal.code, refusal.message),
        };
        results.push(CreatePartitionsTopicResult {
            name: topic.name.clone(),
            error_code,
            error_message,
            ..CreatePartitionsTopicResult::default()
        });
    }
    if validate_only {
        records.clear();
    }
    let response = CreatePartitionsResponse {
        results,
        ..CreatePartitionsResponse::default()
    };
    node.controller_write(ctx, req, records, response, not_controller)
}
