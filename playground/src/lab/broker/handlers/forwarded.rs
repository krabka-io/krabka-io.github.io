//! The controller apis a client sends to a broker, which the broker forwards
//! to the active controller: `CreateTopics` (19), `DeleteTopics` (20),
//! `CreatePartitions` (37) and `DescribeQuorum` (55), as Kafka's
//! `KafkaApis.forwardToController` does in `KRaft` mode.
//!
//! The controller's answer comes back byte for byte. When the forwarding
//! fails, the broker answers in the api's own error shape, Kafka's
//! `getErrorResponse`: `REQUEST_TIMED_OUT` when no controller answered in
//! time, `UNKNOWN_SERVER_ERROR` when the envelope failed, both without a
//! message.

use krabka_protocol::{
    owned::{
        create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
        create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::DescribeQuorumResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{codes, net::Ctx};

/// The first `DeleteTopics` version that names topics in `topics`.
const DELETE_BY_STATE_VERSION: i16 = 6;

/// Kafka's `CreateTopicsRequest.getErrorResponse`: every topic of the
/// request, with `error_code` and `message`.
#[must_use]
pub fn create_topics_error(
    topics: &[CreatableTopic],
    error_code: i16,
    message: Option<&str>,
) -> CreateTopicsResponse {
    CreateTopicsResponse {
        topics: topics
            .iter()
            .map(|topic| CreatableTopicResult {
                name: topic.name.clone(),
                error_code,
                error_message: message.map(str::to_owned),
                ..CreatableTopicResult::default()
            })
            .collect(),
        ..CreateTopicsResponse::default()
    }
}

/// Kafka's `DeleteTopicsRequest.getErrorResponse`: every topic of the
/// request, by name before v6 and as the request names it from v6, with
/// `error_code` and `message`.
#[must_use]
pub fn delete_topics_error(
    topic_names: &[String],
    topics: &[DeleteTopicState],
    version: i16,
    error_code: i16,
    message: Option<&str>,
) -> DeleteTopicsResponse {
    let row = |name: Option<String>, topic_id: WireUuid| DeletableTopicResult {
        name,
        topic_id,
        error_code,
        error_message: message.map(str::to_owned),
        ..DeletableTopicResult::default()
    };
    let responses = if version >= DELETE_BY_STATE_VERSION {
        topics
            .iter()
            .map(|topic| row(topic.name.clone(), topic.topic_id))
            .collect()
    } else {
        topic_names
            .iter()
            .map(|name| row(Some(name.clone()), WireUuid::ZERO))
            .collect()
    };
    DeleteTopicsResponse {
        responses,
        ..DeleteTopicsResponse::default()
    }
}

/// Kafka's `CreatePartitionsRequest.getErrorResponse`: every topic of the
/// request, with `error_code` and `message`.
#[must_use]
pub fn create_partitions_error(
    topics: &[CreatePartitionsTopic],
    error_code: i16,
    message: Option<&str>,
) -> CreatePartitionsResponse {
    CreatePartitionsResponse {
        results: topics
            .iter()
            .map(|topic| CreatePartitionsTopicResult {
                name: topic.name.clone(),
                error_code,
                error_message: message.map(str::to_owned),
                ..CreatePartitionsTopicResult::default()
            })
            .collect(),
        ..CreatePartitionsResponse::default()
    }
}

/// Kafka's `DescribeQuorumRequest.getErrorResponse`: the top-level error
/// alone.
#[must_use]
pub fn describe_quorum_error(error_code: i16) -> DescribeQuorumResponse {
    DescribeQuorumResponse {
        error_code,
        ..DescribeQuorumResponse::default()
    }
}

/// Forward a `CreateTopics`.
pub fn create_topics(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    CreateTopicsRequest { topics, .. }: CreateTopicsRequest,
) -> Outcome<CreateTopicsResponse> {
    node.forward(
        ctx,
        req,
        &create_topics_error(&topics, codes::REQUEST_TIMED_OUT, None),
        &create_topics_error(&topics, codes::UNKNOWN_SERVER_ERROR, None),
    )
}

/// Forward a `DeleteTopics`.
pub fn delete_topics(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    DeleteTopicsRequest {
        topic_names,
        topics,
        ..
    }: DeleteTopicsRequest,
) -> Outcome<DeleteTopicsResponse> {
    let error = |code| delete_topics_error(&topic_names, &topics, req.version, code, None);
    node.forward(
        ctx,
        req,
        &error(codes::REQUEST_TIMED_OUT),
        &error(codes::UNKNOWN_SERVER_ERROR),
    )
}

/// Forward a `CreatePartitions`.
pub fn create_partitions(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    CreatePartitionsRequest { topics, .. }: CreatePartitionsRequest,
) -> Outcome<CreatePartitionsResponse> {
    node.forward(
        ctx,
        req,
        &create_partitions_error(&topics, codes::REQUEST_TIMED_OUT, None),
        &create_partitions_error(&topics, codes::UNKNOWN_SERVER_ERROR, None),
    )
}

/// Forward a `DescribeQuorum`.
pub fn describe_quorum(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    _request: DescribeQuorumRequest,
) -> Outcome<DescribeQuorumResponse> {
    node.forward(
        ctx,
        req,
        &describe_quorum_error(codes::REQUEST_TIMED_OUT),
        &describe_quorum_error(codes::UNKNOWN_SERVER_ERROR),
    )
}
