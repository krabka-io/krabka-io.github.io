//! `CreateTopics` (api key 19): topics through the local controller.
//!
//! Rows follow `ControllerApis.createTopics` and
//! `ReplicationControlManager.createTopics`. Per topic, in request order: the
//! name is checked (`INVALID_TOPIC_EXCEPTION`), an existing topic answers
//! `TOPIC_ALREADY_EXISTS`, a name that collides with an existing one on `.`
//! against `_` answers `INVALID_TOPIC_EXCEPTION`, the configs are validated
//! (`INVALID_CONFIG`), then the counts or the manual assignment
//! (`INVALID_REPLICATION_FACTOR`, `INVALID_PARTITIONS`, `INVALID_REQUEST`,
//! `INVALID_REPLICA_ASSIGNMENT`) and the placement. A `-1` count takes the
//! broker default (KIP-464). From v5 a successful row reports the effective
//! configuration (KIP-525). Every topic is planned against the image as the
//! request found it, and the records of all of them commit together, so two
//! new names that collide with each other are both created, as in Kafka; a
//! `validate_only` request runs every check and commits nothing. A
//! duplicated name answers `INVALID_REQUEST` after the other rows, then the
//! raft metadata topic does.

use std::collections::BTreeMap;

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    create_topics_response::{CreatableTopicConfigs, CreatableTopicResult, CreateTopicsResponse},
};

use super::{
    super::{
        BrokerNode, PlannedTopic, TopicPlan, cluster,
        dispatch::{Outcome, RequestCtx},
    },
    describe_configs::{effective_topic_configs, validate_topic_configs},
    wire_uuid,
};
use crate::lab::{codes, net::Ctx};

/// The first version whose successful rows carry the effective configs.
const CONFIGS_VERSION: i16 = 5;

/// A `-1` count means the broker default (KIP-464).
fn resolve_default<T: PartialEq + From<i8>>(requested: T, default: T) -> T {
    if requested == T::from(-1) {
        default
    } else {
        requested
    }
}

fn error_row(name: String, code: i16, message: Option<String>) -> CreatableTopicResult {
    CreatableTopicResult {
        name,
        error_code: code,
        error_message: message,
        ..CreatableTopicResult::default()
    }
}

/// Serve a `CreateTopics`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: CreateTopicsRequest,
) -> Outcome<CreateTopicsResponse> {
    let CreateTopicsRequest {
        topics,
        validate_only,
        ..
    } = request;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for topic in &topics {
        *counts.entry(topic.name.clone()).or_insert(0) += 1;
    }
    let mut duplicates: Vec<String> = Vec::new();
    let mut metadata_topic = false;
    let image = node.image().clone();
    let mut results = Vec::with_capacity(topics.len());
    let mut records: Vec<MetadataRecord> = Vec::new();
    for topic in topics {
        if counts[&topic.name] > 1 {
            if !duplicates.contains(&topic.name) {
                duplicates.push(topic.name);
            }
            continue;
        }
        if topic.name == cluster::CLUSTER_METADATA_TOPIC {
            metadata_topic = true;
            continue;
        }
        let (result, planned) = create_one(node, ctx, &image, req.version, topic);
        records.extend(planned);
        results.push(result);
    }
    if !validate_only && !records.is_empty() {
        node.apply_metadata(ctx, &records);
    }
    results.extend(duplicates.into_iter().map(|name| {
        error_row(
            name,
            codes::INVALID_REQUEST,
            Some("Duplicate topic name.".to_string()),
        )
    }));
    if metadata_topic {
        results.push(error_row(
            cluster::CLUSTER_METADATA_TOPIC.to_string(),
            codes::INVALID_REQUEST,
            Some(format!(
                "Creation of internal topic {} is prohibited.",
                cluster::CLUSTER_METADATA_TOPIC
            )),
        ));
    }
    Outcome::Reply(CreateTopicsResponse {
        topics: results,
        ..CreateTopicsResponse::default()
    })
}

/// Plan one topic against `image`: its row, and the records that create it.
fn create_one(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    image: &MetadataImage,
    version: i16,
    topic: CreatableTopic,
) -> (CreatableTopicResult, Vec<MetadataRecord>) {
    let CreatableTopic {
        name,
        num_partitions,
        replication_factor,
        assignments,
        configs,
        ..
    } = topic;
    if let Err(message) = cluster::validate_topic_name(&name) {
        return (
            error_row(name, codes::INVALID_TOPIC_EXCEPTION, Some(message)),
            Vec::new(),
        );
    }
    if image.topic(&name).is_some() {
        let message = format!("Topic '{name}' already exists.");
        return (
            error_row(name, codes::TOPIC_ALREADY_EXISTS, Some(message)),
            Vec::new(),
        );
    }
    if let Some(existing) = cluster::colliding_topic(image, &name) {
        let message = format!("Topic '{name}' collides with existing topic: {existing}");
        return (
            error_row(name, codes::INVALID_TOPIC_EXCEPTION, Some(message)),
            Vec::new(),
        );
    }
    let overrides: Vec<(String, Option<String>)> =
        configs.into_iter().map(|c| (c.name, c.value)).collect();
    let configs = match validate_topic_configs(&overrides) {
        Ok(configs) => configs,
        Err(message) => {
            return (
                error_row(name, codes::INVALID_CONFIG, Some(message)),
                Vec::new(),
            );
        }
    };
    let manual = !assignments.is_empty();
    let plan = TopicPlan {
        name,
        partitions: if manual {
            num_partitions
        } else {
            resolve_default(num_partitions, node.config().default_partitions)
        },
        replication_factor: if manual {
            replication_factor
        } else {
            resolve_default(replication_factor, node.config().default_replication_factor)
        },
        configs,
        assignments: assignments
            .into_iter()
            .map(|a| (a.partition_index, a.broker_ids))
            .collect(),
    };
    let topic_id = cluster::new_topic_id(ctx);
    match node.controller.plan_topic(image, &plan, topic_id) {
        Ok(planned) => (created_row(node, version, &plan, &planned), planned.records),
        Err(refusal) => (
            error_row(plan.name, refusal.code, refusal.message),
            Vec::new(),
        ),
    }
}

/// The row of a topic the plan creates: its id and counts, and from v5 its
/// effective configuration (KIP-525).
fn created_row(
    node: &BrokerNode,
    version: i16,
    plan: &TopicPlan,
    planned: &PlannedTopic,
) -> CreatableTopicResult {
    let mut result = CreatableTopicResult {
        name: plan.name.clone(),
        topic_id: wire_uuid(planned.topic_id),
        error_code: codes::NONE,
        error_message: None,
        num_partitions: i32::try_from(planned.assignments.len()).unwrap_or(i32::MAX),
        replication_factor: planned
            .assignments
            .first()
            .and_then(|r| i16::try_from(r.len()).ok())
            .unwrap_or(-1),
        configs: Some(Vec::new()),
        ..CreatableTopicResult::default()
    };
    if version >= CONFIGS_VERSION {
        result.configs = Some(
            effective_topic_configs(node.config(), Some(&plan.configs), None)
                .into_iter()
                .map(|entry| CreatableTopicConfigs {
                    name: entry.name,
                    value: entry.value,
                    read_only: entry.read_only,
                    config_source: entry.config_source,
                    is_sensitive: entry.is_sensitive,
                    ..CreatableTopicConfigs::default()
                })
                .collect(),
        );
    }
    result
}
