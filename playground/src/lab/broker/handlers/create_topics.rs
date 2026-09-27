//! `CreateTopics` (api key 19) on the controller listener: the active
//! controller creates topics.
//!
//! Rows follow `ControllerApis.createTopics` and
//! `ReplicationControlManager.createTopics`. A name the request repeats, and
//! the raft metadata topic, never reach the controller: they answer
//! `INVALID_REQUEST` after the other rows, duplicates first. A node that is
//! not the active controller answers `NOT_CONTROLLER` on every row of the
//! request, with the message naming the controller it knows, unless nothing
//! was left to create. Per topic, in request order: an existing topic
//! answers `TOPIC_ALREADY_EXISTS`, a name that collides with an existing one
//! on `.` against `_`, or that Kafka's `Topic.validate` refuses, answers
//! `INVALID_TOPIC_EXCEPTION`, the configs are validated (`INVALID_CONFIG`),
//! then the counts or the manual assignment and the placement
//! ([`ControllerDecisions::create_topic`]). A `-1` count takes the
//! controller default (KIP-464). From v5 a successful row reports the
//! effective configuration (KIP-525). Every topic is planned against the
//! image as the request found it, and the records of all of them commit
//! together, so two new names that collide with each other are both
//! created, as in Kafka; a `validate_only` request runs every check and
//! commits nothing. The answer waits for the commit.
//!
//! [`ControllerDecisions::create_topic`]: crate::lab::controller::ControllerDecisions::create_topic

use std::collections::BTreeMap;

use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    create_topics_response::{CreatableTopicConfigs, CreatableTopicResult, CreateTopicsResponse},
};

use super::{
    super::{
        BrokerNode, cluster,
        dispatch::{Outcome, RequestCtx},
    },
    describe_configs::{effective_topic_configs, validate_topic_configs},
    forwarded::create_topics_error,
    wire_uuid,
};
use crate::lab::{
    codes,
    controller::{
        ControllerDecisions,
        decisions::{CreateTopicSpec, CreatedTopic},
    },
    net::{Ctx, NodeId},
};

/// The first version whose successful rows carry the effective configs.
const CONFIGS_VERSION: i16 = 5;

fn error_row(name: String, code: i16, message: Option<String>) -> CreatableTopicResult {
    CreatableTopicResult {
        name,
        error_code: code,
        error_message: message,
        ..CreatableTopicResult::default()
    }
}

/// Serve a `CreateTopics` as the controller.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    CreateTopicsRequest {
        topics,
        validate_only,
        ..
    }: CreateTopicsRequest,
) -> Outcome<CreateTopicsResponse> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for topic in &topics {
        *counts.entry(topic.name.as_str()).or_insert(0) += 1;
    }
    let mut duplicates: Vec<String> = Vec::new();
    let mut metadata_topic = false;
    let mut effective: Vec<&CreatableTopic> = Vec::new();
    for topic in &topics {
        if counts[topic.name.as_str()] > 1 {
            if !duplicates.contains(&topic.name) {
                duplicates.push(topic.name.clone());
            }
        } else if topic.name == cluster::CLUSTER_METADATA_TOPIC {
            metadata_topic = true;
        } else {
            effective.push(topic);
        }
    }
    let not_controller =
        |message: &str| create_topics_error(&topics, codes::NOT_CONTROLLER, Some(message));
    let mut results = Vec::with_capacity(topics.len());
    let mut records: Vec<MetadataRecord> = Vec::new();
    if !effective.is_empty() {
        if node.quorum.active.is_none() {
            return Outcome::Reply(not_controller(&node.quorum.not_controller_message()));
        }
        for topic in effective {
            let (row, created) = create_one(node, req.version, topic);
            records.extend(created);
            results.push(row);
        }
    }
    if validate_only {
        records.clear();
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
    let response = CreateTopicsResponse {
        topics: results,
        ..CreateTopicsResponse::default()
    };
    node.controller_write(ctx, req, records, response, not_controller)
}

/// Decide one topic against the active controller's image: its row, and the
/// records that create it.
fn create_one(
    node: &mut BrokerNode,
    version: i16,
    topic: &CreatableTopic,
) -> (CreatableTopicResult, Vec<MetadataRecord>) {
    let broker_config = node.config.clone();
    let Some(active) = node.quorum.active.as_mut() else {
        return (
            error_row(topic.name.clone(), codes::NOT_CONTROLLER, None),
            Vec::new(),
        );
    };
    if let Some(refusal) = ControllerDecisions::topic_refusal(&active.image, &topic.name) {
        return (
            error_row(topic.name.clone(), refusal.code, refusal.message),
            Vec::new(),
        );
    }
    let overrides: Vec<(String, Option<String>)> = topic
        .configs
        .iter()
        .map(|c| (c.name.clone(), c.value.clone()))
        .collect();
    let configs = match validate_topic_configs(&overrides) {
        Ok(configs) => configs,
        Err(message) => {
            return (
                error_row(topic.name.clone(), codes::INVALID_CONFIG, Some(message)),
                Vec::new(),
            );
        }
    };
    let spec = CreateTopicSpec {
        name: topic.name.clone(),
        partitions: topic.num_partitions,
        replication_factor: topic.replication_factor,
        assignments: topic
            .assignments
            .iter()
            .map(|assignment| {
                let replicas = assignment
                    .broker_ids
                    .iter()
                    .map(|&id| NodeId(u32::try_from(id).unwrap_or(u32::MAX)))
                    .collect();
                (assignment.partition_index, replicas)
            })
            .collect(),
        configs,
    };
    match active.decisions.create_topic(&active.image, &spec) {
        Ok(created) => {
            let row = created_row(&broker_config, version, &spec, &created);
            (row, created.records)
        }
        Err(refusal) => (
            error_row(topic.name.clone(), refusal.code, refusal.message),
            Vec::new(),
        ),
    }
}

/// The row of a topic the controller creates: its id and counts, and from
/// v5 its effective configuration (KIP-525).
fn created_row(
    config: &super::super::BrokerConfig,
    version: i16,
    spec: &CreateTopicSpec,
    created: &CreatedTopic,
) -> CreatableTopicResult {
    let mut result = CreatableTopicResult {
        name: created.name.clone(),
        topic_id: wire_uuid(created.topic_id),
        error_code: codes::NONE,
        error_message: None,
        num_partitions: created.partitions,
        replication_factor: created.replication_factor,
        configs: Some(Vec::new()),
        ..CreatableTopicResult::default()
    };
    if version >= CONFIGS_VERSION {
        result.configs = Some(
            effective_topic_configs(config, Some(&spec.configs), None)
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
