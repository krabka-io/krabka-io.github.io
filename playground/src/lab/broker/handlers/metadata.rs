//! `Metadata` (api key 3): the brokers and the partitions of the requested
//! topics, from the metadata image.
//!
//! The topic rows follow `KafkaApis.handleTopicMetadataRequest`: a null
//! `topics` (or an empty one at v0) asks for every topic; from v12 a row may
//! name its topic by id, and an id no topic has answers `UNKNOWN_TOPIC_ID`;
//! at v10 and v11 an id or a null name fails the whole request with
//! `INVALID_REQUEST`. The rows come in Kafka's order: unknown ids, then the
//! topics that exist, then the ones that do not. A missing topic answers
//! `UNKNOWN_TOPIC_OR_PARTITION` (`INVALID_TOPIC_EXCEPTION` for a name Kafka
//! refuses), or `LEADER_NOT_AVAILABLE` when the request allows
//! auto-creation: the topic is then created with the broker's
//! `num.partitions` and `default.replication.factor` (`__consumer_offsets`
//! with its own settings), and the next request sees it, as with Kafka's
//! asynchronous creation.
//!
//! A partition row comes from `KRaftMetadataCache.getPartitionMetadata`: a
//! leader with no registration (a leaderless partition among them) answers
//! `LEADER_NOT_AVAILABLE` with leader `-1`; a fenced or unregistered replica
//! is listed in `offline_replicas`; and v0 lists only live replicas, answering
//! `REPLICA_NOT_AVAILABLE` when it dropped one. The broker list holds the
//! unfenced brokers. Every version the codec knows, 0 to 13, is served.

use std::collections::BTreeMap;

use krabka_metadata::{MetadataImage, PartitionRecord};
use krabka_protocol::{
    owned::{
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::{
    super::{
        BrokerNode, TopicPlan, cluster,
        dispatch::{Outcome, RequestCtx},
    },
    CLUSTER_AUTHORIZED_OPERATIONS, NO_AUTHORIZED_OPERATIONS, TOPIC_AUTHORIZED_OPERATIONS,
    is_internal_topic, uuid_of, wire_uuid,
};
use crate::lab::{codes, net::Ctx};

/// The first version whose rows may carry a topic id or a null name.
const FIRST_TOPIC_ID_VERSION: i16 = 12;
/// The versions that carry `cluster_authorized_operations` (KIP-430).
const CLUSTER_AUTHORIZED_OPERATIONS_VERSIONS: std::ops::RangeInclusive<i16> = 8..=10;
/// The first version that carries `topic_authorized_operations`.
const FIRST_TOPIC_AUTHORIZED_OPERATIONS_VERSION: i16 = 8;

/// The topics a request asks for, before existence is checked.
#[derive(Debug, Default, PartialEq, Eq)]
struct RequestedTopics {
    all: bool,
    unknown_ids: Vec<WireUuid>,
    names: Vec<String>,
}

fn lookup_requested_topics(
    image: &MetadataImage,
    topics: Option<&[MetadataRequestTopic]>,
    version: i16,
) -> Result<RequestedTopics, i16> {
    let topics = match topics {
        Some(topics) if !(version == 0 && topics.is_empty()) => topics,
        _ => {
            let mut names: Vec<String> = image.topics().map(|t| t.name.clone()).collect();
            names.sort();
            return Ok(RequestedTopics {
                all: true,
                names,
                ..RequestedTopics::default()
            });
        }
    };
    let uses_ids = topics
        .iter()
        .any(|t| t.name.is_none() || t.topic_id != WireUuid::ZERO);
    if version < FIRST_TOPIC_ID_VERSION && uses_ids {
        return Err(codes::INVALID_REQUEST);
    }
    let mut requested = RequestedTopics::default();
    let ids: Vec<WireUuid> = topics
        .iter()
        .map(|t| t.topic_id)
        .filter(|id| *id != WireUuid::ZERO)
        .fold(Vec::new(), |mut ids, id| {
            if !ids.contains(&id) {
                ids.push(id);
            }
            ids
        });
    if !ids.is_empty() {
        for id in ids {
            match image.topic_name_by_id(&uuid_of(id)) {
                Some(name) => push_distinct(&mut requested.names, name),
                None => requested.unknown_ids.push(id),
            }
        }
        return Ok(requested);
    }
    for topic in topics {
        let name = topic.name.as_deref().ok_or(codes::UNKNOWN_SERVER_ERROR)?;
        push_distinct(&mut requested.names, name);
    }
    Ok(requested)
}

fn push_distinct(names: &mut Vec<String>, name: &str) {
    if !names.iter().any(|n| n == name) {
        names.push(name.to_string());
    }
}

/// Kafka's `MetadataRequest.getErrorResponse`: every requested row carries
/// the error, and nothing else is filled.
fn error_response(topics: Option<Vec<MetadataRequestTopic>>, error_code: i16) -> MetadataResponse {
    MetadataResponse {
        topics: topics
            .into_iter()
            .flatten()
            .map(|t| MetadataResponseTopic {
                error_code,
                name: Some(t.name.unwrap_or_default()),
                topic_id: t.topic_id,
                ..MetadataResponseTopic::default()
            })
            .collect(),
        ..MetadataResponse::default()
    }
}

/// Serve a `Metadata`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: MetadataRequest,
) -> Outcome<MetadataResponse> {
    let MetadataRequest {
        topics: requested_topics,
        allow_auto_topic_creation,
        include_cluster_authorized_operations,
        include_topic_authorized_operations,
        ..
    } = request;
    let requested =
        match lookup_requested_topics(node.image(), requested_topics.as_deref(), req.version) {
            Ok(requested) => requested,
            Err(code) => return Outcome::Reply(error_response(requested_topics, code)),
        };
    let (existing, missing): (Vec<String>, Vec<String>) = requested
        .names
        .into_iter()
        .partition(|name| node.image().topic(name).is_some());
    let missing_rows: Vec<MetadataResponseTopic> = if requested.all {
        Vec::new()
    } else if allow_auto_topic_creation {
        // Kafka's `AutoTopicCreationManager.createTopics`: the names it
        // refuses first, then the ones it creates.
        let (invalid, creatable): (Vec<String>, Vec<String>) = missing
            .into_iter()
            .partition(|name| cluster::validate_topic_name(name).is_err());
        for name in &creatable {
            auto_create_topic(node, ctx, name);
        }
        invalid
            .iter()
            .map(|name| missing_row(name, codes::INVALID_TOPIC_EXCEPTION))
            .chain(
                creatable
                    .iter()
                    .map(|name| missing_row(name, codes::LEADER_NOT_AVAILABLE)),
            )
            .collect()
    } else {
        missing
            .iter()
            .map(|name| {
                let code = if cluster::validate_topic_name(name).is_ok() {
                    codes::UNKNOWN_TOPIC_OR_PARTITION
                } else {
                    codes::INVALID_TOPIC_EXCEPTION
                };
                missing_row(name, code)
            })
            .collect()
    };
    let unknown_ids = requested
        .unknown_ids
        .iter()
        .map(|id| MetadataResponseTopic {
            error_code: codes::UNKNOWN_TOPIC_ID,
            name: None,
            topic_id: *id,
            ..MetadataResponseTopic::default()
        });
    let mut known: Vec<MetadataResponseTopic> = existing
        .iter()
        .filter_map(|name| node.image().topic(name))
        .map(|record| topic_row(node.image(), &record.name, record.topic_id, req.version))
        .chain(missing_rows)
        .collect();
    if req.version >= FIRST_TOPIC_AUTHORIZED_OPERATIONS_VERSION
        && include_topic_authorized_operations
    {
        for row in &mut known {
            row.topic_authorized_operations = TOPIC_AUTHORIZED_OPERATIONS;
        }
    }
    let topics: Vec<MetadataResponseTopic> = unknown_ids.chain(known).collect();
    let cluster_authorized_operations = if CLUSTER_AUTHORIZED_OPERATIONS_VERSIONS
        .contains(&req.version)
        && include_cluster_authorized_operations
    {
        CLUSTER_AUTHORIZED_OPERATIONS
    } else {
        NO_AUTHORIZED_OPERATIONS
    };
    Outcome::Reply(MetadataResponse {
        brokers: broker_rows(node.image()),
        cluster_id: Some(cluster::cluster_id_string(node.image().cluster_id())),
        controller_id: node.controller_id(),
        topics,
        cluster_authorized_operations,
        ..MetadataResponse::default()
    })
}

/// Create a missing topic with the broker defaults, `__consumer_offsets`
/// with the group coordinator's settings, as Kafka's
/// `AutoTopicCreationManager.creatableTopicResult` does. A creation the
/// controller refuses leaves the topic missing, and the next request tries
/// again.
fn auto_create_topic(node: &mut BrokerNode, ctx: &mut Ctx<'_>, name: &str) {
    let topic_id = cluster::new_topic_id(ctx);
    let planned = if name == cluster::CONSUMER_OFFSETS_TOPIC {
        node.controller.plan_consumer_offsets(&node.image, topic_id)
    } else {
        let plan = TopicPlan {
            name: name.to_string(),
            partitions: node.config().default_partitions,
            replication_factor: node.config().default_replication_factor,
            configs: BTreeMap::new(),
            assignments: Vec::new(),
        };
        node.controller.plan_topic(&node.image, &plan, topic_id)
    };
    if let Ok(planned) = planned {
        node.apply_metadata(ctx, &planned.records);
    }
}

/// The unfenced brokers, ascending by id: Kafka's
/// `MetadataCache.getAliveBrokerNodes`.
#[must_use]
pub fn broker_rows(image: &MetadataImage) -> Vec<MetadataResponseBroker> {
    let mut brokers: Vec<MetadataResponseBroker> = image
        .brokers()
        .filter(|b| !b.fenced)
        .map(|b| MetadataResponseBroker {
            node_id: cluster::wire_id(b.node_id),
            host: b.host.clone(),
            port: i32::from(b.port),
            rack: b.rack.clone(),
            ..MetadataResponseBroker::default()
        })
        .collect();
    brokers.sort_by_key(|b| b.node_id);
    brokers
}

/// Whether the image knows `broker` as a live one: registered and not
/// fenced.
#[must_use]
pub fn broker_is_live(image: &MetadataImage, broker: krabka_metadata::NodeId) -> bool {
    image.broker(broker).is_some_and(|b| !b.fenced)
}

/// The leader `Metadata` reports, Kafka's `getAliveEndpoint`: the leader
/// when it has a registration, fenced or not, else `-1`.
#[must_use]
pub fn reported_leader(image: &MetadataImage, partition: &PartitionRecord) -> i32 {
    if image.broker(partition.leader).is_some() {
        cluster::wire_id(partition.leader)
    } else {
        -1
    }
}

/// The replicas whose broker is fenced or unregistered, Kafka's
/// `getOfflineReplicas`.
#[must_use]
pub fn offline_replicas(image: &MetadataImage, partition: &PartitionRecord) -> Vec<i32> {
    partition
        .replicas
        .iter()
        .filter(|r| !broker_is_live(image, **r))
        .map(|r| cluster::wire_id(*r))
        .collect()
}

/// One partition row, Kafka's `KRaftMetadataCache.getPartitionMetadata`.
fn partition_row(
    image: &MetadataImage,
    p: &PartitionRecord,
    version: i16,
) -> MetadataResponsePartition {
    // Only v0 drops the replicas that are not live (`errorUnavailableEndpoints`).
    let listed = |ids: &[krabka_metadata::NodeId]| -> Vec<i32> {
        ids.iter()
            .filter(|r| version > 0 || broker_is_live(image, **r))
            .map(|r| cluster::wire_id(*r))
            .collect()
    };
    let replica_nodes = listed(&p.replicas);
    let isr_nodes = listed(&p.isr);
    let leader_id = reported_leader(image, p);
    let error_code = if leader_id < 0 {
        codes::LEADER_NOT_AVAILABLE
    } else if replica_nodes.len() < p.replicas.len() || isr_nodes.len() < p.isr.len() {
        codes::REPLICA_NOT_AVAILABLE
    } else {
        codes::NONE
    };
    MetadataResponsePartition {
        error_code,
        partition_index: p.partition,
        leader_id,
        leader_epoch: p.leader_epoch.0,
        replica_nodes,
        isr_nodes,
        offline_replicas: offline_replicas(image, p),
        ..MetadataResponsePartition::default()
    }
}

fn topic_row(
    image: &MetadataImage,
    name: &str,
    topic_id: uuid::Uuid,
    version: i16,
) -> MetadataResponseTopic {
    MetadataResponseTopic {
        error_code: codes::NONE,
        name: Some(name.to_string()),
        topic_id: wire_uuid(topic_id),
        is_internal: is_internal_topic(name),
        partitions: image
            .partitions_of(name)
            .map(|p| partition_row(image, p, version))
            .collect(),
        ..MetadataResponseTopic::default()
    }
}

fn missing_row(name: &str, error_code: i16) -> MetadataResponseTopic {
    MetadataResponseTopic {
        error_code,
        name: Some(name.to_string()),
        topic_id: WireUuid::ZERO,
        is_internal: is_internal_topic(name),
        ..MetadataResponseTopic::default()
    }
}
