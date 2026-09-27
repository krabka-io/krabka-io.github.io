//! `FindCoordinator` (api key 10): which broker coordinates a group.
//!
//! A group's coordinator is the leader of its `__consumer_offsets` partition,
//! `Utils.abs(groupId.hashCode()) % 50`, when that leader is a live broker.
//! As in Kafka's `KafkaApis.getCoordinator`, a request that finds no
//! `__consumer_offsets` topic asks the active controller to create it and
//! answers `COORDINATOR_NOT_AVAILABLE` for every key; a later request finds
//! the coordinator once the creation commits. The lab has no transaction or share coordinator, so those key
//! types answer `COORDINATOR_NOT_AVAILABLE` per key; an unknown key type
//! fails the whole request with `INVALID_REQUEST`, as `CoordinatorType.forId`
//! throws.
//!
//! The messages follow `KafkaApis`: a v4+ row leaves the field at its
//! schema default, the empty string, and a v0-v3 answer carries
//! `Errors.message()` of its code, which for `NONE` is the enum name
//! `"NONE"`. A request that fails as a whole carries the message on every
//! v4+ row.

use krabka_protocol::owned::{
    find_coordinator_request::FindCoordinatorRequest,
    find_coordinator_response::{Coordinator, FindCoordinatorResponse},
};

use super::{
    super::{
        BrokerNode, cluster,
        dispatch::{Outcome, RequestCtx},
    },
    alive_broker,
};
use crate::lab::{codes, net::Ctx};

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;
const KEY_TYPE_SHARE: i8 = 2;

/// The first version that batches keys in `coordinator_keys`.
const BATCHED_KEYS_VERSION: i16 = 4;

/// Kafka's `Errors.message()` for the codes `FindCoordinator` answers.
fn error_message(error_code: i16) -> Option<String> {
    let message = match error_code {
        codes::NONE => "NONE",
        codes::COORDINATOR_NOT_AVAILABLE => "The coordinator is not available.",
        codes::INVALID_REQUEST => {
            "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."
        }
        _ => return None,
    };
    Some(message.to_string())
}

/// Serve a `FindCoordinator`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: FindCoordinatorRequest,
) -> Outcome<FindCoordinatorResponse> {
    let FindCoordinatorRequest {
        key,
        key_type,
        coordinator_keys,
        ..
    } = request;
    let keys: Vec<String> = if req.version < BATCHED_KEYS_VERSION {
        vec![key]
    } else {
        coordinator_keys
    };
    if !keys.is_empty()
        && !matches!(
            key_type,
            KEY_TYPE_GROUP | KEY_TYPE_TRANSACTION | KEY_TYPE_SHARE
        )
    {
        let rows = keys
            .into_iter()
            .map(|key| Coordinator {
                error_message: error_message(codes::INVALID_REQUEST),
                ..no_node(key, codes::INVALID_REQUEST)
            })
            .collect();
        return Outcome::Reply(respond(req.version, rows));
    }
    let topic_exists = node
        .image()
        .topic(cluster::CONSUMER_OFFSETS_TOPIC)
        .is_some();
    if key_type == KEY_TYPE_GROUP && !keys.is_empty() && !topic_exists {
        let topic = node.creatable_topic(cluster::CONSUMER_OFFSETS_TOPIC);
        node.create_topics_internally(ctx, vec![topic]);
    }
    let rows = keys
        .into_iter()
        .map(|key| {
            if key_type == KEY_TYPE_GROUP && topic_exists {
                resolve_group(node, key)
            } else {
                no_node(key, codes::COORDINATOR_NOT_AVAILABLE)
            }
        })
        .collect();
    Outcome::Reply(respond(req.version, rows))
}

/// The coordinator of `key`: the live leader of its partition.
fn resolve_group(node: &BrokerNode, key: String) -> Coordinator {
    let image = node.image();
    let count = image.topic_partition_count(cluster::CONSUMER_OFFSETS_TOPIC);
    let partition = cluster::group_partition(
        &key,
        if count > 0 {
            count
        } else {
            cluster::CONSUMER_OFFSETS_PARTITIONS
        },
    );
    let leader = image
        .partition(cluster::CONSUMER_OFFSETS_TOPIC, partition)
        .and_then(cluster::record_leader)
        .and_then(|leader| alive_broker(image, leader));
    match leader {
        Some(broker) => Coordinator {
            key,
            node_id: cluster::wire_id(broker.node_id),
            host: broker.host.clone(),
            port: i32::from(broker.port),
            error_code: codes::NONE,
            ..Coordinator::default()
        },
        None => no_node(key, codes::COORDINATOR_NOT_AVAILABLE),
    }
}

/// A row with Kafka's `Node.noNode()` in place of the coordinator.
fn no_node(key: String, error_code: i16) -> Coordinator {
    Coordinator {
        key,
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code,
        ..Coordinator::default()
    }
}

/// The response in the shape of `version`: one coordinator in the top-level
/// fields before v4, where an error answers `Node.noNode()` with its
/// message, and the list from v4 on.
fn respond(version: i16, coordinators: Vec<Coordinator>) -> FindCoordinatorResponse {
    if version >= BATCHED_KEYS_VERSION {
        return FindCoordinatorResponse {
            coordinators,
            ..FindCoordinatorResponse::default()
        };
    }
    let one = coordinators
        .into_iter()
        .next()
        .unwrap_or_else(|| no_node(String::new(), codes::COORDINATOR_NOT_AVAILABLE));
    let (node_id, host, port) = if one.error_code == codes::NONE {
        (one.node_id, one.host, one.port)
    } else {
        (-1, String::new(), -1)
    };
    FindCoordinatorResponse {
        error_code: one.error_code,
        error_message: error_message(one.error_code),
        node_id,
        host,
        port,
        ..FindCoordinatorResponse::default()
    }
}
