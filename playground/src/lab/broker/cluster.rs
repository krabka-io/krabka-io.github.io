//! The broker's view of the cluster: the ids, names and hashes every handler
//! shares.
//!
//! Every fact a handler answers about the cluster comes from the broker's
//! [`MetadataImage`], the replay of the committed metadata log, and the image
//! changes only through
//! [`BrokerNode::apply_metadata`](super::BrokerNode::apply_metadata). The
//! records themselves come from the active controller's decisions
//! ([`crate::lab::controller::decisions`]), whose topic-name check this
//! module re-exports for the handlers that check a name.

use base64::Engine as _;
use krabka_metadata::{MetadataImage, NodeId as MetaNodeId, PartitionRecord};
use uuid::Uuid;

pub use crate::lab::controller::decisions::validate_topic_name;

/// The cluster id of every lab cluster.
pub const LAB_CLUSTER_ID: Uuid = Uuid::from_u128(0x4b72_6162_6b61_4c61_6243_6c75_7374_6572);

/// The consumer-offsets topic the group coordinator lives on.
pub const CONSUMER_OFFSETS_TOPIC: &str = "__consumer_offsets";
/// Kafka's `offsets.topic.num.partitions`.
pub const CONSUMER_OFFSETS_PARTITIONS: i32 = 50;
/// Kafka's `offsets.topic.replication.factor`, which the lab caps at the
/// number of brokers.
pub const CONSUMER_OFFSETS_REPLICATION_FACTOR: i16 = 3;

/// The name of the raft metadata topic, which no client may create.
pub const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// The host name a broker registers: the client module resolves `node-<id>`
/// back to the node's Kafka endpoint.
#[must_use]
pub fn broker_host(id: i32) -> String {
    format!("node-{id}")
}

/// The wire form of a cluster id: Kafka's `Uuid.toString()`, URL-safe base64
/// of the 16 bytes without padding.
#[must_use]
pub fn cluster_id_string(id: Uuid) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes())
}

/// The cluster id a wire string names, Kafka's `Uuid.fromString`, or `None`
/// for a string that is not one.
#[must_use]
pub fn parse_cluster_id(id: &str) -> Option<Uuid> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(id)
        .ok()?;
    Uuid::from_slice(&bytes).ok()
}

/// Java's `String.hashCode()` over the UTF-16 code units of `s`.
#[must_use]
pub fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16().fold(0i32, |h, unit| {
        h.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// The `__consumer_offsets` partition of a group: Kafka's
/// `Utils.abs(groupId.hashCode()) % partitionCount`, where `abs` maps
/// `Integer.MIN_VALUE` to zero.
#[must_use]
pub fn group_partition(group_id: &str, partition_count: i32) -> i32 {
    let hash = java_string_hash(group_id);
    let positive = if hash == i32::MIN { 0 } else { hash.abs() };
    positive % partition_count.max(1)
}

/// The wire id of a metadata node id.
#[must_use]
pub fn wire_id(id: MetaNodeId) -> i32 {
    i32::try_from(id.0).unwrap_or(-1)
}

/// The metadata image's "no leader". The `KRaft` translation of the metadata
/// crate maps Kafka's `NO_LEADER` (`-1`) to node 0, which no lab broker uses.
pub const NO_LEADER: MetaNodeId = MetaNodeId(0);

/// The wire id of the leader a partition record names, or `None` for a
/// leaderless partition.
#[must_use]
pub fn record_leader(record: &PartitionRecord) -> Option<i32> {
    (record.leader != NO_LEADER)
        .then(|| wire_id(record.leader))
        .filter(|id| *id >= 0)
}

/// The metadata node id of a wire broker id.
#[must_use]
pub fn meta_id(id: i32) -> MetaNodeId {
    MetaNodeId(u64::try_from(id).unwrap_or(u64::MAX))
}

/// The ids of the brokers that are registered, not fenced and not in
/// controlled shutdown, ascending: Kafka's alive brokers.
#[must_use]
pub fn active_brokers(image: &MetadataImage) -> Vec<i32> {
    let mut ids: Vec<i32> = image
        .brokers()
        .filter(|b| !b.fenced && !b.in_controlled_shutdown)
        .map(|b| wire_id(b.node_id))
        .collect();
    ids.sort_unstable();
    ids
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn java_hash_and_group_partition_match_the_jvm() {
        assert!(java_string_hash("") == 0);
        assert!(java_string_hash("abc") == 96_354);
        assert!(java_string_hash("consumer-group") == -1_738_392_088);
        assert!(java_string_hash("polygenelubricants") == i32::MIN);
        for (group, count, expected) in [
            ("", 50, 0),
            ("abc", 7, 6),
            ("abc", 1, 0),
            ("consumer-group", 50, 38),
            ("🦀", 50, 2),
            ("polygenelubricants", 50, 0),
        ] {
            assert!(group_partition(group, count) == expected, "{group:?}");
        }
    }

    #[test]
    fn cluster_ids_use_kafka_base64_both_ways() {
        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert!(cluster_id_string(id) == "AQIDBAUGBwgJCgsMDQ4PEA");
        assert!(parse_cluster_id("AQIDBAUGBwgJCgsMDQ4PEA") == Some(id));
        assert!(parse_cluster_id(&cluster_id_string(LAB_CLUSTER_ID)) == Some(LAB_CLUSTER_ID));
        assert!(parse_cluster_id("not a cluster id").is_none());
        assert!(parse_cluster_id("AQID").is_none());
    }
}
