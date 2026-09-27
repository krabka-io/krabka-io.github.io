//! The client's view of the cluster: brokers, topics, partitions with their
//! leader and epoch, the controller, and the cluster id.
//!
//! The cache is the replay of the last `Metadata` responses. A broker's
//! endpoint comes from its advertised host: a lab broker advertises
//! `node-<id>`, and the cache maps it to [`Endpoint::kafka`] of that node.

use std::collections::{BTreeMap, BTreeSet};

use krabka_protocol::{owned::metadata_response::MetadataResponse, primitives::uuid::Uuid};
use serde_json::{Value, json};

use crate::lab::{
    codes,
    net::{Endpoint, Millis, NodeId, node_for_ip},
};

/// One broker of the cluster.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    /// The lab endpoint the host names.
    pub endpoint: Endpoint,
}

/// One partition of a topic.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PartitionInfo {
    pub index: i32,
    /// The leader's broker id, or `-1` when the partition has no leader.
    pub leader: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub error_code: i16,
}

/// One topic with its partitions.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopicInfo {
    pub name: String,
    pub topic_id: Uuid,
    pub is_internal: bool,
    pub partitions: BTreeMap<i32, PartitionInfo>,
}

/// The cluster as the last metadata responses described it.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct MetadataCache {
    pub cluster_id: Option<String>,
    /// The controller id the last response named, or `-1`.
    pub controller_id: i32,
    pub brokers: BTreeMap<i32, BrokerInfo>,
    pub topics: BTreeMap<String, TopicInfo>,
    /// Topics the last response reported as unknown.
    pub unknown_topics: BTreeSet<String>,
    /// When the last response was applied.
    pub updated_at: Option<Millis>,
    /// Counts the responses applied.
    pub version: u64,
}

/// The lab endpoint of a broker that advertises `host`: `node-<id>` names
/// the node directly, as does a virtual address `10.0.x.y` (a real broker
/// the page runs in a Worker advertises one); any other host falls back to
/// the broker id as the node id.
#[must_use]
pub fn endpoint_for_host(host: &str, node_id: i32) -> Endpoint {
    let node = host
        .strip_prefix("node-")
        .and_then(|rest| rest.parse::<u32>().ok())
        .or_else(|| host.parse().ok().and_then(node_for_ip).map(|node| node.0))
        .or_else(|| u32::try_from(node_id).ok())
        .unwrap_or(0);
    Endpoint::kafka(NodeId(node))
}

/// A topic id as the page shows it.
#[must_use]
pub fn uuid_hex(id: Uuid) -> String {
    uuid::Uuid::from_bytes(id.0).simple().to_string()
}

/// Whether `response` tells `cache` something about a partition's
/// leadership it did not know: a partition the cache lacks, a topic whose id
/// changed, a partition without a leader epoch, or a higher leader epoch.
/// These are the cases in which Kafka's `Metadata.updateLatestMetadata`
/// starts its count of equivalent responses over; any other answer is
/// equivalent to the last one.
#[must_use]
pub fn moves_epochs(cache: &MetadataCache, response: &MetadataResponse) -> bool {
    response
        .topics
        .iter()
        .filter(|topic| topic.error_code == codes::NONE)
        .any(|topic| {
            let cached = topic
                .name
                .as_deref()
                .and_then(|name| cache.topics.get(name))
                .filter(|t| topic.topic_id == Uuid::ZERO || t.topic_id == topic.topic_id);
            topic.partitions.iter().any(|p| {
                cached
                    .and_then(|t| t.partitions.get(&p.partition_index))
                    .is_none_or(|old| p.leader_epoch < 0 || p.leader_epoch > old.leader_epoch)
            })
        })
}

impl MetadataCache {
    /// Apply a response. `full` says the request asked for every topic, so a
    /// topic the response does not name is gone.
    pub fn apply(&mut self, response: &MetadataResponse, full: bool, now: Millis) {
        self.brokers = response
            .brokers
            .iter()
            .map(|b| {
                (
                    b.node_id,
                    BrokerInfo {
                        node_id: b.node_id,
                        host: b.host.clone(),
                        port: b.port,
                        rack: b.rack.clone(),
                        endpoint: endpoint_for_host(&b.host, b.node_id),
                    },
                )
            })
            .collect();
        self.cluster_id.clone_from(&response.cluster_id);
        self.controller_id = response.controller_id;
        if full {
            self.topics.clear();
        }
        for topic in &response.topics {
            let Some(name) = topic.name.clone() else {
                continue;
            };
            match topic.error_code {
                codes::NONE => {
                    self.unknown_topics.remove(&name);
                    let partitions = topic
                        .partitions
                        .iter()
                        .map(|p| {
                            (
                                p.partition_index,
                                PartitionInfo {
                                    index: p.partition_index,
                                    leader: p.leader_id,
                                    leader_epoch: p.leader_epoch,
                                    replicas: p.replica_nodes.clone(),
                                    isr: p.isr_nodes.clone(),
                                    error_code: p.error_code,
                                },
                            )
                        })
                        .collect();
                    self.topics.insert(
                        name.clone(),
                        TopicInfo {
                            name,
                            topic_id: topic.topic_id,
                            is_internal: topic.is_internal,
                            partitions,
                        },
                    );
                }
                codes::UNKNOWN_TOPIC_OR_PARTITION | codes::UNKNOWN_TOPIC_ID => {
                    self.topics.remove(&name);
                    self.unknown_topics.insert(name);
                }
                // A topic in creation answers LEADER_NOT_AVAILABLE; keep what
                // the cache has and ask again.
                _ => {}
            }
        }
        self.updated_at = Some(now);
        self.version += 1;
    }

    /// The leader's broker id of a partition, when the cache knows one.
    #[must_use]
    pub fn leader(&self, topic: &str, partition: i32) -> Option<i32> {
        self.partition(topic, partition)
            .map(|p| p.leader)
            .filter(|leader| *leader >= 0)
    }

    /// One partition of the cache.
    #[must_use]
    pub fn partition(&self, topic: &str, partition: i32) -> Option<&PartitionInfo> {
        self.topics.get(topic)?.partitions.get(&partition)
    }

    /// The partition count of a topic the cache knows.
    #[must_use]
    pub fn partition_count(&self, topic: &str) -> Option<i32> {
        self.topics
            .get(topic)
            .and_then(|t| i32::try_from(t.partitions.len()).ok())
    }

    /// The topic id of a topic the cache knows.
    #[must_use]
    pub fn topic_id(&self, topic: &str) -> Option<Uuid> {
        self.topics.get(topic).map(|t| t.topic_id)
    }

    /// Adopt the leader an error answer names (KIP-951), as Kafka's
    /// `Metadata.updatePartitionLeadership` does: only for a broker the
    /// cache knows and an epoch newer than the cached one. Returns whether
    /// the cache changed.
    pub fn update_leader(
        &mut self,
        topic: &str,
        partition: i32,
        leader: i32,
        leader_epoch: i32,
    ) -> bool {
        if !self.brokers.contains_key(&leader) {
            return false;
        }
        let Some(p) = self
            .topics
            .get_mut(topic)
            .and_then(|t| t.partitions.get_mut(&partition))
        else {
            return false;
        };
        if leader_epoch <= p.leader_epoch {
            return false;
        }
        p.leader = leader;
        p.leader_epoch = leader_epoch;
        true
    }

    /// The name of the topic with `id`.
    #[must_use]
    pub fn topic_name(&self, id: Uuid) -> Option<&str> {
        self.topics
            .values()
            .find(|t| t.topic_id == id)
            .map(|t| t.name.as_str())
    }

    /// The endpoint of a broker the cache knows.
    #[must_use]
    pub fn broker_endpoint(&self, node_id: i32) -> Option<Endpoint> {
        self.brokers.get(&node_id).map(|b| b.endpoint)
    }

    /// The broker id that listens at `endpoint`.
    #[must_use]
    pub fn broker_at(&self, endpoint: Endpoint) -> Option<i32> {
        self.brokers
            .values()
            .find(|b| b.endpoint == endpoint)
            .map(|b| b.node_id)
    }

    /// How old the cache is, or `None` before the first response.
    #[must_use]
    pub fn age(&self, now: Millis) -> Option<Millis> {
        self.updated_at.map(|at| now.saturating_sub(at))
    }

    /// The cache for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let brokers: Vec<Value> = self
            .brokers
            .values()
            .map(|b| {
                json!({
                    "id": b.node_id,
                    "host": b.host,
                    "port": b.port,
                    "rack": b.rack,
                    "node": b.endpoint.node,
                })
            })
            .collect();
        let topics: serde_json::Map<String, Value> = self
            .topics
            .values()
            .map(|t| {
                let partitions: Vec<Value> = t
                    .partitions
                    .values()
                    .map(|p| {
                        json!({
                            "partition": p.index,
                            "leader": p.leader,
                            "leader_epoch": p.leader_epoch,
                            "replicas": p.replicas,
                            "isr": p.isr,
                            "error": p.error_code,
                        })
                    })
                    .collect();
                (
                    t.name.clone(),
                    json!({
                        "topic_id": uuid_hex(t.topic_id),
                        "internal": t.is_internal,
                        "partitions": partitions,
                    }),
                )
            })
            .collect();
        json!({
            "cluster_id": self.cluster_id,
            "controller": self.controller_id,
            "brokers": brokers,
            "topics": topics,
            "unknown_topics": self.unknown_topics,
            "updated_at": self.updated_at,
            "version": self.version,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn cache_with_leader(leader: i32, leader_epoch: i32) -> MetadataCache {
        let mut cache = MetadataCache::default();
        for id in [1, 2] {
            cache.brokers.insert(
                id,
                BrokerInfo {
                    node_id: id,
                    host: format!("node-{id}"),
                    port: 9092,
                    rack: None,
                    endpoint: endpoint_for_host(&format!("node-{id}"), id),
                },
            );
        }
        cache.topics.insert(
            "orders".to_string(),
            TopicInfo {
                name: "orders".to_string(),
                topic_id: Uuid::ZERO,
                is_internal: false,
                partitions: BTreeMap::from([(
                    0,
                    PartitionInfo {
                        index: 0,
                        leader,
                        leader_epoch,
                        replicas: vec![1, 2],
                        isr: vec![1, 2],
                        error_code: 0,
                    },
                )]),
            },
        );
        cache
    }

    #[test]
    fn a_leader_hint_counts_only_with_a_newer_epoch_and_a_known_broker() {
        // Rows: the hint's leader and epoch against a cached leader 1 at
        // epoch 4, then the leader and epoch the cache holds after it.
        let rows = [
            ("a newer epoch", 2, 5, true, (2, 5)),
            ("the same epoch", 2, 4, false, (1, 4)),
            ("an older epoch", 2, 3, false, (1, 4)),
            ("an unknown broker", 7, 9, false, (1, 4)),
            ("no leader", -1, 9, false, (1, 4)),
        ];
        for (name, leader, epoch, changed, after) in rows {
            let mut cache = cache_with_leader(1, 4);
            assert!(
                cache.update_leader("orders", 0, leader, epoch) == changed,
                "{name}"
            );
            let partition = cache.partition("orders", 0).unwrap();
            assert!(
                (partition.leader, partition.leader_epoch) == after,
                "{name}"
            );
        }
        let mut cache = cache_with_leader(1, 4);
        assert!(!cache.update_leader("other", 0, 2, 5));
        assert!(!cache.update_leader("orders", 3, 2, 5));
    }

    #[test]
    fn hosts_name_lab_nodes_and_fall_back_to_the_broker_id() {
        let cases = [
            ("node-3", 7, 3),
            ("node-12", 1, 12),
            ("10.0.0.5", 9, 5),
            ("10.0.1.2", 9, 258),
            ("10.1.0.5", 9, 9),
            ("broker-3.example", 4, 4),
            ("", 9, 9),
            ("node-x", 2, 2),
            ("node-1", -1, 1),
            ("other", -1, 0),
        ];
        for (host, id, node) in cases {
            assert!(
                endpoint_for_host(host, id) == Endpoint::kafka(NodeId(node)),
                "{host}"
            );
        }
    }
}
