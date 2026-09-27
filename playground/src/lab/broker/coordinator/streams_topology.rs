//! The KIP-1071 topology: the stored form of the wire `Topology`, and its
//! configuration against the cluster's topics.
//!
//! [`configure`] is Kafka's `InternalTopicManager.configureTopics` without
//! the copartition-group checks: a missing source topic stops it with
//! `MISSING_SOURCE_TOPICS`; a repartition topic without a partition count
//! gets the largest count of the topics its writing subtopology reads; a
//! changelog topic gets the task count of its subtopology; an internal topic
//! that exists with another partition count is
//! `INCORRECTLY_PARTITIONED_TOPICS`; and the internal topics that do not
//! exist are returned for the broker to create, under
//! `MISSING_INTERNAL_TOPICS`. The status codes and detail strings are
//! Kafka's, and every list is in name order.

use std::collections::{BTreeMap, BTreeSet};

use krabka_protocol::owned::{
    common::streams_group_describe_response::{
        key_value::KeyValue as DescribedKeyValue, topic_info::TopicInfo as DescribedTopicInfo,
    },
    streams_group_describe_response::{
        Subtopology as DescribedSubtopology, Topology as DescribedTopology,
    },
    streams_group_heartbeat_request::{Subtopology, Topology},
};
use serde::{Deserialize, Serialize};

use super::TopicMetadata;

/// `StreamsGroupHeartbeatResponse.Status` codes.
pub const STALE_TOPOLOGY: i8 = 0;
pub const MISSING_SOURCE_TOPICS: i8 = 1;
pub const INCORRECTLY_PARTITIONED_TOPICS: i8 = 2;
pub const MISSING_INTERNAL_TOPICS: i8 = 3;
pub const SHUTDOWN_APPLICATION: i8 = 4;
pub const ASSIGNMENT_DELAYED: i8 = 5;

/// The replication factor that asks the broker for its default.
pub const DEFAULT_REPLICATION_FACTOR: i16 = -1;

/// A topology as a member sent it.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StoredTopology {
    pub epoch: i32,
    pub subtopologies: Vec<StoredSubtopology>,
}

/// One subtopology of a stored topology.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StoredSubtopology {
    pub id: String,
    pub source_topics: Vec<String>,
    pub source_topic_regex: Vec<String>,
    pub state_changelog_topics: Vec<StoredTopicInfo>,
    pub repartition_sink_topics: Vec<String>,
    pub repartition_source_topics: Vec<StoredTopicInfo>,
}

/// An internal topic as the topology declares it. `partitions` and
/// `replication_factor` are 0 when the topology leaves them to the broker.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StoredTopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: BTreeMap<String, String>,
}

impl StoredTopology {
    /// The stored form of a wire topology.
    #[must_use]
    pub fn from_wire(topology: &Topology) -> Self {
        let info = |infos: &[krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo]| {
            infos
                .iter()
                .map(|t| StoredTopicInfo {
                    name: t.name.clone(),
                    partitions: t.partitions,
                    replication_factor: t.replication_factor,
                    topic_configs: t
                        .topic_configs
                        .iter()
                        .map(|kv| (kv.key.clone(), kv.value.clone()))
                        .collect(),
                })
                .collect()
        };
        Self {
            epoch: topology.epoch,
            subtopologies: topology
                .subtopologies
                .iter()
                .map(|s: &Subtopology| StoredSubtopology {
                    id: s.subtopology_id.clone(),
                    source_topics: s.source_topics.clone(),
                    source_topic_regex: s.source_topic_regex.clone(),
                    state_changelog_topics: info(&s.state_changelog_topics),
                    repartition_sink_topics: s.repartition_sink_topics.clone(),
                    repartition_source_topics: info(&s.repartition_source_topics),
                })
                .collect(),
        }
    }

    /// Kafka's `StreamsTopology.asStreamsGroupDescribeTopology`.
    #[must_use]
    pub fn describe(&self) -> DescribedTopology {
        let mut subtopologies: Vec<DescribedSubtopology> = self
            .subtopologies
            .iter()
            .map(|s| DescribedSubtopology {
                subtopology_id: s.id.clone(),
                source_topics: sorted(s.source_topics.clone()),
                repartition_sink_topics: sorted(s.repartition_sink_topics.clone()),
                state_changelog_topics: described_infos(s.state_changelog_topics.iter().map(|t| {
                    (
                        t.name.clone(),
                        t.partitions,
                        t.replication_factor,
                        t.topic_configs.clone(),
                    )
                })),
                repartition_source_topics: described_infos(s.repartition_source_topics.iter().map(
                    |t| {
                        (
                            t.name.clone(),
                            t.partitions,
                            t.replication_factor,
                            t.topic_configs.clone(),
                        )
                    },
                )),
                ..Default::default()
            })
            .collect();
        subtopologies.sort_by(|a, b| a.subtopology_id.cmp(&b.subtopology_id));
        DescribedTopology {
            epoch: self.epoch,
            subtopologies: Some(subtopologies),
            ..Default::default()
        }
    }

    /// Every topic the topology reads or writes by name: the source topics
    /// and the internal topics.
    #[must_use]
    pub fn required_topics(&self) -> BTreeSet<String> {
        self.subtopologies
            .iter()
            .flat_map(|s| {
                s.source_topics
                    .iter()
                    .chain(s.repartition_sink_topics.iter())
                    .cloned()
                    .chain(s.state_changelog_topics.iter().map(|t| t.name.clone()))
                    .chain(s.repartition_source_topics.iter().map(|t| t.name.clone()))
            })
            .collect()
    }
}

/// The partition counts of the required topics, `None` for a topic that does
/// not exist. A change bumps the group epoch, as Kafka's metadata hash does.
#[must_use]
pub fn metadata_signature(
    topology: &StoredTopology,
    metadata: &dyn TopicMetadata,
) -> BTreeMap<String, Option<i32>> {
    topology
        .required_topics()
        .into_iter()
        .map(|name| {
            let partitions = metadata.partitions(&name);
            (name, partitions)
        })
        .collect()
}

fn sorted(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names
}

fn described_infos(
    infos: impl Iterator<Item = (String, i32, i16, BTreeMap<String, String>)>,
) -> Vec<DescribedTopicInfo> {
    let mut out: Vec<DescribedTopicInfo> = infos
        .map(
            |(name, partitions, replication_factor, configs)| DescribedTopicInfo {
                name,
                partitions,
                replication_factor,
                topic_configs: configs
                    .into_iter()
                    .map(|(key, value)| DescribedKeyValue {
                        key,
                        value,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
        )
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// An internal topic the broker must create: a repartition or changelog
/// topic with its decided partition count, the replication factor the
/// topology asked for or `-1`, which asks `CreateTopics` for the broker
/// default, and its configs.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct InternalTopicToCreate {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub configs: BTreeMap<String, String>,
}

/// Kafka's `ConfiguredSubtopology`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConfiguredSubtopology {
    pub number_of_tasks: i32,
    pub source_topics: BTreeSet<String>,
    pub repartition_source_topics: BTreeMap<String, InternalTopicToCreate>,
    pub repartition_sink_topics: BTreeSet<String>,
    pub state_changelog_topics: BTreeMap<String, InternalTopicToCreate>,
}

/// Kafka's `ConfiguredTopology`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConfiguredTopology {
    pub epoch: i32,
    pub subtopologies: BTreeMap<String, ConfiguredSubtopology>,
    /// The internal topics that do not exist, by name.
    pub internal_topics_to_create: BTreeMap<String, InternalTopicToCreate>,
    /// The `(status code, status detail)` that keeps the group from an
    /// assignment.
    pub status: Option<(i8, String)>,
}

impl ConfiguredTopology {
    /// Whether the group can be assigned.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.status.is_none()
    }

    /// The task count of every subtopology.
    #[must_use]
    pub fn number_of_tasks(&self) -> BTreeMap<String, i32> {
        self.subtopologies
            .iter()
            .map(|(id, s)| (id.clone(), s.number_of_tasks))
            .collect()
    }

    /// The subtopologies with a changelog: the stateful ones.
    #[must_use]
    pub fn stateful(&self) -> BTreeSet<String> {
        self.subtopologies
            .iter()
            .filter(|(_, s)| !s.state_changelog_topics.is_empty())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Kafka's `ConfiguredTopology.asStreamsGroupDescribeTopology`: the
    /// decided partition count of every internal topic.
    #[must_use]
    pub fn describe(&self) -> DescribedTopology {
        let infos = |topics: &BTreeMap<String, InternalTopicToCreate>| {
            described_infos(topics.values().map(|t| {
                (
                    t.name.clone(),
                    t.partitions,
                    if t.replication_factor == DEFAULT_REPLICATION_FACTOR {
                        0
                    } else {
                        t.replication_factor
                    },
                    t.configs.clone(),
                )
            }))
        };
        DescribedTopology {
            epoch: self.epoch,
            subtopologies: Some(
                self.subtopologies
                    .iter()
                    .map(|(id, s)| DescribedSubtopology {
                        subtopology_id: id.clone(),
                        source_topics: s.source_topics.iter().cloned().collect(),
                        repartition_sink_topics: s
                            .repartition_sink_topics
                            .iter()
                            .cloned()
                            .collect(),
                        state_changelog_topics: infos(&s.state_changelog_topics),
                        repartition_source_topics: infos(&s.repartition_source_topics),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }
}

/// A topology that cannot be configured: Kafka's
/// `StreamsInvalidTopologyException`, answered as `STREAMS_INVALID_TOPOLOGY`.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("{0}")]
pub struct InvalidTopology(pub String);

/// Kafka's `InternalTopicManager.configureTopics`.
///
/// # Errors
/// Returns the message of a topology whose repartition topics cannot be
/// sized, or with a subtopology that reads no topic.
pub fn configure(
    topology: &StoredTopology,
    metadata: &dyn TopicMetadata,
) -> Result<ConfiguredTopology, InvalidTopology> {
    let not_ready = |code, detail| ConfiguredTopology {
        epoch: topology.epoch,
        subtopologies: BTreeMap::new(),
        internal_topics_to_create: BTreeMap::new(),
        status: Some((code, detail)),
    };
    let (sources, missing) = resolve_sources(topology, metadata);
    if !missing.is_empty() {
        return Ok(not_ready(
            MISSING_SOURCE_TOPICS,
            format!("Source topics {} are missing.", summarize(&missing)),
        ));
    }
    let decided = repartition_partition_counts(topology, &sources, metadata)?;
    let subtopologies = topology
        .subtopologies
        .iter()
        .map(|s| {
            configure_subtopology(s, &sources[&s.id], &decided, metadata).map(|c| (s.id.clone(), c))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let to_create = match missing_internal_topics(&subtopologies, metadata) {
        Ok(to_create) => to_create,
        Err((code, detail)) => return Ok(not_ready(code, detail)),
    };
    let status = (!to_create.is_empty()).then(|| {
        (
            MISSING_INTERNAL_TOPICS,
            format!(
                "Internal topics are missing: {}",
                summarize(&to_create.keys().cloned().collect())
            ),
        )
    });
    Ok(ConfiguredTopology {
        epoch: topology.epoch,
        subtopologies,
        internal_topics_to_create: to_create,
        status,
    })
}

/// Kafka's `fromPersistedSubtopology`: the task count is the largest
/// partition count of the topics the subtopology reads, a repartition source
/// topic takes its decided count, and a changelog topic the task count.
fn configure_subtopology(
    subtopology: &StoredSubtopology,
    sources: &BTreeSet<String>,
    decided: &BTreeMap<String, i32>,
    metadata: &dyn TopicMetadata,
) -> Result<ConfiguredSubtopology, InvalidTopology> {
    let inputs = sources.iter().filter_map(|t| metadata.partitions(t)).chain(
        subtopology
            .repartition_source_topics
            .iter()
            .filter_map(|t| decided.get(&t.name).copied()),
    );
    let Some(number_of_tasks) = inputs.max() else {
        return Err(InvalidTopology(format!(
            "No source topics found for subtopology {}",
            subtopology.id
        )));
    };
    let repartition_source_topics = subtopology
        .repartition_source_topics
        .iter()
        .map(|t| {
            let partitions = decided.get(&t.name).copied().unwrap_or(number_of_tasks);
            (t.name.clone(), internal_topic(t, partitions))
        })
        .collect();
    let state_changelog_topics = subtopology
        .state_changelog_topics
        .iter()
        .map(|t| {
            let partitions = if t.partitions > 0 {
                t.partitions
            } else {
                number_of_tasks
            };
            (t.name.clone(), internal_topic(t, partitions))
        })
        .collect();
    Ok(ConfiguredSubtopology {
        number_of_tasks,
        source_topics: sources.clone(),
        repartition_source_topics,
        repartition_sink_topics: subtopology
            .repartition_sink_topics
            .iter()
            .cloned()
            .collect(),
        state_changelog_topics,
    })
}

/// An internal topic with its decided partition count. A replication factor
/// the topology leaves at 0 asks for the broker default.
fn internal_topic(topic: &StoredTopicInfo, partitions: i32) -> InternalTopicToCreate {
    InternalTopicToCreate {
        name: topic.name.clone(),
        partitions,
        replication_factor: if topic.replication_factor > 0 {
            topic.replication_factor
        } else {
            DEFAULT_REPLICATION_FACTOR
        },
        configs: topic.topic_configs.clone(),
    }
}

/// Kafka's `missingInternalTopics`: the internal topics that do not exist,
/// or the `INCORRECTLY_PARTITIONED_TOPICS` status of one that exists with
/// another partition count.
fn missing_internal_topics(
    subtopologies: &BTreeMap<String, ConfiguredSubtopology>,
    metadata: &dyn TopicMetadata,
) -> Result<BTreeMap<String, InternalTopicToCreate>, (i8, String)> {
    let internal: BTreeMap<&String, &InternalTopicToCreate> = subtopologies
        .values()
        .flat_map(|s| {
            s.repartition_source_topics
                .iter()
                .chain(s.state_changelog_topics.iter())
        })
        .collect();
    let mut to_create = BTreeMap::new();
    for (name, topic) in internal {
        match metadata.partitions(name) {
            Some(found) if found != topic.partitions => {
                return Err((
                    INCORRECTLY_PARTITIONED_TOPICS,
                    format!(
                        "Existing topic {name} has different number of partitions: expected {}, found {found}",
                        topic.partitions
                    ),
                ));
            }
            Some(_) => {}
            None => {
                to_create.insert(name.clone(), topic.clone());
            }
        }
    }
    Ok(to_create)
}

/// The external source topics of every subtopology, with a regex resolved
/// through the metadata, and the topics or regexes that resolve to nothing.
fn resolve_sources(
    topology: &StoredTopology,
    metadata: &dyn TopicMetadata,
) -> (BTreeMap<String, BTreeSet<String>>, BTreeSet<String>) {
    let mut sources = BTreeMap::new();
    let mut missing = BTreeSet::new();
    for s in &topology.subtopologies {
        let mut names = BTreeSet::new();
        for topic in &s.source_topics {
            if metadata.partitions(topic).is_none() {
                missing.insert(topic.clone());
            }
            names.insert(topic.clone());
        }
        for regex in &s.source_topic_regex {
            let matched = metadata.topics_matching(regex);
            if matched.is_empty() {
                missing.insert(regex.clone());
            }
            names.extend(matched);
        }
        sources.insert(s.id.clone(), names);
    }
    (sources, missing)
}

/// Kafka's `RepartitionTopics.setup`: a repartition topic keeps its explicit
/// partition count, or takes the largest count of the topics the subtopology
/// that writes it reads, until every one is sized.
fn repartition_partition_counts(
    topology: &StoredTopology,
    sources: &BTreeMap<String, BTreeSet<String>>,
    metadata: &dyn TopicMetadata,
) -> Result<BTreeMap<String, i32>, InvalidTopology> {
    let mut counts: BTreeMap<String, i32> = topology
        .subtopologies
        .iter()
        .flat_map(|s| s.repartition_source_topics.iter())
        .filter(|t| t.partitions > 0)
        .map(|t| (t.name.clone(), t.partitions))
        .collect();
    loop {
        let mut needed = false;
        let mut progress = false;
        for s in &topology.subtopologies {
            for sink in &s.repartition_sink_topics {
                if counts.contains_key(sink) {
                    continue;
                }
                let candidate = s
                    .repartition_source_topics
                    .iter()
                    .filter_map(|t| counts.get(&t.name).copied())
                    .chain(sources[&s.id].iter().filter_map(|t| metadata.partitions(t)))
                    .max();
                match candidate {
                    Some(partitions) => {
                        counts.insert(sink.clone(), partitions);
                        progress = true;
                    }
                    None => needed = true,
                }
            }
        }
        if !progress && needed {
            return Err(InvalidTopology(
                "Failed to compute number of partitions for all repartition topics. There may be loops in the topology that cannot be resolved."
                    .to_string(),
            ));
        }
        if !needed {
            break;
        }
    }
    let never_written = topology
        .subtopologies
        .iter()
        .flat_map(|s| s.repartition_source_topics.iter())
        .any(|t| !counts.contains_key(&t.name));
    if never_written {
        return Err(InvalidTopology(
            "Failed to compute number of partitions for all repartition topics, because a repartition source topic is never used as a sink topic."
                .to_string(),
        ));
    }
    Ok(counts)
}

/// Kafka's `InternalTopicManager.summarizeTopics`: at most three names, then
/// the count of the others.
fn summarize(topics: &BTreeSet<String>) -> String {
    if topics.is_empty() {
        return "<none>".to_string();
    }
    let shown = topics
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if topics.len() > 3 {
        format!("{shown} and {} additional topics", topics.len() - 3)
    } else {
        shown
    }
}
