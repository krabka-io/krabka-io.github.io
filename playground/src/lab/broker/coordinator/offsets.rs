//! Committed offsets: `OffsetCommit` and `OffsetFetch`.
//!
//! Offsets live beside the groups, keyed by group id, because a group that
//! only commits offsets has no members: Kafka's "simple" group. The commit
//! validation is the group kind's own rule (`ClassicGroup.validateOffsetCommit`,
//! `ConsumerGroup.validateOffsetCommit`), and every accepted partition is
//! written as one record.

use std::collections::BTreeMap;

use krabka_protocol::{
    owned::{
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
        offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
            OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        },
    },
    primitives::uuid::Uuid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    Coordinator, Group, TopicMetadata, classic,
    consumer::PartitionEpochs,
    ids::{GroupId, TopicId},
    persist::RecordKey,
};
use crate::lab::{codes, net::Millis};

/// One committed offset, and the value of an offset record.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct OffsetEntry {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
    /// The logical time of the commit.
    pub commit_timestamp: Millis,
}

/// The committed offsets of one group, by `(topic, partition)`.
pub type GroupOffsets = BTreeMap<(String, i32), OffsetEntry>;

/// Kafka's `offset.metadata.max.bytes`.
pub const OFFSET_METADATA_MAX_BYTES: usize = 4096;

/// The first `OffsetCommit` version that answers `GROUP_ID_NOT_FOUND` for a
/// group the coordinator does not hold; older versions answer
/// `ILLEGAL_GENERATION`.
const FIRST_GROUP_NOT_FOUND_COMMIT_VERSION: i16 = 9;

/// The first `OffsetCommit` version a member of the consumer protocol
/// (KIP-848) may use.
pub const FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION: i16 = 9;

/// The first `OffsetFetch` version with the per-group request shape.
const FIRST_GROUPS_FETCH_VERSION: i16 = 8;

/// The first `OffsetCommit` and `OffsetFetch` version that names topics by id.
const FIRST_TOPIC_ID_VERSION: i16 = 10;

/// How the partitions of a commit its group accepted are checked: Kafka's
/// `CommitPartitionValidator`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CommitCheck {
    /// Every partition may commit.
    Any,
    /// A member committed with an epoch older than its own (KIP-1251): each
    /// partition must be one it was assigned at that epoch or before.
    AssignedBy {
        epoch: i32,
        assigned: PartitionEpochs,
    },
}

impl CommitCheck {
    /// Kafka's `validate` for one partition: `STALE_MEMBER_EPOCH` when the
    /// partition fails.
    fn check(&self, topic_id: TopicId, partition: i32) -> Result<(), i16> {
        match self {
            Self::Any => Ok(()),
            Self::AssignedBy { epoch, assigned } => {
                match assigned.get(&topic_id).and_then(|ps| ps.get(&partition)) {
                    Some(assigned_at) if epoch >= assigned_at => Ok(()),
                    _ => Err(codes::STALE_MEMBER_EPOCH),
                }
            }
        }
    }
}

/// `OffsetCommit`, behind the topic checks of Kafka 4.3's
/// `KafkaApis.handleOffsetCommitRequest`.
///
/// At v10 a topic id the metadata does not know is `UNKNOWN_TOPIC_ID`; below
/// v10 the topic id is resolved from the name, and a topic without one is
/// `UNKNOWN_TOPIC_OR_PARTITION`, as is a partition that does not exist. Only
/// the partitions that pass reach the group, so a commit where none does
/// neither checks nor creates a group. The response is assembled as Kafka's
/// `OffsetCommitResponse.Builder` assembles it: the refused rows in request
/// order, then the group's rows merged into them by topic.
pub fn commit(
    coord: &mut Coordinator,
    now: Millis,
    req: &OffsetCommitRequest,
    version: i16,
    metadata: &dyn TopicMetadata,
) -> OffsetCommitResponse {
    checked_commit(req, version, metadata, |valid| {
        commit_to_group(coord, now, req, version, valid)
    })
}

/// `OffsetCommit` refused by the group coordinator service with
/// `error_code`, behind the same topic checks as [`commit`]: the partitions
/// that pass them carry the error, as Kafka's `OffsetCommitRequest
/// .getErrorResponse` of the checked request merged into the refused rows.
pub fn commit_refused(
    req: &OffsetCommitRequest,
    version: i16,
    metadata: &dyn TopicMetadata,
    error_code: i16,
) -> OffsetCommitResponse {
    checked_commit(req, version, metadata, |valid| {
        valid
            .into_iter()
            .map(|topic| OffsetCommitResponseTopic {
                name: topic.name,
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| commit_row(partition.partition_index, error_code))
                    .collect(),
                ..Default::default()
            })
            .collect()
    })
}

/// The topic checks of `KafkaApis.handleOffsetCommitRequest`, with the rows
/// of the partitions that pass from `commit_valid`, which runs only when
/// some do.
fn checked_commit(
    req: &OffsetCommitRequest,
    version: i16,
    metadata: &dyn TopicMetadata,
    commit_valid: impl FnOnce(Vec<ValidTopic<'_>>) -> Vec<OffsetCommitResponseTopic>,
) -> OffsetCommitResponse {
    let by_id = version >= FIRST_TOPIC_ID_VERSION;
    let mut response = CommitResponse {
        by_id,
        topics: Vec::new(),
    };
    let mut valid = Vec::new();
    for topic in &req.topics {
        let (name, topic_id) = if by_id {
            let Some(name) = topic_name_by_id(metadata, topic.topic_id) else {
                response.refuse_all(topic.topic_id, &topic.name, topic, codes::UNKNOWN_TOPIC_ID);
                continue;
            };
            (name, topic.topic_id)
        } else {
            let topic_id = metadata.topic_id(&topic.name).unwrap_or(Uuid::ZERO);
            (topic.name.clone(), topic_id)
        };
        if topic_id == Uuid::ZERO {
            response.refuse_all(Uuid::ZERO, &name, topic, codes::UNKNOWN_TOPIC_OR_PARTITION);
            continue;
        }
        let count = metadata.partitions(&name).unwrap_or(0);
        let mut partitions = Vec::new();
        for partition in &topic.partitions {
            if (0..count).contains(&partition.partition_index) {
                partitions.push(partition);
            } else {
                response.topic(topic_id, &name).push(commit_row(
                    partition.partition_index,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                ));
            }
        }
        if !partitions.is_empty() {
            valid.push(ValidTopic {
                topic_id,
                name,
                partitions,
            });
        }
    }
    let rows = if valid.is_empty() {
        Vec::new()
    } else {
        commit_valid(valid)
    };
    OffsetCommitResponse {
        throttle_time_ms: 0,
        topics: response.merge(rows),
        ..Default::default()
    }
}

/// The name of a topic named by id, which the zero id never names.
fn topic_name_by_id(metadata: &dyn TopicMetadata, topic_id: Uuid) -> Option<String> {
    if topic_id == Uuid::ZERO {
        None
    } else {
        metadata.topic_name(topic_id)
    }
}

/// The partitions of one requested topic that passed the topic checks.
struct ValidTopic<'a> {
    /// The topic id, resolved from the name below v10.
    topic_id: Uuid,
    /// The topic name, resolved from the id at v10.
    name: String,
    partitions: Vec<&'a OffsetCommitRequestPartition>,
}

/// Kafka's `OffsetCommitResponse.Builder`: the rows of a topic gather under
/// one entry, found by the topic id at v10 and by the name before.
struct CommitResponse {
    by_id: bool,
    topics: Vec<OffsetCommitResponseTopic>,
}

impl CommitResponse {
    /// The rows of a topic, whose entry is added on first use.
    fn topic(&mut self, topic_id: Uuid, name: &str) -> &mut Vec<OffsetCommitResponsePartition> {
        let by_id = self.by_id;
        let found = self.topics.iter().position(|topic| {
            if by_id {
                topic.topic_id == topic_id
            } else {
                topic.name == name
            }
        });
        let index = found.unwrap_or_else(|| {
            self.topics.push(OffsetCommitResponseTopic {
                name: name.to_string(),
                topic_id,
                ..Default::default()
            });
            self.topics.len() - 1
        });
        &mut self.topics[index].partitions
    }

    /// Refuse every partition of a topic, which gets an entry even when it
    /// names no partition.
    fn refuse_all(
        &mut self,
        topic_id: Uuid,
        name: &str,
        topic: &OffsetCommitRequestTopic,
        error_code: i16,
    ) {
        let rows = self.topic(topic_id, name);
        rows.extend(
            topic
                .partitions
                .iter()
                .map(|partition| commit_row(partition.partition_index, error_code)),
        );
    }

    /// Kafka's `merge`: the group's rows are the whole response when no
    /// partition was refused, and join the rows of their topic otherwise.
    fn merge(mut self, rows: Vec<OffsetCommitResponseTopic>) -> Vec<OffsetCommitResponseTopic> {
        if self.topics.is_empty() {
            return rows;
        }
        for row in rows {
            self.topic(row.topic_id, &row.name).extend(row.partitions);
        }
        self.topics
    }
}

/// Kafka's `OffsetMetadataManager.commitOffset` for the partitions that
/// passed the topic checks: one row per topic. The group checks the commit,
/// then each partition whose metadata fits in request order; the first
/// refusal is the error of every partition, and nothing is written.
fn commit_to_group(
    coord: &mut Coordinator,
    now: Millis,
    req: &OffsetCommitRequest,
    version: i16,
    valid: Vec<ValidTopic<'_>>,
) -> Vec<OffsetCommitResponseTopic> {
    // Kafka refuses only a null group id, which the wire cannot carry here:
    // the empty group id is an ordinary simple group.
    let group_error = match validate_commit(coord, now, req, version) {
        Err(error_code) => Some(error_code),
        Ok(check) => valid
            .iter()
            .flat_map(|topic| topic.partitions.iter().map(move |p| (topic.topic_id, p)))
            .filter(|(_, partition)| !metadata_too_large(partition))
            .find_map(|(topic_id, partition)| {
                check
                    .check(TopicId::from(topic_id), partition.partition_index)
                    .err()
            }),
    };
    let group_id = GroupId::from(req.group_id.as_str());
    valid
        .into_iter()
        .map(|topic| {
            let partitions = topic
                .partitions
                .into_iter()
                .map(|partition| {
                    let error_code = group_error.unwrap_or_else(|| {
                        commit_partition(coord, now, &group_id, &topic.name, partition)
                    });
                    commit_row(partition.partition_index, error_code)
                })
                .collect();
            OffsetCommitResponseTopic {
                name: topic.name,
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            }
        })
        .collect()
}

/// Kafka's `isMetadataInvalid`: metadata longer than
/// `offset.metadata.max.bytes`, counted as Java's `String.length` counts it,
/// in UTF-16 code units.
fn metadata_too_large(partition: &OffsetCommitRequestPartition) -> bool {
    partition
        .committed_metadata
        .as_deref()
        .is_some_and(|metadata| metadata.encode_utf16().count() > OFFSET_METADATA_MAX_BYTES)
}

/// Store one accepted partition's offset, unless its metadata is too large.
fn commit_partition(
    coord: &mut Coordinator,
    now: Millis,
    group_id: &GroupId,
    topic: &str,
    partition: &OffsetCommitRequestPartition,
) -> i16 {
    if metadata_too_large(partition) {
        return codes::OFFSET_METADATA_TOO_LARGE;
    }
    let entry = OffsetEntry {
        offset: partition.committed_offset,
        leader_epoch: partition.committed_leader_epoch,
        metadata: partition.committed_metadata.clone().unwrap_or_default(),
        commit_timestamp: now,
    };
    store(coord, group_id, topic, partition.partition_index, entry);
    codes::NONE
}

fn commit_row(partition_index: i32, error_code: i16) -> OffsetCommitResponsePartition {
    OffsetCommitResponsePartition {
        partition_index,
        error_code,
        ..Default::default()
    }
}

/// Store one committed offset and write its record.
fn store(
    coord: &mut Coordinator,
    group_id: &GroupId,
    topic: &str,
    partition: i32,
    entry: OffsetEntry,
) {
    coord.shared.persist(
        &RecordKey::Offset {
            group: group_id.clone(),
            topic: topic.to_string(),
            partition,
        },
        Some(&entry),
    );
    coord
        .offsets
        .entry(group_id.clone())
        .or_default()
        .insert((topic.to_string(), partition), entry);
}

/// The group-level check of an `OffsetCommit`, by the kind of the group,
/// and how the group checks each partition.
///
/// A group the coordinator does not hold accepts a commit with a negative
/// generation, from the admin client or a consumer without group management,
/// and becomes a simple classic group for it.
fn validate_commit(
    coord: &mut Coordinator,
    now: Millis,
    req: &OffsetCommitRequest,
    version: i16,
) -> Result<CommitCheck, i16> {
    let group_id = GroupId::from(req.group_id.as_str());
    let generation = req.generation_id_or_member_epoch;
    match coord.groups.get_mut(&group_id) {
        None if generation < 0 => {
            coord.groups.insert(
                group_id.clone(),
                Group::Classic(classic::ClassicGroup::new(group_id)),
            );
            Ok(CommitCheck::Any)
        }
        None if version >= FIRST_GROUP_NOT_FOUND_COMMIT_VERSION => Err(codes::GROUP_ID_NOT_FOUND),
        None => Err(codes::ILLEGAL_GENERATION),
        Some(Group::Classic(group)) => {
            classic::validate_offset_commit(
                group,
                &req.member_id,
                req.group_instance_id.as_deref(),
                generation,
            )?;
            classic::refresh_committer_session(group, &mut coord.shared, now, &req.member_id);
            Ok(CommitCheck::Any)
        }
        Some(Group::Consumer(group)) => {
            group.validate_offset_commit(&req.member_id, generation, version)
        }
        Some(Group::Streams(group)) => group
            .validate_offset_commit(&req.member_id, generation, version)
            .map(|()| CommitCheck::Any),
    }
}

/// `OffsetFetch`.
pub fn fetch(
    coord: &Coordinator,
    req: &OffsetFetchRequest,
    version: i16,
    metadata: &dyn TopicMetadata,
) -> OffsetFetchResponse {
    if version < FIRST_GROUPS_FETCH_VERSION {
        return fetch_legacy(coord, req);
    }
    let groups = req
        .groups
        .iter()
        .map(|group| {
            let group_id = GroupId::from(group.group_id.as_str());
            let checked = validate_fetch(
                coord,
                &group_id,
                group.member_id.as_deref(),
                group.member_epoch,
            );
            let (topics, error_code) = if let Err(error_code) = checked {
                (Vec::new(), error_code)
            } else {
                let offsets = coord.offsets.get(&group_id);
                let topics = if let Some(requested) = group.topics.as_deref() {
                    requested
                        .iter()
                        .map(|topic| {
                            let name = if version >= FIRST_TOPIC_ID_VERSION {
                                topic_name_by_id(metadata, topic.topic_id)
                            } else {
                                Some(topic.name.clone())
                            };
                            let partitions = topic
                                .partition_indexes
                                .iter()
                                .map(|&partition| match &name {
                                    None => missing_group_row(partition, codes::UNKNOWN_TOPIC_ID),
                                    Some(name) => group_row(offsets, name, partition),
                                })
                                .collect();
                            OffsetFetchResponseTopics {
                                name: name.unwrap_or_default(),
                                topic_id: topic.topic_id,
                                partitions,
                                ..Default::default()
                            }
                        })
                        .collect()
                } else {
                    fetch_all_groups(offsets, version, metadata)
                };
                (topics, codes::NONE)
            };
            OffsetFetchResponseGroup {
                group_id: group.group_id.clone(),
                topics,
                error_code,
                ..Default::default()
            }
        })
        .collect();
    OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: Vec::new(),
        error_code: codes::NONE,
        groups,
        ..Default::default()
    }
}

/// Every committed offset of a group on the per-group shape, in topic order.
/// The topic id comes from the metadata; at v10, where only the id is on the
/// wire, a topic without one is left out.
fn fetch_all_groups(
    offsets: Option<&GroupOffsets>,
    version: i16,
    metadata: &dyn TopicMetadata,
) -> Vec<OffsetFetchResponseTopics> {
    let mut by_topic: BTreeMap<&str, Vec<OffsetFetchResponsePartitions>> = BTreeMap::new();
    for ((topic, partition), entry) in offsets.into_iter().flatten() {
        by_topic
            .entry(topic.as_str())
            .or_default()
            .push(committed_group_row(*partition, entry));
    }
    by_topic
        .into_iter()
        .filter_map(|(name, partitions)| {
            let topic_id = metadata.topic_id(name).unwrap_or(Uuid::ZERO);
            if version >= FIRST_TOPIC_ID_VERSION && topic_id == Uuid::ZERO {
                return None;
            }
            Some(OffsetFetchResponseTopics {
                name: name.to_string(),
                topic_id,
                partitions,
                ..Default::default()
            })
        })
        .collect()
}

/// `OffsetFetch` below v8: one group, the `topics` list or every topic.
fn fetch_legacy(coord: &Coordinator, req: &OffsetFetchRequest) -> OffsetFetchResponse {
    let group_id = GroupId::from(req.group_id.as_str());
    let offsets = coord.offsets.get(&group_id);
    let topics = if let Some(requested) = req.topics.as_deref() {
        requested
            .iter()
            .map(|topic: &OffsetFetchRequestTopic| OffsetFetchResponseTopic {
                name: topic.name.clone(),
                partitions: topic
                    .partition_indexes
                    .iter()
                    .map(|&partition| legacy_row(offsets, &topic.name, partition))
                    .collect(),
                ..Default::default()
            })
            .collect()
    } else {
        let mut by_topic: BTreeMap<&str, Vec<OffsetFetchResponsePartition>> = BTreeMap::new();
        for ((topic, partition), entry) in offsets.into_iter().flatten() {
            by_topic
                .entry(topic.as_str())
                .or_default()
                .push(committed_legacy_row(*partition, entry));
        }
        by_topic
            .into_iter()
            .map(|(name, partitions)| OffsetFetchResponseTopic {
                name: name.to_string(),
                partitions,
                ..Default::default()
            })
            .collect()
    };
    OffsetFetchResponse {
        throttle_time_ms: 0,
        topics,
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// The group-level check of an `OffsetFetch`, whose v9+ groups carry a
/// member id and epoch. A fetch without a member id and with a negative
/// epoch, from the admin client or a client without group management, always
/// passes; a consumer or streams group checks the member and its epoch; a
/// classic group and a group the coordinator does not hold accept any fetch.
fn validate_fetch(
    coord: &Coordinator,
    group_id: &GroupId,
    member_id: Option<&str>,
    member_epoch: i32,
) -> Result<(), i16> {
    if member_id.is_none() && member_epoch < 0 {
        return Ok(());
    }
    match coord.groups.get(group_id) {
        Some(Group::Consumer(group)) => group.validate_offset_fetch(member_id, member_epoch),
        Some(Group::Streams(group)) => group.validate_offset_fetch(member_id, member_epoch),
        Some(Group::Classic(_)) | None => Ok(()),
    }
}

fn group_row(
    offsets: Option<&GroupOffsets>,
    topic: &str,
    partition: i32,
) -> OffsetFetchResponsePartitions {
    offsets
        .and_then(|offsets| offsets.get(&(topic.to_string(), partition)))
        .map_or_else(
            || missing_group_row(partition, codes::NONE),
            |entry| committed_group_row(partition, entry),
        )
}

fn committed_group_row(partition: i32, entry: &OffsetEntry) -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index: partition,
        committed_offset: entry.offset,
        committed_leader_epoch: entry.leader_epoch,
        metadata: Some(entry.metadata.clone()),
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// A partition with no committed offset: offset -1, leader epoch -1 and the
/// empty metadata string, as Kafka's `OffsetMetadataManager.fetchOffsets`
/// builds it.
fn missing_group_row(partition: i32, error_code: i16) -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index: partition,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code,
        ..Default::default()
    }
}

fn legacy_row(
    offsets: Option<&GroupOffsets>,
    topic: &str,
    partition: i32,
) -> OffsetFetchResponsePartition {
    offsets
        .and_then(|offsets| offsets.get(&(topic.to_string(), partition)))
        .map_or_else(
            || OffsetFetchResponsePartition {
                partition_index: partition,
                committed_offset: -1,
                committed_leader_epoch: -1,
                metadata: Some(String::new()),
                error_code: codes::NONE,
                ..Default::default()
            },
            |entry| committed_legacy_row(partition, entry),
        )
}

fn committed_legacy_row(partition: i32, entry: &OffsetEntry) -> OffsetFetchResponsePartition {
    OffsetFetchResponsePartition {
        partition_index: partition,
        committed_offset: entry.offset,
        committed_leader_epoch: entry.leader_epoch,
        metadata: Some(entry.metadata.clone()),
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// The offsets of every group for the inspector: `{group: [{topic, partition,
/// offset, leader_epoch, metadata, commit_timestamp}]}`.
pub fn snapshot(offsets: &BTreeMap<GroupId, GroupOffsets>) -> Value {
    Value::Object(
        offsets
            .iter()
            .map(|(group, entries)| {
                let rows: Vec<Value> = entries
                    .iter()
                    .map(|((topic, partition), entry)| {
                        json!({
                            "topic": topic,
                            "partition": partition,
                            "offset": entry.offset,
                            "leader_epoch": entry.leader_epoch,
                            "metadata": entry.metadata,
                            "commit_timestamp": entry.commit_timestamp,
                        })
                    })
                    .collect();
                (group.as_str().to_string(), Value::Array(rows))
            })
            .collect(),
    )
}
