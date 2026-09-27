//! The broker's view of the cluster, and the local controller that fills it
//! in until the `KRaft` controller drives it.
//!
//! Every fact a handler answers about the cluster comes from a real
//! [`MetadataImage`], and the image changes only through
//! [`BrokerNode::apply_metadata`](super::BrokerNode::apply_metadata). The
//! [`LocalController`] here produces the records that a controller quorum
//! would commit: the broker's own registration at start, a topic's records on
//! `CreateTopics`, a deletion on `DeleteTopics`, extra partitions on
//! `CreatePartitions`, and the `__consumer_offsets` topic on the first
//! `FindCoordinator`. Replica placement is round-robin over the active
//! brokers, leader first, with a rotating start so partitions spread; a
//! manual assignment keeps its order and leads with its first active replica.

use std::collections::BTreeMap;

use base64::Engine as _;
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, DeleteTopicRecord, LeaderEpoch, MetadataImage,
    MetadataRecord, NodeId as MetaNodeId, PartitionRecord, TopicConfigRecord, TopicRecord,
};
use krabka_security::ListenerProtocol;
use uuid::Uuid;

use crate::lab::{codes, net::Ctx};

/// The cluster id every lab broker reports until a controller assigns one.
pub const LAB_CLUSTER_ID: Uuid = Uuid::from_u128(0x4b72_6162_6b61_4c61_6243_6c75_7374_6572);

/// The listener name every lab broker registers.
pub const LISTENER_NAME: &str = "PLAINTEXT";

/// The consumer-offsets topic the group coordinator lives on.
pub const CONSUMER_OFFSETS_TOPIC: &str = "__consumer_offsets";
/// Kafka's `offsets.topic.num.partitions`.
pub const CONSUMER_OFFSETS_PARTITIONS: i32 = 50;
/// Kafka's `offsets.topic.replication.factor`, capped by the broker count.
pub const CONSUMER_OFFSETS_REPLICATION_FACTOR: i16 = 3;

/// Kafka's `Topic.MAX_NAME_LENGTH`.
pub const MAX_TOPIC_NAME_LENGTH: usize = 249;

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

/// A fresh, non-nil topic id drawn from the node's deterministic generator.
pub fn new_topic_id(ctx: &mut Ctx<'_>) -> Uuid {
    loop {
        let id = Uuid::from_u64_pair(ctx.rand(u64::MAX), ctx.rand(u64::MAX));
        if !id.is_nil() {
            return id;
        }
    }
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

/// The ids of the registered brokers, ascending.
#[must_use]
pub fn registered_brokers(image: &MetadataImage) -> Vec<i32> {
    let mut ids: Vec<i32> = image.brokers().map(|b| wire_id(b.node_id)).collect();
    ids.sort_unstable();
    ids
}

/// The registration record of a lab broker.
#[must_use]
pub fn registration_record(
    broker_id: i32,
    rack: Option<String>,
    incarnation_id: Uuid,
) -> BrokerRegistrationRecord {
    BrokerRegistrationRecord {
        node_id: meta_id(broker_id),
        broker_epoch: 0,
        incarnation_id,
        host: broker_host(broker_id),
        port: crate::lab::net::KAFKA_PORT,
        rack,
        endpoints: vec![BrokerEndpoint {
            name: LISTENER_NAME.to_string(),
            host: broker_host(broker_id),
            port: crate::lab::net::KAFKA_PORT,
            protocol: ListenerProtocol::Plaintext,
        }],
        log_dirs: Vec::new(),
        fenced: false,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        features: BTreeMap::new(),
    }
}

/// Kafka's `Topic.validate`, in Kafka's order, with Kafka's messages.
///
/// # Errors
/// Returns the message `INVALID_TOPIC_EXCEPTION` carries.
pub fn validate_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Topic name is invalid: the empty string is not allowed".into());
    }
    if name == "." {
        return Err("Topic name is invalid: '.' is not allowed".into());
    }
    if name == ".." {
        return Err("Topic name is invalid: '..' is not allowed".into());
    }
    if name.encode_utf16().count() > MAX_TOPIC_NAME_LENGTH {
        return Err(format!(
            "Topic name is invalid: the length of '{name}' is longer than the max allowed length {MAX_TOPIC_NAME_LENGTH}"
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "Topic name is invalid: '{name}' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
        ));
    }
    Ok(())
}

/// Whether two names are the same once `.` and `_` are unified, Kafka's
/// `Topic.hasCollision`.
#[must_use]
pub fn topic_names_collide(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).all(|(x, y)| unify(x) == unify(y))
}

fn unify(byte: u8) -> u8 {
    if byte == b'.' { b'_' } else { byte }
}

/// The existing topic a new `name` collides with, Kafka's
/// `Topic.hasCollisionChars` check: the names differ only in `.` against
/// `_`. The least such name, when several do.
#[must_use]
pub fn colliding_topic<'a>(image: &'a MetadataImage, name: &str) -> Option<&'a str> {
    if !name.contains(['.', '_']) {
        return None;
    }
    image
        .topics()
        .map(|t| t.name.as_str())
        .filter(|existing| topic_names_collide(existing, name))
        .min()
}

/// What a topic creation asks for, after the request's defaults are resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicPlan {
    /// The topic name.
    pub name: String,
    /// `-1` only with a manual assignment.
    pub partitions: i32,
    /// `-1` asks for every active broker, or goes with a manual assignment.
    pub replication_factor: i16,
    /// The topic config overrides, validated.
    pub configs: BTreeMap<String, String>,
    /// A manual assignment: `(partition index, replicas)` in request order.
    pub assignments: Vec<(i32, Vec<i32>)>,
}

/// A refusal of a topic change: the error code and Kafka's message, if the
/// exception Kafka throws carries one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicRefusal {
    /// The Kafka error code.
    pub code: i16,
    /// Kafka's error message, when its exception carries one.
    pub message: Option<String>,
}

impl TopicRefusal {
    fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(message.into()),
        }
    }
}

/// The records a topic creation commits.
#[derive(Clone, Debug, PartialEq)]
pub struct PlannedTopic {
    /// The new topic's id.
    pub topic_id: Uuid,
    /// The replicas of each partition, by partition.
    pub assignments: Vec<Vec<i32>>,
    /// The records that create the topic, in commit order.
    pub records: Vec<MetadataRecord>,
}

/// The ids of the brokers a placement may use: registered, not fenced and not
/// in controlled shutdown, ascending.
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

/// Kafka's `INVALID_REPLICATION_FACTOR` message for a placement the active
/// brokers cannot satisfy.
fn placement_failure(replication_factor: usize, usable: usize) -> TopicRefusal {
    let reason = if usable == 0 {
        "All brokers are currently fenced, or have all their log directories cordoned.".to_string()
    } else {
        format!(
            "The target replication factor of {replication_factor} cannot be reached because only {usable} broker(s) are registered or some brokers have all their log directories cordoned."
        )
    };
    TopicRefusal::new(
        codes::INVALID_REPLICATION_FACTOR,
        format!("Unable to replicate the partition {replication_factor} time(s): {reason}"),
    )
}

/// Round-robin replicas over `brokers`, `count` partitions of
/// `replication_factor` replicas each, starting at broker `start`.
fn round_robin(
    brokers: &[i32],
    count: usize,
    replication_factor: usize,
    start: usize,
) -> Vec<Vec<i32>> {
    (0..count)
        .map(|p| {
            (0..replication_factor)
                .map(|r| brokers[(start + p + r) % brokers.len()])
                .collect()
        })
        .collect()
}

/// The controller decisions a broker makes on its own until the controller
/// batch drives [`super::BrokerNode::apply_metadata`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocalController {
    /// Where the next automatic placement starts in the broker list, so
    /// leaders rotate across topics.
    next_start: usize,
}

impl LocalController {
    /// Validate a topic creation against the image and build its records.
    ///
    /// # Errors
    /// Returns the refusal a `CreateTopics` row reports, in Kafka's order of
    /// checks: name, existence, name collision, then the counts or the manual
    /// assignment, then the placement.
    pub fn plan_topic(
        &mut self,
        image: &MetadataImage,
        plan: &TopicPlan,
        topic_id: Uuid,
    ) -> Result<PlannedTopic, TopicRefusal> {
        validate_topic_name(&plan.name)
            .map_err(|message| TopicRefusal::new(codes::INVALID_TOPIC_EXCEPTION, message))?;
        if image.topic(&plan.name).is_some() {
            return Err(TopicRefusal::new(
                codes::TOPIC_ALREADY_EXISTS,
                format!("Topic '{}' already exists.", plan.name),
            ));
        }
        if let Some(existing) = colliding_topic(image, &plan.name) {
            return Err(TopicRefusal::new(
                codes::INVALID_TOPIC_EXCEPTION,
                format!(
                    "Topic '{}' collides with existing topic: {existing}",
                    plan.name
                ),
            ));
        }
        let (assignments, isrs) = if plan.assignments.is_empty() {
            let assignments = self.automatic_assignments(plan, &active_brokers(image))?;
            let isrs = assignments.clone();
            (assignments, isrs)
        } else {
            manual_assignments(plan, &registered_brokers(image), &active_brokers(image))?
        };
        let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
            name: plan.name.clone(),
            topic_id,
            partitions: i32::try_from(assignments.len()).unwrap_or(i32::MAX),
            replication_factor: i16::try_from(assignments[0].len()).unwrap_or(i16::MAX),
        })];
        records.extend(
            assignments
                .iter()
                .zip(&isrs)
                .enumerate()
                .map(|(index, (replicas, isr))| {
                    partition_record(&plan.name, index, replicas, isr, 0)
                }),
        );
        if !plan.configs.is_empty() {
            records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: plan.name.clone(),
                overrides: plan.configs.clone(),
            }));
        }
        Ok(PlannedTopic {
            topic_id,
            assignments,
            records,
        })
    }

    /// Round-robin placement over the active brokers, with Kafka's refusals
    /// of the counts and of a placement the broker count cannot satisfy.
    fn automatic_assignments(
        &mut self,
        plan: &TopicPlan,
        brokers: &[i32],
    ) -> Result<Vec<Vec<i32>>, TopicRefusal> {
        if plan.replication_factor == 0 || plan.replication_factor < -1 {
            return Err(TopicRefusal::new(
                codes::INVALID_REPLICATION_FACTOR,
                "Replication factor must be larger than 0, or -1 to use the default value.",
            ));
        }
        if plan.partitions == 0 || plan.partitions < -1 {
            return Err(TopicRefusal::new(
                codes::INVALID_PARTITIONS,
                "Number of partitions was set to an invalid non-positive value.",
            ));
        }
        let replication_factor = if plan.replication_factor == -1 {
            brokers.len().max(1)
        } else {
            usize::try_from(plan.replication_factor).unwrap_or(usize::MAX)
        };
        if brokers.is_empty() || replication_factor > brokers.len() {
            return Err(placement_failure(replication_factor, brokers.len()));
        }
        let partitions = usize::try_from(plan.partitions).unwrap_or(1);
        let start = self.next_start % brokers.len();
        self.next_start = self.next_start.wrapping_add(1);
        Ok(round_robin(brokers, partitions, replication_factor, start))
    }

    /// The records that add partitions `existing..count` to a topic.
    ///
    /// # Errors
    /// Returns the refusal a `CreatePartitions` row reports, in Kafka's
    /// order: an unknown topic, a count that does not grow the topic, an
    /// assignment list of the wrong length, a bad assignment, a placement the
    /// active brokers cannot satisfy.
    pub fn plan_partitions(
        &mut self,
        image: &MetadataImage,
        topic: &str,
        count: i32,
        assignments: Option<&[Vec<i32>]>,
    ) -> Result<Vec<MetadataRecord>, TopicRefusal> {
        // Kafka's `UnknownTopicOrPartitionException()` carries no message.
        let Some(record) = image.topic(topic) else {
            return Err(TopicRefusal {
                code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                message: None,
            });
        };
        let existing = image.topic_partition_count(topic);
        if count == existing {
            return Err(TopicRefusal::new(
                codes::INVALID_PARTITIONS,
                format!("Topic already has {existing} partition(s)."),
            ));
        }
        if count < existing {
            return Err(TopicRefusal::new(
                codes::INVALID_PARTITIONS,
                format!(
                    "The topic {topic} currently has {existing} partition(s); {count} would not be an increase."
                ),
            ));
        }
        let added = usize::try_from(count - existing).unwrap_or(0);
        let replication_factor = usize::try_from(record.replication_factor).unwrap_or(1);
        let active = active_brokers(image);
        let (new_assignments, isrs) = if let Some(manual) = assignments {
            if manual.len() != added {
                return Err(TopicRefusal::new(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    format!(
                        "Attempted to add {added} additional partition(s), but only {} assignment(s) were specified.",
                        manual.len()
                    ),
                ));
            }
            // Kafka's `createPartitions` checks each list, then its active
            // replicas, before it moves to the next list.
            let registered = registered_brokers(image);
            let mut validated = Vec::with_capacity(manual.len());
            let mut isrs = Vec::with_capacity(manual.len());
            for (offset, replicas) in manual.iter().enumerate() {
                let replicas =
                    validate_partition_assignment(replicas, &registered, Some(replication_factor))?;
                let partition = existing.saturating_add(i32::try_from(offset).unwrap_or(i32::MAX));
                isrs.push(active_isr(&replicas, &active, partition)?);
                validated.push(replicas);
            }
            (validated, isrs)
        } else {
            if active.is_empty() || replication_factor > active.len() {
                return Err(placement_failure(replication_factor, active.len()));
            }
            let start = self.next_start % active.len();
            self.next_start = self.next_start.wrapping_add(1);
            let placed = round_robin(&active, added, replication_factor, start);
            let isrs = placed.clone();
            (placed, isrs)
        };
        let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
            name: topic.to_string(),
            topic_id: record.topic_id,
            partitions: count,
            replication_factor: record.replication_factor,
        })];
        let first = usize::try_from(existing).unwrap_or(0);
        records.extend(
            new_assignments
                .iter()
                .zip(&isrs)
                .enumerate()
                .map(|(i, (replicas, isr))| partition_record(topic, first + i, replicas, isr, 0)),
        );
        Ok(records)
    }

    /// The record that deletes a topic.
    #[must_use]
    pub fn delete_topic(name: &str) -> MetadataRecord {
        MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: name.to_string(),
        })
    }

    /// The `__consumer_offsets` topic Kafka creates on the first group
    /// request: 50 compacted partitions, replicated on up to three brokers.
    ///
    /// # Errors
    /// Returns the placement refusal when no broker is active.
    pub fn plan_consumer_offsets(
        &mut self,
        image: &MetadataImage,
        topic_id: Uuid,
    ) -> Result<PlannedTopic, TopicRefusal> {
        let replication_factor = i16::try_from(active_brokers(image).len())
            .unwrap_or(i16::MAX)
            .clamp(1, CONSUMER_OFFSETS_REPLICATION_FACTOR);
        self.plan_topic(
            image,
            &TopicPlan {
                name: CONSUMER_OFFSETS_TOPIC.to_string(),
                partitions: CONSUMER_OFFSETS_PARTITIONS,
                replication_factor,
                configs: BTreeMap::from([
                    ("cleanup.policy".to_string(), "compact".to_string()),
                    ("compression.type".to_string(), "producer".to_string()),
                    ("segment.bytes".to_string(), "104857600".to_string()),
                ]),
                assignments: Vec::new(),
            },
            topic_id,
        )
    }
}

/// Replica lists and their ISRs, by partition.
type Placement = (Vec<Vec<i32>>, Vec<Vec<i32>>);

/// Kafka's `ReplicationControlManager.createTopic` checks on a manual
/// assignment, in its order: both counts `-1`; then, list by list in request
/// order, no partition assigned twice, a valid replica list and an active
/// replica; then the partitions `0..n`. The replica lists and their ISRs come
/// back in partition order.
fn manual_assignments(
    plan: &TopicPlan,
    registered: &[i32],
    active: &[i32],
) -> Result<Placement, TopicRefusal> {
    if plan.replication_factor != -1 {
        return Err(TopicRefusal::new(
            codes::INVALID_REQUEST,
            "A manual partition assignment was specified, but replication factor was not set to -1.",
        ));
    }
    if plan.partitions != -1 {
        return Err(TopicRefusal::new(
            codes::INVALID_REQUEST,
            "A manual partition assignment was specified, but numPartitions was not set to -1.",
        ));
    }
    let mut by_partition: BTreeMap<i32, (Vec<i32>, Vec<i32>)> = BTreeMap::new();
    let mut replication_factor = None;
    for (index, replicas) in &plan.assignments {
        if by_partition.contains_key(index) {
            return Err(TopicRefusal::new(
                codes::INVALID_REPLICA_ASSIGNMENT,
                format!("Found multiple manual partition assignments for partition {index}"),
            ));
        }
        let replicas = validate_partition_assignment(replicas, registered, replication_factor)?;
        replication_factor = Some(replicas.len());
        let isr = active_isr(&replicas, active, *index)?;
        by_partition.insert(*index, (replicas, isr));
    }
    if by_partition
        .keys()
        .copied()
        .ne(0..i32::try_from(by_partition.len()).unwrap_or(i32::MAX))
    {
        return Err(TopicRefusal::new(
            codes::INVALID_REPLICA_ASSIGNMENT,
            "partitions should be a consecutive 0-based integer sequence",
        ));
    }
    Ok(by_partition.into_values().unzip())
}

/// Kafka's `validateManualPartitionAssignment` on one replica list: not empty,
/// every broker registered and named once (checked in ascending id order, so
/// the message names the least offender), and as many replicas as the
/// partitions before it.
fn validate_partition_assignment(
    replicas: &[i32],
    registered: &[i32],
    replication_factor: Option<usize>,
) -> Result<Vec<i32>, TopicRefusal> {
    let refuse = |message: String| {
        Err(TopicRefusal::new(
            codes::INVALID_REPLICA_ASSIGNMENT,
            message,
        ))
    };
    if replicas.is_empty() {
        return refuse(
            "The manual partition assignment includes an empty replica list.".to_string(),
        );
    }
    let mut sorted = replicas.to_vec();
    sorted.sort_unstable();
    let mut previous = None;
    for broker in sorted {
        if !registered.contains(&broker) {
            return refuse(format!(
                "The manual partition assignment includes broker {broker}, but no such broker is registered."
            ));
        }
        if previous == Some(broker) {
            return refuse(format!(
                "The manual partition assignment includes the broker {broker} more than once."
            ));
        }
        previous = Some(broker);
    }
    if let Some(expected) = replication_factor
        && replicas.len() != expected
    {
        return refuse(format!(
            "The manual partition assignment includes a partition with {} replica(s), but this is not consistent with previous partitions, which have {expected} replica(s).",
            replicas.len()
        ));
    }
    Ok(replicas.to_vec())
}

/// The ISR of a manually assigned partition: its active replicas in the
/// listed order, the first of which leads. A partition with none is refused,
/// as Kafka refuses it.
fn active_isr(replicas: &[i32], active: &[i32], partition: i32) -> Result<Vec<i32>, TopicRefusal> {
    let isr: Vec<i32> = replicas
        .iter()
        .copied()
        .filter(|r| active.contains(r))
        .collect();
    if isr.is_empty() {
        return Err(TopicRefusal::new(
            codes::INVALID_REPLICA_ASSIGNMENT,
            format!(
                "All brokers specified in the manual partition assignment for partition {partition} are fenced or in controlled shutdown."
            ),
        ));
    }
    Ok(isr)
}

/// A partition record for a fresh partition: the first ISR replica leads
/// ([`NO_LEADER`] with an empty ISR), and the leader epoch and partition
/// epoch are `epoch`.
#[must_use]
pub fn partition_record(
    topic: &str,
    index: usize,
    replicas: &[i32],
    isr: &[i32],
    epoch: i32,
) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition: i32::try_from(index).unwrap_or(i32::MAX),
        leader: isr.first().map_or(NO_LEADER, |&id| meta_id(id)),
        isr: isr.iter().map(|&id| meta_id(id)).collect(),
        directories: vec![Uuid::nil(); replicas.len()],
        replicas: replicas.iter().map(|&id| meta_id(id)).collect(),
        leader_epoch: LeaderEpoch(epoch),
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        partition_epoch: epoch,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn image_with_brokers(ids: &[i32]) -> MetadataImage {
        let mut image = MetadataImage::new(LAB_CLUSTER_ID);
        for &id in ids {
            image.apply(&MetadataRecord::V1BrokerRegistration(registration_record(
                id,
                None,
                Uuid::from_u128(u128::try_from(id).unwrap()),
            )));
        }
        image
    }

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
    fn cluster_id_uses_kafka_base64() {
        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert!(cluster_id_string(id) == "AQIDBAUGBwgJCgsMDQ4PEA");
    }

    #[test]
    fn topic_name_rules_follow_kafka() {
        assert!(validate_topic_name("orders") == Ok(()));
        assert!(validate_topic_name("a.b_c-D9") == Ok(()));
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(".").is_err());
        assert!(validate_topic_name("..").is_err());
        assert!(validate_topic_name("...") == Ok(()));
        assert!(validate_topic_name("a/b").is_err());
        assert!(validate_topic_name(&"a".repeat(250)).is_err());
        assert!(validate_topic_name(&"a".repeat(249)) == Ok(()));
        assert!(topic_names_collide("a.b", "a_b"));
        assert!(!topic_names_collide("a.b", "a-b"));
    }

    fn plan(name: &str, partitions: i32, replication_factor: i16) -> TopicPlan {
        TopicPlan {
            name: name.into(),
            partitions,
            replication_factor,
            configs: BTreeMap::new(),
            assignments: Vec::new(),
        }
    }

    fn apply(image: &mut MetadataImage, records: &[MetadataRecord]) {
        for record in records {
            image.apply(record);
        }
    }

    fn refusal(code: i16, message: &str) -> TopicRefusal {
        TopicRefusal::new(code, message)
    }

    #[test]
    fn plans_round_robin_records_with_configs_and_rotating_leaders() {
        let image = image_with_brokers(&[1, 2, 3]);
        let mut controller = LocalController::default();
        let with_configs = TopicPlan {
            configs: BTreeMap::from([("retention.ms".to_string(), "1000".to_string())]),
            ..plan("orders", 2, 2)
        };
        let planned = controller
            .plan_topic(&image, &with_configs, Uuid::from_u128(9))
            .unwrap();
        let expected = PlannedTopic {
            topic_id: Uuid::from_u128(9),
            assignments: vec![vec![1, 2], vec![2, 3]],
            records: vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "orders".into(),
                    topic_id: Uuid::from_u128(9),
                    partitions: 2,
                    replication_factor: 2,
                }),
                partition_record("orders", 0, &[1, 2], &[1, 2], 0),
                partition_record("orders", 1, &[2, 3], &[2, 3], 0),
                MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: "orders".into(),
                    overrides: with_configs.configs.clone(),
                }),
            ],
        };
        assert!(planned == expected);
        let next = controller
            .plan_topic(&image, &plan("next", 1, -1), Uuid::from_u128(10))
            .unwrap();
        assert!(next.assignments == vec![vec![2, 3, 1]]);
    }

    #[test]
    fn refuses_bad_counts_names_and_unsatisfiable_placements() {
        let image = image_with_brokers(&[1, 2, 3]);
        for (case, request, expected) in [
            (
                "zero partitions",
                plan("t", 0, 1),
                refusal(
                    codes::INVALID_PARTITIONS,
                    "Number of partitions was set to an invalid non-positive value.",
                ),
            ),
            (
                "zero replicas",
                plan("t", 1, 0),
                refusal(
                    codes::INVALID_REPLICATION_FACTOR,
                    "Replication factor must be larger than 0, or -1 to use the default value.",
                ),
            ),
            (
                "more replicas than brokers",
                plan("t", 1, 4),
                refusal(
                    codes::INVALID_REPLICATION_FACTOR,
                    "Unable to replicate the partition 4 time(s): The target replication factor of 4 cannot be reached because only 3 broker(s) are registered or some brokers have all their log directories cordoned.",
                ),
            ),
            (
                "invalid name",
                plan("a/b", 1, 1),
                refusal(
                    codes::INVALID_TOPIC_EXCEPTION,
                    "Topic name is invalid: 'a/b' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
                ),
            ),
        ] {
            let result =
                LocalController::default().plan_topic(&image, &request, Uuid::from_u128(1));
            assert!(result == Err(expected), "{case}");
        }
        let empty = MetadataImage::new(LAB_CLUSTER_ID);
        let none =
            LocalController::default().plan_topic(&empty, &plan("t", 1, 1), Uuid::from_u128(1));
        assert!(
            none == Err(refusal(
                codes::INVALID_REPLICATION_FACTOR,
                "Unable to replicate the partition 1 time(s): All brokers are currently fenced, or have all their log directories cordoned.",
            ))
        );
    }

    #[test]
    fn existing_and_colliding_names_are_refused() {
        let mut image = image_with_brokers(&[1]);
        let mut controller = LocalController::default();
        let planned = controller
            .plan_topic(&image, &plan("a.b", 1, 1), Uuid::from_u128(3))
            .unwrap();
        apply(&mut image, &planned.records);
        let exists = controller.plan_topic(&image, &plan("a.b", 1, 1), Uuid::from_u128(4));
        assert!(
            exists
                == Err(refusal(
                    codes::TOPIC_ALREADY_EXISTS,
                    "Topic 'a.b' already exists."
                ))
        );
        let collides = controller.plan_topic(&image, &plan("a_b", 1, 1), Uuid::from_u128(5));
        assert!(
            collides
                == Err(refusal(
                    codes::INVALID_TOPIC_EXCEPTION,
                    "Topic 'a_b' collides with existing topic: a.b"
                ))
        );
    }

    #[test]
    fn manual_assignments_keep_their_order_and_lead_with_an_active_replica() {
        let mut image = image_with_brokers(&[1, 2, 3]);
        let mut fenced = registration_record(2, None, Uuid::from_u128(2));
        fenced.fenced = true;
        image.apply(&MetadataRecord::V1BrokerRegistration(fenced));
        let manual = TopicPlan {
            assignments: vec![(1, vec![3, 1]), (0, vec![2, 3])],
            ..plan("m", -1, -1)
        };
        let planned = LocalController::default()
            .plan_topic(&image, &manual, Uuid::from_u128(7))
            .unwrap();
        assert!(planned.assignments == vec![vec![2, 3], vec![3, 1]]);
        assert!(planned.records[1] == partition_record("m", 0, &[2, 3], &[3], 0));
        assert!(planned.records[2] == partition_record("m", 1, &[3, 1], &[3, 1], 0));
    }

    #[test]
    fn manual_assignments_are_refused_like_kafka() {
        let mut image = image_with_brokers(&[1, 2, 3]);
        let mut fenced = registration_record(3, None, Uuid::from_u128(3));
        fenced.fenced = true;
        image.apply(&MetadataRecord::V1BrokerRegistration(fenced));
        let assigned = |assignments: Vec<(i32, Vec<i32>)>| TopicPlan {
            assignments,
            ..plan("m", -1, -1)
        };
        for (case, request, expected) in [
            (
                "replication factor set",
                TopicPlan {
                    replication_factor: 1,
                    ..assigned(vec![(0, vec![1])])
                },
                refusal(
                    codes::INVALID_REQUEST,
                    "A manual partition assignment was specified, but replication factor was not set to -1.",
                ),
            ),
            (
                "partition count set",
                TopicPlan {
                    partitions: 1,
                    ..assigned(vec![(0, vec![1])])
                },
                refusal(
                    codes::INVALID_REQUEST,
                    "A manual partition assignment was specified, but numPartitions was not set to -1.",
                ),
            ),
            (
                "partition assigned twice",
                assigned(vec![(0, vec![1]), (0, vec![2])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "Found multiple manual partition assignments for partition 0",
                ),
            ),
            (
                "empty replica list",
                assigned(vec![(0, vec![])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "The manual partition assignment includes an empty replica list.",
                ),
            ),
            (
                "unregistered broker",
                assigned(vec![(0, vec![9, 1])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "The manual partition assignment includes broker 9, but no such broker is registered.",
                ),
            ),
            (
                "broker named twice",
                assigned(vec![(0, vec![2, 1, 2])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "The manual partition assignment includes the broker 2 more than once.",
                ),
            ),
            (
                "inconsistent replica count",
                assigned(vec![(0, vec![1, 2]), (1, vec![1])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "The manual partition assignment includes a partition with 1 replica(s), but this is not consistent with previous partitions, which have 2 replica(s).",
                ),
            ),
            (
                "partition gap",
                assigned(vec![(0, vec![1]), (2, vec![2])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "partitions should be a consecutive 0-based integer sequence",
                ),
            ),
            (
                "only fenced replicas",
                assigned(vec![(0, vec![1]), (1, vec![3])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "All brokers specified in the manual partition assignment for partition 1 are fenced or in controlled shutdown.",
                ),
            ),
            (
                "a fenced list before an unregistered one",
                assigned(vec![(1, vec![3]), (0, vec![9])]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "All brokers specified in the manual partition assignment for partition 1 are fenced or in controlled shutdown.",
                ),
            ),
        ] {
            let result =
                LocalController::default().plan_topic(&image, &request, Uuid::from_u128(1));
            assert!(result == Err(expected), "{case}");
        }
    }

    #[test]
    fn plans_extra_partitions_after_the_existing_ones() {
        let mut image = image_with_brokers(&[1, 2]);
        let mut controller = LocalController::default();
        let planned = controller
            .plan_topic(&image, &plan("t", 1, 2), Uuid::from_u128(3))
            .unwrap();
        apply(&mut image, &planned.records);
        let grown = controller.plan_partitions(&image, "t", 3, None).unwrap();
        let expected = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: Uuid::from_u128(3),
                partitions: 3,
                replication_factor: 2,
            }),
            partition_record("t", 1, &[2, 1], &[2, 1], 0),
            partition_record("t", 2, &[1, 2], &[1, 2], 0),
        ];
        assert!(grown == expected);
        let manual = controller
            .plan_partitions(&image, "t", 2, Some(&[vec![2, 1]]))
            .unwrap();
        assert!(manual[1] == partition_record("t", 1, &[2, 1], &[2, 1], 0));
        for (case, count, assignments, expected) in [
            (
                "no growth",
                1,
                None,
                refusal(
                    codes::INVALID_PARTITIONS,
                    "Topic already has 1 partition(s).",
                ),
            ),
            (
                "shrink",
                0,
                None,
                refusal(
                    codes::INVALID_PARTITIONS,
                    "The topic t currently has 1 partition(s); 0 would not be an increase.",
                ),
            ),
            (
                "too few assignments",
                3,
                Some(vec![vec![1, 2]]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified.",
                ),
            ),
            (
                "wrong replica count",
                2,
                Some(vec![vec![1]]),
                refusal(
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    "The manual partition assignment includes a partition with 1 replica(s), but this is not consistent with previous partitions, which have 2 replica(s).",
                ),
            ),
        ] {
            let result = controller.plan_partitions(&image, "t", count, assignments.as_deref());
            assert!(result == Err(expected), "{case}");
        }
        let unknown = controller.plan_partitions(&image, "nope", 3, None);
        assert!(
            unknown
                == Err(TopicRefusal {
                    code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message: None
                })
        );
    }

    #[test]
    fn consumer_offsets_topic_is_compacted_and_capped_at_three_replicas() {
        let image = image_with_brokers(&[1, 2, 3, 4]);
        let planned = LocalController::default()
            .plan_consumer_offsets(&image, Uuid::from_u128(1))
            .unwrap();
        assert!(planned.assignments.len() == 50);
        assert!(planned.assignments.iter().all(|a| a.len() == 3));
        let configs = BTreeMap::from([
            ("cleanup.policy".to_string(), "compact".to_string()),
            ("compression.type".to_string(), "producer".to_string()),
            ("segment.bytes".to_string(), "104857600".to_string()),
        ]);
        assert!(
            planned.records.last()
                == Some(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: CONSUMER_OFFSETS_TOPIC.into(),
                    overrides: configs
                }))
        );
        let one = LocalController::default()
            .plan_consumer_offsets(&image_with_brokers(&[1]), Uuid::from_u128(1))
            .unwrap();
        assert!(one.assignments.iter().all(|a| a == &vec![1]));
    }

    #[test]
    fn a_leaderless_record_names_node_zero() {
        let record = partition_record("t", 0, &[1, 2], &[], 3);
        let MetadataRecord::V1Partition(partition) = &record else {
            panic!("a partition record");
        };
        assert!(partition.leader == NO_LEADER);
        assert!(record_leader(partition).is_none());
        let led = partition_record("t", 0, &[1, 2], &[2], 3);
        let MetadataRecord::V1Partition(partition) = &led else {
            panic!("a partition record");
        };
        assert!(record_leader(partition) == Some(2));
    }
}
