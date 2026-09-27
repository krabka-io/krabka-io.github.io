//! The request handlers, one module per api, and what they share.
//!
//! Every handler is `fn(&mut BrokerNode, &mut Ctx<'_>, &RequestCtx, Req) ->
//! Outcome<Resp>`, registered once in [`super::dispatch`] for the listener
//! that serves it: the client listener's apis, the controller apis a broker
//! forwards ([`forwarded`]), and the controller listener's apis, which the
//! node answers as the active controller. The helpers here are the
//! decisions several apis make the same way: resolving a topic by name or
//! id, the KIP-320 leader-epoch fence, the topic settings an append honours,
//! and the KIP-430 operation bit fields a cluster with no authorizer
//! reports.

use std::cmp::Ordering;

use krabka_metadata::{BrokerRegistrationRecord, MetadataImage};
use krabka_protocol::{primitives::uuid::Uuid as WireUuid, records::TimestampType};
use uuid::Uuid;

use super::{
    BrokerNode, cluster,
    log::{AppendPolicy, DEFAULT_TIMESTAMP_AFTER_MAX_MS},
};
use crate::lab::codes;

/// `AllocateProducerIds` (67), on the controller listener.
pub mod allocate_producer_ids;
/// `AlterPartition` (56), on the controller listener.
pub mod alter_partition;
/// `ApiVersions` (18), on both listeners.
pub mod api_versions;
/// `BrokerHeartbeat` (63), on the controller listener.
pub mod broker_heartbeat;
/// `BrokerRegistration` (62), on the controller listener.
pub mod broker_registration;
/// `CreatePartitions` (37), on the controller listener.
pub mod create_partitions;
/// `CreateTopics` (19), on the controller listener.
pub mod create_topics;
/// `DeleteTopics` (20), on the controller listener.
pub mod delete_topics;
/// `DescribeCluster` (60).
pub mod describe_cluster;
/// `DescribeConfigs` (32).
pub mod describe_configs;
/// `DescribeQuorum` (55), on the controller listener.
pub mod describe_quorum;
/// `DescribeTopicPartitions` (75).
pub mod describe_topic_partitions;
/// `Envelope` (58), on the controller listener.
pub mod envelope;
/// `Fetch` (1).
pub mod fetch;
/// `FindCoordinator` (10).
pub mod find_coordinator;
/// The controller apis the client listener forwards to the active
/// controller.
pub mod forwarded;
/// The group apis, through the group coordinator.
pub mod groups;
/// `InitProducerId` (22).
pub mod init_producer_id;
/// `ListOffsets` (2).
pub mod list_offsets;
/// `Metadata` (3).
pub mod metadata;
/// `OffsetForLeaderEpoch` (23).
pub mod offset_for_leader_epoch;
/// `Produce` (0).
pub mod produce;
/// `SaslHandshake` (17) and `SaslAuthenticate` (36).
pub mod sasl;

/// Kafka's default `max.message.bytes`.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1_048_588;

/// The KIP-430 bit field of every cluster operation, what Kafka reports with
/// no authorizer configured: `CREATE`, `ALTER`, `DESCRIBE`, `CLUSTER_ACTION`,
/// `DESCRIBE_CONFIGS`, `ALTER_CONFIGS`, `IDEMPOTENT_WRITE`, `CREATE_TOKENS`
/// and `DESCRIBE_TOKENS`.
pub const CLUSTER_AUTHORIZED_OPERATIONS: i32 = (1 << 5)
    | (1 << 7)
    | (1 << 8)
    | (1 << 9)
    | (1 << 10)
    | (1 << 11)
    | (1 << 12)
    | (1 << 13)
    | (1 << 14);

/// The KIP-430 bit field of every topic operation: `READ`, `WRITE`, `CREATE`,
/// `DELETE`, `ALTER`, `DESCRIBE`, `DESCRIBE_CONFIGS` and `ALTER_CONFIGS`.
pub const TOPIC_AUTHORIZED_OPERATIONS: i32 =
    (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6) | (1 << 7) | (1 << 8) | (1 << 10) | (1 << 11);

/// Kafka's "not present" value of an authorized-operations field.
pub const NO_AUTHORIZED_OPERATIONS: i32 = i32::MIN;

/// Kafka's `Topic.isInternal`: the topics the broker owns.
#[must_use]
pub fn is_internal_topic(name: &str) -> bool {
    matches!(
        name,
        cluster::CONSUMER_OFFSETS_TOPIC | "__transaction_state" | "__share_group_state"
    )
}

/// The wire form of a topic id.
#[must_use]
pub fn wire_uuid(id: Uuid) -> WireUuid {
    WireUuid(id.into_bytes())
}

/// The topic id a wire field carries.
#[must_use]
pub fn uuid_of(id: WireUuid) -> Uuid {
    Uuid::from_bytes(id.0)
}

/// The topic a request row names: by id when it carries one, else by name.
///
/// # Errors
/// Returns `UNKNOWN_TOPIC_ID` for an id no topic has, and
/// `UNKNOWN_TOPIC_OR_PARTITION` for a name no topic has.
pub fn resolve_topic(image: &MetadataImage, name: &str, id: WireUuid) -> Result<String, i16> {
    if id != WireUuid::ZERO {
        return image
            .topic_name_by_id(&uuid_of(id))
            .map(str::to_owned)
            .ok_or(codes::UNKNOWN_TOPIC_ID);
    }
    image
        .topic(name)
        .map(|t| t.name.clone())
        .ok_or(codes::UNKNOWN_TOPIC_OR_PARTITION)
}

/// Kafka's `ReplicaManager.getPartitionOrError` for a partition this broker
/// does not host: `NOT_LEADER_OR_FOLLOWER` when the image has it, so the
/// client refreshes its metadata, else `UNKNOWN_TOPIC_OR_PARTITION`.
#[must_use]
pub fn unhosted_error(image: &MetadataImage, topic: &str, partition: i32) -> i16 {
    if image.partition(topic, partition).is_some() {
        codes::NOT_LEADER_OR_FOLLOWER
    } else {
        codes::UNKNOWN_TOPIC_OR_PARTITION
    }
}

/// The registration of `broker` when it is alive, registered and not
/// fenced: Kafka's `MetadataCache.getAliveBrokerNode`.
#[must_use]
pub fn alive_broker(image: &MetadataImage, broker: i32) -> Option<&BrokerRegistrationRecord> {
    image
        .broker(cluster::meta_id(broker))
        .filter(|registration| !registration.fenced)
}

/// The KIP-951 hint of where a partition's leader is, Kafka's
/// `KafkaApis.getCurrentLeader`: the leader and epoch of this broker's
/// replica, else the image's, else `(-1, -1)`, with the leader's
/// registration when it is alive.
#[must_use]
pub fn current_leader<'a>(
    node: &'a BrokerNode,
    topic: &str,
    partition: i32,
) -> (i32, i32, Option<&'a BrokerRegistrationRecord>) {
    let (leader, epoch) = if let Some(replica) = node.replica(topic, partition) {
        (replica.leader.unwrap_or(-1), replica.leader_epoch)
    } else if let Some(record) = node.image().partition(topic, partition) {
        (
            cluster::record_leader(record).unwrap_or(-1),
            record.leader_epoch.0,
        )
    } else {
        (-1, -1)
    };
    (leader, epoch, alive_broker(node.image(), leader))
}

/// The settings an append to `topic` honours, from its config overrides.
#[must_use]
pub fn append_policy(node: &BrokerNode, topic: &str) -> AppendPolicy {
    let config = node.image().topic_config(topic);
    let get = |key: &str| config.and_then(|c| c.get(key)).map(String::as_str);
    AppendPolicy {
        timestamp_type: if get("message.timestamp.type") == Some("LogAppendTime") {
            TimestampType::LogAppendTime
        } else {
            TimestampType::CreateTime
        },
        max_message_bytes: get("max.message.bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_MESSAGE_BYTES),
        compacted: get("cleanup.policy")
            .is_some_and(|policy| policy.split(',').any(|p| p.trim() == "compact")),
        timestamp_before_max_ms: get("message.timestamp.before.max.ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(i64::MAX),
        timestamp_after_max_ms: get("message.timestamp.after.max.ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_TIMESTAMP_AFTER_MAX_MS),
    }
}

/// Which `current_leader_epoch` values mean "no epoch asserted".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochRule {
    /// `Fetch` (`FetchRequest.optionalEpoch`): every negative epoch.
    Fetch,
    /// `ListOffsets` and `OffsetForLeaderEpoch`: only `-1`; any other value
    /// is compared with the partition's epoch.
    ListOffsets,
}

/// The KIP-320 fence of a request's `current_leader_epoch` against the
/// partition's live epoch, Kafka's `Partition.checkCurrentLeaderEpoch`:
/// `FENCED_LEADER_EPOCH` when the client is behind, `UNKNOWN_LEADER_EPOCH`
/// when it is ahead, `None` when they agree or the client asserted nothing.
#[must_use]
pub fn epoch_fence(current: i32, requested: i32, rule: EpochRule) -> Option<i16> {
    if requested == -1 || (rule == EpochRule::Fetch && requested < 0) {
        return None;
    }
    match requested.cmp(&current) {
        Ordering::Less => Some(codes::FENCED_LEADER_EPOCH),
        Ordering::Greater => Some(codes::UNKNOWN_LEADER_EPOCH),
        Ordering::Equal => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn epoch_fence_follows_each_apis_rule() {
        assert!(epoch_fence(3, 3, EpochRule::Fetch).is_none());
        assert!(epoch_fence(3, -1, EpochRule::Fetch).is_none());
        assert!(epoch_fence(3, -2, EpochRule::Fetch).is_none());
        assert!(epoch_fence(3, 2, EpochRule::Fetch) == Some(codes::FENCED_LEADER_EPOCH));
        assert!(epoch_fence(3, 4, EpochRule::Fetch) == Some(codes::UNKNOWN_LEADER_EPOCH));
        assert!(epoch_fence(3, -1, EpochRule::ListOffsets).is_none());
        assert!(epoch_fence(3, -2, EpochRule::ListOffsets) == Some(codes::FENCED_LEADER_EPOCH));
        assert!(epoch_fence(0, 0, EpochRule::ListOffsets).is_none());
    }

    #[test]
    fn authorized_operation_bit_fields_match_kafka() {
        assert!(CLUSTER_AUTHORIZED_OPERATIONS == 32_672);
        assert!(TOPIC_AUTHORIZED_OPERATIONS == 3_576);
    }
}
