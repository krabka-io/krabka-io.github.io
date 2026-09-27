//! The decisions the active controller takes over the metadata image.
//!
//! Every method reads a [`MetadataImage`] and returns the metadata records
//! that carry the decision, for the broker to propose through the quorum.
//! Nothing here touches the log or the network, so the decisions are the
//! same on every controller and can be tested against an image alone. The
//! rules follow Kafka's `QuorumController`: `ClusterControlManager` for
//! registration and the heartbeat state machine, `ReplicationControlManager`
//! for topics, `AlterPartition` and the leader elections a fenced broker
//! forces, and `StripedReplicaPlacer`, simplified to a round-robin stripe
//! over the unfenced brokers.

use std::collections::{BTreeMap, BTreeSet};

use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, DeleteTopicRecord, LeaderEpoch, MetadataImage,
    MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord, UnregisterBrokerRecord,
};
use uuid::Uuid;

use super::{broker_id, lab_id};
use crate::lab::{
    codes,
    net::{Millis, NodeId, Rng},
};

#[cfg(test)]
mod tests;

/// The leader of a partition that has none, as `krabka_metadata` translates
/// Kafka's `-1`. Lab node ids start at 1, so it names no broker.
pub const NO_LEADER: krabka_metadata::NodeId = krabka_metadata::NodeId(0);

/// The group coordinator's topic, created on the first `FindCoordinator`.
pub const CONSUMER_OFFSETS_TOPIC: &str = "__consumer_offsets";

/// Kafka's `Topic.MAX_NAME_LENGTH`.
const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Kafka's `Errors.UNKNOWN_TOPIC_OR_PARTITION.message()`.
const UNKNOWN_TOPIC_OR_PARTITION_MESSAGE: &str = "This server does not host this topic-partition.";

/// The controller's settings, named after the Kafka configs they stand for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerConfig {
    /// `broker.session.timeout.ms`: a broker that sends no heartbeat for this
    /// long is fenced. Default: `9000`.
    pub broker_session_timeout_ms: Millis,
    /// `num.partitions`: the partition count a create request asks for with
    /// `-1`. Default: `1`.
    pub default_partitions: i32,
    /// `default.replication.factor`: the replication factor a create request
    /// asks for with `-1`. Default: `1`.
    pub default_replication_factor: i16,
    /// `unclean.leader.election.enable` for topics that do not set it.
    /// Default: `false`.
    pub unclean_leader_election_enable: bool,
    /// `offsets.topic.num.partitions`. Default: `50`.
    pub offsets_topic_partitions: i32,
    /// `offsets.topic.replication.factor`, capped at the number of unfenced
    /// brokers when the topic is created. Default: `3`.
    pub offsets_topic_replication_factor: i16,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            broker_session_timeout_ms: 9_000,
            default_partitions: 1,
            default_replication_factor: 1,
            unclean_leader_election_enable: false,
            offsets_topic_partitions: 50,
            offsets_topic_replication_factor: 3,
        }
    }
}

/// A `BrokerRegistration` request, as the controller decides on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterBroker {
    pub broker_id: NodeId,
    /// KIP-631: identifies this process of the broker. A restart brings a new
    /// one.
    pub incarnation_id: Uuid,
    pub cluster_id: Uuid,
    pub rack: Option<String>,
    /// The listeners the broker advertises; at least one.
    pub endpoints: Vec<BrokerEndpoint>,
}

/// A `BrokerHeartbeat` request, as the controller decides on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    pub broker_id: NodeId,
    /// The epoch the broker registered at.
    pub broker_epoch: i64,
    /// The last metadata offset the broker applied.
    pub current_metadata_offset: i64,
    pub want_fence: bool,
    pub want_shutdown: bool,
}

/// What a heartbeat decided: the records to propose and the answer.
#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatOutcome {
    pub records: Vec<MetadataRecord>,
    /// The broker has applied its own registration record.
    pub is_caught_up: bool,
    /// The broker is fenced after this heartbeat.
    pub is_fenced: bool,
    /// The broker may stop now.
    pub should_shut_down: bool,
}

/// One topic of a `CreateTopics` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicSpec {
    pub name: String,
    /// The partition count, or `-1` for the controller's default. Must be
    /// `-1` with a manual assignment.
    pub partitions: i32,
    /// The replication factor, or `-1` for the controller's default. Must be
    /// `-1` with a manual assignment.
    pub replication_factor: i16,
    /// The replicas of each partition, in order, when the caller places them.
    /// Empty for automatic placement.
    pub assignments: Vec<Vec<NodeId>>,
    /// The topic's config overrides.
    pub configs: BTreeMap<String, String>,
}

impl CreateTopicSpec {
    /// A topic with automatic placement and no config overrides.
    #[must_use]
    pub fn new(name: &str, partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.to_string(),
            partitions,
            replication_factor,
            assignments: Vec::new(),
            configs: BTreeMap::new(),
        }
    }
}

/// A refused topic operation: Kafka's error code and message for the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicError {
    pub code: i16,
    pub message: String,
}

impl TopicError {
    fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// A topic the controller decided to create.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatedTopic {
    pub name: String,
    pub topic_id: Uuid,
    pub partitions: i32,
    pub replication_factor: i16,
    /// The topic record, one partition record per partition, and the config
    /// record when there are overrides.
    pub records: Vec<MetadataRecord>,
}

/// One member of a proposed ISR, with the broker epoch the leader saw it at,
/// or `-1` when the request carries no epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsrMember {
    pub broker: NodeId,
    pub broker_epoch: i64,
}

/// One partition row of an `AlterPartition` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartition {
    /// The broker that sent the request; it must lead the partition.
    pub broker_id: NodeId,
    pub topic: String,
    pub partition: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    pub new_isr: Vec<IsrMember>,
}

/// An admitted `AlterPartition` row: the partition's new state and the record
/// that carries it.
#[derive(Debug, Clone, PartialEq)]
pub struct AlteredPartition {
    pub leader: NodeId,
    pub leader_epoch: i32,
    pub isr: Vec<NodeId>,
    pub partition_epoch: i32,
    pub records: Vec<MetadataRecord>,
}

/// Kafka's `BrokerControlState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerState {
    Fenced,
    Unfenced,
    ControlledShutdown,
    ShutdownNow,
}

/// Kafka's `BrokerHeartbeatManager.calculateNextBrokerState`, without the
/// wait for every active broker to replicate the controlled-shutdown records.
fn next_broker_state(
    current: BrokerState,
    heartbeat: &Heartbeat,
    caught_up: bool,
    has_leaderships: bool,
) -> BrokerState {
    match current {
        BrokerState::Fenced => {
            if heartbeat.want_shutdown {
                BrokerState::ShutdownNow
            } else if !heartbeat.want_fence && caught_up {
                BrokerState::Unfenced
            } else {
                BrokerState::Fenced
            }
        }
        BrokerState::Unfenced => {
            if heartbeat.want_fence {
                if heartbeat.want_shutdown {
                    BrokerState::ShutdownNow
                } else {
                    BrokerState::Fenced
                }
            } else if heartbeat.want_shutdown {
                if has_leaderships {
                    BrokerState::ControlledShutdown
                } else {
                    BrokerState::ShutdownNow
                }
            } else {
                BrokerState::Unfenced
            }
        }
        BrokerState::ControlledShutdown => {
            if has_leaderships {
                BrokerState::ControlledShutdown
            } else {
                BrokerState::ShutdownNow
            }
        }
        BrokerState::ShutdownNow => BrokerState::ShutdownNow,
    }
}

/// The active controller's decisions.
pub struct ControllerDecisions {
    config: ControllerConfig,
    /// When each broker last heartbeated this controller.
    last_heartbeat: BTreeMap<NodeId, Millis>,
    /// Where the next automatic placement starts its stripe.
    placement_cursor: usize,
    /// Deterministic topic ids.
    rng: Rng,
}

impl ControllerDecisions {
    /// Decisions under `config`. `seed` makes the topic ids deterministic, so
    /// a scenario replays exactly.
    #[must_use]
    pub fn new(config: ControllerConfig, seed: u64) -> Self {
        Self {
            config,
            last_heartbeat: BTreeMap::new(),
            placement_cursor: 0,
            rng: Rng::new(seed),
        }
    }

    #[must_use]
    pub fn config(&self) -> &ControllerConfig {
        &self.config
    }

    /// This controller became active at `now`. Every registered broker gets a
    /// full session before it can be fenced, as Kafka's heartbeat manager
    /// gives after a failover.
    pub fn activate(&mut self, image: &MetadataImage, now: Millis) {
        self.last_heartbeat = registered_brokers(image)
            .into_iter()
            .map(|broker| (broker, now))
            .collect();
    }

    /// When `broker` last heartbeated this controller.
    #[must_use]
    pub fn last_heartbeat(&self, broker: NodeId) -> Option<Millis> {
        self.last_heartbeat.get(&broker).copied()
    }

    /// Decide a `BrokerRegistration`. `next_offset` is the log offset the
    /// records will be appended at; it becomes the broker epoch (KIP-903). A
    /// new incarnation registers fenced and unfences on its first heartbeat;
    /// the same incarnation registering again keeps its epoch and its fence
    /// state.
    ///
    /// # Errors
    /// `INCONSISTENT_CLUSTER_ID` for another cluster's id,
    /// `INVALID_REGISTRATION` without a listener, and
    /// `DUPLICATE_BROKER_REGISTRATION` while the previous incarnation of the
    /// broker still holds an unfenced session.
    pub fn register_broker(
        &mut self,
        image: &MetadataImage,
        request: &RegisterBroker,
        now: Millis,
        next_offset: i64,
    ) -> Result<Vec<MetadataRecord>, i16> {
        if request.cluster_id != image.cluster_id() {
            return Err(codes::INCONSISTENT_CLUSTER_ID);
        }
        let Some(first) = request.endpoints.first() else {
            return Err(codes::INVALID_REGISTRATION);
        };
        let existing = image.broker(broker_id(request.broker_id));
        let same_incarnation =
            existing.is_some_and(|existing| existing.incarnation_id == request.incarnation_id);
        if let Some(existing) = existing
            && !same_incarnation
            && !existing.fenced
            && self.session_valid(request.broker_id, now)
        {
            return Err(codes::DUPLICATE_BROKER_REGISTRATION);
        }
        let mut records = match existing {
            Some(existing) if !same_incarnation && !existing.fenced => {
                self.elect_leaders_after_fence(image, request.broker_id)
            }
            _ => Vec::new(),
        };
        records.push(MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: broker_id(request.broker_id),
                broker_epoch: existing
                    .filter(|_| same_incarnation)
                    .map_or(next_offset, |existing| existing.broker_epoch),
                incarnation_id: request.incarnation_id,
                host: first.host.clone(),
                port: first.port,
                rack: request.rack.clone(),
                endpoints: request.endpoints.clone(),
                log_dirs: Vec::new(),
                fenced: existing
                    .filter(|_| same_incarnation)
                    .is_none_or(|existing| existing.fenced),
                in_controlled_shutdown: false,
                cordoned_log_dirs: None,
                features: BTreeMap::new(),
            },
        ));
        self.last_heartbeat.insert(request.broker_id, now);
        Ok(records)
    }

    /// Decide a `BrokerHeartbeat`: record the contact, and move the broker
    /// through Kafka's heartbeat state machine. A fenced broker that has
    /// applied its own registration unfences, and a leaderless partition it
    /// can lead gets it as leader. An unfenced broker that asks to be fenced,
    /// or that asks to shut down while leading nothing another replica can
    /// take, is fenced and leaves every ISR. One that asks to shut down while
    /// leading enters controlled shutdown, hands its leaderships over, and may
    /// stop once it leads nothing.
    ///
    /// # Errors
    /// `STALE_BROKER_EPOCH` for an unregistered broker or an epoch other than
    /// the registered one.
    pub fn broker_heartbeat(
        &mut self,
        image: &MetadataImage,
        heartbeat: &Heartbeat,
        now: Millis,
    ) -> Result<HeartbeatOutcome, i16> {
        let Some(registration) = image.broker(broker_id(heartbeat.broker_id)) else {
            return Err(codes::STALE_BROKER_EPOCH);
        };
        if registration.broker_epoch != heartbeat.broker_epoch {
            return Err(codes::STALE_BROKER_EPOCH);
        }
        self.last_heartbeat.insert(heartbeat.broker_id, now);
        let caught_up = heartbeat.current_metadata_offset >= registration.broker_epoch;
        let current = if registration.fenced {
            BrokerState::Fenced
        } else if registration.in_controlled_shutdown {
            BrokerState::ControlledShutdown
        } else {
            BrokerState::Unfenced
        };
        let has_leaderships = !Self::movable_leaderships(image, heartbeat.broker_id).is_empty();
        let next = next_broker_state(current, heartbeat, caught_up, has_leaderships);
        let records = match (current, next) {
            (BrokerState::Fenced, BrokerState::Unfenced) => {
                let mut records = Self::elect_offline_partitions(image, heartbeat.broker_id);
                records.push(registration_with(registration, false, false));
                records
            }
            (
                BrokerState::Unfenced | BrokerState::ControlledShutdown,
                BrokerState::Fenced | BrokerState::ShutdownNow,
            ) => self.fence_records(image, heartbeat.broker_id),
            (BrokerState::Unfenced, BrokerState::ControlledShutdown) => {
                let mut records = Self::movable_leaderships(image, heartbeat.broker_id);
                records.push(registration_with(registration, false, true));
                records
            }
            (BrokerState::ControlledShutdown, BrokerState::ControlledShutdown) => {
                Self::movable_leaderships(image, heartbeat.broker_id)
            }
            _ => Vec::new(),
        };
        Ok(HeartbeatOutcome {
            records,
            is_caught_up: caught_up,
            is_fenced: matches!(next, BrokerState::Fenced | BrokerState::ShutdownNow),
            should_shut_down: next == BrokerState::ShutdownNow,
        })
    }

    /// Fence every unfenced broker whose last heartbeat is older than the
    /// session timeout. The active controller calls it on a timer. A broker
    /// this controller has not heard from at all gets a full session from
    /// now.
    pub fn expire_sessions(&mut self, image: &MetadataImage, now: Millis) -> Vec<MetadataRecord> {
        let mut scratch = image.clone();
        let mut records = Vec::new();
        for broker in unfenced_brokers(image) {
            let last = *self.last_heartbeat.entry(broker).or_insert(now);
            if now.saturating_sub(last) < self.config.broker_session_timeout_ms {
                continue;
            }
            let fence = self.fence_records(&scratch, broker);
            for record in &fence {
                scratch.apply(record);
            }
            records.extend(fence);
        }
        records
    }

    /// Decide a `CreateTopics` request, one result per topic in request
    /// order. A name that appears twice is refused on every row it has, as
    /// Kafka refuses it, and a topic sees the topics before it in the same
    /// request.
    pub fn create_topics(
        &mut self,
        image: &MetadataImage,
        specs: &[CreateTopicSpec],
    ) -> Vec<Result<CreatedTopic, TopicError>> {
        let mut seen = BTreeSet::new();
        let duplicates: BTreeSet<&str> = specs
            .iter()
            .map(|spec| spec.name.as_str())
            .filter(|name| !seen.insert(*name))
            .collect();
        let mut scratch = image.clone();
        specs
            .iter()
            .map(|spec| {
                if duplicates.contains(spec.name.as_str()) {
                    return Err(TopicError::new(
                        codes::INVALID_REQUEST,
                        "Duplicate topic name.",
                    ));
                }
                let created = self.create_topic(&scratch, spec)?;
                for record in &created.records {
                    scratch.apply(record);
                }
                Ok(created)
            })
            .collect()
    }

    /// Decide one topic, in the order of Kafka's
    /// `ReplicationControlManager.createTopic`: the name, then existence,
    /// then the counts and the placement.
    fn create_topic(
        &mut self,
        image: &MetadataImage,
        spec: &CreateTopicSpec,
    ) -> Result<CreatedTopic, TopicError> {
        if let Some(message) = topic_name_error(image, &spec.name) {
            return Err(TopicError::new(codes::INVALID_TOPIC_EXCEPTION, message));
        }
        if image.topic(&spec.name).is_some() {
            return Err(TopicError::new(
                codes::TOPIC_ALREADY_EXISTS,
                format!("Topic '{}' already exists.", spec.name),
            ));
        }
        let placement = if spec.assignments.is_empty() {
            if spec.replication_factor < -1 || spec.replication_factor == 0 {
                return Err(TopicError::new(
                    codes::INVALID_REPLICATION_FACTOR,
                    "Replication factor must be larger than 0, or -1 to use the default value.",
                ));
            }
            if spec.partitions < -1 || spec.partitions == 0 {
                return Err(TopicError::new(
                    codes::INVALID_PARTITIONS,
                    "Number of partitions was set to an invalid non-positive value.",
                ));
            }
            let partitions = if spec.partitions == -1 {
                self.config.default_partitions
            } else {
                spec.partitions
            };
            let replication_factor = if spec.replication_factor == -1 {
                self.config.default_replication_factor
            } else {
                spec.replication_factor
            };
            let brokers = unfenced_brokers(image);
            let start = self.placement_cursor;
            self.placement_cursor = self.placement_cursor.wrapping_add(1);
            let assignments = stripe(&brokers, start, 0, partitions, replication_factor)
                .ok_or_else(|| {
                    TopicError::new(
                        codes::INVALID_REPLICATION_FACTOR,
                        placement_failure_message(replication_factor, brokers.len()),
                    )
                })?;
            assignments
                .into_iter()
                .map(|replicas| Placement {
                    leader: replicas[0],
                    isr: replicas.clone(),
                    replicas,
                })
                .collect::<Vec<_>>()
        } else {
            if spec.replication_factor != -1 {
                return Err(TopicError::new(
                    codes::INVALID_REQUEST,
                    "A manual partition assignment was specified, but replication factor was \
                     not set to -1.",
                ));
            }
            if spec.partitions != -1 {
                return Err(TopicError::new(
                    codes::INVALID_REQUEST,
                    "A manual partition assignment was specified, but numPartitions was not set \
                     to -1.",
                ));
            }
            manual_placement(image, &spec.assignments, 0, None)
                .map_err(|message| TopicError::new(codes::INVALID_REPLICA_ASSIGNMENT, message))?
        };
        let topic_id = self.next_topic_id();
        let partitions = i32::try_from(placement.len()).unwrap_or(i32::MAX);
        let replication_factor = placement
            .first()
            .and_then(|placement| i16::try_from(placement.replicas.len()).ok())
            .unwrap_or(-1);
        let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
            name: spec.name.clone(),
            topic_id,
            partitions,
            replication_factor,
        })];
        records.extend(partition_records(&spec.name, 0, &placement));
        if !spec.configs.is_empty() {
            records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: spec.name.clone(),
                overrides: spec.configs.clone(),
            }));
        }
        Ok(CreatedTopic {
            name: spec.name.clone(),
            topic_id,
            partitions,
            replication_factor,
            records,
        })
    }

    /// Decide a `DeleteTopics` request, one result per name in request
    /// order.
    #[must_use]
    pub fn delete_topics(
        &self,
        image: &MetadataImage,
        names: &[String],
    ) -> Vec<Result<Vec<MetadataRecord>, TopicError>> {
        names
            .iter()
            .map(|name| {
                if image.topic(name).is_none() {
                    return Err(TopicError::new(
                        codes::UNKNOWN_TOPIC_OR_PARTITION,
                        UNKNOWN_TOPIC_OR_PARTITION_MESSAGE,
                    ));
                }
                Ok(vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: name.clone(),
                })])
            })
            .collect()
    }

    /// Decide a `CreatePartitions` row: grow `topic` to `count` partitions.
    /// The new partitions continue the topic's stripe, or take the caller's
    /// `assignments`, one per new partition.
    ///
    /// # Errors
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a topic that does not exist,
    /// `INVALID_PARTITIONS` for a count that is not an increase,
    /// `INVALID_REPLICATION_FACTOR` when the stripe cannot be placed, and
    /// `INVALID_REPLICA_ASSIGNMENT` for a manual assignment Kafka refuses.
    pub fn create_partitions(
        &mut self,
        image: &MetadataImage,
        topic: &str,
        count: i32,
        assignments: Option<&[Vec<NodeId>]>,
    ) -> Result<Vec<MetadataRecord>, TopicError> {
        if image.topic(topic).is_none() {
            return Err(TopicError::new(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                UNKNOWN_TOPIC_OR_PARTITION_MESSAGE,
            ));
        }
        let existing = image.topic_partition_count(topic);
        if count == existing {
            return Err(TopicError::new(
                codes::INVALID_PARTITIONS,
                format!("Topic already has {existing} partition(s)."),
            ));
        }
        if count < existing {
            return Err(TopicError::new(
                codes::INVALID_PARTITIONS,
                format!(
                    "The topic {topic} currently has {existing} partition(s); {count} would \
                     not be an increase."
                ),
            ));
        }
        let new_partitions = count - existing;
        let replication_factor = image
            .partition(topic, 0)
            .and_then(|partition| i16::try_from(partition.replicas.len()).ok())
            .unwrap_or(self.config.default_replication_factor);
        let placement = match assignments {
            None => {
                let brokers = unfenced_brokers(image);
                // The stripe continues from where partition 0 started, so the
                // new partitions follow the pattern of the old ones.
                let start = image
                    .partition(topic, 0)
                    .and_then(|partition| lab_id(partition.leader))
                    .and_then(|leader| brokers.iter().position(|&broker| broker == leader))
                    .unwrap_or(0);
                let assignments = stripe(
                    &brokers,
                    start,
                    usize::try_from(existing).unwrap_or(0),
                    new_partitions,
                    replication_factor,
                )
                .ok_or_else(|| {
                    TopicError::new(
                        codes::INVALID_REPLICATION_FACTOR,
                        placement_failure_message(replication_factor, brokers.len()),
                    )
                })?;
                assignments
                    .into_iter()
                    .map(|replicas| Placement {
                        leader: replicas[0],
                        isr: replicas.clone(),
                        replicas,
                    })
                    .collect::<Vec<_>>()
            }
            Some(assignments) => {
                if assignments.len() != usize::try_from(new_partitions).unwrap_or(usize::MAX) {
                    return Err(TopicError::new(
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!(
                            "Attempted to add {new_partitions} partitions, but only {} \
                             assignments were specified.",
                            assignments.len()
                        ),
                    ));
                }
                manual_placement(
                    image,
                    assignments,
                    existing,
                    Some(usize::try_from(replication_factor).unwrap_or(0)),
                )
                .map_err(|message| TopicError::new(codes::INVALID_REPLICA_ASSIGNMENT, message))?
            }
        };
        Ok(partition_records(topic, existing, &placement))
    }

    /// Decide one `AlterPartition` row, in the order of Kafka's
    /// `ReplicationControlManager.validateAlterPartitionData`. An admitted
    /// row bumps the partition epoch and keeps the leader and the leader
    /// epoch.
    ///
    /// # Errors
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a partition that does not exist;
    /// `NOT_CONTROLLER` for an epoch the controller has not seen;
    /// `FENCED_LEADER_EPOCH` for an older leader epoch; `INVALID_REQUEST` when
    /// the sender is not the leader, or the ISR is not a set of assigned
    /// replicas that holds the leader; `INVALID_UPDATE_VERSION` for an older
    /// partition epoch; `INELIGIBLE_REPLICA` for a member that is not
    /// registered, is fenced, is shutting down, or registered at another
    /// epoch than the request names.
    pub fn alter_partition(
        &self,
        image: &MetadataImage,
        request: &AlterPartition,
    ) -> Result<AlteredPartition, i16> {
        let Some(partition) = image.partition(&request.topic, request.partition) else {
            return Err(codes::UNKNOWN_TOPIC_OR_PARTITION);
        };
        let leader_epoch = partition.leader_epoch.get();
        if request.leader_epoch > leader_epoch
            || request.partition_epoch > partition.partition_epoch
        {
            return Err(codes::NOT_CONTROLLER);
        }
        if request.leader_epoch < leader_epoch {
            return Err(codes::FENCED_LEADER_EPOCH);
        }
        if broker_id(request.broker_id) != partition.leader {
            return Err(codes::INVALID_REQUEST);
        }
        if request.partition_epoch < partition.partition_epoch {
            return Err(codes::INVALID_UPDATE_VERSION);
        }
        let mut seen = BTreeSet::new();
        let proposed: Vec<krabka_metadata::NodeId> = request
            .new_isr
            .iter()
            .map(|member| broker_id(member.broker))
            .collect();
        let valid_set = proposed
            .iter()
            .all(|member| partition.replicas.contains(member) && seen.insert(*member));
        if !valid_set || !proposed.contains(&partition.leader) {
            return Err(codes::INVALID_REQUEST);
        }
        let active = active_brokers(image);
        let eligible = request.new_isr.iter().all(|member| {
            let id = broker_id(member.broker);
            active.contains(&id)
                && (member.broker_epoch == -1
                    || image.broker_epoch(id) == Some(member.broker_epoch))
        });
        if !eligible {
            return Err(codes::INELIGIBLE_REPLICA);
        }
        let partition_epoch = partition
            .partition_epoch
            .checked_add(1)
            .ok_or(codes::INVALID_REQUEST)?;
        let record = PartitionRecord {
            isr: proposed,
            partition_epoch,
            ..partition.clone()
        };
        Ok(AlteredPartition {
            leader: request.broker_id,
            leader_epoch,
            isr: request.new_isr.iter().map(|member| member.broker).collect(),
            partition_epoch,
            records: vec![MetadataRecord::V1Partition(record)],
        })
    }

    /// The partition changes a fenced or unregistered broker forces, as
    /// Kafka's `handleBrokerFenced` generates them: `fenced` leaves every ISR
    /// it is in, and every partition it led elects the first replica, in
    /// assignment order, that stays in the ISR and is active. With no such
    /// replica the partition elects the first active replica when the topic
    /// enables unclean leader election, and goes offline with
    /// [`NO_LEADER`] otherwise. An election bumps the leader epoch and the
    /// partition epoch; an ISR shrink bumps the partition epoch alone.
    #[must_use]
    pub fn elect_leaders_after_fence(
        &self,
        image: &MetadataImage,
        fenced: NodeId,
    ) -> Vec<MetadataRecord> {
        let fenced = broker_id(fenced);
        let mut active = active_brokers(image);
        active.remove(&fenced);
        let mut records = Vec::new();
        for partition in sorted_partitions(image) {
            if partition.leader != fenced && !partition.isr.contains(&fenced) {
                continue;
            }
            let target_isr: Vec<krabka_metadata::NodeId> = partition
                .isr
                .iter()
                .copied()
                .filter(|replica| *replica != fenced)
                .collect();
            let leader_stays = partition.leader != fenced
                && target_isr.contains(&partition.leader)
                && active.contains(&partition.leader);
            if leader_stays {
                if target_isr != partition.isr {
                    records.push(MetadataRecord::V1Partition(PartitionRecord {
                        isr: target_isr,
                        partition_epoch: partition.partition_epoch.saturating_add(1),
                        ..partition.clone()
                    }));
                }
                continue;
            }
            let clean = partition
                .replicas
                .iter()
                .copied()
                .find(|replica| target_isr.contains(replica) && active.contains(replica));
            let unclean = || {
                self.unclean_election_enabled(image, &partition.topic)
                    .then(|| {
                        partition
                            .replicas
                            .iter()
                            .copied()
                            .find(|replica| active.contains(replica))
                    })
                    .flatten()
            };
            let (leader, isr) = if let Some(leader) = clean {
                (leader, target_isr)
            } else if let Some(leader) = unclean() {
                (leader, vec![leader])
            } else {
                (NO_LEADER, target_isr)
            };
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                leader,
                isr,
                leader_epoch: partition.leader_epoch.next(),
                partition_epoch: partition.partition_epoch.saturating_add(1),
                ..partition.clone()
            }));
        }
        records
    }

    /// Decide an `UnregisterBroker`: the broker leaves every ISR and every
    /// leadership, and its registration is dropped.
    ///
    /// # Errors
    /// `BROKER_ID_NOT_REGISTERED` for a broker the image does not hold.
    pub fn unregister_broker(
        &mut self,
        image: &MetadataImage,
        broker: NodeId,
    ) -> Result<Vec<MetadataRecord>, i16> {
        if image.broker(broker_id(broker)).is_none() {
            return Err(codes::BROKER_ID_NOT_REGISTERED);
        }
        let mut records = self.elect_leaders_after_fence(image, broker);
        records.push(MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
            node_id: broker_id(broker),
        }));
        self.last_heartbeat.remove(&broker);
        Ok(records)
    }

    /// The spec of the group coordinator's topic: `offsets.topic.num.partitions`
    /// compacted partitions, replicated `offsets.topic.replication.factor`
    /// times or as many as there are unfenced brokers.
    #[must_use]
    pub fn consumer_offsets_spec(&self, image: &MetadataImage) -> CreateTopicSpec {
        let brokers = i16::try_from(unfenced_brokers(image).len()).unwrap_or(i16::MAX);
        let mut spec = CreateTopicSpec::new(
            CONSUMER_OFFSETS_TOPIC,
            self.config.offsets_topic_partitions,
            self.config
                .offsets_topic_replication_factor
                .min(brokers)
                .max(1),
        );
        spec.configs
            .insert("cleanup.policy".to_string(), "compact".to_string());
        spec.configs
            .insert("segment.bytes".to_string(), "104857600".to_string());
        spec
    }

    /// Whether `broker`'s previous incarnation may still be heartbeating.
    fn session_valid(&self, broker: NodeId, now: Millis) -> bool {
        self.last_heartbeat
            .get(&broker)
            .is_some_and(|&last| now.saturating_sub(last) < self.config.broker_session_timeout_ms)
    }

    /// The records that fence `broker`: its partition changes and its
    /// registration with the fence set.
    fn fence_records(&self, image: &MetadataImage, broker: NodeId) -> Vec<MetadataRecord> {
        let mut records = self.elect_leaders_after_fence(image, broker);
        if let Some(registration) = image.broker(broker_id(broker)) {
            records.push(registration_with(registration, true, false));
        }
        records
    }

    /// The leaderships `broker` can hand to another active ISR member, as the
    /// partition records that do so. Kafka's controlled shutdown keeps the
    /// broker in the ISR and moves the leader alone.
    fn movable_leaderships(image: &MetadataImage, broker: NodeId) -> Vec<MetadataRecord> {
        let broker = broker_id(broker);
        let active = active_brokers(image);
        sorted_partitions(image)
            .into_iter()
            .filter(|partition| partition.leader == broker)
            .filter_map(|partition| {
                let successor = partition.replicas.iter().copied().find(|replica| {
                    *replica != broker
                        && partition.isr.contains(replica)
                        && active.contains(replica)
                })?;
                Some(MetadataRecord::V1Partition(PartitionRecord {
                    leader: successor,
                    leader_epoch: partition.leader_epoch.next(),
                    partition_epoch: partition.partition_epoch.saturating_add(1),
                    ..partition.clone()
                }))
            })
            .collect()
    }

    /// Kafka's `handleBrokerUnfenced`: every partition without a leader
    /// elects the first ISR replica that is active now that `unfenced` is.
    fn elect_offline_partitions(image: &MetadataImage, unfenced: NodeId) -> Vec<MetadataRecord> {
        let mut active = active_brokers(image);
        active.insert(broker_id(unfenced));
        sorted_partitions(image)
            .into_iter()
            .filter(|partition| partition.leader == NO_LEADER)
            .filter_map(|partition| {
                let leader =
                    partition.replicas.iter().copied().find(|replica| {
                        partition.isr.contains(replica) && active.contains(replica)
                    })?;
                Some(MetadataRecord::V1Partition(PartitionRecord {
                    leader,
                    leader_epoch: partition.leader_epoch.next(),
                    partition_epoch: partition.partition_epoch.saturating_add(1),
                    ..partition.clone()
                }))
            })
            .collect()
    }

    fn unclean_election_enabled(&self, image: &MetadataImage, topic: &str) -> bool {
        image
            .topic_config(topic)
            .and_then(|configs| configs.get("unclean.leader.election.enable"))
            .map_or(self.config.unclean_leader_election_enable, |value| {
                value.trim().eq_ignore_ascii_case("true")
            })
    }

    /// A topic id no other topic has: never nil, never the metadata topic's,
    /// and never one whose base64 form starts with `-`, which Kafka avoids so
    /// an id is never mistaken for a command-line flag.
    fn next_topic_id(&mut self) -> Uuid {
        loop {
            let id = Uuid::from_u64_pair(self.rng.next_u64(), self.rng.next_u64());
            let starts_with_dash = (id.as_u128() >> 122) == 62;
            if !id.is_nil() && id != Uuid::from_u128(1) && !starts_with_dash {
                return id;
            }
        }
    }
}

/// The leader, ISR and replicas a new partition starts with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Placement {
    leader: NodeId,
    isr: Vec<NodeId>,
    replicas: Vec<NodeId>,
}

/// Round-robin replica placement: partition `p`, counted from
/// `first_partition`, puts its first replica on `brokers[(start + p) % n]`
/// and the others on the brokers after it. `None` when the replication
/// factor cannot be met.
fn stripe(
    brokers: &[NodeId],
    start: usize,
    first_partition: usize,
    partitions: i32,
    replication_factor: i16,
) -> Option<Vec<Vec<NodeId>>> {
    let n = brokers.len();
    let rf = usize::try_from(replication_factor).unwrap_or(0);
    if rf == 0 || rf > n {
        return None;
    }
    let count = usize::try_from(partitions).unwrap_or(0);
    Some(
        (first_partition..first_partition + count)
            .map(|p| (0..rf).map(|i| brokers[(start + p + i) % n]).collect())
            .collect(),
    )
}

/// Kafka's `validateManualPartitionAssignment` and `buildPartitionRegistration`
/// over a caller's assignment: every replica registered, none twice, every
/// partition with the same count, and the ISR made of the active replicas in
/// the listed order with the first of them as leader.
fn manual_placement(
    image: &MetadataImage,
    assignments: &[Vec<NodeId>],
    first_partition: i32,
    replication_factor: Option<usize>,
) -> Result<Vec<Placement>, String> {
    let registered = registered_brokers(image);
    let active = active_brokers(image);
    let mut expected = replication_factor;
    let mut placements = Vec::with_capacity(assignments.len());
    for (index, replicas) in assignments.iter().enumerate() {
        let partition = first_partition.saturating_add(i32::try_from(index).unwrap_or(i32::MAX));
        if replicas.is_empty() {
            return Err("The manual partition assignment includes an empty replica list.".into());
        }
        let mut sorted = replicas.clone();
        sorted.sort_unstable();
        let mut previous = None;
        for &replica in &sorted {
            if !registered.contains(&replica) {
                return Err(format!(
                    "The manual partition assignment includes broker {replica}, but no such \
                     broker is registered."
                ));
            }
            if previous == Some(replica) {
                return Err(format!(
                    "The manual partition assignment includes the broker {replica} more than \
                     once."
                ));
            }
            previous = Some(replica);
        }
        if let Some(expected) = expected
            && replicas.len() != expected
        {
            return Err(format!(
                "The manual partition assignment includes a partition with {} replica(s), but \
                 this is not consistent with previous partitions, which have {expected} \
                 replica(s).",
                replicas.len()
            ));
        }
        expected = Some(replicas.len());
        let isr: Vec<NodeId> = replicas
            .iter()
            .copied()
            .filter(|replica| active.contains(&broker_id(*replica)))
            .collect();
        let Some(&leader) = isr.first() else {
            return Err(format!(
                "All brokers specified in the manual partition assignment for partition \
                 {partition} are fenced or in controlled shutdown."
            ));
        };
        placements.push(Placement {
            leader,
            isr,
            replicas: replicas.clone(),
        });
    }
    Ok(placements)
}

/// One partition record per placement, numbered from `first_partition`, at
/// leader epoch 0 and partition epoch 0.
fn partition_records(
    topic: &str,
    first_partition: i32,
    placement: &[Placement],
) -> Vec<MetadataRecord> {
    placement
        .iter()
        .enumerate()
        .map(|(index, placement)| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_string(),
                partition: first_partition.saturating_add(i32::try_from(index).unwrap_or(i32::MAX)),
                leader: broker_id(placement.leader),
                replicas: placement.replicas.iter().copied().map(broker_id).collect(),
                isr: placement.isr.iter().copied().map(broker_id).collect(),
                leader_epoch: LeaderEpoch::INITIAL,
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: Vec::new(),
                partition_epoch: 0,
            })
        })
        .collect()
}

/// The `INVALID_REPLICATION_FACTOR` message of a placement that cannot put
/// `replication_factor` replicas on `usable` brokers, as Kafka wraps the
/// `StripedReplicaPlacer` refusal.
fn placement_failure_message(replication_factor: i16, usable: usize) -> String {
    let reason = if usable == 0 {
        "All brokers are currently fenced, or have all their log directories cordoned.".to_string()
    } else {
        format!(
            "The target replication factor of {replication_factor} cannot be reached because \
             only {usable} broker(s) are registered or some brokers have all their log \
             directories cordoned."
        )
    };
    format!("Unable to replicate the partition {replication_factor} time(s): {reason}")
}

/// Kafka's `Topic.validate`, plus the `.`/`_` collision check of
/// `ReplicationControlManager.createTopic`: the message of the refusal, if
/// there is one.
fn topic_name_error(image: &MetadataImage, name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("Topic name is illegal, it can't be empty".into());
    }
    if name == "." || name == ".." {
        return Some("Topic name cannot be \".\" or \"..\"".into());
    }
    if name.len() > MAX_TOPIC_NAME_LENGTH {
        return Some(format!(
            "Topic name is illegal, it can't be longer than {MAX_TOPIC_NAME_LENGTH} characters, \
             topic name: {name}"
        ));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_' || byte == b'-')
    {
        return Some(format!(
            "Topic name \"{name}\" is illegal, it contains a character other than ASCII \
             alphanumerics, '.', '_' and '-'"
        ));
    }
    let collides: Vec<&str> = {
        let mut names: Vec<&str> = image
            .topics()
            .map(|topic| topic.name.as_str())
            .filter(|existing| {
                *existing != name && existing.replace('.', "_") == name.replace('.', "_")
            })
            .collect();
        names.sort_unstable();
        names
    };
    if !collides.is_empty() {
        return Some(format!(
            "Topic '{name}' collides with existing topics: {}",
            collides.join(", ")
        ));
    }
    None
}

/// `registration` with its fence and controlled-shutdown flags set.
fn registration_with(
    registration: &BrokerRegistrationRecord,
    fenced: bool,
    in_controlled_shutdown: bool,
) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced,
        in_controlled_shutdown,
        ..registration.clone()
    })
}

/// Every partition of the image, by topic name and partition index, so the
/// records a decision emits are in the same order on every controller.
fn sorted_partitions(image: &MetadataImage) -> Vec<&PartitionRecord> {
    let mut partitions: Vec<&PartitionRecord> = image.all_partitions().collect();
    partitions.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    partitions
}

/// The brokers Kafka's `ClusterControlManager.isActive` admits: registered,
/// not fenced, and not in controlled shutdown.
fn active_brokers(image: &MetadataImage) -> BTreeSet<krabka_metadata::NodeId> {
    image
        .brokers()
        .filter(|broker| !broker.fenced && !broker.in_controlled_shutdown)
        .map(|broker| broker.node_id)
        .collect()
}

/// Every registered broker, in ascending id order.
#[must_use]
pub fn registered_brokers(image: &MetadataImage) -> Vec<NodeId> {
    let mut brokers: Vec<NodeId> = image
        .brokers()
        .filter_map(|broker| lab_id(broker.node_id))
        .collect();
    brokers.sort_unstable();
    brokers
}

/// The brokers a partition can be placed on: registered, not fenced and not
/// in controlled shutdown, in ascending id order.
#[must_use]
pub fn unfenced_brokers(image: &MetadataImage) -> Vec<NodeId> {
    let mut brokers: Vec<NodeId> = active_brokers(image)
        .into_iter()
        .filter_map(lab_id)
        .collect();
    brokers.sort_unstable();
    brokers
}
