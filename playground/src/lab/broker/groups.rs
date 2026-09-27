//! The group coordinator inside the broker: which groups this broker
//! coordinates, the `__consumer_offsets` partitions it loads, the
//! coordinator's timer, and the writes its answers wait for.
//!
//! A broker coordinates the groups of the `__consumer_offsets` partitions it
//! leads. When it becomes the leader of one, it replays the partition's
//! records into the [`Coordinator`] (Kafka's `onElection` and load); when it
//! stops leading one, it unloads the partition's groups, and their held
//! requests answer `NOT_COORDINATOR` (Kafka's `onResignation`). A group
//! request for a group of another partition answers `NOT_COORDINATOR`
//! before the coordinator sees it.
//!
//! Every record the coordinator writes is appended to the group's
//! partition as the leader's own batch, so it replicates like any record.
//! An answer that follows a write waits until the partition's high
//! watermark covers everything appended to it, as Kafka's
//! `CoordinatorRuntime` completes a write once it is committed, and a call
//! that wrote nothing waits for the writes before it. The held `JoinGroup`
//! and `SyncGroup` answers a call completes wait the same way. A write whose
//! partition this broker stops leading answers `NOT_COORDINATOR`, and one
//! still short after `offsets.commit.timeout.ms` answers
//! `COORDINATOR_NOT_AVAILABLE`, Kafka's mapping of its `REQUEST_TIMED_OUT`.
//! Reads (`OffsetFetch`, the describes, `ListGroups`) answer at once, from
//! the coordinator's latest state.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Bytes, BytesMut};
use krabka_metadata::MetadataImage;
use krabka_protocol::{
    Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig},
        join_group_response::JoinGroupResponse,
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use regex::Regex;
use serde_json::{Value, json};

use super::{
    BrokerNode, TopicPartition,
    coordinator::{
        AnyResponse, Completion, Coordinator, CoordinatorConfig, HoldToken, InternalTopicToCreate,
        MemberKey, OFFSETS_TOPIC, RecordKey, TopicMetadata, group_partition,
    },
    dispatch::{DispatchError, HoldReason, Outcome, Reply, RequestCtx, Step, encode_reply},
    handlers::{uuid_of, wire_uuid},
};
use crate::lab::{
    codes,
    net::{Ctx, Millis, node_ip},
};

/// Kafka's `offsets.commit.timeout.ms` default: how long a coordinator write
/// waits for its commit.
pub const DEFAULT_OFFSETS_COMMIT_TIMEOUT_MS: Millis = 5_000;

/// Kafka's `Errors.NOT_COORDINATOR.message()`.
pub const NOT_COORDINATOR_MESSAGE: &str = "This is not the correct coordinator.";

/// What an answer of the coordinator waits for: the high watermark of one
/// `__consumer_offsets` partition this broker leads, at the leader epoch it
/// led it at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupWait {
    partition: i32,
    leader_epoch: i32,
    offset: i64,
}

/// Where a set of waits stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaitState {
    Committed,
    Waiting,
    /// The broker stopped leading a partition the answer waits on.
    Lost,
}

/// A held `JoinGroup` or `SyncGroup` the coordinator answered, and the
/// writes the answer waits for.
#[derive(Clone, Debug, PartialEq, Eq)]
struct HeldAnswer {
    response: AnyResponse,
    waits: Vec<GroupWait>,
    deadline: Millis,
}

/// A coordinator answer waiting for its writes to commit.
#[derive(Debug)]
pub struct PendingGroupWrite {
    /// The answer once the writes committed.
    reply: Reply,
    /// The answer when the broker stopped leading the group's partition.
    not_coordinator: Reply,
    /// The answer when the writes did not commit in time.
    timed_out: Reply,
    waits: Vec<GroupWait>,
    /// When the wait gives up.
    pub deadline: Millis,
}

/// The coordinator state of a broker.
pub struct Groups {
    /// The coordinator of the loaded partitions' groups.
    pub coordinator: Coordinator,
    /// The `__consumer_offsets` partitions this broker leads and loaded.
    loaded: BTreeSet<i32>,
    /// The answers of held requests, by token, until their connection takes
    /// them.
    held: BTreeMap<HoldToken, HeldAnswer>,
}

impl Groups {
    /// A coordinator with nothing loaded.
    #[must_use]
    pub fn new(broker_id: i32, config: CoordinatorConfig) -> Self {
        Self {
            coordinator: Coordinator::new(broker_id, config),
            loaded: BTreeSet::new(),
            held: BTreeMap::new(),
        }
    }

    /// Whether this broker coordinates `group_id`: it leads and loaded the
    /// group's `__consumer_offsets` partition.
    #[must_use]
    pub fn coordinates(&self, group_id: &str) -> bool {
        self.loaded.contains(&group_partition(group_id))
    }

    /// The coordinator for the inspector, with the loaded partitions.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let mut snapshot = self.coordinator.snapshot();
        snapshot["loaded_partitions"] = json!(self.loaded);
        snapshot
    }
}

/// [`TopicMetadata`] over a metadata image, as it is at the call.
pub struct ImageTopics<'a> {
    /// The image.
    pub image: &'a MetadataImage,
}

impl TopicMetadata for ImageTopics<'_> {
    fn partitions(&self, topic: &str) -> Option<i32> {
        self.image
            .topic(topic)
            .map(|_| self.image.topic_partition_count(topic))
    }

    fn topic_id(&self, topic: &str) -> Option<WireUuid> {
        self.image.topic(topic).map(|t| wire_uuid(t.topic_id))
    }

    fn topic_name(&self, id: WireUuid) -> Option<String> {
        self.image.topic_name_by_id(&uuid_of(id)).map(str::to_owned)
    }

    /// The topics a pattern matches as a whole, as RE2J's `matches` does. A
    /// pattern the `regex` crate cannot compile matches nothing.
    fn topics_matching(&self, regex: &str) -> Vec<String> {
        let Ok(pattern) = Regex::new(&format!("^(?:{regex})$")) else {
            return Vec::new();
        };
        let mut names: Vec<String> = self
            .image
            .topics()
            .map(|t| t.name.clone())
            .filter(|name| pattern.is_match(name))
            .collect();
        names.sort();
        names
    }
}

/// The client behind a request, as the coordinator records it: the
/// header's client id and the peer address as Kafka prints it.
#[must_use]
pub fn member_key(req: &RequestCtx) -> MemberKey {
    MemberKey {
        client_id: req.client_id.clone().unwrap_or_default(),
        client_host: format!("/{}", node_ip(req.conn.0.node)),
    }
}

/// A batch of coordinator records, as the leader appends it.
fn record_batch(records: &[(Bytes, Option<Bytes>)], now: Millis) -> Option<Bytes> {
    let timestamp = i64::try_from(now).unwrap_or(i64::MAX);
    let batch = RecordBatch {
        base_timestamp: timestamp,
        max_timestamp: timestamp,
        last_offset_delta: i32::try_from(records.len()).ok()?.checked_sub(1)?,
        records: records
            .iter()
            .enumerate()
            .map(|(index, (key, value))| Record {
                offset_delta: i32::try_from(index).unwrap_or(i32::MAX),
                key: Some(key.clone()),
                value: value.clone(),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    };
    let mut buf = BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).ok()?;
    Some(buf.freeze())
}

/// The answer to a held request whose writes were lost or timed out, as
/// Kafka's runtime fails the response future: `error_code` on an
/// otherwise default response.
fn failed_answer(response: &AnyResponse, error_code: i16) -> AnyResponse {
    match response {
        AnyResponse::JoinGroup(_) => AnyResponse::JoinGroup(JoinGroupResponse {
            error_code,
            ..JoinGroupResponse::default()
        }),
        AnyResponse::SyncGroup(_) => AnyResponse::SyncGroup(SyncGroupResponse {
            error_code,
            ..SyncGroupResponse::default()
        }),
    }
}

impl BrokerNode {
    /// The coordinator settings, from the broker's config.
    pub(super) fn coordinator_config(&self) -> CoordinatorConfig {
        self.config.coordinator.clone()
    }

    /// Take the records and the completions of the call the coordinator
    /// just served: append the records to their partitions, and keep each
    /// completion until its connection takes it, waiting for the same
    /// writes. `partitions` are the partitions of the groups the call named;
    /// the answer waits for everything appended to them and to the
    /// partitions the records went to. Returns the waits of the call's own
    /// answer.
    pub(super) fn after_coordinator_call(
        &mut self,
        ctx: &mut Ctx<'_>,
        partitions: &[i32],
    ) -> Vec<GroupWait> {
        let now = ctx.now();
        let records = self.groups.coordinator.drain_records();
        let mut by_partition: BTreeMap<i32, Vec<(Bytes, Option<Bytes>)>> = BTreeMap::new();
        for (key, value) in records {
            let Some(partition) = RecordKey::decode(&key).map(|k| k.partition()) else {
                continue;
            };
            by_partition
                .entry(partition)
                .or_default()
                .push((key, value));
        }
        let mut failed: BTreeSet<i32> = BTreeSet::new();
        for (partition, records) in &by_partition {
            let appended = record_batch(records, now).and_then(|batch| {
                self.append_local(ctx, OFFSETS_TOPIC, *partition, &batch)
                    .ok()
            });
            if appended.is_none() {
                failed.insert(*partition);
            }
        }
        let touched: BTreeSet<i32> = partitions
            .iter()
            .copied()
            .chain(by_partition.keys().copied())
            .collect();
        let waits: Vec<GroupWait> = touched
            .into_iter()
            .map(|partition| self.group_wait(partition, failed.contains(&partition)))
            .collect();
        let deadline = now + self.config.offsets_commit_timeout_ms;
        for Completion { token, response } in self.groups.coordinator.drain_completions() {
            self.groups.held.insert(
                token,
                HeldAnswer {
                    response,
                    waits: waits.clone(),
                    deadline,
                },
            );
        }
        waits
    }

    /// What an answer must wait for on `partition`: everything appended to
    /// it so far. A partition whose append failed, or that this broker does
    /// not lead, can never commit the answer: its wait names no leader
    /// epoch the partition can have.
    fn group_wait(&self, partition: i32, failed: bool) -> GroupWait {
        let me = self.config.broker_id;
        let replica = self
            .replicas
            .get(&TopicPartition::new(OFFSETS_TOPIC, partition))
            .filter(|r| r.leads(me) && !failed);
        match replica {
            Some(replica) => GroupWait {
                partition,
                leader_epoch: replica.leader_epoch,
                offset: replica.log.log_end_offset(),
            },
            None => GroupWait {
                partition,
                leader_epoch: -1,
                offset: i64::MAX,
            },
        }
    }

    /// Where `waits` stand.
    fn wait_state(&self, waits: &[GroupWait]) -> WaitState {
        let me = self.config.broker_id;
        let mut state = WaitState::Committed;
        for wait in waits {
            let replica = self
                .replicas
                .get(&TopicPartition::new(OFFSETS_TOPIC, wait.partition))
                .filter(|r| r.leads(me) && r.leader_epoch == wait.leader_epoch);
            match replica {
                None => return WaitState::Lost,
                Some(replica) if replica.log.high_watermark() < wait.offset => {
                    state = WaitState::Waiting;
                }
                Some(_) => {}
            }
        }
        state
    }

    /// Answer a coordinator call once `waits` commit: at once when they
    /// already have, else held. `not_coordinator` and `timed_out` are the
    /// api's answers with `NOT_COORDINATOR` and `COORDINATOR_NOT_AVAILABLE`.
    pub(super) fn group_answer<R: Encode + Clone>(
        &mut self,
        ctx: &mut Ctx<'_>,
        req: &RequestCtx,
        waits: Vec<GroupWait>,
        response: R,
        not_coordinator: &R,
        timed_out: &R,
    ) -> Outcome<R> {
        match self.wait_state(&waits) {
            WaitState::Committed => return Outcome::Reply(response),
            WaitState::Lost => return Outcome::Reply(not_coordinator.clone()),
            WaitState::Waiting => {}
        }
        match (
            encode_reply(&response, req.version),
            encode_reply(not_coordinator, req.version),
            encode_reply(timed_out, req.version),
        ) {
            (Ok(reply), Ok(not_coordinator), Ok(timed_out)) => {
                Outcome::Hold(HoldReason::GroupWrite(Box::new(PendingGroupWrite {
                    reply,
                    not_coordinator,
                    timed_out,
                    waits,
                    deadline: ctx.now() + self.config.offsets_commit_timeout_ms,
                })))
            }
            _ => Outcome::Close,
        }
    }

    /// Run a held coordinator write again: committed, lost, timed out, or
    /// waiting.
    pub(super) fn retry_group_write(&mut self, now: Millis, pending: PendingGroupWrite) -> Step {
        match self.wait_state(&pending.waits) {
            WaitState::Committed => Step::Reply(pending.reply),
            WaitState::Lost => Step::Reply(pending.not_coordinator),
            WaitState::Waiting if now >= pending.deadline => Step::Reply(pending.timed_out),
            WaitState::Waiting => Step::Hold(HoldReason::GroupWrite(Box::new(pending))),
        }
    }

    /// Run a held `JoinGroup` or `SyncGroup` again: answered and committed,
    /// failed, or waiting.
    ///
    /// # Errors
    /// Returns the codec error when the answer does not encode at the
    /// request's version.
    pub(super) fn retry_group_hold(
        &mut self,
        ctx: &mut Ctx<'_>,
        req: &RequestCtx,
        token: HoldToken,
    ) -> Result<Step, DispatchError> {
        let Some(held) = self.groups.held.remove(&token) else {
            return Ok(Step::Hold(HoldReason::Group { token }));
        };
        let response = match self.wait_state(&held.waits) {
            WaitState::Committed => held.response,
            WaitState::Lost => failed_answer(&held.response, codes::NOT_COORDINATOR),
            WaitState::Waiting if ctx.now() >= held.deadline => {
                failed_answer(&held.response, codes::COORDINATOR_NOT_AVAILABLE)
            }
            WaitState::Waiting => {
                self.groups.held.insert(token, held);
                return Ok(Step::Hold(HoldReason::Group { token }));
            }
        };
        let reply = match &response {
            AnyResponse::JoinGroup(join) => encode_reply(join, req.version)?,
            AnyResponse::SyncGroup(sync) => encode_reply(sync, req.version)?,
        };
        Ok(Step::Reply(reply))
    }

    /// Drop the answers no connection waits for any more.
    pub(super) fn prune_group_answers(&mut self) {
        let waiting: BTreeSet<HoldToken> = self
            .conns
            .values()
            .filter_map(
                |conn| match conn.queue.front().and_then(|r| r.held.as_ref()) {
                    Some(HoldReason::Group { token }) => Some(*token),
                    _ => None,
                },
            )
            .collect();
        self.groups.held.retain(|token, _| waiting.contains(token));
    }

    /// Load the `__consumer_offsets` partitions this broker leads and did
    /// not load, and unload the ones it no longer leads.
    pub(super) fn sync_coordinator(&mut self, ctx: &mut Ctx<'_>) {
        let me = self.config.broker_id;
        let now = ctx.now();
        let leading: BTreeSet<i32> = self
            .replicas
            .iter()
            .filter(|(key, replica)| key.topic == OFFSETS_TOPIC && replica.leads(me))
            .map(|(key, _)| key.partition)
            .collect();
        let gone: Vec<i32> = self.groups.loaded.difference(&leading).copied().collect();
        for partition in gone {
            self.groups.loaded.remove(&partition);
            let completions = self.groups.coordinator.unload(partition);
            for Completion { token, response } in completions {
                self.groups.held.insert(
                    token,
                    HeldAnswer {
                        response,
                        waits: Vec::new(),
                        deadline: now,
                    },
                );
            }
            ctx.event(
                "coordinator_unloaded",
                json!({ "partition": partition, "level": "info" }),
            );
        }
        let new: Vec<i32> = leading.difference(&self.groups.loaded).copied().collect();
        for partition in new {
            let key = TopicPartition::new(OFFSETS_TOPIC, partition);
            let records: Vec<(Bytes, Option<Bytes>)> = self
                .replicas
                .get(&key)
                .map(|replica| {
                    replica
                        .log
                        .batches()
                        .iter()
                        .flat_map(|stored| {
                            let mut cursor: &[u8] = &stored.bytes;
                            RecordBatch::decode(&mut cursor)
                                .map(|batch| batch.records)
                                .unwrap_or_default()
                        })
                        .filter_map(|record| record.key.map(|key| (key, record.value)))
                        .collect()
                })
                .unwrap_or_default();
            let count = records.len();
            self.groups.coordinator.load(now, records);
            self.groups.loaded.insert(partition);
            ctx.event(
                "coordinator_loaded",
                json!({ "partition": partition, "records": count, "level": "info" }),
            );
        }
    }

    /// Run the coordinator's deadlines that are due.
    pub(super) fn coordinator_tick(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self
            .groups
            .coordinator
            .next_deadline()
            .is_none_or(|at| at > now)
        {
            return;
        }
        let completions = self.groups.coordinator.on_tick(now);
        let waits = self.after_coordinator_call(ctx, &[]);
        let deadline = now + self.config.offsets_commit_timeout_ms;
        for Completion { token, response } in completions {
            self.groups.held.insert(
                token,
                HeldAnswer {
                    response,
                    waits: waits.clone(),
                    deadline,
                },
            );
        }
    }

    /// When the coordinator next needs the timer.
    pub(super) fn coordinator_deadline(&self) -> Option<Millis> {
        self.groups.coordinator.next_deadline()
    }

    /// Ask the controller for a streams group's missing internal topics.
    pub(super) fn create_internal_topics(
        &mut self,
        ctx: &mut Ctx<'_>,
        topics: Vec<InternalTopicToCreate>,
    ) {
        let topics: Vec<CreatableTopic> = topics
            .into_iter()
            .map(|topic| CreatableTopic {
                name: topic.name,
                num_partitions: topic.partitions,
                replication_factor: topic.replication_factor,
                configs: topic
                    .configs
                    .into_iter()
                    .map(|(name, value)| CreatableTopicConfig {
                        name,
                        value: Some(value),
                        ..CreatableTopicConfig::default()
                    })
                    .collect(),
                ..CreatableTopic::default()
            })
            .collect();
        self.create_topics_internally(ctx, topics);
    }
}
