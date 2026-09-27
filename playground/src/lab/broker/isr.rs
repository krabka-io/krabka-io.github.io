//! The leader's side of ISR changes: Kafka's `Partition.maybeShrinkIsr` and
//! `maybeExpandIsr` with their pending states, and its
//! `DefaultAlterPartitionManager`.
//!
//! A leader that decides to shrink or expand a partition's ISR puts the
//! replica in a pending state ([`PendingIsr`]) and queues the proposal, with
//! the broker epoch of every member (KIP-903). The queued proposals travel
//! together in one `AlterPartition` request on the `alter-partition`
//! channel, one request at a time; a proposal made while one is out waits
//! for the next. A partition that already has a proposal queued refuses a
//! second one, and the replica goes back to its committed state, as Kafka's
//! `OPERATION_NOT_ATTEMPTED` makes it. The answer decides each partition:
//!
//! - success: the replica takes the committed ISR and partition epoch
//!   unless its leader epoch moved meanwhile;
//! - `OPERATION_NOT_ATTEMPTED` or `INELIGIBLE_REPLICA`: the replica goes back
//!   to its committed state and may propose again;
//! - `UNKNOWN_TOPIC_OR_PARTITION`, `UNKNOWN_TOPIC_ID`, `FENCED_LEADER_EPOCH`,
//!   `INVALID_UPDATE_VERSION`, `INVALID_REQUEST` and `NEW_LEADER_ELECTED`: the
//!   replica stays pending until a committed record replaces its state;
//! - any other error: the proposal is sent again.
//!
//! A partition the answer leaves out is sent again, and an answer with a
//! top-level error sends everything again after 50 ms.
//!
//! [`PendingIsr`]: super::replica::PendingIsr

use std::collections::{BTreeMap, BTreeSet};

use krabka_protocol::owned::{
    alter_partition_request::{
        AlterPartitionRequest, BrokerState, PartitionData as RequestPartition,
        TopicData as RequestTopic,
    },
    alter_partition_response::AlterPartitionResponse,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    BrokerNode, TopicPartition,
    channel::{ChannelOutcome, ControllerRequest, ControllerResponse, Purpose},
    handlers::{uuid_of, wire_uuid},
    replica::PendingIsr,
};
use crate::lab::{
    codes,
    net::{Ctx, Millis},
};

/// How long a request that failed as a whole waits before it goes out again,
/// Kafka's `scheduleOnce("send-alter-partition", ..., 50)`.
pub const ALTER_PARTITION_RETRY_MS: Millis = 50;

/// Kafka's `Errors.NEW_LEADER_ELECTED`: the change committed, and a
/// reassignment it completed moved the leadership away.
pub const NEW_LEADER_ELECTED: i16 = 108;

/// One partition's proposal as it goes on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Proposal {
    topic_id: Uuid,
    leader_epoch: i32,
    partition_epoch: i32,
    /// The proposed ISR with the broker epoch of each member.
    new_isr: Vec<(i32, i64)>,
}

/// The leader's queue of ISR proposals.
#[derive(Debug, Default)]
pub struct IsrManager {
    /// The proposals not answered yet, sent or waiting to be.
    unsent: BTreeMap<TopicPartition, Proposal>,
    /// The partitions of the request on its way, while one is.
    in_flight: Option<BTreeSet<TopicPartition>>,
    /// No request goes out before this time.
    retry_at: Millis,
}

impl IsrManager {
    /// Forget every proposal, as a restart does.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// The manager for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "queued": self.unsent.keys().map(TopicPartition::label).collect::<Vec<_>>(),
            "in_flight": self.in_flight.is_some(),
        })
    }

    /// When a request that failed as a whole may go out again.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        (!self.unsent.is_empty() && self.in_flight.is_none() && self.retry_at > now)
            .then_some(self.retry_at)
    }
}

impl BrokerNode {
    /// Propose `new_isr` for a partition this broker leads with no change
    /// pending: `adding` names the follower an expansion adds. The replica
    /// turns pending, unless the partition already has a proposal queued.
    pub(super) fn propose_isr(
        &mut self,
        key: &TopicPartition,
        new_isr: Vec<i32>,
        adding: Option<i32>,
    ) {
        let me = self.config.broker_id;
        let own_epoch = self.lifecycle.broker_epoch;
        let Some(replica) = self.replicas.get_mut(key) else {
            return;
        };
        if !replica.leads(me) || replica.pending_isr.is_some() {
            return;
        }
        if self.isr.unsent.contains_key(key) {
            // Kafka's `submit` fails with `OperationNotAttemptedException`,
            // and the partition stays in its committed state.
            return;
        }
        let with_epochs = new_isr
            .iter()
            .map(|&id| {
                let epoch = if id == me {
                    own_epoch
                } else {
                    replica.followers.get(&id).map_or(-1, |f| f.broker_epoch)
                };
                (id, epoch)
            })
            .collect();
        self.isr.unsent.insert(
            key.clone(),
            Proposal {
                topic_id: replica.topic_id,
                leader_epoch: replica.leader_epoch,
                partition_epoch: replica.partition_epoch,
                new_isr: with_epochs,
            },
        );
        replica.pending_isr = Some(PendingIsr {
            proposed: new_isr,
            adding,
            leader_epoch: replica.leader_epoch,
            partition_epoch: replica.partition_epoch,
        });
    }

    /// Send the queued proposals when no request is out.
    pub(super) fn poll_isr(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self.isr.unsent.is_empty() || self.isr.in_flight.is_some() || now < self.isr.retry_at {
            return;
        }
        let mut topics: BTreeMap<Uuid, Vec<RequestPartition>> = BTreeMap::new();
        for (key, proposal) in &self.isr.unsent {
            topics
                .entry(proposal.topic_id)
                .or_default()
                .push(RequestPartition {
                    partition_index: key.partition,
                    leader_epoch: proposal.leader_epoch,
                    new_isr: proposal.new_isr.iter().map(|(id, _)| *id).collect(),
                    new_isr_with_epochs: proposal
                        .new_isr
                        .iter()
                        .map(|&(broker_id, broker_epoch)| BrokerState {
                            broker_id,
                            broker_epoch,
                            ..BrokerState::default()
                        })
                        .collect(),
                    leader_recovery_state: 0,
                    partition_epoch: proposal.partition_epoch,
                    ..RequestPartition::default()
                });
        }
        let request = AlterPartitionRequest {
            broker_id: self.config.broker_id,
            broker_epoch: self.lifecycle.broker_epoch,
            topics: topics
                .into_iter()
                .map(|(topic_id, partitions)| RequestTopic {
                    topic_id: wire_uuid(topic_id),
                    partitions,
                    ..RequestTopic::default()
                })
                .collect(),
            ..AlterPartitionRequest::default()
        };
        self.isr.in_flight = Some(self.isr.unsent.keys().cloned().collect());
        self.alter_partition_channel.enqueue(
            now,
            ControllerRequest::AlterPartition(request),
            Purpose::AlterPartition,
        );
    }

    /// The answer to the request on its way.
    pub(super) fn on_isr_outcome(&mut self, ctx: &mut Ctx<'_>, outcome: ChannelOutcome) {
        let now = ctx.now();
        let sent = self.isr.in_flight.take().unwrap_or_default();
        let response = match outcome {
            ChannelOutcome::Response(ControllerResponse::AlterPartition(response))
                if response.error_code == codes::NONE =>
            {
                response
            }
            ChannelOutcome::Response(ControllerResponse::AlterPartition(response)) => {
                ctx.event(
                    "alter_partition_failed",
                    json!({ "error_code": response.error_code, "level": "warn" }),
                );
                self.isr.retry_at = now + ALTER_PARTITION_RETRY_MS;
                return;
            }
            ChannelOutcome::Response(_)
            | ChannelOutcome::TimedOut
            | ChannelOutcome::VersionMismatch => {
                self.isr.retry_at = now + ALTER_PARTITION_RETRY_MS;
                return;
            }
        };
        for (key, row) in self.alter_partition_rows(&response) {
            if !sent.contains(&key) {
                continue;
            }
            let Some(proposal) = self.isr.unsent.remove(&key) else {
                continue;
            };
            self.on_isr_row(ctx, &key, proposal, row);
        }
    }

    /// The rows of an answer by partition; a topic id the image does not
    /// know is skipped, as Kafka's manager skips it.
    fn alter_partition_rows(
        &self,
        response: &AlterPartitionResponse,
    ) -> Vec<(TopicPartition, IsrAnswer)> {
        let mut rows = Vec::new();
        for topic in &response.topics {
            let Some(name) = self.image.topic_name_by_id(&uuid_of(topic.topic_id)) else {
                continue;
            };
            for row in &topic.partitions {
                let answer = if row.error_code == codes::NONE {
                    IsrAnswer::Committed {
                        leader_epoch: row.leader_epoch,
                        isr: row.isr.clone(),
                        partition_epoch: row.partition_epoch,
                    }
                } else {
                    IsrAnswer::Refused(row.error_code)
                };
                rows.push((TopicPartition::new(name, row.partition_index), answer));
            }
        }
        rows
    }

    /// Kafka's `Partition.submitAlterPartition` callback for one partition:
    /// an answer to a proposal the replica no longer waits on is ignored.
    fn on_isr_row(
        &mut self,
        ctx: &mut Ctx<'_>,
        key: &TopicPartition,
        proposal: Proposal,
        answer: IsrAnswer,
    ) {
        let me = self.config.broker_id;
        let Some(replica) = self.replicas.get_mut(key) else {
            return;
        };
        let waiting = replica.pending_isr.as_ref().is_some_and(|pending| {
            pending.leader_epoch == proposal.leader_epoch
                && pending.partition_epoch == proposal.partition_epoch
                && pending.proposed
                    == proposal
                        .new_isr
                        .iter()
                        .map(|(id, _)| *id)
                        .collect::<Vec<_>>()
        });
        if !waiting {
            return;
        }
        match answer {
            IsrAnswer::Committed {
                leader_epoch,
                isr,
                partition_epoch,
            } => {
                if replica.commit_isr(me, leader_epoch, isr.clone(), partition_epoch) {
                    ctx.event(
                        "isr_change",
                        json!({
                            "topic": key.topic, "partition": key.partition, "isr": isr,
                            "level": if isr.len() < replica.replicas.len() { "warn" } else { "info" },
                        }),
                    );
                }
            }
            IsrAnswer::Refused(code) => {
                ctx.event(
                    "alter_partition_failed",
                    json!({ "topic": key.topic, "partition": key.partition, "error_code": code, "level": "warn" }),
                );
                match code {
                    codes::OPERATION_NOT_ATTEMPTED | codes::INELIGIBLE_REPLICA => {
                        replica.pending_isr = None;
                    }
                    codes::UNKNOWN_TOPIC_OR_PARTITION
                    | codes::UNKNOWN_TOPIC_ID
                    | codes::FENCED_LEADER_EPOCH
                    | codes::INVALID_UPDATE_VERSION
                    | codes::INVALID_REQUEST
                    | NEW_LEADER_ELECTED => {}
                    _ => {
                        self.isr.unsent.insert(key.clone(), proposal);
                    }
                }
            }
        }
    }
}

/// The controller's answer for one partition.
#[derive(Clone, Debug, PartialEq, Eq)]
enum IsrAnswer {
    /// The ISR the controller committed.
    Committed {
        leader_epoch: i32,
        isr: Vec<i32>,
        partition_epoch: i32,
    },
    /// The row's error.
    Refused(i16),
}
