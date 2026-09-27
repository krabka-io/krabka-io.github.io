//! `AlterPartition` (api key 56) on the controller listener: a partition
//! leader changes its ISR.
//!
//! As Kafka's `ReplicationControlManager.alterPartition`: a request with no
//! topic answers at once; a node that is not the active controller answers
//! `NOT_CONTROLLER`, and a sender whose broker epoch is not its
//! registration's `STALE_BROKER_EPOCH`, both at the top level. A topic id
//! the image does not know answers `UNKNOWN_TOPIC_ID` on its partitions, and
//! every other partition is decided by
//! [`ControllerDecisions::alter_partition`]. The admitted rows carry the
//! partition's new state, and the answer waits for the records to commit.
//! Below v3 the ISR comes without broker epochs, which skips their check.
//!
//! [`ControllerDecisions::alter_partition`]: crate::lab::controller::ControllerDecisions::alter_partition

use krabka_protocol::owned::{
    alter_partition_request::AlterPartitionRequest,
    alter_partition_response::{
        AlterPartitionResponse, PartitionData as ResponsePartition, TopicData as ResponseTopic,
    },
};

use super::{
    super::{
        BrokerNode, cluster,
        dispatch::{Outcome, RequestCtx},
    },
    uuid_of,
};
use crate::lab::{
    codes,
    controller::decisions::{AlterPartition, IsrMember},
    net::{Ctx, NodeId},
};

/// The first version whose ISR carries broker epochs (KIP-903).
const ISR_WITH_EPOCHS_VERSION: i16 = 3;

fn refused(error_code: i16) -> AlterPartitionResponse {
    AlterPartitionResponse {
        error_code,
        ..AlterPartitionResponse::default()
    }
}

fn lab_node(id: i32) -> NodeId {
    NodeId(u32::try_from(id).unwrap_or(u32::MAX))
}

/// Serve an `AlterPartition` as the controller.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    AlterPartitionRequest {
        broker_id,
        broker_epoch,
        topics: requested,
        ..
    }: AlterPartitionRequest,
) -> Outcome<AlterPartitionResponse> {
    if requested.is_empty() {
        return Outcome::Reply(AlterPartitionResponse::default());
    }
    let Some(active) = node.quorum.active.as_ref() else {
        return Outcome::Reply(refused(codes::NOT_CONTROLLER));
    };
    if active.image.broker_epoch(cluster::meta_id(broker_id)) != Some(broker_epoch) {
        return Outcome::Reply(refused(codes::STALE_BROKER_EPOCH));
    }
    let mut records = Vec::new();
    let mut topics = Vec::with_capacity(requested.len());
    for topic in &requested {
        let name = active.image.topic_name_by_id(&uuid_of(topic.topic_id));
        let partitions = topic
            .partitions
            .iter()
            .map(|partition| {
                let refused = |error_code| ResponsePartition {
                    partition_index: partition.partition_index,
                    error_code,
                    ..ResponsePartition::default()
                };
                let Some(name) = name else {
                    return refused(codes::UNKNOWN_TOPIC_ID);
                };
                let new_isr = if req.version >= ISR_WITH_EPOCHS_VERSION {
                    partition
                        .new_isr_with_epochs
                        .iter()
                        .map(|member| IsrMember {
                            broker: lab_node(member.broker_id),
                            broker_epoch: member.broker_epoch,
                        })
                        .collect()
                } else {
                    partition
                        .new_isr
                        .iter()
                        .map(|&id| IsrMember {
                            broker: lab_node(id),
                            broker_epoch: -1,
                        })
                        .collect()
                };
                let row = AlterPartition {
                    broker_id: lab_node(broker_id),
                    topic: name.to_string(),
                    partition: partition.partition_index,
                    leader_epoch: partition.leader_epoch,
                    partition_epoch: partition.partition_epoch,
                    new_isr,
                };
                match active.decisions.alter_partition(&active.image, &row) {
                    Ok(altered) => {
                        records.extend(altered.records);
                        ResponsePartition {
                            partition_index: partition.partition_index,
                            error_code: codes::NONE,
                            leader_id: i32::try_from(altered.leader.0).unwrap_or(-1),
                            leader_epoch: altered.leader_epoch,
                            isr: altered
                                .isr
                                .iter()
                                .map(|id| i32::try_from(id.0).unwrap_or(-1))
                                .collect(),
                            leader_recovery_state: 0,
                            partition_epoch: altered.partition_epoch,
                            ..ResponsePartition::default()
                        }
                    }
                    Err(code) => refused(code),
                }
            })
            .collect();
        topics.push(ResponseTopic {
            topic_id: topic.topic_id,
            partitions,
            ..ResponseTopic::default()
        });
    }
    let response = AlterPartitionResponse {
        topics,
        ..AlterPartitionResponse::default()
    };
    node.controller_write(ctx, req, records, response, |_| {
        refused(codes::NOT_CONTROLLER)
    })
}
