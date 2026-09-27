//! `DescribeQuorum` (api key 55) on the controller listener (KIP-919): the
//! metadata quorum as the controller core sees it.
//!
//! As Kafka's `KafkaRaftClient.handleDescribeQuorumRequest`: a request that
//! does not name exactly partition 0 of `__cluster_metadata` answers
//! `UNKNOWN_TOPIC_OR_PARTITION` on every partition it names, a node that
//! does not lead the quorum answers `NOT_LEADER_OR_FOLLOWER`, and the leader
//! describes its voters and observers ([`ControllerCore::describe_quorum`]).
//! A broker forwards the clients' requests here in an `Envelope`.
//!
//! [`ControllerCore::describe_quorum`]: crate::lab::controller::ControllerCore::describe_quorum

use krabka_protocol::owned::{
    describe_quorum_request::{DescribeQuorumRequest, TopicData},
    describe_quorum_response::{
        DescribeQuorumResponse, PartitionData as ResponsePartition, TopicData as ResponseTopic,
    },
};

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{codes, controller::METADATA_TOPIC, net::Ctx};

/// Kafka's `hasValidTopicPartition`: the request names partition 0 of the
/// metadata topic and nothing else.
fn names_the_metadata_partition(topics: &[TopicData]) -> bool {
    matches!(
        topics,
        [topic] if topic.topic_name == METADATA_TOPIC
            && matches!(topic.partitions.as_slice(), [partition] if partition.partition_index == 0)
    )
}

/// Serve a `DescribeQuorum` from the controller core.
pub fn handle(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    DescribeQuorumRequest { topics, .. }: DescribeQuorumRequest,
) -> Outcome<DescribeQuorumResponse> {
    if !names_the_metadata_partition(&topics) {
        // Kafka's `getPartitionLevelErrorResponse`.
        return Outcome::Reply(DescribeQuorumResponse {
            topics: topics
                .iter()
                .map(|topic| ResponseTopic {
                    topic_name: topic.topic_name.clone(),
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| ResponsePartition {
                            partition_index: partition.partition_index,
                            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                            ..ResponsePartition::default()
                        })
                        .collect(),
                    ..ResponseTopic::default()
                })
                .collect(),
            ..DescribeQuorumResponse::default()
        });
    }
    Outcome::Reply(node.quorum.core.describe_quorum())
}
