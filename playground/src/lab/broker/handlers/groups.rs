//! The group apis, wired to the coordinator seam.
//!
//! The group coordinator is a separate module of the lab. Until it plugs in,
//! [`coordinator_seam`] answers every group request with
//! `COORDINATOR_NOT_AVAILABLE`, in the error field of each response shape.
//! Wiring the coordinator in is a change to this one file: each handler hands
//! its decoded request to the coordinator and answers with what it returns.

use krabka_protocol::owned::{
    consumer_group_describe_request::ConsumerGroupDescribeRequest,
    consumer_group_describe_response::{
        ConsumerGroupDescribeResponse, DescribedGroup as ConsumerDescribedGroup,
    },
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    describe_groups_request::DescribeGroupsRequest,
    describe_groups_response::{DescribeGroupsResponse, DescribedGroup},
    heartbeat_request::HeartbeatRequest,
    heartbeat_response::HeartbeatResponse,
    join_group_request::JoinGroupRequest,
    join_group_response::JoinGroupResponse,
    leave_group_request::LeaveGroupRequest,
    leave_group_response::LeaveGroupResponse,
    list_groups_request::ListGroupsRequest,
    list_groups_response::ListGroupsResponse,
    offset_commit_request::OffsetCommitRequest,
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_fetch_request::OffsetFetchRequest,
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_describe_response::{
        DescribedGroup as StreamsDescribedGroup, StreamsGroupDescribeResponse,
    },
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    sync_group_request::SyncGroupRequest,
    sync_group_response::SyncGroupResponse,
};

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{codes, net::Ctx};

/// The error every group api answers with until the coordinator plugs in.
#[must_use]
pub fn coordinator_seam(_node: &mut BrokerNode, _req: &RequestCtx, _group_id: &str) -> i16 {
    codes::COORDINATOR_NOT_AVAILABLE
}

/// Answer a `JoinGroup` through the coordinator seam.
pub fn join_group(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: JoinGroupRequest,
) -> Outcome<JoinGroupResponse> {
    Outcome::Reply(JoinGroupResponse {
        error_code: coordinator_seam(node, req, &request.group_id),
        member_id: request.member_id,
        ..JoinGroupResponse::default()
    })
}

/// Answer a `SyncGroup` through the coordinator seam.
pub fn sync_group(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: SyncGroupRequest,
) -> Outcome<SyncGroupResponse> {
    let SyncGroupRequest { group_id, .. } = request;
    Outcome::Reply(SyncGroupResponse {
        error_code: coordinator_seam(node, req, &group_id),
        ..SyncGroupResponse::default()
    })
}

/// Answer a `Heartbeat` through the coordinator seam.
pub fn heartbeat(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: HeartbeatRequest,
) -> Outcome<HeartbeatResponse> {
    let HeartbeatRequest { group_id, .. } = request;
    Outcome::Reply(HeartbeatResponse {
        error_code: coordinator_seam(node, req, &group_id),
        ..HeartbeatResponse::default()
    })
}

/// Answer a `LeaveGroup` through the coordinator seam.
pub fn leave_group(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: LeaveGroupRequest,
) -> Outcome<LeaveGroupResponse> {
    let LeaveGroupRequest { group_id, .. } = request;
    Outcome::Reply(LeaveGroupResponse {
        error_code: coordinator_seam(node, req, &group_id),
        ..LeaveGroupResponse::default()
    })
}

/// Answer a `OffsetCommit` through the coordinator seam.
pub fn offset_commit(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: OffsetCommitRequest,
) -> Outcome<OffsetCommitResponse> {
    let error_code = coordinator_seam(node, req, &request.group_id);
    Outcome::Reply(OffsetCommitResponse {
        topics: request
            .topics
            .into_iter()
            .map(|topic| OffsetCommitResponseTopic {
                name: topic.name,
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|p| OffsetCommitResponsePartition {
                        partition_index: p.partition_index,
                        error_code,
                        ..OffsetCommitResponsePartition::default()
                    })
                    .collect(),
                ..OffsetCommitResponseTopic::default()
            })
            .collect(),
        ..OffsetCommitResponse::default()
    })
}

/// Answer a `OffsetFetch` through the coordinator seam.
pub fn offset_fetch(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: OffsetFetchRequest,
) -> Outcome<OffsetFetchResponse> {
    // v8 and later ask per group; earlier versions ask for one group.
    if req.version >= 8 {
        let groups = request
            .groups
            .into_iter()
            .map(|group| {
                let error_code = coordinator_seam(node, req, &group.group_id);
                OffsetFetchResponseGroup {
                    group_id: group.group_id,
                    topics: group
                        .topics
                        .unwrap_or_default()
                        .into_iter()
                        .map(|topic| OffsetFetchResponseTopics {
                            name: topic.name,
                            topic_id: topic.topic_id,
                            partitions: topic
                                .partition_indexes
                                .into_iter()
                                .map(|index| OffsetFetchResponsePartitions {
                                    partition_index: index,
                                    committed_offset: -1,
                                    error_code,
                                    ..OffsetFetchResponsePartitions::default()
                                })
                                .collect(),
                            ..OffsetFetchResponseTopics::default()
                        })
                        .collect(),
                    error_code,
                    ..OffsetFetchResponseGroup::default()
                }
            })
            .collect();
        return Outcome::Reply(OffsetFetchResponse {
            groups,
            ..OffsetFetchResponse::default()
        });
    }
    let error_code = coordinator_seam(node, req, &request.group_id);
    Outcome::Reply(OffsetFetchResponse {
        topics: request
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|topic| OffsetFetchResponseTopic {
                name: topic.name,
                partitions: topic
                    .partition_indexes
                    .into_iter()
                    .map(|index| OffsetFetchResponsePartition {
                        partition_index: index,
                        committed_offset: -1,
                        error_code,
                        ..OffsetFetchResponsePartition::default()
                    })
                    .collect(),
                ..OffsetFetchResponseTopic::default()
            })
            .collect(),
        error_code,
        ..OffsetFetchResponse::default()
    })
}

/// Answer a `DescribeGroups` through the coordinator seam.
pub fn describe_groups(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: DescribeGroupsRequest,
) -> Outcome<DescribeGroupsResponse> {
    Outcome::Reply(DescribeGroupsResponse {
        groups: request
            .groups
            .into_iter()
            .map(|group_id| DescribedGroup {
                error_code: coordinator_seam(node, req, &group_id),
                group_id,
                ..DescribedGroup::default()
            })
            .collect(),
        ..DescribeGroupsResponse::default()
    })
}

/// Answer a `ListGroups` through the coordinator seam.
pub fn list_groups(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    _request: ListGroupsRequest,
) -> Outcome<ListGroupsResponse> {
    Outcome::Reply(ListGroupsResponse {
        error_code: coordinator_seam(node, req, ""),
        ..ListGroupsResponse::default()
    })
}

/// Answer a `ConsumerGroupHeartbeat` through the coordinator seam.
pub fn consumer_group_heartbeat(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: ConsumerGroupHeartbeatRequest,
) -> Outcome<ConsumerGroupHeartbeatResponse> {
    let ConsumerGroupHeartbeatRequest { group_id, .. } = request;
    Outcome::Reply(ConsumerGroupHeartbeatResponse {
        error_code: coordinator_seam(node, req, &group_id),
        ..ConsumerGroupHeartbeatResponse::default()
    })
}

/// Answer a `ConsumerGroupDescribe` through the coordinator seam.
pub fn consumer_group_describe(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: ConsumerGroupDescribeRequest,
) -> Outcome<ConsumerGroupDescribeResponse> {
    Outcome::Reply(ConsumerGroupDescribeResponse {
        groups: request
            .group_ids
            .into_iter()
            .map(|group_id| ConsumerDescribedGroup {
                error_code: coordinator_seam(node, req, &group_id),
                group_id,
                ..ConsumerDescribedGroup::default()
            })
            .collect(),
        ..ConsumerGroupDescribeResponse::default()
    })
}

/// Answer a `StreamsGroupHeartbeat` through the coordinator seam.
pub fn streams_group_heartbeat(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: StreamsGroupHeartbeatRequest,
) -> Outcome<StreamsGroupHeartbeatResponse> {
    Outcome::Reply(StreamsGroupHeartbeatResponse {
        error_code: coordinator_seam(node, req, &request.group_id),
        member_id: request.member_id,
        ..StreamsGroupHeartbeatResponse::default()
    })
}

/// Answer a `StreamsGroupDescribe` through the coordinator seam.
pub fn streams_group_describe(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: StreamsGroupDescribeRequest,
) -> Outcome<StreamsGroupDescribeResponse> {
    Outcome::Reply(StreamsGroupDescribeResponse {
        groups: request
            .group_ids
            .into_iter()
            .map(|group_id| StreamsDescribedGroup {
                error_code: coordinator_seam(node, req, &group_id),
                group_id,
                ..StreamsDescribedGroup::default()
            })
            .collect(),
        ..StreamsGroupDescribeResponse::default()
    })
}
