//! The group apis, served by the broker's group coordinator.
//!
//! Each handler makes the checks Kafka's `GroupCoordinatorService` makes
//! before it routes a request, then answers `NOT_COORDINATOR` for a group of
//! a `__consumer_offsets` partition this broker does not lead, and hands
//! the rest to the [`Coordinator`](super::super::coordinator::Coordinator).
//! The error shapes are Kafka's: `JoinGroup` keeps the member id only for
//! the checks before routing; `OffsetCommit` refuses the partitions that
//! pass `KafkaApis`' topic checks; `OffsetFetch` refuses per group, with
//! the group-level error from v2 and per partition in v1; the describes
//! refuse per group; `ConsumerGroupHeartbeat` and `StreamsGroupHeartbeat`
//! carry Kafka's message. An answer after a write waits for the write to
//! commit (see the broker's `groups` module); a timed-out write answers
//! `COORDINATOR_NOT_AVAILABLE`, and a write whose partition this broker
//! stops leading `NOT_COORDINATOR`. A classic `Heartbeat` that finds its
//! group loading answers `NONE`, as Kafka's does; the lab loads a partition
//! at once, so it never does. `ListGroups` lists the groups of every loaded
//! partition.

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
    offset_commit_response::OffsetCommitResponse,
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics},
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
    coordinator::{Coordinator, Pending, group_partition},
    dispatch::{HoldReason, Outcome, RequestCtx},
    groups::{ImageTopics, NOT_COORDINATOR_MESSAGE, member_key},
};
use crate::lab::{codes, net::Ctx};

/// The first `OffsetFetch` version with a top-level error and null topics.
const OFFSET_FETCH_TOP_LEVEL_ERROR_VERSION: i16 = 2;
/// The first `OffsetFetch` version with the per-group shape.
const OFFSET_FETCH_GROUPS_VERSION: i16 = 8;

/// Kafka's answer to a `JoinGroup` its runtime failed with `error_code`.
fn join_error(error_code: i16) -> JoinGroupResponse {
    JoinGroupResponse {
        error_code,
        ..JoinGroupResponse::default()
    }
}

/// Serve a `JoinGroup`.
pub fn join_group(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: JoinGroupRequest,
) -> Outcome<JoinGroupResponse> {
    let refused = |error_code| JoinGroupResponse {
        error_code,
        member_id: request.member_id.clone(),
        ..JoinGroupResponse::default()
    };
    if request.group_id.is_empty() {
        return Outcome::Reply(refused(codes::INVALID_GROUP_ID));
    }
    let config = &node.config.coordinator;
    let session = u64::try_from(request.session_timeout_ms).unwrap_or(0);
    if session < config.classic_min_session_timeout_ms
        || session > config.classic_max_session_timeout_ms
    {
        return Outcome::Reply(refused(codes::INVALID_SESSION_TIMEOUT));
    }
    if !node.groups.coordinates(&request.group_id) {
        return Outcome::Reply(join_error(codes::NOT_COORDINATOR));
    }
    let answer =
        node.groups
            .coordinator
            .join_group(ctx.now(), &member_key(req), &request, req.version);
    let JoinGroupRequest { group_id, .. } = request;
    let waits = node.after_coordinator_call(ctx, &[group_partition(&group_id)]);
    match answer {
        Pending::Ready(response) => node.group_answer(
            ctx,
            req,
            waits,
            response,
            &join_error(codes::NOT_COORDINATOR),
            &join_error(codes::COORDINATOR_NOT_AVAILABLE),
        ),
        Pending::Held(token) => Outcome::Hold(HoldReason::Group { token }),
    }
}

/// Kafka's answer to a `SyncGroup` that fails with `error_code`.
fn sync_error(error_code: i16) -> SyncGroupResponse {
    SyncGroupResponse {
        error_code,
        ..SyncGroupResponse::default()
    }
}

/// Serve a `SyncGroup`.
pub fn sync_group(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: SyncGroupRequest,
) -> Outcome<SyncGroupResponse> {
    if request.group_id.is_empty() {
        return Outcome::Reply(sync_error(codes::INVALID_GROUP_ID));
    }
    if !node.groups.coordinates(&request.group_id) {
        return Outcome::Reply(sync_error(codes::NOT_COORDINATOR));
    }
    let answer = node.groups.coordinator.sync_group(ctx.now(), &request);
    let SyncGroupRequest { group_id, .. } = request;
    let waits = node.after_coordinator_call(ctx, &[group_partition(&group_id)]);
    match answer {
        Pending::Ready(response) => node.group_answer(
            ctx,
            req,
            waits,
            response,
            &sync_error(codes::NOT_COORDINATOR),
            &sync_error(codes::COORDINATOR_NOT_AVAILABLE),
        ),
        Pending::Held(token) => Outcome::Hold(HoldReason::Group { token }),
    }
}

/// Kafka's answer to a `Heartbeat` that fails with `error_code`.
fn heartbeat_error(error_code: i16) -> HeartbeatResponse {
    HeartbeatResponse {
        error_code,
        ..HeartbeatResponse::default()
    }
}

/// Serve a `Heartbeat`.
pub fn heartbeat(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: HeartbeatRequest,
) -> Outcome<HeartbeatResponse> {
    if request.group_id.is_empty() {
        return Outcome::Reply(heartbeat_error(codes::INVALID_GROUP_ID));
    }
    if !node.groups.coordinates(&request.group_id) {
        return Outcome::Reply(heartbeat_error(codes::NOT_COORDINATOR));
    }
    let response = node.groups.coordinator.heartbeat(ctx.now(), &request);
    let HeartbeatRequest { group_id, .. } = request;
    let waits = node.after_coordinator_call(ctx, &[group_partition(&group_id)]);
    node.group_answer(
        ctx,
        req,
        waits,
        response,
        &heartbeat_error(codes::NOT_COORDINATOR),
        &heartbeat_error(codes::COORDINATOR_NOT_AVAILABLE),
    )
}

/// Kafka's answer to a `LeaveGroup` that fails with `error_code`.
fn leave_error(error_code: i16) -> LeaveGroupResponse {
    LeaveGroupResponse {
        error_code,
        ..LeaveGroupResponse::default()
    }
}

/// Serve a `LeaveGroup`.
pub fn leave_group(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: LeaveGroupRequest,
) -> Outcome<LeaveGroupResponse> {
    if request.group_id.is_empty() {
        return Outcome::Reply(leave_error(codes::INVALID_GROUP_ID));
    }
    if !node.groups.coordinates(&request.group_id) {
        return Outcome::Reply(leave_error(codes::NOT_COORDINATOR));
    }
    let response = node
        .groups
        .coordinator
        .leave_group(ctx.now(), &request, req.version);
    let LeaveGroupRequest { group_id, .. } = request;
    let waits = node.after_coordinator_call(ctx, &[group_partition(&group_id)]);
    node.group_answer(
        ctx,
        req,
        waits,
        response,
        &leave_error(codes::NOT_COORDINATOR),
        &leave_error(codes::COORDINATOR_NOT_AVAILABLE),
    )
}

/// Serve an `OffsetCommit`.
pub fn offset_commit(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: OffsetCommitRequest,
) -> Outcome<OffsetCommitResponse> {
    let refused = |node: &BrokerNode, error_code| {
        Coordinator::offset_commit_refused(
            &request,
            req.version,
            &ImageTopics { image: &node.image },
            error_code,
        )
    };
    if !node.groups.coordinates(&request.group_id) {
        return Outcome::Reply(refused(node, codes::NOT_COORDINATOR));
    }
    let response = node.groups.coordinator.offset_commit(
        ctx.now(),
        &request,
        req.version,
        &ImageTopics { image: &node.image },
    );
    let not_coordinator = refused(node, codes::NOT_COORDINATOR);
    let timed_out = refused(node, codes::COORDINATOR_NOT_AVAILABLE);
    let OffsetCommitRequest { group_id, .. } = request;
    let waits = node.after_coordinator_call(ctx, &[group_partition(&group_id)]);
    node.group_answer(ctx, req, waits, response, &not_coordinator, &timed_out)
}

/// Kafka's `OffsetFetchResponse.groupError`: the group-level error from v2,
/// and in v1 the error on every partition the group asked for.
fn fetch_group_error(
    group: &OffsetFetchRequestGroup,
    error_code: i16,
    version: i16,
) -> OffsetFetchResponseGroup {
    let topics = if version >= OFFSET_FETCH_TOP_LEVEL_ERROR_VERSION {
        Vec::new()
    } else {
        group
            .topics
            .iter()
            .flatten()
            .map(|topic| OffsetFetchResponseTopics {
                name: topic.name.clone(),
                partitions: topic
                    .partition_indexes
                    .iter()
                    .map(|&partition_index| OffsetFetchResponsePartitions {
                        partition_index,
                        error_code,
                        committed_offset: -1,
                        metadata: Some(String::new()),
                        committed_leader_epoch: -1,
                        ..OffsetFetchResponsePartitions::default()
                    })
                    .collect(),
                ..OffsetFetchResponseTopics::default()
            })
            .collect()
    };
    OffsetFetchResponseGroup {
        group_id: group.group_id.clone(),
        topics,
        error_code: if version >= OFFSET_FETCH_TOP_LEVEL_ERROR_VERSION {
            error_code
        } else {
            codes::NONE
        },
        ..OffsetFetchResponseGroup::default()
    }
}

/// Kafka's `OffsetFetchResponse.Builder.build` below v8: the one group in
/// the top-level fields.
fn single_group_response(group: OffsetFetchResponseGroup) -> OffsetFetchResponse {
    OffsetFetchResponse {
        error_code: group.error_code,
        topics: group
            .topics
            .into_iter()
            .map(|topic| OffsetFetchResponseTopic {
                name: topic.name,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|p| OffsetFetchResponsePartition {
                        partition_index: p.partition_index,
                        committed_offset: p.committed_offset,
                        committed_leader_epoch: p.committed_leader_epoch,
                        metadata: p.metadata,
                        error_code: p.error_code,
                        ..OffsetFetchResponsePartition::default()
                    })
                    .collect(),
                ..OffsetFetchResponseTopic::default()
            })
            .collect(),
        ..OffsetFetchResponse::default()
    }
}

/// Serve an `OffsetFetch`: below v8 for the one group, from v8 per group,
/// each group this broker does not coordinate refused on its own.
pub fn offset_fetch(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    OffsetFetchRequest {
        group_id,
        topics,
        groups,
        require_stable,
        ..
    }: OffsetFetchRequest,
) -> Outcome<OffsetFetchResponse> {
    let metadata = ImageTopics { image: &node.image };
    if req.version < OFFSET_FETCH_GROUPS_VERSION {
        if node.groups.coordinates(&group_id) {
            let asked = OffsetFetchRequest {
                group_id,
                topics,
                require_stable,
                ..OffsetFetchRequest::default()
            };
            return Outcome::Reply(node.groups.coordinator.offset_fetch(
                &asked,
                req.version,
                &metadata,
            ));
        }
        let group = OffsetFetchRequestGroup {
            group_id,
            topics: topics.map(|topics| {
                topics
                    .into_iter()
                    .map(|topic| OffsetFetchRequestTopics {
                        name: topic.name,
                        partition_indexes: topic.partition_indexes,
                        ..OffsetFetchRequestTopics::default()
                    })
                    .collect()
            }),
            ..OffsetFetchRequestGroup::default()
        };
        return Outcome::Reply(single_group_response(fetch_group_error(
            &group,
            codes::NOT_COORDINATOR,
            req.version,
        )));
    }
    let mine: Vec<OffsetFetchRequestGroup> = groups
        .iter()
        .filter(|group| node.groups.coordinates(&group.group_id))
        .cloned()
        .collect();
    let answered = if mine.is_empty() {
        Vec::new()
    } else {
        let asked = OffsetFetchRequest {
            groups: mine,
            require_stable,
            ..OffsetFetchRequest::default()
        };
        node.groups
            .coordinator
            .offset_fetch(&asked, req.version, &metadata)
            .groups
    };
    let mut answered = answered.into_iter();
    let groups = groups
        .iter()
        .map(|group| {
            if node.groups.coordinates(&group.group_id)
                && let Some(row) = answered.next()
            {
                row
            } else {
                fetch_group_error(group, codes::NOT_COORDINATOR, req.version)
            }
        })
        .collect();
    Outcome::Reply(OffsetFetchResponse {
        groups,
        ..OffsetFetchResponse::default()
    })
}

/// Serve a `DescribeGroups`: every group is routed by its partition, the
/// empty group id among them.
pub fn describe_groups(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    DescribeGroupsRequest {
        groups: requested,
        include_authorized_operations,
        ..
    }: DescribeGroupsRequest,
) -> Outcome<DescribeGroupsResponse> {
    let mine: Vec<String> = requested
        .iter()
        .filter(|group| node.groups.coordinates(group))
        .cloned()
        .collect();
    let mut described = node
        .groups
        .coordinator
        .describe_groups(
            &DescribeGroupsRequest {
                groups: mine,
                include_authorized_operations,
                ..DescribeGroupsRequest::default()
            },
            req.version,
        )
        .groups
        .into_iter();
    let groups = requested
        .into_iter()
        .map(|group_id| {
            if node.groups.coordinates(&group_id)
                && let Some(row) = described.next()
            {
                row
            } else {
                DescribedGroup {
                    group_id,
                    error_code: codes::NOT_COORDINATOR,
                    ..DescribedGroup::default()
                }
            }
        })
        .collect();
    Outcome::Reply(DescribeGroupsResponse {
        groups,
        ..DescribeGroupsResponse::default()
    })
}

/// Serve a `ListGroups`: the groups of every partition this broker loaded.
pub fn list_groups(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    ListGroupsRequest {
        states_filter,
        types_filter,
        ..
    }: ListGroupsRequest,
) -> Outcome<ListGroupsResponse> {
    Outcome::Reply(node.groups.coordinator.list_groups(&ListGroupsRequest {
        states_filter,
        types_filter,
        ..ListGroupsRequest::default()
    }))
}

/// Serve a `ConsumerGroupHeartbeat` (KIP-848).
pub fn consumer_group_heartbeat(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: ConsumerGroupHeartbeatRequest,
) -> Outcome<ConsumerGroupHeartbeatResponse> {
    let error = |error_code, message: Option<String>| ConsumerGroupHeartbeatResponse {
        error_code,
        error_message: message,
        ..ConsumerGroupHeartbeatResponse::default()
    };
    if let Some((code, message)) =
        Coordinator::consumer_group_heartbeat_error(&request, req.version)
    {
        return Outcome::Reply(error(code, Some(message)));
    }
    let not_coordinator = error(
        codes::NOT_COORDINATOR,
        Some(NOT_COORDINATOR_MESSAGE.to_string()),
    );
    if !node.groups.coordinates(&request.group_id) {
        return Outcome::Reply(not_coordinator);
    }
    let response = node.groups.coordinator.consumer_group_heartbeat(
        ctx.now(),
        &member_key(req),
        &request,
        req.version,
        &ImageTopics { image: &node.image },
    );
    let ConsumerGroupHeartbeatRequest { group_id, .. } = request;
    let waits = node.after_coordinator_call(ctx, &[group_partition(&group_id)]);
    node.group_answer(
        ctx,
        req,
        waits,
        response,
        &not_coordinator,
        &error(codes::COORDINATOR_NOT_AVAILABLE, None),
    )
}

/// Serve a `ConsumerGroupDescribe`: an empty group id answers
/// `INVALID_GROUP_ID` first, then every other group in request order,
/// routed by its partition.
pub fn consumer_group_describe(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    ConsumerGroupDescribeRequest {
        group_ids,
        include_authorized_operations,
        ..
    }: ConsumerGroupDescribeRequest,
) -> Outcome<ConsumerGroupDescribeResponse> {
    let invalid = group_ids
        .iter()
        .filter(|id| id.is_empty())
        .map(|_| ConsumerDescribedGroup {
            error_code: codes::INVALID_GROUP_ID,
            ..ConsumerDescribedGroup::default()
        });
    let mine: Vec<String> = group_ids
        .iter()
        .filter(|id| !id.is_empty() && node.groups.coordinates(id))
        .cloned()
        .collect();
    let mut described = node
        .groups
        .coordinator
        .consumer_group_describe(&ConsumerGroupDescribeRequest {
            group_ids: mine,
            include_authorized_operations,
            ..ConsumerGroupDescribeRequest::default()
        })
        .groups
        .into_iter();
    let routed: Vec<ConsumerDescribedGroup> = group_ids
        .iter()
        .filter(|id| !id.is_empty())
        .map(|group_id| {
            if node.groups.coordinates(group_id)
                && let Some(row) = described.next()
            {
                row
            } else {
                ConsumerDescribedGroup {
                    group_id: group_id.clone(),
                    error_code: codes::NOT_COORDINATOR,
                    ..ConsumerDescribedGroup::default()
                }
            }
        })
        .collect();
    Outcome::Reply(ConsumerGroupDescribeResponse {
        groups: invalid.chain(routed).collect(),
        ..ConsumerGroupDescribeResponse::default()
    })
}

/// Serve a `StreamsGroupHeartbeat` (KIP-1071), and ask the controller for
/// the internal topics the group's topology is missing.
pub fn streams_group_heartbeat(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    request: StreamsGroupHeartbeatRequest,
) -> Outcome<StreamsGroupHeartbeatResponse> {
    let error = |error_code, message: Option<String>| StreamsGroupHeartbeatResponse {
        error_code,
        error_message: message,
        ..StreamsGroupHeartbeatResponse::default()
    };
    if let Some((code, message)) = Coordinator::streams_group_heartbeat_error(&request) {
        return Outcome::Reply(error(code, Some(message)));
    }
    let not_coordinator = error(
        codes::NOT_COORDINATOR,
        Some(NOT_COORDINATOR_MESSAGE.to_string()),
    );
    if !node.groups.coordinates(&request.group_id) {
        return Outcome::Reply(not_coordinator);
    }
    let (response, to_create) = node.groups.coordinator.streams_group_heartbeat(
        ctx.now(),
        &member_key(req),
        &request,
        &ImageTopics { image: &node.image },
    );
    let StreamsGroupHeartbeatRequest { group_id, .. } = request;
    let waits = node.after_coordinator_call(ctx, &[group_partition(&group_id)]);
    if !to_create.is_empty() {
        node.create_internal_topics(ctx, to_create);
    }
    node.group_answer(
        ctx,
        req,
        waits,
        response,
        &not_coordinator,
        &error(codes::COORDINATOR_NOT_AVAILABLE, None),
    )
}

/// Serve a `StreamsGroupDescribe`: an empty group id answers
/// `INVALID_GROUP_ID` first, then every other group in request order,
/// routed by its partition.
pub fn streams_group_describe(
    node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    StreamsGroupDescribeRequest {
        group_ids,
        include_authorized_operations,
        ..
    }: StreamsGroupDescribeRequest,
) -> Outcome<StreamsGroupDescribeResponse> {
    let invalid = group_ids
        .iter()
        .filter(|id| id.is_empty())
        .map(|_| StreamsDescribedGroup {
            error_code: codes::INVALID_GROUP_ID,
            ..StreamsDescribedGroup::default()
        });
    let mine: Vec<String> = group_ids
        .iter()
        .filter(|id| !id.is_empty() && node.groups.coordinates(id))
        .cloned()
        .collect();
    let mut described = node
        .groups
        .coordinator
        .streams_group_describe(&StreamsGroupDescribeRequest {
            group_ids: mine,
            include_authorized_operations,
            ..StreamsGroupDescribeRequest::default()
        })
        .groups
        .into_iter();
    let routed: Vec<StreamsDescribedGroup> = group_ids
        .iter()
        .filter(|id| !id.is_empty())
        .map(|group_id| {
            if node.groups.coordinates(group_id)
                && let Some(row) = described.next()
            {
                row
            } else {
                StreamsDescribedGroup {
                    group_id: group_id.clone(),
                    error_code: codes::NOT_COORDINATOR,
                    ..StreamsDescribedGroup::default()
                }
            }
        })
        .collect();
    Outcome::Reply(StreamsGroupDescribeResponse {
        groups: invalid.chain(routed).collect(),
        ..StreamsGroupDescribeResponse::default()
    })
}
