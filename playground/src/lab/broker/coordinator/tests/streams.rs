//! KIP-1071: `StreamsGroupHeartbeat` and `StreamsGroupDescribe`.

use assert2::assert;
use krabka_protocol::owned::{
    common::{
        streams_group_describe_response::{
            assignment::Assignment as DescribedAssignment,
            key_value::KeyValue as DescribedKeyValue, task_ids::TaskIds as DescribedTaskIds,
            topic_info::TopicInfo as DescribedTopicInfo,
        },
        streams_group_heartbeat_request::{
            key_value::KeyValue, task_ids::TaskIds as RequestTaskIds, topic_info::TopicInfo,
        },
        streams_group_heartbeat_response::{status::Status, task_ids::TaskIds},
    },
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_describe_response::{
        DescribedGroup, Member, Subtopology as DescribedSubtopology, Topology as DescribedTopology,
    },
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use super::{
    super::{
        InternalTopicToCreate,
        streams_topology::{
            ASSIGNMENT_DELAYED, MISSING_INTERNAL_TOPICS, MISSING_SOURCE_TOPICS,
            SHUTDOWN_APPLICATION,
        },
    },
    *,
};

fn topic_info(name: &str, configs: &[(&str, &str)]) -> TopicInfo {
    TopicInfo {
        name: name.to_string(),
        partitions: 0,
        replication_factor: 0,
        topic_configs: configs
            .iter()
            .map(|(key, value)| KeyValue {
                key: (*key).to_string(),
                value: (*value).to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Subtopology `0` reads `in`, keeps a store and writes `app-repartition`;
/// subtopology `1` reads `app-repartition` and keeps a store.
fn topology(epoch: i32) -> Topology {
    Topology {
        epoch,
        subtopologies: vec![
            Subtopology {
                subtopology_id: "0".to_string(),
                source_topics: vec!["in".to_string()],
                state_changelog_topics: vec![topic_info(
                    "app-store-changelog",
                    &[("cleanup.policy", "compact")],
                )],
                repartition_sink_topics: vec!["app-repartition".to_string()],
                ..Default::default()
            },
            Subtopology {
                subtopology_id: "1".to_string(),
                state_changelog_topics: vec![topic_info("app1-changelog", &[])],
                repartition_source_topics: vec![topic_info(
                    "app-repartition",
                    &[("cleanup.policy", "delete")],
                )],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

fn task_ids(tasks: &[(&str, &[i32])]) -> Vec<RequestTaskIds> {
    tasks
        .iter()
        .map(|(subtopology, partitions)| RequestTaskIds {
            subtopology_id: (*subtopology).to_string(),
            partitions: partitions.to_vec(),
            ..Default::default()
        })
        .collect()
}

fn join(member: &str, process: &str) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".to_string(),
        member_id: member.to_string(),
        member_epoch: 0,
        rebalance_timeout_ms: 60_000,
        topology: Some(topology(1)),
        active_tasks: Some(Vec::new()),
        standby_tasks: Some(Vec::new()),
        warmup_tasks: Some(Vec::new()),
        process_id: Some(process.to_string()),
        ..Default::default()
    }
}

fn heartbeat(
    member: &str,
    epoch: i32,
    active: Option<&[(&str, &[i32])]>,
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".to_string(),
        member_id: member.to_string(),
        member_epoch: epoch,
        active_tasks: active.map(task_ids),
        standby_tasks: active.map(|_| Vec::new()),
        warmup_tasks: active.map(|_| Vec::new()),
        ..Default::default()
    }
}

fn status(code: i8, detail: &str) -> Status {
    Status {
        status_code: code,
        status_detail: detail.to_string(),
        ..Default::default()
    }
}

fn accepted(
    member: &str,
    epoch: i32,
    status: Vec<Status>,
    active: Option<&[(&str, &[i32])]>,
) -> StreamsGroupHeartbeatResponse {
    let ids = |tasks: &[(&str, &[i32])]| -> Vec<TaskIds> {
        tasks
            .iter()
            .map(|(subtopology, partitions)| TaskIds {
                subtopology_id: (*subtopology).to_string(),
                partitions: partitions.to_vec(),
                ..Default::default()
            })
            .collect()
    };
    StreamsGroupHeartbeatResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        member_id: member.to_string(),
        member_epoch: epoch,
        heartbeat_interval_ms: 5000,
        // Kafka 4.3 sets neither the recovery lag nor the task offset
        // interval.
        acceptable_recovery_lag_legacy: 0,
        task_offset_interval_ms: 0,
        acceptable_recovery_lag: -1,
        status: Some(status),
        active_tasks: active.map(ids),
        standby_tasks: active.map(|_| Vec::new()),
        warmup_tasks: active.map(|_| Vec::new()),
        topology_description_required: false,
        endpoint_information_epoch: 0,
        partitions_by_user_endpoint: None,
        ..Default::default()
    }
}

fn refused(error_code: i16, message: &str) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        error_code,
        error_message: Some(message.to_string()),
        status: Some(Vec::new()),
        ..Default::default()
    }
}

fn internal_topic(name: &str, configs: &[(&str, &str)]) -> InternalTopicToCreate {
    InternalTopicToCreate {
        name: name.to_string(),
        partitions: 3,
        replication_factor: -1,
        configs: configs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    }
}

const MISSING_INTERNAL: &str =
    "Internal topics are missing: app-repartition, app-store-changelog, app1-changelog";

/// The first join registers the topology; while the internal topics are
/// missing the member gets the status and no tasks, and the coordinator
/// hands the topics to create back; once they exist the member gets every
/// task at the next epoch.
#[test]
fn topology_registration_and_missing_topics() {
    let mut c = Coordinator::new(1, undelayed());
    let mut topics = Topics::new(&[("in", 3)]);
    let (response, to_create) =
        c.streams_group_heartbeat(0, &client("c1"), &join("m1", "p1"), &topics);
    assert!(
        response
            == accepted(
                "m1",
                2,
                vec![status(MISSING_INTERNAL_TOPICS, MISSING_INTERNAL)],
                Some(&[])
            )
    );
    assert!(
        to_create
            == vec![
                internal_topic("app-repartition", &[("cleanup.policy", "delete")]),
                internal_topic("app-store-changelog", &[("cleanup.policy", "compact")]),
                internal_topic("app1-changelog", &[]),
            ]
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("app", "streams", "NotReady", "streams")],
                ..Default::default()
            }
    );
    // A later heartbeat repeats the status and the topics while they are
    // missing, and carries no tasks because nothing changed.
    let (response, to_create) =
        c.streams_group_heartbeat(1000, &client("c1"), &heartbeat("m1", 2, None), &topics);
    assert!(
        response
            == accepted(
                "m1",
                2,
                vec![status(MISSING_INTERNAL_TOPICS, MISSING_INTERNAL)],
                None
            )
    );
    assert!(to_create.len() == 3);
    for topic in ["app-repartition", "app-store-changelog", "app1-changelog"] {
        topics.add(topic, 3);
    }
    let (response, to_create) =
        c.streams_group_heartbeat(2000, &client("c1"), &heartbeat("m1", 2, Some(&[])), &topics);
    assert!(
        response
            == accepted(
                "m1",
                3,
                vec![],
                Some(&[("0", &[0, 1, 2]), ("1", &[0, 1, 2])])
            )
    );
    assert!(to_create.is_empty());
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("app", "streams", "Stable", "streams")],
                ..Default::default()
            }
    );
    // A source topic that disappears makes the group `NotReady`, and the
    // target of the next epoch is empty. A heartbeat without owned tasks
    // counts as still owning them (Kafka's `CurrentAssignmentBuilder`), so the
    // member keeps epoch 3 with nothing left to run until it reports that it
    // released them.
    let without_source = Topics::new(&[
        ("app-repartition", 3),
        ("app-store-changelog", 3),
        ("app1-changelog", 3),
    ]);
    let missing_source = || {
        vec![status(
            MISSING_SOURCE_TOPICS,
            "Source topics in are missing.",
        )]
    };
    let (response, to_create) = c.streams_group_heartbeat(
        3000,
        &client("c1"),
        &heartbeat("m1", 3, None),
        &without_source,
    );
    assert!(response == accepted("m1", 3, missing_source(), Some(&[])));
    assert!(to_create.is_empty());
    let (response, _) = c.streams_group_heartbeat(
        4000,
        &client("c1"),
        &heartbeat("m1", 3, Some(&[])),
        &without_source,
    );
    assert!(response == accepted("m1", 4, missing_source(), None));
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("app", "streams", "NotReady", "streams")],
                ..Default::default()
            }
    );
}

/// One heartbeat of a scenario: its name, the request and the whole
/// response it must get.
type Step = (
    &'static str,
    StreamsGroupHeartbeatRequest,
    StreamsGroupHeartbeatResponse,
);

/// Run the steps one second apart; member `mN` runs on client `cN`. No step
/// asks for an internal topic.
fn run_steps(c: &mut Coordinator, topics: &Topics, steps: Vec<Step>) {
    for (i, (name, request, want)) in steps.into_iter().enumerate() {
        let now = u64::try_from(i).unwrap() * 1000;
        let client = client(&request.member_id.replace('m', "c"));
        let (response, to_create) = c.streams_group_heartbeat(now, &client, &request, topics);
        assert!(response == want, "{name}");
        assert!(to_create.is_empty(), "{name}");
    }
}

/// Two members share the tasks; a third member joining moves one task from
/// each, and the reconciliation revokes before it assigns.
fn three_member_steps() -> Vec<Step> {
    let all: &[(&str, &[i32])] = &[("0", &[0, 1, 2]), ("1", &[0, 1, 2])];
    let m1_retained: &[(&str, &[i32])] = &[("0", &[0, 1]), ("1", &[0])];
    let m2_target: &[(&str, &[i32])] = &[("0", &[2]), ("1", &[1, 2])];
    let m1_final: &[(&str, &[i32])] = &[("0", &[0]), ("1", &[0])];
    let m2_final: &[(&str, &[i32])] = &[("0", &[2]), ("1", &[1])];
    vec![
        (
            "m1 joins and gets every task",
            join("m1", "p1"),
            accepted("m1", 2, vec![], Some(all)),
        ),
        (
            "m2 joins at the target epoch, its tasks still owned",
            join("m2", "p2"),
            accepted("m2", 3, vec![], Some(&[])),
        ),
        (
            "m1 learns what to revoke, at its epoch",
            heartbeat("m1", 2, Some(all)),
            accepted("m1", 2, vec![], Some(m1_retained)),
        ),
        (
            "m1 confirms and moves on",
            heartbeat("m1", 2, Some(m1_retained)),
            accepted("m1", 3, vec![], None),
        ),
        (
            "m2 receives the released tasks",
            heartbeat("m2", 3, Some(&[])),
            accepted("m2", 3, vec![], Some(m2_target)),
        ),
        (
            "m3 joins",
            join("m3", "p3"),
            accepted("m3", 4, vec![], Some(&[])),
        ),
        (
            "m1 revokes one task for m3",
            heartbeat("m1", 3, Some(m1_retained)),
            accepted("m1", 3, vec![], Some(m1_final)),
        ),
        (
            "m2 revokes one task for m3",
            heartbeat("m2", 3, Some(m2_target)),
            accepted("m2", 3, vec![], Some(m2_final)),
        ),
        (
            "m1 confirms",
            heartbeat("m1", 3, Some(m1_final)),
            accepted("m1", 4, vec![], None),
        ),
        (
            "m2 confirms",
            heartbeat("m2", 3, Some(m2_final)),
            accepted("m2", 4, vec![], None),
        ),
        (
            "m3 receives its tasks",
            heartbeat("m3", 4, Some(&[])),
            accepted("m3", 4, vec![], Some(&[("0", &[1]), ("1", &[2])])),
        ),
    ]
}

/// The configured form of [`topology`] over topics of `partitions`
/// partitions, as `StreamsGroupDescribe` reports it.
fn described_topology(partitions: i32) -> DescribedTopology {
    let info = |name: &str, configs: &[(&str, &str)]| DescribedTopicInfo {
        name: name.to_string(),
        partitions,
        replication_factor: 0,
        topic_configs: configs
            .iter()
            .map(|(key, value)| DescribedKeyValue {
                key: (*key).to_string(),
                value: (*value).to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    DescribedTopology {
        epoch: 1,
        subtopologies: Some(vec![
            DescribedSubtopology {
                subtopology_id: "0".to_string(),
                source_topics: vec!["in".to_string()],
                repartition_sink_topics: vec!["app-repartition".to_string()],
                state_changelog_topics: vec![info(
                    "app-store-changelog",
                    &[("cleanup.policy", "compact")],
                )],
                repartition_source_topics: Vec::new(),
                ..Default::default()
            },
            DescribedSubtopology {
                subtopology_id: "1".to_string(),
                source_topics: Vec::new(),
                repartition_sink_topics: Vec::new(),
                state_changelog_topics: vec![info("app1-changelog", &[])],
                repartition_source_topics: vec![info(
                    "app-repartition",
                    &[("cleanup.policy", "delete")],
                )],
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

/// A reconciled member as `StreamsGroupDescribe` reports it: its current and
/// target tasks agree. Member `mN` runs on client `cN`.
fn described_member(
    id: &str,
    process: &str,
    epoch: i32,
    active: &[(&str, &[i32])],
    standby: &[(&str, &[i32])],
) -> Member {
    let ids = |tasks: &[(&str, &[i32])]| -> Vec<DescribedTaskIds> {
        tasks
            .iter()
            .map(|(subtopology, partitions)| DescribedTaskIds {
                subtopology_id: (*subtopology).to_string(),
                partitions: partitions.to_vec(),
                ..Default::default()
            })
            .collect()
    };
    let assignment = DescribedAssignment {
        active_tasks: ids(active),
        standby_tasks: ids(standby),
        warmup_tasks: Vec::new(),
        ..Default::default()
    };
    let client = id.replace('m', "c");
    Member {
        member_id: id.to_string(),
        member_epoch: epoch,
        client_host: format!("/{client}"),
        client_id: client,
        topology_epoch: 1,
        process_id: process.to_string(),
        assignment: assignment.clone(),
        target_assignment: assignment,
        ..Default::default()
    }
}

/// A heartbeat that also reports `standby` as owned.
fn owning_standby(
    request: StreamsGroupHeartbeatRequest,
    standby: &[(&str, &[i32])],
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        standby_tasks: Some(task_ids(standby)),
        ..request
    }
}

/// A response that also assigns `standby`.
fn assigning_standby(
    response: StreamsGroupHeartbeatResponse,
    standby: &[(&str, &[i32])],
) -> StreamsGroupHeartbeatResponse {
    let standby = task_ids(standby)
        .into_iter()
        .map(|t| TaskIds {
            subtopology_id: t.subtopology_id,
            partitions: t.partitions,
            ..Default::default()
        })
        .collect();
    StreamsGroupHeartbeatResponse {
        standby_tasks: Some(standby),
        ..response
    }
}

#[test]
fn tasks_are_shared_and_rebalanced_when_a_member_joins() {
    let mut c = Coordinator::new(1, undelayed());
    let topics = Topics::new(&[
        ("in", 3),
        ("app-repartition", 3),
        ("app-store-changelog", 3),
        ("app1-changelog", 3),
    ]);
    run_steps(&mut c, &topics, three_member_steps());
    let member = |id, process, active| described_member(id, process, 4, active, &[]);
    let describe = StreamsGroupDescribeRequest {
        group_ids: vec!["app".to_string()],
        ..Default::default()
    };
    assert!(
        c.streams_group_describe(&describe).groups
            == vec![DescribedGroup {
                group_id: "app".to_string(),
                group_state: "Stable".to_string(),
                group_epoch: 4,
                assignment_epoch: 4,
                topology: Some(described_topology(3)),
                members: vec![
                    member("m1", "p1", &[("0", &[0]), ("1", &[0])]),
                    member("m2", "p2", &[("0", &[2]), ("1", &[1])]),
                    member("m3", "p3", &[("0", &[1]), ("1", &[2])]),
                ],
                ..Default::default()
            }]
    );
}

/// `num.standby.replicas = 1`: every stateful task gets a standby copy on
/// the other process. A standby copy of a task that the same process still
/// runs as active waits until that process released it (Kafka's
/// `isUnreleasedStandbyTask`).
#[test]
fn standby_replicas_land_on_another_process() {
    let config = CoordinatorConfig {
        streams_num_standby_replicas: 1,
        ..undelayed()
    };
    let mut c = Coordinator::new(1, config);
    let topics = Topics::new(&[
        ("in", 2),
        ("app-repartition", 2),
        ("app-store-changelog", 2),
        ("app1-changelog", 2),
    ]);
    let all: &[(&str, &[i32])] = &[("0", &[0, 1]), ("1", &[0, 1])];
    let first: &[(&str, &[i32])] = &[("0", &[0]), ("1", &[0])];
    let second: &[(&str, &[i32])] = &[("0", &[1]), ("1", &[1])];
    let steps: Vec<Step> = vec![
        (
            "m1 joins alone: every task active, no other process for a standby",
            join("m1", "p1"),
            accepted("m1", 2, vec![], Some(all)),
        ),
        (
            "m2 joins: it takes its standby copies at once, its active tasks are still m1's",
            join("m2", "p2"),
            assigning_standby(accepted("m2", 3, vec![], Some(&[])), first),
        ),
        (
            "m1 revokes the tasks m2 will run",
            heartbeat("m1", 2, Some(all)),
            accepted("m1", 2, vec![], Some(first)),
        ),
        (
            "m1 confirms; its standby copies wait for p1 to release those tasks",
            heartbeat("m1", 2, Some(first)),
            accepted("m1", 3, vec![], None),
        ),
        (
            "m2 takes the released active tasks",
            owning_standby(heartbeat("m2", 3, Some(&[])), first),
            assigning_standby(accepted("m2", 3, vec![], Some(second)), first),
        ),
        (
            "m1 takes its standby copies",
            heartbeat("m1", 3, Some(first)),
            assigning_standby(accepted("m1", 3, vec![], Some(first)), second),
        ),
    ];
    run_steps(&mut c, &topics, steps);
    let describe = StreamsGroupDescribeRequest {
        group_ids: vec!["app".to_string()],
        ..Default::default()
    };
    assert!(
        c.streams_group_describe(&describe).groups
            == vec![DescribedGroup {
                group_id: "app".to_string(),
                group_state: "Stable".to_string(),
                group_epoch: 3,
                assignment_epoch: 3,
                topology: Some(described_topology(2)),
                members: vec![
                    described_member("m1", "p1", 3, first, second),
                    described_member("m2", "p2", 3, second, first),
                ],
                ..Default::default()
            }]
    );
}

#[test]
fn topology_epochs_and_shutdown_are_checked() {
    let mut c = Coordinator::new(1, undelayed());
    let topics = Topics::new(&[
        ("in", 3),
        ("app-repartition", 3),
        ("app-store-changelog", 3),
        ("app1-changelog", 3),
    ]);
    let all: &[(&str, &[i32])] = &[("0", &[0, 1, 2]), ("1", &[0, 1, 2])];
    let (response, _) = c.streams_group_heartbeat(0, &client("c1"), &join("m1", "p1"), &topics);
    assert!(response == accepted("m1", 2, vec![], Some(all)));
    let ahead = StreamsGroupHeartbeatRequest {
        topology: Some(topology(2)),
        ..join("m2", "p2")
    };
    let (response, _) = c.streams_group_heartbeat(1000, &client("c2"), &ahead, &topics);
    assert!(
        response
            == refused(
                codes::STREAMS_INVALID_TOPOLOGY_EPOCH,
                "The member's topology epoch 2 is ahead of the group's topology epoch 1."
            )
    );
    let mut different = topology(1);
    different.subtopologies.pop();
    let changed = StreamsGroupHeartbeatRequest {
        topology: Some(different),
        ..join("m2", "p2")
    };
    let (response, _) = c.streams_group_heartbeat(1000, &client("c2"), &changed, &topics);
    assert!(
        response
            == refused(
                codes::INVALID_REQUEST,
                "Topology updates are not supported yet."
            )
    );
    let late = StreamsGroupHeartbeatRequest {
        topology: Some(topology(1)),
        ..heartbeat("m1", 2, None)
    };
    let (response, _) = c.streams_group_heartbeat(1000, &client("c1"), &late, &topics);
    assert!(
        response
            == refused(
                codes::INVALID_REQUEST,
                "Topology can only be provided when (re-)joining."
            )
    );
    let (response, _) =
        c.streams_group_heartbeat(1000, &client("c1"), &heartbeat("m1", 7, None), &topics);
    assert!(
        response
            == refused(
                codes::FENCED_MEMBER_EPOCH,
                "The streams group member has a greater member epoch (7) than the one known by the group coordinator (2). The member must abandon all its partitions and rejoin."
            )
    );
    // A shutdown request reaches every member until the group is empty.
    let shutdown = StreamsGroupHeartbeatRequest {
        shutdown_application: true,
        ..heartbeat("m1", 2, None)
    };
    let (response, _) = c.streams_group_heartbeat(2000, &client("c1"), &shutdown, &topics);
    let detail = "Streams group member m1 encountered a fatal error and requested a shutdown for the entire application.";
    assert!(response == accepted("m1", 2, vec![status(SHUTDOWN_APPLICATION, detail)], None));
    let (response, _) =
        c.streams_group_heartbeat(3000, &client("c1"), &heartbeat("m1", -1, None), &topics);
    assert!(
        response
            == StreamsGroupHeartbeatResponse {
                member_id: "m1".to_string(),
                member_epoch: -1,
                status: Some(Vec::new()),
                ..Default::default()
            }
    );
    let (response, _) = c.streams_group_heartbeat(4000, &client("c2"), &join("m2", "p2"), &topics);
    assert!(response == accepted("m2", 4, vec![], Some(all)));
}

/// Kafka 4.3's `group.streams.initial.rebalance.delay.ms` (3 s) and
/// `group.streams.assignment.interval.ms` (1 s), with the defaults. A member
/// that joins the empty group starts the delay: until it ends every member
/// reconciles toward an empty target at the initial target epoch 1, with
/// `ASSIGNMENT_DELAYED`. When the delay ends the target covers every member
/// that joined; a later join within a second of it waits for the interval.
#[test]
fn assignment_waits_for_the_initial_delay_and_the_interval() {
    let mut c = coord();
    let topics = Topics::new(&[("in", 2)]);
    let join = |member: &str, process: &str| StreamsGroupHeartbeatRequest {
        topology: Some(Topology {
            epoch: 1,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".to_string(),
                source_topics: vec!["in".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..join(member, process)
    };
    let initial_delay = || {
        vec![status(
            ASSIGNMENT_DELAYED,
            "Assignment delayed due to the configured initial rebalance delay.",
        )]
    };
    let interval = || {
        vec![status(
            ASSIGNMENT_DELAYED,
            "Assignment delayed due to the configured assignment interval.",
        )]
    };
    let step = |c: &mut Coordinator, now: u64, request: StreamsGroupHeartbeatRequest, want| {
        let client = client(&request.member_id.replace('m', "c"));
        let (response, _) = c.streams_group_heartbeat(now, &client, &request, &topics);
        assert!(response == want, "{} at {now}", request.member_id);
    };
    step(
        &mut c,
        0,
        join("m1", "p1"),
        accepted("m1", 1, initial_delay(), Some(&[])),
    );
    step(
        &mut c,
        500,
        join("m2", "p2"),
        accepted("m2", 1, initial_delay(), Some(&[])),
    );
    assert!(c.next_deadline() == Some(3000));
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("app", "streams", "Assigning", "streams")],
                ..Default::default()
            }
    );
    assert!(c.on_tick(3000).is_empty());
    step(
        &mut c,
        3100,
        heartbeat("m1", 1, None),
        accepted("m1", 3, vec![], Some(&[("0", &[0])])),
    );
    step(
        &mut c,
        3200,
        heartbeat("m2", 1, None),
        accepted("m2", 3, vec![], Some(&[("0", &[1])])),
    );
    step(
        &mut c,
        3500,
        join("m3", "p3"),
        accepted("m3", 3, interval(), Some(&[])),
    );
    step(
        &mut c,
        3600,
        heartbeat("m1", 3, Some(&[("0", &[0])])),
        accepted("m1", 3, interval(), None),
    );
    step(
        &mut c,
        4000,
        heartbeat("m1", 3, Some(&[("0", &[0])])),
        accepted("m1", 4, vec![], None),
    );
    step(
        &mut c,
        4100,
        heartbeat("m3", 3, Some(&[])),
        accepted("m3", 4, vec![], None),
    );
}

/// Kafka 4.3's `streamsGroupDescribe`: a group that does not exist and a
/// group of another kind are described with `GROUP_ID_NOT_FOUND` and the
/// message of Kafka's lookup.
#[test]
fn describe_refuses_missing_groups_and_other_kinds() {
    let c = stable_two_member_group();
    let described = c.streams_group_describe(&StreamsGroupDescribeRequest {
        group_ids: vec!["nope".to_string(), "g".to_string()],
        ..Default::default()
    });
    let not_found = |group_id: &str, message: &str| DescribedGroup {
        group_id: group_id.to_string(),
        error_code: codes::GROUP_ID_NOT_FOUND,
        error_message: Some(message.to_string()),
        ..Default::default()
    };
    assert!(
        described.groups
            == vec![
                not_found("nope", "Group nope not found."),
                not_found("g", "Group g is not a streams group."),
            ]
    );
}
