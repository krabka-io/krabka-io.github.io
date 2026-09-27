//! `OffsetCommit` and `OffsetFetch`.

use assert2::assert;
use krabka_protocol::owned::{
    consumer_group_heartbeat_request::{
        ConsumerGroupHeartbeatRequest, TopicPartitions as ConsumerTopicPartitions,
    },
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_fetch_request::{
        OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
        OffsetFetchRequestTopics,
    },
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};

use super::*;

fn commit_req(
    group: &str,
    generation: i32,
    member: &str,
    topics: &[(&str, &[(i32, i64)])],
) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: group.to_string(),
        generation_id_or_member_epoch: generation,
        member_id: member.to_string(),
        topics: topics
            .iter()
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                name: (*name).to_string(),
                partitions: partitions
                    .iter()
                    .map(|(partition, offset)| OffsetCommitRequestPartition {
                        partition_index: *partition,
                        committed_offset: *offset,
                        committed_leader_epoch: 3,
                        committed_metadata: Some("m".to_string()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The response rows of a commit below v10, whose topic ids Kafka resolves
/// from the names: the zero id for a topic that does not exist.
fn commit_resp(metadata: &Topics, topics: &[(&str, &[(i32, i16)])]) -> OffsetCommitResponse {
    OffsetCommitResponse {
        throttle_time_ms: 0,
        topics: topics
            .iter()
            .map(|(name, partitions)| OffsetCommitResponseTopic {
                name: (*name).to_string(),
                topic_id: metadata.topic_id(name).unwrap_or(Uuid::ZERO),
                partitions: partitions
                    .iter()
                    .map(|(partition, error_code)| OffsetCommitResponsePartition {
                        partition_index: *partition,
                        error_code: *error_code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn fetch_groups_req(group: &str, topics: Option<&[(&str, &[i32])]>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        groups: vec![OffsetFetchRequestGroup {
            group_id: group.to_string(),
            member_id: None,
            member_epoch: -1,
            topics: topics.map(|topics| {
                topics
                    .iter()
                    .map(|(name, partitions)| OffsetFetchRequestTopics {
                        name: (*name).to_string(),
                        partition_indexes: partitions.to_vec(),
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn row(partition: i32, offset: i64) -> OffsetFetchResponsePartitions {
    if offset < 0 {
        OffsetFetchResponsePartitions {
            partition_index: partition,
            committed_offset: -1,
            committed_leader_epoch: -1,
            metadata: Some(String::new()),
            error_code: codes::NONE,
            ..Default::default()
        }
    } else {
        OffsetFetchResponsePartitions {
            partition_index: partition,
            committed_offset: offset,
            committed_leader_epoch: 3,
            metadata: Some("m".to_string()),
            error_code: codes::NONE,
            ..Default::default()
        }
    }
}

fn fetch_groups_resp(
    group: &str,
    error_code: i16,
    topics: Vec<OffsetFetchResponseTopics>,
) -> OffsetFetchResponse {
    OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: Vec::new(),
        error_code: codes::NONE,
        groups: vec![OffsetFetchResponseGroup {
            group_id: group.to_string(),
            topics,
            error_code,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// `stable_two_member_group` after its members committed `t-0 = 11`,
/// `t-1 = 20` and `u-0 = 5` at generation 1.
fn committed_group() -> (Coordinator, Topics) {
    let mut c = stable_two_member_group();
    let topics = Topics::new(&[("t", 4), ("u", 1)]);
    let first = c.offset_commit(
        7000,
        &commit_req("g", 1, M1, &[("t", &[(0, 10), (1, 20)])]),
        8,
        &topics,
    );
    assert!(first == commit_resp(&topics, &[("t", &[(0, codes::NONE), (1, codes::NONE)])]));
    let second = c.offset_commit(
        7100,
        &commit_req("g", 1, M2, &[("t", &[(0, 11)]), ("u", &[(0, 5)])]),
        8,
        &topics,
    );
    assert!(
        second
            == commit_resp(
                &topics,
                &[("t", &[(0, codes::NONE)]), ("u", &[(0, codes::NONE)])]
            )
    );
    (c, topics)
}

/// Kafka's `ClassicGroup.validateOffsetCommit`: a commit from a group with
/// members names a member of the current generation, and a refused commit
/// changes nothing.
#[test]
fn commits_are_checked_against_the_generation() {
    let (mut c, topics) = committed_group();
    for (name, generation, member, want) in [
        ("stale generation", 0, M1, codes::ILLEGAL_GENERATION),
        ("unknown member", 1, "ghost", codes::UNKNOWN_MEMBER_ID),
        (
            "no member on a live group",
            -1,
            "",
            codes::UNKNOWN_MEMBER_ID,
        ),
    ] {
        let request = commit_req("g", generation, member, &[("t", &[(0, 99)])]);
        let response = c.offset_commit(7200, &request, 8, &topics);
        assert!(
            response == commit_resp(&topics, &[("t", &[(0, want)])]),
            "{name}"
        );
    }
    assert!(
        c.offset_fetch(&fetch_groups_req("g", Some(&[("t", &[0])])), 8, &topics)
            == fetch_groups_resp(
                "g",
                codes::NONE,
                vec![OffsetFetchResponseTopics {
                    name: "t".to_string(),
                    partitions: vec![row(0, 11)],
                    ..Default::default()
                }]
            )
    );
}

/// The per-group shape (v8+) reads the named partitions, a partition nobody
/// committed as `-1`, and every committed offset for a null topic list; a
/// group the coordinator does not hold has no offsets and no error.
#[test]
fn fetch_reads_the_commits_of_a_group() {
    let (c, topics) = committed_group();
    let topic = |name: &str, topic_id: Uuid, partitions| OffsetFetchResponseTopics {
        name: name.to_string(),
        topic_id,
        partitions,
        ..Default::default()
    };
    let named = fetch_groups_req("g", Some(&[("t", &[0, 1, 2]), ("v", &[0])]));
    assert!(
        c.offset_fetch(&named, 8, &topics)
            == fetch_groups_resp(
                "g",
                codes::NONE,
                vec![
                    topic("t", Uuid::ZERO, vec![row(0, 11), row(1, 20), row(2, -1)]),
                    topic("v", Uuid::ZERO, vec![row(0, -1)]),
                ]
            )
    );
    assert!(
        c.offset_fetch(&fetch_groups_req("g", None), 8, &topics)
            == fetch_groups_resp(
                "g",
                codes::NONE,
                vec![
                    topic("t", topics.id("t"), vec![row(0, 11), row(1, 20)]),
                    topic("u", topics.id("u"), vec![row(0, 5)]),
                ]
            )
    );
    assert!(
        c.offset_fetch(&fetch_groups_req("nope", Some(&[("t", &[7])])), 8, &topics)
            == fetch_groups_resp(
                "nope",
                codes::NONE,
                vec![topic("t", Uuid::ZERO, vec![row(7, -1)])]
            )
    );
}

/// The single-group shape below v8.
#[test]
fn fetch_reads_the_commits_on_the_legacy_shape() {
    let (c, topics) = committed_group();
    let request = OffsetFetchRequest {
        group_id: "g".to_string(),
        topics: Some(vec![OffsetFetchRequestTopic {
            name: "t".to_string(),
            partition_indexes: vec![1, 3],
            ..Default::default()
        }]),
        ..Default::default()
    };
    assert!(
        c.offset_fetch(&request, 7, &topics)
            == OffsetFetchResponse {
                throttle_time_ms: 0,
                topics: vec![OffsetFetchResponseTopic {
                    name: "t".to_string(),
                    partitions: vec![
                        OffsetFetchResponsePartition {
                            partition_index: 1,
                            committed_offset: 20,
                            committed_leader_epoch: 3,
                            metadata: Some("m".to_string()),
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                        OffsetFetchResponsePartition {
                            partition_index: 3,
                            committed_offset: -1,
                            committed_leader_epoch: -1,
                            metadata: Some(String::new()),
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                error_code: codes::NONE,
                ..Default::default()
            }
    );
}

/// A commit with generation -1 from a client without group management
/// creates a simple group; a commit to a group the coordinator does not hold
/// with a generation is `GROUP_ID_NOT_FOUND` from v9 and `ILLEGAL_GENERATION`
/// before.
#[test]
fn standalone_commits_create_a_simple_group() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 4)]);
    assert!(
        c.offset_commit(
            0,
            &commit_req("solo", -1, "", &[("t", &[(0, 42)])]),
            8,
            &topics
        ) == commit_resp(&topics, &[("t", &[(0, codes::NONE)])])
    );
    assert!(
        c.offset_commit(
            0,
            &commit_req("managed", 5, "m", &[("t", &[(0, 42)])]),
            9,
            &topics
        ) == commit_resp(&topics, &[("t", &[(0, codes::GROUP_ID_NOT_FOUND)])])
    );
    assert!(
        c.offset_commit(
            0,
            &commit_req("managed", 5, "m", &[("t", &[(0, 42)])]),
            8,
            &topics
        ) == commit_resp(&topics, &[("t", &[(0, codes::ILLEGAL_GENERATION)])])
    );
    assert!(
        c.list_groups(&ListGroupsRequest::default())
            == ListGroupsResponse {
                groups: vec![listed("solo", "", "Empty", "classic")],
                ..Default::default()
            }
    );
    assert!(
        c.offset_fetch(&fetch_groups_req("solo", None), 8, &topics)
            == fetch_groups_resp(
                "solo",
                codes::NONE,
                vec![OffsetFetchResponseTopics {
                    name: "t".to_string(),
                    topic_id: topics.id("t"),
                    partitions: vec![row(0, 42)],
                    ..Default::default()
                },]
            )
    );
}

/// Kafka's `KafkaApis.handleOffsetCommitRequest` checks the topics before the
/// group: a topic or partition that does not exist is refused, only the rest
/// reaches the group, and `OffsetCommitResponse.Builder` lists the refused
/// rows first and merges the group's rows into them by topic name.
#[test]
fn topics_are_checked_before_the_group() {
    use crate::lab::codes::{GROUP_ID_NOT_FOUND, NONE, UNKNOWN_TOPIC_OR_PARTITION as UNKNOWN};
    /// The committed `(partition, offset)` pairs of each topic.
    type Commits = &'static [(&'static str, &'static [(i32, i64)])];
    /// The `(partition, error)` rows of each topic.
    type Rows = &'static [(&'static str, &'static [(i32, i16)])];
    let cases: [(&str, i32, Commits, Rows, bool); 5] = [
        (
            "a missing partition and topic are refused and the rest committed",
            -1,
            &[
                ("t", &[(9, 1), (0, 2)]),
                ("nope", &[(0, 3)]),
                ("u", &[(0, 4)]),
            ],
            &[
                ("t", &[(9, UNKNOWN), (0, NONE)]),
                ("nope", &[(0, UNKNOWN)]),
                ("u", &[(0, NONE)]),
            ],
            true,
        ),
        (
            "a commit with no partition left creates no group",
            -1,
            &[("nope", &[(0, 3)]), ("t", &[(4, 1)])],
            &[("nope", &[(0, UNKNOWN)]), ("t", &[(4, UNKNOWN)])],
            false,
        ),
        (
            "the group's error joins the refused rows",
            5,
            &[("u", &[(0, 4)]), ("t", &[(0, 1), (7, 2)])],
            &[
                ("t", &[(7, UNKNOWN), (0, GROUP_ID_NOT_FOUND)]),
                ("u", &[(0, GROUP_ID_NOT_FOUND)]),
            ],
            false,
        ),
        (
            "once a partition is refused a topic named twice gathers in one row",
            -1,
            &[("t", &[(0, 1)]), ("u", &[(3, 2)]), ("t", &[(1, 5)])],
            &[("u", &[(3, UNKNOWN)]), ("t", &[(0, NONE), (1, NONE)])],
            true,
        ),
        (
            "without a refused partition the group's rows stand as they are",
            -1,
            &[("t", &[(0, 1)]), ("t", &[(1, 5)])],
            &[("t", &[(0, NONE)]), ("t", &[(1, NONE)])],
            true,
        ),
    ];
    let topics = Topics::new(&[("t", 4), ("u", 1)]);
    for (name, generation, request, response, creates_group) in cases {
        let mut c = coord();
        let request = commit_req("solo", generation, "", request);
        assert!(
            c.offset_commit(0, &request, 9, &topics) == commit_resp(&topics, response),
            "{name}"
        );
        let groups = if creates_group {
            vec![listed("solo", "", "Empty", "classic")]
        } else {
            Vec::new()
        };
        assert!(
            c.list_groups(&ListGroupsRequest::default())
                == ListGroupsResponse {
                    groups,
                    ..Default::default()
                },
            "{name}"
        );
    }
}

/// Kafka's `isMetadataInvalid` measures the metadata as Java's
/// `String.length` does: 4096 UTF-16 code units pass whatever their UTF-8
/// length, and longer metadata refuses its own partition alone.
#[test]
fn metadata_is_measured_in_utf16_code_units() {
    let topics = Topics::new(&[("t", 4)]);
    for (name, metadata, want) in [
        ("4096 two-byte characters", "é".repeat(4096), codes::NONE),
        (
            "4097 one-byte characters",
            "a".repeat(4097),
            codes::OFFSET_METADATA_TOO_LARGE,
        ),
        ("2048 surrogate pairs", "🦀".repeat(2048), codes::NONE),
        (
            "2049 surrogate pairs",
            "🦀".repeat(2049),
            codes::OFFSET_METADATA_TOO_LARGE,
        ),
    ] {
        let mut c = coord();
        let mut request = commit_req("solo", -1, "", &[("t", &[(0, 10), (1, 20)])]);
        request.topics[0].partitions[0].committed_metadata = Some(metadata.clone());
        assert!(
            c.offset_commit(0, &request, 9, &topics)
                == commit_resp(&topics, &[("t", &[(0, want), (1, codes::NONE)])]),
            "{name}"
        );
        let first = if want == codes::NONE {
            OffsetFetchResponsePartitions {
                metadata: Some(metadata),
                ..row(0, 10)
            }
        } else {
            row(0, -1)
        };
        assert!(
            c.offset_fetch(
                &fetch_groups_req("solo", Some(&[("t", &[0, 1])])),
                9,
                &topics
            ) == fetch_groups_resp(
                "solo",
                codes::NONE,
                vec![OffsetFetchResponseTopics {
                    name: "t".to_string(),
                    partitions: vec![first, row(1, 20)],
                    ..Default::default()
                }]
            ),
            "{name}"
        );
    }
}

/// At v10 the topics come by id: an id the metadata does not know is
/// `UNKNOWN_TOPIC_ID`, and its refused row comes ahead of the committed one.
#[test]
fn topic_ids_are_resolved_at_v10() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 4)]);
    let unknown = Uuid([9; 16]);
    let request = OffsetCommitRequest {
        topics: vec![
            OffsetCommitRequestTopic {
                topic_id: topics.id("t"),
                ..commit_req("solo", -1, "", &[("", &[(0, 7)])])
                    .topics
                    .remove(0)
            },
            OffsetCommitRequestTopic {
                topic_id: unknown,
                ..commit_req("solo", -1, "", &[("", &[(0, 7)])])
                    .topics
                    .remove(0)
            },
        ],
        ..commit_req("solo", -1, "", &[])
    };
    assert!(
        c.offset_commit(0, &request, 10, &topics)
            == OffsetCommitResponse {
                throttle_time_ms: 0,
                topics: vec![
                    OffsetCommitResponseTopic {
                        topic_id: unknown,
                        partitions: vec![OffsetCommitResponsePartition {
                            partition_index: 0,
                            error_code: codes::UNKNOWN_TOPIC_ID,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    OffsetCommitResponseTopic {
                        name: "t".to_string(),
                        topic_id: topics.id("t"),
                        partitions: vec![OffsetCommitResponsePartition {
                            partition_index: 0,
                            error_code: codes::NONE,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
    );
    let fetch = OffsetFetchRequest {
        groups: vec![OffsetFetchRequestGroup {
            group_id: "solo".to_string(),
            member_id: None,
            member_epoch: -1,
            topics: Some(vec![
                OffsetFetchRequestTopics {
                    topic_id: topics.id("t"),
                    partition_indexes: vec![0],
                    ..Default::default()
                },
                OffsetFetchRequestTopics {
                    topic_id: unknown,
                    partition_indexes: vec![0],
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        c.offset_fetch(&fetch, 10, &topics)
            == fetch_groups_resp(
                "solo",
                codes::NONE,
                vec![
                    OffsetFetchResponseTopics {
                        name: "t".to_string(),
                        topic_id: topics.id("t"),
                        partitions: vec![row(0, 7)],
                        ..Default::default()
                    },
                    OffsetFetchResponseTopics {
                        topic_id: unknown,
                        partitions: vec![OffsetFetchResponsePartitions {
                            error_code: codes::UNKNOWN_TOPIC_ID,
                            ..row(0, -1)
                        }],
                        ..Default::default()
                    },
                ]
            )
    );
}

/// A consumer group where `m1` holds `t-0`, assigned at epoch 2, at member
/// epoch 3: `m1` took both partitions at epoch 2, `m2` joined at epoch 3,
/// and `m1` gave `t-1` up.
fn reconciled_consumer_group() -> (Coordinator, Topics) {
    let mut c = Coordinator::new(1, undelayed());
    let topics = Topics::new(&[("t", 2)]);
    let t = topics.id("t");
    let heartbeat = |member: &str, epoch: i32, owned: &[i32]| ConsumerGroupHeartbeatRequest {
        group_id: "cg".to_string(),
        member_id: member.to_string(),
        member_epoch: epoch,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: (epoch == 0).then(|| vec!["t".to_string()]),
        topic_partitions: Some(if owned.is_empty() {
            Vec::new()
        } else {
            vec![ConsumerTopicPartitions {
                topic_id: t,
                partitions: owned.to_vec(),
                ..Default::default()
            }]
        }),
        ..Default::default()
    };
    for (member, epoch, owned, want_epoch) in [
        ("m1", 0, &[][..], 2),
        ("m2", 0, &[][..], 3),
        ("m1", 2, &[0, 1][..], 2),
        ("m1", 2, &[0][..], 3),
    ] {
        let response = c.consumer_group_heartbeat(
            0,
            &client(member),
            &heartbeat(member, epoch, owned),
            1,
            &topics,
        );
        assert!(response.member_epoch == want_epoch, "{member} at {epoch}");
    }
    (c, topics)
}

/// Kafka 4.3's `ConsumerGroup.validateOffsetCommit`: a member commits at its
/// epoch from v9 on; a newer epoch is stale; an older one (KIP-1251) commits
/// a partition the member was assigned at that epoch or before, and any other
/// partition fails the whole commit.
#[test]
fn consumer_group_members_commit_at_their_epoch() {
    let (mut c, topics) = reconciled_consumer_group();
    let stale = codes::STALE_MEMBER_EPOCH;
    for (name, member, epoch, version, partitions, want) in [
        ("current epoch", "m1", 3, 9, &[0][..], codes::NONE),
        ("newer epoch", "m1", 4, 9, &[0][..], stale),
        (
            "older epoch, assigned by then",
            "m1",
            2,
            9,
            &[0][..],
            codes::NONE,
        ),
        ("older epoch, not assigned", "m1", 2, 9, &[1][..], stale),
        ("older than the assignment", "m1", 1, 9, &[0][..], stale),
        (
            "one partition fails them all",
            "m1",
            2,
            9,
            &[0, 1][..],
            stale,
        ),
        (
            "before v9",
            "m1",
            3,
            8,
            &[0][..],
            codes::UNSUPPORTED_VERSION,
        ),
        (
            "unknown member",
            "m9",
            3,
            9,
            &[0][..],
            codes::UNKNOWN_MEMBER_ID,
        ),
    ] {
        let offsets: Vec<(i32, i64)> = partitions.iter().map(|p| (*p, 5)).collect();
        let response = c.offset_commit(
            1000,
            &commit_req("cg", epoch, member, &[("t", &offsets)]),
            version,
            &topics,
        );
        let rows: Vec<(i32, i16)> = partitions.iter().map(|p| (*p, want)).collect();
        assert!(response == commit_resp(&topics, &[("t", &rows)]), "{name}");
    }
}

/// Kafka 4.3's `ConsumerGroup.validateOffsetFetch`: a fetch names the
/// member's current epoch, or no member and no epoch.
#[test]
fn consumer_group_members_fetch_at_their_epoch() {
    let (mut c, topics) = reconciled_consumer_group();
    let commit = commit_req("cg", 3, "m1", &[("t", &[(0, 5)])]);
    assert!(
        c.offset_commit(1000, &commit, 9, &topics)
            == commit_resp(&topics, &[("t", &[(0, codes::NONE)])])
    );
    for (name, member_id, member_epoch, want) in [
        ("no member and no epoch", None, -1, codes::NONE),
        ("current epoch", Some("m1"), 3, codes::NONE),
        ("older epoch", Some("m1"), 2, codes::STALE_MEMBER_EPOCH),
        ("unknown member", Some("m9"), 3, codes::UNKNOWN_MEMBER_ID),
        (
            "an epoch without a member",
            None,
            3,
            codes::UNKNOWN_MEMBER_ID,
        ),
    ] {
        let request = OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "cg".to_string(),
                member_id: member_id.map(str::to_string),
                member_epoch,
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "t".to_string(),
                    partition_indexes: vec![0, 1],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let rows = if want == codes::NONE {
            vec![OffsetFetchResponseTopics {
                name: "t".to_string(),
                partitions: vec![row(0, 5), row(1, -1)],
                ..Default::default()
            }]
        } else {
            Vec::new()
        };
        assert!(
            c.offset_fetch(&request, 9, &topics) == fetch_groups_resp("cg", want, rows),
            "{name}"
        );
    }
}

/// Kafka refuses only a null group id for these requests, and the wire cannot
/// carry one: the empty group id commits, fetches and describes like any
/// other simple group.
#[test]
fn the_empty_group_id_is_an_ordinary_group_for_offsets() {
    let mut c = coord();
    let topics = Topics::new(&[("t", 1)]);
    assert!(
        c.offset_commit(0, &commit_req("", -1, "", &[("t", &[(0, 3)])]), 8, &topics)
            == commit_resp(&topics, &[("t", &[(0, codes::NONE)])])
    );
    assert!(
        c.offset_fetch(&fetch_groups_req("", Some(&[("t", &[0])])), 8, &topics)
            == fetch_groups_resp(
                "",
                codes::NONE,
                vec![OffsetFetchResponseTopics {
                    name: "t".to_string(),
                    partitions: vec![row(0, 3)],
                    ..Default::default()
                }]
            )
    );
    assert!(
        c.describe_groups(&describe_req(&[""]), 6)
            == DescribeGroupsResponse {
                groups: vec![DescribedGroup {
                    group_state: "Empty".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
}
