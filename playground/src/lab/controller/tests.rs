//! The quorum tests: elections, replication and catch-up over the hand-wired
//! [`Cluster`].

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{DeleteTopicRecord, MetadataRecord};
use krabka_protocol::owned::{
    common::describe_quorum_response::replica_state::ReplicaState,
    describe_quorum_response::{
        DescribeQuorumResponse, Listener, Node as WireNode, PartitionData, TopicData,
    },
};
use serde_json::json;

use super::{
    CommittedBatch, ControllerCore, DurableQuorumState, HIGH_WATERMARK_KEY, KRAFT_LOG_STORE,
    KRAFT_STATE_STORE, NotLeader, ProposalId, QUORUM_STATE_KEY, RAFT_PORT, RaftMessage,
    harness::{CLUSTER_ID, Cluster, ControllerNode},
};
use crate::lab::{
    codes,
    net::{ConnId, DurableImage, DurableOp, Endpoint, Frame, Millis, Node, NodeId, Payload},
    testing::CtxBuffers,
};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const VOTERS: [NodeId; 3] = [N1, N2, N3];

/// The time a healthy three-voter cluster must have a settled leader by.
const ELECTION_BUDGET_MS: Millis = 5_000;

fn delete(name: &str) -> Vec<MetadataRecord> {
    vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
        name: name.into(),
    })]
}

fn three() -> Cluster {
    Cluster::new(&VOTERS, &[])
}

fn settle(cluster: &mut Cluster, budget: Millis) -> NodeId {
    assert!(
        cluster.run_until(|c| c.settled_leader().is_some(), budget),
        "no settled leader by {} ms: leaders {:?}",
        cluster.now(),
        cluster.leaders()
    );
    cluster.settled_leader().expect("settled")
}

/// Propose on `leader` and wait until every live node handed the batch out.
fn propose_and_commit(
    cluster: &mut Cluster,
    leader: NodeId,
    records: Vec<MetadataRecord>,
) -> ProposalId {
    let id = cluster
        .with_node(leader, |core, ctx| core.propose(ctx, records))
        .expect("the leader accepts");
    assert!(
        cluster.run_until(
            |c| c
                .live_ids()
                .iter()
                .all(|&n| c.node(n).high_watermark() > id.0),
            ELECTION_BUDGET_MS
        ),
        "offset {id} did not commit everywhere by {} ms",
        cluster.now()
    );
    id
}

/// Which fault the election runs under.
#[derive(Debug, Clone, Copy)]
enum Fault {
    None,
    /// Every frame the first leader sends is dropped once it is elected.
    DropLeaderFrames,
    /// Random extra latency per frame, so frames overtake each other.
    Reorder,
}

#[test]
fn three_voters_elect_one_leader_under_each_fault() {
    for fault in [Fault::None, Fault::DropLeaderFrames, Fault::Reorder] {
        let mut cluster = three();
        if let Fault::Reorder = fault {
            cluster.set_jitter(40);
        }
        let first = settle(&mut cluster, ELECTION_BUDGET_MS);
        let first_epoch = cluster.node(first).epoch();
        assert!(first_epoch >= 1, "{fault:?}");
        let leader = match fault {
            Fault::DropLeaderFrames => {
                cluster.set_drop(move |frame: &Frame| frame.src.node == first);
                assert!(
                    cluster.run_until(
                        |c| c.leaders().iter().any(|&l| l != first)
                            && c.node(first).epoch() > first_epoch,
                        10_000
                    ),
                    "{fault:?}: no new leader by {} ms",
                    cluster.now()
                );
                cluster.clear_drop();
                let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
                assert!(leader != first, "{fault:?}");
                assert!(cluster.node(leader).epoch() > first_epoch, "{fault:?}");
                leader
            }
            Fault::None | Fault::Reorder => first,
        };
        cluster.assert_one_leader_per_epoch();
        let roles: Vec<&str> = cluster
            .ids()
            .iter()
            .map(|&n| cluster.node(n).role_name())
            .collect();
        assert!(
            roles.iter().filter(|r| **r == "Leader").count() == 1,
            "{fault:?}: {roles:?}"
        );
        assert!(
            roles.iter().filter(|r| **r == "Follower").count() == 2,
            "{fault:?}: {roles:?}"
        );
        // A proposal on the leader commits on all three, in the same order.
        let a = propose_and_commit(&mut cluster, leader, delete("a"));
        let b = propose_and_commit(&mut cluster, leader, delete("b"));
        assert!(b.0 == a.0 + 1, "{fault:?}");
        let committed: Vec<Vec<CommittedBatch>> = cluster
            .ids()
            .into_iter()
            .map(|n| cluster.with_node(n, |core, _| core.take_committed()))
            .collect();
        assert!(committed[0] == committed[1], "{fault:?}");
        assert!(committed[1] == committed[2], "{fault:?}");
        let tail = &committed[0][committed[0].len() - 2..];
        let epoch = cluster.node(leader).epoch();
        assert!(
            tail == [
                CommittedBatch {
                    offset: a.0,
                    epoch,
                    records: delete("a")
                },
                CommittedBatch {
                    offset: b.0,
                    epoch,
                    records: delete("b")
                }
            ],
            "{fault:?}"
        );
        // Every batch was handed out once; the next call is empty.
        for n in cluster.ids() {
            assert!(
                cluster
                    .with_node(n, |core, _| core.take_committed())
                    .is_empty()
            );
        }
        cluster.assert_one_leader_per_epoch();
    }
}

#[test]
fn the_first_election_is_deterministic_and_commits_the_leader_change() {
    let mut cluster = three();
    // Node 1 has the shortest election timeout, so it pre-votes first and wins
    // epoch 1 within a round trip of its timer.
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    assert!(leader == N1);
    assert!(cluster.node(N1).epoch() == 1);
    assert!(cluster.now() < 1_200);
    assert!(cluster.run_until(
        |c| c.ids().iter().all(|&n| c.node(n).high_watermark() == 1),
        2_000
    ));
    for n in cluster.ids() {
        let batches = cluster.with_node(n, |core, _| core.take_committed());
        assert!(
            batches
                == vec![CommittedBatch {
                    offset: 0,
                    epoch: 1,
                    records: vec![]
                }],
            "node {n}"
        );
        assert!(cluster.node(n).log_end_offset() == 1, "node {n}");
    }
    assert!(
        cluster
            .events
            .iter()
            .any(|(_, node, kind, detail)| *node == N1 && *kind == "elect" && detail["epoch"] == 1)
    );
}

#[test]
fn a_proposal_on_a_follower_names_the_leader() {
    let mut cluster = three();
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    for follower in VOTERS.iter().filter(|&&n| n != leader) {
        let result = cluster.with_node(*follower, |core, ctx| core.propose(ctx, delete("x")));
        assert!(
            result
                == Err(NotLeader {
                    leader: Some(leader)
                }),
            "follower {follower}"
        );
    }
    // Before any election nobody leads and nobody knows a leader.
    let fresh = three();
    let mut lone = ControllerNode::new(N4, &VOTERS);
    let mut buffers = CtxBuffers::new(N4);
    let result = buffers.with(0, |ctx| lone.core.propose(ctx, delete("x")));
    assert!(result == Err(NotLeader { leader: None }));
    assert!(fresh.leaders().is_empty());
}

#[test]
fn a_follower_that_missed_entries_catches_up_through_fetch() {
    let mut cluster = three();
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    let lagging = VOTERS.into_iter().find(|&n| n != leader).unwrap();
    cluster.kill(lagging);
    let mut ids = Vec::new();
    for name in ["a", "b", "c"] {
        let id = cluster
            .with_node(leader, |core, ctx| core.propose(ctx, delete(name)))
            .unwrap();
        ids.push(id);
    }
    // The majority commits without the lagging node.
    let last = ids[2].0;
    assert!(cluster.run_until(
        |c| c.node(leader).high_watermark() > last,
        ELECTION_BUDGET_MS
    ));
    assert!(cluster.node(lagging).log_end_offset() <= ids[0].0);
    cluster.restart(lagging);
    assert!(
        cluster.run_until(
            |c| c.node(lagging).high_watermark() > last && c.all_follow(leader),
            ELECTION_BUDGET_MS
        ),
        "the restarted node did not catch up by {} ms: {}",
        cluster.now(),
        cluster.node(lagging).snapshot()
    );
    let expected = cluster.with_node(leader, |core, _| core.take_committed());
    let caught_up = cluster.with_node(lagging, |core, _| core.take_committed());
    assert!(caught_up == expected);
    assert!(
        caught_up
            .iter()
            .map(|batch| batch.records.clone())
            .filter(|records| !records.is_empty())
            .collect::<Vec<_>>()
            == vec![delete("a"), delete("b"), delete("c")]
    );
    cluster.assert_one_leader_per_epoch();
}

#[test]
fn killing_the_leader_elects_another_and_the_old_one_rejoins() {
    let mut cluster = three();
    let first = settle(&mut cluster, ELECTION_BUDGET_MS);
    propose_and_commit(&mut cluster, first, delete("before"));
    cluster.kill(first);
    let successor = settle(&mut cluster, ELECTION_BUDGET_MS);
    assert!(successor != first);
    assert!(cluster.node(successor).epoch() > 1);
    let during = propose_and_commit(&mut cluster, successor, delete("during"));
    cluster.restart(first);
    assert!(
        cluster.run_until(
            |c| c.all_follow(successor) && c.node(first).high_watermark() > during.0,
            ELECTION_BUDGET_MS
        ),
        "the old leader did not rejoin by {} ms: {}",
        cluster.now(),
        cluster.node(first).snapshot()
    );
    assert!(cluster.node(first).role_name() == "Follower");
    let logs: Vec<Vec<CommittedBatch>> = cluster
        .ids()
        .into_iter()
        .map(|n| cluster.with_node(n, |core, _| core.take_committed()))
        .collect();
    assert!(logs[0] == logs[1]);
    assert!(logs[1] == logs[2]);
    cluster.assert_one_leader_per_epoch();
}

#[test]
fn a_partitioned_leader_resigns_and_the_majority_moves_on() {
    let mut cluster = three();
    let first = settle(&mut cluster, ELECTION_BUDGET_MS);
    // Cut every frame to and from the leader: it hears nothing and is heard
    // by nobody, as an isolated node.
    cluster.set_drop(move |frame: &Frame| frame.src.node == first || frame.dst.node == first);
    assert!(cluster.run_until(|c| c.node(first).role_name() != "Leader", 10_000));
    assert!(cluster.run_until(|c| c.leaders().iter().any(|&l| l != first), 10_000));
    cluster.clear_drop();
    let successor = settle(&mut cluster, ELECTION_BUDGET_MS);
    assert!(successor != first);
    assert!(cluster.node(first).leader() == Some(successor));
    cluster.assert_one_leader_per_epoch();
}

#[test]
fn a_lone_voter_leads_and_commits_its_own_proposals() {
    let mut cluster = Cluster::new(&[N1], &[]);
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    assert!(leader == N1);
    assert!(cluster.node(N1).high_watermark() == 1);
    let id = cluster
        .with_node(N1, |core, ctx| core.propose(ctx, delete("solo")))
        .unwrap();
    assert!(id == ProposalId(1));
    assert!(cluster.node(N1).high_watermark() == 2);
    assert!(
        cluster.with_node(N1, |core, _| core.take_committed())
            == vec![
                CommittedBatch {
                    offset: 0,
                    epoch: 1,
                    records: vec![]
                },
                CommittedBatch {
                    offset: 1,
                    epoch: 1,
                    records: delete("solo")
                }
            ]
    );
}

#[test]
fn an_observer_discovers_the_leader_and_replicates() {
    let mut cluster = Cluster::new(&VOTERS, &[N4]);
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    assert!(!cluster.node(N4).is_voter());
    assert!(cluster.node(N4).role_name() == "Observer");
    let id = propose_and_commit(&mut cluster, leader, delete("seen-by-observer"));
    assert!(cluster.node(N4).leader() == Some(leader));
    assert!(cluster.node(N4).high_watermark() > id.0);
    let observed = cluster.with_node(N4, |core, _| core.take_committed());
    let expected = cluster.with_node(leader, |core, _| core.take_committed());
    assert!(observed == expected);
    // The leader lists the observer with its progress, and never elects it.
    let response = cluster.node(leader).describe_quorum();
    let observers: Vec<i32> = response.topics[0].partitions[0]
        .observers
        .iter()
        .map(|state| state.replica_id)
        .collect();
    assert!(observers == vec![4]);
    assert!(cluster.leaders() == vec![leader]);
}

#[test]
fn describe_quorum_reports_the_leader_view_and_refuses_elsewhere() {
    let mut cluster = three();
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    propose_and_commit(&mut cluster, leader, delete("a"));
    // Let every follower fetch at least once after the commit.
    cluster.run_for(1_000);
    let mut response = cluster.node(leader).describe_quorum();
    // The timestamps are the logical times of the fetches; check that they
    // are set, then pin them so the whole shape compares.
    for state in &mut response.topics[0].partitions[0].current_voters {
        assert!(state.last_fetch_timestamp > 0, "voter {}", state.replica_id);
        assert!(
            state.last_caught_up_timestamp > 0,
            "voter {}",
            state.replica_id
        );
        state.last_fetch_timestamp = 0;
        state.last_caught_up_timestamp = 0;
    }
    let voter = |id: i32| ReplicaState {
        replica_id: id,
        log_end_offset: 2,
        last_fetch_timestamp: 0,
        last_caught_up_timestamp: 0,
        ..Default::default()
    };
    let node = |id: i32| WireNode {
        node_id: id,
        listeners: vec![Listener {
            name: "CONTROLLER".into(),
            host: format!("node-{id}"),
            port: RAFT_PORT,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        response
            == DescribeQuorumResponse {
                topics: vec![TopicData {
                    topic_name: "__cluster_metadata".into(),
                    partitions: vec![PartitionData {
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 1,
                        high_watermark: 2,
                        current_voters: vec![voter(1), voter(2), voter(3)],
                        observers: vec![],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                nodes: vec![node(1), node(2), node(3)],
                ..Default::default()
            }
    );
    let follower = cluster.node(N2).describe_quorum();
    assert!(follower.topics.len() == 1);
    assert!(follower.topics[0].partitions[0].error_code == codes::NOT_LEADER_OR_FOLLOWER);
    assert!(follower.topics[0].partitions[0].error_message.is_some());
    assert!(follower.nodes.is_empty());
}

#[test]
fn the_snapshot_has_the_inspector_shape() {
    let mut cluster = three();
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    cluster.run_for(500);
    let snapshot = cluster.node(leader).snapshot();
    assert!(snapshot["role"] == "Leader");
    assert!(snapshot["epoch"] == 1);
    assert!(snapshot["leader"] == leader.0);
    assert!(snapshot["hwm"] == 1);
    assert!(snapshot["leo"] == 1);
    assert!(snapshot["voters"] == json!([1, 2, 3]));
    assert!(snapshot["log_len"] == 1);
    assert!(snapshot["voter"] == true);
    let follower = cluster.node(N2).snapshot();
    assert!(follower["role"] == "Follower");
    assert!(follower["leader"] == leader.0);
}

#[test]
fn frames_leave_from_the_client_socket_to_the_raft_port_on_one_link_per_peer() {
    let mut node = ControllerNode::new(N1, &VOTERS);
    let mut buffers = CtxBuffers::new(N1);
    buffers.with(0, |ctx| node.start(ctx));
    assert!(buffers.take_frames().is_empty());
    assert!(buffers.timer == Some(1_050));
    assert!(node.core.next_deadline() == Some(1_050));
    // The election timer fires: a pre-vote to each other voter, on a freshly
    // opened link per peer.
    buffers.with(1_050, |ctx| node.on_timer(ctx));
    let frames = buffers.take_frames();
    let expected_vote = |peer: NodeId| RaftMessage::VoteRequest {
        cluster_id: CLUSTER_ID,
        voter_id: peer,
        candidate_epoch: 0,
        candidate: N1,
        last_epoch: 0,
        last_offset: 0,
        pre_vote: true,
    };
    let conn = |n: u32| ConnId((1 << 30) + n);
    assert!(
        frames
            == vec![
                Frame::open(Endpoint::client(N1), Endpoint::new(N2, RAFT_PORT), conn(1)),
                Frame::data(
                    Endpoint::client(N1),
                    Endpoint::new(N2, RAFT_PORT),
                    conn(1),
                    expected_vote(N2).encode()
                ),
                Frame::open(Endpoint::client(N1), Endpoint::new(N3, RAFT_PORT), conn(2)),
                Frame::data(
                    Endpoint::client(N1),
                    Endpoint::new(N3, RAFT_PORT),
                    conn(2),
                    expected_vote(N3).encode()
                ),
            ]
    );
    assert!(node.core.role_name() == "Prospective");
    // A grant from node 2 wins the pre-vote: the real vote goes out on the
    // same links, with no second open.
    let grant = Frame::data(
        Endpoint::new(N2, RAFT_PORT),
        Endpoint::client(N1),
        conn(1),
        RaftMessage::VoteResponse {
            epoch: 0,
            granted: true,
        }
        .encode(),
    );
    let _ = grant;
    let grant = Frame::data(
        Endpoint::client(N2),
        Endpoint::new(N1, RAFT_PORT),
        conn(7),
        RaftMessage::VoteResponse {
            epoch: 0,
            granted: true,
        }
        .encode(),
    );
    buffers.with(1_060, |ctx| node.on_frame(ctx, grant));
    let frames = buffers.take_frames();
    assert!(frames.iter().all(|f| matches!(f.payload, Payload::Data(_))));
    assert!(frames.len() == 2);
    assert!(node.core.role_name() == "Candidate");
    assert!(node.core.epoch() == 1);
    // A frame that is not a message is reported, not acted on.
    let junk = Frame::data(
        Endpoint::client(N2),
        Endpoint::new(N1, RAFT_PORT),
        conn(7),
        Bytes::from_static(b"not json"),
    );
    buffers.with(1_061, |ctx| node.on_frame(ctx, junk));
    assert!(buffers.take_frames().is_empty());
    assert!(
        buffers
            .events
            .iter()
            .any(|(kind, d)| *kind == "raft" && d["level"] == "warn")
    );
    // Stopping clears the deadline; a frame while stopped is ignored.
    node.stop();
    assert!(node.core.next_deadline().is_none());
    let late = Frame::data(
        Endpoint::client(N2),
        Endpoint::new(N1, RAFT_PORT),
        conn(7),
        RaftMessage::BeginQuorumEpoch { leader_epoch: 9 }.encode(),
    );
    buffers.with(1_070, |ctx| node.on_frame(ctx, late));
    assert!(node.core.epoch() == 1);
    assert!(buffers.take_frames().is_empty());
}

#[test]
fn the_durable_ops_rebuild_the_log_the_watermark_and_the_quorum_state() {
    let mut cluster = three();
    let leader = settle(&mut cluster, ELECTION_BUDGET_MS);
    for name in ["a", "b", "c"] {
        propose_and_commit(&mut cluster, leader, delete(name));
    }
    cluster.run_for(500);
    for id in cluster.ids() {
        let original = cluster.node(id);
        let image = cluster.durable_image(id);
        // The log store holds one JSON entry per offset, the state store the
        // quorum state and the high watermark.
        let log = &image.logs[KRAFT_LOG_STORE];
        assert!(
            log.iter().map(|e| e.index).collect::<Vec<_>>() == (0..4).collect::<Vec<_>>(),
            "node {id}"
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&log[0].bytes).unwrap()
                == json!({ "epoch": 1, "records": [] }),
            "node {id}"
        );
        let state = &image.kv[KRAFT_STATE_STORE];
        assert!(state[HIGH_WATERMARK_KEY].0 == "4", "node {id}");
        let quorum: DurableQuorumState =
            serde_json::from_slice(&state[QUORUM_STATE_KEY].0).unwrap();
        assert!(quorum == original.durable_quorum_state(), "node {id}");
        // The leader voted for itself; a follower's vote is cleared when it
        // attaches to the leader, as the machine's `BeginQuorumEpoch` does.
        assert!(
            quorum
                == DurableQuorumState {
                    epoch: 1,
                    voted_for: (id == leader).then_some(leader),
                    leader: Some(leader),
                },
            "node {id}"
        );
        // A fresh core loaded from the image is the original again.
        let mut restored = ControllerCore::new(id, &VOTERS, CLUSTER_ID);
        restored.load(&image);
        assert!(restored.log() == original.log(), "node {id}");
        assert!(
            restored.high_watermark() == original.high_watermark(),
            "node {id}"
        );
        assert!(
            restored.quorum_state() == original.quorum_state(),
            "node {id}"
        );
        assert!(restored.log_end_offset() == 4, "node {id}");
        // What was handed out before the reload is not handed out again,
        // unless the broker asks for a replay.
        assert!(restored.take_committed().is_empty(), "node {id}");
        restored.replay_committed();
        assert!(restored.take_committed().len() == 4, "node {id}");
        // Starting keeps the epoch and the vote and drops the leader belief.
        let mut node = ControllerNode { core: restored };
        let mut buffers = CtxBuffers::new(id);
        buffers.with(cluster.now(), |ctx| node.start(ctx));
        assert!(node.core.epoch() == 1, "node {id}");
        assert!(node.core.leader().is_none(), "node {id}");
        assert!(node.core.quorum_state().voted_key == original.quorum_state().voted_key);
    }
    // A truncation reaches the store too, and an empty image loads nothing.
    let mut image = DurableImage::default();
    image.apply(DurableOp::Append {
        store: KRAFT_LOG_STORE.into(),
        index: 0,
        bytes: Bytes::from_static(b"{\"epoch\":1,\"records\":[]}"),
    });
    image.apply(DurableOp::Append {
        store: KRAFT_LOG_STORE.into(),
        index: 1,
        bytes: Bytes::from_static(b"{\"epoch\":1,\"records\":[]}"),
    });
    image.apply(DurableOp::TruncateFrom {
        store: KRAFT_LOG_STORE.into(),
        index: 1,
    });
    let mut core = ControllerCore::new(N1, &VOTERS, CLUSTER_ID);
    core.load(&image);
    assert!(core.log_end_offset() == 1);
    assert!(core.high_watermark() == 0);
    core.load(&DurableImage::default());
    assert!(core.log_end_offset() == 0);
}

#[test]
fn control_proposes_through_json() {
    let mut node = ControllerNode::new(N1, &[N1]);
    let mut buffers = CtxBuffers::new(N1);
    buffers.with(0, |ctx| node.start(ctx));
    buffers.with(1_050, |ctx| node.on_timer(ctx));
    assert!(node.core.is_leader());
    let command = json!({ "cmd": "propose", "records": delete("via-control") });
    assert!(let Ok(answer) = buffers.with(1_100, |ctx| node.control(ctx, command)));
    assert!(answer == json!({ "offset": 1 }));
    assert!(let Err(error) = buffers.with(1_100, |ctx| node.control(ctx, json!({ "cmd": "?" }))));
    assert!(error.contains("unknown controller command"));
    assert!(node.snapshot()["hwm"] == 2);
    let mut core = ControllerCore::new(N2, &[N1], CLUSTER_ID);
    assert!(core.role_name() == "Observer");
    assert!(core.take_committed().is_empty());
}
