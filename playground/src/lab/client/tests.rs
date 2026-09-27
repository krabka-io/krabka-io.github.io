//! The client against the fake broker: negotiation, framing, routing,
//! coordinator lookups, timeouts and reconnection, the metadata refresh and
//! its backoff, and connection-id ranges.

use std::{collections::BTreeSet, rc::Rc};

use assert2::assert;
use krabka_protocol::{
    Decode, ProtocolRequest,
    owned::{
        api_versions_request::ApiVersionsRequest,
        heartbeat_request::HeartbeatRequest,
        heartbeat_response::HeartbeatResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        produce_request::ProduceRequest,
        request_header::RequestHeader,
    },
    primitives::uuid::Uuid,
};

use super::{
    ApiSpec, CONN_ID_RANGE, ClientError, ClientEvent, ClientOptions, CoordinatorType, KafkaClient,
    NodeState, RequestId, Response, Target, VersionTable, conn_base, draw_conn_id,
    fake_broker::{ClusterState, Seen},
    pick_least_loaded,
    request::{frame_request, response_header_version},
    test_support::{Driven, Harness, client, cluster},
};
use crate::lab::{
    codes,
    net::{ConnId, Ctx, Endpoint, Frame, Millis, NodeId},
};

fn responses(events: Vec<ClientEvent>) -> Vec<(RequestId, Result<Response, ClientError>)> {
    events
        .into_iter()
        .filter_map(|e| match e {
            ClientEvent::Response { id, result } => Some((id, result)),
            ClientEvent::MetadataUpdated => None,
        })
        .collect()
}

/// Run until the client has its first metadata, and drop the events of the
/// bootstrap.
fn bootstrap(h: &mut Harness<KafkaClient>) {
    assert!(h.run_until(|h| h.client.metadata().updated_at.is_some(), 1_000));
    h.take_events();
}

fn heartbeat() -> HeartbeatRequest {
    HeartbeatRequest {
        group_id: "g".to_string(),
        generation_id: 1,
        member_id: "m".to_string(),
        ..Default::default()
    }
}

#[test]
fn bootstrap_negotiates_versions_then_fetches_metadata() {
    let state = cluster(&[("orders", 3)]);
    let mut h = Harness::new(client(&[1]), state);
    assert!(h.run_until(|h| h.client.metadata().updated_at.is_some(), 1_000));
    let seen = h.seen(ApiVersionsRequest::API_KEY);
    assert!(seen.len() == 1);
    assert!(seen[0].version == ApiVersionsRequest::LATEST_STABLE_VERSION);
    assert!(seen[0].client_id.as_deref() == Some("test"));
    let request: ApiVersionsRequest = seen[0].decode();
    assert!(request.client_software_name == "krabka-lab");
    let metadata = h.client.metadata();
    assert!(metadata.brokers.keys().copied().collect::<Vec<_>>() == vec![1, 2, 3]);
    assert!(metadata.broker_endpoint(2) == Some(Endpoint::kafka(NodeId(2))));
    assert!(metadata.partition_count("orders") == Some(3));
    assert!(metadata.leader("orders", 1) == Some(2));
    assert!(metadata.controller_id == 1);
    assert!(metadata.cluster_id.as_deref() == Some("lab-cluster"));
    let seen = h.seen(MetadataRequest::API_KEY);
    assert!(seen.len() == 1);
    let request: MetadataRequest = seen[0].decode();
    assert!(
        request
            == MetadataRequest {
                topics: None,
                allow_auto_topic_creation: false,
                ..Default::default()
            }
    );
    let snapshot = h.client.snapshot();
    assert!(snapshot["connections"][0]["state"] == "ready");
    assert!(snapshot["connections"][0]["broker_id"] == 1);
    assert!(
        snapshot["connections"][0]["versions_negotiated"]
            .as_u64()
            .unwrap()
            >= 16
    );
    assert!(snapshot["pending"] == 0);
}

#[test]
fn a_typed_send_decodes_the_whole_response() {
    let state = cluster(&[("orders", 2)]);
    let topic_id = state.borrow().topics["orders"].id;
    let mut h = Harness::new(client(&[1]), state);
    let id = h.with_client(|c, ctx| {
        c.send(
            ctx,
            Target::Any,
            MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some("orders".to_string()),
                    ..Default::default()
                }]),
                allow_auto_topic_creation: false,
                ..Default::default()
            },
        )
    });
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let mut responses = responses(h.take_events());
    assert!(responses.len() == 1);
    let (got_id, result) = responses.remove(0);
    assert!(got_id == id);
    let response = result.unwrap();
    assert!(response.api_key == MetadataRequest::API_KEY);
    assert!(response.version == MetadataRequest::LATEST_STABLE_VERSION);
    assert!(response.endpoint == Endpoint::kafka(NodeId(1)));
    let body = response.downcast::<MetadataResponse>().unwrap();
    let broker = |id: i32| MetadataResponseBroker {
        node_id: id,
        host: format!("node-{id}"),
        port: 9092,
        rack: None,
        ..Default::default()
    };
    let partition = |index: i32, leader: i32, replicas: Vec<i32>| MetadataResponsePartition {
        error_code: 0,
        partition_index: index,
        leader_id: leader,
        leader_epoch: 0,
        replica_nodes: replicas.clone(),
        isr_nodes: replicas,
        offline_replicas: vec![],
        ..Default::default()
    };
    assert!(
        body == MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![broker(1), broker(2), broker(3)],
            cluster_id: Some("lab-cluster".to_string()),
            controller_id: 1,
            topics: vec![MetadataResponseTopic {
                error_code: 0,
                name: Some("orders".to_string()),
                topic_id,
                is_internal: false,
                partitions: vec![
                    partition(0, 1, vec![1, 2, 3]),
                    partition(1, 2, vec![2, 3, 1]),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    );
}

#[test]
fn request_headers_follow_the_flexible_version_of_the_api() {
    // Rows: the api, the version, and the exact header bytes after the
    // length prefix: api key, version, correlation id 7, client id "cid",
    // and the empty tagged-field byte of a v2 header.
    let produce = ApiSpec::of::<ProduceRequest>();
    let api_versions = ApiSpec::of::<ApiVersionsRequest>();
    let cases: [(&str, ApiSpec, i16, Vec<u8>); 3] = [
        (
            "produce v8 is not flexible: header v1",
            produce,
            8,
            vec![0, 0, 0, 8, 0, 0, 0, 7, 0, 3, b'c', b'i', b'd'],
        ),
        (
            "produce v9 is flexible: header v2",
            produce,
            9,
            vec![0, 0, 0, 9, 0, 0, 0, 7, 0, 3, b'c', b'i', b'd', 0],
        ),
        (
            "api versions v3 is flexible: header v2",
            api_versions,
            3,
            vec![0, 18, 0, 3, 0, 0, 0, 7, 0, 3, b'c', b'i', b'd', 0],
        ),
    ];
    for (name, api, version, header) in cases {
        let frame = if api.key == ProduceRequest::API_KEY {
            frame_request(api, version, 7, "cid", &ProduceRequest::default()).unwrap()
        } else {
            frame_request(api, version, 7, "cid", &ApiVersionsRequest::default()).unwrap()
        };
        let len =
            usize::try_from(i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]])).unwrap();
        assert!(len == frame.len() - 4, "{name}");
        assert!(&frame[4..4 + header.len()] == header.as_slice(), "{name}");
        let header_version = if version >= api.flexible_min { 2 } else { 1 };
        let mut cursor = &frame[4..];
        let decoded = RequestHeader::decode(&mut cursor, header_version).unwrap();
        assert!(decoded.correlation_id == 7, "{name}");
        assert!(decoded.client_id.as_deref() == Some("cid"), "{name}");
    }
    let header_versions = [
        ("api versions always v0", 18, 3, 5, 0),
        ("flexible body v1", 0, 9, 9, 1),
        ("non-flexible body v0", 0, 9, 8, 0),
        ("never flexible v0", 4, i16::MAX, 7, 0),
    ];
    for (name, api_key, flexible_min, version, expected) in header_versions {
        assert!(
            response_header_version(api_key, flexible_min, version) == expected,
            "{name}"
        );
    }
}

#[test]
fn negotiation_takes_the_highest_common_version_or_fails() {
    let produce = ApiSpec::of::<ProduceRequest>();
    let cases = [
        ("broker above the client", (3, 20), Ok(produce.max)),
        ("broker below the client", (3, 9), Ok(9)),
        ("same range", (produce.min, produce.max), Ok(produce.max)),
        ("broker only above", (20, 21), Err((20, 21))),
        ("broker only below", (0, 2), Err((0, 2))),
        ("api not listed", (0, -1), Err((0, -1))),
    ];
    for (name, (min, max), expected) in cases {
        let table = if max < 0 {
            VersionTable::default()
        } else {
            VersionTable::from_entries([(produce.key, min, max)])
        };
        let actual = table.negotiate(produce).map_err(|e| match e {
            ClientError::UnsupportedVersion {
                broker_min,
                broker_max,
                ..
            } => (broker_min, broker_max),
            other => panic!("{name}: {other}"),
        });
        assert!(actual == expected, "{name}");
    }
}

#[test]
fn unsupported_api_versions_is_retried_at_the_version_the_broker_lists() {
    // Rows: the ApiVersions maximum the broker advertises in its error, and
    // the versions of the requests the broker then sees.
    let cases = [
        ("the broker lists its real range", None, vec![5, 4]),
        ("the broker lists only v0", Some(0), vec![5, 0]),
    ];
    for (name, max, expected) in cases {
        let state = cluster(&[]);
        {
            let mut s = state.borrow_mut();
            s.knobs.api_versions_unsupported = 1;
            if let Some(max) = max {
                s.knobs
                    .api_versions_max
                    .insert(ApiVersionsRequest::API_KEY, max);
            }
        }
        let mut h = Harness::new(client(&[1]), state);
        assert!(
            h.run_until(|h| h.client.metadata().updated_at.is_some(), 1_000),
            "{name}"
        );
        let versions: Vec<i16> = h
            .seen(ApiVersionsRequest::API_KEY)
            .iter()
            .map(|s| s.version)
            .collect();
        assert!(versions == expected, "{name}");
        assert!(
            h.client.snapshot()["connections"][0]["state"] == "ready",
            "{name}"
        );
    }
}

#[test]
fn leader_requests_follow_the_metadata_and_refresh_after_not_leader() {
    let state = cluster(&[("orders", 1)]);
    let mut h = Harness::new(client(&[3]), Rc::clone(&state));
    bootstrap(&mut h);
    let target = Target::Leader {
        topic: "orders".to_string(),
        partition: 0,
    };
    let send = |h: &mut Harness<KafkaClient>| {
        let target = target.clone();
        h.with_client(move |c, ctx| c.send(ctx, target, heartbeat()))
    };
    send(&mut h);
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let seen = h.seen(HeartbeatRequest::API_KEY);
    assert!(seen.len() == 1);
    assert!(seen[0].broker == NodeId(1));
    h.take_events();
    // Leadership moves; the client learns it from the error and refreshes.
    state.borrow_mut().set_leader("orders", 0, 2);
    let version_before = h.client.metadata().version;
    h.with_client(|c, _| c.note_error(codes::NOT_LEADER_OR_FOLLOWER, &target));
    assert!(h.run_until(|h| h.client.metadata().version > version_before, 1_000));
    send(&mut h);
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let seen = h.seen(HeartbeatRequest::API_KEY);
    assert!(seen.len() == 2);
    assert!(seen[1].broker == NodeId(2));
    assert!(h.client.metadata().leader("orders", 0) == Some(2));
    assert!(
        h.client
            .metadata()
            .partition("orders", 0)
            .unwrap()
            .leader_epoch
            == 1
    );
}

#[test]
fn a_request_for_a_topic_that_appears_later_waits_then_goes_to_its_leader() {
    let state = cluster(&[]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    let target = Target::Leader {
        topic: "later".to_string(),
        partition: 0,
    };
    let id = h.with_client(|c, ctx| c.send(ctx, target, heartbeat()));
    h.run_for(300);
    assert!(responses(h.take_events()).is_empty());
    assert!(h.client.snapshot()["pending"] == 1);
    assert!(h.client.metadata().unknown_topics.contains("later"));
    state.borrow_mut().add_topic("later", 1, 1);
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let results = responses(h.take_events());
    assert!(results.len() == 1);
    assert!(results[0].0 == id);
    assert!(results[0].1.is_ok());
    let seen = h.seen(HeartbeatRequest::API_KEY);
    assert!(seen.len() == 1);
    assert!(seen[0].broker == NodeId(1));
    // The refresh asked for the topic by name while it waited.
    let metadata = h.seen(MetadataRequest::API_KEY);
    let last: MetadataRequest = metadata.last().unwrap().decode();
    assert!(
        last.topics
            == Some(vec![MetadataRequestTopic {
                name: Some("later".to_string()),
                ..Default::default()
            }])
    );
}

#[test]
fn coordinator_lookups_are_cached_until_invalidated() {
    let state = cluster(&[]);
    state.borrow_mut().coordinator = 2;
    let mut h = Harness::new(client(&[1]), state);
    bootstrap(&mut h);
    let target = Target::Coordinator {
        key_type: CoordinatorType::Group,
        key: "billing".to_string(),
    };
    for _ in 0..2 {
        let target = target.clone();
        h.with_client(move |c, ctx| c.send(ctx, target, heartbeat()));
        assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
        h.take_events();
    }
    assert!(h.seen(10).len() == 1);
    let lookup: krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest =
        h.seen(10)[0].decode();
    assert!(lookup.coordinator_keys == vec!["billing".to_string()]);
    assert!(lookup.key_type == 0);
    let seen = h.seen(HeartbeatRequest::API_KEY);
    assert!(seen.iter().all(|s| s.broker == NodeId(2)));
    assert!(
        h.client.coordinator(CoordinatorType::Group, "billing")
            == Some((2, Endpoint::kafka(NodeId(2))))
    );
    h.with_client(|c, _| c.note_error(codes::NOT_COORDINATOR, &target));
    assert!(
        h.client
            .coordinator(CoordinatorType::Group, "billing")
            .is_none()
    );
    h.with_client(|c, ctx| c.send(ctx, target, heartbeat()));
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    assert!(h.seen(10).len() == 2);
}

#[test]
fn a_silent_broker_times_the_request_out_and_closes_the_connection() {
    let state = ClusterState::new(&[(1, 1)]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    state.borrow_mut().knobs.silent = true;
    let id = h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
    h.run_for(29_000);
    assert!(h.events.is_empty());
    assert!(h.open_connections() == 1);
    // The broker answers again, but not the request it already holds.
    state.borrow_mut().knobs.silent = false;
    h.run_for(1_010);
    let results = responses(h.take_events());
    assert!(results.len() == 1);
    assert!(results[0].0 == id);
    assert!(matches!(
        results[0].1,
        Err(ClientError::Timeout {
            api: "Heartbeat",
            timeout_ms: 30_000
        })
    ));
    assert!(h.open_connections() == 0);
    assert!(h.client.snapshot()["connections"][0]["state"] == "closed");
    assert!(h.client.snapshot()["timeouts"] == 1);
    // The next request reconnects and negotiates again.
    h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 5_000));
    assert!(h.seen(ApiVersionsRequest::API_KEY).len() == 2);
    let (_, result) = responses(h.take_events()).remove(0);
    assert!(result.unwrap().downcast::<HeartbeatResponse>().is_some());
}

fn responses_of(events: &[ClientEvent]) -> Vec<RequestId> {
    events
        .iter()
        .filter_map(|e| match e {
            ClientEvent::Response { id, .. } => Some(*id),
            ClientEvent::MetadataUpdated => None,
        })
        .collect()
}

/// The state of the client's first connection, as the inspector shows it.
fn conn_state(h: &Harness<KafkaClient>) -> String {
    h.client.snapshot()["connections"][0]["state"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// How many times the client opened its first connection.
fn opened(h: &Harness<KafkaClient>) -> u64 {
    h.client.snapshot()["connections"][0]["opened"]
        .as_u64()
        .unwrap_or_default()
}

#[test]
fn a_killed_broker_fails_in_flight_requests_and_the_client_reconnects_after_the_backoff() {
    let state = ClusterState::new(&[(1, 1)]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    state.borrow_mut().knobs.silent = true;
    h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
    h.run_for(10);
    h.kill_broker(NodeId(1));
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let (_, result) = responses(h.take_events()).remove(0);
    assert!(matches!(
        result,
        Err(ClientError::Disconnected {
            api: "Heartbeat",
            ..
        })
    ));
    state.borrow_mut().knobs.silent = false;
    // Each reconnect waits `reconnect.backoff.ms`, 50 ms with 20 % jitter,
    // because Kafka's `ClusterConnectionStates.ready` resets the backoff once
    // a connection is ready again.
    for round in 0..2 {
        assert!(conn_state(&h) == "closed", "round {round}");
        let closed_at = h.now();
        h.restart_broker(NodeId(1));
        h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
        let attempts = opened(&h);
        assert!(
            h.run_until(|h| opened(h) > attempts, 5_000),
            "round {round}"
        );
        let waited = h.now() - closed_at;
        assert!(
            (40..=60).contains(&waited),
            "round {round}: waited {waited} ms"
        );
        assert!(
            h.run_until(|h| !responses_of(&h.events).is_empty(), 5_000),
            "round {round}"
        );
        let (_, result) = responses(h.take_events()).remove(0);
        assert!(result.is_ok(), "round {round}");
        h.kill_broker(NodeId(1));
        assert!(
            h.run_until(|h| conn_state(h) == "closed", 1_000),
            "round {round}"
        );
    }
}

#[test]
fn an_unreachable_broker_times_out_the_setup_and_the_backoffs_double() {
    let state = ClusterState::new(&[(1, 1)]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    h.kill_broker(NodeId(1));
    assert!(h.run_until(|h| conn_state(h) == "closed", 1_000));
    // The broker stays down, so every `Open` goes unanswered. Rows: the
    // reconnect backoff before the attempt (`reconnect.backoff.ms` 50 ms,
    // doubling), and the setup timeout that ends it
    // (`socket.connection.setup.timeout.ms` 10 s, doubling), each with 20 %
    // jitter.
    h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
    let rows = [(40..=60, 8_000..=12_000), (80..=120, 16_000..=24_000)];
    let mut closed_at = h.now();
    for (attempt, (backoff, setup)) in rows.into_iter().enumerate() {
        let attempts = opened(&h);
        assert!(
            h.run_until(|h| opened(h) > attempts, 5_000),
            "attempt {attempt}"
        );
        let waited = h.now() - closed_at;
        assert!(
            backoff.contains(&waited),
            "attempt {attempt}: waited {waited} ms"
        );
        assert!(conn_state(&h) == "connecting", "attempt {attempt}");
        let opened_at = h.now();
        assert!(
            h.run_until(|h| conn_state(h) == "closed", 30_000),
            "attempt {attempt}"
        );
        let took = h.now() - opened_at;
        assert!(setup.contains(&took), "attempt {attempt}: took {took} ms");
        closed_at = h.now();
    }
    // The request waited in the queue and expires at `request.timeout.ms`,
    // during the second attempt or after it, as the jitter falls.
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 30_000));
    let results = responses(h.take_events());
    assert!(results.len() == 1);
    assert!(matches!(
        results[0].1,
        Err(ClientError::Timeout {
            api: "Heartbeat",
            timeout_ms: 30_000
        })
    ));
}

/// A pick of the least loaded node: its name, the nodes in the order the
/// client knows them, the node the visit starts from, and the pick.
type Pick = (&'static str, Vec<NodeState>, usize, Option<usize>);

#[test]
fn the_least_loaded_node_is_picked_as_kafka_picks_it() {
    // Kafka's `NetworkClient.leastLoadedNode`, at 1 000 ms with a window of
    // five requests. A node tried at `t` closed then, and its reconnect
    // backoff ended 50 ms later.
    let never = NodeState::Idle {
        retry_at: 0,
        last_attempt: None,
    };
    let tried = |at: Millis| NodeState::Idle {
        retry_at: at + 50,
        last_attempt: Some(at),
    };
    let backing_off = NodeState::Idle {
        retry_at: 1_040,
        last_attempt: Some(990),
    };
    let ready = |load: usize| NodeState::Ready { load };
    let rows: [Pick; 12] = [
        (
            "a ready connection with nothing in flight, at once",
            vec![ready(1), ready(0), ready(0)],
            0,
            Some(1),
        ),
        (
            "the ready connection with the fewest requests",
            vec![ready(3), ready(1), ready(2)],
            0,
            Some(1),
        ),
        (
            "a ready connection before a connection being set up",
            vec![NodeState::Connecting, ready(4), never],
            0,
            Some(1),
        ),
        (
            "a full window is passed over",
            vec![ready(5), NodeState::Connecting],
            0,
            Some(1),
        ),
        (
            "a connection being set up before a new one",
            vec![never, NodeState::Connecting],
            0,
            Some(1),
        ),
        (
            "the last connection being set up the visit meets",
            vec![NodeState::Connecting, NodeState::Connecting, never],
            0,
            Some(1),
        ),
        (
            "a node never tried before a node tried",
            vec![tried(100), never, tried(200)],
            0,
            Some(1),
        ),
        (
            "the node whose last attempt is the oldest",
            vec![tried(300), tried(100), tried(200)],
            0,
            Some(1),
        ),
        (
            "a node in reconnect backoff is passed over",
            vec![backing_off, tried(900)],
            0,
            Some(1),
        ),
        (
            "nothing while every node backs off or is full",
            vec![backing_off, ready(5), backing_off],
            0,
            None,
        ),
        (
            "a tie goes to the first node the visit meets",
            vec![never, never, never],
            2,
            Some(2),
        ),
        (
            "the visit wraps around from the offset",
            vec![ready(0), tried(100), ready(0)],
            1,
            Some(2),
        ),
    ];
    for (name, nodes, offset, expected) in rows {
        assert!(
            pick_least_loaded(&nodes, offset, 1_000, 5) == expected,
            "{name}"
        );
    }
}

/// The state of the client's connection to `node`, as the inspector shows
/// it, or an empty string when there is none.
fn conn_state_of(h: &Harness<KafkaClient>, node: u32) -> String {
    h.client.snapshot()["connections"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|c| c["broker"] == node)
        .and_then(|c| c["state"].as_str())
        .unwrap_or_default()
        .to_string()
}

/// The brokers the client opened a connection to from `from` on, in order:
/// each attempt starts with an `ApiVersions`.
fn tried_since(h: &Harness<KafkaClient>, from: Millis) -> Vec<NodeId> {
    h.seen(ApiVersionsRequest::API_KEY)
        .iter()
        .filter(|s| s.at >= from)
        .map(|s| s.broker)
        .collect()
}

#[test]
fn a_request_for_any_broker_leaves_brokers_whose_connection_setup_times_out() {
    // Brokers 1 and 2 take connections but never answer, not even
    // `ApiVersions`, as a broker cut off from its controller does; broker 3
    // answers, but the client's connection to it just closed, so broker 3
    // waits out its reconnect backoff and was tried last. Kafka's
    // `NetworkClient.leastLoadedNode` passes over a node in reconnect
    // backoff and prefers, among the nodes it may connect to, the one whose
    // last attempt is oldest, a node never tried first. A caller's request
    // waiting for a connection that fails is sent to the node that rule
    // picks next, as `KafkaAdminClient` reassigns the calls of a failed
    // node, and a metadata update that fails waits `retry.backoff.ms` and
    // picks again, as `DefaultMetadataUpdater` does. So each silent broker
    // costs one connection setup timeout (10 s, 20 % jitter), and broker 3
    // answers after two. Rows: a request of the caller for any broker, and
    // the client's own metadata refresh, which the lost connection asked
    // for.
    for caller in [true, false] {
        let state = cluster(&[]);
        let mut h = Harness::new(client(&[3]), Rc::clone(&state));
        bootstrap(&mut h);
        assert!(h.client.metadata().brokers.len() == 3, "caller {caller}");
        state.borrow_mut().knobs.silent_brokers = BTreeSet::from([1, 2]);
        h.kill_broker(NodeId(3));
        assert!(
            h.run_until(|h| conn_state_of(h, 3) == "closed", 1_000),
            "caller {caller}"
        );
        h.restart_broker(NodeId(3));
        let t0 = h.now();
        let version = h.client.metadata().version;
        let sent = caller.then(|| h.with_client(|c, ctx| c.send(ctx, Target::Any, heartbeat())));
        let answered = |h: &Harness<KafkaClient>| {
            if caller {
                !responses_of(&h.events).is_empty()
            } else {
                h.client.metadata().version > version
            }
        };
        assert!(h.run_until(answered, 30_000), "caller {caller}");
        let took = h.now() - t0;
        assert!(
            (16_000..=25_000).contains(&took),
            "caller {caller}: {took} ms"
        );
        if let Some(id) = sent {
            let results = responses(h.take_events());
            assert!(results.len() == 1, "caller {caller}");
            let (got, result) = &results[0];
            assert!(*got == id, "caller {caller}");
            let endpoint = result.as_ref().map(|r| r.endpoint).ok();
            assert!(
                endpoint == Some(Endpoint::kafka(NodeId(3))),
                "caller {caller}"
            );
        }
        // The metadata refresh follows within its failure backoff.
        assert!(
            h.run_until(|h| h.client.metadata().version > version, 1_000),
            "caller {caller}"
        );
        // Each silent broker was tried once, in either order, then broker 3.
        let tried = tried_since(&h, t0);
        let mut silent = tried.get(..2).unwrap_or_default().to_vec();
        silent.sort();
        assert!(
            (silent, tried.get(2..).unwrap_or_default().to_vec())
                == (vec![NodeId(1), NodeId(2)], vec![NodeId(3)]),
            "caller {caller}: {tried:?}"
        );
        // Only broker 3 ever got a request past its `ApiVersions`.
        let metadata_brokers: BTreeSet<NodeId> = h
            .seen(MetadataRequest::API_KEY)
            .iter()
            .filter(|s| s.at >= t0)
            .map(|s| s.broker)
            .collect();
        assert!(
            metadata_brokers == BTreeSet::from([NodeId(3)]),
            "caller {caller}"
        );
    }
}

#[test]
fn a_request_for_any_broker_that_times_out_leaves_the_silent_broker_for_another() {
    // Broker 1 answers the bootstrap, then goes silent. The request for any
    // broker in flight on it fails after `request.timeout.ms`, and the
    // connection closes, as Kafka's `NetworkClient` fails the calls in
    // flight on a connection it closes for a request timeout. The next
    // request for any broker goes to a broker never tried, even once the
    // reconnect backoff of broker 1 has passed: its last attempt is the
    // newest.
    let state = cluster(&[]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    state.borrow_mut().knobs.silent_brokers = BTreeSet::from([1]);
    let first = h.with_client(|c, ctx| c.send(ctx, Target::Any, heartbeat()));
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 31_000));
    let results = responses(h.take_events());
    assert!(results.len() == 1);
    assert!(results[0].0 == first);
    assert!(matches!(
        results[0].1,
        Err(ClientError::Timeout {
            api: "Heartbeat",
            timeout_ms: 30_000
        })
    ));
    h.run_for(1_000);
    let second = h.with_client(|c, ctx| c.send(ctx, Target::Any, heartbeat()));
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let results = responses(h.take_events());
    assert!(results.len() == 1);
    assert!(results[0].0 == second);
    let endpoint = results[0].1.as_ref().map(|r| r.endpoint.node).ok();
    assert!(matches!(endpoint, Some(NodeId(2 | 3))), "{endpoint:?}");
    // Broker 1 was never tried again after the bootstrap.
    assert!(
        tried_since(&h, 0)
            .iter()
            .filter(|b| **b == NodeId(1))
            .count()
            == 1
    );
}

#[test]
fn a_request_for_any_broker_waits_out_the_reconnect_backoff_of_the_only_broker() {
    // With every known node in reconnect backoff, Kafka's `leastLoadedNode`
    // picks none, and the request waits. It goes out when the first backoff
    // ends (`reconnect.backoff.ms`, 50 ms with 20 % jitter) and is answered
    // 20 ms later, after `ApiVersions`.
    let state = ClusterState::new(&[(1, 1)]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    // The metadata refresh the close asks for is due at once, and waits too.
    h.run_for(1_000);
    h.kill_broker(NodeId(1));
    assert!(h.run_until(|h| conn_state(h) == "closed", 1_000));
    let closed_at = h.now();
    h.restart_broker(NodeId(1));
    let id = h.with_client(|c, ctx| c.send(ctx, Target::Any, heartbeat()));
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let took = h.now() - closed_at;
    assert!((60..=79).contains(&took), "{took} ms");
    let results = responses(h.take_events());
    assert!(results.len() == 1);
    assert!(results[0].0 == id);
    assert!(results[0].1.is_ok());
}

#[test]
fn a_request_for_any_broker_prefers_the_brokers_of_the_metadata_to_the_bootstrap_brokers() {
    // Kafka's `leastLoadedNode` picks among the brokers of the metadata;
    // the bootstrap brokers come back only when none of those can be picked
    // and no connection to one is ready (the `rebootstrap` of
    // `metadata.recovery.strategy`). Broker 2 is a bootstrap broker the
    // metadata leaves out, as it leaves out a fenced broker, and it answers
    // nothing. The client's connections to brokers 1 and 3 close when both
    // restart, which makes broker 2 the node whose last attempt is the
    // oldest; still, once the reconnect backoff of 1 and 3 passed, a request
    // for any broker goes to one of them and is answered at once.
    let state = cluster(&[]);
    let mut h = Harness::new(client(&[1, 2, 3]), Rc::clone(&state));
    {
        let mut s = state.borrow_mut();
        s.brokers.remove(&2);
        s.knobs.silent_brokers = BTreeSet::from([2]);
    }
    assert!(h.run_until(|h| h.client.metadata().updated_at.is_some(), 30_000));
    let named: Vec<i32> = h.client.metadata().brokers.keys().copied().collect();
    assert!(named == vec![1, 3]);
    for broker in [1, 3] {
        h.with_client(|c, ctx| c.send(ctx, Target::Broker(broker), heartbeat()));
    }
    assert!(h.run_until(|h| responses_of(&h.events).len() == 2, 1_000));
    h.take_events();
    for node in [1, 3] {
        h.kill_broker(NodeId(node));
    }
    assert!(h.run_until(
        |h| conn_state_of(h, 1) == "closed" && conn_state_of(h, 3) == "closed",
        1_000
    ));
    for node in [1, 3] {
        h.restart_broker(NodeId(node));
    }
    h.run_for(100);
    let sent_at = h.now();
    let id = h.with_client(|c, ctx| c.send(ctx, Target::Any, heartbeat()));
    assert!(h.run_until(|h| !responses_of(&h.events).is_empty(), 1_000));
    let took = h.now() - sent_at;
    assert!(took <= 20, "{took} ms");
    let results = responses(h.take_events());
    assert!(results.len() == 1);
    assert!(results[0].0 == id);
    let answered_by = results[0].1.as_ref().map(|r| r.endpoint.node).ok();
    assert!(
        matches!(answered_by, Some(NodeId(1 | 3))),
        "{answered_by:?}"
    );
}

#[test]
fn the_in_flight_window_holds_five_requests_and_queues_the_rest() {
    let state = ClusterState::new(&[(1, 1)]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    state.borrow_mut().knobs.silent = true;
    for _ in 0..7 {
        h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
    }
    h.run_for(100);
    let connection = &h.client.snapshot()["connections"][0];
    assert!(connection["in_flight"] == 5);
    assert!(connection["queued"] == 2);
    assert!(h.seen(HeartbeatRequest::API_KEY).len() == 5);
    state.borrow_mut().knobs.silent = false;
    h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
    // The broker answers again, but the five in flight stay unanswered and
    // the eighth request waits behind them until they time out. The two
    // queued at the start expire in the queue; the eighth, queued later, goes
    // out on the new connection.
    h.run_for(100);
    assert!(h.seen(HeartbeatRequest::API_KEY).len() == 5);
    h.run_for(31_000);
    assert!(h.seen(HeartbeatRequest::API_KEY).len() == 6);
    let results = responses(h.take_events());
    assert!(results.len() == 8);
    assert!(results.iter().filter(|(_, r)| r.is_ok()).count() == 1);
    assert!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, Err(ClientError::Timeout { .. })))
            .count()
            == 7
    );
    assert!(h.client.snapshot()["timeouts"] == 7);
}

#[test]
fn a_oneway_request_completes_with_an_empty_body_once_written() {
    let state = cluster(&[]);
    let mut h = Harness::new(client(&[1]), state);
    bootstrap(&mut h);
    let id = h.with_client(|c, ctx| c.send_oneway(ctx, Target::Any, heartbeat()));
    let (got, result) = responses(h.take_events()).remove(0);
    assert!(got == id);
    let response = result.unwrap();
    assert!(response.is::<()>());
    assert!(response.downcast::<()>() == Some(()));
    assert!(h.client.snapshot()["connections"][0]["in_flight"] == 0);
    h.run_for(100);
    assert!(h.seen(HeartbeatRequest::API_KEY).len() == 1);
}

#[test]
fn close_fails_everything_pending() {
    let state = cluster(&[]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    state.borrow_mut().knobs.silent = true;
    let id = h.with_client(|c, ctx| c.send(ctx, Target::Broker(1), heartbeat()));
    h.run_for(50);
    h.with_client(KafkaClient::close);
    h.tick();
    let results = responses(h.take_events());
    assert!(
        results
            .iter()
            .any(|(got, r)| *got == id && matches!(r, Err(ClientError::Closed)))
    );
    assert!(h.open_connections() == 0);
    let _ = Uuid::ZERO;
}

/// A `Metadata` request for `topics` by name, as the client sends it.
fn metadata_for(topics: &[&str]) -> MetadataRequest {
    MetadataRequest {
        topics: Some(
            topics
                .iter()
                .map(|name| MetadataRequestTopic {
                    name: Some((*name).to_string()),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation: false,
        ..Default::default()
    }
}

/// The `Metadata` requests the brokers saw, with when each arrived.
fn metadata_requests(h: &Harness<impl Driven>) -> Vec<(Millis, MetadataRequest)> {
    h.seen(MetadataRequest::API_KEY)
        .iter()
        .map(|s| (s.at, s.decode()))
        .collect()
}

#[test]
fn a_topic_tracked_while_a_refresh_is_out_is_looked_up_after_it() {
    // Kafka's `Metadata.update` leaves `needPartialUpdate` set when a topic
    // was added after the answered request left (its `requestVersion` is
    // older), so the new topic is looked up next.
    let state = cluster(&[("a", 1), ("b", 1)]);
    let mut c = client(&[1]);
    c.add_topics(["a"]);
    let mut h = Harness::new(c, Rc::clone(&state));
    // `ApiVersions` answers at 10 ms; `Metadata` for `a` leaves then and
    // is answered at 20 ms.
    h.run_for(12);
    h.with_client(|c, _| c.add_topics(["b"]));
    assert!(h.run_until(|h| h.client.metadata().topics.contains_key("b"), 1_000));
    let asked = metadata_requests(&h);
    let requests: Vec<MetadataRequest> = asked.iter().map(|(_, r)| r.clone()).collect();
    assert!(requests == vec![metadata_for(&["a"]), metadata_for(&["a", "b"])]);
    // The second request waited `retry.backoff.ms` (100 ms, 20 % jitter)
    // after the first answer, and reached the broker 5 ms later.
    assert!((105..=145).contains(&asked[1].0));
}

#[test]
fn metadata_refreshes_back_off_while_a_tracked_topic_stays_unknown() {
    // Each answer that reports the topic unknown asks again (Kafka's
    // `handleMetadataResponse` on an `InvalidMetadataException`), and each
    // such answer moves no leader epoch, so the wait grows: Kafka's
    // equivalent responses, `retry.backoff.ms` 100 ms doubling to
    // `retry.backoff.max.ms` 1 s with 20 % jitter. Rows: the gap between
    // two requests at the broker, which adds the 10 ms round trip.
    let gaps = [90..=129, 170..=249, 330..=489, 650..=969, 810..=1_010];
    let state = cluster(&[]);
    let mut h = Harness::new(client(&[1]), Rc::clone(&state));
    bootstrap(&mut h);
    let t0 = h.now();
    // A new topic is looked up at once, whatever the backoff.
    h.with_client(|c, _| c.add_topics(["later"]));
    let lookups = gaps.len() + 1;
    assert!(h.run_until(|h| metadata_requests(h).len() > lookups, 5_000));
    let asked: Vec<(Millis, MetadataRequest)> = metadata_requests(&h)[1..=lookups].to_vec();
    assert!(asked[0].0 == t0 + 5);
    assert!(asked.iter().all(|(_, r)| *r == metadata_for(&["later"])));
    for (i, range) in gaps.iter().enumerate() {
        let gap = asked[i + 1].0 - asked[i].0;
        assert!(range.contains(&gap), "gap {i}: {gap} ms");
    }
    // Once the topic appears, the answer has what the client wants, and the
    // client asks no more until the metadata ages.
    state.borrow_mut().add_topic("later", 1, 1);
    assert!(h.run_until(|h| h.client.metadata().topics.contains_key("later"), 2_000));
    let seen = h.seen(MetadataRequest::API_KEY).len();
    h.run_for(60_000);
    assert!(h.seen(MetadataRequest::API_KEY).len() == seen);
}

/// A draw of a connection id: its name, the base, the draws made before,
/// the ids open connections hold, then the id drawn and the draws made
/// after it.
type Draw = (&'static str, u32, u32, &'static [u32], (u32, u32));

#[test]
fn connection_ids_are_drawn_in_turn_within_the_range_and_skip_held_ones() {
    const LANE: u32 = conn_base(3);
    let r = CONN_ID_RANGE;
    let rows: [Draw; 6] = [
        ("the first id", 0, 0, &[], (1, 1)),
        ("in turn", 0, 5, &[], (6, 6)),
        ("the last id of the range", 0, r - 2, &[], (r - 1, r - 1)),
        ("back to the first", 0, r - 1, &[], (1, r)),
        ("past the held ids", 0, 0, &[1, 2], (3, 3)),
        (
            "a lane, back to its first id and past it",
            LANE,
            r - 1,
            &[LANE + 1],
            (LANE + 2, r + 1),
        ),
    ];
    for (name, base, drawn, held, (id, after)) in rows {
        let held: BTreeSet<ConnId> = held.iter().map(|id| ConnId(*id)).collect();
        assert!(
            draw_conn_id(base, drawn, &held) == (ConnId(id), after),
            "{name}"
        );
    }
    // Lanes wrap, and every id of every lane stays below `1 << 30`.
    let bases = [(0, 0), (1, r), (1_023, 1_023 * r), (1_024, 0), (1_025, r)];
    for (lane, base) in bases {
        assert!(conn_base(lane) == base, "lane {lane}");
    }
    assert!(conn_base(1_023) + r - 1 < 1 << 30);
}

#[test]
fn a_client_owns_the_connection_ids_of_its_range() {
    let r = CONN_ID_RANGE;
    let with_base = |conn_base: u32| {
        KafkaClient::new(
            vec![Endpoint::kafka(NodeId(1))],
            "test",
            ClientOptions {
                conn_base,
                ..ClientOptions::default()
            },
        )
    };
    // Rows: the base, and the ids with whether the client owns each.
    let rows = [
        (0, vec![(0, false), (1, true), (r - 1, true), (r, false)]),
        (
            conn_base(2),
            vec![
                (2 * r, false),
                (2 * r + 1, true),
                (3 * r - 1, true),
                (3 * r, false),
                (1, false),
            ],
        ),
    ];
    for (base, ids) in rows {
        let c = with_base(base);
        let owned: Vec<(u32, bool)> = ids
            .iter()
            .map(|(id, _)| (*id, c.owns_conn(ConnId(*id))))
            .collect();
        assert!(owned == ids, "base {base}");
    }
}

/// Two clients of one node on different lanes, routing each frame to the
/// client whose range holds its connection id.
struct TwoClients {
    clients: [KafkaClient; 2],
}

impl TwoClients {
    fn deadline(&self, now: Millis) -> Option<Millis> {
        self.clients
            .iter()
            .filter_map(|c| c.next_deadline(now))
            .min()
    }
}

impl Driven for TwoClients {
    type Event = (usize, ClientEvent);

    fn frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> (Vec<Self::Event>, Option<Millis>) {
        let i = usize::from(!self.clients[0].owns_conn(frame.conn));
        let events = self.clients[i].on_frame(ctx, frame);
        let deadline = self.deadline(ctx.now());
        (events.into_iter().map(|e| (i, e)).collect(), deadline)
    }

    fn tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<Self::Event>, Option<Millis>) {
        let mut events = Vec::new();
        for (i, c) in self.clients.iter_mut().enumerate() {
            let (ticked, _) = c.on_tick(ctx);
            events.extend(ticked.into_iter().map(|e| (i, e)));
        }
        (events, self.deadline(ctx.now()))
    }
}

#[test]
fn two_clients_of_one_node_draw_disjoint_connection_ids() {
    let r = CONN_ID_RANGE;
    let on_lane = |lane: u32| {
        KafkaClient::new(
            vec![Endpoint::kafka(NodeId(1))],
            &format!("lane-{lane}"),
            ClientOptions {
                conn_base: conn_base(lane),
                ..ClientOptions::default()
            },
        )
    };
    let pair = TwoClients {
        clients: [on_lane(1), on_lane(2)],
    };
    let mut h = Harness::new(pair, cluster(&[("orders", 1)]));
    // Both bootstrap against the same broker at once, each on its own
    // connection.
    assert!(h.run_until(
        |h| {
            h.client
                .clients
                .iter()
                .all(|c| c.metadata().updated_at.is_some())
        },
        1_000
    ));
    let conns: Vec<(u64, String)> = h
        .client
        .clients
        .iter()
        .map(|c| {
            let conn = &c.snapshot()["connections"][0];
            (
                conn["conn"].as_u64().unwrap_or_default(),
                conn["state"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    assert!(
        conns
            == vec![
                (u64::from(r + 1), "ready".to_string()),
                (u64::from(2 * r + 1), "ready".to_string()),
            ]
    );
    let client_ids: Vec<Option<String>> = h
        .seen(MetadataRequest::API_KEY)
        .iter()
        .map(|s: &Seen| s.client_id.clone())
        .collect();
    assert!(client_ids == vec![Some("lane-1".to_string()), Some("lane-2".to_string())]);
}
