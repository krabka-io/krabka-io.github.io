//! The client against the fake broker: negotiation, framing, routing,
//! coordinator lookups, timeouts and reconnection, and the metadata refresh
//! and its backoff.

use std::rc::Rc;

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
    ApiSpec, ClientError, ClientEvent, CoordinatorType, KafkaClient, RequestId, Response, Target,
    VersionTable,
    fake_broker::ClusterState,
    request::{frame_request, response_header_version},
    test_support::{Driven, Harness, client, cluster},
};
use crate::lab::{
    codes,
    net::{Endpoint, Millis, NodeId},
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
