//! The producer against the fake broker: linger and batch size, the
//! partitioners, acks, idempotent sequences across retries, the epoch a
//! failed batch raises, deduplicated resends, and the delivery timeout.

use std::{collections::BTreeMap, ops::RangeInclusive, rc::Rc};

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        init_producer_id_request::InitProducerIdRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{RecordBatch, RecordHeader, RecordsPayload},
};

use super::{
    Acks, Compression, Producer, ProducerConfig, ProducerEvent, ProducerRecord, RttHistogram,
    SeqNo,
    batch::{BatchRecord, ProducerStamp, build_batch},
    test_support::{Harness, client, cluster},
};
use crate::lab::{
    codes,
    net::{Millis, NodeId},
};

fn producer(config: ProducerConfig) -> Producer {
    Producer::new(client(&[1]), config, 7)
}

/// Run until the producer has metadata and, when it is idempotent, its
/// producer id.
fn ready(h: &mut Harness<Producer>) {
    assert!(h.run_until(
        |h| {
            h.client.client().metadata().updated_at.is_some()
                && (!h.client.config().idempotent() || h.client.producer_id().is_some())
        },
        1_000
    ));
}

fn record(topic: &str, key: Option<&str>, value: &str) -> ProducerRecord {
    ProducerRecord {
        topic: topic.to_string(),
        key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        ..Default::default()
    }
}

fn send(h: &mut Harness<Producer>, record: ProducerRecord) -> SeqNo {
    h.with_client(|p, ctx| p.send(ctx.now(), record))
}

/// The `Produce` requests the brokers saw: when each arrived, where, and
/// its body.
fn produces(h: &Harness<Producer>) -> Vec<(Millis, NodeId, ProduceRequest)> {
    h.seen(ProduceRequest::API_KEY)
        .iter()
        .map(|seen| (seen.at, seen.broker, seen.decode()))
        .collect()
}

/// The record batches a `Produce` request carries.
fn batches_of(request: &ProduceRequest) -> Vec<RecordBatch> {
    request
        .topic_data
        .iter()
        .flat_map(|topic| &topic.partition_data)
        .filter_map(|partition| partition.records.as_ref().and_then(RecordsPayload::as_v2))
        .flatten()
        .cloned()
        .collect()
}

/// `(seq, partition, offset, latency)` of every acknowledgement, by sequence
/// number.
fn acked(events: &[ProducerEvent]) -> Vec<(SeqNo, i32, i64, Millis)> {
    let mut acked: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ProducerEvent::Acked {
                seq,
                partition,
                offset,
                latency_ms,
                ..
            } => Some((*seq, *partition, *offset, *latency_ms)),
            ProducerEvent::Failed { .. } => None,
        })
        .collect();
    acked.sort_unstable();
    acked
}

/// `(seq, partition, code)` of every failure.
fn failed(events: &[ProducerEvent]) -> Vec<(SeqNo, i32, i16)> {
    events
        .iter()
        .filter_map(|event| match event {
            ProducerEvent::Failed {
                seq,
                partition,
                code,
                ..
            } => Some((*seq, *partition, *code)),
            ProducerEvent::Acked { .. } => None,
        })
        .collect()
}

/// `(base offset, base sequence, producer epoch, values)` of every batch of
/// a partition log.
fn log(h: &Harness<Producer>, topic: &str, partition: i32) -> Vec<(i64, i32, i16, Vec<String>)> {
    h.state
        .borrow()
        .batches(topic, partition)
        .iter()
        .map(|batch| {
            let values = batch
                .records
                .iter()
                .map(|r| {
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into_owned()
                })
                .collect();
            (
                batch.base_offset,
                batch.base_sequence,
                batch.producer_epoch,
                values,
            )
        })
        .collect()
}

#[test]
fn records_wait_for_the_linger_then_leave_as_one_idempotent_batch() {
    let state = cluster(&[("orders", 1)]);
    let topic_id = state.borrow().topics["orders"].id;
    let mut h = Harness::new(producer(ProducerConfig::default()), Rc::clone(&state));
    ready(&mut h);
    let t0 = h.now();
    let header = RecordHeader {
        key: "trace".to_string(),
        value: Some(Bytes::from_static(b"t-1")),
    };
    let mut expected = Vec::new();
    for (key, value) in [("k1", "v1"), ("k2", "v2"), ("k3", "v3")] {
        let mut sent = record("orders", Some(key), value);
        sent.headers = vec![header.clone()];
        expected.push(BatchRecord {
            timestamp: i64::try_from(h.now()).unwrap(),
            key: sent.key.clone(),
            value: sent.value.clone(),
            headers: vec![header.clone()],
        });
        send(&mut h, sent);
        h.run_for(1);
    }
    // The first record opened the batch at t0, so `linger.ms` (5) holds it
    // until t0 + 5, and the request reaches the broker 5 ms later.
    h.run_for(1);
    assert!(produces(&h).is_empty());
    assert!(h.run_until(|h| !produces(h).is_empty(), 100));
    let produced = produces(&h);
    assert!(produced.len() == 1);
    let (at, broker, request) = &produced[0];
    assert!(*at == t0 + 10);
    assert!(*broker == NodeId(1));
    let batch = build_batch(
        &expected,
        Some(ProducerStamp {
            producer_id: 1_000,
            producer_epoch: 0,
            base_sequence: 0,
        }),
        0,
    );
    assert!(
        *request
            == ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 30_000,
                topic_data: vec![TopicProduceData {
                    // Produce v13 names the topic by id only.
                    name: String::new(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(RecordsPayload::V2(vec![batch])),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
    // The answer arrives at t0 + 15; each latency runs from the record's send.
    assert!(h.run_until(|h| h.events.len() == 3, 100));
    assert!(
        acked(&h.take_events())
            == vec![
                (SeqNo(1), 0, 0, 15),
                (SeqNo(2), 0, 1, 14),
                (SeqNo(3), 0, 2, 13),
            ]
    );
    let metrics = h.client.metrics();
    assert!(
        (
            metrics.sent,
            metrics.acked,
            metrics.failed,
            metrics.retried,
            metrics.batches_sent
        ) == (3, 3, 0, 0, 1)
    );
    assert!(metrics.last_offsets == BTreeMap::from([(("orders".to_string(), 0), 2)]));
    let init: InitProducerIdRequest = h.seen(InitProducerIdRequest::API_KEY)[0].decode();
    assert!(
        init == InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: i32::MAX,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        }
    );
}

/// When a batch reached the broker after the first send, its record count
/// and its base sequence.
type Arrival = (Millis, usize, i32);

#[test]
fn a_batch_leaves_when_the_linger_passes_or_when_it_is_full() {
    // A 100-byte value makes a 110-byte record, and a v2 batch spends 61
    // bytes before its first record, so 300 bytes hold two records. Rows:
    // `linger.ms`, `batch.size`, and per batch the time it reaches the
    // broker after the first send, its record count and its base sequence.
    let value = "x".repeat(100);
    let rows: [(&str, Millis, usize, Vec<Arrival>); 3] = [
        (
            "the linger holds the records together",
            5,
            16_384,
            vec![(10, 3, 0)],
        ),
        (
            "a full batch leaves at once and the rest after the linger",
            1_000,
            300,
            vec![(5, 2, 0), (1_005, 1, 2)],
        ),
        (
            "without a linger each record leaves at once",
            0,
            16_384,
            vec![(5, 1, 0), (5, 1, 1), (5, 1, 2)],
        ),
    ];
    for (name, linger_ms, batch_size, expected) in rows {
        let config = ProducerConfig {
            linger_ms,
            batch_size,
            ..Default::default()
        };
        let mut h = Harness::new(producer(config), cluster(&[("orders", 1)]));
        ready(&mut h);
        let t0 = h.now();
        for _ in 0..3 {
            send(&mut h, record("orders", Some("k"), &value));
        }
        h.run_for(2_000);
        let actual: Vec<Arrival> = produces(&h)
            .iter()
            .flat_map(|(at, _, request)| {
                batches_of(request)
                    .into_iter()
                    .map(move |b| (at - t0, b.records.len(), b.base_sequence))
            })
            .collect();
        assert!(actual == expected, "{name}");
        assert!(acked(&h.take_events()).len() == 3, "{name}");
    }
}

#[test]
fn keys_pick_the_partition_like_the_jvm_and_keyless_records_stick() {
    let config = ProducerConfig {
        linger_ms: 0,
        sticky_batch_records: 2,
        ..Default::default()
    };
    let mut h = Harness::new(producer(config), cluster(&[("events", 3)]));
    ready(&mut h);
    // `toPositive(murmur2(key)) % 3` over the murmur2 values the partitioner
    // tests pin: "a" is -1563381124, "abc" 479470107, "abcd" -1323649548 and
    // "kafka" -798503068.
    let keyed = [("a", 1), ("abc", 0), ("abcd", 2), ("kafka", 1)];
    for (key, _) in keyed {
        send(&mut h, record("events", Some(key), key));
    }
    for i in 0..6 {
        send(&mut h, record("events", None, &format!("n{i}")));
    }
    h.run_for(1_000);
    let acks = acked(&h.take_events());
    let partitions: Vec<i32> = acks.iter().map(|(_, partition, _, _)| *partition).collect();
    let expected: Vec<i32> = keyed.iter().map(|(_, partition)| *partition).collect();
    assert!(partitions[..4] == expected[..]);
    // Keyless records stay on one partition for `sticky_batch_records`
    // records, then move to another one.
    let keyless = &partitions[4..];
    assert!(keyless.len() == 6);
    for run in keyless.chunks(2) {
        assert!(run[0] == run[1]);
    }
    assert!(keyless[1] != keyless[2] && keyless[3] != keyless[4]);
    // Every record is in the log of the partition it was acked on.
    for partition in 0..3 {
        let values: Vec<String> = log(&h, "events", partition)
            .into_iter()
            .flat_map(|(_, _, _, values)| values)
            .collect();
        let acked_here: Vec<String> = acks
            .iter()
            .filter(|(_, p, _, _)| *p == partition)
            .map(|(seq, _, _, _)| {
                let index = usize::try_from(seq.0).unwrap() - 1;
                if index < 4 {
                    keyed[index].0.to_string()
                } else {
                    format!("n{}", index - 4)
                }
            })
            .collect();
        assert!(values == acked_here, "partition {partition}");
    }
}

#[test]
fn acks_decide_the_wire_value_the_producer_id_and_when_a_record_counts_as_sent() {
    // The broker holds an `acks=-1` answer 20 ms for the replicas and answers
    // `acks=1` at once; `acks=0` gets no answer, and a record counts as sent
    // once it is written. Idempotence needs `acks=all`, so only that row asks
    // for a producer id. Rows: acks, the wire value, InitProducerId requests,
    // the producer id in the batch, the offset and the latency reported.
    let rows = [
        (Acks::All, -1, 1, 1_000, 0, 35),
        (Acks::Leader, 1, 0, -1, 0, 15),
        (Acks::None, 0, 0, -1, -1, 5),
    ];
    for (acks, wire, init_requests, producer_id, offset, latency) in rows {
        let state = cluster(&[("orders", 1)]);
        state.borrow_mut().knobs.produce_delay_ms = 20;
        let config = ProducerConfig {
            acks,
            ..Default::default()
        };
        let mut h = Harness::new(producer(config), Rc::clone(&state));
        ready(&mut h);
        send(&mut h, record("orders", Some("k"), "v"));
        h.run_for(1_000);
        let produced = produces(&h);
        assert!(produced.len() == 1, "{acks:?}");
        assert!(produced[0].2.acks == wire, "{acks:?}");
        assert!(
            batches_of(&produced[0].2)[0].producer_id == producer_id,
            "{acks:?}"
        );
        assert!(
            h.seen(InitProducerIdRequest::API_KEY).len() == init_requests,
            "{acks:?}"
        );
        assert!(
            acked(&h.take_events()) == vec![(SeqNo(1), 0, offset, latency)],
            "{acks:?}"
        );
        let rtt = &h.client.metrics().rtt;
        assert!(
            (rtt.count(), rtt.max(), rtt.mean()) == (1, latency, latency),
            "{acks:?}"
        );
        assert!(log(&h, "orders", 0).len() == 1, "{acks:?}");
    }
}

#[test]
fn the_latency_histogram_counts_each_ack_in_its_bucket() {
    let mut rtt = RttHistogram::default();
    for latency in [0, 1, 3, 35, 35, 6_000] {
        rtt.record(latency);
    }
    let counts: Vec<(Option<Millis>, u64)> = rtt.snapshot()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| (b["le"].as_u64(), b["count"].as_u64().unwrap()))
        .filter(|(_, count)| *count > 0)
        .collect();
    assert!(counts == vec![(Some(1), 2), (Some(5), 1), (Some(50), 2), (None, 1)]);
    assert!((rtt.count(), rtt.max(), rtt.mean()) == (6, 6_000, 1_012));
}

#[test]
fn a_retry_keeps_the_sequences_of_the_batches_behind_it() {
    // Three one-record batches leave together (`linger.ms` 0) with sequences
    // 0, 1 and 2. Rows:
    // - the second answers NOT_LEADER_OR_FOLLOWER, so the third reaches the
    //   broker one sequence early and answers OUT_OF_ORDER_SEQUENCE_NUMBER;
    //   both go again, in order, to the same leader;
    // - the leadership moved before the producer knew: all three answer
    //   NOT_LEADER_OR_FOLLOWER, the producer refreshes its metadata, and all
    //   three go to the new leader one at a time, in order.
    // No row raises the epoch: the answers name no gap in the sequences.
    let rows = [
        (
            "an error on the second batch",
            true,
            false,
            NodeId(1),
            vec![1, 2],
        ),
        (
            "a leader the producer did not know",
            false,
            true,
            NodeId(2),
            vec![0, 1, 2],
        ),
    ];
    for (name, inject, move_leader, retried_to, retried) in rows {
        let state = cluster(&[("orders", 1)]);
        let config = ProducerConfig {
            linger_ms: 0,
            ..Default::default()
        };
        let mut h = Harness::new(producer(config), Rc::clone(&state));
        ready(&mut h);
        if inject {
            state.borrow_mut().knobs.produce_errors.insert(
                ("orders".to_string(), 0),
                [codes::NONE, codes::NOT_LEADER_OR_FOLLOWER].into(),
            );
        }
        if move_leader {
            state.borrow_mut().set_leader("orders", 0, 2);
        }
        for value in ["r1", "r2", "r3"] {
            send(&mut h, record("orders", None, value));
        }
        h.run_for(2_000);
        let values = |v: &str| vec![v.to_string()];
        assert!(
            log(&h, "orders", 0)
                == vec![
                    (0, 0, 0, values("r1")),
                    (1, 1, 0, values("r2")),
                    (2, 2, 0, values("r3")),
                ],
            "{name}"
        );
        let offsets: Vec<(SeqNo, i64)> = acked(&h.take_events())
            .into_iter()
            .map(|(seq, _, offset, _)| (seq, offset))
            .collect();
        assert!(
            offsets == vec![(SeqNo(1), 0), (SeqNo(2), 1), (SeqNo(3), 2)],
            "{name}"
        );
        // Each retry went alone, to the leader the metadata named.
        let resent: Vec<(NodeId, Vec<i32>)> = produces(&h)[3..]
            .iter()
            .map(|(_, broker, request)| {
                let sequences = batches_of(request)
                    .iter()
                    .map(|b| b.base_sequence)
                    .collect();
                (*broker, sequences)
            })
            .collect();
        let expected: Vec<(NodeId, Vec<i32>)> =
            retried.iter().map(|s| (retried_to, vec![*s])).collect();
        assert!(resent == expected, "{name}");
        assert!(
            h.client.metrics().retried == u64::try_from(retried.len()).unwrap(),
            "{name}"
        );
        assert!(h.client.producer_id() == Some((1_000, 0)), "{name}");
        assert!(h.seen(InitProducerIdRequest::API_KEY).len() == 1, "{name}");
    }
}

#[test]
fn a_batch_that_fails_for_good_raises_the_epoch_on_the_client() {
    // INVALID_RECORD fails the second batch for good. Kafka's idempotent
    // producer then raises its epoch itself, without an InitProducerId, and
    // the next batch starts the new epoch at sequence 0, which the broker
    // takes. Rows: the records go one at a time, or all three leave together
    // (`linger.ms` 0): then the third reaches the broker one sequence early,
    // answers OUT_OF_ORDER_SEQUENCE_NUMBER after the epoch rose, and goes
    // again re-sequenced at the new epoch. Last: the `(epoch, sequence)` of
    // every batch sent.
    let rows = [
        ("one at a time", true, vec![(0, 0), (0, 1), (1, 0)]),
        (
            "in flight together",
            false,
            vec![(0, 0), (0, 1), (0, 2), (1, 0)],
        ),
    ];
    for (name, one_at_a_time, sent) in rows {
        let state = cluster(&[("orders", 1)]);
        state.borrow_mut().knobs.produce_errors.insert(
            ("orders".to_string(), 0),
            [codes::NONE, codes::INVALID_RECORD].into(),
        );
        let config = ProducerConfig {
            linger_ms: 0,
            ..Default::default()
        };
        let mut h = Harness::new(producer(config), Rc::clone(&state));
        ready(&mut h);
        for (done, value) in ["r1", "r2", "r3"].into_iter().enumerate() {
            send(&mut h, record("orders", None, value));
            if one_at_a_time {
                assert!(h.run_until(|h| h.events.len() > done, 1_000), "{name}");
            }
        }
        h.run_for(1_000);
        let events = h.take_events();
        let offsets: Vec<(SeqNo, i64)> = acked(&events).iter().map(|a| (a.0, a.2)).collect();
        assert!(offsets == vec![(SeqNo(1), 0), (SeqNo(3), 1)], "{name}");
        assert!(
            failed(&events) == vec![(SeqNo(2), 0, codes::INVALID_RECORD)],
            "{name}"
        );
        let values = |v: &str| vec![v.to_string()];
        assert!(
            log(&h, "orders", 0) == vec![(0, 0, 0, values("r1")), (1, 0, 1, values("r3"))],
            "{name}"
        );
        let stamps: Vec<(i16, i32)> = produces(&h)
            .iter()
            .flat_map(|(_, _, request)| batches_of(request))
            .map(|b| (b.producer_epoch, b.base_sequence))
            .collect();
        assert!(stamps == sent, "{name}");
        assert!(h.client.producer_id() == Some((1_000, 1)), "{name}");
        assert!(h.seen(InitProducerIdRequest::API_KEY).len() == 1, "{name}");
    }
}

#[test]
fn a_resend_after_a_lost_answer_is_deduplicated_by_its_sequence() {
    // The broker appends the batch, but its answer is lost. After
    // `request.timeout.ms` the client gives up the connection, and the
    // producer sends the batch again with the same producer id, epoch and
    // sequence. The broker finds it among the producer's last five batches
    // and answers success with the offset it gave the first time.
    let state = cluster(&[("orders", 1)]);
    let mut h = Harness::new(producer(ProducerConfig::default()), Rc::clone(&state));
    ready(&mut h);
    state.borrow_mut().knobs.drop_responses = 1;
    send(&mut h, record("orders", Some("k"), "once"));
    h.run_for(29_000);
    assert!(h.events.is_empty());
    assert!(h.run_until(|h| !h.events.is_empty(), 5_000));
    assert!(
        acked(&h.take_events())
            .iter()
            .map(|a| (a.0, a.2))
            .collect::<Vec<_>>()
            == vec![(SeqNo(1), 0)]
    );
    assert!(log(&h, "orders", 0) == vec![(0, 0, 0, vec!["once".to_string()])]);
    let stamps: Vec<(i64, i16, i32)> = produces(&h)
        .iter()
        .flat_map(|(_, _, request)| batches_of(request))
        .map(|b| (b.producer_id, b.producer_epoch, b.base_sequence))
        .collect();
    assert!(stamps == vec![(1_000, 0, 0), (1_000, 0, 0)]);
    assert!(h.client.metrics().retried == 1);
}

#[test]
fn compressed_batches_reach_the_log_readable() {
    for compression in [Compression::None, Compression::Gzip, Compression::Snappy] {
        let config = ProducerConfig {
            compression,
            ..Default::default()
        };
        let mut h = Harness::new(producer(config), cluster(&[("orders", 1)]));
        ready(&mut h);
        for value in ["first", "second"] {
            send(&mut h, record("orders", Some("k"), value));
        }
        h.run_for(1_000);
        let batches = h.state.borrow().batches("orders", 0);
        assert!(batches.len() == 1, "{compression:?}");
        assert!(
            batches[0].attributes.0 & 0b111 == compression.attribute_bits(),
            "{compression:?}"
        );
        assert!(
            log(&h, "orders", 0)
                == vec![(0, 0, 0, vec!["first".to_string(), "second".to_string()])],
            "{compression:?}"
        );
    }
}

/// A `Metadata` request for `topics` by name.
fn named(topics: &[&str]) -> MetadataRequest {
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

/// The `Metadata` requests the brokers saw from `from` on, with when each
/// arrived.
fn metadata_since(h: &Harness<Producer>, from: Millis) -> Vec<(Millis, MetadataRequest)> {
    h.seen(MetadataRequest::API_KEY)
        .iter()
        .filter(|s| s.at >= from)
        .map(|s| (s.at, s.decode()))
        .collect()
}

#[test]
fn a_topic_created_after_the_first_lookup_is_found() {
    // The first lookup answers UNKNOWN_TOPIC_OR_PARTITION. As Kafka's
    // `waitOnMetadata`, the producer asks for the topic again at the
    // metadata backoff (100 ms, then 200 ms, doubling, with jitter), and the
    // record goes out once the topic appears. It takes its timestamp then,
    // as `send` does after the wait.
    let state = cluster(&[]);
    let mut h = Harness::new(producer(ProducerConfig::default()), Rc::clone(&state));
    ready(&mut h);
    let t0 = h.now();
    send(&mut h, record("orders", Some("k"), "v"));
    h.run_for(500);
    let lookups = metadata_since(&h, t0);
    let requests: Vec<MetadataRequest> = lookups.iter().map(|(_, r)| r.clone()).collect();
    assert!(requests == vec![named(&["orders"]); 3]);
    assert!(lookups[0].0 == t0 + 5);
    state.borrow_mut().add_topic("orders", 1, 3);
    assert!(h.run_until(|h| !h.events.is_empty(), 2_000));
    let latency_ms = h.now() - t0;
    assert!(
        h.take_events()
            == vec![ProducerEvent::Acked {
                seq: SeqNo(1),
                topic: "orders".to_string(),
                partition: 0,
                offset: 0,
                latency_ms,
            }]
    );
    // The lookup that found the topic was the last; its answer placed the
    // record.
    let placed_at = metadata_since(&h, t0).last().unwrap().0 + h.latency;
    let mut expected = build_batch(
        &[BatchRecord {
            timestamp: i64::try_from(placed_at).unwrap(),
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            headers: Vec::new(),
        }],
        Some(ProducerStamp {
            producer_id: 1_000,
            producer_epoch: 0,
            base_sequence: 0,
        }),
        0,
    );
    expected.partition_leader_epoch = 0;
    assert!(state.borrow().batches("orders", 0) == vec![expected]);
}

#[test]
fn a_topic_asked_for_while_a_refresh_is_out_is_looked_up() {
    let state = cluster(&[("a", 1), ("b", 1)]);
    let mut c = client(&[1]);
    c.add_topics(["a"]);
    let mut h = Harness::new(
        Producer::new(c, ProducerConfig::default(), 7),
        Rc::clone(&state),
    );
    // `Metadata` for `a` leaves at 10 ms and is answered at 20 ms.
    h.run_for(12);
    let t0 = h.now();
    send(&mut h, record("b", Some("k"), "v"));
    assert!(h.run_until(|h| !h.events.is_empty(), 1_000));
    let latency_ms = h.now() - t0;
    assert!(
        h.take_events()
            == vec![ProducerEvent::Acked {
                seq: SeqNo(1),
                topic: "b".to_string(),
                partition: 0,
                offset: 0,
                latency_ms,
            }]
    );
    let requests: Vec<MetadataRequest> =
        metadata_since(&h, 0).into_iter().map(|(_, r)| r).collect();
    assert!(requests == vec![named(&["a"]), named(&["a", "b"])]);
}

#[test]
fn a_record_that_cannot_be_placed_waits_up_to_max_block_ms() {
    // Rows: the topics of the cluster, the partition of the record, whether
    // the producer looks the topic up while the record waits, and the
    // failure Kafka's `send` ends with and when: `waitOnMetadata`'s
    // `TimeoutException` once `max.block.ms` passed, or at once the
    // `IllegalArgumentException` of a negative partition.
    let rows = [
        (
            "a topic that never appears",
            vec![],
            None,
            true,
            (
                -1,
                codes::REQUEST_TIMED_OUT,
                "Topic orders not present in metadata after 2000 ms.",
            ),
            2_000,
        ),
        (
            "a partition beyond the partition count",
            vec![("orders", 1)],
            Some(3),
            true,
            (
                3,
                codes::REQUEST_TIMED_OUT,
                "Partition 3 of topic orders with partition count 1 is not present in metadata after 2000 ms.",
            ),
            2_000,
        ),
        (
            "a negative partition",
            vec![("orders", 1)],
            Some(-1),
            false,
            (
                -1,
                codes::UNKNOWN_SERVER_ERROR,
                "Invalid partition: -1. Partition number should always be non-negative or null.",
            ),
            0,
        ),
    ];
    for (name, topics, partition, looks_up, (failed_partition, code, message), after) in rows {
        let config = ProducerConfig {
            max_block_ms: 2_000,
            ..Default::default()
        };
        let mut h = Harness::new(producer(config), cluster(&topics));
        ready(&mut h);
        let t0 = h.now();
        send(
            &mut h,
            ProducerRecord {
                partition,
                ..record("orders", Some("k"), "v")
            },
        );
        assert!(h.run_until(|h| !h.events.is_empty(), 3_000), "{name}");
        assert!(h.now() == t0 + after, "{name}");
        assert!(
            h.take_events()
                == vec![ProducerEvent::Failed {
                    seq: SeqNo(1),
                    topic: "orders".to_string(),
                    partition: failed_partition,
                    code,
                    message: Some(message.to_string()),
                }],
            "{name}"
        );
        assert!(h.client.pending_records() == 0, "{name}");
        assert!(h.client.metrics().failed == 1, "{name}");
        let lookups = metadata_since(&h, t0);
        assert!(!lookups.is_empty() == looks_up, "{name}");
        assert!(
            lookups.iter().all(|(_, r)| *r == named(&["orders"])),
            "{name}"
        );
    }
}

/// The lookups a producer makes while its records wait for a leader: the
/// error code the broker gives a partition without one, when the leader is
/// back (after the send, `0` for before it), and the gap between each
/// lookup and the next up to the one that finds the leader.
type LeaderlessRow = (&'static str, i16, Millis, Vec<RangeInclusive<Millis>>);

#[test]
fn records_for_a_topic_without_leaders_look_it_up_until_the_leaders_are_back() {
    // For a moment after every broker restarted, no partition has a leader.
    // The producer here tracks no topic when its first metadata answer,
    // which names every topic, shows `orders` that way, so no answer asks
    // for `orders` again by itself. As Kafka's `Sender.sendProducerData`
    // does, a record waiting for a leader puts its topic among
    // `RecordAccumulator.ready`'s `unknownLeaderTopics`, and each pass of
    // the sender asks for a metadata update (`requestUpdate`): the client
    // looks the topic up at once, then again after every answer that still
    // shows no leader, at its backoff of `retry.backoff.ms` (100 ms)
    // doubling while the answers bring no newer leader epoch, with 20 %
    // jitter. The gaps add the 10 ms round trip. Kafka's brokers answer a
    // partition without a leader with `LEADER_NOT_AVAILABLE`, which asks
    // again by itself; with `NONE` only the producer does.
    let doubling = vec![90..=129, 170..=249, 330..=489, 650..=969];
    let rows: [LeaderlessRow; 4] = [
        (
            "LEADER_NOT_AVAILABLE, the leader back before the send",
            codes::LEADER_NOT_AVAILABLE,
            0,
            vec![],
        ),
        (
            "NONE, the leader back before the send",
            codes::NONE,
            0,
            vec![],
        ),
        (
            "LEADER_NOT_AVAILABLE, the leader back a second after the send",
            codes::LEADER_NOT_AVAILABLE,
            1_000,
            doubling.clone(),
        ),
        (
            "NONE, the leader back a second after the send",
            codes::NONE,
            1_000,
            doubling,
        ),
    ];
    for (name, code, back_after, gaps) in rows {
        let state = cluster(&[("orders", 1)]);
        {
            let mut s = state.borrow_mut();
            s.knobs.leaderless_error = code;
            s.set_leader("orders", 0, -1);
        }
        let mut h = Harness::new(producer(ProducerConfig::default()), Rc::clone(&state));
        ready(&mut h);
        assert!(
            h.client.client().metadata().leader("orders", 0).is_none(),
            "{name}"
        );
        // The backoff after the first answer passes, and nothing else asks
        // for the metadata.
        h.run_for(1_000);
        let t0 = h.now();
        if back_after == 0 {
            state.borrow_mut().set_leader("orders", 0, 1);
        }
        send(&mut h, record("orders", Some("k"), "v"));
        if back_after > 0 {
            h.run_for(back_after);
            assert!(metadata_since(&h, t0).len() == gaps.len(), "{name}");
            assert!(h.events.is_empty(), "{name}");
            state.borrow_mut().set_leader("orders", 0, 1);
        }
        assert!(h.run_until(|h| !h.events.is_empty(), 3_000), "{name}");
        let latency_ms = h.now() - t0;
        assert!(
            h.take_events()
                == vec![ProducerEvent::Acked {
                    seq: SeqNo(1),
                    topic: "orders".to_string(),
                    partition: 0,
                    offset: 0,
                    latency_ms,
                }],
            "{name}"
        );
        let lookups = metadata_since(&h, t0);
        let requests: Vec<MetadataRequest> = lookups.iter().map(|(_, r)| r.clone()).collect();
        assert!(
            requests == vec![named(&["orders"]); gaps.len() + 1],
            "{name}"
        );
        assert!(lookups[0].0 == t0 + 5, "{name}");
        for (i, range) in gaps.iter().enumerate() {
            let gap = lookups[i + 1].0 - lookups[i].0;
            assert!(range.contains(&gap), "{name}: gap {i}: {gap} ms");
        }
    }
}

#[test]
fn a_batch_without_a_leader_expires_with_kafkas_message() {
    let state = cluster(&[("orders", 1)]);
    state.borrow_mut().set_leader("orders", 0, -1);
    let config = ProducerConfig {
        delivery_timeout_ms: 2_000,
        linger_ms: 0,
        ..Default::default()
    };
    let mut h = Harness::new(producer(config), Rc::clone(&state));
    ready(&mut h);
    let t0 = h.now();
    send(&mut h, record("orders", Some("k"), "v"));
    assert!(h.run_until(|h| !h.events.is_empty(), 3_000));
    assert!(h.now() == t0 + 2_000);
    assert!(
        h.take_events()
            == vec![ProducerEvent::Failed {
                seq: SeqNo(1),
                topic: "orders".to_string(),
                partition: 0,
                code: codes::REQUEST_TIMED_OUT,
                message: Some(
                    "Expiring 1 record(s) for orders-0:2000 ms has passed since batch creation. \
                     The request has not been sent, or no server response has been received yet."
                        .to_string()
                ),
            }]
    );
    assert!(produces(&h).is_empty());
}
