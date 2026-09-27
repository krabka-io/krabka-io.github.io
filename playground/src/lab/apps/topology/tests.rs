use assert2::assert;
use bytes::Bytes;
use krabka_client_streams::EmbeddedTask;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_request::{key_value::KeyValue, topic_info::TopicInfo},
    streams_group_heartbeat_request::{Subtopology, Topology as WireTopology},
};
use serde_json::{Value, json};

use super::*;

fn spec(doc: &Value) -> TopologySpec {
    TopologySpec::parse(doc).unwrap()
}

fn config(pairs: &[(&str, &str)]) -> Vec<KeyValue> {
    pairs
        .iter()
        .map(|(k, v)| KeyValue {
            key: (*k).to_string(),
            value: (*v).to_string(),
            ..Default::default()
        })
        .collect()
}

fn changelog(name: &str) -> TopicInfo {
    TopicInfo {
        name: name.to_string(),
        partitions: 0,
        replication_factor: -1,
        topic_configs: config(&[
            ("cleanup.policy", "compact"),
            ("message.timestamp.type", "CreateTime"),
        ]),
        ..Default::default()
    }
}

fn node(name: &str, parent: Option<&str>, kind: PlanNodeKind) -> PlanNode {
    PlanNode {
        name: name.to_string(),
        parent: parent.map(str::to_string),
        kind,
    }
}

fn source(topic: &str) -> PlanNodeKind {
    PlanNodeKind::Source {
        topic: topic.to_string(),
    }
}

fn sink(topic: &str) -> PlanNodeKind {
    PlanNodeKind::Sink {
        topic: topic.to_string(),
    }
}

#[test]
fn ops_parse_from_the_spec() {
    let cases = [
        (
            json!({ "op": "filter", "field": "total", "gt": 100 }),
            Op::Filter {
                field: "total".to_string(),
                cmp: Cmp::Gt,
                value: json!(100),
            },
        ),
        (
            json!({ "op": "filter", "field": "tags", "contains": "vip" }),
            Op::Filter {
                field: "tags".to_string(),
                cmp: Cmp::Contains,
                value: json!("vip"),
            },
        ),
        (
            json!({ "op": "map", "select": ["id", "total"], "upper": "name" }),
            Op::Map(MapOp {
                select: Some(vec!["id".to_string(), "total".to_string()]),
                upper: vec!["name".to_string()],
                ..MapOp::default()
            }),
        ),
        (
            json!({ "op": "map", "rename": { "a": "b" }, "set": { "tag": "t-{seq}" } }),
            Op::Map(MapOp {
                rename: vec![("a".to_string(), "b".to_string())],
                set: vec![("tag".to_string(), Template::parse("t-{seq}").unwrap())],
                ..MapOp::default()
            }),
        ),
        (
            json!({ "op": "select_key", "field": "customer" }),
            Op::SelectKey {
                field: "customer".to_string(),
            },
        ),
        (
            json!({ "op": "count_by_key" }),
            Op::CountByKey { store: None },
        ),
        (
            json!({ "op": "count_by_key", "store": "counts" }),
            Op::CountByKey {
                store: Some("counts".to_string()),
            },
        ),
        (
            json!({ "op": "sum_by_key", "field": "total" }),
            Op::SumByKey {
                field: "total".to_string(),
                store: None,
            },
        ),
        (
            json!({ "op": "window_count", "size_ms": 1000 }),
            Op::WindowCount {
                size_ms: 1_000,
                advance_ms: 1_000,
                grace_ms: 0,
                store: None,
            },
        ),
        (
            json!({ "op": "window_count", "size_ms": 1000, "advance_ms": 250, "grace_ms": 50 }),
            Op::WindowCount {
                size_ms: 1_000,
                advance_ms: 250,
                grace_ms: 50,
                store: None,
            },
        ),
    ];
    for (doc, expected) in cases {
        assert!(Op::parse(&doc) == Ok(expected), "{doc}");
    }
}

#[test]
fn a_bad_op_names_its_problem() {
    let cases = [
        (json!({ "op": "join" }), "unknown op `join`"),
        (
            json!({ "field": "x" }),
            "an op needs `op`: {\"field\":\"x\"}",
        ),
        (
            json!({ "op": "filter", "field": "x" }),
            "filter: needs one comparison: gt, gte, lt, lte, eq, ne or contains",
        ),
        (
            json!({ "op": "filter", "field": "x", "gt": 1, "lt": 5 }),
            "filter: takes one comparison",
        ),
        (
            json!({ "op": "filter", "field": "x", "gt": 1, "extra": 1 }),
            "filter: unknown key `extra`",
        ),
        (
            json!({ "op": "map" }),
            "map: needs at least one of select, rename, set or upper",
        ),
        (
            json!({ "op": "map", "set": { "a": "{bogus}" } }),
            "map: `set.a`: unknown placeholder `{bogus}`",
        ),
        (
            json!({ "op": "select_key" }),
            "select_key: `field` is a required string",
        ),
        (
            json!({ "op": "window_count", "size_ms": 0 }),
            "window_count: `size_ms` is a positive number of milliseconds",
        ),
        (
            json!({ "op": "window_count", "size_ms": 100, "advance_ms": 200 }),
            "window_count: `advance_ms` is positive and at most `size_ms`",
        ),
        (
            json!({ "op": "count_by_key", "store": 3 }),
            "count_by_key: `store` is a name, not 3",
        ),
    ];
    for (doc, message) in cases {
        assert!(Op::parse(&doc) == Err(message.to_string()), "{doc}");
    }
    assert!(
        TopologySpec::parse(&json!({ "source": "a", "sink": "b", "ops": [{ "op": "nope" }] }))
            == Err("`topology.ops[0]`: unknown op `nope`".to_string())
    );
    assert!(
        TopologySpec::parse(&json!({ "source": "", "sink": "b" }))
            == Err("`topology.source` is a topic name".to_string())
    );
    assert!(
        TopologySpec::parse(&json!({ "source": "a", "sink": "b", "flow": 1 }))
            == Err("`topology`: unknown key `flow`".to_string())
    );
}

#[test]
fn each_op_compiles_to_its_processor_and_store() {
    let filter = Op::Filter {
        field: "total".to_string(),
        cmp: Cmp::Gt,
        value: json!(100),
    };
    let count = Op::CountByKey { store: None };
    let plan = Plan::new(
        "stats",
        &spec(&json!({ "source": "orders", "ops": [
            { "op": "filter", "field": "total", "gt": 100 },
            { "op": "count_by_key" },
        ], "sink": "counts" })),
    )
    .unwrap();
    assert!(
        plan == Plan {
            application_id: "stats".to_string(),
            nodes: vec![
                node("source", None, source("orders")),
                node("op-0-filter", Some("source"), PlanNodeKind::Op(filter)),
                node(
                    "op-1-count-by-key",
                    Some("op-0-filter"),
                    PlanNodeKind::Op(count)
                ),
                node("sink", Some("op-1-count-by-key"), sink("counts")),
            ],
            stores: vec![PlanStore {
                name: "count-by-key-1".to_string(),
                processor: "op-1-count-by-key".to_string(),
                kind: StoreKind::Count,
                changelog: "stats-count-by-key-1-changelog".to_string(),
            }],
            repartition_topics: Vec::new(),
        }
    );
}

#[test]
fn a_key_change_puts_a_repartition_topic_before_the_next_aggregation() {
    let plan = Plan::new(
        "stats",
        &spec(&json!({ "source": "orders", "ops": [
            { "op": "select_key", "field": "customer" },
            { "op": "map", "select": ["total"] },
            { "op": "sum_by_key", "field": "total", "store": "totals" },
            { "op": "window_count", "size_ms": 1000 },
        ], "sink": "out" })),
    )
    .unwrap();
    let repartition = "stats-totals-repartition";
    assert!(
        plan.nodes
            == vec![
                node("source", None, source("orders")),
                node(
                    "op-0-select-key",
                    Some("source"),
                    PlanNodeKind::Op(Op::SelectKey {
                        field: "customer".to_string()
                    })
                ),
                node(
                    "op-1-map",
                    Some("op-0-select-key"),
                    PlanNodeKind::Op(Op::Map(MapOp {
                        select: Some(vec!["total".to_string()]),
                        ..MapOp::default()
                    }))
                ),
                node(
                    "totals-repartition-filter",
                    Some("op-1-map"),
                    PlanNodeKind::DropNullKeys
                ),
                node(
                    "totals-repartition-sink",
                    Some("totals-repartition-filter"),
                    sink(repartition)
                ),
                node("totals-repartition-source", None, source(repartition)),
                node(
                    "op-2-sum-by-key",
                    Some("totals-repartition-source"),
                    PlanNodeKind::Op(Op::SumByKey {
                        field: "total".to_string(),
                        store: Some("totals".to_string()),
                    })
                ),
                // The sum keeps the key, so the window count needs no second
                // repartition.
                node(
                    "op-3-window-count",
                    Some("op-2-sum-by-key"),
                    PlanNodeKind::Op(Op::WindowCount {
                        size_ms: 1_000,
                        advance_ms: 1_000,
                        grace_ms: 0,
                        store: None,
                    })
                ),
                node("sink", Some("op-3-window-count"), sink("out")),
            ]
    );
    assert!(plan.repartition_topics == vec![repartition.to_string()]);
    assert!(
        plan.stores
            == vec![
                PlanStore {
                    name: "totals".to_string(),
                    processor: "op-2-sum-by-key".to_string(),
                    kind: StoreKind::Sum,
                    changelog: "stats-totals-changelog".to_string(),
                },
                PlanStore {
                    name: "window-count-3".to_string(),
                    processor: "op-3-window-count".to_string(),
                    kind: StoreKind::Window {
                        size_ms: 1_000,
                        advance_ms: 1_000,
                        grace_ms: 0,
                    },
                    changelog: "stats-window-count-3-changelog".to_string(),
                },
            ]
    );
}

#[test]
fn a_key_change_with_no_aggregation_after_it_writes_to_the_sink() {
    let plan = Plan::new(
        "app",
        &spec(&json!({ "source": "in", "ops": [{ "op": "select_key", "field": "id" }], "sink": "out" })),
    )
    .unwrap();
    assert!(plan.repartition_topics.is_empty());
    assert!(plan.nodes.len() == 3);
}

#[test]
fn a_store_name_used_twice_is_refused() {
    let doc = json!({ "source": "in", "ops": [
        { "op": "count_by_key", "store": "s" },
        { "op": "sum_by_key", "field": "x", "store": "s" },
    ], "sink": "out" });
    assert!(Plan::new("app", &spec(&doc)) == Err("the store `s` is named twice".to_string()));
}

#[test]
fn the_built_topology_is_the_kip_1071_wire_topology() {
    let compiled = CompiledTopology::new(
        "stats",
        &spec(&json!({ "source": "orders", "ops": [
            { "op": "filter", "field": "total", "gt": 100 },
            { "op": "count_by_key" },
        ], "sink": "counts" })),
    )
    .unwrap();
    assert!(
        compiled.built.to_wire_request()
            == WireTopology {
                epoch: 0,
                subtopologies: vec![Subtopology {
                    subtopology_id: "0".to_string(),
                    source_topics: vec!["orders".to_string()],
                    state_changelog_topics: vec![changelog("stats-count-by-key-1-changelog")],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
    assert!(compiled.built.subtopology_ids() == vec!["0".to_string()]);
}

#[test]
fn a_repartition_splits_the_wire_topology_in_two_subtopologies() {
    let compiled = CompiledTopology::new(
        "stats",
        &spec(&json!({ "source": "orders", "ops": [
            { "op": "select_key", "field": "customer" },
            { "op": "window_count", "size_ms": 1000, "store": "per-second" },
        ], "sink": "counts" })),
    )
    .unwrap();
    let repartition = "stats-per-second-repartition";
    assert!(
        compiled.built.to_wire_request()
            == WireTopology {
                epoch: 0,
                subtopologies: vec![
                    Subtopology {
                        subtopology_id: "0".to_string(),
                        source_topics: vec!["orders".to_string()],
                        repartition_sink_topics: vec![repartition.to_string()],
                        ..Default::default()
                    },
                    Subtopology {
                        subtopology_id: "1".to_string(),
                        repartition_source_topics: vec![TopicInfo {
                            name: repartition.to_string(),
                            partitions: 0,
                            replication_factor: -1,
                            topic_configs: config(&[
                                ("cleanup.policy", "delete"),
                                ("message.timestamp.type", "CreateTime"),
                                ("retention.ms", "-1"),
                                ("segment.bytes", "52428800"),
                            ]),
                            ..Default::default()
                        }],
                        state_changelog_topics: vec![TopicInfo {
                            name: "stats-per-second-changelog".to_string(),
                            partitions: 0,
                            replication_factor: -1,
                            topic_configs: config(&[
                                ("cleanup.policy", "compact,delete"),
                                ("message.timestamp.type", "CreateTime"),
                                ("retention.ms", "86401000"),
                            ]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
    );
    assert!(compiled.built.repartition_topics() == vec![repartition.to_string()]);
    assert!(
        compiled.built.changelog_topics()
            == [(
                "per-second".to_string(),
                "stats-per-second-changelog".to_string()
            )]
    );
}

/// Pipe `records` (key, value, timestamp) through a one-subtopology
/// topology and return the sink outputs as (topic, key, value).
fn run(
    ops: &Value,
    records: &[(Option<&str>, Value, i64)],
) -> Vec<(String, Option<String>, Value)> {
    let compiled = CompiledTopology::new(
        "app",
        &spec(&json!({ "source": "in", "ops": ops, "sink": "out" })),
    )
    .unwrap();
    let mut task = EmbeddedTask::new(&compiled.built).unwrap();
    for (offset, (key, value, timestamp)) in records.iter().enumerate() {
        let bytes = serde_json::to_vec(value).unwrap();
        task.pipe(
            "in",
            0,
            i64::try_from(offset).unwrap(),
            key.map(str::as_bytes),
            &bytes,
            *timestamp,
        )
        .unwrap();
    }
    outputs(&mut task)
}

fn outputs(task: &mut EmbeddedTask) -> Vec<(String, Option<String>, Value)> {
    task.take_output()
        .into_iter()
        .map(|r| {
            (
                r.topic,
                r.key.map(|k| String::from_utf8(k.to_vec()).unwrap()),
                r.value
                    .map_or(Value::Null, |v| serde_json::from_slice(&v).unwrap()),
            )
        })
        .collect()
}

fn out(key: Option<&str>, value: Value) -> (String, Option<String>, Value) {
    ("out".to_string(), key.map(str::to_string), value)
}

#[test]
fn filters_keep_what_compares_true() {
    let records = [
        (Some("a"), json!({ "total": 50, "tags": ["x"] }), 0),
        (Some("b"), json!({ "total": 150, "tags": ["vip"] }), 1),
        (Some("c"), json!({ "note": "no total" }), 2),
    ];
    let cases = [
        (
            json!({ "op": "filter", "field": "total", "gt": 100 }),
            vec!["b"],
        ),
        (
            json!({ "op": "filter", "field": "total", "lte": 50.0 }),
            vec!["a"],
        ),
        (
            json!({ "op": "filter", "field": "total", "eq": 150.0 }),
            vec!["b"],
        ),
        (
            json!({ "op": "filter", "field": "total", "ne": 50 }),
            vec!["b", "c"],
        ),
        (
            json!({ "op": "filter", "field": "tags", "contains": "vip" }),
            vec!["b"],
        ),
    ];
    for (op, kept) in cases {
        let keys: Vec<String> = run(&json!([op]), &records)
            .into_iter()
            .filter_map(|(_, k, _)| k)
            .collect();
        assert!(keys == kept, "{op}");
    }
}

#[test]
fn map_reshapes_the_value() {
    let ops = json!([{ "op": "map",
        "select": ["id", "name", "old"],
        "rename": { "old": "new" },
        "set": { "offset": "{seq}", "label": "n-{seq}" },
        "upper": ["name"] }]);
    let got = run(
        &ops,
        &[
            (
                Some("k"),
                json!({ "id": 1, "name": "ada", "old": true, "drop": 0 }),
                5,
            ),
            (Some("k"), json!("not an object"), 6),
        ],
    );
    assert!(
        got == vec![
            out(
                Some("k"),
                json!({ "id": 1, "name": "ADA", "new": true, "offset": 0, "label": "n-0" })
            ),
            out(Some("k"), json!("not an object")),
        ]
    );
}

#[test]
fn counts_and_sums_emit_the_running_total_per_key() {
    let records = [
        (Some("a"), json!({ "total": 2 }), 0),
        (Some("b"), json!({ "total": 1.5 }), 1),
        (Some("a"), json!({ "total": 3 }), 2),
        (None, json!({ "total": 9 }), 3),
        (Some("a"), json!({ "note": "no total" }), 4),
    ];
    assert!(
        run(&json!([{ "op": "count_by_key" }]), &records)
            == vec![
                out(Some("a"), json!({ "key": "a", "count": 1 })),
                out(Some("b"), json!({ "key": "b", "count": 1 })),
                out(Some("a"), json!({ "key": "a", "count": 2 })),
                out(Some("a"), json!({ "key": "a", "count": 3 })),
            ]
    );
    assert!(
        run(&json!([{ "op": "sum_by_key", "field": "total" }]), &records)
            == vec![
                out(Some("a"), json!({ "key": "a", "sum": 2 })),
                out(Some("b"), json!({ "key": "b", "sum": 1.5 })),
                out(Some("a"), json!({ "key": "a", "sum": 5 })),
            ]
    );
}

#[test]
fn window_counts_emit_every_window_and_drop_closed_ones() {
    let records = [
        (Some("a"), json!({}), 100),
        (Some("a"), json!({}), 900),
        (Some("a"), json!({}), 1_200),
        // Late: its window [0, 1000) closed when stream time reached 1200.
        (Some("a"), json!({}), 950),
    ];
    let window = |start: i64, end: i64, count: i64| {
        out(
            Some("a"),
            json!({ "key": "a", "window_start": start, "window_end": end, "count": count }),
        )
    };
    assert!(
        run(
            &json!([{ "op": "window_count", "size_ms": 1000 }]),
            &records
        ) == vec![
            window(0, 1_000, 1),
            window(0, 1_000, 2),
            window(1_000, 2_000, 1)
        ]
    );
    // Hopping windows of 1000 every 500: a record is in two windows. With a
    // grace of 200 the late record's window [0, 1000) is closed at stream
    // time 1200, and its window [500, 1500) is still open.
    assert!(
        run(
            &json!([{ "op": "window_count", "size_ms": 1000, "advance_ms": 500, "grace_ms": 200 }]),
            &records
        ) == vec![
            window(0, 1_000, 1),
            window(0, 1_000, 2),
            window(500, 1_500, 1),
            window(500, 1_500, 2),
            window(1_000, 2_000, 1),
            window(500, 1_500, 3),
        ]
    );
}

#[test]
fn a_repartitioned_count_runs_across_two_tasks() {
    let compiled = CompiledTopology::new(
        "app",
        &spec(&json!({ "source": "in", "ops": [
            { "op": "select_key", "field": "customer" },
            { "op": "count_by_key", "store": "counts" },
        ], "sink": "out" })),
    )
    .unwrap();
    let mut upstream = EmbeddedTask::for_subtopology(&compiled.built, "0").unwrap();
    let mut downstream = EmbeddedTask::for_subtopology(&compiled.built, "1").unwrap();
    let orders = [
        json!({ "customer": "c1" }),
        json!({ "customer": "c2" }),
        json!({ "customer": "c1" }),
        json!({ "no": "customer" }),
    ];
    for (offset, order) in orders.iter().enumerate() {
        let bytes = serde_json::to_vec(order).unwrap();
        upstream
            .pipe(
                "in",
                0,
                i64::try_from(offset).unwrap(),
                Some(b"k"),
                &bytes,
                0,
            )
            .unwrap();
    }
    let repartitioned = upstream.take_output();
    // The record without a customer has a null key, and the repartition
    // filter drops it.
    assert!(repartitioned.len() == 3);
    assert!(
        repartitioned
            .iter()
            .all(|r| r.topic == "app-counts-repartition")
    );
    for (offset, r) in repartitioned.iter().enumerate() {
        downstream
            .pipe(
                &r.topic,
                0,
                i64::try_from(offset).unwrap(),
                r.key.as_deref(),
                r.value.as_deref().unwrap_or_default(),
                r.timestamp,
            )
            .unwrap();
    }
    assert!(
        outputs(&mut downstream)
            == vec![
                out(Some("c1"), json!({ "key": "c1", "count": 1 })),
                out(Some("c2"), json!({ "key": "c2", "count": 1 })),
                out(Some("c1"), json!({ "key": "c1", "count": 2 })),
            ]
    );
    let changelog: Vec<(String, Bytes, Option<Bytes>)> = downstream
        .drain_changelogs()
        .into_iter()
        .map(|c| (c.topic, c.key, c.value))
        .collect();
    let long = |n: i64| Some(Bytes::copy_from_slice(&n.to_be_bytes()));
    assert!(
        changelog
            == vec![
                (
                    "app-counts-changelog".to_string(),
                    Bytes::from_static(b"c1"),
                    long(1)
                ),
                (
                    "app-counts-changelog".to_string(),
                    Bytes::from_static(b"c2"),
                    long(1)
                ),
                (
                    "app-counts-changelog".to_string(),
                    Bytes::from_static(b"c1"),
                    long(2)
                ),
            ]
    );
}

#[test]
fn window_starts_follow_kafkas_time_windows() {
    let cases = [
        ((100, 1_000, 1_000), vec![0]),
        ((1_000, 1_000, 1_000), vec![1_000]),
        ((1_200, 1_000, 500), vec![500, 1_000]),
        ((0, 1_000, 250), vec![0]),
        ((999, 1_000, 250), vec![0, 250, 500, 750]),
    ];
    for ((timestamp, size, advance), expected) in cases {
        assert!(window_starts(timestamp, size, advance) == expected);
    }
}

#[test]
fn numbers_add_as_integers_until_one_is_a_float() {
    let cases = [
        (json!(2), json!(3), json!(5)),
        (json!(2), json!(0.5), json!(2.5)),
        (
            json!(i64::MAX),
            json!(1),
            json!(9_223_372_036_854_775_808.0),
        ),
    ];
    for (a, b, sum) in cases {
        assert!(add_numbers(&a, &b) == sum);
    }
}

#[test]
fn the_json_serde_reads_an_empty_value_as_null() {
    let serde = JsonSerde;
    assert!(serde.deserialize("t", b"").unwrap() == Value::Null);
    assert!(serde.deserialize("t", br#"{"a":1}"#).unwrap() == json!({ "a": 1 }));
    assert!(serde.serialize("t", &json!({ "a": 1 })) == Bytes::from_static(br#"{"a":1}"#));
    assert!(serde.deserialize("t", b"{").is_err());
}

#[test]
fn keys_take_the_text_of_the_field() {
    assert!(key_text(&json!("c1")) == "c1");
    assert!(key_text(&json!(7)) == "7");
    assert!(key_text(&json!({ "a": 1 })) == r#"{"a":1}"#);
}
