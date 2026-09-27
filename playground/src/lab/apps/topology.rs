//! The streams node's topology spec, and its compilation into a real
//! `krabka_client_streams` [`Topology`].
//!
//! A spec is a single-input chain from a source topic through ops into a sink
//! topic:
//!
//! ```json
//! { "source": "orders",
//!   "ops": [ { "op": "filter", "field": "total", "gt": 100 },
//!            { "op": "select_key", "field": "customer" },
//!            { "op": "count_by_key" } ],
//!   "sink": "order-counts" }
//! ```
//!
//! Records are string keys and JSON document values. The ops:
//!
//! - `filter {field, <cmp>: value}` keeps the records whose `field` compares
//!   true, with `<cmp>` one of `gt`, `gte`, `lt`, `lte` (numbers, or strings
//!   by their text), `eq`, `ne` (numbers by value, anything else by JSON
//!   equality; a missing field is `ne` everything) and `contains` (a
//!   substring of a string, or an element of an array).
//! - `map` reshapes the value with any of `select: [fields]` (keep only
//!   these), `rename: {from: to}`, `set: {field: template}` and `upper:
//!   field or [fields]`, applied in that order. A `set` template is a
//!   [`templates`](super::templates) template where `{seq}` is the source
//!   record's offset, `{now}` its timestamp, and `{rand}`, `{pick}` draw from
//!   a generator seeded by the partition and offset, so reprocessing a record
//!   gives the same value.
//! - `select_key {field}` re-keys the record by the field's text, or by no
//!   key when the field is missing.
//! - `count_by_key {store?}` counts per key in a key-value store and emits
//!   `{key, count}`; `sum_by_key {field, store?}` sums a numeric field and
//!   emits `{key, sum}`.
//! - `window_count {size_ms, advance_ms?, grace_ms?, store?}` counts per key
//!   in tumbling (`advance_ms` omitted) or hopping windows in a window store,
//!   and emits `{key, window_start, window_end, count}` for every window the
//!   record falls in. A record for a window that closed (its end at or before
//!   the stream time minus `grace_ms`, default 0) is dropped, as Kafka's
//!   windowed aggregation does.
//!
//! The aggregations skip records with a null key or a null value, as Kafka's
//! do. Every update is emitted: the embedded tasks run without a record
//! cache, like Kafka Streams with `statestore.cache.max.bytes=0`.
//!
//! # Names
//!
//! The source node is `source` and the sink node `sink`; op `i` is
//! `op-<i>-<op>`. A stateful op's store is named by its `store` key, or
//! `<op>-<i>` with dashes (`count-by-key-1`). As in the Kafka Streams DSL, a
//! key change (`select_key`) makes the next aggregation read through a
//! repartition topic named after its store, `<application.id>-<store>-repartition`,
//! behind a filter that drops null keys (`<store>-repartition-filter`), a
//! sink (`<store>-repartition-sink`) and a source (`<store>-repartition-source`);
//! a `select_key` that no aggregation follows writes to the sink directly. A
//! store logs to `<application.id>-<store>-changelog`.

use std::collections::BTreeSet;

use bytes::Bytes;
use krabka_client_streams::{
    BuiltTopology, Consumed, I64Serde, NodeHandle, Processor, ProcessorContext, Produced, Record,
    Serde, SerdeError, StringSerde, Topology, impl_processor,
};
use krabka_units::prelude::{Time, TimeExt};
use serde_json::{Map, Value, json};

use super::templates::{Scope, Template};
use crate::lab::net::Rng;

/// A comparison of a `filter` op.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cmp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Ne,
    Contains,
}

impl Cmp {
    const ALL: [(&'static str, Self); 7] = [
        ("gt", Self::Gt),
        ("gte", Self::Gte),
        ("lt", Self::Lt),
        ("lte", Self::Lte),
        ("eq", Self::Eq),
        ("ne", Self::Ne),
        ("contains", Self::Contains),
    ];

    /// Whether `field` (the record's, or `None` when missing) compares true
    /// against `operand`.
    #[must_use]
    pub fn matches(self, field: Option<&Value>, operand: &Value) -> bool {
        let Some(field) = field else {
            return self == Self::Ne;
        };
        match self {
            Self::Eq => json_equal(field, operand),
            Self::Ne => !json_equal(field, operand),
            Self::Gt => order(field, operand).is_some_and(std::cmp::Ordering::is_gt),
            Self::Gte => order(field, operand).is_some_and(std::cmp::Ordering::is_ge),
            Self::Lt => order(field, operand).is_some_and(std::cmp::Ordering::is_lt),
            Self::Lte => order(field, operand).is_some_and(std::cmp::Ordering::is_le),
            Self::Contains => match (field, operand) {
                (Value::String(s), Value::String(sub)) => s.contains(sub.as_str()),
                (Value::Array(items), x) => items.iter().any(|i| json_equal(i, x)),
                _ => false,
            },
        }
    }
}

/// JSON equality, with numbers compared by value (`5` equals `5.0`).
fn json_equal(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x.partial_cmp(&y) == Some(std::cmp::Ordering::Equal),
        _ => a == b,
    }
}

/// The order of two numbers, or of two strings by their text.
fn order(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// The reshaping of a `map` op.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct MapOp {
    pub select: Option<Vec<String>>,
    pub rename: Vec<(String, String)>,
    pub set: Vec<(String, Template)>,
    pub upper: Vec<String>,
}

impl MapOp {
    /// The new value of a record: `select`, then `rename`, `set` and
    /// `upper`. A value that is not an object passes unchanged.
    #[must_use]
    pub fn apply(&self, value: Value, seq: u64, now: u64, seed: u64) -> Value {
        let Value::Object(mut fields) = value else {
            return value;
        };
        if let Some(keep) = &self.select {
            fields.retain(|k, _| keep.contains(k));
        }
        for (from, to) in &self.rename {
            if let Some(v) = fields.remove(from) {
                fields.insert(to.clone(), v);
            }
        }
        let mut rng = Rng::new(seed);
        let mut rand = |n: u64| rng.below(n);
        for (field, template) in &self.set {
            let mut scope = Scope {
                seq,
                now,
                rand: &mut rand,
            };
            fields.insert(field.clone(), template.render_json(&mut scope));
        }
        for field in &self.upper {
            if let Some(Value::String(s)) = fields.get_mut(field) {
                *s = s.to_uppercase();
            }
        }
        Value::Object(fields)
    }
}

/// One op of the chain.
#[derive(Clone, PartialEq, Debug)]
pub enum Op {
    Filter {
        field: String,
        cmp: Cmp,
        value: Value,
    },
    Map(MapOp),
    SelectKey {
        field: String,
    },
    CountByKey {
        store: Option<String>,
    },
    SumByKey {
        field: String,
        store: Option<String>,
    },
    WindowCount {
        size_ms: u64,
        advance_ms: u64,
        grace_ms: u64,
        store: Option<String>,
    },
}

impl Op {
    /// The op's name in the spec.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Filter { .. } => "filter",
            Self::Map(_) => "map",
            Self::SelectKey { .. } => "select_key",
            Self::CountByKey { .. } => "count_by_key",
            Self::SumByKey { .. } => "sum_by_key",
            Self::WindowCount { .. } => "window_count",
        }
    }

    /// The store the op keeps, for a stateful op.
    fn store_kind(&self) -> Option<(StoreKind, Option<&String>)> {
        match self {
            Self::CountByKey { store } => Some((StoreKind::Count, store.as_ref())),
            Self::SumByKey { store, .. } => Some((StoreKind::Sum, store.as_ref())),
            Self::WindowCount {
                size_ms,
                advance_ms,
                grace_ms,
                store,
            } => Some((
                StoreKind::Window {
                    size_ms: *size_ms,
                    advance_ms: *advance_ms,
                    grace_ms: *grace_ms,
                },
                store.as_ref(),
            )),
            Self::Filter { .. } | Self::Map(_) | Self::SelectKey { .. } => None,
        }
    }

    /// Parse one op of a spec.
    ///
    /// # Errors
    /// Returns a message naming the op and the problem: an unknown op or
    /// key, a missing or malformed parameter.
    pub fn parse(doc: &Value) -> Result<Self, String> {
        let fields = doc
            .as_object()
            .ok_or_else(|| format!("an op is an object, not {doc}"))?;
        let name = fields
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("an op needs `op`: {doc}"))?;
        let bad = |reason: String| format!("{name}: {reason}");
        let allowed: &[&str] = match name {
            "filter" => &[
                "op", "field", "gt", "gte", "lt", "lte", "eq", "ne", "contains",
            ],
            "map" => &["op", "select", "rename", "set", "upper"],
            "select_key" => &["op", "field"],
            "count_by_key" => &["op", "store"],
            "sum_by_key" => &["op", "field", "store"],
            "window_count" => &["op", "size_ms", "advance_ms", "grace_ms", "store"],
            other => return Err(format!("unknown op `{other}`")),
        };
        if let Some(key) = fields.keys().find(|k| !allowed.contains(&k.as_str())) {
            return Err(bad(format!("unknown key `{key}`")));
        }
        let text = |key: &str| {
            fields
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| bad(format!("`{key}` is a required string")))
        };
        let store = || match fields.get("store") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if !s.is_empty() => Ok(Some(s.clone())),
            Some(other) => Err(bad(format!("`store` is a name, not {other}"))),
        };
        let millis = |key: &str| match fields.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v
                .as_u64()
                .map(Some)
                .ok_or_else(|| bad(format!("`{key}` is a whole number of milliseconds"))),
        };
        Ok(match name {
            "filter" => {
                let field = text("field")?;
                let mut comparisons = Cmp::ALL
                    .iter()
                    .filter_map(|(key, cmp)| fields.get(*key).map(|v| (*cmp, v.clone())));
                let (cmp, value) = comparisons.next().ok_or_else(|| {
                    bad("needs one comparison: gt, gte, lt, lte, eq, ne or contains".to_string())
                })?;
                if comparisons.next().is_some() {
                    return Err(bad("takes one comparison".to_string()));
                }
                Self::Filter { field, cmp, value }
            }
            "map" => Self::Map(parse_map(fields).map_err(bad)?),
            "select_key" => Self::SelectKey {
                field: text("field")?,
            },
            "count_by_key" => Self::CountByKey { store: store()? },
            "sum_by_key" => Self::SumByKey {
                field: text("field")?,
                store: store()?,
            },
            _ => {
                let size_ms = millis("size_ms")?.filter(|s| *s > 0).ok_or_else(|| {
                    bad("`size_ms` is a positive number of milliseconds".to_string())
                })?;
                let advance_ms = millis("advance_ms")?.unwrap_or(size_ms);
                if advance_ms == 0 || advance_ms > size_ms {
                    return Err(bad(
                        "`advance_ms` is positive and at most `size_ms`".to_string()
                    ));
                }
                Self::WindowCount {
                    size_ms,
                    advance_ms,
                    grace_ms: millis("grace_ms")?.unwrap_or(0),
                    store: store()?,
                }
            }
        })
    }
}

fn parse_map(fields: &Map<String, Value>) -> Result<MapOp, String> {
    let strings = |key: &str, v: &Value| -> Result<Vec<String>, String> {
        match v {
            Value::String(s) => Ok(vec![s.clone()]),
            Value::Array(items) => items
                .iter()
                .map(|i| {
                    i.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| format!("`{key}` lists field names"))
                })
                .collect(),
            _ => Err(format!("`{key}` is a field name or a list of them")),
        }
    };
    let pairs = |key: &str| -> Result<Vec<(String, String)>, String> {
        match fields.get(key) {
            None => Ok(Vec::new()),
            Some(Value::Object(map)) => map
                .iter()
                .map(|(k, v)| {
                    v.as_str()
                        .map(|s| (k.clone(), s.to_string()))
                        .ok_or_else(|| format!("`{key}.{k}` is a string"))
                })
                .collect(),
            Some(_) => Err(format!("`{key}` is an object")),
        }
    };
    let op = MapOp {
        select: fields
            .get("select")
            .map(|v| strings("select", v))
            .transpose()?,
        rename: pairs("rename")?,
        set: pairs("set")?
            .into_iter()
            .map(|(field, text)| {
                Template::parse(&text)
                    .map(|t| (field.clone(), t))
                    .map_err(|e| format!("`set.{field}`: {e}"))
            })
            .collect::<Result<_, _>>()?,
        upper: fields
            .get("upper")
            .map(|v| strings("upper", v))
            .transpose()?
            .unwrap_or_default(),
    };
    if op == MapOp::default() {
        return Err("needs at least one of select, rename, set or upper".to_string());
    }
    Ok(op)
}

/// A parsed topology spec.
#[derive(Clone, PartialEq, Debug)]
pub struct TopologySpec {
    pub source: String,
    pub ops: Vec<Op>,
    pub sink: String,
}

impl TopologySpec {
    /// Parse the `topology` config of a streams node.
    ///
    /// # Errors
    /// Returns a message for a missing or empty topic, an unknown key, or an
    /// op that does not parse (with its index).
    pub fn parse(doc: &Value) -> Result<Self, String> {
        let fields = doc
            .as_object()
            .ok_or_else(|| "`topology` is an object".to_string())?;
        if let Some(key) = fields
            .keys()
            .find(|k| !["source", "ops", "sink"].contains(&k.as_str()))
        {
            return Err(format!("`topology`: unknown key `{key}`"));
        }
        let topic = |key: &str| {
            fields
                .get(key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| format!("`topology.{key}` is a topic name"))
        };
        let ops = match fields.get("ops") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(items)) => items
                .iter()
                .enumerate()
                .map(|(i, op)| Op::parse(op).map_err(|e| format!("`topology.ops[{i}]`: {e}")))
                .collect::<Result<_, _>>()?,
            Some(_) => return Err("`topology.ops` is a list".to_string()),
        };
        Ok(Self {
            source: topic("source")?,
            ops,
            sink: topic("sink")?,
        })
    }
}

/// The kind of a store the plan keeps.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreKind {
    /// Key to count, `i64` values.
    Count,
    /// Key to sum, JSON number values.
    Sum,
    /// A window store: key and window start to count.
    Window {
        size_ms: u64,
        advance_ms: u64,
        grace_ms: u64,
    },
}

/// One store of a plan.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PlanStore {
    pub name: String,
    /// The processor the store is connected to.
    pub processor: String,
    pub kind: StoreKind,
    pub changelog: String,
}

/// What a plan node does.
#[derive(Clone, PartialEq, Debug)]
pub enum PlanNodeKind {
    Source {
        topic: String,
    },
    Sink {
        topic: String,
    },
    /// The filter in front of a repartition sink, which drops null keys.
    DropNullKeys,
    Op(Op),
}

/// One node of a plan.
#[derive(Clone, PartialEq, Debug)]
pub struct PlanNode {
    pub name: String,
    /// The node that feeds it; `None` for a source.
    pub parent: Option<String>,
    pub kind: PlanNodeKind,
}

/// The processor graph a spec compiles to, before the crate builds it.
#[derive(Clone, PartialEq, Debug)]
pub struct Plan {
    pub application_id: String,
    /// The nodes in insertion order, which sets the subtopology ids.
    pub nodes: Vec<PlanNode>,
    pub stores: Vec<PlanStore>,
    pub repartition_topics: Vec<String>,
}

impl Plan {
    /// The plan of `spec` for `application_id`.
    ///
    /// # Errors
    /// Returns a message when two stateful ops name the same store.
    pub fn new(application_id: &str, spec: &TopologySpec) -> Result<Self, String> {
        let mut nodes = vec![PlanNode {
            name: "source".to_string(),
            parent: None,
            kind: PlanNodeKind::Source {
                topic: spec.source.clone(),
            },
        }];
        let mut stores: Vec<PlanStore> = Vec::new();
        let mut repartition_topics = Vec::new();
        let mut parent = "source".to_string();
        let mut rekeyed = false;
        let mut names = BTreeSet::new();
        for (i, op) in spec.ops.iter().enumerate() {
            let name = format!("op-{i}-{}", op.name().replace('_', "-"));
            if let Some((kind, store)) = op.store_kind() {
                let store = store
                    .cloned()
                    .unwrap_or_else(|| format!("{}-{i}", op.name().replace('_', "-")));
                if !names.insert(store.clone()) {
                    return Err(format!("the store `{store}` is named twice"));
                }
                if rekeyed {
                    let topic = format!("{application_id}-{store}-repartition");
                    let filter = format!("{store}-repartition-filter");
                    let sink = format!("{store}-repartition-sink");
                    let source = format!("{store}-repartition-source");
                    nodes.push(PlanNode {
                        name: filter.clone(),
                        parent: Some(parent),
                        kind: PlanNodeKind::DropNullKeys,
                    });
                    nodes.push(PlanNode {
                        name: sink,
                        parent: Some(filter),
                        kind: PlanNodeKind::Sink {
                            topic: topic.clone(),
                        },
                    });
                    nodes.push(PlanNode {
                        name: source.clone(),
                        parent: None,
                        kind: PlanNodeKind::Source {
                            topic: topic.clone(),
                        },
                    });
                    repartition_topics.push(topic);
                    parent = source;
                    rekeyed = false;
                }
                stores.push(PlanStore {
                    changelog: format!("{application_id}-{store}-changelog"),
                    name: store,
                    processor: name.clone(),
                    kind,
                });
            }
            if matches!(op, Op::SelectKey { .. }) {
                rekeyed = true;
            }
            nodes.push(PlanNode {
                name: name.clone(),
                parent: Some(parent),
                kind: PlanNodeKind::Op(op.clone()),
            });
            parent = name;
        }
        nodes.push(PlanNode {
            name: "sink".to_string(),
            parent: Some(parent),
            kind: PlanNodeKind::Sink {
                topic: spec.sink.clone(),
            },
        });
        Ok(Self {
            application_id: application_id.to_string(),
            nodes,
            stores,
            repartition_topics,
        })
    }

    /// The store a name names.
    #[must_use]
    pub fn store(&self, name: &str) -> Option<&PlanStore> {
        self.stores.iter().find(|s| s.name == name)
    }

    /// Build the plan with the streams crate.
    ///
    /// # Errors
    /// Returns the crate's refusal as text; a plan of [`Plan::new`] builds.
    pub fn build(&self) -> Result<BuiltTopology, String> {
        let mut topology = Topology::new();
        for topic in &self.repartition_topics {
            topology.add_repartition_topic(topic.clone());
        }
        let mut handles: Vec<(String, NodeHandle<String, Value>)> = Vec::new();
        let handle = |handles: &[(String, NodeHandle<String, Value>)], name: &str| {
            handles
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, h)| h.clone())
                .ok_or_else(|| format!("the plan names an unknown node `{name}`"))
        };
        for node in &self.nodes {
            let parent = node
                .parent
                .as_deref()
                .map(|p| handle(&handles, p))
                .transpose()?;
            let made = match (&node.kind, parent) {
                (PlanNodeKind::Source { topic }, None) => topology
                    .add_source_explicit::<String, Value, _, _>(
                        node.name.clone(),
                        [topic.clone()],
                        Consumed::with(StringSerde, JsonSerde),
                    ),
                (PlanNodeKind::Sink { topic }, Some(parent)) => {
                    topology.add_sink_explicit::<String, Value, _, _, _, _>(
                        node.name.clone(),
                        topic.clone(),
                        [&parent],
                        Produced::with(StringSerde, JsonSerde),
                    );
                    continue;
                }
                (PlanNodeKind::DropNullKeys, Some(parent)) => {
                    topology.add_processor(node.name.clone(), || DropNullKeys, [&parent])
                }
                (PlanNodeKind::Op(op), Some(parent)) => {
                    self.add_op(&mut topology, &node.name, op, &parent)
                }
                _ => return Err(format!("the plan node `{}` is malformed", node.name)),
            };
            handles.push((node.name.clone(), made));
        }
        topology
            .build(self.application_id.clone())
            .map_err(|e| e.to_string())
    }

    fn add_op(
        &self,
        topology: &mut Topology,
        name: &str,
        op: &Op,
        parent: &NodeHandle<String, Value>,
    ) -> NodeHandle<String, Value> {
        let store = self
            .stores
            .iter()
            .find(|s| s.processor == name)
            .map(|s| s.name.clone())
            .unwrap_or_default();
        match op.clone() {
            Op::Filter { field, cmp, value } => topology.add_processor(
                name,
                move || FilterProc {
                    field: field.clone(),
                    cmp,
                    value: value.clone(),
                },
                [parent],
            ),
            Op::Map(map) => {
                topology.add_processor(name, move || MapProc { op: map.clone() }, [parent])
            }
            Op::SelectKey { field } => topology.add_processor(
                name,
                move || SelectKeyProc {
                    field: field.clone(),
                },
                [parent],
            ),
            Op::CountByKey { .. } => {
                let handle = {
                    let store = store.clone();
                    topology.add_processor(
                        name,
                        move || CountProc {
                            store: store.clone(),
                        },
                        [parent],
                    )
                };
                topology.add_state_store(store, StringSerde, I64Serde, [name]);
                handle
            }
            Op::SumByKey { field, .. } => {
                let handle = {
                    let store = store.clone();
                    topology.add_processor(
                        name,
                        move || SumProc {
                            store: store.clone(),
                            field: field.clone(),
                        },
                        [parent],
                    )
                };
                topology.add_state_store(store, StringSerde, JsonSerde, [name]);
                handle
            }
            Op::WindowCount {
                size_ms,
                advance_ms,
                grace_ms,
                ..
            } => {
                let size = i64::try_from(size_ms).unwrap_or(i64::MAX);
                let advance = i64::try_from(advance_ms).unwrap_or(i64::MAX);
                let grace = i64::try_from(grace_ms).unwrap_or(i64::MAX);
                let handle = {
                    let store = store.clone();
                    topology.add_processor(
                        name,
                        move || WindowCountProc {
                            store: store.clone(),
                            size,
                            advance,
                            grace,
                            observed: i64::MIN,
                        },
                        [parent],
                    )
                };
                topology.add_window_store(
                    store,
                    StringSerde,
                    I64Serde,
                    (
                        Time::from_millis(size),
                        Time::from_millis(size),
                        Time::from_millis(grace),
                    ),
                    [name],
                );
                handle
            }
        }
    }
}

/// A plan and the topology the crate built from it.
pub struct CompiledTopology {
    pub plan: Plan,
    pub built: BuiltTopology,
}

impl CompiledTopology {
    /// Plan and build `spec` for `application_id`.
    ///
    /// # Errors
    /// Returns what [`Plan::new`] or [`Plan::build`] refuses.
    pub fn new(application_id: &str, spec: &TopologySpec) -> Result<Self, String> {
        let plan = Plan::new(application_id, spec)?;
        let built = plan.build()?;
        Ok(Self { plan, built })
    }
}

/// JSON document values: the compact JSON text of the document, with an
/// empty value read as `null`.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonSerde;

impl Serde<Value> for JsonSerde {
    fn serialize(&self, _topic: &str, value: &Value) -> Bytes {
        Bytes::from(serde_json::to_vec(value).unwrap_or_default())
    }

    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<Value, SerdeError> {
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(bytes).map_err(|e| SerdeError(e.to_string()))
    }
}

/// The text of a key a field names: a string as it is, anything else as
/// its JSON text.
#[must_use]
pub fn key_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The sum of two JSON numbers: an integer while both are integers and the
/// sum fits, else a float.
#[must_use]
pub fn add_numbers(a: &Value, b: &Value) -> Value {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64())
        && let Some(sum) = x.checked_add(y)
    {
        return Value::from(sum);
    }
    let sum = a.as_f64().unwrap_or(0.0) + b.as_f64().unwrap_or(0.0);
    serde_json::Number::from_f64(sum).map_or(Value::Null, Value::Number)
}

/// The window starts a record at `timestamp` falls in, for windows of
/// `size` that start every `advance` ms from 0: Kafka's
/// `TimeWindows.windowsFor`.
#[must_use]
pub fn window_starts(timestamp: i64, size: i64, advance: i64) -> Vec<i64> {
    let mut start =
        (timestamp.saturating_sub(size).saturating_add(advance)).max(0) / advance * advance;
    let mut out = Vec::new();
    while start <= timestamp {
        out.push(start);
        start = start.saturating_add(advance);
    }
    out
}

struct DropNullKeys;

impl_processor! {
    impl DropNullKeys: (String, Value) -> (String, Value) {
        async fn process(&mut self, ctx, r) {
            if r.key.is_some() {
                ctx.forward(r);
            }
        }
    }
}

struct FilterProc {
    field: String,
    cmp: Cmp,
    value: Value,
}

#[krabka_client_streams::__async_trait]
impl Processor<String, Value, String, Value> for FilterProc {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, Value>,
        r: Record<String, Value>,
    ) {
        if self.cmp.matches(r.value.get(&self.field), &self.value) {
            ctx.forward(r);
        }
    }
}

struct MapProc {
    op: MapOp,
}

#[krabka_client_streams::__async_trait]
impl Processor<String, Value, String, Value> for MapProc {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, Value>,
        r: Record<String, Value>,
    ) {
        let source = ctx.record_context();
        let offset = u64::try_from(source.offset).unwrap_or(0);
        let partition = u64::from(source.partition.unsigned_abs());
        let now = u64::try_from(r.timestamp).unwrap_or(0);
        let value = self
            .op
            .apply(r.value, offset, now, (partition << 48) ^ offset);
        ctx.forward(Record::new(r.key, value, r.timestamp));
    }
}

struct SelectKeyProc {
    field: String,
}

#[krabka_client_streams::__async_trait]
impl Processor<String, Value, String, Value> for SelectKeyProc {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, Value>,
        r: Record<String, Value>,
    ) {
        let key = r.value.get(&self.field).map(key_text);
        ctx.forward(Record::new(key, r.value, r.timestamp));
    }
}

struct CountProc {
    store: String,
}

#[krabka_client_streams::__async_trait]
impl Processor<String, Value, String, Value> for CountProc {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, Value>,
        r: Record<String, Value>,
    ) {
        let (Some(key), false) = (r.key, r.value.is_null()) else {
            return;
        };
        let count = {
            let Some(store) = ctx.get_state_store::<String, i64>(&self.store) else {
                return;
            };
            let count = store.get(&key).await.unwrap_or(0) + 1;
            store.put(key.clone(), count).await;
            count
        };
        let value = json!({ "key": key, "count": count });
        ctx.forward(Record::new(Some(key), value, r.timestamp));
    }
}

struct SumProc {
    store: String,
    field: String,
}

#[krabka_client_streams::__async_trait]
impl Processor<String, Value, String, Value> for SumProc {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, Value>,
        r: Record<String, Value>,
    ) {
        let Some(key) = r.key else {
            return;
        };
        let Some(amount) = r.value.get(&self.field).filter(|v| v.is_number()).cloned() else {
            return;
        };
        let sum = {
            let Some(store) = ctx.get_state_store::<String, Value>(&self.store) else {
                return;
            };
            let before = store.get(&key).await.unwrap_or_else(|| Value::from(0));
            let sum = add_numbers(&before, &amount);
            store.put(key.clone(), sum.clone()).await;
            sum
        };
        let value = json!({ "key": key, "sum": sum });
        ctx.forward(Record::new(Some(key), value, r.timestamp));
    }
}

struct WindowCountProc {
    store: String,
    size: i64,
    advance: i64,
    grace: i64,
    /// The highest timestamp the processor saw: its stream time.
    observed: i64,
}

#[krabka_client_streams::__async_trait]
impl Processor<String, Value, String, Value> for WindowCountProc {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, Value>,
        r: Record<String, Value>,
    ) {
        let (Some(key), false) = (r.key, r.value.is_null()) else {
            return;
        };
        let timestamp = r.timestamp;
        if timestamp < 0 {
            return;
        }
        self.observed = self.observed.max(timestamp);
        let close = self.observed.saturating_sub(self.grace);
        for start in window_starts(timestamp, self.size, self.advance) {
            let end = start.saturating_add(self.size);
            if end <= close {
                continue;
            }
            let count = {
                let Some(store) = ctx.get_window_store::<String, i64>(&self.store) else {
                    return;
                };
                let count = store.fetch_single(&key, start).await.map_or(0, |(_, c)| c) + 1;
                store.put(key.clone(), start, count, timestamp).await;
                count
            };
            let value = json!({
                "key": key,
                "window_start": start,
                "window_end": end,
                "count": count,
            });
            ctx.forward(Record::new(Some(key.clone()), value, timestamp));
        }
    }
}

#[cfg(test)]
mod tests;
