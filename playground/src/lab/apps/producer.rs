//! The producer node: a Kafka producer that writes templated records at a
//! rate, optionally serialized through a schema registry.
//!
//! # Config
//!
//! ```json
//! { "bootstrap": [1, 2, 3], "topic": "orders", "rate_per_sec": 5, "acks": -1,
//!   "key": { "pattern": "customer-{seq % 10}" },
//!   "value": { "format": "json", "template": { "id": "{seq}", "total": "{rand 1 500}" } },
//!   "serialization": { "registry": 4, "format": "avro", "schema": "{...}", "subject": "orders-value" },
//!   "linger_ms": 5, "batch_size": 16384, "enable_idempotence": true,
//!   "compression": "none", "headers": { "source": "lab-{seq}" },
//!   "transactional_id": "orders-tx", "transaction_records": 10, "abort_every": 3 }
//! ```
//!
//! - `bootstrap` (required): the broker node ids the client connects to first.
//! - `topic` (required): the topic every record goes to.
//! - `rate_per_sec`: records per logical second, fractional allowed; `0`
//!   sends only on the `send` command. Default: 5.
//! - `acks`: `-1`, `0` or `1`. Default: `-1`.
//! - `key`: `{"pattern": <template>}` for a text key, or `null` for null keys,
//!   which the producer spreads with the sticky partitioner. Default: `null`.
//! - `value`: `{"format": "json", "template": <JSON template>}` or
//!   `{"format": "text", "template": <text template>}`. Default:
//!   `{"format": "json", "template": {"id": "{seq}", "total": "{rand 1 500}"}}`.
//! - `serialization`: `null` to send the value bytes as they are, or a schema
//!   the producer registers under `subject` (default `<topic>-value`) on the
//!   registry node `registry` before its first record, and frames every value
//!   with, as Confluent's serializers do (see [`serde`](super::serde)).
//!   `format` is `"avro"` or `"json"`; `schema` is the schema text, or the
//!   schema as a JSON document. Default: `null`.
//! - `linger_ms`, `batch_size`, `enable_idempotence`, `compression`
//!   (`none`, `gzip`, `snappy`, `lz4` or `zstd`): the Kafka producer settings
//!   of the same names, with Kafka's defaults 5, 16384, `true` and `none`.
//! - `headers`: header name to a text template. Default: none.
//! - `transactional_id`: `null`, or Kafka's `transactional.id`: the records
//!   go out in transactions of `transaction_records` records each (default
//!   10), and every `abort_every`-th transaction aborts (default 0, never).
//!   Needs `acks` -1 and `enable_idempotence`, as Kafka's config check does.
//!
//! The templates are those of [`templates`](super::templates); `{seq}`
//! counts the records this run of the node generated, from 0.
//!
//! # Behaviour
//!
//! The rate is exact over time: after `t` ms at rate `r` the node has
//! generated `floor(t × r / 1000)` records, so a fractional rate neither
//! drifts nor bursts. A paused node generates nothing, and resuming starts
//! the count again. With `serialization`, nothing is generated until the
//! registry assigned the schema id, as a Confluent serializer blocks the
//! first `send` on its registration; a refused or failed registration (409
//! incompatible, 422, a refused connection) is retried with a backoff and
//! shown in the snapshot. A document the schema rejects is counted in
//! `serialization.failed` and not sent. Batching, partitioning, idempotence
//! and retries are the client's [`Producer`]; a record whose topic does not
//! exist yet waits for it up to `max.block.ms` (60 s), as Kafka's producer
//! waits on metadata. Each start of the node builds a new client whose
//! connection ids come from a lane of its own ([`conn_base`]), so an answer
//! still on the way to the previous run never reaches the new one.
//!
//! A transactional node numbers its transactions from 1 and marks each
//! record with the header `lab-txn` = `"<number>:commit"` or
//! `"<number>:abort"`, the outcome it plans, so a consumer can tell which
//! records it should never see under `read_committed`. Once a transaction
//! holds `transaction_records` records, the node waits until they are all
//! acknowledged (Kafka's `flush`, so an aborted transaction's records reach
//! the log), then commits or aborts it, and generates nothing while the
//! transaction ends; the rate's records fall due meanwhile and go out
//! after. A paused node, or one at rate 0, ends its open transaction with
//! the records it holds. A transaction that hit an error aborts; a fenced producer
//! (`PRODUCER_FENCED`, for example after the coordinator aborted a
//! transaction past `transaction.timeout.ms`) is closed and a new one takes
//! the transactional id, as an application restarts its producer.
//!
//! # Control commands
//!
//! - `{"cmd": "send", "count": n}` generates `n` records now, paused or not
//!   (after the registration when one is pending, and in turn for a
//!   transactional node whose open transaction is full). Answers
//!   `{"generated": n}`, `{"queued": n}`, or both.
//! - `{"cmd": "rate", "rate_per_sec": x}` changes the rate.
//! - `{"cmd": "pause"}` and `{"cmd": "resume"}`.
//!
//! # Snapshot
//!
//! The [`Producer::snapshot`] object (`acks`, `idempotent`, `producer_id`,
//! `sent`, `acked`, `failed`, `retried`, `bytes`, `rtt`, `partitions`,
//! `client`, ...) plus `topic`, `rate`, `paused`, `generated`,
//! `serialization: {"registry", "subject", "format", "state":
//! "registering"|"ready", "schema_id", "failed", "error", "client"}` (or
//! `null`), and `last_records: [{"seq", "partition", "offset", "key",
//! "value_preview"}]`, the last ten records generated, with the partition
//! and offset once acknowledged. The producer's `acked_upto` maps
//! `"<topic>-<partition>"` to the highest acknowledged offset, and
//! `transactions` is `{"transactional_id", "state", "open", "committed",
//! "aborted", "partitions", "last_error"}` or `null`.
//!
//! # Events
//!
//! `schema_registered`, `registry_error` (warn), `serialization_failed`
//! (warn) and `produce_failed` (warn, once per step with the count, the last
//! error code, and Kafka's exception text when the producer itself failed
//! the record, such as a topic that did not appear within
//! `max.block.ms`); with transactions `transaction_committed`,
//! `transaction_aborted` (`{"txn"}`, the node's number of it),
//! `transaction_error` (warn, `{"code", "fatal"}`) and `producer_restarted`
//! (warn, after a fatal error).

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use krabka_protocol::records::RecordHeader;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    registry_client::{RegistrationEvent, SchemaRegistration},
    serde::SchemaFormat,
    templates::{JsonTemplate, Scope, Template},
};
use crate::lab::{
    LabError,
    client::{
        Acks, CONN_ID_LANES, ClientOptions, Compression, KafkaClient, Producer, ProducerConfig,
        ProducerEvent, ProducerRecord, SeqNo, conn_base,
    },
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId},
    scenario::NodeSpec,
};

/// How many generated records the snapshot lists.
const LAST_RECORDS: usize = 10;

/// The header a transactional node marks each record with.
pub const TXN_HEADER: &str = "lab-txn";

/// A rate of records per second as an exact fraction `num / den`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Rate {
    num: u64,
    den: u64,
}

impl Rate {
    /// No records on a timer.
    pub const ZERO: Self = Self { num: 0, den: 1 };

    /// The rate a JSON number names, read exactly from its decimal text:
    /// `2.5` is 5/2 records per second.
    ///
    /// # Errors
    /// Returns a message for a negative number, a non-number, or one with more
    /// than nine decimal places.
    pub fn from_json(value: &Value) -> Result<Self, String> {
        let Value::Number(number) = value else {
            return Err(format!("the rate {value} is not a number"));
        };
        if let Some(n) = number.as_u64() {
            return Ok(Self { num: n, den: 1 });
        }
        let text = number.to_string();
        if text.starts_with('-') {
            return Err(format!("the rate {text} is negative"));
        }
        let (mantissa, exponent) = match text.split_once(['e', 'E']) {
            Some((m, e)) => (
                m,
                e.parse::<i32>()
                    .map_err(|_| format!("the rate {text} does not parse"))?,
            ),
            None => (text.as_str(), 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits: u64 = format!("{whole}{fraction}")
            .parse()
            .map_err(|_| format!("the rate {text} does not parse"))?;
        let scale = i32::try_from(fraction.len()).unwrap_or(i32::MAX) - exponent;
        let too_fine = || format!("the rate {text} has more than nine decimal places");
        if scale > 9 {
            return Err(too_fine());
        }
        let (num, den) = if scale >= 0 {
            (digits, 10_u64.pow(scale.unsigned_abs()))
        } else {
            let factor = 10_u64
                .checked_pow(scale.unsigned_abs())
                .ok_or_else(too_fine)?;
            (digits.checked_mul(factor).ok_or_else(too_fine)?, 1)
        };
        Ok(Self { num, den })
    }

    /// Whether the rate sends nothing on a timer.
    #[must_use]
    pub fn is_zero(self) -> bool {
        self.num == 0
    }
}

/// When the records of a [`Rate`] are due, counted from an epoch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RateMeter {
    rate: Rate,
    epoch: Millis,
    emitted: u64,
}

impl RateMeter {
    /// A meter that starts counting at `now`.
    #[must_use]
    pub fn new(rate: Rate, now: Millis) -> Self {
        Self {
            rate,
            epoch: now,
            emitted: 0,
        }
    }

    /// Start counting again at `now`, at `rate`.
    pub fn reset(&mut self, rate: Rate, now: Millis) {
        *self = Self::new(rate, now);
    }

    /// The records that fell due by `now` and were not taken yet; they count
    /// as taken.
    pub fn take_due(&mut self, now: Millis) -> u64 {
        let elapsed = u128::from(now.saturating_sub(self.epoch));
        let total = elapsed * u128::from(self.rate.num) / (u128::from(self.rate.den) * 1_000);
        let total = u64::try_from(total).unwrap_or(u64::MAX);
        let due = total.saturating_sub(self.emitted);
        self.emitted = self.emitted.max(total);
        due
    }

    /// When the next record falls due, or `None` at rate zero.
    #[must_use]
    pub fn next_due(&self) -> Option<Millis> {
        if self.rate.is_zero() {
            return None;
        }
        let k = u128::from(self.emitted) + 1;
        let scaled = k * u128::from(self.rate.den) * 1_000;
        let offset = scaled.div_ceil(u128::from(self.rate.num));
        Some(
            self.epoch
                .saturating_add(u64::try_from(offset).unwrap_or(u64::MAX)),
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyConfig {
    pattern: String,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ValueFormat {
    Json,
    Text,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValueConfig {
    format: ValueFormat,
    template: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SerializationConfig {
    registry: NodeId,
    format: SchemaFormat,
    schema: Value,
    #[serde(default)]
    subject: Option<String>,
}

fn default_rate() -> Value {
    json!(5)
}

fn default_acks() -> i16 {
    -1
}

fn default_linger_ms() -> Millis {
    5
}

fn default_batch_size() -> usize {
    16_384
}

fn default_true() -> bool {
    true
}

fn default_compression() -> String {
    "none".to_string()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    bootstrap: Vec<NodeId>,
    topic: String,
    #[serde(default = "default_rate")]
    rate_per_sec: Value,
    #[serde(default = "default_acks")]
    acks: i16,
    #[serde(default)]
    key: Option<KeyConfig>,
    #[serde(default)]
    value: Option<ValueConfig>,
    #[serde(default)]
    serialization: Option<SerializationConfig>,
    #[serde(default = "default_linger_ms")]
    linger_ms: Millis,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    #[serde(default = "default_true")]
    enable_idempotence: bool,
    #[serde(default = "default_compression")]
    compression: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    transactional_id: Option<String>,
    #[serde(default = "default_transaction_records")]
    transaction_records: u64,
    #[serde(default)]
    abort_every: u64,
}

fn default_transaction_records() -> u64 {
    10
}

/// The transactions of a transactional node: the number of the open or the
/// next one, and how many records it holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct TxnPlan {
    records: u64,
    abort_every: u64,
    number: u64,
    held: u64,
}

impl TxnPlan {
    /// Whether the open transaction aborts.
    fn aborts(&self) -> bool {
        self.abort_every > 0 && self.number.is_multiple_of(self.abort_every)
    }

    /// The `lab-txn` header of the open transaction's records.
    fn header(&self) -> String {
        let outcome = if self.aborts() { "abort" } else { "commit" };
        format!("{}:{outcome}", self.number)
    }

    /// The open transaction holds every record it takes.
    fn full(&self) -> bool {
        self.held >= self.records
    }

    /// Move on to the next transaction.
    fn next(&mut self) {
        self.number += 1;
        self.held = 0;
    }
}

/// The value a record carries before serialization.
enum ValueTemplate {
    Json(JsonTemplate),
    Text(Template),
}

/// The value template a config names, or the default one.
fn parse_value(value: Option<ValueConfig>) -> Result<ValueTemplate, String> {
    Ok(match value {
        None => ValueTemplate::Json(
            JsonTemplate::parse(&json!({ "id": "{seq}", "total": "{rand 1 500}" }))
                .map_err(|e| e.to_string())?,
        ),
        Some(ValueConfig {
            format: ValueFormat::Json,
            template,
        }) => ValueTemplate::Json(
            JsonTemplate::parse(&template).map_err(|e| format!("`value.template`: {e}"))?,
        ),
        Some(ValueConfig {
            format: ValueFormat::Text,
            template,
        }) => {
            let text = template
                .as_str()
                .ok_or_else(|| "`value.template` of a text value is a string".to_string())?;
            ValueTemplate::Text(
                Template::parse(text).map_err(|e| format!("`value.template`: {e}"))?,
            )
        }
    })
}

/// The registration a serialization config names, with its schema parsed.
fn parse_serialization(
    config: SerializationConfig,
    topic: &str,
) -> Result<SchemaRegistration, String> {
    let schema_text = match config.schema {
        Value::String(text) => text,
        doc => doc.to_string(),
    };
    SchemaRegistration::new(
        config.registry,
        config.subject.unwrap_or_else(|| format!("{topic}-value")),
        config.format,
        schema_text,
    )
    .map_err(|e| format!("`serialization.schema`: {e}"))
}

/// One generated record, for the snapshot.
struct LastRecord {
    seq: u64,
    handle: SeqNo,
    key: Option<String>,
    value: Value,
    partition: Option<i32>,
    offset: Option<i64>,
}

/// The producer node. See the module documentation.
pub struct ProducerNode {
    bootstrap: Vec<Endpoint>,
    topic: String,
    key: Option<Template>,
    value: ValueTemplate,
    headers: Vec<(String, Template)>,
    config: ProducerConfig,
    rate: Rate,
    rate_json: Value,
    producer: Producer,
    /// How many times the node started: the connection-id lane of its
    /// client.
    starts: u32,
    meter: RateMeter,
    paused: bool,
    next_seq: u64,
    /// Records the `send` command asked for that wait for the registration.
    queued: u64,
    serialization: Option<SchemaRegistration>,
    last_records: VecDeque<LastRecord>,
    /// The transactions, with a `transactional_id`.
    txn: Option<TxnPlan>,
}

impl ProducerNode {
    /// # Errors
    /// Returns a config error for an unknown or malformed key, a template
    /// that does not parse, an `acks` or `compression` Kafka does not know,
    /// or a schema that does not parse.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let bad = |reason: String| LabError::config(spec, reason);
        let config: Config =
            serde_json::from_value(spec.config.clone()).map_err(|e| bad(format!("config: {e}")))?;
        if config.bootstrap.is_empty() {
            return Err(bad("`bootstrap` needs at least one broker".to_string()));
        }
        if config.topic.is_empty() {
            return Err(bad("`topic` is empty".to_string()));
        }
        let rate = Rate::from_json(&config.rate_per_sec).map_err(bad)?;
        let acks = Acks::from_wire(config.acks)
            .ok_or_else(|| bad(format!("`acks` is {}; use -1, 0 or 1", config.acks)))?;
        let compression = Compression::parse(&config.compression).ok_or_else(|| {
            bad(format!(
                "`compression` is `{}`; use none, gzip, snappy, lz4 or zstd",
                config.compression
            ))
        })?;
        if let Some(id) = &config.transactional_id {
            if id.is_empty() {
                return Err(bad("`transactional_id` is empty".to_string()));
            }
            if acks != Acks::All || !config.enable_idempotence {
                return Err(bad(
                    "`transactional_id` needs `acks` -1 and `enable_idempotence` true".to_string(),
                ));
            }
            if config.transaction_records == 0 {
                return Err(bad("`transaction_records` is at least 1".to_string()));
            }
        }
        let key = config
            .key
            .map(|k| Template::parse(&k.pattern))
            .transpose()
            .map_err(|e| bad(format!("`key.pattern`: {e}")))?;
        let value = parse_value(config.value).map_err(bad)?;
        let headers = config
            .headers
            .into_iter()
            .map(|(name, template)| {
                Template::parse(&template)
                    .map(|t| (name.clone(), t))
                    .map_err(|e| bad(format!("`headers.{name}`: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let serialization = config
            .serialization
            .map(|s| parse_serialization(s, &config.topic))
            .transpose()
            .map_err(bad)?;
        let producer_config = ProducerConfig {
            acks,
            linger_ms: config.linger_ms,
            batch_size: config.batch_size,
            enable_idempotence: config.enable_idempotence,
            compression,
            transactional_id: config.transactional_id.clone(),
            ..ProducerConfig::default()
        };
        let txn = config.transactional_id.as_ref().map(|_| TxnPlan {
            records: config.transaction_records,
            abort_every: config.abort_every,
            number: 1,
            held: 0,
        });
        let bootstrap: Vec<Endpoint> = config
            .bootstrap
            .iter()
            .map(|n| Endpoint::kafka(*n))
            .collect();
        let producer = Self::build_producer(&bootstrap, spec.id, &producer_config, 0, 0);
        Ok(Self {
            bootstrap,
            topic: config.topic,
            key,
            value,
            headers,
            config: producer_config,
            rate,
            rate_json: config.rate_per_sec,
            producer,
            starts: 0,
            meter: RateMeter::new(rate, 0),
            paused: false,
            next_seq: 0,
            queued: 0,
            serialization,
            last_records: VecDeque::new(),
            txn,
        })
    }

    /// A producer whose client numbers its connections in `lane`.
    fn build_producer(
        bootstrap: &[Endpoint],
        id: NodeId,
        config: &ProducerConfig,
        seed: u64,
        lane: u32,
    ) -> Producer {
        let client = KafkaClient::new(
            bootstrap.to_vec(),
            &format!("producer-{id}"),
            ClientOptions {
                conn_base: conn_base(lane),
                ..ClientOptions::default()
            },
        );
        Producer::new(client, config.clone(), seed)
    }

    /// Whether records can be generated: no schema to register, or it is
    /// registered, and a transactional producer can take records into its
    /// open transaction, which is not full.
    fn ready(&self) -> bool {
        self.serialization
            .as_ref()
            .is_none_or(|s| s.schema_id().is_some())
            && self.producer.transaction_ready()
            && self.txn.is_none_or(|t| !t.full())
    }

    /// Generate one record and hand it to the producer. Returns whether it
    /// went out.
    fn generate(&mut self, ctx: &mut Ctx<'_>) -> bool {
        let seq = self.next_seq;
        self.next_seq += 1;
        let now = ctx.now();
        let mut rand = |n: u64| ctx.rand(n);
        let mut scope = Scope {
            seq,
            now,
            rand: &mut rand,
        };
        let key = self.key.as_ref().map(|t| t.render(&mut scope));
        let doc = match &self.value {
            ValueTemplate::Json(t) => t.render(&mut scope),
            ValueTemplate::Text(t) => Value::String(t.render(&mut scope)),
        };
        let mut headers: Vec<RecordHeader> = self
            .headers
            .iter()
            .map(|(name, t)| RecordHeader {
                key: name.clone(),
                value: Some(Bytes::from(t.render(&mut scope))),
            })
            .collect();
        if let Some(plan) = &mut self.txn {
            plan.held += 1;
            headers.push(RecordHeader {
                key: TXN_HEADER.to_string(),
                value: Some(Bytes::from(plan.header())),
            });
        }
        let bytes = match &mut self.serialization {
            Some(registration) => match registration.serialize(&doc) {
                Ok(Some(framed)) => framed,
                Ok(None) => return false,
                Err(e) => {
                    ctx.event(
                        "serialization_failed",
                        json!({ "seq": seq, "error": e.to_string(), "level": "warn" }),
                    );
                    return false;
                }
            },
            None => match &doc {
                Value::String(text) if matches!(self.value, ValueTemplate::Text(_)) => {
                    Bytes::from(text.clone())
                }
                doc => Bytes::from(serde_json::to_vec(doc).unwrap_or_default()),
            },
        };
        let handle = self.producer.send(
            now,
            ProducerRecord {
                topic: self.topic.clone(),
                partition: None,
                key: key.clone().map(Bytes::from),
                value: Some(bytes),
                headers,
                timestamp: None,
            },
        );
        if self.last_records.len() == LAST_RECORDS {
            self.last_records.pop_front();
        }
        self.last_records.push_back(LastRecord {
            seq,
            handle,
            key,
            value: doc,
            partition: None,
            offset: None,
        });
        true
    }

    /// Generate what the rate and the `send` command owe. What a full
    /// transaction cannot take waits in `queued` for the next one.
    fn generate_due(&mut self, ctx: &mut Ctx<'_>) {
        if !self.ready() {
            return;
        }
        let due = if self.paused {
            0
        } else {
            self.meter.take_due(ctx.now())
        };
        let mut total = due + std::mem::take(&mut self.queued);
        while total > 0 && self.ready() {
            self.generate(ctx);
            total -= 1;
        }
        self.queued = total;
    }

    /// End a full transaction once its records are all acknowledged, abort
    /// one that hit an error, and replace a fenced producer.
    fn step_transaction(&mut self, ctx: &mut Ctx<'_>) {
        let Some(plan) = &mut self.txn else {
            return;
        };
        if self.producer.transaction_fatal() {
            ctx.event("producer_restarted", json!({ "level": "warn" }));
            self.producer.close(ctx);
            self.producer = Self::build_producer(
                &self.bootstrap,
                ctx.me(),
                &self.config,
                ctx.rand(u64::MAX),
                self.starts % CONN_ID_LANES,
            );
            self.starts = self.starts.wrapping_add(1);
            plan.next();
            return;
        }
        let failed = self.producer.transaction_state() == Some("abortable_error");
        // A paused node, or one that sends only on command, ends the open
        // transaction with what it holds, as an application commits before
        // it idles, so the last stable offset moves on.
        let idle = (self.paused || self.rate.is_zero()) && plan.held > 0 && self.queued == 0;
        let flushed = (plan.full() || idle) && self.producer.pending_records() == 0;
        if !failed && !flushed {
            return;
        }
        let ended = if failed || plan.aborts() {
            self.producer.abort_transaction()
        } else {
            self.producer.commit_transaction()
        };
        if ended.is_ok() {
            plan.next();
        }
    }

    fn on_producer_events(&mut self, ctx: &mut Ctx<'_>, events: Vec<ProducerEvent>) {
        let mut failed = 0_u64;
        let mut last_code = 0;
        let mut last_message = None;
        for event in events {
            match event {
                ProducerEvent::Acked {
                    seq,
                    partition,
                    offset,
                    ..
                } => {
                    if let Some(r) = self.last_records.iter_mut().find(|r| r.handle == seq) {
                        r.partition = Some(partition);
                        r.offset = Some(offset);
                    }
                }
                ProducerEvent::Failed { code, message, .. } => {
                    failed += 1;
                    last_code = code;
                    last_message = message;
                }
                ProducerEvent::TransactionEnded { committed } => {
                    // The plan moved on when the end started.
                    let number = self.txn.map_or(0, |t| t.number.saturating_sub(1));
                    let kind = if committed {
                        "transaction_committed"
                    } else {
                        "transaction_aborted"
                    };
                    ctx.event(kind, json!({ "txn": number }));
                }
                ProducerEvent::TransactionError { code, fatal } => ctx.event(
                    "transaction_error",
                    json!({ "code": code, "fatal": fatal, "level": "warn" }),
                ),
            }
        }
        if failed > 0 {
            ctx.event(
                "produce_failed",
                json!({
                    "records": failed,
                    "code": last_code,
                    "message": last_message,
                    "level": "warn",
                }),
            );
        }
    }

    fn on_registration(&mut self, ctx: &mut Ctx<'_>, events: Vec<RegistrationEvent>) {
        for event in events {
            match event {
                RegistrationEvent::Registered { subject, id } => {
                    ctx.event("schema_registered", json!({ "subject": subject, "id": id }));
                    // The records the rate owes start from the registration.
                    self.meter.reset(self.rate, ctx.now());
                }
                RegistrationEvent::Failed { subject, error } => ctx.event(
                    "registry_error",
                    json!({ "subject": subject, "error": error, "level": "warn" }),
                ),
            }
        }
    }

    /// Move everything on after a frame, a timer or a command, and arm the
    /// next deadline.
    fn drive(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(registration) = &mut self.serialization {
            registration.poll(ctx);
        }
        self.generate_due(ctx);
        let (events, _) = self.producer.on_tick(ctx);
        self.on_producer_events(ctx, events);
        if self.txn.is_some() {
            self.step_transaction(ctx);
            let (events, _) = self.producer.on_tick(ctx);
            self.on_producer_events(ctx, events);
        }
        let now = ctx.now();
        let rate = (self.ready() && !self.paused)
            .then(|| self.meter.next_due())
            .flatten();
        let queued = (self.ready() && self.queued > 0).then_some(now);
        let registry = self
            .serialization
            .as_ref()
            .and_then(SchemaRegistration::next_deadline);
        let deadline = self
            .producer
            .next_deadline(now)
            .into_iter()
            .chain(rate)
            .chain(queued)
            .chain(registry)
            .min();
        if let Some(at) = deadline {
            ctx.arm(at.max(now));
        }
    }
}

impl Node for ProducerNode {
    fn kind(&self) -> &'static str {
        "producer"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        // A process starts over: a new client with connections from a lane
        // of its own, no records generated, and the registration asked for
        // again.
        let seed = ctx.rand(u64::MAX);
        let lane = self.starts % CONN_ID_LANES;
        self.starts = self.starts.wrapping_add(1);
        self.producer = Self::build_producer(&self.bootstrap, ctx.me(), &self.config, seed, lane);
        self.meter.reset(self.rate, ctx.now());
        self.next_seq = 0;
        self.queued = 0;
        self.last_records.clear();
        if let Some(plan) = &mut self.txn {
            plan.number = 1;
            plan.held = 0;
        }
        if let Some(registration) = &mut self.serialization {
            registration.restart(ctx.now());
        }
        self.drive(ctx);
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        let registry_frame = self.serialization.as_ref().is_some_and(|s| s.owns(&frame));
        if registry_frame {
            if let Some(registration) = &mut self.serialization {
                let events = registration.on_frame(ctx, frame);
                self.on_registration(ctx, events);
            }
        } else {
            let (events, _) = self.producer.on_frame(ctx, frame);
            self.on_producer_events(ctx, events);
        }
        self.drive(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(registration) = &mut self.serialization {
            let events = registration.on_tick(ctx);
            self.on_registration(ctx, events);
        }
        self.drive(ctx);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let answer = match command.get("cmd").and_then(Value::as_str) {
            Some("send") => {
                let count = command
                    .get("count")
                    .map_or(Some(1), Value::as_u64)
                    .ok_or_else(|| "`count` is a whole number".to_string())?;
                // A full transaction leaves the rest for the next one.
                let mut generated = 0_u64;
                let mut left = count;
                while left > 0 && self.ready() {
                    if self.generate(ctx) {
                        generated += 1;
                    }
                    left -= 1;
                }
                self.queued += left;
                match (generated, left) {
                    (_, 0) => json!({ "generated": generated }),
                    (0, _) => json!({ "queued": left }),
                    _ => json!({ "generated": generated, "queued": left }),
                }
            }
            Some("rate") => {
                let rate = command
                    .get("rate_per_sec")
                    .ok_or_else(|| "missing `rate_per_sec`".to_string())
                    .and_then(Rate::from_json)?;
                self.rate = rate;
                self.rate_json = command["rate_per_sec"].clone();
                self.meter.reset(rate, ctx.now());
                json!({ "rate": self.rate_json })
            }
            Some("pause") => {
                self.paused = true;
                json!({ "paused": true })
            }
            Some("resume") => {
                if self.paused {
                    self.paused = false;
                    self.meter.reset(self.rate, ctx.now());
                }
                json!({ "paused": false })
            }
            other => return Err(format!("unknown producer command {other:?}")),
        };
        self.drive(ctx);
        Ok(answer)
    }

    fn snapshot(&self) -> Value {
        let mut snapshot = self.producer.snapshot();
        let last: Vec<Value> = self
            .last_records
            .iter()
            .map(|r| {
                json!({
                    "seq": r.seq,
                    "partition": r.partition,
                    "offset": r.offset,
                    "key": r.key,
                    "value_preview": r.value,
                })
            })
            .collect();
        if let Value::Object(map) = &mut snapshot {
            map.insert("topic".to_string(), json!(self.topic));
            map.insert("rate".to_string(), self.rate_json.clone());
            map.insert("paused".to_string(), json!(self.paused));
            map.insert("generated".to_string(), json!(self.next_seq));
            map.insert(
                "serialization".to_string(),
                self.serialization
                    .as_ref()
                    .map_or(Value::Null, SchemaRegistration::snapshot),
            );
            map.insert("last_records".to_string(), Value::Array(last));
        }
        snapshot
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::lab::{
        net::Payload,
        registry::http::{HttpRequest, HttpResponse},
        testing::CtxBuffers,
    };

    #[test]
    fn rates_read_exactly_from_json_numbers() {
        let cases = [
            (json!(5), Ok(Rate { num: 5, den: 1 })),
            (json!(0), Ok(Rate::ZERO)),
            (json!(0.5), Ok(Rate { num: 5, den: 10 })),
            (json!(2.25), Ok(Rate { num: 225, den: 100 })),
            (json!(1e-3), Ok(Rate { num: 1, den: 1_000 })),
            (json!(-1), Err("the rate -1 is negative".to_string())),
            (
                json!("fast"),
                Err("the rate \"fast\" is not a number".to_string()),
            ),
            (
                json!(1e-12),
                Err("the rate 1e-12 has more than nine decimal places".to_string()),
            ),
        ];
        for (value, expected) in cases {
            assert!(Rate::from_json(&value) == expected, "{value}");
        }
    }

    /// The times at which records fall due over `until` ms, ticking at every
    /// `next_due`.
    fn schedule(rate: &Value, until: Millis) -> Vec<(Millis, u64)> {
        let mut meter = RateMeter::new(Rate::from_json(rate).unwrap(), 0);
        let mut out = Vec::new();
        while let Some(at) = meter.next_due() {
            if at > until {
                break;
            }
            out.push((at, meter.take_due(at)));
        }
        out
    }

    #[test]
    fn the_meter_spreads_records_without_drift() {
        // (rate, until, the ticks and the records each takes)
        type Case = (Value, Millis, Vec<(Millis, u64)>);
        let cases: Vec<Case> = vec![
            (json!(3), 1_000, vec![(334, 1), (667, 1), (1_000, 1)]),
            (json!(0.5), 4_000, vec![(2_000, 1), (4_000, 1)]),
            (json!(1.5), 2_000, vec![(667, 1), (1_334, 1), (2_000, 1)]),
            (json!(0), 10_000, vec![]),
        ];
        for (rate, until, expected) in cases {
            assert!(schedule(&rate, until) == expected, "{rate}");
        }
    }

    #[test]
    fn a_late_tick_takes_every_record_due_at_once() {
        let mut meter = RateMeter::new(Rate::from_json(&json!(2_500)).unwrap(), 100);
        assert!(meter.take_due(100) == 0);
        assert!(meter.next_due() == Some(101));
        assert!(meter.take_due(101) == 2);
        assert!(meter.take_due(102) == 3);
        assert!(meter.take_due(1_100) == 2_495);
        assert!(meter.take_due(1_100) == 0);
        meter.reset(Rate::from_json(&json!(1)).unwrap(), 5_000);
        assert!(meter.next_due() == Some(6_000));
    }

    fn node(config: Value) -> Result<ProducerNode, LabError> {
        ProducerNode::from_spec(&NodeSpec::new(9, "producer", "p", config))
    }

    #[test]
    fn the_config_is_checked_at_load() {
        let cases = [
            (json!({ "topic": "t" }), "config: missing field `bootstrap`"),
            (
                json!({ "bootstrap": [1], "topic": "t", "bogus": 1 }),
                "config: unknown field `bogus`",
            ),
            (
                json!({ "bootstrap": [1], "topic": "t", "acks": 2 }),
                "`acks` is 2; use -1, 0 or 1",
            ),
            (
                json!({ "bootstrap": [1], "topic": "t", "compression": "brotli" }),
                "`compression` is `brotli`; use none, gzip, snappy, lz4 or zstd",
            ),
            (
                json!({ "bootstrap": [1], "topic": "t", "transactional_id": "x", "acks": 1 }),
                "`transactional_id` needs `acks` -1 and `enable_idempotence` true",
            ),
            (
                json!({ "bootstrap": [1], "topic": "t", "key": { "pattern": "{nope}" } }),
                "`key.pattern`: unknown placeholder `{nope}`",
            ),
            (
                json!({ "bootstrap": [1], "topic": "t", "rate_per_sec": -2 }),
                "the rate -2 is negative",
            ),
            (
                json!({ "bootstrap": [1], "topic": "t",
                        "value": { "format": "text", "template": { "a": 1 } } }),
                "`value.template` of a text value is a string",
            ),
            (
                json!({ "bootstrap": [1], "topic": "t",
                        "serialization": { "registry": 4, "format": "avro", "schema": "{" } }),
                "`serialization.schema`: the schema does not parse",
            ),
            (
                json!({ "bootstrap": [], "topic": "t" }),
                "`bootstrap` needs at least one broker",
            ),
        ];
        for (config, message) in cases {
            let err = node(config.clone()).err().unwrap().to_string();
            assert!(err.contains(message), "{config}: {err}");
        }
        // The page's probe config takes the defaults.
        assert!(node(json!({ "bootstrap": [1], "topic": "t" })).is_ok());
    }

    #[test]
    fn transactions_take_their_records_and_every_nth_aborts() {
        let mut plan = TxnPlan {
            records: 2,
            abort_every: 3,
            number: 1,
            held: 0,
        };
        let mut headers = Vec::new();
        while plan.number <= 3 {
            plan.held += 1;
            headers.push(plan.header());
            if plan.full() {
                plan.next();
            }
        }
        assert!(
            headers
                == [
                    "1:commit", "1:commit", "2:commit", "2:commit", "3:abort", "3:abort"
                ]
        );
    }

    #[test]
    fn the_node_registers_its_schema_before_it_generates() {
        let mut node = node(json!({
            "bootstrap": [1],
            "topic": "orders",
            "rate_per_sec": 1000,
            "serialization": { "registry": 4, "format": "avro", "schema": "\"string\"" },
            "value": { "format": "text", "template": "order {seq}" },
        }))
        .unwrap();
        let mut bufs = CtxBuffers::new(NodeId(9));
        bufs.with(0, |ctx| node.start(ctx));
        let frames = bufs.take_frames();
        let to_registry: Vec<&Frame> = frames
            .iter()
            .filter(|f| f.dst == Endpoint::http(NodeId(4)))
            .collect();
        assert!(to_registry.len() == 2);
        let (request, _) = HttpRequest::parse(to_registry[1].payload.data().unwrap()).unwrap();
        assert!(request.path == "/subjects/orders-value/versions");
        assert!(request.body_json() == Some(json!({ "schema": "\"string\"" })));
        // Nothing is generated while the registration is pending.
        bufs.with(50, |ctx| node.on_timer(ctx));
        assert!(node.snapshot()["generated"] == 0);
        assert!(node.snapshot()["serialization"]["state"] == "registering");
        let answer = to_registry[1].reply(Payload::Data(
            HttpResponse::ok(&json!({ "id": 3 })).encode(),
        ));
        bufs.with(60, |ctx| node.on_frame(ctx, answer));
        assert!(node.snapshot()["serialization"]["schema_id"] == 3);
        bufs.with(62, |ctx| node.on_timer(ctx));
        assert!(node.snapshot()["generated"] == 2);
        let last = &node.snapshot()["last_records"];
        assert!(last[0]["value_preview"] == "order 0");
        assert!(
            bufs.events
                .iter()
                .any(|(kind, detail)| *kind == "schema_registered"
                    && *detail == json!({ "subject": "orders-value", "id": 3 }))
        );
    }
}
