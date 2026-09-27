//! The consumer node: a consumer group member that processes the records it
//! polls one at a time, so a slow consumer shows its lag.
//!
//! # Config
//!
//! ```json
//! { "bootstrap": [1, 2, 3], "group": "billing", "topics": ["orders"],
//!   "protocol": "consumer", "auto_offset_reset": "earliest", "process_ms": 2,
//!   "max_poll_records": 500, "enable_auto_commit": true,
//!   "auto_commit_interval_ms": 5000, "session_timeout_ms": 45000,
//!   "heartbeat_interval_ms": 3000, "instance_id": null,
//!   "deserialize": { "registry": 4 } }
//! ```
//!
//! - `bootstrap`, `group`, `topics` (required).
//! - `protocol`: `"classic"` (`JoinGroup`/`SyncGroup` with the range
//!   assignor) or `"consumer"` (KIP-848). Default: `"classic"`, Kafka's
//!   `group.protocol` default.
//! - `auto_offset_reset`: `"earliest"` or `"latest"`. Default: `"latest"`.
//! - `process_ms`: the logical time one record takes to process. Default: 0.
//! - `max_poll_records`, `enable_auto_commit`, `auto_commit_interval_ms`:
//!   Kafka's consumer settings, defaults 500, `true` and 5000.
//! - `session_timeout_ms`, `heartbeat_interval_ms`: the classic protocol's
//!   settings, defaults 45000 and 3000. A KIP-848 member takes both from the
//!   broker, as in Kafka.
//! - `instance_id`: static membership. Only `null` is accepted: the lab's
//!   consumer does not send a `group.instance.id` yet.
//! - `deserialize`: `null`, or `{"registry": <node id>}` to decode values in
//!   the Confluent wire format through that registry for the inspector.
//!
//! # Behaviour
//!
//! The node works like a JVM application's poll loop: it polls up to
//! `max_poll_records` records when it has none left to process, and
//! processes them one at a time, each taking `process_ms`. With auto-commit
//! on, it commits in the poll that follows `auto_commit_interval_ms`, before
//! it takes new records, as Kafka's consumers do, so a commit covers only
//! records the node processed; the client's own interval timer is set beyond
//! reach, and the client still commits on a rebalance and on close. Records
//! of revoked partitions that were already polled are still processed, as a
//! JVM application finishes the batch its poll returned; records of lost
//! partitions are dropped. With `deserialize`, a framed value waits for its
//! schema (`GET /schemas/ids/{id}`, cached), as a Confluent deserializer
//! blocks the poll that first meets the id.
//!
//! # Control commands
//!
//! - `{"cmd": "pause"}` and `{"cmd": "resume"}`: stop and start taking
//!   records; the member keeps its heartbeats.
//! - `{"cmd": "process_ms", "ms": n}`: change the processing time.
//! - `{"cmd": "commit"}`: commit the positions now.
//! - `{"cmd": "seek", "topic", "partition", "offset"}`: refused until the
//!   client has a seek (see the batch report).
//!
//! # Snapshot
//!
//! `group`, `protocol`, `state`, `member_id`, `epoch` (the classic
//! generation or the KIP-848 member epoch), `coordinator`, `subscription`,
//! `assignment: [{"topic", "partition", "position", "committed", "hwm",
//! "lag"}]` where `lag` counts the records written and not yet processed,
//! `lag` (their sum), `processed`, `processing_backlog`, `process_ms`,
//! `paused`, `max_poll_records`, `rebalances`, `records`, `polled`,
//! `fetches`, `commits`, `last_records: [{"topic", "partition", "offset",
//! "key", "value_preview", "schema_id"}]` (the last ten processed),
//! `deserialize` and `client`.
//!
//! # Events
//!
//! `group_joined`, `partitions_assigned`, `partitions_revoked`,
//! `partitions_lost` (warn), `schema_error` (warn) and `consumer_error`
//! (warn, once per api and code every five seconds).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    registry_client::{SchemaCache, SchemaLookup},
    serde::{preview, unframe},
};
use crate::lab::{
    LabError,
    client::{
        AutoOffsetReset, ClientOptions, ConsumedRecord, Consumer, ConsumerConfig, ConsumerEvent,
        GroupProtocol, KafkaClient,
    },
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId},
    scenario::NodeSpec,
};

/// How many processed records the snapshot lists.
const LAST_RECORDS: usize = 10;

/// The interval the client's own auto-commit timer gets: beyond any run, so
/// only the node's poll-time commits and the client's rebalance commits
/// happen.
const CLIENT_AUTO_COMMIT_INTERVAL_MS: Millis = 1 << 50;

/// How long a repeated consumer error stays out of the timeline.
const ERROR_EVENT_QUIET_MS: Millis = 5_000;

/// A topic partition.
type Partition = (String, i32);

/// The records a node polled and processes one at a time.
///
/// A record is taken from the backlog when the one before it finished, and
/// it finishes `process_ms` later.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Processing {
    backlog: VecDeque<ConsumedRecord>,
    /// The record in hand and when it finishes.
    current: Option<(ConsumedRecord, Millis)>,
    processed: u64,
}

impl Processing {
    /// Add polled records behind the backlog.
    pub fn accept(&mut self, records: impl IntoIterator<Item = ConsumedRecord>) {
        self.backlog.extend(records);
    }

    /// Whether the node has nothing left to process, so it polls again.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.current.is_none() && self.backlog.is_empty()
    }

    /// Records polled and not yet processed, the one in hand included.
    #[must_use]
    pub fn unprocessed(&self) -> usize {
        self.backlog.len() + usize::from(self.current.is_some())
    }

    /// Records processed so far.
    #[must_use]
    pub fn processed(&self) -> u64 {
        self.processed
    }

    /// The unprocessed records per partition.
    #[must_use]
    pub fn unprocessed_by_partition(&self) -> BTreeMap<Partition, u64> {
        let mut out: BTreeMap<Partition, u64> = BTreeMap::new();
        let current = self.current.as_ref().map(|(r, _)| r);
        for record in current.into_iter().chain(&self.backlog) {
            *out.entry((record.topic.clone(), record.partition))
                .or_default() += 1;
        }
        out
    }

    /// Drop the backlog records of `partitions`; the record in hand
    /// finishes.
    pub fn drop_partitions(&mut self, partitions: &BTreeSet<Partition>) {
        self.backlog
            .retain(|r| !partitions.contains(&(r.topic.clone(), r.partition)));
    }

    /// Finish what is due by `now` and start the next records, taking a
    /// record only when `ready` says it can be processed and `take` is set.
    /// Returns the records that finished, in order.
    pub fn advance(
        &mut self,
        now: Millis,
        process_ms: Millis,
        take: bool,
        mut ready: impl FnMut(&ConsumedRecord) -> bool,
    ) -> Vec<ConsumedRecord> {
        let mut done = Vec::new();
        loop {
            if let Some((_, at)) = &self.current {
                if *at > now {
                    break;
                }
                if let Some((record, finished_at)) = self.current.take() {
                    self.processed += 1;
                    done.push(record);
                    // The next record starts where this one finished.
                    if take
                        && self.backlog.front().is_some_and(&mut ready)
                        && let Some(next) = self.backlog.pop_front()
                    {
                        self.current = Some((next, finished_at + process_ms));
                    }
                }
                continue;
            }
            if !take || !self.backlog.front().is_some_and(&mut ready) {
                break;
            }
            if let Some(next) = self.backlog.pop_front() {
                self.current = Some((next, now + process_ms));
            }
        }
        done
    }

    /// When the record in hand finishes.
    #[must_use]
    pub fn next_finish(&self) -> Option<Millis> {
        self.current.as_ref().map(|(_, at)| *at)
    }
}

/// One partition row of the snapshot.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PartitionRow {
    pub topic: String,
    pub partition: i32,
    /// The offset of the next record to poll.
    pub position: Option<i64>,
    pub committed: Option<i64>,
    pub hwm: Option<i64>,
    /// Records written and not yet processed: `hwm - position` plus the
    /// polled records still waiting.
    pub lag: Option<i64>,
}

/// The rows of `assignment`, from the client's positions and lags and the
/// node's unprocessed records.
#[must_use]
pub fn partition_rows(
    assignment: &[Partition],
    position: impl Fn(&str, i32) -> Option<i64>,
    committed: impl Fn(&str, i32) -> Option<i64>,
    client_lag: &BTreeMap<Partition, i64>,
    unprocessed: &BTreeMap<Partition, u64>,
) -> Vec<PartitionRow> {
    assignment
        .iter()
        .map(|(topic, partition)| {
            let key = (topic.clone(), *partition);
            let position = position(topic, *partition);
            let fetched_lag = client_lag.get(&key).copied();
            let hwm = position.zip(fetched_lag).map(|(p, l)| p + l);
            let waiting = i64::try_from(unprocessed.get(&key).copied().unwrap_or(0)).unwrap_or(0);
            PartitionRow {
                topic: topic.clone(),
                partition: *partition,
                position,
                committed: committed(topic, *partition),
                hwm,
                lag: fetched_lag.map(|l| l + waiting),
            }
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeserializeConfig {
    registry: NodeId,
}

fn default_max_poll_records() -> usize {
    500
}

fn default_true() -> bool {
    true
}

fn default_auto_commit_interval_ms() -> Millis {
    5_000
}

fn default_session_timeout_ms() -> Millis {
    45_000
}

fn default_heartbeat_interval_ms() -> Millis {
    3_000
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    bootstrap: Vec<NodeId>,
    group: String,
    topics: Vec<String>,
    #[serde(default)]
    protocol: GroupProtocol,
    #[serde(default)]
    auto_offset_reset: AutoOffsetReset,
    #[serde(default)]
    process_ms: Millis,
    #[serde(default = "default_max_poll_records")]
    max_poll_records: usize,
    #[serde(default = "default_true")]
    enable_auto_commit: bool,
    #[serde(default = "default_auto_commit_interval_ms")]
    auto_commit_interval_ms: Millis,
    #[serde(default = "default_session_timeout_ms")]
    session_timeout_ms: Millis,
    #[serde(default = "default_heartbeat_interval_ms")]
    heartbeat_interval_ms: Millis,
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    deserialize: Option<DeserializeConfig>,
}

/// One processed record, for the snapshot.
struct LastRecord {
    topic: String,
    partition: i32,
    offset: i64,
    key: Value,
    value: Value,
    schema_id: Option<i32>,
}

/// The consumer node. See the module documentation.
pub struct ConsumerNode {
    bootstrap: Vec<Endpoint>,
    topics: Vec<String>,
    config: ConsumerConfig,
    process_ms: Millis,
    enable_auto_commit: bool,
    auto_commit_interval_ms: Millis,
    consumer: Consumer,
    processing: Processing,
    next_auto_commit_at: Millis,
    paused: bool,
    last_records: VecDeque<LastRecord>,
    schemas: Option<SchemaCache>,
    /// The last error event: api, code and time.
    last_error: Option<(&'static str, i16, Millis)>,
}

impl ConsumerNode {
    /// # Errors
    /// Returns a config error for an unknown or malformed key, an empty
    /// bootstrap list, group or topic list, or an `instance_id`.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let bad = |reason: String| LabError::config(spec, reason);
        let config: Config =
            serde_json::from_value(spec.config.clone()).map_err(|e| bad(format!("config: {e}")))?;
        if config.bootstrap.is_empty() {
            return Err(bad("`bootstrap` needs at least one broker".to_string()));
        }
        if config.group.is_empty() {
            return Err(bad("`group` is empty".to_string()));
        }
        if config.topics.is_empty() {
            return Err(bad("`topics` needs at least one topic".to_string()));
        }
        if config.instance_id.is_some() {
            return Err(bad(
                "`instance_id`: static membership is not available; the lab's consumer sends no `group.instance.id`"
                    .to_string(),
            ));
        }
        let client_config = ConsumerConfig {
            group_id: config.group,
            group_protocol: config.protocol,
            auto_offset_reset: config.auto_offset_reset,
            session_timeout_ms: config.session_timeout_ms,
            heartbeat_interval_ms: config.heartbeat_interval_ms,
            max_poll_records: config.max_poll_records.max(1),
            enable_auto_commit: config.enable_auto_commit,
            auto_commit_interval_ms: CLIENT_AUTO_COMMIT_INTERVAL_MS,
            ..ConsumerConfig::default()
        };
        let bootstrap: Vec<Endpoint> = config
            .bootstrap
            .iter()
            .map(|n| Endpoint::kafka(*n))
            .collect();
        let consumer = Self::build_consumer(&bootstrap, spec.id, &client_config, &config.topics);
        Ok(Self {
            bootstrap,
            topics: config.topics,
            config: client_config,
            process_ms: config.process_ms,
            enable_auto_commit: config.enable_auto_commit,
            auto_commit_interval_ms: config.auto_commit_interval_ms,
            consumer,
            processing: Processing::default(),
            next_auto_commit_at: 0,
            paused: false,
            last_records: VecDeque::new(),
            schemas: config.deserialize.map(|d| SchemaCache::new(d.registry)),
            last_error: None,
        })
    }

    fn build_consumer(
        bootstrap: &[Endpoint],
        id: NodeId,
        config: &ConsumerConfig,
        topics: &[String],
    ) -> Consumer {
        let client = KafkaClient::new(
            bootstrap.to_vec(),
            &format!("consumer-{id}"),
            ClientOptions::default(),
        );
        let mut consumer = Consumer::new(client, config.clone());
        let topics: Vec<&str> = topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topics);
        consumer
    }

    fn on_consumer_events(&mut self, ctx: &mut Ctx<'_>, events: Vec<ConsumerEvent>) {
        for event in events {
            match event {
                ConsumerEvent::Joined {
                    member_id,
                    generation,
                } => ctx.event(
                    "group_joined",
                    json!({ "member_id": member_id, "epoch": generation }),
                ),
                ConsumerEvent::Assigned { partitions } => {
                    ctx.event("partitions_assigned", json!({ "partitions": partitions }));
                }
                ConsumerEvent::Revoked { partitions } => {
                    ctx.event("partitions_revoked", json!({ "partitions": partitions }));
                }
                ConsumerEvent::Lost { partitions } => {
                    let lost: BTreeSet<Partition> = partitions.iter().cloned().collect();
                    self.processing.drop_partitions(&lost);
                    ctx.event(
                        "partitions_lost",
                        json!({ "partitions": partitions, "level": "warn" }),
                    );
                }
                ConsumerEvent::Committed { .. } => {}
                ConsumerEvent::Error { api, code } => {
                    let now = ctx.now();
                    let repeated = self.last_error.is_some_and(|(a, c, at)| {
                        a == api && c == code && now < at + ERROR_EVENT_QUIET_MS
                    });
                    if !repeated {
                        self.last_error = Some((api, code, now));
                        ctx.event(
                            "consumer_error",
                            json!({ "api": api, "code": code, "level": "warn" }),
                        );
                    }
                }
            }
        }
    }

    /// Whether `record` can be processed now: its value is not framed, or
    /// its schema lookup finished. Starts the lookup of a new schema id.
    fn schema_ready(
        schemas: &mut Option<SchemaCache>,
        ctx: &mut Ctx<'_>,
        record: &ConsumedRecord,
    ) -> bool {
        let Some(cache) = schemas else {
            return true;
        };
        let Some((id, _)) = record.value.as_deref().and_then(unframe) else {
            return true;
        };
        !matches!(cache.lookup(ctx, id), SchemaLookup::Pending)
    }

    fn remember(&mut self, ctx: &mut Ctx<'_>, record: &ConsumedRecord) {
        let framed = record.value.as_deref().and_then(unframe);
        let (value, schema_id) = match (&self.schemas, framed) {
            (Some(cache), Some((id, body))) => {
                let value = match cache.peek(id) {
                    SchemaLookup::Ready(schema) => schema.decode(body).unwrap_or_else(|e| {
                        ctx.event(
                            "schema_error",
                            json!({ "schema_id": id, "error": e.to_string(), "level": "warn" }),
                        );
                        json!({ "error": e.to_string() })
                    }),
                    SchemaLookup::Failed(reason) => json!({ "error": reason }),
                    SchemaLookup::Pending => Value::Null,
                };
                (value, Some(id))
            }
            _ => (record.value.as_deref().map_or(Value::Null, preview), None),
        };
        let key = record.key.as_deref().map_or(Value::Null, |k| {
            Value::String(String::from_utf8_lossy(k).into_owned())
        });
        if self.last_records.len() == LAST_RECORDS {
            self.last_records.pop_front();
        }
        self.last_records.push_back(LastRecord {
            topic: record.topic.clone(),
            partition: record.partition,
            offset: record.offset,
            key,
            value,
            schema_id,
        });
    }

    /// Process what is due, poll when the node has nothing left, and arm the
    /// next deadline.
    fn drive(&mut self, ctx: &mut Ctx<'_>) {
        loop {
            let take = !self.paused;
            let finished = {
                let schemas = &mut self.schemas;
                self.processing
                    .advance(ctx.now(), self.process_ms, take, |record| {
                        Self::schema_ready(schemas, ctx, record)
                    })
            };
            for record in &finished {
                self.remember(ctx, record);
            }
            if self.paused || !self.processing.is_idle() {
                break;
            }
            // The poll loop: commit what was processed when the interval
            // passed, then take the next records.
            let now = ctx.now();
            if self.enable_auto_commit && now >= self.next_auto_commit_at {
                self.consumer.commit(ctx);
                self.next_auto_commit_at = now + self.auto_commit_interval_ms;
            }
            let polled = self.consumer.poll(self.config.max_poll_records);
            if polled.is_empty() {
                break;
            }
            self.processing.accept(polled);
        }
        let now = ctx.now();
        let deadline = self
            .consumer
            .next_deadline(now)
            .into_iter()
            .chain(self.processing.next_finish())
            .chain(self.schemas.as_ref().and_then(SchemaCache::next_deadline))
            .min();
        if let Some(at) = deadline {
            ctx.arm(at.max(now));
        }
    }

    fn rows(&self) -> Vec<PartitionRow> {
        partition_rows(
            &self.consumer.assignment(),
            |t, p| self.consumer.position(t, p),
            |t, p| self.consumer.committed(t, p),
            &self.consumer.lag(),
            &self.processing.unprocessed_by_partition(),
        )
    }
}

impl Node for ConsumerNode {
    fn kind(&self) -> &'static str {
        "consumer"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        // A process starts over: a new member, nothing polled.
        self.consumer = Self::build_consumer(&self.bootstrap, ctx.me(), &self.config, &self.topics);
        self.processing = Processing::default();
        self.next_auto_commit_at = ctx.now() + self.auto_commit_interval_ms;
        self.last_records.clear();
        self.last_error = None;
        if let Some(cache) = &mut self.schemas {
            cache.reset();
        }
        let (events, _) = self.consumer.on_tick(ctx);
        self.on_consumer_events(ctx, events);
        self.drive(ctx);
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        match &mut self.schemas {
            Some(cache) if cache.owns(&frame) => {
                cache.on_frame(ctx, frame);
            }
            _ => {
                let (events, _) = self.consumer.on_frame(ctx, frame);
                self.on_consumer_events(ctx, events);
            }
        }
        self.drive(ctx);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(cache) = &mut self.schemas {
            cache.on_tick(ctx);
        }
        let (events, _) = self.consumer.on_tick(ctx);
        self.on_consumer_events(ctx, events);
        self.drive(ctx);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let answer = match command.get("cmd").and_then(Value::as_str) {
            Some("pause") => {
                self.paused = true;
                json!({ "paused": true })
            }
            Some("resume") => {
                self.paused = false;
                json!({ "paused": false })
            }
            Some("process_ms") => {
                let ms = command
                    .get("ms")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| "`ms` is a whole number of milliseconds".to_string())?;
                self.process_ms = ms;
                json!({ "process_ms": ms })
            }
            Some("commit") => {
                self.consumer.commit(ctx);
                json!({ "committing": true })
            }
            Some("seek") => {
                return Err(
                    "seek is not available: the lab's consumer client has no `seek` yet"
                        .to_string(),
                );
            }
            other => return Err(format!("unknown consumer command {other:?}")),
        };
        self.drive(ctx);
        Ok(answer)
    }

    fn snapshot(&self) -> Value {
        let base = self.consumer.snapshot();
        let rows = self.rows();
        let known: Vec<i64> = rows.iter().filter_map(|r| r.lag).collect();
        let lag = (!known.is_empty()).then(|| known.iter().sum::<i64>());
        let assignment: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "topic": r.topic,
                    "partition": r.partition,
                    "position": r.position,
                    "committed": r.committed,
                    "hwm": r.hwm,
                    "lag": r.lag,
                })
            })
            .collect();
        let last: Vec<Value> = self
            .last_records
            .iter()
            .map(|r| {
                json!({
                    "topic": r.topic,
                    "partition": r.partition,
                    "offset": r.offset,
                    "key": r.key,
                    "value_preview": r.value,
                    "schema_id": r.schema_id,
                })
            })
            .collect();
        json!({
            "group": base["group"],
            "protocol": base["protocol"],
            "state": base["state"],
            "member_id": base["member_id"],
            "epoch": base["generation"],
            "coordinator": base["coordinator"],
            "subscription": base["subscription"],
            "assignment": assignment,
            "lag": lag,
            "processed": self.processing.processed(),
            "processing_backlog": self.processing.unprocessed(),
            "process_ms": self.process_ms,
            "paused": self.paused,
            "max_poll_records": self.config.max_poll_records,
            "rebalances": base["rebalances"],
            "records": base["records"],
            "polled": base["polled"],
            "fetches": base["fetches"],
            "commits": base["commits"],
            "last_records": last,
            "deserialize": self.schemas.as_ref().map_or(Value::Null, SchemaCache::snapshot),
            "client": base["client"],
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;

    fn record(topic: &str, partition: i32, offset: i64) -> ConsumedRecord {
        ConsumedRecord {
            topic: topic.to_string(),
            partition,
            offset,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from_static(b"v")),
            headers: Vec::new(),
            leader_epoch: 0,
        }
    }

    fn offsets(records: &[ConsumedRecord]) -> Vec<i64> {
        records.iter().map(|r| r.offset).collect()
    }

    #[test]
    fn records_are_processed_one_at_a_time() {
        let mut p = Processing::default();
        p.accept((0..3).map(|o| record("t", 0, o)));
        // Each record takes 10 ms; the second starts when the first ends.
        let steps: Vec<(Millis, Vec<i64>, usize, Option<Millis>)> = vec![
            (0, vec![], 3, Some(10)),
            (9, vec![], 3, Some(10)),
            (10, vec![0], 2, Some(20)),
            (35, vec![1, 2], 0, None),
        ];
        for (now, finished, unprocessed, next) in steps {
            let done = p.advance(now, 10, true, |_| true);
            assert!(offsets(&done) == finished, "at {now}");
            assert!(p.unprocessed() == unprocessed, "at {now}");
            assert!(p.next_finish() == next, "at {now}");
        }
        assert!(p.processed() == 3);
        assert!(p.is_idle());
    }

    #[test]
    fn instant_processing_drains_the_backlog_at_once() {
        let mut p = Processing::default();
        p.accept((0..4).map(|o| record("t", 1, o)));
        assert!(offsets(&p.advance(7, 0, true, |_| true)) == vec![0, 1, 2, 3]);
        assert!(p.is_idle());
    }

    #[test]
    fn a_record_waits_while_it_is_not_ready_or_the_node_is_paused() {
        let mut p = Processing::default();
        p.accept([record("t", 0, 0), record("t", 0, 1)]);
        assert!(p.advance(0, 5, false, |_| true).is_empty());
        assert!(p.next_finish().is_none());
        assert!(p.advance(0, 5, true, |r| r.offset != 0).is_empty());
        assert!(p.unprocessed() == 2);
        assert!(p.advance(1, 5, true, |_| true).is_empty());
        assert!(offsets(&p.advance(6, 5, true, |_| true)) == vec![0]);
    }

    #[test]
    fn lost_partitions_leave_the_backlog_and_the_rest_is_counted_per_partition() {
        let mut p = Processing::default();
        p.accept([
            record("a", 0, 5),
            record("a", 1, 7),
            record("a", 0, 6),
            record("b", 0, 1),
        ]);
        p.advance(0, 100, true, |_| true);
        let expected: BTreeMap<Partition, u64> = BTreeMap::from([
            (("a".to_string(), 0), 2),
            (("a".to_string(), 1), 1),
            (("b".to_string(), 0), 1),
        ]);
        assert!(p.unprocessed_by_partition() == expected);
        p.drop_partitions(&BTreeSet::from([("a".to_string(), 0)]));
        // The record in hand, a-0 at 5, still finishes.
        let expected: BTreeMap<Partition, u64> = BTreeMap::from([
            (("a".to_string(), 0), 1),
            (("a".to_string(), 1), 1),
            (("b".to_string(), 0), 1),
        ]);
        assert!(p.unprocessed_by_partition() == expected);
    }

    #[test]
    fn lag_counts_what_is_unfetched_and_what_waits_to_be_processed() {
        let assignment = vec![
            ("t".to_string(), 0),
            ("t".to_string(), 1),
            ("t".to_string(), 2),
        ];
        let positions = BTreeMap::from([(0, 40_i64), (1, 10)]);
        let client_lag =
            BTreeMap::from([(("t".to_string(), 0), 60_i64), (("t".to_string(), 1), 0)]);
        let unprocessed = BTreeMap::from([(("t".to_string(), 0), 25_u64)]);
        let rows = partition_rows(
            &assignment,
            |_, p| positions.get(&p).copied(),
            |_, p| (p == 0).then_some(15),
            &client_lag,
            &unprocessed,
        );
        assert!(
            rows == vec![
                PartitionRow {
                    topic: "t".to_string(),
                    partition: 0,
                    position: Some(40),
                    committed: Some(15),
                    hwm: Some(100),
                    lag: Some(85),
                },
                PartitionRow {
                    topic: "t".to_string(),
                    partition: 1,
                    position: Some(10),
                    committed: None,
                    hwm: Some(10),
                    lag: Some(0),
                },
                PartitionRow {
                    topic: "t".to_string(),
                    partition: 2,
                    position: None,
                    committed: None,
                    hwm: None,
                    lag: None,
                },
            ]
        );
    }

    #[test]
    fn the_config_is_checked_at_load() {
        let node =
            |config: Value| ConsumerNode::from_spec(&NodeSpec::new(6, "consumer", "c", config));
        let cases = [
            (
                json!({ "bootstrap": [1], "group": "g" }),
                "missing field `topics`",
            ),
            (
                json!({ "bootstrap": [1], "group": "g", "topics": ["t"], "protocol": "eager" }),
                "unknown variant `eager`",
            ),
            (
                json!({ "bootstrap": [1], "group": "g", "topics": ["t"], "instance_id": "i-1" }),
                "`instance_id`: static membership is not available",
            ),
            (
                json!({ "bootstrap": [1], "group": "g", "topics": [] }),
                "`topics` needs at least one topic",
            ),
            (
                json!({ "bootstrap": [1], "group": "g", "topics": ["t"], "deserialize": { "registry": 4, "x": 1 } }),
                "unknown field `x`",
            ),
        ];
        for (config, message) in cases {
            let err = node(config.clone()).err().unwrap().to_string();
            assert!(err.contains(message), "{config}: {err}");
        }
        assert!(node(json!({ "bootstrap": [1], "group": "g", "topics": ["t"] })).is_ok());
    }
}
