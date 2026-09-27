//! The Kafka-backed `_schemas` store: Confluent's `KafkaStore`.
//!
//! The registry keeps no state of its own that survives it. Its state is
//! the replay of the compacted `_schemas` topic on the scenario's brokers,
//! which [`KafkaStore`] sets up, reads and writes through three Kafka
//! clients, as Confluent's does: an admin client for the startup, a
//! producer, and the reader.
//!
//! # Startup
//!
//! [`KafkaStore::start`] runs Confluent's startup in order, and the registry
//! serves nothing until it is done:
//!
//! 1. The admin steps: the cluster id (`DescribeCluster`), then Confluent's
//!    `createOrVerifySchemaTopic`. The admin client lists the topics
//!    (`Metadata`); a missing topic is created (`CreateTopics` to the
//!    controller) with 1 partition, `cleanup.policy=compact`, and a
//!    replication factor of `min(live brokers,
//!    kafkastore.topic.replication.factor)`, lowered with a warning, and
//!    refused only when no broker is live (`DescribeCluster`); an existing
//!    one must have exactly 1 partition (`DescribeTopicPartitions`) and
//!    `cleanup.policy=compact` (`DescribeConfigs`), and fewer replicas than
//!    desired is a warning.
//! 2. `partitionsFor`: the reader looks the topic up, up to ten times a
//!    second apart, and requires exactly one partition. It then seeks to the
//!    beginning and fetches from there on.
//! 3. `KafkaStore.init` waits until the reader reaches the last offset: the
//!    store produces a `NOOP` record to learn where the end is (Confluent's
//!    `getLatestOffset`) and waits until the reader has read it.
//!
//! Each step has `kafkastore.init.timeout.ms`. A step that fails or times
//! out fails the startup for good: Confluent's process exits, and the lab's
//! registry stays down and refuses connections until it restarts. The store
//! is then ready, and the registry elects its primary. The instance that
//! becomes the primary does not trust its last written offset and catches
//! up once more ([`KafkaStore::begin_leader_catch_up`], Confluent's
//! `setLeader`), so a second `NOOP` follows.
//!
//! # Writes
//!
//! One task runs at a time, which is Confluent's write lock:
//!
//! - [`KafkaStore::begin_catch_up`] is `waitUntilKafkaReaderReachesLastOffset`:
//!   when the last written offset is unknown, a `NOOP` record is produced to
//!   learn it, and then the reader must reach it.
//! - [`KafkaStore::begin_write`] is `KafkaStore.put` for each record in
//!   turn: the producer (idempotent, `acks=-1`) must acknowledge the record
//!   within `kafkastore.timeout.ms` of the send, and the reader must then
//!   read it back within `kafkastore.timeout.ms` of the acknowledgement.
//!
//! A put that fails marks the last written offset unknown, so the next task
//! produces a `NOOP` first. A record whose acknowledgement came too late
//! stays with the producer, which keeps retrying it under its own delivery
//! timeout; the reader applies it if it lands, as in Confluent.

mod reader;
mod setup;

use std::collections::VecDeque;

use serde_json::{Value, json};
use thiserror::Error;

use self::{
    reader::Reader,
    setup::{Progress, Setup, TopicLayout},
};
use crate::lab::{
    client::{
        ClientEvent, ClientOptions, KafkaClient, Producer, ProducerConfig, ProducerEvent,
        ProducerRecord, SeqNo,
    },
    net::{Ctx, Endpoint, Frame, Millis, NodeId},
    registry::{
        ids::LogOffset,
        lane::{self, Lane},
        record::{self, RawRecord},
    },
};

/// How many times the reader looks the topic up before the startup fails:
/// `KafkaStoreReaderThread` tries ten times.
const LOCATE_ATTEMPTS: u32 = 10;
/// The wait between two look-ups of the topic.
const LOCATE_INTERVAL_MS: Millis = 1_000;

/// The store's settings: Confluent's `kafkastore.*` configuration.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StoreConfig {
    /// `kafkastore.topic`. Default: `_schemas`.
    pub topic: String,
    /// `kafkastore.timeout.ms`: how long a write waits for its
    /// acknowledgement, and then for the reader. Default: 500.
    pub timeout_ms: Millis,
    /// `kafkastore.init.timeout.ms`: the timeout of each startup step.
    /// Default: 60 000.
    pub init_timeout_ms: Millis,
    /// `kafkastore.topic.replication.factor`: the replication factor the
    /// store creates the topic with when enough brokers are live. Default: 3.
    pub replication_factor: i16,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            topic: "_schemas".to_string(),
            timeout_ms: 500,
            init_timeout_ms: 60_000,
            replication_factor: 3,
        }
    }
}

/// Why a store task failed.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Confluent's `StoreTimeoutException`: the acknowledgement or the
    /// reader did not come in time.
    #[error("{0}")]
    Timeout(String),
    /// Confluent's `StoreException`: the write failed.
    #[error("{0}")]
    Failed(String),
}

/// Where the startup is.
#[derive(Clone, PartialEq, Eq, Debug)]
enum State {
    /// The admin steps.
    Setup,
    /// The reader looks the topic up: `attempt` counts the look-ups sent.
    /// While one is `waiting` for its answer, `at` is its timeout; between
    /// two, `at` is when the next goes out.
    Locate {
        attempt: u32,
        at: Millis,
        waiting: bool,
    },
    /// `KafkaStore.init` waits until the reader reaches the last offset.
    CatchUp,
    Ready,
    Failed(String),
}

impl State {
    fn name(&self) -> &'static str {
        match self {
            Self::Setup => "setup",
            Self::Locate { .. } => "locate",
            Self::CatchUp => "catch_up",
            Self::Ready => "ready",
            Self::Failed(_) => "failed",
        }
    }
}

/// The step of the running task.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stage {
    /// A record, or the noop when `noop`, is with the producer; its
    /// acknowledgement is due by `deadline`.
    Acking {
        seq: SeqNo,
        noop: bool,
        deadline: Millis,
    },
    /// The reader must read `offset` by `deadline`.
    Reading { offset: i64, deadline: Millis },
}

/// A catch-up, or the puts of a write.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Task {
    /// The records left to put after the current one; `None` for a
    /// catch-up.
    records: Option<VecDeque<RawRecord>>,
    timeout_ms: Millis,
    stage: Stage,
}

/// The Kafka-backed store. See the module documentation.
pub struct KafkaStore {
    config: StoreConfig,
    state: State,
    setup: Option<Setup>,
    layout: Option<TopicLayout>,
    cluster_id: Option<String>,
    reader: Reader,
    producer: Lane<Producer>,
    /// Confluent's `lastWrittenOffset`: `None` while unknown.
    last_written: Option<i64>,
    task: Option<Task>,
    outcome: Option<Result<(), StoreError>>,
    noops: u64,
    puts: u64,
}

impl KafkaStore {
    /// Start a store over `bootstrap`. `generation` counts the node's
    /// starts, so the clients of a restarted node open connections that no
    /// broker confuses with those of the one before.
    pub fn start(
        config: StoreConfig,
        bootstrap: &[NodeId],
        generation: u32,
        ctx: &mut Ctx<'_>,
    ) -> Self {
        let endpoints: Vec<Endpoint> = bootstrap.iter().map(|n| Endpoint::kafka(*n)).collect();
        let lane =
            |role: u32, ctx: &mut Ctx<'_>| (lane::index(generation, role), ctx.rand(u64::MAX));
        let (index, seed) = lane(lane::ADMIN, ctx);
        let admin = KafkaClient::new(endpoints.clone(), "adminclient-1", ClientOptions::default());
        let setup = Setup::new(
            Lane::new(admin, index, seed),
            &config.topic,
            config.replication_factor,
            config.init_timeout_ms,
            ctx.now(),
        );
        let (index, seed) = lane(lane::READER, ctx);
        let reader_client = KafkaClient::new(
            endpoints.clone(),
            &format!("KafkaStore-reader-{}", config.topic),
            ClientOptions::default(),
        );
        let reader = Reader::new(Lane::new(reader_client, index, seed), &config.topic);
        let (index, seed) = lane(lane::PRODUCER, ctx);
        let producer_client = KafkaClient::new(endpoints, "producer-1", ClientOptions::default());
        let producer = Producer::new(
            producer_client,
            ProducerConfig::default(),
            ctx.rand(u64::MAX),
        );
        let mut store = Self {
            config,
            state: State::Setup,
            setup: Some(setup),
            layout: None,
            cluster_id: None,
            reader,
            producer: Lane::new(producer, index, seed),
            last_written: None,
            task: None,
            outcome: None,
            noops: 0,
            puts: 0,
        };
        store.advance(ctx);
        store
    }

    /// Whether the startup finished, so the registry serves.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state == State::Ready
    }

    /// `loading` until the startup finished, then `ready`, or `failed`.
    #[must_use]
    pub fn state_name(&self) -> &'static str {
        match self.state {
            State::Ready => "ready",
            State::Failed(_) => "failed",
            _ => "loading",
        }
    }

    /// Why the startup failed, once it did.
    #[must_use]
    pub fn failure(&self) -> Option<&str> {
        match &self.state {
            State::Failed(message) => Some(message),
            _ => None,
        }
    }

    /// A frame for one of the store's clients. Returns the records the
    /// reader read, in offset order, for the registry to apply before it
    /// looks at the task's outcome.
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> Vec<(LogOffset, RawRecord)> {
        let mut read = Vec::new();
        if self.producer.owns(frame.conn) {
            let frame = self.producer.inbound(frame);
            let (events, _) = self
                .producer
                .run(ctx, |producer, ctx| producer.on_frame(ctx, frame));
            self.on_producer_events(ctx.now(), events);
        } else if self.reader.lane().owns(frame.conn) {
            let frame = self.reader.lane().inbound(frame);
            let events = self
                .reader
                .lane_mut()
                .run(ctx, |client, ctx| client.on_frame(ctx, frame));
            read = self.on_reader_events(ctx, events);
        } else if let Some(setup) = &mut self.setup
            && setup.lane().owns(frame.conn)
        {
            let frame = setup.lane().inbound(frame);
            let events = setup
                .lane_mut()
                .run(ctx, |client, ctx| client.on_frame(ctx, frame));
            let progress = setup.on_events(ctx, events);
            self.on_setup_progress(ctx, progress);
        }
        self.advance(ctx);
        read
    }

    /// The timer fired: drive every client, the startup and the task.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> Vec<(LogOffset, RawRecord)> {
        let (events, _) = self.producer.run(ctx, Producer::on_tick);
        self.on_producer_events(ctx.now(), events);
        let (events, _) = self.reader.lane_mut().run(ctx, KafkaClient::on_tick);
        let read = self.on_reader_events(ctx, events);
        if let Some(setup) = &mut self.setup {
            let (events, _) = setup.lane_mut().run(ctx, KafkaClient::on_tick);
            let progress = setup.on_events(ctx, events);
            self.on_setup_progress(ctx, progress);
        }
        self.advance(ctx);
        read
    }

    fn on_reader_events(
        &mut self,
        ctx: &mut Ctx<'_>,
        events: Vec<ClientEvent>,
    ) -> Vec<(LogOffset, RawRecord)> {
        let metadata_changed = events
            .iter()
            .any(|e| matches!(e, ClientEvent::MetadataUpdated));
        let read = self.reader.on_events(ctx.now(), events);
        if metadata_changed && let State::Locate { waiting: true, .. } = self.state {
            self.check_located(ctx);
        }
        read.into_iter()
            .map(|(offset, record)| (LogOffset(offset), record))
            .collect()
    }

    fn on_setup_progress(&mut self, ctx: &mut Ctx<'_>, progress: Progress) {
        match progress {
            Progress::Working => {}
            Progress::Done => {
                if let Some(mut setup) = self.setup.take() {
                    self.layout = setup.layout().cloned();
                    self.cluster_id = setup.cluster_id().map(str::to_string);
                    // The admin client closes once the topic is set up.
                    setup.lane_mut().run(ctx, KafkaClient::close);
                }
                self.state = State::Locate {
                    attempt: 0,
                    at: ctx.now(),
                    waiting: false,
                };
            }
            Progress::Failed(message) => self.fail(ctx, message),
        }
    }

    /// Close every client: their connections close, and what they had
    /// pending fails.
    pub fn close(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(mut setup) = self.setup.take() {
            setup.lane_mut().run(ctx, KafkaClient::close);
        }
        self.reader.lane_mut().run(ctx, KafkaClient::close);
        self.producer.run(ctx, Producer::close);
        self.task = None;
    }

    /// The startup failed for good: every client closes.
    fn fail(&mut self, ctx: &mut Ctx<'_>, message: String) {
        self.close(ctx);
        ctx.event(
            "kafkastore",
            json!({ "step": "failed", "message": message, "level": "error" }),
        );
        self.state = State::Failed(message);
    }

    /// The look-up answered: go on with one partition, fail with more, and
    /// look again later without the topic.
    fn check_located(&mut self, ctx: &mut Ctx<'_>) {
        let State::Locate { attempt, .. } = self.state else {
            return;
        };
        match self.reader.partitions() {
            Some(1) => {
                self.reader.start(ctx.now());
                self.state = State::CatchUp;
                self.begin_task(ctx, None, self.config.init_timeout_ms);
            }
            Some(count) if count > 1 => self.fail(
                ctx,
                format!(
                    "Unexpected number of partitions in the {} topic. Expected 1 and instead got {count}",
                    self.config.topic
                ),
            ),
            _ if attempt >= LOCATE_ATTEMPTS => self.fail(
                ctx,
                format!(
                    "Unable to subscribe to the Kafka topic {} backing this data store. Topic may not exist.",
                    self.config.topic
                ),
            ),
            _ => {
                self.state = State::Locate {
                    attempt,
                    at: ctx.now() + LOCATE_INTERVAL_MS,
                    waiting: false,
                };
            }
        }
    }

    /// Move everything that can move now: the startup, the reader and the
    /// task.
    fn advance(&mut self, ctx: &mut Ctx<'_>) {
        if let Some(setup) = &mut self.setup {
            let progress = setup.poll(ctx);
            self.on_setup_progress(ctx, progress);
        }
        if let State::Locate {
            attempt,
            at,
            waiting,
        } = self.state
            && ctx.now() >= at
        {
            if waiting {
                // `partitionsFor` gives up after `default.api.timeout.ms`.
                self.fail(
                    ctx,
                    "Timeout expired while fetching topic metadata".to_string(),
                );
                return;
            }
            // The reader's client asks for the topic at its next tick; its
            // answer ends this look-up.
            self.reader.look_up();
            self.state = State::Locate {
                attempt: attempt + 1,
                at: ctx.now() + self.config.init_timeout_ms,
                waiting: true,
            };
        }
        self.reader.poll(ctx);
        self.check_task(ctx);
        self.advance_startup(ctx);
    }

    /// The catch-up of `KafkaStore.init` finishes: the store is ready, or
    /// the startup failed.
    fn advance_startup(&mut self, ctx: &mut Ctx<'_>) {
        if self.state != State::CatchUp {
            return;
        }
        match self.outcome.take() {
            None => {}
            Some(Err(error)) => self.fail(ctx, error.to_string()),
            Some(Ok(())) => {
                ctx.event(
                    "kafkastore",
                    json!({
                        "step": "ready",
                        "topic": self.config.topic,
                        "offset": self.reader.offset(),
                        "level": "info",
                    }),
                );
                self.state = State::Ready;
            }
        }
    }

    /// Start `waitUntilKafkaReaderReachesLastOffset` with
    /// `kafkastore.timeout.ms`. The outcome comes from
    /// [`KafkaStore::take_outcome`].
    pub fn begin_catch_up(&mut self, ctx: &mut Ctx<'_>) {
        self.begin_task(ctx, None, self.config.timeout_ms);
    }

    /// Start the catch-up of an instance that just became the primary,
    /// Confluent's `setLeader`: the last written offset is marked unknown,
    /// so a `NOOP` learns the end of the topic, and the reader must reach
    /// it within `kafkastore.init.timeout.ms`. The outcome comes from
    /// [`KafkaStore::take_outcome`].
    pub fn begin_leader_catch_up(&mut self, ctx: &mut Ctx<'_>) {
        self.last_written = None;
        self.begin_task(ctx, None, self.config.init_timeout_ms);
    }

    /// Start `KafkaStore.put` of each of `records`, in order. The outcome
    /// comes from [`KafkaStore::take_outcome`].
    pub fn begin_write(&mut self, ctx: &mut Ctx<'_>, records: Vec<RawRecord>) {
        self.begin_task(ctx, Some(records.into()), self.config.timeout_ms);
    }

    /// The outcome of the task, once it finished.
    pub fn take_outcome(&mut self) -> Option<Result<(), StoreError>> {
        self.outcome.take()
    }

    fn begin_task(
        &mut self,
        ctx: &mut Ctx<'_>,
        records: Option<VecDeque<RawRecord>>,
        timeout_ms: Millis,
    ) {
        let now = ctx.now();
        self.outcome = None;
        let (records, stage) = match records {
            // A catch-up whose last written offset is known only waits for
            // the reader; one that does not know it produces a noop first.
            None => match self.last_written {
                Some(offset) => (
                    None,
                    Stage::Reading {
                        offset,
                        deadline: now + timeout_ms,
                    },
                ),
                None => (
                    None,
                    self.send(now, record::encode_noop(), true, timeout_ms),
                ),
            },
            Some(mut records) => {
                let Some(first) = records.pop_front() else {
                    self.finish(Ok(()), false);
                    return;
                };
                let stage = self.send(now, first, false, timeout_ms);
                (Some(records), stage)
            }
        };
        self.task = Some(Task {
            records,
            timeout_ms,
            stage,
        });
        self.check_task(ctx);
    }

    /// Hand a record to the producer, for partition 0 of the topic.
    fn send(&mut self, now: Millis, record: RawRecord, noop: bool, timeout_ms: Millis) -> Stage {
        let seq = self.producer.get_mut().send(
            now,
            ProducerRecord {
                topic: self.config.topic.clone(),
                partition: Some(0),
                key: Some(record.key),
                value: record.value,
                ..ProducerRecord::default()
            },
        );
        if noop {
            self.noops += 1;
        } else {
            self.puts += 1;
        }
        Stage::Acking {
            seq,
            noop,
            deadline: now + timeout_ms,
        }
    }

    fn on_producer_events(&mut self, now: Millis, events: Vec<ProducerEvent>) {
        let Some(task) = &mut self.task else {
            return;
        };
        let Stage::Acking { seq, noop, .. } = task.stage else {
            return;
        };
        for event in events {
            match event {
                ProducerEvent::Acked {
                    seq: acked, offset, ..
                } if acked == seq => {
                    self.last_written = Some(offset);
                    task.stage = Stage::Reading {
                        offset,
                        deadline: now + task.timeout_ms,
                    };
                    return;
                }
                ProducerEvent::Failed { seq: failed, .. } if failed == seq => {
                    let error = if noop {
                        StoreError::Failed(
                            "Failed to write Noop record to kafka store.".to_string(),
                        )
                    } else {
                        StoreError::Failed(
                            "Put operation failed while waiting for an ack from Kafka".to_string(),
                        )
                    };
                    self.finish(Err(error), !noop);
                    return;
                }
                _ => {}
            }
        }
    }

    /// End the task. A put that did not succeed marks the last written
    /// offset unknown, as Confluent's `put` does in its `finally`.
    fn finish(&mut self, outcome: Result<(), StoreError>, put: bool) {
        if put && outcome.is_err() {
            self.last_written = None;
        }
        self.task = None;
        self.outcome = Some(outcome);
    }

    /// Move the task on: the next put once the reader has the last one, or
    /// a timeout.
    fn check_task(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        loop {
            let Some(task) = &mut self.task else {
                return;
            };
            match task.stage {
                Stage::Acking { noop, deadline, .. } => {
                    if now >= deadline {
                        let error = if noop {
                            StoreError::Failed(
                                "Failed to write Noop record to kafka store.".to_string(),
                            )
                        } else {
                            StoreError::Timeout(
                                "Put operation timed out while waiting for an ack from Kafka"
                                    .to_string(),
                            )
                        };
                        self.finish(Err(error), !noop);
                    }
                    return;
                }
                Stage::Reading { offset, deadline } => {
                    if self.reader.offset() >= offset {
                        let timeout_ms = task.timeout_ms;
                        let next = task.records.as_mut().and_then(VecDeque::pop_front);
                        let Some(record) = next else {
                            self.finish(Ok(()), false);
                            return;
                        };
                        let stage = self.send(now, record, false, timeout_ms);
                        if let Some(task) = &mut self.task {
                            task.stage = stage;
                        }
                    } else if now >= deadline {
                        let error = StoreError::Timeout(format!(
                            "KafkaStoreReaderThread failed to reach target offset within the timeout interval. targetOffset: {offset}, offsetReached: {}, timeout(ms): {}",
                            self.reader.offset(),
                            task.timeout_ms
                        ));
                        let put = task.records.is_some();
                        self.finish(Err(error), put);
                        return;
                    } else {
                        return;
                    }
                }
            }
        }
    }

    /// The next time the store needs a tick.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        if matches!(self.state, State::Failed(_)) {
            return None;
        }
        let setup = self.setup.as_ref().and_then(|s| s.next_deadline(now));
        let locate = match self.state {
            State::Locate { at, .. } => Some(at.max(now)),
            _ => None,
        };
        let task = self.task.as_ref().map(|t| match t.stage {
            Stage::Acking { deadline, .. } | Stage::Reading { deadline, .. } => deadline.max(now),
        });
        setup
            .into_iter()
            .chain(locate)
            .chain(task)
            .chain(self.reader.next_deadline(now))
            .chain(self.producer.get().next_deadline(now))
            .min()
    }

    /// The store for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let step = match &self.state {
            State::Setup => self.setup.as_ref().map(|s| s.step().name()),
            State::Ready | State::Failed(_) => None,
            other => Some(other.name()),
        };
        let task = self.task.as_ref().map(|t| {
            let (stage, offset, deadline) = match t.stage {
                Stage::Acking { deadline, .. } => ("acking", None, deadline),
                Stage::Reading { offset, deadline } => ("reading", Some(offset), deadline),
            };
            json!({
                "kind": if t.records.is_some() { "write" } else { "catch_up" },
                "stage": stage,
                "offset": offset,
                "deadline": deadline,
                "records_left": t.records.as_ref().map_or(0, VecDeque::len),
            })
        });
        json!({
            "state": self.state_name(),
            "step": step,
            "error": self.failure(),
            "cluster_id": self.cluster_id,
            "topic": self.layout.as_ref().map(|l| json!({
                "name": self.config.topic,
                "partitions": l.partitions,
                "replication_factor": l.replication_factor,
                "cleanup_policy": l.cleanup_policy,
                "created": l.created,
            })),
            "last_written_offset": self.last_written,
            "task": task,
            "noops": self.noops,
            "puts": self.puts,
            "reader": self.reader.snapshot(),
            "producer": self.producer.get().snapshot(),
            "admin": self.setup.as_ref().map(|s| s.lane().get().snapshot()),
        })
    }
}
