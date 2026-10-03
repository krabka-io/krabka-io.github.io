//! The producer: an accumulator per partition, v2 record batches stamped by
//! the idempotent producer id, `Produce` requests grouped per leader, and
//! Kafka's retry rules.
//!
//! The producer follows Kafka's `RecordAccumulator` and `Sender`. A record
//! joins the open batch of its partition, or starts a new one when the open
//! batch cannot take it. A batch is ready when `linger_ms` passed since its
//! first record, when it is full, when a newer batch sits behind it, or when
//! it is a retry whose backoff passed. Ready batches are drained one per
//! partition per request, grouped by leader.
//!
//! With idempotence on, the first drain waits for `InitProducerId`; every
//! batch is stamped with the producer id, the epoch and a base sequence that
//! it keeps across retries, so a resend that lands twice is a duplicate the
//! broker answers with success. A retry goes alone, ahead of the batches
//! created after it. A batch that fails for good raises the epoch on the
//! client, as Kafka's `TransactionManager.handleFailedBatch` and
//! `bumpIdempotentProducerEpoch` do; the batches of the old epoch still in
//! flight return first, and the partition then starts again at sequence 0.
//! A routing error adopts the leader its answer names (KIP-951).
//!
//! A partition that holds records and has no known leader asks for the
//! metadata at every drain, as Kafka's `Sender.sendProducerData` requests an
//! update for the `unknownLeaderTopics` of `RecordAccumulator.ready`, so the
//! lookups repeat at the client's metadata backoff until the leaders are
//! back, as for a moment after every broker restarted.
//!
//! A record for a topic the metadata does not know yet, or for a partition
//! beyond the topic's partition count, waits for the metadata as Kafka's
//! `KafkaProducer.waitOnMetadata` blocks `send`: the producer asks for the
//! topic again after every answer, at the client's metadata backoff of
//! `retry.backoff.ms` doubling up to `retry.backoff.max.ms`, and fails the
//! record with Kafka's `TimeoutException` text once `max.block.ms` passed.
//! A record that waited takes its timestamp and joins a batch when the
//! metadata arrives, as `send` does after the wait.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use derive_more::{Display, From, Into};
use krabka_protocol::{
    owned::{
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{RecordHeader, RecordsPayload, increment_sequence},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    ClientError, ClientEvent, KafkaClient, RequestId, Target,
    batch::{BatchRecord, ProducerStamp, build_batch},
    partitioner::{StickyPartitioner, partition_for_key},
    retry::{self, ErrorClass},
};
use crate::lab::{
    codes,
    net::{Ctx, Frame, Millis, Rng},
};

/// `DUPLICATE_SEQUENCE_NUMBER`: the broker already had the batch.
const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// `UNKNOWN_PRODUCER_ID`: the broker lost the producer's state.
const UNKNOWN_PRODUCER_ID: i16 = 59;
/// The `transaction_timeout_ms` of an idempotent `InitProducerId`: Kafka
/// sends `Integer.MAX_VALUE` when no transactional id is set.
const IDEMPOTENT_TRANSACTION_TIMEOUT: i32 = i32::MAX;
/// `retry.backoff.max.ms` of KIP-580.
const RETRY_BACKOFF_MAX_MS: Millis = 1_000;
/// The most requests in flight an idempotent producer allows per partition.
const MAX_IDEMPOTENT_IN_FLIGHT: usize = 5;
/// The bytes a v2 record batch spends before its first record.
const RECORD_BATCH_OVERHEAD: usize = 61;

/// The `acks` of a produce request.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Acks {
    /// `acks=0`: the broker sends no answer; a record is acked when written.
    None,
    /// `acks=1`: the leader's append.
    Leader,
    /// `acks=-1`: the in-sync replicas.
    #[default]
    All,
}

impl Acks {
    /// The wire value.
    #[must_use]
    pub const fn as_wire(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }

    /// The `acks` of a wire value, `-1`, `0` or `1`.
    #[must_use]
    pub const fn from_wire(value: i16) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::Leader),
            -1 => Some(Self::All),
            _ => None,
        }
    }
}

/// The compression of a batch.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
}

impl Compression {
    /// The compression bits of the batch attributes.
    #[must_use]
    pub const fn attribute_bits(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Snappy => 2,
        }
    }

    /// The `compression.type` name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
        }
    }

    /// The compression a `compression.type` value names.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "gzip" => Some(Self::Gzip),
            "snappy" => Some(Self::Snappy),
            _ => None,
        }
    }
}

/// The settings of a producer, with Kafka's defaults.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProducerConfig {
    /// `acks`. Default: all.
    pub acks: Acks,
    /// `linger.ms`. Default: 5.
    pub linger_ms: Millis,
    /// `batch.size` in bytes. Default: 16 384.
    pub batch_size: usize,
    /// `max.in.flight.requests.per.connection`, applied per partition here.
    /// Default: 5.
    pub max_in_flight: usize,
    /// `enable.idempotence`. Default: true.
    pub enable_idempotence: bool,
    /// `retries`. Default: unlimited; `delivery_timeout_ms` bounds them.
    pub retries: u32,
    /// `retry.backoff.ms`. Default: 100.
    pub retry_backoff_ms: Millis,
    /// `delivery.timeout.ms`. Default: 120 000.
    pub delivery_timeout_ms: Millis,
    /// `request.timeout.ms`, sent as the produce `timeout_ms`. Default:
    /// 30 000.
    pub request_timeout_ms: Millis,
    /// `compression.type`. Default: none.
    pub compression: Compression,
    /// How many keyless records go to a sticky partition before the
    /// partitioner moves on. Default: 32.
    pub sticky_batch_records: u32,
    /// `max.block.ms`: how long a record waits for the metadata of its
    /// topic before it fails. Default: 60 000.
    pub max_block_ms: Millis,
}

impl ProducerConfig {
    /// Whether the producer is idempotent. Kafka enables idempotence only
    /// with `acks=all`, `retries > 0` and at most 5 requests in flight; with
    /// a conflicting setting the default turns it off, and the lab does the
    /// same whatever `enable_idempotence` says.
    #[must_use]
    pub fn idempotent(&self) -> bool {
        self.enable_idempotence
            && self.acks == Acks::All
            && self.retries > 0
            && self.max_in_flight <= MAX_IDEMPOTENT_IN_FLIGHT
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            acks: Acks::All,
            linger_ms: 5,
            batch_size: 16_384,
            max_in_flight: 5,
            enable_idempotence: true,
            retries: u32::MAX,
            retry_backoff_ms: 100,
            delivery_timeout_ms: 120_000,
            request_timeout_ms: 30_000,
            compression: Compression::None,
            sticky_batch_records: 32,
            max_block_ms: 60_000,
        }
    }
}

/// The sequence number of a record handed to [`Producer::send`]. It names the
/// record in [`ProducerEvent`]s.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct SeqNo(pub u64);

/// A record to send.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ProducerRecord {
    pub topic: String,
    /// The partition, or `None` for the partitioner to choose.
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    /// The `CreateTime` timestamp, or `None` for the send time.
    pub timestamp: Option<i64>,
}

/// What the producer reports.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ProducerEvent {
    /// The broker acknowledged the record at `offset` (`-1` with `acks=0`).
    Acked {
        seq: SeqNo,
        topic: String,
        partition: i32,
        offset: i64,
        /// From `send` to the acknowledgement.
        latency_ms: Millis,
    },
    /// The record failed for good with a Kafka error code. `partition` is
    /// `-1` when the record never got one.
    Failed {
        seq: SeqNo,
        topic: String,
        partition: i32,
        code: i16,
        /// The text of the exception Kafka's producer fails the record with
        /// when the producer itself decided the failure: the metadata wait
        /// that ran past `max.block.ms`, or a batch that expired before it
        /// was sent. `None` for an error the broker answered.
        message: Option<String>,
    },
}

/// A histogram of acknowledgement latencies with fixed buckets.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RttHistogram {
    /// One count per bucket of [`RttHistogram::BOUNDS`], plus the overflow.
    counts: [u64; 13],
    sum: Millis,
    count: u64,
    max: Millis,
}

impl RttHistogram {
    /// The upper bound of each bucket, in milliseconds; the last bucket has
    /// none.
    pub const BOUNDS: [Millis; 12] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000];

    /// Count one latency.
    pub fn record(&mut self, latency: Millis) {
        let bucket = Self::BOUNDS
            .iter()
            .position(|bound| latency <= *bound)
            .unwrap_or(Self::BOUNDS.len());
        self.counts[bucket] += 1;
        self.sum += latency;
        self.count += 1;
        self.max = self.max.max(latency);
    }

    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The mean latency, or 0 without a sample.
    #[must_use]
    pub fn mean(&self) -> Millis {
        self.sum.checked_div(self.count).unwrap_or(0)
    }

    #[must_use]
    pub fn max(&self) -> Millis {
        self.max
    }

    /// The histogram for the inspector: `{"buckets":[{"le":ms,"count":n},...,{"le":null,"count":n}],"mean","max","count"}`.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let buckets: Vec<Value> = self
            .counts
            .iter()
            .enumerate()
            .map(|(i, count)| json!({ "le": Self::BOUNDS.get(i), "count": count }))
            .collect();
        json!({
            "buckets": buckets,
            "mean_ms": self.mean(),
            "max_ms": self.max,
            "count": self.count,
        })
    }
}

/// The producer's counters.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ProducerMetrics {
    pub sent: u64,
    pub acked: u64,
    pub failed: u64,
    pub retried: u64,
    /// The bytes of every batch sent, before compression.
    pub bytes: u64,
    pub batches_sent: u64,
    /// The last acknowledged offset per partition.
    pub last_offsets: BTreeMap<(String, i32), i64>,
    pub rtt: RttHistogram,
}

/// A record in the accumulator.
struct PendingRecord {
    seq: SeqNo,
    record: BatchRecord,
    /// When `send` took the record: its latency counts from here.
    sent_at: Millis,
    size: usize,
}

/// A batch in the accumulator or in flight.
struct ProducerBatch {
    /// The creation order: batches of a partition drain in this order, and
    /// a retried batch goes back to its place.
    id: u64,
    records: Vec<PendingRecord>,
    /// The encoded size, batch header included, as Kafka's
    /// `MemoryRecordsBuilder` estimates it against `batch.size`.
    size: usize,
    created_at: Millis,
    /// The base sequence, assigned at the first drain and kept on retries
    /// while the producer epoch stays.
    base_sequence: Option<i32>,
    /// The producer epoch `base_sequence` belongs to. A batch of an older
    /// epoch takes a new sequence when it drains again, as Kafka's
    /// `TxnPartitionMap.startSequencesAtBeginning` re-sequences it.
    epoch: Option<i16>,
    attempts: u32,
    retry_at: Millis,
    /// A newer batch sits behind it, so no record joins it any more.
    closed: bool,
}

impl ProducerBatch {
    fn new(id: u64, first: PendingRecord, now: Millis) -> Self {
        Self {
            id,
            size: RECORD_BATCH_OVERHEAD + first.size,
            records: vec![first],
            created_at: now,
            base_sequence: None,
            epoch: None,
            attempts: 0,
            retry_at: 0,
            closed: false,
        }
    }

    fn count(&self) -> i32 {
        i32::try_from(self.records.len()).unwrap_or(i32::MAX)
    }
}

/// The batches of one partition.
#[derive(Default)]
struct PartitionQueue {
    batches: VecDeque<ProducerBatch>,
    next_sequence: i32,
    /// The sequence the broker expects next: the one after the last
    /// acknowledged batch.
    next_expected: i32,
    in_flight: usize,
    /// The producer epoch of the batches in flight. After an epoch bump the
    /// partition drains again only once they all returned.
    in_flight_epoch: Option<i16>,
}

/// The idempotent producer's identity.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ProducerId {
    /// Idempotence is off, or no id was asked for yet.
    Absent,
    /// `InitProducerId` is in flight.
    Requested(RequestId),
    /// The broker assigned an id; the epoch rises on the client.
    Ready { producer_id: i64, epoch: i16 },
}

/// The broker's answer for one batch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Answer {
    code: i16,
    base_offset: i64,
    log_start_offset: i64,
    /// The leader and its epoch a routing error names (KIP-951), or `-1`.
    leader: (i32, i32),
}

impl Answer {
    /// An answer that carries only an error.
    const fn error(code: i16) -> Self {
        Self {
            code,
            base_offset: -1,
            log_start_offset: -1,
            leader: (-1, -1),
        }
    }
}

/// The batches one produce request carries.
struct InFlightProduce {
    batches: Vec<(String, i32, ProducerBatch)>,
}

/// A record that waits for the metadata of its topic, as Kafka's `send`
/// blocks in `waitOnMetadata`.
struct DeferredRecord {
    seq: SeqNo,
    partition: Option<i32>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<RecordHeader>,
    timestamp: Option<i64>,
    /// When `send` took the record; it fails `max.block.ms` later.
    sent_at: Millis,
}

/// The producer. See the module documentation.
pub struct Producer {
    client: KafkaClient,
    config: ProducerConfig,
    next_seq: u64,
    partitions: BTreeMap<(String, i32), PartitionQueue>,
    deferred: BTreeMap<String, Vec<DeferredRecord>>,
    sticky: StickyPartitioner,
    rng: Rng,
    identity: ProducerId,
    /// When `InitProducerId` may be sent again after a failure.
    identity_retry_at: Millis,
    in_flight: BTreeMap<RequestId, InFlightProduce>,
    next_batch: u64,
    /// `acks=0` acknowledgements made inside a drain, delivered by the next
    /// `on_tick` or `on_frame`.
    stashed: Vec<ProducerEvent>,
    metrics: ProducerMetrics,
    closed: bool,
}

impl Producer {
    /// A producer over `client`. `seed` drives the sticky partitioner's
    /// random choice.
    #[must_use]
    pub fn new(client: KafkaClient, config: ProducerConfig, seed: u64) -> Self {
        Self {
            sticky: StickyPartitioner::new(config.sticky_batch_records),
            client,
            config,
            next_seq: 0,
            partitions: BTreeMap::new(),
            deferred: BTreeMap::new(),
            rng: Rng::new(seed),
            identity: ProducerId::Absent,
            identity_retry_at: 0,
            in_flight: BTreeMap::new(),
            next_batch: 0,
            stashed: Vec::new(),
            metrics: ProducerMetrics::default(),
            closed: false,
        }
    }

    #[must_use]
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut KafkaClient {
        &mut self.client
    }

    #[must_use]
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    #[must_use]
    pub fn metrics(&self) -> &ProducerMetrics {
        &self.metrics
    }

    /// The producer id and epoch, once `InitProducerId` answered.
    #[must_use]
    pub fn producer_id(&self) -> Option<(i64, i16)> {
        match self.identity {
            ProducerId::Ready { producer_id, epoch } => Some((producer_id, epoch)),
            ProducerId::Absent | ProducerId::Requested(_) => None,
        }
    }

    /// The epoch sequences are assigned under now.
    fn current_epoch(&self) -> Option<i16> {
        match self.identity {
            ProducerId::Ready { epoch, .. } => Some(epoch),
            ProducerId::Absent | ProducerId::Requested(_) => None,
        }
    }

    /// Records accepted and not yet acknowledged or failed.
    #[must_use]
    pub fn pending_records(&self) -> usize {
        let queued: usize = self
            .partitions
            .values()
            .flat_map(|q| q.batches.iter())
            .map(|b| b.records.len())
            .sum();
        let in_flight: usize = self
            .in_flight
            .values()
            .flat_map(|r| r.batches.iter())
            .map(|(_, _, b)| b.records.len())
            .sum();
        let deferred: usize = self.deferred.values().map(Vec::len).sum();
        queued + in_flight + deferred
    }

    /// Accept a record. It goes out at the next tick that finds its batch
    /// ready; the outcome arrives as a [`ProducerEvent`] with the returned
    /// sequence number.
    ///
    /// A record the metadata cannot place yet, because its topic or its
    /// partition is unknown, waits for the metadata behind the records of
    /// its topic that already wait (see the module documentation). A
    /// negative partition fails at once with Kafka's
    /// `IllegalArgumentException` text.
    pub fn send(&mut self, now: Millis, record: ProducerRecord) -> SeqNo {
        self.next_seq += 1;
        let seq = SeqNo(self.next_seq);
        self.metrics.sent += 1;
        let ProducerRecord {
            topic,
            partition,
            key,
            value,
            headers,
            timestamp,
        } = record;
        if let Some(invalid) = partition.filter(|p| *p < 0) {
            self.metrics.failed += 1;
            self.stashed.push(ProducerEvent::Failed {
                seq,
                topic,
                partition: invalid,
                code: codes::UNKNOWN_SERVER_ERROR,
                message: Some(format!(
                    "Invalid partition: {invalid}. Partition number should always be non-negative or null."
                )),
            });
            return seq;
        }
        self.client.add_topics([topic.as_str()]);
        let record = DeferredRecord {
            seq,
            partition,
            key,
            value,
            headers,
            timestamp,
            sent_at: now,
        };
        let waiting = if self.deferred.contains_key(&topic) {
            Some(record)
        } else {
            self.place(&topic, record, now)
        };
        if let Some(record) = waiting {
            // The first `requestUpdateForTopic` of Kafka's `waitOnMetadata`.
            self.client.request_metadata_refresh();
            self.deferred.entry(topic).or_default().push(record);
        }
        seq
    }

    /// Put a record in the batch of its partition when the metadata knows
    /// the partition, and give it back when it must wait. It takes its
    /// timestamp now, as Kafka's `send` does once `waitOnMetadata` returns.
    fn place(
        &mut self,
        topic: &str,
        record: DeferredRecord,
        now: Millis,
    ) -> Option<DeferredRecord> {
        let count = self
            .client
            .metadata()
            .partition_count(topic)
            .filter(|count| *count > 0);
        let partition = match (record.partition, count) {
            (Some(partition), Some(count)) => (partition < count).then_some(partition),
            (None, Some(_)) => self.choose_partition(topic, record.key.as_deref()),
            (_, None) => None,
        };
        let Some(partition) = partition else {
            return Some(record);
        };
        let batch_record = BatchRecord {
            timestamp: record
                .timestamp
                .unwrap_or_else(|| i64::try_from(now).unwrap_or(i64::MAX)),
            key: record.key,
            value: record.value,
            headers: record.headers,
        };
        let size = record_size(&batch_record);
        let pending = PendingRecord {
            seq: record.seq,
            record: batch_record,
            sent_at: record.sent_at,
            size,
        };
        self.append(topic, partition, pending, now);
        None
    }

    fn choose_partition(&mut self, topic: &str, key: Option<&[u8]>) -> Option<i32> {
        let count = self.client.metadata().partition_count(topic)?;
        if count <= 0 {
            return None;
        }
        if let Some(key) = key {
            return partition_for_key(key, count);
        }
        let available: Vec<i32> = self
            .client
            .metadata()
            .topics
            .get(topic)
            .map(|t| {
                t.partitions
                    .values()
                    .filter(|p| p.leader >= 0)
                    .map(|p| p.index)
                    .collect()
            })
            .unwrap_or_default();
        let random = self.rng.next_u64();
        Some(self.sticky.partition(topic, &available, count, random))
    }

    fn append(&mut self, topic: &str, partition: i32, record: PendingRecord, now: Millis) {
        let batch_size = self.config.batch_size;
        self.next_batch += 1;
        let id = self.next_batch;
        let queue = self
            .partitions
            .entry((topic.to_string(), partition))
            .or_default();
        if let Some(last) = queue.batches.back_mut() {
            if !last.closed && last.base_sequence.is_none() && last.size + record.size <= batch_size
            {
                last.size += record.size;
                last.records.push(record);
                return;
            }
            last.closed = true;
        }
        queue.batches.push_back(ProducerBatch::new(id, record, now));
    }

    /// The metadata changed: place the waiting records it can place, in
    /// order per topic, and ask for the metadata again while any still
    /// waits, as the loop of Kafka's `waitOnMetadata` calls
    /// `requestUpdateForTopic` after each update that did not bring the
    /// partition.
    fn assign_deferred(&mut self, now: Millis) {
        let topics: Vec<String> = self.deferred.keys().cloned().collect();
        for topic in topics {
            let Some(records) = self.deferred.remove(&topic) else {
                continue;
            };
            let mut waiting = Vec::new();
            for record in records {
                // A record waits behind an earlier one of its topic that waits.
                let left = if waiting.is_empty() {
                    self.place(&topic, record, now)
                } else {
                    Some(record)
                };
                waiting.extend(left);
            }
            if !waiting.is_empty() {
                self.deferred.insert(topic, waiting);
            }
        }
        if !self.deferred.is_empty() {
            self.client.request_metadata_refresh();
        }
    }

    /// A frame arrived for this producer's client. Returns the events and the
    /// next deadline to arm.
    pub fn on_frame(
        &mut self,
        ctx: &mut Ctx<'_>,
        frame: Frame,
    ) -> (Vec<ProducerEvent>, Option<Millis>) {
        let mut events = std::mem::take(&mut self.stashed);
        let client_events = self.client.on_frame(ctx, frame);
        self.handle_client_events(ctx, client_events, &mut events);
        self.drain(ctx);
        events.append(&mut self.stashed);
        (events, self.next_deadline(ctx.now()))
    }

    /// Drive the producer: send ready batches, retries and the producer id
    /// request, and expire records past their delivery timeout.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> (Vec<ProducerEvent>, Option<Millis>) {
        let mut events = std::mem::take(&mut self.stashed);
        let (client_events, _) = self.client.on_tick(ctx);
        self.handle_client_events(ctx, client_events, &mut events);
        self.ensure_producer_id(ctx);
        self.expire(ctx.now(), &mut events);
        self.drain(ctx);
        events.append(&mut self.stashed);
        (events, self.next_deadline(ctx.now()))
    }

    /// Fail every pending record and close the client.
    pub fn close(&mut self, ctx: &mut Ctx<'_>) -> Vec<ProducerEvent> {
        self.closed = true;
        let mut events = Vec::new();
        for (topic, records) in std::mem::take(&mut self.deferred) {
            for deferred in records {
                events.push(ProducerEvent::Failed {
                    seq: deferred.seq,
                    topic: topic.clone(),
                    partition: deferred.partition.unwrap_or(-1),
                    code: codes::UNKNOWN_SERVER_ERROR,
                    message: None,
                });
            }
        }
        let queued: Vec<(String, i32, ProducerBatch)> = std::mem::take(&mut self.partitions)
            .into_iter()
            .flat_map(|((topic, partition), queue)| {
                queue
                    .batches
                    .into_iter()
                    .map(move |b| (topic.clone(), partition, b))
            })
            .collect();
        let in_flight: Vec<(String, i32, ProducerBatch)> = std::mem::take(&mut self.in_flight)
            .into_values()
            .flat_map(|r| r.batches)
            .collect();
        for (topic, partition, batch) in queued.into_iter().chain(in_flight) {
            self.fail_batch(
                &topic,
                partition,
                batch,
                (codes::UNKNOWN_SERVER_ERROR, None),
                &mut events,
            );
        }
        self.client.close(ctx);
        events
    }

    /// The next time the producer needs a tick: `None` when only an answer
    /// or new metadata can move it on, which arrive as frames.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let client = self.client.next_deadline(now);
        // A batch counts only when a drain could send it. A batch blocked by
        // the requests in flight waits for their answers, a batch without a
        // leader for the metadata lookup the last drain asked for, and every
        // batch of an idempotent producer for its producer id; each of those
        // arrives as a frame.
        let can_drain = self.may_send();
        let epoch = self.current_epoch();
        let batches = self.partitions.iter().filter(|_| can_drain).filter_map(
            |((topic, partition), queue)| {
                let head = queue.batches.front()?;
                let is_retry = head.attempts > 0;
                let blocked = queue.in_flight >= self.config.max_in_flight
                    || (queue.in_flight > 0 && (is_retry || queue.in_flight_epoch != epoch));
                if blocked || self.client.metadata().leader(topic, *partition).is_none() {
                    return None;
                }
                let ready_at = if head.closed
                    || is_retry
                    || head.size >= self.config.batch_size
                    || queue.batches.len() > 1
                {
                    now
                } else {
                    head.created_at + self.config.linger_ms
                };
                Some(ready_at.max(head.retry_at))
            },
        );
        let expiries = self
            .partitions
            .values()
            .filter_map(|queue| queue.batches.front())
            .map(|b| b.created_at + self.config.delivery_timeout_ms)
            .chain(
                self.deferred
                    .values()
                    .flatten()
                    .map(|d| d.sent_at + self.config.max_block_ms),
            );
        let producer_id = match self.identity {
            ProducerId::Absent if self.config.idempotent() => Some(self.identity_retry_at),
            _ => None,
        };
        let stashed = (!self.stashed.is_empty()).then_some(now);
        client
            .into_iter()
            .chain(batches)
            .chain(expiries)
            .chain(producer_id)
            .chain(stashed)
            .min()
            .map(|at| at.max(now))
    }

    fn handle_client_events(
        &mut self,
        ctx: &mut Ctx<'_>,
        client_events: Vec<ClientEvent>,
        events: &mut Vec<ProducerEvent>,
    ) {
        for event in client_events {
            match event {
                ClientEvent::MetadataUpdated => self.assign_deferred(ctx.now()),
                ClientEvent::Response { id, result } => {
                    if let Some(in_flight) = self.in_flight.remove(&id) {
                        self.on_produce_response(ctx, in_flight, result, events);
                    } else if self.identity == ProducerId::Requested(id) {
                        self.on_init_producer_id(ctx.now(), result, events);
                    }
                }
            }
        }
    }

    // ---- producer id ------------------------------------------------------------

    fn ensure_producer_id(&mut self, ctx: &mut Ctx<'_>) {
        if !self.config.idempotent()
            || self.identity != ProducerId::Absent
            || ctx.now() < self.identity_retry_at
            || self.closed
        {
            return;
        }
        let request = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: IDEMPOTENT_TRANSACTION_TIMEOUT,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        };
        let id = self.client.send(ctx, Target::Any, request);
        self.identity = ProducerId::Requested(id);
    }

    /// Raise the epoch on the client, so every partition starts again at
    /// sequence 0: Kafka's `TransactionManager.bumpIdempotentProducerEpoch`.
    /// A broker takes a higher epoch of an idempotent producer when its first
    /// batch starts at sequence 0. At the largest epoch the producer asks for
    /// a new producer id instead, as `resetIdempotentProducerId` does.
    fn bump_epoch(&mut self, now: Millis) {
        let ProducerId::Ready { producer_id, epoch } = self.identity else {
            return;
        };
        self.identity = if let Some(epoch) = epoch.checked_add(1) {
            ProducerId::Ready { producer_id, epoch }
        } else {
            self.identity_retry_at = now;
            ProducerId::Absent
        };
        self.reset_sequences();
    }

    fn on_init_producer_id(
        &mut self,
        now: Millis,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ProducerEvent>,
    ) {
        let response = result
            .ok()
            .and_then(super::Response::downcast::<InitProducerIdResponse>);
        match response {
            Some(response) if response.error_code == codes::NONE => {
                self.identity = ProducerId::Ready {
                    producer_id: response.producer_id,
                    epoch: response.producer_epoch,
                };
                self.reset_sequences();
            }
            Some(response) if !retry::class(response.error_code).is_retriable() => {
                // A producer id the broker refuses for good: fail what waits
                // and ask again later, so a fixed cluster recovers.
                self.identity = ProducerId::Absent;
                self.identity_retry_at = now + self.config.retry_backoff_ms;
                let queued: Vec<(String, i32, ProducerBatch)> = self
                    .partitions
                    .iter_mut()
                    .flat_map(|((topic, partition), queue)| {
                        queue
                            .batches
                            .drain(..)
                            .map(|b| (topic.clone(), *partition, b))
                            .collect::<Vec<_>>()
                    })
                    .collect();
                for (topic, partition, batch) in queued {
                    self.fail_batch(
                        &topic,
                        partition,
                        batch,
                        (response.error_code, None),
                        events,
                    );
                }
            }
            _ => {
                self.identity = ProducerId::Absent;
                self.identity_retry_at = now + self.config.retry_backoff_ms;
            }
        }
    }

    /// Every partition starts again at sequence 0 under the new producer
    /// id or epoch. A batch that holds a sequence of the old epoch takes a
    /// new one when it drains again. Kafka's
    /// `TransactionManager.resetSequenceNumbers`.
    fn reset_sequences(&mut self) {
        for queue in self.partitions.values_mut() {
            queue.next_sequence = 0;
            queue.next_expected = 0;
        }
    }

    // ---- draining ---------------------------------------------------------------

    /// Fail queued records past `delivery_timeout_ms` with the text of
    /// Kafka's `Sender.failExpiredBatches`, and records that waited
    /// `max_block_ms` for their metadata with the text of Kafka's
    /// `waitOnMetadata`; both are Kafka's `TimeoutException`,
    /// `REQUEST_TIMED_OUT`.
    fn expire(&mut self, now: Millis, events: &mut Vec<ProducerEvent>) {
        let timeout = self.config.delivery_timeout_ms;
        let mut expired = Vec::new();
        for ((topic, partition), queue) in &mut self.partitions {
            while queue
                .batches
                .front()
                .is_some_and(|b| now >= b.created_at + timeout)
            {
                if let Some(batch) = queue.batches.pop_front() {
                    expired.push((topic.clone(), *partition, batch));
                }
            }
        }
        for (topic, partition, batch) in expired {
            let message = format!(
                "Expiring {} record(s) for {topic}-{partition}:{} ms has passed since batch creation. The request has not been sent, or no server response has been received yet.",
                batch.records.len(),
                now - batch.created_at,
            );
            self.fail_batch(
                &topic,
                partition,
                batch,
                (codes::REQUEST_TIMED_OUT, Some(&message)),
                events,
            );
        }
        let max_block = self.config.max_block_ms;
        for (topic, records) in &mut self.deferred {
            let (late, kept): (Vec<DeferredRecord>, Vec<DeferredRecord>) = std::mem::take(records)
                .into_iter()
                .partition(|d| now >= d.sent_at + max_block);
            *records = kept;
            let count = self
                .client
                .metadata()
                .partition_count(topic)
                .filter(|count| *count > 0);
            for deferred in late {
                self.metrics.failed += 1;
                let message = match (count, deferred.partition) {
                    (Some(count), Some(partition)) => format!(
                        "Partition {partition} of topic {topic} with partition count {count} is not present in metadata after {max_block} ms."
                    ),
                    _ => format!("Topic {topic} not present in metadata after {max_block} ms."),
                };
                events.push(ProducerEvent::Failed {
                    seq: deferred.seq,
                    topic: topic.clone(),
                    partition: deferred.partition.unwrap_or(-1),
                    code: codes::REQUEST_TIMED_OUT,
                    message: Some(message),
                });
            }
        }
        self.deferred.retain(|_, records| !records.is_empty());
    }

    /// Whether the producer may send batches: an idempotent producer waits
    /// for its producer id, as Kafka's `Sender.runOnce` sends no produce
    /// data while the `InitProducerId` of its `TransactionManager` is
    /// pending.
    fn may_send(&self) -> bool {
        match self.identity {
            ProducerId::Ready { .. } => true,
            ProducerId::Requested(_) => false,
            ProducerId::Absent => !self.config.idempotent(),
        }
    }

    /// Send every ready batch, one per partition per request, until nothing
    /// is ready. First ask for the metadata of the topics whose records wait
    /// for a leader, as Kafka's `Sender.sendProducerData` does before it
    /// drains (see [`Producer::look_up_unknown_leaders`]).
    fn drain(&mut self, ctx: &mut Ctx<'_>) {
        if self.closed || !self.may_send() {
            return;
        }
        self.look_up_unknown_leaders();
        while self.drain_once(ctx) {}
    }

    /// Ask for a metadata update while a partition with records has no
    /// leader. Kafka's `RecordAccumulator.ready` puts the topic of every
    /// partition that holds a batch and has no known leader, ready or not,
    /// among its `unknownLeaderTopics`, and `Sender.sendProducerData` adds
    /// each to the metadata (`ProducerMetadata.add`) and calls
    /// `Metadata.requestUpdate`. Every drain after an answer that still
    /// shows no leader asks again, so the lookups repeat at the client's
    /// metadata backoff until the leaders are back, whether or not the
    /// topic was tracked when the metadata last named it.
    fn look_up_unknown_leaders(&mut self) {
        let metadata = self.client.metadata();
        let unknown: BTreeSet<String> = self
            .partitions
            .iter()
            .filter(|((topic, partition), queue)| {
                !queue.batches.is_empty() && metadata.leader(topic, *partition).is_none()
            })
            .map(|((topic, _), _)| topic.clone())
            .collect();
        if unknown.is_empty() {
            return;
        }
        self.client.add_topics(unknown.iter().map(String::as_str));
        self.client.request_metadata_refresh();
    }

    fn drain_once(&mut self, ctx: &mut Ctx<'_>) -> bool {
        let now = ctx.now();
        let stamp = match self.identity {
            ProducerId::Ready { producer_id, epoch } => Some((producer_id, epoch)),
            ProducerId::Requested(_) => return false,
            ProducerId::Absent if self.config.idempotent() => return false,
            ProducerId::Absent => None,
        };
        let epoch = stamp.map(|(_, epoch)| epoch);
        let mut per_leader: BTreeMap<i32, Vec<(String, i32, ProducerBatch)>> = BTreeMap::new();
        for ((topic, partition), queue) in &mut self.partitions {
            let Some(head) = queue.batches.front() else {
                continue;
            };
            let is_retry = head.attempts > 0;
            let ready = head.closed
                || is_retry
                || head.size >= self.config.batch_size
                || now >= head.created_at + self.config.linger_ms
                || queue.batches.len() > 1;
            if !ready || now < head.retry_at || queue.in_flight >= self.config.max_in_flight {
                continue;
            }
            // A retried batch goes alone, and after an epoch bump nothing
            // drains until the batches of the old epoch returned: Kafka's
            // `shouldStopDrainBatchesForPartition`, so the sequences reach
            // the broker in order.
            if queue.in_flight > 0 && (is_retry || queue.in_flight_epoch != epoch) {
                continue;
            }
            // A partition without a leader waits for the lookup `drain`
            // asked for.
            let Some(leader) = self.client.metadata().leader(topic, *partition) else {
                continue;
            };
            let Some(mut batch) = queue.batches.pop_front() else {
                continue;
            };
            if stamp.is_some() && (batch.base_sequence.is_none() || batch.epoch != epoch) {
                batch.base_sequence = Some(queue.next_sequence);
                batch.epoch = epoch;
                queue.next_sequence = increment_sequence(queue.next_sequence, batch.count());
            }
            queue.in_flight += 1;
            queue.in_flight_epoch = epoch;
            per_leader
                .entry(leader)
                .or_default()
                .push((topic.clone(), *partition, batch));
        }
        let sent = !per_leader.is_empty();
        for (leader, batches) in per_leader {
            self.send_produce(ctx, leader, batches, stamp);
        }
        sent
    }

    fn send_produce(
        &mut self,
        ctx: &mut Ctx<'_>,
        leader: i32,
        batches: Vec<(String, i32, ProducerBatch)>,
        stamp: Option<(i64, i16)>,
    ) {
        let now = ctx.now();
        let mut topic_data: BTreeMap<String, Vec<PartitionProduceData>> = BTreeMap::new();
        for (topic, partition, batch) in &batches {
            let records: Vec<BatchRecord> =
                batch.records.iter().map(|r| r.record.clone()).collect();
            let producer_stamp = stamp.zip(batch.base_sequence).map(
                |((producer_id, producer_epoch), base_sequence)| ProducerStamp {
                    producer_id,
                    producer_epoch,
                    base_sequence,
                },
            );
            let record_batch = build_batch(
                &records,
                producer_stamp,
                self.config.compression.attribute_bits(),
            );
            self.metrics.bytes += u64::try_from(record_batch.encoded_len()).unwrap_or(u64::MAX);
            self.metrics.batches_sent += 1;
            topic_data
                .entry(topic.clone())
                .or_default()
                .push(PartitionProduceData {
                    index: *partition,
                    records: Some(RecordsPayload::V2(vec![record_batch])),
                    ..Default::default()
                });
        }
        let request = ProduceRequest {
            transactional_id: None,
            acks: self.config.acks.as_wire(),
            timeout_ms: i32::try_from(self.config.request_timeout_ms).unwrap_or(i32::MAX),
            topic_data: topic_data
                .into_iter()
                .map(|(name, partition_data)| TopicProduceData {
                    topic_id: self.client.metadata().topic_id(&name).unwrap_or(Uuid::ZERO),
                    name,
                    partition_data,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        if self.config.acks == Acks::None {
            self.client
                .send_oneway(ctx, Target::Broker(leader), request);
            let mut events = Vec::new();
            for (topic, partition, batch) in batches {
                if let Some(queue) = self.partitions.get_mut(&(topic.clone(), partition)) {
                    queue.in_flight = queue.in_flight.saturating_sub(1);
                }
                self.ack_batch(&topic, partition, batch, -1, now, &mut events);
            }
            self.stashed.extend(events);
            return;
        }
        let id = self.client.send(ctx, Target::Broker(leader), request);
        self.in_flight.insert(id, InFlightProduce { batches });
    }

    // ---- responses --------------------------------------------------------------

    fn on_produce_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        in_flight: InFlightProduce,
        result: Result<super::Response, ClientError>,
        events: &mut Vec<ProducerEvent>,
    ) {
        let response = match result {
            Ok(response) => response.downcast::<ProduceResponse>(),
            Err(error) => {
                let code = match error {
                    ClientError::Timeout { .. } | ClientError::Disconnected { .. } => {
                        codes::NETWORK_EXCEPTION
                    }
                    ClientError::UnsupportedVersion { .. } => codes::UNSUPPORTED_VERSION,
                    ClientError::Closed => codes::UNKNOWN_SERVER_ERROR,
                    ClientError::Protocol(_)
                    | ClientError::CorrelationMismatch { .. }
                    | ClientError::Broker { .. } => codes::CORRUPT_MESSAGE,
                };
                for (topic, partition, batch) in in_flight.batches {
                    self.settle(ctx, &(topic, partition), batch, Answer::error(code), events);
                }
                return;
            }
        };
        for (topic, partition, batch) in in_flight.batches {
            let answer = response.as_ref().and_then(|r| {
                let topic_id = self.client.metadata().topic_id(&topic);
                r.responses
                    .iter()
                    .find(|t| {
                        t.name == topic
                            || topic_id.is_some_and(|id| id == t.topic_id && id != Uuid::ZERO)
                    })
                    .and_then(|t| t.partition_responses.iter().find(|p| p.index == partition))
                    .map(|p| Answer {
                        code: p.error_code,
                        base_offset: p.base_offset,
                        log_start_offset: p.log_start_offset,
                        leader: (p.current_leader.leader_id, p.current_leader.leader_epoch),
                    })
            });
            // No row for the partition: the broker did not understand the
            // request; Kafka resends after a metadata refresh.
            let answer = answer.unwrap_or(Answer::error(codes::UNKNOWN_TOPIC_OR_PARTITION));
            self.settle(ctx, &(topic, partition), batch, answer, events);
        }
    }

    /// Apply the broker's answer for one batch: acknowledge, retry or fail,
    /// with Kafka's `Sender.completeBatch` and `TransactionManager.canRetry`
    /// rules.
    fn settle(
        &mut self,
        ctx: &mut Ctx<'_>,
        key: &(String, i32),
        batch: ProducerBatch,
        answer: Answer,
        events: &mut Vec<ProducerEvent>,
    ) {
        let now = ctx.now();
        let (topic, partition) = (key.0.as_str(), key.1);
        let expected = self.partitions.get_mut(key).map_or(0, |queue| {
            queue.in_flight = queue.in_flight.saturating_sub(1);
            queue.next_expected
        });
        let idempotent = batch.base_sequence.is_some();
        // The answer to a batch of an earlier epoch says nothing about the
        // sequences of the current one.
        let current = idempotent && batch.epoch == self.current_epoch();
        let code = answer.code;
        match code {
            codes::NONE | DUPLICATE_SEQUENCE_NUMBER => {
                self.ack_batch(topic, partition, batch, answer.base_offset, now, events);
            }
            // Not the next sequence: an earlier batch failed or is still
            // retrying, and this one goes again behind it. The next sequence:
            // there is a real gap, so a new epoch starts the sequences again
            // (KIP-360).
            codes::OUT_OF_ORDER_SEQUENCE_NUMBER if idempotent => {
                if current && batch.base_sequence == Some(expected) {
                    self.bump_epoch(now);
                }
                self.retry_or_fail(ctx, key, batch, code, events);
            }
            // The broker lost the producer state. Without a log start offset
            // the answer is incomplete and the batch goes again as it is.
            UNKNOWN_PRODUCER_ID if idempotent => {
                if current && answer.log_start_offset >= 0 {
                    self.bump_epoch(now);
                }
                self.retry_or_fail(ctx, key, batch, code, events);
            }
            codes::INVALID_PRODUCER_EPOCH
            | codes::PRODUCER_FENCED
            | codes::INVALID_PRODUCER_ID_MAPPING => {
                self.fail_batch(topic, partition, batch, (code, None), events);
                self.identity = ProducerId::Absent;
                self.identity_retry_at = now + self.config.retry_backoff_ms;
                self.reset_sequences();
            }
            _ => match retry::class(code) {
                ErrorClass::InvalidMetadata => {
                    // Kafka adopts the leader the answer names at once
                    // (KIP-951), and still refreshes the metadata.
                    let (leader, leader_epoch) = answer.leader;
                    self.client
                        .update_leader(topic, partition, leader, leader_epoch);
                    let target = Target::Leader {
                        topic: topic.to_string(),
                        partition,
                    };
                    self.client.note_error(code, &target);
                    self.retry_or_fail(ctx, key, batch, code, events);
                }
                ErrorClass::Retriable => self.retry_or_fail(ctx, key, batch, code, events),
                ErrorClass::None | ErrorClass::NotRetriable => {
                    self.fail_batch(topic, partition, batch, (code, None), events);
                    // Kafka's `handleFailedBatch` bumps the epoch of an
                    // idempotent producer, so the sequences after the failed
                    // batch stay valid.
                    if current {
                        self.bump_epoch(now);
                    }
                }
            },
        }
    }

    fn retry_or_fail(
        &mut self,
        ctx: &mut Ctx<'_>,
        key: &(String, i32),
        mut batch: ProducerBatch,
        code: i16,
        events: &mut Vec<ProducerEvent>,
    ) {
        let now = ctx.now();
        let expired = now >= batch.created_at + self.config.delivery_timeout_ms;
        if batch.attempts >= self.config.retries || expired {
            self.fail_batch(&key.0, key.1, batch, (code, None), events);
            return;
        }
        batch.retry_at = now
            + retry::exponential_backoff(
                self.config.retry_backoff_ms,
                RETRY_BACKOFF_MAX_MS,
                batch.attempts,
                ctx.rand(400),
            );
        batch.attempts += 1;
        self.metrics.retried += 1;
        let queue = self.partitions.entry(key.clone()).or_default();
        // A retried batch goes back to its place, ahead of the batches
        // created after it, as Kafka's `RecordAccumulator.reenqueue` puts it
        // at the head of the deque.
        queue.batches.push_front(batch);
        queue.batches.make_contiguous().sort_by_key(|b| b.id);
    }

    fn ack_batch(
        &mut self,
        topic: &str,
        partition: i32,
        batch: ProducerBatch,
        base_offset: i64,
        now: Millis,
        events: &mut Vec<ProducerEvent>,
    ) {
        let current = batch.epoch.is_some() && batch.epoch == self.current_epoch();
        if let (true, Some(queue), Some(base)) = (
            current,
            self.partitions.get_mut(&(topic.to_string(), partition)),
            batch.base_sequence,
        ) {
            queue.next_expected = increment_sequence(base, batch.count());
        }
        let count = batch.records.len();
        for (i, record) in batch.records.into_iter().enumerate() {
            let offset = if base_offset >= 0 {
                base_offset + i64::try_from(i).unwrap_or(0)
            } else {
                -1
            };
            let latency_ms = now.saturating_sub(record.sent_at);
            self.metrics.rtt.record(latency_ms);
            events.push(ProducerEvent::Acked {
                seq: record.seq,
                topic: topic.to_string(),
                partition,
                offset,
                latency_ms,
            });
        }
        self.metrics.acked += u64::try_from(count).unwrap_or(u64::MAX);
        if base_offset >= 0 {
            let last = base_offset + i64::try_from(count.saturating_sub(1)).unwrap_or(0);
            self.metrics
                .last_offsets
                .insert((topic.to_string(), partition), last);
        }
    }

    /// Fail every record of `batch` with `failure`: the error code and, for
    /// a failure the producer decided itself, Kafka's exception text.
    fn fail_batch(
        &mut self,
        topic: &str,
        partition: i32,
        batch: ProducerBatch,
        failure: (i16, Option<&str>),
        events: &mut Vec<ProducerEvent>,
    ) {
        let (code, message) = failure;
        self.metrics.failed += u64::try_from(batch.records.len()).unwrap_or(u64::MAX);
        for record in batch.records {
            events.push(ProducerEvent::Failed {
                seq: record.seq,
                topic: topic.to_string(),
                partition,
                code,
                message: message.map(str::to_string),
            });
        }
    }

    /// The producer for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let (producer_id, epoch) = self.producer_id().unwrap_or((-1, -1));
        let queues: Vec<Value> = self
            .partitions
            .iter()
            .map(|((topic, partition), q)| {
                json!({
                    "topic": topic,
                    "partition": partition,
                    "batches": q.batches.len(),
                    "records": q.batches.iter().map(|b| b.records.len()).sum::<usize>(),
                    "in_flight": q.in_flight,
                    "next_sequence": q.next_sequence,
                    "last_offset": self.metrics.last_offsets.get(&(topic.clone(), *partition)),
                })
            })
            .collect();
        let in_flight_batches: usize = self.in_flight.values().map(|r| r.batches.len()).sum();
        json!({
            "acks": self.config.acks.as_wire(),
            "idempotent": self.config.idempotent(),
            "compression": self.config.compression.name(),
            "producer_id": producer_id,
            "producer_epoch": epoch,
            "sent": self.metrics.sent,
            "acked": self.metrics.acked,
            "failed": self.metrics.failed,
            "retried": self.metrics.retried,
            "bytes": self.metrics.bytes,
            "batches_sent": self.metrics.batches_sent,
            "in_flight_batches": in_flight_batches,
            "in_flight_requests": self.in_flight.len(),
            "pending_records": self.pending_records(),
            "deferred_topics": self.deferred.keys().collect::<Vec<_>>(),
            "partitions": queues,
            "rtt": self.metrics.rtt.snapshot(),
            "client": self.client.snapshot(),
        })
    }
}

/// The wire size of a record, as the batch would carry it.
fn record_size(record: &BatchRecord) -> usize {
    krabka_protocol::records::Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: record.key.clone(),
        value: record.value.clone(),
        headers: record.headers.clone(),
    }
    .encoded_len()
}
