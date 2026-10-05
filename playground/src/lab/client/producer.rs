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
//!
//! # Transactions
//!
//! With a `transactional_id` the producer is Kafka's transactional producer
//! with transaction version 1, as Kafka's `TransactionManager` runs it when
//! the cluster does not enable KIP-890's version 2: `InitProducerId` with the
//! transactional id goes to the transaction coordinator (`FindCoordinator`
//! with key type 1); the first record after a transaction ended opens the
//! next one (Kafka's `beginTransaction`); each partition joins the
//! transaction with `AddPartitionsToTxn` (v3 at most, Kafka's
//! `forClient` range) before its first batch drains; batches carry the
//! transactional attribute and go out in `Produce` at v11 at most;
//! [`Producer::send_offsets_to_transaction`] sends `AddOffsetsToTxn` and then
//! `TxnOffsetCommit` to the group coordinator (KIP-447); and
//! [`Producer::commit_transaction`] waits until every record of the
//! transaction is acknowledged, then sends `EndTxn`, as
//! [`Producer::abort_transaction`] does after failing the records not sent
//! yet. One transaction request is in flight at a time. A coordinator that
//! moved, loads, or still completes the last transaction is asked again
//! after `retry.backoff.ms`; a batch or a request that fails for good makes
//! the transaction abortable only (Kafka's `ABORTABLE_ERROR`: the records
//! not sent yet fail, and the next transaction starts under a new epoch
//! when a sequence was lost); `PRODUCER_FENCED`, `INVALID_PRODUCER_EPOCH`
//! and the other fatal codes stop the producer for good (`FATAL_ERROR`).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::{BufMut, Bytes};
use derive_more::{Display, From, Into};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        add_offsets_to_txn_request::AddOffsetsToTxnRequest,
        add_offsets_to_txn_response::AddOffsetsToTxnResponse,
        add_partitions_to_txn_request::{self, AddPartitionsToTxnRequest},
        add_partitions_to_txn_response::AddPartitionsToTxnResponse,
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        end_txn_request::{self, EndTxnRequest},
        end_txn_response::EndTxnResponse,
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        produce_request::{self, PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        txn_offset_commit_request::{
            self, TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition,
            TxnOffsetCommitRequestTopic,
        },
        txn_offset_commit_response::TxnOffsetCommitResponse,
    },
    primitives::uuid::Uuid,
    records::{RecordHeader, RecordsPayload, increment_sequence},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    ClientError, ClientEvent, CoordinatorType, KafkaClient, RequestId, Target,
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
/// `OPERATION_NOT_ATTEMPTED`: another partition of the same
/// `AddPartitionsToTxn` failed.
const OPERATION_NOT_ATTEMPTED: i16 = 55;
/// The text a record not sent yet fails with when its transaction aborts:
/// Kafka's `TransactionAbortedException`.
const TRANSACTION_ABORTED: &str = "Failing batch since transaction was aborted";

/// A request type sent at most at version `$max`: the last version before
/// KIP-890's transaction version 2, which Kafka's transactional producer
/// caps at while the cluster runs version 1.
macro_rules! capped_request {
    ($(#[$doc:meta])* $name:ident($inner:ty), $module:ident, $response:ty, $max:expr) => {
        $(#[$doc])*
        pub struct $name(pub $inner);

        impl Encode for $name {
            fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
                self.0.encode(buf, version)
            }

            fn encoded_len(&self, version: i16) -> usize {
                self.0.encoded_len(version)
            }
        }

        impl ProtocolRequest for $name {
            const API_KEY: i16 = $module::API_KEY;
            const MIN_VERSION: i16 = $module::MIN_VERSION;
            const MAX_VERSION: i16 = $max;
            const LATEST_STABLE_VERSION: i16 = $max;
            const FLEXIBLE_MIN: i16 = $module::FLEXIBLE_MIN;
            type Response = $response;
        }
    };
}

capped_request!(
    /// A transactional `Produce`: v11 at most (Kafka's
    /// `LAST_BEFORE_TRANSACTION_V2_VERSION`), so the leader expects the
    /// partition added by `AddPartitionsToTxn`.
    TxnProduce(ProduceRequest), produce_request, ProduceResponse, 11
);
capped_request!(
    /// `AddPartitionsToTxn` in the client form: v3 at most, as Kafka's
    /// `AddPartitionsToTxnRequest.Builder.forClient` sends it.
    ClientAddPartitionsToTxn(AddPartitionsToTxnRequest), add_partitions_to_txn_request,
    AddPartitionsToTxnResponse, 3
);
capped_request!(
    /// `EndTxn` v4 at most: v5 bumps the epoch at every end (KIP-890).
    TxnEnd(EndTxnRequest), end_txn_request, EndTxnResponse, 4
);
capped_request!(
    /// `TxnOffsetCommit` v4 at most: v5 is transaction version 2.
    TxnCommitOffsets(TxnOffsetCommitRequest), txn_offset_commit_request,
    TxnOffsetCommitResponse, 4
);

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
    Lz4,
    Zstd,
}

impl Compression {
    /// The compression bits of the batch attributes.
    #[must_use]
    pub const fn attribute_bits(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Snappy => 2,
            Self::Lz4 => 3,
            Self::Zstd => 4,
        }
    }

    /// The `compression.type` name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    /// The compression a `compression.type` value names.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "gzip" => Some(Self::Gzip),
            "snappy" => Some(Self::Snappy),
            "lz4" => Some(Self::Lz4),
            "zstd" => Some(Self::Zstd),
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
    /// `transactional.id`: the producer is transactional (see the module
    /// documentation). Default: none.
    pub transactional_id: Option<String>,
    /// `transaction.timeout.ms`, sent with `InitProducerId`. Default: 60 000.
    pub transaction_timeout_ms: Millis,
}

impl ProducerConfig {
    /// Whether the producer is idempotent. Kafka enables idempotence only
    /// with `acks=all`, `retries > 0` and at most 5 requests in flight; with
    /// a conflicting setting the default turns it off, and the lab does the
    /// same whatever `enable_idempotence` says. A transactional producer is
    /// idempotent under the same conditions; the nodes refuse a
    /// `transactional_id` with a setting that conflicts, as Kafka's config
    /// check does.
    #[must_use]
    pub fn idempotent(&self) -> bool {
        (self.enable_idempotence || self.transactional_id.is_some())
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
            transactional_id: None,
            transaction_timeout_ms: 60_000,
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
    /// The transaction ended: committed, or aborted.
    TransactionEnded { committed: bool },
    /// A request or a batch of the transaction failed with `code`: a fatal
    /// error stops the producer for good, any other leaves the transaction
    /// able only to abort.
    TransactionError { code: i16, fatal: bool },
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

/// Where a transactional producer stands once it has a producer id: Kafka's
/// `TransactionManager.State`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TxnState {
    Ready,
    InTransaction,
    Committing,
    Aborting,
    /// A request or a batch of the open transaction failed: it can only
    /// abort.
    AbortableError,
    /// Fenced or refused for good: the producer sends nothing more.
    FatalError,
}

impl TxnState {
    /// Kafka's name of the state, lower case.
    const fn name(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::InTransaction => "in_transaction",
            Self::Committing => "committing_transaction",
            Self::Aborting => "aborting_transaction",
            Self::AbortableError => "abortable_error",
            Self::FatalError => "fatal_error",
        }
    }
}

/// The transaction request in flight.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TxnCall {
    AddPartitions,
    AddOffsets,
    CommitOffsets,
    End,
}

/// The group position a transaction commits with its records (KIP-447):
/// Kafka's `ConsumerGroupMetadata`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GroupMetadata {
    pub group_id: String,
    pub member_id: String,
    /// The classic generation, or the member epoch of a KIP-848 or KIP-1071
    /// group.
    pub generation: i32,
}

/// Offsets [`Producer::send_offsets_to_transaction`] took.
struct TxnOffsets {
    group: GroupMetadata,
    offsets: Vec<(String, i32, i64)>,
    /// `AddOffsetsToTxn` answered; `TxnOffsetCommit` is next.
    added: bool,
}

/// The transaction manager of a transactional producer.
struct Transactions {
    id: String,
    state: TxnState,
    /// The partitions the coordinator added to the open transaction.
    added: BTreeSet<(String, i32)>,
    /// The partitions the `AddPartitionsToTxn` in flight asks for.
    adding: Vec<(String, i32)>,
    offsets: Option<TxnOffsets>,
    in_flight: Option<(RequestId, TxnCall)>,
    /// A request asked again waits until then.
    retry_at: Millis,
    /// The open transaction reached the coordinator, so it ends with
    /// `EndTxn`; one that added nothing ends on the client.
    started: bool,
    /// A batch with a sequence failed, so the next transaction needs a new
    /// epoch (KIP-360).
    bump: bool,
    committed: u64,
    aborted: u64,
    last_error: Option<i16>,
}

/// How a transaction request's error code is handled, as Kafka's
/// `TransactionManager` handlers sort them.
enum TxnFailure {
    /// Ask again after the backoff, looking the coordinator up again when
    /// the flag says it moved.
    Retry(bool),
    Abortable,
    Fatal,
}

fn txn_failure(code: i16) -> TxnFailure {
    match code {
        codes::NOT_COORDINATOR | codes::COORDINATOR_NOT_AVAILABLE | codes::NETWORK_EXCEPTION => {
            TxnFailure::Retry(true)
        }
        codes::COORDINATOR_LOAD_IN_PROGRESS
        | codes::CONCURRENT_TRANSACTIONS
        | codes::UNKNOWN_TOPIC_OR_PARTITION
        | codes::REQUEST_TIMED_OUT
        | OPERATION_NOT_ATTEMPTED => TxnFailure::Retry(false),
        codes::PRODUCER_FENCED
        | codes::INVALID_PRODUCER_EPOCH
        | codes::INVALID_PRODUCER_ID_MAPPING
        | codes::INVALID_TXN_STATE
        | codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        | codes::TRANSACTION_COORDINATOR_FENCED
        | codes::UNSUPPORTED_VERSION => TxnFailure::Fatal,
        _ => TxnFailure::Abortable,
    }
}

/// The error code a request that got no answer stands for.
fn client_error_code(error: &ClientError) -> i16 {
    match error {
        ClientError::Timeout { .. } | ClientError::Disconnected { .. } => codes::NETWORK_EXCEPTION,
        ClientError::UnsupportedVersion { .. } => codes::UNSUPPORTED_VERSION,
        ClientError::Closed => codes::UNKNOWN_SERVER_ERROR,
        ClientError::Protocol(_)
        | ClientError::CorrelationMismatch { .. }
        | ClientError::Broker { .. } => codes::CORRUPT_MESSAGE,
    }
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
    /// The transaction manager, with a `transactional_id`.
    txn: Option<Transactions>,
}

impl Producer {
    /// A producer over `client`. `seed` drives the sticky partitioner's
    /// random choice.
    #[must_use]
    pub fn new(client: KafkaClient, config: ProducerConfig, seed: u64) -> Self {
        let txn = config.transactional_id.clone().map(|id| Transactions {
            id,
            state: TxnState::Ready,
            added: BTreeSet::new(),
            adding: Vec::new(),
            offsets: None,
            in_flight: None,
            retry_at: 0,
            started: false,
            bump: false,
            committed: 0,
            aborted: 0,
            last_error: None,
        });
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
            txn,
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
        // A transactional record opens the next transaction (Kafka's
        // `beginTransaction`), and is refused while one ends or after an
        // error, as `TransactionManager.maybeAddPartition` refuses it.
        if let Some(txn) = &mut self.txn {
            match txn.state {
                TxnState::Ready => txn.state = TxnState::InTransaction,
                TxnState::InTransaction => {}
                state => {
                    self.metrics.failed += 1;
                    self.stashed.push(ProducerEvent::Failed {
                        seq,
                        topic,
                        partition: partition.unwrap_or(-1),
                        code: codes::INVALID_TXN_STATE,
                        message: Some(format!(
                            "Cannot call send in state {}",
                            state.name().to_uppercase()
                        )),
                    });
                    return seq;
                }
            }
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
        self.step_transaction(ctx);
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
        self.step_transaction(ctx);
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
        // A batch of a transactional producer waits for its partition to
        // join the transaction, whose answer arrives as a frame.
        let in_txn = |key: &(String, i32)| self.txn.as_ref().is_none_or(|t| t.added.contains(key));
        let batches = self
            .partitions
            .iter()
            .filter(|(key, _)| can_drain && in_txn(key))
            .filter_map(|((topic, partition), queue)| {
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
            });
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
        let txn = self.txn_due().and(self.txn.as_ref()).map(|t| t.retry_at);
        client
            .into_iter()
            .chain(batches)
            .chain(expiries)
            .chain(producer_id)
            .chain(stashed)
            .chain(txn)
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
                    let txn_call = self
                        .txn
                        .as_ref()
                        .and_then(|t| t.in_flight)
                        .filter(|(sent, _)| *sent == id);
                    if let Some(in_flight) = self.in_flight.remove(&id) {
                        self.on_produce_response(ctx, in_flight, result, events);
                    } else if self.identity == ProducerId::Requested(id) {
                        self.on_init_producer_id(ctx.now(), result, events);
                    } else if let Some((_, call)) = txn_call {
                        self.on_txn_response(ctx, call, result);
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
            || self.txn_state() == Some(TxnState::FatalError)
        {
            return;
        }
        // A transactional producer asks its transaction coordinator, which
        // fences the earlier epochs of the transactional id and aborts the
        // transaction they left open (Kafka's `initTransactions`).
        let (target, timeout) = match self.txn_target() {
            Some(target) => (
                target,
                i32::try_from(self.config.transaction_timeout_ms).unwrap_or(i32::MAX),
            ),
            None => (Target::Any, IDEMPOTENT_TRANSACTION_TIMEOUT),
        };
        let request = InitProducerIdRequest {
            transactional_id: self.config.transactional_id.clone(),
            transaction_timeout_ms: timeout,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        };
        let id = self.client.send(ctx, target, request);
        self.identity = ProducerId::Requested(id);
    }

    /// The transaction coordinator of a transactional producer.
    fn txn_target(&self) -> Option<Target> {
        self.txn.as_ref().map(|t| Target::Coordinator {
            key_type: CoordinatorType::Transaction,
            key: t.id.clone(),
        })
    }

    fn txn_state(&self) -> Option<TxnState> {
        self.txn.as_ref().map(|t| t.state)
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
        if let Some(target) = self.txn_target() {
            let code = response
                .as_ref()
                .map_or(codes::NETWORK_EXCEPTION, |r| r.error_code);
            match txn_failure(code) {
                _ if code == codes::NONE => {}
                TxnFailure::Fatal => {
                    self.identity = ProducerId::Absent;
                    self.txn_fatal(code, events);
                    return;
                }
                TxnFailure::Retry(_) | TxnFailure::Abortable => {
                    self.forget_coordinator(code, &target);
                }
            }
        }
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

    // ---- transactions -----------------------------------------------------------

    /// Whether a transactional producer can take records into a transaction
    /// now: it has its producer id and no transaction is ending or failed.
    /// Always true without a `transactional_id`.
    #[must_use]
    pub fn transaction_ready(&self) -> bool {
        match self.txn_state() {
            None => true,
            Some(state) => {
                self.producer_id().is_some()
                    && matches!(state, TxnState::Ready | TxnState::InTransaction)
            }
        }
    }

    /// Whether a transaction is open: records or offsets joined it and it
    /// has not ended.
    #[must_use]
    pub fn transaction_open(&self) -> bool {
        !matches!(
            self.txn_state(),
            None | Some(TxnState::Ready | TxnState::FatalError)
        )
    }

    /// Whether the producer stopped for good on a fatal transaction error.
    #[must_use]
    pub fn transaction_fatal(&self) -> bool {
        self.txn_state() == Some(TxnState::FatalError)
    }

    /// Kafka's name of the transaction state, lower case (`ready`,
    /// `in_transaction`, `abortable_error`, ...), or `None` without a
    /// `transactional_id`.
    #[must_use]
    pub fn transaction_state(&self) -> Option<&'static str> {
        self.txn_state().map(TxnState::name)
    }

    /// Commit the open transaction: once every record of it is acknowledged
    /// and its offsets are committed, `EndTxn` commits it, and
    /// [`ProducerEvent::TransactionEnded`] reports it.
    ///
    /// # Errors
    /// Kafka's text for a producer that is not transactional, has no open
    /// transaction, or must abort.
    pub fn commit_transaction(&mut self) -> Result<(), String> {
        let txn = self.txn.as_mut().ok_or_else(not_transactional)?;
        match txn.state {
            TxnState::InTransaction => {
                txn.state = TxnState::Committing;
                Ok(())
            }
            state => Err(invalid_transition(state, TxnState::Committing)),
        }
    }

    /// Abort the open transaction: the records not sent yet fail with
    /// Kafka's `TransactionAbortedException` text, and once the batches in
    /// flight returned `EndTxn` aborts it.
    ///
    /// # Errors
    /// Kafka's text for a producer that is not transactional, has no open
    /// transaction, or was fenced.
    pub fn abort_transaction(&mut self) -> Result<(), String> {
        let txn = self.txn.as_mut().ok_or_else(not_transactional)?;
        match txn.state {
            TxnState::InTransaction | TxnState::AbortableError => {
                txn.state = TxnState::Aborting;
                txn.offsets = None;
                self.fail_unsent(codes::UNKNOWN_SERVER_ERROR, Some(TRANSACTION_ABORTED));
                Ok(())
            }
            state => Err(invalid_transition(state, TxnState::Aborting)),
        }
    }

    /// Commit `offsets`, as `(topic, partition, offset)`, for `group` with
    /// the open transaction (KIP-447): `AddOffsetsToTxn`, then
    /// `TxnOffsetCommit` to the group coordinator, before the commit.
    ///
    /// # Errors
    /// Kafka's text for a producer that is not transactional or cannot take
    /// part in a transaction now.
    pub fn send_offsets_to_transaction(
        &mut self,
        group: GroupMetadata,
        offsets: Vec<(String, i32, i64)>,
    ) -> Result<(), String> {
        let txn = self.txn.as_mut().ok_or_else(not_transactional)?;
        match txn.state {
            TxnState::Ready | TxnState::InTransaction => {
                txn.state = TxnState::InTransaction;
                txn.offsets = Some(TxnOffsets {
                    group,
                    offsets,
                    added: false,
                });
                Ok(())
            }
            state => Err(format!(
                "Cannot send offsets in state {}",
                state.name().to_uppercase()
            )),
        }
    }

    /// The partitions with records queued that the open transaction does not
    /// hold yet.
    fn partitions_to_add(&self) -> Vec<(String, i32)> {
        let Some(txn) = &self.txn else {
            return Vec::new();
        };
        self.partitions
            .iter()
            .filter(|(key, queue)| !queue.batches.is_empty() && !txn.added.contains(*key))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// The transaction request due next, if one is and none is in flight:
    /// partitions to add, offsets to add and commit, then the end once the
    /// records of the transaction returned.
    fn txn_due(&self) -> Option<TxnCall> {
        let txn = self.txn.as_ref()?;
        self.producer_id()?;
        if txn.in_flight.is_some() {
            return None;
        }
        let ending = match txn.state {
            TxnState::InTransaction => false,
            TxnState::Committing => self.pending_records() == 0,
            TxnState::Aborting => self.in_flight.is_empty(),
            TxnState::Ready | TxnState::AbortableError | TxnState::FatalError => return None,
        };
        if txn.state != TxnState::Aborting && !self.partitions_to_add().is_empty() {
            return Some(TxnCall::AddPartitions);
        }
        match &txn.offsets {
            Some(offsets) if !offsets.added => Some(TxnCall::AddOffsets),
            Some(_) => Some(TxnCall::CommitOffsets),
            None => ending.then_some(TxnCall::End),
        }
    }

    /// Send the transaction request due, or end a transaction that never
    /// reached the coordinator on the client, as Kafka's
    /// `TransactionManager.beginCompletingTransaction` does when no
    /// partition was added.
    fn step_transaction(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let Some(call) = self.txn_due() else {
            return;
        };
        let (Some((producer_id, epoch)), Some(target)) = (self.producer_id(), self.txn_target())
        else {
            return;
        };
        let to_add = self.partitions_to_add();
        let Some(txn) = self.txn.as_mut() else {
            return;
        };
        if now < txn.retry_at {
            return;
        }
        let transactional_id = txn.id.clone();
        let id = match call {
            TxnCall::AddPartitions => {
                let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
                for (topic, partition) in &to_add {
                    topics.entry(topic.clone()).or_default().push(*partition);
                }
                txn.adding = to_add;
                let request = ClientAddPartitionsToTxn(AddPartitionsToTxnRequest {
                    v3_and_below_transactional_id: transactional_id,
                    v3_and_below_producer_id: producer_id,
                    v3_and_below_producer_epoch: epoch,
                    v3_and_below_topics: topics
                        .into_iter()
                        .map(|(name, partitions)| AddPartitionsToTxnTopic {
                            name,
                            partitions,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                });
                self.client.send(ctx, target, request)
            }
            TxnCall::CommitOffsets => {
                let Some(offsets) = &txn.offsets else {
                    return;
                };
                let group_target = Target::Coordinator {
                    key_type: CoordinatorType::Group,
                    key: offsets.group.group_id.clone(),
                };
                let request = txn_offset_commit(transactional_id, (producer_id, epoch), offsets);
                self.client.send(ctx, group_target, request)
            }
            TxnCall::AddOffsets => {
                let group_id = txn
                    .offsets
                    .as_ref()
                    .map(|o| o.group.group_id.clone())
                    .unwrap_or_default();
                let request = AddOffsetsToTxnRequest {
                    transactional_id,
                    producer_id,
                    producer_epoch: epoch,
                    group_id,
                    ..Default::default()
                };
                self.client.send(ctx, target, request)
            }
            TxnCall::End if !txn.started => {
                self.end_transaction(now);
                return;
            }
            TxnCall::End => {
                let request = TxnEnd(EndTxnRequest {
                    transactional_id,
                    producer_id,
                    producer_epoch: epoch,
                    committed: txn.state == TxnState::Committing,
                    ..Default::default()
                });
                self.client.send(ctx, target, request)
            }
        };
        if let Some(txn) = self.txn.as_mut() {
            txn.in_flight = Some((id, call));
        }
    }

    /// The transaction ended: count it, report it, and take a new epoch
    /// first when a sequence was lost.
    fn end_transaction(&mut self, now: Millis) {
        let Some(txn) = self.txn.as_mut() else {
            return;
        };
        let committed = txn.state == TxnState::Committing;
        if committed {
            txn.committed += 1;
        } else {
            txn.aborted += 1;
        }
        txn.state = TxnState::Ready;
        txn.added.clear();
        txn.offsets = None;
        txn.started = false;
        if std::mem::take(&mut txn.bump) {
            // KIP-360: a new `InitProducerId` gives the next transaction a
            // new epoch, so every partition starts again at sequence 0.
            self.identity = ProducerId::Absent;
            self.identity_retry_at = now;
        }
        self.stashed
            .push(ProducerEvent::TransactionEnded { committed });
    }

    fn on_txn_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        call: TxnCall,
        result: Result<super::Response, ClientError>,
    ) {
        let now = ctx.now();
        let Some(txn) = self.txn.as_mut() else {
            return;
        };
        txn.in_flight = None;
        let mut target = Target::Coordinator {
            key_type: CoordinatorType::Transaction,
            key: txn.id.clone(),
        };
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                if call == TxnCall::AddPartitions {
                    txn.adding.clear();
                }
                let code = client_error_code(&error);
                self.on_txn_error(now, code, &target);
                return;
            }
        };
        let code = match call {
            TxnCall::AddPartitions => {
                let adding = std::mem::take(&mut txn.adding);
                let r = response.downcast::<AddPartitionsToTxnResponse>();
                let mut worst = r.as_ref().map_or(codes::CORRUPT_MESSAGE, |r| r.error_code);
                for topic in r.iter().flat_map(|r| &r.results_by_topic_v3_and_below) {
                    for p in &topic.results_by_partition {
                        let key = (topic.name.clone(), p.partition_index);
                        let code = p.partition_error_code;
                        if code == codes::NONE && adding.contains(&key) {
                            txn.added.insert(key);
                            txn.started = true;
                        } else if worst == codes::NONE || worst == OPERATION_NOT_ATTEMPTED {
                            // `OPERATION_NOT_ATTEMPTED` marks the partitions
                            // another one's error held back.
                            worst = code;
                        }
                    }
                }
                worst
            }
            TxnCall::AddOffsets => {
                let code = response
                    .downcast::<AddOffsetsToTxnResponse>()
                    .map_or(codes::CORRUPT_MESSAGE, |r| r.error_code);
                if code == codes::NONE
                    && let Some(offsets) = txn.offsets.as_mut()
                {
                    offsets.added = true;
                    txn.started = true;
                }
                code
            }
            TxnCall::CommitOffsets => {
                if let Some(offsets) = &txn.offsets {
                    target = Target::Coordinator {
                        key_type: CoordinatorType::Group,
                        key: offsets.group.group_id.clone(),
                    };
                }
                let code = response.downcast::<TxnOffsetCommitResponse>().map_or(
                    codes::CORRUPT_MESSAGE,
                    |r| {
                        r.topics
                            .iter()
                            .flat_map(|t| &t.partitions)
                            .map(|p| p.error_code)
                            .find(|code| *code != codes::NONE)
                            .unwrap_or(codes::NONE)
                    },
                );
                if code == codes::NONE {
                    txn.offsets = None;
                }
                code
            }
            TxnCall::End => response
                .downcast::<EndTxnResponse>()
                .map_or(codes::CORRUPT_MESSAGE, |r| r.error_code),
        };
        match code {
            codes::NONE if call == TxnCall::End => self.end_transaction(now),
            codes::NONE => {}
            code => self.on_txn_error(now, code, &target),
        }
    }

    /// A transaction request failed with `code`: ask again after the
    /// backoff, or make the transaction abortable, or stop for good.
    fn on_txn_error(&mut self, now: Millis, code: i16, target: &Target) {
        let mut events = Vec::new();
        match txn_failure(code) {
            TxnFailure::Retry(lookup) => {
                if let Some(txn) = self.txn.as_mut() {
                    txn.last_error = Some(code);
                    txn.retry_at = now + self.config.retry_backoff_ms;
                }
                if lookup {
                    self.forget_coordinator(code, target);
                }
            }
            TxnFailure::Abortable => self.txn_abortable(code, &mut events),
            TxnFailure::Fatal => self.txn_fatal(code, &mut events),
        }
        self.stashed.extend(events);
    }

    /// Forget a coordinator that moved or whose connection failed, so the
    /// next request looks it up again.
    fn forget_coordinator(&mut self, code: i16, target: &Target) {
        if code == codes::NETWORK_EXCEPTION {
            if let Target::Coordinator { key_type, key } = target {
                self.client.invalidate_coordinator(*key_type, key);
            }
        } else {
            self.client.note_error(code, target);
        }
    }

    /// The open transaction can only abort now: Kafka's
    /// `transitionToAbortableError`. The records not sent yet fail with
    /// `code`, as Kafka's `Sender` aborts the undrained batches.
    fn txn_abortable(&mut self, code: i16, events: &mut Vec<ProducerEvent>) {
        let Some(txn) = self.txn.as_mut() else {
            return;
        };
        txn.last_error = Some(code);
        if !matches!(txn.state, TxnState::InTransaction | TxnState::Committing) {
            return;
        }
        txn.state = TxnState::AbortableError;
        events.push(ProducerEvent::TransactionError { code, fatal: false });
        self.fail_unsent(code, None);
    }

    /// The producer stops for good: Kafka's `transitionToFatalError`. The
    /// records not sent yet fail with `code`.
    fn txn_fatal(&mut self, code: i16, events: &mut Vec<ProducerEvent>) {
        let Some(txn) = self.txn.as_mut() else {
            return;
        };
        txn.last_error = Some(code);
        if txn.state == TxnState::FatalError {
            return;
        }
        txn.state = TxnState::FatalError;
        events.push(ProducerEvent::TransactionError { code, fatal: true });
        self.fail_unsent(code, None);
    }

    /// Fail every record not sent yet: those waiting for metadata and the
    /// queued batches. The events go out with the next tick or frame.
    fn fail_unsent(&mut self, code: i16, message: Option<&str>) {
        let mut events = Vec::new();
        for (topic, records) in std::mem::take(&mut self.deferred) {
            for deferred in records {
                self.metrics.failed += 1;
                events.push(ProducerEvent::Failed {
                    seq: deferred.seq,
                    topic: topic.clone(),
                    partition: deferred.partition.unwrap_or(-1),
                    code,
                    message: message.map(str::to_string),
                });
            }
        }
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
            self.fail_batch(&topic, partition, batch, (code, message), &mut events);
        }
        self.stashed.extend(events);
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
            // asked for; a transactional one for `AddPartitionsToTxn`.
            let Some(leader) = self.client.metadata().leader(topic, *partition) else {
                continue;
            };
            let key = (topic.clone(), *partition);
            if self.txn.as_ref().is_some_and(|t| !t.added.contains(&key)) {
                continue;
            }
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
            let mut record_batch = build_batch(
                &records,
                producer_stamp,
                self.config.compression.attribute_bits(),
            );
            if self.txn.is_some() {
                record_batch.attributes = record_batch.attributes.with_transactional(true);
            }
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
            transactional_id: self.txn.as_ref().map(|t| t.id.clone()),
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
        let id = if self.txn.is_some() {
            self.client
                .send(ctx, Target::Broker(leader), TxnProduce(request))
        } else {
            self.client.send(ctx, Target::Broker(leader), request)
        };
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
                let code = client_error_code(&error);
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
            // A transactional producer cannot raise its own epoch: the
            // batch fails, and the transaction aborts under a new one.
            codes::OUT_OF_ORDER_SEQUENCE_NUMBER
                if current && self.txn.is_some() && batch.base_sequence == Some(expected) =>
            {
                self.fail_batch(topic, partition, batch, (code, None), events);
            }
            UNKNOWN_PRODUCER_ID if idempotent && self.txn.is_some() => {
                self.fail_batch(topic, partition, batch, (code, None), events);
            }
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
                if self.txn.is_some() {
                    self.txn_fatal(code, events);
                    return;
                }
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
                    // batch stay valid; a transactional one aborts first.
                    if current && self.txn.is_none() {
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
                .entry((topic.to_string(), partition))
                .and_modify(|at| *at = (*at).max(last))
                .or_insert(last);
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
        // A failed batch of a transaction makes it abortable; one that held
        // a sequence leaves a gap only a new epoch closes (KIP-360).
        if let Some(txn) = self.txn.as_mut() {
            txn.bump |= batch.base_sequence.is_some();
            self.txn_abortable(code, events);
        }
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
        let acked_upto: serde_json::Map<String, Value> = self
            .metrics
            .last_offsets
            .iter()
            .map(|((topic, partition), offset)| (format!("{topic}-{partition}"), json!(offset)))
            .collect();
        let transactions = self.txn.as_ref().map_or(Value::Null, |t| {
            let state = if self.producer_id().is_some() || t.state == TxnState::FatalError {
                t.state.name()
            } else {
                "initializing"
            };
            json!({
                "transactional_id": t.id,
                "state": state,
                "open": self.transaction_open(),
                "committed": t.committed,
                "aborted": t.aborted,
                "partitions": t.added.iter().map(|(topic, p)| format!("{topic}-{p}")).collect::<Vec<_>>(),
                "last_error": t.last_error,
            })
        });
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
            "acked_upto": acked_upto,
            "transactions": transactions,
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

/// The `TxnOffsetCommit` of `offsets` for the transaction of
/// `transactional_id` under `(producer id, epoch)`.
fn txn_offset_commit(
    transactional_id: String,
    (producer_id, producer_epoch): (i64, i16),
    offsets: &TxnOffsets,
) -> TxnCommitOffsets {
    let mut topics: BTreeMap<String, Vec<TxnOffsetCommitRequestPartition>> = BTreeMap::new();
    for (topic, partition, offset) in &offsets.offsets {
        topics
            .entry(topic.clone())
            .or_default()
            .push(TxnOffsetCommitRequestPartition {
                partition_index: *partition,
                committed_offset: *offset,
                committed_leader_epoch: -1,
                committed_metadata: Some(String::new()),
                ..Default::default()
            });
    }
    TxnCommitOffsets(TxnOffsetCommitRequest {
        transactional_id,
        group_id: offsets.group.group_id.clone(),
        producer_id,
        producer_epoch,
        generation_id_or_member_epoch: offsets.group.generation,
        member_id: offsets.group.member_id.clone(),
        group_instance_id: None,
        topics: topics
            .into_iter()
            .map(|(name, partitions)| TxnOffsetCommitRequestTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

/// Kafka's text for a transaction call on a producer without a
/// `transactional_id`.
fn not_transactional() -> String {
    "Transactional method invoked on a non-transactional producer.".to_string()
}

/// Kafka's `TransactionManager.transitionTo` text for a call the state does
/// not allow.
fn invalid_transition(from: TxnState, to: TxnState) -> String {
    format!(
        "Invalid transition attempted from state {} to state {}",
        from.name().to_uppercase(),
        to.name().to_uppercase()
    )
}
