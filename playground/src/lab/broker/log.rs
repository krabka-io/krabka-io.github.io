//! The partition log: appended record batches, the offsets around them, the
//! leader-epoch cache and the idempotent-producer state.
//!
//! A [`PartitionLog`] keeps every batch as the bytes a `Fetch` serves, so a
//! consumer reads back exactly what the leader stored. An append assigns the
//! base offset and stamps the partition leader epoch in place; both fields sit
//! outside the CRC, so the producer's checksum stays valid. A topic with
//! `message.timestamp.type=LogAppendTime` is the exception: its batch is
//! re-encoded with the append time as the maximum timestamp, as Kafka's
//! `LogValidator` rewrites it.
//!
//! Every timestamp is a logical millisecond of the world clock: the producers
//! of the lab stamp their records with it, and retention compares against it.

use std::collections::{BTreeMap, VecDeque};

use bytes::{Bytes, BytesMut};
use krabka_protocol::records::{
    RecordBatch, RecordsError, TimestampType, patch_base_offset_and_leader_epoch,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::lab::{codes, net::Millis};

/// How many batches of one producer the log remembers for duplicate
/// detection, Kafka's `ProducerStateEntry.NUM_BATCHES_TO_RETAIN`.
pub const RETAINED_PRODUCER_BATCHES: usize = 5;

/// The `ListOffsets` timestamp sentinel for the latest offset.
pub const LATEST_TIMESTAMP: i64 = -1;
/// The `ListOffsets` timestamp sentinel for the earliest offset.
pub const EARLIEST_TIMESTAMP: i64 = -2;
/// The `ListOffsets` timestamp sentinel for the offset of the record with the
/// highest timestamp (KIP-734).
pub const MAX_TIMESTAMP: i64 = -3;
/// The `ListOffsets` timestamp sentinel for the earliest offset held locally
/// (KIP-405). The lab has no remote tier, so it is the earliest offset.
pub const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

/// Kafka's `RecordBatch.NO_TIMESTAMP`.
pub const NO_TIMESTAMP: i64 = -1;
/// Kafka's `RecordBatch.NO_PARTITION_LEADER_EPOCH`.
pub const NO_LEADER_EPOCH: i32 = -1;
/// Kafka's `ListOffsetsResponse.UNKNOWN_OFFSET`.
pub const UNKNOWN_OFFSET: i64 = -1;
/// Kafka's default `message.timestamp.after.max.ms`: a `CreateTime` record
/// may be at most an hour ahead of the broker clock.
pub const DEFAULT_TIMESTAMP_AFTER_MAX_MS: i64 = 3_600_000;

/// Kafka's `Records.LOG_OVERHEAD`: the base offset and the batch length that
/// precede what a batch header's `batch_length` counts.
const LOG_OVERHEAD: usize = 12;
/// Kafka's `LegacyRecord.RECORD_OVERHEAD_V0`, the smallest length a batch
/// header may declare.
const MIN_BATCH_LENGTH: i32 = 14;
/// Where the magic byte sits in a batch header.
const MAGIC_OFFSET: usize = 16;
/// Where the attributes sit in a v2 batch header.
const ATTRIBUTES_OFFSET: usize = 21;
/// The first produce version that may carry zstd batches (KIP-110).
const ZSTD_MIN_PRODUCE_VERSION: i16 = 7;
/// The compression id of zstd in a batch's attributes.
const ZSTD_COMPRESSION_ID: u8 = 4;

/// One appended batch and the index fields the read path needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBatch {
    /// The batch as it goes out on a `Fetch`: header, offsets and epoch
    /// stamped, records untouched.
    pub bytes: Bytes,
    /// The offset of the first record.
    pub base_offset: i64,
    /// The offset of the last record.
    pub last_offset: i64,
    /// The partition leader epoch stamped on the batch.
    pub leader_epoch: i32,
    /// The batch's base timestamp.
    pub base_timestamp: i64,
    /// The largest record timestamp of the batch, or the append time on a
    /// `LogAppendTime` topic. Retention and timestamp lookups read it.
    pub max_timestamp: i64,
    /// How many records the batch holds.
    pub record_count: i32,
}

impl StoredBatch {
    /// The batch size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.bytes.len()
    }
}

/// Why an append was refused, with the Kafka error code a produce row carries.
///
/// The messages are Kafka's exception messages. Only [`Self::Records`]
/// reaches the wire; Kafka's `ReplicaManager` answers every other refusal
/// without a message.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendError {
    /// A produce's records hold no whole batch.
    #[error("Produce requests with version {0} must have at least one record batch per partition")]
    NoBatch(i16),
    /// A produce's records hold more than one batch.
    #[error(
        "Produce requests with version {0} are only allowed to contain exactly one record batch per partition"
    )]
    NotOneBatch(i16),
    /// A produce's batch is not a v2 batch.
    #[error(
        "Produce requests with version {0} are only allowed to contain record batches with magic version 2"
    )]
    NotMagicV2(i16),
    #[error("{0}")]
    Corrupt(String),
    /// Kafka's `LogValidator.validateBatch` refused the batch.
    #[error("{0}")]
    InvalidRecord(String),
    /// Records of the batch failed `LogValidator.validateRecord`.
    #[error("{}", record_errors_message(.errors, *.invalid_timestamp))]
    Records {
        errors: Vec<RecordError>,
        /// Whether any record has a timestamp out of range, which makes the
        /// whole refusal `INVALID_TIMESTAMP`.
        invalid_timestamp: bool,
    },
    /// zstd before produce v7, or a codec the lab does not build.
    #[error("the batch uses a compression codec this request or the lab cannot carry")]
    UnsupportedCompression,
    #[error(
        "The record batch size in the append to {partition} is {size} bytes which exceeds the maximum configured value of {max}."
    )]
    TooLarge {
        partition: String,
        size: usize,
        max: usize,
    },
    /// The producer's sequence is not the next one after the last batch.
    #[error(
        "Out of order sequence number for producer {producer_id}: {sequence} (incoming seq. number)"
    )]
    OutOfOrderSequence { producer_id: i64, sequence: i32 },
    /// The producer epoch is older than the one the log knows.
    #[error(
        "Epoch of producer {producer_id} is {epoch}, which is smaller than the last seen epoch {current}"
    )]
    InvalidProducerEpoch {
        producer_id: i64,
        epoch: i16,
        current: i16,
    },
    /// A follower append whose base offset is not the log end offset.
    #[error("batch base offset {base_offset} is not the log end offset {log_end_offset}")]
    OffsetMismatch {
        base_offset: i64,
        log_end_offset: i64,
    },
}

impl AppendError {
    /// The Kafka error code of this refusal.
    #[must_use]
    pub fn code(&self) -> i16 {
        match self {
            Self::NoBatch(_)
            | Self::NotOneBatch(_)
            | Self::NotMagicV2(_)
            | Self::InvalidRecord(_)
            | Self::OffsetMismatch { .. } => codes::INVALID_RECORD,
            Self::Records {
                invalid_timestamp, ..
            } => {
                if *invalid_timestamp {
                    codes::INVALID_TIMESTAMP
                } else {
                    codes::INVALID_RECORD
                }
            }
            Self::Corrupt(_) => codes::CORRUPT_MESSAGE,
            Self::UnsupportedCompression => codes::UNSUPPORTED_COMPRESSION_TYPE,
            Self::TooLarge { .. } => codes::MESSAGE_TOO_LARGE,
            Self::OutOfOrderSequence { .. } => codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
            Self::InvalidProducerEpoch { .. } => codes::INVALID_PRODUCER_EPOCH,
        }
    }

    /// The `error_message` a produce row carries: Kafka's
    /// `LogAppendResult.errorMessage` keeps only a record validation's.
    #[must_use]
    pub fn wire_message(&self) -> Option<String> {
        matches!(self, Self::Records { .. }).then(|| self.to_string())
    }

    /// The per-record errors a produce row carries.
    #[must_use]
    pub fn record_errors(&self) -> &[RecordError] {
        match self {
            Self::Records { errors, .. } => errors,
            _ => &[],
        }
    }

    /// Whether the row reports the partition's log start offset. Kafka's
    /// `ReplicaManager.appendToLocalLog` reports it for a refusal the log
    /// raised while validating records and producer state, and `-1` for one
    /// raised before the log was reached, an oversized batch and a corrupt
    /// one.
    #[must_use]
    pub fn reports_log_start(&self) -> bool {
        !matches!(
            self,
            Self::NoBatch(_)
                | Self::NotOneBatch(_)
                | Self::NotMagicV2(_)
                | Self::Corrupt(_)
                | Self::TooLarge { .. }
                | Self::UnsupportedCompression
        )
    }
}

/// Kafka's `LogValidator.processRecordErrors` message.
fn record_errors_message(errors: &[RecordError], invalid_timestamp: bool) -> String {
    if invalid_timestamp {
        return "One or more records have been rejected due to invalid timestamp".to_string();
    }
    let shown: Vec<String> = errors.iter().take(3).map(ToString::to_string).collect();
    format!(
        "One or more records have been rejected due to {} record errors in total, and only showing the first three errors at most: [{}]",
        errors.len(),
        shown.join(", ")
    )
}

impl From<RecordsError> for AppendError {
    fn from(error: RecordsError) -> Self {
        // Every other decode failure (a CRC mismatch, a truncated batch, a
        // record that does not parse) is Kafka's `CorruptRecordException`.
        match error {
            RecordsError::UnsupportedMagic { .. } => Self::InvalidRecord(error.to_string()),
            RecordsError::Compression(_) => Self::UnsupportedCompression,
            _ => Self::Corrupt(error.to_string()),
        }
    }
}

/// What an accepted append reports back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendInfo {
    /// The offset of the first record appended.
    pub first_offset: i64,
    /// The offset of the last record appended.
    pub last_offset: i64,
    /// The `log_append_time_ms` of the produce row: the append time on a
    /// `LogAppendTime` topic and `-1` otherwise. A duplicate reports the
    /// timestamp its original batch was stored with, as Kafka's
    /// `UnifiedLog.append` does.
    pub log_append_time_ms: i64,
    /// The batch was a retry the log had already stored; the offsets are the
    /// original ones and nothing was appended.
    pub duplicate: bool,
}

/// The topic settings an append honours.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendPolicy {
    /// Kafka's `message.timestamp.type`.
    pub timestamp_type: TimestampType,
    /// Kafka's `max.message.bytes`.
    pub max_message_bytes: usize,
    /// Kafka's `cleanup.policy=compact`: every record needs a key.
    pub compacted: bool,
    /// Kafka's `message.timestamp.before.max.ms`.
    pub timestamp_before_max_ms: i64,
    /// Kafka's `message.timestamp.after.max.ms`.
    pub timestamp_after_max_ms: i64,
}

/// A record the append refused, Kafka's `ProduceResponse.RecordError`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordError {
    /// The record's index in its batch.
    pub batch_index: i32,
    /// Why the record was refused.
    pub message: String,
}

impl std::fmt::Display for RecordError {
    // Kafka's `RecordError.toString`, which the refusal message embeds.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordError(batchIndex={}, message='{}')",
            self.batch_index, self.message
        )
    }
}

/// One retained batch of an idempotent producer, Kafka's `BatchMetadata`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerBatch {
    /// The sequence of the batch's first record.
    pub base_sequence: i32,
    /// The sequence of the batch's last record.
    pub last_sequence: i32,
    /// The offset the batch's first record took.
    pub first_offset: i64,
    /// The offset the batch's last record took.
    pub last_offset: i64,
    /// The maximum timestamp the batch was stored with.
    pub timestamp: i64,
}

/// The last batches of one idempotent producer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerEntry {
    /// The producer epoch the log knows.
    pub epoch: i16,
    /// The retained batches, oldest first.
    pub batches: VecDeque<ProducerBatch>,
}

impl ProducerEntry {
    fn last_sequence(&self) -> Option<i32> {
        self.batches.back().map(|b| b.last_sequence)
    }
}

/// Kafka's `ProduceRequest.validateRecords` on one partition's records:
/// exactly one whole v2 batch, and no zstd before produce v7. Bytes after
/// the batch that do not form another one are dropped, as the log's
/// `trimInvalidBytes` drops them. Returns the batch's length.
///
/// # Errors
/// Returns `NoBatch`, `NotOneBatch` or `NotMagicV2` (`INVALID_RECORD`),
/// `Corrupt` for a header Kafka's batch iterator refuses, and
/// `UnsupportedCompression` for zstd before v7.
pub fn single_batch(records: &[u8], version: i16) -> Result<usize, AppendError> {
    let length = next_batch_len(records)?.ok_or(AppendError::NoBatch(version))?;
    if records[MAGIC_OFFSET] != 2 {
        return Err(AppendError::NotMagicV2(version));
    }
    if version < ZSTD_MIN_PRODUCE_VERSION
        && records.get(ATTRIBUTES_OFFSET + 1).map(|low| low & 0x07) == Some(ZSTD_COMPRESSION_ID)
    {
        return Err(AppendError::UnsupportedCompression);
    }
    if next_batch_len(&records[length..])?.is_some() {
        return Err(AppendError::NotOneBatch(version));
    }
    Ok(length)
}

/// Kafka's `ByteBufferLogInputStream.nextBatchSize`: the length of the whole
/// batch at the head of `bytes`, or `None` when the bytes do not hold one.
fn next_batch_len(bytes: &[u8]) -> Result<Option<usize>, AppendError> {
    let Some(length) = bytes
        .get(8..LOG_OVERHEAD)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(i32::from_be_bytes)
    else {
        return Ok(None);
    };
    if length < MIN_BATCH_LENGTH {
        return Err(AppendError::Corrupt(format!(
            "Record size {length} is less than the minimum record overhead ({MIN_BATCH_LENGTH})"
        )));
    }
    let Some(&magic) = bytes.get(MAGIC_OFFSET) else {
        return Ok(None);
    };
    if magic > 2 {
        return Err(AppendError::Corrupt(format!(
            "Invalid magic found in record: {}",
            i8::from_ne_bytes([magic])
        )));
    }
    let total = usize::try_from(length)
        .unwrap_or(usize::MAX)
        .saturating_add(LOG_OVERHEAD);
    Ok((bytes.len() >= total).then_some(total))
}

/// The `(epoch, start_offset)` of the epoch entry that covers an offset.
type EpochEntry = (i32, i64);

/// A change to the stored batches, for the durable log store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogChange {
    /// The batch at `base_offset` was appended.
    Appended(i64),
    /// Every batch at or past `offset` was dropped.
    TruncatedFrom(i64),
    /// Every batch below `offset` was dropped.
    TruncatedBefore(i64),
    /// Every batch was dropped.
    Cleared,
}

/// The small state beside the batches that a reload restores.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The high watermark.
    pub high_watermark: i64,
    /// The log start offset.
    pub log_start_offset: i64,
    /// The leader-epoch cache, `(epoch, start_offset)` ascending.
    pub epoch_cache: Vec<EpochEntry>,
    /// The idempotent producers, by producer id.
    pub producers: BTreeMap<i64, ProducerEntry>,
}

/// The log of one partition replica.
///
/// Two logs are equal when they hold the same batches, offsets, epochs and
/// producer state; the durable journal and the change counter are
/// bookkeeping and take no part.
#[derive(Clone, Debug, Default)]
pub struct PartitionLog {
    batches: VecDeque<StoredBatch>,
    log_start_offset: i64,
    log_end_offset: i64,
    high_watermark: i64,
    /// KIP-101 leader-epoch cache: `(epoch, start_offset)`, ascending.
    epoch_cache: Vec<EpochEntry>,
    producers: BTreeMap<i64, ProducerEntry>,
    size_bytes: usize,
    /// The batch changes since the last [`PartitionLog::take_journal`].
    journal: Vec<LogChange>,
    /// Moves on every change of state, so a caller can tell cheaply whether
    /// the checkpoint may have changed.
    changes: u64,
}

impl PartialEq for PartitionLog {
    fn eq(&self, other: &Self) -> bool {
        self.batches == other.batches
            && self.log_start_offset == other.log_start_offset
            && self.log_end_offset == other.log_end_offset
            && self.high_watermark == other.high_watermark
            && self.epoch_cache == other.epoch_cache
            && self.producers == other.producers
            && self.size_bytes == other.size_bytes
    }
}

impl Eq for PartitionLog {}

impl PartitionLog {
    /// An empty log at offset zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild a log from the batches a durable store kept and the checkpoint
    /// beside them. A batch that does not decode ends the rebuild; the
    /// checkpoint bounds every offset to what the batches hold.
    #[must_use]
    pub fn restore(batches: &[Bytes], checkpoint: Option<Checkpoint>) -> Self {
        let mut log = Self::new();
        for bytes in batches {
            let mut cursor: &[u8] = bytes;
            let Ok(batch) = RecordBatch::decode(&mut cursor) else {
                break;
            };
            if log
                .batches
                .back()
                .is_some_and(|last| last.last_offset >= batch.base_offset)
            {
                break;
            }
            let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
            if log.batches.is_empty() {
                log.log_start_offset = batch.base_offset;
            }
            log.push(StoredBatch {
                bytes: bytes.clone(),
                base_offset: batch.base_offset,
                last_offset,
                leader_epoch: batch.partition_leader_epoch,
                base_timestamp: batch.base_timestamp,
                max_timestamp: batch.max_timestamp,
                record_count: i32::try_from(batch.records.len()).unwrap_or(i32::MAX),
            });
        }
        log.journal.clear();
        if let Some(checkpoint) = checkpoint {
            log.log_start_offset = checkpoint
                .log_start_offset
                .max(log.log_start_offset)
                .min(log.log_end_offset);
            log.log_end_offset = log.log_end_offset.max(log.log_start_offset);
            log.high_watermark = checkpoint
                .high_watermark
                .clamp(log.log_start_offset, log.log_end_offset);
            log.epoch_cache = checkpoint
                .epoch_cache
                .into_iter()
                .filter(|e| e.1 <= log.log_end_offset)
                .collect();
            log.producers = checkpoint.producers;
        } else {
            for batch in &log.batches {
                let epoch = batch.leader_epoch;
                if epoch >= 0 && log.epoch_cache.last().is_none_or(|e| e.0 < epoch) {
                    log.epoch_cache.push((epoch, batch.base_offset));
                }
            }
            log.high_watermark = log.log_start_offset;
        }
        log
    }

    /// The state a reload restores beside the batches.
    #[must_use]
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            high_watermark: self.high_watermark,
            log_start_offset: self.log_start_offset,
            epoch_cache: self.epoch_cache.clone(),
            producers: self.producers.clone(),
        }
    }

    /// The batch changes since the last call, in order.
    pub fn take_journal(&mut self) -> Vec<LogChange> {
        std::mem::take(&mut self.journal)
    }

    /// A counter that moves whenever the log's state changes.
    #[must_use]
    pub fn changes(&self) -> u64 {
        self.changes
    }

    /// The stored batch that starts at `base_offset`.
    #[must_use]
    pub fn batch_at(&self, base_offset: i64) -> Option<&StoredBatch> {
        let at = self
            .batches
            .partition_point(|b| b.base_offset < base_offset);
        self.batches
            .get(at)
            .filter(|b| b.base_offset == base_offset)
    }

    /// The first offset the log serves.
    #[must_use]
    pub fn log_start_offset(&self) -> i64 {
        self.log_start_offset
    }

    /// The offset the next appended record takes.
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        self.log_end_offset
    }

    /// The high watermark: every offset below it is committed.
    #[must_use]
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// The stored batches, oldest first.
    #[must_use]
    pub fn batches(&self) -> &VecDeque<StoredBatch> {
        &self.batches
    }

    /// The bytes of every stored batch.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// The leader-epoch cache, ascending by epoch.
    #[must_use]
    pub fn epoch_cache(&self) -> &[EpochEntry] {
        &self.epoch_cache
    }

    /// The producers the log has state for.
    #[must_use]
    pub fn producers(&self) -> &BTreeMap<i64, ProducerEntry> {
        &self.producers
    }

    /// Raise the high watermark to `offset`, clamped to the log end. The high
    /// watermark never moves backwards.
    pub fn set_high_watermark(&mut self, offset: i64) -> bool {
        let next = offset.min(self.log_end_offset).max(self.high_watermark);
        let advanced = next > self.high_watermark;
        if advanced {
            self.high_watermark = next;
            self.changes += 1;
        }
        advanced
    }

    /// The latest epoch in the cache, if any.
    #[must_use]
    pub fn latest_epoch(&self) -> Option<i32> {
        self.epoch_cache.last().map(|e| e.0)
    }

    /// Record that `epoch` starts at `start_offset`, as a leader does when it
    /// takes the partition and a follower does when it sees a batch of a newer
    /// epoch. An epoch not above the latest one is ignored.
    pub fn assign_epoch(&mut self, epoch: i32, start_offset: i64) {
        if epoch < 0 {
            return;
        }
        match self.epoch_cache.last() {
            Some(&(latest, start))
                if epoch < latest || (epoch == latest && start <= start_offset) => {}
            Some(&(latest, _)) if epoch == latest => {
                if let Some(last) = self.epoch_cache.last_mut() {
                    last.1 = start_offset;
                }
                self.changes += 1;
            }
            _ => {
                self.epoch_cache.push((epoch, start_offset));
                self.changes += 1;
            }
        }
    }

    /// The end offset of `epoch`, Kafka's `LeaderEpochFileCache.endOffsetFor`
    /// (KIP-101). The latest epoch ends at the log end. An older epoch ends
    /// where the next known epoch starts, reported under the largest known
    /// epoch not above it, or under the requested epoch itself when that is
    /// older than every known one. `(-1, -1)` for an empty cache, the
    /// undefined epoch `-1`, or an epoch newer than every known one.
    #[must_use]
    pub fn end_offset_for_epoch(&self, requested: i32) -> (i32, i64) {
        let undefined = (NO_LEADER_EPOCH, UNKNOWN_OFFSET);
        let Some(&(latest, _)) = self.epoch_cache.last() else {
            return undefined;
        };
        if requested == NO_LEADER_EPOCH {
            return undefined;
        }
        if requested == latest {
            return (requested, self.log_end_offset);
        }
        let Some(&(_, next_start)) = self.epoch_cache.iter().find(|e| e.0 > requested) else {
            return undefined;
        };
        match self.epoch_cache.iter().rev().find(|e| e.0 <= requested) {
            Some(&(epoch, _)) => (epoch, next_start),
            None => (requested, next_start),
        }
    }

    /// Where a follower truncates to on its leader's answer about an epoch,
    /// Kafka's `AbstractFetcherThread.getOffsetTruncationState`: the offset,
    /// and whether truncation is complete or the next fetch must ask about an
    /// older epoch the leader has not confirmed yet. An unknown end offset
    /// keeps the fetch offset, which is the log end.
    #[must_use]
    pub fn truncation_target(&self, leader_epoch: i32, leader_end_offset: i64) -> (i64, bool) {
        let log_end = self.log_end_offset;
        if leader_end_offset < 0 {
            return (log_end, true);
        }
        if leader_epoch < 0 {
            return (leader_end_offset.min(log_end), true);
        }
        let (epoch, end) = self.end_offset_for_epoch(leader_epoch);
        if epoch < 0 || end < 0 {
            (leader_end_offset.min(log_end), true)
        } else if epoch == leader_epoch {
            (end.min(leader_end_offset).min(log_end), true)
        } else {
            (end.min(log_end), false)
        }
    }

    /// The epoch of the entry that covers `offset`, or `-1`.
    #[must_use]
    pub fn epoch_for_offset(&self, offset: i64) -> i32 {
        self.epoch_cache
            .iter()
            .rev()
            .find(|e| e.1 <= offset)
            .map_or(NO_LEADER_EPOCH, |e| e.0)
    }

    /// Validate and append a batch a producer sent, assigning its offsets:
    /// Kafka's `UnifiedLog.appendAsLeader` in its order of checks, the base
    /// offset, the size, the checksum, then `LogValidator` on the batch and
    /// its records, then the producer state. `records` is one whole batch, as
    /// [`single_batch`] leaves it; `partition` names the partition in the
    /// refusal messages, Kafka's `topic-partition`.
    ///
    /// # Errors
    /// Returns the refusal a produce row reports; nothing is appended then.
    pub fn append(
        &mut self,
        records: &Bytes,
        leader_epoch: i32,
        now: Millis,
        policy: AppendPolicy,
        partition: &str,
    ) -> Result<AppendInfo, AppendError> {
        let base = records
            .get(..8)
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map(i64::from_be_bytes);
        if let Some(base) = base
            && base != 0
        {
            return Err(AppendError::InvalidRecord(format!(
                "The baseOffset of the record batch in the append to {partition} should be 0, but it is {base}"
            )));
        }
        if records.len() > policy.max_message_bytes {
            return Err(AppendError::TooLarge {
                partition: partition.to_string(),
                size: records.len(),
                max: policy.max_message_bytes,
            });
        }
        let mut cursor: &[u8] = records;
        let batch = RecordBatch::decode(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(AppendError::InvalidRecord(
                "Compressed outer record has more than one batch".to_string(),
            ));
        }
        validate_client_batch(&batch, partition)?;
        let now_ms = i64::try_from(now).unwrap_or(i64::MAX);
        validate_records(&batch, now_ms, policy, partition)?;
        if let Some(duplicate) = self.check_producer(&batch)? {
            return Ok(duplicate);
        }
        let base_offset = self.log_end_offset;
        let last_offset = base_offset + i64::from(batch.last_offset_delta);
        let (bytes, max_timestamp, log_append_time_ms) = match policy.timestamp_type {
            TimestampType::CreateTime => {
                let mut buf = BytesMut::from(records.as_ref());
                patch_base_offset_and_leader_epoch(&mut buf, base_offset, leader_epoch);
                (buf.freeze(), batch.max_timestamp, NO_TIMESTAMP)
            }
            TimestampType::LogAppendTime => {
                let mut stamped = batch.clone();
                stamped.base_offset = base_offset;
                stamped.partition_leader_epoch = leader_epoch;
                stamped.max_timestamp = now_ms;
                stamped.attributes = stamped
                    .attributes
                    .with_timestamp_type(TimestampType::LogAppendTime);
                let mut buf = BytesMut::with_capacity(stamped.encoded_len());
                stamped.encode(&mut buf)?;
                (buf.freeze(), now_ms, now_ms)
            }
        };
        self.push(StoredBatch {
            bytes,
            base_offset,
            last_offset,
            leader_epoch,
            base_timestamp: batch.base_timestamp,
            max_timestamp,
            record_count: i32::try_from(batch.records.len()).unwrap_or(i32::MAX),
        });
        self.record_producer(&batch, base_offset, last_offset, max_timestamp);
        Ok(AppendInfo {
            first_offset: base_offset,
            last_offset,
            log_append_time_ms,
            duplicate: false,
        })
    }

    /// Append batches a leader served, as a follower does. Every batch keeps
    /// its offsets and epoch; the first one must start at the log end.
    ///
    /// # Errors
    /// Returns the first batch that is corrupt or out of place; the batches
    /// before it stay appended.
    pub fn append_replicated(&mut self, records: &[u8]) -> Result<Vec<(i64, i64)>, AppendError> {
        let mut cursor: &[u8] = records;
        let mut appended = Vec::new();
        while !cursor.is_empty() {
            let start = records.len() - cursor.len();
            let batch = RecordBatch::decode(&mut cursor)?;
            let end = records.len() - cursor.len();
            if batch.base_offset != self.log_end_offset {
                return Err(AppendError::OffsetMismatch {
                    base_offset: batch.base_offset,
                    log_end_offset: self.log_end_offset,
                });
            }
            let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
            self.assign_epoch(batch.partition_leader_epoch, batch.base_offset);
            self.push(StoredBatch {
                bytes: Bytes::copy_from_slice(&records[start..end]),
                base_offset: batch.base_offset,
                last_offset,
                leader_epoch: batch.partition_leader_epoch,
                base_timestamp: batch.base_timestamp,
                max_timestamp: batch.max_timestamp,
                record_count: i32::try_from(batch.records.len()).unwrap_or(i32::MAX),
            });
            self.record_producer(&batch, batch.base_offset, last_offset, batch.max_timestamp);
            appended.push((batch.base_offset, last_offset));
        }
        Ok(appended)
    }

    fn push(&mut self, batch: StoredBatch) {
        self.log_end_offset = batch.last_offset + 1;
        self.size_bytes += batch.size();
        self.journal.push(LogChange::Appended(batch.base_offset));
        self.batches.push_back(batch);
        self.changes += 1;
    }

    /// The idempotence check of a produced batch: `Ok(Some)` for a retry the
    /// log already holds, `Ok(None)` for a batch to append.
    fn check_producer(&self, batch: &RecordBatch) -> Result<Option<AppendInfo>, AppendError> {
        if batch.producer_id < 0 {
            return Ok(None);
        }
        let Some(entry) = self.producers.get(&batch.producer_id) else {
            return Ok(None);
        };
        let last_sequence = increment_sequence(batch.base_sequence, batch.last_offset_delta);
        if batch.producer_epoch == entry.epoch
            && let Some(original) = entry.batches.iter().find(|b| {
                b.base_sequence == batch.base_sequence && b.last_sequence == last_sequence
            })
        {
            return Ok(Some(AppendInfo {
                first_offset: original.first_offset,
                last_offset: original.last_offset,
                log_append_time_ms: original.timestamp,
                duplicate: true,
            }));
        }
        if batch.producer_epoch < entry.epoch {
            return Err(AppendError::InvalidProducerEpoch {
                producer_id: batch.producer_id,
                epoch: batch.producer_epoch,
                current: entry.epoch,
            });
        }
        let out_of_order = AppendError::OutOfOrderSequence {
            producer_id: batch.producer_id,
            sequence: batch.base_sequence,
        };
        if batch.producer_epoch > entry.epoch {
            return if batch.base_sequence == 0 {
                Ok(None)
            } else {
                Err(out_of_order)
            };
        }
        match entry.last_sequence() {
            Some(last) if increment_sequence(last, 1) != batch.base_sequence => Err(out_of_order),
            _ => Ok(None),
        }
    }

    fn record_producer(
        &mut self,
        batch: &RecordBatch,
        first_offset: i64,
        last_offset: i64,
        timestamp: i64,
    ) {
        if batch.producer_id < 0 {
            return;
        }
        let last_sequence = increment_sequence(batch.base_sequence, batch.last_offset_delta);
        let entry = self
            .producers
            .entry(batch.producer_id)
            .or_insert_with(|| ProducerEntry {
                epoch: batch.producer_epoch,
                batches: VecDeque::new(),
            });
        if batch.producer_epoch > entry.epoch {
            entry.epoch = batch.producer_epoch;
            entry.batches.clear();
        }
        entry.batches.push_back(ProducerBatch {
            base_sequence: batch.base_sequence,
            last_sequence,
            first_offset,
            last_offset,
            timestamp,
        });
        while entry.batches.len() > RETAINED_PRODUCER_BATCHES {
            entry.batches.pop_front();
        }
        self.changes += 1;
    }

    /// The batches from the one that holds `from_offset` up to, but not
    /// including, the first batch at or past `upper_bound`, as one byte run.
    /// At least one batch is returned when there is one, even past
    /// `max_bytes`, so a fetch behind a large batch still makes progress.
    #[must_use]
    pub fn read(&self, from_offset: i64, max_bytes: usize, upper_bound: i64) -> Bytes {
        let start = self
            .batches
            .partition_point(|b| b.last_offset < from_offset);
        let mut out = BytesMut::new();
        for batch in self.batches.iter().skip(start) {
            if batch.base_offset >= upper_bound {
                break;
            }
            if !out.is_empty() && out.len() + batch.size() > max_bytes {
                break;
            }
            out.extend_from_slice(&batch.bytes);
        }
        out.freeze()
    }

    /// Drop every batch at or past `offset`. The log end becomes the end of
    /// the last batch left, and the high watermark is clamped to it.
    pub fn truncate_to(&mut self, offset: i64) {
        while self.batches.back().is_some_and(|b| b.last_offset >= offset) {
            if let Some(dropped) = self.batches.pop_back() {
                self.size_bytes -= dropped.size();
            }
        }
        self.log_end_offset = self
            .batches
            .back()
            .map_or(self.log_start_offset, |b| b.last_offset + 1);
        self.journal
            .push(LogChange::TruncatedFrom(self.log_end_offset));
        self.changes += 1;
        self.high_watermark = self.high_watermark.min(self.log_end_offset);
        self.epoch_cache.retain(|e| e.1 < self.log_end_offset);
        let log_end = self.log_end_offset;
        self.producers.retain(|_, e| {
            e.batches.retain(|b| b.last_offset < log_end);
            !e.batches.is_empty()
        });
    }

    /// Drop everything and continue at `offset`, as a follower does when the
    /// leader no longer holds the offsets it had.
    pub fn truncate_fully_and_start_at(&mut self, offset: i64) {
        self.batches.clear();
        self.journal.push(LogChange::Cleared);
        self.changes += 1;
        self.size_bytes = 0;
        self.log_start_offset = offset;
        self.log_end_offset = offset;
        self.high_watermark = offset;
        self.epoch_cache.clear();
        self.producers.clear();
    }

    /// Raise the log start offset, dropping the batches that end below it.
    pub fn increment_log_start_offset(&mut self, offset: i64) {
        if offset <= self.log_start_offset {
            return;
        }
        self.log_start_offset = offset.min(self.log_end_offset);
        while self
            .batches
            .front()
            .is_some_and(|b| b.last_offset < self.log_start_offset)
        {
            if let Some(dropped) = self.batches.pop_front() {
                self.size_bytes -= dropped.size();
            }
        }
        self.high_watermark = self.high_watermark.max(self.log_start_offset);
        let kept_from = self
            .batches
            .front()
            .map_or(self.log_start_offset, |b| b.base_offset);
        self.journal.push(LogChange::TruncatedBefore(kept_from));
        self.changes += 1;
        self.truncate_epochs_from_start();
    }

    /// Kafka's `LeaderEpochFileCache.truncateFromStart`: drop the entries
    /// below the log start and move the last of them up to it.
    fn truncate_epochs_from_start(&mut self) {
        let start = self.log_start_offset;
        let below = self.epoch_cache.iter().filter(|e| e.1 < start).count();
        if below == 0 {
            return;
        }
        self.epoch_cache.drain(..below - 1);
        if let Some(first) = self.epoch_cache.first_mut() {
            first.1 = start;
        }
    }

    /// Delete the oldest batches that retention no longer keeps: a batch older
    /// than `retention_ms` by its maximum timestamp, or one that keeps the log
    /// above `retention_bytes`. A negative limit is off. Only batches below
    /// the high watermark are deleted. Returns the number of batches deleted.
    pub fn apply_retention(
        &mut self,
        now: Millis,
        retention_ms: i64,
        retention_bytes: i64,
    ) -> usize {
        let now = i64::try_from(now).unwrap_or(i64::MAX);
        let mut deleted = 0;
        while let Some(first) = self.batches.front() {
            if first.last_offset >= self.high_watermark {
                break;
            }
            let expired =
                retention_ms >= 0 && first.max_timestamp.saturating_add(retention_ms) < now;
            let oversize = retention_bytes >= 0
                && i64::try_from(self.size_bytes).unwrap_or(i64::MAX) > retention_bytes;
            if !expired && !oversize {
                break;
            }
            let new_start = first.last_offset + 1;
            deleted += 1;
            self.increment_log_start_offset(new_start);
        }
        deleted
    }

    /// The offset `ListOffsets` reports for `timestamp`, as
    /// `(timestamp, offset, leader_epoch)`. `bound` is the last fetchable
    /// offset of the caller: the high watermark for a consumer and the log end
    /// for a replica. A lookup that resolves at or past the bound answers
    /// `(-1, -1, -1)`, as Kafka refuses an offset the caller may not read.
    #[must_use]
    pub fn list_offset(&self, timestamp: i64, bound: i64) -> (i64, i64, i32) {
        match timestamp {
            LATEST_TIMESTAMP => (
                NO_TIMESTAMP,
                bound,
                self.latest_epoch().unwrap_or(NO_LEADER_EPOCH),
            ),
            EARLIEST_TIMESTAMP | EARLIEST_LOCAL_TIMESTAMP => {
                let epoch = self
                    .epoch_cache
                    .first()
                    .filter(|e| e.1 <= self.log_start_offset)
                    .map_or(NO_LEADER_EPOCH, |e| e.0);
                (NO_TIMESTAMP, self.log_start_offset, epoch)
            }
            MAX_TIMESTAMP => Self::bounded(self.max_timestamp_offset(), bound),
            ts => Self::bounded(self.offset_for_timestamp(ts), bound),
        }
    }

    fn bounded(found: Option<(i64, i64, i32)>, bound: i64) -> (i64, i64, i32) {
        match found {
            Some((ts, offset, epoch)) if offset < bound => (ts, offset, epoch),
            _ => (NO_TIMESTAMP, UNKNOWN_OFFSET, NO_LEADER_EPOCH),
        }
    }

    /// The record with the largest timestamp, the first of them on a tie,
    /// as Kafka's `UnifiedLog.fetchOffsetByTimestamp` finds it.
    fn max_timestamp_offset(&self) -> Option<(i64, i64, i32)> {
        let max = self.batches.iter().map(|b| b.max_timestamp).max()?;
        let batch = self.batches.iter().find(|b| b.max_timestamp == max)?;
        self.scan_records(batch, |record_ts| record_ts == max)
    }

    /// The first record whose timestamp is at least `target`, Kafka's
    /// `FileRecords.searchForTimestamp`.
    fn offset_for_timestamp(&self, target: i64) -> Option<(i64, i64, i32)> {
        self.batches
            .iter()
            .filter(|b| b.max_timestamp >= target)
            .find_map(|batch| self.scan_records(batch, |record_ts| record_ts >= target))
    }

    /// The `(timestamp, offset, leader_epoch)` of the first record of `batch`
    /// at or past the log start that `pick` accepts, decoding the batch to
    /// read its records.
    fn scan_records(
        &self,
        batch: &StoredBatch,
        pick: impl Fn(i64) -> bool,
    ) -> Option<(i64, i64, i32)> {
        let mut cursor: &[u8] = &batch.bytes;
        let decoded = RecordBatch::decode(&mut cursor).ok()?;
        let log_append = decoded.attributes.timestamp_type() == TimestampType::LogAppendTime;
        decoded
            .records
            .iter()
            .map(|r| {
                let ts = if log_append {
                    decoded.max_timestamp
                } else {
                    decoded.base_timestamp.saturating_add(r.timestamp_delta)
                };
                (ts, decoded.base_offset + i64::from(r.offset_delta))
            })
            .find(|(ts, offset)| pick(*ts) && *offset >= self.log_start_offset)
            .map(|(ts, offset)| (ts, offset, batch.leader_epoch))
    }
}

/// Kafka's `LogValidator.validateBatch` checks on a batch a client produced,
/// in its order and with its messages.
fn validate_client_batch(batch: &RecordBatch, partition: &str) -> Result<(), AppendError> {
    let first = batch.base_offset;
    let last = first + i64::from(batch.last_offset_delta);
    let count_from_offsets = i64::from(batch.last_offset_delta) + 1;
    let count = i64::try_from(batch.records.len()).unwrap_or(i64::MAX);
    let refuse = |message: String| Err(AppendError::InvalidRecord(message));
    if count_from_offsets <= 0 {
        return refuse(format!(
            "Batch has an invalid offset range: [{first}, {last}] in topic partition {partition}"
        ));
    }
    if count <= 0 {
        return refuse(format!(
            "Invalid reported count for record batch: {count} in topic partition {partition}"
        ));
    }
    if count_from_offsets != count {
        return refuse(format!(
            "Inconsistent batch offset range [{first}, {last}] and count of records {count} in topic partition {partition}"
        ));
    }
    if batch.attributes.is_control_batch() {
        return refuse(format!(
            "Clients are not allowed to write control records in topic partition {partition}"
        ));
    }
    if batch.producer_id >= 0 && batch.base_sequence < 0 {
        return refuse(format!(
            "Invalid sequence number {} in record batch with producerId {} in topic partition {partition}",
            batch.base_sequence, batch.producer_id
        ));
    }
    Ok(())
}

/// Kafka's `LogValidator.validateRecord` over every record of a produced
/// batch: on a compacted topic a record needs a key, and on a `CreateTime`
/// topic its timestamp must lie within `message.timestamp.before.max.ms`
/// before and `message.timestamp.after.max.ms` after `now`. A record gets at
/// most one error, its key's first.
fn validate_records(
    batch: &RecordBatch,
    now: i64,
    policy: AppendPolicy,
    partition: &str,
) -> Result<(), AppendError> {
    let log_append_batch = batch.attributes.timestamp_type() == TimestampType::LogAppendTime;
    let mut errors = Vec::new();
    let mut invalid_timestamp = false;
    for (index, record) in batch.records.iter().enumerate() {
        let batch_index = i32::try_from(index).unwrap_or(i32::MAX);
        if policy.compacted && record.key.is_none() {
            errors.push(RecordError {
                batch_index,
                message: format!(
                    "Compacted topic cannot accept message without key in topic partition {partition}."
                ),
            });
            continue;
        }
        if policy.timestamp_type != TimestampType::CreateTime {
            continue;
        }
        let timestamp = if log_append_batch {
            batch.max_timestamp
        } else {
            batch.base_timestamp.wrapping_add(record.timestamp_delta)
        };
        let diff = now.wrapping_sub(timestamp);
        if timestamp != NO_TIMESTAMP
            && (diff > policy.timestamp_before_max_ms
                || diff.wrapping_neg() > policy.timestamp_after_max_ms)
        {
            invalid_timestamp = true;
            errors.push(RecordError {
                batch_index,
                message: format!(
                    "Timestamp {timestamp} of message with offset {} is out of range. The timestamp should be within [{}, {}]",
                    batch.base_offset + i64::from(record.offset_delta),
                    now.wrapping_sub(policy.timestamp_before_max_ms),
                    now.wrapping_add(policy.timestamp_after_max_ms)
                ),
            });
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(AppendError::Records {
            errors,
            invalid_timestamp,
        })
    }
}

/// Advance a producer sequence, wrapping past `i32::MAX` to zero.
#[must_use]
pub const fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    sequence.wrapping_add(increment) & i32::MAX
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::records::Record;

    use super::*;

    /// The partition label the refusal messages name.
    const TP: &str = "t-0";

    fn batch(values: &[&str], ts: i64) -> Bytes {
        let mut b = RecordBatch {
            base_timestamp: ts,
            max_timestamp: ts + i64::try_from(values.len()).unwrap() - 1,
            last_offset_delta: i32::try_from(values.len()).unwrap() - 1,
            ..RecordBatch::default()
        };
        for (i, v) in values.iter().enumerate() {
            b.records.push(Record {
                timestamp_delta: i64::try_from(i).unwrap(),
                offset_delta: i32::try_from(i).unwrap(),
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from((*v).to_string())),
                ..Record::default()
            });
        }
        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();
        buf.freeze()
    }

    fn idempotent(values: &[&str], producer_id: i64, epoch: i16, base_sequence: i32) -> Bytes {
        let mut cursor: &[u8] = &batch(values, 0);
        let mut b = RecordBatch::decode(&mut cursor).unwrap();
        b.producer_id = producer_id;
        b.producer_epoch = epoch;
        b.base_sequence = base_sequence;
        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();
        buf.freeze()
    }

    fn policy() -> AppendPolicy {
        AppendPolicy {
            timestamp_type: TimestampType::CreateTime,
            max_message_bytes: 1_048_588,
            compacted: false,
            timestamp_before_max_ms: i64::MAX,
            timestamp_after_max_ms: DEFAULT_TIMESTAMP_AFTER_MAX_MS,
        }
    }

    #[test]
    fn append_assigns_offsets_and_keeps_the_producer_bytes_valid() {
        let mut log = PartitionLog::new();
        log.assign_epoch(3, 0);
        let a = log
            .append(&batch(&["a", "b"], 100), 3, 0, policy(), TP)
            .unwrap();
        let b = log.append(&batch(&["c"], 200), 3, 0, policy(), TP).unwrap();
        assert!(
            a == AppendInfo {
                first_offset: 0,
                last_offset: 1,
                log_append_time_ms: -1,
                duplicate: false
            }
        );
        assert!(
            b == AppendInfo {
                first_offset: 2,
                last_offset: 2,
                log_append_time_ms: -1,
                duplicate: false
            }
        );
        assert!(log.log_end_offset() == 3);
        let stored = &log.batches()[1];
        let mut cursor: &[u8] = &stored.bytes;
        let decoded = RecordBatch::decode(&mut cursor).unwrap();
        assert!(decoded.base_offset == 2);
        assert!(decoded.partition_leader_epoch == 3);
        assert!(decoded.records[0].value == Some(Bytes::from_static(b"c")));
    }

    #[test]
    fn read_returns_whole_batches_from_the_containing_one_below_the_bound() {
        let mut log = PartitionLog::new();
        log.append(&batch(&["a", "b"], 0), 0, 0, policy(), TP)
            .unwrap();
        log.append(&batch(&["c"], 0), 0, 0, policy(), TP).unwrap();
        log.append(&batch(&["d"], 0), 0, 0, policy(), TP).unwrap();
        log.set_high_watermark(3);
        let first = log.batches()[0].size();
        assert!(log.read(1, 1, 3).len() == first);
        assert!(log.read(1, usize::MAX, 3).len() == first + log.batches()[1].size());
        assert!(log.read(1, usize::MAX, 4).len() == log.size_bytes());
        assert!(log.read(3, usize::MAX, 3).is_empty());
        assert!(
            log.read(2, usize::MAX, 4).len() == log.batches()[1].size() + log.batches()[2].size()
        );
    }

    #[test]
    fn duplicate_sequences_answer_the_original_offsets_and_gaps_are_refused() {
        let mut log = PartitionLog::new();
        let first = log
            .append(&idempotent(&["a", "b"], 7, 0, 0), 0, 0, policy(), TP)
            .unwrap();
        let again = log
            .append(&idempotent(&["a", "b"], 7, 0, 0), 0, 0, policy(), TP)
            .unwrap();
        // Kafka reports the stored batch's timestamp for a duplicate, even
        // on a `CreateTime` topic.
        assert!(
            again
                == AppendInfo {
                    log_append_time_ms: 1,
                    duplicate: true,
                    ..first
                }
        );
        assert!(log.log_end_offset() == 2);
        for (case, epoch, sequence, expected) in [
            (
                "a gap",
                0,
                5,
                AppendError::OutOfOrderSequence {
                    producer_id: 7,
                    sequence: 5,
                },
            ),
            (
                "an older epoch",
                -1,
                2,
                AppendError::InvalidProducerEpoch {
                    producer_id: 7,
                    epoch: -1,
                    current: 0,
                },
            ),
            (
                "a new epoch that does not start at zero",
                1,
                3,
                AppendError::OutOfOrderSequence {
                    producer_id: 7,
                    sequence: 3,
                },
            ),
        ] {
            let refused = log
                .append(&idempotent(&["c"], 7, epoch, sequence), 0, 0, policy(), TP)
                .unwrap_err();
            assert!(refused == expected, "{case}");
        }
        let next = log
            .append(&idempotent(&["c"], 7, 0, 2), 0, 0, policy(), TP)
            .unwrap();
        assert!(next.first_offset == 2);
        let new_epoch = log
            .append(&idempotent(&["d"], 7, 1, 0), 0, 0, policy(), TP)
            .unwrap();
        assert!(new_epoch.first_offset == 3);
    }

    #[test]
    fn produce_framing_follows_kafka_validate_records() {
        let one = batch(&["a"], 0);
        let two = [one.as_ref(), one.as_ref()].concat();
        let trailing = [one.as_ref(), &one[..20]].concat();
        let mut legacy = one.to_vec();
        legacy[MAGIC_OFFSET] = 1;
        let mut zstd = one.to_vec();
        zstd[ATTRIBUTES_OFFSET + 1] |= ZSTD_COMPRESSION_ID;
        let mut short_length = one.to_vec();
        short_length[8..12].copy_from_slice(&3_i32.to_be_bytes());
        for (case, records, version, expected) in [
            ("one batch", one.to_vec(), 9, Ok(one.len())),
            (
                "a partial batch after it is dropped",
                trailing,
                9,
                Ok(one.len()),
            ),
            ("no batch", Vec::new(), 9, Err(AppendError::NoBatch(9))),
            (
                "half a batch",
                one[..30].to_vec(),
                9,
                Err(AppendError::NoBatch(9)),
            ),
            ("two batches", two, 9, Err(AppendError::NotOneBatch(9))),
            ("magic 1", legacy, 9, Err(AppendError::NotMagicV2(9))),
            (
                "zstd before v7",
                zstd.clone(),
                6,
                Err(AppendError::UnsupportedCompression),
            ),
            ("zstd from v7", zstd, 7, Ok(one.len())),
            (
                "a length below the record overhead",
                short_length,
                9,
                Err(AppendError::Corrupt(
                    "Record size 3 is less than the minimum record overhead (14)".to_string(),
                )),
            ),
        ] {
            assert!(single_batch(&records, version) == expected, "{case}");
        }
    }

    #[test]
    fn refusals_carry_kafka_codes_messages_and_log_start_rules() {
        let mut log = PartitionLog::new();
        let mut corrupt = BytesMut::from(batch(&["a"], 0).as_ref());
        corrupt[krabka_protocol::records::HEADER_LEN - 1] ^= 0xFF;
        let err = log
            .append(&corrupt.freeze(), 0, 0, policy(), TP)
            .unwrap_err();
        assert!(err.code() == codes::CORRUPT_MESSAGE);
        assert!(!err.reports_log_start() && err.wire_message().is_none());
        let mut based = BytesMut::from(batch(&["a"], 0).as_ref());
        based[..8].copy_from_slice(&5_i64.to_be_bytes());
        let err = log.append(&based.freeze(), 0, 0, policy(), TP).unwrap_err();
        assert!(
            err == AppendError::InvalidRecord(
                "The baseOffset of the record batch in the append to t-0 should be 0, but it is 5"
                    .to_string()
            )
        );
        assert!(err.reports_log_start() && err.wire_message().is_none());
        let small = AppendPolicy {
            max_message_bytes: 10,
            ..policy()
        };
        let err = log.append(&batch(&["a"], 0), 0, 0, small, TP).unwrap_err();
        assert!(err.code() == codes::MESSAGE_TOO_LARGE && !err.reports_log_start());
        let compact = AppendPolicy {
            compacted: true,
            ..policy()
        };
        let mut keyless = RecordBatch::default();
        keyless.records.push(Record {
            value: Some(Bytes::from_static(b"v")),
            ..Record::default()
        });
        let mut buf = BytesMut::new();
        keyless.encode(&mut buf).unwrap();
        let err = log.append(&buf.freeze(), 0, 0, compact, TP).unwrap_err();
        let key_error = RecordError {
            batch_index: 0,
            message: "Compacted topic cannot accept message without key in topic partition t-0."
                .to_string(),
        };
        assert!(err.code() == codes::INVALID_RECORD);
        assert!(err.record_errors() == [key_error]);
        assert!(
            err.wire_message().as_deref()
                == Some(
                    "One or more records have been rejected due to 1 record errors in total, and only showing the first three errors at most: [RecordError(batchIndex=0, message='Compacted topic cannot accept message without key in topic partition t-0.')]"
                )
        );
        assert!(log.log_end_offset() == 0);
    }

    #[test]
    fn create_time_records_far_from_the_broker_clock_are_refused() {
        let mut log = PartitionLog::new();
        let bounded = AppendPolicy {
            timestamp_before_max_ms: 1_000,
            ..policy()
        };
        let err = log
            .append(&batch(&["a", "b"], 3_600_001), 0, 0, policy(), TP)
            .unwrap_err();
        assert!(
            err == AppendError::Records {
                errors: vec![
                    RecordError {
                        batch_index: 0,
                        message: "Timestamp 3600001 of message with offset 0 is out of range. The timestamp should be within [-9223372036854775807, 3600000]".to_string(),
                    },
                    RecordError {
                        batch_index: 1,
                        message: "Timestamp 3600002 of message with offset 1 is out of range. The timestamp should be within [-9223372036854775807, 3600000]".to_string(),
                    },
                ],
                invalid_timestamp: true,
            }
        );
        assert!(err.code() == codes::INVALID_TIMESTAMP);
        assert!(
            err.wire_message().as_deref()
                == Some("One or more records have been rejected due to invalid timestamp")
        );
        let late = log
            .append(&batch(&["a"], 100), 0, 2_000, bounded, TP)
            .unwrap_err();
        assert!(late.code() == codes::INVALID_TIMESTAMP);
        assert!(
            log.append(&batch(&["a"], 1_500), 0, 2_000, bounded, TP)
                .is_ok()
        );
        let append_time = AppendPolicy {
            timestamp_type: TimestampType::LogAppendTime,
            ..bounded
        };
        assert!(
            log.append(&batch(&["a"], 100), 0, 2_000, append_time, TP)
                .is_ok()
        );
    }

    #[test]
    fn log_append_time_stamps_the_append_time() {
        let mut log = PartitionLog::new();
        let policy = AppendPolicy {
            timestamp_type: TimestampType::LogAppendTime,
            ..policy()
        };
        let info = log
            .append(&batch(&["a", "b"], 100), 0, 4_242, policy, TP)
            .unwrap();
        assert!(info.log_append_time_ms == 4_242);
        let mut cursor: &[u8] = &log.batches()[0].bytes;
        let decoded = RecordBatch::decode(&mut cursor).unwrap();
        assert!(decoded.max_timestamp == 4_242);
        assert!(decoded.attributes.timestamp_type() == TimestampType::LogAppendTime);
        assert!(log.list_offset(4_242, 2) == (4_242, 0, 0));
    }

    #[test]
    fn list_offset_resolves_every_sentinel_and_timestamps() {
        let mut log = PartitionLog::new();
        log.assign_epoch(0, 0);
        log.append(&batch(&["a", "b"], 100), 0, 0, policy(), TP)
            .unwrap();
        log.assign_epoch(1, 2);
        log.append(&batch(&["c"], 300), 1, 0, policy(), TP).unwrap();
        log.set_high_watermark(3);
        assert!(log.list_offset(LATEST_TIMESTAMP, 3) == (-1, 3, 1));
        assert!(log.list_offset(LATEST_TIMESTAMP, 2) == (-1, 2, 1));
        assert!(log.list_offset(EARLIEST_TIMESTAMP, 3) == (-1, 0, 0));
        assert!(log.list_offset(EARLIEST_LOCAL_TIMESTAMP, 3) == (-1, 0, 0));
        assert!(log.list_offset(MAX_TIMESTAMP, 3) == (300, 2, 1));
        assert!(log.list_offset(MAX_TIMESTAMP, 2) == (-1, -1, -1));
        assert!(log.list_offset(101, 3) == (101, 1, 0));
        assert!(log.list_offset(150, 3) == (300, 2, 1));
        assert!(log.list_offset(301, 3) == (-1, -1, -1));
        assert!(log.list_offset(0, 3) == (100, 0, 0));
    }

    #[test]
    fn epoch_cache_answers_kip_101_end_offsets() {
        let mut log = PartitionLog::new();
        assert!(log.end_offset_for_epoch(0) == (-1, -1));
        log.assign_epoch(0, 0);
        log.append(&batch(&["a", "b"], 0), 0, 0, policy(), TP)
            .unwrap();
        log.assign_epoch(2, 2);
        log.append(&batch(&["c"], 0), 2, 0, policy(), TP).unwrap();
        log.assign_epoch(5, 3);
        assert!(log.end_offset_for_epoch(0) == (0, 2));
        assert!(log.end_offset_for_epoch(1) == (0, 2));
        assert!(log.end_offset_for_epoch(2) == (2, 3));
        assert!(log.end_offset_for_epoch(5) == (5, 3));
        assert!(log.end_offset_for_epoch(4) == (2, 3));
        assert!(log.end_offset_for_epoch(6) == (-1, -1));
        assert!(log.end_offset_for_epoch(-1) == (-1, -1));
        assert!(log.end_offset_for_epoch(-2) == (-2, 0));
        let mut later = PartitionLog::new();
        later.assign_epoch(3, 0);
        later.append(&batch(&["a"], 0), 3, 0, policy(), TP).unwrap();
        later.assign_epoch(4, 1);
        // Older than every known epoch: the requested epoch, ending where the
        // first known one starts.
        assert!(later.end_offset_for_epoch(1) == (1, 0));
        assert!(log.epoch_for_offset(2) == 2);
        assert!(log.epoch_for_offset(1) == 0);
        log.truncate_to(2);
        assert!(log.log_end_offset() == 2);
        assert!(log.epoch_cache() == [(0, 0)]);
    }

    #[test]
    fn truncation_targets_follow_the_kafka_fetcher() {
        let mut log = PartitionLog::new();
        log.assign_epoch(0, 0);
        for value in ["a", "b", "c", "d", "e"] {
            log.append(&batch(&[value], 0), 0, 0, policy(), TP).unwrap();
        }
        log.assign_epoch(2, 5);
        for value in ["f", "g", "h"] {
            log.append(&batch(&[value], 0), 2, 0, policy(), TP).unwrap();
        }
        log.set_high_watermark(3);
        for (case, leader_epoch, leader_end, expected) in [
            ("leader has no end offset", -1, -1, (8, true)),
            ("leader answers no epoch", -1, 4, (4, true)),
            ("same latest epoch", 2, 6, (6, true)),
            ("epoch the follower never had", 1, 5, (5, false)),
            ("shared older epoch", 0, 3, (3, true)),
            ("epoch newer than any the follower has", 5, 20, (8, true)),
        ] {
            assert!(
                log.truncation_target(leader_epoch, leader_end) == expected,
                "{case}"
            );
        }
        assert!(PartitionLog::new().truncation_target(0, 7) == (0, true));
    }

    #[test]
    fn replicated_appends_keep_offsets_and_refuse_gaps() {
        let mut leader = PartitionLog::new();
        leader.assign_epoch(1, 0);
        leader
            .append(&batch(&["a"], 0), 1, 0, policy(), TP)
            .unwrap();
        leader
            .append(&batch(&["b", "c"], 0), 1, 0, policy(), TP)
            .unwrap();
        let bytes = leader.read(0, usize::MAX, leader.log_end_offset());
        let mut follower = PartitionLog::new();
        assert!(follower.append_replicated(&bytes).unwrap() == vec![(0, 0), (1, 2)]);
        assert!(follower.batches() == leader.batches());
        assert!(follower.epoch_cache() == [(1, 0)]);
        let err = follower.append_replicated(&bytes).unwrap_err();
        assert!(
            err == AppendError::OffsetMismatch {
                base_offset: 0,
                log_end_offset: 3
            }
        );
    }

    #[test]
    fn retention_deletes_below_the_high_watermark_only() {
        let mut log = PartitionLog::new();
        log.append(&batch(&["a"], 100), 0, 0, policy(), TP).unwrap();
        log.append(&batch(&["b"], 200), 0, 0, policy(), TP).unwrap();
        log.append(&batch(&["c"], 300), 0, 0, policy(), TP).unwrap();
        assert!(log.apply_retention(10_000, 50, -1) == 0);
        log.set_high_watermark(2);
        assert!(log.apply_retention(10_000, 50, -1) == 2);
        assert!(log.log_start_offset() == 2);
        assert!(log.batches().len() == 1);
        log.set_high_watermark(3);
        assert!(log.apply_retention(0, -1, 0) == 1);
        assert!(log.batches().is_empty());
        assert!(log.log_start_offset() == 3 && log.log_end_offset() == 3);
    }

    #[test]
    fn the_journal_and_the_checkpoint_rebuild_the_same_log() {
        let mut log = PartitionLog::new();
        log.assign_epoch(0, 0);
        log.append(&idempotent(&["a", "b"], 5, 0, 0), 0, 0, policy(), TP)
            .unwrap();
        log.append(&batch(&["c"], 7), 0, 0, policy(), TP).unwrap();
        log.append(&batch(&["d"], 8), 0, 0, policy(), TP).unwrap();
        log.set_high_watermark(4);
        log.increment_log_start_offset(2);
        log.truncate_to(3);
        assert!(
            log.take_journal()
                == vec![
                    LogChange::Appended(0),
                    LogChange::Appended(2),
                    LogChange::Appended(3),
                    LogChange::TruncatedBefore(2),
                    LogChange::TruncatedFrom(3),
                ]
        );
        assert!(log.take_journal().is_empty());
        let bytes: Vec<Bytes> = log.batches().iter().map(|b| b.bytes.clone()).collect();
        let restored = PartitionLog::restore(&bytes, Some(log.checkpoint()));
        assert!(restored == log);
        assert!(restored.batch_at(2).is_some() && restored.batch_at(0).is_none());
        let bare = PartitionLog::restore(&bytes, None);
        assert!(bare.log_start_offset() == 2 && bare.log_end_offset() == 3);
        assert!(bare.high_watermark() == 2);
        assert!(bare.epoch_cache() == [(0, 2)]);
    }

    #[test]
    fn full_truncation_and_log_start_moves() {
        let mut log = PartitionLog::new();
        log.assign_epoch(0, 0);
        log.append(&batch(&["a", "b"], 0), 0, 0, policy(), TP)
            .unwrap();
        log.append(&batch(&["c"], 0), 0, 0, policy(), TP).unwrap();
        log.set_high_watermark(3);
        log.increment_log_start_offset(1);
        assert!(log.log_start_offset() == 1);
        assert!(log.batches().len() == 2);
        assert!(log.epoch_cache() == [(0, 1)]);
        log.increment_log_start_offset(2);
        assert!(log.batches().len() == 1);
        log.truncate_fully_and_start_at(10);
        assert!(log.log_start_offset() == 10 && log.log_end_offset() == 10);
        assert!(log.high_watermark() == 10);
        assert!(log.batches().is_empty());
    }
}
