//! Record batches on the client side: a v2 batch for a produce request, and
//! the records of the batches a fetch returned.

use std::collections::BTreeSet;

use bytes::Bytes;
use krabka_protocol::{
    owned::fetch_response::AbortedTransaction,
    records::{Attributes, Record, RecordBatch, RecordHeader},
};

/// One record to put in a batch.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BatchRecord {
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

/// The idempotent producer's stamp on a batch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProducerStamp {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

/// A v2 batch of `records` with `CreateTime` timestamps and the compression
/// bits `compression` (0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd) in its
/// attributes; the codec compresses the records when the batch is encoded. The
/// broker assigns the base offset and the leader epoch.
#[must_use]
pub fn build_batch(
    records: &[BatchRecord],
    stamp: Option<ProducerStamp>,
    compression: i16,
) -> RecordBatch {
    let base_timestamp = records.first().map_or(0, |r| r.timestamp);
    let max_timestamp = records.iter().map(|r| r.timestamp).max().unwrap_or(0);
    let last_offset_delta = i32::try_from(records.len().saturating_sub(1)).unwrap_or(i32::MAX);
    let encoded = records
        .iter()
        .enumerate()
        .map(|(i, r)| Record {
            attributes: 0,
            timestamp_delta: r.timestamp - base_timestamp,
            offset_delta: i32::try_from(i).unwrap_or(i32::MAX),
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r.headers.clone(),
        })
        .collect();
    let (producer_id, producer_epoch, base_sequence) = stamp.map_or((-1, -1, -1), |s| {
        (s.producer_id, s.producer_epoch, s.base_sequence)
    });
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: Attributes(compression & 0b111),
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records: encoded,
    }
}

/// One record a consumer received.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConsumedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    /// The leader epoch of the batch, for the committed offset.
    pub leader_epoch: i32,
}

/// The records of fetched `batches` from `from_offset` on, and the offset to
/// fetch next. A control batch (a transaction marker) is skipped, as Kafka's
/// consumer skips it, and so is a record before `from_offset` in a batch that
/// straddles it.
///
/// `aborted` is the `aborted_transactions` list of a `read_committed` fetch,
/// `None` for `read_uncommitted`. With it the batches of aborted
/// transactions are skipped as Kafka's `CompletedFetch` skips them: a
/// producer's aborted transaction counts from its first offset on, until the
/// producer's abort marker.
#[must_use]
pub fn records_of(
    topic: &str,
    partition: i32,
    batches: &[RecordBatch],
    from_offset: i64,
    aborted: Option<&[AbortedTransaction]>,
) -> (Vec<ConsumedRecord>, Option<i64>) {
    let mut records = Vec::new();
    let mut next = None;
    let mut pending: Vec<(i64, i64)> = aborted
        .unwrap_or_default()
        .iter()
        .map(|t| (t.first_offset, t.producer_id))
        .collect();
    pending.sort_unstable();
    let mut pending = pending.into_iter().peekable();
    let mut aborting: BTreeSet<i64> = BTreeSet::new();
    for batch in batches {
        let last = batch.base_offset + i64::from(batch.last_offset_delta);
        next = Some(next.map_or(last + 1, |n: i64| n.max(last + 1)));
        if aborted.is_some() && batch.producer_id >= 0 {
            while let Some((_, producer_id)) = pending.next_if(|(first, _)| *first <= last) {
                aborting.insert(producer_id);
            }
            if batch.attributes.is_control_batch() {
                if is_abort_marker(batch) {
                    aborting.remove(&batch.producer_id);
                }
            } else if batch.attributes.is_transactional() && aborting.contains(&batch.producer_id) {
                continue;
            }
        }
        if batch.attributes.is_control_batch() || last < from_offset {
            continue;
        }
        for record in &batch.records {
            let offset = batch.base_offset + i64::from(record.offset_delta);
            if offset < from_offset {
                continue;
            }
            records.push(ConsumedRecord {
                topic: topic.to_string(),
                partition,
                offset,
                timestamp: batch.base_timestamp + record.timestamp_delta,
                key: record.key.clone(),
                value: record.value.clone(),
                headers: record.headers.clone(),
                leader_epoch: batch.partition_leader_epoch,
            });
        }
    }
    (records, next)
}

/// Whether a control batch is an abort marker: its record's key is the
/// control record version and type 0, `ABORT` (type 1 is `COMMIT`).
fn is_abort_marker(batch: &RecordBatch) -> bool {
    batch
        .records
        .first()
        .and_then(|r| r.key.as_deref())
        .is_some_and(|key| key.len() >= 4 && key[2..4] == [0, 0])
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;

    use super::*;

    fn record(timestamp: i64, value: &'static str) -> BatchRecord {
        BatchRecord {
            timestamp,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(value.as_bytes())),
            headers: vec![RecordHeader {
                key: "h".to_string(),
                value: Some(Bytes::from_static(b"v")),
            }],
        }
    }

    #[test]
    fn a_built_batch_round_trips_through_the_codec_with_its_stamp() {
        let batch = build_batch(
            &[record(1_000, "a"), record(1_005, "b"), record(1_002, "c")],
            Some(ProducerStamp {
                producer_id: 42,
                producer_epoch: 3,
                base_sequence: 17,
            }),
            0,
        );
        assert!(batch.base_timestamp == 1_000);
        assert!(batch.max_timestamp == 1_005);
        assert!(batch.last_offset_delta == 2);
        assert!(batch.base_sequence == 17);
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();
        let mut cursor: &[u8] = &buf;
        let decoded = RecordBatch::decode(&mut cursor).unwrap();
        assert!(decoded == batch);
    }

    #[test]
    fn records_skip_control_batches_and_offsets_before_the_fetch_position() {
        let mut first = build_batch(&[record(1, "a"), record(2, "b"), record(3, "c")], None, 0);
        first.base_offset = 10;
        first.partition_leader_epoch = 4;
        let mut marker = build_batch(&[record(9, "")], None, 0);
        marker.base_offset = 13;
        marker.attributes = marker.attributes.with_control(true);
        let mut second = build_batch(&[record(5, "d")], None, 0);
        second.base_offset = 14;
        let (records, next) = records_of("t", 0, &[first, marker, second], 12, None);
        let values: Vec<(i64, i64, i32, &[u8])> = records
            .iter()
            .map(|r| {
                (
                    r.offset,
                    r.timestamp,
                    r.leader_epoch,
                    r.value.as_deref().unwrap_or_default(),
                )
            })
            .collect();
        assert!(values == vec![(12, 3, 4, b"c".as_slice()), (14, 5, -1, b"d".as_slice())]);
        assert!(next == Some(15));
        assert!(records_of("t", 0, &[], 0, None) == (Vec::new(), None));
    }

    /// A transactional batch of `producer_id` at `base_offset`, or its
    /// marker: an abort with `Some(false)`, a commit with `Some(true)`.
    fn txn_batch(producer_id: i64, base_offset: i64, marker: Option<bool>) -> RecordBatch {
        let stamp = ProducerStamp {
            producer_id,
            producer_epoch: 0,
            base_sequence: 0,
        };
        let mut batch = match marker {
            None => build_batch(&[record(1, "x")], Some(stamp), 0),
            Some(commit) => {
                let mut r = record(1, "");
                r.key = Some(Bytes::from(vec![0, 0, 0, u8::from(commit)]));
                let mut b = build_batch(&[r], Some(stamp), 0);
                b.attributes = b.attributes.with_control(true);
                b
            }
        };
        batch.attributes = batch.attributes.with_transactional(true);
        batch.base_offset = base_offset;
        batch
    }

    #[test]
    fn read_committed_drops_aborted_transactions_up_to_their_markers() {
        // Producer 7 aborts at 0..=1 and commits at 3..=4; producer 8's
        // transaction at 2 commits; a plain batch at 6.
        let batches = vec![
            txn_batch(7, 0, None),
            txn_batch(8, 2, None),
            txn_batch(7, 1, Some(false)),
            txn_batch(7, 3, None),
            txn_batch(7, 4, Some(true)),
            txn_batch(8, 5, Some(true)),
            build_batch(&[record(1, "p")], None, 0),
        ];
        let mut batches = batches;
        batches[6].base_offset = 6;
        let aborted = [AbortedTransaction {
            producer_id: 7,
            first_offset: 0,
            ..Default::default()
        }];
        let offsets = |aborted| {
            let (records, next) = records_of("t", 0, &batches, 0, aborted);
            (records.iter().map(|r| r.offset).collect::<Vec<_>>(), next)
        };
        assert!(offsets(Some(&aborted[..])) == (vec![2, 3, 6], Some(7)));
        assert!(offsets(None) == (vec![0, 2, 3, 6], Some(7)));
    }
}
