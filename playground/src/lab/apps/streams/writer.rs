//! The streams node's record collector: what the tasks emit, produced with
//! `acks=all`, in order per partition.
//!
//! Kafka Streams hands its tasks' output to one producer through its
//! `RecordCollector`. The lab's node owns a single Kafka client, so this
//! collector does the producer's part over it: it places each record on a
//! partition (the default partitioner's murmur2 of the key, a rotation for a
//! null key, or the task's own partition for a changelog record), keeps one
//! request in flight per partition so records land in order, retries the
//! errors Kafka's producer retries with the exponential backoff of
//! `retry.backoff.ms`, and reports whether everything emitted so far is
//! acknowledged, which a commit waits for.

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use serde_json::{Value, json};

use crate::lab::{
    client::{BatchRecord, RequestId, error_class, exponential_backoff, partition_for_key},
    codes,
    net::Millis,
};

/// The most records one produce request carries for a partition.
const MAX_BATCH_RECORDS: usize = 500;

/// `retry.backoff.ms` and `retry.backoff.max.ms`.
const RETRY_BACKOFF_MS: Millis = 100;
const RETRY_BACKOFF_MAX_MS: Millis = 1_000;

/// A record a task emitted.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Outgoing {
    pub topic: String,
    /// The partition, or `None` for the partitioner to choose.
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: i64,
}

/// A topic partition.
pub type Partition = (String, i32);

#[derive(Default)]
struct Queue {
    records: VecDeque<BatchRecord>,
    in_flight: bool,
    retry_at: Millis,
    attempts: u32,
}

/// What a produce answer did.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Settled {
    Acked {
        partition: Partition,
        records: usize,
    },
    Retrying {
        partition: Partition,
        code: i16,
    },
    Failed {
        partition: Partition,
        records: usize,
        code: i16,
    },
}

/// The collector. See the module documentation.
#[derive(Default)]
pub struct RecordWriter {
    /// Records whose topic has no known partition count yet.
    unplaced: VecDeque<Outgoing>,
    queues: BTreeMap<Partition, Queue>,
    in_flight: BTreeMap<RequestId, (Partition, Vec<BatchRecord>)>,
    /// The next partition for a null-key record, per topic.
    rotation: BTreeMap<String, i32>,
    acked: u64,
    failed: u64,
}

impl RecordWriter {
    /// Take a record to produce.
    pub fn push(&mut self, record: Outgoing) {
        self.unplaced.push_back(record);
    }

    /// Place the waiting records whose topic `partitions` knows, in order.
    /// Returns the topics still unknown, for the node to ask the metadata
    /// for.
    pub fn place(&mut self, partitions: impl Fn(&str) -> Option<i32>) -> Vec<String> {
        let mut unknown = Vec::new();
        let mut waiting = VecDeque::new();
        while let Some(record) = self.unplaced.pop_front() {
            let count = partitions(&record.topic).filter(|n| *n > 0);
            let partition = record.partition.or_else(|| {
                let count = count?;
                let keyed = record
                    .key
                    .as_deref()
                    .and_then(|k| partition_for_key(k, count));
                Some(keyed.unwrap_or_else(|| {
                    let next = self.rotation.entry(record.topic.clone()).or_insert(0);
                    let p = *next % count;
                    *next = (p + 1) % count;
                    p
                }))
            });
            if let Some(p) = partition {
                self.queues
                    .entry((record.topic.clone(), p))
                    .or_default()
                    .records
                    .push_back(BatchRecord {
                        timestamp: record.timestamp,
                        key: record.key,
                        value: record.value,
                        headers: Vec::new(),
                    });
            } else {
                if !unknown.contains(&record.topic) {
                    unknown.push(record.topic.clone());
                }
                waiting.push_back(record);
            }
        }
        self.unplaced = waiting;
        unknown
    }

    /// The next batch that may go out at `now`: a partition with records,
    /// nothing in flight, and its backoff passed.
    pub fn next_batch(&mut self, now: Millis) -> Option<(Partition, Vec<BatchRecord>)> {
        let (partition, queue) = self
            .queues
            .iter_mut()
            .find(|(_, q)| !q.in_flight && !q.records.is_empty() && now >= q.retry_at)?;
        let take = queue.records.len().min(MAX_BATCH_RECORDS);
        let batch: Vec<BatchRecord> = queue.records.drain(..take).collect();
        queue.in_flight = true;
        Some((partition.clone(), batch))
    }

    /// Record that `batch` went out as request `id`.
    pub fn sent(&mut self, id: RequestId, partition: Partition, batch: Vec<BatchRecord>) {
        self.in_flight.insert(id, (partition, batch));
    }

    /// Whether `id` is one of the collector's requests.
    #[must_use]
    pub fn owns(&self, id: RequestId) -> bool {
        self.in_flight.contains_key(&id)
    }

    /// Apply the answer to request `id`: the partition's error code, or
    /// `NETWORK_EXCEPTION` when the request failed on the way. `jitter` is a
    /// draw in `0..400` for the retry backoff.
    pub fn settle(
        &mut self,
        now: Millis,
        id: RequestId,
        code: i16,
        jitter: u64,
    ) -> Option<Settled> {
        let (partition, batch) = self.in_flight.remove(&id)?;
        let queue = self.queues.entry(partition.clone()).or_default();
        queue.in_flight = false;
        let records = batch.len();
        if code == codes::NONE {
            queue.attempts = 0;
            self.acked += u64::try_from(records).unwrap_or(u64::MAX);
            return Some(Settled::Acked { partition, records });
        }
        if error_class(code).is_retriable() {
            // The batch goes back to the front, so the partition's order holds.
            for record in batch.into_iter().rev() {
                queue.records.push_front(record);
            }
            queue.retry_at = now
                + exponential_backoff(
                    RETRY_BACKOFF_MS,
                    RETRY_BACKOFF_MAX_MS,
                    queue.attempts,
                    jitter,
                );
            queue.attempts = queue.attempts.saturating_add(1);
            return Some(Settled::Retrying { partition, code });
        }
        self.failed += u64::try_from(records).unwrap_or(u64::MAX);
        Some(Settled::Failed {
            partition,
            records,
            code,
        })
    }

    /// Whether every record pushed so far is acknowledged or failed.
    #[must_use]
    pub fn is_flushed(&self) -> bool {
        self.unplaced.is_empty()
            && self.in_flight.is_empty()
            && self.queues.values().all(|q| q.records.is_empty())
    }

    /// Records not yet acknowledged.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.unplaced.len()
            + self.queues.values().map(|q| q.records.len()).sum::<usize>()
            + self.in_flight.values().map(|(_, b)| b.len()).sum::<usize>()
    }

    /// When a batch that waits for its backoff may go.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Millis> {
        self.queues
            .values()
            .filter(|q| !q.in_flight && !q.records.is_empty())
            .map(|q| q.retry_at)
            .min()
    }

    /// Forget everything, as a restarted process has nothing in flight.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// The collector for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "pending": self.pending(),
            "in_flight_requests": self.in_flight.len(),
            "acked": self.acked,
            "failed": self.failed,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn record(topic: &str, partition: Option<i32>, key: Option<&str>, value: &str) -> Outgoing {
        Outgoing {
            topic: topic.to_string(),
            partition,
            key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            timestamp: 0,
        }
    }

    fn values(batch: &[BatchRecord]) -> Vec<&[u8]> {
        batch.iter().map(|r| r.value.as_deref().unwrap()).collect()
    }

    #[test]
    fn records_are_placed_by_key_by_rotation_or_as_given() {
        let mut w = RecordWriter::default();
        w.push(record("out", None, Some("a"), "1"));
        w.push(record("out", None, None, "2"));
        w.push(record("out", None, None, "3"));
        w.push(record("app-s-changelog", Some(2), Some("a"), "4"));
        w.push(record("missing", None, Some("a"), "5"));
        let unknown = w.place(|t| (t == "out").then_some(3));
        assert!(unknown == vec!["missing".to_string()]);
        let a = partition_for_key(b"a", 3).unwrap();
        let mut expected: BTreeMap<Partition, Vec<Vec<u8>>> = BTreeMap::new();
        // The keyed record goes where murmur2 puts it; the null keys rotate.
        for ((topic, partition), value) in [
            (("out", a), "1"),
            (("out", 0), "2"),
            (("out", 1), "3"),
            (("app-s-changelog", 2), "4"),
        ] {
            expected
                .entry((topic.to_string(), partition))
                .or_default()
                .push(value.as_bytes().to_vec());
        }
        let mut batches: BTreeMap<Partition, Vec<Vec<u8>>> = BTreeMap::new();
        while let Some((partition, batch)) = w.next_batch(0) {
            batches.insert(
                partition,
                values(&batch).iter().map(|v| v.to_vec()).collect(),
            );
        }
        assert!(batches == expected);
        assert!(!w.is_flushed());
    }

    #[test]
    fn one_request_per_partition_is_in_flight_and_a_retry_keeps_the_order() {
        let mut w = RecordWriter::default();
        for v in ["1", "2"] {
            w.push(record("t", Some(0), None, v));
        }
        w.place(|_| Some(1));
        let (p, batch) = w.next_batch(0).unwrap();
        assert!(values(&batch) == vec![&b"1"[..], b"2"]);
        w.sent(RequestId(7), p.clone(), batch);
        w.push(record("t", Some(0), None, "3"));
        w.place(|_| Some(1));
        // The partition has a request in flight.
        assert!(w.next_batch(0).is_none());
        assert!(
            w.settle(10, RequestId(7), codes::NOT_LEADER_OR_FOLLOWER, 200)
                == Some(Settled::Retrying {
                    partition: p.clone(),
                    code: codes::NOT_LEADER_OR_FOLLOWER
                })
        );
        assert!(w.next_batch(50).is_none());
        assert!(w.next_deadline() == Some(110));
        let (_, batch) = w.next_batch(110).unwrap();
        assert!(values(&batch) == vec![&b"1"[..], b"2", b"3"]);
        w.sent(RequestId(8), p.clone(), batch);
        assert!(
            w.settle(120, RequestId(8), codes::NONE, 200)
                == Some(Settled::Acked {
                    partition: p,
                    records: 3
                })
        );
        assert!(w.is_flushed());
        assert!(
            w.snapshot()
                == json!({ "pending": 0, "in_flight_requests": 0, "acked": 3, "failed": 0 })
        );
    }

    #[test]
    fn a_fatal_error_drops_the_batch() {
        let mut w = RecordWriter::default();
        w.push(record("t", Some(0), None, "x"));
        w.place(|_| Some(1));
        let (p, batch) = w.next_batch(0).unwrap();
        w.sent(RequestId(1), p.clone(), batch);
        assert!(
            w.settle(1, RequestId(1), codes::MESSAGE_TOO_LARGE, 200)
                == Some(Settled::Failed {
                    partition: p,
                    records: 1,
                    code: codes::MESSAGE_TOO_LARGE
                })
        );
        assert!(w.is_flushed());
        assert!(w.settle(1, RequestId(1), codes::NONE, 200).is_none());
    }
}
