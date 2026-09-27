//! The replicated metadata log: one entry per committed or in-flight batch of
//! metadata records, stamped with the leader epoch that appended it.
//!
//! The offset of an entry is its index, so the log is the `LogView` the
//! consensus core reads through, and the leader-change marker the core appends
//! at promotion is an ordinary entry with no records. Offsets therefore mean
//! the same thing on every node, and a broker can name a committed batch by
//! its offset.

use krabka_kraft_core::{
    event::LogEnd,
    types::{Epoch, LogOffsetMetadata, LogView},
};
use krabka_metadata::MetadataRecord;
use serde::{Deserialize, Serialize};

/// One log entry: the metadata records a leader appended as one batch, and
/// the epoch of that leader. A leader-change marker carries no records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The leader epoch that appended the entry.
    pub epoch: Epoch,
    /// The records of the batch, applied in order.
    pub records: Vec<MetadataRecord>,
}

/// The in-memory replicated log of one controller.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetadataLog {
    entries: Vec<Entry>,
    /// `epochs[i]` is the epoch of the entry at offset `i`, kept beside the
    /// entries so the divergence lookup does not allocate.
    epochs: Vec<Epoch>,
}

impl LogView for MetadataLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.entries.len()).unwrap_or(i64::MAX)
    }

    fn last_epoch(&self) -> Epoch {
        self.epochs.last().copied().unwrap_or(0)
    }

    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata {
        LogOffsetMetadata::end_of_epoch_in(&self.epochs, epoch)
    }
}

impl MetadataLog {
    /// Append one entry and return its offset.
    pub fn append(&mut self, epoch: Epoch, records: Vec<MetadataRecord>) -> i64 {
        let offset = self.end_offset();
        self.entries.push(Entry { epoch, records });
        self.epochs.push(epoch);
        offset
    }

    /// Append entries a leader sent, starting at `start_offset`. The entries
    /// are ignored when `start_offset` is not this log's end: a stale or
    /// duplicated response must not create a gap or overwrite an entry.
    /// Returns whether the log grew.
    pub fn append_from(&mut self, start_offset: i64, entries: &[Entry]) -> bool {
        if start_offset != self.end_offset() || entries.is_empty() {
            return false;
        }
        for entry in entries {
            self.epochs.push(entry.epoch);
            self.entries.push(entry.clone());
        }
        true
    }

    /// Drop every entry at or after `offset`.
    pub fn truncate_to(&mut self, offset: i64) {
        let keep = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
        if keep < self.entries.len() {
            self.entries.truncate(keep);
            self.epochs.truncate(keep);
        }
    }

    /// The entries from `offset` onwards, at most `max` of them.
    #[must_use]
    pub fn entries_from(&self, offset: i64, max: usize) -> &[Entry] {
        let start = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
        if start >= self.entries.len() {
            return &[];
        }
        let end = start.saturating_add(max).min(self.entries.len());
        &self.entries[start..end]
    }

    /// Every entry, in offset order.
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// The number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The tip of the log as a candidate advertises it in a vote request.
    #[must_use]
    pub fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{DeleteTopicRecord, MetadataRecord};

    use super::*;

    fn delete(name: &str) -> Vec<MetadataRecord> {
        vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: name.into(),
        })]
    }

    #[test]
    fn offsets_are_indexes_and_epochs_follow_the_entries() {
        let mut log = MetadataLog::default();
        assert!(log.end_offset() == 0);
        assert!(log.last_epoch() == 0);
        assert!(log.append(1, vec![]) == 0);
        assert!(log.append(1, delete("a")) == 1);
        assert!(log.append(3, delete("b")) == 2);
        assert!(log.end_offset() == 3);
        assert!(log.last_epoch() == 3);
        assert!(
            log.log_end()
                == LogEnd {
                    last_epoch: 3,
                    last_offset: 3
                }
        );
        // Kafka's `endOffsetForEpoch`: epoch 1 ends where epoch 3 starts, an
        // epoch the log skipped answers the largest one below it, and an epoch
        // past the tip answers the tip with the last epoch.
        let cases = [
            (1, (2, 1)),
            (2, (2, 1)),
            (3, (3, 3)),
            (7, (3, 3)),
            (0, (0, 0)),
        ];
        for (epoch, (offset, found)) in cases {
            assert!(
                log.end_offset_for_epoch(epoch)
                    == LogOffsetMetadata {
                        offset,
                        epoch: found
                    },
                "epoch {epoch}"
            );
        }
    }

    #[test]
    fn append_from_only_extends_the_tip() {
        let mut log = MetadataLog::default();
        log.append(1, vec![]);
        let entries = vec![
            Entry {
                epoch: 1,
                records: delete("a"),
            },
            Entry {
                epoch: 2,
                records: vec![],
            },
        ];
        assert!(!log.append_from(0, &entries));
        assert!(!log.append_from(2, &entries));
        assert!(!log.append_from(1, &[]));
        assert!(log.append_from(1, &entries));
        assert!(log.len() == 3);
        assert!(log.entries_from(1, 10) == &entries[..]);
        assert!(log.entries_from(1, 1) == &entries[..1]);
        assert!(log.entries_from(3, 10).is_empty());
        assert!(log.entries_from(-1, 10).len() == 3);
    }

    #[test]
    fn truncation_drops_the_tail_and_the_epochs_with_it() {
        let mut log = MetadataLog::default();
        log.append(1, vec![]);
        log.append(2, delete("a"));
        log.append(2, delete("b"));
        log.truncate_to(5);
        assert!(log.len() == 3);
        log.truncate_to(1);
        assert!(log.len() == 1);
        assert!(log.last_epoch() == 1);
        log.truncate_to(-4);
        assert!(log.is_empty());
        assert!(log.last_epoch() == 0);
    }
}
