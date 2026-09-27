//! The `_schemas` log behind the registry, and its in-memory implementation.
//!
//! The real registry writes every change as a record of a compacted Kafka
//! topic and applies it to its state only when its own reader fetches the
//! record back. The registry node keeps that shape: it appends through
//! [`SchemaStore::append`], then folds whatever [`SchemaStore::replay`]
//! returns into its state, and answers the request only once the record's
//! offset is applied. [`SchemaLog`] is the in-memory store of this batch; the
//! read-back is immediate, and a [`Fault::Wipe`] loses it. The Kafka-backed
//! store of the next batch produces to the brokers and fetches back, and
//! nothing above this trait changes.
//!
//! [`Fault::Wipe`]: crate::lab::world::Fault::Wipe

use super::{ids::LogOffset, record::RawRecord};

/// The durable log of `_schemas` records.
pub trait SchemaStore {
    /// Append a record and return the offset it takes.
    fn append(&mut self, record: RawRecord) -> LogOffset;

    /// Every record at or after `from`, in offset order: the reader's tail.
    fn replay(&self, from: LogOffset) -> Vec<(LogOffset, RawRecord)>;

    /// The offset the next appended record takes.
    fn end_offset(&self) -> LogOffset;
}

impl SchemaStore for Box<dyn SchemaStore> {
    fn append(&mut self, record: RawRecord) -> LogOffset {
        self.as_mut().append(record)
    }

    fn replay(&self, from: LogOffset) -> Vec<(LogOffset, RawRecord)> {
        self.as_ref().replay(from)
    }

    fn end_offset(&self) -> LogOffset {
        self.as_ref().end_offset()
    }
}

/// An in-memory `_schemas` log: a vector of records, offset by position.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SchemaLog {
    records: Vec<RawRecord>,
}

impl SchemaLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every record, oldest first.
    #[must_use]
    pub fn records(&self) -> &[RawRecord] {
        &self.records
    }

    fn position(offset: LogOffset) -> usize {
        usize::try_from(offset.0).unwrap_or(0)
    }
}

impl SchemaStore for SchemaLog {
    fn append(&mut self, record: RawRecord) -> LogOffset {
        let offset = self.end_offset();
        self.records.push(record);
        offset
    }

    fn replay(&self, from: LogOffset) -> Vec<(LogOffset, RawRecord)> {
        let start = Self::position(from).min(self.records.len());
        self.records[start..]
            .iter()
            .enumerate()
            .map(|(i, record)| {
                let position = i64::try_from(start + i).unwrap_or(i64::MAX);
                (LogOffset(position), record.clone())
            })
            .collect()
    }

    fn end_offset(&self) -> LogOffset {
        LogOffset(i64::try_from(self.records.len()).unwrap_or(i64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;

    fn record(key: &'static str) -> RawRecord {
        RawRecord {
            key: Bytes::from_static(key.as_bytes()),
            value: None,
        }
    }

    #[test]
    fn log_appends_in_order_and_replays_from_an_offset() {
        let mut log = SchemaLog::new();
        assert!(log.end_offset() == LogOffset(0));
        assert!(log.append(record("a")) == LogOffset(0));
        assert!(log.append(record("b")) == LogOffset(1));
        assert!(log.append(record("c")) == LogOffset(2));
        assert!(log.end_offset() == LogOffset(3));
        assert!(
            log.replay(LogOffset(1))
                == vec![(LogOffset(1), record("b")), (LogOffset(2), record("c"))]
        );
        assert!(log.replay(LogOffset(3)).is_empty());
        assert!(log.replay(LogOffset(99)).is_empty());
        assert!(log.records().len() == 3);
        let mut boxed: Box<dyn SchemaStore> = Box::new(log);
        assert!(boxed.append(record("d")) == LogOffset(3));
        assert!(boxed.end_offset() == LogOffset(4));
        assert!(boxed.replay(LogOffset(3)) == vec![(LogOffset(3), record("d"))]);
    }
}
