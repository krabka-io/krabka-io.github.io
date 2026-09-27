//! The reader: Confluent's `KafkaStoreReaderThread`.
//!
//! The Confluent reader is a `KafkaConsumer` with client id
//! `KafkaStore-reader-<topic>`, `auto.offset.reset=earliest` and no offset
//! commits, assigned partition 0 of the topic by hand (no group) and sought
//! to the beginning. It polls forever and applies every record to the local
//! store, moving `offsetInSchemasTopic` to the offset of the record. The
//! reader here is the same over the lab's [`Consumer`] with the consumer's
//! defaults otherwise: [`Consumer::assign`], [`Consumer::seek_to_beginning`]
//! (a `ListOffsets` for the earliest offset), then a `Fetch` to the
//! partition leader, the next one sent as soon as the records of the last
//! are polled. Errors follow the consumer: `OFFSET_OUT_OF_RANGE` resets to
//! the beginning, a routing error adopts the leader the answer names and
//! refreshes the metadata, and a lost answer is retried after
//! `retry.backoff.ms`.
//!
//! Before it is assigned the reader looks the topic up, Confluent's
//! `partitionsFor`: the consumer's client asks the metadata for it.

use serde_json::{Value, json};

use crate::lab::{
    client::{
        AutoOffsetReset, Consumer, ConsumerConfig, ConsumerError, ConsumerEvent, KafkaClient,
    },
    net::{ConnId, Ctx, Frame, Millis},
    registry::record::RawRecord,
};

/// The partition the store lives on: the topic has exactly one.
const PARTITION: i32 = 0;

/// The consumer of `_schemas`.
pub struct Reader {
    consumer: Consumer,
    topic: String,
    /// Assigned and sought to the beginning.
    started: bool,
    /// Confluent's `offsetInSchemasTopic`: the offset of the last record
    /// read, `-1` before the first.
    offset: i64,
    /// The code of the last error the consumer reported.
    last_error: Option<i16>,
}

impl Reader {
    /// A reader of `topic` over `client`: a consumer with no group,
    /// `auto.offset.reset=earliest` and auto-commit off.
    #[must_use]
    pub fn new(client: KafkaClient, topic: &str) -> Self {
        let config = ConsumerConfig {
            group_id: String::new(),
            auto_offset_reset: AutoOffsetReset::Earliest,
            enable_auto_commit: false,
            ..ConsumerConfig::default()
        };
        Self {
            consumer: Consumer::new(client, config),
            topic: topic.to_string(),
            started: false,
            offset: -1,
            last_error: None,
        }
    }

    /// Whether a connection id belongs to the reader's client.
    #[must_use]
    pub fn owns_conn(&self, conn: ConnId) -> bool {
        self.consumer.client().owns_conn(conn)
    }

    /// The offset of the last record read, `-1` before the first.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Counts the metadata answers the reader's client applied, so the
    /// store sees the answer to a look-up.
    #[must_use]
    pub fn metadata_version(&self) -> u64 {
        self.consumer.client().metadata().version
    }

    /// Ask the metadata for the topic's partitions: `partitionsFor`.
    pub fn look_up(&mut self) {
        let client = self.consumer.client_mut();
        client.add_topics([self.topic.as_str()]);
        client.request_metadata_refresh();
    }

    /// The topic's partition count, once the metadata knows the topic.
    #[must_use]
    pub fn partitions(&self) -> Option<usize> {
        self.consumer
            .client()
            .metadata()
            .topics
            .get(&self.topic)
            .map(|t| t.partitions.len())
    }

    /// Assign partition 0 and seek to the beginning: the next tick asks for
    /// the earliest offset.
    ///
    /// # Errors
    /// What the consumer refuses; a closed consumer is the only case.
    pub fn start(&mut self, ctx: &mut Ctx<'_>) -> Result<(), ConsumerError> {
        let partition = [(self.topic.as_str(), PARTITION)];
        self.consumer.assign(ctx, &partition)?;
        self.consumer.seek_to_beginning(&[])?;
        self.started = true;
        Ok(())
    }

    /// A frame for the reader's client. Returns the records read, each with
    /// its offset, in offset order.
    pub fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) -> Vec<(i64, RawRecord)> {
        let (events, _) = self.consumer.on_frame(ctx, frame);
        self.note(&events);
        self.poll(ctx)
    }

    /// The timer fired: drive the consumer. Returns the records read.
    pub fn on_tick(&mut self, ctx: &mut Ctx<'_>) -> Vec<(i64, RawRecord)> {
        let (events, _) = self.consumer.on_tick(ctx);
        self.note(&events);
        self.poll(ctx)
    }

    fn note(&mut self, events: &[ConsumerEvent]) {
        if let Some(code) = events.iter().rev().find_map(|e| match e {
            ConsumerEvent::Error { code, .. } => Some(*code),
            _ => None,
        }) {
            self.last_error = Some(code);
        }
    }

    /// Poll until the buffer is empty, as the reader thread polls in a
    /// loop; each poll sends the next fetch once the records ran out.
    fn poll(&mut self, ctx: &mut Ctx<'_>) -> Vec<(i64, RawRecord)> {
        let mut read = Vec::new();
        if !self.started {
            return read;
        }
        loop {
            let records = self.consumer.poll_at(ctx, usize::MAX);
            if records.is_empty() {
                return read;
            }
            for record in records {
                self.offset = record.offset;
                read.push((
                    record.offset,
                    RawRecord {
                        key: record.key.unwrap_or_default(),
                        value: record.value,
                    },
                ));
            }
        }
    }

    /// Close the consumer and its client.
    pub fn close(&mut self, ctx: &mut Ctx<'_>) {
        self.consumer.close(ctx);
        self.consumer.client_mut().close(ctx);
    }

    /// The next time the reader needs a tick: the consumer's.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        self.consumer.next_deadline(now)
    }

    /// The reader for the inspector: `offset` is the last record read,
    /// `end_offset` the high watermark of the last fetch and `position` the
    /// next offset to read, each `null` until known.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let position = self.consumer.position(&self.topic, PARTITION);
        let phase = match (self.started, position) {
            (false, _) => "idle",
            (true, None) => "resetting",
            (true, Some(_)) => "fetching",
        };
        json!({
            "phase": phase,
            "offset": self.offset,
            "end_offset": self.consumer.high_watermark(&self.topic, PARTITION),
            "position": position,
            "leader": self.consumer.client().metadata().leader(&self.topic, PARTITION),
            "fetches": self.consumer.metrics().fetches,
            "last_error": self.last_error,
            "consumer": self.consumer.snapshot(),
        })
    }
}
