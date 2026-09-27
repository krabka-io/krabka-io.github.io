//! The reader: Confluent's `KafkaStoreReaderThread` as a fetch loop.
//!
//! The Confluent reader is a `KafkaConsumer` with client id
//! `KafkaStore-reader-<topic>`, `auto.offset.reset=earliest` and no offset
//! commits, assigned partition 0 of the topic by hand (no group) and sought
//! to the beginning. It polls forever and applies every record to the local
//! store, moving `offsetInSchemasTopic` to the offset of the record. The
//! reader here does the same over a [`KafkaClient`]: a `ListOffsets` for the
//! earliest offset, then one `Fetch` at a time to the partition leader with
//! the consumer's defaults (`fetch.max.wait.ms` 500, `fetch.min.bytes` 1,
//! `max.partition.fetch.bytes` 1 MiB, no fetch session), the next one sent as
//! soon as the last one answered. An error follows the consumer: a routing
//! error adopts the leader the answer names and refreshes the metadata,
//! `OFFSET_OUT_OF_RANGE` seeks to the beginning again, and a lost answer is
//! retried after `retry.backoff.ms`.

use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
    },
    primitives::uuid::Uuid,
    records::RecordsPayload,
};
use serde_json::{Value, json};

use crate::lab::{
    client::{ClientError, ClientEvent, KafkaClient, RequestId, Response, Target, records_of},
    codes,
    net::{Ctx, Millis},
    registry::{lane::Lane, record::RawRecord},
};

/// `ListOffsets` timestamp of the earliest offset.
const EARLIEST_TIMESTAMP: i64 = -2;
/// The consumer's `retry.backoff.ms`.
const RETRY_BACKOFF_MS: Millis = 100;
/// The consumer's `fetch.max.wait.ms`.
const FETCH_MAX_WAIT_MS: i32 = 500;
/// The consumer's `fetch.min.bytes`.
const FETCH_MIN_BYTES: i32 = 1;
/// The consumer's `fetch.max.bytes`.
const FETCH_MAX_BYTES: i32 = 52_428_800;
/// The consumer's `max.partition.fetch.bytes`.
const MAX_PARTITION_FETCH_BYTES: i32 = 1_048_576;
/// The partition the store lives on: the topic has exactly one.
const PARTITION: i32 = 0;

/// Where the reader is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    /// Not started: the store is still setting the topic up.
    Idle,
    /// Seeking to the beginning with `ListOffsets`.
    Resetting,
    /// Fetching from `position`.
    Fetching,
}

impl Phase {
    const fn name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Resetting => "resetting",
            Self::Fetching => "fetching",
        }
    }
}

/// The request the reader has out.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Out {
    ListOffsets(RequestId),
    Fetch(RequestId),
}

/// The fetch loop over `_schemas`.
pub struct Reader {
    lane: Lane<KafkaClient>,
    topic: String,
    phase: Phase,
    /// Confluent's `offsetInSchemasTopic`: the offset of the last record
    /// read, `-1` before the first.
    offset: i64,
    /// The offset the next fetch asks for.
    position: i64,
    /// The high watermark the last fetch reported: the end of the topic as
    /// a consumer sees it.
    high_watermark: i64,
    out: Option<Out>,
    retry_at: Millis,
    fetches: u64,
    last_error: Option<i16>,
}

impl Reader {
    /// A reader of `topic` over the client on `lane`.
    #[must_use]
    pub fn new(lane: Lane<KafkaClient>, topic: &str) -> Self {
        Self {
            lane,
            topic: topic.to_string(),
            phase: Phase::Idle,
            offset: -1,
            position: 0,
            high_watermark: -1,
            out: None,
            retry_at: 0,
            fetches: 0,
            last_error: None,
        }
    }

    /// The lane of the reader's client, for routing frames.
    #[must_use]
    pub fn lane(&self) -> &Lane<KafkaClient> {
        &self.lane
    }

    /// The lane of the reader's client.
    pub fn lane_mut(&mut self) -> &mut Lane<KafkaClient> {
        &mut self.lane
    }

    /// The offset of the last record read, `-1` before the first.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Ask the metadata for the topic's partitions: `partitionsFor`.
    pub fn look_up(&mut self) {
        let client = self.lane.get_mut();
        client.add_topics([self.topic.as_str()]);
        client.request_metadata_refresh();
    }

    /// The topic's partition count, once the metadata knows the topic.
    #[must_use]
    pub fn partitions(&self) -> Option<usize> {
        self.lane
            .get()
            .metadata()
            .topics
            .get(&self.topic)
            .map(|t| t.partitions.len())
    }

    /// Assign partition 0 and seek to the beginning.
    pub fn start(&mut self, now: Millis) {
        self.phase = Phase::Resetting;
        self.retry_at = now;
    }

    /// Apply the client's events: the answers to the reader's requests.
    /// Returns the records read, each with its offset, in offset order.
    pub fn on_events(&mut self, now: Millis, events: Vec<ClientEvent>) -> Vec<(i64, RawRecord)> {
        let mut read = Vec::new();
        for event in events {
            let ClientEvent::Response { id, result } = event else {
                continue;
            };
            match self.out {
                Some(Out::ListOffsets(out)) if out == id => {
                    self.out = None;
                    self.on_list_offsets(now, result);
                }
                Some(Out::Fetch(out)) if out == id => {
                    self.out = None;
                    read.extend(self.on_fetch(now, result));
                }
                _ => {}
            }
        }
        read
    }

    fn fail(&mut self, now: Millis, code: i16) {
        self.last_error = Some(code);
        self.retry_at = now + RETRY_BACKOFF_MS;
    }

    fn note_error(&mut self, code: i16) {
        let target = Target::Leader {
            topic: self.topic.clone(),
            partition: PARTITION,
        };
        self.lane.get_mut().note_error(code, &target);
    }

    fn on_list_offsets(&mut self, now: Millis, result: Result<Response, ClientError>) {
        let row = result
            .ok()
            .and_then(Response::downcast::<ListOffsetsResponse>)
            .and_then(|r| {
                r.topics
                    .iter()
                    .find(|t| t.name == self.topic)
                    .and_then(|t| t.partitions.iter().find(|p| p.partition_index == PARTITION))
                    .map(|p| (p.error_code, p.offset))
            });
        match row {
            Some((codes::NONE, offset)) if offset >= 0 => {
                self.position = offset;
                self.phase = Phase::Fetching;
                self.retry_at = now;
            }
            Some((code, _)) => {
                self.note_error(code);
                self.fail(now, code);
            }
            None => self.fail(now, codes::NETWORK_EXCEPTION),
        }
    }

    fn on_fetch(
        &mut self,
        now: Millis,
        result: Result<Response, ClientError>,
    ) -> Vec<(i64, RawRecord)> {
        let Some(response) = result.ok().and_then(Response::downcast::<FetchResponse>) else {
            self.fail(now, codes::NETWORK_EXCEPTION);
            return Vec::new();
        };
        let topic_id = self.lane.get().metadata().topic_id(&self.topic);
        let row = response
            .responses
            .iter()
            .find(|t| {
                t.topic == self.topic
                    || topic_id.is_some_and(|id| id != Uuid::ZERO && id == t.topic_id)
            })
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == PARTITION));
        let Some(row) = row else {
            self.fail(now, codes::UNKNOWN_TOPIC_OR_PARTITION);
            return Vec::new();
        };
        match row.error_code {
            codes::NONE => {
                let batches = row
                    .records
                    .as_ref()
                    .and_then(RecordsPayload::as_v2)
                    .unwrap_or(&[]);
                let (records, next) = records_of(&self.topic, PARTITION, batches, self.position);
                if let Some(next) = next {
                    self.position = self.position.max(next);
                }
                self.high_watermark = row.high_watermark;
                self.retry_at = now;
                records
                    .into_iter()
                    .map(|r| {
                        self.offset = r.offset;
                        let record = RawRecord {
                            key: r.key.unwrap_or_default(),
                            value: r.value,
                        };
                        (r.offset, record)
                    })
                    .collect()
            }
            codes::OFFSET_OUT_OF_RANGE => {
                // `auto.offset.reset=earliest`.
                self.last_error = Some(codes::OFFSET_OUT_OF_RANGE);
                self.phase = Phase::Resetting;
                self.retry_at = now;
                Vec::new()
            }
            code => {
                // KIP-951: the answer names the leader to fetch from.
                let (leader, epoch) = (
                    row.current_leader.leader_id,
                    row.current_leader.leader_epoch,
                );
                self.lane
                    .get_mut()
                    .update_leader(&self.topic, PARTITION, leader, epoch);
                self.note_error(code);
                self.fail(now, code);
                Vec::new()
            }
        }
    }

    /// Send the next request when one is due: a `ListOffsets` while
    /// resetting, else a `Fetch`, to the partition leader.
    pub fn poll(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        if self.phase == Phase::Idle || self.out.is_some() || now < self.retry_at {
            return;
        }
        let metadata = self.lane.get().metadata();
        let Some(leader) = metadata.leader(&self.topic, PARTITION) else {
            self.lane.get_mut().request_metadata_refresh();
            self.retry_at = now + RETRY_BACKOFF_MS;
            return;
        };
        let epoch = metadata
            .partition(&self.topic, PARTITION)
            .map_or(-1, |p| p.leader_epoch);
        let topic_id = metadata.topic_id(&self.topic).unwrap_or(Uuid::ZERO);
        let topic = self.topic.clone();
        let target = Target::Broker(leader);
        self.out = Some(if self.phase == Phase::Resetting {
            let request = ListOffsetsRequest {
                replica_id: -1,
                isolation_level: 0,
                topics: vec![ListOffsetsTopic {
                    name: topic,
                    partitions: vec![ListOffsetsPartition {
                        partition_index: PARTITION,
                        current_leader_epoch: epoch,
                        timestamp: EARLIEST_TIMESTAMP,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            Out::ListOffsets(
                self.lane
                    .run(ctx, |client, ctx| client.send(ctx, target, request)),
            )
        } else {
            let request = FetchRequest {
                replica_id: -1,
                max_wait_ms: FETCH_MAX_WAIT_MS,
                min_bytes: FETCH_MIN_BYTES,
                max_bytes: FETCH_MAX_BYTES,
                isolation_level: 0,
                session_id: 0,
                session_epoch: -1,
                topics: vec![FetchTopic {
                    topic,
                    topic_id,
                    partitions: vec![FetchPartition {
                        partition: PARTITION,
                        current_leader_epoch: epoch,
                        fetch_offset: self.position,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: MAX_PARTITION_FETCH_BYTES,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            self.fetches += 1;
            Out::Fetch(
                self.lane
                    .run(ctx, |client, ctx| client.send(ctx, target, request)),
            )
        });
    }

    /// The next time the reader needs a tick: its client's deadline, and
    /// its retry while no request is out.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let own =
            (self.phase != Phase::Idle && self.out.is_none()).then_some(self.retry_at.max(now));
        self.lane
            .get()
            .next_deadline(now)
            .into_iter()
            .chain(own)
            .min()
    }

    /// The reader for the inspector: `offset` is the last record read and
    /// `end_offset` the high watermark of the last fetch.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        json!({
            "phase": self.phase.name(),
            "offset": self.offset,
            "end_offset": self.high_watermark,
            "position": self.position,
            "leader": self.lane.get().metadata().leader(&self.topic, PARTITION),
            "fetches": self.fetches,
            "last_error": self.last_error,
            "client": self.lane.get().snapshot(),
        })
    }
}
