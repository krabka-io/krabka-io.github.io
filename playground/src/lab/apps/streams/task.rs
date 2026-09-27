//! One task of the streams node: a `(subtopology, partition)` the group
//! assigned, around the [`EmbeddedTask`] that runs its processor graph.
//!
//! An active task restores its stores from their changelog partitions (its
//! own partition of each changelog, read from the start up to the end offset
//! it saw when the restore began, with logging off), then processes its
//! source partitions from the group's committed offsets. A standby task keeps
//! restoring for as long as it lives. The task tracks where each partition
//! stands; the node does the fetching and the producing.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
};

use krabka_client_streams::{
    EmbeddedTask,
    embedded::{ChangelogRecord, OutputRecord},
};
use serde_json::{Value, json};

use super::super::topology::{CompiledTopology, StoreKind};
use crate::lab::{client::ConsumedRecord, net::Millis};

/// How many entries of a store the snapshot shows.
const STORE_ENTRIES: usize = 20;

/// How many windows of a window store the task keeps for the inspector.
const WINDOW_ENTRIES: usize = 1_000;

/// The windows of one window store: `(key, window start)` to `(window end,
/// count)`.
type Windows = BTreeMap<(String, i64), (i64, i64)>;

/// A task id: the subtopology and the partition.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TaskId {
    pub subtopology: String,
    pub partition: i32,
}

impl fmt::Display for TaskId {
    // Kafka Streams writes a task id as `<subtopology>_<partition>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.subtopology, self.partition)
    }
}

/// The role a task has on this member.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Active,
    Standby,
}

/// Where a partition's position stands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Position {
    /// The committed offset is not known yet.
    Unknown,
    /// A lookup (`OffsetFetch` or `ListOffsets`) is on the wire.
    Looking,
    /// The start of the partition is to be looked up with `ListOffsets`.
    Reset,
    /// The next offset to fetch.
    At(i64),
}

/// One source partition of an active task.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Source {
    pub position: Position,
    /// The offset after the last record piped: what a commit stores.
    pub processed: Option<i64>,
    /// The offset last committed.
    pub committed: Option<i64>,
    pub hwm: Option<i64>,
    pub fetching: bool,
    pub retry_at: Millis,
}

impl Source {
    fn new() -> Self {
        Self {
            position: Position::Unknown,
            processed: None,
            committed: None,
            hwm: None,
            fetching: false,
            retry_at: 0,
        }
    }
}

/// The end a restore reads up to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RestoreEnd {
    /// The end offset is to be looked up with `ListOffsets`.
    Unknown,
    Looking,
    At(i64),
    /// A standby follows the changelog without an end.
    Follow,
}

/// One changelog partition a task restores from.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Changelog {
    pub store: String,
    pub position: Position,
    pub end: RestoreEnd,
    pub hwm: Option<i64>,
    pub fetching: bool,
    pub retry_at: Millis,
    pub restored: u64,
}

/// A fetched value, as the task pipes it.
pub enum Decoded {
    /// The value bytes to pipe.
    Ready(Vec<u8>),
    /// The value waits, for its schema; the task stops piping until it can.
    Wait,
    /// The value cannot be read; the task skips the record.
    Failed(String),
}

/// What a task emitted in one [`StreamTask::run`].
#[derive(Default)]
pub struct Emitted {
    /// Records piped.
    pub piped: u64,
    /// Sink and repartition records.
    pub outputs: Vec<OutputRecord>,
    /// Records of the stores' changelogs.
    pub changelogs: Vec<ChangelogRecord>,
    /// Records the task skipped: topic, offset and the error.
    pub skipped: Vec<(String, i64, String)>,
}

/// A task. See the module documentation.
pub struct StreamTask {
    pub id: TaskId,
    pub role: Role,
    pub embedded: EmbeddedTask,
    /// The source partitions, by topic; empty for a standby.
    pub sources: BTreeMap<String, Source>,
    /// The changelog partitions, by topic.
    pub changelogs: BTreeMap<String, Changelog>,
    /// Fetched source records waiting to be piped, in offset order per
    /// partition.
    pub buffer: VecDeque<ConsumedRecord>,
    /// The stores are restored and the task processes.
    pub running: bool,
    /// The task was revoked; it commits and closes.
    pub closing: bool,
    pub records_in: u64,
    pub records_out: u64,
    pub changelog_out: u64,
    pub skipped: u64,
    /// The windows of each window store, `(key, start) -> (end, count)`,
    /// from the changelog records the task wrote or restored.
    windows: BTreeMap<String, Windows>,
    /// The kinds of the task's stores.
    kinds: BTreeMap<String, StoreKind>,
}

impl StreamTask {
    /// A task of `compiled` in `role`.
    ///
    /// # Errors
    /// Returns the crate's refusal when the subtopology does not exist.
    pub fn new(compiled: &CompiledTopology, id: TaskId, role: Role) -> Result<Self, String> {
        let mut embedded = EmbeddedTask::for_subtopology(&compiled.built, &id.subtopology)
            .map_err(|e| e.to_string())?;
        let sources = match role {
            Role::Active => compiled
                .built
                .source_topics_for(&id.subtopology)
                .iter()
                .map(|t| (t.clone(), Source::new()))
                .collect(),
            Role::Standby => BTreeMap::new(),
        };
        let end = match role {
            Role::Active => RestoreEnd::Unknown,
            Role::Standby => RestoreEnd::Follow,
        };
        let mut kinds = BTreeMap::new();
        let changelogs: BTreeMap<String, Changelog> = embedded
            .store_names()
            .into_iter()
            .filter_map(|store| {
                if let Some(plan) = compiled.plan.store(&store) {
                    kinds.insert(store.clone(), plan.kind);
                }
                let topic = embedded.store_changelog_topic(&store)?;
                Some((
                    topic,
                    Changelog {
                        store,
                        position: Position::At(0),
                        end,
                        hwm: None,
                        fetching: false,
                        retry_at: 0,
                        restored: 0,
                    },
                ))
            })
            .collect();
        // Restored writes are not logged again.
        embedded.set_logging(false);
        let mut task = Self {
            id,
            role,
            embedded,
            sources,
            changelogs,
            buffer: VecDeque::new(),
            running: false,
            closing: false,
            records_in: 0,
            records_out: 0,
            changelog_out: 0,
            skipped: 0,
            windows: BTreeMap::new(),
            kinds,
        };
        task.check_restored();
        Ok(task)
    }

    /// Turn a standby into the active task, keeping what it restored, as
    /// Kafka Streams recycles a standby: it reads its changelogs on to their
    /// end, then processes from the committed offsets.
    pub fn promote(&mut self, compiled: &CompiledTopology) {
        self.role = Role::Active;
        self.sources = compiled
            .built
            .source_topics_for(&self.id.subtopology)
            .iter()
            .map(|t| (t.clone(), Source::new()))
            .collect();
        for changelog in self.changelogs.values_mut() {
            changelog.end = RestoreEnd::Unknown;
        }
        self.running = false;
        self.check_restored();
    }

    /// Start running when every changelog reached its end.
    pub fn check_restored(&mut self) -> bool {
        if self.running || self.role == Role::Standby {
            return false;
        }
        let done = self.changelogs.values().all(|c| match (c.end, c.position) {
            (RestoreEnd::At(end), Position::At(position)) => position >= end,
            _ => false,
        });
        if done {
            self.embedded.set_logging(true);
            self.running = true;
        }
        done
    }

    /// Pipe the fetched records in order, each value as `decode` reads it,
    /// until a value waits; then fire the stream-time punctuators. Only a
    /// running active task that is not closing pipes. A record whose value
    /// cannot be read, or that the graph refuses, is skipped, as Kafka
    /// Streams' `LogAndContinueExceptionHandler` does.
    pub fn run(&mut self, mut decode: impl FnMut(&ConsumedRecord) -> Decoded) -> Emitted {
        let mut emitted = Emitted::default();
        if self.role != Role::Active || !self.running || self.closing {
            return emitted;
        }
        while let Some(next) = self.buffer.front() {
            let value = match decode(next) {
                Decoded::Wait => break,
                Decoded::Ready(bytes) => Ok(bytes),
                Decoded::Failed(error) => Err(error),
            };
            let Some(record) = self.buffer.pop_front() else {
                break;
            };
            emitted.piped += 1;
            if let Some(source) = self.sources.get_mut(&record.topic) {
                source.processed = Some(record.offset + 1);
            }
            let outcome = value.and_then(|value| {
                self.embedded
                    .pipe(
                        &record.topic,
                        record.partition,
                        record.offset,
                        record.key.as_deref(),
                        &value,
                        record.timestamp,
                    )
                    .map_err(|e| e.to_string())
            });
            if let Err(error) = outcome {
                self.skipped += 1;
                emitted.skipped.push((record.topic, record.offset, error));
            }
            emitted.outputs.extend(self.embedded.take_output());
            emitted.changelogs.extend(self.embedded.drain_changelogs());
        }
        if emitted.piped > 0 {
            let stream_time = self.embedded.stream_time();
            if self.embedded.punctuate_stream_time(stream_time).is_ok() {
                emitted.outputs.extend(self.embedded.take_output());
                emitted.changelogs.extend(self.embedded.drain_changelogs());
            }
        }
        for record in &emitted.changelogs {
            let store = self
                .changelogs
                .get(&record.topic)
                .map(|c| c.store.clone())
                .unwrap_or_default();
            self.mirror(&store, &record.key, record.value.as_deref());
        }
        self.records_in += emitted.piped;
        self.records_out += u64::try_from(emitted.outputs.len()).unwrap_or(u64::MAX);
        self.changelog_out += u64::try_from(emitted.changelogs.len()).unwrap_or(u64::MAX);
        emitted
    }

    /// Apply restored changelog records of `topic`.
    pub fn restore(&mut self, topic: &str, records: &[ConsumedRecord]) {
        let Some(changelog) = self.changelogs.get_mut(topic) else {
            return;
        };
        let store = changelog.store.clone();
        changelog.restored += u64::try_from(records.len()).unwrap_or(u64::MAX);
        for record in records {
            let key = record.key.clone().unwrap_or_default();
            self.embedded.restore_apply(
                &store,
                key.clone(),
                record.value.clone(),
                record.timestamp,
            );
            self.mirror(&store, &key, record.value.as_deref());
        }
    }

    /// Keep the inspector's view of a window store in step with a changelog
    /// record of it: the key is `key | window start (8 bytes) | seqnum (4
    /// bytes)` and the value `timestamp (8 bytes) | count (8 bytes)`, the
    /// JVM's windowed store layout.
    pub fn mirror(&mut self, store: &str, key: &[u8], value: Option<&[u8]>) {
        let Some(StoreKind::Window { size_ms, .. }) = self.kinds.get(store) else {
            return;
        };
        let Some((user_key, start)) = window_key(key) else {
            return;
        };
        let windows = self.windows.entry(store.to_string()).or_default();
        match value
            .and_then(|v| v.get(8..16))
            .and_then(|c| c.try_into().ok())
        {
            Some(count) => {
                let end = start.saturating_add(i64::try_from(*size_ms).unwrap_or(i64::MAX));
                windows.insert((user_key, start), (end, i64::from_be_bytes(count)));
                while windows.len() > WINDOW_ENTRIES {
                    // Drop the oldest window.
                    let oldest = windows.keys().min_by_key(|(_, s)| *s).cloned();
                    match oldest {
                        Some(k) => {
                            windows.remove(&k);
                        }
                        None => break,
                    }
                }
            }
            None => {
                windows.remove(&(user_key, start));
            }
        }
    }

    /// The source records waiting and the source lag: records written and
    /// not yet piped, over the partitions with a known high watermark.
    #[must_use]
    pub fn lag(&self) -> Option<i64> {
        let mut total = None;
        for source in self.sources.values() {
            let (Some(hwm), Some(done)) = (
                source.hwm,
                source.processed.or(match source.position {
                    Position::At(p) => Some(p),
                    _ => None,
                }),
            ) else {
                continue;
            };
            let waiting = (hwm - done).max(0);
            total = Some(total.unwrap_or(0) + waiting);
        }
        total
    }

    /// Changelog records restored so far.
    #[must_use]
    pub fn restored(&self) -> u64 {
        self.changelogs.values().map(|c| c.restored).sum()
    }

    /// The entries of `store` the task holds: `[key, value]` pairs, or for a
    /// window store `[key, {"window_start", "window_end", "count"}]`.
    #[must_use]
    pub fn entries(&self, store: &str, limit: usize) -> Vec<Value> {
        match self.kinds.get(store) {
            Some(StoreKind::Window { .. }) => self
                .windows
                .get(store)
                .into_iter()
                .flatten()
                .take(limit)
                .map(|((key, start), (end, count))| {
                    json!([key, { "window_start": start, "window_end": end, "count": count }])
                })
                .collect(),
            Some(kind) => self
                .embedded
                .dump_store(store, limit)
                .into_iter()
                .map(|(k, v)| json!([String::from_utf8_lossy(&k), decode_value(*kind, &v)]))
                .collect(),
            None => Vec::new(),
        }
    }

    /// The value of `key` in `store`, if the task holds it.
    #[must_use]
    pub fn query(&self, store: &str, key: &str) -> Option<Value> {
        match self.kinds.get(store)? {
            StoreKind::Window { .. } => {
                let windows: Vec<Value> = self
                    .windows
                    .get(store)?
                    .iter()
                    .filter(|((k, _), _)| k == key)
                    .map(|((_, start), (end, count))| {
                        json!({ "window_start": start, "window_end": end, "count": count })
                    })
                    .collect();
                (!windows.is_empty()).then_some(Value::Array(windows))
            }
            kind => self
                .embedded
                .dump_store(store, usize::MAX)
                .into_iter()
                .find(|(k, _)| k.as_ref() == key.as_bytes())
                .map(|(_, v)| decode_value(*kind, &v)),
        }
    }

    /// The task's stores for the inspector.
    #[must_use]
    pub fn stores(&self) -> Vec<Value> {
        self.changelogs
            .iter()
            .map(|(topic, c)| {
                json!({
                    "name": c.store,
                    "task": self.id.to_string(),
                    "changelog": topic,
                    "entries": self.entries(&c.store, STORE_ENTRIES),
                })
            })
            .collect()
    }

    /// The task for the inspector.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let phase = match (self.role, self.running, self.closing) {
            (_, _, true) => "closing",
            (Role::Standby, _, _) => "standby",
            (Role::Active, true, _) => "running",
            (Role::Active, false, _) => "restoring",
        };
        let mut partitions: Vec<String> = self
            .sources
            .keys()
            .chain(self.changelogs.keys())
            .map(|t| format!("{t}-{}", self.id.partition))
            .collect();
        partitions.dedup();
        json!({
            "id": self.id.to_string(),
            "role": match self.role {
                Role::Active => "active",
                Role::Standby => "standby",
            },
            "phase": phase,
            "partitions": partitions,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "changelog_out": self.changelog_out,
            "skipped": self.skipped,
            "restored": self.restored(),
            "lag": self.lag(),
            "buffered": self.buffer.len(),
        })
    }
}

/// The user key and the window start of a windowed store key.
fn window_key(key: &[u8]) -> Option<(String, i64)> {
    let cut = key.len().checked_sub(12)?;
    let start: [u8; 8] = key.get(cut..cut + 8)?.try_into().ok()?;
    Some((
        String::from_utf8_lossy(&key[..cut]).into_owned(),
        i64::from_be_bytes(start),
    ))
}

/// A store value as JSON: a count is an 8-byte big-endian long, a sum JSON
/// text.
fn decode_value(kind: StoreKind, bytes: &[u8]) -> Value {
    match kind {
        StoreKind::Count | StoreKind::Window { .. } => bytes
            .try_into()
            .map_or(Value::Null, |b: [u8; 8]| Value::from(i64::from_be_bytes(b))),
        StoreKind::Sum => serde_json::from_slice(bytes).unwrap_or(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::lab::apps::topology::TopologySpec;

    fn compiled(ops: &Value) -> CompiledTopology {
        let spec =
            TopologySpec::parse(&json!({ "source": "in", "ops": ops, "sink": "out" })).unwrap();
        CompiledTopology::new("app", &spec).unwrap()
    }

    fn record(topic: &str, offset: i64, key: &[u8], value: Option<Vec<u8>>) -> ConsumedRecord {
        ConsumedRecord {
            topic: topic.to_string(),
            partition: 1,
            offset,
            timestamp: offset,
            key: Some(Bytes::copy_from_slice(key)),
            value: value.map(Bytes::from),
            headers: Vec::new(),
            leader_epoch: 0,
        }
    }

    fn id() -> TaskId {
        TaskId {
            subtopology: "0".to_string(),
            partition: 1,
        }
    }

    #[test]
    fn an_active_task_restores_up_to_the_end_and_then_runs() {
        let topology = compiled(&json!([{ "op": "count_by_key", "store": "counts" }]));
        let mut task = StreamTask::new(&topology, id(), Role::Active).unwrap();
        assert!(task.id.to_string() == "0_1");
        assert!(task.sources.keys().cloned().collect::<Vec<_>>() == vec!["in".to_string()]);
        assert!(
            task.changelogs["app-counts-changelog"]
                == Changelog {
                    store: "counts".to_string(),
                    position: Position::At(0),
                    end: RestoreEnd::Unknown,
                    hwm: None,
                    fetching: false,
                    retry_at: 0,
                    restored: 0,
                }
        );
        assert!(!task.check_restored());
        let changelog = task.changelogs.get_mut("app-counts-changelog").unwrap();
        changelog.end = RestoreEnd::At(2);
        task.restore(
            "app-counts-changelog",
            &[
                record(
                    "app-counts-changelog",
                    0,
                    b"a",
                    Some(1_i64.to_be_bytes().to_vec()),
                ),
                record(
                    "app-counts-changelog",
                    1,
                    b"a",
                    Some(4_i64.to_be_bytes().to_vec()),
                ),
            ],
        );
        task.changelogs
            .get_mut("app-counts-changelog")
            .unwrap()
            .position = Position::At(2);
        assert!(task.check_restored());
        assert!(task.running);
        assert!(task.restored() == 2);
        assert!(task.query("counts", "a") == Some(json!(4)));
        assert!(
            task.stores()
                == vec![json!({
                    "name": "counts", "task": "0_1", "changelog": "app-counts-changelog",
                    "entries": [["a", 4]],
                })]
        );
        // Running, the restored count goes on, and the change is logged.
        task.embedded
            .pipe("in", 1, 0, Some(b"a"), b"{}", 5)
            .unwrap();
        let logged = task.embedded.drain_changelogs();
        assert!(logged.len() == 1);
        assert!(logged[0].value == Some(Bytes::copy_from_slice(&5_i64.to_be_bytes())));
    }

    #[test]
    fn a_task_without_stores_runs_at_once_and_a_standby_never_does() {
        let stateless = compiled(&json!([{ "op": "filter", "field": "x", "gt": 1 }]));
        let task = StreamTask::new(&stateless, id(), Role::Active).unwrap();
        assert!(task.running);
        let stateful = compiled(&json!([{ "op": "count_by_key" }]));
        let mut standby = StreamTask::new(&stateful, id(), Role::Standby).unwrap();
        assert!(standby.sources.is_empty());
        assert!(
            standby
                .changelogs
                .values()
                .all(|c| c.end == RestoreEnd::Follow)
        );
        assert!(!standby.check_restored());
        assert!(standby.snapshot()["phase"] == "standby");
    }

    #[test]
    fn window_stores_are_seen_through_their_changelog_records() {
        let topology = compiled(&json!([{ "op": "window_count", "size_ms": 1000, "store": "w" }]));
        let mut task = StreamTask::new(&topology, id(), Role::Active).unwrap();
        let key = |k: &str, start: i64| {
            let mut out = k.as_bytes().to_vec();
            out.extend_from_slice(&start.to_be_bytes());
            out.extend_from_slice(&0_u32.to_be_bytes());
            out
        };
        let value = |ts: i64, count: i64| {
            let mut out = ts.to_be_bytes().to_vec();
            out.extend_from_slice(&count.to_be_bytes());
            out
        };
        task.mirror("w", &key("a", 0), Some(&value(10, 1)));
        task.mirror("w", &key("a", 0), Some(&value(20, 2)));
        task.mirror("w", &key("a", 1_000), Some(&value(1_500, 1)));
        task.mirror("w", &key("b", 0), Some(&value(30, 1)));
        task.mirror("w", &key("b", 0), None);
        assert!(
            task.query("w", "a")
                == Some(json!([
                    { "window_start": 0, "window_end": 1_000, "count": 2 },
                    { "window_start": 1_000, "window_end": 2_000, "count": 1 },
                ]))
        );
        assert!(task.query("w", "b").is_none());
        assert!(
            task.entries("w", 20)
                == vec![
                    json!(["a", { "window_start": 0, "window_end": 1_000, "count": 2 }]),
                    json!(["a", { "window_start": 1_000, "window_end": 2_000, "count": 1 }]),
                ]
        );
    }

    #[test]
    fn lag_counts_records_written_and_not_yet_piped() {
        let topology = compiled(&json!([]));
        let mut task = StreamTask::new(&topology, id(), Role::Active).unwrap();
        assert!(task.lag().is_none());
        let source = task.sources.get_mut("in").unwrap();
        source.position = Position::At(10);
        source.hwm = Some(25);
        assert!(task.lag() == Some(15));
        let source = task.sources.get_mut("in").unwrap();
        source.processed = Some(20);
        assert!(task.lag() == Some(5));
    }
}
