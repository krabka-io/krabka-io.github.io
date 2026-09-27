//! The records the coordinator writes to its `__consumer_offsets` partition,
//! and the codec that turns them back into state on a replay.
//!
//! Every record is a JSON key and an optional JSON value, so the page can
//! read the partition it renders. A null value is a tombstone. The formats
//! are the lab's own: Kafka's binary `OffsetCommitKey` and `GroupMetadataKey`
//! layouts are not reproduced. The key is one of the [`RecordKey`] shapes; the
//! value shape belongs to the state the key names and is documented there.

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::ids::{GroupId, group_partition};

/// The key of one record: `{"type": "...", ...}`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RecordKey {
    /// A committed offset. The value is an
    /// [`OffsetEntry`](super::OffsetEntry).
    Offset {
        group: GroupId,
        topic: String,
        partition: i32,
    },
    /// A classic group. The value is a `ClassicGroupValue`.
    ClassicGroup { group: GroupId },
    /// A KIP-848 consumer group. The value is a `ConsumerGroupValue`.
    ConsumerGroup { group: GroupId },
    /// A KIP-1071 streams group. The value is a `StreamsGroupValue`.
    StreamsGroup { group: GroupId },
}

impl RecordKey {
    /// Decode a record key. A key that is not one of these shapes is `None`,
    /// and a replay skips its record.
    #[must_use]
    pub fn decode(key: &[u8]) -> Option<Self> {
        serde_json::from_slice(key).ok()
    }

    /// The group the record belongs to.
    #[must_use]
    pub fn group(&self) -> &GroupId {
        match self {
            Self::Offset { group, .. }
            | Self::ClassicGroup { group }
            | Self::ConsumerGroup { group }
            | Self::StreamsGroup { group } => group,
        }
    }

    /// The `__consumer_offsets` partition the record belongs on.
    #[must_use]
    pub fn partition(&self) -> i32 {
        group_partition(self.group().as_str())
    }
}

/// Encode a record. A `None` value is a tombstone.
///
/// # Panics
/// Panics when the value cannot be serialized, which the value types written
/// here never do: they hold only strings, numbers, lists and maps with string
/// keys.
pub fn encode<V: Serialize>(key: &RecordKey, value: Option<&V>) -> (Bytes, Option<Bytes>) {
    let key = serde_json::to_vec(key).expect("a record key serializes");
    let value =
        value.map(|v| Bytes::from(serde_json::to_vec(v).expect("a record value serializes")));
    (Bytes::from(key), value)
}

/// Decode a record value.
#[must_use]
pub fn decode_value<V: DeserializeOwned>(value: &[u8]) -> Option<V> {
    serde_json::from_slice(value).ok()
}

/// `serde` adapter that writes a [`Bytes`] field as a base64 string.
pub mod base64_bytes {
    use base64::Engine as _;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize `bytes` as standard base64.
    ///
    /// # Errors
    /// Returns the serializer's error.
    pub fn serialize<S: Serializer>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    /// Deserialize standard base64 into [`Bytes`].
    ///
    /// # Errors
    /// Returns an error when the string is not valid base64.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
        let text = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map(Bytes::from)
            .map_err(serde::de::Error::custom)
    }
}
