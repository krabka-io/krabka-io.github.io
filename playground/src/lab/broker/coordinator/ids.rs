//! Identifiers of the coordinator's domain, and Kafka's rule that maps a group
//! id onto a `__consumer_offsets` partition.

use derive_more::{Display, From, Into};
use krabka_protocol::primitives::uuid::Uuid;
use serde::{Deserialize, Serialize};

/// The `group.id` of a classic, consumer or streams group.
#[derive(
    Clone,
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
    Default,
)]
#[from(String, &str)]
#[serde(transparent)]
pub struct GroupId(pub String);

impl GroupId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for GroupId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// The id of one member of a group, as the wire carries it.
#[derive(
    Clone,
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
    Default,
)]
#[from(String, &str)]
#[serde(transparent)]
pub struct MemberId(pub String);

impl MemberId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for MemberId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// A topic id in the form the coordinator keys its maps by. The wire type
/// [`Uuid`] has no ordering, and assignment lists reach the wire in map
/// order, so the coordinator keys by this ordered copy of the same 16 bytes.
/// It serializes as 32 hexadecimal characters, so it can key a JSON map.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct TopicId(pub [u8; 16]);

impl std::fmt::Display for TopicId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for TopicId {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != 32 {
            return Err(format!(
                "topic id `{text}` is not 32 hexadecimal characters"
            ));
        }
        let mut bytes = [0_u8; 16];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16)
                .map_err(|e| format!("topic id `{text}`: {e}"))?;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for TopicId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for TopicId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl From<Uuid> for TopicId {
    fn from(id: Uuid) -> Self {
        Self(id.0)
    }
}

impl From<TopicId> for Uuid {
    fn from(id: TopicId) -> Self {
        Self(id.0)
    }
}

/// A `JoinGroup` or `SyncGroup` the coordinator holds. The broker keeps the
/// token next to the connection and the correlation id of the request, and
/// answers it when a [`Completion`](super::Completion) carries the token.
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
pub struct HoldToken(pub u64);

/// The name of the offsets topic every coordinator writes to.
pub const OFFSETS_TOPIC: &str = "__consumer_offsets";

/// The partition count of `__consumer_offsets`: Kafka's
/// `offsets.topic.num.partitions` default.
pub const OFFSETS_TOPIC_PARTITIONS: i32 = 50;

/// Java's `String.hashCode`: `s[0]*31^(n-1) + ... + s[n-1]` over the UTF-16
/// code units, in wrapping `i32` arithmetic.
#[must_use]
pub fn java_hash_code(s: &str) -> i32 {
    s.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// The `__consumer_offsets` partition that owns `group_id`, as Kafka's
/// `Utils.abs(groupId.hashCode()) % offsets.topic.num.partitions` computes it.
/// `Utils.abs` is `Math.abs` with `Integer.MIN_VALUE` mapped to zero.
#[must_use]
pub fn group_partition(group_id: &str) -> i32 {
    java_hash_code(group_id).checked_abs().unwrap_or(0) % OFFSETS_TOPIC_PARTITIONS
}
