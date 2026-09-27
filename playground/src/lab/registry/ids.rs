//! Newtypes for the registry's same-typed identifiers.
//!
//! A registered schema is addressed two ways, both `i32` on the wire: a
//! registry-global [`SchemaId`], keyed by the schema's canonical form and
//! references, and a per-subject [`SchemaVersion`], the 1-based ordinal of
//! the schema within one subject's history. They travel together in every
//! record and response, so a transposed pair would compile as raw integers and
//! mislabel a record; the newtypes make the compiler reject the mix-up.
//! [`LogOffset`] is the position of a record in the `_schemas` log.
//!
//! Every newtype is `#[serde(transparent)]`, so the `_schemas` bytes and the
//! REST bodies carry the bare integer, exactly as Confluent writes them.

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A registry-global schema id. `Default` is `0`, the "no id assigned yet"
/// sentinel that seeds the id high-water mark before the first registration.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct SchemaId(pub i32);

impl SchemaId {
    /// The id after this one.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// A schema's 1-based version within one subject's history. Distinct subjects
/// reuse version numbers, so a version is meaningful only with its subject.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct SchemaVersion(pub i32);

impl SchemaVersion {
    /// The version of a subject's first schema.
    pub const FIRST: Self = Self(1);

    /// The version after this one.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// The position of a record in the `_schemas` log, a Kafka offset.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct LogOffset(pub i64);

impl LogOffset {
    /// The offset after this one.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn ids_serialise_as_bare_integers() {
        assert!(serde_json::to_string(&SchemaId(7)).unwrap() == "7");
        assert!(serde_json::to_string(&SchemaVersion(2)).unwrap() == "2");
        assert!(serde_json::from_str::<LogOffset>("12").unwrap() == LogOffset(12));
        assert!(SchemaId::default().next() == SchemaId(1));
        assert!(SchemaVersion::FIRST.next() == SchemaVersion(2));
        assert!(LogOffset(3).next() == LogOffset(4));
    }
}
