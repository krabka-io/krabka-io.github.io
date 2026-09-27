//! The `_schemas` topic record shapes.
//!
//! Keys drive log compaction and are serialised byte-exactly in Confluent's
//! field order: `{"keytype":"SCHEMA","subject":..,"version":..,"magic":1}`
//! and `{"keytype":"CONFIG"|"MODE"|"DELETE_SUBJECT"|"NOOP","subject":..,
//! "magic":0}`. Values are parsed structurally; the writers here still emit
//! Confluent's order (`subject, version, id, schemaType?, references?,
//! schema, deleted`) so a captured topic compares equal byte for byte.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    format::SchemaType,
    ids::{SchemaId, SchemaVersion},
};

/// One record of the `_schemas` log: a JSON key and a JSON value, or no value
/// for a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRecord {
    pub key: Bytes,
    pub value: Option<Bytes>,
}

impl RawRecord {
    fn of<K: Serialize, V: Serialize>(key: &K, value: Option<&V>) -> Self {
        Self {
            key: Bytes::from(serde_json::to_vec(key).expect("a record key is a plain struct")),
            value: value.map(|v| {
                Bytes::from(serde_json::to_vec(v).expect("a record value is a plain struct"))
            }),
        }
    }
}

/// The key of a `SCHEMA` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaKey {
    /// Always `"SCHEMA"`.
    pub keytype: String,
    pub subject: String,
    pub version: SchemaVersion,
    /// `1` for `SCHEMA` keys; the other key types carry `0`.
    pub magic: u8,
}

impl SchemaKey {
    #[must_use]
    pub fn new(subject: &str, version: SchemaVersion) -> Self {
        Self {
            keytype: "SCHEMA".to_string(),
            subject: subject.to_string(),
            version,
            magic: 1,
        }
    }
}

/// A reference to another registered schema, as a client sends it and as the
/// registry stores it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SchemaReference {
    /// The name the referring schema uses: an Avro type name, a Protobuf
    /// import path, or a JSON Schema `$ref` target.
    pub name: String,
    pub subject: String,
    pub version: SchemaVersion,
}

/// The value of a `SCHEMA` record. `schemaType` is absent for Avro and
/// `references` is absent when empty, as Confluent writes them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaValue {
    pub subject: String,
    pub version: SchemaVersion,
    pub id: SchemaId,
    #[serde(
        rename = "schemaType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    pub schema: String,
    #[serde(default)]
    pub deleted: bool,
}

impl SchemaValue {
    /// The schema type the value names; absent means Avro.
    #[must_use]
    pub fn schema_type(&self) -> SchemaType {
        SchemaType::from_wire(self.schema_type.as_deref()).unwrap_or(SchemaType::Avro)
    }
}

/// The key of a `CONFIG` record; `subject: None` is the global level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigKey {
    pub keytype: String,
    pub subject: Option<String>,
    pub magic: u8,
}

/// The value of a `CONFIG` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValue {
    #[serde(rename = "compatibilityLevel")]
    pub compatibility_level: String,
}

/// The key of a `MODE` record; `subject: None` is the global mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeKey {
    pub keytype: String,
    pub subject: Option<String>,
    pub magic: u8,
}

/// The value of a `MODE` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeValue {
    pub mode: String,
}

/// The key of a `DELETE_SUBJECT` record, the soft-delete marker of a subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubjectKey {
    pub keytype: String,
    pub subject: String,
    pub magic: u8,
}

/// The value of a `DELETE_SUBJECT` record: the subject and the highest
/// version it had at delete time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubjectValue {
    pub subject: String,
    pub version: SchemaVersion,
}

/// The durable per-subject version high-water mark, written before a
/// permanent delete so a compacted log still knows the next version. It uses
/// Confluent's `NOOP` key type, which other registries ignore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionHighWaterKey {
    pub keytype: String,
    pub subject: String,
    pub magic: u8,
}

/// The value of a version high-water record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionHighWaterValue {
    #[serde(rename = "nextVersion")]
    pub next_version: SchemaVersion,
}

/// A decoded `_schemas` record. Unknown key types and undecodable records
/// decode to their own variants, so replay never fails on a foreign record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRecord {
    Schema(SchemaKey, SchemaValue),
    /// A `SCHEMA` key with no value: a permanent delete.
    Tombstone(SchemaKey),
    /// A `CONFIG` record; `None` clears the level.
    Config(ConfigKey, Option<ConfigValue>),
    /// A `MODE` record; `None` clears the mode.
    Mode(ModeKey, Option<ModeValue>),
    DeleteSubject(DeleteSubjectKey, DeleteSubjectValue),
    VersionHighWater(VersionHighWaterKey, VersionHighWaterValue),
    /// A record with nothing to apply.
    Noop,
    Unknown {
        keytype: String,
    },
    Undecodable {
        keytype: Option<String>,
    },
}

impl SchemaRecord {
    /// Decode a raw record. Never fails: a record this registry cannot read
    /// becomes [`SchemaRecord::Unknown`] or [`SchemaRecord::Undecodable`].
    #[must_use]
    pub fn decode(record: &RawRecord) -> Self {
        let Ok(key) = serde_json::from_slice::<serde_json::Value>(&record.key) else {
            return Self::Undecodable { keytype: None };
        };
        let Some(keytype) = key.get("keytype").and_then(|v| v.as_str()) else {
            return Self::Undecodable { keytype: None };
        };
        let value = record.value.as_deref();
        let undecodable = || Self::Undecodable {
            keytype: Some(keytype.to_string()),
        };
        match keytype {
            "SCHEMA" => match (serde_json::from_slice::<SchemaKey>(&record.key), value) {
                (Ok(k), Some(v)) => serde_json::from_slice::<SchemaValue>(v)
                    .map_or_else(|_| undecodable(), |v| Self::Schema(k, v)),
                (Ok(k), None) => Self::Tombstone(k),
                (Err(_), _) => undecodable(),
            },
            "CONFIG" => match (serde_json::from_slice::<ConfigKey>(&record.key), value) {
                (Ok(k), Some(v)) => serde_json::from_slice::<ConfigValue>(v)
                    .map_or_else(|_| undecodable(), |v| Self::Config(k, Some(v))),
                (Ok(k), None) => Self::Config(k, None),
                (Err(_), _) => undecodable(),
            },
            "MODE" => match (serde_json::from_slice::<ModeKey>(&record.key), value) {
                (Ok(k), Some(v)) => serde_json::from_slice::<ModeValue>(v)
                    .map_or_else(|_| undecodable(), |v| Self::Mode(k, Some(v))),
                (Ok(k), None) => Self::Mode(k, None),
                (Err(_), _) => undecodable(),
            },
            "DELETE_SUBJECT" => {
                match (
                    serde_json::from_slice::<DeleteSubjectKey>(&record.key),
                    value,
                ) {
                    (Ok(k), Some(v)) => serde_json::from_slice::<DeleteSubjectValue>(v)
                        .map_or_else(|_| undecodable(), |v| Self::DeleteSubject(k, v)),
                    (Ok(_), None) => Self::Noop,
                    (Err(_), _) => undecodable(),
                }
            }
            "NOOP" => match (
                serde_json::from_slice::<VersionHighWaterKey>(&record.key),
                value.and_then(|v| serde_json::from_slice::<VersionHighWaterValue>(v).ok()),
            ) {
                (Ok(k), Some(v)) => Self::VersionHighWater(k, v),
                _ => Self::Noop,
            },
            "CLEAR_SUBJECTS" | "CLEAR_SUBJECT" => Self::Noop,
            other => Self::Unknown {
                keytype: other.to_string(),
            },
        }
    }
}

/// A `SCHEMA` record for `value`; the key is derived from its subject and
/// version.
#[must_use]
pub fn encode_schema(value: &SchemaValue) -> RawRecord {
    RawRecord::of(&SchemaKey::new(&value.subject, value.version), Some(value))
}

/// The tombstone of a `SCHEMA` record: a permanent delete.
#[must_use]
pub fn encode_tombstone(subject: &str, version: SchemaVersion) -> RawRecord {
    RawRecord::of::<_, ()>(&SchemaKey::new(subject, version), None)
}

fn config_key(subject: Option<&str>) -> ConfigKey {
    ConfigKey {
        keytype: "CONFIG".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    }
}

/// A `CONFIG` record; `subject: None` is the global level.
#[must_use]
pub fn encode_config(subject: Option<&str>, level: &str) -> RawRecord {
    RawRecord::of(
        &config_key(subject),
        Some(&ConfigValue {
            compatibility_level: level.to_string(),
        }),
    )
}

/// The tombstone that clears a `CONFIG` level.
#[must_use]
pub fn config_tombstone(subject: Option<&str>) -> RawRecord {
    RawRecord::of::<_, ()>(&config_key(subject), None)
}

fn mode_key(subject: Option<&str>) -> ModeKey {
    ModeKey {
        keytype: "MODE".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    }
}

/// A `MODE` record; `subject: None` is the global mode.
#[must_use]
pub fn encode_mode(subject: Option<&str>, mode: &str) -> RawRecord {
    RawRecord::of(
        &mode_key(subject),
        Some(&ModeValue {
            mode: mode.to_string(),
        }),
    )
}

/// The tombstone that clears a `MODE` override.
#[must_use]
pub fn mode_tombstone(subject: Option<&str>) -> RawRecord {
    RawRecord::of::<_, ()>(&mode_key(subject), None)
}

/// The soft-delete marker of a subject.
#[must_use]
pub fn encode_delete_subject(subject: &str, version: SchemaVersion) -> RawRecord {
    RawRecord::of(
        &DeleteSubjectKey {
            keytype: "DELETE_SUBJECT".to_string(),
            subject: subject.to_string(),
            magic: 0,
        },
        Some(&DeleteSubjectValue {
            subject: subject.to_string(),
            version,
        }),
    )
}

/// The version high-water record of a subject.
#[must_use]
pub fn encode_version_high_water(subject: &str, next_version: SchemaVersion) -> RawRecord {
    RawRecord::of(
        &VersionHighWaterKey {
            keytype: "NOOP".to_string(),
            subject: subject.to_string(),
            magic: 0,
        },
        Some(&VersionHighWaterValue { next_version }),
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn value(subject: &str, ty: SchemaType, references: Vec<SchemaReference>) -> SchemaValue {
        SchemaValue {
            subject: subject.into(),
            version: SchemaVersion(1),
            id: SchemaId(2),
            schema_type: ty.wire_name().map(str::to_string),
            references,
            schema: "S".into(),
            deleted: false,
        }
    }

    #[test]
    fn keys_match_the_confluent_bytes() {
        let money = SchemaReference {
            name: "Money".into(),
            subject: "av_money".into(),
            version: SchemaVersion(1),
        };
        for (name, record, key, value) in [
            (
                "schema",
                encode_schema(&value("t", SchemaType::Avro, vec![])),
                r#"{"keytype":"SCHEMA","subject":"t","version":1,"magic":1}"#,
                Some(r#"{"subject":"t","version":1,"id":2,"schema":"S","deleted":false}"#),
            ),
            (
                "schema with type and references",
                encode_schema(&value("pb", SchemaType::Protobuf, vec![money.clone()])),
                r#"{"keytype":"SCHEMA","subject":"pb","version":1,"magic":1}"#,
                Some(
                    r#"{"subject":"pb","version":1,"id":2,"schemaType":"PROTOBUF","references":[{"name":"Money","subject":"av_money","version":1}],"schema":"S","deleted":false}"#,
                ),
            ),
            (
                "tombstone",
                encode_tombstone("t", SchemaVersion(3)),
                r#"{"keytype":"SCHEMA","subject":"t","version":3,"magic":1}"#,
                None,
            ),
            (
                "global config",
                encode_config(None, "FULL"),
                r#"{"keytype":"CONFIG","subject":null,"magic":0}"#,
                Some(r#"{"compatibilityLevel":"FULL"}"#),
            ),
            (
                "subject config tombstone",
                config_tombstone(Some("s")),
                r#"{"keytype":"CONFIG","subject":"s","magic":0}"#,
                None,
            ),
            (
                "mode",
                encode_mode(Some("r"), "READONLY"),
                r#"{"keytype":"MODE","subject":"r","magic":0}"#,
                Some(r#"{"mode":"READONLY"}"#),
            ),
            (
                "global mode tombstone",
                mode_tombstone(None),
                r#"{"keytype":"MODE","subject":null,"magic":0}"#,
                None,
            ),
            (
                "delete subject",
                encode_delete_subject("d", SchemaVersion(2)),
                r#"{"keytype":"DELETE_SUBJECT","subject":"d","magic":0}"#,
                Some(r#"{"subject":"d","version":2}"#),
            ),
            (
                "version high water",
                encode_version_high_water("s", SchemaVersion(4)),
                r#"{"keytype":"NOOP","subject":"s","magic":0}"#,
                Some(r#"{"nextVersion":4}"#),
            ),
        ] {
            assert!(record.key == Bytes::from(key), "{name}");
            assert!(
                record.value.as_deref() == value.map(str::as_bytes),
                "{name}"
            );
        }
    }

    #[test]
    fn records_decode_to_what_was_encoded() {
        let schema = value("t", SchemaType::Json, vec![]);
        let cases: Vec<(&str, RawRecord, SchemaRecord)> = vec![
            (
                "schema",
                encode_schema(&schema),
                SchemaRecord::Schema(SchemaKey::new("t", SchemaVersion(1)), schema.clone()),
            ),
            (
                "tombstone",
                encode_tombstone("t", SchemaVersion(1)),
                SchemaRecord::Tombstone(SchemaKey::new("t", SchemaVersion(1))),
            ),
            (
                "config",
                encode_config(Some("s"), "NONE"),
                SchemaRecord::Config(
                    config_key(Some("s")),
                    Some(ConfigValue {
                        compatibility_level: "NONE".into(),
                    }),
                ),
            ),
            (
                "config clear",
                config_tombstone(None),
                SchemaRecord::Config(config_key(None), None),
            ),
            (
                "mode",
                encode_mode(None, "IMPORT"),
                SchemaRecord::Mode(
                    mode_key(None),
                    Some(ModeValue {
                        mode: "IMPORT".into(),
                    }),
                ),
            ),
            (
                "mode clear",
                mode_tombstone(Some("s")),
                SchemaRecord::Mode(mode_key(Some("s")), None),
            ),
            (
                "delete subject",
                encode_delete_subject("s", SchemaVersion(3)),
                SchemaRecord::DeleteSubject(
                    DeleteSubjectKey {
                        keytype: "DELETE_SUBJECT".into(),
                        subject: "s".into(),
                        magic: 0,
                    },
                    DeleteSubjectValue {
                        subject: "s".into(),
                        version: SchemaVersion(3),
                    },
                ),
            ),
            (
                "high water",
                encode_version_high_water("s", SchemaVersion(9)),
                SchemaRecord::VersionHighWater(
                    VersionHighWaterKey {
                        keytype: "NOOP".into(),
                        subject: "s".into(),
                        magic: 0,
                    },
                    VersionHighWaterValue {
                        next_version: SchemaVersion(9),
                    },
                ),
            ),
        ];
        for (name, raw, expected) in cases {
            assert!(SchemaRecord::decode(&raw) == expected, "{name}");
        }
    }

    #[test]
    fn foreign_and_broken_records_never_fail_to_decode() {
        let raw = |key: &'static [u8], value: Option<&'static [u8]>| RawRecord {
            key: Bytes::from_static(key),
            value: value.map(Bytes::from_static),
        };
        for (name, record, expected) in [
            (
                "confluent noop",
                raw(br#"{"keytype":"NOOP","magic":0}"#, Some(b"{}")),
                SchemaRecord::Noop,
            ),
            (
                "clear subjects",
                raw(
                    br#"{"keytype":"CLEAR_SUBJECTS","subject":"s","magic":0}"#,
                    None,
                ),
                SchemaRecord::Noop,
            ),
            (
                "delete subject tombstone",
                raw(
                    br#"{"keytype":"DELETE_SUBJECT","subject":"s","magic":0}"#,
                    None,
                ),
                SchemaRecord::Noop,
            ),
            (
                "unknown",
                raw(br#"{"keytype":"CONTEXT","magic":0}"#, Some(b"{}")),
                SchemaRecord::Unknown {
                    keytype: "CONTEXT".into(),
                },
            ),
            (
                "not json",
                raw(b"nope", None),
                SchemaRecord::Undecodable { keytype: None },
            ),
            (
                "bad schema value",
                raw(
                    br#"{"keytype":"SCHEMA","subject":"s","version":1,"magic":1}"#,
                    Some(b"[]"),
                ),
                SchemaRecord::Undecodable {
                    keytype: Some("SCHEMA".into()),
                },
            ),
        ] {
            assert!(SchemaRecord::decode(&record) == expected, "{name}");
        }
    }
}
