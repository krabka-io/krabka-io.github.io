//! Values serialized through a schema registry, in the Confluent wire format.
//!
//! Confluent's serializers frame a value as the magic byte `0`, the schema id
//! as a four-byte big-endian integer, and the body: the binary Avro encoding
//! of the datum with no container header ([`SchemaFormat::Avro`]), or the JSON
//! text of the document ([`SchemaFormat::Json`]). [`frame`] and [`unframe`]
//! handle the envelope; a [`ValueSchema`] turns a JSON document into a body
//! and back.
//!
//! The lab's records are JSON documents, so an Avro schema maps a document to
//! an Avro datum field by field: records from objects (missing fields take
//! their defaults), strings, `int` and `long` from integers in range, `float`
//! and `double` from any number, booleans, `null`, a union by the first branch
//! the value fits (`null` goes to the `null` branch), arrays, maps, enums from
//! their symbols, `bytes` and `fixed` from the UTF-8 bytes of a string, and the
//! date, time and timestamp logical types from integers. A number or a boolean
//! given for a `string` field is written as its JSON text, since a template's
//! whole-placeholder value turns into a number. A JSON Schema validates the
//! document as it is; a document the schema rejects is not serialized, as the
//! Confluent JSON Schema serializer refuses it.

use std::{collections::HashMap, fmt::Write as _};

use apache_avro::{
    Schema as AvroSchema,
    schema::{Name, RecordSchema, ResolvedSchema, UnionSchema},
    types::Value as AvroValue,
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use thiserror::Error;

/// The first byte of a Confluent-framed value.
pub const MAGIC_BYTE: u8 = 0;

/// The bytes before the body: the magic byte and the schema id.
const HEADER_LEN: usize = 5;

/// Frame `body` for `schema_id`: `0x00 | id (4 bytes, big-endian) | body`.
#[must_use]
pub fn frame(schema_id: i32, body: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(HEADER_LEN + body.len());
    out.put_u8(MAGIC_BYTE);
    out.put_i32(schema_id);
    out.put_slice(body);
    out.freeze()
}

/// The schema id and the body of a Confluent-framed value, or `None` when the
/// bytes do not start with the magic byte and a whole schema id.
#[must_use]
pub fn unframe(bytes: &[u8]) -> Option<(i32, &[u8])> {
    let (header, body) = bytes.split_at_checked(HEADER_LEN)?;
    let (magic, id) = header.split_first()?;
    if *magic != MAGIC_BYTE {
        return None;
    }
    let id = i32::from_be_bytes(id.try_into().ok()?);
    Some((id, body))
}

/// The schema language of a registered schema.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaFormat {
    Avro,
    Json,
}

impl SchemaFormat {
    /// The `schemaType` a registration request carries: none for Avro, which
    /// is the registry's default, as Confluent's clients send it.
    #[must_use]
    pub const fn registry_type(self) -> Option<&'static str> {
        match self {
            Self::Avro => None,
            Self::Json => Some("JSON"),
        }
    }

    /// The format a registry response names in `schemaType`, where a missing
    /// type is Avro.
    ///
    /// # Errors
    /// Returns [`SerdeError::UnsupportedType`] for a type the lab cannot
    /// decode, such as `PROTOBUF`.
    pub fn from_registry_type(ty: Option<&str>) -> Result<Self, SerdeError> {
        match ty {
            None | Some("AVRO") => Ok(Self::Avro),
            Some("JSON") => Ok(Self::Json),
            Some(other) => Err(SerdeError::UnsupportedType(other.to_string())),
        }
    }

    /// The lower-case name, as the config writes it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Avro => "avro",
            Self::Json => "json",
        }
    }
}

/// What can go wrong between a JSON document and a framed value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SerdeError {
    #[error("the schema does not parse: {0}")]
    Schema(String),
    #[error("the registry names schema type `{0}`, which the lab does not decode")]
    UnsupportedType(String),
    /// The document does not fit the schema.
    #[error("{0}")]
    Mismatch(String),
    /// The body is not a datum of the schema.
    #[error("the value does not decode: {0}")]
    Decode(String),
}

/// A parsed Avro schema with the named types it defines.
struct Avro {
    schema: AvroSchema,
    names: HashMap<Name, AvroSchema>,
}

enum Compiled {
    Avro(Box<Avro>),
    Json(Box<jsonschema::Validator>),
}

/// A schema that turns JSON documents into bodies and back. See the module
/// documentation.
pub struct ValueSchema {
    format: SchemaFormat,
    compiled: Compiled,
}

impl std::fmt::Debug for ValueSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueSchema")
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl ValueSchema {
    /// Parse the schema text in `format`.
    ///
    /// # Errors
    /// Returns [`SerdeError::Schema`] when the text is not a schema of the
    /// format.
    pub fn parse(format: SchemaFormat, text: &str) -> Result<Self, SerdeError> {
        let compiled = match format {
            SchemaFormat::Avro => {
                let schema =
                    AvroSchema::parse_str(text).map_err(|e| SerdeError::Schema(e.to_string()))?;
                let names = ResolvedSchema::try_from(&schema)
                    .map_err(|e| SerdeError::Schema(e.to_string()))?
                    .get_names()
                    .iter()
                    .map(|(name, schema)| (name.clone(), (*schema).clone()))
                    .collect();
                Compiled::Avro(Box::new(Avro { schema, names }))
            }
            SchemaFormat::Json => {
                let doc: Value =
                    serde_json::from_str(text).map_err(|e| SerdeError::Schema(e.to_string()))?;
                let validator = jsonschema::validator_for(&doc)
                    .map_err(|e| SerdeError::Schema(e.to_string()))?;
                Compiled::Json(Box::new(validator))
            }
        };
        Ok(Self { format, compiled })
    }

    #[must_use]
    pub fn format(&self) -> SchemaFormat {
        self.format
    }

    /// The body of `doc`: its Avro datum, or its JSON text once the schema
    /// accepts it.
    ///
    /// # Errors
    /// Returns [`SerdeError::Mismatch`] naming the first place the document
    /// does not fit the schema.
    pub fn encode(&self, doc: &Value) -> Result<Vec<u8>, SerdeError> {
        match &self.compiled {
            Compiled::Avro(avro) => {
                let datum = json_to_avro(doc, &avro.schema, &avro.names, "value")
                    .map_err(SerdeError::Mismatch)?;
                apache_avro::to_avro_datum(&avro.schema, datum)
                    .map_err(|e| SerdeError::Mismatch(e.to_string()))
            }
            Compiled::Json(validator) => {
                validator
                    .validate(doc)
                    .map_err(|e| SerdeError::Mismatch(e.to_string()))?;
                serde_json::to_vec(doc).map_err(|e| SerdeError::Mismatch(e.to_string()))
            }
        }
    }

    /// The JSON document a body holds.
    ///
    /// # Errors
    /// Returns [`SerdeError::Decode`] when the body is not a datum of the
    /// schema, or not JSON.
    pub fn decode(&self, body: &[u8]) -> Result<Value, SerdeError> {
        match &self.compiled {
            Compiled::Avro(avro) => {
                let mut reader = body;
                let datum = apache_avro::from_avro_datum(&avro.schema, &mut reader, None)
                    .map_err(|e| SerdeError::Decode(e.to_string()))?;
                Ok(avro_to_json(datum))
            }
            Compiled::Json(_) => {
                serde_json::from_slice(body).map_err(|e| SerdeError::Decode(e.to_string()))
            }
        }
    }
}

/// The Avro datum of `doc` for `schema`, resolving named types through
/// `names`. `path` names the place in the document, for the error.
fn json_to_avro(
    doc: &Value,
    schema: &AvroSchema,
    names: &HashMap<Name, AvroSchema>,
    path: &str,
) -> Result<AvroValue, String> {
    let mismatch = |expected: &str| format!("`{path}`: expected {expected}, got {doc}");
    match schema {
        AvroSchema::Null => match doc {
            Value::Null => Ok(AvroValue::Null),
            _ => Err(mismatch("null")),
        },
        AvroSchema::Boolean => doc
            .as_bool()
            .map(AvroValue::Boolean)
            .ok_or_else(|| mismatch("a boolean")),
        AvroSchema::Int => as_i32(doc)
            .map(AvroValue::Int)
            .ok_or_else(|| mismatch("an int")),
        AvroSchema::Long => as_i64(doc)
            .map(AvroValue::Long)
            .ok_or_else(|| mismatch("a long")),
        // Avro's own resolution narrows the double to the float it writes.
        AvroSchema::Float => doc
            .as_f64()
            .and_then(|x| AvroValue::Double(x).resolve(schema).ok())
            .ok_or_else(|| mismatch("a number")),
        AvroSchema::Double => doc
            .as_f64()
            .map(AvroValue::Double)
            .ok_or_else(|| mismatch("a number")),
        AvroSchema::String => match doc {
            Value::String(s) => Ok(AvroValue::String(s.clone())),
            Value::Number(_) | Value::Bool(_) => Ok(AvroValue::String(doc.to_string())),
            _ => Err(mismatch("a string")),
        },
        AvroSchema::Bytes => doc
            .as_str()
            .map(|s| AvroValue::Bytes(s.as_bytes().to_vec()))
            .ok_or_else(|| mismatch("a string of bytes")),
        AvroSchema::Fixed(fixed) => match doc.as_str() {
            Some(s) if s.len() == fixed.size => {
                Ok(AvroValue::Fixed(fixed.size, s.as_bytes().to_vec()))
            }
            _ => Err(mismatch(&format!("a string of {} bytes", fixed.size))),
        },
        AvroSchema::Enum(e) => {
            let symbol = doc.as_str().ok_or_else(|| mismatch("an enum symbol"))?;
            let index = e
                .symbols
                .iter()
                .position(|s| s == symbol)
                .ok_or_else(|| mismatch(&format!("one of {:?}", e.symbols)))?;
            Ok(AvroValue::Enum(
                u32::try_from(index).unwrap_or(u32::MAX),
                symbol.to_string(),
            ))
        }
        AvroSchema::Array(array) => {
            let items = doc.as_array().ok_or_else(|| mismatch("an array"))?;
            items
                .iter()
                .enumerate()
                .map(|(i, item)| json_to_avro(item, &array.items, names, &format!("{path}[{i}]")))
                .collect::<Result<_, _>>()
                .map(AvroValue::Array)
        }
        AvroSchema::Map(map) => {
            let fields = doc.as_object().ok_or_else(|| mismatch("an object"))?;
            fields
                .iter()
                .map(|(k, v)| {
                    json_to_avro(v, &map.types, names, &format!("{path}.{k}"))
                        .map(|v| (k.clone(), v))
                })
                .collect::<Result<_, _>>()
                .map(AvroValue::Map)
        }
        AvroSchema::Union(union) => union_to_avro(doc, union, names, path),
        AvroSchema::Record(record) => {
            let fields = doc.as_object().ok_or_else(|| mismatch("an object"))?;
            record_to_avro(fields, record, names, path)
        }
        AvroSchema::Ref { name } => {
            let target = names
                .get(name)
                .ok_or_else(|| format!("`{path}`: unknown named type `{name}`"))?;
            json_to_avro(doc, target, names, path)
        }
        other => logical_to_avro(doc, other, path),
    }
}

/// The datum of a logical type written as a number or a string.
fn logical_to_avro(doc: &Value, schema: &AvroSchema, path: &str) -> Result<AvroValue, String> {
    let mismatch = |expected: &str| format!("`{path}`: expected {expected}, got {doc}");
    match schema {
        AvroSchema::Date => as_i32(doc)
            .map(AvroValue::Date)
            .ok_or_else(|| mismatch("a date (days)")),
        AvroSchema::TimeMillis => as_i32(doc)
            .map(AvroValue::TimeMillis)
            .ok_or_else(|| mismatch("a time (ms)")),
        AvroSchema::TimeMicros => as_i64(doc)
            .map(AvroValue::TimeMicros)
            .ok_or_else(|| mismatch("a time (µs)")),
        AvroSchema::TimestampMillis => as_i64(doc)
            .map(AvroValue::TimestampMillis)
            .ok_or_else(|| mismatch("a timestamp (ms)")),
        AvroSchema::TimestampMicros => as_i64(doc)
            .map(AvroValue::TimestampMicros)
            .ok_or_else(|| mismatch("a timestamp (µs)")),
        AvroSchema::TimestampNanos => as_i64(doc)
            .map(AvroValue::TimestampNanos)
            .ok_or_else(|| mismatch("a timestamp (ns)")),
        AvroSchema::LocalTimestampMillis => as_i64(doc)
            .map(AvroValue::LocalTimestampMillis)
            .ok_or_else(|| mismatch("a local timestamp (ms)")),
        AvroSchema::LocalTimestampMicros => as_i64(doc)
            .map(AvroValue::LocalTimestampMicros)
            .ok_or_else(|| mismatch("a local timestamp (µs)")),
        AvroSchema::LocalTimestampNanos => as_i64(doc)
            .map(AvroValue::LocalTimestampNanos)
            .ok_or_else(|| mismatch("a local timestamp (ns)")),
        AvroSchema::Uuid => match doc {
            Value::String(s) => AvroValue::String(s.clone())
                .resolve(schema)
                .map_err(|_| mismatch("a UUID string")),
            _ => Err(mismatch("a UUID string")),
        },
        other => Err(format!(
            "`{path}`: the lab does not write the Avro type {other:?}"
        )),
    }
}

/// A record from an object: every field of the schema from the object, or
/// from its default when the object lacks it.
fn record_to_avro(
    fields: &Map<String, Value>,
    record: &RecordSchema,
    names: &HashMap<Name, AvroSchema>,
    path: &str,
) -> Result<AvroValue, String> {
    let mut out = Vec::with_capacity(record.fields.len());
    for field in &record.fields {
        let field_path = format!("{path}.{}", field.name);
        let value = match (fields.get(&field.name), &field.default) {
            (Some(value), _) => json_to_avro(value, &field.schema, names, &field_path)?,
            // A union field's default belongs to its first branch.
            (None, Some(default)) => match &field.schema {
                AvroSchema::Union(union) => {
                    let first = union
                        .variants()
                        .first()
                        .ok_or_else(|| format!("`{field_path}`: the union has no branch"))?;
                    AvroValue::Union(
                        0,
                        Box::new(json_to_avro(default, first, names, &field_path)?),
                    )
                }
                schema => json_to_avro(default, schema, names, &field_path)?,
            },
            (None, None) => {
                return Err(format!(
                    "`{field_path}`: missing, and the field has no default"
                ));
            }
        };
        out.push((field.name.clone(), value));
    }
    Ok(AvroValue::Record(out))
}

/// A union by the first branch the value fits; `null` takes the `null`
/// branch.
fn union_to_avro(
    doc: &Value,
    union: &UnionSchema,
    names: &HashMap<Name, AvroSchema>,
    path: &str,
) -> Result<AvroValue, String> {
    for (index, branch) in union.variants().iter().enumerate() {
        let fits_null = matches!(branch, AvroSchema::Null);
        if fits_null != doc.is_null() {
            continue;
        }
        if let Ok(value) = json_to_avro(doc, branch, names, path) {
            return Ok(AvroValue::Union(
                u32::try_from(index).unwrap_or(u32::MAX),
                Box::new(value),
            ));
        }
    }
    Err(format!("`{path}`: {doc} fits no branch of the union"))
}

/// An integer in `i64`; a JSON number with a fraction is not one.
fn as_i64(doc: &Value) -> Option<i64> {
    doc.as_i64()
}

/// An integer in `i32`.
fn as_i32(doc: &Value) -> Option<i32> {
    as_i64(doc).and_then(|n| i32::try_from(n).ok())
}

/// How many bytes of a value that is neither JSON nor text a preview shows.
const PREVIEW_BYTES: usize = 32;

/// A preview of raw record bytes for an inspector: the JSON document when the
/// bytes are JSON, the text when they are UTF-8, else `{"bytes": <length>,
/// "hex": <the first 32 bytes>}`.
#[must_use]
pub fn preview(bytes: &[u8]) -> Value {
    if let Ok(doc) = serde_json::from_slice::<Value>(bytes) {
        return doc;
    }
    if let Ok(text) = std::str::from_utf8(bytes) {
        return Value::String(text.to_string());
    }
    let hex = bytes
        .iter()
        .take(PREVIEW_BYTES)
        .fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        });
    serde_json::json!({ "bytes": bytes.len(), "hex": hex })
}

/// The JSON document of an Avro datum: records and maps as objects, enums as
/// their symbols, unions as their value, `bytes` and `fixed` as UTF-8 text
/// when they are text, else as arrays of numbers.
#[must_use]
pub fn avro_to_json(value: AvroValue) -> Value {
    match value {
        AvroValue::Null => Value::Null,
        AvroValue::Boolean(b) => Value::Bool(b),
        AvroValue::Int(n) | AvroValue::Date(n) | AvroValue::TimeMillis(n) => Value::from(n),
        AvroValue::Long(n)
        | AvroValue::TimeMicros(n)
        | AvroValue::TimestampMillis(n)
        | AvroValue::TimestampMicros(n)
        | AvroValue::TimestampNanos(n)
        | AvroValue::LocalTimestampMillis(n)
        | AvroValue::LocalTimestampMicros(n)
        | AvroValue::LocalTimestampNanos(n) => Value::from(n),
        AvroValue::Float(x) => Number::from_f64(f64::from(x)).map_or(Value::Null, Value::Number),
        AvroValue::Double(x) => Number::from_f64(x).map_or(Value::Null, Value::Number),
        AvroValue::String(s) | AvroValue::Enum(_, s) => Value::String(s),
        AvroValue::Bytes(bytes) | AvroValue::Fixed(_, bytes) => match String::from_utf8(bytes) {
            Ok(text) => Value::String(text),
            Err(e) => Value::Array(e.into_bytes().into_iter().map(Value::from).collect()),
        },
        AvroValue::Union(_, inner) => avro_to_json(*inner),
        AvroValue::Array(items) => Value::Array(items.into_iter().map(avro_to_json).collect()),
        AvroValue::Map(entries) => {
            let map: Map<String, Value> = entries
                .into_iter()
                .map(|(k, v)| (k, avro_to_json(v)))
                .collect();
            Value::Object(map)
        }
        AvroValue::Record(fields) => {
            let map: Map<String, Value> = fields
                .into_iter()
                .map(|(k, v)| (k, avro_to_json(v)))
                .collect();
            Value::Object(map)
        }
        AvroValue::Uuid(uuid) => Value::String(uuid.hyphenated().to_string()),
        other => serde_json::Value::try_from(other).unwrap_or(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;

    use super::*;

    const ORDER: &str = r#"{
        "type": "record", "name": "Order", "namespace": "lab",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "total", "type": "double"},
            {"name": "customer", "type": "string"},
            {"name": "note", "type": ["null", "string"], "default": null},
            {"name": "discount", "type": ["null", "double"], "default": null},
            {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
            {"name": "tags", "type": {"type": "array", "items": "string"}, "default": []},
            {"name": "attrs", "type": {"type": "map", "values": "int"}, "default": {}},
            {"name": "urgent", "type": "boolean", "default": false},
            {"name": "previous", "type": ["null", "Status"], "default": null}
        ]
    }"#;

    /// Avro's zig-zag varint of a long.
    fn zigzag(n: i64) -> Vec<u8> {
        let mut z = u64::from_ne_bytes(((n << 1) ^ (n >> 63)).to_ne_bytes());
        let mut out = Vec::new();
        loop {
            let byte = u8::try_from(z & 0x7F).unwrap();
            z >>= 7;
            if z == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn avro_string(s: &str) -> Vec<u8> {
        let mut out = zigzag(i64::try_from(s.len()).unwrap());
        out.extend_from_slice(s.as_bytes());
        out
    }

    #[test]
    fn frame_and_unframe_the_confluent_envelope() {
        let framed = frame(0x0102_0304, b"body");
        assert!(framed == Bytes::from_static(b"\x00\x01\x02\x03\x04body"));
        assert!(unframe(&framed) == Some((0x0102_0304, &b"body"[..])));
        assert!(unframe(b"\x00\x00\x00\x00\x07") == Some((7, &b""[..])));
        let cases: [&[u8]; 3] = [b"\x01\x00\x00\x00\x07x", b"\x00\x00\x00", b"{\"a\":1}"];
        for bytes in cases {
            assert!(unframe(bytes).is_none());
        }
    }

    #[test]
    fn an_avro_body_is_the_binary_datum_of_the_document() {
        let schema = ValueSchema::parse(SchemaFormat::Avro, ORDER).unwrap();
        let doc = json!({
            "id": 7,
            "total": 12,
            "customer": 42,
            "discount": 1.5,
            "status": "PAID",
            "tags": ["a"],
            "attrs": {"k": 3},
            "previous": "NEW",
            "extra": "ignored",
        });
        let body = schema.encode(&doc).unwrap();
        let mut expected = zigzag(7);
        expected.extend_from_slice(&12.0_f64.to_le_bytes());
        expected.extend(avro_string("42"));
        expected.extend(zigzag(0)); // note: the null branch
        expected.extend(zigzag(1)); // discount: the double branch
        expected.extend_from_slice(&1.5_f64.to_le_bytes());
        expected.extend(zigzag(1)); // status: PAID
        expected.extend(zigzag(1)); // tags: one item
        expected.extend(avro_string("a"));
        expected.extend(zigzag(0));
        expected.extend(zigzag(1)); // attrs: one entry
        expected.extend(avro_string("k"));
        expected.extend(zigzag(3));
        expected.extend(zigzag(0));
        expected.push(0); // urgent: false
        expected.extend(zigzag(1)); // previous: the Status branch
        expected.extend(zigzag(0)); // NEW
        assert!(body == expected);
        assert!(
            schema.decode(&body).unwrap()
                == json!({
                    "id": 7,
                    "total": 12.0,
                    "customer": "42",
                    "note": null,
                    "discount": 1.5,
                    "status": "PAID",
                    "tags": ["a"],
                    "attrs": {"k": 3},
                    "urgent": false,
                    "previous": "NEW",
                })
        );
    }

    #[test]
    fn a_document_that_does_not_fit_the_avro_schema_is_refused() {
        let schema = ValueSchema::parse(SchemaFormat::Avro, ORDER).unwrap();
        let base = json!({"id": 1, "total": 2.0, "customer": "c", "status": "NEW"});
        let with = |field: &str, value: Value| {
            let mut doc = base.clone();
            doc[field] = value;
            doc
        };
        let without = |field: &str| {
            let mut doc = base.clone();
            doc.as_object_mut().unwrap().remove(field);
            doc
        };
        let cases = [
            (
                without("id"),
                "`value.id`: missing, and the field has no default",
            ),
            (
                with("id", json!("x")),
                "`value.id`: expected a long, got \"x\"",
            ),
            (
                with("id", json!(1.5)),
                "`value.id`: expected a long, got 1.5",
            ),
            (
                with("status", json!("LOST")),
                "`value.status`: expected one of [\"NEW\", \"PAID\"], got \"LOST\"",
            ),
            (
                with("discount", json!("free")),
                "`value.discount`: \"free\" fits no branch of the union",
            ),
            (
                with("tags", json!(["a", {}])),
                "`value.tags[1]`: expected a string, got {}",
            ),
            (json!([1]), "`value`: expected an object, got [1]"),
        ];
        for (doc, message) in cases {
            assert!(
                schema.encode(&doc) == Err(SerdeError::Mismatch(message.to_string())),
                "{doc}"
            );
        }
    }

    #[test]
    fn int_fields_take_integers_in_range_only() {
        let schema = ValueSchema::parse(
            SchemaFormat::Avro,
            r#"{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}"#,
        )
        .unwrap();
        assert!(schema.encode(&json!({"n": 5})).unwrap() == zigzag(5));
        assert!(
            schema.encode(&json!({"n": 5.5}))
                == Err(SerdeError::Mismatch(
                    "`value.n`: expected an int, got 5.5".to_string()
                ))
        );
        assert!(
            schema.encode(&json!({"n": 3_000_000_000_i64}))
                == Err(SerdeError::Mismatch(
                    "`value.n`: expected an int, got 3000000000".to_string()
                ))
        );
    }

    #[test]
    fn a_json_schema_body_is_the_validated_json_text() {
        let schema = ValueSchema::parse(
            SchemaFormat::Json,
            r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#,
        )
        .unwrap();
        let body = schema.encode(&json!({"id": 3})).unwrap();
        assert!(body == br#"{"id":3}"#.to_vec());
        assert!(schema.decode(&body).unwrap() == json!({"id": 3}));
        assert!(let Err(SerdeError::Mismatch(_)) = schema.encode(&json!({"id": "x"})));
        assert!(let Err(SerdeError::Mismatch(_)) = schema.encode(&json!({})));
        assert!(let Err(SerdeError::Decode(_)) = schema.decode(b"{not json"));
    }

    #[test]
    fn schemas_that_do_not_parse_are_refused() {
        assert!(let Err(SerdeError::Schema(_)) = ValueSchema::parse(SchemaFormat::Avro, "{\"type\": \"nope\"}"));
        assert!(let Err(SerdeError::Schema(_)) = ValueSchema::parse(SchemaFormat::Json, "not json"));
        assert!(
            let Err(SerdeError::Schema(_)) =
                ValueSchema::parse(SchemaFormat::Json, r#"{"type": 12}"#)
        );
    }

    #[test]
    fn registry_types_map_to_formats() {
        let cases = [
            (None, Ok(SchemaFormat::Avro)),
            (Some("AVRO"), Ok(SchemaFormat::Avro)),
            (Some("JSON"), Ok(SchemaFormat::Json)),
            (
                Some("PROTOBUF"),
                Err(SerdeError::UnsupportedType("PROTOBUF".to_string())),
            ),
        ];
        for (ty, expected) in cases {
            assert!(SchemaFormat::from_registry_type(ty) == expected);
        }
        assert!(SchemaFormat::Avro.registry_type().is_none());
        assert!(SchemaFormat::Json.registry_type() == Some("JSON"));
    }

    #[test]
    fn a_framed_avro_value_round_trips() {
        let schema = ValueSchema::parse(SchemaFormat::Avro, r#""string""#).unwrap();
        let framed = frame(9, &schema.encode(&json!("hi")).unwrap());
        let (id, body) = unframe(&framed).unwrap();
        assert!(id == 9);
        assert!(schema.decode(body).unwrap() == json!("hi"));
    }
}
