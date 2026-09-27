//! Schema formats: parsing, the canonical identity that keys global ids, the
//! storage form, and the directional compatibility check of each format.
//!
//! Avro is checked by `apache-avro`, the engine `krabka-schema-registry`
//! uses. JSON Schema and Protobuf are checked by structural diffs written for
//! the lab, each a documented subset of Confluent's rules; see [`json`] and
//! [`protobuf`].

pub mod avro;
pub mod json;
pub mod protobuf;

use std::fmt;

use serde_json::Value;

use super::error::RegistryError;

/// A schema format, with its Confluent wire name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl SchemaType {
    /// Every type, in the order `GET /schemas/types` lists them.
    pub const ALL: [Self; 3] = [Self::Json, Self::Protobuf, Self::Avro];

    /// The type name as the REST API spells it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }

    /// The `schemaType` field of records and responses: absent for Avro.
    #[must_use]
    pub fn wire_name(self) -> Option<&'static str> {
        match self {
            Self::Avro => None,
            Self::Json | Self::Protobuf => Some(self.name()),
        }
    }

    /// The type a `schemaType` field names; absent or empty means Avro, and an
    /// unknown name is `None`.
    #[must_use]
    pub fn from_wire(name: Option<&str>) -> Option<Self> {
        match name {
            None | Some("" | "AVRO") => Some(Self::Avro),
            Some("JSON") => Some(Self::Json),
            Some("PROTOBUF") => Some(Self::Protobuf),
            Some(_) => None,
        }
    }
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A referenced schema resolved from the store, ready for a parser. `name` is
/// the label the referring schema uses: an Avro type name, a Protobuf import
/// path, or a JSON Schema `$ref` target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedReference {
    pub name: String,
    pub ty: SchemaType,
    pub schema: String,
}

/// A schema that parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSchema {
    /// The identity that keys the global id: two schemas with the same
    /// canonical form and references share an id.
    pub canonical: String,
    /// The text the registry stores and echoes: the input verbatim for Avro
    /// and JSON Schema, the normalised text for Protobuf, as Confluent does.
    pub storage_form: String,
}

/// Parse `schema` as `ty` with its resolved references in scope.
///
/// # Errors
/// Returns [`RegistryError::InvalidSchema`] when the schema does not parse or
/// names a type its references do not provide.
pub fn parse(
    ty: SchemaType,
    schema: &str,
    refs: &[ResolvedReference],
) -> Result<ParsedSchema, RegistryError> {
    match ty {
        SchemaType::Avro => {
            avro::parse(schema, refs)?;
            Ok(ParsedSchema {
                canonical: avro::identity(schema)?,
                storage_form: schema.to_string(),
            })
        }
        SchemaType::Json => {
            let parsed = json::parse(schema, refs)?;
            Ok(ParsedSchema {
                canonical: parsed.canonical_form(),
                storage_form: schema.to_string(),
            })
        }
        SchemaType::Protobuf => {
            let parsed = protobuf::parse(schema, refs)?;
            let normalized = parsed.normalized_form().to_string();
            Ok(ParsedSchema {
                canonical: normalized.clone(),
                storage_form: normalized,
            })
        }
    }
}

/// The form `?normalize=true` registers: key-sorted compact JSON for Avro and
/// JSON Schema, the normalised text for Protobuf.
///
/// # Errors
/// Returns [`RegistryError::InvalidSchema`] when the schema does not parse.
pub fn normalize(
    ty: SchemaType,
    schema: &str,
    refs: &[ResolvedReference],
) -> Result<String, RegistryError> {
    match ty {
        SchemaType::Avro | SchemaType::Json => Ok(parse(ty, schema, refs)?.canonical),
        SchemaType::Protobuf => Ok(parse(ty, schema, refs)?.storage_form),
    }
}

/// Whether a reader that uses `reader` can read data written with `writer`.
/// Returns the checker's messages, one per incompatible difference.
///
/// # Errors
/// Returns the messages when the pair is incompatible or one side does not
/// parse.
pub fn check(
    ty: SchemaType,
    reader: &str,
    writer: &str,
    reader_refs: &[ResolvedReference],
    writer_refs: &[ResolvedReference],
) -> Result<(), Vec<String>> {
    match ty {
        SchemaType::Avro => avro::check(reader, writer, reader_refs, writer_refs),
        SchemaType::Json => json::check(reader, writer, reader_refs, writer_refs),
        SchemaType::Protobuf => protobuf::check(reader, writer, reader_refs, writer_refs),
    }
}

/// Compact JSON with object keys sorted at every level, so formatting and key
/// order never change a schema's identity.
#[must_use]
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap_or_default(),
                        canonical_json(&map[k])
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Confluent's wording for one incompatible difference:
/// `Found incompatible change: Difference{<label>='<path>', type=<KIND>}`,
/// where the kind is the `Debug` name of `kind` in screaming snake case.
#[must_use]
pub fn incompatible_message(kind: &dyn fmt::Debug, path_label: &str, path: &str) -> String {
    let debug = format!("{kind:?}");
    let name: String = debug
        .chars()
        .take_while(char::is_ascii_alphanumeric)
        .collect();
    let mut screaming = String::with_capacity(name.len() + 8);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            screaming.push('_');
        }
        screaming.push(c.to_ascii_uppercase());
    }
    format!("Found incompatible change: Difference{{{path_label}='{path}', type={screaming}}}")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn schema_type_wire_names_round_trip() {
        for (ty, wire) in [
            (SchemaType::Avro, None),
            (SchemaType::Json, Some("JSON")),
            (SchemaType::Protobuf, Some("PROTOBUF")),
        ] {
            assert!(ty.wire_name() == wire);
            assert!(SchemaType::from_wire(wire) == Some(ty));
        }
        assert!(SchemaType::from_wire(Some("")) == Some(SchemaType::Avro));
        assert!(SchemaType::from_wire(Some("AVRO")) == Some(SchemaType::Avro));
        assert!(SchemaType::from_wire(Some("THRIFT")).is_none());
        assert!(SchemaType::ALL.map(SchemaType::name) == ["JSON", "PROTOBUF", "AVRO"]);
    }

    #[test]
    fn canonical_json_sorts_keys_and_strips_whitespace() {
        let value: Value =
            serde_json::from_str(r#"{ "b": [1, {"y": 2, "x": 1}], "a": "s" }"#).unwrap();
        assert!(canonical_json(&value) == r#"{"a":"s","b":[1,{"x":1,"y":2}]}"#);
    }

    #[test]
    fn parse_dedups_formatting_but_keeps_annotations() {
        let plain = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;
        let spaced = "{ \"fields\":[ {\"type\":\"int\", \"name\":\"id\"} ], \"name\":\"U\", \"type\":\"record\" }";
        let documented =
            r#"{"type":"record","name":"U","doc":"kept","fields":[{"name":"id","type":"int"}]}"#;
        let a = parse(SchemaType::Avro, plain, &[]).unwrap();
        let b = parse(SchemaType::Avro, spaced, &[]).unwrap();
        let c = parse(SchemaType::Avro, documented, &[]).unwrap();
        assert!(a.canonical == b.canonical);
        assert!(a.canonical != c.canonical);
        assert!(b.storage_form == spaced);
        assert!(normalize(SchemaType::Avro, spaced, &[]).unwrap() == a.canonical);
        assert!(parse(SchemaType::Avro, "{not avro}", &[]).is_err());
    }

    #[test]
    fn protobuf_storage_form_is_the_normalised_text() {
        let parsed = parse(
            SchemaType::Protobuf,
            "syntax = \"proto3\"; message U { int32 id = 1; }",
            &[],
        )
        .unwrap();
        assert!(parsed.storage_form == "syntax = \"proto3\";\n\nmessage U {\n  int32 id = 1;\n}\n");
        assert!(parsed.canonical == parsed.storage_form);
    }

    #[test]
    fn incompatible_messages_use_confluent_wording() {
        #[derive(Debug)]
        enum Kind {
            PropertyAddedToOpenContentModel,
            Grouped { same: bool },
        }
        let grouped = Kind::Grouped { same: true };
        let Kind::Grouped { same } = grouped else {
            panic!("grouped");
        };
        assert!(same);
        assert!(
            incompatible_message(&Kind::PropertyAddedToOpenContentModel, "jsonPath", "#/x")
                == "Found incompatible change: Difference{jsonPath='#/x', type=PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL}"
        );
        assert!(
            incompatible_message(&grouped, "fullPath", "U.#1")
                == "Found incompatible change: Difference{fullPath='U.#1', type=GROUPED}"
        );
    }
}
