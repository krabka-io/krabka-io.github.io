//! Avro through `apache-avro`: parsing with references in scope, the
//! canonical identity, and reader/writer compatibility through
//! [`SchemaCompatibility::can_read`].
//!
//! The crate implements the Avro resolution rules but not three adjustments
//! the Java implementation makes before it compares, so they are applied to
//! the JSON first: logical types are erased (they never change the encoding),
//! a reader alias matches the writer's name, and a reader enum with a
//! `default` accepts symbols the writer has and the reader lacks.

use apache_avro::{
    Schema,
    schema_compatibility::{Compatibility, SchemaCompatibility},
};
use serde_json::Value;

use super::{ResolvedReference, canonical_json};
use crate::lab::registry::error::RegistryError;

fn invalid(error: &apache_avro::Error) -> RegistryError {
    RegistryError::InvalidSchema(format!("Avro: {error}"))
}

/// Parse an Avro schema. References are parsed first so their named types
/// are in scope for the candidate.
///
/// # Errors
/// Returns [`RegistryError::InvalidSchema`] when the schema or a reference
/// does not parse.
pub fn parse(schema: &str, refs: &[ResolvedReference]) -> Result<Schema, RegistryError> {
    if refs.is_empty() {
        return Schema::parse_str(schema).map_err(|e| invalid(&e));
    }
    let sources: Vec<&str> = refs
        .iter()
        .map(|r| r.schema.as_str())
        .chain(std::iter::once(schema))
        .collect();
    // `parse_list` keeps the input order, so the candidate is the last entry.
    Schema::parse_list(sources)
        .map_err(|e| invalid(&e))?
        .pop()
        .ok_or_else(|| RegistryError::InvalidSchema("Avro: empty parse list".to_string()))
}

/// The identity of a schema: its JSON with keys sorted and whitespace
/// removed, so formatting does not create a new id while `doc`, `default`
/// and other annotations still do.
///
/// # Errors
/// Returns [`RegistryError::InvalidSchema`] when the text is not JSON.
pub fn identity(schema: &str) -> Result<String, RegistryError> {
    serde_json::from_str::<Value>(schema)
        .map(|v| canonical_json(&v))
        .map_err(|e| RegistryError::InvalidSchema(format!("Avro: {e}")))
}

/// Whether a reader that uses `reader` can read data written with `writer`.
///
/// # Errors
/// Returns the checker's message when the pair is incompatible, or the parse
/// error when a side does not parse.
pub fn check(
    reader: &str,
    writer: &str,
    reader_refs: &[ResolvedReference],
    writer_refs: &[ResolvedReference],
) -> Result<(), Vec<String>> {
    let mut reader_value: Value =
        serde_json::from_str(reader).map_err(|e| vec![format!("reader: Avro: {e}")])?;
    let mut writer_value: Value =
        serde_json::from_str(writer).map_err(|e| vec![format!("writer: Avro: {e}")])?;
    erase_logical_types(&mut reader_value);
    erase_logical_types(&mut writer_value);
    apply_reader_rules(&mut reader_value, &mut writer_value);
    let reader_schema =
        parse(&reader_value.to_string(), reader_refs).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_schema =
        parse(&writer_value.to_string(), writer_refs).map_err(|e| vec![format!("writer: {e}")])?;
    match SchemaCompatibility::can_read(&writer_schema, &reader_schema) {
        Ok(Compatibility::Full) => Ok(()),
        // Some writer data resolves and some does not: an enum symbol the
        // reader lacks, or a union branch it cannot take. Confluent rejects
        // the pair.
        Ok(Compatibility::Partial) => Err(vec![
            "reader resolves only part of what the writer can produce".to_owned(),
        ]),
        Err(e) => Err(vec![e.to_string()]),
    }
}

/// Drop `logicalType` (and the decimal attributes that go with it) at every
/// level: the encoding is the underlying type's.
fn erase_logical_types(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if object.remove("logicalType").is_some() {
                object.remove("precision");
                object.remove("scale");
            }
            object.values_mut().for_each(erase_logical_types);
        }
        Value::Array(items) => items.iter_mut().for_each(erase_logical_types),
        _ => {}
    }
}

/// Rename the writer to a reader alias and widen a defaulted reader enum, then
/// recurse into the fields the two records share.
fn apply_reader_rules(reader: &mut Value, writer: &mut Value) {
    let (Some(reader_object), Some(writer_object)) =
        (reader.as_object_mut(), writer.as_object_mut())
    else {
        if let (Some(reader_items), Some(writer_items)) =
            (reader.as_array_mut(), writer.as_array_mut())
        {
            for (r, w) in reader_items.iter_mut().zip(writer_items) {
                apply_reader_rules(r, w);
            }
        }
        return;
    };

    let reader_name = reader_object.get("name").and_then(Value::as_str);
    let writer_name = writer_object.get("name").and_then(Value::as_str);
    if let (Some(reader_name), Some(writer_name)) = (reader_name, writer_name)
        && reader_name != writer_name
        && reader_object
            .get("aliases")
            .and_then(Value::as_array)
            .is_some_and(|aliases| aliases.iter().any(|a| a.as_str() == Some(writer_name)))
    {
        writer_object.insert("name".to_string(), reader_name.into());
    }

    if reader_object.get("type").and_then(Value::as_str) == Some("enum")
        && writer_object.get("type").and_then(Value::as_str) == Some("enum")
    {
        let default = reader_object
            .get("default")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let writer_symbols = writer_object
            .get("symbols")
            .and_then(Value::as_array)
            .cloned();
        if let (Some(default), Some(writer_symbols), Some(reader_symbols)) = (
            default,
            writer_symbols,
            reader_object
                .get_mut("symbols")
                .and_then(Value::as_array_mut),
        ) && reader_symbols.iter().any(|s| s.as_str() == Some(&default))
        {
            for symbol in writer_symbols {
                if !reader_symbols.contains(&symbol) {
                    reader_symbols.push(symbol);
                }
            }
        }
    }

    if let (Some(reader_fields), Some(writer_fields)) = (
        reader_object
            .get_mut("fields")
            .and_then(Value::as_array_mut),
        writer_object
            .get_mut("fields")
            .and_then(Value::as_array_mut),
    ) {
        for reader_field in reader_fields {
            let Some(name) = reader_field.get("name").and_then(Value::as_str) else {
                continue;
            };
            let Some(writer_field) = writer_fields
                .iter_mut()
                .find(|f| f.get("name").and_then(Value::as_str) == Some(name))
            else {
                continue;
            };
            if let (Some(reader_type), Some(writer_type)) =
                (reader_field.get_mut("type"), writer_field.get_mut("type"))
            {
                apply_reader_rules(reader_type, writer_type);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::lab::registry::format::SchemaType;

    fn record(fields: &str) -> String {
        format!("{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{fields}]}}")
    }

    const ID: &str = r#"{"name":"id","type":"int"}"#;

    #[test]
    fn adding_a_field_needs_a_default_to_stay_backward_compatible() {
        let old = record(ID);
        let defaulted = record(&format!(
            "{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"
        ));
        let required = record(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        for (name, reader, writer, compatible) in [
            ("new reads old with default", &defaulted, &old, true),
            ("old reads new", &old, &defaulted, true),
            ("new reads old without default", &required, &old, false),
            ("old reads new without default", &old, &required, true),
        ] {
            assert!(
                check(reader, writer, &[], &[]).is_ok() == compatible,
                "{name}"
            );
        }
        let messages = check(&required, &old, &[], &[]).unwrap_err();
        assert!(messages.len() == 1);
        assert!(messages[0].contains("default"));
    }

    #[test]
    fn named_references_resolve_through_the_reference_list() {
        let money = r#"{"type":"record","name":"Money","fields":[{"name":"cents","type":"long"}]}"#;
        let order =
            r#"{"type":"record","name":"Order","fields":[{"name":"price","type":"Money"}]}"#;
        let refs = vec![ResolvedReference {
            name: "Money".into(),
            ty: SchemaType::Avro,
            schema: money.into(),
        }];
        assert!(parse(order, &[]).is_err());
        assert!(parse(order, &refs).is_ok());
        assert!(check(order, order, &refs, &refs).is_ok());
    }

    #[test]
    fn java_rules_for_logical_types_enum_defaults_and_aliases_apply() {
        for (name, reader, writer) in [
            (
                "logical types",
                r#"{"type":"long","logicalType":"timestamp-micros"}"#,
                r#"{"type":"long","logicalType":"timestamp-millis"}"#,
            ),
            (
                "enum default",
                r#"{"type":"enum","name":"E","symbols":["A"],"default":"A"}"#,
                r#"{"type":"enum","name":"E","symbols":["A","B"]}"#,
            ),
            (
                "record alias",
                r#"{"type":"record","name":"New","aliases":["Old"],"fields":[]}"#,
                r#"{"type":"record","name":"Old","fields":[]}"#,
            ),
        ] {
            assert!(check(reader, writer, &[], &[]).is_ok(), "{name}");
        }
        assert!(
            check(
                r#"{"type":"enum","name":"E","symbols":["A"]}"#,
                r#"{"type":"enum","name":"E","symbols":["A","B"]}"#,
                &[],
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn identity_is_key_sorted_compact_json() {
        assert!(identity("{ \"type\" : \"string\" }").unwrap() == r#"{"type":"string"}"#);
        assert!(identity("\"string\"").unwrap() == "\"string\"");
        assert!(identity("{").is_err());
    }
}
