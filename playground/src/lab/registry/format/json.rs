//! JSON Schema: validity through the `jsonschema` crate's meta-schema
//! validation, a canonical identity of key-sorted compact JSON, and a subset
//! of Confluent's structural compatibility rules.
//!
//! # The compatibility subset
//!
//! The checker diffs the writer's schema (the original) against the reader's
//! (the update) and rejects the pair when a difference is
//! backward-incompatible, with Confluent's classification for each kind. It
//! covers:
//!
//! - `type`: narrowed, extended (`integer` to `number`, or a wider list) or
//!   changed;
//! - `properties` added or removed, judged against the other side's content
//!   model: open (no `additionalProperties`, or `true`), closed (`false`), or
//!   partially open (an `additionalProperties` schema, which the added or
//!   removed property is then compared against);
//! - `required` added (with or without a `default`) or removed;
//! - `additionalProperties` added, removed, narrowed or extended, and the
//!   schemas compared when both sides carry one;
//! - `enum` and `const` narrowed, extended or changed;
//! - the numeric, string, array and object-size bounds (`maximum`, `minimum`,
//!   `exclusiveMaximum`, `exclusiveMinimum`, `multipleOf`, `maxLength`,
//!   `minLength`, `pattern`, `maxItems`, `minItems`, `uniqueItems`,
//!   `maxProperties`, `minProperties`), `items` (recursed into, or compared
//!   pairwise for a tuple) and `additionalItems`;
//! - `allOf`, `anyOf` and `oneOf`: branches are matched pairwise by
//!   compatibility, so a list is extended, narrowed or changed; `not`;
//! - `dependencies`, `dependentRequired` and `dependentSchemas`: array
//!   dependencies narrowed or extended, schema dependencies recursed into;
//! - `if`/`then`/`else`: any change is incompatible;
//! - `$ref`: a `#/...` pointer resolves against its own document and any
//!   other target against the registered references by name, before the
//!   targets are diffed; an unresolvable target is permissive.
//!
//! Not covered: `patternProperties`. Matching a property against a pattern
//! needs a regular-expression engine the lab does not carry, so a schema with
//! `patternProperties` and no `additionalProperties` is a partially open
//! model in which an added property counts as compatible and a removed one as
//! incompatible.

use std::collections::BTreeSet;

use serde_json::{Map, Value};

use super::{ResolvedReference, canonical_json, incompatible_message};
use crate::lab::registry::error::RegistryError;

/// A parsed JSON Schema with the registry references it may `$ref`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonSchema {
    value: Value,
    /// `(name, document)` pairs; `name` is the `$ref` target a referring
    /// schema uses. References never join the canonical form, because
    /// Confluent does not inline them.
    refs: Vec<(String, Value)>,
}

impl JsonSchema {
    /// The identity that keys the global id.
    #[must_use]
    pub fn canonical_form(&self) -> String {
        canonical_json(&self.value)
    }

    /// The schema document.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }
}

fn invalid(message: impl Into<String>) -> RegistryError {
    RegistryError::InvalidSchema(message.into())
}

/// Parse a JSON Schema and validate it against the draft it declares
/// (2020-12 when it declares none). A `$schema` the crate does not bundle is
/// accepted as written, because the lab cannot fetch it.
///
/// # Errors
/// Returns [`RegistryError::InvalidSchema`] when the text is not JSON, is
/// neither an object nor a boolean, or violates its meta-schema.
pub fn parse(schema: &str, refs: &[ResolvedReference]) -> Result<JsonSchema, RegistryError> {
    let value: Value =
        serde_json::from_str(schema).map_err(|e| invalid(format!("JSON Schema: {e}")))?;
    if !value.is_object() && !value.is_boolean() {
        return Err(invalid("JSON Schema must be an object or boolean"));
    }
    if jsonschema::Draft::default().detect(&value) != jsonschema::Draft::Unknown {
        jsonschema::meta::validate(&value).map_err(|e| invalid(format!("JSON Schema: {e}")))?;
    }
    let refs = refs
        .iter()
        .filter_map(|r| {
            serde_json::from_str::<Value>(&r.schema)
                .ok()
                .map(|v| (r.name.clone(), v))
        })
        .collect();
    Ok(JsonSchema { value, refs })
}

/// Whether a reader that uses `reader` can read data written with `writer`.
///
/// # Errors
/// Returns one message per incompatible difference, or the parse error when a
/// side does not parse.
pub fn check(
    reader: &str,
    writer: &str,
    reader_refs: &[ResolvedReference],
    writer_refs: &[ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader = parse(reader, reader_refs).map_err(|e| vec![format!("reader: {e}")])?;
    let writer = parse(writer, writer_refs).map_err(|e| vec![format!("writer: {e}")])?;
    let messages: Vec<String> = compare(&writer.value, &reader.value, &writer.refs, &reader.refs)
        .iter()
        .filter(|d| !d.kind.is_backward_compatible())
        .map(|d| incompatible_message(&d.kind, "jsonPath", &d.path))
        .collect();
    if messages.is_empty() {
        Ok(())
    } else {
        Err(messages)
    }
}

/// One structural difference, named as Confluent names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    TypeNarrowed,
    TypeExtended,
    TypeChanged,
    PropertyAddedToOpenContentModel,
    PropertyRemovedFromOpenContentModel,
    PropertyAddedToClosedContentModel,
    PropertyRemovedFromClosedContentModel,
    PropertyAddedCoveredByPartiallyOpenContentModel,
    PropertyAddedNotCoveredByPartiallyOpenContentModel,
    PropertyRemovedCoveredByPartiallyOpenContentModel,
    PropertyRemovedNotCoveredByPartiallyOpenContentModel,
    PropertyWithEmptySchemaAddedToOpenContentModel,
    RequiredAttributeAdded,
    RequiredAttributeRemoved,
    RequiredAttributeWithDefaultAdded,
    RequiredPropertyWithDefaultAddedToClosedContentModel,
    AdditionalPropertiesRemoved,
    AdditionalPropertiesAdded,
    AdditionalPropertiesNarrowed,
    AdditionalPropertiesExtended,
    EnumArrayNarrowed,
    EnumArrayExtended,
    EnumArrayChanged,
    MaximumAdded,
    MaximumRemoved,
    MaximumDecreased,
    MaximumIncreased,
    MinimumAdded,
    MinimumRemoved,
    MinimumDecreased,
    MinimumIncreased,
    ExclusiveMaximumAdded,
    ExclusiveMaximumRemoved,
    ExclusiveMaximumDecreased,
    ExclusiveMaximumIncreased,
    ExclusiveMinimumAdded,
    ExclusiveMinimumRemoved,
    ExclusiveMinimumDecreased,
    ExclusiveMinimumIncreased,
    MultipleOfAdded,
    MultipleOfRemoved,
    MultipleOfReduced,
    MultipleOfExpanded,
    MultipleOfChanged,
    MaxLengthAdded,
    MaxLengthRemoved,
    MaxLengthDecreased,
    MaxLengthIncreased,
    MinLengthAdded,
    MinLengthRemoved,
    MinLengthDecreased,
    MinLengthIncreased,
    PatternAdded,
    PatternRemoved,
    PatternChanged,
    MaxItemsAdded,
    MaxItemsRemoved,
    MaxItemsDecreased,
    MaxItemsIncreased,
    MinItemsAdded,
    MinItemsRemoved,
    MinItemsDecreased,
    MinItemsIncreased,
    AdditionalItemsRemoved,
    AdditionalItemsAdded,
    AdditionalItemsNarrowed,
    AdditionalItemsExtended,
    UniqueItemsAdded,
    UniqueItemsRemoved,
    MaxPropertiesAdded,
    MaxPropertiesRemoved,
    MaxPropertiesDecreased,
    MaxPropertiesIncreased,
    MinPropertiesAdded,
    MinPropertiesRemoved,
    MinPropertiesDecreased,
    MinPropertiesIncreased,
    CombinedTypeChanged,
    CombinedTypeExtended,
    CombinedTypeSubschemasChanged,
    ProductTypeExtended,
    ProductTypeNarrowed,
    SumTypeExtended,
    SumTypeNarrowed,
    NotTypeExtended,
    NotTypeNarrowed,
    DependencyArrayAdded,
    DependencyArrayRemoved,
    DependencyArrayExtended,
    DependencyArrayNarrowed,
    DependencyArrayChanged,
    DependencySchemaAdded,
    DependencySchemaRemoved,
    ConditionalChanged,
}

impl Kind {
    /// Whether a reader with this difference from the writer still reads the
    /// writer's data: Confluent's classification.
    #[must_use]
    pub fn is_backward_compatible(self) -> bool {
        matches!(
            self,
            Self::TypeExtended
                | Self::PropertyRemovedFromOpenContentModel
                | Self::PropertyAddedToClosedContentModel
                | Self::PropertyAddedCoveredByPartiallyOpenContentModel
                | Self::PropertyAddedNotCoveredByPartiallyOpenContentModel
                | Self::PropertyRemovedCoveredByPartiallyOpenContentModel
                | Self::PropertyWithEmptySchemaAddedToOpenContentModel
                | Self::RequiredAttributeRemoved
                | Self::RequiredAttributeWithDefaultAdded
                | Self::RequiredPropertyWithDefaultAddedToClosedContentModel
                | Self::AdditionalPropertiesAdded
                | Self::AdditionalPropertiesExtended
                | Self::EnumArrayExtended
                | Self::MaximumRemoved
                | Self::MaximumIncreased
                | Self::MinimumRemoved
                | Self::MinimumDecreased
                | Self::ExclusiveMaximumRemoved
                | Self::ExclusiveMaximumIncreased
                | Self::ExclusiveMinimumRemoved
                | Self::ExclusiveMinimumDecreased
                | Self::MultipleOfRemoved
                | Self::MultipleOfReduced
                | Self::MaxLengthRemoved
                | Self::MaxLengthIncreased
                | Self::MinLengthRemoved
                | Self::MinLengthDecreased
                | Self::PatternRemoved
                | Self::MaxItemsRemoved
                | Self::MaxItemsIncreased
                | Self::MinItemsRemoved
                | Self::MinItemsDecreased
                | Self::AdditionalItemsAdded
                | Self::AdditionalItemsExtended
                | Self::UniqueItemsRemoved
                | Self::MaxPropertiesRemoved
                | Self::MaxPropertiesIncreased
                | Self::MinPropertiesRemoved
                | Self::MinPropertiesDecreased
                | Self::CombinedTypeExtended
                | Self::ProductTypeNarrowed
                | Self::SumTypeExtended
                | Self::NotTypeNarrowed
                | Self::DependencyArrayRemoved
                | Self::DependencyArrayNarrowed
                | Self::DependencySchemaRemoved
        )
    }
}

/// A difference at a JSON pointer into the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub kind: Kind,
    pub path: String,
}

fn d(kind: Kind, path: &str) -> Difference {
    Difference {
        kind,
        path: path.to_string(),
    }
}

/// The added/removed/decreased/increased kinds of one numeric keyword.
type BoundKinds = (Kind, Kind, Kind, Kind);

/// Each side's document root and reference map, plus the `$ref` pairs on the
/// current walk so a recursive reference terminates.
struct DiffCtx<'a> {
    old_root: &'a Value,
    new_root: &'a Value,
    old_refs: &'a [(String, Value)],
    new_refs: &'a [(String, Value)],
    visiting: BTreeSet<(String, String)>,
}

impl<'a> DiffCtx<'a> {
    fn new(
        old_root: &'a Value,
        new_root: &'a Value,
        old_refs: &'a [(String, Value)],
        new_refs: &'a [(String, Value)],
    ) -> Self {
        Self {
            old_root,
            new_root,
            old_refs,
            new_refs,
            visiting: BTreeSet::new(),
        }
    }

    fn fresh(&self) -> Self {
        Self::new(self.old_root, self.new_root, self.old_refs, self.new_refs)
    }
}

/// Diff two schema documents: every difference of `update` from `original`.
#[must_use]
pub fn compare(
    original: &Value,
    update: &Value,
    original_refs: &[(String, Value)],
    update_refs: &[(String, Value)],
) -> Vec<Difference> {
    let mut out = Vec::new();
    let mut ctx = DiffCtx::new(original, update, original_refs, update_refs);
    compare_schema("#", original, update, &mut ctx, &mut out);
    out
}

fn compare_schema(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    if compare_refs(path, old, new, ctx, out) {
        return;
    }
    compare_type(path, old, new, out);
    compare_enum(path, old, new, out);
    compare_properties(path, old, new, ctx, out);
    compare_required(path, old, new, out);
    compare_additional_properties(path, old, new, ctx, out);
    compare_numeric(path, old, new, out);
    compare_string(path, old, new, out);
    compare_array(path, old, new, ctx, out);
    compare_object_size(path, old, new, out);
    compare_combinators(path, old, new, ctx, out);
    compare_dependencies(path, old, new, ctx, out);
    compare_conditionals(path, old, new, ctx, out);
}

// ---- type ---------------------------------------------------------------------

fn types_of(schema: &Value) -> BTreeSet<String> {
    match schema.get("type") {
        Some(Value::String(s)) => BTreeSet::from([s.clone()]),
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn compare_type(path: &str, old: &Value, new: &Value, out: &mut Vec<Difference>) {
    let (old_types, new_types) = (types_of(old), types_of(new));
    if old_types == new_types {
        return;
    }
    let only = |t: &str| BTreeSet::from([t.to_string()]);
    let kind = if old_types == only("integer") && new_types == only("number") {
        Kind::TypeExtended
    } else if (old_types == only("number") && new_types == only("integer"))
        || (old_types.is_empty() && !new_types.is_empty())
    {
        Kind::TypeNarrowed
    } else if new_types.is_empty() || old_types.is_subset(&new_types) {
        Kind::TypeExtended
    } else if new_types.is_subset(&old_types) {
        Kind::TypeNarrowed
    } else {
        Kind::TypeChanged
    };
    out.push(d(kind, path));
}

// ---- properties -----------------------------------------------------------------

/// What a schema says about properties it does not list.
enum ContentModel<'a> {
    Open,
    Closed,
    /// `additionalProperties` is a schema every extra property must match.
    Schema(&'a Value),
    /// `patternProperties` without `additionalProperties`; see the module
    /// documentation.
    Partial,
}

fn content_model(schema: &Value) -> ContentModel<'_> {
    match schema.get("additionalProperties") {
        Some(Value::Bool(false)) => ContentModel::Closed,
        Some(Value::Bool(true)) => ContentModel::Open,
        Some(value) if value.is_object() => ContentModel::Schema(value),
        _ if schema
            .get("patternProperties")
            .is_some_and(Value::is_object) =>
        {
            ContentModel::Partial
        }
        _ => ContentModel::Open,
    }
}

fn props(schema: &Value) -> Option<&Map<String, Value>> {
    schema.get("properties").and_then(Value::as_object)
}

fn required_set(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn compare_properties(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    let empty = Map::new();
    let old_props = props(old).unwrap_or(&empty);
    let new_props = props(new).unwrap_or(&empty);
    for (name, old_schema) in old_props {
        if new_props.contains_key(name) {
            continue;
        }
        let property_path = format!("{path}/properties/{name}");
        match content_model(new) {
            ContentModel::Open => {
                out.push(d(Kind::PropertyRemovedFromOpenContentModel, &property_path));
            }
            ContentModel::Closed => {
                out.push(d(
                    Kind::PropertyRemovedFromClosedContentModel,
                    &property_path,
                ));
            }
            ContentModel::Partial => out.push(d(
                Kind::PropertyRemovedNotCoveredByPartiallyOpenContentModel,
                &property_path,
            )),
            ContentModel::Schema(allowed) => {
                out.push(d(
                    Kind::PropertyRemovedCoveredByPartiallyOpenContentModel,
                    &property_path,
                ));
                compare_schema(&property_path, old_schema, allowed, ctx, out);
            }
        }
    }
    for (name, new_schema) in new_props {
        let property_path = format!("{path}/properties/{name}");
        if let Some(old_schema) = old_props.get(name) {
            compare_schema(&property_path, old_schema, new_schema, ctx, out);
            continue;
        }
        compare_added_property(&property_path, name, old, new, new_schema, ctx, out);
    }
}

fn compare_added_property(
    property_path: &str,
    name: &str,
    old: &Value,
    new: &Value,
    new_schema: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    let required = required_set(new).contains(name);
    let has_default = new_schema.get("default").is_some();
    if required && has_default && !matches!(content_model(old), ContentModel::Open) {
        out.push(d(
            Kind::RequiredPropertyWithDefaultAddedToClosedContentModel,
            property_path,
        ));
        return;
    }
    if required && !has_default {
        out.push(d(Kind::RequiredAttributeAdded, property_path));
    }
    match content_model(old) {
        ContentModel::Open if new_schema.as_object().is_some_and(Map::is_empty) => out.push(d(
            Kind::PropertyWithEmptySchemaAddedToOpenContentModel,
            property_path,
        )),
        ContentModel::Open => out.push(d(Kind::PropertyAddedToOpenContentModel, property_path)),
        ContentModel::Closed => {
            out.push(d(Kind::PropertyAddedToClosedContentModel, property_path));
        }
        ContentModel::Partial => out.push(d(
            Kind::PropertyAddedNotCoveredByPartiallyOpenContentModel,
            property_path,
        )),
        ContentModel::Schema(allowed) => {
            out.push(d(
                Kind::PropertyAddedCoveredByPartiallyOpenContentModel,
                property_path,
            ));
            compare_schema(property_path, allowed, new_schema, ctx, out);
        }
    }
}

fn compare_required(path: &str, old: &Value, new: &Value, out: &mut Vec<Difference>) {
    let (old_required, new_required) = (required_set(old), required_set(new));
    let empty = Map::new();
    let old_props = props(old).unwrap_or(&empty);
    let new_props = props(new).unwrap_or(&empty);
    for name in new_required
        .difference(&old_required)
        .filter(|name| old_props.contains_key(*name) && new_props.contains_key(*name))
    {
        let kind = if new_props[name].get("default").is_some() {
            Kind::RequiredAttributeWithDefaultAdded
        } else {
            Kind::RequiredAttributeAdded
        };
        out.push(d(kind, &format!("{path}/required/{name}")));
    }
    for name in old_required.difference(&new_required) {
        out.push(d(
            Kind::RequiredAttributeRemoved,
            &format!("{path}/required/{name}"),
        ));
    }
}

fn compare_additional_properties(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    let old_extra = old.get("additionalProperties");
    let new_extra = new.get("additionalProperties");
    let old_closed = matches!(old_extra, Some(Value::Bool(false)));
    let new_closed = matches!(new_extra, Some(Value::Bool(false)));
    if old_closed && !new_closed {
        out.push(d(Kind::AdditionalPropertiesAdded, path));
    } else if !old_closed && new_closed {
        out.push(d(Kind::AdditionalPropertiesRemoved, path));
    } else if old_extra.is_none() && new_extra.is_some_and(|v| !v.is_boolean()) {
        out.push(d(Kind::AdditionalPropertiesNarrowed, path));
    } else if new_extra.is_none() && old_extra.is_some_and(|v| !v.is_boolean()) {
        out.push(d(Kind::AdditionalPropertiesExtended, path));
    } else if let (Some(old_schema), Some(new_schema)) = (old_extra, new_extra)
        && !old_schema.is_boolean()
        && !new_schema.is_boolean()
    {
        compare_schema(
            &format!("{path}/additionalProperties"),
            old_schema,
            new_schema,
            ctx,
            out,
        );
    }
}

// ---- enum and const -----------------------------------------------------------------

fn enum_set(schema: &Value) -> Option<BTreeSet<String>> {
    if let Some(items) = schema.get("enum").and_then(Value::as_array) {
        Some(items.iter().map(canonical_json).collect())
    } else {
        schema
            .get("const")
            .map(|c| BTreeSet::from([canonical_json(c)]))
    }
}

fn compare_enum(path: &str, old: &Value, new: &Value, out: &mut Vec<Difference>) {
    match (enum_set(old), enum_set(new)) {
        (Some(old_set), Some(new_set)) if old_set != new_set => {
            let kind = if new_set.is_subset(&old_set) {
                Kind::EnumArrayNarrowed
            } else if old_set.is_subset(&new_set) {
                Kind::EnumArrayExtended
            } else {
                Kind::EnumArrayChanged
            };
            out.push(d(kind, path));
        }
        (None, Some(_)) => out.push(d(Kind::EnumArrayNarrowed, path)),
        (Some(_), None) => out.push(d(Kind::EnumArrayExtended, path)),
        _ => {}
    }
}

// ---- bounds ---------------------------------------------------------------------

fn number(schema: &Value, key: &str) -> Option<f64> {
    schema.get(key).and_then(Value::as_f64)
}

fn compare_bound(
    path: &str,
    old: &Value,
    new: &Value,
    key: &str,
    kinds: BoundKinds,
    out: &mut Vec<Difference>,
) {
    let (added, removed, decreased, increased) = kinds;
    match (number(old, key), number(new, key)) {
        (None, Some(_)) => out.push(d(added, path)),
        (Some(_), None) => out.push(d(removed, path)),
        (Some(old_bound), Some(new_bound)) => match new_bound.partial_cmp(&old_bound) {
            Some(std::cmp::Ordering::Less) => out.push(d(decreased, path)),
            Some(std::cmp::Ordering::Greater) => out.push(d(increased, path)),
            _ => {}
        },
        (None, None) => {}
    }
}

fn compare_numeric(path: &str, old: &Value, new: &Value, out: &mut Vec<Difference>) {
    let bounds: [(&str, BoundKinds); 4] = [
        (
            "maximum",
            (
                Kind::MaximumAdded,
                Kind::MaximumRemoved,
                Kind::MaximumDecreased,
                Kind::MaximumIncreased,
            ),
        ),
        (
            "minimum",
            (
                Kind::MinimumAdded,
                Kind::MinimumRemoved,
                Kind::MinimumDecreased,
                Kind::MinimumIncreased,
            ),
        ),
        (
            "exclusiveMaximum",
            (
                Kind::ExclusiveMaximumAdded,
                Kind::ExclusiveMaximumRemoved,
                Kind::ExclusiveMaximumDecreased,
                Kind::ExclusiveMaximumIncreased,
            ),
        ),
        (
            "exclusiveMinimum",
            (
                Kind::ExclusiveMinimumAdded,
                Kind::ExclusiveMinimumRemoved,
                Kind::ExclusiveMinimumDecreased,
                Kind::ExclusiveMinimumIncreased,
            ),
        ),
    ];
    for (key, kinds) in bounds {
        compare_bound(path, old, new, key, kinds, out);
    }
    match (number(old, "multipleOf"), number(new, "multipleOf")) {
        (None, Some(_)) => out.push(d(Kind::MultipleOfAdded, path)),
        (Some(_), None) => out.push(d(Kind::MultipleOfRemoved, path)),
        (Some(old_step), Some(new_step))
            if old_step.partial_cmp(&new_step) != Some(std::cmp::Ordering::Equal) =>
        {
            let divisible = |larger: f64, smaller: f64| {
                let quotient = larger / smaller;
                (quotient - quotient.round()).abs() <= f64::EPSILON * quotient.abs().max(1.0)
            };
            let kind = if divisible(old_step, new_step) {
                Kind::MultipleOfReduced
            } else if divisible(new_step, old_step) {
                Kind::MultipleOfExpanded
            } else {
                Kind::MultipleOfChanged
            };
            out.push(d(kind, path));
        }
        _ => {}
    }
}

fn compare_string(path: &str, old: &Value, new: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        old,
        new,
        "maxLength",
        (
            Kind::MaxLengthAdded,
            Kind::MaxLengthRemoved,
            Kind::MaxLengthDecreased,
            Kind::MaxLengthIncreased,
        ),
        out,
    );
    compare_bound(
        path,
        old,
        new,
        "minLength",
        (
            Kind::MinLengthAdded,
            Kind::MinLengthRemoved,
            Kind::MinLengthDecreased,
            Kind::MinLengthIncreased,
        ),
        out,
    );
    match (
        old.get("pattern").and_then(Value::as_str),
        new.get("pattern").and_then(Value::as_str),
    ) {
        (None, Some(_)) => out.push(d(Kind::PatternAdded, path)),
        (Some(_), None) => out.push(d(Kind::PatternRemoved, path)),
        (Some(old_pattern), Some(new_pattern)) if old_pattern != new_pattern => {
            out.push(d(Kind::PatternChanged, path));
        }
        _ => {}
    }
}

fn compare_array(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    match (old.get("items"), new.get("items")) {
        (Some(old_items), Some(new_items)) if old_items.is_object() && new_items.is_object() => {
            compare_schema(&format!("{path}/items"), old_items, new_items, ctx, out);
        }
        (Some(Value::Array(old_items)), Some(Value::Array(new_items))) => {
            for (index, (old_item, new_item)) in old_items.iter().zip(new_items).enumerate() {
                compare_schema(
                    &format!("{path}/items/{index}"),
                    old_item,
                    new_item,
                    ctx,
                    out,
                );
            }
            if new_items.len() > old_items.len() {
                out.push(d(Kind::AdditionalItemsRemoved, &format!("{path}/items")));
            } else if old_items.len() > new_items.len() {
                out.push(d(Kind::AdditionalItemsAdded, &format!("{path}/items")));
            }
        }
        (Some(_), Some(_)) => out.push(d(Kind::TypeChanged, &format!("{path}/items"))),
        _ => {}
    }
    compare_bound(
        path,
        old,
        new,
        "maxItems",
        (
            Kind::MaxItemsAdded,
            Kind::MaxItemsRemoved,
            Kind::MaxItemsDecreased,
            Kind::MaxItemsIncreased,
        ),
        out,
    );
    compare_bound(
        path,
        old,
        new,
        "minItems",
        (
            Kind::MinItemsAdded,
            Kind::MinItemsRemoved,
            Kind::MinItemsDecreased,
            Kind::MinItemsIncreased,
        ),
        out,
    );
    compare_additional_items(path, old, new, ctx, out);
    let unique = |schema: &Value| {
        schema
            .get("uniqueItems")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    };
    match (unique(old), unique(new)) {
        (false, true) => out.push(d(Kind::UniqueItemsAdded, path)),
        (true, false) => out.push(d(Kind::UniqueItemsRemoved, path)),
        _ => {}
    }
}

fn compare_additional_items(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    let old_extra = old.get("additionalItems");
    let new_extra = new.get("additionalItems");
    let old_closed = matches!(old_extra, Some(Value::Bool(false)));
    let new_closed = matches!(new_extra, Some(Value::Bool(false)));
    if !old_closed && new_closed {
        out.push(d(Kind::AdditionalItemsRemoved, path));
    } else if old_closed && !new_closed {
        out.push(d(Kind::AdditionalItemsAdded, path));
    } else if old_extra.is_none() && new_extra.is_some_and(|v| !v.is_boolean()) {
        out.push(d(Kind::AdditionalItemsNarrowed, path));
    } else if new_extra.is_none() && old_extra.is_some_and(|v| !v.is_boolean()) {
        out.push(d(Kind::AdditionalItemsExtended, path));
    } else if let (Some(old_schema), Some(new_schema)) = (old_extra, new_extra)
        && !old_schema.is_boolean()
        && !new_schema.is_boolean()
    {
        compare_schema(
            &format!("{path}/additionalItems"),
            old_schema,
            new_schema,
            ctx,
            out,
        );
    }
}

fn compare_object_size(path: &str, old: &Value, new: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        old,
        new,
        "maxProperties",
        (
            Kind::MaxPropertiesAdded,
            Kind::MaxPropertiesRemoved,
            Kind::MaxPropertiesDecreased,
            Kind::MaxPropertiesIncreased,
        ),
        out,
    );
    compare_bound(
        path,
        old,
        new,
        "minProperties",
        (
            Kind::MinPropertiesAdded,
            Kind::MinPropertiesRemoved,
            Kind::MinPropertiesDecreased,
            Kind::MinPropertiesIncreased,
        ),
        out,
    );
}

// ---- combinators ----------------------------------------------------------------

fn branches<'a>(schema: &'a Value, keyword: &str) -> Option<&'a [Value]> {
    schema
        .get(keyword)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

/// The `anyOf` or `oneOf` branches of a schema, with the keyword.
fn sum_branches(schema: &Value) -> Option<(&'static str, &[Value])> {
    branches(schema, "anyOf")
        .map(|b| ("anyOf", b))
        .or_else(|| branches(schema, "oneOf").map(|b| ("oneOf", b)))
}

/// Whether `new` reads everything `old` allows: no incompatible difference.
fn branch_compatible(old: &Value, new: &Value, ctx: &DiffCtx<'_>) -> bool {
    let mut diffs = Vec::new();
    compare_schema("#", old, new, &mut ctx.fresh(), &mut diffs);
    diffs.iter().all(|diff| diff.kind.is_backward_compatible())
}

/// The size of a maximum matching of old branches onto compatible new
/// branches (augmenting paths over the compatibility graph).
fn maximum_matching(old: &[Value], new: &[Value], ctx: &DiffCtx<'_>) -> usize {
    fn augment(
        index: usize,
        edges: &[Vec<usize>],
        seen: &mut [bool],
        matched: &mut [Option<usize>],
    ) -> bool {
        for &candidate in &edges[index] {
            if seen[candidate] {
                continue;
            }
            seen[candidate] = true;
            if matched[candidate].is_none_or(|owner| augment(owner, edges, seen, matched)) {
                matched[candidate] = Some(index);
                return true;
            }
        }
        false
    }
    let edges: Vec<Vec<usize>> = old
        .iter()
        .map(|old_branch| {
            new.iter()
                .enumerate()
                .filter(|(_, new_branch)| branch_compatible(old_branch, new_branch, ctx))
                .map(|(index, _)| index)
                .collect()
        })
        .collect();
    let mut matched = vec![None; new.len()];
    (0..old.len())
        .filter(|&index| augment(index, &edges, &mut vec![false; new.len()], &mut matched))
        .count()
}

/// How a branch list changed: matched pairwise, then extended or narrowed.
fn list_change(old: &[Value], new: &[Value], ctx: &DiffCtx<'_>) -> Option<std::cmp::Ordering> {
    let matched = maximum_matching(old, new, ctx);
    (matched >= old.len().min(new.len())).then(|| new.len().cmp(&old.len()))
}

fn compare_combinators(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    match (branches(old, "allOf"), branches(new, "allOf")) {
        (Some(old_all), Some(new_all)) if old_all != new_all => {
            let all_path = format!("{path}/allOf");
            match list_change(old_all, new_all, ctx) {
                None => out.push(d(Kind::CombinedTypeSubschemasChanged, &all_path)),
                Some(std::cmp::Ordering::Greater) => {
                    out.push(d(Kind::ProductTypeExtended, &all_path));
                }
                Some(std::cmp::Ordering::Less) => out.push(d(Kind::ProductTypeNarrowed, &all_path)),
                Some(std::cmp::Ordering::Equal) => {}
            }
        }
        (Some(_), None) | (None, Some(_)) => out.push(d(Kind::CombinedTypeChanged, path)),
        _ => {}
    }
    match (sum_branches(old), sum_branches(new)) {
        (Some((old_kind, _)), Some((new_kind, _))) if old_kind != new_kind => {
            let kind = if new_kind == "anyOf" {
                Kind::CombinedTypeExtended
            } else {
                Kind::CombinedTypeChanged
            };
            out.push(d(kind, path));
        }
        (Some((keyword, old_sum)), Some((_, new_sum))) if old_sum != new_sum => {
            let sum_path = format!("{path}/{keyword}");
            match list_change(old_sum, new_sum, ctx) {
                None => out.push(d(Kind::CombinedTypeSubschemasChanged, &sum_path)),
                Some(std::cmp::Ordering::Greater) => out.push(d(Kind::SumTypeExtended, &sum_path)),
                Some(std::cmp::Ordering::Less) => out.push(d(Kind::SumTypeNarrowed, &sum_path)),
                Some(std::cmp::Ordering::Equal) => {}
            }
        }
        (None, Some((keyword, new_sum))) => {
            let kind = if new_sum.iter().any(|b| branch_compatible(old, b, ctx)) {
                Kind::SumTypeExtended
            } else {
                Kind::CombinedTypeChanged
            };
            out.push(d(kind, &format!("{path}/{keyword}")));
        }
        (Some((keyword, old_sum)), None) => {
            let kind = if old_sum.iter().all(|b| branch_compatible(b, new, ctx)) {
                Kind::SumTypeNarrowed
            } else {
                Kind::CombinedTypeChanged
            };
            out.push(d(kind, &format!("{path}/{keyword}")));
        }
        _ => {}
    }
    match (old.get("not"), new.get("not")) {
        (Some(old_not), Some(new_not)) if old_not != new_not => {
            let kind = if branch_compatible(new_not, old_not, ctx) {
                Kind::NotTypeNarrowed
            } else {
                Kind::NotTypeExtended
            };
            out.push(d(kind, &format!("{path}/not")));
        }
        (Some(_), None) | (None, Some(_)) => out.push(d(Kind::CombinedTypeChanged, path)),
        _ => {}
    }
}

// ---- $ref -------------------------------------------------------------------------

/// Resolve a `$ref`: a `#` pointer against `root`, anything else against the
/// registered references by name; `None` when it cannot be resolved.
fn resolve_ref<'a>(
    schema: &Value,
    root: &'a Value,
    refs: &'a [(String, Value)],
) -> Option<&'a Value> {
    let target = schema.get("$ref").and_then(Value::as_str)?;
    if let Some(pointer) = target.strip_prefix('#') {
        return if pointer.is_empty() {
            Some(root)
        } else {
            root.pointer(pointer)
        };
    }
    refs.iter().find(|(name, _)| name == target).map(|(_, v)| v)
}

/// Diff through `$ref`s. Returns whether a side carried one, in which case
/// the resolved targets were compared instead of the schemas themselves.
fn compare_refs(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) -> bool {
    let old_ref = old.get("$ref").and_then(Value::as_str).map(String::from);
    let new_ref = new.get("$ref").and_then(Value::as_str).map(String::from);
    if old_ref.is_none() && new_ref.is_none() {
        return false;
    }
    let key = (
        old_ref.clone().unwrap_or_default(),
        new_ref.clone().unwrap_or_default(),
    );
    if !ctx.visiting.insert(key.clone()) {
        // The same pair is already on the walk: a recursive schema.
        return true;
    }
    let old_target = old_ref
        .as_deref()
        .and_then(|_| resolve_ref(old, ctx.old_root, ctx.old_refs))
        .cloned();
    let new_target = new_ref
        .as_deref()
        .and_then(|_| resolve_ref(new, ctx.new_root, ctx.new_refs))
        .cloned();
    match (old_target, new_target) {
        (Some(old_target), Some(new_target)) => {
            compare_schema(&format!("{path}/$ref"), &old_target, &new_target, ctx, out);
        }
        (Some(old_target), None) => compare_schema(path, &old_target, new, ctx, out),
        (None, Some(new_target)) => compare_schema(path, old, &new_target, ctx, out),
        // An unresolvable target on either side is permissive.
        (None, None) => {}
    }
    ctx.visiting.remove(&key);
    true
}

// ---- dependencies and conditionals ----------------------------------------------

fn dependency_kind(value: &Value, added: bool) -> Kind {
    match (value.is_array(), added) {
        (true, true) => Kind::DependencyArrayAdded,
        (true, false) => Kind::DependencyArrayRemoved,
        (false, true) => Kind::DependencySchemaAdded,
        (false, false) => Kind::DependencySchemaRemoved,
    }
}

fn compare_dependencies(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    let empty = Map::new();
    for keyword in ["dependencies", "dependentRequired", "dependentSchemas"] {
        let old_deps = old
            .get(keyword)
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let new_deps = new
            .get(keyword)
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        for (name, old_dep) in old_deps {
            let dependency_path = format!("{path}/{keyword}/{name}");
            let Some(new_dep) = new_deps.get(name) else {
                out.push(d(dependency_kind(old_dep, false), &dependency_path));
                continue;
            };
            match (old_dep.as_array(), new_dep.as_array()) {
                (Some(old_list), Some(new_list)) => {
                    let old_set: BTreeSet<&str> =
                        old_list.iter().filter_map(Value::as_str).collect();
                    let new_set: BTreeSet<&str> =
                        new_list.iter().filter_map(Value::as_str).collect();
                    if old_set == new_set {
                        continue;
                    }
                    let kind = if new_set.is_superset(&old_set) {
                        Kind::DependencyArrayExtended
                    } else if old_set.is_superset(&new_set) {
                        Kind::DependencyArrayNarrowed
                    } else {
                        Kind::DependencyArrayChanged
                    };
                    out.push(d(kind, &dependency_path));
                }
                (None, None) if old_dep.is_object() && new_dep.is_object() => {
                    compare_schema(&dependency_path, old_dep, new_dep, ctx, out);
                }
                _ if old_dep != new_dep => {
                    out.push(d(Kind::DependencyArrayChanged, &dependency_path));
                }
                _ => {}
            }
        }
        for (name, new_dep) in new_deps {
            if !old_deps.contains_key(name) {
                out.push(d(
                    dependency_kind(new_dep, true),
                    &format!("{path}/{keyword}/{name}"),
                ));
            }
        }
    }
}

fn compare_conditionals(
    path: &str,
    old: &Value,
    new: &Value,
    ctx: &mut DiffCtx<'_>,
    out: &mut Vec<Difference>,
) {
    for keyword in ["if", "then", "else"] {
        match (old.get(keyword), new.get(keyword)) {
            (Some(old_branch), Some(new_branch)) => {
                if canonical_json(old_branch) != canonical_json(new_branch) {
                    let branch_path = format!("{path}/{keyword}");
                    out.push(d(Kind::ConditionalChanged, &branch_path));
                    compare_schema(&branch_path, old_branch, new_branch, ctx, out);
                }
            }
            (None, Some(_)) | (Some(_), None) => {
                out.push(d(Kind::ConditionalChanged, &format!("{path}/{keyword}")));
            }
            (None, None) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;

    use super::*;
    use crate::lab::registry::format::SchemaType;

    fn compatible(reader: &str, writer: &str) -> bool {
        check(reader, writer, &[], &[]).is_ok()
    }

    #[test]
    fn parse_validates_against_the_meta_schema() {
        assert!(parse(r#"{"type":"object"}"#, &[]).is_ok());
        assert!(parse("true", &[]).is_ok());
        assert!(
            parse(
                r#"{"$schema":"http://json-schema.org/draft-07/schema#","type":"string"}"#,
                &[]
            )
            .is_ok()
        );
        assert!(parse(r#"{"type":"nope"}"#, &[]).is_err());
        assert!(parse(r#"{"minimum":"x"}"#, &[]).is_err());
        assert!(parse("[]", &[]).is_err());
        assert!(parse("not json", &[]).is_err());
        let a = parse(
            r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#,
            &[],
        )
        .unwrap();
        let b = parse(
            r#"{"properties":{"a":{"type":"integer"}},"type":"object"}"#,
            &[],
        )
        .unwrap();
        assert!(a.canonical_form() == b.canonical_form());
    }

    #[test]
    fn optional_property_added_is_compatible_only_under_a_closed_model() {
        // Confluent: under BACKWARD a new optional property is compatible only
        // when the earlier schema had `additionalProperties: false`; an open
        // earlier schema let writers put anything under that name.
        let closed_old = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"string"}}}"#;
        let closed_new = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"string"},"b":{"type":"integer"}}}"#;
        let open_old = r#"{"type":"object","properties":{"a":{"type":"string"}}}"#;
        let open_new =
            r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}}}"#;
        for (name, reader, writer, expected) in [
            (
                "closed: add optional property",
                closed_new,
                closed_old,
                true,
            ),
            ("closed: remove property", closed_old, closed_new, false),
            ("open: add optional property", open_new, open_old, false),
            ("open: remove property", open_old, open_new, true),
        ] {
            assert!(compatible(reader, writer) == expected, "{name}");
        }
        let messages = check(open_new, open_old, &[], &[]).unwrap_err();
        assert!(
            messages
                == vec![
                    "Found incompatible change: Difference{jsonPath='#/properties/b', type=PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL}"
                        .to_string()
                ]
        );
    }

    #[test]
    fn type_and_property_rules_are_pinned() {
        for (name, reader, writer, expected) in [
            (
                "integer to number",
                r#"{"type":"number"}"#,
                r#"{"type":"integer"}"#,
                true,
            ),
            (
                "number to integer",
                r#"{"type":"integer"}"#,
                r#"{"type":"number"}"#,
                false,
            ),
            (
                "type list widened",
                r#"{"type":["string","null"]}"#,
                r#"{"type":"string"}"#,
                true,
            ),
            (
                "type list narrowed",
                r#"{"type":"string"}"#,
                r#"{"type":["string","null"]}"#,
                false,
            ),
            ("type dropped", "{}", r#"{"type":"string"}"#, true),
            (
                "type changed",
                r#"{"type":"boolean"}"#,
                r#"{"type":"string"}"#,
                false,
            ),
            (
                "required added",
                r#"{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}"#,
                r#"{"type":"object","properties":{"a":{"type":"string"}}}"#,
                false,
            ),
            (
                "required with default added",
                r#"{"type":"object","properties":{"a":{"type":"string","default":""}},"required":["a"]}"#,
                r#"{"type":"object","properties":{"a":{"type":"string","default":""}}}"#,
                true,
            ),
            (
                "required removed",
                r#"{"type":"object","properties":{"a":{"type":"string"}}}"#,
                r#"{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}"#,
                true,
            ),
        ] {
            assert!(compatible(reader, writer) == expected, "{name}");
        }
    }

    #[test]
    fn bound_rules_are_pinned() {
        for (name, reader, writer, expected) in [
            (
                "enum extended",
                r#"{"enum":["a","b"]}"#,
                r#"{"enum":["a"]}"#,
                true,
            ),
            (
                "enum narrowed",
                r#"{"enum":["a"]}"#,
                r#"{"enum":["a","b"]}"#,
                false,
            ),
            (
                "enum changed",
                r#"{"enum":["a","c"]}"#,
                r#"{"enum":["a","b"]}"#,
                false,
            ),
            (
                "maximum raised",
                r#"{"maximum":10}"#,
                r#"{"maximum":5}"#,
                true,
            ),
            (
                "maximum lowered",
                r#"{"maximum":5}"#,
                r#"{"maximum":10}"#,
                false,
            ),
            ("minimum added", r#"{"minimum":1}"#, "{}", false),
            (
                "multipleOf reduced",
                r#"{"multipleOf":2}"#,
                r#"{"multipleOf":4}"#,
                true,
            ),
            (
                "multipleOf expanded",
                r#"{"multipleOf":4}"#,
                r#"{"multipleOf":2}"#,
                false,
            ),
            (
                "maxLength removed",
                r#"{"type":"string"}"#,
                r#"{"type":"string","maxLength":3}"#,
                true,
            ),
            (
                "pattern added",
                r#"{"type":"string","pattern":"^a"}"#,
                r#"{"type":"string"}"#,
                false,
            ),
            (
                "items narrowed",
                r#"{"type":"array","items":{"type":"integer"}}"#,
                r#"{"type":"array","items":{"type":"number"}}"#,
                false,
            ),
            (
                "uniqueItems added",
                r#"{"type":"array","uniqueItems":true}"#,
                r#"{"type":"array"}"#,
                false,
            ),
        ] {
            assert!(compatible(reader, writer) == expected, "{name}");
        }
    }

    #[test]
    fn object_and_combinator_rules_are_pinned() {
        for (name, reader, writer, expected) in [
            (
                "additionalProperties closed",
                r#"{"type":"object","additionalProperties":false}"#,
                r#"{"type":"object"}"#,
                false,
            ),
            (
                "additionalProperties opened",
                r#"{"type":"object"}"#,
                r#"{"type":"object","additionalProperties":false}"#,
                true,
            ),
            (
                "additionalProperties narrowed",
                r#"{"type":"object","additionalProperties":{"type":"string"}}"#,
                r#"{"type":"object"}"#,
                false,
            ),
            (
                "plain to anyOf",
                r#"{"anyOf":[{"type":"string"},{"type":"integer"}]}"#,
                r#"{"type":"string"}"#,
                true,
            ),
            (
                "anyOf branch dropped",
                r#"{"anyOf":[{"type":"string"}]}"#,
                r#"{"anyOf":[{"type":"string"},{"type":"integer"}]}"#,
                false,
            ),
            (
                "oneOf to anyOf",
                r#"{"anyOf":[{"type":"string"}]}"#,
                r#"{"oneOf":[{"type":"string"}]}"#,
                true,
            ),
            (
                "allOf branch added",
                r#"{"allOf":[{"type":"object"},{"minProperties":1}]}"#,
                r#"{"allOf":[{"type":"object"}]}"#,
                false,
            ),
            (
                "dependency removed",
                r#"{"type":"object"}"#,
                r#"{"type":"object","dependencies":{"a":["b"]}}"#,
                true,
            ),
            (
                "dependency added",
                r#"{"type":"object","dependencies":{"a":["b"]}}"#,
                r#"{"type":"object"}"#,
                false,
            ),
            (
                "conditional changed",
                r#"{"if":{"type":"string"},"then":{"minLength":2}}"#,
                r#"{"if":{"type":"string"},"then":{"minLength":1}}"#,
                false,
            ),
        ] {
            assert!(compatible(reader, writer) == expected, "{name}");
        }
    }

    #[test]
    fn refs_resolve_locally_and_through_registered_references() {
        let local = json!({"$ref": "#/$defs/T", "$defs": {"T": {"type": "integer"}}});
        let inline = json!({"type": "integer"});
        assert!(compare(&local, &inline, &[], &[]).is_empty());
        assert!(compare(&inline, &local, &[], &[]).is_empty());
        let recursive = json!({"$ref": "#"});
        assert!(compare(&recursive, &recursive, &[], &[]).is_empty());

        let amount = |maximum: u32| ResolvedReference {
            name: "Amount".into(),
            ty: SchemaType::Json,
            schema: json!({"type": "integer", "maximum": maximum}).to_string(),
        };
        let with_ref = r#"{"type":"object","properties":{"a":{"$ref":"Amount"}}}"#;
        let refs_old = [amount(10)];
        let refs_new = [amount(5)];
        assert!(check(with_ref, with_ref, &refs_old, &refs_old).is_ok());
        assert!(check(with_ref, with_ref, &refs_new, &refs_old).is_err());
        // An unresolvable target stays permissive.
        assert!(check(with_ref, with_ref, &[], &[]).is_ok());
        // The reference does not change the identity.
        assert!(
            parse(with_ref, &refs_old).unwrap().canonical_form()
                == parse(with_ref, &[]).unwrap().canonical_form()
        );
    }

    #[test]
    fn branch_matching_needs_compatible_coverage() {
        let root = json!({});
        let ctx = DiffCtx::new(&root, &root, &[], &[]);
        let old = vec![json!({"type": "integer"}), json!({"type": "string"})];
        let permuted = vec![json!({"type": "string"}), json!({"type": "number"})];
        let missing = vec![json!({"type": "boolean"}), json!({"type": "number"})];
        assert!(maximum_matching(&old, &permuted, &ctx) == 2);
        assert!(maximum_matching(&old, &missing, &ctx) == 1);
    }
}
