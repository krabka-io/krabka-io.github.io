//! Protobuf: parsing through `protox-parse`, import linking through
//! `prost-reflect`, the normalised text Confluent stores, and a subset of
//! Confluent's structural compatibility rules.
//!
//! # The compatibility subset
//!
//! The checker diffs the writer's file descriptor (the original) against the
//! reader's (the update) and rejects the pair when a difference is
//! backward-incompatible, with Confluent's classification for each kind.
//! Fields and enum values are matched by number, as on the wire. It covers:
//!
//! - fields added or removed, and proto2 `required` fields added or removed;
//! - a field's type changed: between scalars of one wire group (compatible),
//!   across groups, to or from a message, or to another named type; the key
//!   and value types of a map;
//! - a field's label changed between singular and `repeated` where the label
//!   is explicit (proto2, `optional`, `repeated`);
//! - fields moved into a `oneof` with other existing fields, or out of one;
//!   `oneof` members added or removed; `oneof` declarations added or removed;
//! - messages, enums and enum values added or removed; reserved numbers and
//!   names added; the package changed;
//! - a changed import: the referenced files are diffed as well when both
//!   sides name the same import with different text.
//!
//! Not covered: options, extensions, and services (printed, not compared).
//! A field whose number changes is a removal and an addition.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use prost_reflect::{
    DescriptorPool,
    prost_types::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, OneofDescriptorProto, ServiceDescriptorProto,
        descriptor_proto::ReservedRange,
        field_descriptor_proto::{Label, Type as FieldType},
    },
};

use super::{ResolvedReference, SchemaType, incompatible_message};
use crate::lab::registry::error::RegistryError;

fn invalid(message: String) -> RegistryError {
    RegistryError::InvalidSchema(message)
}

/// A parsed `.proto` file.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtobufSchema {
    descriptor: FileDescriptorProto,
    normalized: String,
}

impl ProtobufSchema {
    /// The normalised `.proto` text, which is both the storage form and the
    /// identity.
    #[must_use]
    pub fn normalized_form(&self) -> &str {
        &self.normalized
    }

    /// The unlinked file descriptor `protox-parse` produced.
    #[must_use]
    pub fn descriptor(&self) -> &FileDescriptorProto {
        &self.descriptor
    }
}

/// Parse a `.proto` source. When it imports files or references are given,
/// the candidate and its Protobuf references are linked together with the
/// Google well-known types, so an unresolved import or type is an error.
///
/// # Errors
/// Returns [`RegistryError::InvalidSchema`] when the source, a reference, or
/// the link fails.
pub fn parse(schema: &str, refs: &[ResolvedReference]) -> Result<ProtobufSchema, RegistryError> {
    let descriptor = protox_parse::parse("schema.proto", schema)
        .map_err(|e| invalid(format!("Protobuf: {e}")))?;
    if !descriptor.dependency.is_empty() || !refs.is_empty() {
        let mut files = Vec::with_capacity(refs.len() + 1);
        for r in refs.iter().filter(|r| r.ty == SchemaType::Protobuf) {
            files.push(
                protox_parse::parse(&r.name, &r.schema)
                    .map_err(|e| invalid(format!("Protobuf reference {}: {e}", r.name)))?,
            );
        }
        files.push(descriptor.clone());
        let mut pool = DescriptorPool::global();
        pool.add_file_descriptor_protos(files)
            .map_err(|e| invalid(format!("Protobuf link: {e}")))?;
    }
    let normalized = normalize(&descriptor);
    Ok(ProtobufSchema {
        descriptor,
        normalized,
    })
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
    let reader_schema = parse(reader, reader_refs).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_schema = parse(writer, writer_refs).map_err(|e| vec![format!("writer: {e}")])?;
    let mut diffs = compare(writer_schema.descriptor(), reader_schema.descriptor());
    for writer_ref in writer_refs.iter().filter(|r| r.ty == SchemaType::Protobuf) {
        let Some(reader_ref) = reader_refs
            .iter()
            .find(|r| r.ty == SchemaType::Protobuf && r.name == writer_ref.name)
        else {
            continue;
        };
        if writer_ref.schema == reader_ref.schema {
            continue;
        }
        // A reference is parsed against the other references, never against
        // itself: the closure includes it, and a file linked twice fails.
        let others = |refs: &[ResolvedReference], name: &str| -> Vec<ResolvedReference> {
            refs.iter().filter(|r| r.name != name).cloned().collect()
        };
        let old = parse(&writer_ref.schema, &others(writer_refs, &writer_ref.name))
            .map_err(|e| vec![format!("writer reference: {e}")])?;
        let new = parse(&reader_ref.schema, &others(reader_refs, &reader_ref.name))
            .map_err(|e| vec![format!("reader reference: {e}")])?;
        diffs.extend(compare(old.descriptor(), new.descriptor()));
    }
    let messages: Vec<String> = diffs
        .iter()
        .filter(|diff| !diff.kind.is_backward_compatible())
        .map(|diff| incompatible_message(&diff.kind, "fullPath", &diff.path))
        .collect();
    if messages.is_empty() {
        Ok(())
    } else {
        Err(messages)
    }
}

// ---- normalised text --------------------------------------------------------------

/// The normalised `.proto` text, as Confluent prints a registered schema:
/// `syntax`, `package`, imports, enums, messages and services, two-space
/// indentation, no options or comments.
#[must_use]
pub fn normalize(file: &FileDescriptorProto) -> String {
    let mut out = String::new();
    let syntax = file.syntax.as_deref().unwrap_or("proto3");
    let _ = writeln!(out, "syntax = \"{syntax}\";");
    let package = file.package.as_deref().unwrap_or("");
    if !package.is_empty() {
        let _ = writeln!(out, "package {package};");
    }
    for dependency in &file.dependency {
        out.push('\n');
        let _ = writeln!(out, "import \"{dependency}\";");
    }
    for enumeration in &file.enum_type {
        out.push('\n');
        write_enum(&mut out, enumeration, 0);
    }
    for message in &file.message_type {
        out.push('\n');
        write_message(&mut out, message, 0, package, package, syntax);
    }
    for service in &file.service {
        out.push('\n');
        write_service(&mut out, service, package);
    }
    out
}

fn write_message(
    out: &mut String,
    message: &DescriptorProto,
    depth: usize,
    package: &str,
    parent_name: &str,
    syntax: &str,
) {
    let name = message.name();
    let full_name = if parent_name.is_empty() {
        name.to_string()
    } else {
        format!("{parent_name}.{name}")
    };
    let indent = "  ".repeat(depth);
    let _ = writeln!(out, "{indent}message {name} {{");
    write_reserved(out, message, depth + 1);
    for enumeration in &message.enum_type {
        write_enum(out, enumeration, depth + 1);
    }
    // Fields print in declaration order; a `oneof` prints where its first
    // member is declared, and a proto3 `optional` (a synthetic oneof) prints
    // as the plain field it is.
    let mut printed_oneofs = BTreeSet::new();
    for field in &message.field {
        let Some(index) = field.oneof_index.filter(|_| !field.proto3_optional()) else {
            write_field(out, field, message, &full_name, depth + 1, package, syntax);
            continue;
        };
        if !printed_oneofs.insert(index) {
            continue;
        }
        let Some(oneof) = usize::try_from(index)
            .ok()
            .and_then(|i| message.oneof_decl.get(i))
        else {
            continue;
        };
        let child_indent = "  ".repeat(depth + 1);
        let _ = writeln!(out, "{child_indent}oneof {} {{", oneof.name());
        for member in message
            .field
            .iter()
            .filter(|f| f.oneof_index == Some(index))
        {
            write_field(out, member, message, &full_name, depth + 2, package, syntax);
        }
        let _ = writeln!(out, "{child_indent}}}");
    }
    for nested in message.nested_type.iter().filter(|n| !is_map_entry(n)) {
        write_message(out, nested, depth + 1, package, &full_name, syntax);
    }
    let _ = writeln!(out, "{indent}}}");
}

fn is_map_entry(message: &DescriptorProto) -> bool {
    message
        .options
        .as_ref()
        .is_some_and(|o| o.map_entry.unwrap_or(false))
}

fn write_reserved(out: &mut String, message: &DescriptorProto, depth: usize) {
    let indent = "  ".repeat(depth);
    for range in &message.reserved_range {
        let start = range.start.unwrap_or_default();
        let end = range.end.unwrap_or(start + 1) - 1;
        if start == end {
            let _ = writeln!(out, "{indent}reserved {start};");
        } else {
            let _ = writeln!(out, "{indent}reserved {start} to {end};");
        }
    }
    if !message.reserved_name.is_empty() {
        let names: Vec<String> = message
            .reserved_name
            .iter()
            .map(|n| format!("\"{n}\""))
            .collect();
        let _ = writeln!(out, "{indent}reserved {};", names.join(", "));
    }
}

fn write_enum(out: &mut String, enumeration: &EnumDescriptorProto, depth: usize) {
    let indent = "  ".repeat(depth);
    let _ = writeln!(out, "{indent}enum {} {{", enumeration.name());
    for value in &enumeration.value {
        let _ = writeln!(out, "{indent}  {} = {};", value.name(), value.number());
    }
    let _ = writeln!(out, "{indent}}}");
}

fn write_field(
    out: &mut String,
    field: &FieldDescriptorProto,
    parent: &DescriptorProto,
    parent_name: &str,
    depth: usize,
    package: &str,
    syntax: &str,
) {
    let indent = "  ".repeat(depth);
    let entry = map_entry_of(parent, parent_name, field);
    let label = if field.oneof_index.is_some() && !field.proto3_optional() {
        ""
    } else if field.proto3_optional() {
        "optional "
    } else if entry.is_some() {
        ""
    } else {
        match (syntax, field.label()) {
            (_, Label::Repeated) => "repeated ",
            ("proto2", Label::Required) => "required ",
            ("proto2", Label::Optional) => "optional ",
            _ => "",
        }
    };
    let ty = entry.map_or_else(
        || type_name(field, package),
        |entry| {
            let part = |index: usize| {
                entry
                    .field
                    .get(index)
                    .map_or_else(|| "unknown".to_string(), |f| type_name(f, package))
            };
            format!("map<{}, {}>", part(0), part(1))
        },
    );
    let _ = writeln!(
        out,
        "{indent}{label}{ty} {} = {};",
        field.name(),
        field.number()
    );
}

/// The synthetic entry message of a map field declared in `parent`.
fn map_entry_of<'a>(
    parent: &'a DescriptorProto,
    parent_name: &str,
    field: &FieldDescriptorProto,
) -> Option<&'a DescriptorProto> {
    let field_type = field.type_name.as_deref()?.trim_start_matches('.');
    parent.nested_type.iter().find(|nested| {
        is_map_entry(nested)
            && (field_type == nested.name()
                || field_type == format!("{parent_name}.{}", nested.name()))
    })
}

fn type_name(field: &FieldDescriptorProto, package: &str) -> String {
    if let Some(name) = field.type_name.as_deref().filter(|n| !n.is_empty()) {
        return reference_name(name, package);
    }
    match field.r#type() {
        FieldType::Double => "double",
        FieldType::Float => "float",
        FieldType::Int64 => "int64",
        FieldType::Uint64 => "uint64",
        FieldType::Int32 => "int32",
        FieldType::Fixed64 => "fixed64",
        FieldType::Fixed32 => "fixed32",
        FieldType::Bool => "bool",
        FieldType::String => "string",
        FieldType::Bytes => "bytes",
        FieldType::Uint32 => "uint32",
        FieldType::Sfixed32 => "sfixed32",
        FieldType::Sfixed64 => "sfixed64",
        FieldType::Sint32 => "sint32",
        FieldType::Sint64 => "sint64",
        FieldType::Group | FieldType::Message | FieldType::Enum => "unknown",
    }
    .to_string()
}

/// A type reference relative to the file's package.
fn reference_name(name: &str, package: &str) -> String {
    if !package.is_empty()
        && let Some(local) = name.strip_prefix(&format!(".{package}."))
    {
        return local.to_string();
    }
    name.trim_start_matches('.').to_string()
}

fn write_service(out: &mut String, service: &ServiceDescriptorProto, package: &str) {
    let _ = writeln!(out, "service {} {{", service.name());
    for method in &service.method {
        let stream = |on: bool| if on { "stream " } else { "" };
        let _ = writeln!(
            out,
            "  rpc {} ({}{}) returns ({}{});",
            method.name(),
            stream(method.client_streaming()),
            reference_name(method.input_type(), package),
            stream(method.server_streaming()),
            reference_name(method.output_type(), package),
        );
    }
    out.push_str("}\n");
}

// ---- structural diff ----------------------------------------------------------------

/// One structural difference, named as Confluent names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    FieldAdded,
    FieldRemoved,
    /// A scalar or enum became another scalar or enum; compatible when both
    /// encode in the same wire group.
    FieldScalarKindChanged {
        compatible_group: bool,
    },
    /// A change to or from a message type.
    FieldKindChanged,
    FieldNamedTypeChanged,
    FieldNumericLabelChanged,
    FieldStringOrBytesLabelChanged,
    RequiredFieldAdded,
    RequiredFieldRemoved,
    MessageRemoved,
    MessageAdded,
    OneofFieldMovedIn,
    OneofFieldMovedOut,
    OneofFieldAdded,
    OneofFieldRemoved,
    OneofAdded,
    OneofRemoved,
    ReservedNumberAdded,
    ReservedNameAdded,
    EnumConstAdded,
    EnumConstRemoved,
    EnumAdded,
    EnumRemoved,
    PackageChanged,
}

impl Kind {
    /// Whether a reader with this difference from the writer still reads the
    /// writer's data: Confluent's classification. The diff runs with the
    /// writer as the original, so a mirrored rule is encoded by the two
    /// mirrored kinds: a message only the reader has is `MessageAdded`
    /// (compatible), a message only the writer has is `MessageRemoved`
    /// (incompatible).
    #[must_use]
    pub fn is_backward_compatible(self) -> bool {
        match self {
            Self::FieldAdded
            | Self::FieldRemoved
            | Self::OneofFieldAdded
            | Self::MessageAdded
            | Self::OneofFieldMovedOut
            | Self::OneofAdded
            | Self::OneofRemoved
            | Self::ReservedNumberAdded
            | Self::ReservedNameAdded
            | Self::EnumConstAdded
            | Self::EnumConstRemoved
            | Self::EnumAdded
            | Self::EnumRemoved
            | Self::PackageChanged
            | Self::FieldStringOrBytesLabelChanged => true,
            Self::FieldScalarKindChanged { compatible_group } => compatible_group,
            Self::FieldKindChanged
            | Self::FieldNamedTypeChanged
            | Self::FieldNumericLabelChanged
            | Self::RequiredFieldAdded
            | Self::RequiredFieldRemoved
            | Self::OneofFieldRemoved
            | Self::OneofFieldMovedIn
            | Self::MessageRemoved => false,
        }
    }
}

/// A difference at a path: `#/Outer/Inner/3` names field number 3 of the
/// nested message `Inner`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub kind: Kind,
    pub path: String,
}

fn d(kind: Kind, path: String) -> Difference {
    Difference { kind, path }
}

/// What a field encodes as on the wire. `protox-parse` leaves the `type` of
/// a message- or enum-typed field unset and only fills `type_name`, so a
/// named type is classified by the enum names the file declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldKind {
    Scalar(FieldType),
    Enum,
    Message,
}

/// The wire group of a single-value kind: two kinds in one group are
/// interchangeable on the wire. A message is length-delimited and in no group.
fn wire_group(kind: FieldKind) -> Option<u8> {
    match kind {
        FieldKind::Enum => Some(1),
        FieldKind::Message => None,
        FieldKind::Scalar(ty) => match ty {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Bool => Some(1),
            FieldType::Sint32 | FieldType::Sint64 => Some(2),
            FieldType::String | FieldType::Bytes => Some(3),
            FieldType::Fixed32 | FieldType::Sfixed32 => Some(4),
            FieldType::Fixed64 | FieldType::Sfixed64 => Some(5),
            FieldType::Float => Some(6),
            FieldType::Double => Some(7),
            FieldType::Group | FieldType::Message | FieldType::Enum => None,
        },
    }
}

/// The enum names each side declares, and whether each side is proto2.
struct Resolver<'a> {
    old_enums: BTreeSet<&'a str>,
    new_enums: BTreeSet<&'a str>,
    old_proto2: bool,
    new_proto2: bool,
}

impl Resolver<'_> {
    fn kind(enums: &BTreeSet<&str>, field: &FieldDescriptorProto) -> FieldKind {
        match field.type_name.as_deref() {
            Some(name) => {
                let leaf = name.rsplit('.').next().unwrap_or(name);
                if enums.contains(leaf) {
                    FieldKind::Enum
                } else {
                    FieldKind::Message
                }
            }
            None => FieldKind::Scalar(field.r#type()),
        }
    }

    fn old_kind(&self, field: &FieldDescriptorProto) -> FieldKind {
        Self::kind(&self.old_enums, field)
    }

    fn new_kind(&self, field: &FieldDescriptorProto) -> FieldKind {
        Self::kind(&self.new_enums, field)
    }
}

fn collect_enum_names(file: &FileDescriptorProto) -> BTreeSet<&str> {
    fn walk<'a>(message: &'a DescriptorProto, set: &mut BTreeSet<&'a str>) {
        set.extend(message.enum_type.iter().map(EnumDescriptorProto::name));
        for nested in &message.nested_type {
            walk(nested, set);
        }
    }
    let mut set: BTreeSet<&str> = file
        .enum_type
        .iter()
        .map(EnumDescriptorProto::name)
        .collect();
    for message in &file.message_type {
        walk(message, &mut set);
    }
    set
}

/// Diff two file descriptors: every difference of `update` from `original`.
#[must_use]
pub fn compare(original: &FileDescriptorProto, update: &FileDescriptorProto) -> Vec<Difference> {
    let mut out = Vec::new();
    let resolver = Resolver {
        old_enums: collect_enum_names(original),
        new_enums: collect_enum_names(update),
        old_proto2: original.syntax.as_deref() != Some("proto3"),
        new_proto2: update.syntax.as_deref() != Some("proto3"),
    };
    if original.package != update.package {
        out.push(d(Kind::PackageChanged, "#/package".to_string()));
    }
    compare_messages(
        "#",
        &original.message_type,
        &update.message_type,
        &resolver,
        &mut out,
    );
    compare_enums("#", &original.enum_type, &update.enum_type, &mut out);
    out
}

/// A path element under `prefix`, in Confluent's `#/Outer/Inner/3` shape.
fn join(prefix: &str, name: &str) -> String {
    format!("{prefix}/{name}")
}

fn compare_messages(
    prefix: &str,
    old: &[DescriptorProto],
    new: &[DescriptorProto],
    resolver: &Resolver<'_>,
    out: &mut Vec<Difference>,
) {
    let old_by: BTreeMap<&str, &DescriptorProto> = old.iter().map(|m| (m.name(), m)).collect();
    let new_by: BTreeMap<&str, &DescriptorProto> = new.iter().map(|m| (m.name(), m)).collect();
    for (name, old_message) in &old_by {
        let path = join(prefix, name);
        match new_by.get(name) {
            None => out.push(d(Kind::MessageRemoved, path)),
            Some(new_message) => compare_message(&path, old_message, new_message, resolver, out),
        }
    }
    for name in new_by.keys() {
        if !old_by.contains_key(name) {
            out.push(d(Kind::MessageAdded, join(prefix, name)));
        }
    }
}

fn compare_message(
    path: &str,
    old: &DescriptorProto,
    new: &DescriptorProto,
    resolver: &Resolver<'_>,
    out: &mut Vec<Difference>,
) {
    compare_fields(path, old, new, resolver, out);
    // proto3 `optional` fields sit in synthetic oneofs, named with a leading
    // underscore, which are not declarations a user wrote.
    let real = |oneofs: &[OneofDescriptorProto]| -> BTreeSet<String> {
        oneofs
            .iter()
            .map(OneofDescriptorProto::name)
            .filter(|n| !n.starts_with('_'))
            .map(str::to_string)
            .collect()
    };
    let (old_oneofs, new_oneofs) = (real(&old.oneof_decl), real(&new.oneof_decl));
    for name in old_oneofs.difference(&new_oneofs) {
        out.push(d(Kind::OneofRemoved, join(path, name)));
    }
    for name in new_oneofs.difference(&old_oneofs) {
        out.push(d(Kind::OneofAdded, join(path, name)));
    }
    compare_reserved(path, &old.reserved_range, &new.reserved_range, out);
    let old_names: BTreeSet<&str> = old.reserved_name.iter().map(String::as_str).collect();
    let new_names: BTreeSet<&str> = new.reserved_name.iter().map(String::as_str).collect();
    for name in new_names.difference(&old_names) {
        out.push(d(
            Kind::ReservedNameAdded,
            format!("{path}/reserved_name/{name}"),
        ));
    }
    // Map entry messages are compared through their map fields.
    let nested = |message: &DescriptorProto| -> Vec<DescriptorProto> {
        message
            .nested_type
            .iter()
            .filter(|n| !is_map_entry(n))
            .cloned()
            .collect()
    };
    compare_messages(path, &nested(old), &nested(new), resolver, out);
    compare_enums(path, &old.enum_type, &new.enum_type, out);
}

fn compare_fields(
    path: &str,
    old: &DescriptorProto,
    new: &DescriptorProto,
    resolver: &Resolver<'_>,
    out: &mut Vec<Difference>,
) {
    let old_fields: BTreeMap<i32, &FieldDescriptorProto> =
        old.field.iter().map(|f| (f.number(), f)).collect();
    let new_fields: BTreeMap<i32, &FieldDescriptorProto> =
        new.field.iter().map(|f| (f.number(), f)).collect();
    for (number, old_field) in &old_fields {
        let field_path = format!("{path}/{number}");
        match new_fields.get(number) {
            None => {
                let kind = if real_oneof(old_field)
                    && !oneof_has_surviving_plain_member(old, new, old_field)
                {
                    Kind::OneofFieldRemoved
                } else if old_field.label() == Label::Required {
                    Kind::RequiredFieldRemoved
                } else {
                    Kind::FieldRemoved
                };
                out.push(d(kind, field_path));
            }
            Some(new_field) => match (
                map_entry_of(old, "", old_field),
                map_entry_of(new, "", new_field),
            ) {
                (Some(old_entry), Some(new_entry)) => {
                    compare_map_entries(&field_path, old_entry, new_entry, resolver, out);
                }
                _ => compare_field(&field_path, old_field, new_field, (old, new), resolver, out),
            },
        }
    }
    for (number, new_field) in &new_fields {
        if old_fields.contains_key(number) {
            continue;
        }
        let kind = if real_oneof(new_field) {
            Kind::OneofFieldAdded
        } else if new_field.label() == Label::Required {
            Kind::RequiredFieldAdded
        } else {
            Kind::FieldAdded
        };
        out.push(d(kind, format!("{path}/{number}")));
    }
}

/// Whether a field is a member of a `oneof` a user declared; proto3
/// `optional` is a synthetic one.
fn real_oneof(field: &FieldDescriptorProto) -> bool {
    field.oneof_index.is_some() && !field.proto3_optional()
}

/// Whether another member of the removed field's `oneof` survives as a plain
/// field in the update, which makes the removal a move rather than a loss.
fn oneof_has_surviving_plain_member(
    old: &DescriptorProto,
    new: &DescriptorProto,
    field: &FieldDescriptorProto,
) -> bool {
    let Some(index) = field.oneof_index else {
        return false;
    };
    old.field.iter().any(|member| {
        member.oneof_index == Some(index)
            && new
                .field
                .iter()
                .any(|candidate| candidate.number == member.number && !real_oneof(candidate))
    })
}

/// How many fields of the update's `oneof` around `field` existed in the
/// original as plain fields.
fn moved_existing_member_count(
    old: &DescriptorProto,
    new: &DescriptorProto,
    field: &FieldDescriptorProto,
) -> usize {
    let Some(index) = field.oneof_index else {
        return 0;
    };
    new.field
        .iter()
        .filter(|f| f.oneof_index == Some(index) && !f.proto3_optional())
        .filter(|f| {
            old.field
                .iter()
                .any(|o| o.number == f.number && !real_oneof(o))
        })
        .count()
}

/// Whether the label is one the source spelled out, so a change of it means
/// something: proto2 labels, proto3 `optional`, and `repeated` anywhere.
fn explicit_label(field: &FieldDescriptorProto, proto2: bool) -> bool {
    proto2 || field.proto3_optional() || field.label() == Label::Repeated
}

fn compare_field(
    path: &str,
    old_field: &FieldDescriptorProto,
    new_field: &FieldDescriptorProto,
    messages: (&DescriptorProto, &DescriptorProto),
    resolver: &Resolver<'_>,
    out: &mut Vec<Difference>,
) {
    let (old_message, new_message) = messages;
    match (real_oneof(old_field), real_oneof(new_field)) {
        // A single field moved into its own oneof is wire-identical; grouping
        // two or more formerly independent fields is not.
        (false, true) if moved_existing_member_count(old_message, new_message, new_field) >= 2 => {
            out.push(d(Kind::OneofFieldMovedIn, path.to_string()));
        }
        (true, false) => out.push(d(Kind::OneofFieldMovedOut, path.to_string())),
        _ => {}
    }
    let old_kind = resolver.old_kind(old_field);
    let new_kind = resolver.new_kind(new_field);
    if old_field.label() != new_field.label()
        && explicit_label(old_field, resolver.old_proto2)
        && explicit_label(new_field, resolver.new_proto2)
        && old_kind == new_kind
    {
        let kind = if matches!(
            old_kind,
            FieldKind::Scalar(FieldType::String | FieldType::Bytes)
        ) {
            Kind::FieldStringOrBytesLabelChanged
        } else {
            Kind::FieldNumericLabelChanged
        };
        out.push(d(kind, path.to_string()));
    }
    compare_field_types(
        path,
        (old_kind, new_kind),
        (
            old_field.type_name.as_deref(),
            new_field.type_name.as_deref(),
        ),
        out,
    );
}

/// Classify a type change between two resolved field kinds.
fn compare_field_types(
    path: &str,
    kinds: (FieldKind, FieldKind),
    type_names: (Option<&str>, Option<&str>),
    out: &mut Vec<Difference>,
) {
    let (old_kind, new_kind) = kinds;
    if old_kind == new_kind {
        if matches!(old_kind, FieldKind::Message | FieldKind::Enum) && type_names.0 != type_names.1
        {
            out.push(d(Kind::FieldNamedTypeChanged, path.to_string()));
        }
        return;
    }
    let kind = match (wire_group(old_kind), wire_group(new_kind)) {
        (Some(old_group), Some(new_group)) => Kind::FieldScalarKindChanged {
            compatible_group: old_group == new_group,
        },
        _ => Kind::FieldKindChanged,
    };
    out.push(d(kind, path.to_string()));
}

/// Compare the key (`#1`) and value (`#2`) types of two map entries.
fn compare_map_entries(
    path: &str,
    old_entry: &DescriptorProto,
    new_entry: &DescriptorProto,
    resolver: &Resolver<'_>,
    out: &mut Vec<Difference>,
) {
    for (number, part) in [(1, "key"), (2, "value")] {
        let old_field = old_entry.field.iter().find(|f| f.number() == number);
        let new_field = new_entry.field.iter().find(|f| f.number() == number);
        if let (Some(old_field), Some(new_field)) = (old_field, new_field) {
            compare_field_types(
                &format!("{path}/{part}"),
                (resolver.old_kind(old_field), resolver.new_kind(new_field)),
                (
                    old_field.type_name.as_deref(),
                    new_field.type_name.as_deref(),
                ),
                out,
            );
        }
    }
}

fn compare_reserved(
    path: &str,
    old: &[ReservedRange],
    new: &[ReservedRange],
    out: &mut Vec<Difference>,
) {
    let numbers = |ranges: &[ReservedRange]| -> BTreeSet<i32> {
        ranges
            .iter()
            .flat_map(|r| r.start.unwrap_or(0)..r.end.unwrap_or(0))
            .collect()
    };
    let (old_numbers, new_numbers) = (numbers(old), numbers(new));
    for number in new_numbers.difference(&old_numbers) {
        out.push(d(
            Kind::ReservedNumberAdded,
            format!("{path}/reserved/{number}"),
        ));
    }
}

fn compare_enums(
    prefix: &str,
    old: &[EnumDescriptorProto],
    new: &[EnumDescriptorProto],
    out: &mut Vec<Difference>,
) {
    let old_by: BTreeMap<&str, &EnumDescriptorProto> = old.iter().map(|e| (e.name(), e)).collect();
    let new_by: BTreeMap<&str, &EnumDescriptorProto> = new.iter().map(|e| (e.name(), e)).collect();
    for (name, old_enum) in &old_by {
        let path = join(prefix, name);
        let Some(new_enum) = new_by.get(name) else {
            out.push(d(Kind::EnumRemoved, path));
            continue;
        };
        let old_values: BTreeSet<i32> = old_enum
            .value
            .iter()
            .map(EnumValueDescriptorProto::number)
            .collect();
        let new_values: BTreeSet<i32> = new_enum
            .value
            .iter()
            .map(EnumValueDescriptorProto::number)
            .collect();
        for number in old_values.difference(&new_values) {
            out.push(d(Kind::EnumConstRemoved, format!("{path}/{number}")));
        }
        for number in new_values.difference(&old_values) {
            out.push(d(Kind::EnumConstAdded, format!("{path}/{number}")));
        }
    }
    for name in new_by.keys() {
        if !old_by.contains_key(name) {
            out.push(d(Kind::EnumAdded, join(prefix, name)));
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn message(body: &str) -> String {
        format!("syntax = \"proto3\"; message U {{ {body} }}")
    }

    fn compatible(reader: &str, writer: &str) -> bool {
        check(reader, writer, &[], &[]).is_ok()
    }

    #[test]
    fn parse_normalises_and_rejects_bad_sources() {
        let a = parse("syntax = \"proto3\"; message User { int32 id = 1; }", &[]).unwrap();
        let b = parse(
            "syntax = \"proto3\";\n// hi\nmessage User {\n  int32 id = 1;\n}\n",
            &[],
        )
        .unwrap();
        assert!(a.normalized_form() == b.normalized_form());
        assert!(
            a.normalized_form() == "syntax = \"proto3\";\n\nmessage User {\n  int32 id = 1;\n}\n"
        );
        assert!(parse("this is not protobuf", &[]).is_err());
        assert!(parse("syntax = \"proto3\"; message U { Missing m = 1; }", &[]).is_ok());
        assert!(
            parse(
                "syntax = \"proto3\"; import \"nope.proto\"; message U { int32 a = 1; }",
                &[]
            )
            .is_err()
        );
        assert!(
            parse(
                "syntax = \"proto3\"; import \"google/protobuf/timestamp.proto\"; message U { google.protobuf.Timestamp at = 1; }",
                &[]
            )
            .is_ok()
        );
    }

    #[test]
    fn normalised_text_matches_confluent_layout() {
        let source = "syntax = \"proto3\"; package m; import \"money.proto\"; enum Kind { K0 = 0; K1 = 1; } message Outer { reserved 4; reserved \"old\"; enum Inner { I0 = 0; } message Nested { string id = 1; } optional int32 o = 1; repeated Nested n = 2; map<string, int32> counts = 3; oneof choice { int32 a = 5; string b = 6; } } service S { rpc Do (Outer) returns (stream Outer); }";
        let money = ResolvedReference {
            name: "money.proto".into(),
            ty: SchemaType::Protobuf,
            schema: "syntax = \"proto3\"; package m; message Money { int64 cents = 1; }".into(),
        };
        let parsed = parse(source, &[money]).unwrap();
        assert!(
            parsed.normalized_form()
                == "syntax = \"proto3\";\npackage m;\n\nimport \"money.proto\";\n\nenum Kind {\n  K0 = 0;\n  K1 = 1;\n}\n\nmessage Outer {\n  reserved 4;\n  reserved \"old\";\n  enum Inner {\n    I0 = 0;\n  }\n  optional int32 o = 1;\n  repeated Nested n = 2;\n  map<string, int32> counts = 3;\n  oneof choice {\n    int32 a = 5;\n    string b = 6;\n  }\n  message Nested {\n    string id = 1;\n  }\n}\n\nservice S {\n  rpc Do (Outer) returns (stream Outer);\n}\n"
        );
    }

    #[test]
    fn removing_a_field_is_compatible_but_reusing_its_number_is_not() {
        let v1 = message("int32 id = 1; string name = 2;");
        let v2 = message("int32 id = 1;");
        let v3 = message("int32 id = 1; int64 count = 2;");
        assert!(compatible(&v2, &v1));
        assert!(!compatible(&v3, &v1));
        let messages = check(&v3, &v1, &[], &[]).unwrap_err();
        assert!(
            messages
                == vec![
                    "Found incompatible change: Difference{fullPath='#/U/2', type=FIELD_SCALAR_KIND_CHANGED}"
                        .to_string()
                ]
        );
    }

    const PLAIN: &str = "syntax = \"proto3\"; message U { int32 a = 1; int32 b = 2; }";
    const ONEOF: &str = "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }";
    const SMALL: &str = "syntax = \"proto3\"; message U { int32 id = 1; }";
    const BIG: &str = "syntax = \"proto3\"; message U { int32 id = 1; } message V { int32 a = 1; }";

    #[test]
    fn field_and_oneof_rules_are_pinned() {
        for (name, reader, writer, expected) in [
            (
                "field added",
                message("int32 id = 1; int32 x = 2;"),
                message("int32 id = 1;"),
                true,
            ),
            (
                "same wire group",
                message("int64 id = 1;"),
                message("int32 id = 1;"),
                true,
            ),
            (
                "across wire groups",
                message("string id = 1;"),
                message("int32 id = 1;"),
                false,
            ),
            (
                "singular to repeated",
                message("repeated int32 id = 1;"),
                message("int32 id = 1;"),
                true,
            ),
            (
                "scalar to message",
                "syntax = \"proto3\"; message M {} message U { M id = 1; }".to_string(),
                SMALL.to_string(),
                false,
            ),
            (
                "move into oneof",
                ONEOF.to_string(),
                PLAIN.to_string(),
                false,
            ),
            (
                "move out of oneof",
                PLAIN.to_string(),
                ONEOF.to_string(),
                true,
            ),
            (
                "proto3 optional",
                "syntax = \"proto3\"; message U { optional int32 a = 1; }".to_string(),
                "syntax = \"proto3\"; message U { int32 a = 1; }".to_string(),
                true,
            ),
        ] {
            assert!(compatible(&reader, &writer) == expected, "{name}");
        }
    }

    #[test]
    fn type_enum_message_and_label_rules_are_pinned() {
        for (name, reader, writer, expected) in [
            (
                "reserve number",
                "syntax = \"proto3\"; message U { reserved 2; int32 id = 1; }".to_string(),
                SMALL.to_string(),
                true,
            ),
            (
                "map value across groups",
                "syntax = \"proto3\"; message U { map<string, string> m = 1; }".to_string(),
                "syntax = \"proto3\"; message U { map<string, int32> m = 1; }".to_string(),
                false,
            ),
            (
                "enum constant added",
                "syntax = \"proto3\"; enum E { A = 0; B = 1; } message U { E e = 1; }".to_string(),
                "syntax = \"proto3\"; enum E { A = 0; } message U { E e = 1; }".to_string(),
                true,
            ),
            (
                "nested field across groups",
                "syntax = \"proto3\"; message U { message N { string a = 1; } N n = 1; }"
                    .to_string(),
                "syntax = \"proto3\"; message U { message N { int32 a = 1; } N n = 1; }"
                    .to_string(),
                false,
            ),
            (
                "package renamed",
                "syntax = \"proto3\"; package b; message U { int32 id = 1; }".to_string(),
                "syntax = \"proto3\"; package a; message U { int32 id = 1; }".to_string(),
                true,
            ),
            (
                "int to enum",
                "syntax = \"proto3\"; enum E { A = 0; } message U { E id = 1; }".to_string(),
                SMALL.to_string(),
                true,
            ),
            (
                "reader message added",
                BIG.to_string(),
                SMALL.to_string(),
                true,
            ),
            (
                "reader message removed",
                SMALL.to_string(),
                BIG.to_string(),
                false,
            ),
            (
                "oneof field removed",
                "syntax = \"proto3\"; message U { oneof x { int32 a = 1; } }".to_string(),
                "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }"
                    .to_string(),
                false,
            ),
            (
                "existing plus new moved to oneof",
                "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }"
                    .to_string(),
                "syntax = \"proto3\"; message U { int32 a = 1; }".to_string(),
                true,
            ),
            (
                "proto2 required added",
                "syntax = \"proto2\"; message U { required int32 a = 1; }".to_string(),
                "syntax = \"proto2\"; message U {}".to_string(),
                false,
            ),
            (
                "proto2 required removed",
                "syntax = \"proto2\"; message U {}".to_string(),
                "syntax = \"proto2\"; message U { required int32 a = 1; }".to_string(),
                false,
            ),
            (
                "proto2 numeric label",
                "syntax = \"proto2\"; message U { repeated int32 a = 1; }".to_string(),
                "syntax = \"proto2\"; message U { optional int32 a = 1; }".to_string(),
                false,
            ),
            (
                "string label",
                "syntax = \"proto2\"; message U { repeated string a = 1; }".to_string(),
                "syntax = \"proto2\"; message U { optional string a = 1; }".to_string(),
                true,
            ),
        ] {
            assert!(compatible(&reader, &writer) == expected, "{name}");
        }
    }

    #[test]
    fn changed_imports_are_diffed_through_the_references() {
        let order =
            "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
        let money = |schema: &str| ResolvedReference {
            name: "money.proto".into(),
            ty: SchemaType::Protobuf,
            schema: schema.into(),
        };
        let old = money("syntax = \"proto3\"; package m; message Money { int64 cents = 1; }");
        let widened = money(
            "syntax = \"proto3\"; package m; message Money { int64 cents = 1; string currency = 2; }",
        );
        let broken = money("syntax = \"proto3\"; package m; message Money { string cents = 1; }");
        assert!(parse(order, &[]).is_err());
        assert!(
            check(
                order,
                order,
                std::slice::from_ref(&old),
                std::slice::from_ref(&old)
            )
            .is_ok()
        );
        assert!(check(order, order, &[widened], std::slice::from_ref(&old)).is_ok());
        assert!(check(order, order, &[broken], &[old]).is_err());
    }
}
