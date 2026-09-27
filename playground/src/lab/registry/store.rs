//! The registry state: subjects, versions, ids by canonical form, levels and
//! modes, rebuilt by a replay of `_schemas`.
//!
//! This is a pure data structure. The service decides an id and a version on
//! a clone, appends the record, and applies it to the live instance only when
//! the log reads the record back, so the live state is always the replay of
//! the log. Every map is a `BTreeMap`: subject and version lists reach the
//! wire in order.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    error::RegistryError,
    format::{self, ResolvedReference, SchemaType},
    ids::{SchemaId, SchemaVersion},
    record::{SchemaReference, SchemaValue},
};

/// The outcome of a registration: the global id and the subject's version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registered {
    pub id: SchemaId,
    pub version: SchemaVersion,
}

/// A schema as it is stored under its global id. References are part of the
/// identity, so they live with the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

/// One version of a subject, with its schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedSchema {
    pub id: SchemaId,
    pub version: SchemaVersion,
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

/// One row of `GET /schemas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedSchema {
    pub subject: String,
    pub version: SchemaVersion,
    pub id: SchemaId,
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

/// One version slot of a subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionEntry {
    pub version: SchemaVersion,
    pub id: SchemaId,
    pub deleted: bool,
}

/// The whole registry state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreState {
    subjects: BTreeMap<String, Vec<VersionEntry>>,
    by_id: BTreeMap<SchemaId, RegisteredSchema>,
    by_canonical: BTreeMap<String, SchemaId>,
    canonical_by_id: BTreeMap<SchemaId, String>,
    global_compat: Option<String>,
    default_compat: String,
    subject_compat: BTreeMap<String, String>,
    global_mode: Option<String>,
    default_mode: String,
    subject_mode: BTreeMap<String, String>,
    max_id: SchemaId,
    /// The next version of each subject: the high-water mark survives a
    /// permanent delete, so a version number is never reused.
    next_versions: BTreeMap<String, SchemaVersion>,
}

impl Default for StoreState {
    fn default() -> Self {
        Self::with_defaults("BACKWARD", "READWRITE")
    }
}

impl StoreState {
    /// An empty state with the level and mode that apply until a `CONFIG` or
    /// `MODE` record sets the global ones.
    #[must_use]
    pub fn with_defaults(compatibility: &str, mode: &str) -> Self {
        Self {
            subjects: BTreeMap::new(),
            by_id: BTreeMap::new(),
            by_canonical: BTreeMap::new(),
            canonical_by_id: BTreeMap::new(),
            global_compat: None,
            default_compat: compatibility.to_string(),
            subject_compat: BTreeMap::new(),
            global_mode: None,
            default_mode: mode.to_string(),
            subject_mode: BTreeMap::new(),
            max_id: SchemaId::default(),
            next_versions: BTreeMap::new(),
        }
    }

    // ---- registration ------------------------------------------------------------

    /// Decide the id and version of a registration and apply it here. The
    /// schema is validated; `NONE` compatibility still rejects a schema that
    /// does not parse. The id is global and keyed by canonical form plus
    /// references; the version is per subject. Registering a schema the
    /// subject already holds returns the existing pair.
    ///
    /// `schema` must be in storage form, see
    /// [`format::parse`].
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidSchema`] or
    /// [`RegistryError::ReferenceNotFound`].
    pub fn register(
        &mut self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
        references: &[SchemaReference],
    ) -> Result<Registered, RegistryError> {
        let resolved = self.resolve_closure(references)?;
        let canonical = format::parse(ty, schema, &resolved)?.canonical;
        let key = Self::dedup_key(&canonical, references);
        if let Some(existing) = self.find_under_subject_canonical(subject, &key, true) {
            return Ok(existing);
        }
        let id = if let Some(&id) = self.by_canonical.get(&key) {
            id
        } else {
            let id = self.max_id.next();
            self.max_id = id;
            self.by_canonical.insert(key.clone(), id);
            self.canonical_by_id.insert(id, key);
            self.by_id.insert(
                id,
                RegisteredSchema {
                    ty,
                    schema: schema.to_string(),
                    references: references.to_vec(),
                },
            );
            id
        };
        let version = self.next_version(subject);
        self.observe_version(subject, version);
        self.subjects
            .entry(subject.to_string())
            .or_default()
            .push(VersionEntry {
                version,
                id,
                deleted: false,
            });
        Ok(Registered { id, version })
    }

    /// The id-dedup key: the canonical form joined with a stable fingerprint
    /// of the references, so identical text with different references gets a
    /// distinct id.
    fn dedup_key(canonical: &str, references: &[SchemaReference]) -> String {
        if references.is_empty() {
            return canonical.to_string();
        }
        let mut refs: Vec<String> = references
            .iter()
            .map(|r| format!("{}\u{1}{}\u{1}{}", r.name, r.subject, r.version))
            .collect();
        refs.sort();
        format!("{canonical}\u{0}{}", refs.join("\u{2}"))
    }

    /// Resolve a reference list into its transitive closure, dependencies
    /// before the schemas that use them, one entry per name, cycles guarded by
    /// `(subject, version)`.
    ///
    /// # Errors
    /// Returns [`RegistryError::ReferenceNotFound`] when a referenced
    /// `(subject, version)` does not exist.
    pub fn resolve_closure(
        &self,
        references: &[SchemaReference],
    ) -> Result<Vec<ResolvedReference>, RegistryError> {
        let mut out = Vec::new();
        let mut seen_names = BTreeSet::new();
        let mut visited = BTreeSet::new();
        self.resolve_into(references, &mut out, &mut seen_names, &mut visited)?;
        Ok(out)
    }

    fn resolve_into(
        &self,
        references: &[SchemaReference],
        out: &mut Vec<ResolvedReference>,
        seen_names: &mut BTreeSet<String>,
        visited: &mut BTreeSet<(String, SchemaVersion)>,
    ) -> Result<(), RegistryError> {
        for r in references {
            if !visited.insert((r.subject.clone(), r.version)) {
                continue;
            }
            let registered = self
                .id_of(&r.subject, r.version)
                .and_then(|id| self.by_id.get(&id))
                .ok_or_else(|| RegistryError::ReferenceNotFound {
                    subject: r.subject.clone(),
                    version: r.version,
                })?;
            self.resolve_into(&registered.references, out, seen_names, visited)?;
            if seen_names.insert(r.name.clone()) {
                out.push(ResolvedReference {
                    name: r.name.clone(),
                    ty: registered.ty,
                    schema: registered.schema.clone(),
                });
            }
        }
        Ok(())
    }

    /// The id of a concrete `(subject, version)`, deleted versions included:
    /// a reference can name a soft-deleted version.
    fn id_of(&self, subject: &str, version: SchemaVersion) -> Option<SchemaId> {
        self.subjects
            .get(subject)?
            .iter()
            .find(|v| v.version == version)
            .map(|v| v.id)
    }

    /// The ids of the schemas whose references include `(subject, version)`,
    /// in ascending order.
    #[must_use]
    pub fn referenced_by(
        &self,
        subject: &str,
        version: SchemaVersion,
        include_deleted: bool,
    ) -> Vec<SchemaId> {
        let mut ids = BTreeSet::new();
        for entry in self.subjects.values().flatten() {
            if entry.deleted && !include_deleted {
                continue;
            }
            if self.by_id.get(&entry.id).is_some_and(|reg| {
                reg.references
                    .iter()
                    .any(|r| r.subject == subject && r.version == version)
            }) {
                ids.insert(entry.id);
            }
        }
        ids.into_iter().collect()
    }

    // ---- replay -----------------------------------------------------------------------

    /// Fold a `SCHEMA` record into the state. Idempotent: the version's
    /// `deleted` flag becomes the record's, so one path inserts, soft-deletes
    /// and restores.
    pub fn apply_schema(&mut self, value: &SchemaValue) {
        let ty = value.schema_type();
        self.max_id = self.max_id.max(value.id);
        self.observe_version(&value.subject, value.version);
        if let Some(old_key) = self.canonical_by_id.remove(&value.id)
            && self.by_canonical.get(&old_key) == Some(&value.id)
        {
            self.by_canonical.remove(&old_key);
        }
        self.by_id.insert(
            value.id,
            RegisteredSchema {
                ty,
                schema: value.schema.clone(),
                references: value.references.clone(),
            },
        );
        if let Ok(resolved) = self.resolve_closure(&value.references)
            && let Ok(parsed) = format::parse(ty, &value.schema, &resolved)
        {
            let key = Self::dedup_key(&parsed.canonical, &value.references);
            if let Some(old_id) = self.by_canonical.insert(key.clone(), value.id)
                && old_id != value.id
                && self.canonical_by_id.get(&old_id) == Some(&key)
            {
                self.canonical_by_id.remove(&old_id);
            }
            self.canonical_by_id.insert(value.id, key);
        }
        let entries = self.subjects.entry(value.subject.clone()).or_default();
        if let Some(entry) = entries.iter_mut().find(|v| v.version == value.version) {
            entry.id = value.id;
            entry.deleted = value.deleted;
        } else {
            entries.push(VersionEntry {
                version: value.version,
                id: value.id,
                deleted: value.deleted,
            });
            entries.sort_by_key(|v| v.version);
        }
    }

    fn observe_version(&mut self, subject: &str, version: SchemaVersion) {
        self.observe_next_version(subject, version.next());
    }

    /// Raise a subject's next version to at least `next`.
    pub fn observe_next_version(&mut self, subject: &str, next: SchemaVersion) {
        self.next_versions
            .entry(subject.to_string())
            .and_modify(|current| *current = (*current).max(next))
            .or_insert(next);
    }

    /// The version the subject's next registration gets.
    #[must_use]
    pub fn next_version(&self, subject: &str) -> SchemaVersion {
        self.next_versions
            .get(subject)
            .copied()
            .unwrap_or(SchemaVersion::FIRST)
    }

    /// Whether `id` is bound to a schema other than this one.
    ///
    /// # Errors
    /// Returns an error when the stored or candidate references do not
    /// resolve or the candidate does not parse.
    pub fn schema_id_conflicts(
        &self,
        id: SchemaId,
        ty: SchemaType,
        schema: &str,
        references: &[SchemaReference],
    ) -> Result<bool, RegistryError> {
        let Some(existing) = self.by_id.get(&id) else {
            return Ok(false);
        };
        if existing.ty != ty {
            return Ok(true);
        }
        let existing_key = Self::dedup_key(
            &format::parse(
                existing.ty,
                &existing.schema,
                &self.resolve_closure(&existing.references)?,
            )?
            .canonical,
            &existing.references,
        );
        let candidate_key = Self::dedup_key(
            &format::parse(ty, schema, &self.resolve_closure(references)?)?.canonical,
            references,
        );
        Ok(existing_key != candidate_key)
    }

    fn find_under_subject_canonical(
        &self,
        subject: &str,
        key: &str,
        include_deleted: bool,
    ) -> Option<Registered> {
        let id = *self.by_canonical.get(key)?;
        let entry = self
            .subjects
            .get(subject)?
            .iter()
            .find(|v| v.id == id && (include_deleted || !v.deleted))?;
        Some(Registered {
            id,
            version: entry.version,
        })
    }

    /// The registration of `schema` under `subject`, if it is there:
    /// `POST /subjects/{subject}`.
    #[must_use]
    pub fn find_under_subject(
        &self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
        references: &[SchemaReference],
        include_deleted: bool,
    ) -> Option<Registered> {
        let resolved = self.resolve_closure(references).ok()?;
        let canonical = format::parse(ty, schema, &resolved).ok()?.canonical;
        self.find_under_subject_canonical(
            subject,
            &Self::dedup_key(&canonical, references),
            include_deleted,
        )
    }

    // ---- levels and modes ------------------------------------------------------------

    pub fn set_global_compat(&mut self, level: &str) {
        self.global_compat = Some(level.to_string());
    }

    pub fn clear_global_compat(&mut self) {
        self.global_compat = None;
    }

    pub fn set_subject_compat(&mut self, subject: &str, level: &str) {
        self.subject_compat
            .insert(subject.to_string(), level.to_string());
    }

    pub fn clear_subject_compat(&mut self, subject: &str) {
        self.subject_compat.remove(subject);
    }

    /// The global level: the replayed one, else the configured default.
    #[must_use]
    pub fn global_compat(&self) -> &str {
        self.global_compat
            .as_deref()
            .unwrap_or(&self.default_compat)
    }

    /// A subject's own level, if one is set.
    #[must_use]
    pub fn subject_compat(&self, subject: &str) -> Option<&str> {
        self.subject_compat.get(subject).map(String::as_str)
    }

    pub fn set_global_mode(&mut self, mode: &str) {
        self.global_mode = Some(mode.to_string());
    }

    pub fn clear_global_mode(&mut self) {
        self.global_mode = None;
    }

    pub fn set_subject_mode(&mut self, subject: &str, mode: &str) {
        self.subject_mode
            .insert(subject.to_string(), mode.to_string());
    }

    pub fn clear_subject_mode(&mut self, subject: &str) {
        self.subject_mode.remove(subject);
    }

    /// The global mode: the replayed one, else the configured default.
    #[must_use]
    pub fn global_mode(&self) -> &str {
        self.global_mode.as_deref().unwrap_or(&self.default_mode)
    }

    /// A subject's own mode, if one is set.
    #[must_use]
    pub fn subject_mode(&self, subject: &str) -> Option<&str> {
        self.subject_mode.get(subject).map(String::as_str)
    }

    /// The mode that applies to a subject: its own, else the global one.
    #[must_use]
    pub fn effective_mode(&self, subject: &str) -> &str {
        self.subject_mode(subject)
            .unwrap_or_else(|| self.global_mode())
    }

    // ---- queries -------------------------------------------------------------------------

    /// The subjects with a live version, or with any version when
    /// `include_deleted`, in order.
    #[must_use]
    pub fn subjects(&self, include_deleted: bool) -> Vec<String> {
        self.subjects
            .iter()
            .filter(|(_, vs)| vs.iter().any(|v| include_deleted || !v.deleted))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// The subjects whose every version is soft-deleted.
    #[must_use]
    pub fn deleted_only_subjects(&self) -> Vec<String> {
        self.subjects
            .iter()
            .filter(|(_, vs)| !vs.is_empty() && vs.iter().all(|v| v.deleted))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Every subject with its version slots, in order; the inspector's view.
    pub fn subject_entries(&self) -> impl Iterator<Item = (&str, &[VersionEntry])> {
        self.subjects
            .iter()
            .map(|(name, vs)| (name.as_str(), vs.as_slice()))
    }

    /// How many distinct schemas some version, deleted or not, still names.
    #[must_use]
    pub fn schema_count(&self) -> usize {
        let ids: BTreeSet<SchemaId> = self.subjects.values().flatten().map(|v| v.id).collect();
        ids.len()
    }

    /// The live version numbers of a subject, or every number with
    /// `include_deleted`; `None` when nothing qualifies, which maps to 404.
    #[must_use]
    pub fn versions(&self, subject: &str, include_deleted: bool) -> Option<Vec<SchemaVersion>> {
        let out: Vec<SchemaVersion> = self
            .subjects
            .get(subject)?
            .iter()
            .filter(|v| include_deleted || !v.deleted)
            .map(|v| v.version)
            .collect();
        (!out.is_empty()).then_some(out)
    }

    /// The soft-deleted version numbers of a subject.
    #[must_use]
    pub fn deleted_versions(&self, subject: &str) -> Vec<SchemaVersion> {
        self.subjects
            .get(subject)
            .map(|vs| vs.iter().filter(|v| v.deleted).map(|v| v.version).collect())
            .unwrap_or_default()
    }

    /// A subject's version, or its latest qualifying one for `None`.
    #[must_use]
    pub fn version(
        &self,
        subject: &str,
        version: Option<SchemaVersion>,
        include_deleted: bool,
    ) -> Option<VersionedSchema> {
        let entries = self.subjects.get(subject)?;
        let entry = match version {
            Some(v) => entries
                .iter()
                .find(|e| e.version == v && (include_deleted || !e.deleted))?,
            None => entries.iter().rfind(|e| include_deleted || !e.deleted)?,
        };
        let reg = self.by_id.get(&entry.id)?;
        Some(VersionedSchema {
            id: entry.id,
            version: entry.version,
            ty: reg.ty,
            schema: reg.schema.clone(),
            references: reg.references.clone(),
        })
    }

    /// The schema of a global id, when some qualifying version names it: a
    /// permanently deleted id is gone, and a soft-deleted-only id is hidden
    /// without `include_deleted`.
    #[must_use]
    pub fn schema_by_id(&self, id: SchemaId, include_deleted: bool) -> Option<&RegisteredSchema> {
        let referenced = self
            .subjects
            .values()
            .flatten()
            .any(|v| v.id == id && (include_deleted || !v.deleted));
        if referenced {
            self.by_id.get(&id)
        } else {
            None
        }
    }

    /// The `(subject, version)` pairs that name a global id, in order.
    #[must_use]
    pub fn schema_id_subject_versions(
        &self,
        id: SchemaId,
        include_deleted: bool,
    ) -> Vec<(String, SchemaVersion)> {
        self.subjects
            .iter()
            .flat_map(|(subject, vs)| {
                vs.iter()
                    .filter(move |v| v.id == id && (include_deleted || !v.deleted))
                    .map(move |v| (subject.clone(), v.version))
            })
            .collect()
    }

    /// Every schema `GET /schemas` lists, by subject then version.
    #[must_use]
    pub fn all_schemas(&self, include_deleted: bool) -> Vec<ListedSchema> {
        self.subjects
            .iter()
            .flat_map(|(subject, vs)| {
                vs.iter()
                    .filter(move |v| include_deleted || !v.deleted)
                    .filter_map(move |v| {
                        self.by_id.get(&v.id).map(|reg| ListedSchema {
                            subject: subject.clone(),
                            version: v.version,
                            id: v.id,
                            ty: reg.ty,
                            schema: reg.schema.clone(),
                            references: reg.references.clone(),
                        })
                    })
            })
            .collect()
    }

    /// A subject's live versions as `(type, schema, references)`, ascending:
    /// the versions a compatibility check runs against.
    #[must_use]
    pub fn versions_schemas(
        &self,
        subject: &str,
    ) -> Vec<(SchemaType, String, Vec<SchemaReference>)> {
        self.subjects
            .get(subject)
            .into_iter()
            .flatten()
            .filter(|e| !e.deleted)
            .filter_map(|e| {
                self.by_id
                    .get(&e.id)
                    .map(|reg| (reg.ty, reg.schema.clone(), reg.references.clone()))
            })
            .collect()
    }

    // ---- deletes ----------------------------------------------------------------------

    /// Flag every version of a subject deleted; returns the version numbers,
    /// or `None` for an unknown subject.
    pub fn soft_delete_subject(&mut self, subject: &str) -> Option<Vec<SchemaVersion>> {
        let entries = self.subjects.get_mut(subject)?;
        if entries.is_empty() {
            return None;
        }
        let versions = entries.iter().map(|v| v.version).collect();
        for entry in entries.iter_mut() {
            entry.deleted = true;
        }
        Some(versions)
    }

    /// Remove one version for good, and the subject when it was the last.
    /// Returns `None` when nothing was removed, so a replay is idempotent. The
    /// version high-water mark is not touched: the record that precedes a
    /// tombstone in the log carries it.
    pub fn permanent_delete_version(
        &mut self,
        subject: &str,
        version: SchemaVersion,
    ) -> Option<SchemaVersion> {
        let entries = self.subjects.get_mut(subject)?;
        let before = entries.len();
        entries.retain(|v| v.version != version);
        if entries.len() == before {
            return None;
        }
        if entries.is_empty() {
            self.subjects.remove(subject);
        }
        Some(version)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn av(name: &str) -> String {
        format!("{{\"type\":\"record\",\"name\":\"{name}\",\"fields\":[]}}")
    }

    fn sref(name: &str, subject: &str, version: i32) -> SchemaReference {
        SchemaReference {
            name: name.into(),
            subject: subject.into(),
            version: SchemaVersion(version),
        }
    }

    fn apply(
        state: &mut StoreState,
        subject: &str,
        version: i32,
        id: i32,
        schema: &str,
        deleted: bool,
    ) {
        state.apply_schema(&SchemaValue {
            subject: subject.into(),
            version: SchemaVersion(version),
            id: SchemaId(id),
            schema_type: None,
            references: vec![],
            schema: schema.into(),
            deleted,
        });
    }

    fn register(state: &mut StoreState, subject: &str, schema: &str) -> Registered {
        state
            .register(subject, SchemaType::Avro, schema, &[])
            .unwrap()
    }

    #[test]
    fn ids_are_global_and_versions_are_per_subject() {
        let mut s = StoreState::default();
        let first = register(&mut s, "av-value", &av("A"));
        assert!(
            first
                == Registered {
                    id: SchemaId(1),
                    version: SchemaVersion(1)
                }
        );
        assert!(register(&mut s, "av-value", &av("A")) == first);
        assert!(s.versions("av-value", false) == Some(vec![SchemaVersion(1)]));
        let other = register(&mut s, "other-value", &av("A"));
        assert!(
            other
                == Registered {
                    id: SchemaId(1),
                    version: SchemaVersion(1)
                }
        );
        let second = register(&mut s, "av-value", &av("B"));
        assert!(
            second
                == Registered {
                    id: SchemaId(2),
                    version: SchemaVersion(2)
                }
        );
        assert!(s.versions("av-value", false) == Some(vec![SchemaVersion(1), SchemaVersion(2)]));
        assert!(s.schema_count() == 2);
        assert!(
            s.register("av-value", SchemaType::Avro, "{not avro}", &[])
                .is_err()
        );
    }

    #[test]
    fn formatting_dedups_but_annotations_and_references_do_not() {
        let mut s = StoreState::default();
        register(&mut s, "base", &av("Base"));
        let plain = r#"{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}"#;
        let defaulted =
            r#"{"type":"record","name":"A","fields":[{"name":"id","type":"int","default":0}]}"#;
        let documented =
            r#"{"type":"record","name":"A","doc":"kept","fields":[{"name":"id","type":"int"}]}"#;
        let reordered =
            r#"{ "fields": [{"type":"int", "name":"id"}], "name":"A", "type":"record" }"#;
        let registrations =
            [plain, defaulted, documented, reordered].map(|schema| register(&mut s, "av", schema));
        assert!(registrations[0].id != registrations[1].id);
        assert!(registrations[1].id != registrations[2].id);
        assert!(registrations[0] == registrations[3]);
        assert!(s.versions("av", false).unwrap().len() == 3);
        let with_ref = s
            .register("b", SchemaType::Avro, plain, &[sref("base", "base", 1)])
            .unwrap();
        assert!(with_ref.id != registrations[0].id);
        let again = s
            .register("b", SchemaType::Avro, plain, &[sref("base", "base", 1)])
            .unwrap();
        assert!(again == with_ref);
    }

    #[test]
    fn closures_resolve_transitively_and_referrers_are_listed() {
        let mut s = StoreState::default();
        register(&mut s, "base", &av("Base"));
        s.register(
            "mid",
            SchemaType::Avro,
            &av("Mid"),
            &[sref("base", "base", 1)],
        )
        .unwrap();
        let dep = s
            .register(
                "dep",
                SchemaType::Avro,
                &av("Dep"),
                &[sref("mid", "mid", 1)],
            )
            .unwrap();
        let closure = s.resolve_closure(&[sref("mid", "mid", 1)]).unwrap();
        let names: Vec<&str> = closure.iter().map(|r| r.name.as_str()).collect();
        assert!(names == vec!["base", "mid"]);
        assert!(
            s.resolve_closure(&[sref("x", "nope", 1)])
                == Err(RegistryError::ReferenceNotFound {
                    subject: "nope".into(),
                    version: SchemaVersion(1)
                })
        );
        assert!(s.referenced_by("mid", SchemaVersion(1), false) == vec![dep.id]);
        assert!(s.referenced_by("base", SchemaVersion(99), false).is_empty());
    }

    #[test]
    fn replay_is_idempotent_and_rebinds_ids() {
        let mut s = StoreState::default();
        apply(&mut s, "av", 1, 1, &av("A"), false);
        apply(&mut s, "av", 1, 1, &av("A"), false);
        assert!(s.versions("av", false) == Some(vec![SchemaVersion(1)]));
        assert!(s.schema_by_id(SchemaId(1), false).unwrap().schema == av("A"));
        assert!(
            register(&mut s, "av", &av("A"))
                == Registered {
                    id: SchemaId(1),
                    version: SchemaVersion(1)
                }
        );
        apply(&mut s, "av", 1, 2, &av("B"), false);
        assert!(s.version("av", Some(SchemaVersion(1)), false).unwrap().id == SchemaId(2));
        assert!(s.schema_by_id(SchemaId(1), true).is_none());
        assert!(
            s.schema_id_conflicts(SchemaId(1), SchemaType::Avro, &av("C"), &[])
                .unwrap()
        );
        let spaced = r#"{ "type": "record", "name": "B", "fields": [] }"#;
        assert!(
            !s.schema_id_conflicts(SchemaId(2), SchemaType::Avro, spaced, &[])
                .unwrap()
        );
        assert!(
            !s.schema_id_conflicts(SchemaId(9), SchemaType::Avro, spaced, &[])
                .unwrap()
        );
    }

    #[test]
    fn soft_delete_hides_deleted_shows_and_permanent_removes() {
        let mut s = StoreState::default();
        register(&mut s, "av", &av("A"));
        register(&mut s, "av", &av("B"));
        apply(&mut s, "av", 1, 1, &av("A"), true);
        assert!(s.versions("av", false) == Some(vec![SchemaVersion(2)]));
        assert!(s.versions("av", true) == Some(vec![SchemaVersion(1), SchemaVersion(2)]));
        assert!(s.deleted_versions("av") == vec![SchemaVersion(1)]);
        assert!(s.version("av", Some(SchemaVersion(1)), false).is_none());
        assert!(s.version("av", Some(SchemaVersion(1)), true).is_some());
        assert!(s.schema_by_id(SchemaId(1), false).is_none());
        assert!(s.schema_by_id(SchemaId(1), true).is_some());
        assert!(s.permanent_delete_version("av", SchemaVersion(1)) == Some(SchemaVersion(1)));
        assert!(s.version("av", Some(SchemaVersion(1)), true).is_none());
        assert!(s.schema_by_id(SchemaId(1), true).is_none());
        assert!(
            s.permanent_delete_version("av", SchemaVersion(99))
                .is_none()
        );
        assert!(
            s.permanent_delete_version("nope", SchemaVersion(1))
                .is_none()
        );
        assert!(s.permanent_delete_version("av", SchemaVersion(2)) == Some(SchemaVersion(2)));
        assert!(s.subjects(true).is_empty());
        // The high-water mark survives: the next registration is version 3.
        assert!(register(&mut s, "av", &av("C")).version == SchemaVersion(3));
        let mut compacted = StoreState::default();
        compacted.observe_next_version("av", SchemaVersion(4));
        assert!(register(&mut compacted, "av", &av("D")).version == SchemaVersion(4));
    }

    #[test]
    fn latest_skips_deleted_versions_and_subject_delete_flags_all() {
        let mut s = StoreState::default();
        register(&mut s, "av", &av("A"));
        register(&mut s, "av", &av("B"));
        apply(&mut s, "av", 2, 2, &av("B"), true);
        assert!(s.version("av", None, false).unwrap().version == SchemaVersion(1));
        assert!(s.version("av", None, true).unwrap().version == SchemaVersion(2));
        assert!(s.soft_delete_subject("av") == Some(vec![SchemaVersion(1), SchemaVersion(2)]));
        assert!(s.soft_delete_subject("nope").is_none());
        assert!(s.versions("av", false).is_none());
        assert!(s.subjects(false).is_empty());
        assert!(s.subjects(true) == vec!["av".to_string()]);
        assert!(s.deleted_only_subjects() == vec!["av".to_string()]);
        apply(&mut s, "av", 1, 1, &av("A"), false);
        assert!(s.versions("av", false) == Some(vec![SchemaVersion(1)]));
        assert!(s.deleted_only_subjects().is_empty());
    }

    #[test]
    fn listings_cover_ids_subjects_and_schemas() {
        let mut s = StoreState::default();
        register(&mut s, "a", &av("A"));
        register(&mut s, "b", &av("A"));
        register(&mut s, "b", &av("B"));
        assert!(
            s.schema_id_subject_versions(SchemaId(1), false)
                == vec![
                    ("a".to_string(), SchemaVersion(1)),
                    ("b".to_string(), SchemaVersion(1))
                ]
        );
        let listed = s.all_schemas(false);
        assert!(listed.len() == 3);
        assert!(
            listed[2]
                == ListedSchema {
                    subject: "b".into(),
                    version: SchemaVersion(2),
                    id: SchemaId(2),
                    ty: SchemaType::Avro,
                    schema: av("B"),
                    references: vec![],
                }
        );
        assert!(
            s.versions_schemas("b")
                == vec![
                    (SchemaType::Avro, av("A"), vec![]),
                    (SchemaType::Avro, av("B"), vec![])
                ]
        );
        assert!(s.versions_schemas("missing").is_empty());
        let entries: Vec<(&str, usize)> =
            s.subject_entries().map(|(n, vs)| (n, vs.len())).collect();
        assert!(entries == vec![("a", 1), ("b", 2)]);
        assert!(
            s.find_under_subject("b", SchemaType::Avro, &av("B"), &[], false)
                .unwrap()
                .version
                == SchemaVersion(2)
        );
        assert!(
            s.find_under_subject("a", SchemaType::Avro, &av("B"), &[], false)
                .is_none()
        );
    }

    #[test]
    fn levels_and_modes_layer_defaults_globals_and_subjects() {
        let mut s = StoreState::with_defaults("FULL", "READONLY");
        assert!((s.global_compat(), s.global_mode()) == ("FULL", "READONLY"));
        s.set_global_compat("FORWARD");
        s.set_global_mode("IMPORT");
        assert!((s.global_compat(), s.global_mode()) == ("FORWARD", "IMPORT"));
        s.clear_global_compat();
        s.clear_global_mode();
        assert!((s.global_compat(), s.global_mode()) == ("FULL", "READONLY"));
        assert!(s.subject_compat("x").is_none());
        s.set_subject_compat("x", "NONE");
        assert!(s.subject_compat("x") == Some("NONE"));
        s.clear_subject_compat("x");
        assert!(s.subject_compat("x").is_none());
        assert!(s.effective_mode("x") == "READONLY");
        s.set_subject_mode("x", "READWRITE");
        assert!(s.effective_mode("x") == "READWRITE");
        assert!(s.subject_mode("x") == Some("READWRITE"));
        s.clear_subject_mode("x");
        assert!(s.effective_mode("x") == "READONLY");
        let d = StoreState::default();
        assert!((d.global_compat(), d.global_mode()) == ("BACKWARD", "READWRITE"));
    }
}
