//! The registry service: the real registry's decisions over the replay of
//! `_schemas`.
//!
//! A mutation decides its outcome on the current state and returns the
//! records that carry it; it changes nothing. The state changes only when
//! the store's reader hands a record back to [`RegistryService::apply`], so
//! it is always the replay of the topic, and a mutation's outcome is visible
//! to the next request only once its records have been read back, as in
//! Confluent's `KafkaStore`.

use super::{
    compat::{self, Candidate},
    error::RegistryError,
    format::{self, SchemaType},
    ids::{LogOffset, SchemaId, SchemaVersion},
    record::{self, RawRecord, SchemaRecord, SchemaReference, SchemaValue},
    store::{Registered, StoreState},
};

/// The modes a registry or a subject can be in.
pub const MODES: [&str; 3] = ["READWRITE", "READONLY", "IMPORT"];

/// A registration request, as `POST /subjects/{subject}/versions` carries it.
#[derive(Debug, Clone, Copy)]
pub struct RegisterRequest<'a> {
    pub subject: &'a str,
    pub ty: SchemaType,
    /// The schema text, already normalised when the client asked for it.
    pub schema: &'a str,
    pub references: &'a [SchemaReference],
    /// The id the client supplies; only `IMPORT` mode reads it.
    pub import_id: Option<SchemaId>,
    /// The version the client supplies; only `IMPORT` mode reads it.
    pub import_version: Option<SchemaVersion>,
}

/// The outcome of a mutation: its value and the records that carry it, in
/// the order they must be written. No records means the mutation writes
/// nothing (an idempotent registration, a clear of something already clear),
/// so the response goes out at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written<T> {
    /// What the mutation answers once its records are read back.
    pub value: T,
    /// The records to write, in order.
    pub records: Vec<RawRecord>,
}

impl<T> Written<T> {
    fn nothing(value: T) -> Self {
        Self {
            value,
            records: Vec::new(),
        }
    }

    fn one(value: T, record: RawRecord) -> Self {
        Self {
            value,
            records: vec![record],
        }
    }
}

/// The registry state and its decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryService {
    state: StoreState,
    /// The offset the reader applies next.
    applied: LogOffset,
    records: u64,
    unknown_records: u64,
    undecodable_records: u64,
}

impl RegistryService {
    /// An empty registry, with the level and mode that apply until the topic
    /// sets global ones.
    #[must_use]
    pub fn new(default_compatibility: &str, default_mode: &str) -> Self {
        Self {
            state: StoreState::with_defaults(default_compatibility, default_mode),
            applied: LogOffset::default(),
            records: 0,
            unknown_records: 0,
            undecodable_records: 0,
        }
    }

    /// The live state: the replay of the applied records.
    #[must_use]
    pub fn state(&self) -> &StoreState {
        &self.state
    }

    /// The offset after the last record applied; every record below it is in
    /// the state.
    #[must_use]
    pub fn applied(&self) -> LogOffset {
        self.applied
    }

    /// How many records the reader applied, noops included.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.records
    }

    /// Replayed records with a key type this registry does not know.
    #[must_use]
    pub fn unknown_records(&self) -> u64 {
        self.unknown_records
    }

    /// Replayed records whose key or value did not decode.
    #[must_use]
    pub fn undecodable_records(&self) -> u64 {
        self.undecodable_records
    }

    /// Fold the record at `offset` into the state: the reader's step, as
    /// Confluent's `KafkaStoreReaderThread` applies each record it consumes.
    pub fn apply(&mut self, offset: LogOffset, record: &RawRecord) {
        self.apply_decoded(&SchemaRecord::decode(record));
        self.records += 1;
        self.applied = offset.next();
    }

    fn apply_decoded(&mut self, record: &SchemaRecord) {
        match record {
            SchemaRecord::Schema(_, value) => self.state.apply_schema(value),
            SchemaRecord::Tombstone(key) => {
                self.state
                    .permanent_delete_version(&key.subject, key.version);
            }
            SchemaRecord::DeleteSubject(key, _) => {
                self.state.soft_delete_subject(&key.subject);
            }
            SchemaRecord::VersionHighWater(key, value) => {
                self.state
                    .observe_next_version(&key.subject, value.next_version);
            }
            SchemaRecord::Config(key, value) => match (key.subject.as_deref(), value) {
                (Some(subject), Some(v)) => self
                    .state
                    .set_subject_compat(subject, &v.compatibility_level),
                (Some(subject), None) => self.state.clear_subject_compat(subject),
                (None, Some(v)) => self.state.set_global_compat(&v.compatibility_level),
                (None, None) => self.state.clear_global_compat(),
            },
            SchemaRecord::Mode(key, value) => match (key.subject.as_deref(), value) {
                (Some(subject), Some(v)) => self.state.set_subject_mode(subject, &v.mode),
                (Some(subject), None) => self.state.clear_subject_mode(subject),
                (None, Some(v)) => self.state.set_global_mode(&v.mode),
                (None, None) => self.state.clear_global_mode(),
            },
            SchemaRecord::Noop => {}
            SchemaRecord::Unknown { .. } => self.unknown_records += 1,
            SchemaRecord::Undecodable { .. } => self.undecodable_records += 1,
        }
    }

    fn ensure_writable(&self, subject: &str) -> Result<(), RegistryError> {
        if self.state.effective_mode(subject) == "READONLY" {
            return Err(RegistryError::OperationNotPermitted(format!(
                "Subject {subject} is in read-only mode"
            )));
        }
        Ok(())
    }

    // ---- registration ----------------------------------------------------------------

    /// Register a schema: dedup, compatibility, id and version assignment,
    /// the `SCHEMA` record. In `IMPORT` mode the client's id and version are
    /// persisted as given, with no compatibility check; `READONLY` rejects.
    ///
    /// # Errors
    /// Returns the Confluent error the request maps to: an invalid schema or
    /// reference (42201), an incompatible one (409), a read-only subject or a
    /// missing import id (42205), an id bound to another schema (42205).
    pub fn register(&self, req: RegisterRequest<'_>) -> Result<Written<Registered>, RegistryError> {
        self.ensure_writable(req.subject)?;
        let resolved = self.state.resolve_closure(req.references)?;
        let parsed = format::parse(req.ty, req.schema, &resolved)?;
        let schema = parsed.storage_form.as_str();
        if self.state.effective_mode(req.subject) == "IMPORT" {
            let Some(id) = req.import_id else {
                return Err(RegistryError::OperationNotPermitted(
                    "Invalid id for import: null or negative".to_string(),
                ));
            };
            if self
                .state
                .schema_id_conflicts(id, req.ty, schema, req.references)?
            {
                return Err(RegistryError::SchemaIdConflict(id));
            }
            let version = req
                .import_version
                .unwrap_or_else(|| self.state.next_version(req.subject));
            let record = record::encode_schema(&SchemaValue {
                subject: req.subject.to_string(),
                version,
                id,
                schema_type: req.ty.wire_name().map(str::to_string),
                references: req.references.to_vec(),
                schema: schema.to_string(),
                deleted: false,
            });
            return Ok(Written::one(Registered { id, version }, record));
        }
        if let Some(existing) =
            self.state
                .find_under_subject(req.subject, req.ty, schema, req.references, false)
        {
            return Ok(Written::nothing(existing));
        }
        compat::check_registration(
            &self.state,
            req.subject,
            Candidate {
                ty: req.ty,
                schema,
                refs: &resolved,
            },
        )?;
        // The id and version are decided on a throwaway clone; the live state
        // changes only when the record comes back.
        let registered =
            self.state
                .clone()
                .register(req.subject, req.ty, schema, req.references)?;
        let record = record::encode_schema(&SchemaValue {
            subject: req.subject.to_string(),
            version: registered.version,
            id: registered.id,
            schema_type: req.ty.wire_name().map(str::to_string),
            references: req.references.to_vec(),
            schema: schema.to_string(),
            deleted: false,
        });
        Ok(Written::one(registered, record))
    }

    // ---- levels ----------------------------------------------------------------------------

    /// Set the global level (`subject: None`) or a subject's.
    ///
    /// # Errors
    /// Returns 42205 when the scope is read-only.
    pub fn set_compat(
        &self,
        subject: Option<&str>,
        level: &str,
    ) -> Result<Written<()>, RegistryError> {
        let mode = subject.map_or_else(
            || self.state.global_mode(),
            |s| self.state.effective_mode(s),
        );
        if mode == "READONLY" {
            return Err(RegistryError::OperationNotPermitted(format!(
                "Subject {} is in read-only mode",
                subject.unwrap_or("global")
            )));
        }
        Ok(Written::one((), record::encode_config(subject, level)))
    }

    /// Remove a subject's level so it inherits the global one. Returns the
    /// removed level, or `None` when the subject had none.
    ///
    /// # Errors
    /// Returns 42205 when the subject is read-only.
    pub fn delete_subject_compat(
        &self,
        subject: &str,
    ) -> Result<Written<Option<String>>, RegistryError> {
        self.ensure_writable(subject)?;
        let Some(level) = self.state.subject_compat(subject).map(str::to_string) else {
            return Ok(Written::nothing(None));
        };
        Ok(Written::one(
            Some(level),
            record::config_tombstone(Some(subject)),
        ))
    }

    /// Remove the global level so the configured default applies again.
    /// Returns the level that was in force.
    ///
    /// # Errors
    /// Returns 42205 when the registry is read-only.
    pub fn delete_global_compat(&self) -> Result<Written<String>, RegistryError> {
        if self.state.global_mode() == "READONLY" {
            return Err(RegistryError::OperationNotPermitted(
                "Subject global is in read-only mode".to_string(),
            ));
        }
        let level = self.state.global_compat().to_string();
        Ok(Written::one(level, record::config_tombstone(None)))
    }

    // ---- deletes ---------------------------------------------------------------------------

    fn ensure_not_referenced(
        &self,
        subject: &str,
        version: SchemaVersion,
        include_deleted: bool,
    ) -> Result<(), RegistryError> {
        if self
            .state
            .referenced_by(subject, version, include_deleted)
            .is_empty()
        {
            Ok(())
        } else {
            Err(RegistryError::ReferencedByOthers(format!(
                "{{magic=1,keytype=SCHEMA,subject={subject},version={version}}}"
            )))
        }
    }

    /// Soft-delete a version: its `SCHEMA` record is written again with
    /// `deleted: true`.
    ///
    /// # Errors
    /// Returns 40401 for an unknown subject, 40402 for an unknown version,
    /// 42206 when a live schema references it, 42205 when read-only.
    pub fn soft_delete_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Written<SchemaVersion>, RegistryError> {
        self.ensure_writable(subject)?;
        if self.state.versions(subject, true).is_none() {
            return Err(RegistryError::SubjectNotFound(subject.to_string()));
        }
        let found = self
            .state
            .version(subject, Some(version), false)
            .ok_or_else(|| RegistryError::VersionNotFound(version.to_string()))?;
        self.ensure_not_referenced(subject, version, false)?;
        let record = record::encode_schema(&SchemaValue {
            subject: subject.to_string(),
            version: found.version,
            id: found.id,
            schema_type: found.ty.wire_name().map(str::to_string),
            references: found.references,
            schema: found.schema,
            deleted: true,
        });
        Ok(Written::one(found.version, record))
    }

    /// Delete a version for good: a tombstone, preceded by the subject's
    /// version high-water mark so the number is never reused. The version
    /// must be soft-deleted first. When it was the subject's last version,
    /// the subject's level and mode go with it.
    ///
    /// # Errors
    /// Returns 40401, 40402, 40407 when it was not soft-deleted, 42206 when a
    /// schema (deleted or not) references it, 42205 when read-only.
    pub fn permanent_delete_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Written<SchemaVersion>, RegistryError> {
        self.ensure_writable(subject)?;
        if self.state.versions(subject, true).is_none() {
            return Err(RegistryError::SubjectNotFound(subject.to_string()));
        }
        if self.state.version(subject, Some(version), true).is_none() {
            return Err(RegistryError::VersionNotFound(version.to_string()));
        }
        if self.state.version(subject, Some(version), false).is_some() {
            return Err(RegistryError::VersionNotSoftDeleted {
                subject: subject.to_string(),
                version,
            });
        }
        self.ensure_not_referenced(subject, version, true)?;
        let mut records = vec![
            record::encode_version_high_water(subject, self.state.next_version(subject)),
            record::encode_tombstone(subject, version),
        ];
        let last = self
            .state
            .versions(subject, true)
            .is_some_and(|vs| vs == [version]);
        if last {
            records.extend(self.scope_tombstones(subject));
        }
        Ok(Written {
            value: version,
            records,
        })
    }

    /// The tombstones that drop a subject's own level and mode.
    fn scope_tombstones(&self, subject: &str) -> Vec<RawRecord> {
        let mut records = Vec::new();
        if self.state.subject_mode(subject).is_some() {
            records.push(record::mode_tombstone(Some(subject)));
        }
        if self.state.subject_compat(subject).is_some() {
            records.push(record::config_tombstone(Some(subject)));
        }
        records
    }

    /// Soft-delete a subject with a `DELETE_SUBJECT` marker. Returns the
    /// versions it had.
    ///
    /// # Errors
    /// Returns 40401 for an unknown subject, 40404 when it is soft-deleted
    /// already, 42206 when another subject's live schema references a
    /// version, 42205 when read-only.
    pub fn soft_delete_subject(
        &self,
        subject: &str,
    ) -> Result<Written<Vec<SchemaVersion>>, RegistryError> {
        self.ensure_writable(subject)?;
        let versions = match self.state.versions(subject, false) {
            Some(versions) => versions,
            None if self.state.versions(subject, true).is_some() => {
                return Err(RegistryError::SubjectSoftDeleted(subject.to_string()));
            }
            None => return Err(RegistryError::SubjectNotFound(subject.to_string())),
        };
        self.ensure_no_foreign_referrer(subject, &versions, false)?;
        let high = versions.iter().copied().max().unwrap_or(SchemaVersion(0));
        Ok(Written::one(
            versions,
            record::encode_delete_subject(subject, high),
        ))
    }

    /// Whether a subject other than `subject` references one of `versions`.
    fn ensure_no_foreign_referrer(
        &self,
        subject: &str,
        versions: &[SchemaVersion],
        include_deleted: bool,
    ) -> Result<(), RegistryError> {
        for &version in versions {
            let foreign = self
                .state
                .referenced_by(subject, version, include_deleted)
                .into_iter()
                .any(|id| {
                    self.state
                        .schema_id_subject_versions(id, include_deleted)
                        .iter()
                        .any(|(s, _)| s != subject)
                });
            if foreign {
                return Err(RegistryError::ReferencedByOthers(format!(
                    "{{magic=1,keytype=SCHEMA,subject={subject},version={version}}}"
                )));
            }
        }
        Ok(())
    }

    /// Delete a subject for good: the version high-water mark, a tombstone
    /// per version (highest first, so a version referencing an earlier one
    /// goes first), and the subject's level and mode. The subject must be
    /// soft-deleted first. Returns the versions it had.
    ///
    /// # Errors
    /// Returns 40401 for an unknown subject, 40405 when a live version
    /// remains, 42206 when another subject references a version, 42205 when
    /// read-only.
    pub fn permanent_delete_subject(
        &self,
        subject: &str,
    ) -> Result<Written<Vec<SchemaVersion>>, RegistryError> {
        self.ensure_writable(subject)?;
        let versions = self
            .state
            .versions(subject, true)
            .ok_or_else(|| RegistryError::SubjectNotFound(subject.to_string()))?;
        if self.state.versions(subject, false).is_some() {
            return Err(RegistryError::SubjectNotSoftDeleted(subject.to_string()));
        }
        self.ensure_no_foreign_referrer(subject, &versions, true)?;
        let mut records = vec![record::encode_version_high_water(
            subject,
            self.state.next_version(subject),
        )];
        records.extend(
            versions
                .iter()
                .rev()
                .map(|&v| record::encode_tombstone(subject, v)),
        );
        records.extend(self.scope_tombstones(subject));
        Ok(Written {
            value: versions,
            records,
        })
    }

    // ---- modes -------------------------------------------------------------------------------

    /// Set the global mode (`subject: None`) or a subject's. Switching to
    /// `IMPORT` needs the scope to hold no schema unless `force`.
    ///
    /// # Errors
    /// Returns 42204 for an unknown mode and 42205 when schemas exist.
    pub fn set_mode(
        &self,
        subject: Option<&str>,
        mode: &str,
        force: bool,
    ) -> Result<Written<()>, RegistryError> {
        if !MODES.contains(&mode) {
            return Err(RegistryError::InvalidMode);
        }
        let current = subject.map_or_else(
            || self.state.global_mode(),
            |s| self.state.effective_mode(s),
        );
        if mode == "IMPORT" && current != "IMPORT" && !force {
            let occupied = match subject {
                Some(s) => self.state.versions(s, true).is_some(),
                None => !self.state.subjects(true).is_empty(),
            };
            if occupied {
                return Err(RegistryError::OperationNotPermitted(
                    "Cannot import since found existing subjects".to_string(),
                ));
            }
        }
        Ok(Written::one((), record::encode_mode(subject, mode)))
    }

    /// Remove the global mode so the configured default applies again.
    /// Returns the mode that was in force.
    #[must_use]
    pub fn clear_global_mode(&self) -> Written<String> {
        let mode = self.state.global_mode().to_string();
        Written::one(mode, record::mode_tombstone(None))
    }

    /// Remove a subject's mode so it inherits the global one. Returns the
    /// removed mode, or `None` when the subject had none.
    #[must_use]
    pub fn clear_subject_mode(&self, subject: &str) -> Written<Option<String>> {
        let Some(mode) = self.state.subject_mode(subject).map(str::to_string) else {
            return Written::nothing(None);
        };
        Written::one(Some(mode), record::mode_tombstone(Some(subject)))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use assert2::assert;

    use super::*;

    fn av(name: &str) -> String {
        format!(
            "{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{{\"name\":\"{name}\",\"type\":\"int\",\"default\":0}}]}}"
        )
    }

    /// A registry whose store reads every record back as soon as it is
    /// written, with the log it wrote.
    struct Loopback {
        service: RegistryService,
        log: Vec<RawRecord>,
    }

    impl Loopback {
        fn new() -> Self {
            Self {
                service: RegistryService::new("BACKWARD", "READWRITE"),
                log: Vec::new(),
            }
        }

        /// Write the records a mutation decided, read them back, and give its
        /// value.
        fn commit<T>(
            &mut self,
            written: Result<Written<T>, RegistryError>,
        ) -> Result<T, RegistryError> {
            let written = written?;
            for record in written.records {
                let offset = LogOffset(i64::try_from(self.log.len()).unwrap());
                self.service.apply(offset, &record);
                self.log.push(record);
            }
            Ok(written.value)
        }
    }

    impl Deref for Loopback {
        type Target = RegistryService;

        fn deref(&self) -> &RegistryService {
            &self.service
        }
    }

    fn request<'a>(subject: &'a str, schema: &'a str) -> RegisterRequest<'a> {
        RegisterRequest {
            subject,
            ty: SchemaType::Avro,
            schema,
            references: &[],
            import_id: None,
            import_version: None,
        }
    }

    #[test]
    fn a_write_is_visible_once_its_record_is_read_back() {
        let mut s = RegistryService::new("BACKWARD", "READWRITE");
        let written = s.register(request("s", &av("A"))).unwrap();
        let value = SchemaValue {
            subject: "s".into(),
            version: SchemaVersion(1),
            id: SchemaId(1),
            schema_type: None,
            references: vec![],
            schema: av("A"),
            deleted: false,
        };
        assert!(
            written
                == Written {
                    value: Registered {
                        id: SchemaId(1),
                        version: SchemaVersion(1)
                    },
                    records: vec![record::encode_schema(&value)],
                }
        );
        // The decision changed nothing: the state moves when the record is
        // read back.
        assert!(s.state().versions("s", false).is_none());
        assert!(s.applied() == LogOffset(0));
        s.apply(LogOffset(0), &written.records[0]);
        assert!(s.applied() == LogOffset(1));
        assert!(s.record_count() == 1);
        assert!(s.state().versions("s", false) == Some(vec![SchemaVersion(1)]));
        // Registering the same schema again writes nothing.
        let again = s.register(request("s", &av("A"))).unwrap();
        assert!(again == Written::nothing(written.value));
    }

    #[test]
    fn the_state_is_the_replay_of_the_log() {
        let mut s = Loopback::new();
        s.commit(s.register(request("s", &av("A")))).unwrap();
        s.commit(s.register(request("s", &av("B")))).unwrap();
        s.commit(s.set_compat(Some("s"), "FULL")).unwrap();
        s.commit(s.set_mode(Some("t"), "READONLY", false)).unwrap();
        s.commit(s.soft_delete_version("s", SchemaVersion(1)))
            .unwrap();
        let mut replayed = RegistryService::new("BACKWARD", "READWRITE");
        for (offset, record) in (0..).map(LogOffset).zip(&s.log) {
            replayed.apply(offset, record);
        }
        assert!(replayed == s.service);
        assert!(replayed.record_count() == 5);
        assert!(replayed.state().versions("s", false) == Some(vec![SchemaVersion(2)]));
        assert!(replayed.state().subject_compat("s") == Some("FULL"));
        assert!(replayed.state().effective_mode("t") == "READONLY");
    }

    #[test]
    fn incompatible_and_invalid_schemas_write_nothing() {
        let mut s = Loopback::new();
        let base = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;
        let bad = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#;
        s.commit(s.register(request("s", base))).unwrap();
        let error = s.register(request("s", bad)).unwrap_err();
        assert!(matches!(error, RegistryError::Incompatible { .. }));
        assert!(s.register(request("s", "{nope")).unwrap_err().error_code() == 42201);
        assert!(s.log.len() == 1);
        s.commit(s.set_compat(Some("s"), "NONE")).unwrap();
        assert!(s.commit(s.register(request("s", bad))).unwrap().version == SchemaVersion(2));
    }

    #[test]
    fn modes_gate_writes_and_import_takes_client_ids() {
        let mut s = Loopback::new();
        s.commit(s.register(request("s", &av("A")))).unwrap();
        assert!(s.set_mode(None, "IMPORT", false).unwrap_err().error_code() == 42205);
        assert!(s.set_mode(None, "SIDEWAYS", false) == Err(RegistryError::InvalidMode));
        s.commit(s.set_mode(Some("s"), "READONLY", false)).unwrap();
        assert!(s.register(request("s", &av("B"))).unwrap_err().error_code() == 42205);
        assert!(s.soft_delete_subject("s").unwrap_err().error_code() == 42205);
        assert!(s.set_compat(Some("s"), "FULL").unwrap_err().error_code() == 42205);
        assert!(s.commit(Ok(s.clear_subject_mode("s"))) == Ok(Some("READONLY".to_string())));
        assert!(s.clear_subject_mode("s") == Written::nothing(None));
        s.commit(s.set_mode(None, "IMPORT", true)).unwrap();
        assert!(s.register(request("t", &av("T"))).unwrap_err().error_code() == 42205);
        let imported = s
            .commit(s.register(RegisterRequest {
                import_id: Some(SchemaId(42)),
                import_version: Some(SchemaVersion(7)),
                ..request("t", &av("T"))
            }))
            .unwrap();
        assert!(
            imported
                == Registered {
                    id: SchemaId(42),
                    version: SchemaVersion(7)
                }
        );
        assert!(
            s.register(RegisterRequest {
                import_id: Some(SchemaId(42)),
                import_version: None,
                ..request("u", &av("U"))
            }) == Err(RegistryError::SchemaIdConflict(SchemaId(42)))
        );
        let next = s
            .commit(s.register(RegisterRequest {
                import_id: Some(SchemaId(43)),
                import_version: None,
                ..request("t", &av("T2"))
            }))
            .unwrap();
        assert!(next.version == SchemaVersion(8));
    }

    #[test]
    fn deletes_follow_the_soft_then_permanent_protocol() {
        let mut s = Loopback::new();
        s.commit(s.register(request("s", &av("A")))).unwrap();
        s.commit(s.register(request("s", &av("B")))).unwrap();
        s.commit(s.set_compat(Some("s"), "FULL")).unwrap();
        let refused = [
            s.permanent_delete_version("s", SchemaVersion(1)),
            s.soft_delete_version("s", SchemaVersion(9)),
            s.soft_delete_version("nope", SchemaVersion(1)),
        ]
        .map(|r| r.unwrap_err().error_code());
        assert!(refused == [40407, 40402, 40401]);
        assert!(s.commit(s.soft_delete_version("s", SchemaVersion(1))) == Ok(SchemaVersion(1)));
        assert!(
            s.soft_delete_version("s", SchemaVersion(1))
                .unwrap_err()
                .error_code()
                == 40402
        );
        assert!(
            s.commit(s.permanent_delete_version("s", SchemaVersion(1))) == Ok(SchemaVersion(1))
        );
        assert!(s.permanent_delete_subject("s").unwrap_err().error_code() == 40405);
        assert!(s.commit(s.soft_delete_subject("s")) == Ok(vec![SchemaVersion(2)]));
        assert!(s.soft_delete_subject("s").unwrap_err().error_code() == 40404);
        assert!(s.soft_delete_subject("nope").unwrap_err().error_code() == 40401);
        assert!(s.state().subjects(false).is_empty());
        assert!(s.commit(s.permanent_delete_subject("s")) == Ok(vec![SchemaVersion(2)]));
        assert!(s.state().subjects(true).is_empty());
        assert!(s.state().subject_compat("s").is_none());
        // The version numbers are not reused after a permanent delete.
        assert!(
            s.commit(s.register(request("s", &av("C"))))
                .unwrap()
                .version
                == SchemaVersion(3)
        );
    }

    #[test]
    fn references_block_deletes_of_their_targets() {
        let mut s = Loopback::new();
        s.commit(s.register(request(
            "base",
            r#"{"type":"record","name":"Base","fields":[]}"#,
        )))
        .unwrap();
        let reference = SchemaReference {
            name: "Base".into(),
            subject: "base".into(),
            version: SchemaVersion(1),
        };
        s.commit(s.register(RegisterRequest {
            references: std::slice::from_ref(&reference),
            ..request(
                "dep",
                r#"{"type":"record","name":"Dep","fields":[{"name":"b","type":"Base"}]}"#,
            )
        }))
        .unwrap();
        assert!(
            s.soft_delete_version("base", SchemaVersion(1))
                .unwrap_err()
                .error_code()
                == 42206
        );
        assert!(s.soft_delete_subject("base").unwrap_err().error_code() == 42206);
        s.commit(s.soft_delete_subject("dep")).unwrap();
        assert!(s.commit(s.soft_delete_subject("base")) == Ok(vec![SchemaVersion(1)]));
        assert!(s.permanent_delete_subject("base").unwrap_err().error_code() == 42206);
        s.commit(s.permanent_delete_subject("dep")).unwrap();
        assert!(s.commit(s.permanent_delete_subject("base")) == Ok(vec![SchemaVersion(1)]));
        let missing = s.register(RegisterRequest {
            references: std::slice::from_ref(&reference),
            ..request("dep", &av("Dep"))
        });
        assert!(missing.unwrap_err().error_code() == 42201);
    }

    #[test]
    fn levels_can_be_set_and_cleared() {
        let mut s = Loopback::new();
        assert!(s.delete_subject_compat("s") == Ok(Written::nothing(None)));
        s.commit(s.set_compat(None, "FORWARD")).unwrap();
        s.commit(s.set_compat(Some("s"), "NONE")).unwrap();
        assert!(s.state().global_compat() == "FORWARD");
        assert!(s.state().subject_compat("s") == Some("NONE"));
        assert!(s.commit(s.delete_subject_compat("s")) == Ok(Some("NONE".to_string())));
        assert!(s.state().subject_compat("s").is_none());
        assert!(s.commit(s.delete_global_compat()) == Ok("FORWARD".to_string()));
        assert!(s.state().global_compat() == "BACKWARD");
        s.commit(s.set_mode(None, "READONLY", false)).unwrap();
        assert!(s.delete_global_compat().unwrap_err().error_code() == 42205);
        assert!(s.commit(Ok(s.clear_global_mode())) == Ok("READONLY".to_string()));
        assert!(s.state().global_mode() == "READWRITE");
    }

    #[test]
    fn foreign_records_are_counted_not_applied() {
        let mut s = RegistryService::new("BACKWARD", "READWRITE");
        let records = [
            RawRecord {
                key: bytes::Bytes::from_static(br#"{"keytype":"CONTEXT","magic":0}"#),
                value: None,
            },
            RawRecord {
                key: bytes::Bytes::from_static(b"garbage"),
                value: None,
            },
            record::encode_noop(),
            record::encode_config(None, "FULL"),
        ];
        for (offset, record) in (0..).map(LogOffset).zip(&records) {
            s.apply(offset, record);
        }
        assert!(s.unknown_records() == 1);
        assert!(s.undecodable_records() == 1);
        assert!(s.state().global_compat() == "FULL");
        assert!(s.applied() == LogOffset(4));
        assert!(s.record_count() == 4);
    }
}
