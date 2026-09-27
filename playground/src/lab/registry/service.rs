//! The registry service: the real registry's write path over a
//! [`SchemaStore`].
//!
//! Every mutation decides its outcome on a clone of the state, appends the
//! record, and folds the log's tail back into the live state. The live state
//! only ever changes by applying records, so it is always the replay of the
//! log, and a mutation's outcome is visible to the next request only when the
//! record has been read back. The node holds the HTTP response until
//! [`RegistryService::applied`] passes the record's offset: with the in-memory
//! log that is immediate; with a Kafka-backed store it is the fetch loop.

use super::{
    compat::{self, Candidate},
    error::RegistryError,
    format::{self, SchemaType},
    ids::{LogOffset, SchemaId, SchemaVersion},
    log::SchemaStore,
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

/// The outcome of a mutation: its value and the offset of the last record it
/// wrote, which the response must wait for. `None` when the mutation wrote
/// nothing (an idempotent registration, a clear of something already clear),
/// so the response goes out at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written<T> {
    pub value: T,
    pub offset: Option<LogOffset>,
}

/// The registry over its log.
pub struct RegistryService<S> {
    state: StoreState,
    store: S,
    /// The offset the reader applies next.
    applied: LogOffset,
    /// Records this service appended and has not yet handed to the node.
    appended: Vec<(LogOffset, RawRecord)>,
    unknown_records: u64,
    undecodable_records: u64,
}

impl<S: SchemaStore> RegistryService<S> {
    /// A service over `store`, with the level and mode that apply until the
    /// log sets global ones. Records already in the store are applied.
    pub fn new(store: S, default_compatibility: &str, default_mode: &str) -> Self {
        let mut service = Self {
            state: StoreState::with_defaults(default_compatibility, default_mode),
            store,
            applied: LogOffset::default(),
            appended: Vec::new(),
            unknown_records: 0,
            undecodable_records: 0,
        };
        service.poll();
        service
    }

    /// The live state: the replay of the applied records.
    #[must_use]
    pub fn state(&self) -> &StoreState {
        &self.state
    }

    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The offset the reader applies next; every record below it is in the
    /// state.
    #[must_use]
    pub fn applied(&self) -> LogOffset {
        self.applied
    }

    /// How many records the log holds.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        u64::try_from(self.store.end_offset().0).unwrap_or(0)
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

    /// Fold every record the store holds past `applied` into the state; the
    /// reader's step. Returns how many records were applied.
    pub fn poll(&mut self) -> usize {
        let tail = self.store.replay(self.applied);
        let count = tail.len();
        for (offset, record) in tail {
            self.apply(&SchemaRecord::decode(&record));
            self.applied = offset.next();
        }
        count
    }

    /// The records appended since the last call, for the node to persist.
    pub fn take_appended(&mut self) -> Vec<(LogOffset, RawRecord)> {
        std::mem::take(&mut self.appended)
    }

    /// Put records the host kept back into the log and apply them: a restart
    /// replaying `_schemas`. They are not reported by
    /// [`RegistryService::take_appended`], because the host already has them.
    pub fn restore(&mut self, records: impl IntoIterator<Item = RawRecord>) -> usize {
        for record in records {
            self.store.append(record);
        }
        self.poll()
    }

    fn apply(&mut self, record: &SchemaRecord) {
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

    /// Append a record, then fold the log's tail back. Returns the record's
    /// offset; the state reflects the record once `applied` passes it.
    fn write(&mut self, record: RawRecord) -> LogOffset {
        let offset = self.store.append(record.clone());
        self.appended.push((offset, record));
        self.poll();
        offset
    }

    /// Append several records in order; returns the last offset.
    fn write_all(&mut self, records: Vec<RawRecord>) -> LogOffset {
        let mut last = self.applied;
        for record in records {
            last = self.write(record);
        }
        last
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
    pub fn register(
        &mut self,
        req: RegisterRequest<'_>,
    ) -> Result<Written<Registered>, RegistryError> {
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
            let offset = self.write(record::encode_schema(&SchemaValue {
                subject: req.subject.to_string(),
                version,
                id,
                schema_type: req.ty.wire_name().map(str::to_string),
                references: req.references.to_vec(),
                schema: schema.to_string(),
                deleted: false,
            }));
            return Ok(Written {
                value: Registered { id, version },
                offset: Some(offset),
            });
        }
        if let Some(existing) =
            self.state
                .find_under_subject(req.subject, req.ty, schema, req.references, false)
        {
            return Ok(Written {
                value: existing,
                offset: None,
            });
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
        let offset = self.write(record::encode_schema(&SchemaValue {
            subject: req.subject.to_string(),
            version: registered.version,
            id: registered.id,
            schema_type: req.ty.wire_name().map(str::to_string),
            references: req.references.to_vec(),
            schema: schema.to_string(),
            deleted: false,
        }));
        Ok(Written {
            value: registered,
            offset: Some(offset),
        })
    }

    // ---- levels ----------------------------------------------------------------------------

    /// Set the global level (`subject: None`) or a subject's.
    ///
    /// # Errors
    /// Returns 42205 when the scope is read-only.
    pub fn set_compat(
        &mut self,
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
        let offset = self.write(record::encode_config(subject, level));
        Ok(Written {
            value: (),
            offset: Some(offset),
        })
    }

    /// Remove a subject's level so it inherits the global one. Returns the
    /// removed level, or `None` when the subject had none.
    ///
    /// # Errors
    /// Returns 42205 when the subject is read-only.
    pub fn delete_subject_compat(
        &mut self,
        subject: &str,
    ) -> Result<Written<Option<String>>, RegistryError> {
        self.ensure_writable(subject)?;
        let Some(level) = self.state.subject_compat(subject).map(str::to_string) else {
            return Ok(Written {
                value: None,
                offset: None,
            });
        };
        let offset = self.write(record::config_tombstone(Some(subject)));
        Ok(Written {
            value: Some(level),
            offset: Some(offset),
        })
    }

    /// Remove the global level so the configured default applies again.
    /// Returns the level that was in force.
    ///
    /// # Errors
    /// Returns 42205 when the registry is read-only.
    pub fn delete_global_compat(&mut self) -> Result<Written<String>, RegistryError> {
        if self.state.global_mode() == "READONLY" {
            return Err(RegistryError::OperationNotPermitted(
                "Subject global is in read-only mode".to_string(),
            ));
        }
        let level = self.state.global_compat().to_string();
        let offset = self.write(record::config_tombstone(None));
        Ok(Written {
            value: level,
            offset: Some(offset),
        })
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
        &mut self,
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
        let offset = self.write(record::encode_schema(&SchemaValue {
            subject: subject.to_string(),
            version: found.version,
            id: found.id,
            schema_type: found.ty.wire_name().map(str::to_string),
            references: found.references,
            schema: found.schema,
            deleted: true,
        }));
        Ok(Written {
            value: found.version,
            offset: Some(offset),
        })
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
        &mut self,
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
        let offset = self.write_all(records);
        Ok(Written {
            value: version,
            offset: Some(offset),
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
        &mut self,
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
        let offset = self.write(record::encode_delete_subject(subject, high));
        Ok(Written {
            value: versions,
            offset: Some(offset),
        })
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
        &mut self,
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
        let offset = self.write_all(records);
        Ok(Written {
            value: versions,
            offset: Some(offset),
        })
    }

    // ---- modes -------------------------------------------------------------------------------

    /// Set the global mode (`subject: None`) or a subject's. Switching to
    /// `IMPORT` needs the scope to hold no schema unless `force`.
    ///
    /// # Errors
    /// Returns 42204 for an unknown mode and 42205 when schemas exist.
    pub fn set_mode(
        &mut self,
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
        let offset = self.write(record::encode_mode(subject, mode));
        Ok(Written {
            value: (),
            offset: Some(offset),
        })
    }

    /// Remove the global mode so the configured default applies again.
    /// Returns the mode that was in force.
    pub fn clear_global_mode(&mut self) -> Written<String> {
        let mode = self.state.global_mode().to_string();
        let offset = self.write(record::mode_tombstone(None));
        Written {
            value: mode,
            offset: Some(offset),
        }
    }

    /// Remove a subject's mode so it inherits the global one. Returns the
    /// removed mode, or `None` when the subject had none.
    pub fn clear_subject_mode(&mut self, subject: &str) -> Written<Option<String>> {
        let Some(mode) = self.state.subject_mode(subject).map(str::to_string) else {
            return Written {
                value: None,
                offset: None,
            };
        };
        let offset = self.write(record::mode_tombstone(Some(subject)));
        Written {
            value: Some(mode),
            offset: Some(offset),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::lab::registry::log::SchemaLog;

    fn av(name: &str) -> String {
        format!(
            "{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{{\"name\":\"{name}\",\"type\":\"int\",\"default\":0}}]}}"
        )
    }

    fn service() -> RegistryService<SchemaLog> {
        RegistryService::new(SchemaLog::new(), "BACKWARD", "READWRITE")
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
        let mut s = service();
        let written = s.register(request("s", &av("A"))).unwrap();
        assert!(
            written
                == Written {
                    value: Registered {
                        id: SchemaId(1),
                        version: SchemaVersion(1)
                    },
                    offset: Some(LogOffset(0))
                }
        );
        assert!(s.applied() == LogOffset(1));
        assert!(s.record_count() == 1);
        assert!(s.state().versions("s", false) == Some(vec![SchemaVersion(1)]));
        let appended = s.take_appended();
        assert!(appended.len() == 1);
        assert!(appended[0].0 == LogOffset(0));
        assert!(
            SchemaRecord::decode(&appended[0].1)
                == SchemaRecord::Schema(
                    record::SchemaKey::new("s", SchemaVersion(1)),
                    SchemaValue {
                        subject: "s".into(),
                        version: SchemaVersion(1),
                        id: SchemaId(1),
                        schema_type: None,
                        references: vec![],
                        schema: av("A"),
                        deleted: false
                    }
                )
        );
        assert!(s.take_appended().is_empty());
        // Registering the same schema again writes nothing.
        let again = s.register(request("s", &av("A"))).unwrap();
        assert!(again.value == written.value);
        assert!(s.record_count() == 1);
    }

    #[test]
    fn the_state_is_the_replay_of_the_log() {
        let mut s = service();
        s.register(request("s", &av("A"))).unwrap();
        s.register(request("s", &av("B"))).unwrap();
        s.set_compat(Some("s"), "FULL").unwrap();
        s.set_mode(Some("t"), "READONLY", false).unwrap();
        s.soft_delete_version("s", SchemaVersion(1)).unwrap();
        let records: Vec<RawRecord> = s.store().records().to_vec();
        let mut replayed = RegistryService::new(SchemaLog::new(), "BACKWARD", "READWRITE");
        assert!(replayed.restore(records) == 5);
        assert!(replayed.state() == s.state());
        assert!(replayed.take_appended().is_empty());
        assert!(replayed.state().versions("s", false) == Some(vec![SchemaVersion(2)]));
        assert!(replayed.state().subject_compat("s") == Some("FULL"));
        assert!(replayed.state().effective_mode("t") == "READONLY");
    }

    #[test]
    fn incompatible_and_invalid_schemas_write_nothing() {
        let mut s = service();
        let base = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;
        let bad = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#;
        s.register(request("s", base)).unwrap();
        let error = s.register(request("s", bad)).unwrap_err();
        assert!(matches!(error, RegistryError::Incompatible { .. }));
        assert!(s.register(request("s", "{nope")).unwrap_err().error_code() == 42201);
        assert!(s.record_count() == 1);
        s.set_compat(Some("s"), "NONE").unwrap();
        assert!(s.register(request("s", bad)).unwrap().value.version == SchemaVersion(2));
    }

    #[test]
    fn modes_gate_writes_and_import_takes_client_ids() {
        let mut s = service();
        s.register(request("s", &av("A"))).unwrap();
        assert!(s.set_mode(None, "IMPORT", false).unwrap_err().error_code() == 42205);
        assert!(s.set_mode(None, "SIDEWAYS", false) == Err(RegistryError::InvalidMode));
        s.set_mode(Some("s"), "READONLY", false).unwrap();
        assert!(s.register(request("s", &av("B"))).unwrap_err().error_code() == 42205);
        assert!(s.soft_delete_subject("s").unwrap_err().error_code() == 42205);
        assert!(s.set_compat(Some("s"), "FULL").unwrap_err().error_code() == 42205);
        assert!(s.clear_subject_mode("s").value == Some("READONLY".to_string()));
        assert!(s.clear_subject_mode("s").value.is_none());
        s.set_mode(None, "IMPORT", true).unwrap();
        assert!(s.register(request("t", &av("T"))).unwrap_err().error_code() == 42205);
        let imported = s
            .register(RegisterRequest {
                import_id: Some(SchemaId(42)),
                import_version: Some(SchemaVersion(7)),
                ..request("t", &av("T"))
            })
            .unwrap();
        assert!(
            imported.value
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
            .register(RegisterRequest {
                import_id: Some(SchemaId(43)),
                import_version: None,
                ..request("t", &av("T2"))
            })
            .unwrap();
        assert!(next.value.version == SchemaVersion(8));
    }

    #[test]
    fn deletes_follow_the_soft_then_permanent_protocol() {
        let mut s = service();
        s.register(request("s", &av("A"))).unwrap();
        s.register(request("s", &av("B"))).unwrap();
        s.set_compat(Some("s"), "FULL").unwrap();
        assert!(
            s.permanent_delete_version("s", SchemaVersion(1))
                .unwrap_err()
                .error_code()
                == 40407
        );
        assert!(
            s.soft_delete_version("s", SchemaVersion(9))
                .unwrap_err()
                .error_code()
                == 40402
        );
        assert!(
            s.soft_delete_version("nope", SchemaVersion(1))
                .unwrap_err()
                .error_code()
                == 40401
        );
        assert!(s.soft_delete_version("s", SchemaVersion(1)).unwrap().value == SchemaVersion(1));
        assert!(
            s.soft_delete_version("s", SchemaVersion(1))
                .unwrap_err()
                .error_code()
                == 40402
        );
        assert!(
            s.permanent_delete_version("s", SchemaVersion(1))
                .unwrap()
                .value
                == SchemaVersion(1)
        );
        assert!(s.permanent_delete_subject("s").unwrap_err().error_code() == 40405);
        assert!(s.soft_delete_subject("s").unwrap().value == vec![SchemaVersion(2)]);
        assert!(s.soft_delete_subject("s").unwrap_err().error_code() == 40404);
        assert!(s.soft_delete_subject("nope").unwrap_err().error_code() == 40401);
        assert!(s.state().subjects(false).is_empty());
        assert!(s.permanent_delete_subject("s").unwrap().value == vec![SchemaVersion(2)]);
        assert!(s.state().subjects(true).is_empty());
        assert!(s.state().subject_compat("s").is_none());
        // The version numbers are not reused after a permanent delete.
        assert!(s.register(request("s", &av("C"))).unwrap().value.version == SchemaVersion(3));
    }

    #[test]
    fn references_block_deletes_of_their_targets() {
        let mut s = service();
        s.register(request(
            "base",
            r#"{"type":"record","name":"Base","fields":[]}"#,
        ))
        .unwrap();
        let reference = SchemaReference {
            name: "Base".into(),
            subject: "base".into(),
            version: SchemaVersion(1),
        };
        s.register(RegisterRequest {
            references: std::slice::from_ref(&reference),
            ..request(
                "dep",
                r#"{"type":"record","name":"Dep","fields":[{"name":"b","type":"Base"}]}"#,
            )
        })
        .unwrap();
        assert!(
            s.soft_delete_version("base", SchemaVersion(1))
                .unwrap_err()
                .error_code()
                == 42206
        );
        assert!(s.soft_delete_subject("base").unwrap_err().error_code() == 42206);
        s.soft_delete_subject("dep").unwrap();
        assert!(s.soft_delete_subject("base").unwrap().value == vec![SchemaVersion(1)]);
        assert!(s.permanent_delete_subject("base").unwrap_err().error_code() == 42206);
        s.permanent_delete_subject("dep").unwrap();
        assert!(s.permanent_delete_subject("base").unwrap().value == vec![SchemaVersion(1)]);
        let missing = s.register(RegisterRequest {
            references: std::slice::from_ref(&reference),
            ..request("dep", &av("Dep"))
        });
        assert!(missing.unwrap_err().error_code() == 42201);
    }

    #[test]
    fn levels_can_be_set_and_cleared() {
        let mut s = service();
        assert!(s.delete_subject_compat("s").unwrap().value.is_none());
        s.set_compat(None, "FORWARD").unwrap();
        s.set_compat(Some("s"), "NONE").unwrap();
        assert!(s.state().global_compat() == "FORWARD");
        assert!(s.state().subject_compat("s") == Some("NONE"));
        assert!(s.delete_subject_compat("s").unwrap().value == Some("NONE".to_string()));
        assert!(s.state().subject_compat("s").is_none());
        assert!(s.delete_global_compat().unwrap().value == "FORWARD");
        assert!(s.state().global_compat() == "BACKWARD");
        s.set_mode(None, "READONLY", false).unwrap();
        assert!(s.delete_global_compat().unwrap_err().error_code() == 42205);
        assert!(s.clear_global_mode().value == "READONLY");
        assert!(s.state().global_mode() == "READWRITE");
    }

    #[test]
    fn foreign_records_are_counted_not_applied() {
        let mut log = SchemaLog::new();
        log.append(RawRecord {
            key: bytes::Bytes::from_static(br#"{"keytype":"CONTEXT","magic":0}"#),
            value: None,
        });
        log.append(RawRecord {
            key: bytes::Bytes::from_static(b"garbage"),
            value: None,
        });
        log.append(record::encode_config(None, "FULL"));
        let s = RegistryService::new(log, "BACKWARD", "READWRITE");
        assert!(s.unknown_records() == 1);
        assert!(s.undecodable_records() == 1);
        assert!(s.state().global_compat() == "FULL");
        assert!(s.applied() == LogOffset(3));
    }
}
