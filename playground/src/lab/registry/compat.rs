//! The compatibility engine: the effective level of a subject, the version
//! set it selects, and the directions each level checks.
//!
//! The eight Confluent levels reduce to a direction set and a version set. A
//! `BACKWARD` level asks whether the candidate can read every earlier
//! version; `FORWARD` whether every earlier version can read the candidate;
//! `FULL` both. The plain levels check the latest live version only; the
//! `_TRANSITIVE` levels check every live version. The per-format check is
//! [`format::check`].

use std::fmt;

use super::{
    error::RegistryError,
    format::{self, ResolvedReference, SchemaType},
    ids::SchemaVersion,
    store::StoreState,
};

/// A Confluent compatibility level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityLevel {
    None,
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
}

/// One directional check.
#[derive(Debug, Clone, Copy)]
enum Direction {
    /// The candidate reads what the existing version wrote.
    NewReadsOld,
    /// The existing version reads what the candidate writes.
    OldReadsNew,
}

impl CompatibilityLevel {
    /// Every level, in Confluent's order.
    pub const ALL: [Self; 7] = [
        Self::None,
        Self::Backward,
        Self::BackwardTransitive,
        Self::Forward,
        Self::ForwardTransitive,
        Self::Full,
        Self::FullTransitive,
    ];

    /// Parse a level name, case-insensitively as Confluent does.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|level| level.as_str().eq_ignore_ascii_case(name))
    }

    /// The level name as the API spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
        }
    }

    /// Whether the level checks every earlier version rather than the latest.
    #[must_use]
    pub fn is_transitive(self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }

    fn directions(self) -> &'static [Direction] {
        match self {
            Self::None => &[],
            Self::Backward | Self::BackwardTransitive => &[Direction::NewReadsOld],
            Self::Forward | Self::ForwardTransitive => &[Direction::OldReadsNew],
            Self::Full | Self::FullTransitive => &[Direction::NewReadsOld, Direction::OldReadsNew],
        }
    }
}

impl fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The outcome of a compatibility check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub is_compatible: bool,
    /// One message per incompatible difference, in the checker's wording.
    pub messages: Vec<String>,
}

impl Verdict {
    fn from_messages(messages: Vec<String>) -> Self {
        Self {
            is_compatible: messages.is_empty(),
            messages,
        }
    }
}

/// A candidate schema with its resolved references.
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    pub ty: SchemaType,
    pub schema: &'a str,
    pub refs: &'a [ResolvedReference],
}

/// The level that applies to a subject: its own, else the global one.
#[must_use]
pub fn effective_level(state: &StoreState, subject: &str) -> CompatibilityLevel {
    let name = state
        .subject_compat(subject)
        .unwrap_or_else(|| state.global_compat());
    CompatibilityLevel::parse(name).unwrap_or(CompatibilityLevel::Backward)
}

/// Check a candidate against one existing version in every direction of the
/// level, collecting the messages.
fn check_pair(
    candidate: Candidate<'_>,
    existing: &str,
    existing_refs: &[ResolvedReference],
    directions: &[Direction],
    out: &mut Vec<String>,
) {
    for direction in directions {
        let (reader, writer, reader_refs, writer_refs) = match direction {
            Direction::NewReadsOld => (candidate.schema, existing, candidate.refs, existing_refs),
            Direction::OldReadsNew => (existing, candidate.schema, existing_refs, candidate.refs),
        };
        if let Err(messages) = format::check(candidate.ty, reader, writer, reader_refs, writer_refs)
        {
            out.extend(messages);
        }
    }
}

/// The verdict a registration under `subject` gets: compatible when the
/// level is `NONE`, when the subject has no live version, or when the
/// candidate passes against the level's version set.
#[must_use]
pub fn verdict_for_registration(
    state: &StoreState,
    subject: &str,
    candidate: Candidate<'_>,
) -> Verdict {
    let level = effective_level(state, subject);
    let directions = level.directions();
    let versions = state.versions_schemas(subject);
    if directions.is_empty() || versions.is_empty() {
        return Verdict::from_messages(Vec::new());
    }
    let targets = if level.is_transitive() {
        &versions[..]
    } else {
        &versions[versions.len() - 1..]
    };
    let mut messages = Vec::new();
    for (_, schema, references) in targets {
        // A stored version resolved when it was registered; an unresolvable
        // closure now means a reference was deleted, and the check proceeds
        // without it.
        let existing_refs = state.resolve_closure(references).unwrap_or_default();
        check_pair(candidate, schema, &existing_refs, directions, &mut messages);
    }
    Verdict::from_messages(messages)
}

/// Enforce compatibility on a registration.
///
/// # Errors
/// Returns [`RegistryError::Incompatible`] with the checker's messages.
pub fn check_registration(
    state: &StoreState,
    subject: &str,
    candidate: Candidate<'_>,
) -> Result<(), RegistryError> {
    let verdict = verdict_for_registration(state, subject, candidate);
    if verdict.is_compatible {
        Ok(())
    } else {
        Err(RegistryError::Incompatible {
            subject: subject.to_string(),
            messages: verdict.messages,
        })
    }
}

/// The verdict of a candidate against one version of a subject, `None` being
/// the latest live one. As Confluent does, a subject or a latest version that
/// does not exist is compatible, and a concrete version that does not exist
/// is an error.
///
/// # Errors
/// Returns [`RegistryError::VersionNotFound`] for a concrete version the
/// subject does not have.
pub fn check_against_version(
    state: &StoreState,
    subject: &str,
    candidate: Candidate<'_>,
    version: Option<SchemaVersion>,
) -> Result<Verdict, RegistryError> {
    let Some(existing) = state.version(subject, version, false) else {
        return match version {
            None => Ok(Verdict::from_messages(Vec::new())),
            Some(v) => Err(RegistryError::VersionNotFound(v.to_string())),
        };
    };
    let directions = effective_level(state, subject).directions();
    let existing_refs = state
        .resolve_closure(&existing.references)
        .unwrap_or_default();
    let mut messages = Vec::new();
    check_pair(
        candidate,
        &existing.schema,
        &existing_refs,
        directions,
        &mut messages,
    );
    Ok(Verdict::from_messages(messages))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn av(fields: &str) -> String {
        format!("{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{fields}]}}")
    }

    const ID: &str = r#"{"name":"id","type":"int"}"#;

    fn candidate(schema: &str) -> Candidate<'_> {
        Candidate {
            ty: SchemaType::Avro,
            schema,
            refs: &[],
        }
    }

    fn state_with(level: &str, versions: &[&str]) -> StoreState {
        let mut state = StoreState::default();
        state.set_subject_compat("s", level);
        for schema in versions {
            state.register("s", SchemaType::Avro, schema, &[]).unwrap();
        }
        state
    }

    #[test]
    fn levels_parse_case_insensitively_and_know_their_shape() {
        for (input, level, transitive, directions) in [
            ("BACKWARD", CompatibilityLevel::Backward, false, 1),
            ("backward", CompatibilityLevel::Backward, false, 1),
            (
                "Forward_Transitive",
                CompatibilityLevel::ForwardTransitive,
                true,
                1,
            ),
            (
                "FULL_TRANSITIVE",
                CompatibilityLevel::FullTransitive,
                true,
                2,
            ),
            ("NONE", CompatibilityLevel::None, false, 0),
        ] {
            let parsed = CompatibilityLevel::parse(input).unwrap();
            assert!(parsed == level);
            assert!(parsed.is_transitive() == transitive);
            assert!(parsed.directions().len() == directions);
            assert!(CompatibilityLevel::parse(parsed.as_str()) == Some(parsed));
        }
        assert!(CompatibilityLevel::parse("SIDEWAYS").is_none());
        assert!(CompatibilityLevel::Full.to_string() == "FULL");
    }

    #[test]
    fn backward_requires_defaults_and_forward_allows_the_reverse() {
        let base = av(ID);
        let with_default = av(&format!(
            "{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"
        ));
        let without_default = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        let narrowed = av("");
        for (name, level, existing, new, compatible) in [
            (
                "first version passes",
                "BACKWARD",
                &[][..],
                &without_default,
                true,
            ),
            (
                "backward: defaulted field",
                "BACKWARD",
                &[base.as_str()],
                &with_default,
                true,
            ),
            (
                "backward: required field",
                "BACKWARD",
                &[base.as_str()],
                &without_default,
                false,
            ),
            (
                "backward: dropping a field",
                "BACKWARD",
                &[base.as_str()],
                &narrowed,
                true,
            ),
            (
                "forward: required field",
                "FORWARD",
                &[base.as_str()],
                &without_default,
                true,
            ),
            (
                "forward: dropping a field",
                "FORWARD",
                &[base.as_str()],
                &narrowed,
                false,
            ),
            (
                "full: defaulted field",
                "FULL",
                &[base.as_str()],
                &with_default,
                true,
            ),
            (
                "full: required field",
                "FULL",
                &[base.as_str()],
                &without_default,
                false,
            ),
            (
                "none: anything goes",
                "NONE",
                &[base.as_str()],
                &without_default,
                true,
            ),
        ] {
            let state = state_with(level, existing);
            let ok = check_registration(&state, "s", candidate(new)).is_ok();
            assert!(ok == compatible, "{name}");
        }
    }

    #[test]
    fn transitive_levels_check_every_version() {
        let v1 = av(ID);
        // v2 defaults both fields, so it reads anything.
        let v2 =
            av(r#"{"name":"id","type":"int","default":0},{"name":"x","type":"int","default":0}"#);
        // v3 drops `id`: v1 cannot read it, v2 can.
        let v3 = av(r#"{"name":"x","type":"int","default":0}"#);
        // v4 brings back `id` without a default: it reads v1 and v2 (which
        // write id) but not v3 (which lacks it).
        let v4 = av(&format!(
            "{{\"name\":\"x\",\"type\":\"int\",\"default\":0}},{ID}"
        ));
        for (name, level, existing, new, compatible) in [
            (
                "backward: latest only",
                "BACKWARD",
                &[v1.as_str(), v2.as_str(), v3.as_str()][..],
                &v4,
                false,
            ),
            (
                "backward: latest is readable",
                "BACKWARD",
                &[v1.as_str(), v3.as_str(), v2.as_str()][..],
                &v4,
                true,
            ),
            (
                "backward transitive: every version",
                "BACKWARD_TRANSITIVE",
                &[v1.as_str(), v3.as_str(), v2.as_str()][..],
                &v4,
                false,
            ),
            (
                "forward transitive: v1 must read v3",
                "FORWARD_TRANSITIVE",
                &[v1.as_str(), v2.as_str()][..],
                &v3,
                false,
            ),
            (
                "forward: only v2 must read v3",
                "FORWARD",
                &[v1.as_str(), v2.as_str()][..],
                &v3,
                true,
            ),
        ] {
            let state = state_with(level, existing);
            let ok = check_registration(&state, "s", candidate(new)).is_ok();
            assert!(ok == compatible, "{name}");
        }
        // FULL_TRANSITIVE over [v1, v2] with v3: only v1 reading v3 fails.
        let state = state_with("FULL_TRANSITIVE", &[v1.as_str(), v2.as_str()]);
        let verdict = verdict_for_registration(&state, "s", candidate(&v3));
        assert!(!verdict.is_compatible);
        assert!(verdict.messages.len() == 1);
        // FULL over [v1] with v4: v4 reads v1, but v1 cannot read v4 without x's
        // value... it can, since x has a default only on the reader side and
        // v1 ignores fields it lacks; both directions pass.
        let state = state_with("FULL", &[v1.as_str()]);
        assert!(verdict_for_registration(&state, "s", candidate(&v4)).is_compatible);
    }

    #[test]
    fn registration_errors_carry_the_subject_and_messages() {
        let state = state_with("BACKWARD", &[av(ID).as_str()]);
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        let error = check_registration(&state, "s", candidate(&bad)).unwrap_err();
        let RegistryError::Incompatible { subject, messages } = error else {
            panic!("expected an incompatibility");
        };
        assert!(subject == "s");
        assert!(messages.len() == 1);
    }

    #[test]
    fn version_checks_follow_confluent_for_missing_versions() {
        let good = av(&format!(
            "{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"
        ));
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        let state = state_with("BACKWARD", &[av(ID).as_str()]);
        for (name, subject, schema, version, expected) in [
            ("compatible", "s", &good, None, Ok(true)),
            ("incompatible", "s", &bad, None, Ok(false)),
            (
                "concrete version",
                "s",
                &bad,
                Some(SchemaVersion(1)),
                Ok(false),
            ),
            ("missing subject, latest", "nope", &bad, None, Ok(true)),
            (
                "missing version",
                "s",
                &good,
                Some(SchemaVersion(7)),
                Err(RegistryError::VersionNotFound("7".into())),
            ),
        ] {
            let actual = check_against_version(&state, subject, candidate(schema), version)
                .map(|v| v.is_compatible);
            assert!(actual == expected, "{name}");
        }
        assert!(effective_level(&state, "other") == CompatibilityLevel::Backward);
        assert!(effective_level(&state, "s") == CompatibilityLevel::Backward);
    }
}
