//! The Confluent error model: a numeric `error_code`, an HTTP status and a
//! message, serialised as `{"error_code":N,"message":"..."}`.
//!
//! Serdes and the JVM tools branch on `error_code`, so the numbers are exact.
//! They are the `Errors` table of Confluent Schema Registry 7.9; the
//! incompatible-schema code is `409` there (8.0 renamed it `40901`, with the
//! same HTTP status).

use thiserror::Error;

use super::{
    http::HttpResponse,
    ids::{SchemaId, SchemaVersion},
};

/// What a REST request can fail with.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RegistryError {
    /// The path matches no route.
    #[error("HTTP 404 Not Found")]
    NotFound,
    /// The path exists but not for this method.
    #[error("HTTP 405 Method Not Allowed")]
    MethodNotAllowed,
    /// The request is malformed at the HTTP or JSON level.
    #[error("{0}")]
    InvalidRequest(String),
    #[error("Subject '{0}' not found.")]
    SubjectNotFound(String),
    /// The version does not exist under the subject; the text is the version
    /// as the client wrote it (`-1` for `latest`).
    #[error("Version {0} not found.")]
    VersionNotFound(String),
    #[error("Schema not found")]
    SchemaNotFound,
    /// `GET /schemas/ids/{id}` for an unknown id.
    #[error("Schema {0} not found")]
    SchemaIdNotFound(SchemaId),
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    /// A referenced `(subject, version)` does not exist; Confluent reports it
    /// as an invalid schema.
    #[error(
        "Invalid schema: No schema reference found for subject \"{subject}\" and version {version}"
    )]
    ReferenceNotFound {
        subject: String,
        version: SchemaVersion,
    },
    #[error(
        "The specified version '{0}' is not a valid version id. Allowed values are between [1, 2^31-1] and the string \"latest\""
    )]
    InvalidVersion(String),
    #[error(
        "Invalid compatibility level. Valid values are none, backward, forward, full, backward_transitive, forward_transitive, and full_transitive"
    )]
    InvalidCompatibilityLevel,
    #[error("Invalid mode. Valid values are readwrite, readonly, and import")]
    InvalidMode,
    /// The candidate is incompatible with one or more earlier versions; the
    /// messages are the checker's findings, one per difference.
    #[error(
        "Schema being registered is incompatible with an earlier schema for subject \"{subject}\", details: [{}]",
        messages.join(", ")
    )]
    Incompatible {
        subject: String,
        messages: Vec<String>,
    },
    /// A write on a read-only subject, an `IMPORT` switch on a non-empty
    /// registry, or a missing id in `IMPORT` mode.
    #[error("{0}")]
    OperationNotPermitted(String),
    /// `IMPORT` mode: the id is bound to a different schema already.
    #[error("Overwrite new schema with id {0} is not permitted.")]
    SchemaIdConflict(SchemaId),
    #[error("Subject '{0}' was soft deleted. Set permanent=true to delete permanently")]
    SubjectSoftDeleted(String),
    #[error("Subject '{0}' was not deleted first before being permanently deleted")]
    SubjectNotSoftDeleted(String),
    #[error(
        "Subject '{subject}' Version {version} was not deleted first before being permanently deleted"
    )]
    VersionNotSoftDeleted {
        subject: String,
        version: SchemaVersion,
    },
    #[error("Subject '{0}' does not have subject-level compatibility configured")]
    SubjectCompatibilityNotConfigured(String),
    #[error("Subject '{0}' does not have subject-level mode configured")]
    SubjectModeNotConfigured(String),
    /// A delete was blocked because another schema still references the target.
    #[error("One or more references exist to the schema {0}.")]
    ReferencedByOthers(String),
    /// The store did not read a write back.
    #[error("Error in the backend data store: {0}")]
    Backend(String),
}

impl RegistryError {
    /// The Confluent `error_code` of the JSON body.
    #[must_use]
    pub fn error_code(&self) -> i32 {
        match self {
            Self::NotFound => 404,
            Self::MethodNotAllowed => 405,
            Self::InvalidRequest(_) => 400,
            Self::SubjectNotFound(_) => 40401,
            Self::VersionNotFound(_) => 40402,
            Self::SchemaNotFound | Self::SchemaIdNotFound(_) => 40403,
            Self::SubjectSoftDeleted(_) => 40404,
            Self::SubjectNotSoftDeleted(_) => 40405,
            Self::VersionNotSoftDeleted { .. } => 40407,
            Self::SubjectCompatibilityNotConfigured(_) => 40408,
            Self::SubjectModeNotConfigured(_) => 40409,
            Self::Incompatible { .. } => 409,
            Self::InvalidSchema(_) | Self::ReferenceNotFound { .. } => 42201,
            Self::InvalidVersion(_) => 42202,
            Self::InvalidCompatibilityLevel => 42203,
            Self::InvalidMode => 42204,
            Self::OperationNotPermitted(_) | Self::SchemaIdConflict(_) => 42205,
            Self::ReferencedByOthers(_) => 42206,
            Self::Backend(_) => 50001,
        }
    }

    /// The HTTP status of the response.
    #[must_use]
    pub fn status(&self) -> u16 {
        match self {
            Self::InvalidRequest(_) => 400,
            Self::NotFound
            | Self::SubjectNotFound(_)
            | Self::VersionNotFound(_)
            | Self::SchemaNotFound
            | Self::SchemaIdNotFound(_)
            | Self::SubjectSoftDeleted(_)
            | Self::SubjectNotSoftDeleted(_)
            | Self::VersionNotSoftDeleted { .. }
            | Self::SubjectCompatibilityNotConfigured(_)
            | Self::SubjectModeNotConfigured(_) => 404,
            Self::MethodNotAllowed => 405,
            Self::Incompatible { .. } => 409,
            Self::InvalidSchema(_)
            | Self::ReferenceNotFound { .. }
            | Self::InvalidVersion(_)
            | Self::InvalidCompatibilityLevel
            | Self::InvalidMode
            | Self::OperationNotPermitted(_)
            | Self::SchemaIdConflict(_)
            | Self::ReferencedByOthers(_) => 422,
            Self::Backend(_) => 500,
        }
    }

    /// The response Confluent sends for this error.
    #[must_use]
    pub fn to_response(&self) -> HttpResponse {
        HttpResponse::error(self.status(), self.error_code(), self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn codes_and_statuses_follow_the_confluent_table() {
        for (error, code, status) in [
            (RegistryError::NotFound, 404, 404),
            (RegistryError::MethodNotAllowed, 405, 405),
            (RegistryError::InvalidRequest("x".into()), 400, 400),
            (RegistryError::SubjectNotFound("s".into()), 40401, 404),
            (RegistryError::VersionNotFound("3".into()), 40402, 404),
            (RegistryError::SchemaNotFound, 40403, 404),
            (RegistryError::SchemaIdNotFound(SchemaId(9)), 40403, 404),
            (RegistryError::SubjectSoftDeleted("s".into()), 40404, 404),
            (RegistryError::SubjectNotSoftDeleted("s".into()), 40405, 404),
            (
                RegistryError::VersionNotSoftDeleted {
                    subject: "s".into(),
                    version: SchemaVersion(2),
                },
                40407,
                404,
            ),
            (
                RegistryError::SubjectCompatibilityNotConfigured("s".into()),
                40408,
                404,
            ),
            (
                RegistryError::SubjectModeNotConfigured("s".into()),
                40409,
                404,
            ),
            (
                RegistryError::Incompatible {
                    subject: "s".into(),
                    messages: vec![],
                },
                409,
                409,
            ),
            (RegistryError::InvalidSchema("bad".into()), 42201, 422),
            (
                RegistryError::ReferenceNotFound {
                    subject: "s".into(),
                    version: SchemaVersion(1),
                },
                42201,
                422,
            ),
            (RegistryError::InvalidVersion("0".into()), 42202, 422),
            (RegistryError::InvalidCompatibilityLevel, 42203, 422),
            (RegistryError::InvalidMode, 42204, 422),
            (RegistryError::OperationNotPermitted("x".into()), 42205, 422),
            (RegistryError::SchemaIdConflict(SchemaId(1)), 42205, 422),
            (RegistryError::ReferencedByOthers("s:1".into()), 42206, 422),
            (RegistryError::Backend("x".into()), 50001, 500),
        ] {
            assert!(error.error_code() == code, "{error:?}");
            assert!(error.status() == status, "{error:?}");
        }
    }

    #[test]
    fn incompatible_message_lists_the_details_like_a_java_list() {
        let error = RegistryError::Incompatible {
            subject: "orders-value".into(),
            messages: vec!["a".into(), "b".into()],
        };
        assert!(
            error.to_string()
                == "Schema being registered is incompatible with an earlier schema for subject \"orders-value\", details: [a, b]"
        );
        let response = error.to_response();
        assert!(response.status == 409);
        assert!(response.body_json().unwrap()["error_code"] == 409);
    }
}
