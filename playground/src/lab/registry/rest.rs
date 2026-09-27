//! The Confluent REST surface: one HTTP request in, one response out, over a
//! [`RegistryService`].
//!
//! Routes and error codes follow Confluent Schema Registry. A mutation's
//! response carries the offset of the record it wrote, so the node can hold
//! the response until the record has been read back.

use serde_json::{Value, json};

use super::{
    compat::{self, Candidate},
    error::RegistryError,
    format::{self, SchemaType},
    http::{HttpRequest, HttpResponse},
    ids::{LogOffset, SchemaId, SchemaVersion},
    log::SchemaStore,
    record::SchemaReference,
    service::{RegisterRequest, RegistryService, Written},
    store::StoreState,
};

/// A response and, for a mutation, the record offset it must wait for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub response: HttpResponse,
    pub wait_for: Option<LogOffset>,
}

impl Outcome {
    fn now(response: HttpResponse) -> Self {
        Self {
            response,
            wait_for: None,
        }
    }

    fn after<T>(written: &Written<T>, response: HttpResponse) -> Self {
        Self {
            response,
            wait_for: written.offset,
        }
    }
}

/// A version selector as a path carries it: a concrete version, or the latest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSelector {
    Latest,
    Exact(SchemaVersion),
}

impl VersionSelector {
    /// Parse `latest`, `-1`, or a positive integer.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidVersion`] for anything else.
    pub fn parse(text: &str) -> Result<Self, RegistryError> {
        if text == "latest" || text == "-1" {
            return Ok(Self::Latest);
        }
        match text.parse::<i32>() {
            Ok(v) if v > 0 => Ok(Self::Exact(SchemaVersion(v))),
            _ => Err(RegistryError::InvalidVersion(text.to_string())),
        }
    }

    fn exact(self) -> Option<SchemaVersion> {
        match self {
            Self::Latest => None,
            Self::Exact(v) => Some(v),
        }
    }
}

/// The body of a registration or lookup request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBody {
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub id: Option<SchemaId>,
    pub version: Option<SchemaVersion>,
}

impl SchemaBody {
    /// Parse `{"schema": .., "schemaType"?: .., "references"?: [..],
    /// "id"?: .., "version"?: ..}`.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidRequest`] when the body is not JSON
    /// and [`RegistryError::InvalidSchema`] when it lacks a schema or names
    /// an unknown type.
    pub fn parse(req: &HttpRequest) -> Result<Self, RegistryError> {
        let body: Value = serde_json::from_slice(&req.body)
            .map_err(|e| RegistryError::InvalidRequest(format!("Invalid JSON body: {e}")))?;
        let schema = body
            .get("schema")
            .and_then(Value::as_str)
            .ok_or_else(|| RegistryError::InvalidSchema("Empty schema".to_string()))?;
        let ty_name = body.get("schemaType").and_then(Value::as_str);
        let ty = SchemaType::from_wire(ty_name).ok_or_else(|| {
            RegistryError::InvalidSchema(format!(
                "Invalid schema type {}",
                ty_name.unwrap_or_default()
            ))
        })?;
        let references = match body.get("references") {
            None | Some(Value::Null) => Vec::new(),
            Some(refs) => serde_json::from_value(refs.clone())
                .map_err(|e| RegistryError::InvalidSchema(format!("Invalid references: {e}")))?,
        };
        let int = |key: &str| {
            body.get(key)
                .and_then(Value::as_i64)
                .and_then(|n| i32::try_from(n).ok())
                .filter(|n| *n > 0)
        };
        Ok(Self {
            ty,
            schema: schema.to_string(),
            references,
            id: int("id").map(SchemaId),
            version: int("version").map(SchemaVersion),
        })
    }
}

fn schema_json(
    ty: SchemaType,
    schema: &str,
    references: &[SchemaReference],
) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    if let Some(name) = ty.wire_name() {
        map.insert("schemaType".to_string(), name.into());
    }
    if !references.is_empty() {
        map.insert(
            "references".to_string(),
            serde_json::to_value(references).unwrap_or_default(),
        );
    }
    map.insert("schema".to_string(), schema.into());
    map
}

/// The `{subject, version, id, schemaType?, references?, schema}` shape.
fn version_json(
    subject: &str,
    version: SchemaVersion,
    id: SchemaId,
    ty: SchemaType,
    schema: &str,
    references: &[SchemaReference],
) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("subject".to_string(), subject.into());
    map.insert("version".to_string(), json!(version));
    map.insert("id".to_string(), json!(id));
    map.extend(schema_json(ty, schema, references));
    Value::Object(map)
}

/// Serve one request.
pub fn handle<S: SchemaStore>(service: &mut RegistryService<S>, req: &HttpRequest) -> Outcome {
    match route(service, req) {
        Ok(outcome) => outcome,
        Err(error) => Outcome::now(error.to_response()),
    }
}

fn route<S: SchemaStore>(
    service: &mut RegistryService<S>,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let segments = req.segments();
    let parts: Vec<&str> = segments.iter().map(String::as_str).collect();
    let method = req.method.as_str();
    match parts.as_slice() {
        [] => match method {
            "GET" => Ok(Outcome::now(HttpResponse::ok(&json!({})))),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["subjects", ..] => route_subjects(service, req, &parts),
        ["schemas", ..] => route_schemas(service, req, &parts),
        ["config"] => config(service, None, req),
        ["config", subject] => config(service, Some(subject), req),
        ["mode"] => mode(service, None, req),
        ["mode", subject] => mode(service, Some(subject), req),
        ["compatibility", "subjects", subject, "versions"] => match method {
            "POST" => check_compatibility(service.state(), subject, None, req),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["compatibility", "subjects", subject, "versions", version] => match method {
            "POST" => check_compatibility(service.state(), subject, Some(version), req),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        _ => Err(RegistryError::NotFound),
    }
}

/// The `/subjects/...` routes.
fn route_subjects<S: SchemaStore>(
    service: &mut RegistryService<S>,
    req: &HttpRequest,
    parts: &[&str],
) -> Result<Outcome, RegistryError> {
    let method = req.method.as_str();
    match parts {
        ["subjects"] => match method {
            "GET" => Ok(list_subjects(service.state(), req)),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["subjects", subject] => match method {
            "POST" => lookup(service, subject, req),
            "DELETE" => delete_subject(service, subject, req),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["subjects", subject, "versions"] => match method {
            "GET" => list_versions(service.state(), subject, req),
            "POST" => register(service, subject, req),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["subjects", subject, "versions", version] => match method {
            "GET" => get_version(service.state(), subject, version, req).map(Outcome::now),
            "DELETE" => delete_version(service, subject, version, req),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["subjects", subject, "versions", version, "schema"] => match method {
            "GET" => {
                let found = find_version(service.state(), subject, version, req.flag("deleted"))?;
                Ok(Outcome::now(HttpResponse::raw(found.schema)))
            }
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["subjects", subject, "versions", version, "referencedby"] => match method {
            "GET" => {
                let found = find_version(service.state(), subject, version, req.flag("deleted"))?;
                let ids = service.state().referenced_by(subject, found.version, false);
                Ok(Outcome::now(HttpResponse::ok(&ids)))
            }
            _ => Err(RegistryError::MethodNotAllowed),
        },
        _ => Err(RegistryError::NotFound),
    }
}

/// The `/schemas/...` routes.
fn route_schemas<S: SchemaStore>(
    service: &mut RegistryService<S>,
    req: &HttpRequest,
    parts: &[&str],
) -> Result<Outcome, RegistryError> {
    let method = req.method.as_str();
    match parts {
        ["schemas"] => match method {
            "GET" => Ok(list_schemas(service.state(), req)),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["schemas", "types"] => match method {
            "GET" => Ok(Outcome::now(HttpResponse::ok(
                &SchemaType::ALL.map(SchemaType::name),
            ))),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["schemas", "ids", id] => match method {
            "GET" => get_schema_by_id(service.state(), id, req),
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["schemas", "ids", id, "schema"] => match method {
            "GET" => {
                let id = parse_id(id)?;
                let found = service
                    .state()
                    .schema_by_id(id, req.flag("deleted"))
                    .ok_or(RegistryError::SchemaIdNotFound(id))?;
                Ok(Outcome::now(HttpResponse::raw(found.schema.clone())))
            }
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["schemas", "ids", id, "versions"] => match method {
            "GET" => {
                let id = parse_id(id)?;
                let pairs = service
                    .state()
                    .schema_id_subject_versions(id, req.flag("deleted"));
                if pairs.is_empty() {
                    return Err(RegistryError::SchemaNotFound);
                }
                let rows: Vec<Value> = pairs
                    .into_iter()
                    .map(|(subject, version)| json!({ "subject": subject, "version": version }))
                    .collect();
                Ok(Outcome::now(HttpResponse::ok(&rows)))
            }
            _ => Err(RegistryError::MethodNotAllowed),
        },
        ["schemas", "ids", id, "subjects"] => match method {
            "GET" => {
                let id = parse_id(id)?;
                let pairs = service
                    .state()
                    .schema_id_subject_versions(id, req.flag("deleted"));
                if pairs.is_empty() {
                    return Err(RegistryError::SchemaNotFound);
                }
                let mut subjects: Vec<String> = pairs.into_iter().map(|(s, _)| s).collect();
                subjects.dedup();
                Ok(Outcome::now(HttpResponse::ok(&subjects)))
            }
            _ => Err(RegistryError::MethodNotAllowed),
        },
        _ => Err(RegistryError::NotFound),
    }
}

fn parse_id(text: &str) -> Result<SchemaId, RegistryError> {
    text.parse::<i32>()
        .ok()
        .filter(|n| *n > 0)
        .map(SchemaId)
        .ok_or_else(|| RegistryError::InvalidRequest(format!("Invalid schema id {text}")))
}

// ---- subjects ---------------------------------------------------------------------

fn list_subjects(state: &StoreState, req: &HttpRequest) -> Outcome {
    let mut subjects = if req.flag("deletedOnly") {
        state.deleted_only_subjects()
    } else {
        state.subjects(req.flag("deleted"))
    };
    if let Some(prefix) = req.query("subjectPrefix") {
        subjects.retain(|s| s.starts_with(prefix));
    }
    Outcome::now(HttpResponse::ok(&subjects))
}

/// The schema text a request registers or looks up: normalised on request.
fn effective_schema(
    state: &StoreState,
    body: &SchemaBody,
    normalize: bool,
) -> Result<String, RegistryError> {
    if !normalize {
        return Ok(body.schema.clone());
    }
    let refs = state.resolve_closure(&body.references)?;
    format::normalize(body.ty, &body.schema, &refs)
}

fn lookup<S: SchemaStore>(
    service: &mut RegistryService<S>,
    subject: &str,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let body = SchemaBody::parse(req)?;
    let state = service.state();
    let include_deleted = req.flag("deleted");
    let schema = effective_schema(state, &body, req.flag("normalize"))?;
    let refs = state.resolve_closure(&body.references)?;
    let parsed = format::parse(body.ty, &schema, &refs)?;
    let Some(found) = state.find_under_subject(
        subject,
        body.ty,
        &parsed.storage_form,
        &body.references,
        include_deleted,
    ) else {
        return Err(if state.versions(subject, include_deleted).is_none() {
            RegistryError::SubjectNotFound(subject.to_string())
        } else {
            RegistryError::SchemaNotFound
        });
    };
    let stored = state
        .version(subject, Some(found.version), include_deleted)
        .ok_or(RegistryError::SchemaNotFound)?;
    Ok(Outcome::now(HttpResponse::ok(&version_json(
        subject,
        stored.version,
        stored.id,
        stored.ty,
        &stored.schema,
        &stored.references,
    ))))
}

fn delete_subject<S: SchemaStore>(
    service: &mut RegistryService<S>,
    subject: &str,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let written = if req.flag("permanent") {
        service.permanent_delete_subject(subject)?
    } else {
        service.soft_delete_subject(subject)?
    };
    Ok(Outcome::after(&written, HttpResponse::ok(&written.value)))
}

// ---- versions ------------------------------------------------------------------------

fn list_versions(
    state: &StoreState,
    subject: &str,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let deleted_only = req.flag("deletedOnly");
    let include_deleted = req.flag("deleted") || deleted_only;
    if state.versions(subject, include_deleted).is_none() {
        return Err(RegistryError::SubjectNotFound(subject.to_string()));
    }
    let versions = if deleted_only {
        state.deleted_versions(subject)
    } else {
        state.versions(subject, include_deleted).unwrap_or_default()
    };
    Ok(Outcome::now(HttpResponse::ok(&versions)))
}

fn register<S: SchemaStore>(
    service: &mut RegistryService<S>,
    subject: &str,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let body = SchemaBody::parse(req)?;
    let schema = effective_schema(service.state(), &body, req.flag("normalize"))?;
    let written = service.register(RegisterRequest {
        subject,
        ty: body.ty,
        schema: &schema,
        references: &body.references,
        import_id: body.id,
        import_version: body.version,
    })?;
    Ok(Outcome::after(
        &written,
        HttpResponse::ok(&json!({ "id": written.value.id })),
    ))
}

fn find_version(
    state: &StoreState,
    subject: &str,
    version: &str,
    include_deleted: bool,
) -> Result<super::store::VersionedSchema, RegistryError> {
    let selector = VersionSelector::parse(version)?;
    if state.versions(subject, include_deleted).is_none() {
        return Err(RegistryError::SubjectNotFound(subject.to_string()));
    }
    state
        .version(subject, selector.exact(), include_deleted)
        .ok_or_else(|| RegistryError::VersionNotFound(version.to_string()))
}

fn get_version(
    state: &StoreState,
    subject: &str,
    version: &str,
    req: &HttpRequest,
) -> Result<HttpResponse, RegistryError> {
    let found = find_version(state, subject, version, req.flag("deleted"))?;
    Ok(HttpResponse::ok(&version_json(
        subject,
        found.version,
        found.id,
        found.ty,
        &found.schema,
        &found.references,
    )))
}

fn delete_version<S: SchemaStore>(
    service: &mut RegistryService<S>,
    subject: &str,
    version: &str,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let selector = VersionSelector::parse(version)?;
    let permanent = req.flag("permanent");
    let resolved = match selector {
        VersionSelector::Exact(v) => v,
        VersionSelector::Latest => {
            service
                .state()
                .version(subject, None, permanent)
                .ok_or_else(|| {
                    if service.state().versions(subject, true).is_none() {
                        RegistryError::SubjectNotFound(subject.to_string())
                    } else {
                        RegistryError::VersionNotFound(version.to_string())
                    }
                })?
                .version
        }
    };
    let written = if permanent {
        service.permanent_delete_version(subject, resolved)?
    } else {
        service.soft_delete_version(subject, resolved)?
    };
    Ok(Outcome::after(&written, HttpResponse::ok(&written.value)))
}

// ---- schemas ----------------------------------------------------------------------------

fn get_schema_by_id(
    state: &StoreState,
    id: &str,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let id = parse_id(id)?;
    let include_deleted = req.flag("deleted");
    let found = state
        .schema_by_id(id, include_deleted)
        .ok_or(RegistryError::SchemaIdNotFound(id))?;
    if let Some(subject) = req.query("subject")
        && !state
            .schema_id_subject_versions(id, include_deleted)
            .iter()
            .any(|(s, _)| s == subject)
    {
        return Err(RegistryError::SchemaIdNotFound(id));
    }
    let mut map = schema_json(found.ty, &found.schema, &found.references);
    if req.flag("fetchMaxId") {
        let max = state
            .all_schemas(true)
            .iter()
            .map(|s| s.id)
            .max()
            .unwrap_or_default();
        map.insert("maxId".to_string(), json!(max));
    }
    Ok(Outcome::now(HttpResponse::ok(&Value::Object(map))))
}

fn list_schemas(state: &StoreState, req: &HttpRequest) -> Outcome {
    let mut rows = state.all_schemas(req.flag("deleted"));
    if let Some(prefix) = req.query("subjectPrefix") {
        rows.retain(|s| s.subject.starts_with(prefix));
    }
    if req.flag("latestOnly") {
        let mut latest: Vec<super::store::ListedSchema> = Vec::new();
        for row in rows {
            match latest.last_mut() {
                Some(last) if last.subject == row.subject => *last = row,
                _ => latest.push(row),
            }
        }
        rows = latest;
    }
    let rows: Vec<Value> = rows
        .iter()
        .map(|s| version_json(&s.subject, s.version, s.id, s.ty, &s.schema, &s.references))
        .collect();
    Outcome::now(HttpResponse::ok(&rows))
}

// ---- config and mode --------------------------------------------------------------------

fn config<S: SchemaStore>(
    service: &mut RegistryService<S>,
    subject: Option<&str>,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    match req.method.as_str() {
        "GET" => {
            let state = service.state();
            let level = match subject {
                None => state.global_compat().to_string(),
                Some(s) => match state.subject_compat(s) {
                    Some(level) => level.to_string(),
                    None if req.flag("defaultToGlobal") => state.global_compat().to_string(),
                    None => {
                        return Err(RegistryError::SubjectCompatibilityNotConfigured(
                            s.to_string(),
                        ));
                    }
                },
            };
            Ok(Outcome::now(HttpResponse::ok(
                &json!({ "compatibilityLevel": level }),
            )))
        }
        "PUT" => {
            let body: Value = serde_json::from_slice(&req.body)
                .map_err(|e| RegistryError::InvalidRequest(format!("Invalid JSON body: {e}")))?;
            let level = body
                .get("compatibility")
                .and_then(Value::as_str)
                .and_then(compat::CompatibilityLevel::parse)
                .ok_or(RegistryError::InvalidCompatibilityLevel)?;
            let written = service.set_compat(subject, level.as_str())?;
            Ok(Outcome::after(
                &written,
                HttpResponse::ok(&json!({ "compatibility": level.as_str() })),
            ))
        }
        "DELETE" => match subject {
            None => {
                let written = service.delete_global_compat()?;
                Ok(Outcome::after(&written, HttpResponse::ok(&written.value)))
            }
            Some(s) => {
                let written = service.delete_subject_compat(s)?;
                let Some(level) = written.value.clone() else {
                    return Err(RegistryError::SubjectNotFound(s.to_string()));
                };
                Ok(Outcome::after(&written, HttpResponse::ok(&level)))
            }
        },
        _ => Err(RegistryError::MethodNotAllowed),
    }
}

fn mode<S: SchemaStore>(
    service: &mut RegistryService<S>,
    subject: Option<&str>,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    match req.method.as_str() {
        "GET" => {
            let state = service.state();
            let mode = match subject {
                None => state.global_mode().to_string(),
                Some(s) => match state.subject_mode(s) {
                    Some(mode) => mode.to_string(),
                    None if req.flag("defaultToGlobal") => state.global_mode().to_string(),
                    None => return Err(RegistryError::SubjectModeNotConfigured(s.to_string())),
                },
            };
            Ok(Outcome::now(HttpResponse::ok(&json!({ "mode": mode }))))
        }
        "PUT" => {
            let body: Value = serde_json::from_slice(&req.body)
                .map_err(|e| RegistryError::InvalidRequest(format!("Invalid JSON body: {e}")))?;
            let mode = body
                .get("mode")
                .and_then(Value::as_str)
                .map(str::to_ascii_uppercase)
                .ok_or(RegistryError::InvalidMode)?;
            let written = service.set_mode(subject, &mode, req.flag("force"))?;
            Ok(Outcome::after(
                &written,
                HttpResponse::ok(&json!({ "mode": mode })),
            ))
        }
        "DELETE" => match subject {
            None => {
                let written = service.clear_global_mode();
                Ok(Outcome::after(&written, HttpResponse::ok(&written.value)))
            }
            Some(s) => {
                let written = service.clear_subject_mode(s);
                let Some(mode) = written.value.clone() else {
                    return Err(RegistryError::SubjectNotFound(s.to_string()));
                };
                Ok(Outcome::after(&written, HttpResponse::ok(&mode)))
            }
        },
        _ => Err(RegistryError::MethodNotAllowed),
    }
}

// ---- compatibility ------------------------------------------------------------------------

fn check_compatibility(
    state: &StoreState,
    subject: &str,
    version: Option<&str>,
    req: &HttpRequest,
) -> Result<Outcome, RegistryError> {
    let selector = version.map(VersionSelector::parse).transpose()?;
    let body = SchemaBody::parse(req)?;
    let schema = effective_schema(state, &body, req.flag("normalize"))?;
    let refs = state.resolve_closure(&body.references)?;
    let parsed = format::parse(body.ty, &schema, &refs)?;
    let candidate = Candidate {
        ty: body.ty,
        schema: &parsed.storage_form,
        refs: &refs,
    };
    let verdict = match selector {
        // The whole-subject check: the registration verdict, level and all.
        None => compat::verdict_for_registration(state, subject, candidate),
        Some(selector) => {
            compat::check_against_version(state, subject, candidate, selector.exact())?
        }
    };
    let mut map = serde_json::Map::new();
    map.insert("is_compatible".to_string(), verdict.is_compatible.into());
    if req.flag("verbose") {
        map.insert("messages".to_string(), json!(verdict.messages));
    }
    Ok(Outcome::now(HttpResponse::ok(&Value::Object(map))))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::lab::registry::log::SchemaLog;

    type Registry = RegistryService<SchemaLog>;

    fn registry() -> Registry {
        RegistryService::new(SchemaLog::new(), "BACKWARD", "READWRITE")
    }

    fn av(name: &str) -> String {
        format!(
            "{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{{\"name\":\"{name}\",\"type\":\"int\",\"default\":0}}]}}"
        )
    }

    fn call(service: &mut Registry, method: &str, path: &str, body: Option<Value>) -> HttpResponse {
        let mut req = HttpRequest::new(method, path);
        if let Some(body) = body {
            req = req.with_json(&body);
        }
        let (req, _) = HttpRequest::parse(&req.encode()).unwrap();
        handle(service, &req).response
    }

    fn register(service: &mut Registry, subject: &str, schema: &str) -> HttpResponse {
        call(
            service,
            "POST",
            &format!("/subjects/{subject}/versions"),
            Some(json!({ "schema": schema })),
        )
    }

    #[test]
    fn register_lookup_and_read_back() {
        let mut s = registry();
        let resp = register(&mut s, "orders-value", &av("Order"));
        assert!(resp.status == 200);
        assert!(resp.body_json() == Some(json!({ "id": 1 })));
        let outcome = {
            let (req, _) = HttpRequest::parse(
                &HttpRequest::new("POST", "/subjects/orders-value/versions")
                    .with_json(&json!({ "schema": av("Order2") }))
                    .encode(),
            )
            .unwrap();
            handle(&mut s, &req)
        };
        assert!(outcome.wait_for == Some(LogOffset(1)));
        assert!(
            call(&mut s, "GET", "/subjects/orders-value/versions/1", None).body_json()
                == Some(
                    json!({ "subject": "orders-value", "version": 1, "id": 1, "schema": av("Order") })
                )
        );
        assert!(
            call(
                &mut s,
                "GET",
                "/subjects/orders-value/versions/latest",
                None
            )
            .body_json()
            .unwrap()["version"]
                == 2
        );
        assert!(
            call(
                &mut s,
                "GET",
                "/subjects/orders-value/versions/-1/schema",
                None
            )
            .body
                == av("Order2")
        );
        assert!(
            call(&mut s, "GET", "/schemas/ids/1", None).body_json()
                == Some(json!({ "schema": av("Order") }))
        );
        assert!(call(&mut s, "GET", "/schemas/ids/1/schema", None).body == av("Order"));
        assert!(
            call(&mut s, "GET", "/schemas/ids/2/versions", None).body_json()
                == Some(json!([{ "subject": "orders-value", "version": 2 }]))
        );
        assert!(
            call(&mut s, "GET", "/schemas/ids/2/subjects", None).body_json()
                == Some(json!(["orders-value"]))
        );
        assert!(
            call(&mut s, "GET", "/subjects", None).body_json() == Some(json!(["orders-value"]))
        );
        assert!(
            call(&mut s, "GET", "/subjects/orders-value/versions", None).body_json()
                == Some(json!([1, 2]))
        );
        let lookup = call(
            &mut s,
            "POST",
            "/subjects/orders-value",
            Some(json!({ "schema": av("Order2") })),
        );
        assert!(lookup.body_json().unwrap()["id"] == 2);
        assert!(
            call(&mut s, "GET", "/schemas/types", None).body_json()
                == Some(json!(["JSON", "PROTOBUF", "AVRO"]))
        );
        let all = call(&mut s, "GET", "/schemas?latestOnly=true", None)
            .body_json()
            .unwrap();
        assert!(all.as_array().unwrap().len() == 1);
        assert!(all[0]["id"] == 2);
        assert!(
            call(&mut s, "GET", "/schemas/ids/1?fetchMaxId=true", None)
                .body_json()
                .unwrap()["maxId"]
                == 2
        );
    }

    #[test]
    fn lookup_errors_carry_confluent_codes() {
        let mut s = registry();
        register(&mut s, "s", &av("A"));
        for (name, method, path, body, status, code) in [
            (
                "unknown subject",
                "GET",
                "/subjects/nope/versions",
                None,
                404,
                40401,
            ),
            (
                "unknown version",
                "GET",
                "/subjects/s/versions/9",
                None,
                404,
                40402,
            ),
            (
                "bad version",
                "GET",
                "/subjects/s/versions/zero",
                None,
                422,
                42202,
            ),
            ("unknown id", "GET", "/schemas/ids/9", None, 404, 40403),
            (
                "id versions",
                "GET",
                "/schemas/ids/9/versions",
                None,
                404,
                40403,
            ),
            (
                "invalid schema",
                "POST",
                "/subjects/s/versions",
                Some(json!({ "schema": "{" })),
                422,
                42201,
            ),
            (
                "missing schema",
                "POST",
                "/subjects/s/versions",
                Some(json!({})),
                422,
                42201,
            ),
            (
                "bad type",
                "POST",
                "/subjects/s/versions",
                Some(json!({ "schema": "{}", "schemaType": "THRIFT" })),
                422,
                42201,
            ),
            (
                "lookup miss",
                "POST",
                "/subjects/s",
                Some(json!({ "schema": av("Z") })),
                404,
                40403,
            ),
            (
                "lookup unknown subject",
                "POST",
                "/subjects/nope",
                Some(json!({ "schema": av("Z") })),
                404,
                40401,
            ),
        ] {
            let resp = call(&mut s, method, path, body);
            assert!(resp.status == status, "{name}: {}", resp.body);
            assert!(
                resp.body_json().unwrap()["error_code"] == code,
                "{name}: {}",
                resp.body
            );
        }
    }

    #[test]
    fn config_mode_and_route_errors_carry_confluent_codes() {
        let mut s = registry();
        register(&mut s, "s", &av("A"));
        for (name, method, path, body, status, code) in [
            (
                "bad level",
                "PUT",
                "/config",
                Some(json!({ "compatibility": "SIDEWAYS" })),
                422,
                42203,
            ),
            (
                "bad mode",
                "PUT",
                "/mode",
                Some(json!({ "mode": "SIDEWAYS" })),
                422,
                42204,
            ),
            ("no subject config", "GET", "/config/s", None, 404, 40408),
            ("no subject mode", "GET", "/mode/s", None, 404, 40409),
            (
                "delete missing config",
                "DELETE",
                "/config/s",
                None,
                404,
                40401,
            ),
            ("wrong method", "PUT", "/subjects", None, 405, 405),
            ("unknown route", "GET", "/nope", None, 404, 404),
            ("not json", "POST", "/subjects/s/versions", None, 400, 400),
            (
                "permanent first",
                "DELETE",
                "/subjects/s/versions/1?permanent=true",
                None,
                404,
                40407,
            ),
        ] {
            let resp = call(&mut s, method, path, body);
            assert!(resp.status == status, "{name}: {}", resp.body);
            assert!(
                resp.body_json().unwrap()["error_code"] == code,
                "{name}: {}",
                resp.body
            );
        }
        let incompatible = json!({ "schema": r#"{"type":"record","name":"U","fields":[{"name":"x","type":"int"}]}"# });
        let resp = call(&mut s, "POST", "/subjects/s/versions", Some(incompatible));
        assert!(resp.status == 409);
        assert!(resp.body_json().unwrap()["error_code"] == 409);
    }

    #[test]
    fn deletes_config_and_mode_round_trip() {
        let mut s = registry();
        register(&mut s, "s", &av("A"));
        register(&mut s, "s", &av("B"));
        assert!(call(&mut s, "DELETE", "/subjects/s/versions/1", None).body == "1");
        assert!(call(&mut s, "GET", "/subjects/s/versions?deleted=true", None).body == "[1,2]");
        assert!(call(&mut s, "GET", "/subjects/s/versions?deletedOnly=true", None).body == "[1]");
        assert!(call(&mut s, "GET", "/subjects/s/versions/1?deleted=true", None).status == 200);
        assert!(
            call(
                &mut s,
                "DELETE",
                "/subjects/s/versions/1?permanent=true",
                None
            )
            .body
                == "1"
        );
        assert!(call(&mut s, "DELETE", "/subjects/s/versions/latest", None).body == "2");
        assert!(call(&mut s, "GET", "/subjects?deleted=true", None).body == r#"["s"]"#);
        assert!(call(&mut s, "GET", "/subjects?deletedOnly=true", None).body == r#"["s"]"#);
        assert!(call(&mut s, "GET", "/subjects", None).body == "[]");
        assert!(call(&mut s, "DELETE", "/subjects/s?permanent=true", None).body == "[2]");
        assert!(call(&mut s, "GET", "/subjects?deleted=true", None).body == "[]");

        assert!(
            call(&mut s, "GET", "/config", None).body == r#"{"compatibilityLevel":"BACKWARD"}"#
        );
        assert!(
            call(
                &mut s,
                "PUT",
                "/config",
                Some(json!({ "compatibility": "full" }))
            )
            .body
                == r#"{"compatibility":"FULL"}"#
        );
        assert!(
            call(
                &mut s,
                "PUT",
                "/config/t",
                Some(json!({ "compatibility": "NONE" }))
            )
            .status
                == 200
        );
        assert!(call(&mut s, "GET", "/config/t", None).body == r#"{"compatibilityLevel":"NONE"}"#);
        assert!(
            call(&mut s, "GET", "/config/u?defaultToGlobal=true", None).body
                == r#"{"compatibilityLevel":"FULL"}"#
        );
        assert!(call(&mut s, "DELETE", "/config/t", None).body == r#""NONE""#);
        assert!(call(&mut s, "DELETE", "/config", None).body == r#""FULL""#);
        assert!(
            call(&mut s, "GET", "/config", None).body == r#"{"compatibilityLevel":"BACKWARD"}"#
        );

        assert!(call(&mut s, "GET", "/mode", None).body == r#"{"mode":"READWRITE"}"#);
        assert!(
            call(
                &mut s,
                "PUT",
                "/mode/t",
                Some(json!({ "mode": "readonly" }))
            )
            .body
                == r#"{"mode":"READONLY"}"#
        );
        assert!(call(&mut s, "GET", "/mode/t", None).body == r#"{"mode":"READONLY"}"#);
        assert!(register(&mut s, "t", &av("T")).body_json().unwrap()["error_code"] == 42205);
        assert!(call(&mut s, "DELETE", "/mode/t", None).body == r#""READONLY""#);
        assert!(
            call(&mut s, "GET", "/mode/t?defaultToGlobal=true", None).body
                == r#"{"mode":"READWRITE"}"#
        );
        assert!(call(&mut s, "PUT", "/mode", Some(json!({ "mode": "IMPORT" }))).status == 200);
        let imported = call(
            &mut s,
            "POST",
            "/subjects/i/versions",
            Some(json!({ "schema": av("I"), "id": 77, "version": 3 })),
        );
        assert!(imported.body == r#"{"id":77}"#);
        assert!(call(&mut s, "GET", "/subjects/i/versions", None).body == "[3]");
        assert!(call(&mut s, "DELETE", "/mode", None).body == r#""IMPORT""#);
    }

    #[test]
    fn compatibility_endpoint_reports_the_verdict() {
        let mut s = registry();
        let base = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;
        let bad = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#;
        let good = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int","default":0}]}"#;
        register(&mut s, "s", base);
        let path = "/compatibility/subjects/s/versions/latest";
        assert!(
            call(&mut s, "POST", path, Some(json!({ "schema": good }))).body
                == r#"{"is_compatible":true}"#
        );
        let verbose = call(
            &mut s,
            "POST",
            &format!("{path}?verbose=true"),
            Some(json!({ "schema": bad })),
        )
        .body_json()
        .unwrap();
        assert!(verbose["is_compatible"] == false);
        assert!(verbose["messages"].as_array().unwrap().len() == 1);
        assert!(
            call(
                &mut s,
                "POST",
                "/compatibility/subjects/s/versions",
                Some(json!({ "schema": bad }))
            )
            .body
                == r#"{"is_compatible":false}"#
        );
        assert!(
            call(
                &mut s,
                "POST",
                "/compatibility/subjects/s/versions/9",
                Some(json!({ "schema": bad }))
            )
            .body_json()
            .unwrap()["error_code"]
                == 40402
        );
        assert!(
            call(
                &mut s,
                "POST",
                "/compatibility/subjects/nope/versions/latest",
                Some(json!({ "schema": bad }))
            )
            .body
                == r#"{"is_compatible":true}"#
        );
    }

    #[test]
    fn normalize_registers_the_canonical_text_and_references_resolve() {
        let mut s = registry();
        let spaced = "{ \"type\": \"record\", \"name\": \"A\", \"fields\": [] }";
        assert!(register(&mut s, "a", spaced).body == r#"{"id":1}"#);
        let stored = call(&mut s, "GET", "/subjects/a/versions/1/schema", None).body;
        assert!(stored == spaced);
        let normalized = call(
            &mut s,
            "POST",
            "/subjects/b/versions?normalize=true",
            Some(json!({ "schema": spaced })),
        );
        assert!(normalized.body == r#"{"id":1}"#);
        let dep = json!({
            "schema": r#"{"type":"record","name":"Dep","fields":[{"name":"a","type":"A"}]}"#,
            "references": [{ "name": "A", "subject": "a", "version": 1 }]
        });
        assert!(call(&mut s, "POST", "/subjects/dep/versions", Some(dep)).body == r#"{"id":2}"#);
        assert!(call(&mut s, "GET", "/subjects/a/versions/1/referencedby", None).body == "[2]");
        assert!(
            call(&mut s, "DELETE", "/subjects/a", None)
                .body_json()
                .unwrap()["error_code"]
                == 42206
        );
        let got = call(&mut s, "GET", "/subjects/dep/versions/1", None)
            .body_json()
            .unwrap();
        assert!(got["references"] == json!([{ "name": "A", "subject": "a", "version": 1 }]));
        let pb = json!({ "schemaType": "PROTOBUF", "schema": "syntax = \"proto3\"; message U { int32 id = 1; }" });
        assert!(call(&mut s, "POST", "/subjects/pb/versions", Some(pb)).body == r#"{"id":3}"#);
        let got = call(&mut s, "GET", "/schemas/ids/3", None)
            .body_json()
            .unwrap();
        assert!(got["schemaType"] == "PROTOBUF");
        assert!(got["schema"] == "syntax = \"proto3\";\n\nmessage U {\n  int32 id = 1;\n}\n");
    }
}
