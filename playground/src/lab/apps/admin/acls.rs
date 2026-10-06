//! Provision real Kafka ACL records, then verify every broker before clients run.
use krabka_protocol::owned::{
    create_acls_request::{AclCreation, CreateAclsRequest},
    create_acls_response::CreateAclsResponse,
    delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
    delete_acls_response::DeleteAclsResponse,
    describe_acls_request::DescribeAclsRequest,
    describe_acls_response::DescribeAclsResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::lab::{
    client::{ClientError, KafkaClient, RequestId, Response, Target},
    net::{Ctx, Millis, NodeId},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
    pub acls: Vec<Rule>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub principal: String,
    pub resource_type: String,
    pub resource_name: String,
    pub pattern_type: String,
    pub operation: String,
    pub permission: String,
}

fn code(value: &str, names: &[(&str, i8)]) -> Result<i8, String> {
    names
        .iter()
        .find(|(name, _)| *name == value)
        .map(|(_, n)| *n)
        .ok_or_else(|| format!("unknown ACL value {value:?}"))
}

impl Rule {
    fn creation(&self) -> Result<AclCreation, String> {
        let principal = self
            .principal
            .strip_prefix("User:")
            .ok_or("ACL principal needs User: prefix")?;
        if principal != "*"
            && !principal
                .strip_prefix("node-")
                .and_then(|s| s.strip_suffix("@LAB.KRABKA"))
                .and_then(|s| s.parse::<u32>().ok())
                .is_some_and(|id| {
                    (1..=10000).contains(&id) && principal == format!("node-{id}@LAB.KRABKA")
                })
        {
            return Err("ACL principal must be User:node-N@LAB.KRABKA or User:*".to_owned());
        }
        if self.resource_name.is_empty() {
            return Err("ACL resource_name cannot be empty".to_owned());
        }
        Ok(AclCreation {
            resource_type: code(
                &self.resource_type,
                &[
                    ("topic", 2),
                    ("group", 3),
                    ("cluster", 4),
                    ("transactional-id", 5),
                ],
            )?,
            resource_name: self.resource_name.clone(),
            resource_pattern_type: code(&self.pattern_type, &[("literal", 3), ("prefixed", 4)])?,
            principal: self.principal.clone(),
            host: "*".to_owned(),
            operation: code(
                &self.operation,
                &[
                    ("all", 2),
                    ("read", 3),
                    ("write", 4),
                    ("create", 5),
                    ("delete", 6),
                    ("alter", 7),
                    ("describe", 8),
                    ("cluster-action", 9),
                    ("describe-configs", 10),
                    ("alter-configs", 11),
                    ("idempotent-write", 12),
                ],
            )?,
            permission_type: code(&self.permission, &[("deny", 2), ("allow", 3)])?,
            ..Default::default()
        })
    }
}

impl Authorization {
    /// # Errors
    /// Rejects unknown fields, invalid principals, duplicate or excessive rules.
    pub fn validate(&self) -> Result<(), String> {
        if self.acls.len() > 128 {
            return Err("the lab supports at most 128 ACL rules".to_owned());
        }
        for (i, rule) in self.acls.iter().enumerate() {
            rule.creation()?;
            if self.acls[..i].contains(rule) {
                return Err("duplicate ACL rule".to_owned());
            }
        }
        Ok(())
    }
}

pub(super) struct Setup {
    authorization: Authorization,
    brokers: Vec<NodeId>,
    stage: usize,
    request: Option<RequestId>,
    retry_at: Millis,
    pub(super) ready: bool,
    error: Option<String>,
}

impl Setup {
    pub(super) fn new(authorization: Authorization, brokers: Vec<NodeId>) -> Self {
        Self {
            authorization,
            brokers,
            stage: 0,
            request: None,
            retry_at: 0,
            ready: false,
            error: None,
        }
    }
    pub(super) fn restart(&mut self) {
        self.stage = 0;
        self.request = None;
        self.retry_at = 0;
        self.ready = false;
        self.error = None;
    }
    pub(super) fn deadline(&self) -> Option<Millis> {
        (!self.ready && self.request.is_none()).then_some(self.retry_at)
    }
    pub(super) fn owns(&self, id: RequestId) -> bool {
        self.request == Some(id)
    }
    pub(super) fn snapshot(&self) -> serde_json::Value {
        json!({"ready":self.ready,"rules":self.authorization.acls,"verified_brokers":self.stage.saturating_sub(2),"error":self.error})
    }
    pub(super) fn drive(&mut self, ctx: &mut Ctx<'_>, client: &mut KafkaClient) {
        if self.ready || self.request.is_some() || ctx.now() < self.retry_at {
            return;
        }
        let request = match self.stage {
            0 => client.send(
                ctx,
                Target::Controller,
                DeleteAclsRequest {
                    filters: vec![DeleteAclsFilter {
                        resource_type_filter: 1,
                        // Generated defaults are empty-string filters; Kafka null means any.
                        resource_name_filter: None,
                        principal_filter: None,
                        host_filter: None,
                        pattern_type_filter: 1,
                        operation: 1,
                        permission_type: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ),
            1 => {
                if self.authorization.acls.is_empty() {
                    self.stage = 2;
                    self.drive(ctx, client);
                    return;
                }
                client.send(
                    ctx,
                    Target::Controller,
                    CreateAclsRequest {
                        creations: self
                            .authorization
                            .acls
                            .iter()
                            .map(|r| r.creation().expect("validated ACL"))
                            .collect(),
                        ..Default::default()
                    },
                )
            }
            n => client.send(
                ctx,
                Target::Broker(self.brokers[n - 2].0.cast_signed()),
                DescribeAclsRequest {
                    resource_type_filter: 1,
                    // Generated defaults are empty-string filters; Kafka null means any.
                    resource_name_filter: None,
                    principal_filter: None,
                    host_filter: None,
                    pattern_type_filter: 1,
                    operation: 1,
                    permission_type: 1,
                    ..Default::default()
                },
            ),
        };
        self.request = Some(request);
    }
    pub(super) fn answer(&mut self, ctx: &mut Ctx<'_>, result: Result<Response, ClientError>) {
        self.request = None;
        let accepted = match result {
            Ok(response) if self.stage == 0 => {
                response.downcast::<DeleteAclsResponse>().is_some_and(|r| {
                    r.filter_results.len() == 1
                        && r.filter_results.iter().all(|f| {
                            f.error_code == 0 && f.matching_acls.iter().all(|a| a.error_code == 0)
                        })
                })
            }
            Ok(response) if self.stage == 1 => {
                response.downcast::<CreateAclsResponse>().is_some_and(|r| {
                    r.results.len() == self.authorization.acls.len()
                        && r.results.iter().all(|a| a.error_code == 0)
                })
            }
            Ok(response) => response
                .downcast::<DescribeAclsResponse>()
                .is_some_and(|r| self.matches(&r)),
            Err(_) => false,
        };
        if accepted {
            self.stage += 1;
            self.error = None;
            if self.stage == self.brokers.len() + 2 {
                self.ready = true;
                ctx.event(
                    "acls_ready",
                    json!({"rules":self.authorization.acls.len(),"brokers":self.brokers}),
                );
            }
        } else {
            self.retry_at = ctx.now() + 500;
            self.error = Some(format!(
                "waiting for ACL stage {} (delete, create, verify brokers)",
                self.stage
            ));
        }
    }
    fn matches(&self, response: &DescribeAclsResponse) -> bool {
        response.error_code == 0
            && response
                .resources
                .iter()
                .map(|r| r.acls.len())
                .sum::<usize>()
                == self.authorization.acls.len()
            && self.authorization.acls.iter().all(|rule| {
                let a = rule.creation().expect("validated ACL");
                response.resources.iter().any(|r| {
                    r.resource_type == a.resource_type
                        && r.resource_name == a.resource_name
                        && r.pattern_type == a.resource_pattern_type
                        && r.acls.iter().any(|b| {
                            b.principal == a.principal
                                && b.host == a.host
                                && b.operation == a.operation
                                && b.permission_type == a.permission_type
                        })
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krabka_protocol::owned::describe_acls_response::{AclDescription, DescribeAclsResource};
    #[test]
    fn verified_acl_set_requires_every_binding_and_rejects_extra_grants() {
        let rule = Rule {
            principal: "User:node-4@LAB.KRABKA".into(),
            resource_type: "topic".into(),
            resource_name: "orders".into(),
            pattern_type: "literal".into(),
            operation: "write".into(),
            permission: "allow".into(),
        };
        let auth = Authorization {
            acls: vec![rule.clone()],
        };
        auth.validate().unwrap();
        let setup = Setup::new(auth, vec![NodeId(1)]);
        let mut response = DescribeAclsResponse {
            resources: vec![DescribeAclsResource {
                resource_type: 2,
                resource_name: "orders".into(),
                pattern_type: 3,
                acls: vec![AclDescription {
                    principal: rule.principal,
                    host: "*".into(),
                    operation: 4,
                    permission_type: 3,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(setup.matches(&response));
        response.resources[0].acls[0].permission_type = 2;
        assert!(!setup.matches(&response));
        response.resources[0].acls[0].permission_type = 3;
        response.resources[0].acls.push(AclDescription::default());
        assert!(!setup.matches(&response));
        let mut invalid = setup.authorization.clone();
        invalid.acls[0].operation = "wriet".into();
        assert!(invalid.validate().is_err());
        invalid.acls[0].operation = "write".into();
        invalid.acls[0].principal = "User:ANONYMOUS".into();
        assert!(invalid.validate().is_err());
    }
}
