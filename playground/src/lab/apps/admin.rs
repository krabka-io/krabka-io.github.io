//! The scenario's admin client: a node that creates the scenario's topics
//! with real `CreateTopics` requests, and creates or deletes topics on
//! command from the page.
//!
//! Config: `{ "bootstrap": [node ids], "topics": [TopicSpec...], "observe_ms": 1000 }`. The node
//! connects at start and sends one `CreateTopics` per topic to the controller
//! the metadata names, with `timeout_ms` 30 000, as `kafka-topics --create`
//! does. `NOT_CONTROLLER`, `COORDINATOR_NOT_AVAILABLE`, the other retriable
//! codes and a lost connection make it try again 500 ms later. A scenario
//! topic whose replica count fits its bootstrap brokers also retries
//! `INVALID_REPLICATION_FACTOR` while those brokers register: every 500 ms
//! for the 30 s of `timeout_ms`, then every 5 s, for a broker that starts
//! cut off or down registers only once the reader heals or restarts it.
//!
//! The node also watches the cluster: every `observe_ms` lab ms (default
//! 1 000; 0 turns it off) its [`observer`] asks for the metadata, the
//! brokers, the `KRaft` quorum, the offsets and the groups, and the snapshot's
//! `cluster` field shows the last good answers.
//!
//! Commands: `{"cmd":"create_topic","name":..,"partitions":..,"replication_factor":..}`
//! and `{"cmd":"delete_topic","name":..}` queue the work and answer at once;
//! the snapshot shows the outcome. The operator commands send one request
//! each and answer at once; the outcome is an `admin_done` or `admin_error`
//! event carrying the command:
//!
//! - `{"cmd":"alter_config","resource":"topic"|"broker","name":..,"set":{k:v},"delete":[k]}`:
//!   `IncrementalAlterConfigs`. The inspector's command bar sets one key as
//!   `"config":k,"value":v` instead.
//! - `{"cmd":"describe_config","resource":..,"name":..}`: `DescribeConfigs`;
//!   the result lands in the snapshot's `configs["<resource>:<name>"]`.
//! - `{"cmd":"reassign","topic":..,"partition":..,"replicas":[..]}` and
//!   `{"cmd":"cancel_reassign","topic":..,"partition":..}`:
//!   `AlterPartitionReassignments` (`replicas` may also be text, `"3, 1, 2"`); the observer's
//!   `ListPartitionReassignments` shows the progress in `reassignments`.
//! - `{"cmd":"elect_leaders","type":"preferred"|"unclean","topic":..,"partition":..}`:
//!   `ElectLeaders`, for every partition of the topic when `partition` is
//!   left out.
//! - `{"cmd":"reset_offsets","group":..,"topic":..,"to":"earliest"|"latest"|<offset>}`:
//!   an `OffsetCommit` for every partition of the topic, as
//!   `kafka-consumer-groups --reset-offsets` does. Only a group without
//!   members can be reset; the offsets come from the observer.
//!
//! Snapshot: `{"topics":[{"name","partitions","replication_factor","status":"pending|created|exists|failed|deleting|deleted","error"}],"cluster":{..}|null,"configs":{..},"reassignments":[..],"client":{..}}`.
//! Events: `topics_created` once every topic of the config exists,
//! `topic_created`, `topic_deleted` and `admin_error` per outcome.

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    alter_partition_reassignments_request::{
        AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
    },
    alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
    create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    create_topics_response::CreateTopicsResponse,
    delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    delete_topics_response::DeleteTopicsResponse,
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::DescribeConfigsResponse,
    elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
    elect_leaders_response::ElectLeadersResponse,
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_commit_response::OffsetCommitResponse,
};
use serde_json::{Value, json};

use self::observer::{Observer, error_text};
use crate::lab::{
    LabError,
    client::{
        ClientError, ClientEvent, ClientOptions, CoordinatorType, KafkaClient, OffsetCommitByName,
        RequestId, Response, Target, error_class,
    },
    codes, config_field, config_field_or,
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId},
    scenario::{NodeSpec, TopicSpec},
};

pub mod acls;
pub mod observer;

/// The `timeout_ms` of the admin requests.
const ADMIN_TIMEOUT_MS: i32 = 30_000;
/// The wait before a failed request goes out again.
const RETRY_MS: Millis = 500;
/// The wait between retries of a scenario topic whose brokers have not all
/// registered, once `ADMIN_TIMEOUT_MS` of retries has passed.
const SLOW_RETRY_MS: Millis = 5_000;
/// The default period of the cluster observer.
const OBSERVE_MS: Millis = 1_000;

/// Where a topic of the admin node is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Pending,
    Created,
    Exists,
    Failed,
    Deleting,
    Deleted,
}

impl Status {
    const fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Created => "created",
            Self::Exists => "exists",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }

    const fn is_done(self) -> bool {
        matches!(self, Self::Created | Self::Exists)
    }
}

/// One topic the node manages.
struct Managed {
    spec: TopicSpec,
    status: Status,
    error: Option<i16>,
    request: Option<RequestId>,
    retry_at: Millis,
    attempts: u32,
    /// The topic came from the config, so it counts for `topics_created`.
    from_config: bool,
}

impl Managed {
    /// The wait before the topic's next request after a retriable `code`.
    fn retry_delay(&self, code: i16) -> Millis {
        let waited = u64::from(self.attempts) * RETRY_MS > ADMIN_TIMEOUT_MS as u64;
        if code == codes::INVALID_REPLICATION_FACTOR && waited {
            SLOW_RETRY_MS
        } else {
            RETRY_MS
        }
    }
}

/// The admin node. See the module documentation.
pub struct AdminNode {
    id: NodeId,
    bootstrap: Vec<NodeId>,
    client: KafkaClient,
    topics: BTreeMap<String, Managed>,
    /// `topics_created` was recorded.
    announced: bool,
    observer: Observer,
    /// The operator commands in flight: the command, as the events echo it.
    commands: BTreeMap<RequestId, Value>,
    /// `DescribeConfigs` results by `<resource>:<name>`.
    configs: BTreeMap<String, Value>,
    acls: Option<acls::Setup>,
}

impl AdminNode {
    /// # Errors
    /// Returns a config error when `bootstrap` is missing or empty, or a
    /// topic spec does not parse.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        let bootstrap: Vec<NodeId> = config_field(spec, "bootstrap")?;
        if bootstrap.is_empty() {
            return Err(LabError::config(
                spec,
                "`bootstrap` needs at least one broker",
            ));
        }
        let topics: Vec<TopicSpec> = config_field_or(spec, "topics", Vec::new())?;
        for key in spec
            .config
            .as_object()
            .map(|o| o.keys())
            .into_iter()
            .flatten()
        {
            if !matches!(
                key.as_str(),
                "bootstrap" | "topics" | "observe_ms" | "authorization"
            ) {
                return Err(LabError::config(
                    spec,
                    format!("unknown config field `{key}`"),
                ));
            }
        }
        let observe_ms: Millis = config_field_or(spec, "observe_ms", OBSERVE_MS)?;
        let authorization: Option<acls::Authorization> =
            config_field_or(spec, "authorization", None)?;
        if let Some(auth) = &authorization {
            auth.validate()
                .map_err(|reason| LabError::config(spec, reason))?;
        }
        let acls = authorization.map(|a| acls::Setup::new(a, bootstrap.clone()));
        let client = Self::build_client(&bootstrap, spec.id);
        let observer = Observer::new(&bootstrap, &format!("admin-{}", spec.id), observe_ms);
        Ok(Self {
            id: spec.id,
            bootstrap,
            client,
            topics: topics
                .into_iter()
                .map(|t| {
                    (
                        t.name.clone(),
                        Managed {
                            spec: t,
                            status: Status::Pending,
                            error: None,
                            request: None,
                            retry_at: 0,
                            attempts: 0,
                            from_config: true,
                        },
                    )
                })
                .collect(),
            announced: false,
            observer,
            commands: BTreeMap::new(),
            configs: BTreeMap::new(),
            acls,
        })
    }

    fn build_client(bootstrap: &[NodeId], id: NodeId) -> KafkaClient {
        KafkaClient::new(
            bootstrap.iter().map(|n| Endpoint::kafka(*n)).collect(),
            &format!("admin-{id}"),
            ClientOptions::default(),
        )
    }

    fn drive(&mut self, ctx: &mut Ctx<'_>, events: Vec<ClientEvent>) {
        for event in events {
            if let ClientEvent::Response { id, result } = event {
                if let Some(acls) = &mut self.acls
                    && acls.owns(id)
                {
                    acls.answer(ctx, result);
                } else {
                    self.on_response(ctx, id, result);
                }
            }
        }
        if let Some(acls) = &mut self.acls {
            acls.drive(ctx, &mut self.client);
        }
        self.send_due(ctx);
        self.announce(ctx);
        let deadline = self
            .client
            .next_deadline(ctx.now())
            .into_iter()
            .chain(self.observer.next_deadline(ctx.now()))
            .chain(self.acls.as_ref().and_then(acls::Setup::deadline))
            .chain(
                self.topics
                    .values()
                    .filter(|t| {
                        t.request.is_none()
                            && matches!(t.status, Status::Pending | Status::Deleting)
                    })
                    .map(|t| t.retry_at),
            )
            .min();
        if let Some(at) = deadline {
            ctx.arm(at.max(ctx.now()));
        }
    }

    fn send_due(&mut self, ctx: &mut Ctx<'_>) {
        let now = ctx.now();
        let due: Vec<String> = self
            .topics
            .iter()
            .filter(|(_, t)| {
                t.request.is_none()
                    && matches!(t.status, Status::Pending | Status::Deleting)
                    && now >= t.retry_at
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            let Some(topic) = self.topics.get_mut(&name) else {
                continue;
            };
            topic.attempts += 1;
            let id = if topic.status == Status::Deleting {
                let request = DeleteTopicsRequest {
                    topics: vec![DeleteTopicState {
                        name: Some(name.clone()),
                        ..Default::default()
                    }],
                    topic_names: vec![name.clone()],
                    timeout_ms: ADMIN_TIMEOUT_MS,
                    ..Default::default()
                };
                self.client.send(ctx, Target::Controller, request)
            } else {
                let request = CreateTopicsRequest {
                    topics: vec![CreatableTopic {
                        name: name.clone(),
                        num_partitions: topic.spec.partitions,
                        replication_factor: topic.spec.replication_factor,
                        assignments: Vec::new(),
                        configs: topic
                            .spec
                            .configs
                            .iter()
                            .map(|(k, v)| CreatableTopicConfig {
                                name: k.clone(),
                                value: Some(v.clone()),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    timeout_ms: ADMIN_TIMEOUT_MS,
                    validate_only: false,
                    ..Default::default()
                };
                self.client.send(ctx, Target::Controller, request)
            };
            if let Some(topic) = self.topics.get_mut(&name) {
                topic.request = Some(id);
            }
        }
    }

    fn on_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        id: RequestId,
        result: Result<Response, ClientError>,
    ) {
        if let Some(command) = self.commands.remove(&id) {
            self.on_command_answer(ctx, command, result);
            return;
        }
        let Some(name) = self
            .topics
            .iter()
            .find(|(_, t)| t.request == Some(id))
            .map(|(name, _)| name.clone())
        else {
            return;
        };
        let now = ctx.now();
        let Ok(response) = result else {
            // A lost connection or a timeout: try again after the backoff.
            self.client.request_metadata_refresh();
            if let Some(topic) = self.topics.get_mut(&name) {
                topic.request = None;
                topic.retry_at = now + RETRY_MS;
            }
            return;
        };
        let code = if response.is::<CreateTopicsResponse>() {
            response
                .downcast::<CreateTopicsResponse>()
                .and_then(|r| {
                    r.topics
                        .iter()
                        .find(|t| t.name == name)
                        .map(|t| t.error_code)
                })
                .unwrap_or(codes::UNKNOWN_SERVER_ERROR)
        } else {
            response
                .downcast::<DeleteTopicsResponse>()
                .and_then(|r| {
                    r.responses
                        .iter()
                        .find(|t| t.name.as_deref() == Some(name.as_str()))
                        .map(|t| t.error_code)
                })
                .unwrap_or(codes::UNKNOWN_SERVER_ERROR)
        };
        let Some(topic) = self.topics.get_mut(&name) else {
            return;
        };
        topic.request = None;
        let deleting = topic.status == Status::Deleting;
        match code {
            codes::NONE => {
                topic.status = if deleting {
                    Status::Deleted
                } else {
                    Status::Created
                };
                topic.error = None;
                ctx.event(
                    if deleting {
                        "topic_deleted"
                    } else {
                        "topic_created"
                    },
                    json!({ "topic": name, "partitions": topic.spec.partitions }),
                );
            }
            codes::TOPIC_ALREADY_EXISTS if !deleting => {
                topic.status = Status::Exists;
                topic.error = None;
                ctx.event("topic_created", json!({ "topic": name, "existed": true }));
            }
            codes::UNKNOWN_TOPIC_OR_PARTITION if deleting => {
                topic.status = Status::Deleted;
                topic.error = None;
                ctx.event("topic_deleted", json!({ "topic": name, "existed": false }));
            }
            code if error_class(code).is_retriable()
                || code == codes::NOT_CONTROLLER
                || (code == codes::INVALID_REPLICATION_FACTOR
                    && !deleting
                    && topic.from_config
                    && topic.spec.replication_factor > 0
                    && usize::try_from(topic.spec.replication_factor)
                        .is_ok_and(|replicas| replicas <= self.bootstrap.len())) =>
            {
                self.client.note_error(code, &Target::Controller);
                topic.retry_at = now + topic.retry_delay(code);
                topic.error = Some(code);
                ctx.event(
                    "admin_retry",
                    json!({ "topic": name, "code": code, "level": "warn" }),
                );
            }
            code => {
                topic.status = Status::Failed;
                topic.error = Some(code);
                ctx.event(
                    "admin_error",
                    json!({ "topic": name, "code": code, "level": "error" }),
                );
            }
        }
    }

    fn announce(&mut self, ctx: &mut Ctx<'_>) {
        if self.announced {
            return;
        }
        let from_config: Vec<&Managed> = self.topics.values().filter(|t| t.from_config).collect();
        if from_config.is_empty() || !from_config.iter().all(|t| t.status.is_done()) {
            return;
        }
        self.announced = true;
        let names: Vec<&str> = from_config.iter().map(|t| t.spec.name.as_str()).collect();
        ctx.event("topics_created", json!({ "topics": names }));
    }
}

// ---- operator commands ----------------------------------------------------------

/// A string field of a command.
fn text<'a>(command: &'a Value, key: &str) -> Result<&'a str, String> {
    command
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing `{key}`"))
}

/// An `i32` field of a command, or `None` when it is absent.
fn int(command: &Value, key: &str) -> Result<Option<i32>, String> {
    match command.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| format!("`{key}` must be an integer")),
    }
}

/// The `resource_type` of a config resource: Kafka's `ConfigResource.Type`.
fn resource_type(resource: &str) -> Result<i8, String> {
    match resource {
        "topic" => Ok(2),
        "broker" => Ok(4),
        other => Err(format!("unknown resource {other:?}; use topic or broker")),
    }
}

/// Where a config request goes: a broker's own configs to that broker, as
/// Kafka's admin client sends them, a topic's to any broker.
fn config_target(resource: &str, name: &str) -> Result<Target, String> {
    if resource == "broker" {
        name.parse::<i32>()
            .map(Target::Broker)
            .map_err(|_| format!("broker {name:?} is not a broker id"))
    } else {
        Ok(Target::Any)
    }
}

/// The `IncrementalAlterConfigs` of an `alter_config` command and where it
/// goes.
fn alter_config(command: &Value) -> Result<(Target, IncrementalAlterConfigsRequest), String> {
    let resource = text(command, "resource")?;
    let name = text(command, "name")?;
    let mut configs: Vec<AlterableConfig> = Vec::new();
    for (key, value) in command
        .get("set")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let value = match value {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        configs.push(AlterableConfig {
            name: key.clone(),
            config_operation: 0,
            value: Some(value),
            ..Default::default()
        });
    }
    for key in command
        .get("delete")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        configs.push(AlterableConfig {
            name: key.to_string(),
            config_operation: 1,
            value: None,
            ..Default::default()
        });
    }
    // The inspector's command bar sends one key as `config` and
    // `value`.
    if let (Some(key), Some(value)) = (
        command.get("config").and_then(Value::as_str),
        command.get("value").and_then(Value::as_str),
    ) {
        configs.push(AlterableConfig {
            name: key.to_string(),
            config_operation: 0,
            value: Some(value.to_string()),
            ..Default::default()
        });
    }
    if configs.is_empty() {
        return Err("nothing to change: give `set` or `delete`".to_string());
    }
    let request = IncrementalAlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: resource_type(resource)?,
            resource_name: name.to_string(),
            configs,
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    Ok((config_target(resource, name)?, request))
}

/// The first error of an answer's `(code, message)` rows, as the event's
/// `code` and `message`.
fn first_error(rows: impl IntoIterator<Item = (i16, Option<String>)>) -> Result<(), (i16, String)> {
    match rows.into_iter().find(|(code, _)| *code != codes::NONE) {
        Some((code, message)) => Err((code, error_text(code, message))),
        None => Ok(()),
    }
}

impl AdminNode {
    fn operator_command(
        &mut self,
        ctx: &mut Ctx<'_>,
        cmd: &str,
        command: &Value,
    ) -> Result<Value, String> {
        let id = match cmd {
            "alter_config" => {
                let (target, request) = alter_config(command)?;
                self.client.send(ctx, target, request)
            }
            "describe_config" => {
                let resource = text(command, "resource")?;
                let name = text(command, "name")?;
                let request = DescribeConfigsRequest {
                    resources: vec![DescribeConfigsResource {
                        resource_type: resource_type(resource)?,
                        resource_name: name.to_string(),
                        configuration_keys: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                self.client
                    .send(ctx, config_target(resource, name)?, request)
            }
            "reassign" | "cancel_reassign" => {
                let topic = text(command, "topic")?;
                let partition = int(command, "partition")?.ok_or("missing `partition`")?;
                let replicas = if cmd == "reassign" {
                    // A list of broker ids, or the command bar's "3, 1, 2".
                    let replicas: Vec<i32> = match command.get("replicas") {
                        Some(Value::String(list)) => list
                            .split(',')
                            .filter_map(|id| id.trim().parse().ok())
                            .collect(),
                        other => other
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()))
                            .collect(),
                    };
                    if replicas.is_empty() {
                        return Err("missing `replicas`".to_string());
                    }
                    Some(replicas)
                } else {
                    None
                };
                self.client.send(
                    ctx,
                    Target::Controller,
                    reassignment(&[(topic.to_string(), partition, replicas)]),
                )
            }
            "elect_leaders" => {
                let election_type = match command.get("type").and_then(Value::as_str) {
                    None | Some("preferred") => 0,
                    Some("unclean") => 1,
                    Some(other) => return Err(format!("unknown election type {other:?}")),
                };
                let topic = text(command, "topic")?;
                let partitions = match int(command, "partition")? {
                    Some(p) => vec![p],
                    None => self
                        .observer
                        .view()
                        .partitions_of(topic)
                        .ok_or_else(|| format!("topic {topic:?} is not known yet"))?,
                };
                self.client.send(
                    ctx,
                    Target::Controller,
                    elect_leaders(election_type, &[(topic.to_string(), partitions)]),
                )
            }
            _ => self.reset_offsets(ctx, command)?,
        };
        self.commands.insert(id, command.clone());
        self.drive(ctx, Vec::new());
        Ok(json!({ "cmd": cmd, "status": "sent" }))
    }

    fn reset_offsets(&mut self, ctx: &mut Ctx<'_>, command: &Value) -> Result<RequestId, String> {
        let group = text(command, "group")?;
        let topic = text(command, "topic")?;
        let view = self.observer.view();
        if let Some(members) = view.members_of(group).filter(|m| *m > 0) {
            return Err(format!(
                "group {group:?} has {members} member(s); stop them before resetting its offsets"
            ));
        }
        let partitions = view
            .offsets_of(topic)
            .ok_or_else(|| format!("topic {topic:?} is not known yet"))?;
        let to = command.get("to").cloned().unwrap_or(Value::Null);
        let mut offsets = Vec::new();
        for (partition, log_start, hwm) in partitions {
            let offset = match &to {
                Value::String(s) if s == "earliest" => log_start,
                Value::String(s) if s == "latest" => hwm,
                Value::Number(n) => n.as_i64(),
                _ => return Err("`to` must be earliest, latest or an offset".to_string()),
            }
            .ok_or_else(|| format!("the offsets of {topic}-{partition} are not known yet"))?;
            offsets.push(OffsetCommitRequestPartition {
                partition_index: partition,
                committed_offset: offset,
                committed_leader_epoch: -1,
                committed_metadata: Some(String::new()),
                ..Default::default()
            });
        }
        let request = OffsetCommitByName(OffsetCommitRequest {
            group_id: group.to_string(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: topic.to_string(),
                partitions: offsets,
                ..Default::default()
            }],
            ..Default::default()
        });
        let target = Target::Coordinator {
            key_type: CoordinatorType::Group,
            key: group.to_string(),
        };
        Ok(self.client.send(ctx, target, request))
    }

    /// An operator command's request ended: an `admin_done` or an
    /// `admin_error` event that carries the command.
    fn on_command_answer(
        &mut self,
        ctx: &mut Ctx<'_>,
        command: Value,
        result: Result<Response, ClientError>,
    ) {
        let outcome = match result {
            Err(error) => Err((codes::UNKNOWN_SERVER_ERROR, error.to_string())),
            Ok(response) => self.command_outcome(&command, response),
        };
        let mut detail = command;
        match outcome {
            Ok(()) => {
                ctx.event("admin_done", detail);
                // A changed cluster is worth a look at once.
                self.observer.poke(ctx.now());
            }
            Err((code, message)) => {
                if let Some(fields) = detail.as_object_mut() {
                    fields.insert("code".to_string(), json!(code));
                    fields.insert("message".to_string(), json!(message));
                    fields.insert("level".to_string(), json!("error"));
                }
                ctx.event("admin_error", detail);
            }
        }
    }

    /// What an operator command's answer says: nothing, or its first error.
    /// A `DescribeConfigs` result goes to `configs`.
    fn command_outcome(
        &mut self,
        command: &Value,
        response: Response,
    ) -> Result<(), (i16, String)> {
        let unexpected = || (codes::UNKNOWN_SERVER_ERROR, "unexpected answer".to_string());
        if response.is::<IncrementalAlterConfigsResponse>() {
            let r = response
                .downcast::<IncrementalAlterConfigsResponse>()
                .ok_or_else(unexpected)?;
            first_error(
                r.responses
                    .into_iter()
                    .map(|x| (x.error_code, x.error_message)),
            )
        } else if response.is::<DescribeConfigsResponse>() {
            let r = response
                .downcast::<DescribeConfigsResponse>()
                .ok_or_else(unexpected)?;
            let result = r.results.into_iter().next().ok_or_else(unexpected)?;
            first_error([(result.error_code, result.error_message)])?;
            let configs: serde_json::Map<String, Value> = result
                .configs
                .into_iter()
                .map(|c| (c.name, json!(c.value)))
                .collect();
            let key = format!(
                "{}:{}",
                command["resource"].as_str().unwrap_or(""),
                command["name"].as_str().unwrap_or("")
            );
            self.configs.insert(key, Value::Object(configs));
            Ok(())
        } else if response.is::<AlterPartitionReassignmentsResponse>() {
            let r = response
                .downcast::<AlterPartitionReassignmentsResponse>()
                .ok_or_else(unexpected)?;
            first_error(reassignment_errors(r))
        } else if response.is::<ElectLeadersResponse>() {
            let r = response
                .downcast::<ElectLeadersResponse>()
                .ok_or_else(unexpected)?;
            first_error(election_errors(r))
        } else if response.is::<OffsetCommitResponse>() {
            let r = response
                .downcast::<OffsetCommitResponse>()
                .ok_or_else(unexpected)?;
            first_error(
                r.topics
                    .into_iter()
                    .flat_map(|t| t.partitions)
                    .map(|p| (p.error_code, None)),
            )
        } else {
            Err(unexpected())
        }
    }
}

/// An `AlterPartitionReassignments` for `(topic, partition, replicas)` rows;
/// `None` replicas cancel the partition's reassignment.
#[must_use]
pub fn reassignment(
    rows: &[(String, i32, Option<Vec<i32>>)],
) -> AlterPartitionReassignmentsRequest {
    let mut topics: BTreeMap<&str, Vec<ReassignablePartition>> = BTreeMap::new();
    for (topic, partition, replicas) in rows {
        topics
            .entry(topic.as_str())
            .or_default()
            .push(ReassignablePartition {
                partition_index: *partition,
                replicas: replicas.clone(),
                ..Default::default()
            });
    }
    AlterPartitionReassignmentsRequest {
        timeout_ms: ADMIN_TIMEOUT_MS,
        allow_replication_factor_change: true,
        topics: topics
            .into_iter()
            .map(|(name, partitions)| ReassignableTopic {
                name: name.to_string(),
                partitions,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The `(code, message)` rows of an `AlterPartitionReassignments` answer.
#[must_use]
pub fn reassignment_errors(r: AlterPartitionReassignmentsResponse) -> Vec<(i16, Option<String>)> {
    std::iter::once((r.error_code, r.error_message))
        .chain(
            r.responses
                .into_iter()
                .flat_map(|t| t.partitions)
                .map(|p| (p.error_code, p.error_message)),
        )
        .collect()
}

/// An `ElectLeaders` of `election_type` (0 preferred, 1 unclean) for
/// `(topic, partitions)` rows.
#[must_use]
pub fn elect_leaders(election_type: i8, rows: &[(String, Vec<i32>)]) -> ElectLeadersRequest {
    ElectLeadersRequest {
        election_type,
        topic_partitions: Some(
            rows.iter()
                .map(|(topic, partitions)| TopicPartitions {
                    topic: topic.clone(),
                    partitions: partitions.clone(),
                    ..Default::default()
                })
                .collect(),
        ),
        timeout_ms: ADMIN_TIMEOUT_MS,
        ..Default::default()
    }
}

/// The `(code, message)` rows of an `ElectLeaders` answer. A partition its
/// preferred replica leads already needs no election, which is no failure.
#[must_use]
pub fn election_errors(r: ElectLeadersResponse) -> Vec<(i16, Option<String>)> {
    std::iter::once((r.error_code, None))
        .chain(
            r.replica_election_results
                .into_iter()
                .flat_map(|t| t.partition_result)
                .filter(|p| p.error_code != codes::ELECTION_NOT_NEEDED)
                .map(|p| (p.error_code, p.error_message)),
        )
        .collect()
}

impl Node for AdminNode {
    fn kind(&self) -> &'static str {
        "admin"
    }

    fn start(&mut self, ctx: &mut Ctx<'_>) {
        self.client = Self::build_client(&self.bootstrap, self.id);
        self.observer.restart(&self.bootstrap);
        self.commands.clear();
        if let Some(acls) = &mut self.acls {
            acls.restart();
        }
        for topic in self.topics.values_mut() {
            topic.request = None;
            topic.retry_at = 0;
            if matches!(topic.status, Status::Failed) {
                topic.status = Status::Pending;
            }
        }
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }

    fn on_frame(&mut self, ctx: &mut Ctx<'_>, frame: Frame) {
        if self.observer.owns(&frame) {
            self.observer.on_frame(ctx, frame);
            self.drive(ctx, Vec::new());
            return;
        }
        let events = self.client.on_frame(ctx, frame);
        self.drive(ctx, events);
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_>) {
        self.observer.on_tick(ctx);
        let (events, _) = self.client.on_tick(ctx);
        self.drive(ctx, events);
    }

    fn control(&mut self, ctx: &mut Ctx<'_>, command: Value) -> Result<Value, String> {
        let cmd = command.get("cmd").and_then(Value::as_str).unwrap_or("");
        if matches!(
            cmd,
            "alter_config"
                | "describe_config"
                | "reassign"
                | "cancel_reassign"
                | "elect_leaders"
                | "reset_offsets"
        ) {
            let cmd = cmd.to_string();
            return self.operator_command(ctx, &cmd, &command);
        }
        let name = command
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "missing `name`".to_string())?
            .to_string();
        match command.get("cmd").and_then(Value::as_str) {
            Some("create_topic") => {
                let partitions = command
                    .get("partitions")
                    .and_then(Value::as_i64)
                    .and_then(|p| i32::try_from(p).ok())
                    .unwrap_or(1);
                let replication_factor = command
                    .get("replication_factor")
                    .and_then(Value::as_i64)
                    .and_then(|r| i16::try_from(r).ok())
                    .unwrap_or(-1);
                self.topics.insert(
                    name.clone(),
                    Managed {
                        spec: TopicSpec {
                            name: name.clone(),
                            partitions,
                            replication_factor,
                            configs: BTreeMap::new(),
                        },
                        status: Status::Pending,
                        error: None,
                        request: None,
                        retry_at: 0,
                        attempts: 0,
                        from_config: false,
                    },
                );
                self.drive(ctx, Vec::new());
                Ok(json!({ "topic": name, "status": "pending" }))
            }
            Some("delete_topic") => {
                let topic = self.topics.entry(name.clone()).or_insert_with(|| Managed {
                    spec: TopicSpec {
                        name: name.clone(),
                        partitions: 1,
                        replication_factor: -1,
                        configs: BTreeMap::new(),
                    },
                    status: Status::Deleting,
                    error: None,
                    request: None,
                    retry_at: 0,
                    attempts: 0,
                    from_config: false,
                });
                topic.status = Status::Deleting;
                topic.request = None;
                topic.retry_at = 0;
                self.drive(ctx, Vec::new());
                Ok(json!({ "topic": name, "status": "deleting" }))
            }
            other => Err(format!("unknown admin command {other:?}")),
        }
    }

    fn snapshot(&self) -> Value {
        let topics: Vec<Value> = self
            .topics
            .values()
            .map(|t| {
                json!({
                    "name": t.spec.name,
                    "partitions": t.spec.partitions,
                    "replication_factor": t.spec.replication_factor,
                    "status": t.status.name(),
                    "error": t.error,
                    "attempts": t.attempts,
                })
            })
            .collect();
        json!({
            "topics": topics,
            "authorization": self.acls.as_ref().map(acls::Setup::snapshot),
            "cluster": self.observer.snapshot(),
            "configs": self.configs,
            "reassignments": self.observer.reassignments(),
            "client": self.client.snapshot(),
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    use assert2::assert;
    use krabka_protocol::{
        ProtocolRequest,
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        },
        primitives::uuid::Uuid,
    };
    use serde_json::{Value, json};

    use super::AdminNode;
    use crate::lab::{
        LabError,
        client::fake_broker::{ClusterState, FakeBroker, FakeMember, GroupState, Seen},
        codes,
        net::{Frame, Millis, Node, NodeId, TimedFrame},
        scenario::{NodeSpec, Scenario},
        testing::CtxBuffers,
        world::World,
    };

    /// The node under test, admin or rebalancer.
    pub(in crate::lab::apps) const ADMIN: NodeId = NodeId(10);

    /// The link latency between the admin and the brokers, each way.
    const LATENCY: Millis = 5;

    /// A world that hosts only the admin node, with three fake brokers
    /// outside it: frames for a broker leave through the egress, as they
    /// would for a broker another tab hosts, reach it at their `deliver_at`,
    /// and the answers come back through the ingress one link latency later.
    pub(in crate::lab::apps) struct Remote {
        pub world: World,
        brokers: BTreeMap<NodeId, (FakeBroker, CtxBuffers)>,
        pub state: Rc<RefCell<ClusterState>>,
        /// Frames on their way to a broker.
        outbound: Vec<TimedFrame>,
        /// Answers on their way back.
        inbound: Vec<Answer>,
    }

    /// A broker's answer on its way back, with its arrival time.
    type Answer = (Millis, Frame);

    impl Remote {
        fn new(config: Value) -> Self {
            Self::with_node("admin", config)
        }

        /// The harness around a node of `kind` with `config`.
        pub fn with_node(kind: &str, config: Value) -> Self {
            let state = ClusterState::new(&[(1, 1), (2, 2), (3, 3)]);
            let scenario = Scenario {
                nodes: vec![NodeSpec::new(ADMIN.0, kind, kind, config)],
                ..Scenario::empty(7)
            };
            let world = World::from_scenario_hosted(&scenario, &[ADMIN]).unwrap();
            let brokers = (1..=3)
                .map(|id| {
                    let node = NodeId(id);
                    let broker_id = i32::try_from(id).unwrap();
                    let mut broker = FakeBroker::new(node, broker_id, Rc::clone(&state));
                    let mut bufs = CtxBuffers::new(node);
                    bufs.with(0, |ctx| broker.start(ctx));
                    (node, (broker, bufs))
                })
                .collect();
            Self {
                world,
                brokers,
                state,
                outbound: Vec::new(),
                inbound: Vec::new(),
            }
        }

        /// Advance `ms` of logical time a millisecond at a time, carrying
        /// the frames both ways after each step.
        pub fn run_for(&mut self, ms: Millis) {
            let end = self.world.now() + ms;
            while self.world.now() < end {
                let next = self.world.now() + 1;
                self.world.step_until(next);
                self.exchange();
            }
        }

        /// Hand the brokers what reached them by now, and the world the
        /// answers that came back by now.
        fn exchange(&mut self) {
            let now = self.world.now();
            self.outbound.extend(self.world.drain_egress());
            let (due, later): (Vec<TimedFrame>, Vec<TimedFrame>) =
                std::mem::take(&mut self.outbound)
                    .into_iter()
                    .partition(|timed| timed.deliver_at <= now);
            self.outbound = later;
            for timed in due {
                let frame = timed.frame;
                if let Some((broker, bufs)) = self.brokers.get_mut(&frame.dst.node) {
                    bufs.with(now, |ctx| broker.on_frame(ctx, frame));
                    let answers = bufs.take_frames();
                    self.inbound
                        .extend(answers.into_iter().map(|frame| (now + LATENCY, frame)));
                }
            }
            let (arrived, travelling): (Vec<Answer>, Vec<Answer>) =
                std::mem::take(&mut self.inbound)
                    .into_iter()
                    .partition(|(at, _)| *at <= now);
            self.inbound = travelling;
            if !arrived.is_empty() {
                self.world
                    .push_ingress(arrived.into_iter().map(|(_, frame)| frame).collect());
                self.world.step_until(now);
            }
        }

        pub fn snapshot(&self) -> Value {
            self.world.node_snapshot(ADMIN).unwrap()
        }

        /// `(name, status, error, attempts)` of each topic of the snapshot.
        fn statuses(&self) -> Vec<(String, String, Value, u64)> {
            self.snapshot()["topics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| {
                    (
                        t["name"].as_str().unwrap().to_string(),
                        t["status"].as_str().unwrap().to_string(),
                        t["error"].clone(),
                        t["attempts"].as_u64().unwrap(),
                    )
                })
                .collect()
        }

        pub fn events(&self, kind: &str) -> Vec<Value> {
            self.world
                .events()
                .filter(|e| e.kind == kind)
                .map(|e| e.detail.clone())
                .collect()
        }

        fn requests<R>(&self) -> Vec<R>
        where
            R: ProtocolRequest + for<'de> krabka_protocol::Decode<'de>,
        {
            self.state
                .borrow()
                .seen(R::API_KEY)
                .iter()
                .map(Seen::decode)
                .collect()
        }
    }

    fn create(name: &str, partitions: i32, replication_factor: i16) -> CreateTopicsRequest {
        CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: partitions,
                replication_factor,
                ..Default::default()
            }],
            timeout_ms: 30_000,
            validate_only: false,
            ..Default::default()
        }
    }

    fn status(
        name: &str,
        status: &str,
        error: Value,
        attempts: u64,
    ) -> (String, String, Value, u64) {
        (name.to_string(), status.to_string(), error, attempts)
    }

    #[test]
    fn the_admin_creates_the_scenario_topics_through_the_controller() {
        let mut remote = Remote::new(json!({
            "bootstrap": [1],
            "topics": [
                { "name": "orders", "partitions": 3, "replication_factor": 3 },
                { "name": "audit" },
            ],
        }));
        remote.run_for(200);
        // One CreateTopics per topic, v7 with `timeout_ms` 30 000, to the
        // controller the metadata named. The broker picks the replication
        // factor of `audit`.
        assert!(
            remote.requests::<CreateTopicsRequest>()
                == vec![create("audit", 1, -1), create("orders", 3, 3)]
        );
        let seen = remote.state.borrow().seen(19);
        assert!(seen.iter().all(|s| s.version == 7 && s.broker == NodeId(1)));
        let layout: Vec<(String, usize, usize)> = remote
            .state
            .borrow()
            .topics
            .iter()
            .map(|(name, t)| {
                (
                    name.clone(),
                    t.partitions.len(),
                    t.partitions[&0].replicas.len(),
                )
            })
            .collect();
        assert!(layout == vec![("audit".to_string(), 1, 3), ("orders".to_string(), 3, 3)]);
        assert!(
            remote.statuses()
                == vec![
                    status("audit", "created", Value::Null, 1),
                    status("orders", "created", Value::Null, 1),
                ]
        );
        assert!(remote.events("topics_created") == vec![json!({ "topics": ["audit", "orders"] })]);
        assert!(
            remote.events("topic_created")
                == vec![
                    json!({ "topic": "audit", "partitions": 1 }),
                    json!({ "topic": "orders", "partitions": 3 }),
                ]
        );
        assert!(remote.snapshot()["client"]["connections"][0]["state"] == "ready");
    }

    #[test]
    fn the_admin_retries_what_can_succeed_and_reports_the_rest() {
        // `orders` exists already; the first answer for `audit` is
        // NOT_CONTROLLER, which Kafka's admin client retries; `bad` gets
        // INVALID_REPLICATION_FACTOR, which it does not.
        let mut remote = Remote::new(json!({
            "bootstrap": [2],
            "topics": [{ "name": "orders" }, { "name": "audit" }, { "name": "bad" }],
        }));
        {
            let mut state = remote.state.borrow_mut();
            state.add_topic("orders", 1, 3);
            state.knobs.create_topics_errors =
                [codes::NOT_CONTROLLER, codes::INVALID_REPLICATION_FACTOR].into();
        }
        remote.run_for(2_000);
        assert!(
            remote.statuses()
                == vec![
                    status("audit", "created", Value::Null, 2),
                    status("bad", "failed", json!(codes::INVALID_REPLICATION_FACTOR), 1),
                    status("orders", "exists", Value::Null, 1),
                ]
        );
        let times: Vec<Millis> = remote
            .state
            .borrow()
            .seen(19)
            .iter()
            .filter(|s| s.decode::<CreateTopicsRequest>().topics[0].name == "audit")
            .map(|s| s.at)
            .collect();
        // The retry waits 500 ms from the answer, so the two requests reach
        // the broker 500 ms and a round trip apart.
        assert!(times.len() == 2);
        assert!(times[1] - times[0] == 500 + 2 * LATENCY);
        assert!(
            remote.events("admin_retry")
                == vec![
                    json!({ "topic": "audit", "code": codes::NOT_CONTROLLER, "level": "warn" })
                ]
        );
        assert!(
            remote.events("admin_error")
                == vec![json!({
                    "topic": "bad",
                    "code": codes::INVALID_REPLICATION_FACTOR,
                    "level": "error",
                })]
        );
        // A topic failed for good, so the scenario's topics never all exist.
        assert!(remote.events("topics_created").is_empty());

        // A preset can ask for three replicas before all three real brokers
        // have registered. Retry that startup answer, then create the topic.
        let mut remote = Remote::new(json!({
            "bootstrap": [1, 2, 3],
            "topics": [{ "name": "orders", "partitions": 3, "replication_factor": 3 }],
        }));
        remote.state.borrow_mut().knobs.create_topics_errors =
            [codes::INVALID_REPLICATION_FACTOR].into();
        remote.run_for(2_000);
        assert!(remote.statuses() == vec![status("orders", "created", Value::Null, 2)]);

        // A broker that starts cut off registers only when the reader heals
        // it, which can be long after the 30 s budget: the admin keeps
        // asking, every 5 s once the budget is spent.
        let mut remote = Remote::new(json!({
            "bootstrap": [1, 2, 3],
            "topics": [{ "name": "orders", "partitions": 3, "replication_factor": 3 }],
        }));
        remote.state.borrow_mut().knobs.create_topics_errors =
            vec![codes::INVALID_REPLICATION_FACTOR; 70].into();
        remote.run_for(60_000);
        assert!(
            remote.statuses()
                == vec![status(
                    "orders",
                    "pending",
                    json!(codes::INVALID_REPLICATION_FACTOR),
                    66
                )]
        );
        remote.run_for(30_000);
        assert!(remote.statuses() == vec![status("orders", "created", Value::Null, 71)]);
        assert!(remote.events("admin_error").is_empty());
    }

    #[test]
    fn commands_create_and_delete_topics_with_real_requests() {
        let mut remote = Remote::new(json!({ "bootstrap": [1] }));
        remote.run_for(50);
        let answer = remote
            .world
            .control(
                ADMIN,
                json!({ "cmd": "create_topic", "name": "temp", "partitions": 2, "replication_factor": 1 }),
            )
            .unwrap();
        assert!(answer == json!({ "topic": "temp", "status": "pending" }));
        remote.run_for(100);
        assert!(remote.requests::<CreateTopicsRequest>() == vec![create("temp", 2, 1)]);
        assert!(remote.state.borrow().topics["temp"].partitions.len() == 2);
        assert!(remote.statuses() == vec![status("temp", "created", Value::Null, 1)]);

        for name in ["temp", "missing"] {
            let answer = remote
                .world
                .control(ADMIN, json!({ "cmd": "delete_topic", "name": name }))
                .unwrap();
            assert!(answer == json!({ "topic": name, "status": "deleting" }));
        }
        remote.run_for(100);
        let delete = |name: &str| DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(name.to_string()),
                topic_id: Uuid::ZERO,
                ..Default::default()
            }],
            timeout_ms: 30_000,
            ..Default::default()
        };
        let mut deletes = remote.requests::<DeleteTopicsRequest>();
        deletes.sort_by(|a, b| a.topics[0].name.cmp(&b.topics[0].name));
        assert!(deletes == vec![delete("missing"), delete("temp")]);
        assert!(!remote.state.borrow().topics.contains_key("temp"));
        assert!(
            remote.statuses()
                == vec![
                    status("missing", "deleted", Value::Null, 1),
                    status("temp", "deleted", Value::Null, 2),
                ]
        );
        // Each command drives the node at once, so `temp` went first.
        assert!(
            remote.events("topic_deleted")
                == vec![
                    json!({ "topic": "temp", "partitions": 2 }),
                    json!({ "topic": "missing", "existed": false }),
                ]
        );
        let refused = [
            (json!({ "cmd": "create_topic" }), "missing `name`"),
            (
                json!({ "cmd": "rename", "name": "x" }),
                "unknown admin command Some(\"rename\")",
            ),
        ];
        for (command, expected) in refused {
            assert!(remote.world.control(ADMIN, command).unwrap_err() == expected);
        }
    }

    #[test]
    fn the_config_is_checked_at_load() {
        let rows = [
            (json!({}), "missing config field `bootstrap`"),
            (
                json!({ "bootstrap": [] }),
                "`bootstrap` needs at least one broker",
            ),
            (
                json!({ "bootstrap": [1], "topic": [] }),
                "unknown config field `topic`",
            ),
            (
                json!({ "bootstrap": [1], "topics": [{ "name": "t", "partition": 2 }] }),
                "config field `topics`: unknown field `partition`",
            ),
        ];
        for (config, reason) in rows {
            let spec = NodeSpec::new(ADMIN.0, "admin", "admin", config.clone());
            let Err(LabError::Config { reason: actual, .. }) = AdminNode::from_spec(&spec) else {
                panic!("{config} was accepted");
            };
            assert!(actual.starts_with(reason), "{config}: {actual}");
        }
        let spec = NodeSpec::new(ADMIN.0, "admin", "admin", json!({ "bootstrap": [1, 2] }));
        let node = AdminNode::from_spec(&spec).unwrap();
        assert!(node.kind() == "admin");
        assert!(node.snapshot()["topics"] == json!([]));
    }

    /// `(leader, hwm, log_start)` of each partition of `topic` in the
    /// observer's snapshot.
    fn partitions(remote: &Remote, topic: &str) -> Vec<(Value, Value, Value)> {
        let cluster = &remote.snapshot()["cluster"];
        let topic = cluster["topics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == topic)
            .unwrap();
        topic["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                (
                    p["leader"].clone(),
                    p["hwm"].clone(),
                    p["log_start"].clone(),
                )
            })
            .collect()
    }

    fn set_log(remote: &Remote, topic: &str, partition: i32, start: i64, end: i64) {
        let mut state = remote.state.borrow_mut();
        let p = state
            .topics
            .get_mut(topic)
            .unwrap()
            .partitions
            .get_mut(&partition)
            .unwrap();
        p.log_start = start;
        p.log_end = end;
    }

    #[test]
    fn the_observer_reports_the_cluster_every_period() {
        let mut remote = Remote::new(json!({ "bootstrap": [1, 2, 3] }));
        {
            let mut state = remote.state.borrow_mut();
            state.add_topic("orders", 3, 3);
            state.add_topic("__consumer_offsets", 1, 3);
            state.admin.quorum_end = 120;
            let group = state.groups.entry("billing".to_string()).or_default();
            group.state = Some(GroupState::Empty);
            group.committed.insert(("orders".to_string(), 0), (7, 0));
            group.committed.insert(("orders".to_string(), 1), (3, 0));
        }
        set_log(&remote, "orders", 0, 2, 10);
        set_log(&remote, "orders", 1, 0, 5);
        remote.run_for(500);
        let cluster = remote.snapshot()["cluster"].clone();
        assert!(cluster["cluster_id"] == "lab-cluster");
        assert!(
            cluster["brokers"]
                == json!([
                    { "id": 1, "rack": "rack-1", "fenced": false },
                    { "id": 2, "rack": "rack-2", "fenced": false },
                    { "id": 3, "rack": "rack-3", "fenced": false },
                ])
        );
        assert!(cluster["controller"] == 1);
        assert!(
            cluster["quorum"]
                == json!({
                    "leader": 1, "epoch": 1, "high_watermark": 120,
                    "voters": [
                        { "id": 1, "log_end_offset": 120, "lag": 0 },
                        { "id": 2, "log_end_offset": 119, "lag": 1 },
                        { "id": 3, "log_end_offset": 119, "lag": 1 },
                    ],
                    "observers": [],
                })
        );
        let names: Vec<(&str, bool)> = cluster["topics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| {
                (
                    t["name"].as_str().unwrap(),
                    t["internal"].as_bool().unwrap(),
                )
            })
            .collect();
        assert!(names == [("__consumer_offsets", true), ("orders", false)]);
        let orders = &cluster["topics"][1]["partitions"][0];
        assert!(orders["replicas"].as_array().unwrap().len() == 3);
        assert!(orders["isr"] == orders["replicas"]);
        assert!(orders["offline"] == json!([]));
        assert!(
            partitions(&remote, "orders")
                == vec![
                    (json!(1), json!(10), json!(2)),
                    (json!(2), json!(5), json!(0)),
                    (json!(3), json!(0), json!(0)),
                ]
        );
        assert!(
            cluster["groups"]
                == json!([{
                    "id": "billing", "type": "classic", "state": "Empty", "members": 0, "lag": 5,
                    "offsets": [
                        { "topic": "orders", "partition": 0, "committed": 7, "lag": 3 },
                        { "topic": "orders", "partition": 1, "committed": 3, "lag": 2 },
                    ],
                }])
        );
        assert!(cluster["errors"] == json!([]));
        // One round a period, each asking the quorum once at a broker
        // listener: rounds start at 0, 1000 and 2000.
        remote.run_for(2_000);
        assert!(remote.snapshot()["cluster"]["rounds"] == 3);
        assert!(remote.state.borrow().seen(55).len() == 3);
        let observer = remote.state.borrow().seen(55);
        assert!(
            observer
                .iter()
                .all(|s| s.client_id.as_deref() == Some("admin-10"))
        );
    }

    #[test]
    fn a_failed_question_keeps_the_last_good_answer() {
        let mut remote = Remote::new(json!({ "bootstrap": [1, 2, 3], "observe_ms": 500 }));
        remote.state.borrow_mut().add_topic("orders", 3, 3);
        set_log(&remote, "orders", 0, 0, 4);
        set_log(&remote, "orders", 2, 0, 6);
        remote.run_for(400);
        assert!(partitions(&remote, "orders")[2] == (json!(3), json!(6), json!(0)));
        // Broker 3 stops answering; its partition's offsets stay as they
        // were, broker 1's move on, and the round names what failed.
        remote.state.borrow_mut().knobs.silent_brokers.insert(3);
        set_log(&remote, "orders", 0, 0, 8);
        set_log(&remote, "orders", 2, 0, 9);
        remote.run_for(8_000);
        let rows = partitions(&remote, "orders");
        assert!(rows[0] == (json!(1), json!(8), json!(0)));
        assert!(rows[2] == (json!(3), json!(6), json!(0)));
        let errors = remote.snapshot()["cluster"]["errors"].clone();
        assert!(
            errors
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e.as_str().unwrap().contains("from broker 3")),
            "{errors}"
        );
        // `observe_ms` 0 turns the observer off.
        let mut quiet = Remote::new(json!({ "bootstrap": [1], "observe_ms": 0 }));
        quiet.run_for(3_000);
        assert!(quiet.snapshot()["cluster"].is_null());
        assert!(quiet.state.borrow().seen(55).is_empty());
    }

    fn send(remote: &mut Remote, command: &Value) {
        let answer = remote.world.control(ADMIN, command.clone()).unwrap();
        assert!(answer == json!({ "cmd": command["cmd"], "status": "sent" }));
        remote.run_for(100);
    }

    #[test]
    fn operator_commands_send_their_requests_and_report_the_outcome() {
        let mut remote = Remote::new(json!({ "bootstrap": [1, 2, 3] }));
        remote.state.borrow_mut().add_topic("orders", 2, 2);
        set_log(&remote, "orders", 0, 3, 10);
        set_log(&remote, "orders", 1, 1, 4);
        remote
            .state
            .borrow_mut()
            .groups
            .entry("billing".to_string())
            .or_default();
        remote.run_for(300);
        send(
            &mut remote,
            &json!({ "cmd": "alter_config", "resource": "topic", "name": "orders",
                     "set": { "retention.ms": "60000" } }),
        );
        send(
            &mut remote,
            &json!({ "cmd": "describe_config", "resource": "topic", "name": "orders" }),
        );
        assert!(
            remote.snapshot()["configs"] == json!({ "topic:orders": { "retention.ms": "60000" } })
        );
        send(
            &mut remote,
            &json!({ "cmd": "reassign", "topic": "orders", "partition": 0, "replicas": [3, 2] }),
        );
        assert!(remote.state.borrow().topics["orders"].partitions[&0].replicas == vec![3, 2]);
        send(
            &mut remote,
            &json!({ "cmd": "elect_leaders", "type": "preferred", "topic": "orders" }),
        );
        assert!(remote.state.borrow().topics["orders"].partitions[&0].leader == 3);
        send(
            &mut remote,
            &json!({ "cmd": "reset_offsets", "group": "billing", "topic": "orders", "to": "earliest" }),
        );
        let committed = remote.state.borrow().groups["billing"].committed.clone();
        assert!(
            committed
                == [
                    (("orders".to_string(), 0), (3, -1)),
                    (("orders".to_string(), 1), (1, -1))
                ]
                .into()
        );
        // A reassignment the brokers keep in progress shows in the snapshot
        // until it is cancelled.
        remote.state.borrow_mut().admin.hold = true;
        send(
            &mut remote,
            &json!({ "cmd": "reassign", "topic": "orders", "partition": 1, "replicas": [1, 3] }),
        );
        remote.run_for(1_000);
        let current = remote.state.borrow().topics["orders"].partitions[&1]
            .replicas
            .clone();
        assert!(current == vec![2, 3]);
        assert!(
            remote.snapshot()["reassignments"]
                == json!([{ "topic": "orders", "partition": 1, "replicas": [1, 3],
                            "adding": [1], "removing": [2] }])
        );
        send(
            &mut remote,
            &json!({ "cmd": "cancel_reassign", "topic": "orders", "partition": 1 }),
        );
        remote.run_for(1_000);
        assert!(remote.snapshot()["reassignments"] == json!([]));
        let done: Vec<Value> = remote
            .events("admin_done")
            .iter()
            .map(|e| e["cmd"].clone())
            .collect();
        assert!(
            done == [
                "alter_config",
                "describe_config",
                "reassign",
                "elect_leaders",
                "reset_offsets",
                "reassign",
                "cancel_reassign",
            ]
        );
        // A cancel with nothing in progress is the broker's error.
        send(
            &mut remote,
            &json!({ "cmd": "cancel_reassign", "topic": "orders", "partition": 1 }),
        );
        let errors = remote.events("admin_error");
        assert!(errors.len() == 1);
        assert!(errors[0]["code"] == codes::NO_REASSIGNMENT_IN_PROGRESS);
        assert!(errors[0]["cmd"] == "cancel_reassign");

        // A group with members is refused at once, as are bad commands.
        remote
            .state
            .borrow_mut()
            .groups
            .get_mut("billing")
            .unwrap()
            .members
            .insert(
                "m-1".to_string(),
                FakeMember {
                    protocols: Vec::new(),
                    joined: 1,
                    session_timeout_ms: 1_000_000,
                    last_seen: 0,
                    instance_id: None,
                },
            );
        remote.run_for(1_100);
        let refused = [
            (
                json!({ "cmd": "reset_offsets", "group": "billing", "topic": "orders", "to": "latest" }),
                "group \"billing\" has 1 member(s); stop them before resetting its offsets",
            ),
            (
                json!({ "cmd": "alter_config", "resource": "cluster", "name": "x", "set": { "a": "b" } }),
                "unknown resource \"cluster\"; use topic or broker",
            ),
            (
                json!({ "cmd": "elect_leaders", "type": "random", "topic": "orders" }),
                "unknown election type \"random\"",
            ),
            (
                json!({ "cmd": "reassign", "topic": "orders" }),
                "missing `partition`",
            ),
        ];
        for (command, expected) in refused {
            assert!(remote.world.control(ADMIN, command).unwrap_err() == expected);
        }
    }
}
