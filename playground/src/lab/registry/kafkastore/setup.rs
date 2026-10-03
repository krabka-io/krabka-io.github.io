//! The admin half of the store's startup: Confluent's `kafkaClusterId` and
//! `KafkaStore.createOrVerifySchemaTopic`.
//!
//! The steps, each with its own `kafkastore.init.timeout.ms`, are the
//! requests Confluent's `AdminClient` sends:
//!
//! 1. `DescribeCluster` for the cluster id.
//! 2. `Metadata` for every topic (`listTopics`).
//! 3. When the topic is missing: `DescribeCluster` again, to count the live
//!    brokers, then `CreateTopics` to the controller with 1 partition,
//!    `cleanup.policy=compact`, and a replication factor of
//!    `min(live brokers, kafkastore.topic.replication.factor)`; Confluent
//!    lowers the factor with a warning rather than refuse, and refuses only
//!    when no broker is live. `TOPIC_ALREADY_EXISTS` goes on to step 4.
//! 4. When the topic exists: `DescribeTopicPartitions` (the topic must have
//!    exactly one partition; fewer replicas than desired is a warning), then
//!    `DescribeConfigs` (its `cleanup.policy` must be `compact`).
//!
//! A lost answer, a timeout and a retriable error code send the step's
//! request again after the admin client's backoff (`retry.backoff.ms` 100,
//! doubling to 1 000). Anything else fails the startup with Confluent's
//! message.

use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    create_topics_response::CreateTopicsResponse,
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::DescribeClusterResponse,
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::DescribeConfigsResponse,
    describe_topic_partitions_request::{DescribeTopicPartitionsRequest, TopicRequest},
    describe_topic_partitions_response::DescribeTopicPartitionsResponse,
    metadata_request::MetadataRequest,
    metadata_response::MetadataResponse,
};
use serde_json::json;

use crate::lab::{
    client::{
        ClientError, ClientEvent, KafkaClient, RequestId, Response, Target, error_class,
        exponential_backoff,
    },
    codes,
    net::{Ctx, Millis},
};

/// The admin client's `retry.backoff.ms`.
const RETRY_BACKOFF_MS: Millis = 100;
/// The admin client's `retry.backoff.max.ms`.
const RETRY_BACKOFF_MAX_MS: Millis = 1_000;
/// The admin client's `request.timeout.ms`, which caps the `timeout_ms` of
/// a `CreateTopics`.
const ADMIN_REQUEST_TIMEOUT_MS: i32 = 30_000;
/// `DescribeTopicsOptions.partitionSizeLimitPerResponse`.
const PARTITION_LIMIT: i32 = 2_000;
/// The `resource_type` of a topic in `DescribeConfigs`.
const TOPIC_RESOURCE: i8 = 2;
/// The topic config Confluent sets and checks.
const CLEANUP_POLICY: &str = "cleanup.policy";

/// A step of the admin half of the startup.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Step {
    /// `DescribeCluster` for the cluster id.
    ClusterId,
    /// `Metadata` for every topic.
    ListTopics,
    /// `DescribeCluster` for the live brokers.
    CountBrokers,
    /// `CreateTopics` with this replication factor.
    CreateTopic { replication_factor: i16 },
    /// `DescribeTopicPartitions` of the topic.
    DescribeTopic,
    /// `DescribeConfigs` of the topic.
    DescribeConfigs,
}

impl Step {
    /// The step's name in the snapshot.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ClusterId => "cluster_id",
            Self::ListTopics => "list_topics",
            Self::CountBrokers => "count_brokers",
            Self::CreateTopic { .. } => "create_topic",
            Self::DescribeTopic => "describe_topic",
            Self::DescribeConfigs => "describe_configs",
        }
    }
}

/// What the setup found out about the topic.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TopicLayout {
    pub partitions: usize,
    pub replication_factor: usize,
    pub cleanup_policy: String,
    /// The setup created the topic, rather than found it.
    pub created: bool,
}

/// Where the setup stands after a call.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Progress {
    Working,
    /// The topic exists and is fit for the store.
    Done,
    /// The startup failed, with Confluent's message.
    Failed(String),
}

/// The admin half of the startup. See the module documentation.
pub struct Setup {
    client: KafkaClient,
    topic: String,
    desired_replication_factor: i16,
    init_timeout_ms: Millis,
    step: Step,
    out: Option<RequestId>,
    retry_at: Millis,
    deadline: Millis,
    attempts: u32,
    cluster_id: Option<String>,
    layout: Option<TopicLayout>,
}

impl Setup {
    /// A setup of `topic` over the admin `client`, starting now.
    #[must_use]
    pub fn new(
        client: KafkaClient,
        topic: &str,
        desired_replication_factor: i16,
        init_timeout_ms: Millis,
        now: Millis,
    ) -> Self {
        Self {
            client,
            topic: topic.to_string(),
            desired_replication_factor,
            init_timeout_ms,
            step: Step::ClusterId,
            out: None,
            retry_at: now,
            deadline: now + init_timeout_ms,
            attempts: 0,
            cluster_id: None,
            layout: None,
        }
    }

    /// The admin client, for routing frames.
    #[must_use]
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    /// The admin client.
    pub fn client_mut(&mut self) -> &mut KafkaClient {
        &mut self.client
    }

    #[must_use]
    pub fn step(&self) -> Step {
        self.step
    }

    /// The cluster id `DescribeCluster` reported.
    #[must_use]
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// What the setup found out about the topic, once it did.
    #[must_use]
    pub fn layout(&self) -> Option<&TopicLayout> {
        self.layout.as_ref()
    }

    fn enter(&mut self, step: Step, now: Millis) {
        self.step = step;
        self.out = None;
        self.retry_at = now;
        self.deadline = now + self.init_timeout_ms;
        self.attempts = 0;
    }

    /// Send the step's request again after the admin client's backoff.
    fn retry(&mut self, ctx: &mut Ctx<'_>) {
        self.out = None;
        self.retry_at = ctx.now()
            + exponential_backoff(
                RETRY_BACKOFF_MS,
                RETRY_BACKOFF_MAX_MS,
                self.attempts,
                ctx.rand(400),
            );
        self.attempts += 1;
    }

    /// Retry after a retriable error code, refreshing what the code says is
    /// stale, or fail with `message`.
    fn retry_or_fail(
        &mut self,
        ctx: &mut Ctx<'_>,
        code: i16,
        target: &Target,
        message: String,
    ) -> Progress {
        if error_class(code).is_retriable() {
            self.client.note_error(code, target);
            self.retry(ctx);
            Progress::Working
        } else {
            Progress::Failed(message)
        }
    }

    /// Apply the admin client's events.
    pub fn on_events(&mut self, ctx: &mut Ctx<'_>, events: Vec<ClientEvent>) -> Progress {
        for event in events {
            let ClientEvent::Response { id, result } = event else {
                continue;
            };
            if self.out != Some(id) {
                continue;
            }
            self.out = None;
            let progress = self.on_response(ctx, result);
            if progress != Progress::Working {
                return progress;
            }
        }
        Progress::Working
    }

    fn on_response(
        &mut self,
        ctx: &mut Ctx<'_>,
        result: Result<Response, ClientError>,
    ) -> Progress {
        let Ok(response) = result else {
            self.retry(ctx);
            return Progress::Working;
        };
        match self.step {
            Step::ClusterId | Step::CountBrokers => {
                match response.downcast::<DescribeClusterResponse>() {
                    Some(r) => self.on_describe_cluster(ctx, &r),
                    None => self.retry_and_work(ctx),
                }
            }
            Step::ListTopics => match response.downcast::<MetadataResponse>() {
                Some(r) => self.on_list_topics(ctx, &r),
                None => self.retry_and_work(ctx),
            },
            Step::CreateTopic { replication_factor } => {
                match response.downcast::<CreateTopicsResponse>() {
                    Some(r) => self.on_create_topic(ctx, &r, replication_factor),
                    None => self.retry_and_work(ctx),
                }
            }
            Step::DescribeTopic => match response.downcast::<DescribeTopicPartitionsResponse>() {
                Some(r) => self.on_describe_topic(ctx, &r),
                None => self.retry_and_work(ctx),
            },
            Step::DescribeConfigs => match response.downcast::<DescribeConfigsResponse>() {
                Some(r) => self.on_describe_configs(ctx, &r),
                None => self.retry_and_work(ctx),
            },
        }
    }

    fn retry_and_work(&mut self, ctx: &mut Ctx<'_>) -> Progress {
        self.retry(ctx);
        Progress::Working
    }

    fn on_describe_cluster(
        &mut self,
        ctx: &mut Ctx<'_>,
        response: &DescribeClusterResponse,
    ) -> Progress {
        let now = ctx.now();
        if response.error_code != codes::NONE {
            let message =
                self.failure_message(response.error_code, response.error_message.as_deref());
            return self.retry_or_fail(ctx, response.error_code, &Target::Any, message);
        }
        if self.step == Step::ClusterId {
            self.cluster_id = Some(response.cluster_id.clone());
            self.enter(Step::ListTopics, now);
            return Progress::Working;
        }
        let live = i16::try_from(response.brokers.iter().filter(|b| !b.is_fenced).count())
            .unwrap_or(i16::MAX);
        if live <= 0 {
            return Progress::Failed("No live Kafka brokers".to_string());
        }
        let replication_factor = live.min(self.desired_replication_factor);
        if replication_factor < self.desired_replication_factor {
            ctx.event(
                "kafkastore",
                json!({
                    "step": "create_topic",
                    "level": "warn",
                    "message": format!(
                        "Creating the schema topic {} using a replication factor of {replication_factor}, which is less than the desired one of {}. If this is a production environment, it's crucial to add more brokers and increase the replication factor of the topic.",
                        self.topic, self.desired_replication_factor
                    ),
                }),
            );
        }
        self.enter(Step::CreateTopic { replication_factor }, now);
        Progress::Working
    }

    fn on_list_topics(&mut self, ctx: &mut Ctx<'_>, response: &MetadataResponse) -> Progress {
        let exists = response.topics.iter().any(|t| {
            t.error_code == codes::NONE
                && !t.is_internal
                && t.name.as_deref() == Some(self.topic.as_str())
        });
        let next = if exists {
            Step::DescribeTopic
        } else {
            Step::CountBrokers
        };
        self.enter(next, ctx.now());
        Progress::Working
    }

    fn on_create_topic(
        &mut self,
        ctx: &mut Ctx<'_>,
        response: &CreateTopicsResponse,
        replication_factor: i16,
    ) -> Progress {
        let Some(row) = response.topics.iter().find(|t| t.name == self.topic) else {
            return self.retry_and_work(ctx);
        };
        match row.error_code {
            codes::NONE => {
                let layout = TopicLayout {
                    partitions: 1,
                    replication_factor: usize::try_from(replication_factor).unwrap_or(0),
                    cleanup_policy: "compact".to_string(),
                    created: true,
                };
                ctx.event(
                    "kafkastore",
                    json!({
                        "step": "topic_created",
                        "topic": self.topic,
                        "partitions": layout.partitions,
                        "replication_factor": layout.replication_factor,
                        "level": "info",
                    }),
                );
                self.layout = Some(layout);
                Progress::Done
            }
            codes::TOPIC_ALREADY_EXISTS => {
                self.enter(Step::DescribeTopic, ctx.now());
                Progress::Working
            }
            code => {
                let message = self.failure_message(code, row.error_message.as_deref());
                self.retry_or_fail(ctx, code, &Target::Controller, message)
            }
        }
    }

    fn on_describe_topic(
        &mut self,
        ctx: &mut Ctx<'_>,
        response: &DescribeTopicPartitionsResponse,
    ) -> Progress {
        let Some(row) = response
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(self.topic.as_str()))
        else {
            return self.retry_and_work(ctx);
        };
        if row.error_code != codes::NONE {
            let message = self.failure_message(row.error_code, None);
            return self.retry_or_fail(ctx, row.error_code, &Target::Any, message);
        }
        let partitions = row.partitions.len();
        if partitions != 1 {
            return Progress::Failed(format!(
                "The schema topic {} should have only 1 partition but has {partitions}",
                self.topic
            ));
        }
        let replicas = row.partitions[0].replica_nodes.len();
        if replicas < usize::try_from(self.desired_replication_factor).unwrap_or(0) {
            ctx.event(
                "kafkastore",
                json!({
                    "step": "describe_topic",
                    "level": "warn",
                    "message": format!(
                        "The replication factor of the schema topic {} is less than the desired one of {}. If this is a production environment, it's crucial to add more brokers and increase the replication factor of the topic.",
                        self.topic, self.desired_replication_factor
                    ),
                }),
            );
        }
        self.layout = Some(TopicLayout {
            partitions,
            replication_factor: replicas,
            cleanup_policy: String::new(),
            created: false,
        });
        self.enter(Step::DescribeConfigs, ctx.now());
        Progress::Working
    }

    fn on_describe_configs(
        &mut self,
        ctx: &mut Ctx<'_>,
        response: &DescribeConfigsResponse,
    ) -> Progress {
        let Some(result) = response
            .results
            .iter()
            .find(|r| r.resource_type == TOPIC_RESOURCE && r.resource_name == self.topic)
        else {
            return self.retry_and_work(ctx);
        };
        if result.error_code != codes::NONE {
            let message = self.failure_message(result.error_code, result.error_message.as_deref());
            return self.retry_or_fail(ctx, result.error_code, &Target::Any, message);
        }
        let policy = result
            .configs
            .iter()
            .find(|c| c.name == CLEANUP_POLICY)
            .and_then(|c| c.value.clone());
        if policy.as_deref() != Some("compact") {
            return Progress::Failed(format!(
                "The retention policy of the schema topic {} is incorrect. Expected cleanup.policy to be 'compact' but it is {}",
                self.topic,
                policy.as_deref().unwrap_or("null")
            ));
        }
        if let Some(layout) = &mut self.layout {
            layout.cleanup_policy = "compact".to_string();
        }
        ctx.event(
            "kafkastore",
            json!({ "step": "topic_verified", "topic": self.topic, "level": "info" }),
        );
        Progress::Done
    }

    /// Confluent's message for a step that failed on an error code.
    fn failure_message(&self, code: i16, message: Option<&str>) -> String {
        let context = match self.step {
            Step::ClusterId => "Failed to get Kafka cluster ID",
            _ => "Failed trying to create or validate schema topic configuration",
        };
        match message {
            Some(message) => format!("{context}: error code {code}: {message}"),
            None => format!("{context}: error code {code}"),
        }
    }

    /// Send the step's request when it is due, and fail the step past its
    /// timeout.
    pub fn poll(&mut self, ctx: &mut Ctx<'_>) -> Progress {
        let now = ctx.now();
        if now >= self.deadline {
            return Progress::Failed(match self.step {
                Step::ClusterId => "Failed to get Kafka cluster ID".to_string(),
                _ => {
                    "Timed out trying to create or validate schema topic configuration".to_string()
                }
            });
        }
        if self.out.is_some() || now < self.retry_at {
            return Progress::Working;
        }
        let topic = self.topic.clone();
        let id = match self.step {
            Step::ClusterId | Step::CountBrokers => {
                self.send(ctx, Target::Any, DescribeClusterRequest::default())
            }
            Step::ListTopics => self.send(
                ctx,
                Target::Any,
                MetadataRequest {
                    topics: None,
                    allow_auto_topic_creation: false,
                    ..Default::default()
                },
            ),
            Step::CreateTopic { replication_factor } => self.send(
                ctx,
                Target::Controller,
                CreateTopicsRequest {
                    topics: vec![CreatableTopic {
                        name: topic,
                        num_partitions: 1,
                        replication_factor,
                        assignments: Vec::new(),
                        configs: vec![CreatableTopicConfig {
                            name: CLEANUP_POLICY.to_string(),
                            value: Some("compact".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    timeout_ms: ADMIN_REQUEST_TIMEOUT_MS,
                    validate_only: false,
                    ..Default::default()
                },
            ),
            Step::DescribeTopic => self.send(
                ctx,
                Target::Any,
                DescribeTopicPartitionsRequest {
                    topics: vec![TopicRequest {
                        name: topic,
                        ..Default::default()
                    }],
                    response_partition_limit: PARTITION_LIMIT,
                    cursor: None,
                    ..Default::default()
                },
            ),
            Step::DescribeConfigs => self.send(
                ctx,
                Target::Any,
                DescribeConfigsRequest {
                    resources: vec![DescribeConfigsResource {
                        resource_type: TOPIC_RESOURCE,
                        resource_name: topic,
                        configuration_keys: None,
                        ..Default::default()
                    }],
                    include_synonyms: false,
                    include_documentation: false,
                    ..Default::default()
                },
            ),
        };
        self.out = Some(id);
        Progress::Working
    }

    fn send<R>(&mut self, ctx: &mut Ctx<'_>, target: Target, request: R) -> RequestId
    where
        R: krabka_protocol::ProtocolRequest + 'static,
        R::Response: 'static,
    {
        self.client.send(ctx, target, request)
    }

    /// The next time the setup needs a tick: the admin client's deadline,
    /// the step's retry while no request is out, and the step's timeout.
    #[must_use]
    pub fn next_deadline(&self, now: Millis) -> Option<Millis> {
        let retry = self.out.is_none().then_some(self.retry_at.max(now));
        self.client
            .next_deadline(now)
            .into_iter()
            .chain(retry)
            .chain(Some(self.deadline.max(now)))
            .min()
    }
}
