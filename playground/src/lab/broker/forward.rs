//! Requests the broker hands to the active controller: Kafka's
//! `ForwardingManager` and `AutoTopicCreationManager`.
//!
//! A client may send a controller api (`CreateTopics`, `DeleteTopics`,
//! `CreatePartitions`, `DescribeQuorum`) to any broker. The broker wraps the
//! request, header and body as the client framed them, in an `Envelope`
//! (KIP-590) with the client's principal and address, sends it to the
//! active controller over the `forwarding` channel, and answers the client
//! with the response the envelope brings back, byte for byte. The request
//! stays at the head of its connection meanwhile. Kafka's answers when the
//! forwarding fails: `REQUEST_TIMED_OUT` in the api's error shape when no
//! controller answered within the channel's retry timeout,
//! `UNKNOWN_SERVER_ERROR` when the envelope itself failed, and a closed
//! connection when the controller no longer speaks the request's version.
//!
//! Topics the broker creates on its own go the same way as plain
//! `CreateTopics` requests: a topic `Metadata` auto-creates, the
//! `__consumer_offsets` topic `FindCoordinator` needs, and a streams group's
//! internal topics. A name already on its way is not sent again, as Kafka's
//! `inflightTopics` set does.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        default_principal_data::DefaultPrincipalData,
        envelope_request::EnvelopeRequest,
    },
};

use super::{
    BrokerNode,
    channel::{ChannelOutcome, ControllerRequest, ControllerResponse, Purpose},
    cluster::{
        CONSUMER_OFFSETS_PARTITIONS, CONSUMER_OFFSETS_REPLICATION_FACTOR, CONSUMER_OFFSETS_TOPIC,
    },
    conn::parse_response_bytes,
    dispatch::{HoldReason, Outcome, Reply, RequestCtx, Step, encode_reply},
};
use crate::lab::{
    codes,
    net::{Ctx, node_ip},
};

/// Kafka's `KafkaPrincipal.ANONYMOUS` name, the principal of every plaintext
/// client.
pub const ANONYMOUS: &str = "ANONYMOUS";

/// A forwarded request waiting for the controller's answer.
#[derive(Debug)]
pub struct PendingForward {
    /// The token the answer comes back under.
    token: u64,
    /// The api's error answer with `REQUEST_TIMED_OUT`.
    timed_out: Reply,
    /// The api's error answer with `UNKNOWN_SERVER_ERROR`.
    failed: Reply,
}

/// How a forwarded request ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForwardResult {
    /// The controller answered: the response header and body it wrapped.
    Answered(Bytes),
    /// No controller answered within the retry timeout.
    TimedOut,
    /// The envelope failed.
    Failed,
    /// The controller no longer speaks the request's version: the broker
    /// closes the client's connection so it negotiates again.
    Close,
}

/// The forwarding state of a broker.
#[derive(Debug, Default)]
pub struct Forwarding {
    next_token: u64,
    /// Answers that arrived for held requests, by token.
    results: BTreeMap<u64, ForwardResult>,
    /// Topics the broker asked the controller to create and has no answer
    /// for yet.
    creating: BTreeSet<String>,
}

impl Forwarding {
    /// Forget everything, as a restart does.
    pub fn reset(&mut self) {
        self.results.clear();
        self.creating.clear();
    }

    /// Whether the broker already asked for `topic`.
    #[must_use]
    pub fn is_creating(&self, topic: &str) -> bool {
        self.creating.contains(topic)
    }
}

/// Kafka's `DefaultKafkaPrincipalBuilder.serialize` of the anonymous
/// principal: a big-endian version, then `DefaultPrincipalData` at v0.
#[must_use]
pub fn anonymous_principal() -> Bytes {
    let data = DefaultPrincipalData {
        type_: "User".to_string(),
        name: ANONYMOUS.to_string(),
        token_authenticated: false,
        ..DefaultPrincipalData::default()
    };
    let mut out = BytesMut::with_capacity(2 + data.encoded_len(0));
    out.put_i16(0);
    // The principal is two short strings and a flag, which encode at v0.
    if data.encode(&mut out, 0).is_err() {
        return Bytes::new();
    }
    out.freeze()
}

impl BrokerNode {
    /// Forward the request `req` carries to the active controller, and hold
    /// it until the answer comes back. `timed_out` and `failed` are the api's
    /// error answers with `REQUEST_TIMED_OUT` and `UNKNOWN_SERVER_ERROR`.
    pub(super) fn forward<R: Encode>(
        &mut self,
        ctx: &mut Ctx<'_>,
        req: &RequestCtx,
        timed_out: &R,
        failed: &R,
    ) -> Outcome<R> {
        let (Ok(timed_out), Ok(failed)) = (
            encode_reply(timed_out, req.version),
            encode_reply(failed, req.version),
        ) else {
            return Outcome::Close;
        };
        self.forwarding.next_token += 1;
        let token = self.forwarding.next_token;
        let envelope = EnvelopeRequest {
            request_data: req.raw.clone(),
            request_principal: Some(anonymous_principal()),
            client_host_address: Bytes::copy_from_slice(&node_ip(req.conn.0.node).octets()),
            ..EnvelopeRequest::default()
        };
        self.forwarding_channel.enqueue(
            ctx.now(),
            ControllerRequest::Envelope(envelope),
            Purpose::Forward { token },
        );
        Outcome::Hold(HoldReason::Forward(Box::new(PendingForward {
            token,
            timed_out,
            failed,
        })))
    }

    /// Record how a forwarded request ended, for its held request to pick
    /// up. An answer whose client has gone is dropped.
    pub(super) fn on_forward_outcome(&mut self, token: u64, outcome: ChannelOutcome) {
        let result = match outcome {
            ChannelOutcome::Response(ControllerResponse::Envelope(envelope)) => {
                match (envelope.error_code, envelope.response_data) {
                    (codes::NONE, Some(data)) => ForwardResult::Answered(data),
                    (codes::UNSUPPORTED_VERSION, _) => ForwardResult::Close,
                    _ => ForwardResult::Failed,
                }
            }
            ChannelOutcome::TimedOut => ForwardResult::TimedOut,
            ChannelOutcome::Response(_) | ChannelOutcome::VersionMismatch => ForwardResult::Failed,
        };
        let waiting = self.conns.values().any(|conn| {
            matches!(
                conn.queue.front().and_then(|r| r.held.as_ref()),
                Some(HoldReason::Forward(pending)) if pending.token == token
            )
        });
        if waiting {
            self.forwarding.results.insert(token, result);
        }
    }

    /// Run a held forwarded request again: answered, failed, or waiting.
    pub(super) fn retry_forward(&mut self, req: &RequestCtx, pending: PendingForward) -> Step {
        match self.forwarding.results.remove(&pending.token) {
            None => Step::Hold(HoldReason::Forward(Box::new(pending))),
            Some(ForwardResult::Answered(bytes)) => {
                match parse_response_bytes(&bytes, req.api_key, req.version) {
                    Ok((correlation, body)) if correlation == req.correlation_id => {
                        Step::Reply(Reply {
                            body,
                            version: req.version,
                        })
                    }
                    _ => Step::Reply(pending.failed),
                }
            }
            Some(ForwardResult::TimedOut) => Step::Reply(pending.timed_out),
            Some(ForwardResult::Failed) => Step::Reply(pending.failed),
            Some(ForwardResult::Close) => Step::Close,
        }
    }

    /// Kafka's `AutoTopicCreationManager.creatableTopicResult`: a missing
    /// topic with the broker's `num.partitions` and
    /// `default.replication.factor`, and `__consumer_offsets` with the group
    /// coordinator's settings: 50 compacted partitions, replicated
    /// `offsets.topic.replication.factor` times or on every broker a
    /// placement may use when there are fewer.
    pub(super) fn creatable_topic(&self, name: &str) -> CreatableTopic {
        if name != CONSUMER_OFFSETS_TOPIC {
            return CreatableTopic {
                name: name.to_string(),
                num_partitions: self.config.default_partitions,
                replication_factor: self.config.default_replication_factor,
                ..CreatableTopic::default()
            };
        }
        let brokers = self
            .image
            .brokers()
            .filter(|b| !b.in_controlled_shutdown)
            .count();
        let config = |name: &str, value: &str| CreatableTopicConfig {
            name: name.to_string(),
            value: Some(value.to_string()),
            ..CreatableTopicConfig::default()
        };
        CreatableTopic {
            name: name.to_string(),
            num_partitions: CONSUMER_OFFSETS_PARTITIONS,
            replication_factor: i16::try_from(brokers)
                .unwrap_or(i16::MAX)
                .clamp(1, CONSUMER_OFFSETS_REPLICATION_FACTOR),
            configs: vec![
                config("cleanup.policy", "compact"),
                config("compression.type", "producer"),
                config("segment.bytes", "104857600"),
            ],
            ..CreatableTopic::default()
        }
    }

    /// Ask the controller to create `topics`, skipping the names already on
    /// their way. Returns the names this call sent.
    pub(super) fn create_topics_internally(
        &mut self,
        ctx: &mut Ctx<'_>,
        topics: Vec<CreatableTopic>,
    ) -> Vec<String> {
        let topics: Vec<CreatableTopic> = topics
            .into_iter()
            .filter(|t| self.forwarding.creating.insert(t.name.clone()))
            .collect();
        if topics.is_empty() {
            return Vec::new();
        }
        let names: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
        let request = CreateTopicsRequest {
            topics,
            timeout_ms: i32::try_from(self.config.request_timeout_ms).unwrap_or(i32::MAX),
            validate_only: false,
            ..CreateTopicsRequest::default()
        };
        self.forwarding_channel.enqueue(
            ctx.now(),
            ControllerRequest::CreateTopics(request),
            Purpose::CreateTopics {
                topics: names.clone(),
            },
        );
        names
    }

    /// A creation the broker asked for ended.
    pub(super) fn on_create_topics_outcome(
        &mut self,
        ctx: &mut Ctx<'_>,
        topics: &[String],
        outcome: &ChannelOutcome,
    ) {
        for topic in topics {
            self.forwarding.creating.remove(topic);
        }
        if let ChannelOutcome::Response(ControllerResponse::CreateTopics(response)) = outcome {
            for row in &response.topics {
                if !matches!(row.error_code, codes::NONE | codes::TOPIC_ALREADY_EXISTS) {
                    ctx.event(
                        "topic_creation_failed",
                        serde_json::json!({
                            "topic": row.name, "error_code": row.error_code,
                            "message": row.error_message, "level": "warn",
                        }),
                    );
                }
            }
        }
    }
}
