//! The dispatch registries: every request each listener serves, listed once.
//!
//! A broker has two listeners, as a Kafka node in combined mode does: the
//! client listener ([`Listener::Broker`], port 9092) and the controller
//! listener ([`Listener::Controller`], port 9093), which serves the requests
//! brokers send the active controller and the `--bootstrap-controller`
//! admin requests (KIP-919). The [`registry!`] macro derives each listener's
//! `ApiVersions` table from [`ProtocolRequest`] (the latest stable version,
//! or a lower maximum an entry names as `[max = n]` where Kafka serves less
//! than the codec knows), decodes a request at its negotiated version, runs
//! its handler, and encodes the reply at the same version. A handler is `fn(&mut BrokerNode, &mut Ctx<'_>, &RequestCtx, Req)
//! -> Outcome<Resp>`; an entry may name a body type of its own after `as`,
//! which the dispatcher decodes in place of the request type (`Produce` reads
//! its framing only). A held outcome parks the request at the head of its
//! connection until a timer or a state change lets [`retry`] finish it.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    ApiKey, Decode, Encode, ProtocolError, ProtocolRequest,
    owned::{
        allocate_producer_ids_request::AllocateProducerIdsRequest,
        alter_partition_request::AlterPartitionRequest, api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersion, broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_registration_request::BrokerRegistrationRequest,
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        create_partitions_request::CreatePartitionsRequest,
        create_topics_request::CreateTopicsRequest, delete_topics_request::DeleteTopicsRequest,
        describe_cluster_request::DescribeClusterRequest,
        describe_configs_request::DescribeConfigsRequest,
        describe_groups_request::DescribeGroupsRequest,
        describe_quorum_request::DescribeQuorumRequest,
        describe_topic_partitions_request::DescribeTopicPartitionsRequest,
        envelope_request::EnvelopeRequest, fetch_request::FetchRequest,
        find_coordinator_request::FindCoordinatorRequest, heartbeat_request::HeartbeatRequest,
        init_producer_id_request::InitProducerIdRequest, join_group_request::JoinGroupRequest,
        leave_group_request::LeaveGroupRequest, list_groups_request::ListGroupsRequest,
        list_offsets_request::ListOffsetsRequest, metadata_request::MetadataRequest,
        offset_commit_request::OffsetCommitRequest, offset_fetch_request::OffsetFetchRequest,
        offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
        produce_request::ProduceRequest, sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        sync_group_request::SyncGroupRequest,
    },
};
use thiserror::Error;

use super::{
    BrokerNode, forward::PendingForward, groups::PendingGroupWrite, handlers,
    handlers::produce::PendingProduce, quorum::PendingWrite,
};
use crate::lab::{
    broker::coordinator::HoldToken,
    controller::RAFT_PORT,
    net::{ConnId, Ctx, Endpoint, KAFKA_PORT, Millis, NodeId},
};

/// The `(client endpoint, connection)` pair that identifies a connection.
pub type ConnKey = (Endpoint, ConnId);

/// A listener of the broker node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Listener {
    /// The client listener, `PLAINTEXT` on [`KAFKA_PORT`].
    Broker,
    /// The controller listener, `CONTROLLER` on [`RAFT_PORT`]. The raft
    /// messages between controllers arrive on the same port as JSON.
    Controller,
}

impl Listener {
    /// The port the listener sits on.
    #[must_use]
    pub const fn port(self) -> u16 {
        match self {
            Self::Broker => KAFKA_PORT,
            Self::Controller => RAFT_PORT,
        }
    }

    /// The listener's endpoint on `node`.
    #[must_use]
    pub const fn endpoint(self, node: NodeId) -> Endpoint {
        Endpoint::new(node, self.port())
    }

    /// The listener's name, as Kafka's configs name it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Broker => "PLAINTEXT",
            Self::Controller => "CONTROLLER",
        }
    }

    /// The versions the listener serves of `api_key`, or `None` for an api
    /// it does not serve.
    #[must_use]
    pub fn versions(self, api_key: ApiKey) -> Option<VersionRange> {
        match self {
            Self::Broker => broker_versions(api_key),
            Self::Controller => controller_versions(api_key),
        }
    }

    /// The listener's `ApiVersions` table.
    #[must_use]
    pub fn api_versions_table(self) -> Vec<ApiVersion> {
        match self {
            Self::Broker => broker_api_versions_table(),
            Self::Controller => controller_api_versions_table(),
        }
    }

    /// Decode `body` as the request `req` names, run the listener's handler
    /// and encode the reply.
    ///
    /// # Errors
    /// Returns an error the connection closes on: an api the listener does
    /// not serve, a version outside the served range of an api other than
    /// `ApiVersions`, or a body that does not decode.
    pub fn dispatch(
        self,
        node: &mut BrokerNode,
        ctx: &mut Ctx<'_>,
        req: &RequestCtx,
        body: &[u8],
    ) -> Result<Step, DispatchError> {
        match self {
            Self::Broker => dispatch_broker(node, ctx, req, body),
            Self::Controller => dispatch_controller(node, ctx, req, body),
        }
    }
}

/// What a handler knows about the request it serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestCtx {
    /// The connection the request came on.
    pub conn: ConnKey,
    /// The listener the connection belongs to.
    pub listener: Listener,
    /// The request's api.
    pub api_key: ApiKey,
    /// The request's version, which the reply follows.
    pub version: i16,
    /// The correlation id the reply echoes.
    pub correlation_id: i32,
    /// The header's `client_id`.
    pub client_id: Option<String>,
    /// The request as the client framed it, header and body, without the
    /// length prefix: what a forwarding broker wraps in an `Envelope`.
    pub raw: Bytes,
    /// The deadline of a held request being retried; `None` on the first
    /// run. A handler that retries must not repeat its side effects.
    pub held_until: Option<Millis>,
}

impl RequestCtx {
    /// Whether this run is a retry of a held request.
    #[must_use]
    pub fn is_retry(&self) -> bool {
        self.held_until.is_some()
    }
}

/// Why a request waits at the head of its connection.
#[derive(Debug)]
pub enum HoldReason {
    /// A `Fetch` short of `min_bytes` waits until `deadline`; the handler
    /// runs again on every state change and at the deadline.
    Fetch { deadline: Millis },
    /// A `Produce` with `acks=-1` waits for the high watermark.
    Produce(Box<PendingProduce>),
    /// A request forwarded to the active controller waits for its answer.
    Forward(Box<PendingForward>),
    /// A controller answer waits for its records to commit.
    ControllerWrite(Box<PendingWrite>),
    /// A `JoinGroup` or `SyncGroup` the group coordinator holds.
    Group { token: HoldToken },
    /// A group coordinator answer waits for its records to reach the ISR.
    GroupWrite(Box<PendingGroupWrite>),
}

impl HoldReason {
    /// When the wait ends whatever happens. A wait another part of the
    /// broker ends answers `Millis::MAX`.
    #[must_use]
    pub fn deadline(&self) -> Millis {
        match self {
            Self::Fetch { deadline } => *deadline,
            Self::Produce(pending) => pending.deadline,
            Self::GroupWrite(pending) => pending.deadline,
            Self::Forward(_) | Self::ControllerWrite(_) | Self::Group { .. } => Millis::MAX,
        }
    }
}

/// What a handler decided.
#[derive(Debug)]
pub enum Outcome<R> {
    /// Answer now.
    Reply(R),
    /// Answer later; the request stays at the head of its connection.
    Hold(HoldReason),
    /// Answer nothing, as Kafka does for `acks=0`.
    Silent,
    /// Close the connection without an answer.
    Close,
}

/// An encoded reply and the version its header follows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply {
    /// The encoded response body.
    pub body: Bytes,
    /// The version the body and its header follow.
    pub version: i16,
}

/// A handler outcome with its reply encoded.
#[derive(Debug)]
pub enum Step {
    Reply(Reply),
    Hold(HoldReason),
    Silent,
    Close,
}

/// Why a request could not be dispatched. Every variant closes the
/// connection, as Kafka does.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DispatchError {
    #[error("unknown api key {0}")]
    UnknownApi(i16),
    #[error("{api:?} version {version} is not served")]
    UnsupportedVersion { api: ApiKey, version: i16 },
    #[error("wire: {0}")]
    Protocol(#[from] ProtocolError),
}

/// The versions a listener serves of one api.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionRange {
    /// The lowest version served.
    pub min: i16,
    /// The highest version served, the latest stable one.
    pub max: i16,
    /// The first version with flexible (KIP-482) framing, `i16::MAX` when
    /// there is none.
    pub flexible_min: i16,
}

impl VersionRange {
    /// Whether `version` is served.
    #[must_use]
    pub fn contains(self, version: i16) -> bool {
        (self.min..=self.max).contains(&version)
    }

    /// Whether `version` has flexible (KIP-482) framing.
    #[must_use]
    pub fn is_flexible(self, version: i16) -> bool {
        version >= self.flexible_min
    }
}

/// Encode a response body at `version`.
///
/// # Errors
/// Returns the codec error when the body cannot be encoded at that version.
pub fn encode_body<R: Encode>(resp: &R, version: i16) -> Result<Bytes, ProtocolError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Encode a reply at `version`, for an answer a handler keeps to send later.
///
/// # Errors
/// Returns the codec error when the body cannot be encoded at that version.
pub fn encode_reply<R: Encode>(resp: &R, version: i16) -> Result<Reply, ProtocolError> {
    Ok(Reply {
        body: encode_body(resp, version)?,
        version,
    })
}

macro_rules! registry {
    (@body $req:ty) => { $req };
    (@body $req:ty, $body:ty) => { $body };
    (@max $req:ty) => { <$req as ProtocolRequest>::LATEST_STABLE_VERSION };
    (@max $req:ty, $max:literal) => { $max };
    (
        listener: $listener:expr,
        table: $table:ident,
        versions: $versions:ident,
        dispatch: $dispatch:ident;
        $( $variant:ident : $req:ty $(as $body:ty)? $([max = $max:literal])? => $handler:path, )*
    ) => {
        /// The listener's `ApiVersions` table: every served api with its
        /// version range, ascending by api key. The maximum is the latest
        /// stable version.
        #[must_use]
        pub fn $table() -> Vec<ApiVersion> {
            let mut table = vec![
                $( ApiVersion {
                    api_key: <$req as ProtocolRequest>::API_KEY,
                    min_version: <$req as ProtocolRequest>::MIN_VERSION,
                    max_version: registry!(@max $req $(, $max)?),
                    ..ApiVersion::default()
                }, )*
            ];
            table.sort_by_key(|entry| entry.api_key);
            table
        }

        /// The versions the listener serves of `api_key`, or `None` for an
        /// api it does not serve.
        #[must_use]
        pub fn $versions(api_key: ApiKey) -> Option<VersionRange> {
            match api_key {
                $( ApiKey::$variant => Some(VersionRange {
                    min: <$req as ProtocolRequest>::MIN_VERSION,
                    max: registry!(@max $req $(, $max)?),
                    flexible_min: <$req as ProtocolRequest>::FLEXIBLE_MIN,
                }), )*
                _ => None,
            }
        }

        /// Decode `body` as the request `req` names, run its handler and
        /// encode the reply.
        ///
        /// # Errors
        /// Returns an error the connection closes on: an api the listener
        /// does not serve, a version outside the served range of an api
        /// other than `ApiVersions`, or a body that does not decode.
        pub fn $dispatch(
            node: &mut BrokerNode,
            ctx: &mut Ctx<'_>,
            req: &RequestCtx,
            body: &[u8],
        ) -> Result<Step, DispatchError> {
            let range = $versions(req.api_key).ok_or(DispatchError::UnknownApi(req.api_key as i16))?;
            if !range.contains(req.version) {
                if req.api_key == ApiKey::ApiVersions {
                    let body = encode_body(&handlers::api_versions::unsupported_version($listener), 0)?;
                    return Ok(Step::Reply(Reply { body, version: 0 }));
                }
                return Err(DispatchError::UnsupportedVersion {
                    api: req.api_key,
                    version: req.version,
                });
            }
            let mut cursor = body;
            match req.api_key {
                $( ApiKey::$variant => {
                    let decoded: registry!(@body $req $(, $body)?) =
                        Decode::decode(&mut cursor, req.version)?;
                    Ok(match $handler(node, ctx, req, decoded) {
                        Outcome::Reply(resp) => Step::Reply(Reply {
                            body: encode_body(&resp, req.version)?,
                            version: req.version,
                        }),
                        Outcome::Hold(reason) => Step::Hold(reason),
                        Outcome::Silent => Step::Silent,
                        Outcome::Close => Step::Close,
                    })
                } )*
                other => Err(DispatchError::UnknownApi(other as i16)),
            }
        }
    };
}

registry! {
    listener: Listener::Broker,
    table: broker_api_versions_table,
    versions: broker_versions,
    dispatch: dispatch_broker;
    Produce: ProduceRequest as handlers::produce::ProduceBody => handlers::produce::handle,
    Fetch: FetchRequest => handlers::fetch::handle,
    ListOffsets: ListOffsetsRequest => handlers::list_offsets::handle,
    Metadata: MetadataRequest => handlers::metadata::handle,
    OffsetCommit: OffsetCommitRequest => handlers::groups::offset_commit,
    OffsetFetch: OffsetFetchRequest => handlers::groups::offset_fetch,
    FindCoordinator: FindCoordinatorRequest => handlers::find_coordinator::handle,
    JoinGroup: JoinGroupRequest => handlers::groups::join_group,
    Heartbeat: HeartbeatRequest => handlers::groups::heartbeat,
    LeaveGroup: LeaveGroupRequest => handlers::groups::leave_group,
    SyncGroup: SyncGroupRequest => handlers::groups::sync_group,
    DescribeGroups: DescribeGroupsRequest => handlers::groups::describe_groups,
    ListGroups: ListGroupsRequest => handlers::groups::list_groups,
    SaslHandshake: SaslHandshakeRequest => handlers::sasl::handshake,
    ApiVersions: ApiVersionsRequest => handlers::api_versions::handle,
    CreateTopics: CreateTopicsRequest => handlers::forwarded::create_topics,
    DeleteTopics: DeleteTopicsRequest => handlers::forwarded::delete_topics,
    InitProducerId: InitProducerIdRequest => handlers::init_producer_id::handle,
    OffsetForLeaderEpoch: OffsetForLeaderEpochRequest => handlers::offset_for_leader_epoch::handle,
    DescribeConfigs: DescribeConfigsRequest => handlers::describe_configs::handle,
    SaslAuthenticate: SaslAuthenticateRequest => handlers::sasl::authenticate,
    CreatePartitions: CreatePartitionsRequest => handlers::forwarded::create_partitions,
    DescribeQuorum: DescribeQuorumRequest => handlers::forwarded::describe_quorum,
    DescribeCluster: DescribeClusterRequest => handlers::describe_cluster::handle,
    ConsumerGroupHeartbeat: ConsumerGroupHeartbeatRequest => handlers::groups::consumer_group_heartbeat,
    ConsumerGroupDescribe: ConsumerGroupDescribeRequest => handlers::groups::consumer_group_describe,
    DescribeTopicPartitions: DescribeTopicPartitionsRequest => handlers::describe_topic_partitions::handle,
    // Kafka 4.3 serves v0 of the two streams group apis; the codec knows v1.
    StreamsGroupHeartbeat: StreamsGroupHeartbeatRequest [max = 0] => handlers::groups::streams_group_heartbeat,
    StreamsGroupDescribe: StreamsGroupDescribeRequest [max = 0] => handlers::groups::streams_group_describe,
}

registry! {
    listener: Listener::Controller,
    table: controller_api_versions_table,
    versions: controller_versions,
    dispatch: dispatch_controller;
    ApiVersions: ApiVersionsRequest => handlers::api_versions::handle,
    CreateTopics: CreateTopicsRequest => handlers::create_topics::handle,
    DeleteTopics: DeleteTopicsRequest => handlers::delete_topics::handle,
    CreatePartitions: CreatePartitionsRequest => handlers::create_partitions::handle,
    DescribeQuorum: DescribeQuorumRequest => handlers::describe_quorum::handle,
    AlterPartition: AlterPartitionRequest => handlers::alter_partition::handle,
    Envelope: EnvelopeRequest => handlers::envelope::handle,
    BrokerRegistration: BrokerRegistrationRequest => handlers::broker_registration::handle,
    BrokerHeartbeat: BrokerHeartbeatRequest => handlers::broker_heartbeat::handle,
    AllocateProducerIds: AllocateProducerIdsRequest => handlers::allocate_producer_ids::handle,
}

/// Run a held request again.
///
/// # Errors
/// Returns the dispatch error of the run, which closes the connection.
pub fn retry(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    body: &[u8],
    held: HoldReason,
) -> Result<Step, DispatchError> {
    match held {
        HoldReason::Fetch { .. } => req.listener.dispatch(node, ctx, req, body),
        HoldReason::Produce(pending) => {
            Ok(match handlers::produce::retry(node, ctx, req, *pending) {
                Outcome::Reply(resp) => Step::Reply(Reply {
                    body: encode_body(&resp, req.version)?,
                    version: req.version,
                }),
                Outcome::Hold(reason) => Step::Hold(reason),
                Outcome::Silent => Step::Silent,
                Outcome::Close => Step::Close,
            })
        }
        HoldReason::Forward(pending) => Ok(node.retry_forward(req, *pending)),
        HoldReason::ControllerWrite(pending) => Ok(node.retry_controller_write(*pending)),
        HoldReason::Group { token } => node.retry_group_hold(ctx, req, token),
        HoldReason::GroupWrite(pending) => Ok(node.retry_group_write(ctx.now(), *pending)),
    }
}

/// The versions any listener serves of `api_key`: the client listener's,
/// else the controller listener's.
fn any_versions(api_key: ApiKey) -> Option<VersionRange> {
    broker_versions(api_key).or_else(|| controller_versions(api_key))
}

/// The request header version of a request: 2 with flexible framing, else 1.
/// An api neither listener serves reads as a non-flexible header, which
/// decodes the fields both versions share.
#[must_use]
pub fn request_header_version(api_key: ApiKey, version: i16) -> i16 {
    match any_versions(api_key) {
        Some(range) if range.is_flexible(version) => 2,
        _ => 1,
    }
}

/// The response header version: 1 when the body is flexible, else 0.
/// `ApiVersions` always answers with header 0, as Kafka does.
#[must_use]
pub fn response_header_version(api_key: ApiKey, version: i16) -> i16 {
    if api_key == ApiKey::ApiVersions {
        return 0;
    }
    match any_versions(api_key) {
        Some(range) if range.is_flexible(version) => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn the_tables_are_sorted_and_derived_from_the_codec() {
        let table = broker_api_versions_table();
        assert!(table.windows(2).all(|w| w[0].api_key < w[1].api_key));
        let fetch = table.iter().find(|e| e.api_key == 1).unwrap();
        assert!(fetch.min_version == FetchRequest::MIN_VERSION);
        assert!(fetch.max_version == FetchRequest::LATEST_STABLE_VERSION);
        assert!(broker_versions(ApiKey::Fetch).unwrap().flexible_min == 12);
        assert!(broker_versions(ApiKey::DeleteRecords).is_none());
        assert!(table.len() == 29);
        let controller: Vec<i16> = controller_api_versions_table()
            .iter()
            .map(|entry| entry.api_key)
            .collect();
        assert!(controller == vec![18, 19, 20, 37, 55, 56, 58, 62, 63, 67]);
        assert!(Listener::Broker.versions(ApiKey::BrokerHeartbeat).is_none());
        assert!(Listener::Controller.versions(ApiKey::Produce).is_none());
    }

    #[test]
    fn header_versions_follow_kip_482_and_the_api_versions_exception() {
        assert!(request_header_version(ApiKey::Fetch, 11) == 1);
        assert!(request_header_version(ApiKey::Fetch, 12) == 2);
        assert!(request_header_version(ApiKey::ApiVersions, 3) == 2);
        assert!(request_header_version(ApiKey::ApiVersions, 2) == 1);
        assert!(request_header_version(ApiKey::BrokerRegistration, 0) == 2);
        assert!(response_header_version(ApiKey::ApiVersions, 3) == 0);
        assert!(response_header_version(ApiKey::Metadata, 9) == 1);
        assert!(response_header_version(ApiKey::Metadata, 8) == 0);
        assert!(response_header_version(ApiKey::SaslHandshake, 1) == 0);
        assert!(response_header_version(ApiKey::Envelope, 0) == 1);
    }
}
