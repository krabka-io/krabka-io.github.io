//! The dispatch registry: every request the broker serves, listed once.
//!
//! The [`registry!`] macro derives the version ranges of the `ApiVersions`
//! table from [`ProtocolRequest`], decodes a request at its negotiated version,
//! runs its handler, and encodes the reply at the same version. A handler is
//! `fn(&mut BrokerNode, &mut Ctx<'_>, &RequestCtx, Req) -> Outcome<Resp>`; an
//! entry may name a body type of its own after `as`, which the dispatcher
//! decodes in place of the request type (`Produce` reads its framing only).
//! A held outcome parks the request at the head of its connection until a
//! timer or a state change lets [`retry`] finish it.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    ApiKey, Decode, Encode, ProtocolError, ProtocolRequest,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersion,
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        create_partitions_request::CreatePartitionsRequest,
        create_topics_request::CreateTopicsRequest, delete_topics_request::DeleteTopicsRequest,
        describe_cluster_request::DescribeClusterRequest,
        describe_configs_request::DescribeConfigsRequest,
        describe_groups_request::DescribeGroupsRequest,
        describe_topic_partitions_request::DescribeTopicPartitionsRequest,
        fetch_request::FetchRequest, find_coordinator_request::FindCoordinatorRequest,
        heartbeat_request::HeartbeatRequest, init_producer_id_request::InitProducerIdRequest,
        join_group_request::JoinGroupRequest, leave_group_request::LeaveGroupRequest,
        list_groups_request::ListGroupsRequest, list_offsets_request::ListOffsetsRequest,
        metadata_request::MetadataRequest, offset_commit_request::OffsetCommitRequest,
        offset_fetch_request::OffsetFetchRequest,
        offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
        produce_request::ProduceRequest, sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        sync_group_request::SyncGroupRequest,
    },
};
use thiserror::Error;

use super::{BrokerNode, handlers, handlers::produce::PendingProduce};
use crate::lab::net::{ConnId, Ctx, Endpoint, Millis};

/// The `(client endpoint, connection)` pair that identifies a connection.
pub type ConnKey = (Endpoint, ConnId);

/// What a handler knows about the request it serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestCtx {
    /// The connection the request came on.
    pub conn: ConnKey,
    /// The request's api.
    pub api_key: ApiKey,
    /// The request's version, which the reply follows.
    pub version: i16,
    /// The correlation id the reply echoes.
    pub correlation_id: i32,
    /// The header's `client_id`.
    pub client_id: Option<String>,
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
}

impl HoldReason {
    /// When the wait ends whatever happens.
    #[must_use]
    pub fn deadline(&self) -> Millis {
        match self {
            Self::Fetch { deadline } => *deadline,
            Self::Produce(pending) => pending.deadline,
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

/// The versions the broker serves of one api.
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

macro_rules! registry {
    (@body $req:ty) => { $req };
    (@body $req:ty, $body:ty) => { $body };
    ($( $variant:ident : $req:ty $(as $body:ty)? => $handler:path, )*) => {
        /// The `ApiVersions` table: every served api with its version range,
        /// ascending by api key. The maximum is the latest stable version.
        #[must_use]
        pub fn api_versions_table() -> Vec<ApiVersion> {
            let mut table = vec![
                $( ApiVersion {
                    api_key: <$req as ProtocolRequest>::API_KEY,
                    min_version: <$req as ProtocolRequest>::MIN_VERSION,
                    max_version: <$req as ProtocolRequest>::LATEST_STABLE_VERSION,
                    ..ApiVersion::default()
                }, )*
            ];
            table.sort_by_key(|entry| entry.api_key);
            table
        }

        /// The versions served of `api_key`, or `None` for an api the broker
        /// does not serve.
        #[must_use]
        pub fn versions(api_key: ApiKey) -> Option<VersionRange> {
            match api_key {
                $( ApiKey::$variant => Some(VersionRange {
                    min: <$req as ProtocolRequest>::MIN_VERSION,
                    max: <$req as ProtocolRequest>::LATEST_STABLE_VERSION,
                    flexible_min: <$req as ProtocolRequest>::FLEXIBLE_MIN,
                }), )*
                _ => None,
            }
        }

        /// Decode `body` as the request `req` names, run its handler and
        /// encode the reply.
        ///
        /// # Errors
        /// Returns an error the connection closes on: an api the broker does
        /// not serve, a version outside the served range of an api other
        /// than `ApiVersions`, or a body that does not decode.
        pub fn dispatch(
            node: &mut BrokerNode,
            ctx: &mut Ctx<'_>,
            req: &RequestCtx,
            body: &[u8],
        ) -> Result<Step, DispatchError> {
            let range = versions(req.api_key).ok_or(DispatchError::UnknownApi(req.api_key as i16))?;
            if !range.contains(req.version) {
                if req.api_key == ApiKey::ApiVersions {
                    let body = encode_body(&handlers::api_versions::unsupported_version(), 0)?;
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
    CreateTopics: CreateTopicsRequest => handlers::create_topics::handle,
    DeleteTopics: DeleteTopicsRequest => handlers::delete_topics::handle,
    InitProducerId: InitProducerIdRequest => handlers::init_producer_id::handle,
    OffsetForLeaderEpoch: OffsetForLeaderEpochRequest => handlers::offset_for_leader_epoch::handle,
    DescribeConfigs: DescribeConfigsRequest => handlers::describe_configs::handle,
    SaslAuthenticate: SaslAuthenticateRequest => handlers::sasl::authenticate,
    CreatePartitions: CreatePartitionsRequest => handlers::create_partitions::handle,
    DescribeCluster: DescribeClusterRequest => handlers::describe_cluster::handle,
    ConsumerGroupHeartbeat: ConsumerGroupHeartbeatRequest => handlers::groups::consumer_group_heartbeat,
    ConsumerGroupDescribe: ConsumerGroupDescribeRequest => handlers::groups::consumer_group_describe,
    DescribeTopicPartitions: DescribeTopicPartitionsRequest => handlers::describe_topic_partitions::handle,
    StreamsGroupHeartbeat: StreamsGroupHeartbeatRequest => handlers::groups::streams_group_heartbeat,
    StreamsGroupDescribe: StreamsGroupDescribeRequest => handlers::groups::streams_group_describe,
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
        HoldReason::Fetch { .. } => dispatch(node, ctx, req, body),
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
    }
}

/// The request header version of a request: 2 with flexible framing, else 1.
/// An unknown api reads as a non-flexible header, which decodes the fields
/// both versions share.
#[must_use]
pub fn request_header_version(api_key: ApiKey, version: i16) -> i16 {
    match versions(api_key) {
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
    match versions(api_key) {
        Some(range) if range.is_flexible(version) => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn the_table_is_sorted_and_derived_from_the_codec() {
        let table = api_versions_table();
        assert!(table.windows(2).all(|w| w[0].api_key < w[1].api_key));
        let fetch = table.iter().find(|e| e.api_key == 1).unwrap();
        assert!(fetch.min_version == FetchRequest::MIN_VERSION);
        assert!(fetch.max_version == FetchRequest::LATEST_STABLE_VERSION);
        assert!(versions(ApiKey::Fetch).unwrap().flexible_min == 12);
        assert!(versions(ApiKey::DeleteRecords).is_none());
        assert!(table.len() == 28);
    }

    #[test]
    fn header_versions_follow_kip_482_and_the_api_versions_exception() {
        assert!(request_header_version(ApiKey::Fetch, 11) == 1);
        assert!(request_header_version(ApiKey::Fetch, 12) == 2);
        assert!(request_header_version(ApiKey::ApiVersions, 3) == 2);
        assert!(request_header_version(ApiKey::ApiVersions, 2) == 1);
        assert!(response_header_version(ApiKey::ApiVersions, 3) == 0);
        assert!(response_header_version(ApiKey::Metadata, 9) == 1);
        assert!(response_header_version(ApiKey::Metadata, 8) == 0);
        assert!(response_header_version(ApiKey::SaslHandshake, 1) == 0);
    }
}
