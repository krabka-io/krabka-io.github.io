//! `Envelope` (api key 58) on the controller listener (KIP-590): a request a
//! broker forwards for its client.
//!
//! As Kafka's `EnvelopeUtils.handleEnvelopeRequest` unwraps it: a principal
//! that does not deserialize answers `PRINCIPAL_DESERIALIZATION_FAILURE`, a
//! client address that is not an IPv4 or IPv6 address, a request header that
//! does not parse, or an api Kafka does not forward answers
//! `INVALID_REQUEST`, and a request body that does not parse at its version
//! answers `UNSUPPORTED_VERSION`, all in the envelope. The inner request
//! then runs on the controller listener, and its answer, response header and
//! body, comes back in the envelope. An answer that carries
//! `NOT_CONTROLLER` anywhere comes back as an envelope with that error and
//! no data instead, as Kafka's `RequestChannel.buildResponseSend` does, so
//! the forwarding broker looks for the controller again. An inner request
//! that waits for its records to commit keeps the envelope waiting with it.

use bytes::Bytes;
use krabka_protocol::{
    ApiKey, Decode,
    owned::{
        create_partitions_response::CreatePartitionsResponse,
        create_topics_response::CreateTopicsResponse, default_principal_data::DefaultPrincipalData,
        delete_topics_response::DeleteTopicsResponse,
        describe_quorum_response::DescribeQuorumResponse, envelope_request::EnvelopeRequest,
        envelope_response::EnvelopeResponse,
    },
};

use super::super::{
    BrokerNode,
    conn::{parse_request_bytes, response_frame},
    dispatch::{
        DispatchError, HoldReason, Listener, Outcome, Reply, RequestCtx, Step, encode_reply,
    },
};
use crate::lab::{codes, net::Ctx};

/// The apis Kafka's `ApiKeys.forwardable` admits that the controller
/// listener serves.
const FORWARDABLE: [ApiKey; 5] = [
    ApiKey::CreateTopics,
    ApiKey::DeleteTopics,
    ApiKey::CreatePartitions,
    ApiKey::DescribeQuorum,
    ApiKey::AllocateProducerIds,
];

fn refused(error_code: i16) -> EnvelopeResponse {
    EnvelopeResponse {
        error_code,
        response_data: None,
        ..EnvelopeResponse::default()
    }
}

/// Kafka's `DefaultKafkaPrincipalBuilder.deserialize`: a big-endian version
/// the schema knows, then `DefaultPrincipalData` at it.
fn principal_decodes(principal: &[u8]) -> bool {
    let Some((version, mut data)) = principal
        .split_first_chunk::<2>()
        .map(|(version, rest)| (i16::from_be_bytes(*version), rest))
    else {
        return false;
    };
    version == 0 && DefaultPrincipalData::decode(&mut data, version).is_ok()
}

/// Whether an inner answer carries `NOT_CONTROLLER`, Kafka's
/// `errorCounts().containsKey(NOT_CONTROLLER)`.
fn carries_not_controller(api_key: ApiKey, reply: &Reply) -> bool {
    let mut cursor: &[u8] = &reply.body;
    let not_controller = |code: i16| code == codes::NOT_CONTROLLER;
    match api_key {
        ApiKey::CreateTopics => CreateTopicsResponse::decode(&mut cursor, reply.version)
            .is_ok_and(|r| r.topics.iter().any(|t| not_controller(t.error_code))),
        ApiKey::DeleteTopics => DeleteTopicsResponse::decode(&mut cursor, reply.version)
            .is_ok_and(|r| r.responses.iter().any(|t| not_controller(t.error_code))),
        ApiKey::CreatePartitions => CreatePartitionsResponse::decode(&mut cursor, reply.version)
            .is_ok_and(|r| r.results.iter().any(|t| not_controller(t.error_code))),
        ApiKey::DescribeQuorum => DescribeQuorumResponse::decode(&mut cursor, reply.version)
            .is_ok_and(|r| {
                not_controller(r.error_code)
                    || r.topics
                        .iter()
                        .flat_map(|t| t.partitions.iter())
                        .any(|p| not_controller(p.error_code))
            }),
        _ => false,
    }
}

/// The envelope around an inner answer: the response header and body, or
/// `NOT_CONTROLLER` alone.
fn wrap(inner: &RequestCtx, reply: &Reply) -> EnvelopeResponse {
    if carries_not_controller(inner.api_key, reply) {
        return refused(codes::NOT_CONTROLLER);
    }
    let frame = response_frame(
        inner.api_key,
        reply.version,
        inner.correlation_id,
        &reply.body,
    );
    EnvelopeResponse {
        error_code: codes::NONE,
        response_data: Some(Bytes::copy_from_slice(&frame[4..])),
        ..EnvelopeResponse::default()
    }
}

/// Serve an `Envelope`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    EnvelopeRequest {
        request_data,
        request_principal,
        client_host_address,
        ..
    }: EnvelopeRequest,
) -> Outcome<EnvelopeResponse> {
    if !request_principal.as_deref().is_some_and(principal_decodes) {
        return Outcome::Reply(refused(codes::PRINCIPAL_DESERIALIZATION_FAILURE));
    }
    if !matches!(client_host_address.len(), 4 | 16) {
        return Outcome::Reply(refused(codes::INVALID_REQUEST));
    }
    let Ok(parsed) = parse_request_bytes(request_data) else {
        return Outcome::Reply(refused(codes::INVALID_REQUEST));
    };
    if !FORWARDABLE.contains(&parsed.api_key) {
        return Outcome::Reply(refused(codes::INVALID_REQUEST));
    }
    let inner = RequestCtx {
        conn: req.conn,
        listener: Listener::Controller,
        api_key: parsed.api_key,
        version: parsed.version,
        correlation_id: parsed.correlation_id,
        client_id: parsed.client_id.clone(),
        raw: parsed.raw.clone(),
        held_until: None,
    };
    match Listener::Controller.dispatch(node, ctx, &inner, &parsed.body) {
        Ok(Step::Reply(reply)) => Outcome::Reply(wrap(&inner, &reply)),
        Ok(Step::Hold(HoldReason::ControllerWrite(pending))) => {
            let envelope_version = req.version;
            let rewrapped =
                BrokerNode::rewrap_controller_write(*pending, &|reply, not_controller| {
                    let envelope = if not_controller {
                        refused(codes::NOT_CONTROLLER)
                    } else {
                        wrap(&inner, reply)
                    };
                    encode_reply(&envelope, envelope_version).ok()
                });
            match rewrapped {
                Some(pending) => Outcome::Hold(HoldReason::ControllerWrite(Box::new(pending))),
                None => Outcome::Close,
            }
        }
        Ok(Step::Hold(_) | Step::Silent | Step::Close) => {
            Outcome::Reply(refused(codes::UNKNOWN_SERVER_ERROR))
        }
        Err(DispatchError::UnknownApi(_)) => Outcome::Reply(refused(codes::INVALID_REQUEST)),
        Err(_) => Outcome::Reply(refused(codes::UNSUPPORTED_VERSION)),
    }
}
