//! `SaslHandshake` (api key 17) and `SaslAuthenticate` (api key 36).
//!
//! The lab's listener is plaintext, so both reach Kafka's `KafkaApis`, which
//! answers them as requests after authentication: `ILLEGAL_SASL_STATE`, with
//! no mechanism for the handshake and Kafka's message for the authenticate.

use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
    sasl_handshake_request::SaslHandshakeRequest, sasl_handshake_response::SaslHandshakeResponse,
};

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{codes, net::Ctx};

/// Serve a `SaslHandshake`: Kafka's `KafkaApis.handleSaslHandshakeRequest`.
pub fn handshake(
    _node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    _request: SaslHandshakeRequest,
) -> Outcome<SaslHandshakeResponse> {
    Outcome::Reply(SaslHandshakeResponse {
        error_code: codes::ILLEGAL_SASL_STATE,
        mechanisms: Vec::new(),
        ..SaslHandshakeResponse::default()
    })
}

/// Serve a `SaslAuthenticate`: Kafka's
/// `KafkaApis.handleSaslAuthenticateRequest`.
pub fn authenticate(
    _node: &mut BrokerNode,
    _ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    _request: SaslAuthenticateRequest,
) -> Outcome<SaslAuthenticateResponse> {
    Outcome::Reply(SaslAuthenticateResponse {
        error_code: codes::ILLEGAL_SASL_STATE,
        error_message: Some(
            "SaslAuthenticate request received after successful authentication".to_string(),
        ),
        ..SaslAuthenticateResponse::default()
    })
}
