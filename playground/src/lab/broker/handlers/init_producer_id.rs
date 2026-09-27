//! `InitProducerId` (api key 22): a producer id and epoch for an idempotent
//! producer.
//!
//! The lab has no transaction coordinator, so a request that names a
//! `transactional_id`, the empty string included, is refused with
//! `INVALID_REQUEST`, and a request that carries half a producer identity
//! (KIP-360: one of the id and the epoch `-1`) is refused the same way, as
//! Kafka refuses it before the coordinator sees it; a refusal carries
//! producer id and epoch `-1`, Kafka's `getErrorResponse`. Ids come from a
//! block the active controller allocated to the broker; while the broker
//! waits for one, the request answers `COORDINATOR_LOAD_IN_PROGRESS` with
//! the same `-1` identity, which a producer retries. See
//! [`BrokerNode::generate_producer_id`].

use krabka_protocol::owned::{
    init_producer_id_request::InitProducerIdRequest,
    init_producer_id_response::InitProducerIdResponse,
};

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{codes, net::Ctx};

/// Kafka's `RecordBatch.NO_PRODUCER_ID`.
const NO_PRODUCER_ID: i64 = -1;
/// Kafka's `RecordBatch.NO_PRODUCER_EPOCH`.
const NO_PRODUCER_EPOCH: i16 = -1;

/// Serve an `InitProducerId`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    _req: &RequestCtx,
    request: InitProducerIdRequest,
) -> Outcome<InitProducerIdResponse> {
    let InitProducerIdRequest {
        transactional_id,
        producer_id,
        producer_epoch,
        ..
    } = request;
    let half_identity = (producer_id == NO_PRODUCER_ID) != (producer_epoch == NO_PRODUCER_EPOCH);
    if transactional_id.is_some() || half_identity {
        return Outcome::Reply(InitProducerIdResponse {
            error_code: codes::INVALID_REQUEST,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            ..InitProducerIdResponse::default()
        });
    }
    Outcome::Reply(match node.generate_producer_id(ctx) {
        Ok(producer_id) => InitProducerIdResponse {
            producer_id,
            producer_epoch: 0,
            ..InitProducerIdResponse::default()
        },
        Err(error_code) => InitProducerIdResponse {
            error_code,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            ..InitProducerIdResponse::default()
        },
    })
}
