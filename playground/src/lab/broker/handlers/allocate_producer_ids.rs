//! `AllocateProducerIds` (api key 67) on the controller listener: a broker
//! claims the next block of producer ids.
//!
//! As Kafka's `ProducerIdControlManager.generateNextProducerId` decides it
//! ([`ControllerDecisions::allocate_producer_ids`]): a broker that is not
//! registered, or names another broker epoch, answers `STALE_BROKER_EPOCH`;
//! otherwise the block after every block the log holds is written, and the
//! answer names it once the record commits. A node that is not the active
//! controller answers `NOT_CONTROLLER`.
//!
//! [`ControllerDecisions::allocate_producer_ids`]: crate::lab::controller::ControllerDecisions::allocate_producer_ids

use krabka_protocol::owned::{
    allocate_producer_ids_request::AllocateProducerIdsRequest,
    allocate_producer_ids_response::AllocateProducerIdsResponse,
};

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{
    codes,
    net::{Ctx, NodeId},
};

fn refused(error_code: i16) -> AllocateProducerIdsResponse {
    AllocateProducerIdsResponse {
        error_code,
        ..AllocateProducerIdsResponse::default()
    }
}

/// Serve an `AllocateProducerIds` as the controller.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    AllocateProducerIdsRequest {
        broker_id,
        broker_epoch,
        ..
    }: AllocateProducerIdsRequest,
) -> Outcome<AllocateProducerIdsResponse> {
    let Some(active) = node.quorum.active.as_ref() else {
        return Outcome::Reply(refused(codes::NOT_CONTROLLER));
    };
    let Ok(broker) = u32::try_from(broker_id) else {
        return Outcome::Reply(refused(codes::STALE_BROKER_EPOCH));
    };
    match active
        .decisions
        .allocate_producer_ids(&active.image, NodeId(broker), broker_epoch)
    {
        Ok(block) => {
            let response = AllocateProducerIdsResponse {
                error_code: codes::NONE,
                producer_id_start: block.start,
                producer_id_len: block.len,
                ..AllocateProducerIdsResponse::default()
            };
            node.controller_write(ctx, req, block.records, response, |_| {
                refused(codes::NOT_CONTROLLER)
            })
        }
        Err(code) => Outcome::Reply(refused(code)),
    }
}
