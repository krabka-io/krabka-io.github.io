//! `BrokerHeartbeat` (api key 63) on the controller listener: a registered
//! broker keeps its session and moves through Kafka's heartbeat states.
//!
//! As Kafka's `ReplicationControlManager.processBrokerHeartbeat` decides it
//! ([`ControllerDecisions::broker_heartbeat`]): a broker that is not
//! registered, or names another broker epoch, answers `STALE_BROKER_EPOCH`;
//! a fenced broker that has applied its own registration and no longer asks
//! to stay fenced is unfenced; a broker that asks to be fenced is. The answer
//! says whether the broker caught up, whether it is fenced, and whether it
//! may shut down, once the records the heartbeat decided commit. A node that
//! is not the active controller answers `NOT_CONTROLLER`.
//!
//! [`ControllerDecisions::broker_heartbeat`]: crate::lab::controller::ControllerDecisions::broker_heartbeat

use krabka_protocol::owned::{
    broker_heartbeat_request::BrokerHeartbeatRequest,
    broker_heartbeat_response::BrokerHeartbeatResponse,
};
use serde_json::json;

use super::super::{
    BrokerNode,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{
    codes,
    controller::decisions::Heartbeat,
    net::{Ctx, NodeId},
};

fn refused(error_code: i16) -> BrokerHeartbeatResponse {
    BrokerHeartbeatResponse {
        error_code,
        ..BrokerHeartbeatResponse::default()
    }
}

/// Serve a `BrokerHeartbeat` as the controller.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    BrokerHeartbeatRequest {
        broker_id,
        broker_epoch,
        current_metadata_offset,
        want_fence,
        want_shut_down,
        ..
    }: BrokerHeartbeatRequest,
) -> Outcome<BrokerHeartbeatResponse> {
    let now = ctx.now();
    let Some(active) = node.quorum.active.as_mut() else {
        return Outcome::Reply(refused(codes::NOT_CONTROLLER));
    };
    let Ok(broker) = u32::try_from(broker_id) else {
        return Outcome::Reply(refused(codes::STALE_BROKER_EPOCH));
    };
    let heartbeat = Heartbeat {
        broker_id: NodeId(broker),
        broker_epoch,
        current_metadata_offset,
        want_fence,
        want_shutdown: want_shut_down,
    };
    let was_fenced = active
        .image
        .broker(crate::lab::controller::broker_id(NodeId(broker)))
        .is_some_and(|b| b.fenced);
    match active
        .decisions
        .broker_heartbeat(&active.image, &heartbeat, now)
    {
        Ok(outcome) => {
            if was_fenced != outcome.is_fenced {
                ctx.event(
                    "broker_fence_changed",
                    json!({
                        "broker": broker_id, "fenced": outcome.is_fenced,
                        "level": if outcome.is_fenced { "warn" } else { "info" },
                    }),
                );
            }
            let response = BrokerHeartbeatResponse {
                error_code: codes::NONE,
                is_caught_up: outcome.is_caught_up,
                is_fenced: outcome.is_fenced,
                should_shut_down: outcome.should_shut_down,
                ..BrokerHeartbeatResponse::default()
            };
            node.controller_write(ctx, req, outcome.records, response, |_| {
                refused(codes::NOT_CONTROLLER)
            })
        }
        Err(code) => Outcome::Reply(refused(code)),
    }
}
