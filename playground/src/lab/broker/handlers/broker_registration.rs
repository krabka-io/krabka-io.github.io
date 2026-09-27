//! `BrokerRegistration` (api key 62) on the controller listener: a broker
//! registers a new incarnation with the active controller.
//!
//! As Kafka's `ClusterControlManager.registerBroker` decides it
//! ([`ControllerDecisions::register_broker`]): another cluster's id answers
//! `INCONSISTENT_CLUSTER_ID`, a registration without a listener
//! `INVALID_REGISTRATION`, and a new incarnation while the previous one
//! still holds a session `DUPLICATE_BROKER_REGISTRATION`. An accepted
//! registration answers the broker epoch, the offset the registration record
//! is written at (KIP-903), once the record commits. A node that is not the
//! active controller answers `NOT_CONTROLLER`.
//!
//! [`ControllerDecisions::register_broker`]: crate::lab::controller::ControllerDecisions::register_broker

use krabka_metadata::{BrokerEndpoint, MetadataRecord};
use krabka_protocol::owned::{
    broker_registration_request::BrokerRegistrationRequest,
    broker_registration_response::BrokerRegistrationResponse,
};
use krabka_security::ListenerProtocol;
use uuid::Uuid;

use super::super::{
    BrokerNode, cluster,
    dispatch::{Outcome, RequestCtx},
};
use crate::lab::{
    codes,
    controller::decisions::RegisterBroker,
    net::{Ctx, NodeId},
};

/// The listener protocol of Kafka's `SecurityProtocol` id.
fn listener_protocol(id: i16) -> ListenerProtocol {
    match id {
        1 => ListenerProtocol::Ssl,
        2 => ListenerProtocol::SaslPlaintext,
        3 => ListenerProtocol::SaslSsl,
        _ => ListenerProtocol::Plaintext,
    }
}

fn refused(error_code: i16) -> BrokerRegistrationResponse {
    BrokerRegistrationResponse {
        error_code,
        ..BrokerRegistrationResponse::default()
    }
}

/// Serve a `BrokerRegistration` as the controller.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    BrokerRegistrationRequest {
        broker_id,
        cluster_id,
        incarnation_id,
        listeners,
        rack,
        ..
    }: BrokerRegistrationRequest,
) -> Outcome<BrokerRegistrationResponse> {
    let now = ctx.now();
    let next_offset = node.quorum.core.log_end_offset();
    let Some(active) = node.quorum.active.as_mut() else {
        return Outcome::Reply(refused(codes::NOT_CONTROLLER));
    };
    let Ok(broker) = u32::try_from(broker_id) else {
        return Outcome::Reply(refused(codes::INVALID_REGISTRATION));
    };
    let registration = RegisterBroker {
        broker_id: NodeId(broker),
        incarnation_id: Uuid::from_bytes(incarnation_id.0),
        cluster_id: cluster::parse_cluster_id(&cluster_id).unwrap_or_else(Uuid::nil),
        rack,
        endpoints: listeners
            .into_iter()
            .map(|listener| BrokerEndpoint {
                protocol: listener_protocol(listener.security_protocol),
                name: listener.name,
                host: listener.host,
                port: listener.port,
            })
            .collect(),
    };
    match active
        .decisions
        .register_broker(&active.image, &registration, now, next_offset)
    {
        Ok(records) => {
            let broker_epoch = records
                .iter()
                .find_map(|record| match record {
                    MetadataRecord::V1BrokerRegistration(r)
                        if cluster::wire_id(r.node_id) == broker_id =>
                    {
                        Some(r.broker_epoch)
                    }
                    _ => None,
                })
                .unwrap_or(next_offset);
            let response = BrokerRegistrationResponse {
                error_code: codes::NONE,
                broker_epoch,
                ..BrokerRegistrationResponse::default()
            };
            node.controller_write(ctx, req, records, response, |_| {
                refused(codes::NOT_CONTROLLER)
            })
        }
        Err(code) => Outcome::Reply(refused(code)),
    }
}
