//! `DescribeCluster` (api key 60): the cluster id, the controller and the
//! registered brokers.
//!
//! A broker serves the broker endpoint type (`1`), as Kafka's
//! `AuthHelper.computeDescribeClusterResponse` does: the controller type
//! (`2`) answers `MISMATCHED_ENDPOINT_TYPE` and any other type
//! `UNSUPPORTED_ENDPOINT_TYPE` from v1, and both answer `INVALID_REQUEST` at
//! v0, whose error table predates KIP-919. Fenced brokers appear only when
//! `include_fenced_brokers` asks for them (v2). In `KRaft` the controller id
//! is a random live broker, Kafka's `getRandomAliveBrokerId`, and `-1` when
//! the listed brokers do not include it. The KIP-430 bit field is filled
//! only when the request opts in.

use krabka_protocol::owned::{
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
};

use super::{
    super::{
        BrokerNode, cluster,
        dispatch::{Outcome, RequestCtx},
    },
    CLUSTER_AUTHORIZED_OPERATIONS, NO_AUTHORIZED_OPERATIONS,
};
use crate::lab::{codes, net::Ctx};

/// KIP-919 `endpoint_type`: the broker listener.
const ENDPOINT_TYPE_BROKER: i8 = 1;
/// KIP-919 `endpoint_type`: the controller listener.
const ENDPOINT_TYPE_CONTROLLER: i8 = 2;

/// Serve a `DescribeCluster`.
pub fn handle(
    node: &mut BrokerNode,
    ctx: &mut Ctx<'_>,
    req: &RequestCtx,
    DescribeClusterRequest {
        include_cluster_authorized_operations,
        endpoint_type,
        include_fenced_brokers,
        ..
    }: DescribeClusterRequest,
) -> Outcome<DescribeClusterResponse> {
    let refusal = match endpoint_type {
        ENDPOINT_TYPE_BROKER => None,
        ENDPOINT_TYPE_CONTROLLER => Some((
            codes::MISMATCHED_ENDPOINT_TYPE,
            "The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER".to_string(),
        )),
        other => Some((
            codes::UNSUPPORTED_ENDPOINT_TYPE,
            format!("Unsupported endpoint type {other}"),
        )),
    };
    if let Some((code, message)) = refusal {
        return Outcome::Reply(DescribeClusterResponse {
            error_code: if req.version == 0 {
                codes::INVALID_REQUEST
            } else {
                code
            },
            error_message: Some(message),
            ..DescribeClusterResponse::default()
        });
    }
    let image = node.image();
    let mut brokers: Vec<DescribeClusterBroker> = image
        .brokers()
        .filter(|b| include_fenced_brokers || !b.fenced)
        .map(|b| DescribeClusterBroker {
            broker_id: cluster::wire_id(b.node_id),
            host: b.host.clone(),
            port: i32::from(b.port),
            rack: b.rack.clone(),
            is_fenced: b.fenced,
            ..DescribeClusterBroker::default()
        })
        .collect();
    brokers.sort_by_key(|b| b.broker_id);
    let controller_id = node.random_alive_broker(ctx);
    let controller_id = if brokers.iter().any(|b| b.broker_id == controller_id) {
        controller_id
    } else {
        -1
    };
    Outcome::Reply(DescribeClusterResponse {
        endpoint_type,
        cluster_id: cluster::cluster_id_string(image.cluster_id()),
        controller_id,
        brokers,
        cluster_authorized_operations: if include_cluster_authorized_operations {
            CLUSTER_AUTHORIZED_OPERATIONS
        } else {
            NO_AUTHORIZED_OPERATIONS
        },
        ..DescribeClusterResponse::default()
    })
}
