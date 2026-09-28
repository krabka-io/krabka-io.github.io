//! The Cluster Lab: a sans-IO simulation of Krabka brokers, a schema registry,
//! and Kafka client applications, driven from the browser.
//!
//! Every node is a synchronous state machine behind [`net::Node`]. The
//! [`world::World`] owns the logical clock, the link model and the delivery
//! queue; the page owns wall-clock time and, for a session spread over several
//! browser tabs, the WebRTC transport between worlds. The design and the
//! contract between the modules are in `playground/docs/lab-design.md`.
//!
//! # Modules
//!
//! - [`net`] — frames, endpoints, the node trait and the node-side context.
//! - [`world`] — the scheduler, links, faults and distributed hosting.
//! - [`scenario`] — the JSON document that describes a world.
//! - [`events`] — the bounded timeline.
//! - [`codes`] — Kafka error codes.
//! - [`broker`], [`controller`], [`registry`], [`client`], [`apps`] — the node
//!   kinds.
//! - [`testing`] — the test harness and the diagnostic node kinds.
//! - [`wasm`] — the `wasm-bindgen` surface the page calls.

use thiserror::Error;

pub mod apps;
pub mod broker;
pub mod client;
pub mod codes;
pub mod controller;
pub mod events;
pub mod external;
pub mod net;
pub mod registry;
pub mod scenario;
pub mod testing;
pub mod wasm;
pub mod world;

pub use self::{
    net::{Ctx, Endpoint, Frame, Millis, Node, NodeId, Payload},
    scenario::{NodeSpec, Scenario},
    world::{Fault, World},
};

/// What can go wrong when a world is built or changed.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LabError {
    #[error("invalid scenario: {0}")]
    InvalidScenario(String),
    #[error("unknown node kind `{0}`")]
    UnknownNodeKind(String),
    #[error("no node {0}")]
    NoSuchNode(NodeId),
    /// A node rejected its `config`.
    #[error("node {id} ({kind}): {reason}")]
    Config {
        id: NodeId,
        kind: String,
        reason: String,
    },
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

impl LabError {
    /// A configuration error for the node `spec` describes.
    #[must_use]
    pub fn config(spec: &NodeSpec, reason: impl Into<String>) -> Self {
        Self::Config {
            id: spec.id,
            kind: spec.kind.clone(),
            reason: reason.into(),
        }
    }
}

/// Build the node a spec describes. This is the one place a node kind is
/// named; every module exposes a `from_spec` constructor.
///
/// # Errors
/// Returns an error when the kind is unknown or the node rejects its config.
pub fn build_node(spec: &NodeSpec) -> Result<Box<dyn Node>, LabError> {
    Ok(match spec.kind.as_str() {
        "broker" => Box::new(broker::BrokerNode::from_spec(spec)?),
        "schema-registry" => Box::new(registry::RegistryNode::from_spec(spec)?),
        "producer" => Box::new(apps::ProducerNode::from_spec(spec)?),
        "consumer" => Box::new(apps::ConsumerNode::from_spec(spec)?),
        "streams" => Box::new(apps::StreamsNode::from_spec(spec)?),
        "admin" => Box::new(apps::AdminNode::from_spec(spec)?),
        "echo" => Box::new(testing::EchoNode::from_spec(spec)?),
        "pinger" => Box::new(testing::PingerNode::from_spec(spec)?),
        external::REAL_BROKER_KIND | external::LOCAL_CLIENT_KIND => {
            Box::new(external::ExternalNode::from_spec(spec)?)
        }
        other => return Err(LabError::UnknownNodeKind(other.to_string())),
    })
}

/// Read a required field of a node config.
///
/// # Errors
/// Returns a config error naming the field when it is absent or of the wrong
/// type.
pub fn config_field<T: serde::de::DeserializeOwned>(
    spec: &NodeSpec,
    field: &str,
) -> Result<T, LabError> {
    let value = spec
        .config
        .get(field)
        .ok_or_else(|| LabError::config(spec, format!("missing config field `{field}`")))?;
    serde_json::from_value(value.clone())
        .map_err(|e| LabError::config(spec, format!("config field `{field}`: {e}")))
}

/// Read an optional field of a node config, with a default.
///
/// # Errors
/// Returns a config error naming the field when it is present but of the
/// wrong type.
pub fn config_field_or<T: serde::de::DeserializeOwned>(
    spec: &NodeSpec,
    field: &str,
    default: T,
) -> Result<T, LabError> {
    match spec.config.get(field) {
        None | Some(serde_json::Value::Null) => Ok(default),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|e| LabError::config(spec, format!("config field `{field}`: {e}"))),
    }
}
