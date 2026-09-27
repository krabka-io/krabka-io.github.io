//! The application nodes: producer, consumer, streams, and the scenario's
//! admin client.
//!
//! Each node is a thin layer over the lab's Kafka client
//! ([`client`](super::client)): config parsing, timers, templates,
//! serialization through a schema registry, control commands and snapshots.
//!
//! - [`ProducerNode`] (`"producer"`) writes templated records at a rate.
//! - [`ConsumerNode`] (`"consumer"`) is a consumer group member that
//!   processes what it polls one record at a time.
//! - [`StreamsNode`] (`"streams"`) runs a `krabka-client-streams` topology
//!   as a KIP-1071 streams group member.
//! - [`AdminNode`] (`"admin"`) creates the scenario's topics.
//!
//! [`templates`] renders record keys and values, [`serde`] frames values in
//! the Confluent wire format, and [`registry_client`] talks to a schema
//! registry over HTTP.

use serde_json::Value;

use super::{
    LabError,
    net::{Ctx, Frame, Node},
    scenario::NodeSpec,
};

mod admin;
mod consumer;
mod producer;
pub mod registry_client;
pub mod serde;
pub mod templates;
pub mod topology;

pub use self::{
    admin::AdminNode,
    consumer::{ConsumerNode, PartitionRow, Processing, partition_rows},
    producer::{ProducerNode, Rate, RateMeter},
};

macro_rules! stub_node {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("A `", $kind, "` node.")]
        pub struct $name;

        impl $name {
            /// # Errors
            /// Always fails until the node is implemented.
            pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
                Err(LabError::config(
                    spec,
                    concat!("the ", $kind, " node is not implemented yet"),
                ))
            }
        }

        impl Node for $name {
            fn kind(&self) -> &'static str {
                $kind
            }
            fn start(&mut self, _ctx: &mut Ctx<'_>) {}
            fn on_frame(&mut self, _ctx: &mut Ctx<'_>, _frame: Frame) {}
            fn on_timer(&mut self, _ctx: &mut Ctx<'_>) {}
            fn control(&mut self, _ctx: &mut Ctx<'_>, _command: Value) -> Result<Value, String> {
                Err("not implemented".to_string())
            }
            fn snapshot(&self) -> Value {
                Value::Null
            }
        }
    };
}

stub_node!(StreamsNode, "streams");
