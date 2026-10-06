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
//! - [`AdminNode`] (`"admin"`) creates the scenario's topics and observes
//!   the cluster.
//! - [`RebalancerNode`] (`"rebalancer"`) evens out replicas and leaders
//!   across the brokers.
//!
//! [`templates`] renders record keys and values, [`serde`] frames values in
//! the Confluent wire format, [`registry_client`] talks to a schema registry
//! over HTTP, and [`topology`] compiles a streams node's topology spec.

mod admin;
mod consumer;
mod producer;
mod rebalancer;
pub mod registry_client;
pub mod serde;
pub mod streams;
pub mod templates;
pub mod topology;

pub use self::{
    admin::AdminNode,
    consumer::{ConsumerNode, PartitionRow, Processing, partition_rows},
    producer::{ProducerNode, Rate, RateMeter},
    rebalancer::RebalancerNode,
    streams::StreamsNode,
};

/// Scenario ACL schema and the admin provisioning path.
pub use admin::acls;
