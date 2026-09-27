//! The `KRaft` controller: the quorum driver over `krabka-kraft-core`, the
//! replicated metadata log, and the decisions the active controller takes.
//!
//! A broker embeds one [`ControllerCore`] and hands it the frames that arrive
//! on [`RAFT_PORT`] and the timer ticks it asks for. The core runs the real
//! consensus state machine, replicates the [`MetadataLog`] between the
//! brokers, and hands every committed batch back through
//! [`ControllerCore::take_committed`], so every broker applies the same
//! records to its metadata image in the same order.
//!
//! [`ControllerDecisions`] is what the active controller decides on top of the
//! image: broker registration and fencing, topic creation with round-robin
//! replica placement, partition growth and deletion, `AlterPartition`
//! validation, and leader election when a broker is fenced. It is pure over a
//! [`MetadataImage`](krabka_metadata::MetadataImage) and returns the records
//! to propose.
//!
//! Node ids and broker ids are the same numbers: a lab [`NodeId`] names the
//! node, and [`broker_id`] is the same id as the metadata records and the
//! quorum carry it.

use derive_more::{Display, From, Into};
use krabka_kraft_core::types::Epoch;
use krabka_metadata::MetadataRecord;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod decisions;
mod driver;
mod log;
mod wire;

#[cfg(test)]
mod harness;
#[cfg(test)]
mod tests;

pub use self::{
    decisions::ControllerDecisions,
    driver::{
        BASE_ELECTION_TIMEOUT_MS, ControllerCore, DISCOVERY_RETRY_MS, DurableQuorumState,
        ELECTION_TIMEOUT_STAGGER_MS, FETCH_MAX_WAIT_MS, HEARTBEAT_MS, HIGH_WATERMARK_KEY,
        KRAFT_LOG_STORE, KRAFT_STATE_STORE, MAX_FETCH_ENTRIES, METADATA_TOPIC, QUORUM_STATE_KEY,
        RAFT_CONN_BASE, RECONNECT_BACKOFF_MS, election_timeout_ms,
    },
    log::{Entry, MetadataLog},
    wire::{FetchResponse, LogPoint, MalformedFrame, RaftMessage},
};
use crate::lab::net::NodeId;

/// The port a controller's raft listener sits on. Frames between controllers
/// go from a node's client socket to `Endpoint::new(peer, RAFT_PORT)`.
pub const RAFT_PORT: u16 = 9093;

/// The log offset a proposed batch was appended at. The batch is committed
/// once a [`CommittedBatch`] with this offset comes out of
/// [`ControllerCore::take_committed`].
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ProposalId(pub i64);

/// One committed log entry, with the offset every node agrees on.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedBatch {
    pub offset: i64,
    /// The leader epoch that appended the batch.
    pub epoch: Epoch,
    /// The records, in order. Empty for a leader-change marker.
    pub records: Vec<MetadataRecord>,
}

/// A proposal reached a node that does not lead the quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("not the active controller; the leader is {leader:?}")]
pub struct NotLeader {
    /// The leader this node knows, for the broker to forward to.
    pub leader: Option<NodeId>,
}

/// The broker id of a lab node, as the metadata records and the quorum carry
/// it.
#[must_use]
pub fn broker_id(node: NodeId) -> krabka_ids::NodeId {
    krabka_ids::NodeId(u64::from(node.0))
}

/// The lab node of a broker id, if the id fits a node id.
#[must_use]
pub fn lab_id(id: krabka_ids::NodeId) -> Option<NodeId> {
    u32::try_from(id.0).ok().map(NodeId)
}
