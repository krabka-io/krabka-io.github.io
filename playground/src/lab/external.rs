//! Nodes that run outside the world.
//!
//! A real `krabka-broker` compiled for `wasm32-wasip1` runs in a Web Worker
//! the page hosts, not inside this crate. The world keeps a slot for it, so
//! the network, the faults and the snapshots treat it like any other node,
//! but the frames it receives leave through [`World::drain_external`] and the
//! frames it sends come back through [`World::route_external`]. The page reads
//! the node's configuration from its [`NodeSpec`] and reports its state with
//! [`World::apply_remote_snapshot`].
//!
//! [`World::drain_external`]: crate::lab::world::World::drain_external
//! [`World::route_external`]: crate::lab::world::World::route_external
//! [`World::apply_remote_snapshot`]: crate::lab::world::World::apply_remote_snapshot

use serde_json::{Value, json};

use crate::lab::{
    LabError,
    net::{Ctx, Frame, Node},
    scenario::NodeSpec,
};

/// The node kind of a real broker the page runs in a Worker.
pub const REAL_BROKER_KIND: &str = "krabka-broker";

/// The world's stand-in for a process the page hosts.
#[derive(Debug)]
pub struct ExternalNode {
    kind: &'static str,
}

impl ExternalNode {
    /// The stand-in for `spec`. The configuration belongs to the page, which
    /// validates it when it starts the process.
    ///
    /// # Errors
    /// Returns an error for a kind that is not an external kind.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        match spec.kind.as_str() {
            REAL_BROKER_KIND => Ok(Self {
                kind: REAL_BROKER_KIND,
            }),
            other => Err(LabError::UnknownNodeKind(other.to_string())),
        }
    }
}

impl Node for ExternalNode {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn external(&self) -> bool {
        true
    }

    fn start(&mut self, _ctx: &mut Ctx<'_>) {}

    fn on_frame(&mut self, _ctx: &mut Ctx<'_>, _frame: Frame) {}

    fn on_timer(&mut self, _ctx: &mut Ctx<'_>) {}

    fn control(&mut self, _ctx: &mut Ctx<'_>, _command: Value) -> Result<Value, String> {
        Err(format!(
            "a {} runs in a process the page hosts; send it commands through the page",
            self.kind
        ))
    }

    fn snapshot(&self) -> Value {
        json!({ "external": true })
    }
}
