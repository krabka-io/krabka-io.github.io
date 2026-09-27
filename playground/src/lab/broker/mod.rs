//! The simulated broker.
//!
//! Not built yet: the constructor rejects every spec until the broker lands.

use serde_json::Value;

use super::{
    LabError,
    net::{Ctx, Frame, Node},
    scenario::NodeSpec,
};

/// A broker node.
pub struct BrokerNode;

impl BrokerNode {
    /// # Errors
    /// Always fails until the broker is implemented.
    pub fn from_spec(spec: &NodeSpec) -> Result<Self, LabError> {
        Err(LabError::config(
            spec,
            "the broker node is not implemented yet",
        ))
    }
}

impl Node for BrokerNode {
    fn kind(&self) -> &'static str {
        "broker"
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
