//! The application nodes: producer, consumer, streams, and the scenario's
//! admin client.
//!
//! Not built yet: every constructor rejects its spec until the apps land.

use serde_json::Value;

use super::{
    LabError,
    net::{Ctx, Frame, Node},
    scenario::NodeSpec,
};

mod admin;

pub use self::admin::AdminNode;

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

stub_node!(ProducerNode, "producer");
stub_node!(ConsumerNode, "consumer");
stub_node!(StreamsNode, "streams");
