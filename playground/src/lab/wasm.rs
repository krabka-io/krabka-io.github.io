//! The `wasm-bindgen` surface of the lab.
//!
//! Every argument and result is a JSON string, so the page never sees a Rust
//! type and the same JSON travels unchanged between browser tabs. Bytes inside
//! JSON are base64.

use wasm_bindgen::prelude::*;

use std::collections::BTreeMap;

use serde::Serialize;

use super::{
    net::{DurableImage, DurableOp, Frame, NodeId, TimedFrame},
    scenario::{NodeSpec, Scenario},
    world::{Fault, World},
};

/// One durable op with the node it belongs to, as `drainDurable` returns it.
#[derive(Serialize)]
struct NodeDurableOp {
    node: NodeId,
    #[serde(flatten)]
    op: DurableOp,
}

/// One lab world, as the page holds it.
#[wasm_bindgen]
pub struct Lab {
    world: World,
}

fn js<E: std::fmt::Display>(e: E) -> JsError {
    JsError::new(&e.to_string())
}

#[wasm_bindgen]
impl Lab {
    /// An empty world with `seed`.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(seed: u64) -> Self {
        console_error_panic_hook::set_once();
        Self {
            world: World::new(seed),
        }
    }

    /// Replace the world with one built from a scenario document.
    ///
    /// # Errors
    /// Returns the scenario error as a JavaScript error.
    #[wasm_bindgen(js_name = loadScenario)]
    pub fn load_scenario(&mut self, json: &str) -> Result<(), JsError> {
        let scenario: Scenario = serde_json::from_str(json).map_err(js)?;
        self.world = World::from_scenario(&scenario).map_err(js)?;
        Ok(())
    }

    /// Replace the world with one built from a scenario document, running only
    /// the nodes in the JSON array of ids (an empty array runs all).
    ///
    /// # Errors
    /// Returns the scenario error as a JavaScript error.
    #[wasm_bindgen(js_name = loadScenarioHosted)]
    pub fn load_scenario_hosted(&mut self, json: &str, ids_json: &str) -> Result<(), JsError> {
        let scenario: Scenario = serde_json::from_str(json).map_err(js)?;
        let ids: Vec<u32> = serde_json::from_str(ids_json).map_err(js)?;
        let ids: Vec<NodeId> = ids.into_iter().map(NodeId).collect();
        self.world = World::from_scenario_hosted(&scenario, &ids).map_err(js)?;
        Ok(())
    }

    /// Replace the world with one built from a scenario document, running only
    /// the nodes in the JSON array of ids (an empty array runs all), and hand
    /// each node listed in `images_json` (`{"<node id>": DurableImage}`) the
    /// durable state the page restored from `IndexedDB` before it starts.
    ///
    /// # Errors
    /// Returns the scenario error as a JavaScript error.
    #[wasm_bindgen(js_name = loadScenarioWithState)]
    pub fn load_scenario_with_state(
        &mut self,
        json: &str,
        ids_json: &str,
        images_json: &str,
    ) -> Result<(), JsError> {
        let scenario: Scenario = serde_json::from_str(json).map_err(js)?;
        let ids: Vec<u32> = serde_json::from_str(ids_json).map_err(js)?;
        let ids: Vec<NodeId> = ids.into_iter().map(NodeId).collect();
        let images: BTreeMap<u32, DurableImage> = serde_json::from_str(images_json).map_err(js)?;
        let images = images
            .into_iter()
            .map(|(id, image)| (NodeId(id), image))
            .collect();
        self.world = World::from_scenario_with_state(&scenario, &ids, images).map_err(js)?;
        Ok(())
    }

    /// Durable-state ops recorded since the last drain, as a JSON array of
    /// `{"node": id, "op": "append"|"truncate_before"|"truncate_from"|"put"|"delete"|"clear"|"clear_all", ...}`
    /// objects, in order. The page writes them to `IndexedDB`.
    ///
    /// # Errors
    /// Returns an error when the ops cannot be serialized.
    #[wasm_bindgen(js_name = drainDurable)]
    pub fn drain_durable(&mut self) -> Result<String, JsError> {
        let ops: Vec<NodeDurableOp> = self
            .world
            .drain_durable()
            .into_iter()
            .map(|(node, op)| NodeDurableOp { node, op })
            .collect();
        serde_json::to_string(&ops).map_err(js)
    }

    /// The current scenario document, positions included.
    ///
    /// # Errors
    /// Returns an error when the document cannot be serialized.
    pub fn scenario(&self) -> Result<String, JsError> {
        serde_json::to_string(&self.world.scenario()).map_err(js)
    }

    /// Add a node from a `NodeSpec` JSON object and return its id.
    ///
    /// # Errors
    /// Returns the configuration error as a JavaScript error.
    #[wasm_bindgen(js_name = addNode)]
    pub fn add_node(&mut self, spec_json: &str) -> Result<u32, JsError> {
        let spec: NodeSpec = serde_json::from_str(spec_json).map_err(js)?;
        self.world.add_node(spec).map(|id| id.0).map_err(js)
    }

    #[wasm_bindgen(js_name = removeNode)]
    pub fn remove_node(&mut self, id: u32) {
        self.world.remove_node(NodeId(id));
    }

    /// Replace a node's spec; the node restarts from nothing.
    ///
    /// # Errors
    /// Returns the configuration error as a JavaScript error.
    #[wasm_bindgen(js_name = updateNode)]
    pub fn update_node(&mut self, id: u32, spec_json: &str) -> Result<(), JsError> {
        let spec: NodeSpec = serde_json::from_str(spec_json).map_err(js)?;
        self.world.update_node(NodeId(id), spec).map_err(js)
    }

    #[wasm_bindgen(js_name = setPosition)]
    pub fn set_position(&mut self, id: u32, x: f64, y: f64) {
        self.world.set_position(NodeId(id), x, y);
    }

    #[must_use]
    pub fn now(&self) -> u64 {
        self.world.now()
    }

    /// Run everything due at or before `until_ms` and return the step count.
    #[wasm_bindgen(js_name = stepUntil)]
    pub fn step_until(&mut self, until_ms: u64) -> u32 {
        u32::try_from(self.world.step_until(until_ms)).unwrap_or(u32::MAX)
    }

    /// Whether anything is due at or before `until_ms`.
    #[wasm_bindgen(js_name = hasWorkBy)]
    #[must_use]
    pub fn has_work_by(&self, until_ms: u64) -> bool {
        self.world.has_work_by(until_ms)
    }

    /// Apply a `Fault` JSON object.
    ///
    /// # Errors
    /// Returns a parse error as a JavaScript error.
    pub fn fault(&mut self, json: &str) -> Result<(), JsError> {
        let fault: Fault = serde_json::from_str(json).map_err(js)?;
        self.world.fault(fault);
        Ok(())
    }

    /// Send a control command to a node and return its JSON answer.
    ///
    /// # Errors
    /// Returns the node's error text as a JavaScript error.
    pub fn control(&mut self, id: u32, json: &str) -> Result<String, JsError> {
        let command: serde_json::Value = serde_json::from_str(json).map_err(js)?;
        let answer = self.world.control(NodeId(id), command).map_err(js)?;
        serde_json::to_string(&answer).map_err(js)
    }

    /// The `WorldSnapshot` JSON.
    ///
    /// # Errors
    /// Returns an error when the snapshot cannot be serialized.
    pub fn snapshot(&self) -> Result<String, JsError> {
        serde_json::to_string(&self.world.snapshot()).map_err(js)
    }

    /// The events with an index of at least `index`, as a JSON array.
    ///
    /// # Errors
    /// Returns an error when the events cannot be serialized.
    #[wasm_bindgen(js_name = eventsSince)]
    pub fn events_since(&self, index: usize) -> Result<String, JsError> {
        serde_json::to_string(&self.world.events_since(index)).map_err(js)
    }

    #[wasm_bindgen(js_name = eventCount)]
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.world.event_count()
    }

    // ---- distributed hosting ----------------------------------------------------

    /// Run only the nodes in the JSON array of ids; an empty array runs all.
    ///
    /// # Errors
    /// Returns a parse error as a JavaScript error.
    #[wasm_bindgen(js_name = setHosted)]
    pub fn set_hosted(&mut self, ids_json: &str) -> Result<(), JsError> {
        let ids: Vec<u32> = serde_json::from_str(ids_json).map_err(js)?;
        let ids: Vec<NodeId> = ids.into_iter().map(NodeId).collect();
        self.world.set_hosted(&ids);
        Ok(())
    }

    /// The JSON array of ids this world runs.
    ///
    /// # Errors
    /// Returns an error when the list cannot be serialized.
    pub fn hosted(&self) -> Result<String, JsError> {
        serde_json::to_string(&self.world.hosted()).map_err(js)
    }

    /// Frames for nodes hosted elsewhere, as a JSON array of `TimedFrame`.
    ///
    /// # Errors
    /// Returns an error when the frames cannot be serialized.
    #[wasm_bindgen(js_name = drainEgress)]
    pub fn drain_egress(&mut self) -> Result<String, JsError> {
        let frames: Vec<TimedFrame> = self.world.drain_egress();
        serde_json::to_string(&frames).map_err(js)
    }

    /// Deliver frames that arrived from another peer: a JSON array of `Frame`.
    ///
    /// # Errors
    /// Returns a parse error as a JavaScript error.
    #[wasm_bindgen(js_name = pushIngress)]
    pub fn push_ingress(&mut self, json: &str) -> Result<(), JsError> {
        let frames: Vec<Frame> = serde_json::from_str(json).map_err(js)?;
        self.world.push_ingress(frames);
        Ok(())
    }

    /// Record the state another peer reported for a node it runs.
    ///
    /// # Errors
    /// Returns a parse error as a JavaScript error.
    #[wasm_bindgen(js_name = applyRemoteSnapshot)]
    pub fn apply_remote_snapshot(&mut self, id: u32, json: &str) -> Result<(), JsError> {
        let snapshot: serde_json::Value = serde_json::from_str(json).map_err(js)?;
        self.world.apply_remote_snapshot(NodeId(id), snapshot);
        Ok(())
    }
}
