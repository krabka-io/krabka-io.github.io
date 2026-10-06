//! The scenario: the JSON document that describes a lab world.
//!
//! The page edits a scenario in the builder and hands it to the world; the
//! world echoes it back (positions included) so a session can be saved and
//! shared. Node configuration under `config` belongs to the node kind's
//! module; the world only carries it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    net::{Millis, NodeId},
    security::SecurityMode,
};

/// The scenario document version this crate reads and writes.
pub const SCENARIO_VERSION: u32 = 1;

/// The default one-way link latency when the scenario does not set one.
pub const DEFAULT_LATENCY_MS: Millis = 5;

/// A whole lab world, as the page saves and loads it.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub version: u32,
    /// A stable identity the page assigns, which keys the durable state it
    /// keeps in `IndexedDB`. Empty means the scenario is not persisted.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default)]
    pub seed: u64,
    /// SSPI transport adapters at each node, outside the application/broker.
    #[serde(default, skip_serializing_if = "SecurityMode::is_plaintext")]
    pub security: SecurityMode,
    /// Initial Kafka ACL records; omitted leaves the broker authorizer disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<super::apps::acls::Authorization>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub links: LinkDefaults,
    #[serde(default)]
    pub link_overrides: Vec<LinkOverride>,
    pub nodes: Vec<NodeSpec>,
    /// Topics created through an admin client once the cluster has a
    /// controller, the way `kafka-topics --create` would.
    #[serde(default)]
    pub topics: Vec<TopicSpec>,
    /// A scripted experiment the page runs against this scenario: timed
    /// faults and commands, and the checks that judge the run. The world
    /// keeps it as it came and gives it back, so it travels with the
    /// scenario through saves and share links.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experiment: Option<serde_json::Value>,
}

impl Scenario {
    /// An empty scenario with `seed`.
    #[must_use]
    pub fn empty(seed: u64) -> Self {
        Self {
            version: SCENARIO_VERSION,
            id: String::new(),
            seed,
            security: SecurityMode::default(),
            authorization: None,
            name: String::new(),
            links: LinkDefaults::default(),
            link_overrides: Vec::new(),
            nodes: Vec::new(),
            topics: Vec::new(),
            experiment: None,
        }
    }
}

/// Link parameters that apply to every pair without an override.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkDefaults {
    pub default_latency_ms: Millis,
}

impl Default for LinkDefaults {
    fn default() -> Self {
        Self {
            default_latency_ms: DEFAULT_LATENCY_MS,
        }
    }
}

/// Link parameters for one pair of nodes. Links are symmetric.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkOverride {
    pub a: NodeId,
    pub b: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<Millis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loss_permille: Option<u32>,
    #[serde(default)]
    pub cut: bool,
}

/// One node of the scenario.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    pub id: NodeId,
    pub kind: String,
    #[serde(default)]
    pub name: String,
    /// Canvas position. The crate stores and echoes it and never reads it.
    #[serde(default)]
    pub x: f64,
    #[serde(default)]
    pub y: f64,
    #[serde(default)]
    pub config: serde_json::Value,
}

impl NodeSpec {
    /// A spec with a config, for tests and the builder.
    #[must_use]
    pub fn new(id: u32, kind: &str, name: &str, config: serde_json::Value) -> Self {
        Self {
            id: NodeId(id),
            kind: kind.to_string(),
            name: name.to_string(),
            x: 0.0,
            y: 0.0,
            config,
        }
    }

    /// The name shown on the canvas: the configured name, or `kind-id`.
    #[must_use]
    pub fn display_name(&self) -> String {
        if self.name.is_empty() {
            format!("{}-{}", self.kind, self.id)
        } else {
            self.name.clone()
        }
    }
}

/// A topic the scenario creates at start.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicSpec {
    pub name: String,
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: i16,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configs: BTreeMap<String, String>,
}

fn default_partitions() -> i32 {
    1
}

fn default_replication_factor() -> i16 {
    -1
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn scenario_parses_with_defaults_and_rejects_unknown_keys() {
        let s: Scenario = serde_json::from_str(
            r#"{"version":1,"nodes":[{"id":1,"kind":"krabka-broker","config":{"broker_id":1}}],
                "topics":[{"name":"orders"}]}"#,
        )
        .unwrap();
        assert!(s.seed == 0);
        assert!(s.links.default_latency_ms == DEFAULT_LATENCY_MS);
        assert!(s.nodes[0].display_name() == "krabka-broker-1");
        assert!(s.topics[0].partitions == 1);
        assert!(s.topics[0].replication_factor == -1);

        let err = serde_json::from_str::<Scenario>(r#"{"version":1,"nodes":[],"bogus":1}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("bogus"));
    }

    #[test]
    fn scenario_round_trips_positions_and_overrides() {
        let mut s = Scenario::empty(9);
        let mut spec = NodeSpec::new(3, "consumer", "billing", serde_json::json!({"group": "g"}));
        spec.x = 12.5;
        spec.y = -3.0;
        s.nodes.push(spec);
        s.link_overrides.push(LinkOverride {
            a: NodeId(1),
            b: NodeId(3),
            latency_ms: Some(40),
            loss_permille: None,
            cut: true,
        });
        let json = serde_json::to_string(&s).unwrap();
        let back: Scenario = serde_json::from_str(&json).unwrap();
        assert!(back == s);
    }
}
