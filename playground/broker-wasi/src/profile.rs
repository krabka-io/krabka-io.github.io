//! The broker's configuration in the lab: a profile built from the process
//! contract, with `KRABKA_CONFIG` applied on top through the broker's own
//! `broker.toml` schema.

use std::{net::SocketAddr, path::PathBuf};

use krabka_broker::{
    BootstrapMode, BrokerConfig,
    bootstrap::MetaProperties,
    config::NodeRole,
    file_config::{FileAuditConfig, FileConfigError},
};

use crate::contract::{CONTROLLER_PORT, Contract, KAFKA_PORT};

/// The broker's log directory on the node's volume, which the runtime mounts
/// at `/data`. It holds the metadata log and every partition.
pub const LOG_DIR: &str = "/data/log";

/// The node's `BrokerConfig`.
///
/// A voter is a combined controller and broker, `process.roles` =
/// `controller,broker`; any other node is a broker alone. The broker listens
/// on `KRABKA_HOST:9092` and advertises its loopback bridge address, a voter's controller
/// on `KRABKA_HOST:9093`, and the controller quorum is the static
/// `KRABKA_VOTERS`. The cluster and directory ids come from the volume's
/// `meta.properties`. Everything that needs a network stack or a thread
/// `wasm32-wasip1` does not have (metrics, OTLP, JWKS, OPA, schema
/// validation, tiered storage over the topic-based RLMM) stays unset, as the
/// broker's defaults leave it, and so does the audit log: `broker.toml`
/// without an `[audit]` table means audit on, so the profile adds the table
/// with `enabled = false` to a `KRABKA_CONFIG` that has none.
///
/// # Errors
/// Returns the error of applying `KRABKA_CONFIG`, a value the broker refuses.
pub fn broker_config(
    contract: &Contract,
    formatted: &MetaProperties,
    bootstrap_mode: BootstrapMode,
) -> Result<BrokerConfig, FileConfigError> {
    let listen_addr = SocketAddr::new(contract.host, KAFKA_PORT);
    let mut config = BrokerConfig {
        broker_id: contract.node_id,
        node_id: contract.raft_node_id(),
        roles: if contract.is_voter() {
            vec![NodeRole::Controller, NodeRole::Broker]
        } else {
            vec![NodeRole::Broker]
        },
        listen_addr,
        advertised_listener: format!("127.0.0.1:{}", 9091 + contract.node_id),
        controller_listen_addr: SocketAddr::new(contract.host, CONTROLLER_PORT),
        controller_quorum_voters: contract.voters.clone(),
        log_dir: PathBuf::from(LOG_DIR),
        cluster_id: Some(formatted.cluster_id),
        directory_id: formatted.directory_id,
        bootstrap_mode,
        ..BrokerConfig::default()
    };
    let mut file_config = contract.file_config.clone();
    file_config.audit.get_or_insert_with(|| FileAuditConfig {
        enabled: false,
        ..FileAuditConfig::default()
    });
    file_config.apply_to(&mut config)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use assert2::assert;
    use krabka_broker::{NodeId, file_config::FileConfig};
    use krabka_units::convert::TimeExt as _;
    use uuid::Uuid;

    use super::*;
    use crate::contract::Listener;

    /// What the profile decides of a `BrokerConfig`.
    #[derive(Debug, PartialEq)]
    struct Profile {
        broker_id: i32,
        node_id: NodeId,
        roles: Vec<NodeRole>,
        listen_addr: SocketAddr,
        advertised_listener: String,
        controller_listen_addr: SocketAddr,
        controller_quorum_voters: Vec<(NodeId, String)>,
        log_dir: PathBuf,
        cluster_id: Option<Uuid>,
        directory_id: Uuid,
        bootstrap_mode: BootstrapMode,
        audit_enabled: bool,
        heartbeat_timeout: Duration,
        rack: Option<String>,
        num_partitions: i32,
        default_replication_factor: i16,
        default_min_insync_replicas: i32,
        replica_lag_time_max: Duration,
    }

    impl From<&BrokerConfig> for Profile {
        fn from(config: &BrokerConfig) -> Self {
            Self {
                broker_id: config.broker_id,
                node_id: config.node_id,
                roles: config.roles.clone(),
                listen_addr: config.listen_addr,
                advertised_listener: config.advertised_listener.clone(),
                controller_listen_addr: config.controller_listen_addr,
                controller_quorum_voters: config.controller_quorum_voters.clone(),
                log_dir: config.log_dir.clone(),
                cluster_id: config.cluster_id,
                directory_id: config.directory_id,
                bootstrap_mode: config.bootstrap_mode,
                audit_enabled: config.audit_enabled,
                heartbeat_timeout: config.heartbeat_timeout.to_std(),
                rack: config.rack.clone(),
                num_partitions: config.num_partitions,
                default_replication_factor: config.default_replication_factor,
                default_min_insync_replicas: config.default_min_insync_replicas,
                replica_lag_time_max: config.replica_lag_time_max.to_std(),
            }
        }
    }

    const CLUSTER: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    const DIRECTORY: Uuid = Uuid::from_u128(0x2a);

    fn voters() -> Vec<(NodeId, String)> {
        (1..=3)
            .map(|id| (NodeId(id), format!("10.0.0.{id}:9093")))
            .collect()
    }

    fn contract(node_id: i32, file_config: &str) -> Contract {
        Contract {
            node_id,
            host: IpAddr::V4(Ipv4Addr::new(10, 0, 0, u8::try_from(node_id).unwrap())),
            listeners: vec![
                Listener { fd: 4, port: 9092 },
                Listener { fd: 5, port: 9093 },
            ],
            dial_fd: 6,
            voters: voters(),
            cluster_id: CLUSTER,
            file_config: serde_json::from_str::<FileConfig>(file_config).unwrap(),
        }
    }

    fn formatted() -> MetaProperties {
        MetaProperties {
            cluster_id: CLUSTER,
            directory_id: DIRECTORY,
            version: krabka_broker::bootstrap::META_PROPERTIES_VERSION,
        }
    }

    /// The profile of node `id` of the three-voter quorum, before `KRABKA_CONFIG`.
    fn expected(id: u8, roles: Vec<NodeRole>, bootstrap_mode: BootstrapMode) -> Profile {
        let defaults = Profile::from(&BrokerConfig::default());
        let host = IpAddr::V4(Ipv4Addr::new(10, 0, 0, id));
        Profile {
            broker_id: i32::from(id),
            node_id: NodeId(u64::from(id)),
            roles,
            listen_addr: SocketAddr::new(host, 9092),
            advertised_listener: format!("127.0.0.1:{}", 9091 + id),
            controller_listen_addr: SocketAddr::new(host, 9093),
            controller_quorum_voters: voters(),
            log_dir: PathBuf::from("/data/log"),
            cluster_id: Some(CLUSTER),
            directory_id: DIRECTORY,
            bootstrap_mode,
            audit_enabled: false,
            ..defaults
        }
    }

    #[test]
    fn a_voter_is_a_combined_node_and_any_other_a_broker() {
        let cases = [
            (
                2,
                BootstrapMode::Bootstrap,
                expected(
                    2,
                    vec![NodeRole::Controller, NodeRole::Broker],
                    BootstrapMode::Bootstrap,
                ),
            ),
            (
                2,
                BootstrapMode::Rejoin,
                expected(
                    2,
                    vec![NodeRole::Controller, NodeRole::Broker],
                    BootstrapMode::Rejoin,
                ),
            ),
            (
                4,
                BootstrapMode::Bootstrap,
                expected(4, vec![NodeRole::Broker], BootstrapMode::Bootstrap),
            ),
        ];
        for (node, mode, profile) in cases {
            let config = broker_config(&contract(node, "{}"), &formatted(), mode).unwrap();
            assert!(Profile::from(&config) == profile, "node {node}, {mode:?}");
        }
    }

    #[test]
    fn krabka_config_lands_through_the_broker_toml_schema() {
        let config = broker_config(
            &contract(
                2,
                r#"{"rack":"a","runtime":{"num_partitions":3,"default_replication_factor":3,"default_min_insync_replicas":2},"replica_lag_time_max":"10000ms"}"#,
            ),
            &formatted(),
            BootstrapMode::Bootstrap,
        )
        .unwrap();
        assert!(
            Profile::from(&config)
                == Profile {
                    rack: Some("a".to_owned()),
                    num_partitions: 3,
                    default_replication_factor: 3,
                    default_min_insync_replicas: 2,
                    replica_lag_time_max: Duration::from_secs(10),
                    ..expected(
                        2,
                        vec![NodeRole::Controller, NodeRole::Broker],
                        BootstrapMode::Bootstrap
                    )
                }
        );
    }

    #[test]
    fn the_audit_log_is_off_unless_krabka_config_turns_it_on() {
        let cases = [("{}", false), (r#"{"audit":{"enabled":true}}"#, true)];
        for (file_config, audit_enabled) in cases {
            let config = broker_config(
                &contract(2, file_config),
                &formatted(),
                BootstrapMode::Bootstrap,
            )
            .unwrap();
            assert!(config.audit_enabled == audit_enabled, "{file_config}");
        }
    }

    #[test]
    fn a_value_the_broker_refuses_is_an_error() {
        let refused = broker_config(
            &contract(2, r#"{"replica_lag_time_max":"0ms"}"#),
            &formatted(),
            BootstrapMode::Bootstrap,
        );
        assert!(
            refused.map(|_| ()).map_err(|err| err.to_string())
                == Err(
                    "invalid config: replica_lag_time_max: must be finite and positive".to_owned()
                )
        );
    }
}
