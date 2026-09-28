//! The process contract: what the lab hands a real broker through its
//! environment (`playground/docs/lab-real-broker.md`).
//!
//! The browser WASI runtime sets `KRABKA_LISTEN_FDS`, `KRABKA_LISTEN_PORTS` and
//! `KRABKA_DIAL_FD`; the lab page sets `KRABKA_NODE_ID`, `KRABKA_HOST`,
//! `KRABKA_VOTERS`, `KRABKA_CLUSTER_ID` and `KRABKA_CONFIG`. [`Contract`] reads
//! all of them at once, so a malformed environment stops the process before
//! it touches its volume.

use std::{collections::BTreeSet, fmt::Display, net::IpAddr, os::fd::RawFd, str::FromStr};

use krabka_broker::{
    NodeId,
    file_config::{FileConfig, parse_quorum_voter},
};
use thiserror::Error;
use uuid::Uuid;

/// The port of the Kafka listener.
pub const KAFKA_PORT: u16 = 9092;
/// The port of the `KRaft` controller listener, which `KRABKA_VOTERS` names.
pub const CONTROLLER_PORT: u16 = 9093;

/// The URL-safe base64 alphabet of a Kafka `Uuid`'s text form.
const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// A listening socket the runtime preopened, and the port the lab routes to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Listener {
    /// The descriptor.
    pub fd: RawFd,
    /// The port on the node's virtual address.
    pub port: u16,
}

/// The environment of a real broker process, checked.
#[derive(Debug, PartialEq)]
pub struct Contract {
    /// `KRABKA_NODE_ID`: the broker's `node.id`, 1 through 10000.
    pub node_id: i32,
    /// `KRABKA_HOST`: the node's virtual address, which it listens on and
    /// advertises.
    pub host: IpAddr,
    /// `KRABKA_LISTEN_FDS` and `KRABKA_LISTEN_PORTS`, zipped in their order.
    pub listeners: Vec<Listener>,
    /// `KRABKA_DIAL_FD`: the dialer every outbound connection goes through.
    pub dial_fd: RawFd,
    /// `KRABKA_VOTERS`: the static KIP-595 controller quorum, as
    /// `controller.quorum.voters` gives it. Empty when the scenario has no
    /// voter.
    pub voters: Vec<(NodeId, String)>,
    /// `KRABKA_CLUSTER_ID`, decoded from Kafka's text form.
    pub cluster_id: Uuid,
    /// `KRABKA_CONFIG`: the JSON form of the broker's `broker.toml`.
    pub file_config: FileConfig,
}

/// Why the environment is not the lab's process contract.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContractError {
    /// A variable of the contract is not set.
    #[error("{0} is not set")]
    Missing(&'static str),
    /// A variable does not hold what the contract says it holds.
    #[error("{name} is {value:?}: {reason}")]
    Invalid {
        /// The variable.
        name: &'static str,
        /// Its value, or the part of it that is wrong.
        value: String,
        /// What is wrong with it.
        reason: String,
    },
    /// `KRABKA_LISTEN_FDS` and `KRABKA_LISTEN_PORTS` differ in length.
    #[error("KRABKA_LISTEN_FDS names {fds} descriptors and KRABKA_LISTEN_PORTS {ports} ports")]
    ListenerCount {
        /// The descriptors named.
        fds: usize,
        /// The ports named.
        ports: usize,
    },
    /// No listener has the port a role of the node needs.
    #[error("no listener has port {port}, which {role} needs")]
    NoListener {
        /// The port.
        port: u16,
        /// Who needs it.
        role: &'static str,
    },
}

impl Contract {
    /// Reads the contract from the process environment.
    ///
    /// # Errors
    /// Returns the first variable that is missing or malformed.
    pub fn from_env() -> Result<Self, ContractError> {
        // A value that is not UTF-8 goes on lossily, and its check says what
        // is wrong with it.
        Self::from_vars(|name| {
            std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
        })
    }

    /// Reads the contract from `var`, which returns a variable's value or
    /// `None` when it is not set.
    ///
    /// # Errors
    /// Returns the first variable that is missing or malformed.
    pub fn from_vars(var: impl Fn(&'static str) -> Option<String>) -> Result<Self, ContractError> {
        let required = |name: &'static str| var(name).ok_or(ContractError::Missing(name));

        let fds: Vec<RawFd> = list("KRABKA_LISTEN_FDS", &required("KRABKA_LISTEN_FDS")?)?;
        let ports: Vec<u16> = list("KRABKA_LISTEN_PORTS", &required("KRABKA_LISTEN_PORTS")?)?;
        if fds.len() != ports.len() {
            return Err(ContractError::ListenerCount {
                fds: fds.len(),
                ports: ports.len(),
            });
        }
        for &fd in &fds {
            check_fd("KRABKA_LISTEN_FDS", fd)?;
        }
        let mut seen = BTreeSet::new();
        for &port in &ports {
            if port == 0 || !seen.insert(port) {
                return Err(ContractError::Invalid {
                    name: "KRABKA_LISTEN_PORTS",
                    value: port.to_string(),
                    reason: "every listener has its own port, from 1 to 65535".to_owned(),
                });
            }
        }
        let listeners: Vec<Listener> = fds
            .into_iter()
            .zip(ports)
            .map(|(fd, port)| Listener { fd, port })
            .collect();
        let dial_fd = check_fd(
            "KRABKA_DIAL_FD",
            one("KRABKA_DIAL_FD", &required("KRABKA_DIAL_FD")?)?,
        )?;

        let node_id: i32 = one("KRABKA_NODE_ID", &required("KRABKA_NODE_ID")?)?;
        if !(1..=10000).contains(&node_id) {
            return Err(ContractError::Invalid {
                name: "KRABKA_NODE_ID",
                value: node_id.to_string(),
                reason: "the lab node id must be between 1 and 10000".to_owned(),
            });
        }
        let host: IpAddr = one("KRABKA_HOST", &required("KRABKA_HOST")?)?;

        let voters = required("KRABKA_VOTERS")?
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                parse_quorum_voter(entry).map_err(|err| ContractError::Invalid {
                    name: "KRABKA_VOTERS",
                    value: entry.to_owned(),
                    reason: err.to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let cluster_text = required("KRABKA_CLUSTER_ID")?;
        let cluster_id = kafka_uuid(&cluster_text).ok_or_else(|| ContractError::Invalid {
            name: "KRABKA_CLUSTER_ID",
            value: cluster_text.clone(),
            reason: "a Kafka cluster id is 16 bytes in 22 characters of URL-safe base64".to_owned(),
        })?;

        let config_text = required("KRABKA_CONFIG")?;
        let file_config: FileConfig =
            serde_json::from_str(&config_text).map_err(|err| ContractError::Invalid {
                name: "KRABKA_CONFIG",
                value: config_text.clone(),
                reason: err.to_string(),
            })?;

        let contract = Self {
            node_id,
            host,
            listeners,
            dial_fd,
            voters,
            cluster_id,
            file_config,
        };
        contract.require_listener(KAFKA_PORT, "the broker")?;
        if contract.is_voter() {
            contract.require_listener(CONTROLLER_PORT, "a controller quorum voter")?;
        }
        Ok(contract)
    }

    /// The node id as the raft layer counts it.
    #[must_use]
    pub fn raft_node_id(&self) -> NodeId {
        NodeId(u64::from(self.node_id.unsigned_abs()))
    }

    /// Whether this node is in `KRABKA_VOTERS`: a combined controller and
    /// broker rather than a broker alone.
    #[must_use]
    pub fn is_voter(&self) -> bool {
        let me = self.raft_node_id();
        self.voters.iter().any(|(id, _)| *id == me)
    }

    fn require_listener(&self, port: u16, role: &'static str) -> Result<(), ContractError> {
        if self.listeners.iter().any(|listener| listener.port == port) {
            Ok(())
        } else {
            Err(ContractError::NoListener { port, role })
        }
    }
}

/// Parses one value.
fn one<T>(name: &'static str, text: &str) -> Result<T, ContractError>
where
    T: FromStr,
    T::Err: Display,
{
    text.trim()
        .parse()
        .map_err(|err: T::Err| ContractError::Invalid {
            name,
            value: text.to_owned(),
            reason: err.to_string(),
        })
}

/// Parses a comma-separated list; empty entries do not count.
fn list<T>(name: &'static str, text: &str) -> Result<Vec<T>, ContractError>
where
    T: FromStr,
    T::Err: Display,
{
    text.split(',')
        .filter(|part| !part.trim().is_empty())
        .map(|part| one(name, part))
        .collect()
}

fn check_fd(name: &'static str, fd: RawFd) -> Result<RawFd, ContractError> {
    if fd < 0 {
        return Err(ContractError::Invalid {
            name,
            value: fd.to_string(),
            reason: "a descriptor is never negative".to_owned(),
        });
    }
    Ok(fd)
}

/// Decodes the text form of a Kafka `Uuid` (`Uuid.toString()`): 16 bytes in
/// 22 characters of URL-safe base64 without padding. The last character
/// carries two bits and four zero bits, so it is one of `A`, `Q`, `g` and `w`.
#[must_use]
pub fn kafka_uuid(text: &str) -> Option<Uuid> {
    let bytes = text.as_bytes();
    if bytes.len() != 22 {
        return None;
    }
    let mut bits: u128 = 0;
    for &byte in &bytes[..21] {
        bits = (bits << 6) | u128::from(sextet(byte)?);
    }
    let last = sextet(bytes[21])?;
    if last & 0x0f != 0 {
        return None;
    }
    Some(Uuid::from_u128((bits << 2) | u128::from(last >> 4)))
}

fn sextet(byte: u8) -> Option<u8> {
    BASE64URL
        .iter()
        .position(|&b| b == byte)
        .and_then(|index| u8::try_from(index).ok())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, net::Ipv4Addr};

    use assert2::assert;

    use super::*;

    /// The environment the lab gives node 2 of a scenario with voters 1 to 3.
    fn node_two() -> BTreeMap<&'static str, String> {
        [
            ("KRABKA_LISTEN_FDS", "4,5"),
            ("KRABKA_LISTEN_PORTS", "9092,9093"),
            ("KRABKA_DIAL_FD", "6"),
            ("KRABKA_NODE_ID", "2"),
            ("KRABKA_HOST", "10.0.0.2"),
            (
                "KRABKA_VOTERS",
                "1@10.0.0.1:9093,2@10.0.0.2:9093,3@10.0.0.3:9093",
            ),
            ("KRABKA_CLUSTER_ID", "AQIDBAUGBwgJCgsMDQ4PEA"),
            ("KRABKA_CONFIG", r#"{"rack":"a"}"#),
        ]
        .into_iter()
        .map(|(name, value)| (name, value.to_owned()))
        .collect()
    }

    fn read(env: &BTreeMap<&'static str, String>) -> Result<Contract, ContractError> {
        Contract::from_vars(|name| env.get(name).cloned())
    }

    /// `node_two()` with `name` set to `value`, or removed when `value` is `None`.
    fn with(name: &'static str, value: Option<&str>) -> BTreeMap<&'static str, String> {
        let mut env = node_two();
        match value {
            Some(value) => env.insert(name, value.to_owned()),
            None => env.remove(name),
        };
        env
    }

    fn invalid(name: &'static str, value: &str, reason: &str) -> ContractError {
        ContractError::Invalid {
            name,
            value: value.to_owned(),
            reason: reason.to_owned(),
        }
    }

    #[test]
    fn reads_the_whole_contract() {
        assert!(
            read(&node_two())
                == Ok(Contract {
                    node_id: 2,
                    host: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                    listeners: vec![
                        Listener { fd: 4, port: 9092 },
                        Listener { fd: 5, port: 9093 },
                    ],
                    dial_fd: 6,
                    voters: vec![
                        (NodeId(1), "10.0.0.1:9093".to_owned()),
                        (NodeId(2), "10.0.0.2:9093".to_owned()),
                        (NodeId(3), "10.0.0.3:9093".to_owned()),
                    ],
                    cluster_id: Uuid::from_bytes([
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                    ]),
                    file_config: FileConfig {
                        rack: Some("a".to_owned()),
                        ..FileConfig::default()
                    },
                })
        );
    }

    #[test]
    fn a_node_votes_when_its_own_id_is_in_the_voters() {
        let cases = [
            ("2", "1@10.0.0.1:9093,2@10.0.0.2:9093,3@10.0.0.3:9093", true),
            ("2", "2@10.0.0.2:9093", true),
            (
                "4",
                "1@10.0.0.1:9093,2@10.0.0.2:9093,3@10.0.0.3:9093",
                false,
            ),
            ("2", "", false),
        ];
        for (node, voters, voter) in cases {
            let mut env = with("KRABKA_NODE_ID", Some(node));
            env.insert("KRABKA_VOTERS", voters.to_owned());
            let contract = read(&env).expect("a valid contract");
            assert!(
                contract.is_voter() == voter,
                "node {node} with voters {voters:?}"
            );
        }
    }

    #[test]
    fn a_broker_alone_needs_no_controller_listener() {
        let mut env = with("KRABKA_NODE_ID", Some("4"));
        env.insert("KRABKA_LISTEN_FDS", "4".to_owned());
        env.insert("KRABKA_LISTEN_PORTS", "9092".to_owned());
        let contract = read(&env).expect("a valid contract");
        assert!(contract.listeners == vec![Listener { fd: 4, port: 9092 }]);
        assert!(!contract.is_voter());
    }

    #[test]
    fn a_missing_variable_is_named() {
        for name in [
            "KRABKA_LISTEN_FDS",
            "KRABKA_LISTEN_PORTS",
            "KRABKA_DIAL_FD",
            "KRABKA_NODE_ID",
            "KRABKA_HOST",
            "KRABKA_VOTERS",
            "KRABKA_CLUSTER_ID",
            "KRABKA_CONFIG",
        ] {
            assert!(read(&with(name, None)) == Err(ContractError::Missing(name)));
        }
    }

    /// Checks that `node_two()` with each variable set to each value is
    /// refused with its error.
    fn refuses<const N: usize>(cases: [(&'static str, &str, ContractError); N]) {
        for (name, value, error) in cases {
            assert!(
                read(&with(name, Some(value))) == Err(error),
                "{name}={value}"
            );
        }
    }

    #[test]
    fn malformed_descriptors_are_refused() {
        refuses([
            (
                "KRABKA_DIAL_FD",
                "-6",
                invalid("KRABKA_DIAL_FD", "-6", "a descriptor is never negative"),
            ),
            (
                "KRABKA_LISTEN_FDS",
                "4,x",
                invalid("KRABKA_LISTEN_FDS", "x", "invalid digit found in string"),
            ),
            (
                "KRABKA_LISTEN_FDS",
                "4",
                ContractError::ListenerCount { fds: 1, ports: 2 },
            ),
            (
                "KRABKA_LISTEN_PORTS",
                "9092,9092",
                invalid(
                    "KRABKA_LISTEN_PORTS",
                    "9092",
                    "every listener has its own port, from 1 to 65535",
                ),
            ),
            (
                "KRABKA_LISTEN_PORTS",
                "9093,9094",
                ContractError::NoListener {
                    port: 9092,
                    role: "the broker",
                },
            ),
            (
                "KRABKA_LISTEN_PORTS",
                "9092,9094",
                ContractError::NoListener {
                    port: 9093,
                    role: "a controller quorum voter",
                },
            ),
        ]);
    }

    #[test]
    fn a_malformed_identity_or_configuration_is_refused() {
        refuses([
            (
                "KRABKA_NODE_ID",
                "two",
                invalid("KRABKA_NODE_ID", "two", "invalid digit found in string"),
            ),
            (
                "KRABKA_NODE_ID",
                "-1",
                invalid("KRABKA_NODE_ID", "-1", "the lab node id must be between 1 and 10000"),
            ),
            (
                "KRABKA_NODE_ID",
                "2147483648",
                invalid(
                    "KRABKA_NODE_ID",
                    "2147483648",
                    "number too large to fit in target type",
                ),
            ),
            (
                "KRABKA_HOST",
                "node-2",
                invalid("KRABKA_HOST", "node-2", "invalid IP address syntax"),
            ),
            (
                "KRABKA_VOTERS",
                "1@10.0.0.1:9093,two@10.0.0.2:9093",
                invalid(
                    "KRABKA_VOTERS",
                    "two@10.0.0.2:9093",
                    "invalid controller_quorum_voters entry: \"two@10.0.0.2:9093\": invalid node id \"two\": invalid digit found in string",
                ),
            ),
            (
                "KRABKA_VOTERS",
                "1@10.0.0.1",
                invalid(
                    "KRABKA_VOTERS",
                    "1@10.0.0.1",
                    "invalid controller_quorum_voters entry: \"1@10.0.0.1\": expected `<host>:<port>` after `@` (missing `:port`)",
                ),
            ),
            (
                "KRABKA_CLUSTER_ID",
                "7a9f1a4e-3f1d-4a8e-9d4b-2c6e1f0a5b3c",
                invalid(
                    "KRABKA_CLUSTER_ID",
                    "7a9f1a4e-3f1d-4a8e-9d4b-2c6e1f0a5b3c",
                    "a Kafka cluster id is 16 bytes in 22 characters of URL-safe base64",
                ),
            ),
            (
                "KRABKA_CONFIG",
                r#"{"runtime":{"num_partitions":"three"}}"#,
                invalid(
                    "KRABKA_CONFIG",
                    r#"{"runtime":{"num_partitions":"three"}}"#,
                    "invalid type: string \"three\", expected i32 at line 1 column 36",
                ),
            ),
        ]);
    }

    #[test]
    fn decodes_the_text_form_of_a_kafka_uuid() {
        let cases = [
            (
                "AQIDBAUGBwgJCgsMDQ4PEA",
                Some(Uuid::from_bytes([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                ])),
            ),
            ("AAAAAAAAAAAAAAAAAAAAAA", Some(Uuid::nil())),
            ("_____________________w", Some(Uuid::max())),
            // Too short, too long, a character outside the alphabet, and a
            // last character with bits beyond the 16 bytes.
            ("AQIDBAUGBwgJCgsMDQ4PE", None),
            ("AQIDBAUGBwgJCgsMDQ4PEAA", None),
            ("AQIDBAUGBwgJCgsMDQ4P+A", None),
            ("AQIDBAUGBwgJCgsMDQ4PEB", None),
        ];
        for (text, uuid) in cases {
            assert!(kafka_uuid(text) == uuid, "{text}");
        }
    }
}
