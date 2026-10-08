//! The real `krabka-broker` as a process of the Cluster Lab.
//!
//! The lab page (`/docs/lab`) runs this command, compiled for
//! `wasm32-wasip1`, in a Web Worker on the site's browser WASI runtime
//! (`public/playground/wasi/`), one process per "Krabka broker (real)" node.
//! The page and the runtime hand it a volume, preopened sockets and its
//! identity; this crate turns them into a broker, following the process
//! contract of `playground/docs/lab-real-broker.md`:
//!
//! 1. It reads and checks the whole environment ([`contract`]) and exits
//!    with code 2 and a line on stderr when the environment is malformed.
//! 2. It adopts the preopened listening sockets and installs the dialer as
//!    the transport connector of every outbound connection ([`fds`]), before
//!    the runtime starts.
//! 3. On a current-thread tokio runtime, whose clocks and timers are the
//!    lab's, it formats `/data/log` in process when the volume is new, reads
//!    `meta.properties` back, and boots in `Rejoin` mode when the metadata log
//!    has state, in `Bootstrap` mode otherwise.
//! 4. It builds the node's `BrokerConfig` ([`profile`]), applies
//!    `KRABKA_CONFIG`, and starts the broker on the adopted listeners. It
//!    then runs until the broker stops on its own, and exits with code 1
//!    then, or when it cannot start.
//!
//! The process logs one JSON object per line to stderr ([`logging`]), stamped
//! with the time since it started, which the lab's clock drives: added to the
//! process's start in the inspector, it gives the lab time of the event.
//! `KRABKA_LOG` sets the levels. After a kill or a page reload the broker
//! recovers its log from the volume, as it does after a crash.

mod contract;
mod fds;
mod logging;
mod profile;

use std::{net::TcpListener, path::Path, process::ExitCode, time::Instant};

use krabka_broker::{BootstrapMode, Broker, bootstrap::initialize_log_dirs};
use krabka_client_core::transport::install_connector;

use crate::{
    contract::{CONTROLLER_PORT, Contract},
    fds::Dialer,
    profile::LOG_DIR,
};

/// The exit code for an environment that is not the lab's process contract.
const EXIT_CONTRACT: u8 = 2;
/// The exit code for a broker that cannot start, or that stopped on its own.
const EXIT_FATAL: u8 = 1;

fn main() -> ExitCode {
    logging::init(Instant::now());
    let contract = match Contract::from_env() {
        Ok(contract) => contract,
        Err(err) => {
            tracing::error!("the environment is not the lab's process contract: {err}");
            return ExitCode::from(EXIT_CONTRACT);
        }
    };
    let listeners = match Listeners::adopt(&contract) {
        Ok(listeners) => listeners,
        Err(err) => {
            tracing::error!(
                "the lab's process contract names a descriptor that is no listener: {err}"
            );
            return ExitCode::from(EXIT_CONTRACT);
        }
    };
    if let Err(err) = install_connector(Box::new(Dialer::adopt(contract.dial_fd))) {
        tracing::error!("the dialer cannot carry the broker's connections: {err}");
        return ExitCode::from(EXIT_FATAL);
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            tracing::error!("no tokio runtime: {err}");
            return ExitCode::from(EXIT_FATAL);
        }
    };
    runtime.block_on(run(contract, listeners))
}

/// Formats the volume when it is new, then starts the broker and serves until
/// it stops on its own.
async fn run(contract: Contract, listeners: Listeners) -> ExitCode {
    let node_id = contract.node_id.to_string();
    let cluster_id = contract.cluster_id.to_string();
    let code = krabka_format::run_from_args([
        "krabka-format",
        "--log-dir",
        LOG_DIR,
        "--cluster-id",
        &cluster_id,
        "--node-id",
        &node_id,
        "--ignore-formatted",
    ])
    .await;
    if code != 0 {
        tracing::error!(code, "krabka-format could not format {LOG_DIR}");
        return ExitCode::from(u8::try_from(code).unwrap_or(EXIT_FATAL));
    }
    let log_dir = Path::new(LOG_DIR);
    let identity = match initialize_log_dirs(
        log_dir,
        &[log_dir.to_path_buf()],
        contract.raft_node_id(),
        Some(contract.cluster_id),
    ) {
        Ok(identity) => identity,
        Err(err) => {
            tracing::error!("{LOG_DIR} is not this cluster's log directory: {err}");
            return ExitCode::from(EXIT_FATAL);
        }
    };
    // The broker binary's rule: a metadata log with raft state is a restart.
    let bootstrap_mode = if krabka_raft::metadata_log_nonempty(&log_dir.join("__cluster_metadata"))
    {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    };
    let config = match profile::broker_config(&contract, &identity, bootstrap_mode) {
        Ok(config) => config,
        Err(err) => {
            tracing::error!("KRABKA_CONFIG: {err}");
            return ExitCode::from(EXIT_CONTRACT);
        }
    };
    tracing::info!(
        node_id = contract.node_id,
        voter = contract.is_voter(),
        ?bootstrap_mode,
        directory_id = %identity.directory_id,
        "starting the broker on {LOG_DIR}"
    );
    let (controller, data) = match listeners.into_tokio() {
        Ok(listeners) => listeners,
        Err(err) => {
            tracing::error!("the runtime refused a listener: {err}");
            return ExitCode::from(EXIT_FATAL);
        }
    };
    let advertised = config.advertised_listener.clone();
    let handle = match Broker::start_with_listeners(config, controller, data).await {
        Ok(handle) => handle,
        Err(err) => {
            tracing::error!("the broker did not start: {err}");
            return ExitCode::from(EXIT_FATAL);
        }
    };
    tracing::info!("krabka-broker serving on {advertised}");
    // The broker decides to stop on its own only when every log directory
    // went offline (KIP-112); the lab then reports the process's end.
    let mut stop = handle.should_shutdown_rx();
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            break;
        }
    }
    tracing::error!("the broker stops on its own");
    handle.shutdown().await;
    ExitCode::from(EXIT_FATAL)
}

/// The listening sockets, split the way `Broker::start_with_listeners` takes
/// them.
struct Listeners {
    /// The controller listener of a voter.
    controller: Option<TcpListener>,
    /// Every other listener, in the contract's order: the data plane, which
    /// `BrokerConfig::effective_listeners` describes.
    data: Vec<TcpListener>,
}

impl Listeners {
    /// Adopts every listener of the contract. A node that is not a voter has
    /// no controller: it closes that listener, so a connection to its port
    /// 9093 is refused, as on a Kafka broker without the controller role.
    fn adopt(contract: &Contract) -> std::io::Result<Self> {
        let voter = contract.is_voter();
        let mut controller = None;
        let mut data = Vec::new();
        for listener in &contract.listeners {
            let socket = fds::adopt_listener(listener.fd)?;
            if listener.port != CONTROLLER_PORT {
                data.push(socket);
            } else if voter {
                controller = Some(socket);
            }
        }
        Ok(Self { controller, data })
    }

    /// Hands the sockets to tokio, which has to run by then.
    fn into_tokio(
        self,
    ) -> std::io::Result<(
        Option<tokio::net::TcpListener>,
        Vec<tokio::net::TcpListener>,
    )> {
        let controller = self
            .controller
            .map(tokio::net::TcpListener::from_std)
            .transpose()?;
        let data = self
            .data
            .into_iter()
            .map(tokio::net::TcpListener::from_std)
            .collect::<std::io::Result<Vec<_>>>()?;
        Ok((controller, data))
    }
}
