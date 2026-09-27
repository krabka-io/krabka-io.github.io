//! Test guest for the Cluster Lab's browser WASI runtime
//! (`public/playground/wasi/`).
//!
//! `scripts/check-wasi.mjs` builds this crate for `wasm32-wasip1` and runs it in
//! a Web Worker behind the runtime's WASI shim, the way the lab will run the
//! real broker. It exercises every preview-1 call the broker makes:
//!
//! - Two echo servers on the first two preopened listeners. The first returns
//!   bytes unchanged; the second upper-cases ASCII letters, so a test can tell
//!   which listener answered.
//! - A control server on the third listener, speaking length-prefixed commands
//!   (see [`control`]): dial out, accept on a spare listener in blocking mode,
//!   sleep, read the clocks, run the file-system suite, read and write files,
//!   print, exit and panic.
//! - A heartbeat: a 100 ms `tokio::time::interval` whose tick count the control
//!   server reports, so a test can watch guest timers stop while a host-driven
//!   clock is paused and fire when it advances.
//!
//! The runtime announces its descriptors through the environment:
//! `KRABKA_LISTEN_FDS=<echo>,<upper>,<control>[,<spare>...]` and
//! `KRABKA_DIAL_FD=<fd>`.
//!
//! With `KRABKA_NODE_ID` set, the guest runs in [`lab`] mode instead: it
//! stands in for a real broker as a node of the Cluster Lab, following the
//! lab's process contract.

#[cfg(not(target_os = "wasi"))]
compile_error!(
    "krabka-wasi-guest builds for wasm32-wasip1 only: cargo build --target wasm32-wasip1"
);

mod control;
mod echo;
mod fsuite;
mod lab;
mod net;

use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The heartbeat period, in guest time.
const HEARTBEAT: Duration = Duration::from_millis(100);

/// State the servers share.
pub struct Guest {
    /// When `main` started, on the guest's monotonic clock.
    pub started: Instant,
    /// The dialer descriptor from `KRABKA_DIAL_FD`.
    pub dial_fd: RawFd,
    /// Listeners past the first three: nobody accepts on them until `ACCEPTRAW` does.
    pub spare_listeners: Vec<RawFd>,
    /// Heartbeat ticks so far.
    pub ticks: AtomicU64,
    /// Bytes echoed by the plain and the upper-casing server.
    pub echoed: [AtomicU64; 2],
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let started = Instant::now();
    if std::env::var_os("KRABKA_NODE_ID").is_some() {
        lab::run(started).await;
        return;
    }
    let fds = match net::Fds::from_env() {
        Ok(fds) if fds.listeners.len() >= 3 => fds,
        Ok(fds) => {
            eprintln!(
                "[guest] need three listeners (echo, upper, control), got {:?}",
                fds.listeners
            );
            std::process::exit(2);
        }
        Err(err) => {
            eprintln!("[guest] {err}");
            std::process::exit(2);
        }
    };
    let guest = Arc::new(Guest {
        started,
        dial_fd: fds.dial,
        spare_listeners: fds.listeners[3..].to_vec(),
        ticks: AtomicU64::new(0),
        echoed: [AtomicU64::new(0), AtomicU64::new(0)],
    });

    let adopt = |fd: RawFd| {
        net::listener(fd).unwrap_or_else(|err| {
            eprintln!("[guest] cannot adopt listener fd {fd}: {err}");
            std::process::exit(2);
        })
    };
    let plain = adopt(fds.listeners[0]);
    let upper = adopt(fds.listeners[1]);
    let control = adopt(fds.listeners[2]);

    tokio::spawn(heartbeat(Arc::clone(&guest)));
    tokio::spawn(echo::serve(plain, false, Arc::clone(&guest)));
    tokio::spawn(echo::serve(upper, true, Arc::clone(&guest)));

    println!(
        "ready listeners={:?} dial={} args={:?}",
        fds.listeners,
        fds.dial,
        std::env::args().collect::<Vec<_>>()
    );
    eprintln!("[guest] serving after {:?}", started.elapsed());
    control::serve(control, guest).await;
}

/// Counts [`HEARTBEAT`] ticks. The first tick is one period after start, so
/// the count equals the guest time elapsed in whole periods.
async fn heartbeat(guest: Arc<Guest>) {
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + HEARTBEAT, HEARTBEAT);
    loop {
        interval.tick().await;
        let ticks = guest.ticks.fetch_add(1, Ordering::Relaxed) + 1;
        if ticks.is_multiple_of(100) {
            eprintln!("[guest] heartbeat {ticks}");
        }
    }
}
