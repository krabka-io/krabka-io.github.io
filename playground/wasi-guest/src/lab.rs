//! Lab mode: the guest plays a `krabka-broker` node of the Cluster Lab.
//!
//! When `KRABKA_NODE_ID` is set, the guest follows the process contract of
//! a real broker in the lab (`playground/docs/lab-real-broker.md`) instead of
//! serving the runtime checks. It reads the whole contract from the
//! environment and exits with code 2 on a malformed one, records each boot in
//! its volume, runs a 100 ms heartbeat, and serves every listener. Like a
//! combined-mode broker that reaches its own controller over the network, it
//! dials its own controller listener at every boot and reports on stdout
//! whether a Kafka frame came back ([`self_dial`]). It logs JSON lines on
//! stderr like the broker does ([`crate::log`]): one line per level at every
//! boot, then an `INFO` heartbeat line per second of guest time, all
//! filtered by `KRABKA_LOG`. The first bytes of a connection decide what it
//! is:
//!
//! | First line | Effect |
//! | --- | --- |
//! | `DIAL <ip>:<port>` | dials through the dialer, sends the rest of the connection's bytes there and relays both ways until either side closes; `DIAL-ERR <kind> <message>` when the dial fails |
//! | `TICKS` | answers `TICKS <heartbeats> <guest ms since start>` and closes |
//! | `ENV` | answers the `KRABKA_*` variables, one per line, then `END`, and closes |
//! | `EXIT <code>` | the process exits with that code |
//! | `PANIC <message>` | the guest panics, which traps |
//!
//! Anything else is echoed from its first byte, so a lab `pinger`'s pings or
//! a Kafka frame come back unchanged.

use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::log::{self, Level};
use crate::{control, net};

/// The heartbeat period, in guest time.
const HEARTBEAT: Duration = Duration::from_millis(100);
/// The controller listener, which the boot's self-dial reaches.
const CONTROLLER_PORT: u16 = 9093;
/// The file each boot appends a line to.
const BOOTS: &str = "/data/lab-guest/boots";
/// A first line longer than this is not a command.
const MAX_COMMAND: usize = 1024;
/// The line commands, as they start a connection.
const COMMANDS: [&[u8]; 5] = [b"DIAL ", b"TICKS", b"ENV", b"EXIT ", b"PANIC "];
/// The URL-safe base64 alphabet of a Kafka `Uuid`.
const BASE64URL: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// One controller-quorum voter from `KRABKA_VOTERS`: `id@ip:port`.
pub struct Voter {
    /// The voter's node id.
    pub id: u32,
    /// Its virtual address.
    pub host: Ipv4Addr,
    /// Its controller port.
    pub port: u16,
}

/// The process contract, as the lab hands it over in the environment.
pub struct Contract {
    /// `KRABKA_NODE_ID`.
    pub node_id: u32,
    /// `KRABKA_HOST`: the node's virtual address.
    pub host: Ipv4Addr,
    /// `KRABKA_VOTERS`.
    pub voters: Vec<Voter>,
    /// `KRABKA_CLUSTER_ID`: 22 characters of URL-safe base64.
    pub cluster_id: String,
    /// `KRABKA_CONFIG`: a JSON object of extra broker properties.
    pub config: String,
    /// The listeners with their ports, from `KRABKA_LISTEN_FDS` and `KRABKA_LISTEN_PORTS`.
    pub listeners: Vec<(RawFd, u16)>,
    /// `KRABKA_DIAL_FD`.
    pub dial: RawFd,
}

impl Contract {
    /// Reads and checks the contract.
    pub fn from_env() -> Result<Self, String> {
        let fds = net::Fds::from_env().map_err(|err| err.to_string())?;
        let ports = var("KRABKA_LISTEN_PORTS")?
            .split(',')
            .filter(|part| !part.trim().is_empty())
            .map(|part| {
                part.trim()
                    .parse::<u16>()
                    .map_err(|err| format!("KRABKA_LISTEN_PORTS: {part:?}: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if ports.len() != fds.listeners.len() {
            return Err(format!(
                "KRABKA_LISTEN_PORTS names {} ports for {} listeners",
                ports.len(),
                fds.listeners.len()
            ));
        }
        let node_id: u32 = var("KRABKA_NODE_ID")?
            .parse()
            .map_err(|err| format!("KRABKA_NODE_ID: {err}"))?;
        let host: Ipv4Addr = var("KRABKA_HOST")?
            .parse()
            .map_err(|err| format!("KRABKA_HOST: {err}"))?;
        if host != node_ip(node_id) {
            return Err(format!(
                "KRABKA_HOST is {host}, but node {node_id} is {}",
                node_ip(node_id)
            ));
        }
        let voters = var("KRABKA_VOTERS")?
            .split(',')
            .filter(|part| !part.trim().is_empty())
            .map(parse_voter)
            .collect::<Result<Vec<_>, _>>()?;
        let cluster_id = var("KRABKA_CLUSTER_ID")?;
        check_cluster_id(&cluster_id)?;
        let config = var("KRABKA_CONFIG")?;
        let trimmed = config.trim();
        if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
            return Err(format!("KRABKA_CONFIG is not a JSON object: {config:?}"));
        }
        Ok(Self {
            node_id,
            host,
            voters,
            cluster_id,
            config,
            listeners: fds.listeners.into_iter().zip(ports).collect(),
            dial: fds.dial,
        })
    }
}

fn var(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|err| format!("{name}: {err}"))
}

/// The virtual address of a node, as the lab computes it: `10.0.(n >> 8).(n & 255)`.
fn node_ip(node: u32) -> Ipv4Addr {
    let [_, _, high, low] = node.to_be_bytes();
    Ipv4Addr::new(10, 0, high, low)
}

fn parse_voter(text: &str) -> Result<Voter, String> {
    let bad = |why: &str| format!("KRABKA_VOTERS: {text:?}: {why}");
    let (id, address) = text.trim().split_once('@').ok_or_else(|| bad("no `@`"))?;
    let (host, port) = address.rsplit_once(':').ok_or_else(|| bad("no port"))?;
    Ok(Voter {
        id: id.parse().map_err(|_| bad("the id is not a number"))?,
        host: host
            .parse()
            .map_err(|_| bad("the host is not an IPv4 address"))?,
        port: port.parse().map_err(|_| bad("the port is not a number"))?,
    })
}

/// A Kafka `Uuid` in URL-safe base64: 22 characters encoding 16 bytes, so the
/// last character carries two zero bits and is one of `A`, `Q`, `g` or `w`.
fn check_cluster_id(id: &str) -> Result<(), String> {
    let bytes = id.as_bytes();
    let canonical = bytes.len() == 22
        && bytes.iter().all(|b| BASE64URL.contains(b))
        && matches!(bytes[21], b'A' | b'Q' | b'g' | b'w');
    if canonical {
        Ok(())
    } else {
        Err(format!(
            "KRABKA_CLUSTER_ID {id:?} is not a Kafka Uuid in URL-safe base64"
        ))
    }
}

/// What the connections share.
struct Lab {
    started: Instant,
    dial: RawFd,
    ticks: AtomicU64,
}

/// Runs the lab mode: checks the contract, records the boot, then serves
/// until the process is stopped.
pub async fn run(started: Instant) {
    log::init(started);
    let contract = Contract::from_env().unwrap_or_else(|err| {
        eprintln!("[lab-guest] bad process contract: {err}");
        std::process::exit(2);
    });
    let boots = record_boot().unwrap_or_else(|err| {
        eprintln!("[lab-guest] cannot record the boot in {BOOTS}: {err}");
        std::process::exit(2);
    });
    let lab = Arc::new(Lab {
        started,
        dial: contract.dial,
        ticks: AtomicU64::new(0),
    });
    for &(fd, port) in &contract.listeners {
        let listener = net::listener(fd).unwrap_or_else(|err| {
            eprintln!("[lab-guest] cannot adopt listener fd {fd} (port {port}): {err}");
            std::process::exit(2);
        });
        tokio::spawn(serve(listener, Arc::clone(&lab)));
    }
    for level in [Level::Trace, Level::Debug, Level::Info, Level::Warn, Level::Error] {
        log::log(
            level,
            "lab_guest::boot",
            &format!("boot {boots} logs at {}", level.name()),
            &[("node_id", contract.node_id.to_string())],
        );
    }
    tokio::spawn(heartbeat(Arc::clone(&lab)));
    tokio::spawn(self_dial(contract.host, contract.dial));
    let voters: Vec<String> = contract
        .voters
        .iter()
        .map(|v| format!("{}@{}:{}", v.id, v.host, v.port))
        .collect();
    let ports: Vec<String> = contract
        .listeners
        .iter()
        .map(|(_, port)| port.to_string())
        .collect();
    println!(
        "ready node={} host={} cluster={} voters={} listeners={} config={} boots={boots}",
        contract.node_id,
        contract.host,
        contract.cluster_id,
        voters.join(","),
        ports.join(","),
        contract.config.trim(),
    );
    std::future::pending::<()>().await;
}

/// Appends a line to [`BOOTS`], makes it durable, and returns how many boots
/// the volume has seen.
fn record_boot() -> io::Result<usize> {
    fs::create_dir_all("/data/lab-guest")?;
    let mut file = OpenOptions::new().create(true).append(true).open(BOOTS)?;
    file.write_all(b"boot\n")?;
    file.sync_all()?;
    Ok(fs::read_to_string(BOOTS)?.lines().count())
}

/// Dials this node's own controller listener through the lab, sends one Kafka
/// frame and checks that the echo comes back, then says so on stdout:
/// `self-dial ok: <ip>:9093 echoed <n> bytes`, or `self-dial failed: ...`.
async fn self_dial(host: Ipv4Addr, dial: RawFd) {
    let payload = format!("self-dial from {host}");
    let outcome = async {
        let mut stream = net::connect(dial, &host.to_string(), CONTROLLER_PORT)?;
        stream
            .write_all(&control::frame(payload.as_bytes()))
            .await?;
        let answer = control::read_frame(&mut stream).await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the listener closed before answering",
            )
        })?;
        stream.shutdown().await?;
        Ok::<_, io::Error>(answer)
    }
    .await;
    match outcome {
        Ok(answer) if answer == payload.as_bytes() => {
            println!(
                "self-dial ok: {host}:{CONTROLLER_PORT} echoed {} bytes",
                answer.len()
            );
        }
        Ok(answer) => println!(
            "self-dial failed: {host}:{CONTROLLER_PORT} answered {:?}",
            String::from_utf8_lossy(&answer)
        ),
        Err(err) => println!("self-dial failed: {host}:{CONTROLLER_PORT}: {err}"),
    }
}

async fn heartbeat(lab: Arc<Lab>) {
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + HEARTBEAT, HEARTBEAT);
    loop {
        interval.tick().await;
        let ticks = lab.ticks.fetch_add(1, Ordering::Relaxed) + 1;
        if ticks.is_multiple_of(10) {
            log::log(
                Level::Info,
                "lab_guest::heartbeat",
                "heartbeat",
                &[("ticks", ticks.to_string())],
            );
        }
    }
}

async fn serve(listener: TcpListener, lab: Arc<Lab>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let lab = Arc::clone(&lab);
                tokio::spawn(async move {
                    if let Err(err) = connection(stream, &lab).await {
                        eprintln!("[lab-guest] connection failed: {err}");
                    }
                });
            }
            Err(err) => {
                eprintln!("[lab-guest] accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// What the first bytes of a connection make it.
enum Head {
    /// Not enough bytes to tell yet.
    Undecided,
    /// Echo everything, starting with these bytes.
    Echo,
    /// A command line ending at this offset (the `\n`).
    Command(usize),
}

fn classify(head: &[u8]) -> Head {
    let newline = head.iter().position(|&b| b == b'\n');
    let line = &head[..newline.unwrap_or(head.len())];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let command = COMMANDS
        .iter()
        .any(|c| line == *c || (c.ends_with(b" ") && line.starts_with(c)));
    let prefix = COMMANDS.iter().any(|c| c.starts_with(line));
    match newline {
        Some(end) if command => Head::Command(end),
        None if (command || prefix) && head.len() <= MAX_COMMAND => Head::Undecided,
        _ => Head::Echo,
    }
}

async fn connection(mut stream: TcpStream, lab: &Lab) -> io::Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    let mut head = Vec::new();
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            if !head.is_empty() {
                stream.write_all(&head).await?;
            }
            return stream.shutdown().await;
        }
        head.extend_from_slice(&buf[..n]);
        match classify(&head) {
            Head::Undecided => {}
            Head::Echo => {
                stream.write_all(&head).await?;
                return echo(stream, buf).await;
            }
            Head::Command(end) => {
                let line = String::from_utf8_lossy(&head[..end]).trim().to_owned();
                return command(stream, &line, &head[end + 1..], lab).await;
            }
        }
    }
}

/// Echoes until the peer shuts its side down, then shuts ours down.
async fn echo(mut stream: TcpStream, mut buf: Vec<u8>) -> io::Result<()> {
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return stream.shutdown().await;
        }
        stream.write_all(&buf[..n]).await?;
    }
}

async fn command(mut stream: TcpStream, line: &str, rest: &[u8], lab: &Lab) -> io::Result<()> {
    let (word, args) = line.split_once(' ').unwrap_or((line, ""));
    match word {
        "DIAL" => return relay(stream, args, rest, lab.dial).await,
        "TICKS" => {
            let reply = format!(
                "TICKS {} {}\n",
                lab.ticks.load(Ordering::Relaxed),
                lab.started.elapsed().as_millis()
            );
            stream.write_all(reply.as_bytes()).await?;
        }
        "ENV" => {
            let mut vars: Vec<(String, String)> = std::env::vars()
                .filter(|(key, _)| key.starts_with("KRABKA_"))
                .collect();
            vars.sort();
            let mut reply = String::new();
            for (key, value) in vars {
                let _ = writeln!(reply, "{key}={value}");
            }
            reply.push_str("END\n");
            stream.write_all(reply.as_bytes()).await?;
        }
        "EXIT" => {
            eprintln!("[lab-guest] exiting with code {args} on request");
            std::process::exit(args.trim().parse().unwrap_or(1));
        }
        "PANIC" => panic!("{args}"),
        _ => stream.write_all(b"ERR unknown command\n").await?,
    }
    stream.shutdown().await
}

/// Dials `address` through the dialer, forwards `first` and then relays both
/// ways until either side closes.
async fn relay(mut client: TcpStream, address: &str, first: &[u8], dial: RawFd) -> io::Result<()> {
    let target = address
        .trim()
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)));
    let Some((host, port)) = target else {
        client
            .write_all(b"DIAL-ERR InvalidInput usage: DIAL <ip>:<port>\n")
            .await?;
        return client.shutdown().await;
    };
    let mut remote = match net::connect(dial, host, port) {
        Ok(remote) => remote,
        Err(err) => return refuse(client, &err).await,
    };
    // The connection completes asynchronously: a refusal surfaces on the
    // first write.
    if let Err(err) = remote.write_all(first).await {
        return refuse(client, &err).await;
    }
    if let Err(err) = tokio::io::copy_bidirectional(&mut client, &mut remote).await {
        eprintln!("[lab-guest] relay to {host}:{port} ended: {err}");
    }
    Ok(())
}

async fn refuse(mut client: TcpStream, err: &io::Error) -> io::Result<()> {
    let line = format!("DIAL-ERR {:?} {err}\n", err.kind());
    client.write_all(line.as_bytes()).await?;
    client.shutdown().await
}
