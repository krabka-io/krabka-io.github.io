//! The control server: length-prefixed commands from the test driver.
//!
//! A frame is a 4-byte big-endian length and a payload. A request payload is a
//! command line, optionally followed by `\n` and a binary body; a reply is a
//! status line, optionally followed by `\n` and a binary body. Each connection
//! runs its commands in order; connections run concurrently.
//!
//! | Command | Reply |
//! | --- | --- |
//! | `PING` | `PONG` |
//! | `ENV` | `ENV`, body: the `KRABKA_*` variables and the arguments, one per line |
//! | `TICKS` | `TICKS <heartbeat ticks> <guest ms since start>` |
//! | `NOW` | `NOW <monotonic ns> <realtime ns>` |
//! | `SLEEP <ms>` | `SLEPT <ms> <elapsed ms>` once that much guest time has passed |
//! | `DIAL <host> <port> <payload>` | `DIALED <answer>`, or `DIAL-ERR <kind> <message>` |
//! | `DIALRAW <host> <port> <payload>` | the same over a blocking socket and the raw socket calls |
//! | `ACCEPTRAW` | `ACCEPTED <line>` after a blocking accept on the first spare listener |
//! | `FSTEST <dir>` | `FS-OK <checks>`, or `FS-FAIL <check>: <message>` |
//! | `PUT <path>`, body | `OK <len>`: create or truncate, write, fsync |
//! | `APPEND <path>`, body | `OK <len>`: create or append, fsync |
//! | `CAT <path>` | `OK <len>`, body: the bytes; or `ERR <message>` |
//! | `LS <path>` | `OK`, body: sorted `<name>\t<dir or file>\t<len>` lines |
//! | `RM <path>` | `OK`, or `ERR <message>` |
//! | `STAT <path>` | `OK <dir or file> <len> <mtime ns>`, or `ERR <message>` |
//! | `ECHOED` | `ECHOED <plain bytes> <upper bytes>` |
//! | `STDOUT <text>`, `STDERR <text>` | `OK` after printing the line |
//! | `EXIT <code>` | none: the process exits with that code |
//! | `PANIC <message>` | none: the guest panics, which traps (panics abort) |
//!
//! `DIAL` opens a connection through the dialer, sends the payload as one
//! frame, and answers with the first frame the peer sends back. `DIALRAW` does
//! the same without tokio: it clears the socket's `NONBLOCK` flag and drives it
//! with `sock_send`, `sock_recv` (a peek, then wait-all reads) and
//! `sock_shutdown`, so the runtime parks the whole guest inside the calls. Its
//! peer must therefore live outside this guest. `ACCEPTRAW` does the same on
//! the listening side: a `std` accept on a listener left in blocking mode, a
//! blocking read of one line, and the line upper-cased back to the peer.

use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::mem::ManuallyDrop;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::{Guest, fsuite, net};

/// The largest frame either side accepts.
const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Serves control connections forever.
pub async fn serve(listener: TcpListener, guest: Arc<Guest>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(session(stream, Arc::clone(&guest)));
            }
            Err(err) => {
                eprintln!("[guest] control accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

async fn session(mut stream: TcpStream, guest: Arc<Guest>) {
    loop {
        let request = match read_frame(&mut stream).await {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(err) => {
                eprintln!("[guest] control connection failed: {err}");
                return;
            }
        };
        let (line, body) = split(&request);
        let answer = handle(&line, body, &guest).await;
        if let Err(err) = stream.write_all(&frame(&answer)).await {
            eprintln!("[guest] control reply failed: {err}");
            return;
        }
    }
}

async fn handle(line: &str, body: &[u8], guest: &Guest) -> Vec<u8> {
    let (command, rest) = line.split_once(' ').unwrap_or((line, ""));
    match command {
        "PING" => reply("PONG", &[]),
        "ENV" => {
            let mut text = String::new();
            for (key, value) in std::env::vars().filter(|(key, _)| key.starts_with("KRABKA_")) {
                let _ = writeln!(text, "{key}={value}");
            }
            for arg in std::env::args() {
                let _ = writeln!(text, "arg={arg}");
            }
            reply("ENV", text.as_bytes())
        }
        "TICKS" => reply(
            &format!(
                "TICKS {} {}",
                guest.ticks.load(Ordering::Relaxed),
                guest.started.elapsed().as_millis()
            ),
            &[],
        ),
        "NOW" => now(),
        "SLEEP" => match rest.parse::<u64>() {
            Ok(ms) => {
                let started = Instant::now();
                tokio::time::sleep(Duration::from_millis(ms)).await;
                reply(
                    &format!("SLEPT {ms} {}", started.elapsed().as_millis()),
                    &[],
                )
            }
            Err(err) => reply(&format!("ERR SLEEP {rest:?}: {err}"), &[]),
        },
        "DIAL" => dial(rest, guest).await,
        "DIALRAW" => dial_raw(rest, guest),
        "ACCEPTRAW" => accept_raw(guest),
        "FSTEST" => match fsuite::run(&Path::new("/data").join(rest)) {
            Ok(checks) => reply(&format!("FS-OK {checks}"), &[]),
            Err(failure) => reply(&format!("FS-FAIL {failure}"), &[]),
        },
        "PUT" => outcome(put(rest, body, false).map(|len| format!("OK {len}"))),
        "APPEND" => outcome(put(rest, body, true).map(|len| format!("OK {len}"))),
        "CAT" => match fs::read(rest) {
            Ok(bytes) => reply(&format!("OK {}", bytes.len()), &bytes),
            Err(err) => reply(&format!("ERR {err}"), &[]),
        },
        "LS" => match list(rest) {
            Ok(text) => reply("OK", text.as_bytes()),
            Err(err) => reply(&format!("ERR {err}"), &[]),
        },
        "RM" => outcome(remove(rest).map(|()| "OK".to_owned())),
        "STAT" => outcome(stat(rest)),
        "ECHOED" => reply(
            &format!(
                "ECHOED {} {}",
                guest.echoed[0].load(Ordering::Relaxed),
                guest.echoed[1].load(Ordering::Relaxed)
            ),
            &[],
        ),
        "STDOUT" => {
            println!("{rest}");
            reply("OK", &[])
        }
        "STDERR" => {
            eprintln!("{rest}");
            reply("OK", &[])
        }
        "EXIT" => std::process::exit(rest.parse().unwrap_or(1)),
        "PANIC" => panic!("{rest}"),
        _ => reply(&format!("ERR unknown command {command:?}"), &[]),
    }
}

/// The raw clock readings, so a test can compare them with the host's clock.
fn now() -> Vec<u8> {
    // SAFETY: `clock_time_get` only writes its result, which the wrapper returns.
    let monotonic = unsafe { wasi::clock_time_get(wasi::CLOCKID_MONOTONIC, 1) };
    let realtime = SystemTime::now().duration_since(UNIX_EPOCH);
    match (monotonic, realtime) {
        (Ok(monotonic), Ok(realtime)) => {
            reply(&format!("NOW {monotonic} {}", realtime.as_nanos()), &[])
        }
        (monotonic, realtime) => reply(&format!("ERR {monotonic:?} {realtime:?}"), &[]),
    }
}

const DIAL_USAGE: &str = "DIAL-ERR InvalidInput usage: DIAL <host> <port> <payload>";

/// Splits `<host> <port> <payload>`.
fn dial_args(args: &str) -> Option<(&str, u16, &str)> {
    let mut parts = args.splitn(3, ' ');
    let host = parts.next()?;
    let port = parts.next()?.parse().ok()?;
    Some((host, port, parts.next().unwrap_or("")))
}

fn dialed(result: io::Result<Vec<u8>>) -> Vec<u8> {
    match result {
        Ok(answer) => reply(&format!("DIALED {}", String::from_utf8_lossy(&answer)), &[]),
        Err(err) => reply(&format!("DIAL-ERR {:?} {err}", err.kind()), &[]),
    }
}

async fn dial(args: &str, guest: &Guest) -> Vec<u8> {
    let Some((host, port, payload)) = dial_args(args) else {
        return reply(DIAL_USAGE, &[]);
    };
    dialed(ask(guest.dial_fd, host, port, payload.as_bytes()).await)
}

async fn ask(dial_fd: RawFd, host: &str, port: u16, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut stream = net::connect(dial_fd, host, port)?;
    stream.write_all(&frame(payload)).await?;
    let answer = read_frame(&mut stream).await?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the peer closed before answering",
        )
    })?;
    stream.shutdown().await?;
    Ok(answer)
}

fn dial_raw(args: &str, guest: &Guest) -> Vec<u8> {
    let Some((host, port, payload)) = dial_args(args) else {
        return reply(DIAL_USAGE, &[]);
    };
    dialed(ask_raw(guest.dial_fd, host, port, payload.as_bytes()))
}

fn ask_raw(dial_fd: RawFd, host: &str, port: u16, payload: &[u8]) -> io::Result<Vec<u8>> {
    let stream = net::dial(dial_fd, host, port)?;
    stream.set_nonblocking(false)?;
    let fd = u32::try_from(stream.as_raw_fd())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let request = frame(payload);
    let mut sent = 0;
    while sent < request.len() {
        let rest = &request[sent..];
        let iov = [wasi::Ciovec {
            buf: rest.as_ptr(),
            buf_len: rest.len(),
        }];
        // SAFETY: `fd` belongs to `stream`, which outlives the call, and the
        // iovec points into `request`.
        sent += unsafe { wasi::sock_send(fd, &iov, 0) }.map_err(errno_io)?;
    }
    let mut len = [0u8; 4];
    let peeked = recv(
        fd,
        &mut len,
        wasi::RIFLAGS_RECV_PEEK | wasi::RIFLAGS_RECV_WAITALL,
    )?;
    let consumed = recv(fd, &mut len, wasi::RIFLAGS_RECV_WAITALL)?;
    if (peeked, consumed) != (4, 4) {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("a {peeked}-byte peek and a {consumed}-byte read of a frame header"),
        ));
    }
    let mut answer = vec![0u8; usize::try_from(u32::from_be_bytes(len)).unwrap_or(usize::MAX)];
    let got = recv(fd, &mut answer, wasi::RIFLAGS_RECV_WAITALL)?;
    answer.truncate(got);
    stream.shutdown(std::net::Shutdown::Write)?;
    Ok(answer)
}

fn accept_raw(guest: &Guest) -> Vec<u8> {
    let Some(&fd) = guest.spare_listeners.first() else {
        return reply("ERR no spare listener: list a fourth listener port", &[]);
    };
    match accept_line(fd) {
        Ok(line) => reply(&format!("ACCEPTED {line}"), &[]),
        Err(err) => reply(&format!("ERR {err}"), &[]),
    }
}

/// Accepts one connection on a listener that stays in blocking mode, reads one
/// line from it (blocking too), and answers it upper-cased.
fn accept_line(fd: RawFd) -> io::Result<String> {
    // SAFETY: the runtime preopened `fd` as a listening socket for this process;
    // `ManuallyDrop` keeps it open when this call returns.
    let listener = ManuallyDrop::new(unsafe { std::net::TcpListener::from_raw_fd(fd) });
    let (mut stream, _) = listener.accept()?;
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    while stream.read(&mut byte)? == 1 && byte[0] != b'\n' {
        line.push(byte[0]);
    }
    let line = String::from_utf8_lossy(&line).into_owned();
    stream.write_all(format!("{}\n", line.to_uppercase()).as_bytes())?;
    Ok(line)
}

/// One `sock_recv` into `buf`.
fn recv(fd: u32, buf: &mut [u8], flags: wasi::Riflags) -> io::Result<usize> {
    let iov = [wasi::Iovec {
        buf: buf.as_mut_ptr(),
        buf_len: buf.len(),
    }];
    // SAFETY: the iovec points into `buf`, which outlives the call.
    unsafe { wasi::sock_recv(fd, &iov, flags) }
        .map(|(n, _)| n)
        .map_err(errno_io)
}

fn errno_io(err: wasi::Errno) -> io::Error {
    io::Error::from_raw_os_error(i32::from(err.raw()))
}

fn put(path: &str, body: &[u8], append: bool) -> io::Result<u64> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = if append {
        OpenOptions::new().create(true).append(true).open(path)?
    } else {
        File::create(path)?
    };
    file.write_all(body)?;
    file.sync_all()?;
    Ok(file.metadata()?.len())
}

fn list(path: &str) -> io::Result<String> {
    let mut rows = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        let kind = if meta.is_dir() { "dir" } else { "file" };
        rows.push(format!(
            "{}\t{kind}\t{}",
            entry.file_name().to_string_lossy(),
            meta.len()
        ));
    }
    rows.sort();
    Ok(rows.join("\n"))
}

fn remove(path: &str) -> io::Result<()> {
    if fs::symlink_metadata(path)?.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn stat(path: &str) -> io::Result<String> {
    let meta = fs::metadata(path)?;
    let mtime = meta
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    let kind = if meta.is_dir() { "dir" } else { "file" };
    Ok(format!("OK {kind} {} {}", meta.len(), mtime.as_nanos()))
}

fn outcome(result: io::Result<String>) -> Vec<u8> {
    match result {
        Ok(line) => reply(&line, &[]),
        Err(err) => reply(&format!("ERR {err}"), &[]),
    }
}

/// Splits a request into its command line and its body.
fn split(payload: &[u8]) -> (String, &[u8]) {
    match payload.iter().position(|&b| b == b'\n') {
        Some(end) => (
            String::from_utf8_lossy(&payload[..end]).into_owned(),
            &payload[end + 1..],
        ),
        None => (String::from_utf8_lossy(payload).into_owned(), &[]),
    }
}

/// A reply payload: the status line, then `\n` and the body when there is one.
fn reply(line: &str, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(line.len() + 1 + body.len());
    out.extend_from_slice(line.as_bytes());
    if !body.is_empty() {
        out.push(b'\n');
        out.extend_from_slice(body);
    }
    out
}

/// Prefixes `payload` with its length.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Reads one frame; `None` on a clean end of stream between frames.
pub async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len = [0u8; 4];
    match stream.read_exact(&mut len).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = usize::try_from(u32::from_be_bytes(len)).unwrap_or(usize::MAX);
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a frame of {len} bytes"),
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(Some(payload))
}
