//! Descriptor adoption: the runtime's preopened listeners and its dialer.
//!
//! WASI preview 1 has neither `bind` nor `connect`. The runtime preopens one
//! listening socket per listener and one "dialer" descriptor, and names them in
//! `KRABKA_LISTEN_FDS` and `KRABKA_DIAL_FD`. [`dial`] is the helper the broker's
//! embedder needs: it writes `host:port\n` to the dialer, reads back the number
//! of a fresh socket in state "connecting" (or `ERR <reason>`), and adopts it.
//! A refused connection surfaces as `ECONNREFUSED` on the first read or write.

use std::fs::File;
use std::io::{self, Read, Write};
use std::mem::ManuallyDrop;
use std::os::fd::{FromRawFd, RawFd};

/// The descriptors the runtime announces through the environment.
pub struct Fds {
    /// Listening sockets, in the order the host listed its listener ports.
    pub listeners: Vec<RawFd>,
    /// The dialer.
    pub dial: RawFd,
}

impl Fds {
    /// Reads `KRABKA_LISTEN_FDS` and `KRABKA_DIAL_FD`.
    pub fn from_env() -> io::Result<Self> {
        let listeners = env("KRABKA_LISTEN_FDS")?
            .split(',')
            .filter(|part| !part.trim().is_empty())
            .map(|part| parse_fd("KRABKA_LISTEN_FDS", part))
            .collect::<io::Result<Vec<_>>>()?;
        let dial = parse_fd("KRABKA_DIAL_FD", &env("KRABKA_DIAL_FD")?)?;
        Ok(Self { listeners, dial })
    }
}

fn env(name: &str) -> io::Result<String> {
    std::env::var(name)
        .map_err(|err| io::Error::new(io::ErrorKind::NotFound, format!("{name}: {err}")))
}

fn parse_fd(name: &str, text: &str) -> io::Result<RawFd> {
    text.trim().parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name}: {text:?}: {err}"),
        )
    })
}

/// Adopts a preopened listening socket as a tokio listener.
pub fn listener(fd: RawFd) -> io::Result<tokio::net::TcpListener> {
    // SAFETY: the runtime preopened `fd` as a listening socket for this process,
    // and nothing else in the guest owns it.
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    listener.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(listener)
}

/// Opens an outbound connection through the dialer descriptor.
///
/// The runtime allocates the socket at once; the connection itself completes
/// asynchronously and shows up as write readiness, or as `ECONNREFUSED` on the
/// first read or write when the host refuses it.
pub fn dial(dial_fd: RawFd, host: &str, port: u16) -> io::Result<std::net::TcpStream> {
    // SAFETY: the runtime preopened `dial_fd` for the whole life of the process;
    // `ManuallyDrop` keeps this temporary `File` from closing it.
    let mut control = ManuallyDrop::new(unsafe { File::from_raw_fd(dial_fd) });
    control.write_all(format!("{host}:{port}\n").as_bytes())?;
    let mut reply = Vec::with_capacity(16);
    let mut buf = [0u8; 64];
    let end = loop {
        if let Some(end) = reply.iter().position(|&b| b == b'\n') {
            break end;
        }
        let n = control.read(&mut buf)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the dialer closed",
            ));
        }
        reply.extend_from_slice(&buf[..n]);
    };
    let line = std::str::from_utf8(&reply[..end])
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("dialer reply: {err}")))?
        .trim();
    if let Some(reason) = line.strip_prefix("ERR ") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            reason.to_owned(),
        ));
    }
    let fd: RawFd = line.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("dialer reply {line:?}: {err}"),
        )
    })?;
    // SAFETY: the runtime allocated `fd` for this connection just now and hands
    // its ownership to the caller.
    let stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    Ok(stream)
}

/// [`dial`], wrapped as a tokio stream.
pub fn connect(dial_fd: RawFd, host: &str, port: u16) -> io::Result<tokio::net::TcpStream> {
    tokio::net::TcpStream::from_std(dial(dial_fd, host, port)?)
}
