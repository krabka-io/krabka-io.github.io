//! The descriptors the lab's WASI runtime preopens for the broker: its
//! listening sockets and the dialer.
//!
//! WASI preview 1 has neither `bind` nor `connect`. The runtime preopens one
//! listening socket per port of the node and a dialer, a character device
//! that opens outbound connections: the process writes `host:port\n` and reads
//! back `<fd>\n`, the descriptor of a new socket that is still connecting, or
//! `ERR <reason>\n` for an address it cannot parse. The lab decides the dial
//! afterwards, and its verdict arrives on the socket the way a kernel reports
//! a TCP connect: the socket turns writable, or its first read or write fails
//! with `ConnectionRefused`, `TimedOut` or `HostUnreachable`.
//!
//! Adopting a descriptor the runtime handed over is the only `unsafe` code of
//! this crate, and every adoption happens in this module.

use std::{
    fs::File,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    os::fd::{FromRawFd, RawFd},
    sync::{Mutex, PoisonError},
};

use krabka_client_core::transport::{ConnectFuture, Connector};

/// A dialer reply longer than this is not one.
const MAX_REPLY: usize = 64;

/// Adopts a listening socket the runtime preopened, in non-blocking mode for
/// tokio.
///
/// # Errors
/// Returns the error of switching it to non-blocking mode, which a descriptor
/// that is not a socket gives.
#[allow(unsafe_code)]
pub fn adopt_listener(fd: RawFd) -> io::Result<TcpListener> {
    // SAFETY: the runtime preopens `fd` as a listening socket for the whole
    // life of the process, the contract names it once, and this is the only
    // place that takes it, so the listener is its only owner.
    let listener = unsafe { TcpListener::from_raw_fd(fd) };
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// The process's outbound connections, through the dialer descriptor.
///
/// It is the broker's transport [`Connector`]: raft traffic between the
/// controllers, heartbeats and forwarding to the active controller (this
/// node's own included), replica fetchers and transaction markers all dial
/// through it.
pub struct Dialer {
    control: Mutex<File>,
}

impl Dialer {
    /// Adopts the dialer descriptor for the life of the process.
    #[allow(unsafe_code)]
    #[must_use]
    pub fn adopt(fd: RawFd) -> Self {
        // SAFETY: the runtime preopens `fd` as the dialer for the whole life
        // of the process, the contract names it once, and this is the only
        // place that takes it, so the dialer is its only owner.
        let control = unsafe { File::from_raw_fd(fd) };
        Self {
            control: Mutex::new(control),
        }
    }

    /// Opens a connection to `host:port`, still connecting when it returns.
    #[allow(unsafe_code)]
    fn dial(&self, host: &str, port: u16) -> io::Result<TcpStream> {
        let fd = {
            let mut control = self.control.lock().unwrap_or_else(PoisonError::into_inner);
            round_trip(&mut *control, host, port)?
        };
        // SAFETY: the dialer allocated `fd` for this connection just now and
        // hands it to its caller, who owns it from here on.
        let stream = unsafe { TcpStream::from_raw_fd(fd) };
        stream.set_nonblocking(true)?;
        Ok(stream)
    }
}

impl Connector for Dialer {
    fn connect(&self, host: &str, port: u16) -> ConnectFuture {
        // The round trip does not block: the runtime answers a dial as soon
        // as it reads the line, with a socket whose connection the lab
        // decides afterwards.
        let stream = self.dial(host, port);
        Box::pin(async move { tokio::net::TcpStream::from_std(stream?) })
    }
}

/// Asks the dialer on `control` for a connection to `host:port` and returns
/// the descriptor of its socket.
fn round_trip(control: &mut (impl Read + Write), host: &str, port: u16) -> io::Result<RawFd> {
    control.write_all(dial_line(host, port).as_bytes())?;
    // Byte by byte, so nothing past this reply is taken from the dialer.
    let mut reply = Vec::with_capacity(8);
    let mut byte = [0u8; 1];
    loop {
        match control.read(&mut byte) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the dialer closed",
                ));
            }
            Ok(_) if byte[0] == b'\n' => break,
            Ok(_) if reply.len() < MAX_REPLY => reply.push(byte[0]),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the dialer's reply has no end",
                ));
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "the dialer did not answer",
                ));
            }
            Err(err) => return Err(err),
        }
    }
    parse_reply(&reply)
}

/// The line that asks the dialer for `host:port`; an IPv6 host goes in brackets.
fn dial_line(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}\n")
    } else {
        format!("{host}:{port}\n")
    }
}

/// The descriptor in a dialer reply, or the error it reports.
fn parse_reply(reply: &[u8]) -> io::Result<RawFd> {
    let line = std::str::from_utf8(reply)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("dialer reply: {err}")))?
        .trim();
    if let Some(reason) = line.strip_prefix("ERR ") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("the dialer refused the address: {reason}"),
        ));
    }
    line.parse::<RawFd>()
        .ok()
        .filter(|fd| *fd >= 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the dialer answered {line:?}"),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::assert;

    use super::*;

    /// A dialer that answers with canned bytes, one read at a time, and
    /// records what it was sent.
    struct FakeDialer {
        sent: Vec<u8>,
        replies: VecDeque<io::Result<Vec<u8>>>,
    }

    impl FakeDialer {
        fn answering(replies: Vec<io::Result<Vec<u8>>>) -> Self {
            Self {
                sent: Vec::new(),
                replies: replies.into(),
            }
        }
    }

    impl Write for FakeDialer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.sent.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Read for FakeDialer {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match self.replies.pop_front() {
                None => Ok(0),
                Some(Err(err)) => Err(err),
                Some(Ok(mut bytes)) => {
                    let n = buf.len().min(bytes.len());
                    buf[..n].copy_from_slice(&bytes[..n]);
                    if n < bytes.len() {
                        bytes.drain(..n);
                        self.replies.push_front(Ok(bytes));
                    }
                    Ok(n)
                }
            }
        }
    }

    /// What a round trip returned: the descriptor, or the error's kind and text.
    type Outcome = Result<RawFd, (io::ErrorKind, String)>;

    fn outcome(result: io::Result<RawFd>) -> Outcome {
        result.map_err(|err| (err.kind(), err.to_string()))
    }

    fn failed(kind: io::ErrorKind, text: &str) -> Outcome {
        Err((kind, text.to_owned()))
    }

    /// One dial: the address, the line the dialer should get, its reply and
    /// what the round trip should return.
    struct Case {
        host: &'static str,
        port: u16,
        line: &'static str,
        reply: io::Result<Vec<u8>>,
        expected: Outcome,
    }

    #[test]
    fn a_dial_writes_one_line_and_reads_one_reply() {
        let cases = [
            Case {
                host: "10.0.0.2",
                port: 9093,
                line: "10.0.0.2:9093\n",
                reply: Ok(b"7\n".to_vec()),
                expected: Ok(7),
            },
            Case {
                host: "::1",
                port: 9092,
                line: "[::1]:9092\n",
                reply: Ok(b"12\n".to_vec()),
                expected: Ok(12),
            },
            Case {
                host: "[::1]",
                port: 9092,
                line: "[::1]:9092\n",
                reply: Ok(b"12\n".to_vec()),
                expected: Ok(12),
            },
            Case {
                host: "10.0.0.2",
                port: 0,
                line: "10.0.0.2:0\n",
                reply: Ok(b"ERR bad address \"10.0.0.2:0\"\n".to_vec()),
                expected: failed(
                    io::ErrorKind::InvalidInput,
                    "the dialer refused the address: bad address \"10.0.0.2:0\"",
                ),
            },
            Case {
                host: "10.0.0.2",
                port: 9092,
                line: "10.0.0.2:9092\n",
                reply: Ok(b"seven\n".to_vec()),
                expected: failed(io::ErrorKind::InvalidData, "the dialer answered \"seven\""),
            },
            Case {
                host: "10.0.0.2",
                port: 9092,
                line: "10.0.0.2:9092\n",
                reply: Err(io::ErrorKind::WouldBlock.into()),
                expected: failed(io::ErrorKind::WouldBlock, "the dialer did not answer"),
            },
        ];
        for case in cases {
            let mut dialer = FakeDialer::answering(vec![case.reply]);
            let got = outcome(round_trip(&mut dialer, case.host, case.port));
            assert!(got == case.expected, "{}:{}", case.host, case.port);
            assert!(dialer.sent == case.line.as_bytes());
        }
    }

    #[test]
    fn a_reply_that_comes_in_pieces_is_read_to_its_end_and_no_further() {
        let mut dialer = FakeDialer::answering(vec![
            Ok(b"1".to_vec()),
            Ok(b"4\n".to_vec()),
            Ok(b"15\n".to_vec()),
        ]);
        assert!(outcome(round_trip(&mut dialer, "10.0.0.1", 9093)) == Ok(14));
        assert!(outcome(round_trip(&mut dialer, "10.0.0.3", 9093)) == Ok(15));
        assert!(dialer.sent == b"10.0.0.1:9093\n10.0.0.3:9093\n");
    }

    #[test]
    fn a_dialer_that_closes_or_never_ends_its_reply_fails_the_dial() {
        let cases = [
            (
                Ok(b"3".to_vec()),
                failed(io::ErrorKind::UnexpectedEof, "the dialer closed"),
            ),
            (
                Ok(vec![b'1'; MAX_REPLY + 1]),
                failed(io::ErrorKind::InvalidData, "the dialer's reply has no end"),
            ),
        ];
        for (reply, expected) in cases {
            let mut dialer = FakeDialer::answering(vec![reply]);
            assert!(outcome(round_trip(&mut dialer, "10.0.0.1", 9093)) == expected);
        }
    }
}
