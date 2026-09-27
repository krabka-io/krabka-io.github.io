//! Echo servers: bytes back unchanged, or with ASCII letters upper-cased.

use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::Guest;

/// Accepts connections on `listener` forever, one echo task each.
pub async fn serve(listener: TcpListener, upper: bool, guest: Arc<Guest>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let guest = Arc::clone(&guest);
                tokio::spawn(async move {
                    if let Err(err) = echo(stream, upper, &guest).await {
                        eprintln!("[guest] echo connection failed: {err}");
                    }
                });
            }
            Err(err) => {
                eprintln!("[guest] accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Echoes until the peer shuts its side down, then shuts ours down.
async fn echo(mut stream: TcpStream, upper: bool, guest: &Guest) -> io::Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return stream.shutdown().await;
        }
        let chunk = &mut buf[..n];
        if upper {
            chunk.make_ascii_uppercase();
        }
        stream.write_all(chunk).await?;
        guest.echoed[usize::from(upper)].fetch_add(n as u64, Ordering::Relaxed);
    }
}
