//! A thin, synchronous client for breadcastd's IPC socket (see
//! `breadcast_core::ipc` for the wire types and
//! `breadcastd/src/ipc.rs` for the server side). No async runtime here:
//! GTK4 already has its own main loop (glib), so this uses a plain
//! blocking reader thread feeding an `mpsc::Receiver` the GTK side polls
//! via `glib::timeout_add_local` (see `main.rs`), rather than pulling in
//! tokio just for one socket.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

use anyhow::{Context, Result};
use breadcast_core::ipc::{ClientRequest, ServerMessage, socket_path};

pub struct IpcClient {
    write_half: UnixStream,
    next_id: AtomicU64,
}

impl IpcClient {
    /// Connects to breadcastd's socket and starts a background thread
    /// forwarding every parsed `ServerMessage` (both responses and pushed
    /// events; the caller distinguishes them, see `ServerMessage`'s
    /// variants) to the returned receiver, until the connection closes
    /// (breadcastd not running, or it exited).
    pub fn connect() -> Result<(Self, mpsc::Receiver<ServerMessage>)> {
        let path = socket_path()?;
        let write_half = UnixStream::connect(&path)
            .with_context(|| format!("failed to connect to breadcastd at {}, is it running?", path.display()))?;
        // A wedged daemon must not freeze the GTK main thread on write.
        let _ = write_half.set_write_timeout(Some(std::time::Duration::from_secs(5)));
        let read_half = write_half.try_clone().context("failed to duplicate the IPC socket handle")?;

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let reader = BufReader::new(read_half);
            for line in reader.lines() {
                let Ok(line) = line else { break };
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<ServerMessage>(&line) {
                    Ok(message) => {
                        if tx.send(message).is_err() {
                            break;
                        }
                    }
                    Err(e) => eprintln!("breadcast: malformed IPC message from breadcastd, ignoring: {e} ({line})"),
                }
            }
        });

        Ok((Self { write_half, next_id: AtomicU64::new(1) }, rx))
    }

    /// Sends a request and returns its id (so the caller can match it
    /// against the `ServerMessage::Response` that arrives later on the
    /// receiver from [`Self::connect`]: this method doesn't itself wait
    /// for a reply, matching the GTK main loop's non-blocking event style).
    pub fn send(&mut self, method: &str, params: serde_json::Value) -> Result<u64> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = ClientRequest { id, method: method.to_string(), params };
        let mut line = serde_json::to_string(&request).context("failed to serialize IPC request")?;
        line.push('\n');
        self.write_half.write_all(line.as_bytes()).context("failed to write to breadcastd's IPC socket")?;
        Ok(id)
    }
}
