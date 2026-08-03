//! breadcastd's private control socket: newline-delimited JSON over a
//! `UnixListener` at `$XDG_RUNTIME_DIR/breadcast/breadcastd.sock`, separate
//! from breadd's own pub/sub bus (`bread_events.rs`) — that's for external
//! automation (a Hyprland keybind emitting `bread.command.cast.*`), this is
//! for the `breadcast` GTK4 popup's actual live state, which needs pushed
//! updates a best-effort pub/sub bus doesn't fit as naturally.
//!
//! The wire message types (`ClientRequest`/`ServerMessage`/`StateInfo`) live
//! in `breadcast_core::ipc`, shared with the `breadcast` GUI client so the
//! two never drift out of sync.
//!
//! Multiple clients (in practice: zero or one `breadcast` popup instance at
//! a time, but nothing here assumes that) can connect concurrently; each
//! gets its own copy of every broadcast event.

use anyhow::{Context, Result};
use breadcast_core::ipc::{ClientRequest, ServerMessage, socket_path};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc};

use crate::daemon::DaemonCommand;

/// Removes any stale socket left behind by an unclean shutdown, then binds
/// and serves forever. `daemon_tx` is how request handling reaches the
/// daemon's single state-owning actor (see `daemon.rs`); `events_tx`'s
/// receiver half is (re-)subscribed per connection, so every connection
/// sees the same `device_list_changed`/`state_changed` events from the
/// point it connects onward.
pub async fn serve(daemon_tx: mpsc::Sender<DaemonCommand>, events_tx: broadcast::Sender<ServerMessage>) -> Result<()> {
    let socket_path = socket_path()?;
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create socket dir {}", parent.display()))?;
    }
    // A stale socket file from an unclean previous exit makes bind() fail
    // with AddrInUse even though nothing is actually listening -- the
    // daemon's own singleton lock (see main.rs) already guarantees at most
    // one live breadcastd, so it's always safe to clear this before binding.
    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("failed to bind {}", socket_path.display()))?;
    tracing::info!(path = %socket_path.display(), "IPC socket listening");

    loop {
        let (stream, _addr) = listener.accept().await.context("failed to accept IPC connection")?;
        let daemon_tx = daemon_tx.clone();
        let events_rx = events_tx.subscribe();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, daemon_tx, events_rx).await {
                tracing::debug!(error = %e, "IPC connection ended");
            }
        });
    }
}

async fn handle_connection(
    stream: UnixStream,
    daemon_tx: mpsc::Sender<DaemonCommand>,
    mut events_rx: broadcast::Receiver<ServerMessage>,
) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    // A single task owns the write half and serializes everything written
    // to it (responses and pushed events both funnel through `writer_tx`)
    // -- two independent tasks (one per direction) writing directly to the
    // same stream could interleave partial JSON lines.
    let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<ServerMessage>();
    let writer_task = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(msg) = writer_rx.recv().await {
            let Ok(mut line) = serde_json::to_string(&msg) else { continue };
            line.push('\n');
            if write_half.write_all(line.as_bytes()).await.is_err() {
                return;
            }
        }
    });

    let forward_tx = writer_tx.clone();
    let forward_task = tokio::spawn(async move {
        loop {
            match events_rx.recv().await {
                Ok(event) => {
                    if forward_tx.send(event).is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });

    while let Some(line) = reader.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let request: ClientRequest = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(e) => {
                tracing::debug!(error = %e, %line, "malformed IPC request, ignoring");
                continue;
            }
        };
        let response = handle_request(request, &daemon_tx).await;
        if writer_tx.send(response).is_err() {
            break;
        }
    }

    forward_task.abort();
    drop(writer_tx);
    let _ = writer_task.await;
    Ok(())
}

async fn handle_request(request: ClientRequest, daemon_tx: &mpsc::Sender<DaemonCommand>) -> ServerMessage {
    let id = request.id;
    match request.method.as_str() {
        "list_devices" => {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            if daemon_tx.send(DaemonCommand::ListDevices { reply: reply_tx }).await.is_err() {
                return ServerMessage::err(id, "daemon actor has stopped");
            }
            match reply_rx.await {
                Ok(devices) => ServerMessage::ok(id, serde_json::json!(devices)),
                Err(_) => ServerMessage::err(id, "daemon actor dropped the reply"),
            }
        }
        "get_state" => {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            if daemon_tx.send(DaemonCommand::GetState { reply: reply_tx }).await.is_err() {
                return ServerMessage::err(id, "daemon actor has stopped");
            }
            match reply_rx.await {
                Ok(state) => ServerMessage::ok(id, serde_json::json!(state)),
                Err(_) => ServerMessage::err(id, "daemon actor dropped the reply"),
            }
        }
        "start_cast" => {
            let Some(device_id) = request.params.get("device_id").and_then(Value::as_str) else {
                return ServerMessage::err(id, "start_cast requires a string \"device_id\" param");
            };
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            if daemon_tx
                .send(DaemonCommand::StartCast { device_id: device_id.to_string(), reply: reply_tx })
                .await
                .is_err()
            {
                return ServerMessage::err(id, "daemon actor has stopped");
            }
            match reply_rx.await {
                Ok(Ok(())) => ServerMessage::ok(id, Value::Null),
                Ok(Err(e)) => ServerMessage::err(id, e),
                Err(_) => ServerMessage::err(id, "daemon actor dropped the reply"),
            }
        }
        "stop_cast" => {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            if daemon_tx.send(DaemonCommand::StopCast { reply: reply_tx }).await.is_err() {
                return ServerMessage::err(id, "daemon actor has stopped");
            }
            match reply_rx.await {
                Ok(Ok(())) => ServerMessage::ok(id, Value::Null),
                Ok(Err(e)) => ServerMessage::err(id, e),
                Err(_) => ServerMessage::err(id, "daemon actor dropped the reply"),
            }
        }
        other => ServerMessage::err(id, format!("unknown method \"{other}\"")),
    }
}
