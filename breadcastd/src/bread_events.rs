//! `bread.cast.*` event integration: optional, non-blocking. See
//! `EVENTS.md` at the repo root for the full contract. breadcastd works
//! identically with or without breadd running; every call here is
//! fire-and-forget (`BreadClient::emit` never blocks or errors this
//! process) so a missing or restarting breadd never affects discovery or
//! mirroring itself.

use bread_utils::bread_client::{BreadClient, BreadEvent};
use tokio::sync::{mpsc, oneshot};

use crate::daemon::DaemonCommand;

/// This app's id in bread's sibling-app namespace registry
/// (`bread_shared::apps::KNOWN_APPS`); events publish as `bread.cast.*`,
/// commands arrive on `bread.command.cast.*`.
pub const APP_ID: &str = "cast";

pub fn emit_device_found(client: &BreadClient, id: &str, name: &str, model: &str, protocol: &str) {
    client.emit(
        "bread.cast.device_found",
        serde_json::json!({
            "id": id,
            "name": name,
            "model": model,
            "protocol": protocol,
        }),
    );
}

pub fn emit_mirroring_started(client: &BreadClient, device_id: &str, device_name: &str, protocol: &str) {
    client.emit(
        "bread.cast.mirroring_started",
        serde_json::json!({
            "device_id": device_id,
            "device_name": device_name,
            "protocol": protocol,
        }),
    );
}

pub fn emit_mirroring_stopped(client: &BreadClient) {
    client.emit("bread.cast.mirroring_stopped", serde_json::json!({}));
}

pub fn emit_mirroring_failed(client: &BreadClient, device_id: &str, error: &str) {
    client.emit(
        "bread.cast.mirroring_failed",
        serde_json::json!({
            "device_id": device_id,
            "error": error,
        }),
    );
}

/// Reacts to `bread.command.cast.*` verbs, e.g. from a Hyprland keybind.
/// Runs on `BreadClient::subscribe`'s dedicated background thread (a plain
/// `std::thread`, not a tokio worker): `mpsc::Sender::blocking_send` is
/// safe to call from here for exactly that reason, but would panic if
/// called from inside the tokio runtime.
///
/// Both verbs reply is intentionally not awaited: the reply channel exists
/// because `DaemonCommand::StartCast`/`StopCast` need one for the IPC
/// socket's request/response use (see `ipc.rs`), but a fire-and-forget bus
/// command has nowhere to deliver a reply to anyway; the outcome shows up
/// as a `bread.cast.mirroring_started`/`.failed`/`.stopped` event instead
/// (see `daemon.rs`'s `start_cast`/`StopCast` handling).
pub fn handle_command(event: &BreadEvent, daemon_tx: &mpsc::Sender<DaemonCommand>) {
    let Some(verb) = event.event.strip_prefix("bread.command.cast.") else {
        return;
    };
    match verb {
        "start" => {
            let Some(device_id) = event.data.get("device_id").and_then(|v| v.as_str()) else {
                tracing::warn!("bread.command.cast.start missing a string \"device_id\", ignoring");
                return;
            };
            let (reply, _reply_rx) = oneshot::channel();
            if daemon_tx
                .blocking_send(DaemonCommand::StartCast { device_id: device_id.to_string(), reply })
                .is_err()
            {
                tracing::warn!("daemon actor unavailable, dropping bread.command.cast.start");
            }
        }
        "stop" => {
            let (reply, _reply_rx) = oneshot::channel();
            if daemon_tx.blocking_send(DaemonCommand::StopCast { reply }).is_err() {
                tracing::warn!("daemon actor unavailable, dropping bread.command.cast.stop");
            }
        }
        other => tracing::info!(verb = other, "ignoring unknown bread.command.cast verb"),
    }
}
