//! breadcastd — background daemon: mDNS (Cast) and SSDP (DLNA) device
//! discovery, both mirroring session lifecycles (Cast Streaming and
//! DLNA/AVTransport), and a private IPC socket the `breadcast` GTK4 popup
//! talks to. Also does optional breadd event integration for external
//! automation (a Hyprland keybind, etc.) — see `bread_events.rs`; that's
//! separate from (and does not replace) the IPC socket, since a popup UI
//! needs live pushed state that a best-effort pub/sub bus doesn't fit as
//! naturally. See `ipc.rs`'s doc comment.

mod bread_events;
mod cast_mirror;
mod daemon;
mod dlna_mirror;
mod ipc;

use std::collections::HashMap;

use bread_utils::singleton::{Acquire, try_acquire};
use breadcast_core::{CastDevice, DlnaDevice, DlnaDiscovery, DlnaDiscoveryEvent, Discovery, DiscoveryEvent};
use daemon::DaemonCommand;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let _guard = match try_acquire(bread_events::APP_ID)? {
        Acquire::Acquired(guard) => guard,
        Acquire::HeldByOther(pid) => {
            tracing::error!(?pid, "breadcastd already running, exiting");
            std::process::exit(1);
        }
    };

    let bread_client = bread_utils::bread_client::BreadClient::connect(bread_events::APP_ID);

    let (events_tx, _events_rx) = tokio::sync::broadcast::channel(64);
    let daemon_tx = daemon::spawn(events_tx.clone(), bread_client.clone());

    let commands_daemon_tx = daemon_tx.clone();
    let _commands = bread_client.subscribe("bread.command.cast.**", move |event| {
        bread_events::handle_command(&event, &commands_daemon_tx);
    });

    let ipc_daemon_tx = daemon_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = ipc::serve(ipc_daemon_tx, events_tx).await {
            tracing::error!(error = ?e, "IPC server ended unexpectedly");
        }
    });

    let (_discovery, mut cast_events) = Discovery::start()?;
    let (_dlna_discovery, mut dlna_events) = DlnaDiscovery::start();
    tracing::info!("breadcastd started, browsing for Cast and DLNA devices");

    // `DiscoveryEvent::Found`/`DlnaDiscoveryEvent::Found` fire on every
    // re-resolution/re-poll (see `discovery.rs`'s and `dlna/discovery.rs`'s
    // doc comments), not just the first sighting. Every event is forwarded
    // to the daemon actor unconditionally (its `HashMap` re-insert is a
    // harmless no-op for an unchanged device, aside from a redundant but
    // cheap `device_list_changed` broadcast) — but `bread.cast.device_found`
    // on the breadd bus is gated on an actual *change*, so anything
    // subscribed there expecting "on device_found, do X" doesn't fire
    // repeatedly for the same device.
    let mut known_cast_devices: HashMap<String, CastDevice> = HashMap::new();
    let mut known_dlna_devices: HashMap<String, DlnaDevice> = HashMap::new();

    loop {
        tokio::select! {
            event = cast_events.recv() => {
                match event {
                    Some(DiscoveryEvent::Found(device)) => {
                        if known_cast_devices.get(&device.id) != Some(&device) {
                            tracing::info!(?device, "Cast device found");
                            bread_events::emit_device_found(&bread_client, &device.id, &device.name, &device.model, "cast");
                            known_cast_devices.insert(device.id.clone(), device.clone());
                            // Forward only on an actual change. The daemon
                            // actor is single-threaded and `Found` fires on
                            // every mDNS re-resolution (per address, per
                            // re-browse), so forwarding identical devices too
                            // would spam `device_list_changed` broadcasts and
                            // delay `list_devices`/`start_cast` replies that
                            // share the same actor queue. The daemon's own
                            // `CastDeviceLost`→refind logic still works, since
                            // a loss is a state change on its side too.
                            let _ = daemon_tx.send(DaemonCommand::CastDeviceFound(device)).await;
                        }
                    }
                    Some(DiscoveryEvent::Lost { id }) => {
                        tracing::info!(%id, "Cast device lost");
                        known_cast_devices.remove(&id);
                        let _ = daemon_tx.send(DaemonCommand::CastDeviceLost(id)).await;
                    }
                    // The mDNS browse thread itself ended (daemon shutdown,
                    // an unrecoverable browse error) — that's Cast
                    // discovery's one job gone, not a normal exit.
                    // Returning `Ok(())` from `main` here would exit 0, and
                    // systemd's `Restart=on-failure` would never trigger.
                    None => {
                        tracing::error!("Cast discovery channel closed unexpectedly, exiting");
                        std::process::exit(1);
                    }
                }
            }
            event = dlna_events.recv() => {
                match event {
                    Some(DlnaDiscoveryEvent::Found(device)) => {
                        if known_dlna_devices.get(&device.url) != Some(&device) {
                            tracing::info!(?device, "DLNA device found");
                            bread_events::emit_device_found(&bread_client, &device.url, &device.friendly_name, "DLNA renderer", "dlna");
                            known_dlna_devices.insert(device.url.clone(), device.clone());
                            // Same rationale as the Cast arm above: each SSDP
                            // poll only reports devices it *this* poll
                            // confirmed, but a device re-confirmed unchanged
                            // needs no daemon flush.
                            let _ = daemon_tx.send(DaemonCommand::DlnaDeviceFound(device)).await;
                        }
                    }
                    Some(DlnaDiscoveryEvent::Lost { url }) => {
                        tracing::info!(%url, "DLNA device lost");
                        known_dlna_devices.remove(&url);
                        let _ = daemon_tx.send(DaemonCommand::DlnaDeviceLost(url)).await;
                    }
                    // Same reasoning as the Cast arm above -- `DlnaDiscovery`'s
                    // background task only ends via a panic or this process
                    // dropping every sender, neither of which should happen
                    // while this loop is still the one holding the receiver.
                    None => {
                        tracing::error!("DLNA discovery channel closed unexpectedly, exiting");
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}
