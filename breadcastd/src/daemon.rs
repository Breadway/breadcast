//! The daemon's single state-owning actor: current `StateInfo`, the known
//! Cast and DLNA device lists, and the active mirroring session (if any)
//! all live here, touched only by this task -- the same "single owner +
//! channel" pattern `breadcast-core::cast_sender::CastSession` uses for its
//! io thread, for the same reason (avoids retrofitting locks around state
//! that multiple IPC connections and two independent discovery loops all
//! need to touch).

use std::collections::HashMap;

use bread_utils::bread_client::BreadClient;
use breadcast_core::ipc::{DeviceInfo, Protocol, ServerMessage, StateInfo};
use breadcast_core::{CastDevice, DlnaDevice};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::bread_events;
use crate::cast_mirror::CastMirrorSession;
use crate::dlna_mirror::DlnaMirrorSession;

pub enum DaemonCommand {
    ListDevices { reply: oneshot::Sender<Vec<DeviceInfo>> },
    GetState { reply: oneshot::Sender<StateInfo> },
    StartCast { device_id: String, reply: oneshot::Sender<Result<(), String>> },
    StopCast { reply: oneshot::Sender<Result<(), String>> },
    CastDeviceFound(CastDevice),
    CastDeviceLost(String),
    DlnaDeviceFound(DlnaDevice),
    /// DLNA devices have no separate stable id -- their description URL
    /// (`DlnaDevice::url`) doubles as one, see `DlnaDevice`'s doc comment.
    DlnaDeviceLost(String),
    /// Sent by an active session's own background thread/task when it ends
    /// on its own (portal "stop sharing", a GStreamer error, the receiver
    /// dropping the connection or stopping playback) -- as opposed to
    /// `StopCast` being called. Either way the daemon needs to forget the
    /// (now-dead) session and go back to `Idle`.
    SessionEnded,
}

pub fn spawn(events_tx: broadcast::Sender<ServerMessage>, bread_client: BreadClient) -> mpsc::Sender<DaemonCommand> {
    let (command_tx, mut command_rx) = mpsc::channel(32);
    let self_tx = command_tx.clone();
    tokio::spawn(async move {
        let mut daemon = Daemon {
            cast_devices: HashMap::new(),
            dlna_devices: HashMap::new(),
            state: StateInfo::Idle,
            active_session: None,
            events_tx,
            bread_client,
            self_tx,
        };
        while let Some(command) = command_rx.recv().await {
            daemon.handle(command).await;
        }
    });
    command_tx
}

/// The currently active mirroring session, if any -- exactly one of the two
/// protocol-specific session types, chosen by which device map `StartCast`
/// found the requested device id in.
enum ActiveSession {
    Cast(CastMirrorSession),
    Dlna(Box<DlnaMirrorSession>),
}

impl ActiveSession {
    async fn stop(self) {
        match self {
            ActiveSession::Cast(session) => session.stop().await,
            ActiveSession::Dlna(session) => session.stop().await,
        }
    }
}

struct Daemon {
    cast_devices: HashMap<String, CastDevice>,
    /// Keyed by `DlnaDevice::url`, the closest thing DLNA has to a stable
    /// device id -- see `breadcast_core::ipc::DeviceInfo`'s doc comment.
    dlna_devices: HashMap<String, DlnaDevice>,
    state: StateInfo,
    active_session: Option<ActiveSession>,
    events_tx: broadcast::Sender<ServerMessage>,
    /// Used to publish `bread.cast.mirroring_started`/`.stopped`/`.failed`
    /// on state transitions -- see `bread_events.rs`. A no-op if breadd
    /// isn't running (see that module's doc comment).
    bread_client: BreadClient,
    /// A clone of this actor's own command sender, handed to each mirror
    /// session so its background pump/poll can report unprompted death
    /// (see `DaemonCommand::SessionEnded`) without this module needing to
    /// expose anything beyond the command channel itself.
    self_tx: mpsc::Sender<DaemonCommand>,
}

impl Daemon {
    async fn handle(&mut self, command: DaemonCommand) {
        match command {
            DaemonCommand::ListDevices { reply } => {
                let _ = reply.send(self.device_list());
            }
            DaemonCommand::GetState { reply } => {
                let _ = reply.send(self.state.clone());
            }
            DaemonCommand::StartCast { device_id, reply } => {
                self.start_cast(device_id, reply).await;
            }
            DaemonCommand::StopCast { reply } => {
                if let Some(session) = self.active_session.take() {
                    session.stop().await;
                    bread_events::emit_mirroring_stopped(&self.bread_client);
                }
                self.state = StateInfo::Idle;
                self.broadcast_state();
                let _ = reply.send(Ok(()));
            }
            DaemonCommand::SessionEnded => {
                // The session already tore itself down (that's what
                // triggered this) -- just drop our handle to it and update
                // state. Ignored if this arrives after an explicit
                // `StopCast` already cleared `active_session` (the
                // background pump/poll it came from may briefly outlive
                // that call).
                if self.active_session.take().is_some() {
                    tracing::info!("mirror session ended on its own, returning to idle");
                    self.state = StateInfo::Idle;
                    self.broadcast_state();
                    bread_events::emit_mirroring_stopped(&self.bread_client);
                }
            }
            DaemonCommand::CastDeviceFound(device) => {
                self.cast_devices.insert(device.id.clone(), device);
                self.broadcast_devices();
            }
            DaemonCommand::CastDeviceLost(id) => {
                self.cast_devices.remove(&id);
                self.broadcast_devices();
            }
            DaemonCommand::DlnaDeviceFound(device) => {
                self.dlna_devices.insert(device.url.clone(), device);
                self.broadcast_devices();
            }
            DaemonCommand::DlnaDeviceLost(url) => {
                self.dlna_devices.remove(&url);
                self.broadcast_devices();
            }
        }
    }

    async fn start_cast(&mut self, device_id: String, reply: oneshot::Sender<Result<(), String>>) {
        if self.active_session.is_some() {
            let _ = reply.send(Err("already casting -- stop the current session first".to_string()));
            return;
        }

        if let Some(device) = self.cast_devices.get(&device_id).cloned() {
            match CastMirrorSession::start(device.clone(), self.self_tx.clone()).await {
                Ok(session) => {
                    self.active_session = Some(ActiveSession::Cast(session));
                    self.state =
                        StateInfo::Casting { device_id: device.id.clone(), device_name: device.name.clone(), protocol: Protocol::Cast };
                    self.broadcast_state();
                    bread_events::emit_mirroring_started(&self.bread_client, &device.id, &device.name, "cast");
                    let _ = reply.send(Ok(()));
                }
                Err(e) => {
                    // `{e:#}` (not `{e}`/`to_string()`) so the full anyhow
                    // context chain reaches the caller/GUI instead of just
                    // the outermost ".context()" message.
                    bread_events::emit_mirroring_failed(&self.bread_client, &device.id, &format!("{e:#}"));
                    let _ = reply.send(Err(format!("{e:#}")));
                }
            }
            return;
        }

        if let Some(device) = self.dlna_devices.get(&device_id).cloned() {
            match DlnaMirrorSession::start(device.clone(), self.self_tx.clone()).await {
                Ok(session) => {
                    self.active_session = Some(ActiveSession::Dlna(Box::new(session)));
                    self.state = StateInfo::Casting {
                        device_id: device.url.clone(),
                        device_name: device.friendly_name.clone(),
                        protocol: Protocol::Dlna,
                    };
                    self.broadcast_state();
                    bread_events::emit_mirroring_started(&self.bread_client, &device.url, &device.friendly_name, "dlna");
                    let _ = reply.send(Ok(()));
                }
                Err(e) => {
                    bread_events::emit_mirroring_failed(&self.bread_client, &device.url, &format!("{e:#}"));
                    let _ = reply.send(Err(format!("{e:#}")));
                }
            }
            return;
        }

        let _ = reply.send(Err(format!("unknown device id \"{device_id}\"")));
    }

    fn device_list(&self) -> Vec<DeviceInfo> {
        let mut devices: Vec<DeviceInfo> = self
            .cast_devices
            .values()
            .map(|d| DeviceInfo { id: d.id.clone(), name: d.name.clone(), model: d.model.clone(), protocol: Protocol::Cast })
            .collect();
        devices.extend(self.dlna_devices.values().map(|d| DeviceInfo {
            id: d.url.clone(),
            name: d.friendly_name.clone(),
            model: "DLNA renderer".to_string(),
            protocol: Protocol::Dlna,
        }));
        devices
    }

    fn broadcast_state(&self) {
        let data = serde_json::to_value(&self.state).expect("StateInfo always serializes");
        let _ = self.events_tx.send(ServerMessage::Event { event: "state_changed".to_string(), data });
    }

    fn broadcast_devices(&self) {
        let data = serde_json::to_value(self.device_list()).expect("Vec<DeviceInfo> always serializes");
        let _ = self.events_tx.send(ServerMessage::Event { event: "device_list_changed".to_string(), data });
    }
}
