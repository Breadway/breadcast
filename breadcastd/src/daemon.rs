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
    SessionEnded { generation: u64 },
    /// Result of a `StartCast` that ran off this actor (the portal picker
    /// and OFFER/ANSWER wait must not stall `list_devices` / `stop_cast`).
    /// Ignored when `generation` no longer matches -- that means `StopCast`
    /// cancelled the in-flight start.
    StartFinished {
        generation: u64,
        outcome: Result<StartedSession, StartFailed>,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// A session that finished starting, ready to be installed as `active_session`.
pub(crate) struct StartedSession {
    pub session: ActiveSession,
    pub device_id: String,
    pub device_name: String,
    pub protocol: Protocol,
}

pub(crate) struct StartFailed {
    pub device_id: String,
    pub error: String,
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
            starting: false,
            start_generation: 0,
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
pub(crate) enum ActiveSession {
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

/// `host` strings come straight from mDNS resolution, so this just checks
/// the address, not whether a zone id is attached (mDNS never gives us one).
fn is_link_local_v6(host: &str) -> bool {
    matches!(host.parse::<std::net::IpAddr>(), Ok(std::net::IpAddr::V6(v6)) if (v6.segments()[0] & 0xffc0) == 0xfe80)
}

struct Daemon {
    cast_devices: HashMap<String, CastDevice>,
    /// Keyed by `DlnaDevice::url`, the closest thing DLNA has to a stable
    /// device id -- see `breadcast_core::ipc::DeviceInfo`'s doc comment.
    dlna_devices: HashMap<String, DlnaDevice>,
    state: StateInfo,
    active_session: Option<ActiveSession>,
    /// True while a `StartCast` is running off this actor (portal picker /
    /// negotiation). Distinct from `active_session` so a second start is
    /// rejected before the first one has a session to install.
    starting: bool,
    /// Bumped by `StopCast` so a `StartFinished` from a cancelled start
    /// tears its session down instead of installing it.
    start_generation: u64,
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
                if self.starting {
                    // Invalidate the in-flight start so its StartFinished
                    // tears the session down instead of installing it.
                    self.starting = false;
                    self.start_generation = self.start_generation.wrapping_add(1);
                }
                if let Some(session) = self.active_session.take() {
                    session.stop().await;
                    bread_events::emit_mirroring_stopped(&self.bread_client);
                }
                self.state = StateInfo::Idle;
                self.broadcast_state();
                let _ = reply.send(Ok(()));
            }
            DaemonCommand::SessionEnded { generation } => {
                // Ignore a late notification from a session that already
                // stopped (or whose start was cancelled). An abandoned
                // pump after a 5s join timeout used to take() the *next*
                // session and flip the UI to Idle while it was still live.
                if generation != self.start_generation {
                    return;
                }
                // The session's pump/poll noticed death, but the session
                // handle itself has *not* been torn down -- there is no
                // Drop impl. Dropping it here used to skip CastSession::stop
                // (TV left on the last frame), skip portal close (PipeWire
                // leak), and destroy the FFI sender while pump threads were
                // still calling into it. Always run the ordered stop.
                if let Some(session) = self.active_session.take() {
                    tracing::info!("mirror session ended on its own, returning to idle");
                    session.stop().await;
                    self.start_generation = self.start_generation.wrapping_add(1);
                    self.state = StateInfo::Idle;
                    self.broadcast_state();
                    bread_events::emit_mirroring_stopped(&self.bread_client);
                }
            }
            DaemonCommand::StartFinished { generation, outcome, reply } => {
                self.on_start_finished(generation, outcome, reply).await;
            }
            DaemonCommand::CastDeviceFound(device) => {
                // mDNS resolves one physical device on every local address
                // it has -- typically a private IPv4 and a link-local IPv6
                // -- as separate events carrying the same id. A bare
                // `fe80::` address has no interface scope attached, so
                // `TcpStream::connect`ing to it fails outright; don't let
                // one clobber an already-usable host just because it
                // happened to resolve more recently.
                let should_replace = !matches!(
                    self.cast_devices.get(&device.id),
                    Some(existing) if is_link_local_v6(&device.host) && !is_link_local_v6(&existing.host)
                );
                if should_replace {
                    self.cast_devices.insert(device.id.clone(), device);
                    self.broadcast_devices();
                }
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
        if self.active_session.is_some() || self.starting {
            let _ = reply.send(Err("already casting -- stop the current session first".to_string()));
            return;
        }

        if let Some(device) = self.cast_devices.get(&device_id).cloned() {
            self.starting = true;
            let generation = self.start_generation;
            let daemon_tx = self.self_tx.clone();
            tokio::spawn(async move {
                let outcome = match CastMirrorSession::start(device.clone(), daemon_tx.clone(), generation).await {
                    Ok(session) => Ok(StartedSession {
                        session: ActiveSession::Cast(session),
                        device_id: device.id,
                        device_name: device.name,
                        protocol: Protocol::Cast,
                    }),
                    // `{e:#}` (not `{e}`/`to_string()`) so the full anyhow
                    // context chain reaches the caller/GUI instead of just
                    // the outermost ".context()" message.
                    Err(e) => Err(StartFailed { device_id: device.id, error: format!("{e:#}") }),
                };
                let _ = daemon_tx.send(DaemonCommand::StartFinished { generation, outcome, reply }).await;
            });
            return;
        }

        if let Some(device) = self.dlna_devices.get(&device_id).cloned() {
            self.starting = true;
            let generation = self.start_generation;
            let daemon_tx = self.self_tx.clone();
            tokio::spawn(async move {
                let outcome = match DlnaMirrorSession::start(device.clone(), daemon_tx.clone(), generation).await {
                    Ok(session) => Ok(StartedSession {
                        session: ActiveSession::Dlna(Box::new(session)),
                        device_id: device.url,
                        device_name: device.friendly_name,
                        protocol: Protocol::Dlna,
                    }),
                    Err(e) => Err(StartFailed { device_id: device.url, error: format!("{e:#}") }),
                };
                let _ = daemon_tx.send(DaemonCommand::StartFinished { generation, outcome, reply }).await;
            });
            return;
        }

        let error = format!("unknown device id \"{device_id}\"");
        bread_events::emit_mirroring_failed(&self.bread_client, &device_id, &error);
        let _ = reply.send(Err(error));
    }

    async fn on_start_finished(
        &mut self,
        generation: u64,
        outcome: Result<StartedSession, StartFailed>,
        reply: oneshot::Sender<Result<(), String>>,
    ) {
        if generation != self.start_generation || !self.starting {
            // StopCast cancelled this start while the portal/negotiation
            // was still running. Tear the session down if it succeeded
            // anyway, so we don't leak a sender or leave the TV casting.
            if let Ok(started) = outcome {
                started.session.stop().await;
            }
            let _ = reply.send(Err("start cancelled".to_string()));
            return;
        }
        self.starting = false;
        match outcome {
            Ok(started) => {
                self.active_session = Some(started.session);
                self.state = StateInfo::Casting {
                    device_id: started.device_id.clone(),
                    device_name: started.device_name.clone(),
                    protocol: started.protocol,
                };
                self.broadcast_state();
                let protocol = match started.protocol {
                    Protocol::Cast => "cast",
                    Protocol::Dlna => "dlna",
                };
                bread_events::emit_mirroring_started(
                    &self.bread_client,
                    &started.device_id,
                    &started.device_name,
                    protocol,
                );
                let _ = reply.send(Ok(()));
            }
            Err(failed) => {
                bread_events::emit_mirroring_failed(&self.bread_client, &failed.device_id, &failed.error);
                let _ = reply.send(Err(failed.error));
            }
        }
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
