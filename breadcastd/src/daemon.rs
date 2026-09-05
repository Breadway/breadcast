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
    /// A real `CastMirrorSession`/`DlnaMirrorSession` needs a portal
    /// capture session, a GStreamer encode pipeline, and (for `Cast`) a
    /// live receiver on the LAN, so it can't be built in a unit test (see
    /// the `#[cfg(test)] mod tests` doc comment below). This stub stands in
    /// for "a session is installed" wherever a test needs `active_session`
    /// to be `Some` without any of that -- `stop()` is a no-op.
    #[cfg(test)]
    Stub,
}

impl ActiveSession {
    async fn stop(self) {
        match self {
            ActiveSession::Cast(session) => session.stop().await,
            ActiveSession::Dlna(session) => session.stop().await,
            #[cfg(test)]
            ActiveSession::Stub => {}
        }
    }
}

/// `host` strings come straight from mDNS resolution, and mdns-sd renders a
/// link-local IPv6 it resolved via `ScopedIp` with a `%zone` suffix (e.g.
/// `fe80::…%wlan0`) so the address stays connectable. `IpAddr` can't parse
/// that suffix (std rejects zone-ids in `FromStr`), so the zone has to be
/// stripped before the network prefix can be examined -- otherwise a scoped
/// `fe80::…%iface` host would fail this check and, in `CastDeviceFound`, be
/// allowed to clobber a good routable host that `rust_cast` could actually
/// connect to.
fn is_link_local_v6(host: &str) -> bool {
    let host = host.split('%').next().unwrap_or(host);
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
                    // Bump the generation here too, exactly as the
                    // SessionEnded handler below already does when a
                    // session ends on its own. Without this, a stop of an
                    // *installed* session (as opposed to an in-flight
                    // start) left `start_generation` unchanged, so a late
                    // `SessionEnded` from this session's own background
                    // thread/task -- sent after `stop()` already returned,
                    // e.g. DLNA's bus-watcher thread, which isn't joined
                    // before `stop()` returns -- could match whatever gets
                    // started next and wrongly cancel or tear it down
                    // (CAST-4).
                    self.start_generation = self.start_generation.wrapping_add(1);
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
                    return;
                }
                // No installed session, but a start is currently in flight
                // (portal picker / OFFER-ANSWER). The event pump only reports
                // SessionEnded once negotiation succeeded, so a matching
                // generation here means the in-flight session died between
                // `Negotiated` and `StartFinished` landing. If we ignore it,
                // the pending `StartFinished` would install the dead session
                // and leave the UI stuck on "Casting" until the frame pump
                // happens to notice. Invalidate the pending start exactly as
                // `StopCast` does -- bump the generation so
                // `on_start_finished` tears the session down instead of
                // installing it.
                if self.starting {
                    tracing::debug!("in-flight mirror session ended before it was installed");
                    self.starting = false;
                    self.start_generation = self.start_generation.wrapping_add(1);
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bread_utils::bread_client::BreadClient;
    use tokio::sync::{broadcast, mpsc, oneshot};

    use super::*;

    /// A daemon parked mid-`StartCast`: `starting` is set and a concrete
    /// generation is outstanding, but no `StartFinished` has landed yet.
    /// This is exactly the window both races below target -- the portal
    /// picker / OFFER/ANSWER negotiation is still running off the actor,
    /// and the session that `StartFinished` would install is not installed.
    ///
    /// A real `MirrorSession` can't be built in a unit test (it needs the
    /// portal capture session, a GStreamer encode pipeline, and a live
    /// renderer on the LAN), so the `Stale StartFinished` probes below use
    /// an `Err(StartFailed)` outcome. That still exercises the whole point
    /// of the guard: `on_start_finished` checks `generation`/`starting`
    /// *before* branching on the outcome, so the stale event is rejected
    /// with "start cancelled", never installs a session, and the UI stays
    /// `Idle`. The `Ok` branch of that same guard calls
    /// `started.session.stop()` to tear the dead session down -- the literal
    /// teardown line -- which is the only part not separately asserted here.
    fn daemon_with_in_flight_start(generation: u64) -> (Daemon, broadcast::Receiver<ServerMessage>) {
        let (events_tx, events_rx) = broadcast::channel(16);
        let (self_tx, _self_rx) = mpsc::channel(16);
        let daemon = Daemon {
            cast_devices: HashMap::new(),
            dlna_devices: HashMap::new(),
            state: StateInfo::Idle,
            active_session: None,
            starting: true,
            start_generation: generation,
            events_tx,
            bread_client: BreadClient::connect("daemon-test"),
            self_tx,
        };
        (daemon, events_rx)
    }

    /// A daemon with a session already installed (the normal "actively
    /// casting" state) at the given generation -- as opposed to
    /// `daemon_with_in_flight_start`, which is mid-`StartCast` with nothing
    /// installed yet. Used by the `StopCast`-on-an-installed-session race
    /// below (CAST-4): unlike the in-flight-start races, this path takes
    /// `active_session`, so it needs a real `Some(ActiveSession)` --
    /// `ActiveSession::Stub` stands in for the real session (see its doc
    /// comment).
    fn daemon_with_installed_session(generation: u64) -> (Daemon, broadcast::Receiver<ServerMessage>) {
        let (events_tx, events_rx) = broadcast::channel(16);
        let (self_tx, _self_rx) = mpsc::channel(16);
        let daemon = Daemon {
            cast_devices: HashMap::new(),
            dlna_devices: HashMap::new(),
            state: StateInfo::Casting {
                device_id: "living-room-tv".to_string(),
                device_name: "Living Room TV".to_string(),
                protocol: Protocol::Dlna,
            },
            active_session: Some(ActiveSession::Stub),
            starting: false,
            start_generation: generation,
            events_tx,
            bread_client: BreadClient::connect("daemon-test"),
            self_tx,
        };
        (daemon, events_rx)
    }

    /// A `StartFinished` carrying the *dead* session's generation -- the
    /// probe both race tests replay after invalidating the in-flight start,
    /// to prove it can't sneak a session back in.
    fn stale_start_finished(reply: oneshot::Sender<Result<(), String>>) -> DaemonCommand {
        DaemonCommand::StartFinished {
            generation: 7,
            outcome: Err(StartFailed {
                device_id: "living-room-tv".to_string(),
                error: "negotiation failed".to_string(),
            }),
            reply,
        }
    }

    #[test]
    fn link_local_v6_is_detected_with_and_without_a_zone() {
        // Scoped form: mdns-sd's ScopedIp Display renders link-local IPv6 as
        // `fe80::…%<iface>`, and IpAddr can't parse the `%` suffix -- this is
        // the exact form the daemon-clobber bug shipped with.
        assert!(is_link_local_v6("fe80::1234%wlan0"));
        assert!(is_link_local_v6("fe80::1234%3"));
        // Unscoped (bare)`fe80::…` also matches.
        assert!(is_link_local_v6("fe80::1234"));
    }

    #[test]
    fn non_link_local_addresses_are_not_marked_link_local() {
        assert!(!is_link_local_v6("fe80"));
        assert!(!is_link_local_v6("192.168.1.50"));
        assert!(!is_link_local_v6("fd00::1"));       // ULA
        assert!(!is_link_local_v6("2606:4700::1111")); // public
        assert!(!is_link_local_v6("2606:4700::1111%wlan0")); // public + spurious zone
        assert!(!is_link_local_v6("not-an-ip"));
    }

    /// The session's event pump reports death (`SessionEnded`) *between*
    /// negotiation succeeding and `StartFinished` landing -- the exact race
    /// that used to install the already-dead session and leave the UI stuck
    /// on "Casting" until the frame pump happened to notice.
    #[tokio::test]
    async fn session_ended_during_an_in_flight_start_invalidates_the_pending_start() {
        let (mut daemon, mut events_rx) = daemon_with_in_flight_start(7);

        daemon.handle(DaemonCommand::SessionEnded { generation: 7 }).await;

        // Invalidate the pending start exactly as StopCast does: clear
        // `starting` and bump the generation so on_start_finished tears the
        // dead session down instead of installing it.
        assert!(!daemon.starting, "in-flight start must be cancelled");
        assert_eq!(daemon.start_generation, 8);
        assert!(daemon.active_session.is_none());
        assert!(matches!(daemon.state, StateInfo::Idle));

        // The stale StartFinished from the dead session must not install it,
        // and the caller must hear that the start was cancelled.
        let (reply, reply_rx) = oneshot::channel();
        daemon.handle(stale_start_finished(reply)).await;

        assert_eq!(reply_rx.await.unwrap(), Err("start cancelled".to_string()));
        assert!(daemon.active_session.is_none(), "dead session must not be installed");
        assert!(matches!(daemon.state, StateInfo::Idle), "UI must return to / stay Idle");

        // A cancelled in-flight start broadcasts nothing -- no bogus
        // state_changed to a UI that is already Idle.
        assert!(events_rx.try_recv().is_err());
    }

    /// `StopCast` while the portal/negotiation is still running must cancel
    /// the in-flight start, so its eventual `StartFinished` is rejected
    /// rather than installing a session the user just explicitly stopped.
    #[tokio::test]
    async fn stop_cast_cancels_an_in_flight_start() {
        let (mut daemon, mut events_rx) = daemon_with_in_flight_start(7);

        let (reply, reply_rx) = oneshot::channel();
        daemon.handle(DaemonCommand::StopCast { reply }).await;

        // StopCast still resolves Ok -- there was nothing installed to stop,
        // only a start to cancel.
        assert!(reply_rx.await.unwrap().is_ok());
        assert!(!daemon.starting, "StopCast must cancel the in-flight start");
        assert_eq!(daemon.start_generation, 8);
        assert!(daemon.active_session.is_none());
        assert!(matches!(daemon.state, StateInfo::Idle));

        // A late StartFinished from the cancelled start never installs.
        let (reply, reply_rx) = oneshot::channel();
        daemon.handle(stale_start_finished(reply)).await;
        assert_eq!(reply_rx.await.unwrap(), Err("start cancelled".to_string()));
        assert!(daemon.active_session.is_none(), "cancelled start must not install a session");
        assert!(matches!(daemon.state, StateInfo::Idle));

        // StopCast itself broadcast the transition back to Idle.
        assert!(events_rx.try_recv().is_ok());
    }

    /// Defensive: a `SessionEnded` whose generation no longer matches (a
    /// late notification from an already-stopped or already-cancelled
    /// session) must be ignored without touching the current generation, so
    /// it can't confuse a brand-new later start.
    #[tokio::test]
    async fn a_stale_session_ended_while_idle_is_ignored_without_bumping_generation() {
        let (mut daemon, _events_rx) = daemon_with_in_flight_start(0);
        daemon.starting = false; // not starting -- the stale event's own forerunner already finished

        daemon.handle(DaemonCommand::SessionEnded { generation: 99 }).await;

        assert!(!daemon.starting);
        assert_eq!(daemon.start_generation, 0, "stale SessionEnded must not bump a future start's generation");
        assert!(daemon.active_session.is_none());
        assert!(matches!(daemon.state, StateInfo::Idle));
    }

    /// CAST-4 regression. `StopCast` on an *installed* session (the normal
    /// "user clicks Stop" path, as opposed to cancelling an in-flight
    /// start) must bump `start_generation` exactly as the `SessionEnded`
    /// handler already does when a session ends on its own -- otherwise a
    /// stop -> start sequence keeps the same generation throughout, and a
    /// late `SessionEnded` from the *stopped* session's own background
    /// thread/task (e.g. DLNA's bus-watcher thread, which used to not be
    /// joined before `stop()` returned, so it could fire an unbounded time
    /// later) is indistinguishable from a legitimate event about whatever
    /// gets started next.
    ///
    /// This drives the race through `Daemon::handle` directly with a
    /// deterministic sequence of commands -- no real session, thread, or
    /// timing is involved, so there's nothing to make flaky.
    #[tokio::test]
    async fn stop_cast_on_an_installed_session_bumps_generation_so_a_stale_watcher_thread_cannot_kill_the_next_session() {
        let (mut daemon, _events_rx) = daemon_with_installed_session(7);

        let (reply, reply_rx) = oneshot::channel();
        daemon.handle(DaemonCommand::StopCast { reply }).await;

        assert!(reply_rx.await.unwrap().is_ok());
        assert!(daemon.active_session.is_none());
        assert_eq!(
            daemon.start_generation, 8,
            "StopCast must bump the generation when it tears down an installed session, not just an in-flight start"
        );
        assert!(matches!(daemon.state, StateInfo::Idle));

        // A new session starts and gets installed under the new
        // generation -- simulating the "quick stop, then start again"
        // sequence from the bug report.
        daemon.active_session = Some(ActiveSession::Stub);
        daemon.state = StateInfo::Casting {
            device_id: "bedroom-tv".to_string(),
            device_name: "Bedroom TV".to_string(),
            protocol: Protocol::Dlna,
        };

        // The OLD (just-stopped) session's bus-watcher thread only now
        // delivers its SessionEnded. It captured generation 7 at spawn
        // time, so it carries that stale generation no matter how late it
        // arrives relative to stop() having already returned.
        daemon.handle(DaemonCommand::SessionEnded { generation: 7 }).await;

        // Before this fix, generation 7 still matched `start_generation`
        // (StopCast never bumped it for an installed session), so this
        // branch would have `active_session.take()`n the brand-new session
        // and torn it down, flipping the UI back to Idle even though it
        // had just started successfully mirroring.
        assert!(
            daemon.active_session.is_some(),
            "the new session must survive a stale SessionEnded left over from the session that was just stopped"
        );
        assert!(
            matches!(daemon.state, StateInfo::Casting { .. }),
            "the UI must not be kicked back to Idle by a stale event about a superseded session"
        );
        assert_eq!(daemon.start_generation, 8, "a stale, non-matching SessionEnded must not itself bump the generation");
    }
}
