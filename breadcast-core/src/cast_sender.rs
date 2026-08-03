use std::sync::mpsc;
use std::thread;

use anyhow::{Context, Result};
use rust_cast::{
    CastDevice as RustCastDevice, ChannelMessage,
    channels::{
        heartbeat::HeartbeatResponse,
        media::{Media, MediaResponse, Status, StreamType},
        receiver::CastDeviceApp,
    },
    message_manager::CastMessagePayload,
};

use crate::device::CastDevice;

enum Command {
    Load {
        content_url: String,
        content_type: String,
        stream_type: StreamType,
        reply: mpsc::Sender<Result<Status>>,
    },
    /// Sends a message on an arbitrary namespace to the launched app's
    /// transport id — used for the Cast Streaming OFFER/ANSWER exchange
    /// (see [`crate::caststream`]), which the built-in `media`/`receiver`
    /// channels have no support for. Routed through this session's io
    /// thread rather than sent directly, for the same reason `Load`/`Stop`
    /// are: `rust_cast`'s `MessageManager` has no way to demultiplex
    /// responses across threads (see this struct's own doc comment).
    SendRaw {
        namespace: String,
        message: String,
        reply: mpsc::Sender<Result<()>>,
    },
    Stop {
        reply: mpsc::Sender<Result<()>>,
    },
}

/// An unsolicited message received on a namespace none of `rust_cast`'s
/// built-in channels claim (`ChannelMessage::Raw`) — e.g. an ANSWER on Cast
/// Streaming's `urn:x-cast:com.google.cast.webrtc` namespace. Binary
/// payloads are dropped (Cast Streaming's control messages are always JSON
/// text; nothing in this project's protocol usage sends binary here).
pub struct RawMessage {
    pub source_id: String,
    pub namespace: String,
    pub message: String,
}

/// The stream of [`RawMessage`]s received on namespaces
/// [`CastSession::connect`]'s default app (the Default Media Receiver)
/// never sends, but a custom-app session started via
/// [`CastSession::connect_app`] (e.g. the Cast Streaming Mirroring receiver)
/// does. Draining this is required for such sessions to make any progress —
/// see `cast_stream_test.rs`.
pub struct RawMessages {
    rx: mpsc::Receiver<RawMessage>,
}

impl RawMessages {
    pub fn recv(&self) -> Option<RawMessage> {
        self.rx.recv().ok()
    }
}

/// A live CASTV2 session to a device: connected, Default Media Receiver
/// launched, ready to load media.
///
/// All device I/O happens on a single dedicated thread spawned by
/// [`CastSession::connect`] — `rust_cast`'s `MessageManager` has no way to
/// demultiplex responses by request id across threads. `load()`/`stop()`
/// each do their own blocking read internally (`receive_find_map`); if a
/// *different* thread were concurrently blocked in `device.receive()` (e.g.
/// pumping heartbeats/media status, as a previous version of this struct
/// did), whichever thread's read happens to be in flight when a response
/// arrives "wins" it — the other blocks forever waiting for a response that
/// was already consumed and handled elsewhere. Routing every command and
/// every unsolicited message through one thread's loop avoids that race
/// entirely. `CastSession` is `Clone` (cheap: just clones a channel sender)
/// so multiple callers can issue commands concurrently without needing
/// `&mut` or reintroducing the race.
#[derive(Clone)]
pub struct CastSession {
    command_tx: mpsc::Sender<Command>,
    /// The launched app's transport id — also its CASTV2 destination id on
    /// namespaces outside the built-in channels (see
    /// [`Self::send_raw_message`]/[`crate::caststream`]).
    transport_id: String,
}

/// The stream of unsolicited `MediaResponse` messages (player-state
/// transitions, load failures, etc.) pushed by the device — separate from
/// [`CastSession`] so a caller can hold one "watch what's happening" handle
/// alongside any number of cheap [`CastSession`] clones used to issue
/// commands, without either needing exclusive access.
pub struct MediaEvents {
    rx: mpsc::Receiver<MediaResponse>,
}

impl MediaEvents {
    /// Blocks until the next `MediaResponse`, or returns `None` once the
    /// session's io thread has ended (device disconnected, session
    /// stopped, or a fatal receive error).
    pub fn recv(&self) -> Option<MediaResponse> {
        self.rx.recv().ok()
    }

    /// Like [`Self::recv`] in a loop, invoking `on_media` for each message
    /// until the session ends. Useful for interactive smoke testing.
    pub fn pump_with_media_callback(&self, mut on_media: impl FnMut(&MediaResponse)) {
        while let Some(media) = self.recv() {
            on_media(&media);
        }
    }
}

impl CastSession {
    /// Connects to `target`, launches the Default Media Receiver, and spawns
    /// the session's io thread (which immediately starts servicing
    /// heartbeats on its own — a caller no longer needs to run anything just
    /// to keep the connection alive). Returns a `CastSession` for issuing
    /// commands plus a `MediaEvents` for observing player state.
    ///
    /// Uses `connect_without_host_verification`: Cast receivers present a
    /// self-signed certificate by design (every Cast sender, including
    /// Google's own, connects this way) — this is not a shortcut to harden
    /// later, verifying against a CA chain will simply never succeed here.
    pub fn connect(target: &CastDevice) -> Result<(Self, MediaEvents)> {
        let (session, media, _raw) = Self::connect_app(target, CastDeviceApp::DefaultMediaReceiver)?;
        Ok((session, media))
    }

    /// Like [`Self::connect`], but launches an arbitrary app (e.g.
    /// [`crate::caststream::MIRRORING_APP_ID`] via
    /// `CastDeviceApp::Custom(MIRRORING_APP_ID.to_string())`) and also
    /// returns a [`RawMessages`] stream for namespaces the built-in channels
    /// don't claim — required for anything beyond the Default Media
    /// Receiver's `media`/`receiver` namespaces, e.g. Cast Streaming's
    /// OFFER/ANSWER exchange.
    pub fn connect_app(target: &CastDevice, app: CastDeviceApp) -> Result<(Self, MediaEvents, RawMessages)> {
        let device: RustCastDevice<'static> =
            RustCastDevice::connect_without_host_verification(target.host.clone(), target.port)
                .map_err(|e| anyhow::anyhow!("{e}"))
                .with_context(|| format!("failed to connect to {} ({}:{})", target.name, target.host, target.port))?;

        device
            .connection
            .connect("receiver-0")
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("failed to open the receiver-0 connection")?;

        let launched = device
            .receiver
            .launch_app(&app)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("failed to launch app {app:?}"))?;

        device
            .connection
            .connect(launched.transport_id.as_str())
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("failed to open the app transport connection")?;

        let (command_tx, command_rx) = mpsc::channel();
        let (media_tx, media_rx) = mpsc::channel();
        let (raw_tx, raw_rx) = mpsc::channel();

        let transport_id = launched.transport_id.clone();
        thread::spawn(move || {
            run_io_loop(device, launched.transport_id, launched.session_id, command_rx, media_tx, raw_tx)
        });

        Ok((
            Self { command_tx, transport_id },
            MediaEvents { rx: media_rx },
            RawMessages { rx: raw_rx },
        ))
    }

    /// The launched app's transport id — the CASTV2 destination id to use
    /// with [`Self::send_raw_message`] and, equivalently, as the
    /// `receiver_id` a [`crate::caststream::CastStreamSender`] targets.
    pub fn transport_id(&self) -> &str {
        &self.transport_id
    }

    /// Sends `message` on `namespace` to the launched app's transport id.
    /// Blocks until the session's io thread has handed it off to
    /// `rust_cast` (not until any reply — Cast Streaming's ANSWER, for
    /// example, arrives later as a [`RawMessage`] on the [`RawMessages`]
    /// stream, not as this call's return value).
    pub fn send_raw_message(&self, namespace: &str, message: &str) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.command_tx
            .send(Command::SendRaw {
                namespace: namespace.to_string(),
                message: message.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| anyhow::anyhow!("cast session io thread has already ended"))?;
        reply_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("cast session io thread ended before replying to send_raw_message"))?
    }

    /// Loads `content_url` for playback. `content_type` is the MIME type
    /// (e.g. `"video/mp4"` for a one-shot file, `"application/vnd.apple.mpegurl"`
    /// for an HLS stream). Blocks until the device acknowledges the load or
    /// the session's io thread ends.
    pub fn load(&self, content_url: &str, content_type: &str, stream_type: StreamType) -> Result<Status> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.command_tx
            .send(Command::Load {
                content_url: content_url.to_string(),
                content_type: content_type.to_string(),
                stream_type,
                reply: reply_tx,
            })
            .map_err(|_| anyhow::anyhow!("cast session io thread has already ended"))?;
        reply_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("cast session io thread ended before replying to load"))?
    }

    /// Stops the receiver app and disconnects, ending the session (and its
    /// io thread) cleanly. Without this, the TV is left showing a frozen
    /// last frame indefinitely after the sender process exits — there is no
    /// `Drop` impl doing this automatically because it needs a round trip
    /// with the device that can fail, and silently swallowing that on drop
    /// would hide exactly the kind of failure this project has already lost
    /// a lot of time chasing blind.
    pub fn stop(&self) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        if self.command_tx.send(Command::Stop { reply: reply_tx }).is_err() {
            return Ok(()); // io thread already ended — nothing left to stop
        }
        reply_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("cast session io thread ended before replying to stop"))?
    }
}

/// Owns the device connection for the session's lifetime, on its own
/// thread. Interleaves servicing queued commands (each a blocking
/// request/response round trip via `rust_cast`'s internals — safe here
/// since this is the only thread ever calling into `device`) with draining
/// unsolicited messages (heartbeat ping -> pong, media status -> forwarded
/// to `media_tx`).
///
/// Commands are only picked up between `device.receive()` calls, so
/// worst-case latency to service one is bounded by how often the device
/// pushes a message — in practice the receiver's own heartbeat ping (every
/// few seconds), not "forever": `rust_cast` gives no way to set a read
/// timeout on the underlying stream to poll more eagerly than that.
fn run_io_loop(
    device: RustCastDevice<'static>,
    transport_id: String,
    session_id: String,
    command_rx: mpsc::Receiver<Command>,
    media_tx: mpsc::Sender<MediaResponse>,
    raw_tx: mpsc::Sender<RawMessage>,
) {
    loop {
        match command_rx.try_recv() {
            Ok(Command::Load { content_url, content_type, stream_type, reply }) => {
                let media = Media {
                    content_id: content_url,
                    stream_type,
                    content_type,
                    metadata: None,
                    duration: None,
                };
                let result = device
                    .media
                    .load(transport_id.as_str(), session_id.as_str(), &media)
                    .map_err(|e| anyhow::anyhow!("{e}"))
                    .context("failed to load media");
                let _ = reply.send(result);
            }
            Ok(Command::SendRaw { namespace, message, reply }) => {
                let result = device
                    .send_message(&namespace, transport_id.as_str(), &message)
                    .map_err(|e| anyhow::anyhow!("{e}"))
                    .context("failed to send raw message");
                let _ = reply.send(result);
            }
            Ok(Command::Stop { reply }) => {
                let result = device
                    .receiver
                    .stop_app(session_id.as_str())
                    .map_err(|e| anyhow::anyhow!("{e}"))
                    .context("failed to stop the receiver app");
                let _ = device.connection.disconnect(transport_id.as_str());
                let _ = reply.send(result);
                return; // session over — end the io thread
            }
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => return, // every CastSession clone dropped
        }

        match device.receive() {
            // Only pong an actual PING — matching every `Heartbeat(_)`
            // variant (as a previous version did) would also fire on PONGs,
            // which is harmless only by accident today since nothing here
            // sends its own PING yet.
            Ok(ChannelMessage::Heartbeat(HeartbeatResponse::Ping)) => {
                if device.heartbeat.pong().is_err() {
                    return;
                }
            }
            Ok(ChannelMessage::Heartbeat(_)) => {}
            Ok(ChannelMessage::Media(media_response)) => {
                let _ = media_tx.send(media_response); // no listener is fine — nobody's watching
            }
            Ok(ChannelMessage::Raw(msg)) => {
                if let CastMessagePayload::String(message) = msg.payload {
                    let _ = raw_tx.send(RawMessage {
                        source_id: msg.source,
                        namespace: msg.namespace,
                        message,
                    });
                }
                // Binary payloads are silently dropped — see `RawMessage`'s
                // doc comment for why that's fine for this project's usage.
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(error = %e, "cast session io loop ending: receive error");
                return;
            }
        }
    }
}
