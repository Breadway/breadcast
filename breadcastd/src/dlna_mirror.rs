//! Owns one active DLNA/UPnP mirroring session: portal capture, the
//! GStreamer HLS encode pipeline, the local HTTP server, and the
//! `AVTransport` control session. This is `dlna_mirror_test.rs`'s
//! orchestration, restructured into something the daemon can start and
//! stop on demand — the DLNA counterpart to `cast_mirror.rs`'s
//! `CastMirrorSession`.
//!
//! Unlike the Cast Streaming path, there's no persistent bidirectional
//! connection to a DLNA renderer to read events from — `AVTransport` is a
//! plain SOAP request/response protocol (see `DlnaSession`'s doc comment),
//! so "did the renderer stop on its own" can only be *polled*, not pushed.

use std::time::Duration;

use anyhow::{Context, Result};
use breadcast_core::http_server::HttpServer;
use breadcast_core::net::local_lan_ip;
use breadcast_core::pipeline::{
    build_video_pipeline, hls_output_dir, run_until_eos_or_error, wait_for_playlist_segments,
};
use breadcast_core::{CaptureSession, DlnaDevice, DlnaSession};
use gstreamer as gst;
use gstreamer::prelude::*;

use crate::daemon::DaemonCommand;

/// How often to poll `GetTransportInfo` for an unprompted stop (the user
/// stopped playback from the TV's own remote, or the renderer just dropped
/// the stream) — see [`DlnaSession::transport_state`]'s doc comment for why
/// this is a poll, not a push.
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// How long the playlist may sit unchanged before we treat the encode
/// side as dead. The Cast path has `pull_encoded_frame`'s stall watchdog;
/// this is the HLS equivalent -- portal/encoder stalls produce no bus
/// error, just a playlist that stops growing.
const PLAYLIST_STALL_TIMEOUT: Duration = Duration::from_secs(15);

pub struct DlnaMirrorSession {
    pipeline: gst::Pipeline,
    session: DlnaSession,
    capture: Option<CaptureSession>,
    http: HttpServer,
    output_dir: std::path::PathBuf,
    poll_task: tokio::task::JoinHandle<()>,
    stall_task: tokio::task::JoinHandle<()>,
    /// The GStreamer bus-watcher thread (see `start`'s doc comment on it).
    /// Stored and joined (bounded) in `stop()`, the same way
    /// `CastMirrorSession` stores and joins `frame_pump` -- without this,
    /// `stop()` could return before the thread noticed the pipeline reached
    /// `Null` and sent its `SessionEnded`, leaving that send racing an
    /// arbitrary amount of time past `stop()`'s return (CAST-4).
    bus_watcher: std::thread::JoinHandle<()>,
}

impl DlnaMirrorSession {
    /// Starts mirroring to `device`. Blocks (briefly) on the portal picker,
    /// pipeline startup, and the renderer accepting the `SetAVTransportURI`
    /// + `Play` actions before returning.
    ///
    /// `daemon_tx` is used to report an unprompted session end (the
    /// renderer stopping playback on its own, a GStreamer error, or the
    /// renderer becoming unreachable) back to the daemon actor — mirrors
    /// `CastMirrorSession::start`'s same use of it.
    pub async fn start(
        device: DlnaDevice,
        daemon_tx: tokio::sync::mpsc::Sender<DaemonCommand>,
        generation: u64,
    ) -> Result<Self> {
        let capture = CaptureSession::start().await.context("failed to start portal screen capture")?;
        let video_node_id = capture.video_node_id();

        let output_dir = match hls_output_dir("dlna-mirror") {
            Ok(dir) => dir,
            Err(e) => {
                let _ = capture.close().await;
                return Err(e);
            }
        };
        let pipeline = match build_video_pipeline(video_node_id, &output_dir) {
            Ok(p) => p,
            Err(e) => {
                let _ = capture.close().await;
                let _ = std::fs::remove_dir_all(&output_dir);
                return Err(e).context("failed to build the encode pipeline");
            }
        };

        if let Err(e) = pipeline.set_state(gst::State::Playing) {
            let _ = capture.close().await;
            let _ = std::fs::remove_dir_all(&output_dir);
            return Err(e).context("failed to start the encode pipeline");
        }

        let peer = host_ip_from_url(&device.url);
        let lan_ip = match peer.map(breadcast_core::net::local_lan_ip_for).unwrap_or_else(local_lan_ip) {
            Ok(ip) => ip,
            Err(e) => {
                abort_partial(&pipeline, capture, None, &output_dir).await;
                return Err(e).context("failed to determine this machine's LAN-reachable IP");
            }
        };
        // Bind an ephemeral port (`:0`) rather than a fixed one like the
        // `dlna_mirror_test` example uses -- the daemon may need to run
        // alongside that example, or a future concurrent-session mode,
        // without a bind conflict. The server is held on the session and
        // shut down in `stop()` so the last screen-recording segments are
        // not left reachable on the LAN.
        let http = match HttpServer::start("0.0.0.0:0", output_dir.clone()) {
            Ok(http) => http,
            Err(e) => {
                abort_partial(&pipeline, capture, None, &output_dir).await;
                return Err(e).context("failed to start the HLS HTTP server");
            }
        };
        let stream_url = http.url(lan_ip, "playlist.m3u8");

        // Two segments, not three: this is a "don't hand the renderer a 404
        // playlist" guard, and every segment waited for here is a segment of
        // already-stale video sitting between the renderer and live (see
        // `build_video_pipeline`'s note on HLS latency). Two is the minimum
        // that still proves the encoder is genuinely producing output rather
        // than having emitted one segment and stalled.
        if let Err(e) = wait_for_playlist_segments(&output_dir.join("playlist.m3u8"), 2, Duration::from_secs(20)).await
        {
            abort_partial(&pipeline, capture, Some(http), &output_dir).await;
            return Err(e).context("encode pipeline never produced playable HLS segments");
        }

        let session = match DlnaSession::connect(&device).await {
            Ok(session) => session,
            Err(e) => {
                abort_partial(&pipeline, capture, Some(http), &output_dir).await;
                return Err(e).context("failed to connect to the DLNA renderer");
            }
        };
        if let Err(e) = session.load(&stream_url).await {
            abort_partial(&pipeline, capture, Some(http), &output_dir).await;
            return Err(e).context("renderer rejected the stream load");
        }
        tracing::info!(device = %device.friendly_name, %stream_url, "DLNA mirroring started");

        let poll_task = {
            let session = session.clone();
            let device_name = device.friendly_name.clone();
            let daemon_tx = daemon_tx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    match session.transport_state().await {
                        Ok(state) if state == "STOPPED" || state == "NO_MEDIA_PRESENT" => {
                            tracing::info!(device = %device_name, %state, "DLNA renderer ended playback on its own");
                            let _ = daemon_tx.send(DaemonCommand::SessionEnded { generation }).await;
                            return;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(device = %device_name, error = ?e, "DLNA transport state poll failed, treating renderer as gone");
                            let _ = daemon_tx.send(DaemonCommand::SessionEnded { generation }).await;
                            return;
                        }
                    }
                }
            })
        };

        let stall_task = {
            let playlist = output_dir.join("playlist.m3u8");
            let daemon_tx = daemon_tx.clone();
            tokio::spawn(async move {
                let mut last_mtime = None;
                let mut stalled_for = Duration::ZERO;
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let mtime = std::fs::metadata(&playlist).and_then(|m| m.modified()).ok();
                    if mtime != last_mtime {
                        last_mtime = mtime;
                        stalled_for = Duration::ZERO;
                        continue;
                    }
                    stalled_for += Duration::from_secs(1);
                    if stalled_for >= PLAYLIST_STALL_TIMEOUT {
                        tracing::warn!(
                            "DLNA encode stalled: playlist unchanged for {}s",
                            PLAYLIST_STALL_TIMEOUT.as_secs()
                        );
                        let _ = daemon_tx.send(DaemonCommand::SessionEnded { generation }).await;
                        return;
                    }
                }
            })
        };

        // Portal "stop sharing" / a real GStreamer error used to only log
        // -- the bus watcher never told the daemon, so the UI stayed on
        // Casting and the renderer kept looping stale segments. Notify.
        let bus_watcher = {
            let pipeline_watch = pipeline.clone();
            let daemon_tx = daemon_tx.clone();
            std::thread::spawn(move || {
                match run_until_eos_or_error(&pipeline_watch) {
                    Ok(outcome) => tracing::debug!(?outcome, "DLNA encode pipeline bus watcher ended"),
                    Err(e) => tracing::error!(error = ?e, "DLNA encode pipeline error"),
                }
                let _ = daemon_tx.blocking_send(DaemonCommand::SessionEnded { generation });
            })
        };

        Ok(Self {
            pipeline,
            session,
            capture: Some(capture),
            http,
            output_dir,
            poll_task,
            stall_task,
            bus_watcher,
        })
    }

    /// Tears down the session: stops polling, tells the renderer to stop,
    /// stops the encode pipeline, shuts the HLS server, closes the portal
    /// capture session, and deletes the recording directory.
    pub async fn stop(mut self) {
        // A request, not a wait -- if the poll task is mid-poll and sends
        // one more `SessionEnded` right as this races it, that's harmless:
        // `daemon.rs`'s handler already no-ops when `active_session` was
        // already cleared by this explicit stop.
        self.poll_task.abort();
        self.stall_task.abort();

        if let Err(e) = self.session.stop().await {
            tracing::warn!(error = ?e, "failed to cleanly stop the DLNA session");
        }
        if let Err(e) = self.pipeline.set_state(gst::State::Null) {
            tracing::warn!(error = ?e, "failed to stop the encode pipeline cleanly");
        }
        // The bus watcher blocks in `run_until_eos_or_error` until EOS,
        // a bus error, or the pipeline reaches `Null` -- the state change
        // just above unblocks it, so this join should return almost
        // immediately. Bounded the same way `CastMirrorSession::stop`
        // bounds its `frame_pump` join (`cast_mirror.rs`'s `join_pump`):
        // joining here, before `stop()` returns, guarantees the thread has
        // finished (and sent its `SessionEnded`, if any) rather than racing
        // an unbounded amount of time past `stop()`'s return (CAST-4).
        match tokio::time::timeout(Duration::from_secs(5), tokio::task::spawn_blocking(move || self.bus_watcher.join())).await {
            Ok(Err(panic)) => tracing::warn!(error = ?panic, "DLNA bus watcher join task panicked"),
            Err(_) => tracing::warn!("DLNA bus watcher thread did not exit within 5s -- abandoning it"),
            Ok(Ok(_)) => {}
        }
        self.http.shutdown();
        if let Some(capture) = self.capture.take() {
            match tokio::time::timeout(Duration::from_secs(5), capture.close()).await {
                Ok(Err(e)) => tracing::warn!(error = ?e, "failed to cleanly close the portal capture session"),
                Err(_) => tracing::warn!("portal capture session did not close within 5s -- abandoning it"),
                Ok(Ok(())) => {}
            }
        }
        if let Err(e) = std::fs::remove_dir_all(&self.output_dir) {
            tracing::debug!(error = %e, dir = %self.output_dir.display(), "failed to remove HLS output dir");
        }
    }
}

async fn abort_partial(
    pipeline: &gst::Pipeline,
    capture: CaptureSession,
    http: Option<HttpServer>,
    output_dir: &std::path::Path,
) {
    let _ = pipeline.set_state(gst::State::Null);
    if let Some(mut http) = http {
        http.shutdown();
    }
    match tokio::time::timeout(Duration::from_secs(5), capture.close()).await {
        Ok(Err(e)) => tracing::warn!(error = ?e, "failed to close portal capture after a failed DLNA start"),
        Err(_) => tracing::warn!("portal capture did not close within 5s after a failed DLNA start"),
        Ok(Ok(())) => {}
    }
    let _ = std::fs::remove_dir_all(output_dir);
}

fn host_ip_from_url(url: &str) -> Option<std::net::IpAddr> {
    let rest = url.split("://").nth(1)?;
    let hostport = rest.split('/').next()?;
    let host = if let Some(inside) = hostport.strip_prefix('[') {
        inside.split(']').next()?
    } else {
        hostport.rsplit_once(':').map(|(h, _)| h).unwrap_or(hostport)
    };
    host.parse().ok()
}
