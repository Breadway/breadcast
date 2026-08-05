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
use breadcast_core::net::local_lan_ip;
use breadcast_core::pipeline::{build_video_pipeline, hls_output_dir, wait_for_playlist_segments};
use breadcast_core::{CaptureSession, DlnaDevice, DlnaSession};
use gstreamer as gst;
use gstreamer::prelude::*;

use crate::daemon::DaemonCommand;

/// How often to poll `GetTransportInfo` for an unprompted stop (the user
/// stopped playback from the TV's own remote, or the renderer just dropped
/// the stream) — see [`DlnaSession::transport_state`]'s doc comment for why
/// this is a poll, not a push.
const POLL_INTERVAL: Duration = Duration::from_secs(3);

pub struct DlnaMirrorSession {
    pipeline: gst::Pipeline,
    session: DlnaSession,
    capture: Option<CaptureSession>,
    poll_task: tokio::task::JoinHandle<()>,
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
    pub async fn start(device: DlnaDevice, daemon_tx: tokio::sync::mpsc::Sender<DaemonCommand>) -> Result<Self> {
        let capture = CaptureSession::start().await.context("failed to start portal screen capture")?;
        let video_node_id = capture.video_node_id();

        let output_dir = hls_output_dir("dlna-mirror")?;
        let pipeline =
            build_video_pipeline(video_node_id, &output_dir).context("failed to build the encode pipeline")?;

        // Fire-and-forget, same as `CastMirrorSession::start`'s identical
        // block: nothing joins this thread, it just self-terminates on
        // pipeline error, EOS, or its own 1-hour timeout, whichever is
        // first — see that function's doc comment for why that's fine.
        {
            let pipeline_watch = pipeline.clone();
            std::thread::spawn(move || {
                match breadcast_core::pipeline::run_until_error_or_timeout(&pipeline_watch, gst::ClockTime::from_seconds(3600))
                {
                    Ok(outcome) => tracing::debug!(?outcome, "DLNA encode pipeline bus watcher ended"),
                    Err(e) => tracing::error!(error = ?e, "DLNA encode pipeline error"),
                }
            });
        }

        pipeline.set_state(gst::State::Playing).context("failed to start the encode pipeline")?;

        let lan_ip = local_lan_ip().context("failed to determine this machine's LAN-reachable IP")?;
        // Bind an ephemeral port (`:0`) rather than a fixed one like the
        // `dlna_mirror_test` example uses -- the daemon may need to run
        // alongside that example, or a future concurrent-session mode,
        // without a bind conflict. `HttpServer::start`'s worker threads
        // outlive this session once it stops (documented pre-existing
        // limitation, see `http_server.rs` -- not something introduced
        // here); one leaked idle listener per DLNA cast is an accepted
        // cost until that gets a real shutdown path.
        let http = breadcast_core::http_server::HttpServer::start("0.0.0.0:0", output_dir.clone())
            .context("failed to start the HLS HTTP server")?;
        let stream_url = http.url(lan_ip, "playlist.m3u8");

        // Two segments, not three: this is a "don't hand the renderer a 404
        // playlist" guard, and every segment waited for here is a segment of
        // already-stale video sitting between the renderer and live (see
        // `build_video_pipeline`'s note on HLS latency). Two is the minimum
        // that still proves the encoder is genuinely producing output rather
        // than having emitted one segment and stalled.
        wait_for_playlist_segments(&output_dir.join("playlist.m3u8"), 2, Duration::from_secs(20))
            .await
            .context("encode pipeline never produced playable HLS segments")?;

        let session = DlnaSession::connect(&device).await.context("failed to connect to the DLNA renderer")?;
        session.load(&stream_url).await.context("renderer rejected the stream load")?;
        tracing::info!(device = %device.friendly_name, %stream_url, "DLNA mirroring started");

        let poll_task = {
            let session = session.clone();
            let device_name = device.friendly_name.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    match session.transport_state().await {
                        Ok(state) if state == "STOPPED" || state == "NO_MEDIA_PRESENT" => {
                            tracing::info!(device = %device_name, %state, "DLNA renderer ended playback on its own");
                            let _ = daemon_tx.send(DaemonCommand::SessionEnded).await;
                            return;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(device = %device_name, error = ?e, "DLNA transport state poll failed, treating renderer as gone");
                            let _ = daemon_tx.send(DaemonCommand::SessionEnded).await;
                            return;
                        }
                    }
                }
            })
        };

        Ok(Self { pipeline, session, capture: Some(capture), poll_task })
    }

    /// Tears down the session: stops polling, tells the renderer to stop,
    /// stops the encode pipeline, and closes the portal capture session.
    pub async fn stop(mut self) {
        // A request, not a wait -- if the poll task is mid-poll and sends
        // one more `SessionEnded` right as this races it, that's harmless:
        // `daemon.rs`'s handler already no-ops when `active_session` was
        // already cleared by this explicit stop.
        self.poll_task.abort();

        if let Err(e) = self.session.stop().await {
            tracing::warn!(error = ?e, "failed to cleanly stop the DLNA session");
        }
        if let Err(e) = self.pipeline.set_state(gst::State::Null) {
            tracing::warn!(error = ?e, "failed to stop the encode pipeline cleanly");
        }
        if let Some(capture) = self.capture.take() {
            if let Err(e) = capture.close().await {
                tracing::warn!(error = ?e, "failed to cleanly close the portal capture session");
            }
        }
    }
}
