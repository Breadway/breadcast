//! Owns one active Cast Streaming mirroring session end-to-end: portal
//! capture, the GStreamer encode pipeline, the CASTV2 connection to the
//! Mirroring receiver, and the three pump threads that shuttle
//! OFFER/ANSWER messages and encoded frames between them. This is
//! `cast_stream_test.rs`'s orchestration, restructured into something the
//! daemon can start and stop on demand instead of running for a fixed
//! duration from a CLI `main`.
//!
//! The Cast Streaming (low-latency, RTP-based) path — see `dlna_mirror.rs`
//! for the DLNA/UPnP counterpart (HLS-over-HTTP, polled instead of pushed).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use breadcast_core::caststream::{CastStreamEvent, VideoParams, WEBRTC_NAMESPACE};
use breadcast_core::pipeline::{
    build_video_pipeline_for_streaming, pull_encoded_frame, request_key_frame, set_video_bitrate_kbps,
};
use breadcast_core::{CastDevice, CaptureSession, CastSession, CastStreamSender};
use gstreamer as gst;
use gstreamer::prelude::*;
use rust_cast::channels::receiver::CastDeviceApp;

use crate::daemon::DaemonCommand;

pub struct CastMirrorSession {
    pipeline: gst::Pipeline,
    session: CastSession,
    capture: Option<CaptureSession>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl CastMirrorSession {
    /// Starts mirroring to `device`. Blocks (briefly) on the portal picker,
    /// the CASTV2 handshake, and OFFER/ANSWER negotiation before returning
    /// -- by the time this resolves, frames are already flowing.
    ///
    /// `daemon_tx` is used to report unprompted session death (a GStreamer
    /// error, the user clicking "stop sharing" in the portal picker, the
    /// receiver dropping the connection) back to the daemon actor, so it
    /// can transition back to `Idle` and notify GUI clients even if nobody
    /// called `stop()`.
    pub async fn start(device: CastDevice, daemon_tx: tokio::sync::mpsc::Sender<DaemonCommand>) -> Result<Self> {
        let capture = CaptureSession::start().await.context("failed to start portal screen capture")?;
        let video_node_id = capture.video_node_id();

        let (pipeline, appsink, encoder) =
            build_video_pipeline_for_streaming(video_node_id).context("failed to build the encode pipeline")?;

        {
            let pipeline_watch = pipeline.clone();
            std::thread::spawn(move || {
                match breadcast_core::pipeline::run_until_error_or_timeout(&pipeline_watch, gst::ClockTime::from_seconds(3600))
                {
                    Ok(outcome) => tracing::debug!(?outcome, "encode pipeline bus watcher ended"),
                    Err(e) => tracing::error!(error = ?e, "encode pipeline error"),
                }
            });
        }

        // The blocking CASTV2 TCP+TLS handshake + app launch is quick
        // (milliseconds on a LAN) but still blocking I/O -- run it off the
        // async worker thread pool rather than stalling it, even briefly.
        let device_for_connect = device.clone();
        let (session, _media_events, raw_messages) = tokio::task::spawn_blocking(move || {
            CastSession::connect_app(
                &device_for_connect,
                CastDeviceApp::Custom(breadcast_core::caststream::MIRRORING_APP_ID.to_string()),
            )
        })
        .await
        .context("connect_app task panicked")?
        .context("failed to connect and launch the Mirroring receiver")?;

        let (sender, stream_events) =
            CastStreamSender::start(&device.host, "sender-0", session.transport_id(), VideoParams::default())
                .context("failed to start the Cast Streaming session")?;
        let sender = Arc::new(sender);

        let mut threads = Vec::new();

        threads.push({
            let sender = sender.clone();
            std::thread::spawn(move || {
                while let Some(msg) = raw_messages.recv() {
                    if msg.namespace == WEBRTC_NAMESPACE {
                        sender.on_message(&msg.source_id, &msg.namespace, &msg.message);
                    }
                }
            })
        });

        let negotiated = Arc::new(AtomicBool::new(false));
        threads.push({
            let session = session.clone();
            let negotiated = negotiated.clone();
            std::thread::spawn(move || {
                while let Ok(event) = stream_events.recv() {
                    match event {
                        CastStreamEvent::OutboundMessage { message, .. } => {
                            if let Err(e) = session.send_raw_message(WEBRTC_NAMESPACE, &message) {
                                tracing::warn!(error = ?e, "failed to send Cast Streaming message");
                            }
                        }
                        CastStreamEvent::Negotiated => negotiated.store(true, Ordering::Release),
                        CastStreamEvent::Error(message) => tracing::warn!(%message, "Cast Streaming error"),
                        CastStreamEvent::PictureLost => tracing::debug!("receiver reported picture loss"),
                    }
                }
            })
        });

        tracing::info!(device = %device.name, "sending Cast Streaming OFFER");
        sender.negotiate();

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
        while !negotiated.load(Ordering::Acquire) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        if !negotiated.load(Ordering::Acquire) {
            let _ = session.stop();
            capture.close().await.ok();
            anyhow::bail!("never received an ANSWER from {} (negotiation timed out)", device.name);
        }

        pipeline.set_state(gst::State::Playing).context("failed to start the encode pipeline")?;
        tracing::info!(device = %device.name, "mirroring started");

        threads.push({
            let device_name = device.name.clone();
            std::thread::spawn(move || {
                let result = frame_pump_loop(&appsink, &encoder, &sender);
                if let Err(e) = result {
                    tracing::warn!(device = %device_name, error = ?e, "frame pump ended with an error");
                }
                // Best-effort: if this is running, the daemon actor is (or
                // was, very recently) still alive. If the channel is full or
                // closed, there's nothing more useful to do from this
                // thread than drop the notification.
                let _ = daemon_tx.blocking_send(DaemonCommand::SessionEnded);
            })
        });

        Ok(Self { pipeline, session, capture: Some(capture), threads })
    }

    /// Tears down the session: stops the pipeline (which unblocks the frame
    /// pump thread's blocking `appsink.pull_sample()` call), stops the
    /// CASTV2 session (which ends its io thread, closing the channels the
    /// other two pump threads block on), then joins every thread.
    pub async fn stop(mut self) {
        if let Err(e) = self.pipeline.set_state(gst::State::Null) {
            tracing::warn!(error = ?e, "failed to stop the encode pipeline cleanly");
        }
        if let Err(e) = self.session.stop() {
            tracing::warn!(error = ?e, "failed to cleanly stop the cast session");
        }
        if let Some(capture) = self.capture.take() {
            if let Err(e) = capture.close().await {
                tracing::warn!(error = ?e, "failed to cleanly close the portal capture session");
            }
        }
        for thread in self.threads.drain(..) {
            // These threads all end once the pipeline/session teardown
            // above propagates to them (see this method's own doc comment)
            // -- `spawn_blocking` just keeps `.join()`'s wait off the async
            // runtime's worker threads.
            if let Err(panic) = tokio::task::spawn_blocking(move || thread.join()).await {
                tracing::warn!(error = ?panic, "mirror session pump thread join task panicked");
            }
        }
    }
}

fn frame_pump_loop(
    appsink: &gstreamer_app::AppSink,
    encoder: &gst::Element,
    sender: &CastStreamSender,
) -> Result<()> {
    let mut last_bitrate_update = std::time::Instant::now();
    loop {
        let Some((data, is_key_frame, capture_time_us)) = pull_encoded_frame(appsink)? else {
            return Ok(()); // EOS -- pipeline was set to Null, or the portal source ended
        };

        if sender.needs_key_frame() && !is_key_frame {
            request_key_frame(appsink);
        }

        if let Err(e) = sender.enqueue_frame(&data, is_key_frame, capture_time_us) {
            tracing::debug!(error = ?e, "dropped a frame (not negotiated yet or backpressure)");
        }

        if last_bitrate_update.elapsed() >= std::time::Duration::from_secs(1) {
            let bps = sender.estimated_bandwidth_bps();
            let target_kbps = ((bps as f64 * 0.85) / 1000.0).max(500.0) as u32;
            set_video_bitrate_kbps(encoder, target_kbps);
            last_bitrate_update = std::time::Instant::now();
        }
    }
}
