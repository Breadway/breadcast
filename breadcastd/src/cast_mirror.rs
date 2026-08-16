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
use breadcast_core::caststream::{CastStreamEvent, WEBRTC_NAMESPACE};
use breadcast_core::pipeline::{
    build_video_pipeline_for_streaming, pull_encoded_frame, request_key_frame, set_video_bitrate_kbps,
};
use breadcast_core::{CastDevice, CaptureSession, CastSession, CastStreamSender};
use gstreamer as gst;
use gstreamer::prelude::*;
use rust_cast::channels::receiver::CastDeviceApp;

use crate::daemon::DaemonCommand;

/// The encoder's starting target bitrate, in kbps -- must match the
/// `bitrate=` property `build_video_pipeline_for_streaming` builds the
/// `vah264enc` with, since [`bitrate_control_step`] treats it as the value
/// already in effect at t=0.
const INITIAL_BITRATE_KBPS: u32 = 4000;
/// Never encode below this. 720p30 below roughly 1.5 Mbps is a wall of
/// blocking artifacts -- if the link genuinely can't carry that, dropping
/// frames is a better failure mode than shipping unwatchable video.
const MIN_BITRATE_KBPS: u32 = 1500;
/// Never encode above this, regardless of how much headroom the estimator
/// reports. Matches `VideoParams::default().max_bitrate_bps`, i.e. what the
/// OFFER told the receiver to expect -- see that constant's doc comment for
/// why this is 6 Mbps and not higher: real hardware testing showed the AIMD
/// probe below pinning to whatever this ceiling is for the *entire* session
/// (the estimator it trusts read a suspiciously flat ~20 Mbps almost the
/// whole time), and 8 Mbps sustained was more than the previous
/// network+receiver could actually hold without repeated multi-second
/// freezes -- though the deeper cause of those freezes turned out to be
/// `frame_chain_broken` in `facade.cc`, not bitrate on its own. 6 Mbps is
/// still a very generous ceiling for 720p30; revisit only with real
/// evidence this specific link+receiver can sustain more, not just because
/// the estimator claims there's headroom.
const MAX_BITRATE_KBPS: u32 = 6000;

pub struct CastMirrorSession {
    pipeline: gst::Pipeline,
    session: CastSession,
    capture: Option<CaptureSession>,
    /// The FFI Cast Streaming session. Held here (rather than only inside
    /// the pump-thread closures, as an earlier version did) so its
    /// `Drop` -- which calls `breadcast_caststream_sender_destroy` and
    /// blocks until openscreen's threads stop -- happens at an explicit,
    /// deterministic point in [`Self::stop`], instead of "whichever
    /// detached pump thread happened to drop the last `Arc`."
    sender: Option<Arc<CastStreamSender>>,
    /// Forwards inbound CASTV2 `urn:x-cast:com.google.cast.webrtc` messages
    /// (the ANSWER) into the FFI session. Ends when [`CastSession::stop`]
    /// closes the raw-message channel. Holds an `Arc<CastStreamSender>`.
    message_pump: Option<std::thread::JoinHandle<()>>,
    /// Forwards outbound FFI events (the OFFER) onto the CASTV2 connection.
    /// Ends only once the `CastStreamSender` itself is dropped (that is what
    /// closes the event channel), so it must be joined *after* `sender` is
    /// dropped, not before -- joining it first would deadlock.
    event_pump: Option<std::thread::JoinHandle<()>>,
    /// Pulls encoded frames from the appsink into the FFI session. Ends on
    /// pipeline EOS/flush. Holds an `Arc<CastStreamSender>`.
    frame_pump: Option<std::thread::JoinHandle<()>>,
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
    pub async fn start(
        device: CastDevice,
        daemon_tx: tokio::sync::mpsc::Sender<DaemonCommand>,
        generation: u64,
    ) -> Result<Self> {
        let capture = CaptureSession::start().await.context("failed to start portal screen capture")?;
        let video_node_id = capture.video_node_id();

        // `video_params` describes what this pipeline will *actually* encode
        // -- it isn't a constant, because the pipeline picks its capture path
        // at runtime (see `build_video_pipeline_for_streaming`) and the two
        // paths differ in resolution and frame rate. It's threaded into the
        // OFFER below rather than re-derived there, so the advertised stream
        // and the encoded stream cannot drift apart.
        let (pipeline, appsink, encoder, video_params) =
            match build_video_pipeline_for_streaming(video_node_id) {
                Ok(built) => built,
                Err(e) => {
                    close_capture_bounded(capture).await;
                    return Err(e).context("failed to build the encode pipeline");
                }
            };

        {
            let pipeline_watch = pipeline.clone();
            std::thread::spawn(move || {
                match breadcast_core::pipeline::run_until_eos_or_error(&pipeline_watch) {
                    Ok(outcome) => tracing::debug!(?outcome, "encode pipeline bus watcher ended"),
                    Err(e) => tracing::error!(error = ?e, "encode pipeline error"),
                }
            });
        }

        // The blocking CASTV2 TCP+TLS handshake + app launch is quick
        // (milliseconds on a LAN) but still blocking I/O -- run it off the
        // async worker thread pool rather than stalling it, even briefly.
        let device_for_connect = device.clone();
        let connect = tokio::task::spawn_blocking(move || {
            CastSession::connect_app(
                &device_for_connect,
                CastDeviceApp::Custom(breadcast_core::caststream::MIRRORING_APP_ID.to_string()),
            )
        })
        .await;
        let (session, _media_events, raw_messages) = match connect {
            Ok(Ok(connected)) => connected,
            Ok(Err(e)) => {
                let _ = pipeline.set_state(gst::State::Null);
                close_capture_bounded(capture).await;
                return Err(e).context("failed to connect and launch the Mirroring receiver");
            }
            Err(e) => {
                let _ = pipeline.set_state(gst::State::Null);
                close_capture_bounded(capture).await;
                return Err(e).context("connect_app task panicked");
            }
        };

        let (sender, stream_events) =
            match CastStreamSender::start(&device.host, "sender-0", session.transport_id(), video_params) {
                Ok(started) => started,
                Err(e) => {
                    stop_session_bounded(&session).await;
                    let _ = pipeline.set_state(gst::State::Null);
                    close_capture_bounded(capture).await;
                    return Err(e).context("failed to start the Cast Streaming session");
                }
            };
        let sender = Arc::new(sender);

        let message_pump = {
            let sender = sender.clone();
            std::thread::spawn(move || {
                while let Some(msg) = raw_messages.recv() {
                    if msg.namespace == WEBRTC_NAMESPACE {
                        sender.on_message(&msg.source_id, &msg.namespace, &msg.message);
                    }
                }
            })
        };

        let negotiated = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(AtomicBool::new(false));
        let event_pump = {
            let session = session.clone();
            let negotiated = negotiated.clone();
            let failed = failed.clone();
            let daemon_tx = daemon_tx.clone();
            std::thread::spawn(move || {
                while let Ok(event) = stream_events.recv() {
                    match event {
                        CastStreamEvent::OutboundMessage { message, .. } => {
                            if let Err(e) = session.send_raw_message(WEBRTC_NAMESPACE, &message) {
                                tracing::warn!(error = ?e, "failed to send Cast Streaming message");
                            }
                        }
                        CastStreamEvent::Negotiated => negotiated.store(true, Ordering::Release),
                        CastStreamEvent::Error(message) => {
                            tracing::warn!(%message, "Cast Streaming error");
                            failed.store(true, Ordering::Release);
                            // After negotiation the frame pump is running
                            // and this is an unprompted death; before
                            // negotiation, start() itself observes `failed`
                            // and tears down.
                            if negotiated.load(Ordering::Acquire) {
                                let _ = daemon_tx.blocking_send(DaemonCommand::SessionEnded { generation });
                            }
                        }
                        CastStreamEvent::PictureLost => tracing::debug!("receiver reported picture loss"),
                    }
                }
            })
        };

        // From here every error path must use the same join/drop order as
        // `stop()`. Building the session now and calling `stop()` on it is
        // what keeps a leaked `CastStreamSender` from leaving
        // PlatformClientPosix alive -- the next start would then hit
        // OSP_CHECK(!instance_) and abort the daemon.
        let mut started = Self {
            pipeline,
            session,
            capture: Some(capture),
            sender: Some(sender),
            message_pump: Some(message_pump),
            event_pump: Some(event_pump),
            frame_pump: None,
        };

        tracing::info!(device = %device.name, "sending Cast Streaming OFFER");
        if let Some(sender) = started.sender.as_ref() {
            sender.negotiate();
        }

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
        while !negotiated.load(Ordering::Acquire)
            && !failed.load(Ordering::Acquire)
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        if failed.load(Ordering::Acquire) {
            started.stop().await;
            anyhow::bail!("Cast Streaming session error from {} during negotiation", device.name);
        }
        if !negotiated.load(Ordering::Acquire) {
            started.stop().await;
            anyhow::bail!("never received an ANSWER from {} (negotiation timed out)", device.name);
        }

        if let Err(e) = started.pipeline.set_state(gst::State::Playing) {
            started.stop().await;
            return Err(e).context("failed to start the encode pipeline");
        }
        tracing::info!(device = %device.name, "mirroring started");

        let frame_pump = {
            let device_name = device.name.clone();
            let sender = started.sender.as_ref().expect("sender installed above").clone();
            std::thread::spawn(move || {
                let result = frame_pump_loop(&appsink, &encoder, &sender);
                if let Err(e) = result {
                    tracing::warn!(device = %device_name, error = ?e, "frame pump ended with an error");
                }
                // Best-effort: if this is running, the daemon actor is (or
                // was, very recently) still alive. If the channel is full or
                // closed, there's nothing more useful to do from this
                // thread than drop the notification.
                let _ = daemon_tx.blocking_send(DaemonCommand::SessionEnded { generation });
            })
        };
        started.frame_pump = Some(frame_pump);

        Ok(started)
    }

    /// Tears down the session. Order matters and is not interchangeable:
    ///
    /// 1. Pipeline to `Null` -- unblocks the frame pump's blocking
    ///    `appsink.pull_sample()`, so it can exit and release its
    ///    `Arc<CastStreamSender>`.
    /// 2. Stop the CASTV2 session -- ends its io thread, closing the
    ///    raw-message channel the message pump blocks on, so it too can exit
    ///    and release its `Arc`.
    /// 3. Join those two. After this, no thread is calling into the FFI
    ///    session and `self.sender` holds the only remaining `Arc`.
    /// 4. Drop `self.sender` -- runs `breadcast_caststream_sender_destroy`
    ///    (blocking until openscreen's threads stop) at a point where
    ///    nothing else can be mid-call into it, and closes the FFI event
    ///    channel.
    /// 5. Only *then* join the event pump, which blocks on that channel and
    ///    would deadlock if joined before step 4.
    pub async fn stop(mut self) {
        if let Err(e) = self.pipeline.set_state(gst::State::Null) {
            tracing::warn!(error = ?e, "failed to stop the encode pipeline cleanly");
        }
        stop_session_bounded(&self.session).await;
        if let Some(capture) = self.capture.take() {
            close_capture_bounded(capture).await;
        }

        join_pump(self.frame_pump.take(), "frame").await;
        join_pump(self.message_pump.take(), "message").await;

        // Step 4: the blocking FFI teardown, kept off the async runtime's
        // worker threads for the same reason the joins are.
        if let Some(sender) = self.sender.take() {
            let _ = tokio::task::spawn_blocking(move || drop(sender)).await;
        }

        join_pump(self.event_pump.take(), "event").await;
    }
}

/// `CastSession::stop` blocks on a round trip the receiver has to answer,
/// over a `rust_cast` connection that offers no read timeout -- if the
/// receiver has gone unresponsive (wedged decoder, dropped off the
/// network, etc.) that round trip never returns. Both callers run inside
/// the single-threaded daemon actor, so an unbounded wait here doesn't
/// just fail one stop -- it permanently freezes the entire daemon (every
/// future IPC request hangs too), recoverable only by killing the
/// process. Bound it: if the receiver hasn't answered in 5s, give up on a
/// graceful stop and let teardown continue anyway. The io thread may leak
/// (still blocked in that same call), but a single leaked thread beats an
/// unrecoverable daemon.
async fn stop_session_bounded(session: &CastSession) {
    let session = session.clone();
    match tokio::time::timeout(std::time::Duration::from_secs(5), tokio::task::spawn_blocking(move || session.stop())).await {
        Ok(Ok(Err(e))) => tracing::warn!(error = ?e, "failed to cleanly stop the cast session"),
        Ok(Err(panic)) => tracing::warn!(error = ?panic, "cast session stop task panicked"),
        Err(_) => tracing::warn!("cast session did not acknowledge stop within 5s (receiver unresponsive?) -- tearing down anyway"),
        Ok(Ok(Ok(()))) => {}
    }
}

/// Same reasoning as [`stop_session_bounded`]: `capture.close()` is a
/// `Session::close()` D-Bus call to the xdg-desktop-portal backend, which
/// this project has observed to be flaky (the "Failed to populate
/// properties cache... UnknownMethod" warnings logged on every portal
/// session) -- an unbounded `.await` here is just as capable of freezing
/// the whole daemon actor if that call never gets a reply.
async fn close_capture_bounded(capture: CaptureSession) {
    match tokio::time::timeout(std::time::Duration::from_secs(5), capture.close()).await {
        Ok(Err(e)) => tracing::warn!(error = ?e, "failed to cleanly close the portal capture session"),
        Err(_) => tracing::warn!("portal capture session did not close within 5s -- abandoning it"),
        Ok(Ok(())) => {}
    }
}

/// Bounded to 5s for the same reason [`CastMirrorSession::stop`]'s own wait
/// on `CastSession::stop` is: `message_pump` blocks on a channel only the
/// (possibly wedged, per that comment) cast session io thread ever closes,
/// so an unbounded join here is exactly as capable of freezing the whole
/// daemon actor forever. A timed-out thread is abandoned rather than
/// joined -- it may still be running, but nothing here waits on it again.
async fn join_pump(handle: Option<std::thread::JoinHandle<()>>, what: &str) {
    let Some(handle) = handle else { return };
    match tokio::time::timeout(std::time::Duration::from_secs(5), tokio::task::spawn_blocking(move || handle.join())).await {
        Ok(Err(panic)) => tracing::warn!(pump = what, error = ?panic, "mirror session pump thread join task panicked"),
        Err(_) => tracing::warn!(pump = what, "mirror session pump thread did not exit within 5s -- abandoning it"),
        Ok(Ok(_)) => {}
    }
}

/// One step of the encoder-bitrate congestion-control loop: given the
/// currently-applied target and openscreen's latest bandwidth estimate,
/// returns the new target in kbps.
///
/// openscreen's `BandwidthEstimator` deliberately *under*-estimates capacity
/// whenever the transmit rate is below it (see its class comment in
/// `vendor/openscreen/cast/streaming/impl/bandwidth_estimator.h`), and
/// prescribes a TCP-like response: cut hard when the estimate is below the
/// current target, ramp back up *gradually* when it's above. An earlier
/// version of this loop instead did `target = 0.85 * estimate` every second
/// unconditionally, which multiplies the target by <= 0.85 once a second
/// with no way back up -- 4000 kbps collapses past 1500 within ~6 seconds
/// and pins at the floor, which is exactly the "low quality / compression
/// artifacts" symptom, on a perfectly healthy LAN.
///
/// An estimate of 0 means "not enough recent data to say" (documented
/// return value), and must leave the target alone rather than be treated as
/// a zero-bandwidth link.
fn bitrate_control_step(current_kbps: u32, estimate_bps: i32) -> u32 {
    if estimate_bps <= 0 {
        return current_kbps;
    }
    let estimate_kbps = (estimate_bps / 1000) as u32;
    let next = if estimate_kbps < current_kbps {
        // Below target: back off immediately to just under the estimate.
        ((estimate_kbps as f64) * 0.85) as u32
    } else {
        // Headroom: probe upward by 10% per second, not straight to the
        // estimate -- the estimate is a lower bound, and jumping to it
        // oscillates.
        current_kbps + current_kbps / 10
    };
    next.clamp(MIN_BITRATE_KBPS, MAX_BITRATE_KBPS)
}

fn frame_pump_loop(
    appsink: &gstreamer_app::AppSink,
    encoder: &gst::Element,
    sender: &CastStreamSender,
) -> Result<()> {
    let mut last_bitrate_update = std::time::Instant::now();
    let mut current_kbps = INITIAL_BITRATE_KBPS;
    // `needs_key_frame()` is a snapshot of an atomic the C++ side only
    // refreshes every 100ms, so it stays true for several frames after a
    // request has already been sent upstream. Firing a force-key-unit event
    // per frame in that window makes the encoder emit a burst of IDRs, which
    // under CBR eats the whole bitrate budget and produces a visible quality
    // dip on every picture-loss report. One request per refresh window is
    // enough.
    let mut last_key_frame_request: Option<std::time::Instant> = None;
    // Pulled-frame and successful-enqueue counters, logged once/sec
    // alongside the bitrate step below -- otherwise a stalled pipeline
    // (upstream not producing samples) and a stalled sender (producing
    // samples nobody can get rid of) are both silent: this is a per-frame
    // hot loop, so anything more than a periodic summary would flood the
    // log rather than help debug either case.
    let mut pulled_since_log: u32 = 0;
    let mut enqueued_since_log: u32 = 0;
    loop {
        let Some((data, is_key_frame, capture_time_us)) = pull_encoded_frame(appsink)? else {
            return Ok(()); // EOS -- pipeline was set to Null, or the portal source ended
        };
        pulled_since_log += 1;

        if sender.needs_key_frame()
            && !is_key_frame
            && last_key_frame_request.is_none_or(|t| t.elapsed() >= std::time::Duration::from_millis(250))
        {
            request_key_frame(appsink);
            last_key_frame_request = Some(std::time::Instant::now());
        }
        if is_key_frame {
            last_key_frame_request = None;
        }

        match sender.enqueue_frame(&data, is_key_frame, capture_time_us) {
            Ok(()) => enqueued_since_log += 1,
            Err(e) => tracing::debug!(error = ?e, "dropped a frame (not negotiated yet or backpressure)"),
        }

        if last_bitrate_update.elapsed() >= std::time::Duration::from_secs(1) {
            let next_kbps = bitrate_control_step(current_kbps, sender.estimated_bandwidth_bps());
            if next_kbps != current_kbps {
                current_kbps = next_kbps;
                set_video_bitrate_kbps(encoder, current_kbps);
            }
            // `enqueued_fps` counts *posted* frames, not accepted ones (see
            // `CastStreamSender::enqueue_stats`) -- it is the `accepted_fps`
            // and `rejected_*` fields below that say whether video is
            // actually reaching the receiver. A run where `enqueued_fps`
            // holds at 30 while `accepted_fps` drops to 0 is a frozen
            // picture, and nothing else logged here would show it.
            let stats = sender.enqueue_stats();
            tracing::debug!(
                pulled_fps = pulled_since_log,
                enqueued_fps = enqueued_since_log,
                accepted_fps = stats.enqueue_ok,
                rejected_in_flight = stats.enqueue_max_duration_in_flight,
                rejected_id_span = stats.enqueue_id_span_limit,
                rejected_too_large = stats.enqueue_payload_too_large,
                dropped_non_monotonic = stats.dropped_non_monotonic,
                in_flight_frames = stats.in_flight_frames,
                in_flight_ms = stats.in_flight_ms,
                max_in_flight_ms = stats.max_in_flight_ms,
                rtt_ms = stats.round_trip_time_ms,
                bitrate_kbps = current_kbps,
                estimated_bandwidth_bps = sender.estimated_bandwidth_bps(),
                "frame pump rate"
            );
            pulled_since_log = 0;
            enqueued_since_log = 0;
            last_bitrate_update = std::time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_estimate_leaves_the_target_alone() {
        assert_eq!(bitrate_control_step(4000, 0), 4000);
        assert_eq!(bitrate_control_step(4000, -1), 4000);
    }

    #[test]
    fn headroom_ramps_up_gradually_and_is_capped() {
        assert_eq!(bitrate_control_step(4000, 20_000_000), 4400);
        assert_eq!(bitrate_control_step(MAX_BITRATE_KBPS, 20_000_000), MAX_BITRATE_KBPS);
    }

    #[test]
    fn a_low_estimate_backs_off_but_not_below_the_floor() {
        assert_eq!(bitrate_control_step(4000, 2_000_000), 1700);
        assert_eq!(bitrate_control_step(4000, 100_000), MIN_BITRATE_KBPS);
    }

    /// The regression this loop exists to prevent: a *steady* estimate at
    /// roughly the current encode rate must hold the target there (AIMD
    /// oscillates a little around it, which is fine), not ratchet it down
    /// once per second the way `target = 0.85 * estimate` did -- that
    /// reached the floor in about a dozen iterations.
    #[test]
    fn a_steady_estimate_does_not_spiral_downward() {
        let mut kbps = 4000;
        for _ in 0..60 {
            kbps = bitrate_control_step(kbps, 4_000_000);
            assert!(kbps >= 3000, "target spiralled down to {kbps} kbps on a steady 4 Mbps estimate");
        }
    }
}
