//! Low-latency mirroring via Cast Streaming (the same protocol Chrome's
//! tab/desktop casting uses) instead of `mirror_test`'s HLS approach: no
//! HTTP server, no multi-second segment-buffering floor, and it targets the
//! Chromecast's built-in Mirroring receiver (app id `0F5096E8`) instead of
//! the Default Media Receiver. Run with:
//! cargo run -p breadcast-core --example cast_stream_test [name substring]
//! Defaults to "master bedroom" if no argument is given.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use breadcast_core::caststream::{CastStreamEvent, WEBRTC_NAMESPACE};
use breadcast_core::net::local_lan_ip;
use breadcast_core::pipeline::{
    build_video_pipeline_for_streaming, pull_encoded_frame, request_key_frame, set_video_bitrate_kbps,
};
use breadcast_core::{CastSession, CastStreamSender, Discovery, DiscoveryEvent};
use gstreamer as gst;
use gstreamer::prelude::*;
use rust_cast::channels::receiver::CastDeviceApp;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    gst::init()?;

    let name_filter = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "master bedroom".to_string())
        .to_lowercase();

    let lan_ip = local_lan_ip()?;
    println!("This machine's LAN IP: {lan_ip}");

    println!("Looking for a Cast device matching \"{name_filter}\"...");
    let (_discovery, mut events) = Discovery::start()?;
    let device = loop {
        let event = tokio::time::timeout(Duration::from_secs(25), events.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for a device matching \"{name_filter}\""))?
            .ok_or_else(|| anyhow::anyhow!("discovery channel closed"))?;

        if let DiscoveryEvent::Found(device) = event {
            let name = device.name.to_lowercase();
            let model = device.model.to_lowercase();
            if name.contains(&name_filter)
                && model.contains("chromecast")
                && device.host.parse::<std::net::Ipv4Addr>().is_ok()
            {
                break device;
            }
        }
    };
    println!("Found {} ({}) at {}:{}", device.name, device.model, device.host, device.port);

    println!("Opening the portal screen-cast picker (look for a system dialog)...");
    let capture = breadcast_core::CaptureSession::start().await?;
    println!("Got PipeWire video node id: {}", capture.video_node_id());

    let (pipeline, appsink, encoder, video_params) = build_video_pipeline_for_streaming(capture.video_node_id())?;

    // Watch the encode pipeline's own bus in the background -- see
    // mirror_test.rs's identical block for why this matters.
    {
        let pipeline_watch = pipeline.clone();
        std::thread::spawn(move || {
            match breadcast_core::pipeline::run_until_error_or_timeout(&pipeline_watch, gst::ClockTime::from_seconds(120)) {
                Ok(outcome) => tracing::debug!(?outcome, "encode pipeline bus watcher ended"),
                Err(e) => eprintln!("ENCODE PIPELINE ERROR: {e:?}"),
            }
        });
    }

    println!("Connecting and launching the Mirroring receiver ({} )...", breadcast_core::caststream::MIRRORING_APP_ID);
    let (session, _media_events, raw_messages) =
        CastSession::connect_app(&device, CastDeviceApp::Custom(breadcast_core::caststream::MIRRORING_APP_ID.to_string()))?;
    println!("Mirroring receiver launched, transport_id={}", session.transport_id());

    let (sender, stream_events) = CastStreamSender::start(
        &device.host,
        "sender-0",
        session.transport_id(),
        video_params,
    )?;
    let sender = Arc::new(sender);

    // Forwards inbound webrtc-namespace messages (the ANSWER) from the
    // existing CASTV2 connection into the Cast Streaming session.
    let inbound_pump = {
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

    // Forwards outbound webrtc-namespace messages (the OFFER) out over the
    // existing CASTV2 connection, and tracks negotiation completion.
    let outbound_pump = {
        let session = session.clone();
        let negotiated = negotiated.clone();
        std::thread::spawn(move || {
            while let Ok(event) = stream_events.recv() {
                match event {
                    CastStreamEvent::OutboundMessage { message, .. } => {
                        if let Err(e) = session.send_raw_message(WEBRTC_NAMESPACE, &message) {
                            eprintln!("failed to send Cast Streaming message: {e:?}");
                        }
                    }
                    CastStreamEvent::Negotiated => {
                        println!("Cast Streaming negotiated -- receiver is ready for frames.");
                        negotiated.store(true, Ordering::Release);
                    }
                    CastStreamEvent::Error(message) => {
                        eprintln!("Cast Streaming error: {message}");
                    }
                    CastStreamEvent::PictureLost => {
                        println!("receiver reported picture loss");
                    }
                }
            }
        })
    };

    println!("Sending OFFER...");
    sender.negotiate();

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !negotiated.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !negotiated.load(Ordering::Acquire) {
        anyhow::bail!("never received an ANSWER (negotiation timed out after 10s)");
    }

    pipeline.set_state(gst::State::Playing)?;
    println!("Pipeline playing. Mirroring for up to 60s -- check the TV.");

    let sender_for_frames = sender.clone();
    let frame_pump = std::thread::spawn(move || -> anyhow::Result<()> {
        let mut last_bitrate_update = std::time::Instant::now();
        loop {
            let Some((data, is_key_frame, capture_time_us)) = pull_encoded_frame(&appsink)? else {
                return Ok(()); // EOS
            };

            if sender_for_frames.needs_key_frame() && !is_key_frame {
                request_key_frame(&appsink);
            }

            if let Err(e) = sender_for_frames.enqueue_frame(&data, is_key_frame, capture_time_us) {
                tracing::debug!(error = ?e, "dropped a frame (not negotiated yet or backpressure)");
            }

            if last_bitrate_update.elapsed() >= Duration::from_secs(1) {
                let bps = sender_for_frames.estimated_bandwidth_bps();
                // Leave headroom below the raw estimate for RTP/RTCP
                // overhead and estimation noise, and never go below a
                // usable floor.
                let target_kbps = ((bps as f64 * 0.85) / 1000.0).max(500.0) as u32;
                set_video_bitrate_kbps(&encoder, target_kbps);
                last_bitrate_update = std::time::Instant::now();
            }
        }
    });

    let mirror_duration = Duration::from_secs(60);
    let mut elapsed = Duration::ZERO;
    while elapsed < mirror_duration && !frame_pump.is_finished() && !outbound_pump.is_finished() {
        tokio::time::sleep(Duration::from_secs(1)).await;
        elapsed += Duration::from_secs(1);
    }

    pipeline.set_state(gst::State::Null)?;
    capture.close().await?;
    if let Err(e) = session.stop() {
        eprintln!("failed to cleanly stop the cast session: {e:?}");
    }
    match frame_pump.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("frame pump ended with an error: {e:?}"),
        Err(panic) => eprintln!("frame pump thread panicked: {panic:?}"),
    }
    // `session.stop()` above ends the CastSession's io thread, which in turn
    // closes the raw_messages/stream_events channels these two pump threads
    // are blocked reading from -- so both should already be finishing.
    if let Err(panic) = inbound_pump.join() {
        eprintln!("inbound message pump thread panicked: {panic:?}");
    }
    if let Err(panic) = outbound_pump.join() {
        eprintln!("outbound message pump thread panicked: {panic:?}");
    }
    println!("Stopped.");

    Ok(())
}
