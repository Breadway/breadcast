//! Phase 2 finale: captures this machine's screen, encodes+serves it as
//! HLS, discovers a Cast device by name substring, and casts the live
//! stream to it. Run with:
//! cargo run -p breadcast-core --example mirror_test [name substring]
//! Defaults to "master bedroom" if no argument is given.

use std::time::Duration;

use breadcast_core::pipeline::{hls_output_dir, wait_for_playlist_segments};
use breadcast_core::{CaptureSession, CastSession, Discovery, DiscoveryEvent};
use breadcast_core::net::local_lan_ip;
use gstreamer as gst;
use gstreamer::prelude::*;
use rust_cast::channels::media::{MediaResponse, StreamType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    gst::init()?;

    let name_filter = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "master bedroom".to_string())
        .to_lowercase();
    let stream_type = match std::env::args().nth(2).as_deref() {
        Some("live") => StreamType::Live,
        _ => StreamType::Buffered,
    };
    println!("Using stream_type={stream_type:?}");

    let lan_ip = local_lan_ip()?;
    println!("This machine's LAN IP: {lan_ip}");

    println!("Opening the portal screen-cast picker (look for a system dialog)...");
    let capture = CaptureSession::start().await?;
    println!("Got PipeWire video node id: {}", capture.video_node_id());

    let output_dir = hls_output_dir("cast-mirror")?;
    let pipeline = breadcast_core::pipeline::build_video_pipeline(capture.video_node_id(), &output_dir)?;
    pipeline.set_state(gst::State::Playing)?;
    println!("Pipeline playing, writing HLS to {}", output_dir.display());

    // Watch the encode pipeline's own bus in the background: without
    // this, a vah264enc/hlssink3 error partway through mirroring is
    // invisible (nothing else polls this pipeline's bus), and the failure
    // would only ever show up indirectly as the Cast session stalling.
    {
        let pipeline_watch = pipeline.clone();
        std::thread::spawn(move || {
            match breadcast_core::pipeline::run_until_error_or_timeout(&pipeline_watch, gst::ClockTime::from_seconds(120)) {
                Ok(outcome) => tracing::debug!(?outcome, "encode pipeline bus watcher ended"),
                Err(e) => eprintln!("ENCODE PIPELINE ERROR: {e:?}"),
            }
        });
    }

    // Fixed port (not 0/ephemeral) so a firewall rule can be added once and
    // stay valid across runs instead of chasing a random port every time.
    let http = breadcast_core::http_server::HttpServer::start("0.0.0.0:8825", output_dir.clone())?;
    let stream_url = http.url(lan_ip, "playlist.m3u8");
    println!("Serving at {stream_url}");

    wait_for_playlist_segments(&output_dir.join("playlist.m3u8"), 3, Duration::from_secs(20)).await?;
    println!("Playlist has segments, proceeding to cast.");

    println!("Looking for a Cast device matching \"{name_filter}\"...");
    let (_discovery, mut events) = Discovery::start()?;
    let device = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(25), events.recv())
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

    let (session, media_events) = CastSession::connect(&device)?;
    println!("Connected, Default Media Receiver launched. Loading the live stream...");
    let status = session.load(&stream_url, "application/vnd.apple.mpegurl", stream_type)?;
    println!("Receiver acknowledged the load: {status:#?}");

    println!("Mirroring for up to 60s; check the TV. Watching player state live:");
    let pump = std::thread::spawn(move || {
        media_events.pump_with_media_callback(|media| match media {
            MediaResponse::Status(status) => {
                for entry in &status.entries {
                    println!(
                        "  player_state={:?} idle_reason={:?} extended_status={:?}",
                        entry.player_state,
                        entry.idle_reason,
                        entry.extended_status.as_ref().map(|e| e.player_state)
                    );
                }
            }
            MediaResponse::LoadFailed(f) => println!("  LOAD FAILED: {f:?}"),
            MediaResponse::LoadCancelled(c) => println!("  LOAD CANCELLED: {c:?}"),
            MediaResponse::Error(e) => println!("  MEDIA ERROR: {e:?}"),
            MediaResponse::InvalidRequest(r) => println!("  INVALID REQUEST: {r:?}"),
            other => println!("  other media message: {other:?}"),
        })
    });

    let mirror_duration = Duration::from_secs(60);
    let mut elapsed = Duration::ZERO;
    while elapsed < mirror_duration && !pump.is_finished() {
        tokio::time::sleep(Duration::from_secs(1)).await;
        elapsed += Duration::from_secs(1);
    }
    if pump.is_finished() {
        println!("Cast session ended early (after {elapsed:?}): connection likely dropped or the receiver stopped playback.");
    }

    if let Err(e) = session.stop() {
        eprintln!("failed to cleanly stop the cast session: {e:?}");
    }
    pipeline.set_state(gst::State::Null)?;
    capture.close().await?;
    if let Err(panic) = pump.join() {
        eprintln!("cast session pump thread panicked: {panic:?}");
    }
    println!("Stopped.");

    Ok(())
}
