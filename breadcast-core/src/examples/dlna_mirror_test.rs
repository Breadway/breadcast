//! DLNA equivalent of `mirror_test`: captures this machine's screen,
//! encodes+serves it as HLS, discovers a DLNA/UPnP media renderer by name
//! substring, and casts the live stream to it via `AVTransport`. Run with:
//! cargo run -p breadcast-core --example dlna_mirror_test [name substring]
//! Matches any renderer found if no argument is given.
//!
//! This proves the same milestone `mirror_test` proved for Cast — a real
//! receiver actually playing this pipeline's live HLS output — but for a
//! different, unproven protocol path. DLNA renderer support for a
//! *live-growing* (never-ending) HLS playlist is much less consistent
//! across devices than Chromecast's: DLNA's classic use case is "play this
//! one finite file," and plenty of renderers' UPnP stacks predate HLS
//! entirely. Whether a given real renderer handles this is exactly what
//! this example is for finding out, not something to assume from the Cast
//! path working.

use std::time::Duration;

use breadcast_core::net::local_lan_ip;
use breadcast_core::pipeline::{hls_output_dir, wait_for_playlist_segments};
use breadcast_core::{CaptureSession, DlnaDiscovery, DlnaDiscoveryEvent, DlnaSession};
use gstreamer as gst;
use gstreamer::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    gst::init()?;

    let name_filter = std::env::args().nth(1).unwrap_or_default().to_lowercase();

    let lan_ip = local_lan_ip()?;
    println!("This machine's LAN IP: {lan_ip}");

    println!("Opening the portal screen-cast picker (look for a system dialog)...");
    let capture = CaptureSession::start().await?;
    println!("Got PipeWire video node id: {}", capture.video_node_id());

    let output_dir = hls_output_dir("dlna-mirror")?;
    let pipeline = breadcast_core::pipeline::build_video_pipeline(capture.video_node_id(), &output_dir)?;
    pipeline.set_state(gst::State::Playing)?;
    println!("Pipeline playing, writing HLS to {}", output_dir.display());

    // Watch the encode pipeline's own bus in the background — see
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

    // A different fixed port than mirror_test's 8825, so both examples can
    // run at once (e.g. testing Cast and DLNA against the same live
    // desktop) without a bind conflict.
    let http = breadcast_core::http_server::HttpServer::start("0.0.0.0:8826", output_dir.clone())?;
    let stream_url = http.url(lan_ip, "playlist.m3u8");
    println!("Serving at {stream_url}");

    wait_for_playlist_segments(&output_dir.join("playlist.m3u8"), 3, Duration::from_secs(20)).await?;
    println!("Playlist has segments, proceeding to cast.");

    println!("Searching for a DLNA renderer matching \"{name_filter}\"...");
    let (_discovery, mut events) = DlnaDiscovery::start();
    let device = loop {
        let event = tokio::time::timeout(Duration::from_secs(35), events.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for a renderer matching \"{name_filter}\""))?
            .ok_or_else(|| anyhow::anyhow!("discovery channel closed"))?;

        if let DlnaDiscoveryEvent::Found(device) = event {
            if device.friendly_name.to_lowercase().contains(&name_filter) {
                break device;
            }
        }
    };
    println!("Found {} [{}]", device.friendly_name, device.url);

    let session = DlnaSession::connect(&device).await?;
    println!("Connected to AVTransport. Loading the live stream...");
    session.load(&stream_url).await?;
    println!("Renderer accepted the load.");

    println!("Mirroring for up to 60s — check the display. Polling transport state:");
    let mirror_duration = Duration::from_secs(60);
    let mut elapsed = Duration::ZERO;
    while elapsed < mirror_duration {
        match session.transport_state().await {
            Ok(state) => println!("  transport_state={state}"),
            Err(e) => println!("  failed to poll transport state: {e:?}"),
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        elapsed += Duration::from_secs(3);
    }

    if let Err(e) = session.stop().await {
        eprintln!("failed to cleanly stop the DLNA session: {e:?}");
    }
    pipeline.set_state(gst::State::Null)?;
    capture.close().await?;
    println!("Stopped.");

    Ok(())
}
