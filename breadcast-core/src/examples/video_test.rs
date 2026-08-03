//! Phase 2 step 2: opens the portal picker, captures video only, encodes
//! via VA-API, and writes an HLS playlist+segments to a local dir. Run
//! with: cargo run -p breadcast-core --example video_test [output_dir]
//! Then, in another terminal: ffplay <output_dir>/playlist.m3u8

use breadcast_core::CaptureSession;
use gstreamer as gst;
use gstreamer::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    gst::init()?;

    let output_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/breadcast-hls-test".to_string());
    let output_dir = std::path::PathBuf::from(output_dir);

    println!("Opening the portal screen-cast picker (look for a system dialog)...");
    let capture = CaptureSession::start().await?;
    println!("Got PipeWire video node id: {}", capture.video_node_id());

    let pipeline = breadcast_core::pipeline::build_video_pipeline(capture.video_node_id(), &output_dir)?;
    pipeline.set_state(gst::State::Playing)?;
    println!(
        "Pipeline playing. Writing HLS to {}. Point ffplay/vlc at {}/playlist.m3u8 now.",
        output_dir.display(),
        output_dir.display()
    );
    println!("Running for 30s...");

    let outcome =
        breadcast_core::pipeline::run_until_error_or_timeout(&pipeline, gst::ClockTime::from_seconds(30));

    pipeline.set_state(gst::State::Null)?;
    capture.close().await?;

    match outcome? {
        breadcast_core::pipeline::RunOutcome::Eos => println!("Done: pipeline reached end-of-stream (source stopped sharing)."),
        breadcast_core::pipeline::RunOutcome::Timeout => println!("Done: 30s elapsed with no pipeline errors."),
    }
    Ok(())
}
