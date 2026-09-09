//! Phase 2 step 1: opens the portal's screen-cast picker and prints the
//! PipeWire node id it hands back. Run with:
//! cargo run -p breadcast-core --example capture_test
//! A system picker dialog should appear: pick a monitor or window.

use breadcast_core::CaptureSession;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("Opening the portal screen-cast picker (look for a system dialog)...");
    let session = CaptureSession::start().await?;
    println!("Got PipeWire video node id: {}", session.video_node_id());

    println!("Holding the session open for 5s, then closing...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    session.close().await?;
    println!("Closed.");

    Ok(())
}
