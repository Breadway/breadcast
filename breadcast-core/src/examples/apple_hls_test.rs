use breadcast_core::{CastSession, Discovery, DiscoveryEvent};
use rust_cast::channels::media::{MediaResponse, StreamType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let (_d, mut events) = Discovery::start()?;
    let device = loop {
        let ev = tokio::time::timeout(std::time::Duration::from_secs(45), events.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for a matching device"))?
            .ok_or_else(|| anyhow::anyhow!("discovery channel closed before a matching device was found"))?;
        if let DiscoveryEvent::Found(d) = ev {
            if d.name.to_lowercase().contains("master bedroom") && d.model.to_lowercase().contains("chromecast") && d.host.parse::<std::net::Ipv4Addr>().is_ok() {
                break d;
            }
        }
    };
    println!("Found {}", device.name);
    let (session, media_events) = CastSession::connect(&device)?;
    let stream_type = std::env::args().nth(1).unwrap_or_default();
    let stream_type = if stream_type == "live" { StreamType::Live } else { StreamType::Buffered };
    println!("Using stream_type={stream_type:?}");
    let status = session.load("http://devimages.apple.com/iphone/samples/bipbop/bipbopall.m3u8", "application/vnd.apple.mpegurl", stream_type)?;
    println!("Load status: {status:#?}");
    std::thread::spawn(move || {
        media_events.pump_with_media_callback(|m| match m {
            MediaResponse::Status(s) => for e in &s.entries { println!("  player_state={:?} idle_reason={:?}", e.player_state, e.idle_reason); },
            MediaResponse::LoadFailed(f) => println!("  LOAD FAILED: {f:?}"),
            other => println!("  other: {other:?}"),
        })
    });
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    let _ = session.stop();
    Ok(())
}
