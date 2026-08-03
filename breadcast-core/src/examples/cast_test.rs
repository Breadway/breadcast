//! Phase 3 smoke test: discovers a Cast device by name substring and casts a
//! sample video to it, to prove real CASTV2 device control ahead of building
//! the actual capture pipeline.
//! Run with: cargo run -p breadcast-core --example cast_test [name substring]
//! Defaults to "master bedroom" if no argument is given.

use breadcast_core::{CastSession, Discovery, DiscoveryEvent};
use rust_cast::channels::media::{MediaResponse, StreamType};

// Google's old gtv-videos-bucket sample assets (from the classic Cast SDK
// docs) now 403 — confirmed dead via `curl -I`, not a Cast-side issue. The
// 21MB samplelib.com/mp4/sample-30s.mp4 loaded but then the receiver killed
// the sender connection ("failed to fill whole buffer" / EOF) on both real
// devices tested — consistent with a non-"fast-start" MP4 (moov atom at the
// end) stalling the receiver's simple HTML5 video player. This one is small
// (788KB/10s) and known-good.
const SAMPLE_VIDEO: &str = "https://www.w3schools.com/html/mov_bbb.mp4";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let name_filter = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "master bedroom".to_string())
        .to_lowercase();

    let (_discovery, mut events) = Discovery::start()?;
    println!("Looking for a Cast device matching \"{name_filter}\"...");

    let device = loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(15), events.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for a device matching \"{name_filter}\""))?
            .ok_or_else(|| anyhow::anyhow!("discovery channel closed"))?;

        if let DiscoveryEvent::Found(device) = event {
            let name = device.name.to_lowercase();
            let model = device.model.to_lowercase();
            // Restrict to actual Chromecast-family receivers (excludes a
            // TV's own "Smart TV" Cast platform, which shares the "TV" name
            // prefix with its paired dongle in this household).
            //
            // Hold out for an IPv4 address specifically, not just
            // non-link-local: mDNS resolution races between interfaces, and
            // a device's *global* IPv6 address can arrive in a Found event
            // before its IPv4 address does, or before its IPv4 address is
            // even reachable (seen in practice: "No route to host" on a
            // global IPv6 the LAN doesn't actually route). IPv4 is the safe
            // default on this network; a later phase can add real
            // reachability probing instead of an address-family heuristic.
            if name.contains(&name_filter)
                && model.contains("chromecast")
                && device.host.parse::<std::net::Ipv4Addr>().is_ok()
            {
                break device;
            }
        }
    };

    println!(
        "Found {} ({}) at {}:{} — connecting...",
        device.name, device.model, device.host, device.port
    );

    let (session, media_events) = CastSession::connect(&device)?;
    println!("Connected, Default Media Receiver launched. Loading sample video...");

    let status = session.load(SAMPLE_VIDEO, "video/mp4", StreamType::Buffered)?;
    println!("Receiver acknowledged the load: {status:#?}");
    println!("Watching player state for 90s (video is ~30s, but buffering can be slow)...");

    let pump = std::thread::spawn(move || {
        media_events.pump_with_media_callback(|media| {
            if let MediaResponse::Status(status) = media {
                for entry in &status.entries {
                    println!(
                        "  player_state={:?} idle_reason={:?} current_time={:?} extended_status={:?}",
                        entry.player_state, entry.idle_reason, entry.current_time,
                        entry.extended_status.as_ref().map(|e| e.player_state)
                    );
                }
            }
        });
    });

    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
    if let Err(e) = session.stop() {
        eprintln!("failed to cleanly stop the cast session: {e:?}");
    }
    let _ = pump.join(); // media_events channel is now closed (io thread ended), so this returns immediately
    println!("Done.");

    Ok(())
}
