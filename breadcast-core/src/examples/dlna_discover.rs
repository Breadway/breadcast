//! Prints DLNA/UPnP media renderers as they appear/disappear on the LAN
//! (via periodic SSDP search for `AVTransport` services, the class of
//! device Windows' own "Cast to Device", Win+K, targets). Run with:
//! cargo run -p breadcast-core --example dlna_discover

use breadcast_core::{DlnaDiscovery, DlnaDiscoveryEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let (_discovery, mut events) = DlnaDiscovery::start();
    println!("Searching for DLNA/UPnP AVTransport devices every 30s... (Ctrl+C to stop)");

    while let Some(event) = events.recv().await {
        match event {
            DlnaDiscoveryEvent::Found(device) => {
                println!("+ {} [{}]", device.friendly_name, device.url);
            }
            DlnaDiscoveryEvent::Lost { url } => {
                println!("- {url}");
            }
        }
    }

    Ok(())
}
