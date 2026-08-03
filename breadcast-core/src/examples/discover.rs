//! Prints Chromecast/Google TV devices as they appear/disappear on the LAN.
//! Run with: cargo run -p breadcast-core --example discover

use breadcast_core::{Discovery, DiscoveryEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let (_discovery, mut events) = Discovery::start()?;
    println!("Browsing for _googlecast._tcp.local. devices... (Ctrl+C to stop)");

    while let Some(event) = events.recv().await {
        match event {
            DiscoveryEvent::Found(device) => {
                println!("+ {} ({}) at {}:{} [{}]", device.name, device.model, device.host, device.port, device.id);
            }
            DiscoveryEvent::Lost { id } => {
                println!("- {id}");
            }
        }
    }

    Ok(())
}
