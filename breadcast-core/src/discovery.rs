use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mdns_sd::{ScopedIp, ServiceDaemon, ServiceEvent};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::device::CastDevice;

/// How long a resolved address is trusted without being re-seen in a later
/// `ServiceResolved` event. mDNS re-resolves well inside this window in
/// normal operation, so an address that goes quiet for this long is more
/// likely stale (DHCP lease change, interface removed) than merely unlucky
/// timing.
const ADDRESS_STALE_AFTER: Duration = Duration::from_secs(300);

/// Picks the best address to connect to a resolved device on: routable IPv4
/// first (works unambiguously with rust_cast's TLS connect and with URLs
/// embedded in Cast media-load requests), falling back to a non-link-local
/// IPv6 address. Link-local IPv6 (`fe80::...%wlan0`) is deliberately last
/// resort: its zone-id suffix doesn't round-trip through `IpAddr`'s
/// `FromStr`/`Display`, so it can silently become unconnectable downstream.
///
/// `addresses` is sorted most-recently-seen first (with a stable tie-break)
/// before picking, rather than iterated in whatever order a `HashSet` (or a
/// `Vec` built from one) happens to produce: with a device that resolves
/// to two IPv4 addresses (e.g. wired + wireless, or a DHCP lease change),
/// an unordered choice can silently pick a stale/unreachable one, and pick
/// a *different* one across otherwise-identical runs.
fn pick_address(addresses: &[(ScopedIp, Instant)]) -> Option<String> {
    let now = Instant::now();
    let mut candidates: Vec<&(ScopedIp, Instant)> = addresses
        .iter()
        .filter(|(_, seen)| now.duration_since(*seen) < ADDRESS_STALE_AFTER)
        .collect();
    candidates.sort_by(|a, b| {
        b.1.cmp(&a.1) // most-recently-seen first
            .then_with(|| a.0.to_ip_addr().to_string().cmp(&b.0.to_ip_addr().to_string()))
    });

    candidates
        .iter()
        .find(|(ip, _)| ip.is_ipv4())
        .or_else(|| {
            candidates.iter().find(|(ip, _)| match ip.to_ip_addr() {
                IpAddr::V6(v6) => !v6.is_unicast_link_local(),
                IpAddr::V4(_) => false,
            })
        })
        .or_else(|| candidates.first())
        // Prefer ScopedIp's Display so a last-resort link-local IPv6
        // keeps its `%iface` zone -- `IpAddr` drops it and
        // `TcpStream::connect("fe80::…")` then fails with EINVAL.
        .map(|(ip, _)| ip.to_string())
}

const SERVICE_TYPE: &str = "_googlecast._tcp.local.";

/// A change in the set of Cast devices visible on the LAN.
///
/// `Found` fires on every mDNS re-resolution of a device (e.g. once per
/// network interface, or periodically as records refresh), not just the
/// first sighting; consumers should treat it as an upsert keyed by
/// `CastDevice::id`, not an append-only log.
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    Found(CastDevice),
    Lost { id: String },
}

/// Browses `_googlecast._tcp.local.` on a background thread and forwards
/// found/lost devices over an unbounded channel. Dropping the returned
/// `Discovery` stops the underlying mDNS daemon (via an explicit `Drop`
/// impl below: `mdns_sd::ServiceDaemon` has none of its own, so without
/// it every dropped `Discovery` would leak its daemon thread and 5353
/// multicast socket for the rest of the process's life).
pub struct Discovery {
    daemon: ServiceDaemon,
}

impl Drop for Discovery {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

impl Discovery {
    /// Starts browsing and returns the handle plus a receiver of events.
    /// `id=` TXT records are used to dedupe: a `ServiceResolved` for an
    /// already-known id is treated as an update, not a duplicate `Found`.
    pub fn start() -> Result<(Self, mpsc::UnboundedReceiver<DiscoveryEvent>)> {
        let daemon = ServiceDaemon::new().context("failed to start mDNS daemon")?;
        let browse_rx = daemon
            .browse(SERVICE_TYPE)
            .context("failed to browse _googlecast._tcp.local.")?;

        let (tx, rx) = mpsc::unbounded_channel();

        // mdns-sd's receiver is a blocking `flume` channel, so it needs its
        // own OS thread rather than a tokio task; forwarding into an
        // unbounded tokio channel is a non-blocking send from here.
        std::thread::spawn(move || {
            // fullname -> last-known device id, so a ServiceRemoved (which
            // only carries the fullname) can still emit the right Lost{id}.
            let mut fullname_to_id: HashMap<String, String> = HashMap::new();
            // fullname -> every address seen for it, with a last-seen
            // timestamp each. mDNS resolves progressively: the first
            // ServiceResolved for a device often carries only a link-local
            // IPv6 address, with the routable IPv4/global-IPv6 address
            // arriving in a later event for the same fullname. Accumulating
            // (not replacing) means `pick_address` always chooses from
            // everything seen so far, not just whatever happened to be in
            // the latest packet; the timestamp lets it also prefer the
            // freshest address and ignore ones that have gone stale.
            let mut fullname_to_addrs: HashMap<String, Vec<(ScopedIp, std::time::Instant)>> = HashMap::new();

            while let Ok(event) = browse_rx.recv() {
                match event {
                    ServiceEvent::ServiceResolved(info) => {
                        let Some(id) = info.get_property_val_str("id").map(str::to_string) else {
                            warn!(fullname = %info.get_fullname(), "cast device missing id= TXT record, skipping");
                            continue;
                        };
                        let name = info
                            .get_property_val_str("fn")
                            .unwrap_or_else(|| info.get_hostname())
                            .to_string();
                        let model = info
                            .get_property_val_str("md")
                            .unwrap_or("Chromecast")
                            .to_string();

                        let addrs = fullname_to_addrs
                            .entry(info.get_fullname().to_string())
                            .or_default();
                        let now = std::time::Instant::now();
                        for addr in info.get_addresses().iter().cloned() {
                            match addrs.iter_mut().find(|(seen, _)| *seen == addr) {
                                Some(entry) => entry.1 = now,
                                None => addrs.push((addr, now)),
                            }
                        }

                        let Some(host) = pick_address(addrs) else {
                            warn!(%id, "cast device resolved with no usable addresses, skipping");
                            continue;
                        };
                        // First ServiceResolved is often IPv6-only (or a
                        // zoneless fe80::). rust_cast / Cast Streaming
                        // cannot connect to that. Wait for a later
                        // resolve that carries IPv4 or a scoped address.
                        if host.parse::<std::net::Ipv6Addr>().is_ok()
                            && host.starts_with("fe80:")
                            && !host.contains('%')
                        {
                            warn!(%id, %host, "cast device resolved to an unscoped link-local IPv6, waiting for a better address");
                            continue;
                        }

                        fullname_to_id.insert(info.get_fullname().to_string(), id.clone());

                        let device = CastDevice {
                            id,
                            name,
                            model,
                            host,
                            port: info.get_port(),
                        };
                        debug!(?device, "cast device found");
                        if tx.send(DiscoveryEvent::Found(device)).is_err() {
                            break; // receiver dropped, stop the thread
                        }
                    }
                    ServiceEvent::ServiceRemoved(_ty, fullname) => {
                        fullname_to_addrs.remove(&fullname);
                        if let Some(id) = fullname_to_id.remove(&fullname) {
                            debug!(%id, "cast device lost");
                            if tx.send(DiscoveryEvent::Lost { id }).is_err() {
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok((Self { daemon }, rx))
    }

    /// Stops the mDNS daemon and its browse thread.
    pub fn stop(self) -> Result<()> {
        self.daemon
            .shutdown()
            .context("failed to shut down mDNS daemon")?;
        Ok(())
    }
}
