use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rupnp::ssdp::SearchTarget;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::device::DlnaDevice;
use super::{AV_TRANSPORT, MEDIA_RENDERER};

/// How often to re-issue an SSDP search burst. Unlike mDNS (continuous
/// multicast browsing via `mdns-sd` in [`crate::discovery`]), SSDP has no
/// "subscribe and get pushed updates" primitive exposed by `rupnp` —
/// discovery here is "ask, wait [`SEARCH_TIMEOUT`], collect whoever
/// answered," repeated on this interval, rather than event-driven.
const POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for M-SEARCH responses on each poll. SSDP devices are
/// expected to jitter their response within a device-chosen window, so this
/// needs to be more than an instant, but this is still per-poll latency
/// before newly-found devices are reported.
const SEARCH_TIMEOUT: Duration = Duration::from_secs(3);

/// A device not reconfirmed within this many consecutive polls is reported
/// lost. More than one (rather than declaring it gone after a single missed
/// poll) tolerates an occasional dropped UDP response — SSDP runs over
/// unreliable multicast, so one missed reply is routine, not a signal the
/// device actually left the network.
const MISSED_POLLS_BEFORE_LOST: u32 = 2;

/// A change in the set of DLNA renderers visible on the LAN. Mirrors
/// [`crate::discovery::DiscoveryEvent`]'s shape for consistency between the
/// two casting protocols, though the underlying mechanism differs (see
/// [`DlnaDiscovery`]'s docs).
#[derive(Debug, Clone)]
pub enum DlnaDiscoveryEvent {
    Found(DlnaDevice),
    Lost { url: String },
}

/// Periodically searches for UPnP `AVTransport` services (i.e. media
/// renderers — smart TVs, DLNA-capable receivers, and what Windows' own
/// "Cast to Device" targets) on the LAN via SSDP, and forwards found/lost
/// devices over a channel.
///
/// Runs as a background `tokio` task rather than the dedicated OS thread
/// [`crate::discovery::Discovery`] needs — `mdns-sd`'s browse channel is a
/// blocking `flume` receiver with no async-friendly interface, but `rupnp`
/// and `ssdp-client` are natively `tokio`-async, so a plain spawned task is
/// both sufficient and more idiomatic here. Dropping the returned
/// `DlnaDiscovery` aborts that task.
pub struct DlnaDiscovery {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for DlnaDiscovery {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl DlnaDiscovery {
    /// Starts polling and returns the handle plus a receiver of events.
    pub fn start() -> (Self, mpsc::UnboundedReceiver<DlnaDiscoveryEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(async move {
            // description URL -> (device, consecutive polls since last confirmed)
            let mut known: HashMap<String, (DlnaDevice, u32)> = HashMap::new();

            loop {
                let search_target = SearchTarget::URN(MEDIA_RENDERER);
                match rupnp::discover(&search_target, SEARCH_TIMEOUT, None).await {
                    Ok(stream) => {
                        use futures_util::StreamExt;
                        let mut stream = std::pin::pin!(stream);
                        let mut confirmed: HashSet<String> = HashSet::new();

                        while let Some(result) = stream.next().await {
                            let device = match result {
                                Ok(device) => device,
                                Err(e) => {
                                    warn!(error = %e, "failed to resolve a discovered UPnP device, skipping");
                                    continue;
                                }
                            };

                            let url = device.url().to_string();
                            if device.find_service(&AV_TRANSPORT).is_none() {
                                debug!(%url, "UPnP device has no AVTransport, skipping");
                                continue;
                            }
                            confirmed.insert(url.clone());

                            if let Some(entry) = known.get_mut(&url) {
                                entry.1 = 0;
                                continue;
                            }

                            let dlna_device = DlnaDevice {
                                friendly_name: device.friendly_name().to_string(),
                                url: url.clone(),
                            };
                            known.insert(url, (dlna_device.clone(), 0));
                            debug!(?dlna_device, "DLNA renderer found");
                            if tx.send(DlnaDiscoveryEvent::Found(dlna_device)).is_err() {
                                return; // receiver dropped, stop polling
                            }
                        }

                        known.retain(|url, (_, missed)| {
                            if confirmed.contains(url) {
                                true
                            } else {
                                *missed += 1;
                                if *missed >= MISSED_POLLS_BEFORE_LOST {
                                    debug!(%url, "DLNA renderer lost");
                                    let _ = tx.send(DlnaDiscoveryEvent::Lost { url: url.clone() });
                                    false
                                } else {
                                    true
                                }
                            }
                        });
                    }
                    Err(e) => warn!(error = %e, "SSDP search for AVTransport devices failed"),
                }

                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });

        (Self { task }, rx)
    }
}
