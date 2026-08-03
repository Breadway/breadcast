//! DLNA/UPnP media-renderer casting: discovery via SSDP, control via the
//! `AVTransport` service's SOAP actions. This is the protocol behind most
//! non-Chromecast smart TVs (Samsung, LG/Tizen, Sony) and behind Windows'
//! own "Cast to Device" (Win+K) flyout — a different device population
//! than [`crate::cast_sender`]'s Cast V2/CASTV2, not an alternate path to
//! the same devices.
//!
//! Reuses the rest of breadcast-core as-is: the same [`crate::capture`],
//! [`crate::pipeline`], and [`crate::http_server`] produce the HLS stream a
//! [`DlnaSession`] is handed a URL to — only discovery and the
//! device-control protocol differ from the Cast path.

mod device;
mod discovery;
mod session;

use rupnp::ssdp::URN;

pub use device::DlnaDevice;
pub use discovery::{DlnaDiscovery, DlnaDiscoveryEvent};
pub use session::DlnaSession;

const AV_TRANSPORT: URN = URN::service("schemas-upnp-org", "AVTransport", 1);
