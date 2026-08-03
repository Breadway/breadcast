use serde::{Deserialize, Serialize};

/// A Chromecast / Google TV device discovered on the LAN via mDNS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CastDevice {
    /// Stable device id from the `id=` TXT record.
    pub id: String,
    /// Friendly name from the `fn=` TXT record (e.g. "Living Room TV").
    pub name: String,
    /// Model name from the `md=` TXT record (e.g. "Chromecast").
    pub model: String,
    pub host: String,
    pub port: u16,
}
