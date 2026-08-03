/// A DLNA/UPnP media renderer discovered on the LAN via SSDP — the class of
/// device Windows' own "Cast to Device" (Win+K) targets, and what most
/// non-Chromecast smart TVs (Samsung, LG/Tizen, Sony) expose alongside or
/// instead of Google Cast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlnaDevice {
    /// Human-readable name from the device's `friendlyName` element.
    pub friendly_name: String,
    /// The device description XML URL (e.g. `http://192.168.1.50:1400/desc.xml`).
    /// This, not a separately-tracked id, is what uniquely identifies a UPnP
    /// device here — UPnP itself has a UDN concept, but reading it requires
    /// enabling `rupnp`'s `full_device_spec` feature for that one field,
    /// while the description URL is already available for free and is
    /// exactly what `rupnp::Device` itself keys its own identity on.
    pub url: String,
}
