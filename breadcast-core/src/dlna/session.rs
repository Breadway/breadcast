use anyhow::{Context, Result};
use rupnp::Service;
use std::time::Duration;

use super::device::DlnaDevice;
use super::AV_TRANSPORT;

/// A connected UPnP `AVTransport` control point for a single DLNA media
/// renderer.
///
/// Unlike [`CastSession`](crate::CastSession), there is no persistent
/// connection or background thread to manage here: every UPnP action is an
/// independent SOAP-over-HTTP request, so `DlnaSession` is just a cheap,
/// `Clone`-able handle to the renderer's resolved control endpoint.
/// Concurrent `load()`/`stop()` calls are naturally safe: each is its own
/// HTTP request, with none of the single-socket message-demultiplexing
/// hazard `CastSession` has to run an actor thread to avoid for CASTV2.
#[derive(Clone)]
pub struct DlnaSession {
    device_url: rupnp::http::Uri,
    service: Service,
}

impl DlnaSession {
    /// Fetches `device`'s full description and resolves its `AVTransport`
    /// service. Fails if the device no longer answers, or turns out not to
    /// expose `AVTransport` after all; shouldn't happen given discovery
    /// already searched for exactly that service, but a device's
    /// description can in principle change between being found and being
    /// connected to.
    pub async fn connect(device: &DlnaDevice) -> Result<Self> {
        let device_url: rupnp::http::Uri = device
            .url
            .parse()
            .with_context(|| format!("invalid device description URL: {}", device.url))?;

        let full_device = rupnp::Device::from_url(device_url.clone())
            .await
            .with_context(|| format!("failed to fetch device description from {}", device.url))?;

        let service = full_device
            .find_service(&AV_TRANSPORT)
            .with_context(|| format!("{} has no AVTransport service", device.friendly_name))?
            .clone();

        Ok(Self { device_url, service })
    }

    /// Sets `content_url` as the renderer's current transport URI and
    /// starts playback.
    ///
    /// `content_url` is XML-escaped before being embedded in the SOAP
    /// request body: in breadcast's own usage it's always a URL this
    /// process generated itself (safe by construction), but nothing about
    /// this function's signature guarantees that stays true for every
    /// caller, and an unescaped `&` alone would produce malformed XML the
    /// renderer would reject with no useful diagnostic.
    pub async fn load(&self, content_url: &str) -> Result<()> {
        let escaped = xml_escape(content_url);
        // Many Samsung/LG renderers reject an empty CurrentURIMetaData and
        // need DIDL-Lite + protocolInfo before they will play a live HLS
        // playlist. The DIDL itself is then XML-escaped for the SOAP body.
        let didl = format!(
            r#"<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/"><item id="0" parentID="-1" restricted="1"><dc:title>breadcast</dc:title><upnp:class>object.item.videoItem</upnp:class><res protocolInfo="http-get:*:application/vnd.apple.mpegurl:*">{escaped}</res></item></DIDL-Lite>"#
        );
        let metadata = xml_escape(&didl);
        let set_uri_payload = format!(
            "<InstanceID>0</InstanceID><CurrentURI>{escaped}</CurrentURI><CurrentURIMetaData>{metadata}</CurrentURIMetaData>"
        );
        self.service
            .action(&self.device_url, "SetAVTransportURI", &set_uri_payload)
            .await
            .context("SetAVTransportURI failed")?;

        // Some renderers auto-play after SetURI and then reject Play;
        // others stay TRANSITIONING for a moment. Either PLAYING state is
        // success.
        match self
            .service
            .action(&self.device_url, "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
            .await
        {
            Ok(_) => {}
            Err(e) => {
                // The renderer may already be auto-playing after SetURI (and
                // reject a redundant Play), or may still be TRANSITIONING and
                // only reach PLAYING a moment later -- either is success.
                // A single immediate GetTransportInfo isn't enough for the
                // latter, so give it a short bounded window before giving up
                // on the Play error.
                let mut attempts = 0u32;
                loop {
                    if self.transport_state().await.ok().as_deref() == Some("PLAYING") {
                        break;
                    }
                    attempts += 1;
                    if attempts >= 5 {
                        return Err(e).context("Play failed");
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }

        Ok(())
    }

    /// Stops playback. Unlike `CastSession::stop`, there's no persistent
    /// session or launched app to tear down: this is just the `Stop`
    /// action.
    pub async fn stop(&self) -> Result<()> {
        self.service
            .action(&self.device_url, "Stop", "<InstanceID>0</InstanceID>")
            .await
            .context("Stop failed")?;
        Ok(())
    }

    /// Polls `GetTransportInfo` and returns the renderer's own reported
    /// `CurrentTransportState` (e.g. `"PLAYING"`, `"TRANSITIONING"`,
    /// `"STOPPED"`, `"NO_MEDIA_PRESENT"`).
    ///
    /// This is a request/response poll, not a push subscription:
    /// `AVTransport` does support UPnP eventing (`Service::subscribe` in
    /// `rupnp`) for state pushed as it changes, but that needs a locally
    /// bound HTTP callback listener, which is more machinery than a status
    /// check is worth for now. A caller that wants live updates polls this
    /// on an interval instead.
    pub async fn transport_state(&self) -> Result<String> {
        let response = self
            .service
            .action(&self.device_url, "GetTransportInfo", "<InstanceID>0</InstanceID>")
            .await
            .context("GetTransportInfo failed")?;
        response
            .get("CurrentTransportState")
            .cloned()
            .context("GetTransportInfo response had no CurrentTransportState")
    }
}

pub(crate) fn xml_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::xml_escape;

    #[test]
    fn xml_escape_covers_the_five_markup_chars() {
        assert_eq!(xml_escape(r#"a&b<c>d"e'f"#), "a&amp;b&lt;c&gt;d&quot;e&apos;f");
        assert_eq!(xml_escape("http://10.0.0.1:1/t/playlist.m3u8"), "http://10.0.0.1:1/t/playlist.m3u8");
    }
}
