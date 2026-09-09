use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};

/// Number of worker threads pulling requests off the server's shared queue.
/// tiny_http's `Server::recv()` takes `&self` specifically so it can be
/// called from multiple threads concurrently (its own docs' recommended
/// pattern); bounding this at a small fixed number, instead of spawning a
/// fresh OS thread per request, keeps the request volume any single LAN
/// host can inflict on this process capped, since this server ends up
/// reachable by every device on the LAN, not just the Cast receiver it's
/// meant for.
const WORKER_THREADS: usize = 8;

/// Serves `root` (the `hlssink3` output directory: `playlist.m3u8` +
/// `segment*.ts`) over plain HTTP. Runs a small fixed pool of worker
/// threads. [`HttpServer::shutdown`] (also invoked from `Drop`) unblocks
/// those workers and closes the listener so a finished DLNA session does
/// not keep the last screen-recording segments reachable on the LAN.
///
/// Every servable path is namespaced under a random token
/// (`/<token>/playlist.m3u8`, etc.; see [`HttpServer::token`]) rather than
/// served at the root. There's no way to add an `Authorization` header a
/// Cast receiver will send back, so this is the standard mitigation for a
/// server that must stay unauthenticated but shouldn't let every other
/// device on the LAN casually load `/playlist.m3u8` and watch this
/// machine's screen.
///
/// Sets `Content-Type` per Google's Cast media docs (the receiver's HTTP
/// client needs a correct type to load segments reliably), `Cache-Control`
/// (critical for `playlist.m3u8`, which is rewritten every segment: a
/// cached stale copy stalls playback with no error anywhere), Range/206
/// support, and a permissive CORS header; get this right from the start
/// since a Chromecast has no devtools console to debug a silent load
/// failure against.
pub struct HttpServer {
    addr: SocketAddr,
    token: String,
    server: Option<Arc<tiny_http::Server>>,
}

impl HttpServer {
    /// Binds on `bind_addr` (use `0.0.0.0:0` to let the OS pick a free
    /// port; read the actual port back via [`HttpServer::addr`]) and
    /// starts serving `root` in the background. Build URLs to hand to a
    /// Cast device with [`HttpServer::url`], not by hand: it includes the
    /// required path token.
    pub fn start(bind_addr: &str, root: PathBuf) -> Result<Self> {
        let root = root
            .canonicalize()
            .with_context(|| format!("failed to canonicalize HLS root {}", root.display()))?;

        let server = tiny_http::Server::http(bind_addr)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("failed to bind HTTP server on {bind_addr}"))?;
        let addr = match server.server_addr() {
            tiny_http::ListenAddr::IP(addr) => addr,
            other => anyhow::bail!("HTTP server bound to a non-IP address: {other:?}"),
        };

        let server = Arc::new(server);
        let token = random_token();

        for _ in 0..WORKER_THREADS {
            let server = Arc::clone(&server);
            let root = root.clone();
            let token = token.clone();
            std::thread::spawn(move || {
                while let Ok(request) = server.recv() {
                    if let Err(e) = handle_request(request, &root, &token) {
                        tracing::warn!(error = %e, "HLS HTTP request failed");
                    }
                }
            });
        }

        Ok(Self { addr, token, server: Some(server) })
    }

    /// Unblocks every worker and drops the listener. After this returns the
    /// bind address is free and the path token no longer serves anything.
    /// Safe to call more than once.
    pub fn shutdown(&mut self) {
        let Some(server) = self.server.take() else { return };
        // `unblock` wakes one `recv()` at a time; wake every worker so
        // they all observe the error and drop their Arc.
        for _ in 0..WORKER_THREADS {
            server.unblock();
        }
    }

    /// The bound address, e.g. `0.0.0.0:41823`. Combine with this
    /// machine's LAN IP (not `0.0.0.0` itself) to build the URL handed to
    /// the Cast device: `0.0.0.0` only means anything to sockets on this
    /// host.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Builds a full URL for `relative` (e.g. `"playlist.m3u8"`), rooted at
    /// `host` (this machine's LAN-reachable IP; see [`HttpServer::addr`]'s
    /// doc for why that can't just be `self.addr()`), including the
    /// unguessable path token every request must carry.
    pub fn url(&self, host: std::net::IpAddr, relative: &str) -> String {
        let port = self.addr.port();
        match host {
            std::net::IpAddr::V6(v6) => format!("http://[{v6}]:{port}/{}/{relative}", self.token),
            std::net::IpAddr::V4(v4) => format!("http://{v4}:{port}/{}/{relative}", self.token),
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Generates a 32-hex-character unguessable token from `/dev/urandom`. This
/// is a Linux-only project already (PipeWire, Hyprland's portal, VA-API) so
/// reaching for the platform's random device directly is fine: no `rand`
/// crate dependency for one call site.
fn random_token() -> String {
    let mut bytes = [0u8; 16];
    let read_ok = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| {
            use std::io::Read;
            f.read_exact(&mut bytes)
        })
        .is_ok();
    if !read_ok {
        // Unreachable in practice on Linux, but better than a zero-entropy
        // token if it ever happened. Mix several low-cost, not-directly-
        // foreseeable sources (two time reads a hair apart, pid, a process
        // counter) into both hashes and write all 16 bytes, so the fallback
        // never reduces to a single low-resolution clock tick or per-pid
        // reuse -- still far weaker than /dev/urandom, which is why it stays
        // a fallback, but not trivially guessable.
        let mix = |salt: u64| {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::time::SystemTime::now().hash(&mut hasher);
            std::time::SystemTime::UNIX_EPOCH
                .elapsed()
                .ok()
                .map(|d| d.as_nanos())
                .unwrap_or_default()
                .hash(&mut hasher);
            std::process::id().hash(&mut hasher);
            salt.hash(&mut hasher);
            hasher.finish()
        };
        let a = mix(0x9e3779b97f4a7c15);
        let b = mix(0xdead_beef_dead_beef);
        bytes[..8].copy_from_slice(&a.to_le_bytes());
        bytes[8..].copy_from_slice(&b.to_le_bytes());
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Parses a single-range `Range: bytes=start-end` header value against a
/// body of `len` bytes. Returns `None` for anything absent, malformed, or
/// multi-range (multipart ranges aren't needed for HLS segment fetches, and
/// falling back to a full 200 response for those is always a valid
/// response under the HTTP spec).
pub(crate) fn parse_range(value: &str, len: usize) -> Option<(usize, usize)> {
    let spec = value.strip_prefix("bytes=")?;
    if spec.contains(',') || len == 0 {
        return None;
    }
    let (start, end) = spec.split_once('-')?;
    let last = len - 1;
    match (start.trim(), end.trim()) {
        ("", "") => None,
        ("", suffix_len) => {
            let n: usize = suffix_len.parse().ok()?;
            Some((len.saturating_sub(n), last))
        }
        (start, "") => {
            let start: usize = start.parse().ok()?;
            Some((start, last))
        }
        (start, end) => {
            let start: usize = start.parse().ok()?;
            let end: usize = end.parse().ok()?;
            Some((start, end))
        }
    }
}

fn handle_request(request: tiny_http::Request, root: &Path, token: &str) -> Result<()> {
    // `Split::next()` on a non-empty pattern always yields at least one
    // item, so this never actually hits a `None` case.
    let url_path = request.url().split('?').next().expect("split always yields at least one item").to_string();
    let remote = request
        .remote_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "?".to_string());

    // Reject anything not under the unguessable token prefix before even
    // touching the filesystem; see the struct docs for why this exists.
    let Some(relative) = url_path
        .trim_start_matches('/')
        .strip_prefix(token)
        .and_then(|rest| rest.strip_prefix('/'))
    else {
        tracing::info!(%remote, path = %url_path, status = 404, "HLS request (bad or missing path token)");
        return respond_status(request, 404);
    };

    // Reject any path that could escape `root` (e.g. `../../etc/passwd`):
    // the URL is attacker-controlled input the moment this server is
    // reachable from the LAN, which it is by design (the Cast device is a
    // different host). `root` itself is canonicalized once in `start()`, so
    // this comparison is meaningful even if `root` was originally relative
    // or contained a symlinked component: comparing a canonical path
    // against a non-canonical one would make `starts_with` spuriously fail
    // and 404 every request.
    let requested = root.join(relative);
    let Ok(canonical) = requested.canonicalize() else {
        tracing::info!(%remote, path = %url_path, status = 404, "HLS request (not found)");
        return respond_status(request, 404);
    };
    if !canonical.starts_with(root) || !canonical.is_file() {
        tracing::info!(%remote, path = %url_path, status = 404, "HLS request (outside root or not a file)");
        return respond_status(request, 404);
    }

    let data = match std::fs::read(&canonical) {
        Ok(data) => data,
        Err(_) => {
            // hlssink3 rotates out old segments (`max-files`) concurrently
            // with requests for them; a file that existed at
            // canonicalize() above but is gone by the time it's read is
            // routine for a live stream, not a server fault. Answer with a
            // normal 404 (a receiver skips it and asks for the next
            // segment) instead of letting the request drop unanswered,
            // which tiny_http turns into an unexplained bare 500.
            tracing::info!(%remote, path = %url_path, status = 404, "HLS request (file removed before read, likely segment rotation)");
            return respond_status(request, 404);
        }
    };

    let extension = canonical.extension().and_then(|e| e.to_str());
    let content_type = match extension {
        Some("m3u8") => "application/vnd.apple.mpegurl",
        Some("ts") => "video/mp2t",
        _ => "application/octet-stream",
    };
    // The playlist is rewritten in place on every segment: must never be
    // cached, or a receiver/intermediary replaying a stale copy stalls
    // playback with no error anywhere. Segments are written once under a
    // unique numbered filename and never modified after that, so they're
    // safe to cache aggressively.
    let cache_control = match extension {
        Some("m3u8") => "no-cache, no-store, must-revalidate",
        _ => "public, max-age=3600, immutable",
    };

    let range = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Range"))
        .and_then(|h| parse_range(h.value.as_str(), data.len()));

    let mut headers = vec![
        ("Content-Type".to_string(), content_type.to_string()),
        ("Cache-Control".to_string(), cache_control.to_string()),
        ("Access-Control-Allow-Origin".to_string(), "*".to_string()),
        ("Access-Control-Allow-Headers".to_string(), "Range, Accept-Encoding".to_string()),
        ("Access-Control-Expose-Headers".to_string(), "Content-Length, Content-Range".to_string()),
        ("Accept-Ranges".to_string(), "bytes".to_string()),
    ];

    let (status, body) = match range {
        // RFC 7233: a last-byte-pos past the end is clamped, not 416.
        // HLS clients often probe `bytes=0-1048575` against a ~400KB
        // segment; answering 416 stalls playback with no encoder error.
        Some((start, end)) if start < data.len() && start <= end => {
            let end = end.min(data.len() - 1);
            headers.push(("Content-Range".to_string(), format!("bytes {start}-{end}/{}", data.len())));
            (206u16, data[start..=end].to_vec())
        }
        Some(_) => {
            headers.push(("Content-Range".to_string(), format!("bytes */{}", data.len())));
            tracing::info!(%remote, path = %url_path, status = 416, "HLS request (unsatisfiable range)");
            return respond(request, 416, Vec::new(), &headers);
        }
        None => (200u16, data),
    };

    tracing::info!(%remote, path = %url_path, status, "HLS request");
    respond(request, status, body, &headers)
}

fn respond(request: tiny_http::Request, status: u16, body: Vec<u8>, headers: &[(String, String)]) -> Result<()> {
    let mut response = tiny_http::Response::from_data(body).with_status_code(status);
    for (name, value) in headers {
        if let Ok(header) = tiny_http::Header::from_bytes(name.as_bytes(), value.as_bytes()) {
            response.add_header(header);
        }
    }
    request.respond(response).context("failed to write HTTP response")
}

fn respond_status(request: tiny_http::Request, status: u16) -> Result<()> {
    request
        .respond(tiny_http::Response::empty(status))
        .context("failed to write HTTP error response")
}

#[cfg(test)]
mod tests {
    use super::parse_range;

    #[test]
    fn parse_range_accepts_the_usual_hls_shapes() {
        assert_eq!(parse_range("bytes=0-99", 200), Some((0, 99)));
        assert_eq!(parse_range("bytes=50-", 200), Some((50, 199)));
        assert_eq!(parse_range("bytes=-20", 200), Some((180, 199)));
        assert_eq!(parse_range("bytes=0-0", 200), Some((0, 0)));
    }

    #[test]
    fn parse_range_rejects_malformed_or_multipart() {
        assert_eq!(parse_range("bytes=", 200), None);
        assert_eq!(parse_range("bytes=-", 200), None);
        assert_eq!(parse_range("bytes=0-10,20-30", 200), None);
        assert_eq!(parse_range("items=0-10", 200), None);
        assert_eq!(parse_range("bytes=0-10", 0), None);
    }
}
