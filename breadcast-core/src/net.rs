use std::net::{IpAddr, Ipv4Addr};

use anyhow::{Context, Result};

/// Finds this machine's LAN-reachable IPv4 address by enumerating network
/// interfaces directly, rather than the more common "UDP-connect to a
/// public address and read back the local endpoint" trick — that trick
/// asks the kernel's *default route*, which Tailscale can silently take
/// over via policy routing (confirmed on a real machine: with Tailscale
/// active, `ip route get 8.8.8.8` resolves via `tailscale0`, not the real
/// LAN interface). Using it handed a Cast device a 100.64.0.0/10 Tailscale
/// CGNAT address it could never reach, which surfaced only as an
/// unexplained `LOAD FAILED` — exactly the class of silent failure this
/// project has already spent a lot of time chasing. Explicitly excluding
/// VPN/virtual interface name prefixes sidesteps the whole problem
/// regardless of what the default route happens to be.
///
/// Shared by every casting protocol (Cast, DLNA, ...) — they all need to
/// embed this machine's own address in a URL handed to a receiver device.
pub fn local_lan_ip() -> Result<IpAddr> {
    const EXCLUDED_PREFIXES: &[&str] = &["tailscale", "wg", "docker", "veth", "br-", "virbr", "lo"];

    let output = std::process::Command::new("ip")
        .args(["-4", "-o", "addr", "show", "scope", "global", "up"])
        .output()
        .context("failed to run `ip addr show` to find this machine's LAN IP")?;
    if !output.status.success() {
        anyhow::bail!("`ip addr show` exited with {}", output.status);
    }
    let text = String::from_utf8_lossy(&output.stdout);

    let mut candidates: Vec<(String, Ipv4Addr)> = Vec::new();
    for line in text.lines() {
        // Format: "3: wlan0    inet 10.179.161.89/23 brd ... scope global dynamic wlan0"
        let mut fields = line.split_whitespace();
        let Some(_index) = fields.next() else { continue };
        let Some(iface) = fields.next() else { continue };
        if EXCLUDED_PREFIXES.iter().any(|p| iface.starts_with(p)) {
            continue;
        }
        if fields.next() != Some("inet") {
            continue;
        }
        let Some(cidr) = fields.next() else { continue };
        let Some(addr) = cidr.split('/').next().and_then(|a| a.parse::<Ipv4Addr>().ok()) else { continue };
        if !addr.is_private() {
            continue;
        }
        candidates.push((iface.to_string(), addr));
    }

    // Prefer a conventionally-named physical/Wi-Fi interface when there's a
    // choice, but any private, non-excluded address is acceptable.
    candidates.sort_by_key(|(iface, _)| !(iface.starts_with("wl") || iface.starts_with("en") || iface.starts_with("eth")));

    candidates
        .into_iter()
        .map(|(_, addr)| IpAddr::V4(addr))
        .next()
        .context("no LAN-reachable IPv4 address found (excluding loopback/VPN/virtual interfaces) — is this machine connected to a network?")
}
