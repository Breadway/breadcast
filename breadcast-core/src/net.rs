use std::net::{IpAddr, Ipv4Addr};

use anyhow::{Context, Result};

const EXCLUDED_PREFIXES: &[&str] = &["tailscale", "wg", "docker", "veth", "br-", "virbr", "lo"];

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
    pick_lan_ip(None)
}

/// Like [`local_lan_ip`], but prefers the interface whose subnet contains
/// `peer`. Dual-homed machines (ethernet + wifi, two VLANs) otherwise
/// embed the wrong host in the HLS URL and the renderer cannot fetch it —
/// the same silent `LOAD FAILED` class as the Tailscale case above.
pub fn local_lan_ip_for(peer: IpAddr) -> Result<IpAddr> {
    pick_lan_ip(Some(peer))
}

fn pick_lan_ip(peer: Option<IpAddr>) -> Result<IpAddr> {
    let mut candidates = lan_ipv4_candidates()?;

    if let Some(IpAddr::V4(peer_v4)) = peer {
        if let Some((_, addr, _)) = candidates
            .iter()
            .filter(|(_, addr, prefix)| same_subnet(*addr, peer_v4, *prefix))
            .max_by_key(|(_, _, prefix)| *prefix)
        {
            return Ok(IpAddr::V4(*addr));
        }
    }

    // Prefer a conventionally-named physical/Wi-Fi interface when there's a
    // choice, but any private, non-excluded address is acceptable.
    candidates.sort_by_key(|(iface, _, _)| !(iface.starts_with("wl") || iface.starts_with("en") || iface.starts_with("eth")));

    candidates
        .into_iter()
        .map(|(_, addr, _)| IpAddr::V4(addr))
        .next()
        .context("no LAN-reachable IPv4 address found (excluding loopback/VPN/virtual interfaces) — is this machine connected to a network?")
}

/// Enumerates this machine's private, LAN-reachable IPv4 addresses as
/// `(interface, address, prefix-len)` tuples. Primary path parses `ip -o
/// addr show` -- the formatting it relies on is stable, and it bakes in the
/// `scope global up` filter (excluding link-local 169.254/16 and down
/// interfaces) for free -- falling back to a `getifaddrs`-based enumeration
/// (`if-addrs`, already in the dependency tree via `mdns-sd`) when the `ip`
/// command is unavailable, so the LAN-IP discovery doesn't silently depend on
/// iproute2 being installed.
fn lan_ipv4_candidates() -> Result<Vec<(String, Ipv4Addr, u8)>> {
    match ip_command_candidates() {
        Ok(candidates) if !candidates.is_empty() => return Ok(candidates),
        Ok(_) => {} // `ip` ran but found no private address -- genuine, fall through
        Err(e) => {
            tracing::debug!(error = ?e, "`ip addr show` unavailable, falling back to getifaddrs");
        }
    }
    getifaddrs_candidates()
}

fn ip_command_candidates() -> Result<Vec<(String, Ipv4Addr, u8)>> {
    let output = std::process::Command::new("ip")
        .args(["-4", "-o", "addr", "show", "scope", "global", "up"])
        .output()
        .context("failed to run `ip addr show` to find this machine's LAN IP")?;
    if !output.status.success() {
        anyhow::bail!("`ip addr show` exited with {}", output.status);
    }
    let text = String::from_utf8_lossy(&output.stdout);

    let mut candidates: Vec<(String, Ipv4Addr, u8)> = Vec::new();
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
        let mut parts = cidr.split('/');
        let Some(addr) = parts.next().and_then(|a| a.parse::<Ipv4Addr>().ok()) else { continue };
        let prefix: u8 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(32);
        if !addr.is_private() {
            continue;
        }
        candidates.push((iface.to_string(), addr, prefix.min(32)));
    }
    Ok(candidates)
}

/// `getifaddrs`-based counterpart to [`ip_command_candidates`], mirroring the
/// `ip` path's filters (up, non-loopback, private, not link-local, not an
/// excluded interface name) so the two produce equivalent candidate sets.
fn getifaddrs_candidates() -> Result<Vec<(String, Ipv4Addr, u8)>> {
    let ifaces = if_addrs::get_if_addrs().context("failed to enumerate network interfaces (getifaddrs fallback)")?;
    let mut candidates: Vec<(String, Ipv4Addr, u8)> = Vec::new();
    for iface in ifaces {
        if EXCLUDED_PREFIXES.iter().any(|p| iface.name.starts_with(p)) {
            continue;
        }
        if !iface.is_oper_up() {
            continue;
        }
        let if_addrs::IfAddr::V4(v4) = iface.addr else { continue };
        // Mirrors `type(-4 -o addr show scope global up)`: drop loopback,
        // link-local (169.254) and non-private networks.
        if v4.ip.is_loopback() || v4.ip.is_link_local() || !v4.ip.is_private() {
            continue;
        }
        candidates.push((iface.name.clone(), v4.ip, v4.prefixlen.min(32)));
    }
    Ok(candidates)
}

fn same_subnet(a: Ipv4Addr, b: Ipv4Addr, prefix: u8) -> bool {
    if prefix == 0 {
        return true;
    }
    let mask = if prefix >= 32 {
        u32::MAX
    } else {
        !((1u32 << (32 - prefix)) - 1)
    };
    (u32::from(a) & mask) == (u32::from(b) & mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_subnet_respects_prefix_length() {
        let a: Ipv4Addr = "10.179.161.89".parse().unwrap();
        let b: Ipv4Addr = "10.179.160.1".parse().unwrap();
        let other: Ipv4Addr = "10.0.0.1".parse().unwrap();
        assert!(same_subnet(a, b, 23));
        assert!(!same_subnet(a, other, 23));
        assert!(same_subnet(a, a, 32));
        assert!(!same_subnet(a, b, 32));
    }
}
