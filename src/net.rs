//! Peer transport setup.
//!
//! All network-facing configuration lives here, so that the "no external
//! server" guarantee is easy to audit: in the default [`RelayChoice::Lan`] mode
//! the endpoint runs with relays disabled, no n0 DNS/address lookup, no
//! STUN/QAD probes, and no gateway port mapping. It only ever sends UDP to
//! peers that are reachable directly, i.e. on the same LAN.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr, NetReportConfig, RelayMode, RelayUrl,
    endpoint::{PortmapperConfig, presets},
};
use iroh_mdns_address_lookup::MdnsAddressLookup;

/// How this endpoint reaches its peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayChoice {
    /// LAN only: no relay, no DNS lookup service, no internet probes.
    ///
    /// This is the default. Both machines must be able to reach each other
    /// directly (same LAN/Wi-Fi, or a hotspot between them).
    Lan,
    /// n0's public relays plus n0's DNS/pkarr lookup service.
    ///
    /// Needs working DNS and outbound TCP/UDP to `*.iroh.link`; fails behind
    /// networks that block those (many campus networks do).
    N0,
    /// A self-hosted relay, e.g. your own `iroh-relay` binary on the LAN.
    Url(RelayUrl),
}

impl Default for RelayChoice {
    fn default() -> Self {
        Self::Lan
    }
}

impl FromStr for RelayChoice {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "lan" | "off" | "local" => Ok(Self::Lan),
            "n0" | "default" | "public" => Ok(Self::N0),
            _ => {
                let url: RelayUrl = s
                    .trim()
                    .parse()
                    .map_err(|e| format!("invalid relay URL {s:?}: {e}"))?;
                Ok(Self::Url(url))
            }
        }
    }
}

impl RelayChoice {
    /// Whether this mode contacts a relay server at all.
    pub fn uses_relay(&self) -> bool {
        !matches!(self, Self::Lan)
    }

    /// Short human-readable description, for the startup banner.
    pub fn label(&self) -> String {
        match self {
            Self::Lan => "LAN only (no relay, no external server)".to_string(),
            Self::N0 => "n0 public relay (needs internet)".to_string(),
            Self::Url(url) => format!("relay {url}"),
        }
    }
}

/// Build an endpoint for the given relay mode, with mDNS for local discovery.
///
/// mDNS is only a convenience: the ticket already carries the sender's direct
/// addresses, so a transfer works even where multicast is blocked.
pub async fn create_endpoint(relay: &RelayChoice) -> Result<Endpoint> {
    let builder = match relay {
        RelayChoice::Lan => Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .net_report_config(NetReportConfig::minimal())
            .portmapper_config(PortmapperConfig::Disabled)
            .address_lookup(MdnsAddressLookup::builder()),
        RelayChoice::N0 => {
            Endpoint::builder(presets::N0).address_lookup(MdnsAddressLookup::builder())
        }
        RelayChoice::Url(url) => Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::custom([url.clone()]))
            .address_lookup(MdnsAddressLookup::builder()),
    };

    builder.bind().await.context("failed to create iroh endpoint")
}

/// Wait (bounded) for the endpoint to reach a relay.
///
/// [`Endpoint::online`] waits for a home relay connection and never completes
/// when relays are disabled or unreachable, so it must always be wrapped in a
/// timeout. Returns `false` on timeout.
pub async fn wait_online(endpoint: &Endpoint, timeout: Duration) -> bool {
    tokio::time::timeout(timeout, endpoint.online()).await.is_ok()
}

/// The direct (IP) addresses this endpoint advertises, e.g. `10.131.183.8:41641`.
pub fn ip_addrs(addr: &EndpointAddr) -> Vec<SocketAddr> {
    addr.ip_addrs().copied().collect()
}

/// Render a comma-separated address list, or a placeholder when empty.
pub fn format_addrs(addrs: &[SocketAddr]) -> String {
    if addrs.is_empty() {
        return "<none>".to_string();
    }
    addrs
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_relay_choices() {
        assert_eq!("none".parse::<RelayChoice>().unwrap(), RelayChoice::Lan);
        assert_eq!("LAN".parse::<RelayChoice>().unwrap(), RelayChoice::Lan);
        assert_eq!("n0".parse::<RelayChoice>().unwrap(), RelayChoice::N0);
        assert!(matches!(
            "https://relay.example.com".parse::<RelayChoice>().unwrap(),
            RelayChoice::Url(_)
        ));
        assert!("not a url".parse::<RelayChoice>().is_err());
    }

    #[test]
    fn lan_is_the_default_and_uses_no_relay() {
        assert_eq!(RelayChoice::default(), RelayChoice::Lan);
        assert!(!RelayChoice::Lan.uses_relay());
        assert!(RelayChoice::N0.uses_relay());
    }
}
