//! Short session codes and LAN discovery.
//!
//! So the receiver does not have to paste a 100+ character ticket, the sender
//! announces a six character session code on the local network: a UDP probe /
//! reply pair on [`BEACON_PORT`] carries a truncated hash of the code plus the
//! sender's endpoint id and direct addresses. The receiver types the code, finds
//! the matching sender and then fetches the ticket over the authenticated
//! `ftrans-meta` ALPN.
//!
//! Only the *hash* of the code is broadcast; the code itself is only ever sent
//! inside the encrypted QUIC connection, so a passive listener on the LAN learns
//! no more than "somebody is offering a session".

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// UDP port used for discovery probes and announcements.
pub const BEACON_PORT: u16 = 53535;

/// How often the sender re-announces itself.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(2);

/// Probe datagram payload. Anything else on the port is ignored.
const PROBE: &[u8] = b"ftrans-discover-v1";

/// Magic string identifying our beacon JSON.
const MAGIC: &str = "ftrans-v1";

/// Code alphabet: digits plus letters, minus the easily confused ones (I, L, O).
const ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Length of a session code.
pub const CODE_LEN: usize = 6;

/// Generate a random session code.
pub fn generate_code() -> String {
    // SecretKey::generate is backed by the OS RNG; we only use its bytes as
    // entropy, the key itself is thrown away.
    let seed = SecretKey::generate().to_bytes();
    (0..CODE_LEN)
        // 256 is an exact multiple of the 32 character alphabet, so the
        // modulo introduces no bias.
        .map(|i| ALPHABET[(seed[i] % ALPHABET.len() as u8) as usize] as char)
        .collect()
}

/// Normalize user input into a session code: case-insensitive, and tolerant of
/// dashes/spaces and of the I/L/O lookalikes.
pub fn normalize_code(input: &str) -> Option<String> {
    let mut out = String::with_capacity(CODE_LEN);
    for ch in input.chars() {
        if ch == '-' || ch == '_' || ch.is_whitespace() {
            continue;
        }
        let mapped = match ch.to_ascii_uppercase() {
            'I' | 'L' => '1',
            'O' => '0',
            other => other,
        };
        if !ALPHABET.contains(&(mapped as u8)) {
            return None;
        }
        out.push(mapped);
    }
    (out.chars().count() == CODE_LEN).then_some(out)
}

/// Truncated hash of a code, safe to broadcast.
pub fn code_tag(code: &str) -> String {
    let hash = iroh_blobs::Hash::new(code.as_bytes());
    hash.as_bytes()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Host name, for telling several senders apart.
pub fn hostname() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

/// What a sender announces on the LAN.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Beacon {
    magic: String,
    /// Truncated hash of the session code.
    pub tag: String,
    /// Sender's endpoint id, as its base32 string form.
    pub id: String,
    /// Sender's direct addresses (`ip:port`), reachable on the local network.
    pub addrs: Vec<String>,
    /// Sender's host name.
    pub host: String,
}

impl Beacon {
    pub fn new(code: &str, id: EndpointId, addrs: &[SocketAddr], host: String) -> Self {
        Self {
            magic: MAGIC.to_string(),
            tag: code_tag(code),
            id: id.to_string(),
            addrs: addrs.iter().map(|a| a.to_string()).collect(),
            host,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Parse a datagram, returning `None` for anything that is not ours.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let beacon: Self = serde_json::from_slice(data).ok()?;
        (beacon.magic == MAGIC).then_some(beacon)
    }

    /// The sender's addresses, parsed.
    pub fn socket_addrs(&self) -> Vec<SocketAddr> {
        self.addrs
            .iter()
            .filter_map(|a| a.parse().ok())
            .collect()
    }
}

/// Announce a session: answer probes on [`BEACON_PORT`] and periodically
/// broadcast the beacon from every local IPv4 interface.
///
/// The broadcast is sent from a socket bound to each interface address
/// explicitly: a socket bound to `0.0.0.0` would send the limited broadcast out
/// of whichever interface has the default route (e.g. the campus Wi-Fi), which
/// is not where the other machine is.
pub async fn announce(beacon: Beacon, local_ips: Vec<IpAddr>) -> Result<()> {
    let payload = beacon.encode().context("failed to encode beacon")?;

    match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, BEACON_PORT)).await {
        Ok(sock) => {
            tokio::spawn(respond_to_probes(sock, payload.clone()));
        }
        Err(e) => {
            eprintln!(
                "warning: cannot listen for discovery probes on udp/{BEACON_PORT} ({e}); \
                 the receiver will need --addr <this-machine-ip>"
            );
        }
    }

    tokio::spawn(broadcast_beacons(payload, local_ips));
    Ok(())
}

async fn respond_to_probes(sock: UdpSocket, payload: Vec<u8>) {
    let mut buf = [0u8; 1024];
    loop {
        match sock.recv_from(&mut buf).await {
            Ok((n, src)) if &buf[..n] == PROBE => {
                let _ = sock.send_to(&payload, src).await;
            }
            // Unrelated datagram on our port: ignore it.
            Ok(_) => {}
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

async fn broadcast_beacons(payload: Vec<u8>, local_ips: Vec<IpAddr>) {
    let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), BEACON_PORT);
    loop {
        for ip in local_ips.iter().filter(|ip| ip.is_ipv4()) {
            if let Ok(sock) = UdpSocket::bind(SocketAddr::new(*ip, 0)).await {
                let _ = sock.set_broadcast(true);
                let _ = sock.send_to(&payload, target).await;
            }
        }
        tokio::time::sleep(ANNOUNCE_INTERVAL).await;
    }
}

/// Look for senders: broadcast a probe from every local interface, and probe
/// `targets` directly (used by `--addr`, which also covers networks where
/// broadcast is filtered).
///
/// Blocks until `timeout` has elapsed, collecting every distinct reply.
pub async fn discover(
    local_ips: &[IpAddr],
    targets: &[IpAddr],
    timeout: Duration,
) -> Vec<Beacon> {
    let deadline = Instant::now() + timeout;
    let (tx, mut rx) = mpsc::channel(32);

    for ip in local_ips.iter().filter(|ip| ip.is_ipv4()) {
        let tx = tx.clone();
        let bind = SocketAddr::new(*ip, 0);
        tokio::spawn(async move {
            let Ok(sock) = UdpSocket::bind(bind).await else {
                return;
            };
            let _ = sock.set_broadcast(true);
            let broadcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), BEACON_PORT);
            let _ = sock.send_to(PROBE, broadcast).await;
            collect_replies(&sock, deadline, tx).await;
        });
    }

    for target in targets {
        let tx = tx.clone();
        let dest = SocketAddr::new(*target, BEACON_PORT);
        tokio::spawn(async move {
            let Ok(sock) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await else {
                return;
            };
            let _ = sock.send_to(PROBE, dest).await;
            collect_replies(&sock, deadline, tx).await;
        });
    }

    drop(tx);
    let mut found: Vec<Beacon> = Vec::new();
    while let Some(beacon) = rx.recv().await {
        if !found.contains(&beacon) {
            found.push(beacon);
        }
    }
    found
}

async fn collect_replies(sock: &UdpSocket, deadline: Instant, tx: mpsc::Sender<Beacon>) {
    let mut buf = [0u8; 2048];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        match tokio::time::timeout(remaining, sock.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if let Some(beacon) = Beacon::decode(&buf[..n]) {
                    if tx.send(beacon).await.is_err() {
                        return;
                    }
                }
            }
            // Timeout, or a socket error: this prober is done.
            _ => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_codes_are_valid() {
        for _ in 0..64 {
            let code = generate_code();
            assert_eq!(code.chars().count(), CODE_LEN);
            assert_eq!(normalize_code(&code).as_deref(), Some(code.as_str()));
        }
    }

    #[test]
    fn normalization_is_forgiving() {
        assert_eq!(normalize_code("4k7m2p").as_deref(), Some("4K7M2P"));
        assert_eq!(normalize_code("4K7-M2P").as_deref(), Some("4K7M2P"));
        assert_eq!(normalize_code(" 4k7 m2p ").as_deref(), Some("4K7M2P"));
        // lookalikes: I/L -> 1, O -> 0
        assert_eq!(normalize_code("4k7m2o").as_deref(), Some("4K7M20"));
        assert_eq!(normalize_code("4k7m2l").as_deref(), Some("4K7M21"));
        // wrong length or characters outside the alphabet
        assert_eq!(normalize_code("4K7M2"), None);
        assert_eq!(normalize_code("4K7M2PQ"), None);
        assert_eq!(normalize_code("4K7M2!"), None);
    }

    #[test]
    fn tag_is_stable_and_short() {
        let tag = code_tag("4K7M2P");
        assert_eq!(tag, code_tag("4K7M2P"));
        assert_ne!(tag, code_tag("4K7M2Q"));
        assert_eq!(tag.len(), 16);
    }

    #[test]
    fn beacon_round_trip() {
        let key = SecretKey::generate();
        let id = key.public();
        let addrs: Vec<SocketAddr> = vec!["192.168.137.1:41641".parse().unwrap()];
        let beacon = Beacon::new("4K7M2P", id, &addrs, "desktop".to_string());

        let encoded = beacon.encode().unwrap();
        let decoded = Beacon::decode(&encoded).unwrap();
        assert_eq!(decoded, beacon);
        assert_eq!(decoded.socket_addrs(), addrs);

        // Foreign datagrams are ignored rather than misinterpreted.
        assert!(Beacon::decode(b"not json").is_none());
        assert!(Beacon::decode(br#"{"magic":"something-else"}"#).is_none());
    }
}
