use std::net::{IpAddr, Ipv4Addr};

use kuska_sodiumoxide::crypto::auth::Key as NetworkKey;
use kuska_ssb::{crypto::ed25519::PublicKey, discovery};

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Peer(s) to connect to over TCP. Each entry includes an address
    /// (IP / hostname and port) and the corresponding public key.
    pub connect: Vec<(String, PublicKey)>,

    /// Secret handshake HMAC key (aka. network key, caps key, SHS key).
    pub key: NetworkKey,

    /// Run LAN discovery (default: false).
    pub lan_discovery: bool,

    /// IP to bind for TCP server (default: 0.0.0.0).
    pub ip: IpAddr,

    /// Port to bind for TCP server (default: 8008).
    pub port: u16,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connect: Vec::new(),
            key: discovery::ssb_net_id(),
            lan_discovery: false,
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port: 8008,
        }
    }
}
