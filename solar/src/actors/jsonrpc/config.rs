use std::net::{IpAddr, Ipv4Addr};

#[derive(Debug, Clone)]
pub struct JsonRpcConfig {
    /// Run the JSON-RPC server (default: true).
    pub server: bool,

    /// IP to bind for JSON-RPC server (default: 127.0.0.1).
    pub ip: IpAddr,

    /// Port to bind for JSON-RPC server (default: 3030).
    pub port: u16,
}

impl Default for JsonRpcConfig {
    fn default() -> Self {
        Self {
            server: true,
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 3030,
        }
    }
}
