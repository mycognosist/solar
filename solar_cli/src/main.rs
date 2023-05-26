use std::{
    convert::{TryFrom, TryInto},
    env,
    path::PathBuf,
};

use clap::{error::ErrorKind as ClapErrorKind, CommandFactory, Parser};
use kuska_sodiumoxide::crypto::auth::Key as NetworkKey;
use kuska_ssb::{crypto::ToSodiumObject, discovery};
use url::Url;

use solar::{ApplicationConfig, JsonRpcConfig, NetworkConfig, Node, ReplicationConfig, Result};

/// Generate a command line parser.
/// This defines the options that are exposed when running the solar binary.
#[derive(Parser, Debug)]
#[command(name = "🌞 Solar", about = "🌞 Solar: Sunbathing scuttlecrabs in kuskaland", version=env!("SOLAR_VERSION"))]
struct Cli {
    /// Directory where data is stored (default: ~/.local/share/local)
    #[arg(short, long)]
    pub data_dir: Option<PathBuf>,

    /// Cache capacity of the key-value database in bytes (default: 1000000000)
    #[arg(long)]
    pub database_cache_capacity: Option<u64>,

    /// Connect to a remote peer by specifying a URL
    /// (e.g. tcp://<host>:<port>?shs=<public key>).
    /// Pass a comma-separated list of URLs to connect to multiple peers
    /// (no spaces)
    #[arg(short, long)]
    pub connect: Option<String>,

    /// IP to bind for TCP server (default: 0.0.0.0)
    #[arg(short, long)]
    pub ip: Option<String>,

    /// Port to bind for TCP server (default: 8008)
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Network key to be used during the secret handshake (aka. SHS key or caps key)
    /// (default: d4a1cb88a66f02f8db635ce26441cc5dac1b08420ceaac230839b755845a9ffb)
    #[arg(short, long)]
    pub network_key: Option<String>,

    /// Run LAN discovery (default: false)
    #[arg(short, long)]
    pub lan: Option<bool>,

    /// Run the JSON-RPC server (default: true)
    #[arg(short, long)]
    pub jsonrpc: Option<bool>,

    /// IP to bind for JSON-RPC server (default: 127.0.0.1)
    #[arg(long)]
    pub jsonrpc_ip: Option<String>,

    /// Port to bind for JSON-RPC server (default: 3030)
    #[arg(long)]
    pub jsonrpc_port: Option<u16>,

    /// Resync the local database by requesting the local feed from peers
    #[arg(long)]
    pub resync: Option<bool>,

    /// Only replicate with peers whose public keys are stored in
    /// `replication.toml` (default: true)
    #[arg(short, long)]
    pub selective: Option<bool>,
}

impl Cli {
    /// Run custom validators on parsed CLI input and return help messages
    /// if errors are found.
    fn validate(self) -> Self {
        // Ensure peer connection URLs are valid.
        if let Some(urls) = self.connect.to_owned() {
            for peer_url in urls.split(',') {
                if let Ok(parsed_url) = Url::parse(peer_url) {
                    if parsed_url.scheme() != "tcp" {
                        // Print a help message about the incorrect scheme and exit.
                        Cli::command()
                            .error(
                                ClapErrorKind::ValueValidation,
                                "URLs passed via '--connect' must start with tcp://",
                            )
                            .exit()
                    }
                    if parsed_url.host().is_none() {
                        // Print a help message about the missing host and exit.
                        Cli::command()
                            .error(
                                ClapErrorKind::ValueValidation,
                                "URLs passed via '--connect' must include an IP or host address",
                            )
                            .exit()
                    }
                    if parsed_url.port().is_none() {
                        // Print a help message about the missing port and exit.
                        Cli::command()
                            .error(
                                ClapErrorKind::ValueValidation,
                                "URLs passed via '--connect' must include a port",
                            )
                            .exit()
                    }
                    if parsed_url.query().is_none() {
                        // Print a help message about the missing query parameter and exit.
                        Cli::command()
                            .error(
                                ClapErrorKind::ValueValidation,
                                "URLs passed via '--connect' must include an 'shs' query parameter and corresponding public key",
                            )
                            .exit()
                    }
                }
            }
        }

        // Ensure the network key is valid.
        if let Some(key) = self.network_key.to_owned() {
            match &hex::decode(key) {
                Ok(decoded_key) => {
                    if NetworkKey::from_slice(decoded_key).is_none() {
                        // Print a help message about the invalid network key and exit.
                        Cli::command()
                            .error(
                                ClapErrorKind::ValueValidation,
                                "network key passed via '--network-key' is invalid; check byte length",
                            )
                            .exit()
                    }
                }
                Err(_) => {
                    // Print a help message about the invalid network key and exit.
                    Cli::command()
                        .error(
                            ClapErrorKind::ValueValidation,
                            "network key passed via '--network-key' must be a valid hex string",
                        )
                        .exit()
                }
            }
        }

        self
    }
}

impl TryFrom<Cli> for ApplicationConfig {
    type Error = solar::Error;

    /// Parse the configuration options provided via the CLI and environment
    /// variables, fall back to defaults when necessary and return the
    /// application configuration.
    fn try_from(cli_args: Cli) -> Result<Self> {
        let mut config = ApplicationConfig::new(cli_args.data_dir)?;

        // Retrieve application configuration parameters from the parsed CLI input.
        // Set defaults if options have not been provided.
        let database_cache_capacity = cli_args.database_cache_capacity.unwrap_or(1_000_000_000);
        let ip = cli_args.ip.unwrap_or("0.0.0.0".to_string());
        let port = cli_args.port.unwrap_or(8008);
        let lan_discovery = cli_args.lan.unwrap_or(false);
        let jsonrpc = cli_args.jsonrpc.unwrap_or(true);
        let jsonrpc_ip = cli_args.jsonrpc_ip.unwrap_or("127.0.0.1".to_string());
        let jsonrpc_port = cli_args.jsonrpc_port.unwrap_or(3030);
        let resync = cli_args.resync.unwrap_or(false);
        let selective = cli_args.selective.unwrap_or(true);

        let network_key = match cli_args.network_key {
            // The key has already been validated so it's safe to unwrap here.
            Some(key) => NetworkKey::from_slice(&hex::decode(key).unwrap()).unwrap(),
            // Use the default network key for the "main" Scuttlebutt network.
            None => discovery::ssb_net_id(),
        };

        // Socket address (IP and port) and public key details for peers to whom
        // a connection will be attempt.
        let mut peer_connections = Vec::new();

        // Parse peer connection details from the provided CLI options.
        // Each URL is separated from the others and divided into its
        // constituent parts. A tuple of the parts is then pushed to the
        // `peer_connections` vector.
        if let Some(connection_data) = cli_args.connect {
            for peer_url in connection_data.split(',') {
                let parsed_url = Url::parse(peer_url)?;

                // Retrieve the host from the URL.
                // We have already validated that host is Some, therefore
                // it's safe to unwrap here. The same is true for port and query.
                let host = parsed_url.host().unwrap().to_string();
                // Retrieve the port from the URL.
                let port = parsed_url.port().unwrap();
                // Create an address from the host and port.
                let addr = format!("{host}:{port}");

                // Retrieve the public key from the URL.
                let shs_query_parameter = parsed_url.query().unwrap();
                // Split the public key from `shs=` (appears at the beginning of
                // the query parameter).
                let (_, public_key) = shs_query_parameter.split_at(4);
                // Format the public key as an `ed25519` hash.
                let public_key_without_suffix = public_key.to_ed25519_pk_no_suffix()?;

                // Push the peer connection details to the vector.
                peer_connections.push((addr, public_key_without_suffix));
            }
        }

        // Define the key-value database cache capacity.
        config.database_cache_capacity = database_cache_capacity;

        // Define the JSON-RPC configuration parameters.
        config.jsonrpc = JsonRpcConfig {
            server: jsonrpc,
            ip: jsonrpc_ip.parse()?,
            port: jsonrpc_port,
        };

        // Define the network configuration parameters.
        config.network = NetworkConfig {
            connect: peer_connections,
            key: network_key,
            lan_discovery,
            ip: ip.parse()?,
            port,
        };

        // Define the replication configuration parameters.
        config.replication = ReplicationConfig {
            resync,
            selective,
            ..ReplicationConfig::default()
        };

        Ok(config)
    }
}

#[async_std::main]
async fn main() {
    // Initialise the logger.
    env_logger::init();
    log::set_max_level(log::LevelFilter::max());

    // Parse command line arguments and run custom validators.
    let cli = Cli::parse().validate();

    // Load configuration parameters and apply defaults.
    let config = cli.try_into().expect("could not load configuration");

    // Start the solar node in async runtime.
    let _node = Node::start(config).await;
}
