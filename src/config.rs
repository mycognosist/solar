use std::{env, path::PathBuf};

use async_std::{
    fs::File,
    io::{ReadExt, WriteExt},
};
use kuska_ssb::{
    crypto::{ed25519, ToSodiumObject, ToSsbId},
    keystore::OwnedIdentity,
};
use log::{debug, info};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use sled::Config as KvConfig;
use structopt::StructOpt;

use crate::Result;

// Define the IP used for TCP connections (boxstream and MUXRPC).
const MUXRPC_IP: &str = "0.0.0.0";
// Define the port used for TCP connections (boxstream and MUXRPC).
const MUXRPC_PORT: u16 = 8008;
// Define the IP used for the JSON-RPC server.
const JSONRPC_IP: &str = "127.0.0.1";
// Define the port used for the JSON-RPC server.
const JSONRPC_PORT: u16 = 3030;

// Write once store for the list of Scuttlebutt peers to replicate.
pub static REPLICATION_CONFIG: OnceCell<ReplicationConfig> = OnceCell::new();
// Write once store for the database resync configuration.
pub static RESYNC_CONFIG: OnceCell<bool> = OnceCell::new();
// Write-once store for the public-private keypair.
pub static SECRET_CONFIG: OnceCell<SecretConfig> = OnceCell::new();

/// Generate a command line parser.
/// This defines the options that are exposed when running the solar binary.
#[derive(StructOpt, Debug)]
#[structopt(name = "🌞 Solar", about = "Sunbathing scuttlecrabs in kuskaland", version=env!("SOLAR_VERSION"))]
struct Cli {
    /// Where data is stored (default: ~/.local/share/local)
    #[structopt(short, long, parse(from_os_str))]
    data: Option<PathBuf>,

    /// Connect to peers (e.g. host:port:publickey, host:port:publickey)
    #[structopt(short, long)]
    connect: Option<String>,

    // TODO: think about other ways of exposing the "connect" feature
    /// List of peers to replicate; "connect" magic word means that peers
    /// specified with --connect are added to the replication list
    #[structopt(short, long)]
    replicate: Option<String>,

    /// Port to bind (default: 8008)
    #[structopt(short, long)]
    port: Option<u16>,

    /// Run LAN discovery (default: false)
    #[structopt(short, long)]
    lan: Option<bool>,

    /// Run the JSON-RPC server (default: true)
    #[structopt(short, long)]
    jsonrpc: Option<bool>,

    /// Resync the local database by requesting the local feed from peers
    #[structopt(long)]
    resync: Option<bool>,
}

/// Application configuration for solar.
pub struct ApplicationConfig {
    /// Root data directory.
    pub base_path: PathBuf,

    /// Path to the blobstore.
    pub blobs_folder: PathBuf,

    /// Peers to connect to over TCP. Data for each peer connection includes a
    /// server address, port and peer public key.
    pub connect: Option<String>,

    /// Path to the feed store.
    pub feeds_folder: PathBuf,

    /// Run the JSON-RPC server (default: true)
    pub jsonrpc: bool,

    /// JSON-RPC IP and port to bind (default: 127.0.0.1:3030)
    pub jsonrpc_addr: String,

    /// Sled key-value cache capacity.
    pub kv_cache_capacity: u64,

    /// Run LAN discovery (default: false)
    pub lan_discov: bool,

    /// MUXRPC IP and port to bind (default: 0.0.0.0:8008)
    pub muxrpc_addr: String,

    /// MUXRPC port to bind (default: 8008)
    pub muxrpc_port: u16,

    /// List of peers to replicate; "connect" magic word means that peers
    /// specified with --connect are added to the replication list
    pub replicate: Option<String>,

    /// Resync the local database by requesting the local feed from peers
    pub resync: bool,
}

impl ApplicationConfig {
    /// Parse the configuration options provided via the CLI and environment
    /// variables, fall back to defaults when necessary and return the
    /// application configuration.
    pub fn from_cli() -> Result<ApplicationConfig> {
        let cli_args = Cli::from_args();

        // Retrieve application configuration parameters from the parsed CLI input.
        // Set defaults if options have not been provided.
        let lan_discov = cli_args.lan.unwrap_or(false);
        let muxrpc_port = cli_args.port.unwrap_or(MUXRPC_PORT);
        let muxrpc_addr = format!("{}:{}", MUXRPC_IP, muxrpc_port);
        let jsonrpc = cli_args.jsonrpc.unwrap_or(true);
        let resync = cli_args.resync.unwrap_or(false);

        let jsonrpc_ip = match env::var("SOLAR_JSONRPC_IP") {
            Ok(ip) => ip,
            Err(_) => JSONRPC_IP.to_string(),
        };
        let jsonrpc_port = match env::var("SOLAR_JSONRPC_PORT") {
            Ok(port) => port,
            Err(_) => JSONRPC_PORT.to_string(),
        };
        let jsonrpc_addr = format!("{}:{}", jsonrpc_ip, jsonrpc_port);

        // Read KV database cache capacity setting from environment variable.
        // Define default value (1 GB) if env var is unset.
        let kv_cache_capacity: u64 = match env::var("SLED_CACHE_CAPACITY") {
            Ok(val) => val.parse().unwrap_or(1000 * 1000 * 1000),
            Err(_) => 1000 * 1000 * 1000,
        };

        // Create the root data directory for solar.
        // This is the path at which application data is stored, including the
        // public-private keypair, key-value database and blob store.
        let base_path = cli_args
            .data
            .unwrap_or(xdg::BaseDirectories::new()?.create_data_directory("solar")?);

        info!("Base directory is {:?}", base_path);

        let app_config = ApplicationConfig {
            base_path,
            blobs_folder: PathBuf::new(),
            connect: cli_args.connect,
            feeds_folder: PathBuf::new(),
            jsonrpc,
            jsonrpc_addr,
            kv_cache_capacity,
            lan_discov,
            muxrpc_port,
            muxrpc_addr,
            replicate: cli_args.replicate,
            resync,
        };

        Ok(app_config)
    }

    /// Configure the application based on CLI options, environment variables
    /// and defaults.
    pub async fn configure() -> Result<(
        ApplicationConfig,
        KvConfig,
        Vec<(String, u32, ed25519::PublicKey)>,
        OwnedIdentity,
    )> {
        let mut application_config = ApplicationConfig::from_cli()?;

        let mut secret_key_file = application_config.base_path.clone();
        let mut replication_config_file = application_config.base_path.clone();
        let mut feeds_folder = application_config.base_path.clone();
        let mut blobs_folder = application_config.base_path.clone();

        // Define the filename of the secret config file.
        secret_key_file.push("secret.toml");
        // Define the filename of the replication config file.
        replication_config_file.push("replication.toml");
        // Define the directory name for the feed store.
        feeds_folder.push("feeds");
        // Define the directory name for the blob store.
        blobs_folder.push("blobs");
        // Create the feed and blobs directories.
        std::fs::create_dir_all(&feeds_folder)?;
        std::fs::create_dir_all(&blobs_folder)?;

        application_config.blobs_folder = blobs_folder;
        application_config.feeds_folder = feeds_folder;

        // Define configuration parameters for KV database (Sled).
        let kv_storage_config = KvConfig::new()
            .path(&application_config.feeds_folder)
            .cache_capacity(application_config.kv_cache_capacity);

        let mut peer_connections = Vec::new();
        // Parse peer connection details from the provided CLI options.
        // Each instance of `host:port:publickey` is separated from the others
        // and divided into its constituent parts. The tuple of the parts is
        // then pushed to the `connections` vector.
        if let Some(connect) = &application_config.connect {
            for peer in connect.split(',') {
                let invalid_peer_msg = || format!("invalid peer {}", peer);
                let parts = peer.split(':').collect::<Vec<&str>>();
                if parts.len() != 3 {
                    panic!("{}", invalid_peer_msg());
                }
                let server = parts[0].to_string();
                let port = parts[1]
                    .parse::<u32>()
                    .unwrap_or_else(|_| panic!("{}", invalid_peer_msg()));
                let peer_pk = parts[2]
                    .to_ed25519_pk_no_suffix()
                    .unwrap_or_else(|_| panic!("{}", invalid_peer_msg()));
                peer_connections.push((server, port, peer_pk));
            }
        }

        let replication_config = ReplicationConfig::parse_and_update_configuration(
            &peer_connections,
            &application_config.replicate,
            replication_config_file,
        )
        .await?;

        // Log the list of public keys identifying peers whose data will be replicated.
        debug!("peers to be replicated are {:?}", &replication_config.peers);

        let secret_config = SecretConfig::configure(secret_key_file).await?;
        let owned_identity = secret_config.owned_identity()?;

        // Set the value of the secret configuration cell.
        let _err = SECRET_CONFIG.set(secret_config);
        // Set the value of the replication configuration cell.
        let _err = REPLICATION_CONFIG.set(replication_config);
        // Set the value of the resync configuration cell.
        let _err = RESYNC_CONFIG.set(application_config.resync);

        Ok((
            application_config,
            kv_storage_config,
            peer_connections,
            owned_identity,
        ))
    }
}

/// List of peers whose data will be replicated.
#[derive(Default, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Public keys.
    pub peers: Vec<String>,
}

impl ReplicationConfig {
    /// Serialize an instance of `ReplicationConfig` as a TOML byte vector.
    pub fn to_toml(&self) -> Result<Vec<u8>> {
        Ok(toml::to_vec(&self)?)
    }

    /// Deserialize a TOML byte slice into an instance of `ReplicationConfig`.
    pub fn from_toml(s: &[u8]) -> Result<Self> {
        Ok(toml::from_slice::<ReplicationConfig>(s)?)
    }

    /// If the replication config file is not found, generate a new one and
    /// write it to file.
    pub async fn configure(replication_config_file: &PathBuf) -> Result<Self> {
        if !replication_config_file.is_file() {
            println!(
                "Replication configuration file not found, generated new one in {:?}",
                replication_config_file
            );
            let config = ReplicationConfig::default();
            let mut file = File::create(&replication_config_file).await?;
            file.write_all(&config.to_toml()?).await?;
            Ok(config)
        } else {
            // If the config file exists, open it and read the contents.
            let mut file = File::open(&replication_config_file).await?;
            let mut raw: Vec<u8> = Vec::new();
            file.read_to_end(&mut raw).await?;
            ReplicationConfig::from_toml(&raw)
        }
    }

    /// Parse a list of peers to be replicated and peer connections to be
    /// attempted. Write the public keys of the replication peers to file
    /// if they are not already stored there.
    async fn parse_and_update_configuration(
        peer_connections: &Vec<(String, u32, ed25519::PublicKey)>,
        replication_list: &Option<String>,
        replication_config_file: PathBuf,
    ) -> Result<Self> {
        let mut replication_config = ReplicationConfig::configure(&replication_config_file).await?;

        // Parse the list of public keys identifying peers whose data should be
        // replicated. Write the keys to file if they are not stored there already.
        if let Some(peers) = replication_list {
            for peer in peers.split(',') {
                // If `connect` appears in the input, add the public key of each
                // peer specified in the `connect` option to the list of peers to
                // be replicated.
                if peer == "connect" {
                    for conn in peer_connections {
                        let conn_id = format!("@{}", conn.2.to_ssb_id());
                        if !replication_config.peers.contains(&conn_id) {
                            replication_config.peers.push(conn_id);
                        }
                    }
                // Prevent duplicate entries by checking if the given public key
                // is already contained in the config.
                } else if !replication_config.peers.contains(&peer.to_string()) {
                    replication_config.peers.push(peer.to_string())
                }
            }
            let mut file = File::create(replication_config_file).await?;
            file.write_all(&replication_config.to_toml()?).await?;
        }

        Ok(replication_config)
    }
}

/// Public-private keypair.
#[derive(Serialize, Deserialize)]
pub struct SecretConfig {
    /// Public key.
    pub id: String,
    /// Private key.
    pub secret: String,
}

impl SecretConfig {
    /// Generate a new, unique public-private keypair.
    pub fn create() -> Self {
        let OwnedIdentity { id, sk, .. } = OwnedIdentity::create();

        SecretConfig {
            id,
            secret: sk.to_ssb_id(),
        }
    }

    /// Serialize an instance of `SecretConfig` as a TOML byte vector.
    pub fn to_toml(&self) -> Result<Vec<u8>> {
        Ok(toml::to_vec(&self)?)
    }

    /// Deserialize a TOML byte slice into an instance of `SecretConfig`.
    pub fn from_toml(s: &[u8]) -> Result<Self> {
        Ok(toml::from_slice::<SecretConfig>(s)?)
    }

    /// Generate an `OwnedIdentity` from the public-private keypair.
    pub fn owned_identity(&self) -> Result<OwnedIdentity> {
        Ok(OwnedIdentity {
            id: self.id.clone(),
            pk: self.id[1..].to_ed25519_pk()?,
            sk: self.secret.to_ed25519_sk()?,
        })
    }

    /// If the secret config file is not found, generate a new one and write it
    /// to file. This includes the creation of a unique public-private keypair.
    pub async fn configure(secret_key_file: PathBuf) -> Result<Self> {
        if !secret_key_file.is_file() {
            println!(
                "Private key not found, generated new one in {:?}",
                secret_key_file
            );
            let config = SecretConfig::create();
            let mut file = File::create(&secret_key_file).await?;
            file.write_all(&config.to_toml()?).await?;
            Ok(config)
        } else {
            // If the config file exists, open it and read the contents.
            let mut file = File::open(&secret_key_file).await?;
            let mut raw: Vec<u8> = Vec::new();
            file.read_to_end(&mut raw).await?;
            SecretConfig::from_toml(&raw)
        }
    }
}
