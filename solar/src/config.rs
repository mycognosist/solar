use std::{collections::HashMap, path::PathBuf};

use kuska_sodiumoxide::crypto::auth::Key as NetworkKey;
use kuska_ssb::crypto::ed25519::PublicKey;
use log::{debug, info};
use once_cell::sync::OnceCell;
use sled::Config as DatabaseConfig;
use xdg::BaseDirectories;

use crate::{
    actors::{
        jsonrpc::config::JsonRpcConfig, network::config::NetworkConfig,
        replication::config::ReplicationConfig,
    },
    secret_config::SecretConfig,
    Result,
};

// Write once store for the network key (aka. SHS key or caps key).
pub static NETWORK_KEY: OnceCell<NetworkKey> = OnceCell::new();
// Write once store for the list of Scuttlebutt peers to replicate.
pub static PEERS_TO_REPLICATE: OnceCell<HashMap<PublicKey, String>> = OnceCell::new();
// Write once store for the database resync configuration.
pub static RESYNC_CONFIG: OnceCell<bool> = OnceCell::new();
// Write-once store for the public-private keypair.
pub static SECRET_CONFIG: OnceCell<SecretConfig> = OnceCell::new();

/// Application configuration for solar.
#[derive(Debug, Default, Clone)]
pub struct ApplicationConfig {
    /// Root data directory.
    pub base_path: Option<PathBuf>,

    /// Sled key-value database configuration.
    pub database: DatabaseConfig,

    /// Sled key-value cache capacity.
    pub database_cache_capacity: u64,

    /// JSON-RPC configuration.
    pub jsonrpc: JsonRpcConfig,

    /// Network configuration.
    pub network: NetworkConfig,

    /// Replication configuration.
    pub replication: ReplicationConfig,

    /// Public-private keypair configuration.
    pub secret: SecretConfig,
}

impl ApplicationConfig {
    /// Create the root data directory for solar, along with the feed and blob
    /// directories. This is where all application data is stored, including
    /// the public-private keypair, key-value database and blob store.
    fn create_data_directories(path: Option<PathBuf>) -> Result<(PathBuf, PathBuf)> {
        let base_path = path.unwrap_or(BaseDirectories::new()?.create_data_directory("solar")?);

        // Define the directory name for the feed store.
        let feeds_path = base_path.join("feeds");
        // Define the directory name for the blob store.
        let blobs_path = base_path.join("blobs");

        // Create the feed and blobs directories.
        std::fs::create_dir_all(&feeds_path)?;
        std::fs::create_dir_all(blobs_path)?;

        Ok((base_path, feeds_path))
    }

    /// Configure the application based on CLI options, environment variables
    /// and defaults.
    pub fn new(path: Option<PathBuf>) -> Result<Self> {
        // Create the application data directories if they don't already exist.
        let (base_path, feeds_path) = Self::create_data_directories(path)?;

        info!("Base directory is {:?}", base_path);

        let mut config = ApplicationConfig::default();

        config.database = config.database.path(feeds_path);
        config.replication = ReplicationConfig::return_or_create_file(&base_path)?;
        config.secret = SecretConfig::return_or_create_file(&base_path)?;
        config.base_path = Some(base_path);

        // Log the list of public keys identifying peers whose data will be replicated.
        debug!("Peers to be replicated are {:?}", &config.replication.peers);

        // Set the value of the network key (aka. secret handshake key or caps key).
        let _err = NETWORK_KEY.set(config.network.key.to_owned());
        // Set the value of the peers to replicate cell.
        let _err = PEERS_TO_REPLICATE.set(config.replication.peers.to_owned());
        // Set the value of the resync configuration cell.
        let _err = RESYNC_CONFIG.set(config.replication.resync);
        // Set the value of the secret configuration cell.
        let _err = SECRET_CONFIG.set(config.secret.to_owned());

        Ok(config)
    }
}
