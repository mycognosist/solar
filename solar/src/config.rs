use std::{collections::HashMap, path::PathBuf};

use async_std::{
    fs::File,
    io::{ReadExt, WriteExt},
};
use kuska_sodiumoxide::crypto::auth::Key as NetworkKey;
use kuska_ssb::{
    crypto::{ed25519::PublicKey, ToSodiumObject, ToSsbId},
    keystore::OwnedIdentity,
};
use log::debug;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use sled::Config as KvConfig;
use url::Url;

use crate::Result;

// Define the default IP used for TCP connections (boxstream and MUXRPC).
pub const MUXRPC_IP: &str = "0.0.0.0";
// Define the default port used for TCP connections (boxstream and MUXRPC).
pub const MUXRPC_PORT: u16 = 8008;

// Write once store for the network key (aka. SHS key or caps key).
pub static NETWORK_KEY: OnceCell<NetworkKey> = OnceCell::new();
// Write once store for the list of Scuttlebutt peers to replicate.
pub static REPLICATION_CONFIG: OnceCell<ReplicationConfig> = OnceCell::new();
// Write once store for the database resync configuration.
pub static RESYNC_CONFIG: OnceCell<bool> = OnceCell::new();
// Write-once store for the public-private keypair.
pub static SECRET_CONFIG: OnceCell<SecretConfig> = OnceCell::new();

/// Application configuration for solar.
pub struct ApplicationConfig {
    /// Root data directory.
    pub base_path: PathBuf,

    /// Path to the blobstore.
    pub blobs_folder: PathBuf,

    /// Peer(s) to connect to over TCP.
    /// Data includes a URL for each peer connection. Multiple URLs may appear
    /// as a comma-separated list (no spaces).
    pub connect: Option<String>,

    /// Path to the feed store.
    pub feeds_folder: PathBuf,

    /// Run the JSON-RPC server (default: true).
    pub jsonrpc: bool,

    /// JSON-RPC IP and port to bind (default: 127.0.0.1:3030).
    pub jsonrpc_addr: String,

    /// Sled key-value cache capacity.
    pub kv_cache_capacity: u64,

    /// Run LAN discovery (default: false).
    pub lan_discov: bool,

    /// MUXRPC address (default: 0.0.0.0:8008).
    pub muxrpc_addr: String,

    /// MUXRPC IP to bind (default: 0.0.0.0).
    pub muxrpc_ip: String,

    /// MUXRPC port to bind (default: 8008).
    pub muxrpc_port: u16,

    /// Secret handshake HMAC key (aka. network key, caps key, SHS key).
    pub network_key: NetworkKey,

    /// List of peers to replicate; "connect" magic word means that peers
    /// specified with --connect are added to the replication list.
    pub replicate: Option<String>,

    /// Resync the local database by requesting the local feed from peers.
    pub resync: bool,

    /// Deny replication attempts from peers who are not defined in the
    /// replication configuration (default: true).
    pub selective_replication: bool,
}

impl ApplicationConfig {
    /// Configure the application based on CLI options, environment variables
    /// and defaults.
    pub async fn configure(
        mut application_config: ApplicationConfig,
    ) -> Result<(
        ApplicationConfig,
        KvConfig,
        Vec<(Url, String, u16, PublicKey)>,
        OwnedIdentity,
    )> {
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

        // Server, host and public key details for peers to whom a connection
        // will be attempt.
        let mut peer_connections = Vec::new();

        // Parse peer connection details from the provided CLI options.
        // Each URL is separated from the others and divided into its
        // constituent parts. A tuple of the parts is then pushed to the
        // `peer_connections` vector.
        if let Some(connect) = &application_config.connect {
            for peer_url in connect.split(',') {
                let parsed_url = Url::parse(peer_url).map_err(|e| {
                    eprintln!("failed to parse peer connection URL: {}", peer_url);
                    eprintln!("error: {}", e);
                    e
                })?;
                // Retrieve the host from the URL.
                let server = parsed_url
                    .host()
                    .expect("peer connection url is missing host")
                    .to_string();
                // Retrieve the port from the URL.
                let port = parsed_url
                    .port()
                    .expect("peer connection url is missing port");
                // Retrieve the public key from the URL.
                let query_param = parsed_url
                    .query()
                    .expect("peer connection url is missing public key query parameter");
                // Split the public key from `shs=` (appears at the beginning of
                // the query parameter).
                let (_, public_key) = query_param.split_at(4);
                // Format the public key as an `ed25519` hash.
                let peer_pk = public_key.to_ed25519_pk_no_suffix()?;

                // Push the peer connection details to the vector.
                peer_connections.push((parsed_url, server, port, peer_pk));
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

        // Set the value of the network key (aka. secret handshake key or caps key).
        let _err = NETWORK_KEY.set(application_config.network_key.to_owned());
        // Set the value of the replication configuration cell.
        let _err = REPLICATION_CONFIG.set(replication_config);
        // Set the value of the resync configuration cell.
        let _err = RESYNC_CONFIG.set(application_config.resync);
        // Set the value of the secret configuration cell.
        let _err = SECRET_CONFIG.set(secret_config);
        // Set the value of the unfiltered replication cell.
        //let _err = UNFILTERED_REPLICATION.set(application_config.unfiltered_replication);

        Ok((
            application_config,
            kv_storage_config,
            peer_connections,
            owned_identity,
        ))
    }
}

/// List of peers to be replicated.
#[derive(Default, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Peer data. Each entry includes a public key (key) and URL (value).
    /// The URL contains the host, port and public key of the peer's node.
    pub peers: HashMap<String, String>,
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
    /// write it to file. Otherwise, read the list of peer replication data
    /// from the file and return it.
    pub async fn configure(replication_config_file: &PathBuf) -> Result<Self> {
        if !replication_config_file.is_file() {
            println!(
                "Replication configuration file not found, generated new one in {replication_config_file:?}"
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
        peer_connections: &Vec<(Url, String, u16, PublicKey)>,
        replication_list: &Option<String>,
        replication_config_file: PathBuf,
    ) -> Result<Self> {
        let mut replication_config = ReplicationConfig::configure(&replication_config_file).await?;

        // Parse the list of public keys identifying peers whose data should be
        // replicated. Write the keys to file if they are not stored there already.
        if let Some(peers) = replication_list {
            // Split the peer public keys if more than one has been specified.
            for peer in peers.split(',') {
                // Add the key to the peer replication list if isn't already
                // there. A blank `String` stands in place of the peer's URL.
                if !replication_config.peers.contains_key(&peer.to_string()) {
                    replication_config
                        .peers
                        .insert(peer.to_string(), "".to_string())
                        .unwrap();
                }
                // If `connect` appears in the input, add the public key of each
                // peer specified in the `connect` option to the list of peers to
                // be replicated.
                else if peer == "connect" {
                    for conn in peer_connections {
                        // Retrieve and format the public key of the peer from
                        // the connection data.
                        let conn_id = format!("@{}", conn.3.to_ssb_id());
                        // Query the peers HashMap for the public key and URL
                        // matching the given public key.
                        let peer_key_value = replication_config.peers.get_key_value(&conn_id);
                        if let Some((peer_key, peer_url)) = peer_key_value {
                            // Avoid overwriting the URL of the peer if it already
                            // appears in the peer replication list.
                            if peer_url.is_empty() {
                                replication_config
                                    .peers
                                    .insert(peer_key.to_string(), conn.0.to_string())
                                    .unwrap();
                            }
                        } else if peer_key_value.is_none() {
                            // Add the peer public key and URL to the peer
                            // replication list.
                            replication_config
                                .peers
                                .insert(conn_id, conn.0.to_string())
                                .unwrap();
                        }
                    }
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
            println!("Private key not found, generated new one in {secret_key_file:?}");
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
