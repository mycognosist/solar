#![recursion_limit = "256"]

use std::{env, path::PathBuf};

use async_std::{
    fs::File,
    io::ReadExt,
    prelude::*,
    sync::{Arc, RwLock},
};
use log::debug;
use once_cell::sync::{Lazy, OnceCell};
use sled::Config as KvConfig;
use structopt::StructOpt;

// Generate a command line parser.
// This defines the options that are exposed when running the solar binary.
#[derive(StructOpt, Debug)]
#[structopt(name = "🌞 Solar", about = "Sunbathing scuttlecrabs in kuskaland", version=env!("SOLAR_VERSION"))]
struct Opt {
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

mod actors;
mod broker;
mod config;
mod error;
mod storage;

use broker::*;
use config::{ReplicationConfig, SecretConfig};
use kuska_ssb::crypto::{ToSodiumObject, ToSsbId};
use storage::{blob::BlobStorage, kv::KvStorage};

/// Convenience Result that returns `solar::Error`.
pub type Result<T> = std::result::Result<T, error::Error>;

// Define the port used for TCP connections (boxstream and MUXRPC).
const RPC_PORT: u16 = 8008;
// Define the port used for the JSON-RPC server.
const JSON_RPC_PORT: u16 = 3030;

// Instantiate the key-value store.
pub static KV_STORAGE: Lazy<Arc<RwLock<KvStorage>>> =
    Lazy::new(|| Arc::new(RwLock::new(KvStorage::default())));
// Instantiate the blob store.
pub static BLOB_STORAGE: Lazy<Arc<RwLock<BlobStorage>>> =
    Lazy::new(|| Arc::new(RwLock::new(BlobStorage::default())));

// Instantiate the application configuration.
// This includes the public-private keypair.
pub static SECRET_CONFIG: OnceCell<SecretConfig> = OnceCell::new();

// Instantiate the replication configuration.
// This is currently just a list of Scuttlebutt peers to replicate.
pub static REPLICATION_CONFIG: OnceCell<ReplicationConfig> = OnceCell::new();

// Instantiate the database resync configuration.
pub static RESYNC_CONFIG: OnceCell<bool> = OnceCell::new();

#[async_std::main]
async fn main() -> Result<()> {
    // Parse the CLI arguments.
    let opt = Opt::from_args();

    // Retrieve application configuration parameters from the parsed CLI input.
    // Set defaults if options have not been provided.
    let rpc_port = opt.port.unwrap_or(RPC_PORT);
    let listen = format!("0.0.0.0:{}", rpc_port);
    let lan_discovery = opt.lan.unwrap_or(false);
    let jsonrpc_server = opt.jsonrpc.unwrap_or(true);
    let resync = opt.resync.unwrap_or(false);

    // Set the value of the resync configuration cell.
    let _err = RESYNC_CONFIG.set(resync);

    // Initialise the logger.
    env_logger::init();
    log::set_max_level(log::LevelFilter::max());

    // Create the root data directory for solar.
    // This is the path at which application data is stored, including the
    // public-private keypair, key-value database and blob store.
    let base_path = opt
        .data
        .unwrap_or(xdg::BaseDirectories::new()?.create_data_directory("solar")?);

    println!("Base configuration is {:?}", base_path);

    let mut secret_key_file = base_path.clone();
    let mut replication_config_file = base_path.clone();
    let mut feeds_folder = base_path.clone();
    let mut blobs_folder = base_path;

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

    // If the secret config file is not found, generate a new one and write it
    // to file. This includes the creation of a unique public-private keypair.
    let secret_config = if !secret_key_file.is_file() {
        println!(
            "Private key not found, generated new one in {:?}",
            secret_key_file
        );
        let config = SecretConfig::create();
        let mut file = File::create(&secret_key_file).await?;
        file.write_all(&config.to_toml()?).await?;
        config
    } else {
        // If the config file exists, open it and read the contents.
        let mut file = File::open(&secret_key_file).await?;
        let mut raw: Vec<u8> = Vec::new();
        file.read_to_end(&mut raw).await?;
        SecretConfig::from_toml(&raw)?
    };

    // If the replication config file is not found, generate a new one and
    // write it to file.
    let mut replication_config = if !replication_config_file.is_file() {
        println!(
            "Replication configuration file not found, generated new one in {:?}",
            replication_config_file
        );
        let config = ReplicationConfig::create();
        let mut file = File::create(&replication_config_file).await?;
        file.write_all(&config.to_toml()?).await?;
        config
    } else {
        // If the config file exists, open it and read the contents.
        let mut file = File::open(&replication_config_file).await?;
        let mut raw: Vec<u8> = Vec::new();
        file.read_to_end(&mut raw).await?;
        ReplicationConfig::from_toml(&raw)?
    };

    let mut connects = Vec::new();
    // Parse peer connection details from the provided CLI options.
    // Each instance of `host:port:publickey` is separated from the others
    // and divided into its constituent parts. The tuple of the parts is
    // then pushed to the `connects` vector.
    if let Some(connect) = opt.connect {
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
            connects.push((server, port, peer_pk));
        }
    }

    // Parse the list of public keys identifying peers whose data should be
    // replicated. Write the keys to file if they are not stored there already.
    if let Some(peers) = opt.replicate {
        for peer in peers.split(',') {
            // If `connect` appears in the input, add the public key of each
            // peer specified in the `connect` option to the list of peers to
            // be replicated.
            if peer == "connect" {
                for conn in &connects {
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

    // Log the list of public keys idetifying peers whose data will be
    // replicated.
    debug!(target:"solar", "peers to be replicated are {:?}", replication_config.peers);

    // Retrieve the public-private keypair of the local solar node.
    let owned_id = secret_config.owned_identity()?;

    // Set the value of the secret configuration cell.
    let _err = SECRET_CONFIG.set(secret_config);

    // Set the value of the replication configuration cell.
    let _err = REPLICATION_CONFIG.set(replication_config);

    // Print 'server started' announcement.
    println!(
        "Server started on {}:{}",
        listen,
        base64::encode(&owned_id.pk[..])
    );

    // Read KV database cache capacity setting from environment variable.
    // Define default value (1 GB) if env var is unset.
    // TODO: find a neater way to do this. Consider using config file.
    let kv_cache_capacity: u64 = match env::var("SLED_CACHE_CAPACITY") {
        Ok(val) => val.parse().unwrap_or(1000 * 1000 * 1000),
        Err(_) => 1000 * 1000 * 1000,
    };

    // Define configuration parameters for KV database (Sled).
    let kv_storage_config = KvConfig::new()
        .path(&feeds_folder)
        .cache_capacity(kv_cache_capacity);

    // Open the key-value store using the given configuration parameters and
    // an unbounded sender channel for message passing.
    KV_STORAGE
        .write()
        .await
        .open(kv_storage_config, BROKER.lock().await.create_sender())?;

    // Open the blobstore using the given folder path and an unbounded sender
    // channel for message passing.
    BLOB_STORAGE
        .write()
        .await
        .open(blobs_folder, BROKER.lock().await.create_sender());

    // Spawn the ctrlc actor. Listens for SIGINT termination signal.
    Broker::spawn(actors::ctrlc::actor());
    // Spawn the TCP server. Facilitates peer connections.
    Broker::spawn(actors::tcp_server::actor(owned_id.clone(), listen));

    // Spawn the JSON-RPC server if the option has been set to true in the
    // CLI arguments. Facilitates operator queries during runtime.
    if jsonrpc_server {
        Broker::spawn(actors::jsonrpc_server::actor(
            owned_id.clone(),
            JSON_RPC_PORT,
        ));
    }

    // Spawn the LAN discovery actor. Listens for and broadcasts UDP packets
    // to allow LAN-local peer connections.
    if lan_discovery {
        Broker::spawn(actors::lan_discovery::actor(owned_id.clone(), RPC_PORT));
    }

    // Spawn the peer actor for each set of provided connection parameters.
    // Facilitates replication.
    for (server, port, peer_pk) in connects {
        Broker::spawn(actors::peer::actor(
            owned_id.clone(),
            actors::peer::Connect::TcpServer {
                server,
                port,
                peer_pk,
            },
        ));
    }

    // Spawn the broker message loop.
    let msgloop = BROKER.lock().await.take_msgloop();
    msgloop.await;

    println!("Gracefully finished");

    Ok(())
}
