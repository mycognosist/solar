#![recursion_limit = "256"]

use async_std::sync::{Arc, RwLock};
use once_cell::sync::Lazy;

mod actors;
mod broker;
mod cli;
mod config;
mod error;
mod storage;

use broker::*;
use config::ApplicationConfig;
use storage::{blob::BlobStorage, kv::KvStorage};

/// Convenience Result that returns `solar::Error`.
pub type Result<T> = std::result::Result<T, error::Error>;

// Instantiate the key-value store.
pub static KV_STORAGE: Lazy<Arc<RwLock<KvStorage>>> =
    Lazy::new(|| Arc::new(RwLock::new(KvStorage::default())));
// Instantiate the blob store.
pub static BLOB_STORAGE: Lazy<Arc<RwLock<BlobStorage>>> =
    Lazy::new(|| Arc::new(RwLock::new(BlobStorage::default())));

#[async_std::main]
async fn main() -> Result<()> {
    // Initialise the logger.
    env_logger::init();
    log::set_max_level(log::LevelFilter::max());

    // Configure the application.
    let (app_config, kv_storage_config, peer_connections, secret_config) =
        ApplicationConfig::configure().await?;

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
        .open(app_config.blobs_folder, BROKER.lock().await.create_sender());

    // Spawn the ctrlc actor. Listens for SIGINT termination signal.
    Broker::spawn(actors::ctrlc::actor());

    // Print 'starting server' announcement.
    println!(
        "Starting TCP server on {}:{}",
        &app_config.muxrpc_addr,
        base64::encode(&secret_config.pk[..]),
    );

    // Spawn the TCP server. Facilitates peer connections.
    Broker::spawn(actors::tcp_server::actor(
        secret_config.clone(),
        app_config.muxrpc_addr,
    ));

    // Print the network key.
    println!(
        "Node deploy on network: {}",
        hex::encode(app_config.network_key)
    );

    // Spawn the JSON-RPC server if the option has been set to true in the
    // CLI arguments. Facilitates operator queries during runtime.
    if app_config.jsonrpc {
        Broker::spawn(actors::jsonrpc_server::actor(
            secret_config.clone(),
            app_config.jsonrpc_addr,
        ));
    }

    // Spawn the LAN discovery actor. Listens for and broadcasts UDP packets
    // to allow LAN-local peer connections.
    if app_config.lan_discov {
        Broker::spawn(actors::lan_discovery::actor(
            secret_config.clone(),
            app_config.muxrpc_port,
        ));
    }

    // Spawn the peer actor for each set of provided connection parameters.
    // Facilitates replication.
    for (server, port, peer_pk) in peer_connections {
        Broker::spawn(actors::peer::actor(
            secret_config.clone(),
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
