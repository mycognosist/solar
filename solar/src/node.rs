use async_std::sync::{Arc, RwLock};
use futures::SinkExt;
use once_cell::sync::Lazy;

use crate::{
    actors::{
        jsonrpc,
        network::{
            connection_manager, connection_manager::CONNECTION_MANAGER, lan_discovery,
            secret_handshake, tcp_server,
        },
    },
    broker::*,
    config::ApplicationConfig,
    storage::{blob::BlobStorage, kv::KvStorage},
    Result,
};

// Instantiate the key-value store.
pub static KV_STORE: Lazy<Arc<RwLock<KvStorage>>> =
    Lazy::new(|| Arc::new(RwLock::new(KvStorage::default())));
// Instantiate the blob store.
pub static BLOB_STORE: Lazy<Arc<RwLock<BlobStorage>>> =
    Lazy::new(|| Arc::new(RwLock::new(BlobStorage::default())));

/// Main runtime managing the solar node process.
pub struct Node;

impl Node {
    /// Start the solar node with full storage and networking capabilities.
    pub async fn start(config: ApplicationConfig) -> Result<()> {
        // Configure the application.
        let (app_config, kv_store_config, peer_connections, secret_config) =
            ApplicationConfig::configure(config).await?;

        // Open the key-value store using the given configuration parameters and
        // an unbounded sender channel for message passing.
        KV_STORE
            .write()
            .await
            .open(kv_store_config, BROKER.lock().await.create_sender())?;

        // Open the blobstore using the given folder path and an unbounded sender
        // channel for message passing.
        BLOB_STORE
            .write()
            .await
            .open(app_config.blobs_folder, BROKER.lock().await.create_sender());

        // Spawn the ctrlc actor. Listens for SIGINT termination signal.
        Broker::spawn(crate::actors::ctrlc::actor());

        // Print 'starting server' announcement.
        println!(
            "Starting TCP server on {}:{}",
            &app_config.muxrpc_addr,
            base64::encode(&secret_config.pk[..]),
        );

        // Spawn the TCP server. Facilitates peer connections.
        Broker::spawn(tcp_server::actor(
            secret_config.clone(),
            app_config.muxrpc_addr,
            app_config.selective_replication,
        ));

        // Print the network key.
        println!(
            "Node deploy on network: {}",
            hex::encode(app_config.network_key)
        );

        // Spawn the JSON-RPC server if the option has been set to true in the
        // CLI arguments. Facilitates operator queries during runtime.
        if app_config.jsonrpc {
            Broker::spawn(jsonrpc::server::actor(
                secret_config.clone(),
                app_config.jsonrpc_addr,
            ));
        }

        // Spawn the LAN discovery actor. Listens for and broadcasts UDP packets
        // to allow LAN-local peer connections.
        if app_config.lan_discov {
            Broker::spawn(lan_discovery::actor(
                secret_config.clone(),
                app_config.muxrpc_port,
                app_config.selective_replication,
            ));
        }

        // Spawn the secret handshake actor for each set of provided connection
        // parameters. Facilitates replication.
        for (_url, server, port, peer_pk) in peer_connections {
            Broker::spawn(secret_handshake::actor(
                secret_config.clone(),
                connection_manager::TcpConnection::TcpServer {
                    server,
                    port,
                    peer_pk,
                },
                app_config.selective_replication,
            ));
        }

        // Spawn the connection manager message loop.
        let connection_manager_msgloop = CONNECTION_MANAGER.write().await.take_msgloop();
        connection_manager_msgloop.await;

        // Spawn the broker message loop.
        let broker_msgloop = BROKER.lock().await.take_msgloop();
        broker_msgloop.await;

        println!("Gracefully finished");

        Ok(())
    }

    /// Shutdown the node by sending a termination signal to all actors.
    pub async fn shutdown() {
        // Create a sender channel to pass messages to the broker message loop.
        let mut sender = BROKER.lock().await.create_sender();

        // Send the termination event signal.
        let _ = sender.send(BrokerEvent::Terminate).await;
    }
}
