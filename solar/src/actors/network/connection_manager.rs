use std::{collections::HashSet, net::Shutdown};

use async_std::{
    net::TcpStream,
    sync::{Arc, RwLock},
    task,
    task::JoinHandle,
};
use futures::{select_biased, stream::StreamExt, FutureExt, SinkExt};
use kuska_ssb::{
    crypto::{ed25519, ToSsbId},
    handshake::async_std::{handshake_client, handshake_server},
    keystore::OwnedIdentity,
};
use log::{error, info, trace};
use once_cell::sync::Lazy;

use crate::{
    actors::network::connection::{ConnectionData, StreamData},
    broker::{ActorEndpoint, BrokerEvent, Destination, BROKER},
    config::{NETWORK_KEY, PEERS_TO_REPLICATE},
};

/// The connection manager for the solar node.
pub static CONNECTION_MANAGER: Lazy<Arc<RwLock<ConnectionManager>>> =
    Lazy::new(|| Arc::new(RwLock::new(ConnectionManager::new())));

/// Connection events with associated connection data.
#[derive(Debug)]
pub enum ConnectionEvent<'a> {
    Connecting(ConnectionData, OwnedIdentity, bool),
    Handshaking(ConnectionData, OwnedIdentity, &'a mut TcpStream, bool),
    Connected(ConnectionData, StreamData, bool),
    Replicating(ConnectionData, StreamData, bool),
    Disconnecting(ConnectionData, Option<StreamData>),
    Disconnected(ConnectionData),
    Error(ConnectionData, String),
}

/// Connection manager (broker).
#[derive(Debug)]
pub struct ConnectionManager {
    /// The public keys of all peers to whom we are currently connected.
    pub connected_peers: HashSet<ed25519::PublicKey>,
    /// Idle connection timeout limit.
    pub idle_timeout_limit: u8,
    /// ID number of the most recently registered connection.
    last_connection_id: usize,
    /// Message loop handle.
    msgloop: Option<JoinHandle<()>>,
    // TODO: keep a list of active connections.
    // Then we can query total active connections using `.len()`.
    //active_connections: HashSet<usize>,
}

impl ConnectionManager {
    /// Instantiate a new `ConnectionManager`.
    pub fn new() -> Self {
        // Spawn the connection event message loop.
        let msgloop = task::spawn(Self::msg_loop());

        Self {
            last_connection_id: 0,
            msgloop: Some(msgloop),
            idle_timeout_limit: 30,
            connected_peers: HashSet::new(),
        }
    }

    /// Query the number of active peer connections.
    pub fn _count_connections(&self) -> usize {
        self.connected_peers.len()
    }

    /// Query whether the list of connected peers contains the given peer.
    /// Returns `true` if the peer is in the list, otherwise a `false` value is
    /// returned.
    pub fn contains_connected_peer(&self, peer_id: &ed25519::PublicKey) -> bool {
        self.connected_peers.contains(peer_id)
    }

    /// Add a peer to the list of connected peers.
    /// Returns `true` if the peer was not already in the list, otherwise a
    /// `false` value is returned.
    pub fn insert_connected_peer(&mut self, peer_id: ed25519::PublicKey) -> bool {
        self.connected_peers.insert(peer_id)
    }

    /// Remove a peer from the list of connected peers.
    /// Returns `true` if the peer was in the list, otherwise a `false` value
    /// is returned.
    pub fn remove_connected_peer(&mut self, peer_id: ed25519::PublicKey) -> bool {
        self.connected_peers.remove(&peer_id)
    }

    /// Return a handle for the connection event message loop.
    pub fn take_msgloop(&mut self) -> JoinHandle<()> {
        self.msgloop.take().unwrap()
    }

    /// Register a new connection with the connection manager.
    pub fn register(&mut self) -> usize {
        // Increment the last connection ID value.
        self.last_connection_id += 1;

        trace!(target: "connection-manager", "Registered new connection: {}", self.last_connection_id);

        self.last_connection_id
    }

    /// Start the connection manager event loop.
    ///
    /// Listen for connection event messages via the broker and update
    /// connection state accordingly.
    pub async fn msg_loop() {
        // Register the connection manager actor with the broker.
        let ActorEndpoint {
            ch_terminate,
            mut ch_broker,
            ch_msg,
            actor_id: _,
            ..
        } = BROKER
            .lock()
            .await
            .register("connection-manager", true)
            .await
            .unwrap();

        // Fuse internal termination channel with external channel.
        // This allows termination of the peer loop to be initiated from outside
        // this function.
        let mut ch_terminate_fuse = ch_terminate.fuse();

        let mut broker_msg_ch = ch_msg.unwrap();

        // Listen for connection events via the broker message bus.
        loop {
            select_biased! {
                _value = ch_terminate_fuse => {
                    break;
                },
                msg = broker_msg_ch.next().fuse() => {
                    if let Some(msg) = msg {
                        if let Some(conn_event) = msg.downcast_ref::<ConnectionEvent>() {
                            match conn_event {
                                ConnectionEvent::Connecting(connection_data, identity, selective_replication) => {
                                    trace!(target: "connection-manager", "Connecting: {connection_data}");

                                    // Check if we are already connected to the selected peer.
                                    // If yes, remove this connection.
                                    if let Some(peer_public_key) = &connection_data.peer_public_key {
                                        if CONNECTION_MANAGER
                                            .read()
                                            .await
                                            .contains_connected_peer(peer_public_key)
                                        {
                                            info!("peer {} is already connected", peer_public_key.to_ssb_id());

                                            // Since we already have an active connection to this peer,
                                            // we can disconnect the redundant connection.

                                            // Send 'disconnecting' connection event message via the broker.
                                            ch_broker
                                                .send(BrokerEvent::new(
                                                    Destination::Broadcast,
                                                    ConnectionEvent::Disconnecting(connection_data.to_owned(), None),
                                                ))
                                                // TODO: Handle unwrap.
                                                .await.unwrap();
                                        } else {
                                            if let Some(addr) = &connection_data.peer_addr {
                                            // Attempt connection.
                                            if let Ok(mut stream) = TcpStream::connect(&addr).await {
                                                // Send 'handshaking' connection event message via the broker.
                                                ch_broker
                                                    .send(BrokerEvent::new(
                                                        Destination::Broadcast,
                                                        ConnectionEvent::Handshaking(connection_data.to_owned(), identity.to_owned(), &mut stream, *selective_replication),
                                                    ))
                                                    .await.unwrap();
                                            }
                                        }
                                        }
                                    }
                                }
                                ConnectionEvent::Handshaking(connection_data, identity, stream, selective_replication) => {
                                    trace!(target: "connection-manager", "Handshaking: {connection_data}");

                                    // Parse the public key and secret key from the SSB identity.
                                    let OwnedIdentity { pk, sk, .. } = identity.to_owned();

                                    // Define the network key to be used for the secret handshake.
                                    let network_key = NETWORK_KEY.get().unwrap().to_owned();

                                    // Attempt a secret handshake.
                                    let handshake =
                                        handshake_client(stream, network_key.to_owned(), pk, sk, connection_data.peer_public_key.unwrap()).await.unwrap();

                                    // Encapsulate the completed handshake and the TCP stream;
                                    // to be sent to the connection manager.
                                    let stream_data = StreamData { handshake, stream: stream.to_owned() };

                                    // Send 'connected' connection event message via the broker.
                                    ch_broker
                                        .send(BrokerEvent::new(
                                            Destination::Broadcast,
                                            ConnectionEvent::Connected(connection_data.to_owned(), stream_data, *selective_replication),
                                        ))
                                        .await.unwrap();
                                }
                                ConnectionEvent::Connected(connection_data, stream_data, selective_replication) => {
                                    trace!(target: "connection-manager", "Connected: {connection_data}");

                                    // Add the peer to the list of connected peers.
                                    if let Some(public_key) = connection_data.peer_public_key {
                                        info!("💃 connected to peer {}", public_key.to_ssb_id());

                                        CONNECTION_MANAGER
                                            .write()
                                            .await
                                            .insert_connected_peer(public_key);
                                    }

                                    // Send 'replicating' connection event message via the broker.
                                    ch_broker
                                        .send(BrokerEvent::new(
                                            Destination::Broadcast,
                                            ConnectionEvent::Replicating(*connection_data, *stream_data, *selective_replication),
                                        ))
                                        .await.unwrap();
                                }
                                ConnectionEvent::Replicating(connection_data, stream, selective_replication) => {
                                    trace!(target: "connection-manager", "Replicating: {connection_data}");

                                    // Shutdown the connection if the peer is not in the list of peers
                                    // to be replicated, unless replication is set to nonselective.
                                    // This ensures we do not replicate with unknown peers.
                                    // TODO: Need to find a way to get `selective_replication`
                                    // config value into the loop...
                                    if selective_replication & !PEERS_TO_REPLICATE
                                            .get()
                                            .unwrap()
                                            .contains_key(&connection_data.peer_public_key.unwrap().to_ssb_id())
                                    {
                                        info!(
                                            "peer {} is not in replication list and selective replication is enabled; dropping connection",
                                            connection_data.peer_public_key.unwrap().to_ssb_id()
                                        );

                                        // Send connection event message via the broker.
                                        ch_broker
                                            .send(BrokerEvent::new(
                                                Destination::Broadcast,
                                                ConnectionEvent::Disconnecting(connection_data.to_owned(), Some(*stream)),
                                            ))
                                            .await.unwrap();
                                    } else {
                                        // TODO: Attempt EBT replication, using classic replication
                                        // as fallback.
                                    }
                                }
                                ConnectionEvent::Disconnecting(connection_data, stream_data) => {
                                    trace!(target: "connection-manager", "Disconnecting: {connection_data}");

                                    if let Some(data) = stream_data {
                                        // This may not be necessary; the connection should close when
                                        // the stream is dropped.
                                        data.stream.shutdown(Shutdown::Both).unwrap();
                                    }

                                    ch_broker
                                        .send(BrokerEvent::new(
                                            Destination::Broadcast,
                                            ConnectionEvent::Disconnected(*connection_data),
                                        ))
                                        .await.unwrap();
                                }
                                ConnectionEvent::Disconnected(data) => {
                                    trace!(target: "connection-manager", "Disconnected: {data}");

                                    // Remove the peer from the list of connected peers.
                                    if let Some(public_key) = data.peer_public_key {
                                    CONNECTION_MANAGER
                                        .write()
                                        .await
                                        .remove_connected_peer(public_key);

                                    }
                                }
                                ConnectionEvent::Error(data, err) => {
                                    trace!(target: "connection-manager", "Error: {data}: {err}");

                                    // Remove the peer from the list of connected peers.
                                    if let Some(public_key) = data.peer_public_key {
                                    CONNECTION_MANAGER
                                        .write()
                                        .await
                                        .remove_connected_peer(public_key);

                                    }
                                }
                            }
                        }
                    }
                },
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::{secret_config::SecretConfig, Result};

    // A helper function to instantiate a new connection manager for each test.
    //
    // If we don't use this approach and simply use CONNECTION_MANAGER
    // instead, we end up sharing state across tests (since they all end up
    // operating on the same instance).
    fn instantiate_new_connection_manager() -> Lazy<Arc<RwLock<ConnectionManager>>> {
        Lazy::new(|| Arc::new(RwLock::new(ConnectionManager::new())))
    }

    #[async_std::test]
    async fn test_connection_manager_defaults() -> Result<()> {
        let connection_manager = instantiate_new_connection_manager();

        let connection_idle_timeout_limit = connection_manager.read().await.idle_timeout_limit;
        assert_eq!(connection_idle_timeout_limit, 30);

        let last_connection_id = connection_manager.read().await.last_connection_id;
        assert_eq!(last_connection_id, 0);

        let msgloop = &connection_manager.read().await.msgloop;
        assert!(msgloop.is_some());

        let connected_peers = &connection_manager.read().await.connected_peers;
        assert!(connected_peers.is_empty());

        Ok(())
    }

    #[async_std::test]
    async fn test_register_new_connection() -> Result<()> {
        let connection_manager = instantiate_new_connection_manager();

        for i in 1..=4 {
            // Register a new connection.
            let connection_id = connection_manager.write().await.register();

            // Ensure the connection ID is incremented for each new connection.
            assert_eq!(connection_id, i as usize);
        }

        Ok(())
    }

    #[async_std::test]
    async fn test_count_connections() -> Result<()> {
        let connection_manager = instantiate_new_connection_manager();

        let active_connections = connection_manager.read().await._count_connections();
        assert_eq!(active_connections, 0);

        Ok(())
    }

    #[async_std::test]
    async fn test_connected_peers() -> Result<()> {
        let connection_manager = instantiate_new_connection_manager();

        // Create a unique keypair to sign messages.
        let keypair = SecretConfig::create().to_owned_identity().unwrap();

        // Insert a new connected peer.
        let insert_result = connection_manager
            .write()
            .await
            .insert_connected_peer(keypair.pk);
        assert_eq!(insert_result, true);

        // Query the list of connected peers.
        let query_result = connection_manager
            .read()
            .await
            .contains_connected_peer(&keypair.pk);
        assert_eq!(query_result, true);

        // Attempt to insert the same peer ID for a second time.
        let reinsert_result = connection_manager
            .write()
            .await
            .insert_connected_peer(keypair.pk);
        assert_eq!(reinsert_result, false);

        // Count the active connections.
        let connections = connection_manager.read().await._count_connections();
        assert_eq!(connections, 1);

        // Remove a peer from the list of connected peers.
        let remove_result = connection_manager
            .write()
            .await
            .remove_connected_peer(keypair.pk);
        assert_eq!(remove_result, true);

        // Count the active connections.
        let conns = connection_manager.read().await._count_connections();
        assert_eq!(conns, 0);

        Ok(())
    }
}
