//! Event-driven connection manager.
//!
//! The connection manager serves to register new TCP connections and handle
//! each connection as it passes through several phases (eg. connection
//! establishment, handshake, replication etc.).
//!
//! An event loop listens for connection events and executes handlers for each.
//!
//! Connection data, including the underlying TCP stream, is passed around with
//! each event variant - allowing the handlers to take ownership of the data.

use std::net::Shutdown;

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
use log::{debug, error, info, trace};
use once_cell::sync::Lazy;

use crate::{
    actors::{
        network::{
            connection,
            connection::{ConnectionData, TcpConnection},
        },
        replication::ebt::EbtEvent,
    },
    broker::{
        ActorEndpoint, Broker, BrokerEvent, BrokerMessage, ChBrokerSend, Destination, BROKER,
    },
    config::{NETWORK_KEY, PEERS_TO_REPLICATE},
    error::Error,
    Result,
};

/// The connection manager for the solar node.
pub static CONNECTION_MANAGER: Lazy<Arc<RwLock<ConnectionManager>>> =
    Lazy::new(|| Arc::new(RwLock::new(ConnectionManager::new())));

type EnableSelectiveReplication = bool;
type IsListener = bool;

/// Connection events with associated connection data.
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    LanDiscovery(TcpConnection, OwnedIdentity, EnableSelectiveReplication),
    Staging(ConnectionData, OwnedIdentity, EnableSelectiveReplication),
    Connecting(ConnectionData, OwnedIdentity, EnableSelectiveReplication),
    Handshaking(
        ConnectionData,
        OwnedIdentity,
        EnableSelectiveReplication,
        IsListener,
    ),
    Connected(ConnectionData, EnableSelectiveReplication, IsListener),
    Replicate(ConnectionData, EnableSelectiveReplication, IsListener),
    ReplicatingEbt(ConnectionData, IsListener),
    ReplicatingClassic(ConnectionData),
    Disconnecting(ConnectionData),
    Disconnected(ConnectionData),
    Error(ConnectionData, String),
}

/// Connection manager (broker).
#[derive(Debug)]
pub struct ConnectionManager {
    /// The public keys of all peers to whom we are currently connected.
    pub connected_peers: Vec<(ed25519::PublicKey, usize)>,
    /// The public keys of all peers to whom we are currently attempting a
    /// connection
    pub connecting_peers: Vec<(ed25519::PublicKey, usize)>,
    /// Idle connection timeout limit.
    pub idle_timeout_limit: u8,
    /// ID number of the most recently registered connection.
    last_connection_id: usize,
    /// Message loop handle.
    msgloop: Option<JoinHandle<()>>,
}

impl ConnectionManager {
    /// Instantiate a new `ConnectionManager`.
    pub fn new() -> Self {
        // Spawn the connection event message loop.
        let msgloop = task::spawn(Self::msg_loop());

        Self {
            connected_peers: Vec::new(),
            connecting_peers: Vec::new(),
            idle_timeout_limit: 30,
            last_connection_id: 0,
            msgloop: Some(msgloop),
        }
    }

    /// Query the number of active peer connections.
    fn _count_connections(&self) -> usize {
        self.connected_peers.len()
    }

    /// Query whether the list of connected peers contains the given peer.
    /// Returns `true` if the peer is in the list, otherwise a `false` value is
    /// returned.
    pub fn contains_connected_peer(&self, peer_id: &ed25519::PublicKey) -> bool {
        self.connected_peers
            .iter()
            .any(|(connected_peer_id, _)| connected_peer_id == peer_id)
    }

    /// Add a peer to the list of connected peers.
    /// Returns `true` if the peer was not already in the list, otherwise a
    /// `false` value is returned.
    fn insert_connected_peer(&mut self, peer_id: ed25519::PublicKey, connection_id: usize) {
        self.connected_peers.push((peer_id, connection_id));
    }

    /// Remove a peer from the list of connected peers.
    /// Returns `true` if the peer was in the list, otherwise a `false` value
    /// is returned.
    fn remove_connected_peer(&mut self, peer_id: ed25519::PublicKey, connection_id: usize) {
        if let Some(index) = self
            .connected_peers
            .iter()
            .position(|&entry| entry == (peer_id, connection_id))
        {
            // Ignore the return value.
            let _ = self.connected_peers.remove(index);
        }
    }

    /// Query whether the list of connecting peers contains the given peer.
    /// Returns `true` if the peer is in the list, otherwise a `false` value is
    /// returned.
    pub fn contains_connecting_peer(&self, peer_id: &ed25519::PublicKey) -> bool {
        self.connecting_peers
            .iter()
            .any(|(connecting_peer_id, _)| connecting_peer_id == peer_id)
    }

    /// Add a peer to the list of connecting peers.
    /// Returns `true` if the peer was not already in the list, otherwise a
    /// `false` value is returned.
    fn insert_connecting_peer(&mut self, peer_id: ed25519::PublicKey, connection_id: usize) {
        self.connecting_peers.push((peer_id, connection_id))
    }

    /// Remove a peer from the list of connecting peers.
    /// Returns `true` if the peer was in the list, otherwise a `false` value
    /// is returned.
    fn remove_connecting_peer(&mut self, peer_id: ed25519::PublicKey, connection_id: usize) {
        if let Some(index) = self
            .connecting_peers
            .iter()
            .position(|&entry| entry == (peer_id, connection_id))
        {
            // Ignore the return value.
            let _ = self.connecting_peers.remove(index);
        }
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

    /// Handle a LAN discovery event.
    async fn handle_lan_discovery(
        tcp_connection: TcpConnection,
        identity: OwnedIdentity,
        selective_replication: EnableSelectiveReplication,
    ) -> Result<()> {
        // First ensure there is no active or in-progress connection
        // with the given peer.
        if let TcpConnection::Dial { public_key, .. } = &tcp_connection {
            if !CONNECTION_MANAGER
                .read()
                .await
                .contains_connected_peer(public_key)
                && !CONNECTION_MANAGER
                    .read()
                    .await
                    .contains_connecting_peer(public_key)
            {
                // Spawn a connection actor with the given connection parameters.
                //
                // The connection actor is responsible for initiating the
                // outbound TCP connection.
                Broker::spawn(connection::actor(
                    tcp_connection,
                    identity,
                    selective_replication,
                ));
            }
        }

        Ok(())
    }

    /// Handle a staging event.
    async fn handle_staging(
        connection_data: ConnectionData,
        identity: OwnedIdentity,
        selective_replication: EnableSelectiveReplication,
        mut ch_broker: ChBrokerSend,
    ) -> Result<()> {
        if let Some(peer_public_key) = &connection_data.peer_public_key {
            // Only proceed with a connection attempt if there is not an
            // active connection or connection attempt in-progress.
            if !CONNECTION_MANAGER
                .read()
                .await
                .contains_connected_peer(peer_public_key)
                && !CONNECTION_MANAGER
                    .read()
                    .await
                    .contains_connecting_peer(peer_public_key)
            {
                // If no connection or connection attempt exists, initiate
                // the connection process.

                // Send 'connecting' connection event message via the broker.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        BrokerMessage::Connection(ConnectionEvent::Connecting(
                            connection_data,
                            identity,
                            selective_replication,
                        )),
                    ))
                    .await?;
            }
        }

        Ok(())
    }

    /// Handle a connecting event.
    async fn handle_connecting(
        mut connection_data: ConnectionData,
        identity: OwnedIdentity,
        selective_replication: EnableSelectiveReplication,
        mut ch_broker: ChBrokerSend,
    ) -> Result<()> {
        if let Some(peer_public_key) = &connection_data.peer_public_key {
            if let Some(peer_addr) = &connection_data.peer_addr {
                CONNECTION_MANAGER
                    .write()
                    .await
                    .insert_connecting_peer(*peer_public_key, connection_data.id);

                // Attempt connection.
                if let Ok(stream) = TcpStream::connect(&peer_addr).await {
                    connection_data.stream = Some(stream);

                    // Send 'handshaking' connection event message via the broker.
                    ch_broker
                        .send(BrokerEvent::new(
                            Destination::Broadcast,
                            BrokerMessage::Connection(ConnectionEvent::Handshaking(
                                connection_data,
                                identity,
                                selective_replication,
                                false, // Dialer.
                            )),
                        ))
                        .await?;
                } else {
                    // If the connection attempt fails, send 'disconnecting'
                    // connection event message via the broker.
                    //
                    // This removes the connection from the list of in-progress
                    // attempts, ensuring that future connection attempts to
                    // this peer are not blocked.
                    ch_broker
                        .send(BrokerEvent::new(
                            Destination::Broadcast,
                            BrokerMessage::Connection(ConnectionEvent::Disconnecting(
                                connection_data,
                            )),
                        ))
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Handle a handshaking event.
    async fn handle_handshaking(
        mut connection_data: ConnectionData,
        identity: OwnedIdentity,
        selective_replication: EnableSelectiveReplication,
        listener: IsListener,
        mut ch_broker: ChBrokerSend,
    ) -> Result<()> {
        // Parse the public key and secret key from the SSB identity.
        let OwnedIdentity { pk, sk, .. } = identity;

        // Define the network key to be used for the secret handshake.
        let network_key = NETWORK_KEY.get().ok_or(Error::OptionIsNone)?.to_owned();
        let mut stream = connection_data.stream.clone().ok_or(Error::OptionIsNone)?;

        // Attempt a secret handshake as server or client.
        let handshake = if listener {
            debug!("Attempting secret handshake as server...");
            handshake_server(&mut stream, network_key, pk, sk).await?
        } else {
            let peer_public_key = connection_data.peer_public_key.ok_or(Error::OptionIsNone)?;
            debug!("Attempting secret handshake as client...");
            handshake_client(&mut stream, network_key, pk, sk, peer_public_key).await?
        };

        debug!("Secret handshake complete");

        // `handshake.peer_pk` is of type `ed25519::PublicKey`.
        connection_data.peer_public_key = Some(handshake.peer_pk);
        connection_data.handshake = Some(handshake);

        // Send 'connected' connection event message via the broker.
        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                BrokerMessage::Connection(ConnectionEvent::Connected(
                    connection_data,
                    selective_replication,
                    listener,
                )),
            ))
            .await?;

        Ok(())
    }

    /// Handle a connected event.
    async fn handle_connected(
        connection_data: ConnectionData,
        selective_replication: EnableSelectiveReplication,
        listener: IsListener,
        mut ch_broker: ChBrokerSend,
    ) -> Result<()> {
        // Add the peer to the list of connected peers.
        if let Some(public_key) = connection_data.peer_public_key {
            info!("💃 connected to peer {}", public_key.to_ssb_id());

            CONNECTION_MANAGER
                .write()
                .await
                .remove_connecting_peer(public_key, connection_data.id);

            CONNECTION_MANAGER
                .write()
                .await
                .insert_connected_peer(public_key, connection_data.id);
        }

        // Send 'replicate' connection event message via the broker.
        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                BrokerMessage::Connection(ConnectionEvent::Replicate(
                    connection_data,
                    selective_replication,
                    listener,
                )),
            ))
            .await?;

        Ok(())
    }

    /// Handle a replicate event.
    async fn handle_replicate(
        connection_data: ConnectionData,
        selective_replication: EnableSelectiveReplication,
        listener: IsListener,
        mut ch_broker: ChBrokerSend,
    ) -> Result<()> {
        let peer_public_key = connection_data
            .peer_public_key
            .ok_or(Error::OptionIsNone)?
            .to_ssb_id();

        // Shutdown the connection if the peer is not in the list of peers
        // to be replicated, unless replication is set to nonselective.
        // This ensures we do not replicate with unknown peers.
        if selective_replication
            & !PEERS_TO_REPLICATE
                .get()
                .ok_or(Error::OptionIsNone)?
                .contains_key(&peer_public_key)
        {
            info!(
                "peer {} is not in replication list and selective replication is enabled; dropping connection",
                peer_public_key
            );

            // Send 'disconnecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Connection(ConnectionEvent::Disconnecting(connection_data)),
                ))
                .await?;
        } else {
            // Send 'replicating ebt' connection event message via the broker.
            //
            // If the EBT replication attempt is unsuccessful, the EBT replication
            // actor will emit a `ConnectionEvent::ReplicatingClassic` message.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Connection(ConnectionEvent::ReplicatingEbt(
                        connection_data,
                        listener,
                    )),
                ))
                .await?;
        }

        Ok(())
    }

    /// Handle a classic replication (`create_history_stream`) event.
    async fn handle_replicating_classic(connection_data: ConnectionData) -> Result<()> {
        debug!("Attempting classic replication with peer...");

        // Spawn the classic replication actor and await the result.
        Broker::spawn(crate::actors::replication::classic::actor(connection_data)).await;

        Ok(())
    }

    /// Handle an EBT replication event.
    async fn handle_replicating_ebt(
        connection_data: ConnectionData,
        listener: IsListener,
        mut ch_broker: ChBrokerSend,
    ) -> Result<()> {
        debug!("Attempting EBT replication with peer...");

        // The listener (aka. responder or server) waits for an EBT session to
        // be requested by the client (aka. requestor or dialer).
        if listener {
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Ebt(EbtEvent::WaitForSessionRequest(connection_data)),
                ))
                .await?;
        } else {
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Ebt(EbtEvent::RequestSession(connection_data)),
                ))
                .await?;
        }

        Ok(())
    }

    /// Handle a disconnecting event.
    async fn handle_disconnecting(
        connection_data: ConnectionData,
        mut ch_broker: ChBrokerSend,
    ) -> Result<()> {
        if let Some(stream) = &connection_data.stream {
            // This may not be necessary; the connection should close when
            // the stream is dropped.
            stream.shutdown(Shutdown::Both)?;
        }

        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                BrokerMessage::Connection(ConnectionEvent::Disconnected(connection_data)),
            ))
            .await?;

        Ok(())
    }

    /// Handle a disconnected event.
    async fn handle_disconnected(connection_data: ConnectionData) -> Result<()> {
        if let Some(public_key) = connection_data.peer_public_key {
            CONNECTION_MANAGER
                .write()
                .await
                .remove_connected_peer(public_key, connection_data.id);

            CONNECTION_MANAGER
                .write()
                .await
                .remove_connecting_peer(public_key, connection_data.id);
        }

        Ok(())
    }

    /// Start the connection manager event loop.
    ///
    /// Listen for connection event messages via the broker and update
    /// connection state accordingly.
    async fn msg_loop() {
        // Register the connection manager actor with the broker.
        let ActorEndpoint {
            ch_terminate,
            ch_broker,
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
                    if let Some(BrokerMessage::Connection(event)) = msg {
                        match event {
                            ConnectionEvent::LanDiscovery(tcp_connection, identity, selective_replication) => {
                                if let Err(err) = ConnectionManager::handle_lan_discovery(
                                    tcp_connection,
                                    identity,
                                    selective_replication,
                                ).await {
                                    error!("Error while handling 'lan discovery' event: {}", err)
                                }
                            }
                            ConnectionEvent::Staging(connection_data, identity, selective_replication,) => {
                                trace!(target: "connection-manager", "Staging: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_staging(
                                    connection_data,
                                    identity,
                                    selective_replication,
                                    ch_broker.clone()
                                ).await {
                                    error!("Error while handling 'staging' event: {}", err)
                                }
                            }
                            ConnectionEvent::Connecting(connection_data, identity, selective_replication,) => {
                                trace!(target: "connection-manager", "Connecting: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_connecting(
                                    connection_data,
                                    identity,
                                    selective_replication,
                                    ch_broker.clone()
                                ).await {
                                    error!("Error while handling 'connecting' event: {}", err)
                                }
                            }
                            ConnectionEvent::Handshaking(connection_data, identity, selective_replication, listener) => {
                                trace!(target: "connection-manager", "Handshaking: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_handshaking(
                                    connection_data,
                                    identity,
                                    selective_replication,
                                    listener,
                                    ch_broker.clone()
                                ).await {
                                    error!("Error while handling 'handshaking' event: {}", err)
                                }
                            }
                            ConnectionEvent::Connected(connection_data, selective_replication, listener) => {
                                trace!(target: "connection-manager", "Connected: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_connected(
                                    connection_data,
                                    selective_replication,
                                    listener,
                                    ch_broker.clone()
                                ).await {
                                    error!("Error while handling 'connected' event: {}", err)
                                }
                            }
                            ConnectionEvent::Replicate(connection_data, selective_replication, listener) => {
                                trace!(target: "connection-manager", "Replicate: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_replicate(
                                    connection_data,
                                    selective_replication,
                                    listener,
                                    ch_broker.clone()
                                ).await {
                                    error!("Error while handling 'replicate' event: {}", err)
                                }
                            }
                            ConnectionEvent::ReplicatingClassic(connection_data) => {
                                trace!(target: "connection-manager", "Replicating classic: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_replicating_classic(
                                    connection_data,
                                ).await {
                                    error!("Error while handling 'replicating classic' event: {}", err)
                                }
                            }
                            ConnectionEvent::ReplicatingEbt(connection_data, listener) => {
                                trace!(target: "connection-manager", "Replicating EBT: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_replicating_ebt(
                                    connection_data,
                                    listener,
                                    ch_broker.clone()
                                ).await {
                                    error!("Error while handling 'replicating EBT' event: {}", err)
                                }
                            }
                            ConnectionEvent::Disconnecting(connection_data) => {
                                trace!(target: "connection-manager", "Disconnecting: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_disconnecting(
                                    connection_data,
                                    ch_broker.clone()
                                ).await {
                                    error!("Error while handling 'disconnecting' event: {}", err)
                                }
                            }
                            ConnectionEvent::Disconnected(connection_data) => {
                                trace!(target: "connection-manager", "Disconnected: {connection_data}");

                                if let Err(err) = ConnectionManager::handle_disconnected(
                                    connection_data,
                                ).await {
                                    error!("Error while handling 'disconnected' event: {}", err)
                                }
                            }
                            ConnectionEvent::Error(connection_data, err) => {
                                trace!(target: "connection-manager", "Error: {connection_data}: {err}");
                                error!("Connection error: {connection_data}: {err}");

                                if let Err(err) = ConnectionManager::handle_disconnected(
                                    connection_data,
                                ).await {
                                    error!("Error while handling 'disconnected' event: {}", err)
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
