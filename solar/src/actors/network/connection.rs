use std::{fmt::Display, net::Shutdown};

use async_std::net::TcpStream;
use futures::SinkExt;
use kuska_ssb::{
    crypto::{ed25519, ToSsbId},
    handshake::{
        async_std::{handshake_client, handshake_server},
        HandshakeComplete,
    },
    keystore::OwnedIdentity,
};
use log::info;

use crate::{
    actors::network::connection_manager::{ConnectionEvent, CONNECTION_MANAGER},
    broker::*,
    config::{NETWORK_KEY, PEERS_TO_REPLICATE},
    Result,
};

/// Encapsulate inbound and outbound TCP connections.
pub enum TcpConnection {
    /// An outbound TCP connection.
    Dial {
        /// The address of a remote peer.
        addr: String,
        /// The public key of a remote peer.
        public_key: ed25519::PublicKey,
    },
    /// An inbound TCP connection.
    Listen { stream: TcpStream },
}

/// Stream data.
#[derive(Debug, Clone)]
pub struct StreamData {
    /// Completed secret handshake.
    pub handshake: HandshakeComplete,
    /// TCP stream.
    pub stream: TcpStream,
}

/// Connection data.
#[derive(Debug, Clone)]
pub struct ConnectionData {
    /// Connection identifier.
    pub id: usize,
    /// The address of the remote peer.
    pub peer_addr: Option<String>,
    /// The public key of the remote peer.
    pub peer_public_key: Option<ed25519::PublicKey>,
}

// Custom `Display` implementation so we can easily log connection data in
// the message loop.
impl Display for ConnectionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let peer_addr = match &self.peer_addr {
            Some(addr) => addr.to_string(),
            None => "_".to_string(),
        };

        let peer_public_key = match &self.peer_public_key {
            Some(key) => {
                let ssb_id = key.to_ssb_id();
                if ssb_id.starts_with('@') {
                    ssb_id
                } else {
                    format!("@{}", ssb_id)
                }
            }
            None => "_".to_string(),
        };

        write!(
            f,
            "<Connection {} / {} / {}>",
            &self.id, peer_public_key, peer_addr
        )
    }
}

impl ConnectionData {
    pub fn new(
        id: usize,
        peer_addr: Option<String>,
        peer_public_key: Option<ed25519::PublicKey>,
    ) -> Self {
        ConnectionData {
            id,
            peer_addr,
            peer_public_key,
        }
    }
}

pub async fn actor(
    identity: OwnedIdentity,
    connection: TcpConnection,
    selective_replication: bool,
) -> Result<()> {
    // Register a new connection with the connection manager.
    let connection_id = CONNECTION_MANAGER.write().await.register();

    // Record the data associated with this connection.
    let mut connection_data = ConnectionData::new(connection_id, None, None);

    // Register the "connection" actor endpoint with the broker.
    let ActorEndpoint { mut ch_broker, .. } =
        BROKER.lock().await.register("connection", true).await?;

    // Parse the public key and secret key from the SSB identity.
    //let OwnedIdentity { pk, sk, .. } = identity;

    /*
    // Define the network key to be used for the secret handshake.
    let network_key = NETWORK_KEY.get().unwrap().to_owned();
    */

    // Handle a TCP connection event (inbound or outbound).
    match connection {
        // Handle an outbound TCP connection event.
        TcpConnection::Dial { addr, public_key } => {
            // Update the data associated with this connection.
            connection_data.peer_addr = Some(addr.to_owned());
            connection_data.peer_public_key = Some(public_key);

            // Send 'connecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connecting(
                        connection_data.to_owned(),
                        identity,
                        selective_replication,
                    ),
                ))
                .await?;

            /*

            // Attempt a TCP connection.
            let mut stream = TcpStream::connect(addr).await?;

            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Handshaking(connection_data.to_owned()),
                ))
                .await?;

            // Attempt a secret handshake.
            let handshake =
                handshake_client(&mut stream, network_key.to_owned(), pk, sk, public_key).await?;

            info!("💃 connected to peer {}", handshake.peer_pk.to_ssb_id());

            // Encapsulate the completed handshake and the TCP stream;
            // to be sent to the connection manager.
            let stream_data = StreamData { handshake, stream };

            // Send 'connected' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connected(connection_data.to_owned(), stream_data),
                ))
                .await?;
            */
        }
        // Handle an incoming TCP connection event.
        TcpConnection::Listen { mut stream } => {
            // Retrieve the origin (address) of the incoming connection.
            let peer_addr = stream.peer_addr()?.to_string();

            // Update the data associated with this connection.
            connection_data.peer_addr = Some(peer_addr);
            connection_data.peer_public_key = None;

            // Since the connection has been established, the handshake can
            // now be attempted.

            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Handshaking(
                        connection_data.to_owned(),
                        identity,
                        &mut stream,
                        selective_replication,
                    ),
                ))
                .await?;

            /*
            // Send 'connecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connecting(connection_data.to_owned()),
                ))
                .await?;

            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Handshaking(connection_data.to_owned()),
                ))
                .await?;

            // Attempt a secret handshake.
            let handshake = handshake_server(&mut stream, network_key.to_owned(), pk, sk).await?;

            // Convert the public key to a `String`.
            let ssb_id = handshake.peer_pk.to_ssb_id();

            // Add the sigil link ('@') if it's missing.
            let peer_public_key = if ssb_id.starts_with('@') {
                ssb_id
            } else {
                format!("@{ssb_id}")
            };

            // Update the connection data to include the public key of the
            // remote peer (sourced from the successful handshake data).
            connection_data.peer_public_key = Some(handshake.peer_pk.to_owned());

            // Encapsulate the completed handshake and the TCP stream;
            // to be sent to the connection manager.
            let stream_data = StreamData { handshake, stream };

            // Send 'connected' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connected(connection_data.to_owned(), stream_data),
                ))
                .await?;

            // TODO: Move this check into the connection manager.
            // Specifically, inside the Connected event handler.
            //
            // Check if we are already connected to the selected peer.
            // If yes, return immediately.
            // If no, return the stream and handshake.
            if CONNECTION_MANAGER
                .read()
                .await
                .contains_connected_peer(&handshake.peer_pk)
            {
                info!("peer {} is already connected", &peer_public_key);

                // Since we already have an active connection to this peer,
                // we can disconnect the redundant connection.

                // Send 'disconnecting' connection event message via the broker.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        ConnectionEvent::Disconnecting(connection_data.to_owned()),
                    ))
                    .await?;

                return Ok(connection_data);
            }

            info!("💃 received connection from peer {}", &peer_public_key);

            // Shutdown the connection if the peer is not in the list of peers
            // to be replicated, unless replication is set to nonselective.
            // This ensures we do not replicate with unknown peers.
            if selective_replication
                & !PEERS_TO_REPLICATE
                    .get()
                    .unwrap()
                    .contains_key(&peer_public_key)
            {
                info!(
                    "peer {} is not in replication list and selective replication is enabled; dropping connection",
                    peer_public_key
                );

                // Send connection event message via the broker.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        ConnectionEvent::Disconnecting(connection_data.to_owned()),
                    ))
                    .await?;

                // This may not be necessary; the connection should close when
                // the stream is dropped.
                stream.shutdown(Shutdown::Both)?;

                return Ok(connection_data);
            }

            (stream, handshake)
            */
        }
    };

    /*
    // Attempt a connection and secret handshake.
    let connection_result = actor_inner(
        identity,
        connection,
        connection_data.to_owned(),
        selective_replication,
    )
    .await;

    // Match on the result of the peer connection and replication attempt.
    match connection_result {
        Err(err) => {
            // Send 'error' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Error(connection_data, err.to_string()),
                ))
                .await?;
        }
        Ok(connection_data) => {
            // Send 'disconnected' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Disconnected(connection_data.to_owned()),
                ))
                .await?;
        }
    }
    */

    Ok(())
}

/*

/// Handle a TCP connection with a peer, perform the secret handshake
/// and spawn the replication actor endpoint if the handshake is successful.
pub async fn actor_inner(
    identity: OwnedIdentity,
    connection: TcpConnection,
    mut connection_data: ConnectionData,
    selective_replication: bool,
) -> Result<()> {
    // Register the "connection" actor endpoint with the broker.
    let ActorEndpoint { mut ch_broker, .. } =
        BROKER.lock().await.register("connection", true).await?;

    // Parse the public key and secret key from the SSB identity.
    let OwnedIdentity { pk, sk, .. } = identity;

    // Define the network key to be used for the secret handshake.
    let network_key = NETWORK_KEY.get().unwrap().to_owned();

    // Handle a TCP connection event (inbound or outbound).
    match connection {
        // Handle an outbound TCP connection event.
        TcpConnection::Dial { addr, public_key } => {
            // Update the data associated with this connection.
            connection_data.peer_addr = Some(addr.to_owned());
            connection_data.peer_public_key = Some(public_key);

            // Send 'connecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connecting(connection_data.to_owned()),
                ))
                .await?;

            // Attempt a TCP connection.
            let mut stream = TcpStream::connect(addr).await?;

            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Handshaking(connection_data.to_owned()),
                ))
                .await?;

            // Attempt a secret handshake.
            let handshake =
                handshake_client(&mut stream, network_key.to_owned(), pk, sk, public_key).await?;

            info!("💃 connected to peer {}", handshake.peer_pk.to_ssb_id());

            // Encapsulate the completed handshake and the TCP stream;
            // to be sent to the connection manager.
            let stream_data = StreamData { handshake, stream };

            // Send 'connected' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connected(connection_data.to_owned(), stream_data),
                ))
                .await?;
        }
        // Handle an incoming TCP connection event.
        TcpConnection::Listen { mut stream } => {
            // Retrieve the origin (address) of the incoming connection.
            let peer_addr = stream.peer_addr()?.to_string();

            //let mut connection_data = ConnectionData::new(connection_id, Some(peer_addr), None);
            // Update the data associated with this connection.
            connection_data.peer_addr = Some(peer_addr);
            connection_data.peer_public_key = None;

            // Send 'connecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connecting(connection_data.to_owned()),
                ))
                .await?;

            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Handshaking(connection_data.to_owned()),
                ))
                .await?;

            // Attempt a secret handshake.
            let handshake = handshake_server(&mut stream, network_key.to_owned(), pk, sk).await?;

            // Convert the public key to a `String`.
            let ssb_id = handshake.peer_pk.to_ssb_id();

            // Add the sigil link ('@') if it's missing.
            let peer_public_key = if ssb_id.starts_with('@') {
                ssb_id
            } else {
                format!("@{ssb_id}")
            };

            // Update the connection data to include the public key of the
            // remote peer (sourced from the successful handshake data).
            connection_data.peer_public_key = Some(handshake.peer_pk.to_owned());

            // Encapsulate the completed handshake and the TCP stream;
            // to be sent to the connection manager.
            let stream_data = StreamData { handshake, stream };

            // Send 'connected' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connected(connection_data.to_owned(), stream_data),
                ))
                .await?;

            // TODO: Move this check into the connection manager.
            // Specifically, inside the Connected event handler.
            //
            // Check if we are already connected to the selected peer.
            // If yes, return immediately.
            // If no, return the stream and handshake.
            if CONNECTION_MANAGER
                .read()
                .await
                .contains_connected_peer(&handshake.peer_pk)
            {
                info!("peer {} is already connected", &peer_public_key);

                // Since we already have an active connection to this peer,
                // we can disconnect the redundant connection.

                // Send 'disconnecting' connection event message via the broker.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        ConnectionEvent::Disconnecting(connection_data.to_owned()),
                    ))
                    .await?;

                return Ok(connection_data);
            }

            info!("💃 received connection from peer {}", &peer_public_key);

            // Shutdown the connection if the peer is not in the list of peers
            // to be replicated, unless replication is set to nonselective.
            // This ensures we do not replicate with unknown peers.
            if selective_replication
                & !PEERS_TO_REPLICATE
                    .get()
                    .unwrap()
                    .contains_key(&peer_public_key)
            {
                info!(
                    "peer {} is not in replication list and selective replication is enabled; dropping connection",
                    peer_public_key
                );

                // Send connection event message via the broker.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        ConnectionEvent::Disconnecting(connection_data.to_owned()),
                    ))
                    .await?;

                // This may not be necessary; the connection should close when
                // the stream is dropped.
                stream.shutdown(Shutdown::Both)?;

                return Ok(connection_data);
            }

            (stream, handshake)
        }
    };

    // Parse the peer public key from the handshake.
    let peer_public_key = handshake.peer_pk;

    // TODO: Move this to the connection manager.
    // First try EBT replication and fallback to classic replication
    // if that fails.

    // Spawn the classic replication actor and await the result.
    Broker::spawn(crate::actors::replication::classic::actor(
        connection_data.to_owned(),
        stream.clone(),
        stream,
        handshake,
        peer_public_key,
    ))
    .await;

    Ok(connection_data)
}
*/
