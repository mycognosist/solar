use std::net::Shutdown;

use async_std::net::TcpStream;
use futures::SinkExt;
use kuska_ssb::{
    crypto::ToSsbId,
    handshake::async_std::{handshake_client, handshake_server},
    keystore::OwnedIdentity,
};
use log::{info, warn};

use crate::{
    actors::network::connection_manager::{ConnectionEvent, TcpConnection, CONNECTION_MANAGER},
    broker::*,
    config::{NETWORK_KEY, REPLICATION_CONFIG},
    Result,
};

pub async fn actor(
    identity: OwnedIdentity,
    connection: TcpConnection,
    selective_replication: bool,
) -> Result<()> {
    // Register a new connection with the connection manager.
    let connection_id = CONNECTION_MANAGER.write().await.register();

    // Catch any errors which occur during the peer connection and replication.
    if let Err(err) = actor_inner(identity, connection, connection_id, selective_replication).await
    {
        warn!("peer failed: {:?}", err);

        let mut ch_broker = BROKER.lock().await.create_sender();

        // Send 'error' connection event message via the broker.
        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                ConnectionEvent::Error(connection_id, err.to_string()),
            ))
            .await
            .unwrap();
    }

    Ok(())
}

/// Perform the secret handshake with a connected peer and spawn the
/// replication actor endpoint if the handshake is successful.
pub async fn actor_inner(
    identity: OwnedIdentity,
    connection: TcpConnection,
    connection_id: usize,
    selective_replication: bool,
) -> Result<usize> {
    // Register the "secret-handshake" actor endpoint with the broker.
    let ActorEndpoint { mut ch_broker, .. } = BROKER
        .lock()
        .await
        .register("secret-handshake", true)
        .await?;

    // Parse the public key and secret key from the SSB identity.
    let OwnedIdentity { pk, sk, .. } = identity;

    // Define the network key to be used for the secret handshake.
    let network_key = NETWORK_KEY.get().unwrap().to_owned();

    // Send 'connecting' connection event message via the broker.
    ch_broker
        .send(BrokerEvent::new(
            Destination::Broadcast,
            ConnectionEvent::Connecting(connection_id),
        ))
        .await
        .unwrap();

    // Handle a TCP connection event (inbound or outbound).
    let (stream, handshake) = match connection {
        // Handle an outbound TCP connection event.
        TcpConnection::TcpServer {
            server,
            port,
            peer_pk,
        } => {
            // TODO: move this check into the scheduler.
            //
            // First check if we are already connected to the selected peer.
            // If yes, return immediately.
            // If no, continue with the connection attempt.
            if CONNECTION_MANAGER
                .read()
                .await
                .contains_connected_peer(&peer_pk)
            {
                return Ok(connection_id);
            }

            // Define the server address and port.
            let server_port = format!("{server}:{port}");
            // Attempt a TCP connection.
            let mut stream = TcpStream::connect(server_port).await?;

            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Handshaking(connection_id),
                ))
                .await
                .unwrap();

            // Attempt a secret handshake.
            let handshake = handshake_client(&mut stream, network_key, pk, sk, peer_pk).await?;

            info!("💃 connected to peer {}", handshake.peer_pk.to_ssb_id());

            // Send 'connected' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connected(connection_id),
                ))
                .await
                .unwrap();

            (stream, handshake)
        }
        // Handle an incoming TCP connection event.
        TcpConnection::ClientStream { mut stream } => {
            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Handshaking(connection_id),
                ))
                .await
                .unwrap();

            // Attempt a secret handshake.
            let handshake = handshake_server(&mut stream, network_key, pk, sk).await?;

            // Send 'connected' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Connected(connection_id),
                ))
                .await
                .unwrap();

            // Convert the public key to a `String`.
            let ssb_id = handshake.peer_pk.to_ssb_id();

            // Add the sigil link ('@') if it's missing.
            let peer_pk = if ssb_id.starts_with('@') {
                ssb_id
            } else {
                format!("@{ssb_id}")
            };

            // Check if we are already connected to the selected peer.
            // If yes, return immediately.
            // If no, return the stream and handshake.
            if CONNECTION_MANAGER
                .read()
                .await
                .contains_connected_peer(&handshake.peer_pk)
            {
                info!("peer {} is already connected", &peer_pk);

                // Since we already have an active connection to this peer,
                // we can disconnect the redundant connection.

                // Send 'disconnecting' connection event message via the broker.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        ConnectionEvent::Disconnecting(connection_id),
                    ))
                    .await
                    .unwrap();

                return Ok(connection_id);
            }

            info!("💃 received connection from peer {}", &peer_pk);

            // Shutdown the connection if the peer is not in the list of peers
            // to be replicated, unless replication is set to nonselective.
            // This ensures we do not replicate with unknown peers.
            if selective_replication
                & !REPLICATION_CONFIG
                    .get()
                    .unwrap()
                    .peers
                    .contains_key(&peer_pk)
            {
                info!(
                    "peer {} is not in replication list and selective replication is enabled; dropping connection",
                    peer_pk
                );

                // Send connection event message via the broker.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        ConnectionEvent::Disconnecting(connection_id),
                    ))
                    .await
                    .unwrap();

                // This may not be necessary; the connection should close when
                // the stream is dropped.
                stream.shutdown(Shutdown::Both)?;

                return Ok(connection_id);
            }

            (stream, handshake)
        }
    };

    // Parse the peer public key from the handshake.
    let peer_pk = handshake.peer_pk;

    // Add the peer to the list of connected peers.
    CONNECTION_MANAGER
        .write()
        .await
        .insert_connected_peer(peer_pk);

    // Spawn the classic replication actor.
    Broker::spawn(crate::actors::replication::classic::actor(
        connection_id,
        stream.clone(),
        stream,
        handshake,
        peer_pk,
    ));

    Ok(connection_id)
}
