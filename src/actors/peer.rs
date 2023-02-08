use std::{net::Shutdown, time::Duration};

use async_std::{
    io::{Read, Write},
    net::TcpStream,
    task,
};
use futures::{pin_mut, select_biased, stream::StreamExt, FutureExt, SinkExt};
use kuska_ssb::{
    api::ApiCaller,
    crypto::{ed25519, ToSsbId},
    handshake::{
        async_std::{handshake_client, handshake_server, BoxStream},
        HandshakeComplete,
    },
    keystore::OwnedIdentity,
    rpc::{RpcReader, RpcWriter},
};
use log::{error, info, trace, warn};

use crate::{
    actors::{
        connection_manager::{ConnectionEvent, CONNECTION_MANAGER},
        rpc::{
            BlobsGetHandler, BlobsWantsHandler, GetHandler, HistoryStreamHandler, RpcHandler,
            RpcInput, WhoAmIHandler,
        },
    },
    broker::*,
    config::{NETWORK_KEY, REPLICATION_CONFIG},
    Result,
};

pub enum Connect {
    TcpServer {
        server: String,
        port: u16,
        peer_pk: ed25519::PublicKey,
    },
    ClientStream {
        stream: TcpStream,
    },
}

pub async fn actor(id: OwnedIdentity, connect: Connect, selective_replication: bool) -> Result<()> {
    // Register a new connection with the connection manager.
    let connection_id = CONNECTION_MANAGER.write().await.register();

    // Set the connection idle timeout limit according to the connection
    // manager configuration. This value is used to break out of the peer loop
    // after n consecutive idle seconds.
    let connection_idle_timeout_limit = CONNECTION_MANAGER.read().await.idle_timeout_limit;

    // Catch any errors which occur during the peer connection and replication.
    if let Err(err) = actor_inner(
        id,
        connect,
        selective_replication,
        connection_id,
        connection_idle_timeout_limit,
    )
    .await
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

/// Handle a TCP connection, update the list of connected peers, register the
/// peer actor endpoint, spawn the peer loop and report on the connection
/// outcome.
pub async fn actor_inner(
    id: OwnedIdentity,
    connect: Connect,
    selective_replication: bool,
    connection_id: usize,
    connection_idle_timeout_limit: u8,
) -> Result<usize> {
    // Register the "peer" actor endpoint with the broker.
    let ActorEndpoint {
        ch_terminate,
        mut ch_broker,
        ch_msg,
        actor_id,
        ..
    } = BROKER.lock().await.register("peer", true).await?;

    // Parse the public key and secret key from the identity.
    let OwnedIdentity { pk, sk, .. } = id;

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
    let (stream, handshake) = match connect {
        // Handle an outbound TCP connection event.
        Connect::TcpServer {
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
        Connect::ClientStream { mut stream } => {
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

    // Send 'replicating' connection event message via the broker.
    ch_broker
        .send(BrokerEvent::new(
            Destination::Broadcast,
            ConnectionEvent::Replicating(connection_id),
        ))
        .await
        .unwrap();

    // Spawn the peer loop (responsible for negotiating RPC requests).
    let res = peer_loop(
        actor_id,
        &stream,
        &stream,
        handshake,
        ch_terminate,
        ch_msg.unwrap(),
        connection_idle_timeout_limit,
    )
    .await;

    // Remove the peer from the list of connected peers.
    CONNECTION_MANAGER
        .write()
        .await
        .remove_connected_peer(peer_pk);

    if let Err(err) = res {
        warn!("💀 client terminated with error {:?}", err);

        // TODO: Use the `ConnectionError` as the type for `err`.
        //
        // Send 'error' connection event message via the broker.
        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                ConnectionEvent::Error(connection_id, err.to_string()),
            ))
            .await
            .unwrap();
    } else {
        info!("👋 finished connection with {}", &peer_pk.to_ssb_id());

        // Send 'disconnected' connection event message via the broker.
        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                ConnectionEvent::Disconnected(connection_id),
            ))
            .await
            .unwrap();
    }

    let _ = ch_broker.send(BrokerEvent::Disconnect { actor_id }).await;

    Ok(connection_id)
}

async fn peer_loop<R: Read + Unpin + Send + Sync, W: Write + Unpin + Send + Sync>(
    actor_id: usize,
    reader: R,
    writer: W,
    handshake: HandshakeComplete,
    ch_terminate: ChSigRecv,
    mut ch_msg: ChMsgRecv,
    connection_idle_timeout_limit: u8,
) -> Result<()> {
    // Parse the peer public key from the handshake.
    let peer_ssb_id = handshake.peer_pk.to_ssb_id();

    // Instantiate a box stream and split it into reader and writer streams.
    let (box_stream_read, box_stream_write) =
        BoxStream::from_handshake(reader, writer, handshake, 0x8000).split_read_write();

    // Instantiate RPC reader and writer using the box streams.
    let rpc_reader = RpcReader::new(box_stream_read);
    let rpc_writer = RpcWriter::new(box_stream_write);
    let mut api = ApiCaller::new(rpc_writer);

    // Instantiate the MUXRPC handlers.
    let mut history_stream_handler = HistoryStreamHandler::new(actor_id);
    let mut whoami_handler = WhoAmIHandler::new(&peer_ssb_id);
    let mut get_handler = GetHandler::default();
    let mut blobs_get_handler = BlobsGetHandler::default();
    let mut blobs_wants_handler = BlobsWantsHandler::default();

    let mut handlers: Vec<&mut dyn RpcHandler<W>> = vec![
        &mut history_stream_handler,
        &mut whoami_handler,
        &mut get_handler,
        &mut blobs_get_handler,
        &mut blobs_wants_handler,
    ];

    // Create channel to send messages to broker.
    let mut ch_broker = BROKER.lock().await.create_sender();
    // Fuse internal termination channel with external channel.
    // This allows termination of the peer loop to be initiated from outside
    // this function.
    let mut ch_terminate_fuse = ch_terminate.fuse();

    // Convert the box stream reader into a stream.
    let rpc_recv_stream = rpc_reader.into_stream().fuse();
    pin_mut!(rpc_recv_stream);

    // Instantiate a timer counter.
    //
    // This counter is used to break out of the input loop after n consecutive
    // timer events. Since the sleep duration is currently set to 1 second,
    // this means that the input loop will be exited after n seconds of idle
    // activity (ie. no incoming packets or messages).
    let mut timer_counter = 0;

    trace!(target: "peer-loop", "initiating peer loop with: {}", peer_ssb_id);

    loop {
        // Poll multiple futures and streams simultaneously, executing the
        // branch for the future that finishes first. If multiple futures are
        // ready, one will be selected in order of declaration.
        let input = select_biased! {
            _value = ch_terminate_fuse =>  {
                break;
            },
            packet = rpc_recv_stream.select_next_some() => {
                // Reset the timer counter.
                timer_counter = 0;
                let (rpc_id, packet) = packet;
                RpcInput::Network(rpc_id, packet)
            },
            msg = ch_msg.next().fuse() => {
                // Reset the timer counter.
                timer_counter = 0;
                if let Some(msg) = msg {
                    RpcInput::Message(msg)
                } else {
                    RpcInput::None
                }
            },
            _ = task::sleep(Duration::from_secs(1)).fuse() => {
                // Break out of the peer loop if the connection idle timeout
                // limit has been reached.
                if timer_counter >= connection_idle_timeout_limit {
                    break
                } else {
                    // Increment the timer counter.
                    timer_counter += 1;
                    RpcInput::Timer
                }
            }
        };

        let mut handled = false;
        for handler in handlers.iter_mut() {
            match handler.handle(&mut api, &input, &mut ch_broker).await {
                Ok(has_been_handled) => {
                    if has_been_handled {
                        handled = true;
                        break;
                    }
                }
                Err(err) => {
                    error!("handler {} failed with {:?}", handler.name(), err);
                }
            }
        }
        if !handled {
            trace!(target: "peer-loop", "message not processed: {:?}", input);
        }
    }

    trace!(target: "peer-loop", "peer loop concluded with: {}", peer_ssb_id);

    Ok(())
}
