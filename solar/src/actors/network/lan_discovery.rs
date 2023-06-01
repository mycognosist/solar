#![allow(clippy::single_match)]

use std::time::Duration;

use async_std::{net::UdpSocket, task};
use futures::{select_biased, FutureExt};
use kuska_ssb::{discovery::LanBroadcast, keystore::OwnedIdentity};
use log::warn;

use crate::{
    actors::network::{connection, connection::TcpConnection},
    broker::*,
    Result,
};

/// Register the LAN discovery endpoint, send and receive UDP broadcasts and
/// spawn a secret handshake actor for each successfully parsed broadcast message.
pub async fn actor(
    server_id: OwnedIdentity,
    rpc_port: u16,
    selective_replication: bool,
) -> Result<()> {
    // Instantiate a new LAN broadcaster with the given public key and port.
    let broadcaster = LanBroadcast::new(&server_id.pk, rpc_port).await?;

    // Register the "lan_discovery" actor endpoint with the broker.
    let broker = BROKER.lock().await.register("lan_discovery", false).await?;
    // Fuse internal termination channel with external channel.
    // This allows termination of the peer loop to be initiated from outside
    // this function.
    let mut ch_terminate = broker.ch_terminate.fuse();

    loop {
        // Create a UDP socket with the given address.
        let socket = UdpSocket::bind(format!("0.0.0.0:{rpc_port}")).await?;
        // Allow the socket to send packets to the broadcast address.
        socket.set_broadcast(true)?;

        // Create an empty buffer to store received messages.
        let mut buf = [0; 256];

        // Poll multiple futures and streams simultaneously, executing the
        // branch for the future that finishes first. If multiple futures are
        // ready, one will be selected in order of declaration.
        select_biased! {
            _ = ch_terminate => break,
            // Receive data from the socket.
            recv = socket.recv_from(&mut buf).fuse() => {
                // `amt` is the number of bytes read.
                if let Ok((amt, _)) = recv {
                    // Process the received data. Log any errors.
                    if let Err(err) = process_broadcast(
                        &server_id,
                        &buf[..amt],
                        selective_replication
                        ).await {
                            warn!("failed to process broadcast: {:?}", err);
                        }
                }
            }
            // Sleep for 5 seconds.
            _ = task::sleep(Duration::from_secs(5)).fuse() => {}
        }

        // Drop the socket connection.
        drop(socket);
        // Send out a UDP broadcast advertising the local public key and IP
        // address. This allows other nodes on the network to discover this
        // one.
        broadcaster.send().await;
    }

    // Send terminated signal back to the broker.
    let _ = broker.ch_terminated.send(Void {});

    Ok(())
}

/// Process a UDP broadcast message and spawn a peer actor if the broadcast
/// parsing is successful. This will result in a TCP connection attempt with
/// the peer whose details are contained in the broadcast message.
async fn process_broadcast(
    server_id: &OwnedIdentity,
    buff: &[u8],
    selective_replication: bool,
) -> Result<()> {
    let msg = String::from_utf8_lossy(buff);

    // Attempt to parse the IP / hostname, port and public key from the received
    // UDP broadcast message.
    if let Some((server, port, peer_public_key)) = LanBroadcast::parse(&msg) {
        let addr = format!("{server}:{port}");

        // Spawn a connection actor with the given connection parameters.
        Broker::spawn(connection::actor(
            server_id.clone(),
            TcpConnection::Dial {
                addr,
                peer_public_key,
            },
            selective_replication,
        ));
    } else {
        warn!("failed to parse broadcast {}", msg);
    }

    Ok(())
}
