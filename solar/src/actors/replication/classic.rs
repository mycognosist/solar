use std::time::Duration;

use async_std::{
    io::{Read, Write},
    task,
};
use futures::{pin_mut, select_biased, stream::StreamExt, FutureExt, SinkExt};
use kuska_ssb::{
    api::ApiCaller,
    crypto::{ed25519, ToSsbId},
    handshake::{async_std::BoxStream, HandshakeComplete},
    rpc::{RpcReader, RpcWriter},
};
use log::{error, info, trace, warn};

use crate::{
    actors::{
        muxrpc::{
            BlobsGetHandler, BlobsWantsHandler, GetHandler, HistoryStreamHandler, RpcHandler,
            RpcInput, WhoAmIHandler,
        },
        network::{
            connection::ConnectionData,
            connection_manager::{ConnectionEvent, CONNECTION_MANAGER},
        },
    },
    broker::*,
    Result,
};

//pub async fn actor<R: Read + Unpin + Send + Sync, W: Write + Unpin + Send + Sync>(
pub async fn actor(connection_data: ConnectionData) -> Result<()> {
    let mut ch_broker = BROKER.lock().await.create_sender();

    // Attempt replication.
    let replication_result = actor_inner(connection_data.to_owned()).await;

    // TODO: Handle unwrap.
    let peer_pk = connection_data.peer_public_key.unwrap().to_ssb_id();

    match replication_result {
        Ok(connection_data) => {
            info!("👋 finished replication with {}", peer_pk);

            // Send 'disconnecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Disconnecting(connection_data),
                ))
                .await?;
        }
        Err(err) => {
            warn!(
                "💀 replication with {} terminated with error {:?}",
                peer_pk, err
            );

            // Send 'error' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Error(connection_data, err.to_string()),
                ))
                .await?;
        }
    }

    Ok(())
}

/// Spawn the replication loop and report on the connection outcome.
//pub async fn actor_inner<R: Read + Unpin + Send + Sync, W: Write + Unpin + Send + Sync>(
pub async fn actor_inner(
    connection_data: ConnectionData, // TODO: This might need to be `mut`.
) -> Result<ConnectionData> {
    // Register the "replication" actor endpoint with the broker.
    let ActorEndpoint {
        ch_terminate,
        mut ch_broker,
        ch_msg,
        actor_id,
        ..
    } = BROKER.lock().await.register("replication", true).await?;

    // Set the connection idle timeout limit according to the connection
    // manager configuration. This value is used to break out of the
    // replication loop after n consecutive idle seconds.
    let connection_idle_timeout_limit = CONNECTION_MANAGER.read().await.idle_timeout_limit;

    /* TODO: Need to create separate `Replicate` and `Replicating` events.
     *
    // Send 'replicating' connection event message via the broker.
    ch_broker
        .send(BrokerEvent::new(
            Destination::Broadcast,
            ConnectionEvent::Replicating(connection_data.to_owned(), _),
        ))
        .await?;
    */

    // Spawn the replication loop (responsible for negotiating RPC requests).
    let conn_data = replication_loop(
        actor_id,
        connection_data,
        ch_terminate,
        ch_msg.unwrap(),
        connection_idle_timeout_limit,
    )
    .await?;

    let _ = ch_broker.send(BrokerEvent::Disconnect { actor_id }).await;

    Ok(conn_data)
}

//async fn replication_loop<R: Read + Unpin + Send + Sync, W: Write + Unpin + Send + Sync>(
async fn replication_loop(
    actor_id: usize,
    connection_data: ConnectionData,
    ch_terminate: ChSigRecv,
    mut ch_msg: ChMsgRecv,
    connection_idle_timeout_limit: u8,
) -> Result<ConnectionData> {
    // TODO: Handle the unwrap.
    //
    // Parse the peer public key from the handshake.
    let peer_ssb_id = connection_data.handshake.unwrap().peer_pk.to_ssb_id();

    // TODO: Handle the unwraps.
    //
    // Instantiate a box stream and split it into reader and writer streams.
    let (box_stream_read, box_stream_write) = BoxStream::from_handshake(
        &connection_data.stream.unwrap(),
        &connection_data.stream.unwrap(),
        connection_data.handshake.unwrap(),
        0x8000,
    )
    .split_read_write();

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
    // This allows termination of the replication loop to be initiated from
    // outside this function.
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

    trace!(target: "replication-loop", "initiating replication loop with: {}", peer_ssb_id);

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
                // Break out of the replication loop if the connection idle
                // timeout limit has been reached.
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
            trace!(target: "replication-loop", "message not processed: {:?}", input);
        }
    }

    trace!(target: "replication-loop", "peer loop concluded with: {}", peer_ssb_id);

    Ok(connection_data)
}
