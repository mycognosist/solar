use std::time::{Duration, Instant};

use futures::{pin_mut, select_biased, FutureExt, SinkExt, StreamExt};
use kuska_ssb::{
    api::{dto::EbtReplicate, ApiCaller},
    crypto::ToSsbId,
    handshake::async_std::BoxStream,
    rpc::{RpcReader, RpcWriter},
};
use log::{error, trace};

use crate::{
    actors::{
        muxrpc::{EbtReplicateHandler, RpcInput},
        network::connection::ConnectionData,
        replication::ebt::{EbtEvent, SessionRole},
    },
    broker::{ActorEndpoint, BrokerEvent, BrokerMessage, Destination, BROKER},
    Error, Result,
};

pub async fn run(
    connection_data: ConnectionData,
    session_role: SessionRole,
    session_wait_timeout: u64,
) -> Result<()> {
    // Register the EBT replication loop actor with the broker.
    let ActorEndpoint {
        ch_terminate,
        ch_msg,
        actor_id,
        ..
    } = BROKER
        .lock()
        .await
        .register("ebt-replication-loop", true)
        .await?;

    let mut ch_msg = ch_msg.ok_or(Error::OptionIsNone)?;

    let stream_reader = connection_data.stream.clone().ok_or(Error::OptionIsNone)?;
    let stream_writer = connection_data.stream.clone().ok_or(Error::OptionIsNone)?;
    let handshake = connection_data
        .handshake
        .clone()
        .ok_or(Error::OptionIsNone)?;
    let peer_ssb_id = handshake.peer_pk.to_ssb_id();

    // Instantiate a box stream and split it into reader and writer streams.
    let (box_stream_read, box_stream_write) =
        BoxStream::from_handshake(stream_reader, stream_writer, handshake, 0x8000)
            .split_read_write();

    // Instantiate RPC reader and writer using the box streams.
    let rpc_reader = RpcReader::new(box_stream_read);
    let rpc_writer = RpcWriter::new(box_stream_write);
    let mut api = ApiCaller::new(rpc_writer);

    // Instantiate the MUXRPC handler.
    let mut ebt_replicate_handler = EbtReplicateHandler::new(actor_id);

    // Fuse internal termination channel with external channel.
    // This allows termination of the peer loop to be initiated from outside
    // this function.
    let mut ch_terminate_fuse = ch_terminate.fuse();

    // Convert the box stream reader into a stream.
    let rpc_recv_stream = rpc_reader.into_stream().fuse();
    pin_mut!(rpc_recv_stream);

    // Create channel to send messages to broker.
    let mut ch_broker = BROKER.lock().await.create_sender();

    trace!(target: "ebt-session", "Initiating EBT replication session with: {}", peer_ssb_id);

    let mut session_initiated = false;
    let mut ebt_begin_waiting = None;

    if let SessionRole::Requester = session_role {
        // Send EBT request.
        let ebt_args = EbtReplicate::default();
        api.ebt_replicate_req_send(&ebt_args).await?;
    } else {
        // Record the time at which we begin waiting to receive an EBT
        // replicate request.
        //
        // This is later used to break out of the loop if no request
        // is received within the given time allowance.
        ebt_begin_waiting = Some(Instant::now());
    }

    loop {
        // Poll multiple futures and streams simultaneously, executing the
        // branch for the future that finishes first. If multiple futures are
        // ready, one will be selected in order of declaration.
        let input = select_biased! {
            _value = ch_terminate_fuse =>  {
                break;
            },
            packet = rpc_recv_stream.select_next_some() => {
                let (req_no, packet) = packet;
                RpcInput::Network(req_no, packet)
            },
            msg = ch_msg.next().fuse() => {
                // Listen for a 'session initiated' event.
                if let Some(BrokerMessage::Ebt(EbtEvent::SessionInitiated(_, ref ssb_id, ref session_role))) = msg {
                    if peer_ssb_id == *ssb_id && *session_role == SessionRole::Responder {
                        session_initiated = true;
                    }
                }
                if let Some(msg) = msg {
                    RpcInput::Message(msg)
                } else {
                    RpcInput::None
                }
            },
        };

        match ebt_replicate_handler
            .handle(&mut api, &input, &mut ch_broker, peer_ssb_id.to_owned())
            .await
        {
            Ok(handled) if handled == true => break,
            Err(err) => {
                error!("EBT replicate handler failed: {:?}", err);

                // Break out of the input processing loop to conclude
                // the replication session.
                break;
            }
            _ => (),
        }

        // If no active session has been initiated within 5 seconds of
        // waiting to receive a replicate request, exit the replication
        // loop and send an outbound request.
        if let Some(time) = ebt_begin_waiting {
            if !session_initiated && time.elapsed() >= Duration::from_secs(session_wait_timeout) {
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        BrokerMessage::Ebt(EbtEvent::RequestSession(connection_data)),
                    ))
                    .await?;

                // Break out of the input processing loop to conclude
                // the replication session.
                break;
            }
        }
    }

    ch_broker
        .send(BrokerEvent::new(
            Destination::Broadcast,
            BrokerMessage::Ebt(EbtEvent::SessionConcluded(peer_ssb_id)),
        ))
        .await?;

    Ok(())
}
