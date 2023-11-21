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
    let mut ebt_replicate_handler = EbtReplicateHandler::new();

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
    let mut replicate_req_no = None;

    // Record the time at which we begin the EBT session.
    //
    // This is later used to implement a timeout if no request or response is
    // received.
    let ebt_session_start = Instant::now();

    if let SessionRole::Requester = session_role {
        // Send EBT request.
        let ebt_args = EbtReplicate::default();
        let req_no = api.ebt_replicate_req_send(&ebt_args).await?;
        // Set the request number to be passed into the MUXRPC EBT handler.
        // This allows tracking of the request (ensuring we respond to
        // MUXRPC responses with this request number).
        replicate_req_no = Some(req_no);
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
            .handle(
                &mut api,
                &input,
                &mut ch_broker,
                peer_ssb_id.to_owned(),
                replicate_req_no,
            )
            .await
        {
            Ok(true) => break,
            Err(err) => {
                error!("EBT replicate handler failed: {:?}", err);

                if let Error::EbtReplicate((req_no, err_msg)) = err {
                    ch_broker
                        .send(BrokerEvent::new(
                            Destination::Broadcast,
                            BrokerMessage::Ebt(EbtEvent::Error(
                                connection_data,
                                req_no,
                                peer_ssb_id.to_owned(),
                                err_msg,
                            )),
                        ))
                        .await?;
                }

                // Break out of the input processing loop to conclude
                // the replication session.
                break;
            }
            _ => (),
        }

        // If no active session has been initiated within 5 seconds of
        // waiting to receive a replicate request, broadcast a session timeout
        // event (leading to initiation of classic replication).
        if !session_initiated
            && session_role == SessionRole::Responder
            && ebt_session_start.elapsed() >= Duration::from_secs(session_wait_timeout)
        {
            trace!(target: "ebt-session", "Timeout while waiting for {} to initiate EBT replication session", peer_ssb_id);

            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Ebt(EbtEvent::SessionTimeout(connection_data)),
                ))
                .await?;

            // Break out of the input processing loop to conclude
            // the replication session.
            break;
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
