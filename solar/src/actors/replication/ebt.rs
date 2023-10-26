//! Epidemic Broadcast Tree (EBT) Replication.
//!
//! Two kinds of messages are sent by both peers during an EBT session:
//!
//!  - control messages known as notes
//!  - feed messages
//!
//! Each note is a JSON object containing one or more name/value pairs.

// EBT in ScuttleGo:
//
// https://github.com/planetary-social/scuttlego/commit/e1412a550652c791dd97c72797ed512f385669e8
// http://dev.planetary.social/replication/ebt.html

// serde_json::to_string(&notes)?;
// "{\"@FCX/tsDLpubCPKKfIrw4gc+SQkHcaD17s7GI6i/ziWY=.ed25519\":123,\"@l1sGqWeCZRA99gN+t9sI6+UOzGcHq3KhLQUYEwb4DCo=.ed25519\":-1}"

use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    time::Duration,
};

use async_std::task;
use futures::{pin_mut, select_biased, FutureExt, StreamExt};
use kuska_ssb::{
    api::{dto::EbtReplicate, ApiCaller},
    crypto::ToSsbId,
    handshake::async_std::BoxStream,
    rpc::{RpcReader, RpcWriter},
};
use log::{debug, error, trace};

use crate::{
    actors::{
        muxrpc::{EbtReplicateHandler, RpcHandler, RpcInput},
        network::connection::ConnectionData,
    },
    broker::{ActorEndpoint, BrokerMessage, BROKER},
    node::KV_STORE,
    Error, Result,
};

// TODO: Do we need this? Is there another way?
// Especially since we won't need to access this from outside the module.
// Have a look at the connection manager too.
/// The EBT replication manager for the solar node.
/*
pub static EBT_MANAGER: Lazy<Arc<RwLock<EbtManager>>> =
    Lazy::new(|| Arc::new(RwLock::new(EbtManager::new())));
*/

/// An SSB identity in the form `@...=.ed25519`.
// The validity of this string should be confirmed when a note is received.
// Receiving a note with a malformed feed identifier should terminate the EBT
// session with an error.
type SsbId = String;
/// The encoded vector clock value.
// Receiving a note with a malformed value should terminate the EBT session
// with an error.
type EncodedClockValue = i64;

/// A vector clock which maps an SSB ID to an encoded vector clock value.
type VectorClock = HashMap<SsbId, EncodedClockValue>;

/*
// TODO: Might be better like this.
struct VectorClock {
    id: SsbId,
    replicate_flag: bool,
    receive_flag: Option<bool>,
    sequence: Option<u64>,
}
*/

/// EBT replication events.
#[derive(Debug, Clone)]
pub enum EbtEvent {
    WaitForSessionRequest(ConnectionData),
    RequestSession(ConnectionData),
}
/*
    SessionInitiated,
    ReceivedClock,
    ReceivedMessage,
    SendClock,
    SendMessage,
    Error,
}
*/

// TODO: Track active replication sessions.

pub struct EbtManager {
    /// The SSB ID of the local node.
    id: SsbId,
    /// Duration to wait before switching feed request to a different peer.
    feed_wait_timeout: u8,
    /// Duration to wait for a connected peer to initiate an EBT session.
    session_wait_timeout: u8,
    /// The local vector clock.
    local_clock: VectorClock,
    /// The vector clock for each known peer.
    peer_clocks: HashMap<SsbId, VectorClock>,
    /// A set of all the feeds for which active requests are open.
    ///
    /// This allows us to avoid requesting a feed from multiple peers
    /// simultaneously.
    requested_feeds: HashSet<SsbId>,
    /// The state of the replication loop.
    is_replication_loop_active: bool,
}

impl Default for EbtManager {
    fn default() -> Self {
        EbtManager {
            id: String::new(),
            feed_wait_timeout: 3,
            session_wait_timeout: 5,
            local_clock: HashMap::new(),
            peer_clocks: HashMap::new(),
            requested_feeds: HashSet::new(),
            is_replication_loop_active: false,
        }
    }
}

impl EbtManager {
    // Read peer clock state from file.
    // fn load_peer_clocks()
    // Write peer clock state to file.
    // fn persist_peer_clocks()

    /*
    async fn actor() -> Result<()> {
        let mut ch_msg = ch_msg.ok_or(Error::OptionIsNone)?;

        let mut ebt = Ebt::default();
        Ebt::event_loop(ebt, ch_terminate, ch_msg).await;

        Ok(())
    }

    /// Instantiate a new `EbtManager`.
    pub fn new() -> Self {
        let mut ebt_manager = EbtManager::default();

        // Spawn the EBT event message loop.
        let event_loop = task::spawn(Self::event_loop());

        ebt_manager.event_loop = Some(event_loop);

        ebt_manager
    }
    */

    /// Start the EBT event loop.
    ///
    /// Listen for EBT event messages via the broker and update EBT session
    /// state accordingly.
    pub async fn event_loop(self) -> Result<()> {
        debug!("Started EBT event loop");

        // Register the EBT event loop actor with the broker.
        let ActorEndpoint {
            ch_terminate,
            ch_msg,
            ..
        } = BROKER.lock().await.register("ebt-event-loop", true).await?;

        let mut ch_terminate_fuse = ch_terminate.fuse();
        let mut broker_msg_ch = ch_msg.unwrap();

        // Listen for EBT events via the broker message bus.
        loop {
            select_biased! {
                _value = ch_terminate_fuse => {
                    break;
                },
                msg = broker_msg_ch.next().fuse() => {
                    if let Some(BrokerMessage::Ebt(event)) = msg {
                        debug!("Received EBT event message from broker");
                        match event {
                            EbtEvent::WaitForSessionRequest(connection_data) => {
                                trace!(target: "ebt", "Waiting for EBT session request");
                                if let Err(err) = task::spawn(EbtManager::replication_loop(connection_data, false, 5)).await {
                                    error!("Failed to spawn EBT replication loop: {}", err)
                                }

                            }
                            EbtEvent::RequestSession(connection_data) => {
                                trace!(target: "ebt", "Requesting an EBT session with {:?}", connection_data.peer_public_key.unwrap());
                                // TODO: First check for an active session for this peer.
                                if let Err(err) = task::spawn(EbtManager::replication_loop(connection_data, false, 5)).await {
                                    error!("Failed to spawn EBT replication loop: {}", err)
                                }
                            }
                            /*
                            EbtEvent::SessionInitiated => {
                                trace!(target: "ebt-replication", "Initiated an EBT session with {}", data.peer_public_key);
                                // TODO: Update active sessions.
                            }
                            EbtEvent::ReceivedClock => {
                                trace!(target: "ebt-replication", "Received vector clock from {}", data.peer_public_key);
                                // TODO: Update peer clocks.
                            }
                            EbtEvent::ReceivedMessage => {
                                trace!(target: "ebt-replication", "Received message from {}", data.peer_public_key);
                                // TODO: Append message to feed.
                            }
                            EbtEvent::SendClock => {
                                trace!(target: "ebt-replication", "Sending vector clock to {}", data.peer_public_key);
                                // TODO: Update sent clocks.
                            }
                            EbtEvent::SendMessage => {
                                trace!(target: "ebt-replication", "Sending message to {}", data.peer_public_key);
                                // TODO: Update sent messages.
                            }
                            */
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn replication_loop(
        connection_data: ConnectionData,
        is_session_requester: bool,
        session_wait_timeout: u8,
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

        // Instantiate a timer counter.
        //
        // This counter is used to break out of the input loop after n consecutive
        // timer events. Since the sleep duration is currently set to 1 second,
        // this means that the input loop will be exited after n seconds of idle
        // activity (ie. no incoming packets or messages).
        let mut timer_counter = 0;

        trace!(target: "ebt-session", "Initiating EBT replication session with: {}", peer_ssb_id);

        if is_session_requester {
            // Send EBT request.
            let ebt_args = EbtReplicate::default();
            api.ebt_replicate_req_send(&ebt_args).await?;
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
                    if timer_counter >= session_wait_timeout {
                        break
                    } else {
                        // Increment the timer counter.
                        timer_counter += 1;
                        RpcInput::Timer
                    }
                }
            };

            let mut handled = false;
            match ebt_replicate_handler
                .handle(&mut api, &input, &mut ch_broker)
                .await
            {
                Ok(has_been_handled) => {
                    if has_been_handled {
                        handled = true;
                        break;
                    }
                }
                Err(err) => {
                    error!("ebt replicate handler failed with {:?}", err);
                }
            }
            if !handled {
                trace!(target: "ebt-session", "Message not processed: {:?}", input);
            }
        }

        trace!(target: "ebt-session", "EBT replication session concluded with: {}", peer_ssb_id);

        Ok(())
    }

    /*
    pub fn take_event_loop(&mut self) -> JoinHandle<()> {
        self.event_loop.take().unwrap()
    }
    */

    /*
    async fn init() -> Result<Self> {
        let mut ebt = Ebt::default();
        let event_loop = task::spawn(Ebt::event_loop());
        ebt.event_loop = Some(event_loop);

        // Get list of peers to replicate.
        if let Some(peers) = PEERS_TO_REPLICATE.get() {
            // Request replication of each peer.
            for peer in peers.keys() {
                ebt.replicate(peer).await?;
            }
        }
        // TODO: Load peer clocks from file and update `peer_clocks`.

        Ok(ebt)
    }
    */

    /// Retrieve the vector clock for the given SSB ID.
    fn get_clock(self, peer_id: &SsbId) -> Option<VectorClock> {
        if peer_id == &self.id {
            Some(self.local_clock)
        } else {
            self.peer_clocks.get(peer_id).cloned()
        }
    }

    /// Set or update the vector clock for the given SSB ID.
    fn set_clock(&mut self, peer_id: &SsbId, clock: VectorClock) {
        if peer_id == &self.id {
            self.local_clock = clock
        } else {
            self.peer_clocks.insert(peer_id.to_owned(), clock);
        }
    }

    /// Request that the feed represented by the given SSB ID be replicated.
    async fn replicate(&mut self, peer_id: &SsbId) -> Result<()> {
        // Look up the latest sequence for the given ID.
        if let Some(seq) = KV_STORE.read().await.get_latest_seq(peer_id)? {
            // Encode the replicate flag, receive flag and sequence.
            let encoded_value: EncodedClockValue = EbtManager::encode(true, Some(true), Some(seq))?;
            // Insert the ID and encoded sequence into the local clock.
            self.local_clock.insert(peer_id.to_owned(), encoded_value);
        }

        Ok(())
    }

    /// Revoke a replication request for the feed represented by the given SSB
    /// ID.
    fn revoke(&mut self, peer_id: &SsbId) {
        self.local_clock.remove(peer_id);
    }

    /// Request the feed represented by the given SSB ID from a peer.
    fn request(&mut self, peer_id: &SsbId) {
        self.requested_feeds.insert(peer_id.to_owned());
    }

    /// Decode a value from a control message (aka. note), returning the values
    /// of the replicate flag, receive flag and sequence.
    ///
    /// If the replicate flag is `false`, the peer does not wish to receive
    /// messages for the referenced feed.
    ///
    /// If the replicate flag is `true`, values will be returned for the receive
    /// flag and sequence. The sequence refers to a sequence number of the
    /// referenced feed.
    fn decode(value: i64) -> Result<(bool, Option<bool>, Option<u64>)> {
        let (replicate_flag, receive_flag, sequence) = if value < 0 {
            // Replicate flag is `false`.
            // Peer does not wish to receive messages for this feed.
            (false, None, None)
        } else {
            // Get the least-significant bit (aka. rightmost bit).
            let lsb = value & 1;
            // Set the receive flag value.
            let receive_flag = lsb == 0;
            // Perform a single bit arithmetic right shift to obtain the sequence
            // number.
            let sequence: u64 = (value >> 1).try_into()?;

            (true, Some(receive_flag), Some(sequence))
        };

        Ok((replicate_flag, receive_flag, sequence))
    }

    /// Encode a replicate flag, receive flag and sequence number as a control
    /// message (aka. note) value.
    ///
    /// If the replicate flag is `false`, a value of `-1` is returned.
    ///
    /// If the replicate flag is `true` and the receive flag is `true`, a single
    /// bit arithmetic left shift is performed on the sequence number and the
    /// least-significant bit is set to `0`.
    ///
    /// If the replicate flag is `true` and the receive flag is `false`, a single
    /// bit arithmetic left shift is performed on the sequence number and the
    /// least-significant bit is set to `1`.
    fn encode(
        replicate_flag: bool,
        receive_flag: Option<bool>,
        sequence: Option<u64>,
    ) -> Result<i64> {
        let value = if replicate_flag {
            // Perform a single bit arithmetic left shift.
            let mut signed: i64 = (sequence.unwrap() << 1).try_into()?;
            // Get the least-significant bit (aka. rightmost bit).
            let lsb = signed & 1;
            // Set the least-significant bit based on the value of the receive flag.
            if let Some(_flag @ true) = receive_flag {
                // Set the LSB to 0.
                signed |= 0 << lsb;
            } else {
                // Set the LSB to 1.
                signed |= 1 << lsb;
            }
            signed
        } else {
            -1
        };

        Ok(value)
    }
}

/*
pub async fn actor<R: Read + Unpin + Send + Sync, W: Write + Unpin + Send + Sync>(
    connection_data: ConnectionData,
    stream_reader: R,
    stream_writer: W,
    handshake: HandshakeComplete,
    peer_pk: ed25519::PublicKey,
) -> Result<()> {
    let mut ch_broker = BROKER.lock().await.create_sender();

    // Attempt replication.
    let replication_result = actor_inner(
        connection_data.to_owned(),
        stream_reader,
        stream_writer,
        handshake,
    )
    .await;

    match replication_result {
        Ok(connection_data) => {
            info!("👋 finished EBT replication with {}", peer_pk.to_ssb_id());

            // Send 'disconnecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    ConnectionEvent::Disconnecting(connection_data.to_owned()),
                ))
                .await?;
        }
        Err(err) => {
            warn!(
                "💀 EBT replication with {} terminated with error {:?}",
                peer_pk.to_ssb_id(),
                err
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

/// Spawn the EBT replication loop and report on the connection outcome.
pub async fn actor_inner<R: Read + Unpin + Send + Sync, W: Write + Unpin + Send + Sync>(
    connection_data: ConnectionData,
    mut stream_reader: R,
    mut stream_writer: W,
    handshake: HandshakeComplete,
) -> Result<ConnectionData> {
    // Register the "ebt-replication" actor endpoint with the broker.
    let ActorEndpoint {
        ch_terminate,
        mut ch_broker,
        ch_msg,
        actor_id,
        ..
    } = BROKER
        .lock()
        .await
        .register("ebt-replication", true)
        .await?;

    // Set the connection idle timeout limit according to the connection
    // manager configuration. This value is used to break out of the
    // replication loop after n consecutive idle seconds.
    let connection_idle_timeout_limit = CONNECTION_MANAGER.read().await.idle_timeout_limit;

    // Send 'replicating' connection event message via the broker.
    ch_broker
        .send(BrokerEvent::new(
            Destination::Broadcast,
            ConnectionEvent::Replicating(connection_data.to_owned()),
        ))
        .await?;

    // Spawn the replication loop (responsible for negotiating RPC requests).
    replication_loop(
        actor_id,
        &mut stream_reader,
        &mut stream_writer,
        handshake,
        ch_terminate,
        ch_msg.unwrap(),
        connection_idle_timeout_limit,
    )
    .await?;

    let _ = ch_broker.send(BrokerEvent::Disconnect { actor_id }).await;

    Ok(connection_data)
}

async fn replication_loop<R: Read + Unpin + Send + Sync, W: Write + Unpin + Send + Sync>(
    actor_id: usize,
    stream_reader: R,
    stream_writer: W,
    handshake: HandshakeComplete,
    ch_terminate: ChSigRecv,
    mut ch_msg: ChMsgRecv,
    connection_idle_timeout_limit: u8,
) -> Result<()> {
    // Parse the peer public key from the handshake.
    let peer_ssb_id = handshake.peer_pk.to_ssb_id();

    // Instantiate a box stream and split it into reader and writer streams.
    let (box_stream_read, box_stream_write) =
        BoxStream::from_handshake(stream_reader, stream_writer, handshake, 0x8000)
            .split_read_write();

    // Instantiate RPC reader and writer using the box streams.
    let rpc_reader = RpcReader::new(box_stream_read);
    let rpc_writer = RpcWriter::new(box_stream_write);
    let mut api = ApiCaller::new(rpc_writer);

    // TODO: Do we need to instantiate all of these handlers when replicating
    // with EBT? Or only for classic replication?

    // Instantiate the MUXRPC handlers.
    //let mut history_stream_handler = HistoryStreamHandler::new(actor_id);
    let mut ebt_replicate_handler = EbtReplicateHandler::new(actor_id);
    let mut whoami_handler = WhoAmIHandler::new(&peer_ssb_id);
    let mut get_handler = GetHandler::default();
    let mut blobs_get_handler = BlobsGetHandler::default();
    let mut blobs_wants_handler = BlobsWantsHandler::default();

    let mut handlers: Vec<&mut dyn RpcHandler<W>> = vec![
        //&mut history_stream_handler,
        &mut ebt_replicate_handler,
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

    trace!(target: "ebt-replication-loop", "initiating ebt replication loop with: {}", peer_ssb_id);

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
            trace!(target: "ebt-replication-loop", "message not processed: {:?}", input);
        }
    }

    trace!(target: "ebt-replication-loop", "ebt peer loop concluded with: {}", peer_ssb_id);

    Ok(())
}
*/

#[cfg(test)]
mod test {
    use super::*;

    const VALUES: [i64; 5] = [-1, 0, 1, 2, 3];
    const NOTES: [(bool, std::option::Option<bool>, std::option::Option<u64>); 5] = [
        (false, None, None),
        (true, Some(true), Some(0)),
        (true, Some(false), Some(0)),
        (true, Some(true), Some(1)),
        (true, Some(false), Some(1)),
    ];

    #[test]
    fn test_decode() {
        VALUES
            .iter()
            .zip(NOTES)
            .for_each(|(value, note)| assert_eq!(Ebt::decode(*value).unwrap(), note));
    }

    #[test]
    fn test_encode() {
        VALUES.iter().zip(NOTES).for_each(|(value, note)| {
            assert_eq!(Ebt::encode(note.0, note.1, note.2).unwrap(), *value)
        });
    }
}
