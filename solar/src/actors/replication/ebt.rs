//! Epidemic Broadcast Tree (EBT) Replication.
//!
//! Two kinds of messages are sent by both peers during an EBT session:
//!
//!  - control messages known as notes
//!  - feed messages
//!
//! Each note is a JSON object containing one or more name/value pairs.

use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    fmt::Display,
    thread,
    time::{Duration, Instant},
};

use async_std::task;
use futures::{pin_mut, select_biased, FutureExt, SinkExt, StreamExt};
use kuska_ssb::{
    api::{dto::EbtReplicate, ApiCaller},
    crypto::ToSsbId,
    feed::Message,
    handshake::async_std::BoxStream,
    rpc::{RpcReader, RpcWriter},
};
use log::{debug, error, trace, warn};
use serde_json::Value;

use crate::{
    actors::{
        muxrpc::{EbtReplicateHandler, ReqNo, RpcInput},
        network::connection::ConnectionData,
    },
    broker::{ActorEndpoint, BrokerEvent, BrokerMessage, Destination, BROKER},
    config::PEERS_TO_REPLICATE,
    node::KV_STORE,
    Error, Result,
};

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
pub type VectorClock = HashMap<SsbId, EncodedClockValue>;

/// EBT replication events.
#[derive(Debug, Clone)]
pub enum EbtEvent {
    WaitForSessionRequest(ConnectionData),
    RequestSession(ConnectionData),
    SessionInitiated(ReqNo, SsbId, EbtSessionRole),
    SendClock(ReqNo, VectorClock),
    ReceivedClock(ReqNo, SsbId, VectorClock),
    ReceivedMessage(Message),
    SendMessage(ReqNo, SsbId, Value),
}
/*
    SessionConcluded,
    Error,
}
*/

/// Role of a peer in an EBT session.
#[derive(Debug, Clone, PartialEq)]
pub enum EbtSessionRole {
    Requester,
    Responder,
}

impl Display for EbtSessionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            EbtSessionRole::Requester => write!(f, "requester"),
            EbtSessionRole::Responder => write!(f, "responder"),
        }
    }
}

#[derive(Debug)]
pub struct EbtManager {
    /// Active EBT peer sessions.
    active_sessions: HashSet<SsbId>,
    /// Duration to wait before switching feed request to a different peer.
    feed_wait_timeout: u8,
    /// The state of the replication loop.
    is_replication_loop_active: bool,
    /// The local vector clock.
    local_clock: VectorClock,
    /// The SSB ID of the local node.
    local_id: SsbId,
    /// The vector clock for each known peer.
    peer_clocks: HashMap<SsbId, VectorClock>,
    /// A set of all the feeds for which active requests are open.
    ///
    /// This allows us to avoid requesting a feed from multiple peers
    /// simultaneously.
    requested_feeds: HashSet<SsbId>,
    /// Duration to wait for a connected peer to initiate an EBT session.
    session_wait_timeout: u8,
}

impl Default for EbtManager {
    fn default() -> Self {
        EbtManager {
            active_sessions: HashSet::new(),
            feed_wait_timeout: 3,
            is_replication_loop_active: false,
            local_clock: HashMap::new(),
            local_id: String::new(),
            peer_clocks: HashMap::new(),
            requested_feeds: HashSet::new(),
            session_wait_timeout: 5,
        }
    }
}

impl EbtManager {
    // Read peer clock state from file.
    // fn load_peer_clocks()
    // Write peer clock state to file.
    // fn persist_peer_clocks()

    /// Initialise the local clock based on peers to be replicated.
    ///
    /// This defines the public keys of all feeds we wish to replicate,
    /// along with the latest sequence number for each.
    //async fn init_local_clock(mut self, local_id: &SsbId) -> Result<()> {
    async fn init_local_clock(&mut self) -> Result<()> {
        debug!("Initialising local EBT clock");

        let local_id = self.local_id.to_owned();

        // Set the local feed to be replicated.
        self.replicate(&local_id).await?;

        // Get list of peers to replicate.
        if let Some(peers) = PEERS_TO_REPLICATE.get() {
            // Request replication of each peer.
            for peer in peers.keys() {
                self.replicate(peer).await?;
            }
        }

        // TODO: Load peer clocks from file and update `peer_clocks`.

        Ok(())
    }

    /// Start the EBT event loop.
    ///
    /// Listen for EBT event messages via the broker and update EBT session
    /// state accordingly.
    pub async fn event_loop(mut self, local_id: SsbId) -> Result<()> {
        debug!("Started EBT event loop");

        // Set the ID (@-prefixed public key) of the local node.
        self.local_id = local_id;

        // Initialise the local clock based on peers to be replicated.
        self.init_local_clock().await?;

        // Register the EBT event loop actor with the broker.
        let ActorEndpoint {
            ch_terminate,
            ch_msg,
            mut ch_broker,
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
                                let session_role = EbtSessionRole::Responder;
                                task::spawn(EbtManager::replication_loop(connection_data, session_role, 5));
                            }
                            EbtEvent::RequestSession(connection_data) => {
                                if let Some(peer_public_key) = &connection_data.peer_public_key {
                                    let peer_ssb_id = peer_public_key.to_ssb_id();
                                    // Only proceed with session initiation if there
                                    // is no currently active session with the given peer.
                                    if !self.active_sessions.contains(&peer_ssb_id) {
                                        trace!(target: "ebt", "Requesting an EBT session with {:?}", connection_data.peer_public_key.unwrap());
                                        let session_role = EbtSessionRole::Requester;
                                        task::spawn(EbtManager::replication_loop(connection_data, session_role, 5));
                                    }
                                }
                            }
                            EbtEvent::SessionInitiated(req_no, peer_ssb_id, session_role) => {
                                trace!(target: "ebt-replication", "Initiated EBT session with {} as {}", peer_ssb_id, session_role);
                                self.register_session(&peer_ssb_id);

                                let local_clock = self.local_clock.to_owned();

                                match session_role {
                                    EbtSessionRole::Responder => {
                                        ch_broker
                                            .send(BrokerEvent::new(
                                                Destination::Broadcast,
                                                BrokerMessage::Ebt(EbtEvent::SendClock(req_no, local_clock)),
                                            ))
                                            .await?;
                                    }
                                    EbtSessionRole::Requester => {
                                        // TODO: ReceiveClock ?
                                        trace!(target: "ebt-replication", "EBT session requester: {}", req_no);
                                    }
                                }
                            }
                            EbtEvent::SendClock(_, clock) => {
                                trace!(target: "ebt-replication", "Sending vector clock: {:?}", clock);
                                // TODO: Update sent clocks.
                            }
                            EbtEvent::ReceivedClock(req_no, peer_ssb_id, clock) => {
                                trace!(target: "ebt-replication", "Received vector clock: {:?}", clock);

                                // If a clock is received without a prior EBT replicate
                                // request having been received from the associated peer, it is
                                // assumed that the clock was sent in response to a locally-sent
                                // EBT replicate request. Ie. The session was requested by the
                                // local peer.
                                if !self.active_sessions.contains(&peer_ssb_id) {
                                   ch_broker
                                        .send(BrokerEvent::new(
                                            Destination::Broadcast,
                                            BrokerMessage::Ebt(
                                                EbtEvent::SessionInitiated(
                                                    req_no, peer_ssb_id.to_owned(), EbtSessionRole::Requester
                                                )
                                            ),
                                        ))
                                        .await?;
                                }

                                self.set_clock(&peer_ssb_id, clock.to_owned());

                                let msgs = EbtManager::retrieve_requested_messages(&req_no, &peer_ssb_id, clock).await?;
                                for msg in msgs {
                                    ch_broker
                                        .send(BrokerEvent::new(
                                            Destination::Broadcast,
                                            BrokerMessage::Ebt(EbtEvent::SendMessage(
                                                req_no,
                                                peer_ssb_id.to_owned(),
                                                msg,
                                            )),
                                        ))
                                        .await?;
                                }
                            }
                            EbtEvent::ReceivedMessage(msg) => {
                                trace!(target: "ebt-replication", "Received message: {:?}", msg);

                                // Retrieve the sequence number of the most recent message for
                                // the peer that authored the received message.
                                let last_seq = KV_STORE
                                    .read()
                                    .await
                                    .get_latest_seq(&msg.author().to_string())?
                                    .unwrap_or(0);

                                // Validate the sequence number.
                                if msg.sequence() == last_seq + 1 {
                                    // Append the message to the feed.
                                    KV_STORE.write().await.append_feed(msg.clone()).await?;

                                    debug!(
                                        "Received message number {} from {}",
                                        msg.sequence(),
                                        msg.author()
                                    );
                                } else {
                                    warn!(
                                        "Received out-of-order message from {}; received: {}, expected: {} + 1",
                                        &msg.author().to_string(),
                                        msg.sequence(),
                                        last_seq
                                    );
                                }
                            }
                            EbtEvent::SendMessage(req_no, peer_ssb_id, msg) => {
                                trace!(target: "ebt-replication", "Sending message...");
                                // TODO: Update sent messages.
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // TODO: Consider moving this into a separate module.
    async fn replication_loop(
        connection_data: ConnectionData,
        session_role: EbtSessionRole,
        session_wait_timeout: u8,
    ) -> Result<()> {
        // Register the EBT replication loop actor with the broker.
        let ActorEndpoint {
            ch_terminate,
            ch_msg,
            actor_id,
            ch_broker,
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

        let mut session_initiated = false;
        let mut ebt_begin_waiting = None;

        if let EbtSessionRole::Requester = session_role {
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
                    // Reset the timer counter.
                    timer_counter = 0;
                    let (req_no, packet) = packet;
                    RpcInput::Network(req_no, packet)
                },
                msg = ch_msg.next().fuse() => {
                    // Listen for a 'session initiated' event.
                    if let Some(BrokerMessage::Ebt(EbtEvent::SessionInitiated(_, ref ssb_id, ref session_role))) = msg {
                        if peer_ssb_id == *ssb_id && *session_role == EbtSessionRole::Responder {
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

            let mut handled = false;
            match ebt_replicate_handler
                .handle(&mut api, &input, &mut ch_broker, peer_ssb_id.to_owned())
                .await
            {
                Ok(has_been_handled) => {
                    //trace!(target: "ebt-session", "EBT handler returned: {}", has_been_handled);
                    if has_been_handled {
                        handled = true;
                        break;
                    }
                }
                Err(err) => {
                    error!("ebt replicate handler failed with {:?}", err);
                }
            }

            // If no active session has been initiated within 3 seconds of
            // waiting to receive a replicate request, exit the replication
            // loop and send an outbound request.
            if let Some(time) = ebt_begin_waiting {
                if !session_initiated && time.elapsed() >= Duration::new(3, 0) {
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

        // TODO: Emit SessionConcluded event with peer_ssb_id.

        trace!(target: "ebt-session", "EBT replication session concluded with: {}", peer_ssb_id);

        Ok(())
    }

    /// Retrieve the vector clock for the given SSB ID.
    fn get_clock(self, peer_id: &SsbId) -> Option<VectorClock> {
        if peer_id == &self.local_id {
            Some(self.local_clock)
        } else {
            self.peer_clocks.get(peer_id).cloned()
        }
    }

    /// Set or update the vector clock for the given SSB ID.
    fn set_clock(&mut self, peer_id: &SsbId, clock: VectorClock) {
        if peer_id == &self.local_id {
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
        } else {
            // No messages are stored in the local database for this feed.
            // Set replicate flag to `true`, receive to `false` and `seq` to 0.
            let encoded_value: EncodedClockValue = EbtManager::encode(true, Some(false), Some(0))?;
            self.local_clock.insert(peer_id.to_owned(), encoded_value);
        }

        Ok(())
    }

    /// Register a new EBT session for the given peer.
    fn register_session(&mut self, peer_ssb_id: &SsbId) {
        self.active_sessions.insert(peer_ssb_id.to_owned());

        trace!(target: "ebt-session", "Registered new EBT session for {}", peer_ssb_id);
    }

    fn has_active_session(self, peer_ssb_id: &SsbId) {}

    /// Revoke a replication request for the feed represented by the given SSB
    /// ID.
    fn revoke(&mut self, peer_id: &SsbId) {
        self.local_clock.remove(peer_id);
    }

    /// Request the feed represented by the given SSB ID from a peer.
    fn request(&mut self, peer_id: &SsbId) {
        self.requested_feeds.insert(peer_id.to_owned());
    }

    /// Decode a peer's vector clock and retrieve all requested messages.
    async fn retrieve_requested_messages(
        req_no: &ReqNo,
        peer_ssb_id: &SsbId,
        clock: VectorClock,
    ) -> Result<Vec<Value>> {
        let mut messages_to_be_sent = Vec::new();

        // Iterate over all key-value pairs in the vector clock.
        for (feed_id, encoded_seq_no) in clock.iter() {
            if *encoded_seq_no != -1 {
                // Decode the encoded vector clock sequence number.
                // TODO: Match properly on the values of replicate_flag and receive_flag.
                let (replicate_flag, receive_flag, sequence) = EbtManager::decode(*encoded_seq_no)?;
                if let Some(last_seq) = KV_STORE.read().await.get_latest_seq(&feed_id)? {
                    if let Some(seq) = sequence {
                        for n in seq..(last_seq + 1) {
                            if let Some(msg_kvt) = KV_STORE.read().await.get_msg_kvt(&feed_id, n)? {
                                messages_to_be_sent.push(msg_kvt.value)
                            }
                        }
                    }
                }
            }
        }

        Ok(messages_to_be_sent)
    }

    /// Decode a value from a control message (aka. note), returning the values
    /// of the replicate flag, receive flag and sequence.
    ///
    /// If the replicate flag is `false`, the peer does not wish to receive
    /// messages for the referenced feed.
    ///
    /// If the replicate flag is `true`, values will be returned for the receive
    /// flag and sequence.
    ///
    /// The sequence refers to a sequence number of the referenced feed.
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
