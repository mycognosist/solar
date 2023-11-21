//! Epidemic Broadcast Tree (EBT) Replication.
//!
//! Two kinds of messages are sent by both peers during an EBT session:
//!
//!  - Vector clocks (also known as control messages or notes)
//!  - Feed messages
//!
//! Each vector clock is a JSON object containing one or more name/value pairs.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use async_std::task;
use futures::{select_biased, FutureExt, SinkExt, StreamExt};
use kuska_ssb::{api::dto::content::SsbId, crypto::ToSsbId, feed::Message};
use log::{debug, error, trace, warn};
use serde_json::Value;

use crate::{
    actors::{
        muxrpc::ReqNo,
        network::{connection::ConnectionData, connection_manager::ConnectionEvent},
        replication::ebt::{clock, replicator, EncodedClockValue, VectorClock},
    },
    broker::{ActorEndpoint, BrokerEvent, BrokerMessage, Destination, BROKER},
    config::PEERS_TO_REPLICATE,
    node::KV_STORE,
    Result,
};

/// EBT replication events.
#[derive(Debug, Clone)]
pub enum EbtEvent {
    WaitForSessionRequest(ConnectionData),
    RequestSession(ConnectionData),
    SessionInitiated(ReqNo, SsbId, SessionRole),
    SendClock(ReqNo, VectorClock),
    SendMessage(ReqNo, SsbId, Value),
    ReceivedClock(ReqNo, SsbId, VectorClock),
    ReceivedMessage(Message),
    SessionConcluded(SsbId),
    SessionTimeout(ConnectionData),
    Error(String, SsbId),
}

/// Role of a peer in an EBT session.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionRole {
    Requester,
    Responder,
}

impl Display for SessionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            SessionRole::Requester => write!(f, "requester"),
            SessionRole::Responder => write!(f, "responder"),
        }
    }
}

#[derive(Debug)]
pub struct EbtManager {
    /// Active EBT peer sessions.
    active_sessions: HashSet<SsbId>,
    /// Duration to wait before switching feed request to a different peer.
    feed_wait_timeout: u64,
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
    session_wait_timeout: u64,
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
            let encoded_value: EncodedClockValue = clock::encode(true, Some(true), Some(seq))?;
            // Insert the ID and encoded sequence into the local clock.
            self.local_clock.insert(peer_id.to_owned(), encoded_value);
        } else {
            // No messages are stored in the local database for this feed.
            // Set replicate flag to `true`, receive to `false` and `seq` to 0.
            let encoded_value: EncodedClockValue = clock::encode(true, Some(false), Some(0))?;
            self.local_clock.insert(peer_id.to_owned(), encoded_value);
        }

        Ok(())
    }

    /// Register a new EBT session for the given peer.
    fn register_session(&mut self, peer_ssb_id: &SsbId) {
        self.active_sessions.insert(peer_ssb_id.to_owned());

        trace!(target: "ebt-session", "Registered new EBT session for {}", peer_ssb_id);
    }

    /// Remove the given peer from the list of active session.
    fn remove_session(&mut self, peer_ssb_id: &SsbId) {
        let _ = self.active_sessions.remove(peer_ssb_id);
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

    /// Decode a peer's vector clock and retrieve all requested messages.
    async fn retrieve_requested_messages(
        // TODO: Do we need these two parameters?
        _req_no: &ReqNo,
        _peer_ssb_id: &SsbId,
        clock: VectorClock,
    ) -> Result<Vec<Value>> {
        let mut messages_to_be_sent = Vec::new();

        // Iterate over all key-value pairs in the vector clock.
        for (feed_id, encoded_seq_no) in clock.iter() {
            if *encoded_seq_no != -1 {
                // Decode the encoded vector clock sequence number.
                // TODO: Match properly on the values of replicate_flag and receive_flag.
                let (_replicate_flag, _receive_flag, sequence) = clock::decode(*encoded_seq_no)?;
                if let Some(last_seq) = KV_STORE.read().await.get_latest_seq(feed_id)? {
                    if let Some(seq) = sequence {
                        for n in seq..(last_seq + 1) {
                            if let Some(msg_kvt) = KV_STORE.read().await.get_msg_kvt(feed_id, n)? {
                                messages_to_be_sent.push(msg_kvt.value)
                            }
                        }
                    }
                }
            }
        }

        Ok(messages_to_be_sent)
    }

    /* ------------------ */
    /* EbtEvent handlers. */
    /* ------------------ */

    async fn handle_wait_for_session_request(&self, connection_data: ConnectionData) {
        trace!(target: "ebt", "Waiting for EBT session request");

        let session_role = SessionRole::Responder;
        task::spawn(replicator::run(
            connection_data,
            session_role,
            self.session_wait_timeout,
        ));
    }

    async fn handle_request_session(&self, connection_data: ConnectionData) {
        if let Some(peer_public_key) = &connection_data.peer_public_key {
            let peer_ssb_id = peer_public_key.to_ssb_id();

            // Only proceed with session initiation if there
            // is no currently active session with the given peer.
            if !self.active_sessions.contains(&peer_ssb_id) {
                trace!(
                    target: "ebt",
                    "Requesting an EBT session with {:?}",
                    connection_data.peer_public_key.unwrap()
                );

                let session_role = SessionRole::Requester;
                task::spawn(replicator::run(
                    connection_data,
                    session_role,
                    self.session_wait_timeout,
                ));
            }
        }
    }

    async fn handle_session_initiated(
        &mut self,
        req_no: ReqNo,
        peer_ssb_id: SsbId,
        session_role: SessionRole,
    ) -> Result<()> {
        trace!(target: "ebt-replication", "Initiated EBT session with {} as {}", peer_ssb_id, session_role);

        self.register_session(&peer_ssb_id);
        let local_clock = self.local_clock.to_owned();

        match session_role {
            SessionRole::Responder => {
                // Create channel to send messages to broker.
                let mut ch_broker = BROKER.lock().await.create_sender();

                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        BrokerMessage::Ebt(EbtEvent::SendClock(req_no, local_clock)),
                    ))
                    .await?;
            }
            SessionRole::Requester => {
                trace!(target: "ebt-replication", "EBT session requester: {}", req_no);
                // The requester waits for a clock to be sent by the responder.
            }
        }

        Ok(())
    }

    async fn handle_received_clock(
        &mut self,
        req_no: ReqNo,
        peer_ssb_id: SsbId,
        clock: VectorClock,
    ) -> Result<()> {
        trace!(target: "ebt-replication", "Received vector clock: {:?}", clock);

        // Create channel to send messages to broker.
        let mut ch_broker = BROKER.lock().await.create_sender();

        // If a clock is received without a prior EBT replicate
        // request having been received from the associated peer, it is
        // assumed that the clock was sent in response to a locally-sent
        // EBT replicate request. Ie. The session was requested by the
        // local peer.
        if !self.active_sessions.contains(&peer_ssb_id) {
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Ebt(EbtEvent::SessionInitiated(
                        req_no,
                        peer_ssb_id.to_owned(),
                        SessionRole::Requester,
                    )),
                ))
                .await?;
        }

        self.set_clock(&peer_ssb_id, clock.to_owned());

        let msgs = EbtManager::retrieve_requested_messages(&req_no, &peer_ssb_id, clock).await?;
        for msg in msgs {
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Ebt(EbtEvent::SendMessage(req_no, peer_ssb_id.to_owned(), msg)),
                ))
                .await?;
        }

        Ok(())
    }

    async fn handle_received_message(&mut self, msg: Message) -> Result<()> {
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
        Ok(())
    }

    async fn handle_session_concluded(&mut self, peer_ssb_id: SsbId) {
        trace!(target: "ebt-replication", "Session concluded with: {}", peer_ssb_id);
        self.remove_session(&peer_ssb_id);
    }

    async fn handle_session_timeout(&mut self, connection_data: ConnectionData) -> Result<()> {
        trace!(target: "ebt-replication", "Session timeout while waiting for request");

        // Create channel to send messages to broker.
        let mut ch_broker = BROKER.lock().await.create_sender();

        // Fallback to classic replication.
        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                BrokerMessage::Connection(ConnectionEvent::ReplicatingClassic(connection_data)),
            ))
            .await?;

        Ok(())
    }

    async fn handle_error(&mut self, err: String, peer_ssb_id: SsbId) {
        trace!(target: "ebt-replication", "Session error with {}: {}", peer_ssb_id, err);
        // The active session should be removed by the 'session concluded'
        // handler but we attempt removal here just in case.
        self.remove_session(&peer_ssb_id);
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
                                self.handle_wait_for_session_request(connection_data).await;
                            }
                            EbtEvent::RequestSession(connection_data) => {
                                self.handle_request_session(connection_data).await;
                            }
                            EbtEvent::SessionInitiated(req_no, peer_ssb_id, session_role) => {
                                if let Err(err) = self.handle_session_initiated(req_no, peer_ssb_id, session_role).await {
                                    error!("Error while handling 'session initiated' event: {}", err)
                                }
                            }
                            EbtEvent::SendClock(_, clock) => {
                                trace!(target: "ebt-replication", "Sending vector clock: {:?}", clock);
                                // TODO: Update sent clocks.
                            }
                            EbtEvent::ReceivedClock(req_no, peer_ssb_id, clock) => {
                                if let Err(err) = self.handle_received_clock(req_no, peer_ssb_id, clock).await {
                                    error!("Error while handling 'received clock' event: {}", err)
                                }
                            }
                            EbtEvent::ReceivedMessage(msg) => {
                                if let Err(err) = self.handle_received_message(msg).await {
                                    error!("Error while handling 'received message' event: {}", err)
                                }
                            }
                            EbtEvent::SendMessage(_req_no, _peer_ssb_id, _msg) => {
                                trace!(target: "ebt-replication", "Sending message...");
                                // TODO: Update sent messages.
                            }
                            EbtEvent::SessionConcluded(connection_data) => {
                                self.handle_session_concluded(connection_data).await;
                            }
                            EbtEvent::SessionTimeout(connection_data) => {
                                if let Err(err) = self.handle_session_timeout(connection_data).await {
                                    error!("Error while handling 'session timeout' event: {}", err)
                                }
                            }
                            EbtEvent::Error(err, peer_ssb_id) => {
                                self.handle_error(err, peer_ssb_id).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
