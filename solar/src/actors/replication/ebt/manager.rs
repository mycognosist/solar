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
use kuska_ssb::{
    api::dto::{content::SsbId, BlobsGetIn},
    crypto::ToSsbId,
    feed::Message,
};
use log::{debug, error, trace, warn};
use serde_json::Value;

use crate::{
    actors::{
        muxrpc::{ReqNo, RpcBlobsGetEvent},
        network::{connection::ConnectionData, connection_manager::ConnectionEvent},
        replication::{
            blobs,
            ebt::{clock, replicator, EncodedClockValue, VectorClock},
        },
    },
    broker::{ActorEndpoint, BrokerEvent, BrokerMessage, Destination, BROKER},
    config::PEERS_TO_REPLICATE,
    node::{BLOB_STORE, KV_STORE},
    storage::kv::StoreKvEvent,
    Error, Result,
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
    Error(ConnectionData, ReqNo, SsbId, String),
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
    active_sessions: HashMap<SsbId, (ReqNo, SessionRole)>,
    /// Duration to wait before switching feed request to a different peer.
    _feed_wait_timeout: u64,
    /// The state of the replication loop.
    _is_replication_loop_active: bool,
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
    _requested_feeds: HashSet<SsbId>,
    /// Duration to wait for a connected peer to initiate an EBT session.
    session_wait_timeout: u64,
    /// The latest vector clock sent for each session, identified by the
    /// request number.
    //
    // TODO: Do we want to remove each entry when the session concludes?
    sent_clocks: HashMap<ReqNo, VectorClock>,
    /// The sequence number of the latest message sent to each peer
    /// for each requested feed.
    sent_messages: HashMap<SsbId, HashMap<SsbId, u64>>,
}

impl Default for EbtManager {
    fn default() -> Self {
        EbtManager {
            active_sessions: HashMap::new(),
            _feed_wait_timeout: 3,
            _is_replication_loop_active: false,
            local_clock: HashMap::new(),
            local_id: String::new(),
            peer_clocks: HashMap::new(),
            _requested_feeds: HashSet::new(),
            session_wait_timeout: 5,
            sent_clocks: HashMap::new(),
            sent_messages: HashMap::new(),
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

    /// Retrieve either the local vector clock or the stored vector clock
    /// for the peer represented by the given SSB ID.
    fn get_clock(&self, ssb_id: Option<&SsbId>) -> Option<VectorClock> {
        match ssb_id {
            Some(id) => self.peer_clocks.get(id).cloned(),
            None => Some(self.local_clock.to_owned()),
        }
    }

    /// Set or update the vector clock for the given SSB ID.
    fn set_clock(&mut self, ssb_id: &SsbId, clock: VectorClock) {
        if ssb_id == &self.local_id {
            self.local_clock = clock
        } else {
            self.peer_clocks.insert(ssb_id.to_owned(), clock);
        }
    }

    /// Retrieve the stored vector clock for the first peer, check for the
    /// second peer in the vector clock and return the value of the receive
    /// flag.
    fn _is_receiving(&self, peer_ssb_id: SsbId, ssb_id: SsbId) -> Result<bool> {
        // Retrieve the vector clock for the first peer.
        if let Some(clock) = self.get_clock(Some(&peer_ssb_id)) {
            // Check if the second peer is represented in the vector clock.
            if let Some(encoded_seq_no) = clock.get(&ssb_id) {
                // Check if the receive flag is true.
                if let (_replicate_flag, Some(true), _seq) = clock::decode(*encoded_seq_no)? {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Get the sequence number of the latest message sent to the given
    /// peer SSB ID for the feed represented by the given SSB ID.
    fn _get_latest_sent_seq(self, peer_ssb_id: &SsbId, ssb_id: &SsbId) -> Option<u64> {
        // Get the state of the messages sent to `peer_ssb_id`.
        if let Some(sent_state) = self.sent_messages.get(peer_ssb_id) {
            // Get the sequence number of the latest message sent for feed
            // `ssb_id`.
            sent_state.get(ssb_id).copied()
        } else {
            None
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
    fn register_session(&mut self, peer_ssb_id: &SsbId, req_no: ReqNo, session_role: SessionRole) {
        self.active_sessions
            .insert(peer_ssb_id.to_owned(), (req_no, session_role));

        trace!(target: "ebt-session", "Registered new EBT session {} for {}", req_no, peer_ssb_id);
    }

    /// Remove the given peer from the list of active session.
    fn remove_session(&mut self, peer_ssb_id: &SsbId) {
        let _ = self.active_sessions.remove(peer_ssb_id);
    }

    /// Revoke a replication request for the feed represented by the given SSB
    /// ID.
    fn _revoke(&mut self, peer_id: &SsbId) {
        self.local_clock.remove(peer_id);
    }

    /// Request the feed represented by the given SSB ID from a peer.
    fn _request(&mut self, peer_id: &SsbId) {
        self._requested_feeds.insert(peer_id.to_owned());
    }

    /// Decode the encoded sequence number from a vector clock and push
    /// the latest desired messages to the given vector of messages.
    ///
    /// This method will only push messages to the vector if the replicate
    /// flag is set to `true`.
    async fn retrieve_latest_messages(
        encoded_seq_no: i64,
        feed_id: &SsbId,
        messages: &mut Vec<Value>,
    ) -> Result<()> {
        if encoded_seq_no != -1 {
            if let (_replicate_flag, Some(true), Some(seq)) = clock::decode(encoded_seq_no)? {
                if let Some(last_seq) = KV_STORE.read().await.get_latest_seq(feed_id)? {
                    for n in seq..(last_seq + 1) {
                        if let Some(msg_kvt) = KV_STORE.read().await.get_msg_kvt(feed_id, n)? {
                            messages.push(msg_kvt.value)
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Decode a peer's vector clock and retrieve all requested messages.
    ///
    /// If an SSB ID is supplied, retrieve only the lastest requested
    /// messages authored by that ID.
    ///
    /// If no SSB ID is supplied, retrieve the latest requested messages
    /// for all authors listed in the vector clock.
    async fn retrieve_requested_messages(
        peer_ssb_id: Option<&SsbId>,
        clock: VectorClock,
    ) -> Result<Vec<Value>> {
        let mut messages_to_be_sent = Vec::new();

        // We only want to retrieve messages authored by `peer_ssb_id`.
        if let Some(feed_id) = peer_ssb_id {
            if let Some(encoded_seq_no) = clock.get(feed_id) {
                EbtManager::retrieve_latest_messages(
                    *encoded_seq_no,
                    feed_id,
                    &mut messages_to_be_sent,
                )
                .await?;
            }
        } else {
            // We want to retrieve messages for all feeds in the vector clock.
            for (feed_id, encoded_seq_no) in clock.iter() {
                EbtManager::retrieve_latest_messages(
                    *encoded_seq_no,
                    feed_id,
                    &mut messages_to_be_sent,
                )
                .await?;
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
            if !self.active_sessions.contains_key(&peer_ssb_id) {
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

        self.register_session(&peer_ssb_id, req_no, session_role.to_owned());
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

    fn handle_send_clock(&mut self, req_no: ReqNo, clock: VectorClock) -> Option<VectorClock> {
        self.sent_clocks.insert(req_no, clock)
    }

    async fn handle_received_clock(
        &mut self,
        req_no: ReqNo,
        peer_ssb_id: SsbId,
        clock: VectorClock,
    ) -> Result<()> {
        trace!(target: "ebt-replication", "Received vector clock: {:?}", clock);

        // Update the stored vector clock for the remote peer.
        self.set_clock(&peer_ssb_id, clock.to_owned());

        // Create channel to send messages to broker.
        let mut ch_broker = BROKER.lock().await.create_sender();

        // If a clock is received without a prior EBT replicate
        // request having been received from the associated peer, it is
        // assumed that the clock was sent in response to a locally-sent
        // EBT replicate request. Ie. the session was requested by the
        // local peer.
        if !self.active_sessions.contains_key(&peer_ssb_id) {
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

        // If we have not previously sent a clock, send one now.
        //
        // This indicates that the local peer is acting as the session
        // requester.
        if self.sent_clocks.get(&req_no).is_none() {
            let local_clock = self.local_clock.to_owned();
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Ebt(EbtEvent::SendClock(req_no, local_clock)),
                ))
                .await?;
        }

        // We want messages for all feeds in the clock, therefore the
        // `peer_ssb_id` parameter is set to `None`.
        let msgs = EbtManager::retrieve_requested_messages(None, clock).await?;
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

    async fn handle_send_message(&mut self, peer_ssb_id: SsbId, msg: Value) -> Result<()> {
        // Update the hashmap of sent messages.
        //
        // For each peer, keep a list of feed ID's and the sequence of the
        // latest sent message for each. This is useful to consult when a new
        // message is appended to the local store and may need to be sent to
        // peers with whom we have an active EBT session.

        let msg_author = msg["author"]
            .as_str()
            .ok_or(Error::OptionIsNone)?
            .to_string();
        let msg_sequence = msg["sequence"].as_u64().ok_or(Error::OptionIsNone)?;

        if let Some(feeds) = self.sent_messages.get_mut(&peer_ssb_id) {
            feeds.insert(msg_author, msg_sequence);
        } else {
            let mut feeds = HashMap::new();
            feeds.insert(msg_author, msg_sequence);
            self.sent_messages.insert(peer_ssb_id, feeds);
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

            // Create channel to send messages to broker.
            let mut ch_broker = BROKER.lock().await.create_sender();

            // Extract blob references from the received message and
            // request those blobs if they are not already in the local
            // blobstore.
            for key in blobs::extract_blob_refs(&msg) {
                if !BLOB_STORE.read().await.exists(&key) {
                    let event = RpcBlobsGetEvent(BlobsGetIn::new(key));
                    let broker_msg =
                        BrokerEvent::new(Destination::Broadcast, BrokerMessage::RpcBlobsGet(event));
                    ch_broker.send(broker_msg).await?;
                }
            }
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

    /// Look up the latest sequence number for the updated feed, encode it as
    /// the single entry of a vector clock and send that to any active session
    /// peers.
    async fn handle_local_store_updated(&self, ssb_id: SsbId) -> Result<()> {
        // Iterate over all active EBT sessions.
        for (_peer_ssb_id, (req_no, _session_role)) in self.active_sessions.iter() {
            // Look up the latest sequence for the given ID.
            if let Some(seq) = KV_STORE.read().await.get_latest_seq(&ssb_id)? {
                // Encode the replicate flag, receive flag and sequence.
                let encoded_value: EncodedClockValue = clock::encode(true, Some(true), Some(seq))?;

                // Update the entry for `ssb_id` in the local vector clock.
                if let Some(mut local_clock) = self.get_clock(None) {
                    local_clock.insert(ssb_id.to_owned(), encoded_value);
                }

                // Create a vector clock with a single entry.
                let mut updated_clock = HashMap::new();
                updated_clock.insert(ssb_id.to_owned(), encoded_value);

                // Create channel to send messages to broker.
                let mut ch_broker = BROKER.lock().await.create_sender();

                // Send the single-entry vector clock to the active session.
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        BrokerMessage::Ebt(EbtEvent::SendClock(*req_no, updated_clock)),
                    ))
                    .await?;
            }
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

    async fn handle_error(
        &mut self,
        connection_data: ConnectionData,
        req_no: ReqNo,
        peer_ssb_id: SsbId,
        err_msg: String,
    ) -> Result<()> {
        trace!(target: "ebt-replication", "Session error with {} for request number {}: {}", peer_ssb_id, req_no, err_msg);

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
                            EbtEvent::SendClock(req_no, clock) => {
                                trace!(target: "ebt-replication", "Sending vector clock: {:?}", clock);
                                let _ = self.handle_send_clock(req_no, clock);
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
                            EbtEvent::SendMessage(_req_no, peer_ssb_id, msg) => {
                                trace!(target: "ebt-replication", "Sending message: {:?}...", msg);
                                if let Err(err) = self.handle_send_message(peer_ssb_id, msg).await {
                                    error!("Error while handling 'send message' event: {}", err)
                                }
                            }
                            EbtEvent::SessionConcluded(connection_data) => {
                                self.handle_session_concluded(connection_data).await;
                            }
                            EbtEvent::SessionTimeout(connection_data) => {
                                if let Err(err) = self.handle_session_timeout(connection_data).await {
                                    error!("Error while handling 'session timeout' event: {}", err)
                                }
                            }
                            EbtEvent::Error(connection_data, req_no, peer_ssb_id, err_msg) => {
                                if let Err(err) = self.handle_error(connection_data, req_no, peer_ssb_id, err_msg).await {
                                    error!("Error while handling 'error' event: {}", err)
                                }
                            }
                        }
                    } else if let Some(BrokerMessage::StoreKv(StoreKvEvent(ssb_id))) = msg {
                        debug!("Received KV store event from broker");

                        // Respond to a key-value store state change for the given peer.
                        // This is triggered when a new message is appended to the local feed.
                        if let Err(err) = self.handle_local_store_updated(ssb_id).await {
                            error!("Error while handling 'local store updated' event: {}", err)
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
