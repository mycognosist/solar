use std::{collections::HashMap, marker::PhantomData, string::ToString};

use async_std::io::Write;
use async_trait::async_trait;
use futures::SinkExt;
use kuska_ssb::{
    api::{dto, ApiCaller, ApiMethod},
    feed::Message,
    rpc,
};
use log::{debug, info, warn};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::{
    actors::rpc::handler::{RpcHandler, RpcInput},
    broker::{BrokerEvent, ChBrokerSend, Destination},
    storage::kv::StoKvEvent,
    Result, BLOB_STORAGE, KV_STORAGE, REPLICATION_CONFIG, SECRET_CONFIG,
};

/// Regex pattern used to match blob references.
pub static BLOB_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(&[0-9A-Za-z/+=]*.sha256)").unwrap());

#[derive(Debug)]
struct HistoryStreamRequest {
    req_no: i32,
    args: dto::CreateHistoryStreamIn,
    from: u64, // check, not sure if ok
}

pub struct HistoryStreamHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    initialized: bool,
    _actor_id: usize,
    reqs: HashMap<String, HistoryStreamRequest>,
    peers: HashMap<i32, String>,
    phantom: PhantomData<W>,
}

#[async_trait]
impl<W> RpcHandler<W> for HistoryStreamHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    fn name(&self) -> &'static str {
        "HistoryStreamHandler"
    }

    /// Handle an RPC event.
    async fn handle(
        &mut self,
        api: &mut ApiCaller<W>,
        op: &RpcInput,
        ch_broker: &mut ChBrokerSend,
    ) -> Result<bool> {
        match op {
            // Handle an incoming MUXRPC request.
            RpcInput::Network(req_no, rpc::RecvMsg::RpcRequest(req)) => {
                match ApiMethod::from_rpc_body(req) {
                    Some(ApiMethod::CreateHistoryStream) => {
                        self.recv_createhistorystream(api, *req_no, req).await
                    }
                    _ => Ok(false),
                }
            }
            // Handle an outgoing MUXRPC response.
            RpcInput::Network(req_no, rpc::RecvMsg::RpcResponse(_type, res)) => {
                self.recv_rpc_response(api, ch_broker, *req_no, res).await
            }
            // Handle an outgoing MUXRPC 'cancel stream' response.
            RpcInput::Network(req_no, rpc::RecvMsg::CancelStreamRespose()) => {
                self.recv_cancelstream(api, *req_no).await
            }
            // Handle an outgoing MUXRPC error response.
            RpcInput::Network(req_no, rpc::RecvMsg::ErrorResponse(err)) => {
                self.recv_error_response(api, *req_no, err).await
            }
            // Handle a broker message.
            RpcInput::Message(msg) => {
                if let Some(kv_event) = msg.downcast_ref::<StoKvEvent>() {
                    match kv_event {
                        StoKvEvent::IdChanged(id) => {
                            return self.recv_storageevent_idchanged(api, id).await
                        }
                    }
                }
                Ok(false)
            }
            // Handle a timer event.
            RpcInput::Timer => self.on_timer(api).await,
            _ => Ok(false),
        }
    }
}

impl<W> HistoryStreamHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    /// Instantiate a new instance of `HistoryStreamHandler` with the given
    /// actor ID.
    pub fn new(actor_id: usize) -> Self {
        Self {
            _actor_id: actor_id,
            initialized: false,
            peers: HashMap::new(),
            reqs: HashMap::new(),
            phantom: PhantomData,
        }
    }

    /// Initialize the history stream handler.
    ///
    /// Calls `create_history_stream` for every peer in the replication list,
    /// requesting the latest messages.
    async fn on_timer(&mut self, api: &mut ApiCaller<W>) -> Result<bool> {
        if !self.initialized {
            debug!(target: "solar", "initializing historystreamhandler");

            // Create a history stream request for the local feed.
            let args = dto::CreateHistoryStreamIn::new(SECRET_CONFIG.get().unwrap().id.clone());
            // NOTE: I don't understand why this is being called.
            // We are essentially requesting our own feed.
            let _ = api.create_history_stream_req_send(&args).await?;

            // Loop through all peers in the replication list.
            for peer in &REPLICATION_CONFIG.get().unwrap().peers {
                // Instantiate the history stream request args for the given peer.
                let mut args = dto::CreateHistoryStreamIn::new(peer.to_string()).live(true);

                // Retrieve the sequence number of the most recent message for
                // this peer from the local key-value store.
                if let Some(last_seq) = KV_STORAGE.read().await.get_latest_seq(peer)? {
                    // Use the latest sequence number to update the request args.
                    args = args.after_seq(last_seq);
                }

                // Send the history stream request.
                let id = api.create_history_stream_req_send(&args).await?;

                // Insert the history stream request ID and peer ID
                // (public key) into the peers hash map.
                self.peers.insert(id, peer.to_string());

                debug!(target: "solar", "requesting feeds from peer {} starting with {:?}", peer, args.seq);
            }

            self.initialized = true;
        }

        Ok(false)
    }

    /// Extract blob references from post-type messages.
    fn extract_blob_refs(&mut self, msg: &Message) -> Vec<String> {
        let mut refs = Vec::new();

        let msg = serde_json::from_value(msg.content().clone());

        if let Ok(dto::content::TypedMessage::Post { text, .. }) = msg {
            for cap in BLOB_REGEX.captures_iter(&text) {
                let key = cap.get(0).unwrap().as_str().to_owned();
                refs.push(key);
            }
        }

        refs
    }

    /// Process an incoming MUXRPC response. The response is expected to
    /// contain an SSB message.
    async fn recv_rpc_response(
        &mut self,
        _api: &mut ApiCaller<W>,
        ch_broker: &mut ChBrokerSend,
        req_no: i32,
        res: &[u8],
    ) -> Result<bool> {
        // Only handle the response if we made the request.
        if self.peers.contains_key(&req_no) {
            // Deserialize a message value from the response.
            let msg = Message::from_slice(res)?;
            // Retrieve the sequence number of the most recent message for
            // the peer that authored the received message.
            let last_seq = KV_STORAGE
                .read()
                .await
                .get_latest_seq(&msg.author().to_string())?
                .unwrap_or(0);

            // Validate the sequence number.
            if msg.sequence() == last_seq + 1 {
                // Append the message to the feed.
                KV_STORAGE.write().await.append_feed(msg.clone()).await?;

                info!("Received msg no {} from {}", msg.author(), msg.sequence());

                // Extract blob references from the received message and
                // request those blobs if they are not already in the local
                // blobstore.
                for key in self.extract_blob_refs(&msg) {
                    if !BLOB_STORAGE.read().await.exists(&key) {
                        let event = super::blobs_get::RpcBlobsGetEvent::Get(dto::BlobsGetIn {
                            key,
                            size: None,
                            max: None,
                        });
                        let broker_msg = BrokerEvent::new(Destination::Broadcast, event);
                        ch_broker.send(broker_msg).await.unwrap();
                    }
                }
            } else {
                warn!(
                    "Received out-of-order message from {}; recv: {} db: {}",
                    &msg.author().to_string(),
                    msg.sequence(),
                    last_seq
                );
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Process and respond to an incoming history stream request.
    async fn recv_createhistorystream(
        &mut self,

        api: &mut ApiCaller<W>,
        req_no: i32,
        req: &rpc::Body,
    ) -> Result<bool> {
        // Deserialize the args from an incoming history stream request.
        let mut args: Vec<dto::CreateHistoryStreamIn> = serde_json::from_value(req.args.clone())?;

        // Why do we pop the args? Related: why do we deserialize into a
        // vec in the first place?
        // TODO: add some debug logging to understand this.
        let args = args.pop().unwrap();
        // Define the sequence number for the history stream query.
        // This marks the first message in the sequence we will send to the
        // requester.
        let from = args.seq.unwrap_or(1u64);

        let mut req = HistoryStreamRequest { args, from, req_no };

        // Send the requested messages from the local feed.
        self.send_history(api, &mut req).await?;

        if req.args.live.unwrap_or(false) {
            // Keep the stream open for communication.
            self.reqs.insert(req.args.id.clone(), req);
        } else {
            // Send an end of file response to the caller.
            api.rpc().send_stream_eof(req_no).await?;
        }

        Ok(true)
    }

    /// Close the stream and remove the public key of the peer from the list
    /// of active streams (`reqs`).
    async fn recv_cancelstream(&mut self, api: &mut ApiCaller<W>, req_no: i32) -> Result<bool> {
        if let Some(key) = self.find_key_by_req_no(req_no) {
            api.rpc().send_stream_eof(-req_no).await?;
            self.reqs.remove(&key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Report a MUXRPC error and remove the public key of the peer from the
    /// list of active streams (`reqs`).
    async fn recv_error_response(
        &mut self,
        _api: &mut ApiCaller<W>,
        req_no: i32,
        error_msg: &str,
    ) -> Result<bool> {
        if let Some(key) = self.find_key_by_req_no(req_no) {
            warn!("MUXRPC error {}", error_msg);
            self.reqs.remove(&key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    // TODO: figure out how / when this is used.
    async fn recv_storageevent_idchanged(
        &mut self,
        api: &mut ApiCaller<W>,
        id: &str,
    ) -> Result<bool> {
        // If the given public key is in the list of active streams, remove it,
        // send the requested messages from the local feed and then readd
        // the public key to the list of active streams.
        if let Some(mut req) = self.reqs.remove(id) {
            self.send_history(api, &mut req).await?;
            self.reqs.insert(id.to_string(), req);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Return the public key matching a given MUXRPC request.
    /// In other words, return the author ID of a request.
    fn find_key_by_req_no(&self, req_no: i32) -> Option<String> {
        self.reqs
            .iter()
            .find(|(_, val)| val.req_no == req_no)
            .map(|(key, _)| key.clone())
    }

    /// Send a stream of messages from the local key-value database to a peer.
    async fn send_history(
        &mut self,
        api: &mut ApiCaller<W>,
        req: &mut HistoryStreamRequest,
    ) -> Result<()> {
        // Determine the public key of the peer requesting the feed.
        let req_id = if req.args.id.starts_with('@') {
            req.args.id.clone()
        } else {
            format!("@{}", req.args.id).to_string()
        };

        // Lookup the sequence number of the most recently published message
        // in the local feed.
        let last_seq = KV_STORAGE
            .read()
            .await
            .get_latest_seq(&req_id)?
            .map_or(0, |x| x + 1);
        // Determine if the messages should be sent as message values or as
        // message KVTs (Key Value Timestamp).
        let with_keys = req.args.keys.unwrap_or(true);

        info!(
            "Sending history stream to {} (from sequence {} to {})",
            req.args.id, req.from, last_seq
        );

        // Iterate over the range of requested messages, read them from the
        // local key-value database and send them to the requesting peer.
        for n in req.from..last_seq {
            let data = KV_STORAGE.read().await.get_msg_kvt(&req_id, n)?.unwrap();
            // Send either the whole KVT or just the value.
            let data = if with_keys {
                data.to_string()
            } else {
                data.value.to_string()
            };
            api.feed_res_send(req.req_no, &data).await?;
        }

        // Update the starting sequence number for the request.
        req.from = last_seq;

        Ok(())
    }
}
