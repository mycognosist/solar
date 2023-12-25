use std::{collections::HashMap, marker::PhantomData, string::ToString};

use async_std::io::Write;
use async_trait::async_trait;
use futures::SinkExt;
use kuska_ssb::{
    api::{dto, ApiCaller, ApiMethod},
    feed::{Feed as MessageKvt, Message},
    rpc,
};
use log::{debug, info, warn};

use crate::{
    actors::{
        muxrpc::{
            blobs_get::RpcBlobsGetEvent,
            handler::{RpcHandler, RpcInput},
        },
        replication::blobs,
    },
    broker::{BrokerEvent, BrokerMessage, ChBrokerSend, Destination},
    config::{PEERS_TO_REPLICATE, RESYNC_CONFIG, SECRET_CONFIG},
    error::Error,
    node::BLOB_STORE,
    node::KV_STORE,
    storage::kv::StoreKvEvent,
    Result,
};

#[derive(Debug)]
struct HistoryStreamRequest {
    req_no: i32,
    args: dto::CreateHistoryStreamIn,
    from: u64, // check, not sure if ok
}

/// History stream handler. Tracks active requests and peer connections.
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
            // Handle an incoming MUXRPC response.
            RpcInput::Network(req_no, rpc::RecvMsg::RpcResponse(_type, res)) => {
                self.recv_rpc_response(api, ch_broker, *req_no, res).await
            }
            // Handle an incoming MUXRPC 'cancel stream' response.
            RpcInput::Network(req_no, rpc::RecvMsg::CancelStreamResponse()) => {
                self.recv_cancelstream(api, *req_no).await
            }
            // Handle an incoming MUXRPC error response.
            RpcInput::Network(req_no, rpc::RecvMsg::ErrorResponse(err)) => {
                self.recv_error_response(api, *req_no, err).await
            }
            // Handle a broker message.
            RpcInput::Message(BrokerMessage::StoreKv(StoreKvEvent(id))) => {
                // Notification from the key-value store indicating that
                // a new message has just been appended to the feed
                // identified by `id`.
                return self.recv_storageevent_idchanged(api, id).await;
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
            debug!("initializing history stream handler");

            // If local database resync has been selected...
            if *RESYNC_CONFIG.get().ok_or(Error::OptionIsNone)? {
                info!("database resync selected; requesting local feed from peers");
                // Read the local public key from the secret config file.
                // The public key is @-prefixed (at-prefixed).
                let local_public_key = &SECRET_CONFIG.get().ok_or(Error::OptionIsNone)?.public_key;
                // Create a history stream request for the local feed.
                let args =
                    dto::CreateHistoryStreamIn::new(local_public_key.to_owned()).after_seq(1);
                let req_id = api.create_history_stream_req_send(&args).await?;

                // Insert the history stream request ID and peer public key
                // into the peers hash map.
                self.peers.insert(req_id, local_public_key.to_owned());
            }

            // Loop through the public keys of all peers in the replication list.
            for peer_pk in PEERS_TO_REPLICATE.get().unwrap().keys() {
                // Instantiate the history stream request args for the given peer.
                // The `live` arg means: keep the connection open after initial
                // replication.
                let mut args = dto::CreateHistoryStreamIn::new(peer_pk.to_owned()).live(true);

                // Retrieve the sequence number of the most recent message for
                // this peer from the local key-value store.
                if let Some(last_seq) = KV_STORE.read().await.get_latest_seq(peer_pk)? {
                    // Use the latest sequence number to update the request args.
                    args = args.after_seq(last_seq);
                }

                // Send the history stream request.
                let id = api.create_history_stream_req_send(&args).await?;

                // Insert the history stream request ID and peer ID
                // (public key) into the peers hash map.
                self.peers.insert(id, peer_pk.to_owned());

                info!(
                    "requesting messages authored by peer {} after {:?}",
                    peer_pk, args.seq
                );
            }

            self.initialized = true;
        }

        Ok(false)
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
            // First try to deserialize the response into a message value.
            // If that fails, try to deserialize into a message KVT and then
            // convert that into a message value. Return an error if that fails.
            // This approach allows us to handle the unlikely event that
            // messages are sent as KVTs and not simply values.
            //
            // Validation of the message signature and fields is also performed
            // as part of the call to `from_slice`.
            let msg = match Message::from_slice(res) {
                Ok(msg) => msg,
                Err(_) => MessageKvt::from_slice(res)?.into_message()?,
            };

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

                info!(
                    "received msg number {} from {}",
                    msg.sequence(),
                    msg.author()
                );

                // Extract blob references from the received message and
                // request those blobs if they are not already in the local
                // blobstore.
                for key in blobs::extract_blob_refs(&msg) {
                    if !BLOB_STORE.read().await.exists(&key) {
                        let event = RpcBlobsGetEvent(dto::BlobsGetIn::new(key));
                        let broker_msg = BrokerEvent::new(
                            Destination::Broadcast,
                            BrokerMessage::RpcBlobsGet(event),
                        );
                        ch_broker.send(broker_msg).await.unwrap();
                    }
                }
            } else {
                warn!(
                    "received out-of-order msg from {}; recv: {} db: {}",
                    &msg.author().to_string(),
                    msg.sequence(),
                    last_seq
                );

                // Return to avoid handling multiple successive out-of-order
                // messages.
                return Ok(true);
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
        // Retrieve the `CreateHistoryStreamIn` args from the array.
        let args = args.pop().unwrap();

        // Define the first message in the sequence to be sent to the requester.
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

    /// Respond to a key-value store state change for the given peer.
    /// This is triggered when a new message is appended to the local feed.
    /// Remove the peer from the list of active streams, send the requested
    /// messages from the local feed to the peer and then reinsert the public
    /// key of the peer to the list of active streams.
    async fn recv_storageevent_idchanged(
        &mut self,
        api: &mut ApiCaller<W>,
        id: &str,
    ) -> Result<bool> {
        // Attempt to remove the peer from the list of active streams.
        if let Some(mut req) = self.reqs.remove(id) {
            // Send local messages to the peer.
            self.send_history(api, &mut req).await?;
            // Reinsert the peer into the list of active streams.
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
        // Determine the public key of the feed being requested.
        let req_id = if req.args.id.starts_with('@') {
            req.args.id.clone()
        } else {
            format!("@{}", req.args.id).to_string()
        };

        // Lookup the sequence number of the most recently published message
        // in the local feed.
        let last_seq = KV_STORE.read().await.get_latest_seq(&req_id)?.unwrap_or(0);

        // Determine if the messages should be sent as message values or as
        // message KVTs (Key Value Timestamp).
        // Defaults to message values if unset.
        let with_keys = req.args.keys.unwrap_or(false);

        // Only send messages that the peer doesn't already have
        // (ie. if the requested `from` sequence number is smaller than or
        // equal to the latest sequence number for that feed in the local
        // database).
        if req.from <= last_seq {
            // Determine the public key of the peer who requested the history
            // stream.
            let requester = self
                .find_key_by_req_no(req.req_no)
                .unwrap_or_else(|| "unknown".to_string());

            info!(
                "sending messages authored by {} to {} (from sequence {} to {})",
                req.args.id, requester, req.from, last_seq
            );

            // Iterate over the range of requested messages, read them from the
            // local key-value database and send them to the requesting peer.
            // The "to" value (`last_seq`) is exclusive so we need to add one to
            // include it in the range.
            for n in req.from..(last_seq + 1) {
                let data = KV_STORE.read().await.get_msg_kvt(&req_id, n)?.unwrap();
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
        }

        Ok(())
    }
}
