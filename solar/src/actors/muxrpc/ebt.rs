//! Epidemic Broadcast Tree (EBT) Replication Handler.

use std::{collections::HashMap, marker::PhantomData};

use async_std::io::Write;
use futures::SinkExt;
use kuska_ssb::{
    api::{
        dto::{self, content::SsbId},
        ApiCaller, ApiMethod,
    },
    feed::{Feed as MessageKvt, Message},
    rpc,
};
use log::{trace, warn};

use crate::{
    actors::{
        muxrpc::{ReqNo, RpcInput},
        replication::ebt::{EbtEvent, SessionRole},
    },
    broker::{BrokerEvent, BrokerMessage, ChBrokerSend, Destination, BROKER},
    error::Error,
    Result,
};

/// EBT replicate handler. Tracks active requests and peer connections.
pub struct EbtReplicateHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    /// EBT-related requests which are known and allowed.
    active_requests: HashMap<ReqNo, SsbId>,
    phantom: PhantomData<W>,
}

impl<W> EbtReplicateHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    /// Handle an RPC event.
    pub async fn handle(
        &mut self,
        api: &mut ApiCaller<W>,
        op: &RpcInput,
        ch_broker: &mut ChBrokerSend,
        peer_ssb_id: String,
        active_request: Option<ReqNo>,
        connection_id: usize,
    ) -> Result<bool> {
        trace!(target: "muxrpc-ebt-handler", "Received MUXRPC input: {:?}", op);

        // An outbound EBT replicate request was made before the handler was
        // called; add it to the map of active requests.
        if let Some(session_req_no) = active_request {
            let _ = self
                .active_requests
                .insert(session_req_no, peer_ssb_id.to_owned());
        }

        match op {
            // Handle an incoming MUXRPC request.
            RpcInput::Network(req_no, rpc::RecvMsg::RpcRequest(req)) => {
                match ApiMethod::from_rpc_body(req) {
                    Some(ApiMethod::EbtReplicate) => {
                        self.recv_ebtreplicate(api, *req_no, req, peer_ssb_id, connection_id)
                            .await
                    }
                    _ => Ok(false),
                }
            }
            RpcInput::Network(req_no, rpc::RecvMsg::OtherRequest(_type, req)) => {
                // Attempt to deserialize bytes into vector clock hashmap.
                // If the deserialization is successful, emit a 'received clock'
                // event.
                if let Ok(clock) = serde_json::from_slice(req) {
                    ch_broker
                        .send(BrokerEvent::new(
                            Destination::Broadcast,
                            BrokerMessage::Ebt(EbtEvent::ReceivedClock(
                                connection_id,
                                *req_no,
                                peer_ssb_id,
                                clock,
                            )),
                        ))
                        .await?;
                }

                Ok(false)
            }
            // Handle an incoming MUXRPC response.
            RpcInput::Network(req_no, rpc::RecvMsg::RpcResponse(_type, res)) => {
                self.recv_rpc_response(ch_broker, *req_no, res, peer_ssb_id, connection_id)
                    .await
            }
            // Handle an incoming MUXRPC 'cancel stream' response.
            RpcInput::Network(req_no, rpc::RecvMsg::CancelStreamResponse()) => {
                self.recv_cancelstream(api, *req_no).await
            }
            // Handle an incoming MUXRPC error response.
            RpcInput::Network(req_no, rpc::RecvMsg::ErrorResponse(err)) => {
                self.recv_error_response(*req_no, err).await
            }
            // Handle a broker message.
            RpcInput::Message(msg) => match msg {
                BrokerMessage::Ebt(EbtEvent::SendClock(conn_id, req_no, clock)) => {
                    // Only send the clock if the associated connection is
                    // being handled by this instance of the handler.
                    //
                    // This prevents the clock being sent to every peer with
                    // whom we have an active session and matching request
                    // number.
                    if *conn_id == connection_id {
                        // Serialize the vector clock as a JSON string.
                        let json_clock = serde_json::to_string(&clock)?;
                        // The request number must be negative (response).
                        api.ebt_clock_res_send(-(*req_no), &json_clock).await?;

                        trace!(target: "ebt", "Sent clock to connection {} with request number {}", conn_id, req_no);
                    }

                    Ok(false)
                }
                BrokerMessage::Ebt(EbtEvent::SendMessage(conn_id, req_no, ssb_id, msg)) => {
                    // Only send the message if the associated connection is
                    // being handled by this instance of the handler.
                    //
                    // This prevents the message being sent to every peer with
                    // whom we have an active session and matching request
                    // number.
                    if *conn_id == connection_id {
                        // TODO: Remove this check; made redundant by the
                        // connection ID check.
                        //
                        // Ensure the message is sent to the correct peer.
                        if peer_ssb_id == *ssb_id {
                            let json_msg = msg.to_string();
                            // The request number must be negative (response).
                            api.ebt_feed_res_send(-(*req_no), &json_msg).await?;

                            trace!(target: "ebt", "Sent message to {} on connection {}", ssb_id, conn_id);
                        }
                    }

                    Ok(false)
                }
                _ => Ok(false),
            },
            _ => Ok(false),
        }
    }
}

impl<W> EbtReplicateHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    /// Instantiate a new instance of `EbtReplicateHandler`.
    pub fn new() -> Self {
        Self {
            active_requests: HashMap::new(),
            phantom: PhantomData,
        }
    }

    /// Process and respond to an incoming EBT replicate request.
    async fn recv_ebtreplicate(
        &mut self,
        api: &mut ApiCaller<W>,
        req_no: ReqNo,
        req: &rpc::Body,
        peer_ssb_id: String,
        connection_id: usize,
    ) -> Result<bool> {
        // Deserialize the args from an incoming EBT replicate request.
        let mut args: Vec<dto::EbtReplicate> = serde_json::from_value(req.args.clone())?;
        trace!(target: "ebt-handler", "Received replicate request: {:?}", args);

        // Retrieve the `EbtReplicate` args from the array.
        let args = args.pop().unwrap();

        let mut ch_broker = BROKER.lock().await.create_sender();

        // Validate the EBT request args (`version` and `format`).
        // Terminate the stream with an error response if expectations are
        // not met.
        if !args.version == 3 {
            let err_msg = String::from("ebt version != 3");
            api.rpc().send_error(req_no, req.rpc_type, &err_msg).await?;

            return Err(Error::EbtReplicate((req_no, err_msg)));
        } else if args.format.as_str() != "classic" {
            let err_msg = String::from("ebt format != classic");
            api.rpc().send_error(req_no, req.rpc_type, &err_msg).await?;

            return Err(Error::EbtReplicate((req_no, err_msg)));
        }

        trace!(target: "ebt-handler", "Successfully validated replicate request arguments");

        // Insert the request number and peer public key into the active
        // requests map.
        self.active_requests.insert(req_no, peer_ssb_id.to_owned());

        ch_broker
            .send(BrokerEvent::new(
                Destination::Broadcast,
                BrokerMessage::Ebt(EbtEvent::SessionInitiated(
                    connection_id,
                    req_no,
                    peer_ssb_id,
                    SessionRole::Responder,
                )),
            ))
            .await?;

        Ok(false)
    }

    /// Process an incoming MUXRPC response.
    /// The response is expected to contain a vector clock or an SSB message.
    async fn recv_rpc_response(
        &mut self,
        ch_broker: &mut ChBrokerSend,
        req_no: ReqNo,
        res: &[u8],
        peer_ssb_id: String,
        connection_id: usize,
    ) -> Result<bool> {
        trace!(target: "ebt-handler", "Received RPC response: {}", req_no);

        // Only handle the response if the associated request number is known
        // to us, either because we sent or received the initiating replicate
        // request.
        if self.active_requests.contains_key(&req_no) {
            // The response may be a vector clock (aka. notes) or an SSB message.
            //
            // Since there is no explicit way to determine which was received,
            // we first attempt deserialization of a vector clock and move on
            // to attempting message deserialization if that fails.
            if let Ok(clock) = serde_json::from_slice(res) {
                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        BrokerMessage::Ebt(EbtEvent::ReceivedClock(
                            connection_id,
                            req_no,
                            peer_ssb_id,
                            clock,
                        )),
                    ))
                    .await?;
            } else {
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

                ch_broker
                    .send(BrokerEvent::new(
                        Destination::Broadcast,
                        BrokerMessage::Ebt(EbtEvent::ReceivedMessage(msg)),
                    ))
                    .await?;
            }
        }

        Ok(false)
    }

    /// Remove the associated request from the map of active requests and close
    /// the stream.
    async fn recv_cancelstream(&mut self, api: &mut ApiCaller<W>, req_no: ReqNo) -> Result<bool> {
        trace!(target: "ebt-handler", "Received cancel stream RPC response: {}", req_no);

        self.active_requests.remove(&req_no);
        api.rpc().send_stream_eof(-req_no).await?;

        Ok(true)
    }

    /// Report a MUXRPC error and remove the associated request from the map of
    /// active requests.
    async fn recv_error_response(&mut self, req_no: ReqNo, err_msg: &str) -> Result<bool> {
        warn!("Received MUXRPC error response: {}", err_msg);

        self.active_requests.remove(&req_no);

        Err(Error::EbtReplicate((req_no, err_msg.to_string())))
    }
}
