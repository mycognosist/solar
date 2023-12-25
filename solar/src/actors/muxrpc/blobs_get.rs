#![allow(clippy::single_match)]

use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
};

use async_std::io::Write;
use async_trait::async_trait;
use kuska_ssb::{
    api::{dto, ApiCaller, ApiMethod},
    rpc,
};
use log::{info, trace, warn};

use crate::{
    actors::muxrpc::handler::{RpcHandler, RpcInput},
    broker::{BrokerMessage, ChBrokerSend},
    node::BLOB_STORE,
    storage::blob::ToBlobHashId,
    Result,
};

/// Regex pattern used to match blob references.
pub static BLOB_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(&[0-9A-Za-z/+=]*.sha256)").unwrap());

/// Extract blob references from post-type messages.
pub fn extract_blob_refs(msg: &Message) -> Vec<String> {
    let mut refs = Vec::new();

    let msg = serde_json::from_value(msg.content().clone());

    if let Ok(TypedMessage::Post { text, .. }) = msg {
        for cap in BLOB_REGEX.captures_iter(&text) {
            let key = cap.get(0).unwrap().as_str().to_owned();
            refs.push(key);
        }
    }

    refs
}

#[derive(Debug, Clone)]
pub struct RpcBlobsGetEvent(pub dto::BlobsGetIn);

pub struct BlobsGetHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    incoming_reqs: HashSet<i32>,
    outcoming_reqs: HashMap<i32, String>,
    phantom: PhantomData<W>,
}

impl<W> Default for BlobsGetHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    fn default() -> Self {
        Self {
            incoming_reqs: HashSet::new(),
            outcoming_reqs: HashMap::new(),
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<W> RpcHandler<W> for BlobsGetHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    fn name(&self) -> &'static str {
        "BlobsGetHandler"
    }

    async fn handle(
        &mut self,
        api: &mut ApiCaller<W>,
        op: &RpcInput,
        _ch_broker: &mut ChBrokerSend,
    ) -> Result<bool> {
        match op {
            RpcInput::Network(req_no, rpc::RecvMsg::RpcRequest(req)) => {
                match ApiMethod::from_rpc_body(req) {
                    Some(ApiMethod::BlobsGet) => return self.recv_get(api, *req_no, req).await,
                    _ => {}
                }
            }
            RpcInput::Network(req_no, rpc::RecvMsg::CancelStreamResponse()) => {
                return self.recv_cancelstream(api, *req_no).await;
            }
            RpcInput::Network(req_no, rpc::RecvMsg::RpcResponse(_type, res)) => {
                return self.recv_rpc_response(api, *req_no, res).await;
            }
            RpcInput::Message(BrokerMessage::RpcBlobsGet(RpcBlobsGetEvent(req))) => {
                return self.event_get(api, req).await;
            }
            _ => {}
        }

        Ok(false)
    }
}

impl<W> BlobsGetHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    async fn recv_get(
        &mut self,
        api: &mut ApiCaller<W>,
        req_no: i32,
        req: &rpc::Body,
    ) -> Result<bool> {
        let mut args: Vec<dto::BlobsGetIn> = serde_json::from_value(req.args.clone())?;
        let args = args.pop().unwrap();

        trace!(target: "ssb-blob", "requested blob {}", args.key);

        let data = BLOB_STORE.read().await.get(&args.key)?;

        if let Some(expected_size) = args.size {
            if data.len() != expected_size as usize {
                trace!(target: "ssb-blob", "not sending blob: blob.len != expected");
                api.rpc()
                    .send_error(req_no, req.rpc_type, "blob.len != expected")
                    .await?;
                return Ok(true);
            }
        }

        if let Some(max) = args.max {
            if data.len() > max as usize {
                trace!(target: "ssb-blob", "not sending blob: blob.len > max");
                api.rpc()
                    .send_error(req_no, req.rpc_type, "blob.len > max")
                    .await?;
                return Ok(true);
            }
        }

        api.blobs_get_res_send(req_no, &data).await?;
        self.incoming_reqs.insert(req_no);

        info!("Sent blob {}", args.key);

        Ok(true)
    }

    async fn recv_cancelstream(&mut self, _api: &mut ApiCaller<W>, req_no: i32) -> Result<bool> {
        Ok(self.incoming_reqs.remove(&req_no))
    }

    async fn recv_rpc_response(
        &mut self,
        _api: &mut ApiCaller<W>,
        req_no: i32,
        res: &[u8],
    ) -> Result<bool> {
        if let Some(expected_blob_id) = self.outcoming_reqs.remove(&req_no) {
            let received_blob_id = res.blob_hash_id();
            if received_blob_id != expected_blob_id {
                warn!(
                    "Received a blob with bad hash, received={} expected={}",
                    received_blob_id, expected_blob_id
                );
            } else {
                info!("Received blob {}", received_blob_id);
                BLOB_STORE.write().await.insert(res).await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn event_get(&mut self, api: &mut ApiCaller<W>, req: &dto::BlobsGetIn) -> Result<bool> {
        info!("Requesting blob {}", req.key);

        let req_no = api.blobs_get_req_send(req).await?;
        self.outcoming_reqs.insert(req_no, req.key.clone());

        Ok(true)
    }
}
