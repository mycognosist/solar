use std::marker::PhantomData;

use async_std::io::Write;
use async_trait::async_trait;
use kuska_ssb::{
    api::{ApiCaller, ApiMethod},
    rpc,
};

use crate::{
    actors::muxrpc::handler::{RpcHandler, RpcInput},
    broker::ChBrokerSend,
    node::KV_STORE,
    Result,
    error::Error
};
use crate::node::INVITE_MANAGER;

pub struct InviteHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    phantom: PhantomData<W>,
}

impl<W> Default for InviteHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    fn default() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<W> RpcHandler<W> for InviteHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    fn name(&self) -> &'static str {
        "InviteHandler"
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
                    Some(ApiMethod::InviteUse) => self.recv_invite_use(api, *req_no, req).await,
                    _ => Ok(false),
                }
            }
            _ => Ok(false),
        }
    }
}

impl<W> InviteHandler<W>
where
    W: Write + Unpin + Send + Sync,
{
    async fn recv_invite_use(
        &mut self,
        api: &mut ApiCaller<W>,
        req_no: i32,
        req: &rpc::Body,
    ) -> Result<bool> {
        // This will contain the invite code.
        let args: Vec<String> = serde_json::from_value(req.args.clone())?;

        // Parse the public key and secret from the invite code (ignore the hostname and port).
        // `one.butt.nz:8008:@...=.ed25519~<base64 invite secret key seed>=
        let delimiter = ".ed25519~";

        let parsed_invite_secret = match args[0].split_once(delimiter) {
            Some((part1, invite_secret)) => {
                println!("Part 1: {}", part1);
                println!("Part 2: {}", invite_secret);
                Ok(invite_secret)
            }
            None => {
                println!("Delimiter not found in the string.");
                Err(Error::InviteUse)
            }
        };
        let invite_secret = parsed_invite_secret.expect("invalid format of invite string");


        // then use the invite
        let mut invite_manager = INVITE_MANAGER.write().await;
        let result = invite_manager.use_invite(invite_secret);
        match result {
            // We need to send a success or error response to the remote peer.
            // TODO: kuska needs api.invite_res_send(..).
            Ok(_) =>   {
                api.whoami_res_send(req_no,"invite accepted".to_string()).await?;
            }
            Err(err) => {
                let msg = format!("{err}");
                api.rpc().send_error(req_no, req.rpc_type, &msg).await?
            }
        }

        Ok(true)
    }
}
