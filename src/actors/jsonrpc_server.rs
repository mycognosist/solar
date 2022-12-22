// src/actors/json_rpc_server.rs

use async_std::task;
use futures::FutureExt;
use jsonrpc_http_server::{
    jsonrpc_core::*, AccessControlAllowOrigin, DomainsValidation, ServerBuilder,
};
use kuska_ssb::{api::dto::content::TypedMessage, feed::Message, keystore::OwnedIdentity};
use log::{info, warn};
use serde::Deserialize;
use serde_json::json;

use crate::{broker::*, error::Error, Result, KV_STORAGE};

/// Message reference containing the key (sha256 hash) of a message.
/// Used to parse the key from the parameters supplied to the `message`
/// endpoint.
#[derive(Debug, Deserialize)]
struct MsgRef {
    msg_ref: String,
}

/// The public key (ID) of a peer.
#[derive(Debug, Deserialize)]
struct PubKey {
    pub_key: String,
}

/// Register the JSON-RPC server endpoint, define the JSON-RPC methods
/// and spawn the server.
///
/// Listens for a termination signal from the broker. When received, the
/// JSON-RPC server is closed and a terminated signal is sent to the broker.
pub async fn actor(server_id: OwnedIdentity, server_addr: String) -> Result<()> {
    let broker = BROKER
        .lock()
        .await
        .register("jsonrpc-listener", false)
        .await?;

    let ch_terminate = broker.ch_terminate.fuse();

    let mut io = IoHandler::default();

    // Retrieve a feed by public key.
    // Returns an array of messages as a KVTs.
    io.add_sync_method("feed", move |params: Params| {
        task::block_on(async {
            // Parse the parameter containing the public key.
            let pub_key: PubKey = params.parse()?;

            // Open the primary KV database for reading.
            let db = KV_STORAGE.read().await;

            // Retrieve the message value for the requested message.
            let feed = db.get_feed(&pub_key.pub_key)?;

            let response = json!(feed);

            Ok(response)
        })
    });

    // Retrieve a message by key.
    // Returns the message as a KVT.
    io.add_sync_method("message", move |params: Params| {
        task::block_on(async {
            // Parse the parameter containing the message reference (key).
            let msg_ref: MsgRef = params.parse()?;

            // Open the primary KV database for reading.
            let db = KV_STORAGE.read().await;

            // Retrieve the message value for the requested message.
            let msg_val = db.get_msg_val(&msg_ref.msg_ref)?;

            // Retrieve the message KVT for the requested message using the
            // author and sequence fields from the message value.
            let msg_kvt = if let Some(val) = msg_val {
                db.get_msg_kvt(val.author(), val.sequence())?
            } else {
                None
            };

            let response = json!(msg_kvt);

            Ok(response)
        })
    });

    // Return the public key and latest sequence number for all feeds in the
    // local database.
    io.add_sync_method("peers", |_| {
        task::block_on(async {
            let db = KV_STORAGE.read().await;
            let peers = db.get_peers().await?;

            let response = json!(peers);

            Ok(response)
        })
    });

    // Simple `ping` endpoint.
    io.add_sync_method("ping", |_| Ok(Value::String("pong!".to_owned())));

    // Clone the local public key (ID) so it can later be captured by the
    // `whoami` closure.
    let local_pk = server_id.id.clone();

    // Publish a typed message (raw).
    // Returns the key (hash) and sequence number of the published message.
    io.add_sync_method("publish", move |params: Params| {
        task::block_on(async {
            // Parse the parameter containing the post content.
            let post_content: TypedMessage = params.parse()?;

            // Open the primary KV database for writing.
            let db = KV_STORAGE.write().await;

            // Lookup the last message published on the local feed.
            // Return `None` if no messages have yet been published on the feed.
            let last_msg = db.get_latest_msg_val(&server_id.id)?;
            // Map the error to a variant of our crate-specific error type.
            // The `?` operator then performs the `From` conversion to
            // the `jsonrpc_core::Error` type if an error occurs.
            //.map_err(Error::SledDatabase)?;

            // Instantiate and cryptographically-sign a new message using `post`.
            let msg = Message::sign(last_msg.as_ref(), &server_id, json!(post_content))
                .map_err(Error::Validation)?;

            // Append the signed message to the feed.
            let seq = db.append_feed(msg.clone()).await?;

            info!(
                "published message {} with sequence number {}",
                msg.id().to_string(),
                seq
            );

            let response = json![{ "msg_ref": msg.id().to_string(), "seq_num": seq }];

            Ok(response)
        })
    });

    // Return the public key of the local SSB server.
    io.add_sync_method("whoami", move |_| Ok(Value::String(local_pk.to_owned())));

    let server = ServerBuilder::new(io)
        .cors(DomainsValidation::AllowOnly(vec![
            AccessControlAllowOrigin::Null,
        ]))
        .start_http(&server_addr.parse()?)?;

    // Create a close handle to be used when the termination signal is
    // received.
    let close_handle = server.close_handle();

    // Start the JSON-RPC server in a task.
    // This allows us to listen for the termination signal (without blocking).
    task::spawn(async {
        server.wait();
    });

    // Listen for termination signal from broker.
    if let Err(err) = ch_terminate.await {
        warn!("ch_terminate sender dropped: {}", err)
    }

    // When received, close (stop) the server.
    close_handle.close();

    // Then send terminated signal back to broker.
    let _ = broker.ch_terminated.send(Void {});

    Ok(())
}
