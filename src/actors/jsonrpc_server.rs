// src/actors/json_rpc_server.rs

use async_std::task;
use futures::FutureExt;
use jsonrpc_http_server::{
    jsonrpc_core::*, AccessControlAllowOrigin, DomainsValidation, ServerBuilder,
};
use kuska_ssb::{api::dto::content::TypedMessage, feed::Message, keystore::OwnedIdentity};
use log::{info, warn};
use serde_json::json;

use crate::{broker::*, error::Error, Result, KV_STORAGE};

/// Register the JSON-RPC server endpoint, define the JSON-RPC methods
/// and spawn the server.
///
/// Listens for a termination signal from the broker. When received, the
/// JSON-RPC server is closed and a terminated signal is sent to the broker.
pub async fn actor(server_id: OwnedIdentity, port: u16) -> Result<()> {
    let broker = BROKER
        .lock()
        .await
        .register("jsonrpc-listener", false)
        .await?;

    let ch_terminate = broker.ch_terminate.fuse();

    let mut io = IoHandler::default();

    // Simple `ping` endpoint.
    io.add_sync_method("ping", |_| Ok(Value::String("pong!".to_owned())));

    let local_pk = server_id.id.clone();

    // Return the public key of the local SSB server.
    io.add_sync_method("whoami", move |_| Ok(Value::String(local_pk.to_owned())));

    // Publish a typed message (raw).
    // Returns the key (hash) and sequence number of the published message.
    io.add_sync_method("publish", move |params: Params| {
        task::block_on(async {
            // Parse the parameter containing the post content.
            let post_content: TypedMessage = params.parse()?;

            // Open the primary KV database for writing.
            let feed_storage = KV_STORAGE.write().await;

            // Lookup the last message published on the local feed.
            // Return `None` if no messages have yet been published on the feed.
            let last_msg = feed_storage
                .get_last_message(&server_id.id)
                // Map the error to a variant of our crate-specific error type.
                // The `?` operator then performs the `From` conversion to
                // the `jsonrpc_core::Error` type if an error occurs.
                .map_err(Error::KV)?;

            // Instantiate and cryptographically-sign a new message using `post`.
            let msg = Message::sign(last_msg.as_ref(), &server_id, json!(post_content))
                .map_err(Error::KuskaFeed)?;

            // Append the signed message to the feed.
            let seq = feed_storage
                .append_feed(msg.clone())
                .await
                .map_err(Error::KV)?;

            info!(
                "published message {} with sequence number {}",
                msg.id().to_string(),
                seq
            );

            let response = json![{ "msg_ref": msg.id().to_string(), "seq": seq }];

            Ok(response)
        })
    });

    let server_addr = format!("0.0.0.0:{}", port);
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
