// src/actors/json_rpc_server.rs

use std::net::SocketAddr;

use async_std::task;
use futures::FutureExt;
use jsonrpsee::server::{logger::Params, RpcModule, ServerBuilder};
use jsonrpsee::types::error::ErrorObject as JsonRpcError;
use kuska_ssb::{api::dto::content::TypedMessage, feed::Message, keystore::OwnedIdentity};
use log::{info, warn};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{broker::*, error::Error, node::KV_STORE, Result};

/// The name of a channel.
#[derive(Debug, Deserialize)]
struct Channel(String);

/// The public keys (ID) of two peers.
#[derive(Debug, Deserialize)]
struct IsFollowing {
    peer_a: String,
    peer_b: String,
}

/// Message reference containing the key (sha256 hash) of a message.
/// Used to parse the key from the parameters supplied to the `message`
/// endpoint.
#[derive(Debug, Deserialize)]
struct MsgRef(String);

/// The public key (ID) of a peer.
#[derive(Debug, Deserialize)]
struct PubKey(String);

/// Register the JSON-RPC server endpoint, define the JSON-RPC methods
/// and spawn the server.
///
/// Listens for a termination signal from the broker. When received, the
/// JSON-RPC server is closed and a terminated signal is sent to the broker.
pub async fn actor(server_id: OwnedIdentity, server_addr: SocketAddr) -> Result<()> {
    let broker = BROKER
        .lock()
        .await
        .register("jsonrpc-listener", false)
        .await?;

    let ch_terminate = broker.ch_terminate.fuse();

    let server = ServerBuilder::default()
        .http_only()
        .build(&server_addr)
        .await?;

    let mut rpc_module = RpcModule::new(());

    // Retrieve the public keys of all feeds blocked by the given public key.
    //
    // Returns an array of public keys.
    rpc_module.register_method("blocks", move |params: Params, _| {
        task::block_on(async {
            // Parse the parameter containing the public key.
            let pub_key = params.parse::<Vec<String>>()?;

            // Open the primary KV database for reading.
            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let blocks = indexes.get_blocks(&pub_key[0])?;
            let response = json!(blocks);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the public keys of all feeds blocking the given public key.
    //
    // Returns an array of public keys.
    rpc_module.register_method("blockers", move |params: Params, _| {
        task::block_on(async {
            let pub_key = params.parse::<Vec<String>>()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let blockers = indexes.get_blockers(&pub_key[0])?;
            let response = json!(blockers);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the descriptions for the given public key.
    //
    // Returns an array of descriptions.
    rpc_module.register_method("descriptions", move |params: Params, _| {
        task::block_on(async {
            let pub_key = params.parse::<Vec<String>>()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let descriptions = indexes.get_descriptions(&pub_key[0])?;
            let response = json!(descriptions);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the self-assigned descriptions for the given public key.
    //
    // Returns an array of descriptions.
    rpc_module.register_method("self_descriptions", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let descriptions = indexes.get_self_assigned_descriptions(&pub_key.0)?;
            let response = json!(descriptions);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the latest (most-recent) description for the given public key.
    //
    // Returns a string.
    rpc_module.register_method("latest_description", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let description = indexes.get_latest_description(&pub_key.0)?;
            let response = json!(description);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the latest (most-recent) self-assigned description for the given
    // public key.
    //
    // Returns a string.
    rpc_module.register_method("latest_self_description", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let description = indexes.get_latest_self_assigned_description(&pub_key.0)?;
            let response = json!(description);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the public keys of all feeds followed by the given public key.
    //
    // Returns an array of public keys.
    rpc_module.register_method("follows", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let follows = indexes.get_follows(&pub_key.0)?;
            let response = json!(follows);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the public keys of all feeds following the given public key.
    //
    // Returns an array of public keys.
    rpc_module.register_method("followers", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let followers = indexes.get_followers(&pub_key.0)?;
            let response = json!(followers);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the follow state of two peers (ie. does peer A follow peer B?).
    //
    // Returns a boolean.
    rpc_module.register_method("is_following", move |params: Params, _| {
        task::block_on(async {
            let peers: IsFollowing = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let is_following = indexes.is_following(&peers.peer_a, &peers.peer_b)?;
            let response = json!(is_following);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the public keys of all friends (mutual follows) of the given
    // public key.
    //
    // Returns an array of public keys.
    rpc_module.register_method("friends", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let friends = indexes.get_friends(&pub_key.0)?;
            let response = json!(friends);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the image references for the given public key.
    //
    // Returns an array of strings.
    rpc_module.register_method("images", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let images = indexes.get_images(&pub_key.0)?;
            let response = json!(images);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the self-assigned image references for the given public key.
    //
    // Returns an array of strings.
    rpc_module.register_method("self_images", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let images = indexes.get_self_assigned_images(&pub_key.0)?;
            let response = json!(images);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the latest image reference for the given public key.
    //
    // Returns a string.
    rpc_module.register_method("latest_image", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let image = indexes.get_latest_image(&pub_key.0)?;
            let response = json!(image);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the latest self-assigned image reference for the given public
    // key.
    //
    // Returns an array of strings.
    rpc_module.register_method("latest_self_image", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let image = indexes.get_latest_self_assigned_image(&pub_key.0)?;
            let response = json!(image);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the names for the given public key.
    //
    // Returns an array of strings.
    rpc_module.register_method("names", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let names = indexes.get_names(&pub_key.0)?;
            let response = json!(names);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the self-assigned names for the given public key.
    //
    // Returns an array of strings.
    rpc_module.register_method("self_names", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let names = indexes.get_self_assigned_names(&pub_key.0)?;
            let response = json!(names);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the latest name for the given public key.
    //
    // Returns a string.
    rpc_module.register_method("latest_name", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let names = indexes.get_latest_name(&pub_key.0)?;
            let response = json!(names);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the latest self-assigned name for the given public key.
    //
    // Returns a string.
    rpc_module.register_method("latest_self_name", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let names = indexes.get_latest_self_assigned_name(&pub_key.0)?;
            let response = json!(names);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve the public keys of all feeds subscribed to the given channel.
    //
    // Returns an array of public keys.
    rpc_module.register_method("subscribers", move |params: Params, _| {
        task::block_on(async {
            let channel: Channel = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let subscribers = indexes.get_channel_subscribers(&channel.0)?;
            let response = json!(subscribers);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve all channels to which the given public key is subscribed.
    //
    // Returns an array of channel names.
    rpc_module.register_method("subscriptions", move |params: Params, _| {
        task::block_on(async {
            let pub_key: PubKey = params.parse()?;

            let db = KV_STORE.read().await;

            let indexes = &db.indexes.as_ref().ok_or(Error::Indexes)?;
            let subscriptions = indexes.get_channel_subscriptions(&pub_key.0)?;
            let response = json!(subscriptions);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve a feed by public key.
    // Returns an array of messages as a KVTs.
    rpc_module.register_method("feed", move |params: Params, _| {
        task::block_on(async {
            // Parse the parameter containing the public key.
            let pub_key: PubKey = params.parse()?;

            // Open the primary KV database for reading.
            let db = KV_STORE.read().await;

            // Retrieve the message value for the requested message.
            let feed = db.get_feed(&pub_key.0)?;
            let response = json!(feed);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Retrieve a message by key.
    // Returns the message as a KVT.
    rpc_module.register_method("message", move |params: Params, _| {
        task::block_on(async {
            // Parse the parameter containing the message reference (key).
            let msg_ref: MsgRef = params.parse()?;

            // Open the primary KV database for reading.
            let db = KV_STORE.read().await;

            // Retrieve the message value for the requested message.
            let msg_val = db.get_msg_val(&msg_ref.0)?;

            // Retrieve the message KVT for the requested message using the
            // author and sequence fields from the message value.
            let msg_kvt = if let Some(val) = msg_val {
                db.get_msg_kvt(val.author(), val.sequence())?
            } else {
                None
            };

            let response = json!(msg_kvt);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Return the public key and latest sequence number for all feeds in the
    // local database.
    rpc_module.register_method("peers", |_, _| {
        task::block_on(async {
            let db = KV_STORE.read().await;
            let peers = db.get_peers().await?;
            let response = json!(peers);

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Simple `ping` endpoint.
    rpc_module.register_method("ping", |_, _| "pong!")?;

    // Clone the local public key (ID) so it can later be captured by the
    // `whoami` closure.
    let local_pk = server_id.id.clone();

    // Publish a typed message (raw).
    // Returns the key (hash) and sequence number of the published message.
    rpc_module.register_method("publish", move |params: Params, _| {
        task::block_on(async {
            // Parse the parameter containing the post content.
            let post_content: TypedMessage = params.parse()?;

            // Open the primary KV database for writing.
            let db = KV_STORE.write().await;

            // Lookup the last message published on the local feed.
            // Return `None` if no messages have yet been published on the feed.
            let last_msg = db.get_latest_msg_val(&server_id.id)?;

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

            Ok::<Value, JsonRpcError>(response)
        })
    })?;

    // Return the public key of the local SSB server.
    rpc_module.register_method("whoami", move |_, _| local_pk.clone())?;

    let addr = server.local_addr()?;
    let handle = server.start(rpc_module)?;
    info!("JSON-RPC server started on: {}", addr);

    // Listen for termination signal from broker.
    if let Err(err) = ch_terminate.await {
        warn!("ch_terminate sender dropped: {}", err)
    }

    // When received, close (stop) the server.
    handle.stop()?;

    // Then send terminated signal back to broker.
    let _ = broker.ch_terminated.send(Void {});

    Ok(())
}
