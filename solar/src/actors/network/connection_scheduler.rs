//! Connection Scheduler
//!
//! The scheduler takes a list of peers to connect to (including the SSB public key and an address
//! for each) and places all of them into an "eager" queue. Each peer is dialed, one by one, with
//! a delay of x seconds between each dial attempt.
//!
//! If the connection and handshake are successful, the peer is pushed to the back of the "eager"
//! queue once the connection is complete.
//!
//! If the connection or handshake are unsuccessful, the peer is pushed to the back of the "lazy"
//! queue once the connection is complete.
//!
//! Each peer in the "lazy" queue is dialed, one by one, with a delay of x * 10 seconds between
//! each dial attempt.
//!
//! The success or failure of each dial attempt is determined by listening to connection events from
//! the connection manager. This allows peers to be moved between queues when required.
use std::{collections::VecDeque, time::Duration};

use async_std::stream;
use futures::{select_biased, stream::StreamExt, FutureExt};
use kuska_ssb::crypto::ed25519::PublicKey;

use crate::{
    actors::network::{
        connection_manager,
        connection_manager::{ConnectionEvent, CONNECTION_MANAGER},
        secret_handshake,
    },
    broker::{ActorEndpoint, Broker, BROKER},
    config::SECRET_CONFIG,
    Result,
};

#[derive(Debug)]
struct ConnectionScheduler {
    /// Peers with whom the last connection attempt was successful.
    /// These peers are dialed more frequently than the lazy peers.
    eager_peers: VecDeque<(PublicKey, String)>,
    /// Peers with whom the last connection attempt was unsuccessful.
    /// These peers are dialed less frequently than the eager peers.
    lazy_peers: VecDeque<(PublicKey, String)>,
    /// The interval in seconds between dial attempts for eager peers.
    /// Defaults to 5 seconds.
    eager_interval: Duration,
    /// The interval in seconds between dial attempts for lazy peers.
    /// Defaults to 60 seconds.
    lazy_interval: Duration,
}

impl Default for ConnectionScheduler {
    fn default() -> Self {
        Self {
            eager_peers: VecDeque::new(),
            lazy_peers: VecDeque::new(),
            eager_interval: Duration::from_secs(5),
            lazy_interval: Duration::from_secs(60),
        }
    }
}

impl ConnectionScheduler {
    /// Create a new connection scheduler and populate it with a list of peers
    /// to dial.
    fn new(peers: Vec<(PublicKey, String)>) -> Self {
        ConnectionScheduler {
            eager_peers: VecDeque::from(peers),
            ..Default::default()
        }
    }
}

/// Start the connection scheduler.
///
/// Register the connection scheduler with the broker (as an actor), start
/// the eager and lazy dialers and  listen for connection events emitted by
/// the connection manager. Update the eager and lazy peer queues according
/// to connection outcomes.
pub async fn actor(peers: Vec<(PublicKey, String)>, selective_replication: bool) -> Result<()> {
    // Register the connection scheduler actor with the broker.
    let ActorEndpoint {
        ch_terminate,
        ch_broker: _,
        ch_msg,
        actor_id: _,
        ..
    } = BROKER
        .lock()
        .await
        .register("connection-scheduler", true)
        .await?;

    // Create a new connection scheduler and populate it with a list of peers
    // to dial.
    let mut scheduler = ConnectionScheduler::new(peers);

    // Create the tickers (aka. metronomes) which will emit messages at
    // the predetermined interval. These tickers control the rates at which
    // we dial peers.
    let mut eager_ticker = stream::interval(scheduler.eager_interval).fuse();
    let mut lazy_ticker = stream::interval(scheduler.lazy_interval).fuse();

    // Fuse internal termination channel with external channel.
    // This allows termination of the scheduler loop to be initiated from
    // outside this function.
    let mut ch_terminate_fuse = ch_terminate.fuse();

    let mut broker_msg_ch = ch_msg.unwrap();

    // Listen for connection events via the broker message bus and dial
    // peers each time an eager or lazy tick is emitted.
    loop {
        select_biased! {
            // Received termination signal. Break out of the loop.
            _value = ch_terminate_fuse => {
                break;
            },
            // Eager ticker emitted a tick.
            eager_tick = eager_ticker.next() => {
                if let Some(_tick) = eager_tick {
                    // Pop a peer from the list of eager peers.
                    if let Some((peer_public_key, addr)) = scheduler.eager_peers.pop_front() {
                        // Check if we're already connected to this peer. If so,
                        // push them to the back of the eager queue.
                        if CONNECTION_MANAGER.read().await.contains_connected_peer(&peer_public_key) {
                            scheduler.eager_peers.push_back((peer_public_key, addr))
                        } else {
                            // Otherwise, dial the peer.
                            Broker::spawn(secret_handshake::actor(
                                // TODO: make this neater once config-sharing story has improved.
                                SECRET_CONFIG.get().unwrap().to_owned_identity()?,
                                connection_manager::TcpConnection::Dial {
                                    addr,
                                    peer_public_key,
                                },
                                selective_replication,
                            ));
                        }
                    }
                }
            },
            // Lazy ticker emitted a tick.
            lazy_tick = lazy_ticker.next() => {
                if let Some(_tick) = lazy_tick {
                    // Pop a peer from the list of lazy peers.
                    if let Some((peer_public_key, addr)) = scheduler.lazy_peers.pop_front() {
                        // Check if we're already connected to this peer. If so,
                        // push them to the back of the eager queue.
                        if CONNECTION_MANAGER.read().await.contains_connected_peer(&peer_public_key) {
                            scheduler.eager_peers.push_back((peer_public_key, addr))
                        } else {
                            // Otherwise, dial the peer.
                            Broker::spawn(secret_handshake::actor(
                                SECRET_CONFIG.get().unwrap().to_owned_identity()?,
                                connection_manager::TcpConnection::Dial {
                                    addr,
                                    peer_public_key,
                                },
                                selective_replication,
                            ));
                        }
                    }
                }
            },
            // Received a message from the connection manager via the broker.
            msg = broker_msg_ch.next().fuse() => {
                if let Some(msg) = msg {
                    if let Some(conn_event) = msg.downcast_ref::<ConnectionEvent>() {
                        match conn_event {
                            ConnectionEvent::Replicating(data) => {
                                // This connection was "successful".
                                // Push the peer to the back of the eager queue.
                                if let Some(public_key) = data.peer_public_key {
                                    if let Some(addr) = &data.peer_addr {
                                        // Only push if the peer is not already in the queue.
                                        if !scheduler.eager_peers.contains(&(public_key, addr.to_string())) {
                                            scheduler.eager_peers.push_back((public_key, addr.to_owned()))
                                        }
                                    }
                                }
                            }
                            ConnectionEvent::Disconnected(data) => {
                                // This connection may or may not have been "successful".
                                // If it was successful (ie. replication took place) then
                                // the peer should have already been pushed back to the eager
                                // queue. If not, push the peer to the back of the lazy queue.
                                if let Some(public_key) = data.peer_public_key {
                                    if let Some(addr) = &data.peer_addr {
                                        // Only push if the peer is not in the eager queue.
                                        if !scheduler.eager_peers.contains(&(public_key, addr.to_string())) {
                                            scheduler.lazy_peers.push_back((public_key, addr.to_owned()))
                                        }
                                    }
                                }
                            }
                            ConnectionEvent::Error(data, _err) => {
                                // This connection was "unsuccessful".
                                // Push the peer to the back of the lazy queue.
                                if let Some(public_key) = data.peer_public_key {
                                    if let Some(addr) = &data.peer_addr {
                                        // Only push if the peer is not already in the queue.
                                        if !scheduler.lazy_peers.contains(&(public_key, addr.to_string())) {
                                            scheduler.lazy_peers.push_back((public_key, addr.to_owned()))
                                        }
                                    }
                                }
                            }
                            // Ignore all other connection event variants.
                            _ => (),
                        }
                    }
                }
            },
        }
    }

    Ok(())
}
