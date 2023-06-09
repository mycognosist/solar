//! Connection Scheduler
//!
//! Peers to be dialed are added to the scheduler, with the SSB public key and an address being
//! provided for each one. These peers are initially placed into an "eager" queue by the scheduler.
//! Each peer is dialed, one by one, with a delay of x seconds between each dial attempt.
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
        connection,
        connection::TcpConnection,
        connection_manager::{ConnectionEvent, CONNECTION_MANAGER},
    },
    broker::{ActorEndpoint, Broker, BROKER},
    config::SECRET_CONFIG,
    Result,
};

/// A request to dial the peer identified by the given public key.
pub struct DialRequest(pub PublicKey);

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
    /// Add a peer to the scheduler. The peer will be added to the queue of
    /// eager peers for the initial dial attempt, as long as it is not
    /// already present in either the eager or lazy queue.
    fn add_peer(&mut self, peer: (PublicKey, String)) {
        // Only insert the peer if it hasn't already been added to the queue
        // of eager or lazy peers.
        if !self.eager_peers.contains(&peer) & !self.lazy_peers.contains(&peer) {
            self.eager_peers.push_back(peer)
        }
    }

    /// Remove a peer from the scheduler, checking both the eager and lazy
    /// queues.
    fn _remove_peer(&mut self, peer: (PublicKey, String)) {
        // First search the queue of eager peers for the given peer.
        // If found, use the returned index to remove the peer.
        if let Ok(index) = self
            .eager_peers
            .binary_search_by_key(&peer.0, |(key, _addr)| *key)
        {
            self.eager_peers.remove(index);
        }

        // Then search the queue of lazy peers and remove the peer if found.
        if let Ok(index) = self
            .lazy_peers
            .binary_search_by_key(&peer.0, |(key, _addr)| *key)
        {
            self.lazy_peers.remove(index);
        }
    }
}

/// Start the connection scheduler.
///
/// Register the connection scheduler with the broker (as an actor), start
/// the eager and lazy dialers and listen for connection events emitted by
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

    // Create a new connection scheduler.
    let mut scheduler = ConnectionScheduler::default();

    // Populate the scheduler with the peers to be dialed.
    // These peers are added to the queue of eager peers if they have not
    // previously been added to the scheduler.
    for peer in peers {
        scheduler.add_peer(peer)
    }

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
                    // Pop a peer from the queue of eager peers.
                    if let Some((public_key, addr)) = scheduler.eager_peers.pop_front() {
                        // Check if we're already connected to this peer. If so,
                        // push them to the back of the eager queue.
                        if CONNECTION_MANAGER.read().await.contains_connected_peer(&public_key) {
                            scheduler.eager_peers.push_back((public_key, addr))
                        } else {
                            // Otherwise, dial the peer.
                            Broker::spawn(connection::actor(
                                // TODO: make this neater once config-sharing story has improved.
                                SECRET_CONFIG.get().unwrap().to_owned_identity()?,
                                TcpConnection::Dial {
                                    addr,
                                    public_key,
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
                    // Pop a peer from the queue of lazy peers.
                    if let Some((public_key, addr)) = scheduler.lazy_peers.pop_front() {
                        // Check if we're already connected to this peer. If so,
                        // push them to the back of the eager queue.
                        if CONNECTION_MANAGER.read().await.contains_connected_peer(&public_key) {
                            scheduler.eager_peers.push_back((public_key, addr))
                        } else {
                            // Otherwise, dial the peer.
                            Broker::spawn(connection::actor(
                                SECRET_CONFIG.get().unwrap().to_owned_identity()?,
                                TcpConnection::Dial {
                                    addr,
                                    public_key,
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

#[cfg(test)]
mod test {
    use super::*;

    use kuska_ssb::crypto::ToSodiumObject;

    #[async_std::test]
    async fn test_add_and_remove_peers() -> Result<()> {
        let mut connection_scheduler = ConnectionScheduler::default();

        // Ensure the eager peers queue is empty.
        assert!(connection_scheduler.eager_peers.len() == 0);

        // Add a peer.
        connection_scheduler.add_peer((
            "QlQwWaj48J1Du5rHQXTPfifUFsPKLrOo6T5EfWfkqXU=.ed25519".to_ed25519_pk()?,
            "ssb.mycelial.technology:8008".to_string(),
        ));
        assert!(connection_scheduler.eager_peers.len() == 1);

        // Add a second peer.
        connection_scheduler.add_peer((
            "HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519".to_ed25519_pk()?,
            "127.0.0.1:8008".to_string(),
        ));
        assert!(connection_scheduler.eager_peers.len() == 2);

        // Attempt to add the first peer again. The queue length should not
        // change (no duplicate entries permitted).
        connection_scheduler.add_peer((
            "HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519".to_ed25519_pk()?,
            "127.0.0.1:8008".to_string(),
        ));
        assert!(connection_scheduler.eager_peers.len() == 2);

        // Remove the second peer.
        connection_scheduler._remove_peer((
            "HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519".to_ed25519_pk()?,
            "127.0.0.1:8008".to_string(),
        ));
        assert!(connection_scheduler.eager_peers.len() == 1);

        // Remove the first peer.
        connection_scheduler._remove_peer((
            "QlQwWaj48J1Du5rHQXTPfifUFsPKLrOo6T5EfWfkqXU=.ed25519".to_ed25519_pk()?,
            "ssb.mycelial.technology:8008".to_string(),
        ));

        // Ensure the eager peers queue is empty once again.
        assert!(connection_scheduler.eager_peers.is_empty());

        Ok(())
    }
}
