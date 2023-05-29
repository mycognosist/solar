/*
 * Connection Scheduler
 *
 * Take a list of peers to connect to (`Vec<(String, PublicKey)>`) and convert to a VecDeque.
 *
 * Create two queues (`VecDeque`): `eager` and `lazy`.
 *
 * Start by adding the full list of peers to the `eager` queue.
 *
 * Dial each peer, one by one, with a delay of x seconds between each dial attempt.
 *
 * If the connection and handshake are successful, push the peer to the `eager` queue once the
 * connection is complete.
 *
 * If the connection or handshake are unsuccessful, push the peer to the `lazy` queue once the
 * connection is complete.
 *
 * Dial each peer in the `lazy` queue, one by one, with a delay of x * 10 seconds between each dial
 * attempt.
 *
 * The success or failure of each dial attempt is determined by listening to connection events from
 * the connection manager. This allows peers to be moved between queues when required.
*/

use std::{collections::VecDeque, time::Duration};

use async_std::stream;
use futures::{select_biased, stream::StreamExt, FutureExt};
use kuska_ssb::crypto::ed25519::PublicKey;

use crate::{
    actors::network::{connection_manager, connection_manager::ConnectionEvent, secret_handshake},
    broker::{ActorEndpoint, Broker, BROKER},
    config::SECRET_CONFIG,
    Result,
};

#[derive(Debug)]
struct ConnectionScheduler {
    /// Peers with whom the last connection attempt was successful.
    /// These peers are dialed more frequently than the lazy peers.
    eager_peers: VecDeque<(String, PublicKey)>,
    /// Peers with whom the last connection attempt was unsuccessful.
    /// These peers are dialed less frequently than the eager peers.
    lazy_peers: VecDeque<(String, PublicKey)>,
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
    fn new(peers: Vec<(String, PublicKey)>) -> Self {
        let mut scheduler = ConnectionScheduler::default();

        scheduler.eager_peers = VecDeque::from(peers);

        scheduler
    }

    /// Start the connection scheduler.
    ///
    /// Register the connection scheduler with the broker (as an actor), start
    /// the eager and lazy dialers and  listen for connection events emitted by
    /// the connection manager. Update the eager and lazy peer queues according
    /// to connection outcomes.
    async fn start(mut self, selective_replication: bool) -> Result<()> {
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
            .await
            .unwrap();

        // Create the tickers (aka. metronomes) which will emit messages at
        // the predetermined interval. These tickers control the rates at which
        // we dial peers.
        let mut eager_ticker = stream::interval(self.eager_interval).fuse();
        let mut lazy_ticker = stream::interval(self.lazy_interval).fuse();

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
                        // Pop a peer from the list of eager peers and dial it.
                        if let Some((addr, peer_public_key)) = self.eager_peers.pop_front() {
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
                },
                // Lazy ticker emitted a tick.
                lazy_tick = lazy_ticker.next() => {
                    if let Some(_tick) = lazy_tick {
                    // Pop a peer from the list of lazy peers and dial it.
                    if let Some((addr, peer_public_key)) = self.lazy_peers.pop_front() {
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
                },
                // Received a message from the connection manager via the broker.
                msg = broker_msg_ch.next().fuse() => {
                    if let Some(msg) = msg {
                        if let Some(conn_event) = msg.downcast_ref::<ConnectionEvent>() {
                            match conn_event {
                                ConnectionEvent::Connected(id) => {
                                    todo!()
                                }
                                ConnectionEvent::Replicating(id) => {
                                    todo!()
                                }
                                ConnectionEvent::Disconnected(id) => {
                                    todo!()
                                }
                                ConnectionEvent::Error(id, err) => {
                                    todo!()
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
}
