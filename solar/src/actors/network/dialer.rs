//! Dialer
//!
//! Dial requests are received from the connection scheduler via the broker
//! message bus. Each request includes the public key and address of the peer
//! to be dialed. Upon receiving a request, the dialer spawns the connection actor.
use futures::{select_biased, FutureExt, StreamExt};

use crate::{
    actors::network::{connection, connection::TcpConnection, connection_scheduler::DialRequest},
    broker::{ActorEndpoint, Broker, BROKER},
    config::SECRET_CONFIG,
    Result,
};

/// Start the dialer.
///
/// Register the connection dialer with the broker (as an actor) and listen
/// for dial requests from the scheduler. Once received, use the attached
/// public key and outbound address to dial the peer by spawning the connection
/// actor.
pub async fn actor(selective_replication: bool) -> Result<()> {
    // Register the connection dialer actor with the broker.
    let ActorEndpoint {
        ch_terminate,
        ch_broker: _,
        ch_msg,
        actor_id: _,
        ..
    } = BROKER.lock().await.register("dialer", true).await?;

    // Fuse internal termination channel with external channel.
    // This allows termination of the dialer loop to be initiated from
    // outside this function.
    let mut ch_terminate_fuse = ch_terminate.fuse();

    let mut broker_msg_ch = ch_msg.unwrap();

    // Listen for dial request events via the broker message bus and dial peers.
    loop {
        select_biased! {
            // Received termination signal. Break out of the loop.
            _value = ch_terminate_fuse => {
                break;
            },
            // Received a message from the connection scheduler via the broker.
            msg = broker_msg_ch.next().fuse() => {
                if let Some(msg) = msg {
                    if let Some(DialRequest((public_key, addr))) = msg.downcast_ref::<DialRequest>() {
                        Broker::spawn(connection::actor(
                            SECRET_CONFIG.get().unwrap().to_owned_identity()?,
                            TcpConnection::Dial {
                                addr: addr.to_string(),
                                public_key: *public_key,
                            },
                            selective_replication,
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}