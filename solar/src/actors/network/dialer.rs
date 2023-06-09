//! Dialer
//!
//! Dial requests are received from the connection scheduler via the broker
//! message bus. Each request includes the public key of the peer to be dialed.
//! Upon receiving a request, the dialer queries the address book to determine
//! the associated address for the peer and then spawns the connection actor.

use futures::{select_biased, FutureExt, StreamExt};
use kuska_ssb::crypto::ed25519::PublicKey;

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
/// public key to lookup the outbound address from the address book and dial
/// the peer by spawning the connection actor.
pub async fn actor(peers: Vec<(PublicKey, String)>, selective_replication: bool) -> Result<()> {
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

    // Listen for dial-peer events via the broker message bus, lookup
    // the associated address and dial the peer.
    loop {
        select_biased! {
            // Received termination signal. Break out of the loop.
            _value = ch_terminate_fuse => {
                break;
            },
            // Received a message from the connection scheduler via the broker.
            msg = broker_msg_ch.next().fuse() => {
                if let Some(msg) = msg {
                    if let Some(dial_request) = msg.downcast_ref::<DialRequest>() {
                        match dial_request {
                            DialRequest(public_key) => {
                                // Retrieve the address associated with this key.
                                if let addr = AddressBook::get(public_key) {
                                    // Spawn the connection actor.
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
                            _ => (),
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
