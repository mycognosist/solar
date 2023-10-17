use std::fmt::Display;

use async_std::net::TcpStream;
use futures::SinkExt;
use kuska_ssb::{
    crypto::{ed25519, ToSsbId},
    handshake::HandshakeComplete,
    keystore::OwnedIdentity,
};

use crate::{
    actors::network::connection_manager::{ConnectionEvent, CONNECTION_MANAGER},
    broker::{ActorEndpoint, BrokerEvent, BrokerMessage, Destination, BROKER},
    Result,
};

/// Encapsulate inbound and outbound TCP connections.
pub enum TcpConnection {
    /// An outbound TCP connection.
    Dial {
        /// The address of a remote peer.
        addr: String,
        /// The public key of a remote peer.
        public_key: ed25519::PublicKey,
    },
    /// An inbound TCP connection.
    Listen { stream: TcpStream },
}

/// Connection data.
#[derive(Debug, Default, Clone)]
pub struct ConnectionData {
    /// Connection identifier.
    pub id: usize,
    /// The address of the remote peer.
    pub peer_addr: Option<String>,
    /// The public key of the remote peer.
    pub peer_public_key: Option<ed25519::PublicKey>,
    /// Completed secret handshake.
    pub handshake: Option<HandshakeComplete>,
    /// TCP stream.
    pub stream: Option<TcpStream>,
}

// Custom `Display` implementation so we can easily log connection data in
// the message loop.
impl Display for ConnectionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let peer_addr = match &self.peer_addr {
            Some(addr) => addr.to_string(),
            None => "_".to_string(),
        };

        let peer_public_key = match &self.peer_public_key {
            Some(key) => {
                let ssb_id = key.to_ssb_id();
                if ssb_id.starts_with('@') {
                    ssb_id
                } else {
                    format!("@{}", ssb_id)
                }
            }
            None => "_".to_string(),
        };

        write!(
            f,
            "<Connection {} / {} / {}>",
            &self.id, peer_public_key, peer_addr
        )
    }
}

impl ConnectionData {
    pub fn new(id: usize) -> Self {
        ConnectionData {
            id,
            ..ConnectionData::default()
        }
    }
}

pub async fn actor(
    identity: OwnedIdentity,
    connection: TcpConnection,
    selective_replication: bool,
) -> Result<()> {
    // Register a new connection with the connection manager.
    let connection_id = CONNECTION_MANAGER.write().await.register();

    // Record the data associated with this connection.
    let mut connection_data = ConnectionData::new(connection_id);

    // Register the "connection" actor endpoint with the broker.
    let ActorEndpoint { mut ch_broker, .. } =
        BROKER.lock().await.register("connection", true).await?;

    // Handle a TCP connection event (inbound or outbound).
    match connection {
        // Handle an outbound TCP connection event.
        TcpConnection::Dial { addr, public_key } => {
            // Update the data associated with this connection.
            connection_data.peer_addr = Some(addr.to_owned());
            connection_data.peer_public_key = Some(public_key);

            // Send 'connecting' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Connection(ConnectionEvent::Connecting(
                        connection_data,
                        identity,
                        selective_replication,
                    )),
                ))
                .await?;
        }
        // Handle an incoming TCP connection event.
        TcpConnection::Listen { stream } => {
            // Retrieve the origin (address) of the incoming connection.
            let peer_addr = stream.peer_addr()?.to_string();

            // Update the data associated with this connection.
            connection_data.peer_addr = Some(peer_addr);
            connection_data.stream = Some(stream);

            // Since the connection has been established, the handshake can
            // now be attempted.

            // Send 'handshaking' connection event message via the broker.
            ch_broker
                .send(BrokerEvent::new(
                    Destination::Broadcast,
                    BrokerMessage::Connection(ConnectionEvent::Handshaking(
                        connection_data,
                        identity,
                        selective_replication,
                        true, // Listener.
                    )),
                ))
                .await?;
        }
    };

    Ok(())
}
