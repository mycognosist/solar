// We need to get data out of the `peer_loop` in the peer actor.
// How to achieve that?
// One option is to use a message passing channel.
// Should we use the existing broker messaging system?
// Or create a secondary event channel?

// I think we should start by leveraging the existing system.

// The peer loop sends a `BrokerEvent::Message` to the connection
// manager actor. The type of that message value should be `ConnectionEvent`.

/// All possible errors while negotiating connections.
pub enum ConnectionError {
    Io(std::io::Error),
    Handshake,
}

/// Connection data for a peer.
pub struct Connection {
    /// Peer data.
    peer: PeerData,

    /// Connection state.
    state: ConnectionState,
}

#[derive(Debug)]
/// The state of the connection.
pub enum ConnectionState {
    Ready,
    // Does the message passing approach negate the need for a stateful
    // struct? At least in terms of the TcpConnection, TcpStream and
    // HandshakeComplete?
    Connecting(TcpConnection),
    Connected(TcpStream, HandshakeComplete),
    Handshaking,
    Replicating,
    Disconnecting(PublicKey),
    Disconnected,
    Finished,
    Error(Option<Error>, Option<PublicKey>),
}

#[derive(Debug)]
/// Connection events.
pub enum ConnectionEvent {
    Connecting,
    Connected,
    Handshaking,
    Replicating,
    Disconnecting,
    Disconnected,
    Error(ConnectionError),
}
