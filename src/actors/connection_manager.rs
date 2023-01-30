use async_std::{sync::Mutex, task, task::JoinHandle};
use futures::{select_biased, stream::StreamExt, FutureExt};
use log::trace;
use once_cell::sync::Lazy;

use crate::{ActorEndpoint, BROKER};

/*
/// All possible errors while negotiating connections.
pub enum ConnectionError {
    Io(std::io::Error),
    Handshake,
}

/// Peer connection data.
pub struct Connection {
    /// Peer data.
    peer: PeerData,

    /// Connection state.
    state: ConnectionState,

    /// Connection identifier.
    id: usize,
}

impl Connection {
    pub fn new() {
    }
}
*/

/// Connection events. The `usize` represents the connection ID.
#[derive(Debug)]
pub enum ConnectionEvent {
    Connecting(usize),
    Handshaking(usize),
    Connected(usize),
    Replicating(usize),
    Disconnecting(usize),
    Disconnected(usize),
    // TODO: use `ConnectionError` instead of `String`.
    Error(usize, String),
}

/// Connection manager (broker).
#[derive(Debug)]
pub struct ConnectionManager {
    /// ID number of the most recently registered connection.
    last_connection_id: usize,
    /// Message loop handle.
    msgloop: Option<JoinHandle<()>>,
    //active_connections: usize,
}

pub static CONNECTION_MANAGER: Lazy<Mutex<ConnectionManager>> =
    Lazy::new(|| Mutex::new(ConnectionManager::new()));

impl ConnectionManager {
    /// Instantiate a new `ConnectionManager` instance.
    pub fn new() -> Self {
        // Spawn the connection event message loop.
        let msgloop = task::spawn(Self::msg_loop());

        Self {
            last_connection_id: 0,
            msgloop: Some(msgloop),
        }
    }

    /// Return a handle for the connection event message loop.
    pub fn take_msgloop(&mut self) -> JoinHandle<()> {
        self.msgloop.take().unwrap()
    }

    /// Register a new connection with the connection manager.
    pub fn register(&mut self) -> usize {
        // Increment the last connection ID value.
        self.last_connection_id += 1;

        trace!(target: "connection-manager", "registering new connection: {}", self.last_connection_id);

        self.last_connection_id
    }

    /// Start the connection manager event loop.
    ///
    /// Listen for connection event messages via the broker and update
    /// connection state accordingly.
    pub async fn msg_loop() {
        // Register the connection manager actor with the broker.
        let ActorEndpoint {
            ch_terminate,
            ch_broker: _,
            ch_msg,
            actor_id: _,
            ..
        } = BROKER
            .lock()
            .await
            .register("connection-manager", true)
            .await
            .unwrap();

        // Fuse internal termination channel with external channel.
        // This allows termination of the peer loop to be initiated from outside
        // this function.
        let mut ch_terminate_fuse = ch_terminate.fuse();

        let mut broker_msg_ch = ch_msg.unwrap();

        // Listen for connection events via the broker message bus.
        loop {
            select_biased! {
                _value = ch_terminate_fuse => {
                    break;
                },
                msg = broker_msg_ch.next().fuse() => {
                    if let Some(msg) = msg {
                        if let Some(conn_event) = msg.downcast_ref::<ConnectionEvent>() {
                            match conn_event {
                                ConnectionEvent::Connecting(id) => {
                                    trace!(target: "connection-manager", "connecting: {}", id);
                                }
                                ConnectionEvent::Handshaking(id) => {
                                    trace!(target: "connection-manager", "handshaking: {}", id);
                                }
                                ConnectionEvent::Connected(id) => {
                                    trace!(target: "connection-manager", "connected: {}", id);
                                }
                                ConnectionEvent::Replicating(id) => {
                                    trace!(target: "connection-manager", "replicating: {}", id);
                                }
                                ConnectionEvent::Disconnecting(id) => {
                                    trace!(target: "connection-manager", "disconnecting: {}", id);
                                }
                                ConnectionEvent::Disconnected(id) => {
                                    trace!(target: "connection-manager", "disconnected: {}", id);
                                }
                                ConnectionEvent::Error(id, err) => {
                                    trace!(target: "connection-manager", "error: {}: {}", id, err);
                                }
                            }
                        }
                    }
                },
            };
        }
    }
}

/*

// LEFT-OVER CODE FROM ACTOR APPROCH

// TODO: remove this code when certain of the msgloop approach used above

pub async fn actor() -> Result<()> {
    if let Err(err) = actor_inner().await {
        warn!("connection manager failed: {}", err);
    }
    Ok(())
}

pub async fn actor_inner() -> Result<()> {
    // Register the connection manager actor with the broker.
    let ActorEndpoint {
        ch_terminate,
        ch_broker: _,
        ch_msg,
        actor_id: _,
        ..
    } = BROKER
        .lock()
        .await
        .register("connection-manager", true)
        .await?;

    let _connection_manager = ConnectionManager::new();

    // Fuse internal termination channel with external channel.
    // This allows termination of the peer loop to be initiated from outside
    // this function.
    let mut ch_terminate_fuse = ch_terminate.fuse();

    let mut broker_msg_ch = ch_msg.unwrap();

    // Listen for connection events via the broker message bus.
    loop {
        select_biased! {
            _value = ch_terminate_fuse => {
                break;
            },
            msg = broker_msg_ch.next().fuse() => {
                if let Some(msg) = msg {
                    if let Some(conn_event) = msg.downcast_ref::<ConnectionEvent>() {
                        match conn_event {
                            ConnectionEvent::Connecting(id) => {
                                trace!(target: "connection-manager", "Connecting: {}", id);
                            }
                            ConnectionEvent::Handshaking(id) => {
                                trace!(target: "connection-manager", "Handshaking: {}", id);
                            }
                            ConnectionEvent::Connected(id) => {
                                trace!(target: "connection-manager", "Connected: {}", id);
                            } ConnectionEvent::Replicating(id) => {
                                trace!(target: "connection-manager", "Replicating: {}", id);
                            }
                            ConnectionEvent::Disconnecting(id) => {
                                trace!(target: "connection-manager", "Disconnecting: {}", id);
                            }
                            ConnectionEvent::Disconnected(id) => {
                                trace!(target: "connection-manager", "Disconnected: {}", id);
                            }
                            ConnectionEvent::Error(id, err) => {
                                trace!(target: "connection-manager", "Error: {}: {}", id, err);
                            }
                        }
                    }
                }
            },
        };
    }

    Ok(())
}
*/
