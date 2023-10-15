use std::collections::hash_map::HashMap;

use async_std::{prelude::*, sync::Mutex, task, task::JoinHandle};
use futures::{
    channel::{mpsc, oneshot},
    select_biased, FutureExt, SinkExt,
};
use log::{info, trace};
use once_cell::sync::Lazy;

use crate::{
    actors::{
        muxrpc::{RpcBlobsGetEvent, RpcBlobsWantsEvent},
        network::{connection_manager::ConnectionEvent, connection_scheduler::DialRequest},
    },
    storage::{blob::StoBlobEvent, kv::StoKvEvent},
    Result,
};

#[derive(Debug)]
pub struct Void {}

#[derive(Clone)]
pub enum BrokerMessage {
    Connection(ConnectionEvent),
    Dial(DialRequest),
    RpcBlobsGet(RpcBlobsGetEvent),
    RpcBlobsWants(RpcBlobsWantsEvent),
    // TODO: Rename these to `StoreBlob` and `StoreBlobEvent`.
    StoBlob(StoBlobEvent),
    // TODO: Rename these to `StoreKv` and `StoreKvEvent`.
    StoKv(StoKvEvent),
}

pub type ChBrokerSend = mpsc::UnboundedSender<BrokerEvent>;
pub type ChSigSend = oneshot::Sender<Void>;
pub type ChSigRecv = oneshot::Receiver<Void>;
pub type ChMsgSend = mpsc::UnboundedSender<BrokerMessage>;
pub type ChMsgRecv = mpsc::UnboundedReceiver<BrokerMessage>;

/// Destination to which a message is addressed.
#[derive(Eq, PartialEq, Debug)]
pub enum Destination {
    /// A single actor identified by ID.
    Actor(usize),
    /// All actors.
    Broadcast,
}

/// All events that can occur during the broker message loop.
#[derive(Debug)]
pub enum BrokerEvent {
    /// Actor registration.
    Connect(BrokerEndpoint),
    /// Actor deregistration.
    Disconnect { actor_id: usize },
    /// Actor message.
    Message { to: Destination, msg: BrokerMessage },
    /// Termination signal.
    Terminate,
}

impl BrokerEvent {
    pub fn new(to: Destination, msg: BrokerMessage) -> Self {
        BrokerEvent::Message { to, msg }
    }
}

/// The broker-end of an actor-broker connection.
#[derive(Debug)]
pub struct BrokerEndpoint {
    /// Actor ID.
    pub actor_id: usize,
    /// Terminate signal sender.
    pub ch_terminate: ChSigSend,
    /// Terminated signal receiver.
    pub ch_terminated: ChSigRecv,
    /// Message sender.
    pub ch_msg: Option<ChMsgSend>,
}

/// The actor-end of an actor-broker connection.
#[derive(Debug)]
pub struct ActorEndpoint {
    /// Actor ID.
    pub actor_id: usize,
    /// Broker sender.
    pub ch_broker: ChBrokerSend,
    /// Terminate signal receiver.
    pub ch_terminate: ChSigRecv,
    /// Terminated signal sender.
    pub ch_terminated: ChSigSend,
    /// Message receiver.
    pub ch_msg: Option<ChMsgRecv>,
}

/// Broker of the actor-broker system.
#[derive(Debug)]
pub struct Broker {
    /// ID of the most recently registered actor.
    last_actor_id: usize,
    /// Broker event sender.
    sender: ChBrokerSend,
    /// Message loop handle.
    msgloop: Option<JoinHandle<()>>,
}

pub static BROKER: Lazy<Mutex<Broker>> = Lazy::new(|| Mutex::new(Broker::new()));

impl Broker {
    /// Instantiate a new `Broker` instance.
    pub fn new() -> Self {
        // Create and split an unbounded message passing channel.
        let (sender, receiver) = mpsc::unbounded();
        // Spawn the broker message loop.
        let msgloop = task::spawn(Self::msg_loop(receiver));

        Self {
            last_actor_id: 0,
            sender,
            msgloop: Some(msgloop),
        }
    }

    /// Return a handle for the broker message loop.
    pub fn take_msgloop(&mut self) -> JoinHandle<()> {
        self.msgloop.take().unwrap()
    }

    /// Register a new actor with the broker.
    pub async fn register(&mut self, name: &str, msg_notify: bool) -> Result<ActorEndpoint> {
        // Increment the last actor ID value.
        self.last_actor_id += 1;

        trace!(target:"solar-actor", "registering actor {}={}", self.last_actor_id, name);

        // Create oneshot message passing channels for sending and receiving
        // termination signals.
        let (terminate_sender, terminate_receiver) = oneshot::channel::<Void>();
        let (terminated_sender, terminated_receiver) = oneshot::channel::<Void>();

        // Create and split an unbounded message passing channel for broker
        // messages.
        //
        // This forms the primary communication mechanism linking the broker
        // and actor.
        let (msg_sender, msg_receiver) = if msg_notify {
            let (s, r) = mpsc::unbounded::<BrokerMessage>();
            (Some(s), Some(r))
        } else {
            (None, None)
        };

        // Instantiate a broker endpoint.
        let broker_endpoint = BrokerEndpoint {
            actor_id: self.last_actor_id,
            ch_terminate: terminate_sender,
            ch_terminated: terminated_receiver,
            ch_msg: msg_sender,
        };

        // Instantiate an actor endpoint.
        let actor_endpoint = ActorEndpoint {
            actor_id: self.last_actor_id,
            ch_broker: self.sender.clone(),
            ch_terminate: terminate_receiver,
            ch_terminated: terminated_sender,
            ch_msg: msg_receiver,
        };

        // Send a connection event to the broker.
        // This completes the registration process.
        self.sender
            .send(BrokerEvent::Connect(broker_endpoint))
            .await
            .unwrap();

        Ok(actor_endpoint)
    }

    /// Clone the broker event sender.
    pub fn create_sender(&self) -> ChBrokerSend {
        self.sender.clone()
    }

    /// Spawn an asynchronous task.
    pub fn spawn<F>(fut: F) -> task::JoinHandle<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        task::spawn(async move {
            if let Err(e) = fut.await {
                eprintln!("{e}")
            }
        })
    }

    /// Start the broker event loop.
    ///
    /// This is the switchboard of the application. It listens for `BrokerEvent`
    /// messages in a loop and acts accordingly. It is also responsible for
    /// sending actor termination signals to allow for graceful shutdown of the
    /// system.
    async fn msg_loop(mut events: mpsc::UnboundedReceiver<BrokerEvent>) {
        let mut actors: HashMap<usize, BrokerEndpoint> = HashMap::new();

        loop {
            let event = select_biased! {
                event = events.next().fuse() => match event {
                    None => break,
                    Some(event) => event,
                },
            };

            match event {
                BrokerEvent::Terminate => {
                    info!("Msg Got terminate ");
                    break;
                }
                BrokerEvent::Connect(actor) => {
                    trace!(target:"solar-actor", "Registering actor {}", actor.actor_id);
                    actors.insert(actor.actor_id, actor);
                }
                BrokerEvent::Disconnect { actor_id } => {
                    trace!(target:"solar-actor", "Deregistering actor {}", actor_id);
                    actors.remove(&actor_id);
                }
                BrokerEvent::Message { to, msg } => {
                    for actor in actors.values_mut() {
                        // Send the message to a single, specific actor or to
                        // all actors.
                        if to == Destination::Actor(actor.actor_id) || to == Destination::Broadcast
                        {
                            if let Some(ch) = &mut actor.ch_msg {
                                let _ = ch.send(msg.clone()).await;
                            }
                        }
                    }
                }
            }
        }

        trace!(target:"solar-actor", "***Loop finished**");

        // Collect all terminate and terminated channels.
        let (terms, termds): (Vec<_>, Vec<_>) = actors
            .drain()
            .map(|(_, actor)| {
                (
                    (actor.actor_id, actor.ch_terminate),
                    (actor.actor_id, actor.ch_terminated),
                )
            })
            .unzip();

        // Send termination signal to all registered actors.
        for (actor_id, term) in terms {
            trace!(target:"solar-actor", "Sending term signal to {}", actor_id);
            let _ = term.send(Void {});
        }

        // Wait to receive termination confirmation signals from actors.
        for (actor_id, termd) in termds {
            trace!(target:"solar-actor", "Awaiting termd signal from {}", actor_id);
            let _ = termd.await;
        }

        trace!(target:"solar-actor", "***All actors finished**");

        drop(actors);
    }
}
