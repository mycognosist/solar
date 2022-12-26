use async_std::{
    net::{TcpListener, ToSocketAddrs},
    prelude::*,
};
use futures::{select_biased, FutureExt};
use kuska_ssb::keystore::OwnedIdentity;

use crate::{broker::*, Result};

pub async fn actor(
    server_id: OwnedIdentity,
    addr: impl ToSocketAddrs,
    selective_replication: bool,
) -> Result<()> {
    let broker = BROKER.lock().await.register("sbot-listener", false).await?;

    let mut ch_terminate = broker.ch_terminate.fuse();

    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();

    loop {
        select_biased! {
            _ = ch_terminate => break,
            stream = incoming.next().fuse() => {
                if let Some(stream) = stream {
                    if let Ok(stream) = stream {
                        Broker::spawn(super::peer::actor(server_id.clone(), super::peer::Connect::ClientStream{stream}, selective_replication));
                    }
                } else {
                    break;
                }
            },
        }
    }

    let _ = broker.ch_terminated.send(Void {});

    Ok(())
}
