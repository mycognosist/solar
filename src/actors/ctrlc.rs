use async_std::sync::Arc;

use async_ctrlc::CtrlC;
use futures::SinkExt;

use crate::broker::*;
use anyhow::Result;

pub async fn actor() -> Result<()> {
    let mut broker = BROKER.lock().await.register("crtlc", true).await?;

    let ctrlc = CtrlC::new().expect("cannot create Ctrl+C handler?");
    ctrlc.await;

    let _ = broker
        .ch_broker
        .send(BrokerEvent::Message {
            to: Destination::Actor("oled".to_string()),
            msg: Arc::new("Terminating solar".to_string()),
        })
        .await;

    println!("Got CTRL-C, sending termination signal to jobs...");

    let _ = broker.ch_broker.send(BrokerEvent::Terminate).await;
    Ok(())
}
