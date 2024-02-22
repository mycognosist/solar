use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const CHANNEL: &str = "myco";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let subscribers = client.subscribers(CHANNEL).await?;
    println!("{:#?}", subscribers);
    // [
    //     "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519",
    // ]

    Ok(())
}
