use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const PUB_KEY: &str = "@qK93G/R9R5J2fiqK+kxV72HqqPUcss+rth8rACcYr4s=.ed25519";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let feed = client.feed(PUB_KEY).await?;
    println!("{:#?}", feed);
    // TODO

    Ok(())
}
