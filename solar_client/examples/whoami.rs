use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned());

    let whoami = client.whoami().await?;
    println!("{}", whoami);
    // @qK93G/R9R5J2fiqK+kxV72HqqPUcss+rth8rACcYr4s=.ed25519

    Ok(())
}
