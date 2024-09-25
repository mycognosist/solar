use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let num_uses: i32 = 4;
    let invite_code = client.create_invite(num_uses).await?;
    println!("{}", invite_code);

    Ok(())
}
