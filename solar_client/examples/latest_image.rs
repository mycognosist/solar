use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const PUB_KEY: &str = "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let latest_image = client.latest_image(PUB_KEY).await?;
    println!("{:#?}", latest_image);
    // (
    //     "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519",
    //     "&8M2JFEFHlxJ5q8Lmu3P4bDdCHg0SLB27Q321cy9Upx4=.sha256",
    // )

    Ok(())
}
