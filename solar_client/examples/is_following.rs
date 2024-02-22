use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const PEER_A: &str = "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519";
const PEER_B: &str = "@2il4IGrTUzMyInSjCyv6vx6tftELOCxh/FdUuzvj7tE=.ed25519";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let is_following = client.is_following(PEER_A, PEER_B).await?;
    println!("{}", is_following);
    // true

    Ok(())
}
