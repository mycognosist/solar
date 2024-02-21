use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    // Get the public key and latest sequence number for all peers in the
    // local database.
    let peers = client.peers().await?;
    println!("{:#?}", peers);
    // [
    //     (
    //         "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519",
    //         26681,
    //     ),
    //     (
    //         "@L/g6qZQE/2FdO2UhSJ0uyDiZb5LjJLatM/d8MN+INSM=.ed25519",
    //         46,
    //     ),
    //     (
    //         "@bMUudXOb9+FrVXKIxyn6ro+jo4drbrKXdSoZ9yXp8rc=.ed25519",
    //         3,
    //     ),
    //     (
    //         "@qK93G/R9R5J2fiqK+kxV72HqqPUcss+rth8rACcYr4s=.ed25519",
    //         227,
    //     ),
    // ]

    Ok(())
}
