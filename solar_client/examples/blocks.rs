use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const PUB_KEY: &str = "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let blocks = client.blocks(PUB_KEY).await?;
    println!("{:#?}", blocks);
    // [
    //     "@dW5ch5miTnxLJDVDtB4ZCvrVxh+S8kGCQIBbd5paLhw=.ed25519",
    //     "@QIlKZ8DMw9XpjpRZ96RBLpfkLnOUZSqamC6WMddGh3I=.ed25519",
    //     ...
    //     "@+rMXLy1md42gvbBq+6l6rp95/drh6QyACO1ZZMMnWI0=.ed25519",
    // ]

    Ok(())
}
