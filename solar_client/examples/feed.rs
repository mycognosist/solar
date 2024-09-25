use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
// const PUB_KEY: &str = "@5Pt3dKy2HTJ0mWuS78oIiklIX0gBz6BTfEnXsbvke9c=.ed25519";
const PUB_KEY: &str = "@3TOzePzvEXgEDf7/jKmpDjlTHRs1e98GDzfIItAQHM0=.ed25519";


#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let feed = client.feed(PUB_KEY).await?;
    println!("{:#?}", feed);
    // TODO

    Ok(())
}
