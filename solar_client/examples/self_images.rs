use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const PUB_KEY: &str = "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let self_images = client.self_images(PUB_KEY).await?;
    println!("{:#?}", self_images);
    // [
    //     "&dmLPhR0npN7tHiICGfM1WLBMHhhzh5I5VR3rEKvmOXw=.sha256",
    //     "&f4YiGjvj3ClOc3VA0XYQnPZAflQm/GROfTkHBjAb3a0=.sha256",
    //     "&dmLPhR0npN7tHiICGfM1WLBMHhhzh5I5VR3rEKvmOXw=.sha256",
    //     "&Z8X3/bszQTRZ3cmyEM6sUoa8hlDbS+qfecziI5gRXpY=.sha256",
    //     "&LesplCjgV8Q355b93QOFCoL5G1KdyXTqIsm4EL9R2Fw=.sha256",
    //     "&8M2JFEFHlxJ5q8Lmu3P4bDdCHg0SLB27Q321cy9Upx4=.sha256",
    // ]

    Ok(())
}
