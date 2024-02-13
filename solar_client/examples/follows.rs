use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const PUB_KEY: &str = "@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let follows = client.follows(PUB_KEY).await?;
    println!("{:#?}", follows);
    // [
    //     "@XeP2o+FcSdb5YdTvt/nuYPU2PHJSSwRt+X2y8B2dK5U=.ed25519",
    //     "@2il4IGrTUzMyInSjCyv6vx6tftELOCxh/FdUuzvj7tE=.ed25519",
    //     ...
    //     "@OKRij/n7Uu42A0Z75ty0JI0cZxcieD2NyjXrRdYKNOQ=.ed25519",
    // ]

    Ok(())
}
