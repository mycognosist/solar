use anyhow::Result;
use serde_json::json;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const TEXT: &str = "Testing message publishing via the solar JSON-RPC client";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    // Construct a JSON post.
    //
    // This can take any shape conforming to
    // `kuska_ssb::api::dto::content::TypedMessage`.
    //
    // Consult the `kuska_ssb` repo for more details:
    // https://github.com/Kuska-ssb/ssb
    let post = json!({
        "type": "post",
        "text": TEXT,
    });

    let post = json!({
        "type": "contact",
        "contact": "@5Pt3dKy2HTJ0mWuS78oIiklIX0gBz6BTfEnXsbvke9c=.ed25519",
        "following": true
    });

    let msg_ref_and_seq_num = client.publish(post).await?;
    println!("{:?}", msg_ref_and_seq_num);
    // ("%J0k3hbXE6CjoA2yFpUsy+A1K+GRcTc5Eh6LH7OuzzUE=.sha256", 231)

    // Or destructure the tuple into individual components:
    // let (msg_ref, seq_num) = client.publish(post).await?;

    Ok(())
}
