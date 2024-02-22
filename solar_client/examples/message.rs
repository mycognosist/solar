use anyhow::Result;
use solar_client::{Client, SolarClient};

const SERVER_ADDR: &str = "http://127.0.0.1:3030";
const MSG_REF: &str = "%RCb++/ZhqV1lJNIcoNrk4yM3AfBobT7u8seObZgcEbA=.sha256";

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(SERVER_ADDR.to_owned())?;

    let message = client.message(MSG_REF).await?;
    println!("{:#?}", message);
    /*
    Object {
        "key": String("%RCb++/ZhqV1lJNIcoNrk4yM3AfBobT7u8seObZgcEbA=.sha256"),
        "value": Object {
            "previous": String("%uhtIvUJeHicd+i/biMI/IlLiLkN4pAYgrVq4CA2rSYA=.sha256"),
            "sequence": Number(227),
            "author": String("@qK93G/R9R5J2fiqK+kxV72HqqPUcss+rth8rACcYr4s=.ed25519"),
            "timestamp": Number(1707730214482),
            "hash": String("sha256"),
            "content": Object {
                "type": String("post"),
                "text": String("Testing out the solar JSON-RPC client"),
            },
            "signature": String("fsDScOQ3Zbm9sRpcUfKV+Rtf/I70vKpRW3BTIuK3IoZGhYj9Z/pdHDGAWUh+9ixToAevdKltgJZWa7DUWdAuCw==.sig.ed25519"),
        },
        "timestamp": Number(1707730214.4837818),
        "rts": Null,
    }
    */

    Ok(())
}
