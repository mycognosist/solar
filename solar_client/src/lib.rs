use anyhow::Result;
// TODO: Should we rather be using a custom kuska type?
use serde_json::Value;

#[jsonrpc_client::api]
pub trait SolarClient {
    async fn blocks(&self, pub_key: &str) -> Vec<String>;

    async fn blockers(&self, pub_key: &str) -> Vec<String>;

    async fn descriptions(&self, pub_key: &str) -> Vec<(String, String)>;

    async fn self_descriptions(&self, pub_key: &str) -> Vec<String>;

    async fn latest_description(&self, pub_key: &str) -> String;

    async fn latest_self_description(&self, pub_key: &str) -> String;

    async fn feed(&self, pub_key: &str) -> Vec<Value>;

    async fn follows(&self, pub_key: &str) -> Vec<String>;

    async fn followers(&self, pub_key: &str) -> Vec<String>;

    async fn is_following(&self, peer_a: &str, peer_b: &str) -> bool;

    async fn friends(&self, pub_key: &str) -> Vec<String>;

    async fn images(&self, pub_key: &str) -> Vec<(String, String)>;

    async fn self_images(&self, pub_key: &str) -> Vec<String>;

    async fn latest_image(&self, pub_key: &str) -> (String, String);

    async fn latest_self_image(&self, pub_key: &str) -> String;

    async fn message(&self, msg_ref: &str) -> Value;

    async fn names(&self, pub_key: &str) -> Vec<(String, String)>;

    async fn self_names(&self, pub_key: &str) -> Vec<String>;

    async fn latest_name(&self, pub_key: &str) -> String;

    async fn latest_self_name(&self, pub_key: &str) -> String;

    async fn peers(&self) -> Vec<(String, u64)>;

    async fn ping(&self) -> String;

    async fn subscribers(&self, channel: &str) -> Vec<String>;

    async fn whoami(&self) -> String;
}

#[jsonrpc_client::implement(SolarClient)]
pub struct Client {
    inner: reqwest::Client,
    base_url: reqwest::Url,
}

impl Client {
    pub fn new(base_url: String) -> Result<Self> {
        Ok(Self {
            inner: reqwest::Client::new(),
            base_url: base_url.parse()?,
        })
    }
}
