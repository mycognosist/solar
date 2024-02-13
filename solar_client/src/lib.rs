use anyhow::Result;

#[jsonrpc_client::api]
pub trait SolarClient {
    async fn blocks(&self, pub_key: &str) -> Vec<String>;

    async fn blockers(&self, pub_key: &str) -> Vec<String>;

    async fn descriptions(&self, pub_key: &str) -> Vec<(String, String)>;

    async fn self_descriptions(&self, pub_key: &str) -> Vec<String>;

    async fn latest_description(&self, pub_key: &str) -> String;

    async fn latest_self_description(&self, pub_key: &str) -> String;

    async fn feed(&self, pub_key: &str) -> Vec<String>;

    async fn follows(&self, pub_key: &str) -> Vec<String>;

    async fn ping(&self) -> String;

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
