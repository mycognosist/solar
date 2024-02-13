use anyhow::Result;

#[jsonrpc_client::api]
pub trait SolarClient {
    async fn blocks(&self, pub_key: &str) -> Vec<String>;

    async fn blockers(&self, pub_key: &str) -> Vec<String>;

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
