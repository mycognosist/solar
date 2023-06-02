use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::Result;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicationConfig {
    /// Resync the local database by requesting the local feed from peers
    /// (default: false).
    #[serde(skip)]
    pub resync: bool,

    /// Deny replication attempts from peers who are not defined in the
    /// replication configuration (default: true).
    #[serde(skip)]
    pub selective: bool,

    /// List of peers to be replicated. Each entry includes a public key and
    /// a URL. The URL contains the host and port of the peer's node.
    pub peers: HashMap<String, String>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            resync: false,
            selective: true,
            peers: HashMap::default(),
        }
    }
}

impl ReplicationConfig {
    /// Serialize the replication configuration as a TOML string.
    pub fn to_toml(&self) -> Result<String> {
        Ok(toml::to_string(&self)?)
    }

    /// Deserialize a TOML string slice into replication configuration data.
    pub fn from_toml(serialized_config: &str) -> Result<Self> {
        Ok(toml::from_str::<ReplicationConfig>(serialized_config)?)
    }

    /// If the replication config file is not found, generate a new one and
    /// write it to file. Otherwise, read the list of peer replication data
    /// from the file and return it.
    pub fn return_or_create_file(base_path: &Path) -> Result<Self> {
        // Define the filename of the replication config file.
        let replication_config_file = base_path.join("replication.toml");

        if !replication_config_file.is_file() {
            println!(
                "Replication configuration file not found, generated new one in {replication_config_file:?}"
            );
            let config = ReplicationConfig::default();
            let toml_config = config.to_toml()?;

            let mut file = File::create(&replication_config_file)?;
            write!(file, "{}", toml_config)?;

            Ok(config)
        } else {
            // If the config file exists, open it and read the contents.
            let mut file = File::open(&replication_config_file)?;
            let mut file_contents = String::new();
            file.read_to_string(&mut file_contents)?;
            ReplicationConfig::from_toml(&file_contents)
        }
    }
}
