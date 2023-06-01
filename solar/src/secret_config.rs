use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

use kuska_ssb::{
    crypto::{ToSodiumObject, ToSsbId},
    keystore::OwnedIdentity,
};
use serde::{Deserialize, Serialize};

use crate::Result;

/// Public-private keypair.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SecretConfig {
    /// Public key.
    pub public_key: String,
    /// Private key.
    pub private_key: String,
}

impl SecretConfig {
    /// Generate a new, unique public-private keypair.
    pub fn create() -> Self {
        let OwnedIdentity { id, sk, .. } = OwnedIdentity::create();

        SecretConfig {
            public_key: id,
            private_key: sk.to_ssb_id(),
        }
    }

    /// Serialize an instance of `SecretConfig` as a TOML string.
    pub fn to_toml(&self) -> Result<String> {
        Ok(toml::to_string(&self)?)
    }

    /// Deserialize a TOML string slice into an instance of `SecretConfig`.
    pub fn from_toml(serialized_config: &str) -> Result<Self> {
        Ok(toml::from_str::<SecretConfig>(serialized_config)?)
    }

    /// Generate an `OwnedIdentity` from the public-private keypair.
    pub fn to_owned_identity(&self) -> Result<OwnedIdentity> {
        Ok(OwnedIdentity {
            id: self.public_key.clone(),
            pk: self.public_key[1..].to_ed25519_pk()?,
            sk: self.private_key.to_ed25519_sk()?,
        })
    }

    /// If the secret config file is not found, generate a new one and write it
    /// to file. This includes the creation of a unique public-private keypair.
    pub fn return_or_create_file(base_path: &Path) -> Result<Self> {
        // Define the filename of the secret config file.
        let secret_key_file = base_path.join("secret.toml");

        if !secret_key_file.is_file() {
            println!("Private key not found, generated new one in {secret_key_file:?}");
            let config = SecretConfig::create();
            let toml_config = config.to_toml()?;

            let mut file = File::create(&secret_key_file)?;
            write!(file, "{}", toml_config)?;

            Ok(config)
        } else {
            // If the config file exists, open it and read the contents.
            let mut file = File::open(&secret_key_file)?;
            let mut file_contents = String::new();
            file.read_to_string(&mut file_contents)?;
            SecretConfig::from_toml(&file_contents)
        }
    }
}
