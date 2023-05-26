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

    /// Serialize an instance of `SecretConfig` as a TOML byte vector.
    pub fn to_toml(&self) -> Result<Vec<u8>> {
        Ok(toml::to_vec(&self)?)
    }

    /// Deserialize a TOML byte slice into an instance of `SecretConfig`.
    pub fn from_toml(s: &[u8]) -> Result<Self> {
        Ok(toml::from_slice::<SecretConfig>(s)?)
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
            let mut file = File::create(&secret_key_file)?;
            file.write_all(&config.to_toml()?)?;
            Ok(config)
        } else {
            // If the config file exists, open it and read the contents.
            let mut file = File::open(&secret_key_file)?;
            let mut raw: Vec<u8> = Vec::new();
            file.read_to_end(&mut raw)?;
            SecretConfig::from_toml(&raw)
        }
    }
}
