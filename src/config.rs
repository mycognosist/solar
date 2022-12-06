use kuska_ssb::{
    crypto::{ToSodiumObject, ToSsbId},
    keystore::OwnedIdentity,
};
use serde::{Deserialize, Serialize};

use crate::Result;

/// Public-private keypair.
#[derive(Serialize, Deserialize)]
pub struct SecretConfig {
    /// Public key.
    pub id: String,
    /// Private key.
    pub secret: String,
}

impl SecretConfig {
    /// Generate a new, unique public-private keypair.
    pub fn create() -> Self {
        let OwnedIdentity { id, sk, .. } = OwnedIdentity::create();

        SecretConfig {
            id,
            secret: sk.to_ssb_id(),
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
    pub fn owned_identity(&self) -> Result<OwnedIdentity> {
        Ok(OwnedIdentity {
            id: self.id.clone(),
            pk: self.id[1..].to_ed25519_pk()?,
            sk: self.secret.to_ed25519_sk()?,
        })
    }
}

/// List of peers whose data will be replicated.
#[derive(Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Public keys.
    pub peers: Vec<String>,
}

impl ReplicationConfig {
    /// Generate a new instance.
    // TODO: consider replacing this with a `Default` derivation
    pub fn create() -> Self {
        ReplicationConfig { peers: Vec::new() }
    }

    /// Serialize an instance of `ReplicationConfig` as a TOML byte vector.
    pub fn to_toml(&self) -> Result<Vec<u8>> {
        Ok(toml::to_vec(&self)?)
    }

    /// Deserialize a TOML byte slice into an instance of `ReplicationConfig`.
    pub fn from_toml(s: &[u8]) -> Result<Self> {
        Ok(toml::from_slice::<ReplicationConfig>(s)?)
    }
}
