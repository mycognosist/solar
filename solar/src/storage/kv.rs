use futures::SinkExt;
use kuska_ssb::feed::{Feed as MessageKvt, Message as MessageValue};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use sled::{Config as DbConfig, Db};

use crate::{
    broker::{BrokerEvent, ChBrokerSend, Destination},
    error::Error,
    storage::indexes::Indexes,
    Result,
};

// TODO: Consider replacing prefix-based approach with separate db trees.
/// Prefix for a key to the latest sequence number for a stored feed.
const PREFIX_LATEST_SEQ: u8 = 0u8;
/// Prefix for a key to a message KVT (Key Value Timestamp).
const PREFIX_MSG_KVT: u8 = 1u8;
/// Prefix for a key to a message value (the 'V' in KVT).
const PREFIX_MSG_VAL: u8 = 2u8;
/// Prefix for a key to a blob.
const PREFIX_BLOB: u8 = 3u8;
/// Prefix for a key to a peer.
const PREFIX_PEER: u8 = 4u8;

#[derive(Debug, Clone)]
pub enum StoKvEvent {
    IdChanged(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStatus {
    retrieved: bool,
    users: Vec<String>,
}

/// The public key (ID) of a peer and a message sequence number.
#[derive(Debug, Serialize, Deserialize)]
pub struct PubKeyAndSeqNum {
    pub_key: String,
    seq_num: u64,
}

// TODO: Can we remove the `Option` from all of these fields?
// Will make the rest of the code more compact (no need to match on an
// `Option` every time).
// Will probably require some changes in `solar_cli` config.
#[derive(Default)]
pub struct KvStorage {
    /// The core database which stores messages and blob references.
    db: Option<Db>,
    /// Indexes to allow for efficient database value look-ups.
    pub indexes: Option<Indexes>,
    /// A message-passing sender.
    ch_broker: Option<ChBrokerSend>,
}

impl KvStorage {
    /// Open the key-value database using the given configuration, open the
    /// database index trees and populate the instance of `KvStorage`
    /// with the database, indexes and message-passing sender.
    pub fn open(&mut self, config: DbConfig, ch_broker: ChBrokerSend) -> Result<()> {
        let db = config.open()?;
        let indexes = Indexes::open(&db)?;

        self.db = Some(db);
        self.indexes = Some(indexes);
        self.ch_broker = Some(ch_broker);

        Ok(())
    }

    /// Generate a key for the latest sequence number of the feed authored by
    /// the given public key.
    fn key_latest_seq(user_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_LATEST_SEQ);
        key.extend_from_slice(user_id.as_bytes());
        key
    }

    /// Generate a key for a message KVT authored by the given public key and
    /// with the given message sequence number.
    fn key_msg_kvt(user_id: &str, msg_seq: u64) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_MSG_KVT);
        key.extend_from_slice(&msg_seq.to_be_bytes()[..]);
        key.extend_from_slice(user_id.as_bytes());
        key
    }

    /// Generate a key for a message value with the given ID (reference).
    fn key_msg_val(msg_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_MSG_VAL);
        key.extend_from_slice(msg_id.as_bytes());
        key
    }

    /// Generate a key for a blob with the given ID (reference).
    fn key_blob(blob_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_BLOB);
        key.extend_from_slice(blob_id.as_bytes());
        key
    }

    /// Generate a key for a peer with the given public key.
    fn key_peer(user_id: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(PREFIX_PEER);
        key.extend_from_slice(user_id.as_bytes());
        key
    }

    /// Get the status of a blob with the given ID.
    pub fn get_blob(&self, blob_id: &str) -> Result<Option<BlobStatus>> {
        let db = self.db.as_ref().unwrap();
        if let Some(raw) = db.get(Self::key_blob(blob_id))? {
            Ok(serde_cbor::from_slice(&raw)?)
        } else {
            Ok(None)
        }
    }

    /// Set the status of a blob with the given ID.
    pub fn set_blob(&self, blob_id: &str, blob: &BlobStatus) -> Result<()> {
        let db = self.db.as_ref().unwrap();
        let raw = serde_cbor::to_vec(blob)?;
        db.insert(Self::key_blob(blob_id), raw)?;

        Ok(())
    }

    /// Get a list of IDs for all blobs which have not yet been retrieved.
    pub fn get_pending_blobs(&self) -> Result<Vec<String>> {
        let mut list = Vec::new();

        let db = self.db.as_ref().unwrap();
        let scan_key: &[u8] = &[PREFIX_BLOB];
        for item in db.range(scan_key..) {
            let (k, v) = item?;
            let blob: BlobStatus = serde_cbor::from_slice(&v)?;
            if !blob.retrieved {
                list.push(String::from_utf8_lossy(&k[1..]).to_string());
            }
        }

        Ok(list)
    }

    /// Get the sequence number of the latest message in the feed authored by
    /// the peer with the given public key.
    pub fn get_latest_seq(&self, user_id: &str) -> Result<Option<u64>> {
        let db = self.db.as_ref().unwrap();
        let key = Self::key_latest_seq(user_id);
        let seq = if let Some(value) = db.get(key)? {
            let mut u64_buffer = [0u8; 8];
            u64_buffer.copy_from_slice(&value);
            Some(u64::from_be_bytes(u64_buffer))
        } else {
            None
        };

        Ok(seq)
    }

    /// Get the message KVT (Key Value Timestamp) for the given author and
    /// message sequence number.
    pub fn get_msg_kvt(&self, user_id: &str, msg_seq: u64) -> Result<Option<MessageKvt>> {
        let db = self.db.as_ref().unwrap();
        if let Some(raw) = db.get(Self::key_msg_kvt(user_id, msg_seq))? {
            Ok(Some(MessageKvt::from_slice(&raw)?))
        } else {
            Ok(None)
        }
    }

    /// Get the message value for the given message ID (key).
    pub fn get_msg_val(&self, msg_id: &str) -> Result<Option<MessageValue>> {
        let db = self.db.as_ref().unwrap();

        if let Some(raw) = db.get(Self::key_msg_val(msg_id))? {
            let msg_ref = serde_cbor::from_slice::<PubKeyAndSeqNum>(&raw)?;
            let msg = self
                .get_msg_kvt(&msg_ref.pub_key, msg_ref.seq_num)?
                .unwrap()
                .into_message()?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Get the latest message value authored by the given public key.
    pub fn get_latest_msg_val(&self, user_id: &str) -> Result<Option<MessageValue>> {
        let latest_msg = if let Some(last_id) = self.get_latest_seq(user_id)? {
            Some(
                self.get_msg_kvt(user_id, last_id)?
                    .unwrap()
                    .into_message()?,
            )
        } else {
            None
        };

        Ok(latest_msg)
    }

    /// Add the public key and latest sequence number of a peer to the list of
    /// peers.
    pub async fn set_peer(&self, user_id: &str, latest_seq: u64) -> Result<()> {
        // TODO: Replace unwrap with none error and try operator.
        let db = self.db.as_ref().unwrap();
        db.insert(Self::key_peer(user_id), &latest_seq.to_be_bytes()[..])?;

        // TODO: Should we be flushing here?
        // Flush may have a performance impact. It may also be unnecessary
        // depending on where / when this method is called.

        Ok(())
    }

    /// Return the public key and latest sequence number for all peers in the
    /// database.
    pub async fn get_peers(&self) -> Result<Vec<PubKeyAndSeqNum>> {
        let db = self.db.as_ref().unwrap();
        let mut peers = Vec::new();

        // Use the generic peer prefix to return an iterator over all peers.
        let scan_peer_key: &[u8] = &[PREFIX_PEER];
        for peer in db.range(scan_peer_key..) {
            let (peer_key, _) = peer?;
            // Drop the prefix byte and convert the remaining bytes to
            // a string.
            let pub_key = String::from_utf8_lossy(&peer_key[1..]).to_string();
            // Get the latest sequence number for the peer.
            // Fallback to a value of 0 if a `None` value is returned.
            let seq_num = self.get_latest_seq(&pub_key)?.unwrap_or(0);
            let peer_latest_sequence = PubKeyAndSeqNum { pub_key, seq_num };
            peers.push(peer_latest_sequence)
        }

        Ok(peers)
    }

    /// Append a message value to a feed.
    pub async fn append_feed(&self, msg_val: MessageValue) -> Result<u64> {
        debug!("Appending message to feed in database");
        let seq_num = self.get_latest_seq(msg_val.author())?.map_or(0, |num| num) + 1;

        // TODO: We should really be performing more comprehensive validation.
        // Are there other checks in place behind the scenes?
        if msg_val.sequence() != seq_num {
            return Err(Error::InvalidSequence);
        }

        let author = msg_val.author().to_owned();
        let db = self.db.as_ref().unwrap();

        let msg_ref = serde_cbor::to_vec(&PubKeyAndSeqNum {
            pub_key: author.clone(),
            seq_num,
        })?;
        db.insert(Self::key_msg_val(&msg_val.id().to_string()), msg_ref)?;

        let mut msg_kvt = MessageKvt::new(msg_val.clone());
        msg_kvt.rts = None;
        db.insert(
            Self::key_msg_kvt(&author, seq_num),
            msg_kvt.to_string().as_bytes(),
        )?;
        db.insert(Self::key_latest_seq(&author), &seq_num.to_be_bytes()[..])?;

        // Add the public key and latest sequence number for this peer to the
        // list of peers.
        self.set_peer(&author, seq_num).await?;

        debug!("Passing message to indexer");
        // Pass the author and message value to the indexer.
        if let Some(indexes) = &self.indexes {
            indexes.index_msg(&author, msg_val)?
        }

        db.flush_async().await?;

        // Publish a notification that the feed belonging to the given public
        // key has been updated.
        let broker_msg = BrokerEvent::new(Destination::Broadcast, StoKvEvent::IdChanged(author));

        // Matching on the error here (instead of unwrapping) allows us to
        // write unit tests for `append_feed`; a case where we do not have
        // a broker deployed to receive the event message.
        if let Err(err) = self.ch_broker.as_ref().unwrap().send(broker_msg).await {
            warn!(
                "Failed to notify broker of message appended to kv store: {}",
                err
            )
        };

        Ok(seq_num)
    }

    /// Get all messages comprising the feed authored by the given public key.
    pub fn get_feed(&self, user_id: &str) -> Result<Vec<MessageKvt>> {
        let mut feed = Vec::new();

        // Lookup the latest sequence number for the given peer.
        if let Some(latest_seq) = self.get_latest_seq(user_id)? {
            // Iterate through the messages in the feed.
            for msg_seq in 1..=latest_seq {
                // Get the message KVT for the given author and message
                // sequence number and add it to the feed vector.
                //
                // TODO: consider handling the `None` case instead of
                // unwrapping.
                feed.push(self.get_msg_kvt(user_id, msg_seq)?.unwrap())
            }
        }

        Ok(feed)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use kuska_ssb::{api::dto::content::TypedMessage, keystore::OwnedIdentity};
    use serde_json::json;
    use sled::Config;

    use crate::secret_config::SecretConfig;

    fn open_temporary_kv() -> Result<KvStorage> {
        let mut kv = KvStorage::default();
        let (sender, _) = futures::channel::mpsc::unbounded();
        let path = tempdir::TempDir::new("solardb").unwrap();
        let config = Config::new().path(path.path());
        kv.open(config, sender).unwrap();

        Ok(kv)
    }

    fn initialise_keypair_and_kv() -> Result<(OwnedIdentity, KvStorage)> {
        // Create a unique keypair to sign messages.
        let keypair = SecretConfig::create().to_owned_identity()?;

        // Open a temporary key-value store.
        let kv = open_temporary_kv()?;

        Ok((keypair, kv))
    }

    #[async_std::test]
    async fn test_feed_length() -> Result<()> {
        use kuska_ssb::feed::Message;

        let (keypair, kv) = initialise_keypair_and_kv()?;

        let mut last_msg: Option<Message> = None;
        for i in 1..=4 {
            // Create a post-type message.
            let msg_content = TypedMessage::Post {
                text: format!("Important announcement #{i}"),
                mentions: None,
            };

            let msg = MessageValue::sign(last_msg.as_ref(), &keypair, json!(msg_content))?;

            // Append the signed message to the feed. Returns the sequence number
            // of the appended message.
            let seq = kv.append_feed(msg).await?;
            assert_eq!(seq, i);

            last_msg = kv.get_latest_msg_val(&keypair.id)?;

            let feed = kv.get_feed(&keypair.id)?;
            assert_eq!(feed.len(), i as usize);
        }

        Ok(())
    }

    #[async_std::test]
    async fn test_single_message_content_matches() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        // Create a post-type message.
        let msg_content = TypedMessage::Post {
            text: "A strange rambling expiration of my own conscious".to_string(),
            mentions: None,
        };

        let last_msg = kv.get_latest_msg_val(&keypair.id)?;
        let msg = MessageValue::sign(last_msg.as_ref(), &keypair, json!(msg_content))?;

        // Append the signed message to the feed. Returns the sequence number
        // of the appended message.
        let seq = kv.append_feed(msg).await?;
        assert_eq!(seq, 1);

        let latest_seq = kv.get_latest_seq(&keypair.id)?;
        assert_eq!(latest_seq, Some(1));

        // Lookup the value of the previous message. This will be `None`
        let last_msg = kv.get_latest_msg_val(&keypair.id)?;
        assert!(last_msg.is_some());
        let expected = serde_json::value::to_value(msg_content)?;
        let last_msg = last_msg.unwrap().content().clone();

        assert_eq!(last_msg, expected);

        Ok(())
    }

    #[async_std::test]
    async fn test_new_feed_is_empty() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        // Lookup the value of the previous message. This will be `None`
        let last_msg = kv.get_latest_msg_val(&keypair.id)?;
        assert!(last_msg.is_none());

        let latest_seq = kv.get_latest_seq(&keypair.id)?;
        assert!(latest_seq.is_none());

        Ok(())
    }

    // In reality this test covers more than just the append method.
    // It tests multiple methods exposed by the kv database.
    // The main reason for combining the tests is the cost of setting up
    // testable conditions (ie. creating the keypair and database and
    // it with messages). Perhaps this could be broken up in the future.
    #[async_std::test]
    async fn test_append_feed() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        // Create a post-type message.
        let msg_content = TypedMessage::Post {
            text: "A solar flare is an intense localized eruption of electromagnetic radiation."
                .to_string(),
            mentions: None,
        };

        // Lookup the value of the previous message. This will be `None`.
        let last_msg = kv.get_latest_msg_val(&keypair.id)?;

        // Sign the message content using the temporary keypair and value of
        // the previous message.
        let msg = MessageValue::sign(last_msg.as_ref(), &keypair, json!(msg_content))?;

        // Append the signed message to the feed. Returns the sequence number
        // of the appended message.
        let seq = kv.append_feed(msg).await?;

        // Ensure that the message is the first in the feed.
        assert_eq!(seq, 1);

        // Get the latest sequence number.
        let latest_seq = kv.get_latest_seq(&keypair.id)?;

        // Ensure the stored sequence number matches that of the appended
        // message.
        assert_eq!(latest_seq, Some(seq));

        // Get a list of all replicated peers and their latest sequence
        // numbers. This list is expected to contain an entry for the
        // local keypair.
        let peers = kv.get_peers().await?;

        // Ensure there is only one entry in the peers list.
        assert_eq!(peers.len(), 1);
        // Ensure the public key of the peer matches expectations and that
        // the sequence number is correct.
        assert_eq!(peers[0].pub_key, keypair.id);
        assert_eq!(peers[0].seq_num, 1);

        // Create, sign and append a second post-type message.
        let msg_content_2 = TypedMessage::Post {
            text: "When the sun shone upon her.".to_string(),
            mentions: None,
        };
        let last_msg_2 = kv.get_latest_msg_val(&keypair.id)?;
        let msg_2 = MessageValue::sign(last_msg_2.as_ref(), &keypair, json!(msg_content_2))?;
        let msg_2_clone = msg_2.clone();
        let seq_2 = kv.append_feed(msg_2).await?;

        // Ensure that the message is the second in the feed.
        assert_eq!(seq_2, 2);

        // Get the second message in the key-value store in the form of a KVT.
        let msg_kvt = kv.get_msg_kvt(&keypair.id, 2)?;
        assert!(msg_kvt.is_some());

        // Retrieve the key from the KVT.
        let msg_kvt_key = msg_kvt.unwrap().key;

        // Get the second message in the key-value store in the form of a value.
        let msg_val = kv.get_msg_val(&msg_kvt_key)?;

        // Ensure the retrieved message value matches the previously created
        // and signed message.
        assert_eq!(msg_val, Some(msg_2_clone));

        // Get all messages comprising the feed.
        let feed = kv.get_feed(&keypair.id)?;

        // Ensure that two messages are returned.
        assert_eq!(feed.len(), 2);

        Ok(())
    }

    #[test]
    fn test_blobs() -> Result<()> {
        let kv = open_temporary_kv()?;

        assert!(kv.get_blob("1")?.is_none());

        kv.set_blob(
            "b1",
            &BlobStatus {
                retrieved: true,
                users: ["u1".to_string()].to_vec(),
            },
        )?;

        kv.set_blob(
            "b2",
            &BlobStatus {
                retrieved: false,
                users: ["u2".to_string()].to_vec(),
            },
        )?;

        if let Some(blob) = kv.get_blob("b1")? {
            assert!(blob.retrieved);
            assert_eq!(blob.users, ["u1".to_string()].to_vec());
            assert_eq!(kv.get_pending_blobs().unwrap(), ["b2".to_string()].to_vec());
        }

        kv.set_blob(
            "b1",
            &BlobStatus {
                retrieved: false,
                users: ["u7".to_string()].to_vec(),
            },
        )?;

        if let Some(blob) = kv.get_blob("b1")? {
            assert!(!blob.retrieved);
            assert_eq!(blob.users, ["u7".to_string()].to_vec());
            assert_eq!(
                kv.get_pending_blobs()?,
                ["b1".to_string(), "b2".to_string()].to_vec()
            );
        }

        Ok(())
    }
}
