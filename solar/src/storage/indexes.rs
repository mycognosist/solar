use std::collections::HashSet;

use kuska_ssb::{
    api::dto::content::{Image, TypedMessage as MessageContent},
    feed::Message as MessageValue,
};
use sled::{Db, Tree};

use crate::Result;

/// Database indexes, each stored in a tree of the main database.
pub struct Indexes {
    /// Blocks.
    blocks: Tree,
    /// Blockers.
    blockers: Tree,
    /// Channel subscribers.
    channel_subscribers: Tree,
    /// Channel subscriptions.
    channel_subscriptions: Tree,
    /// Descriptions.
    descriptions: Tree,
    /// Follows.
    follows: Tree,
    /// Followers.
    followers: Tree,
    /// Friends.
    friends: Tree,
    /// Image references.
    images: Tree,
    /// Names.
    names: Tree,
}

impl Indexes {
    /// Open a database tree for each index.
    pub fn open(db: &Db) -> Result<Indexes> {
        let blocks = db.open_tree("blocks")?;
        let blockers = db.open_tree("blockers")?;
        let channel_subscribers = db.open_tree("channel_subscribers")?;
        let channel_subscriptions = db.open_tree("channel_subscriptions")?;
        let descriptions = db.open_tree("descriptions")?;
        let follows = db.open_tree("follows")?;
        let followers = db.open_tree("followers")?;
        let friends = db.open_tree("friends")?;
        let images = db.open_tree("images")?;
        let names = db.open_tree("names")?;

        let indexes = Indexes {
            blocks,
            blockers,
            channel_subscribers,
            channel_subscriptions,
            descriptions,
            follows,
            followers,
            friends,
            images,
            names,
        };

        Ok(indexes)
    }

    /// Index a message based on the author (SSB ID) and content type.
    pub fn index_msg(&self, author: &str, msg_val: MessageValue) -> Result<()> {
        if let Some(content_val) = msg_val.value.get("content") {
            let content: MessageContent = serde_json::from_value(content_val.to_owned())?;

            match content {
                MessageContent::About { .. } => self.index_about(author, content)?,
                MessageContent::Channel {
                    channel,
                    subscribed,
                } => self.index_channel(author, channel, subscribed)?,
                MessageContent::Contact { .. } => self.index_contact(author, content)?,
                _ => (),
            }
        }

        Ok(())
    }

    /// Index the content of an about-type message.
    fn index_about(&self, user_id: &str, msg_content: MessageContent) -> Result<()> {
        // Match on each field of an about-type message and call each individual
        // indexer as required. This allows us to catch the possibility that
        // multiple fields are set in a single message (such as name and
        // description).
        if let MessageContent::About {
            about,
            description,
            image,
            name,
            ..
        } = msg_content
        {
            if let Some(description) = description {
                self.index_description(user_id, &about, description)?
            }
            if let Some(image) = image {
                self.index_image(user_id, &about, image)?
            }
            if let Some(name) = name {
                self.index_name(user_id, &about, name)?
            }
        }

        Ok(())
    }

    /// Add the given block to the block indexes.
    fn index_blocking(&self, user_id: &str, contact: &str, blocking: bool) -> Result<()> {
        self.index_block(user_id, contact, blocking)?;
        self.index_blocker(user_id, contact, blocking)?;

        Ok(())
    }

    /// Update the blocks index for the given blocker ID, blocked ID and block
    /// state.
    fn index_block(&self, blocker_id: &str, blocked_id: &str, blocked: bool) -> Result<()> {
        let mut blocks = self.get_blocks(blocker_id)?;

        if blocked {
            blocks.insert(blocked_id.to_owned());
        } else {
            blocks.remove(blocked_id);
        }

        self.blocks
            .insert(blocker_id, serde_cbor::to_vec(&blocks)?)?;

        Ok(())
    }

    /// Return the public keys representing all peers blocked by the given
    /// public key.
    fn get_blocks(&self, blocker_id: &str) -> Result<HashSet<String>> {
        let blocks = if let Some(raw) = self.blocks.get(blocker_id)? {
            serde_cbor::from_slice::<HashSet<String>>(&raw)?
        } else {
            HashSet::new()
        };

        Ok(blocks)
    }

    /// Update the blockers index for the given blocker ID, blocked ID and block
    /// state.
    fn index_blocker(&self, blocker_id: &str, blocked_id: &str, blocked: bool) -> Result<()> {
        let mut blockers = self.get_blockers(blocked_id)?;

        if blocked {
            blockers.insert(blocker_id.to_owned());
        } else {
            blockers.remove(blocker_id);
        }

        self.blockers
            .insert(blocked_id, serde_cbor::to_vec(&blockers)?)?;

        Ok(())
    }

    /// Return the public keys representing all peers blocking the given
    /// public key.
    fn get_blockers(&self, blocked_id: &str) -> Result<HashSet<String>> {
        let blockers = if let Some(raw) = self.blockers.get(blocked_id)? {
            serde_cbor::from_slice::<HashSet<String>>(&raw)?
        } else {
            HashSet::new()
        };

        Ok(blockers)
    }

    /// Add the given channel to the channel indexes.
    fn index_channel(&self, user_id: &str, channel: String, subscribed: bool) -> Result<()> {
        self.index_channel_subscriber(user_id, &channel, subscribed)?;
        self.index_channel_subscription(user_id, &channel, subscribed)?;

        Ok(())
    }

    /// Update the channel subscribers index for the given public key, channel
    /// and subscription state.
    fn index_channel_subscriber(
        &self,
        user_id: &str,
        channel: &str,
        subscribed: bool,
    ) -> Result<()> {
        let mut subscribers = self.get_channel_subscribers(channel)?;

        if subscribed {
            subscribers.insert(user_id.to_owned());
        } else {
            subscribers.remove(user_id);
        }

        self.channel_subscribers
            .insert(channel, serde_cbor::to_vec(&subscribers)?)?;

        Ok(())
    }

    /// Return all subscribers of the given channel.
    fn get_channel_subscribers(&self, channel: &str) -> Result<HashSet<String>> {
        let subscribers = if let Some(raw) = self.channel_subscribers.get(channel)? {
            serde_cbor::from_slice::<HashSet<String>>(&raw)?
        } else {
            HashSet::new()
        };

        Ok(subscribers)
    }

    /// Update the channel subscription index for the given public key, channel
    /// and subscription state.
    fn index_channel_subscription(
        &self,
        user_id: &str,
        channel: &str,
        subscribed: bool,
    ) -> Result<()> {
        let mut subscriptions = self.get_channel_subscriptions(user_id)?;

        if subscribed {
            subscriptions.insert(channel.to_owned());
        } else {
            subscriptions.remove(channel);
        }

        self.channel_subscriptions
            .insert(user_id, serde_cbor::to_vec(&subscriptions)?)?;

        Ok(())
    }

    /// Return all the channel subscriptions for the given public key.
    fn get_channel_subscriptions(&self, user_id: &str) -> Result<HashSet<String>> {
        let subscriptions = if let Some(raw) = self.channel_subscriptions.get(user_id)? {
            serde_cbor::from_slice::<HashSet<String>>(&raw)?
        } else {
            HashSet::new()
        };

        Ok(subscriptions)
    }

    /// Index the content of a contact-type message.
    fn index_contact(&self, user_id: &str, msg_content: MessageContent) -> Result<()> {
        if let MessageContent::Contact {
            contact: Some(contact),
            blocking,
            following,
            ..
        } = msg_content
        {
            if let Some(blocking) = blocking {
                self.index_blocking(user_id, &contact, blocking)?
            }
            if let Some(following) = following {
                self.index_following(user_id, &contact, following)?
            }
        }

        Ok(())
    }

    /// Add the given description to the description index for the associated
    /// public key.
    fn index_description(
        &self,
        author_id: &str,
        about_id: &str,
        description: String,
    ) -> Result<()> {
        let mut descriptions = self.get_descriptions(about_id)?;
        descriptions.push((author_id.to_owned(), description));
        self.descriptions
            .insert(about_id, serde_cbor::to_vec(&descriptions)?)?;

        Ok(())
    }

    /// Return all indexed descriptions for the given public key.
    fn get_descriptions(&self, user_id: &str) -> Result<Vec<(String, String)>> {
        let descriptions = if let Some(raw) = self.descriptions.get(user_id)? {
            serde_cbor::from_slice::<Vec<(String, String)>>(&raw)?
        } else {
            Vec::new()
        };

        Ok(descriptions)
    }

    /// Return all indexed self-assigned descriptions for the given public key.
    fn get_self_assigned_descriptions(&self, user_id: &str) -> Result<Vec<(String, String)>> {
        let mut descriptions = self.get_descriptions(user_id)?;
        descriptions.retain(|(author, _description)| author == user_id);

        Ok(descriptions)
    }

    /// Return the most recently indexed description for the given public key.
    fn get_latest_description(&self, user_id: &str) -> Result<Option<(String, String)>> {
        let descriptions = self.get_descriptions(user_id)?;
        let description = descriptions.last().cloned();

        Ok(description)
    }

    /// Return the most recently indexed self-assigned description for the given
    /// public key.
    fn get_latest_self_assigned_description(
        &self,
        user_id: &str,
    ) -> Result<Option<(String, String)>> {
        let self_descriptions = self.get_self_assigned_descriptions(user_id)?;
        let description = self_descriptions.last().cloned();

        Ok(description)
    }

    /// Add the given follow to the follow indexes.
    fn index_following(&self, user_id: &str, contact: &str, following: bool) -> Result<()> {
        self.index_follow(user_id, contact, following)?;
        self.index_follower(user_id, contact, following)?;
        self.index_friend(user_id, contact)?;

        Ok(())
    }

    /// Update the follows index for the given follower ID, followed ID and
    /// follow state.
    fn index_follow(&self, follower_id: &str, followed_id: &str, followed: bool) -> Result<()> {
        let mut follows = self.get_follows(follower_id)?;

        if followed {
            follows.insert(followed_id.to_owned());
        } else {
            follows.remove(followed_id);
        }

        self.follows
            .insert(follower_id, serde_cbor::to_vec(&follows)?)?;

        Ok(())
    }

    /// Return the public keys representing all peers followed by the given
    /// public key.
    fn get_follows(&self, follower_id: &str) -> Result<HashSet<String>> {
        let follows = if let Some(raw) = self.follows.get(follower_id)? {
            serde_cbor::from_slice::<HashSet<String>>(&raw)?
        } else {
            HashSet::new()
        };

        Ok(follows)
    }

    /// Update the followers index for the given follower ID, followed ID and
    /// follow state.
    fn index_follower(&self, follower_id: &str, followed_id: &str, followed: bool) -> Result<()> {
        let mut followers = self.get_followers(followed_id)?;

        if followed {
            followers.insert(follower_id.to_owned());
        } else {
            followers.remove(follower_id);
        }

        self.followers
            .insert(followed_id, serde_cbor::to_vec(&followers)?)?;

        Ok(())
    }

    /// Return the public keys representing all peers who follow the given
    /// public key.
    fn get_followers(&self, followed_id: &str) -> Result<HashSet<String>> {
        let followers = if let Some(raw) = self.followers.get(followed_id)? {
            serde_cbor::from_slice::<HashSet<String>>(&raw)?
        } else {
            HashSet::new()
        };

        Ok(followers)
    }

    /// Query whether or not the first given public key follows the second.
    fn is_following(&self, user_id: &str, peer_id: &str) -> Result<bool> {
        let follows = self.get_follows(user_id)?;
        let following = follows.contains(peer_id);

        Ok(following)
    }

    /// Update the friends index for the given pair of peers.
    fn index_friend(&self, peer_a: &str, peer_b: &str) -> Result<()> {
        let mut peer_a_friends = self.get_friends(peer_a)?;
        let mut peer_b_friends = self.get_friends(peer_b)?;

        if self.is_following(peer_a, peer_b)? && self.is_following(peer_b, peer_a)? {
            peer_a_friends.insert(peer_b.to_owned());
            peer_b_friends.insert(peer_a.to_owned());
        } else {
            peer_a_friends.remove(peer_b);
            peer_b_friends.remove(peer_a);
        }

        self.friends
            .insert(peer_a, serde_cbor::to_vec(&peer_a_friends)?)?;
        self.friends
            .insert(peer_b, serde_cbor::to_vec(&peer_b_friends)?)?;

        Ok(())
    }

    /// Return the public keys representing all the friends (mutual follows)
    /// of the given public key.
    fn get_friends(&self, ssb_id: &str) -> Result<HashSet<String>> {
        let friends = if let Some(raw) = self.friends.get(ssb_id)? {
            serde_cbor::from_slice::<HashSet<String>>(&raw)?
        } else {
            HashSet::new()
        };

        Ok(friends)
    }

    /// Add the given image reference to the image index for the associated
    /// public key.
    fn index_image(&self, author_id: &str, about_id: &str, image: Image) -> Result<()> {
        // TODO: Handle `Image::Complete { .. }` variant.
        if let Image::OnlyLink(ssb_hash) = image {
            let mut images = self.get_images(about_id)?;
            images.push((author_id.to_owned(), ssb_hash));
            self.images.insert(about_id, serde_cbor::to_vec(&images)?)?;
        }

        Ok(())
    }

    /// Return all indexed image references for the given public key.
    fn get_images(&self, user_id: &str) -> Result<Vec<(String, String)>> {
        let images = if let Some(raw) = self.images.get(user_id)? {
            serde_cbor::from_slice::<Vec<(String, String)>>(&raw)?
        } else {
            Vec::new()
        };

        Ok(images)
    }

    /// Return all indexed self-assigned image references for the given public
    /// key.
    fn get_self_assigned_images(&self, user_id: &str) -> Result<Vec<(String, String)>> {
        let mut images = self.get_images(user_id)?;
        images.retain(|(author, _image)| author == user_id);

        Ok(images)
    }

    /// Return the most recently indexed image reference for the given public
    /// key.
    fn get_latest_image(&self, user_id: &str) -> Result<Option<(String, String)>> {
        let images = self.get_images(user_id)?;
        let image = images.last().cloned();

        Ok(image)
    }

    /// Return the most recently indexed self-assigned image reference for the
    /// given public key.
    fn get_latest_self_assigned_image(&self, user_id: &str) -> Result<Option<(String, String)>> {
        let images = self.get_self_assigned_images(user_id)?;
        let image = images.last().cloned();

        Ok(image)
    }

    /// Add the given name to the name index for the associated public key.
    fn index_name(&self, author_id: &str, about_id: &str, name: String) -> Result<()> {
        // TODO: Do we also want to store the hash of the associated message?
        let mut names = self.get_names(about_id)?;
        names.push((author_id.to_owned(), name));
        self.names.insert(about_id, serde_cbor::to_vec(&names)?)?;

        Ok(())
    }

    /// Return all indexed names for the given public key.
    fn get_names(&self, user_id: &str) -> Result<Vec<(String, String)>> {
        let names = if let Some(raw) = self.names.get(user_id)? {
            serde_cbor::from_slice::<Vec<(String, String)>>(&raw)?
        } else {
            Vec::new()
        };

        Ok(names)
    }

    /// Return all indexed self-assigned names for the given public key.
    fn get_self_assigned_names(&self, user_id: &str) -> Result<Vec<(String, String)>> {
        let mut names = self.get_names(user_id)?;
        names.retain(|(author, _image)| author == user_id);

        Ok(names)
    }

    /// Return the most recently indexed name for the given public key.
    fn get_latest_name(&self, user_id: &str) -> Result<Option<(String, String)>> {
        let names = self.get_names(user_id)?;
        let name = names.last().cloned();

        Ok(name)
    }

    /// Return the most recently indexed self-assigned name for the given public
    /// key.
    fn get_latest_self_assigned_name(&self, user_id: &str) -> Result<Option<(String, String)>> {
        let names = self.get_self_assigned_names(user_id)?;
        let name = names.last().cloned();

        Ok(name)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use kuska_ssb::{
        api::dto::content::{Image, TypedMessage},
        feed::Message as MessageValue,
        keystore::OwnedIdentity,
    };
    use serde_json::json;
    use sled::Config;

    use crate::secret_config::SecretConfig;
    use crate::storage::kv::KvStorage;

    fn open_temporary_kv() -> Result<KvStorage> {
        let mut kv = KvStorage::default();
        let (sender, _) = futures::channel::mpsc::unbounded();
        let path = tempdir::TempDir::new("solardb")?;
        let config = Config::new().path(path.path());
        kv.open(config, sender)?;

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
    async fn test_about_indexes() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        if let Some(indexes) = kv.indexes.as_ref() {
            let first_name = "mycognosist".to_string();
            let first_description = "just a humble fungi".to_string();
            let image_ref = "&8M2JFEFHlxJ5q8Lmu3P4bDdCHg0SLB27Q321cy9Upx4=.sha256".to_string();

            // Create an about-type message which assigns a name.
            let first_msg_content = TypedMessage::About {
                about: keypair.id.to_owned(),
                name: Some(first_name.to_owned()),
                branch: None,
                description: Some(first_description.to_owned()),
                image: Some(Image::OnlyLink(image_ref.to_owned())),
                location: None,
                start_datetime: None,
                title: None,
            };

            let last_msg = kv.get_latest_msg_val(&keypair.id).unwrap();
            let first_msg =
                MessageValue::sign(last_msg.as_ref(), &keypair, json!(first_msg_content)).unwrap();

            indexes.index_msg(&keypair.id, first_msg)?;

            if let Some((_author, description)) = indexes.get_latest_description(&keypair.id)? {
                assert_eq!(description, first_description);
            }

            if let Some((_author, image)) = indexes.get_latest_image(&keypair.id)? {
                assert_eq!(image, image_ref);
            }

            if let Some((_author, name)) = indexes.get_latest_name(&keypair.id)? {
                assert_eq!(name, first_name);
            }

            let second_name = "glyph".to_string();
            let second_description =
                "[ sowing seeds of symbiosis | weaving webs of wu wei ]".to_string();

            let second_msg_content = TypedMessage::About {
                about: keypair.id.to_owned(),
                name: Some(second_name.to_owned()),
                branch: None,
                description: Some(second_description.to_owned()),
                image: None,
                location: None,
                start_datetime: None,
                title: None,
            };

            let last_msg = kv.get_latest_msg_val(&keypair.id).unwrap();
            let second_msg =
                MessageValue::sign(last_msg.as_ref(), &keypair, json!(second_msg_content)).unwrap();

            indexes.index_msg(&keypair.id, second_msg)?;

            if let Some((_author, lastest_name)) = indexes.get_latest_name(&keypair.id)? {
                assert_eq!(lastest_name, second_name);
            }

            if let Some((_author, latest_description)) =
                indexes.get_latest_description(&keypair.id)?
            {
                assert_eq!(latest_description, second_description);
            }
        }

        Ok(())
    }

    #[async_std::test]
    async fn test_channel_indexes() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;

        if let Some(indexes) = kv.indexes.as_ref() {
            let channel = "myco".to_string();
            let subscribed = true;

            // Create a channel-type message which subscribes to a channel.
            let subscribe_msg_content = TypedMessage::Channel {
                channel: channel.to_owned(),
                subscribed,
            };

            let last_msg = kv.get_latest_msg_val(&keypair.id).unwrap();
            let subscribe_msg =
                MessageValue::sign(last_msg.as_ref(), &keypair, json!(subscribe_msg_content))
                    .unwrap();

            indexes.index_msg(&keypair.id, subscribe_msg)?;

            let subscribers = indexes.get_channel_subscribers(&channel)?;
            assert!(subscribers.contains(&keypair.id));

            let subscriptions = indexes.get_channel_subscriptions(&keypair.id)?;
            assert!(subscriptions.contains(&channel));

            // Create a channel-type message which unsubscribes to a channel.
            let unsubscribe_msg_content = TypedMessage::Channel {
                channel: channel.to_owned(),
                subscribed: false,
            };

            let last_msg = kv.get_latest_msg_val(&keypair.id).unwrap();
            let unsubscribe_msg =
                MessageValue::sign(last_msg.as_ref(), &keypair, json!(unsubscribe_msg_content))
                    .unwrap();

            indexes.index_msg(&keypair.id, unsubscribe_msg)?;

            let subscribers = indexes.get_channel_subscribers(&channel)?;
            assert!(!subscribers.contains(&keypair.id));

            let subscriptions = indexes.get_channel_subscriptions(&keypair.id)?;
            assert!(!subscriptions.contains(&channel));
        }

        Ok(())
    }

    #[async_std::test]
    async fn test_contact_indexes() -> Result<()> {
        let (keypair, kv) = initialise_keypair_and_kv()?;
        let blocked_keypair = SecretConfig::create().to_owned_identity()?;

        if let Some(indexes) = kv.indexes.as_ref() {
            // Create a contact-type message which blocks an ID.
            let block_msg_content = TypedMessage::Contact {
                contact: Some(blocked_keypair.id.to_owned()),
                blocking: Some(true),
                following: Some(false),
                autofollow: None,
            };

            let last_msg = kv.get_latest_msg_val(&keypair.id).unwrap();
            let block_msg =
                MessageValue::sign(last_msg.as_ref(), &keypair, json!(block_msg_content)).unwrap();

            indexes.index_msg(&keypair.id, block_msg)?;

            let blocks = indexes.get_blocks(&keypair.id)?;
            assert!(blocks.contains(&blocked_keypair.id));

            let blockers = indexes.get_blockers(&blocked_keypair.id)?;
            assert!(blockers.contains(&keypair.id));

            let follows = indexes.get_follows(&keypair.id)?;
            assert!(!follows.contains(&blocked_keypair.id));

            let followers = indexes.get_followers(&blocked_keypair.id)?;
            assert!(!followers.contains(&keypair.id));

            // Create a contact-type message which unblocks an ID.
            let unblock_msg_content = TypedMessage::Contact {
                contact: Some(blocked_keypair.id.to_owned()),
                blocking: Some(false),
                following: Some(true),
                autofollow: None,
            };

            let last_msg = kv.get_latest_msg_val(&keypair.id).unwrap();
            let unblock_msg =
                MessageValue::sign(last_msg.as_ref(), &keypair, json!(unblock_msg_content))
                    .unwrap();

            indexes.index_msg(&keypair.id, unblock_msg)?;

            let blocks = indexes.get_blocks(&keypair.id)?;
            assert!(!blocks.contains(&blocked_keypair.id));

            let blockers = indexes.get_blockers(&blocked_keypair.id)?;
            assert!(!blockers.contains(&keypair.id));

            let follows = indexes.get_follows(&keypair.id)?;
            assert!(follows.contains(&blocked_keypair.id));

            let followers = indexes.get_followers(&blocked_keypair.id)?;
            assert!(followers.contains(&keypair.id));

            let friends = indexes.get_friends(&keypair.id)?;
            assert!(!friends.contains(&blocked_keypair.id));

            // Create a contact-type message which defines a follow of the
            // initial keypair by the second keypair.
            let follow_back_msg_content = TypedMessage::Contact {
                contact: Some(keypair.id.to_owned()),
                blocking: Some(false),
                following: Some(true),
                autofollow: None,
            };

            let last_msg = kv.get_latest_msg_val(&blocked_keypair.id).unwrap();
            let follow_back_msg = MessageValue::sign(
                last_msg.as_ref(),
                &blocked_keypair,
                json!(follow_back_msg_content),
            )
            .unwrap();

            indexes.index_msg(&blocked_keypair.id, follow_back_msg)?;

            // The peers should now be friends (mutual followers).
            let friends = indexes.get_friends(&keypair.id)?;
            assert!(friends.contains(&blocked_keypair.id));
        }

        Ok(())
    }
}
