use kuska_ssb::{api::dto::content::TypedMessage, feed::Message};
use once_cell::sync::Lazy;
use regex::Regex;

/// Regex pattern used to match blob references.
pub static BLOB_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(&[0-9A-Za-z/+=]*.sha256)").unwrap());

/// Extract blob references from post-type messages.
pub fn extract_blob_refs(msg: &Message) -> Vec<String> {
    let mut refs = Vec::new();

    let msg = serde_json::from_value(msg.content().clone());

    if let Ok(TypedMessage::Post { text, .. }) = msg {
        for cap in BLOB_REGEX.captures_iter(&text) {
            let key = cap.get(0).unwrap().as_str().to_owned();
            refs.push(key);
        }
    }

    refs
}
