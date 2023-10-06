//! Epidemic Broadcast Tree (EBT) Replication.
//!
//! Two kinds of messages are sent by both peers during an EBT session:
//!
//!  - control messages known as notes
//!  - feed messages
//!
//! Each note is a JSON object containing one or more name/value pairs.

// EBT in ScuttleGo:
//
// https://github.com/planetary-social/scuttlego/commit/e1412a550652c791dd97c72797ed512f385669e8
// http://dev.planetary.social/replication/ebt.html

// serde_json::to_string(&notes)?;
// "{\"@FCX/tsDLpubCPKKfIrw4gc+SQkHcaD17s7GI6i/ziWY=.ed25519\":123,\"@l1sGqWeCZRA99gN+t9sI6+UOzGcHq3KhLQUYEwb4DCo=.ed25519\":-1}"

use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
};

use crate::{config::PEERS_TO_REPLICATE, node::KV_STORE, Result};

/// An SSB identity in the form `@...=.ed25519`.
// The validity of this string should be confirmed when a note is received.
// Receiving a note with a malformed feed identifier should terminate the EBT
// session with an error.
type SsbId = String;
/// The encoded vector clock value.
// Receiving a note with a malformed value should terminate the EBT session
// with an error.
type EncodedClockValue = i64;

/// A vector clock which maps an SSB ID to an encoded vector clock value.
type VectorClock = HashMap<SsbId, EncodedClockValue>;

struct Ebt {
    /// The SSB ID of the local node.
    id: SsbId,
    /// Duration to wait before switching feed request to a different peer.
    timeout: u16,
    /// The local vector clock.
    local_clock: VectorClock,
    /// The vector clock for each known peer.
    peer_clocks: HashMap<SsbId, VectorClock>,
    /// A set of all the feeds for which active requests are open.
    ///
    /// This allows us to avoid requesting a feed from multiple peers
    /// simultaneously.
    requested_feeds: HashSet<SsbId>,
}

impl Default for Ebt {
    fn default() -> Self {
        Ebt {
            id: String::new(),
            timeout: 3000,
            local_clock: HashMap::new(),
            peer_clocks: HashMap::new(),
            requested_feeds: HashSet::new(),
        }
    }
}

impl Ebt {
    // Read peer clock state from file.
    // fn load_peer_clocks()
    // Write peer clock state to file.
    // fn persist_peer_clocks()

    async fn init() -> Result<Self> {
        let mut ebt = Ebt::default();

        // Get list of peers to replicate.
        if let Some(peers) = PEERS_TO_REPLICATE.get() {
            // Request replication of each peer.
            for peer in peers.keys() {
                ebt.request(peer).await?;
            }
        }
        // TODO: Load peer clocks from file and update `peer_clocks`.

        Ok(ebt)
    }

    /// Retrieve the vector clock for the given SSB ID.
    fn get_clock(self, peer_id: &SsbId) -> Option<VectorClock> {
        if peer_id == &self.id {
            Some(self.local_clock)
        } else {
            self.peer_clocks.get(peer_id).cloned()
        }
    }

    /// Set or update the vector clock for the given SSB ID.
    fn set_clock(&mut self, peer_id: &SsbId, clock: VectorClock) {
        if peer_id == &self.id {
            self.local_clock = clock
        } else {
            self.peer_clocks.insert(peer_id.to_owned(), clock);
        }
    }

    /// Request that the feed represented by the given SSB ID be replicated.
    async fn replicate(&mut self, peer_id: &SsbId) -> Result<()> {
        // Look up the latest sequence for the given ID.
        if let Some(seq) = KV_STORE.read().await.get_latest_seq(peer_id)? {
            // Encode the replicate flag, receive flag and sequence.
            let encoded_value: EncodedClockValue = Ebt::encode(true, Some(true), Some(seq))?;
            // Insert the ID and encoded sequence into the local clock.
            self.local_clock.insert(peer_id.to_owned(), encoded_value);
        }

        Ok(())
    }

    /// Revoke a replication request for the feed represented by the given SSB
    /// ID.
    fn revoke(&mut self, peer_id: &SsbId) {
        self.local_clock.remove(peer_id);
    }

    /// Request the feed represented by the given SSB ID from a peer.
    fn request(&mut self, peer_id: &SsbId) {
        self.requested_feeds.insert(peer_id);
    }

    /// Decode a value from a control message (aka. note), returning the values
    /// of the replicate flag, receive flag and sequence.
    ///
    /// If the replicate flag is `false`, the peer does not wish to receive
    /// messages for the referenced feed.
    ///
    /// If the replicate flag is `true`, values will be returned for the receive
    /// flag and sequence. The sequence refers to a sequence number of the
    /// referenced feed.
    fn decode(value: i64) -> Result<(bool, Option<bool>, Option<u64>)> {
        let (replicate_flag, receive_flag, sequence) = if value < 0 {
            // Replicate flag is `false`.
            // Peer does not wish to receive messages for this feed.
            (false, None, None)
        } else {
            // Get the least-significant bit (aka. rightmost bit).
            let lsb = value & 1;
            // Set the receive flag value.
            let receive_flag = lsb == 0;
            // Perform a single bit arithmetic right shift to obtain the sequence
            // number.
            let sequence: u64 = (value >> 1).try_into()?;

            (true, Some(receive_flag), Some(sequence))
        };

        Ok((replicate_flag, receive_flag, sequence))
    }

    /// Encode a replicate flag, receive flag and sequence number as a control
    /// message (aka. note) value.
    ///
    /// If the replicate flag is `false`, a value of `-1` is returned.
    ///
    /// If the replicate flag is `true` and the receive flag is `true`, a single
    /// bit arithmetic left shift is performed on the sequence number and the
    /// least-significant bit is set to `0`.
    ///
    /// If the replicate flag is `true` and the receive flag is `false`, a single
    /// bit arithmetic left shift is performed on the sequence number and the
    /// least-significant bit is set to `1`.
    fn encode(
        replicate_flag: bool,
        receive_flag: Option<bool>,
        sequence: Option<u64>,
    ) -> Result<i64> {
        let value = if replicate_flag {
            // Perform a single bit arithmetic left shift.
            let mut signed: i64 = (sequence.unwrap() << 1).try_into()?;
            // Get the least-significant bit (aka. rightmost bit).
            let lsb = signed & 1;
            // Set the least-significant bit based on the value of the receive flag.
            if let Some(_flag @ true) = receive_flag {
                // Set the LSB to 0.
                signed |= 0 << lsb;
            } else {
                // Set the LSB to 1.
                signed |= 1 << lsb;
            }
            signed
        } else {
            -1
        };

        Ok(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const VALUES: [i64; 5] = [-1, 0, 1, 2, 3];
    const NOTES: [(bool, std::option::Option<bool>, std::option::Option<u64>); 5] = [
        (false, None, None),
        (true, Some(true), Some(0)),
        (true, Some(false), Some(0)),
        (true, Some(true), Some(1)),
        (true, Some(false), Some(1)),
    ];

    #[test]
    fn test_decode() {
        VALUES
            .iter()
            .zip(NOTES)
            .for_each(|(value, note)| assert_eq!(Ebt::decode(*value).unwrap(), note));
    }

    #[test]
    fn test_encode() {
        VALUES.iter().zip(NOTES).for_each(|(value, note)| {
            assert_eq!(Ebt::encode(note.0, note.1, note.2).unwrap(), *value)
        });
    }
}
