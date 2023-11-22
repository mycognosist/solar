use std::{collections::HashMap, convert::TryInto};

use kuska_ssb::api::dto::content::SsbId;

use crate::Result;

/// The encoded vector clock value.
pub type EncodedClockValue = i64;

/// A vector clock which maps an SSB ID to an encoded vector clock value.
pub type VectorClock = HashMap<SsbId, EncodedClockValue>;

/// Decode a value from a control message (aka. note), returning the values
/// of the replicate flag, receive flag and sequence.
///
/// If the replicate flag is `false`, the peer does not wish to replicate
/// messages for the referenced feed.
///
/// If the replicate flag is `true`, values will be returned for the receive
/// flag and sequence.
///
/// The sequence refers to a sequence number of the referenced feed.
pub fn decode(value: EncodedClockValue) -> Result<(bool, Option<bool>, Option<u64>)> {
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
pub fn encode(
    replicate_flag: bool,
    receive_flag: Option<bool>,
    sequence: Option<u64>,
) -> Result<EncodedClockValue> {
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
            .for_each(|(value, note)| assert_eq!(decode(*value).unwrap(), note));
    }

    #[test]
    fn test_encode() {
        VALUES
            .iter()
            .zip(NOTES)
            .for_each(|(value, note)| assert_eq!(encode(note.0, note.1, note.2).unwrap(), *value));
    }
}
