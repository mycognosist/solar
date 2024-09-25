// src/invite_manager.rs

use std::collections::HashMap;

use crate::error::Error;

// An invite code.
//
// https://ssbc.github.io/scuttlebutt-protocol-guide/#invites
//
// Format like so:
//
// `one.butt.nz:8008:@...=.ed25519~<base64 invite secret key seed>=`
//
// We may choose to parse the invite secret key in the MUXRPC handler and pass just that into the
// invite manager. In which case `Invite` will just be the base64-encoded secret (without the
// hostname / IP etc.).
type Invite = String;

struct InviteManager {
    // A map of an invite code to the number of remaining uses for that code.
    active_invites: HashMap<Invite, u16>,
}

impl InviteManager {
    pub fn new() -> Self {
        Self {
            active_invites: HashMap::new(),
        }
    }

    pub fn create_invite(uses: u16) -> Result<Invite, Error> {
        // Generate the invite.
        // Insert the invite and number of uses into the HashMap.
        // Return the invite.
        todo!()
    }

    pub fn use_invite(invite: Invite) -> Result<(), Error> {
        // Update the `active_invites` HashMap.
        // Either decrement the number of uses for the invite or remove it entirely.
        // NOTE: Following the associated peer is handled elsewhere.
        todo!()
    }
}
