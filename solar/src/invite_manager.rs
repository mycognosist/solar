// src/invite_manager.rs

use std::collections::HashMap;
use rand::{distributions::Alphanumeric, Rng};

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

#[derive(Default)]
pub struct InviteManager {
    // A map of an invite code to the number of remaining uses for that code.
    active_invites: HashMap<String, u16>,
}

impl InviteManager {
    pub fn new() -> Self {
        Self {
            active_invites: HashMap::new(),
        }
    }

    pub fn create_invite(&mut self, uses: u16) -> Result<String, Error> {
        // Generate the invite.
        let random_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        // Insert the invite and number of uses into the HashMap.
        self.active_invites.insert(random_string.clone(), uses);

        // Return the invite.
        Ok(random_string)
    }

    pub fn use_invite(&mut self, invite: &str) -> Result<(), Error> {
        // Update the `active_invites` HashMap.
        let found_invite = self.active_invites.get(invite);
        match found_invite {
            Some(num_uses) => {
                // decrement the number of uses
                if (num_uses < &1u16) {
                    Err(Error::InviteUse)
                } else if num_uses == &1u16 {
                    // remove the invite code
                    self.active_invites.remove(invite);
                    Ok(())
                } else {
                    let new_uses = num_uses - 1;
                    self.active_invites.insert(invite.to_string(), new_uses);
                    Ok(())
                }
            }
            None => {
                Err(Error::InviteUse)
            }
        }
    }
}
