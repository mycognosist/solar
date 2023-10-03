# 🌞 Solar

A minimal, embeddable Secure Scuttlebutt node capable of lightweight replication and feed storage.

:warning: **Solar is alpha software; expect breaking changes** :construction:

[Features](#features) | [Development](#development) | [Configuration](#configuration) | [JSON-RPC API](#json-rpc) | [Indexes](#indexes) | [License](#license)

## Features

 - **Keypair creation:** Automatically generate a new public-private keypair
 - **Feed generation:** Store published and replicated messages in a key-value database
 - **LAN discovery:** Broadcast and listen for peer connection messages over UDP
 - **Legacy replication:** Replicate with peers using MUXRPC (`createHistoryStream` etc.)
 - **Local feed resync:** Recover lost local feed messages from peers
 - **Interoperability:** Connect and replicate with [Patchwork](https://github.com/ssbc/patchwork)
   and [Go-SSB](https://github.com/ssbc/go-ssb)¹
 - **Selective replication:** Only replicate with specified peers
 - **JSON-RPC interface:** Interact with the node using JSON-RPC over HTTP
 - **Alternative network key:** Operate with a unique network key
 - **Database indexes:** Look up state with efficient queries

_¹ - this is possible because those implementations support legacy replication (using `createHistoryStream`)_

## Development

```
# Clone the repository and move into the solar directory
git clone git@github.com:mycognosist/solar.git && cd solar

# Run the tests
cargo test
```

## Configuration

The public-private keypair is stored in `~/.local/share/solar/secret.toml` (or equivalent path according to the [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/latest/)). 

Likewise, replication configuration is stored in `~/.local/share/solar/replication.toml`. This file consists of a series of key-value pairs and defines the peers with whom the local node will attempt to replicate.

Peers can be manually added to the replication configuration:

`vim ~/.local/share/solar/replication.toml`

```toml
[peers]
# Peer data takes the form of key-value pairs.
# The key is the public key of a peer.
# The value is the connection address of the peer (without the '@' prefix).
# The connection address takes the form: <host>:<port>.
# The value must be an empty string if the URL is unknown.
"o8lWpyLeSqV/BJV9pbxFhKpwm6Lw5k+sqexYK+zT9Tc=.ed25519" = "[200:9730:17c:7f5b:c7c6:c999:7b2a:c958]:8008"
"HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519" = ""
```

Alternatively, peers can be added to the replication configuration via CLI options (note the inclusion of the `shs` query parameter containing the public key of the remote peer):

`solar --connect "tcp://[200:df93:fed8:e5ff:5c43:eab7:6c74:9d94]:8010?shs=MDErHCTxklXc7QZ43fnyzERbRJ7fccRfCYF11EqIFEI=" --replicate connect`

### Environment Variables

Log-level can be defined by setting the `RUST_LOG` environment variable.

## JSON-RPC API

While running, a solar node can be queried using JSON-RPC over HTTP.

| Method | Parameters | Response | Description |
| --- | --- | --- | --- |
| `blocks` | `"<@...=.ed25519>"` | `[<@...=.ed25519>]` | Returns an array of public keys |
| `blockers` | `"<@...=.ed25519>"` | `[<@...=.ed25519>]` | Returns an array of public keys |
| `descriptions` | `"<@...=.ed25519>"` | `[<description>]` | Returns an array of descriptions |
| `self_descriptions` | `"<@...=.ed25519>"` | `[<description>]` | Returns an array of descriptions |
| `latest_description` | `"<@...=.ed25519>"` | `<description>` | Returns a single description |
| `latest_self_description` | `"<@...=.ed25519>"` | `<description>` | Returns a single description |
| `feed` | `"<@...=.ed25519>"` | `[{ "key": "<%...=.sha256>", "value": <value>, "timestamp": <timestamp>, "rts": null }]` | Returns an array of message KVTs (key, value, timestamp) from the local database |
| `follows` | `"<@...=.ed25519>"` | `[<@...=.ed25519>]` | Returns an array of public keys |
| `followers` | `"<@...=.ed25519>"` | `[<@...=.ed25519>]` | Returns an array of public keys |
| `is_following` | `{ "peer_a": "<@...=.ed25519>", "peer_b": "<@...=.ed25519>" }` | `[<@...=.ed25519>]` | Returns a boolean |
| `friends` | `"<@...=.ed25519>"` | `[<@...=.ed25519>]` | Returns an array of public keys |
| `images` | `"<@...=.ed25519>"` | `[<&...=.sha256>]` | Returns an array of image references |
| `self_images` | `"<@...=.ed25519>"` | `[<&...=.sha256>]` | Returns an array of image references |
| `latest_image` | `"<@...=.ed25519>"` | `<&...=.sha256>` | Returns a single image reference |
| `latest_self_image` | `"<@...=.ed25519>"` | `<&...=.sha256>` | Returns a single image reference |
| `message` | `"<%...=.sha256>"` | `{ "key": "<%...=.sha256>", "value": <value>, "timestamp": <timestamp>, "rts": null }` | Returns a single message KVT (key, value, timestamp) from the local database |
| `names` | `"<@...=.ed25519>"` | `[<name>]` | Returns an array of names |
| `self_names` | `"<@...=.ed25519>"` | `[<name>]` | Returns an array of names |
| `latest_name` | `"<@...=.ed25519>"` | `<name>` | Returns a single name |
| `latest_self_name` | `"<@...=.ed25519>"` | `<name>` | Returns a single name |
| `peers` | | `[{ "pub_key": "<@...=.ed25519>", "seq_num": <int> }` | Returns an array of public key and latest sequence number for each peer in the local database |
| `ping` | | `pong!` | Responds if the JSON-RPC server is running |
| `publish` | `<content>` | `{ "msg_ref": "<%...=.sha256>", "seq_num": <int> }` | Publishes a message and returns the reference (message hash) and sequence number |
| `subscribers` | `"<channel>"` | `[<@...=.ed25519>]` | Returns an array of public keys |
| `subscriptions` | `"<@...=.ed25519>"` | `[<channel>]` | Returns an array of channel names |
| `whoami` | | `<@...=.ed25519>` | Returns the public key of the local node |

### Examples

`curl` can be used to invoke the available methods from the commandline.

Request:

`curl -X POST -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "method": "ping", "id":1 }' 127.0.0.1:3030`

Response:

`{"jsonrpc":"2.0","result":"pong!","id":1}`

Request:

`curl -X POST -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "method": "publish", "params": {"type": "about", "about": "@o8lWpyLeSqV/BJV9pbxFhKpwm6Lw5k+sqexYK+zT9Tc=.ed25519", "name": "solar_glyph", "description": "glyph's experimental solar (rust) node"}, "id":1 }' 127.0.0.1:3030`

Response:

`{"jsonrpc":"2.0","result":{"msg_ref":"%ZwYwLxMHgU8eC43HOziJvYURjZzAzwFk3v5RYS/NbQY=.sha256","seq": 3,"id":1}`

_Note: You might find it easier to save your JSON to file and pass that to `curl` instead._

```
curl -X POST -H "Content-Type: application/json" --data @publish.json 127.0.0.1:3030
```

## Indexes

Database indexes are provided to allow efficient queries of the underlying data. The indexes cover most of the message types commonly of relevance to client development. See the tests in the `indexes` module for usage examples (`solar/src/storage/indexes.rs`).

## License

AGPL-3.0
