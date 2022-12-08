# 🌞 Solar

Solar was written by [@adria0](https://github.com/adria0) with the idea to 
enable community hardware devices to speak [Secure Scuttlebutt](https://scuttlebutt.nz/)
using the [Kuska](https://github.com/Kuska-ssb) Rust libraries, mainly based on 
[async_std](https://async.rs/).

This fork aims to evolve solar into a minimal Scuttlebutt node capable of 
lightweight replication and feed storage. Much like 
[scuttlego](https://github.com/planetary-social/scuttlego), this fork is not
intended to reproduce the full suite of MUXRPC methods used in the JS SSB
ecosystem. It will only implement the core MUXRPC methods required for 
message publishing and replication. Indexing of database messages will be
offloaded to client applications (ie. piping feeds from solar into a SQLite
database).

## Quick Start

Clone the source and build the binary:

```
git clone git@github.com:mycognosist/solar.git
cd solar
cargo build --release
```

Add peer(s) to `replication.toml` (public key(s) of feeds you wish to replicate):

```
vim ~/.local/share/solar/replication.toml

peers = ["@...=.ed25519"]
```

Run solar with LAN discovery enabled:

```
./target/release/solar --lan true
```

_Note: a new public-private keypair will be generated and saved to
`~/.local/share/solar/secret.toml` (or in the equivalent directory on your
operating system)._

## Core Features

- [X] auto-create private key if not exists
- [X] broadcast the identity via lan discovery
- [X] automatic feed generation
- [X] minimal [sled](https://github.com/spacejam/sled) database to store generate feeds
- [X] mux-rpc implementation
  - [X] `whoami`
  - [X] `get`
  - [X] `createHistoryStream`
  - [X] `blobs createWants`
  - [X] `blobs get`
  - [X] [patchwork](https://github.com/ssbc/patchwork) and [go-ssb](https://github.com/ssbc/go-ssb) interoperability
- [X] legacy replication (using `createHistoryStream`)

## Extensions

_Undergoing active development. Expect APIs to change._

- [X] json-rpc server for user queries
  - [X] ping
  - [X] whoami
  - [X] publish
  - [X] get_peers
  - [ ] ...
- [ ] improved connection handler
- [ ] ebt replication

## Documentation

_Undergoing active development._

- [ ] comprehensive doc comments
- [ ] comprehensive code comments

## JSON-RPC API

The server currently supports HTTP.

| Method | Parameters | Response | Description |
| --- | --- | --- | --- |
| `ping` | | `pong!` | Responds if the JSON-RPC server is running |
| `whoami` | | `<@...=.ed25519>` | Returns the public key of the local node |
| `publish` | `<content>` | `{ "msg_ref": "<%...=.sha256>", "seq": <int> }` | Publishes a message and returns the reference (message hash) and sequence number |
| `get_peers` | | `[{ "id": "<@...=.ed25519>", "sequence": <int> }` | Return the public key and latest sequence number for all peers in the local database |

`curl` can be used to invoke the available methods from the commandline:

```
curl -X POST -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "method": "ping", "id":1 }' 127.0.0.1:3030

{"jsonrpc":"2.0","result":"pong!","id":1}
```

```
curl -X POST -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "method": "publish", "params": {"type": "about", "about": "@o8lWpyLeSqV/BJV9pbxFhKpwm6Lw5k+sqexYK+zT9Tc=.ed25519", "name": "solar_glyph", "description": "glyph's experimental solar (rust) node"}, "id":1 }' 127.0.0.1:3030

{"jsonrpc":"2.0","result":{"msg_ref":"%ZwYwLxMHgU8eC43HOziJvYURjZzAzwFk3v5RYS/NbQY=.sha256","seq": 3,"id":1}
```

_Note: You might find it easier to save your JSON to file and pass that to `curl` instead._

```
curl -X POST -H "Content-Type: application/json" --data @publish.json 127.0.0.1:3030
```

## License

AGPL-3.0
