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

Clone the source and build the binary (see [RPi build instructions](https://mycelial.technology/computers/rust-compilation.html) if required):

```
git clone git@github.com:mycognosist/solar.git
cd solar
cargo build --release
```

Add peer(s) to `replication.toml` (public key(s) of feeds you wish to replicate):

`vim ~/.local/share/solar/replication.toml`

```toml
[peers]
# Peer data takes the form of key-value pairs.
# The key is the public key of a peer.
# The value is a URL specifying the connection address of the peer.
# The URL takes the form: <scheme>://<host>:<port>?shs=<public key>.
# The value must be an empty string if the URL is unknown.
"@...=.ed25519" = ""
```

Run solar with LAN discovery enabled:

```
./target/release/solar --lan true
```

_Note: a new public-private keypair will be generated and saved to
`~/.local/share/solar/secret.toml` (or in the equivalent directory on your
operating system)._

## CLI

Solar can be configured and launched using the CLI interface.

`solar --help`

```shell
🌞 Solar 0.3.2-1bb6bed
Sunbathing scuttlecrabs in kuskaland

USAGE:
    solar [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --connect <connect>        Connect to peers (e.g. host:port:publickey, host:port:publickey)
    -d, --data <data>              Where data is stored (default: ~/.local/share/local)
    -i, --ip <ip>                  IP to bind (default: 0.0.0.0)
    -j, --jsonrpc <jsonrpc>        Run the JSON-RPC server (default: true)
    -l, --lan <lan>                Run LAN discovery (default: false)
    -p, --port <port>              Port to bind (default: 8008)
    -r, --replicate <replicate>    List of peers to replicate; "connect" magic word means that peers specified with
                                   --connect are added to the replication list
        --resync <resync>          Resync the local database by requesting the local feed from peers
    -s, --selective <selective>    Only replicate with peers whose public keys are stored in `replication.toml`
                                   (default: true)
```

## Environment Variables

Additional configuration parameters can be supplied via environment variables.

```
RUST_LOG
SLED_CACHE_CAPACITY
SOLAR_JSONRPC_IP
SOLAR_JSONRPC_PORT
SOLAR_NETWORK_KEY
```

For example, run `solar` with a log-level of `debug` and an alternative network key:

`RUST_LOG=solar=debug SOLAR_NETWORK_KEY=3c42fff79381c451fcafd73cec3c9f897bb2232949dcdd35936d64d67c47a374 solar`

## Core Features

- [X] auto-create private key if not exists
- [X] broadcast the identity via lan discovery
- [X] automatic feed generation
- [X] minimal [sled](https://github.com/spacejam/sled) database to store generate feeds
- [X] mux-rpc implementation
  - [X] `blobs createWants`
  - [X] `blobs get`
  - [X] `createHistoryStream`
  - [X] `get`
  - [X] `whoami`
  - [X] [patchwork](https://github.com/ssbc/patchwork) and [go-ssb](https://github.com/ssbc/go-ssb) interoperability
- [X] resync local feed from peers
- [X] legacy replication (using `createHistoryStream`)

## Extensions

_Undergoing active development. Expect APIs to change._

- [X] json-rpc server for user queries
  - [X] feed
  - [X] message
  - [X] peers
  - [X] ping
  - [X] publish
  - [X] whoami
  - [ ] ...
- [ ] improved connection handler
- [ ] ebt replication

## JSON-RPC API

The server currently supports HTTP.

| Method | Parameters | Response | Description |
| --- | --- | --- | --- |
| `feed` | `{ "pub_key": "<@...=.ed25519>" }` | `[{ "key": "<%...=.sha256>", "value": <value>, "timestamp": <timestamp>, "rts": null }]` | Return an array of message KVTs (key, value, timestamp) from the local database |
| `message` | `{ "msg_ref": <key> }` | `{ "key": "<%...=.sha256>", "value": <value>, "timestamp": <timestamp>, "rts": null }` | Return a single message KVT (key, value, timestamp) from the local database |
| `peers` | | `[{ "pub_key": "<@...=.ed25519>", "seq_num": <int> }` | Return the public key and latest sequence number for all peers in the local database |
| `ping` | | `pong!` | Responds if the JSON-RPC server is running |
| `publish` | `<content>` | `{ "msg_ref": "<%...=.sha256>", "seq_num": <int> }` | Publishes a message and returns the reference (message hash) and sequence number |
| `whoami` | | `<@...=.ed25519>` | Returns the public key of the local node |

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
