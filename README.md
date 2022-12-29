# 🌞 Solar

A minimal Secure Scuttlebutt node capable of lightweight replication and feed storage.

:warning: **Solar is alpha software; expect breaking changes** :construction:

[Background](#background) | [Features](#features) | [Installation](#installation) | [Usage](#usage) | [Examples](#examples) | [Command-line options](#options) | [Configuration](#configuration) | [JSON-RPC API](#json-rpc) | [License](#license)

## Background

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

_¹ - this is possible because those implementations support legacy replication (using `createHistoryStream`)_

## Installation

Download the latest [release](https://github.com/mycognosist/solar/releases) and copy the binary to `/usr/bin` or similar directory.

Alternatively, clone the source and build the binary (see [RPi build instructions](https://mycelial.technology/computers/rust-compilation.html) if required):

```
git clone git@github.com:mycognosist/solar.git
cd solar
cargo build --release
```

## Usage

`solar`

### Examples

Enable LAN discovery:

`solar --lan true`

Listen for TCP connections on the IPv6 wildcard and non-default port:

`solar --ip :: --port 8010`

Enable log reporting at the `debug` level:

`RUST_LOG=solar=debug solar`

Attempt a connection with a peer:

`solar --connect "tcp://[200:df93:fed8:e5ff:5c43:eab7:6c74:9d94]:8010?shs=MDErHCTxklXc7QZ43fnyzERbRJ7fccRfCYF11EqIFEI="`

### Options

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

## Configuration

The public-private keypair is stored in `~/.local/share/solar/secret.toml` (or equivalent path according to the [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/latest/)). 

Likewise, replication configuration is stored in `~/.local/share/solar/replication.toml`. This file consists of a series of key-value pairs and defines the peers with whom the local node will attempt to replicate.

Peers can be manually added to the replication configuration:

`vim ~/.local/share/solar/replication.toml`

```toml
[peers]
# Peer data takes the form of key-value pairs.
# The key is the public key of a peer.
# The value is a URL specifying the connection address of the peer.
# The URL takes the form: <scheme>://<host>:<port>?shs=<public key>.
# The value must be an empty string if the URL is unknown.
"@o8lWpyLeSqV/BJV9pbxFhKpwm6Lw5k+sqexYK+zT9Tc=.ed25519" = "tcp://[200:9730:17c:7f5b:c7c6:c999:7b2a:c958]:8008"
"@HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519" = ""
```

Alternatively, peers can be added to the replication configuration via CLI options:

`solar --connect "tcp://[200:df93:fed8:e5ff:5c43:eab7:6c74:9d94]:8010?shs=MDErHCTxklXc7QZ43fnyzERbRJ7fccRfCYF11EqIFEI=" --replicate connect`

### Environment Variables

Additional configuration parameters can be supplied via environment variables.

```
RUST_LOG
SLED_CACHE_CAPACITY
SOLAR_JSONRPC_IP
SOLAR_JSONRPC_PORT
SOLAR_NETWORK_KEY
```

## JSON-RPC API

While running, a solar node can be queried using JSON-RPC over HTTP.

| Method | Parameters | Response | Description |
| --- | --- | --- | --- |
| `feed` | `{ "pub_key": "<@...=.ed25519>" }` | `[{ "key": "<%...=.sha256>", "value": <value>, "timestamp": <timestamp>, "rts": null }]` | Return an array of message KVTs (key, value, timestamp) from the local database |
| `message` | `{ "msg_ref": <key> }` | `{ "key": "<%...=.sha256>", "value": <value>, "timestamp": <timestamp>, "rts": null }` | Return a single message KVT (key, value, timestamp) from the local database |
| `peers` | | `[{ "pub_key": "<@...=.ed25519>", "seq_num": <int> }` | Return the public key and latest sequence number for all peers in the local database |
| `ping` | | `pong!` | Responds if the JSON-RPC server is running |
| `publish` | `<content>` | `{ "msg_ref": "<%...=.sha256>", "seq_num": <int> }` | Publishes a message and returns the reference (message hash) and sequence number |
| `whoami` | | `<@...=.ed25519>` | Returns the public key of the local node |

### Examples

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
