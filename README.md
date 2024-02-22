# 🌞 Solar

A minimal, embeddable Secure Scuttlebutt node capable of lightweight replication
and feed storage.

The node can be run as a [commandline application](https://github.com/mycognosist/solar/blob/main/solar_cli)
or embedded into another Rust application as a [library](https://github.com/mycognosist/solar/blob/main/solar).

:warning: **Solar is alpha software; expect breaking changes** :construction:

[Background](#background) | [Features](#features) | [Installation](#installation) | [Usage](#usage) | [Examples](#examples) | [CLI Options](#options) | [Configuration](#configuration) | [License](#license)

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
message publishing and replication. Indexes are provided to facilitate client
creation.

## Features

 - **Keypair creation:** Automatically generate a new public-private keypair
 - **Feed generation:** Store published and replicated messages in a key-value database
 - **LAN discovery:** Broadcast and listen for peer connection messages over UDP
 - **EBT replication:** Replicate with peers using epidemic broadcast trees
 - **Legacy replication:** Replicate with peers using MUXRPC (`createHistoryStream` etc.)
 - **Local feed resync:** Recover lost local feed messages from peers
 - **Interoperability:** Connect and replicate with [Manyverse](https://www.manyver.se/),
   [Patchwork](https://github.com/ssbc/patchwork) and [Go-SSB](https://github.com/ssbc/go-ssb)
 - **Selective replication:** Only replicate with specified peers
 - **JSON-RPC interface:** Interact with the node using JSON-RPC over HTTP
 - **Alternative network key:** Operate with a unique network key
 - **Database indexes:** Look up state with efficient queries

## Installation

Download the latest [release](https://github.com/mycognosist/solar/releases) and copy the binary to `/usr/bin` or similar directory.

Alternatively, clone the source and build the binary (see [RPi build instructions](https://mycelial.technology/computers/rust-compilation.html) if required):

```
git clone git@github.com:mycognosist/solar.git
cd solar
cargo build --release
```

## Usage

Embed solar into a Rust application:

```rust
use solar::{ApplicationConfig, Node};

let config = ApplicationConfig::default();
let node = Node::start(config).await;
```

Or run it as a commandline application:

```
# Run the debug build
cargo run

# Build the release build
cargo build --release

# Run the release build with logging
RUST_LOG=info ./target/release/solar
```

See the [commandline application README](https://github.com/mycognosist/solar/blob/main/solar_cli/README.md)
for a full list of usage options.

### Examples

Enable LAN discovery:

`solar --lan true`

Listen for TCP connections on the IPv6 wildcard and non-default port:

`solar --ip :: --port 8010`

Enable log reporting at the `debug` level:

`RUST_LOG=solar=debug solar`

Attempt a connection with a peer:

`solar --connect "tcp://[200:df93:fed8:e5ff:5c43:eab7:6c74:9d94]:8010?shs=MDErHCTxklXc7QZ43fnyzERbRJ7fccRfCYF11EqIFEI="`

## Configuration

The public-private keypair is stored in `~/.local/share/solar/secret.toml` (or equivalent path according to the [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/latest/)). 

Likewise, replication configuration is stored in `~/.local/share/solar/replication.toml`. This file consists of a series of key-value pairs and defines the peers with whom the local node will attempt to replicate.

Peers can be manually added to the replication configuration:

`vim ~/.local/share/solar/replication.toml`

```toml
[peers]
# Peer data takes the form of key-value pairs.
# The key is the public key of a peer (without the '@' prefix).
# The value is the connection address of the peer.
# The connection address takes the form: <host>:<port>.
# The value must be an empty string if the URL is unknown.
"o8lWpyLeSqV/BJV9pbxFhKpwm6Lw5k+sqexYK+zT9Tc=.ed25519" = "[200:9730:17c:7f5b:c7c6:c999:7b2a:c958]:8008"
"HEqy940T6uB+T+d9Jaa58aNfRzLx9eRWqkZljBmnkmk=.ed25519" = ""
```

Alternatively, peers can be added to the replication configuration via CLI options:

`solar --connect "tcp://[200:df93:fed8:e5ff:5c43:eab7:6c74:9d94]:8010?shs=MDErHCTxklXc7QZ43fnyzERbRJ7fccRfCYF11EqIFEI=" --replicate connect`

### Environment Variables

Log-level can be defined by setting the `RUST_LOG` environment variable.

## License

AGPL-3.0
