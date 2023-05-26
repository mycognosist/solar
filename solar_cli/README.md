# 🌞 Solar CLI

Commandline interface for solar: a minimal Secure Scuttlebutt node capable of lightweight replication and feed storage.

:warning: **Solar is alpha software; expect breaking changes** :construction:

[Usage](#usage) | [JSON-RPC API](#json-rpc) | [Development](#development) | [License](#license)

## Usage

`solar`

### Options

`solar --help`

```
🌞 Solar: Sunbathing scuttlecrabs in kuskaland

Usage: solar [OPTIONS]

Options:
  -d, --data-dir <DATA_DIR>
          Directory where data is stored (default: ~/.local/share/local)
      --database-cache-capacity <DATABASE_CACHE_CAPACITY>
          Cache capacity of the key-value database in bytes (default: 1000000000)
  -c, --connect <CONNECT>
          Connect to a remote peer by specifying a URL (e.g. tcp://<host>:<port>?shs=<public key>). Pass a comma-separated list of URLs to connect to multiple peers (no spaces)
  -i, --ip <IP>
          IP to bind for TCP server (default: 0.0.0.0)
  -p, --port <PORT>
          Port to bind for TCP server (default: 8008)
  -n, --network-key <NETWORK_KEY>
          Network key to be used during the secret handshake (aka. SHS key or caps key) (default: d4a1cb88a66f02f8db635ce26441cc5dac1b08420ceaac230839b755845a9ffb)
  -l, --lan <LAN>
          Run LAN discovery (default: false) [possible values: true, false]
  -j, --jsonrpc <JSONRPC>
          Run the JSON-RPC server (default: true) [possible values: true, false]
      --jsonrpc-ip <JSONRPC_IP>
          IP to bind for JSON-RPC server (default: 127.0.0.1)
      --jsonrpc-port <JSONRPC_PORT>
          Port to bind for JSON-RPC server (default: 3030)
      --resync <RESYNC>
          Resync the local database by requesting the local feed from peers [possible values: true, false]
  -s, --selective <SELECTIVE>
          Only replicate with peers whose public keys are stored in `replication.toml` (default: true) [possible values: true, false]
  -h, --help
          Print help
  -V, --version
          Print version
```

### Examples

Enable LAN discovery:

`solar --lan true`

Listen for TCP connections on the IPv6 wildcard and non-default port:

`solar --ip :: --port 8010`

Enable log reporting at the `debug` level:

`RUST_LOG=solar=debug solar`

Attempt a connection with a peer:

`solar --connect "tcp://[200:df93:fed8:e5ff:5c43:eab7:6c74:9d94]:8010?shs=MDErHCTxklXc7QZ43fnyzERbRJ7fccRfCYF11EqIFEI="`

### Environment Variables

Log-level can be defined by setting the `RUST_LOG` environment variable.

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

## Development

```
cargo run
cargo test
cargo build --release
RUST_LOG=info ./target/release/solar
```

## License

AGPL-3.0
