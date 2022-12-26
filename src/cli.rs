use std::path::PathBuf;

use structopt::StructOpt;

/// Generate a command line parser.
/// This defines the options that are exposed when running the solar binary.
#[derive(StructOpt, Debug)]
#[structopt(name = "🌞 Solar", about = "Sunbathing scuttlecrabs in kuskaland", version=env!("SOLAR_VERSION"))]
pub struct Cli {
    /// Where data is stored (default: ~/.local/share/local)
    #[structopt(short, long, parse(from_os_str))]
    pub data: Option<PathBuf>,

    /// Connect to peers (e.g. host:port:publickey, host:port:publickey)
    #[structopt(short, long)]
    pub connect: Option<String>,

    // TODO: think about other ways of exposing the "connect" feature
    /// List of peers to replicate; "connect" magic word means that peers
    /// specified with --connect are added to the replication list
    #[structopt(short, long)]
    pub replicate: Option<String>,

    /// Port to bind (default: 8008)
    #[structopt(short, long)]
    pub port: Option<u16>,

    /// Run LAN discovery (default: false)
    #[structopt(short, long)]
    pub lan: Option<bool>,

    /// Run the JSON-RPC server (default: true)
    #[structopt(short, long)]
    pub jsonrpc: Option<bool>,

    /// Resync the local database by requesting the local feed from peers
    #[structopt(long)]
    pub resync: Option<bool>,

    /// Only replicate with peers whose public keys are stored in
    /// `replication.toml` (default: true)
    #[structopt(short, long)]
    pub selective: Option<bool>,
}
