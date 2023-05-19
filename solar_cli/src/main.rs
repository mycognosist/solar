use std::{
    convert::{TryFrom, TryInto},
    env,
    path::PathBuf,
};

use kuska_sodiumoxide::crypto::auth::Key as NetworkKey;
use kuska_ssb::discovery;
use log::info;
use structopt::StructOpt;
use xdg::BaseDirectories;

// TODO: clean up the config story of the solar lib
use solar::{ApplicationConfig, Node, Result, JSONRPC_IP, JSONRPC_PORT, MUXRPC_IP, MUXRPC_PORT};

/// Generate a command line parser.
/// This defines the options that are exposed when running the solar binary.
#[derive(StructOpt, Debug)]
#[structopt(name = "🌞 Solar", about = "Sunbathing scuttlecrabs in kuskaland", version=env!("SOLAR_VERSION"))]
struct Cli {
    /// Where data is stored (default: ~/.local/share/local)
    #[structopt(short, long, parse(from_os_str))]
    pub data: Option<PathBuf>,

    /// Connect to a remote peer by specifying a URL
    /// (e.g. tcp://<host>:<port>?shs=<public key>).
    /// Pass a comma-separated list of URLs to connect to multiple peers
    /// (no spaces)
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

    /// IP to bind (default: 0.0.0.0)
    #[structopt(short, long)]
    pub ip: Option<String>,

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

impl TryFrom<Cli> for ApplicationConfig {
    type Error = solar::Error;

    /// Parse the configuration options provided via the CLI and environment
    /// variables, fall back to defaults when necessary and return the
    /// application configuration.
    fn try_from(cli_args: Cli) -> Result<Self> {
        // Retrieve application configuration parameters from the parsed CLI input.
        // Set defaults if options have not been provided.
        let lan_discov = cli_args.lan.unwrap_or(false);
        let muxrpc_ip = cli_args.ip.unwrap_or_else(|| MUXRPC_IP.to_string());
        let muxrpc_port = cli_args.port.unwrap_or(MUXRPC_PORT);
        let muxrpc_addr = format!("{muxrpc_ip}:{muxrpc_port}");
        let jsonrpc = cli_args.jsonrpc.unwrap_or(true);
        let resync = cli_args.resync.unwrap_or(false);
        let selective_replication = cli_args.selective.unwrap_or(true);

        // Set the JSON-RPC server IP address.
        // First check for an env var before falling back to the default.
        let jsonrpc_ip = match env::var("SOLAR_JSONRPC_IP") {
            Ok(ip) => ip,
            Err(_) => JSONRPC_IP.to_string(),
        };
        // Set the JSON-RPC server port number.
        // First check for an env var before falling back to the default.
        let jsonrpc_port = match env::var("SOLAR_JSONRPC_PORT") {
            Ok(port) => port,
            Err(_) => JSONRPC_PORT.to_string(),
        };
        let jsonrpc_addr = format!("{jsonrpc_ip}:{jsonrpc_port}");

        // Read KV database cache capacity setting from environment variable.
        // Define default value (1 GB) if env var is unset.
        let kv_cache_capacity: u64 = match env::var("SOLAR_KV_CACHE_CAPACITY") {
            Ok(val) => val.parse().unwrap_or(1000 * 1000 * 1000),
            Err(_) => 1000 * 1000 * 1000,
        };

        // Define the default HMAC-SHA-512-256 key for secret handshakes.
        // This is also sometimes known as the SHS key, caps key or network key.
        let network_key = match env::var("SOLAR_NETWORK_KEY") {
            Ok(key) => NetworkKey::from_slice(&hex::decode(key)
                .expect("shs key supplied via SOLAR_NETWORK_KEY env var is not valid hex"))
                .expect("failed to instantiate an authentication key from the supplied shs key; check byte length"),
            Err(_) => discovery::ssb_net_id(),
        };

        // Create the root data directory for solar.
        // This is the path at which application data is stored, including the
        // public-private keypair, key-value database and blob store.
        let base_path = cli_args
            .data
            .unwrap_or(BaseDirectories::new()?.create_data_directory("solar")?);

        info!("Base directory is {:?}", base_path);

        let app_config = ApplicationConfig {
            base_path,
            blobs_folder: PathBuf::new(),
            connect: cli_args.connect,
            feeds_folder: PathBuf::new(),
            jsonrpc,
            jsonrpc_addr,
            kv_cache_capacity,
            lan_discov,
            muxrpc_ip,
            muxrpc_port,
            muxrpc_addr,
            network_key,
            replicate: cli_args.replicate,
            resync,
            selective_replication,
        };

        Ok(app_config)
    }
}

#[async_std::main]
async fn main() {
    // Initialise the logger.
    env_logger::init();
    log::set_max_level(log::LevelFilter::max());

    // Parse command line arguments.
    let cli = Cli::from_args();

    // Load configuration parameters and apply defaults.
    let config = cli.try_into().expect("could not load configuration");

    // Start the solar node in async runtime.
    let _node = Node::start(config).await;
}
