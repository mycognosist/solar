#![recursion_limit = "256"]

mod actors;
mod broker;
mod config;
mod error;
mod node;
// TODO: `pub` can be removed once blob-related functions are used.
mod secret_config;
pub mod storage;

/// Convenience Result that returns `solar::Error`.
pub type Result<T> = std::result::Result<T, error::Error>;

pub use actors::jsonrpc::config::JsonRpcConfig;
pub use actors::network::config::NetworkConfig;
pub use actors::replication::config::ReplicationConfig;
pub use config::ApplicationConfig;
pub use error::Error;
pub use node::Node;
