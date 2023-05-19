#![recursion_limit = "256"]

mod actors;
mod broker;
mod config;
mod error;
mod node;
// TODO: `pub` can be removed once blob-related functions are used.
pub mod storage;

/// Convenience Result that returns `solar::Error`.
pub type Result<T> = std::result::Result<T, error::Error>;

pub use actors::jsonrpc::config::{JSONRPC_IP, JSONRPC_PORT};
pub use config::{ApplicationConfig, MUXRPC_IP, MUXRPC_PORT};
pub use error::Error;
pub use node::Node;
