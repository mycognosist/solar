use std::{fmt, io, net};

use jsonrpc_http_server::jsonrpc_core;
use kuska_ssb::{api, crypto, discovery, feed, handshake, rpc};
use toml::{de, ser};

/// Possible solar errors.
#[derive(Debug)]
pub enum Error {
    /// IP address parsing error.
    AddrParse(net::AddrParseError),
    /// xdg::BaseDirectoriesError.
    BaseDirectories(xdg::BaseDirectoriesError),
    /// SSB cryptograpy error.
    Crypto(crypto::Error),
    /// Sled database error.
    Database(sled::Error),
    /// Failed to deserialization TOML.
    DeserializeToml(de::Error),
    /// Validation error; invalid message sequence number.
    InvalidSequence,
    /// io::Error.
    Io(io::Error),
    /// LAN UDP discovery error.
    LanDiscovery(discovery::Error),
    /// SSB RPC error.
    MuxRpc(rpc::Error),
    /// Secret handshake error.
    SecretHandshake(handshake::async_std::Error),
    /// Serde CBOR error.
    SerdeCbor(serde_cbor::Error),
    /// Serde JSON error.
    SerdeJson(serde_json::Error),
    /// Failed to serialization TOML.
    SerializeToml(ser::Error),
    /// SSB API error.
    SsbApi(api::Error),
    /// SSB message validation error.
    Validation(feed::Error),
    /// Unknown error.
    Other(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::AddrParse(err) => write!(f, "failed to parse ip address: {}", err),
            Error::BaseDirectories(err) => write!(f, "base directory error: {}", err),
            Error::Crypto(err) => write!(f, "ssb cryptographic error: {}", err),
            Error::Database(err) => write!(f, "key-value database error: {}", err),
            Error::DeserializeToml(err) => write!(f, "failed to deserialize toml: {}", err),
            // TODO: Attach context so we know the identity of the offending message.
            Error::InvalidSequence => write!(
                f,
                "validation error. message contains incorrect sequence number"
            ),
            Error::Io(err) => write!(f, "i/o error: {}", err),
            Error::LanDiscovery(err) => write!(f, "lan udp discovery error: {}", err),
            Error::MuxRpc(err) => write!(f, "muxrpc error: {}", err),
            Error::SecretHandshake(err) => write!(f, "secret handshake error: {}", err),
            Error::SerdeCbor(err) => write!(f, "serde cbor error: {}", err),
            Error::SerdeJson(err) => write!(f, "serde json error: {}", err),
            Error::SerializeToml(err) => write!(f, "failed to serialize toml: {}", err),
            Error::SsbApi(err) => write!(f, "ssb api error: {}", err),
            Error::Validation(err) => write!(f, "message validation error: {}", err),
            Error::Other(err) => write!(f, "uncategorized error: {}", err),
        }
    }
}

impl From<net::AddrParseError> for Error {
    fn from(err: net::AddrParseError) -> Error {
        Error::AddrParse(err)
    }
}

impl From<xdg::BaseDirectoriesError> for Error {
    fn from(err: xdg::BaseDirectoriesError) -> Error {
        Error::BaseDirectories(err)
    }
}

impl From<crypto::Error> for Error {
    fn from(err: crypto::Error) -> Error {
        Error::Crypto(err)
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Error {
        Error::Database(err)
    }
}

impl From<de::Error> for Error {
    fn from(err: de::Error) -> Error {
        Error::DeserializeToml(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<discovery::Error> for Error {
    fn from(err: discovery::Error) -> Error {
        Error::LanDiscovery(err)
    }
}

impl From<rpc::Error> for Error {
    fn from(err: rpc::Error) -> Error {
        Error::MuxRpc(err)
    }
}

impl From<handshake::async_std::Error> for Error {
    fn from(err: handshake::async_std::Error) -> Error {
        Error::SecretHandshake(err)
    }
}

impl From<serde_cbor::Error> for Error {
    fn from(err: serde_cbor::Error) -> Error {
        Error::SerdeCbor(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        Error::SerdeJson(err)
    }
}

impl From<ser::Error> for Error {
    fn from(err: ser::Error) -> Error {
        Error::SerializeToml(err)
    }
}

impl From<api::Error> for Error {
    fn from(err: api::Error) -> Error {
        Error::SsbApi(err)
    }
}

impl From<feed::Error> for Error {
    fn from(err: feed::Error) -> Error {
        Error::Validation(err)
    }
}

// Conversions for errors which occur in the context of a JSON-RPC method call.
// Crate-local error variants are converted to JSON-RPC errors which are
// then return to the caller.
impl From<Error> for jsonrpc_core::Error {
    fn from(err: Error) -> Self {
        match &err {
            Error::Validation(err_msg) => jsonrpc_core::Error {
                code: jsonrpc_core::ErrorCode::ServerError(-32001),
                message: err_msg.to_string(),
                data: None,
            },
            Error::SerdeJson(err_msg) => jsonrpc_core::Error {
                code: jsonrpc_core::ErrorCode::ServerError(-32002),
                message: err_msg.to_string(),
                data: None,
            },
            _ => todo!(),
        }
    }
}
