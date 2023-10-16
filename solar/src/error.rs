use std::{fmt, io, net};

use futures::channel::mpsc;
use jsonrpsee::types::error::ErrorObjectOwned as JsonRpcErrorOwned;
use jsonrpsee::types::error::SERVER_ERROR_MSG;
use kuska_ssb::{api, crypto, discovery, feed, handshake, rpc};
use toml::{de, ser};

/// Possible solar errors.
#[derive(Debug)]
pub enum Error {
    /// IP address parsing error.
    AddrParse(net::AddrParseError),
    /// xdg::BaseDirectoriesError.
    BaseDirectories(xdg::BaseDirectoriesError),
    /// Configuration error.
    Config(String),
    /// SSB cryptograpy error.
    Crypto(crypto::Error),
    /// Sled database error.
    Database(sled::Error),
    /// Failed to deserialization TOML.
    DeserializeToml(de::Error),
    /// Failed to send message on futures channel.
    FuturesChannel(mpsc::SendError),
    /// Database indexes.
    Indexes,
    /// Validation error; invalid message sequence number.
    InvalidSequence,
    /// io::Error.
    Io(io::Error),
    /// JSON RPC error.
    JsonRpc(jsonrpsee::core::Error),
    /// LAN UDP discovery error.
    LanDiscovery(discovery::Error),
    /// SSB message type field error.
    MessageType(String),
    /// SSB RPC error.
    MuxRpc(rpc::Error),
    /// None error (expected an `Option` to be `Some`).
    OptionIsNone,
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
    /// URL parsing error.
    UrlParse(url::ParseError),
    /// SSB message validation error.
    Validation(feed::Error),
    /// Unknown error.
    Other(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::AddrParse(err) => write!(f, "Failed to parse IP address: {err}"),
            Error::BaseDirectories(err) => write!(f, "Base directory error: {err}"),
            Error::Config(err) => write!(f, "Configuration error: {err}"),
            Error::Crypto(err) => write!(f, "SSB cryptographic error: {err}"),
            Error::Database(err) => write!(f, "Key-value database error: {err}"),
            Error::DeserializeToml(err) => write!(f, "Failed to deserialize TOML: {err}"),
            Error::FuturesChannel(err) => {
                write!(f, "Failed to send message on futures channel: {err}")
            }
            Error::Indexes => write!(f, "Indexes error: indexes not initialised"),
            // TODO: Attach context so we know the identity of the offending message.
            Error::InvalidSequence => write!(
                f,
                "Validation error: message contains incorrect sequence number"
            ),
            Error::Io(err) => write!(f, "I/O error: {err}"),
            Error::JsonRpc(err) => write!(f, "JSON-RPC error: {err}"),
            Error::LanDiscovery(err) => write!(f, "LAN UDP discovery error: {err}"),
            Error::MessageType(err) => write!(f, "SSB message type field error: {err}"),
            Error::MuxRpc(err) => write!(f, "MUXRPC error: {err}"),
            Error::OptionIsNone => write!(f, "None error: expected Some"),
            Error::SecretHandshake(err) => write!(f, "Secret handshake error: {err}"),
            Error::SerdeCbor(err) => write!(f, "Serde CBOR error: {err}"),
            Error::SerdeJson(err) => write!(f, "Serde JSON error: {err}"),
            Error::SerializeToml(err) => write!(f, "Failed to serialize TOML: {err}"),
            Error::SsbApi(err) => write!(f, "SSB API error: {err}"),
            Error::UrlParse(err) => write!(f, "Failed to parse URL: {err}"),
            Error::Validation(err) => write!(f, "Message validation error: {err}"),
            Error::Other(err) => write!(f, "Uncategorized error: {err}"),
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

impl From<mpsc::SendError> for Error {
    fn from(err: mpsc::SendError) -> Error {
        Error::FuturesChannel(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<jsonrpsee::core::Error> for Error {
    fn from(err: jsonrpsee::core::Error) -> Error {
        Error::JsonRpc(err)
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

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Error {
        Error::UrlParse(err)
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
impl From<Error> for JsonRpcErrorOwned {
    fn from(err: Error) -> Self {
        match &err {
            Error::SerdeJson(err_msg) => {
                JsonRpcErrorOwned::owned(-32000, SERVER_ERROR_MSG, Some(err_msg.to_string()))
            }
            Error::UrlParse(err_msg) => {
                JsonRpcErrorOwned::owned(-32001, SERVER_ERROR_MSG, Some(err_msg.to_string()))
            }
            Error::Validation(err_msg) => {
                JsonRpcErrorOwned::owned(-32002, SERVER_ERROR_MSG, Some(err_msg.to_string()))
            }
            Error::Indexes => JsonRpcErrorOwned::owned(-32003, SERVER_ERROR_MSG, None::<String>),
            _ => todo!(),
        }
    }
}
