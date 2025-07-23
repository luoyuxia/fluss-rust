use std::sync::Arc;
use thiserror::Error;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RpcError {


    #[error("Cannot write message: {0}")]
    WriteMessageError(#[from] crate::rpc::frame::WriteError),

    #[error("Cannot read framed message: {0}")]
    ReadMessageError(#[from] crate::rpc::frame::ReadError),

    #[error("connection error")]
    ConnectionError(String),

    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Connection is poisoned: {0}")]
    Poisoned(Arc<RpcError>),

    #[error(
        "Data left at the end of the message. Got {message_size} bytes but only read {read} bytes. api_key={api_key:?} api_version={api_version}"
    )]
    TooMuchData {
        message_size: u64,
        read: u64,
        api_key: ApiKey,
        api_version: ApiVersion,
    },
}

