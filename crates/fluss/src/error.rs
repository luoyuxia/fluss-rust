use std::{io, result};
use arrow_schema::ArrowError;
use thiserror::Error;
use crate::rpc::RpcError;

pub type Result<T> = result::Result<T, Error>;


#[derive(Debug, Error)]
pub enum Error {

    #[error(transparent)]
    Io(#[from] io::Error),
    
    #[error("Invalid table")]
    InvalidTableError(String),

    #[error("Json serde error")]
    JsonSerdeError(String),

    #[error("Rpc error")]
    RpcError(#[from] RpcError),

    #[error("Row convert error")]
    RowConvertError(String),

    #[error("arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("Write error: {0}")]
    WriteError(String),

    #[error("Illegal argument error: {0}")]
    IllegalArgument(String),
    
}
