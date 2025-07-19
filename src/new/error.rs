use crate::new::messenger::RequestError;
use arrow_schema::ArrowError;
use std::io;
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),

    /// An error as reported by a remote FLuss server
    #[error("Fluss Error ({0:?})")]
    Fluss(FlussCode),

    #[error("Flush error: {0}")]
    WriteError(String),

    #[error("Illegal argument error: {0}")]
    IllegalArgument(String),

    #[error("arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("connection error")]
    ConnectionError(String),

    #[error("request error")]
    RequestError(#[from] RequestError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlussCode {
    /// An unexpected server error
    Unknown = -1,

    /// This error is thrown if the client attempts to send messages
    /// to a replica that is not the leader for some bucket. It
    /// indicates that the clients metadata is out of date.
    NotLeaderOrFollower = 12,

    /// This indicates that a record contents does not match its CRC
    CorruptRecord = 14,

    /// This error is thrown if we are in the middle of a leadership
    /// election and there is currently no leader for this bucket
    /// and hence it is unavailable for writes.
    LeaderNotAvaliable = 44,
}
