use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::frame::ReadError;
use crate::new::protocol::message::{ReadVersionedType, WriteVersionedType};
use bytes::{Buf, BufMut};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ReadVersionedError {
    #[error("Read error: {0}")]
    ReadError(#[from] ReadError),
}

#[derive(Debug, PartialEq, Eq)]
pub struct RequestHeader {
    /// The API key of this request.
    pub request_api_key: ApiKey,

    pub request_api_version: ApiVersion,

    pub request_id: i32,

    pub client_id: Option<String>,
}

impl<W> WriteVersionedType<W> for RequestHeader
where
    W: BufMut,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        todo!()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResponseHeader {
    pub request_id: i32,
}

impl<R> ReadVersionedType<R> for ResponseHeader
where
    R: Buf,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        todo!()
    }
}
