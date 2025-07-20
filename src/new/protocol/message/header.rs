use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::frame::ReadError;
use crate::new::protocol::message::{ReadVersionedType, WriteVersionedType};
use bytes::{Buf, BufMut};
use std::io::Write;
use thiserror::Error;
const REQUEST_HEADER_LENGTH: i32 = 8;
const SUCCESS_RESPONSE: u8 = 0;
const ERROR_RESPONSE: u8 = 1;
const SERVER_FAILURE: u8 = 2;

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
        writer.put_i16(self.request_api_key.into());
        writer.put_i16(self.request_api_version.0);
        writer.put_i32(self.request_id);
        Ok(())
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
        let resp_type = reader.get_u8();
        // todo: handle when resp_type is not success
        let request_id = reader.get_i32();
        Ok(ResponseHeader { request_id })
    }
}
