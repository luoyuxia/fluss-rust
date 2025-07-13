use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::message::header::ReadVersionedError;
use bytes::{Buf, BufMut};
use std::io::{Read, Write};

mod create_table;
pub mod header;
pub mod write;

pub trait WriteVersionedType<W>: Sized
where
    W: BufMut,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError>;
}

pub trait ReadVersionedType<R>: Sized
where
    R: Buf,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        todo!()
    }
}

pub trait RequestBody {
    type ResponseBody;

    const API_KEY: ApiKey;

    const REQUEST_VERSION: ApiVersion;
}

impl<T: RequestBody> RequestBody for &T {
    type ResponseBody = T::ResponseBody;

    const API_KEY: ApiKey = T::API_KEY;

    const REQUEST_VERSION: ApiVersion = T::REQUEST_VERSION;
}
