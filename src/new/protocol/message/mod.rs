use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::message::header::ReadVersionedError;
use bytes::{Buf, BufMut};

pub mod create_table;
pub mod fetch;
pub mod get_table;
pub mod header;
pub mod update_metadata;
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

#[macro_export]
macro_rules! impl_write_version_type {
    ($type:ty) => {
        impl<W> WriteVersionedType<W> for $type
        where
            W: BufMut,
        {
            fn write_versioned(
                &self,
                writer: &mut W,
                version: ApiVersion,
            ) -> Result<(), WriteVersionedError> {
                Ok(self.inner_request.encode(writer).unwrap())
            }
        }
    };
}

#[macro_export]
macro_rules! impl_read_version_type {
    ($type:ty) => {
        impl<R> ReadVersionedType<R> for $type
        where
            R: Buf,
        {
            fn read_versioned(
                reader: &mut R,
                version: ApiVersion,
            ) -> Result<Self, ReadVersionedError> {
                Ok(<$type>::decode(reader).unwrap())
            }
        }
    };
}
