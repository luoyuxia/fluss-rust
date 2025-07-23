use bytes::{Buf, BufMut};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};

mod header;
mod create_table;
mod fetch;
mod get_table;
mod update_metadata;
mod produce_log;

pub use create_table::*;
pub use fetch::*;
pub use get_table::*;
pub use update_metadata::*;
pub use produce_log::*;
pub use header::*;


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

pub trait WriteVersionedType<W>: Sized
where
    W: BufMut,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteError>;
}

pub trait ReadVersionedType<R>: Sized
where
    R: Buf,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadError>;
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
            ) -> Result<(), WriteError> {
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
            ) -> Result<Self, ReadError> {
                Ok(<$type>::decode(reader).unwrap())
            }
        }
    };
}