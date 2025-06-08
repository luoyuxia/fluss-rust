pub mod error;

pub mod admin;
pub mod args;
pub mod connection;
pub mod metadata;
pub mod record;
mod rpc;
mod table;
mod util;

pub use self::error::{Error, Result};

use prost::Message;

use std::io::Cursor;

// Include the `items` module, which is generated from items.proto.
pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/rpc.messages.rs"));
}

pub fn serialize_shirt(request: &messages::MetadataRequest) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.reserve(request.encoded_len());
    // Unwrap is safe, since we have reserved sufficient capacity in the vector.
    request.encode(&mut buf).unwrap();
    buf
}
