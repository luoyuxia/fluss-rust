use crate::messages::CreateTableResponse;
use crate::metadata::metadata_serde::JsonSerde;
use crate::metadata::{TableDescriptor, TablePath};
use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::message::header::ReadVersionedError;
use crate::new::protocol::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::rpc::to_table_path;
use crate::{impl_read_version_type, impl_write_version_type, messages};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct CreateTableRequest {
    pub inner_request: messages::CreateTableRequest,
}

impl CreateTableRequest {
    pub fn new(
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: bool,
    ) -> Self {
        CreateTableRequest {
            inner_request: messages::CreateTableRequest {
                table_path: to_table_path(table_path),
                table_json: serde_json::to_vec(&table_descriptor.serialize_json()).unwrap(),
                ignore_if_exists,
            },
        }
    }
}

impl RequestBody for CreateTableRequest {
    type ResponseBody = CreateTableResponse;

    const API_KEY: ApiKey = ApiKey::CreateTable;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(CreateTableRequest);
impl_read_version_type!(CreateTableResponse);
