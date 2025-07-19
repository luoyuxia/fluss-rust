use crate::messages::{GetTableInfoRequest, GetTableInfoResponse, PbTablePath};
use crate::metadata::TablePath;
use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::message::header::ReadVersionedError;
use crate::new::protocol::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::{impl_read_version_type, impl_write_version_type};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetTableRequest {
    pub inner_request: GetTableInfoRequest,
}

impl GetTableRequest {
    pub fn new(table_path: &TablePath) -> Self {
        let inner_request = GetTableInfoRequest {
            table_path: PbTablePath {
                database_name: table_path.database().to_owned(),
                table_name: table_path.table().to_owned(),
            },
        };

        Self { inner_request }
    }
}

impl RequestBody for GetTableRequest {
    type ResponseBody = GetTableInfoResponse;
    const API_KEY: ApiKey = ApiKey::GetTable;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(GetTableRequest);
impl_read_version_type!(GetTableInfoResponse);
