use crate::proto::{GetTableInfoRequest, GetTableInfoResponse, PbTablePath};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};

use crate::metadata::TablePath;
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
