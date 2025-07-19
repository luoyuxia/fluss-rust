use crate::messages::{MetadataResponse, PbTablePath};
use crate::metadata::TablePath;
use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::message::header::ReadVersionedError;
use crate::new::protocol::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::{impl_read_version_type, impl_write_version_type, messages};
use bytes::{Buf, BufMut};
use prost::Message;

pub struct UpdateMetadataRequest {
    pub inner_request: messages::MetadataRequest,
}

impl UpdateMetadataRequest {
    pub fn new(table_paths: &[&TablePath]) -> Self {
        UpdateMetadataRequest {
            inner_request: messages::MetadataRequest {
                table_path: table_paths
                    .iter()
                    .map(|path| PbTablePath {
                        database_name: path.database().to_string(),
                        table_name: path.table().to_string(),
                    })
                    .collect(),
                partitions_path: vec![],
                partitions_id: vec![],
            },
        }
    }
}

impl RequestBody for UpdateMetadataRequest {
    type ResponseBody = MetadataResponse;

    const API_KEY: ApiKey = ApiKey::MetaData;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(UpdateMetadataRequest);
impl_read_version_type!(MetadataResponse);
