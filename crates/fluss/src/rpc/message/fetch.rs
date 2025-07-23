use crate::{impl_read_version_type, impl_write_version_type, proto};
use crate::proto::FetchLogResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};
use prost::Message;

use bytes::{Buf, BufMut};

const LOG_FETCH_MAX_BYTES: i32 = 16 * 1024 * 1024;
const LOG_FETCH_MIN_BYTES: i32 = 1;
const LOG_FETCH_WAIT_MAX_TIME: i32 = 500;

pub struct FetchLogRequest {
    pub inner_request: proto::FetchLogRequest,
}

impl FetchLogRequest {
    pub fn new(fetch_log_request: proto::FetchLogRequest) -> Self {
        Self {
            inner_request: fetch_log_request,
        }
    }
}

impl RequestBody for FetchLogRequest {
    type ResponseBody = FetchLogResponse;

    const API_KEY: ApiKey = ApiKey::FetchLog;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(FetchLogRequest);
impl_read_version_type!(FetchLogResponse);