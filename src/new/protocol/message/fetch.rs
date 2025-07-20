use crate::messages::FetchLogResponse;
use crate::new::messenger::WriteVersionedError;
use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::message::header::ReadVersionedError;
use crate::new::protocol::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::{impl_read_version_type, impl_write_version_type, messages};
use bytes::{Buf, BufMut};
use prost::Message;

const LOG_FETCH_MAX_BYTES: i32 = 16 * 1024 * 1024;
const LOG_FETCH_MIN_BYTES: i32 = 1;
const LOG_FETCH_WAIT_MAX_TIME: i32 = 500;

pub struct FetchLogRequest {
    pub inner_request: messages::FetchLogRequest,
}

impl FetchLogRequest {
    pub fn new(fetch_log_request: messages::FetchLogRequest) -> Self {
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
