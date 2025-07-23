use std::sync::Arc;
use crate::{impl_read_version_type, impl_write_version_type, proto};
use crate::proto::{PbProduceLogReqForBucket, ProduceLogResponse};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::error::Result as FlussResult;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};


use bytes::{Buf, BufMut};
use prost::Message;
use crate::client::ReadyWriteBatch;

pub struct ProduceLogRequest {
    pub inner_request: proto::ProduceLogRequest,
}

impl ProduceLogRequest {
    pub fn new(
        table_id: i64,
        ack: i16,
        max_request_timeout_ms: i32,
        ready_batches: Vec<&Arc<ReadyWriteBatch>>,
    ) -> FlussResult<Self> {
        let mut request = proto::ProduceLogRequest::default();
        request.table_id = table_id;
        request.acks = ack as i32;
        request.timeout_ms = max_request_timeout_ms;
        for ready_batch in ready_batches {
            request.buckets_req.push(PbProduceLogReqForBucket {
                partition_id: ready_batch.table_bucket.partition_id(),
                bucket_id: ready_batch.table_bucket.bucket_id(),
                records: ready_batch.write_batch.build()?,
            })
        }

        Ok(ProduceLogRequest {
            inner_request: request,
        })
    }
}

impl RequestBody for ProduceLogRequest {
    type ResponseBody = ProduceLogResponse;

    const API_KEY: ApiKey = ApiKey::ProduceLog;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(ProduceLogRequest);
impl_read_version_type!(ProduceLogResponse);