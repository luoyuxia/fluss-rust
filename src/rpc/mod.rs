use std::{i8, io::Write, mem};

use arrow::array::RecordBatch;
use byteorder::{BigEndian, WriteBytesExt};
use prost::Message;

use crate::{
    connection::{ServerNode, ServerType},
    error::{Error, FlussCode, Result},
    messages::{
        self, CreateTableRequest, FetchLogRequest, GetTableInfoResponse, PbProduceLogReqForBucket,
        PbServerNode, PbTablePath, ProduceLogRequest,
    },
    metadata::TablePath,
    metadata::{TableDescriptor, TableInfo, metadata_serde::JsonSerde},
    record::log_records::MemoryLogRecordsArrowBuilder,
};

// -----------------
const REQUEST_HEADER_LENGTH: i32 = 8;
pub const API_KEY_METADATA: i16 = 1012;
pub const API_VERSION: i16 = 0;
pub const API_KEY_PRODUCE_LOG: i16 = 1014;
pub const API_KEY_CREATE_TABLE: i16 = 1005;
pub const API_KEY_GET_TABLE: i16 = 1007;
pub const API_KEY_FETCH_LOG: i16 = 1015;

pub trait ResponseParser {
    type T;

    fn parse(&self, response: Vec<u8>) -> Result<Self::T>;
}

impl FlussCode {
    fn from_protocol(n: i16) -> Option<FlussCode> {
        if n == 0 {
            return None;
        }

        if n >= FlussCode::NotLeaderOrFollower as i16 && n <= FlussCode::LeaderNotAvaliable as i16 {
            return Some(unsafe { mem::transmute(n as i8) });
        }
        Some(FlussCode::Unknown)
    }
}

impl Error {
    fn from_protocol(n: i16) -> Option<Error> {
        FlussCode::from_protocol(n).map(Error::Fluss)
    }
}

#[derive(Debug)]
pub struct HeaderRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub request_id: i32,
}

impl HeaderRequest {
    fn new(api_key: i16, api_version: i16, request_id: i32) -> HeaderRequest {
        HeaderRequest {
            api_key,
            api_version,
            request_id,
        }
    }
}

#[derive(Debug)]
pub struct FlussRequest<T: Message> {
    pub header: HeaderRequest,
    pub request: T,
}

pub fn encode_request<M, W>(
    request: &M,
    api_key: i16,
    api_version: i16,
    request_id: i32,
    buf: &mut W,
) -> Result<()>
where
    M: Message,
    W: Write,
{
    let request_size = request.encoded_len();
    let frame_length = REQUEST_HEADER_LENGTH + (request_size as i32);

    // header
    buf.write_i32::<BigEndian>(frame_length)?;
    buf.write_i16::<BigEndian>(api_key)?;
    buf.write_i16::<BigEndian>(api_version)?;
    buf.write_i32::<BigEndian>(request_id)?;

    // payload
    let mut request_buf = Vec::with_capacity(request.encoded_len());
    request.encode(&mut request_buf).unwrap();
    buf.write_all(&request_buf).map_err(From::from)
}

pub fn build_metadata_request(
    table_paths: &[TablePath],
) -> FlussRequest<messages::MetadataRequest> {
    let metadata_request = messages::MetadataRequest {
        table_path: table_paths
            .iter()
            .map(|path| PbTablePath {
                database_name: path.database().to_string(),
                table_name: path.table().to_string(),
            })
            .collect(),
        partitions_path: vec![],
        partitions_id: vec![],
    };
    FlussRequest {
        header: HeaderRequest {
            api_key: API_KEY_METADATA,
            api_version: API_VERSION,
            request_id: -1,
        },
        request: metadata_request,
    }
}

pub fn build_create_table_request(
    table_path: &TablePath,
    table_descriptor: &TableDescriptor,
    ignore_if_exists: bool,
) -> FlussRequest<CreateTableRequest> {
    let create_table_request = messages::CreateTableRequest {
        table_path: to_table_path(table_path),
        table_json: serde_json::to_vec(&table_descriptor.serialize_json()).unwrap(),
        ignore_if_exists,
    };
    FlussRequest {
        header: HeaderRequest {
            api_key: API_KEY_CREATE_TABLE,
            api_version: API_VERSION,
            request_id: -1,
        },
        request: create_table_request,
    }
}

pub fn build_fetch_log_request(
    fetch_log_request: FetchLogRequest,
) -> FlussRequest<FetchLogRequest> {
    FlussRequest {
        header: HeaderRequest {
            api_key: API_KEY_FETCH_LOG,
            api_version: API_VERSION,
            request_id: -1,
        },
        request: fetch_log_request,
    }
}

pub fn build_get_table_request(
    table_path: &TablePath,
) -> FlussRequest<messages::GetTableInfoRequest> {
    let get_table_request = messages::GetTableInfoRequest {
        table_path: PbTablePath {
            database_name: table_path.database().to_owned(),
            table_name: table_path.table().to_owned(),
        },
    };
    FlussRequest {
        header: HeaderRequest {
            api_key: API_KEY_GET_TABLE,
            api_version: API_VERSION,
            request_id: -1,
        },
        request: get_table_request,
    }
}

pub fn build_produce_log_request(
    table_id: i64,
    schema_id: i32,
    bucket_id: i32,
    record_batch: RecordBatch,
    acks: i32,
    timeout_ms: i32,
) -> FlussRequest<ProduceLogRequest> {
    let mut bucket_req = vec![];
    let mut record_bytes_builder = MemoryLogRecordsArrowBuilder::new(schema_id, &record_batch);
    bucket_req.push(PbProduceLogReqForBucket {
        partition_id: Option::None,
        bucket_id,
        records: record_bytes_builder.build().unwrap(),
    });

    let produce_request = ProduceLogRequest {
        acks,
        table_id,
        timeout_ms,
        buckets_req: bucket_req,
    };

    FlussRequest {
        header: HeaderRequest {
            api_key: API_KEY_PRODUCE_LOG,
            api_version: API_VERSION,
            request_id: -1,
        },
        request: produce_request,
    }
}

pub fn to_table_path(table_path: &TablePath) -> PbTablePath {
    PbTablePath {
        database_name: table_path.database().to_string(),
        table_name: table_path.table().to_string(),
    }
}

pub fn from_pb_table_path(pb_table_path: &PbTablePath) -> TablePath {
    TablePath::new(
        pb_table_path.database_name.to_string(),
        pb_table_path.table_name.to_string(),
    )
}

pub fn from_pb_server_node(pb_server_node: PbServerNode, server_type: ServerType) -> ServerNode {
    ServerNode::new(
        pb_server_node.node_id,
        pb_server_node.host,
        pb_server_node.port as u32,
        server_type,
    )
}

pub fn from_pb_get_table_response(
    table_path: &TablePath,
    get_table_response: GetTableInfoResponse,
) -> TableInfo {
    let GetTableInfoResponse {
        table_id,
        schema_id,
        table_json,
        created_time,
        modified_time,
    } = get_table_response;
    let v: &[u8] = &table_json[..];
    let table_descriptor = TableDescriptor::deserialize_json(&serde_json::from_slice(v).unwrap());
    TableInfo::of(
        table_path.clone(),
        table_id,
        schema_id,
        table_descriptor,
        created_time,
        modified_time,
    )
}
