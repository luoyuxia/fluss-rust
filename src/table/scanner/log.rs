use std::{
    cell::Cell,
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::distr::Map;

const LOG_FETCH_MAX_BYTES: i32 = 16 * 1024 * 1024;
const LOG_FETCH_MAX_BYTES_FOR_BUCKET: i32 = 1 * 1024;
const LOG_FETCH_MIN_BYTES: i32 = 1;
const LOG_FETCH_WAIT_MAX_TIME: i32 = 500;

use crate::{
    Result,
    connection::Connections,
    messages::{FetchLogRequest, FetchLogResponse, PbFetchLogReqForBucket, PbFetchLogReqForTable},
    metadata::TablePath,
    metadata::{TableBucket, TableInfo, metadata_updater::MetadataUpdater},
    record::{
        ScanRecord, ScanRecords,
        log_records::{LogRecordsBatchs, ReadContext, to_arrow_schema},
    },
    rpc::build_fetch_log_request,
    util::FairBucketStatusMap,
};

pub struct LogScanner {
    table_path: TablePath,
    table_id: i64,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
    log_scanner_status: Arc<LogScannerStatus>,
    log_fetcher: LogFetcher,
}

impl LogScanner {
    pub fn new(
        table_info: &TableInfo,
        metadata_updater: Arc<Mutex<MetadataUpdater>>,
        conns: Arc<Mutex<Connections>>,
    ) -> Self {
        let log_scanner_status = Arc::new(LogScannerStatus::new());
        Self {
            table_path: table_info.table_path.clone(),
            table_id: table_info.table_id,
            metadata_updater: metadata_updater.clone(),
            log_scanner_status: log_scanner_status.clone(),
            log_fetcher: LogFetcher::new(
                table_info.clone(),
                conns,
                metadata_updater.clone(),
                log_scanner_status.clone(),
            ),
        }
    }

    pub async fn poll(&self, timeout: Duration) -> Result<ScanRecords> {
        Ok(ScanRecords::new(self.poll_for_fetches().await?))
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) {
        let table_bucket = TableBucket::new(self.table_id, bucket);
        self.metadata_updater
            .lock()
            .unwrap()
            .check_and_update_table_metadta(&vec![self.table_path.clone()])
            .await;
        self.log_scanner_status
            .assign_scan_bucket(table_bucket, offset);
    }

    async fn poll_for_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        Ok(self.log_fetcher.send_fetches_and_collect().await?)
    }
}

struct LogFetcher {
    table_path: TablePath,
    conns: Arc<Mutex<Connections>>,
    table_info: TableInfo,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
    log_scanner_status: Arc<LogScannerStatus>,
}

impl LogFetcher {
    pub fn new(
        table_info: TableInfo,
        conns: Arc<Mutex<Connections>>,
        metadata_updater: Arc<Mutex<MetadataUpdater>>,
        log_scanner_status: Arc<LogScannerStatus>,
    ) -> Self {
        LogFetcher {
            table_path: table_info.table_path.clone(),
            conns: conns.clone(),
            table_info: table_info.clone(),
            metadata_updater: metadata_updater.clone(),
            log_scanner_status: log_scanner_status.clone(),
        }
    }

    fn collect_fetch(&self) -> HashMap<TableBucket, Vec<ScanRecord>> {
        todo!()
    }

    async fn send_fetches_and_collect(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let fetch_request = self.prepare_fetch_log_requests().await;
        let mut result: HashMap<TableBucket, Vec<ScanRecord>> = HashMap::new();
        for (leader, fetch_request) in fetch_request {
            let server_node = self
                .metadata_updater
                .lock()
                .unwrap()
                .get_server_node(leader);
            let mut conn_lock = self.conns.lock().unwrap();
            let conn = conn_lock.get_conn(&server_node).await.unwrap();
            let fetch_response = conn
                .send_recieve::<FetchLogRequest, FetchLogResponse>(build_fetch_log_request(
                    fetch_request,
                ))
                .await?;
            for pb_fetch_log_resp in fetch_response.tables_resp {
                let table_id = pb_fetch_log_resp.table_id;
                let fetch_log_for_buckets = pb_fetch_log_resp.buckets_resp;
                let arrow_schema = to_arrow_schema(self.table_info.get_row_type());
                for fetch_log_for_bucket in fetch_log_for_buckets {
                    let mut fetch_records = vec![];
                    let bucket: i32 = fetch_log_for_bucket.bucket_id;
                    let table_bucket = TableBucket::new(table_id, bucket);
                    if fetch_log_for_bucket.records.is_some() {
                        let data = fetch_log_for_bucket.records.unwrap();
                        for log_record in &mut LogRecordsBatchs::new(&data) {
                            let last_offset = log_record.last_log_offset();
                            fetch_records
                                .extend(log_record.records(ReadContext::new(arrow_schema.clone())));
                            self.log_scanner_status
                                .update_offset(&table_bucket, last_offset + 1);
                        }
                    }
                    result.insert(table_bucket, fetch_records);
                }
            }
        }
        Ok(result)
    }

    async fn prepare_fetch_log_requests(&self) -> HashMap<i32, FetchLogRequest> {
        let mut fetch_log_req_for_buckets = HashMap::new();
        let mut table_id = None;
        let mut ready_for_fetch_count = 0;
        for bucket in self.fetchable_buckets() {
            if table_id.is_none() {
                table_id = Some(bucket.table_id());
            }

            let offset = match self.log_scanner_status.get_bucket_offset(&bucket) {
                Some(offset) => offset,
                None => {
                    // todo: debug
                    continue;
                }
            };

            let leader = self.get_table_bucket_leader(&bucket);
            let fetch_log_req_for_bucket = PbFetchLogReqForBucket {
                partition_id: None,
                bucket_id: bucket.bucket_id(),
                fetch_offset: offset,
                // 1M
                max_fetch_bytes: 1 * 1024 * 1024,
            };
            fetch_log_req_for_buckets
                .entry(leader)
                .or_insert_with(Vec::new)
                .push(fetch_log_req_for_bucket);
            ready_for_fetch_count += 1;
        }

        if ready_for_fetch_count == 0 {
            HashMap::new()
        } else {
            fetch_log_req_for_buckets
                .into_iter()
                .map(|(leader_id, feq_for_buckets)| {
                    let req_for_table = PbFetchLogReqForTable {
                        table_id: table_id.unwrap(),
                        projection_pushdown_enabled: false,
                        projected_fields: vec![],
                        buckets_req: feq_for_buckets,
                    };

                    let fetch_log_request = FetchLogRequest {
                        follower_server_id: -1,
                        max_bytes: LOG_FETCH_MAX_BYTES,
                        tables_req: vec![req_for_table],
                        max_wait_ms: Some(LOG_FETCH_WAIT_MAX_TIME),
                        min_bytes: Some(LOG_FETCH_MIN_BYTES),
                    };
                    (leader_id, fetch_log_request)
                })
                .collect()
        }
    }

    fn fetchable_buckets(&self) -> Vec<TableBucket> {
        // always available now
        self.log_scanner_status.fetchable_buckets(|_| true)
    }

    fn get_table_bucket_leader(&self, tb: &TableBucket) -> i32 {
        // todo: leader may not exits
        self.metadata_updater
            .lock()
            .unwrap()
            .get_bucket_localtion(tb)
            .leader()
            .id()
    }
}

pub struct LogScannerStatus {
    bucket_status_map: Arc<Mutex<FairBucketStatusMap<BucketScanStatus>>>,
}

impl LogScannerStatus {
    pub fn new() -> Self {
        Self {
            bucket_status_map: Arc::new(Mutex::new(FairBucketStatusMap::new())),
        }
    }

    pub fn prepare_to_poll(&self) -> bool {
        let map = self.bucket_status_map.lock().unwrap();
        map.size() > 0
    }

    pub fn move_bucket_to_end(&self, table_bucket: TableBucket) {
        let mut map = self.bucket_status_map.lock().unwrap();
        map.move_to_end(table_bucket);
    }

    /// Gets the offset of a bucket if it exists
    pub fn get_bucket_offset(&self, table_bucket: &TableBucket) -> Option<i64> {
        let map = self.bucket_status_map.lock().unwrap();
        map.status_value(table_bucket).map(|status| status.offset())
    }

    pub fn update_high_watermark(&self, table_bucket: &TableBucket, high_watermark: i64) {
        if let Some(status) = self.get_status(table_bucket) {
            status.set_high_watermark(high_watermark);
        }
    }

    pub fn update_offset(&self, table_bucket: &TableBucket, offset: i64) {
        if let Some(status) = self.get_status(table_bucket) {
            status.set_offset(offset);
        }
    }

    pub fn assign_scan_buckets(&self, scan_bucket_offsets: HashMap<TableBucket, i64>) {
        let mut map = self.bucket_status_map.lock().unwrap();
        for (bucket, offset) in scan_bucket_offsets {
            let status = map
                .status_value(&bucket)
                .cloned()
                .unwrap_or_else(|| Arc::new(BucketScanStatus::new(offset)));
            status.set_offset(offset);
            map.update(bucket, status);
        }
    }

    pub fn assign_scan_bucket(&self, table_bucket: TableBucket, offset: i64) {
        let status = Arc::new(BucketScanStatus::new(offset));
        self.bucket_status_map
            .lock()
            .unwrap()
            .update(table_bucket, status);
    }

    /// Unassigns scan buckets
    pub fn unassign_scan_buckets(&self, buckets: &[TableBucket]) {
        let mut map = self.bucket_status_map.lock().unwrap();
        for bucket in buckets {
            map.remove(bucket);
        }
    }

    /// Gets fetchable buckets based on availability predicate
    pub fn fetchable_buckets<F>(&self, is_available: F) -> Vec<TableBucket>
    where
        F: Fn(&TableBucket) -> bool,
    {
        let map = self.bucket_status_map.lock().unwrap();
        let mut result = Vec::new();
        map.for_each(|bucket, _| {
            if is_available(bucket) {
                result.push(bucket.clone());
            }
        });
        result
    }

    /// Helper to get bucket status
    fn get_status(&self, table_bucket: &TableBucket) -> Option<Arc<BucketScanStatus>> {
        let map = self.bucket_status_map.lock().unwrap();
        map.status_value(table_bucket).cloned()
    }
}

impl Default for LogScannerStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct BucketScanStatus {
    offset: Cell<i64>,
    high_watermark: Cell<i64>,
}

impl BucketScanStatus {
    pub fn new(offset: i64) -> Self {
        Self {
            offset: Cell::new(offset),
            high_watermark: Cell::new(0),
        }
    }

    pub fn offset(&self) -> i64 {
        self.offset.get()
    }

    pub fn set_offset(&self, offset: i64) {
        self.offset.set(offset);
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark.get()
    }

    pub fn set_high_watermark(&self, high_watermark: i64) {
        self.high_watermark.set(high_watermark);
    }
}

struct LogFetchCollector {
    table_path: TablePath,
    log_scanner_status: Arc<LogScannerStatus>,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
}

impl LogFetchCollector {
    fn new(
        table_path: TablePath,
        scanner_status: Arc<LogScannerStatus>,
        metadata_updater: Arc<Mutex<MetadataUpdater>>,
    ) -> LogFetchCollector {
        LogFetchCollector {
            table_path: table_path.clone(),
            log_scanner_status: scanner_status.clone(),
            metadata_updater: metadata_updater.clone(),
        }
    }

    fn collect_fetch() -> HashMap<TableBucket, Vec<ScanRecord>> {
        HashMap::default()
    }
}
