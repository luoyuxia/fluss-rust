use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;

use crate::{
    Result,
    connection::Connections,
    messages::{ProduceLogRequest, ProduceLogResponse},
    metadata::TablePath,
    metadata::{TableBucket, TableInfo, metadata_updater::MetadataUpdater},
    rpc::build_produce_log_request,
};

use super::scanner::TableScan;

pub struct Table {
    conn: Arc<Mutex<Connections>>,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
    table_path: TablePath,
    table_info: TableInfo,
}

impl Table {
    pub async fn new(
        con: Arc<Mutex<Connections>>,
        metadata_updater: Arc<Mutex<MetadataUpdater>>,
        table_path: &TablePath,
    ) -> Table {
        let metadata_lock = metadata_updater.lock().unwrap();
        metadata_lock
            .update_table_metadata(&[table_path.clone()])
            .await
            .unwrap();
        let table_info = metadata_lock.get_tableinfo_or_throw(table_path);
        Table {
            conn: con,
            metadata_updater: metadata_updater.clone(),
            table_path: table_path.clone(),
            table_info: table_info,
        }
    }

    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn new_scan(&self) -> TableScan {
        TableScan::new(
            self.conn.clone(),
            self.table_info.clone(),
            self.metadata_updater.clone(),
        )
    }

    pub fn new_append(&self) -> TableAppend {
        assert!(
            !self.table_info.has_primary_key(),
            "Table {:?} is not a Log Table and does't support AppendWriter.",
            &self.table_info.table_path
        );
        TableAppend {
            conn: self.conn.clone(),
            metadata_updater: self.metadata_updater.clone(),
            table_info: self.table_info.clone(),
        }
    }
}

pub struct TableAppend {
    conn: Arc<Mutex<Connections>>,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
    table_info: TableInfo,
}

impl TableAppend {
    pub fn create_writer(&self) -> AppendWriter {
        AppendWriter {
            conn: self.conn.clone(),
            metadata_updater: self.metadata_updater.clone(),
            table_info: self.table_info.clone(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum RequiredAcks {
    None = 0,
    One = 1,
    All = -1,
}

pub const DEFAULT_ACK_TIMEOUT_MILLS: i32 = 30 * 10_000;
pub const DEFAULT_REQUIRED_ACKS: RequiredAcks = RequiredAcks::One;

pub struct AppendWriter {
    conn: Arc<Mutex<Connections>>,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
    table_info: TableInfo,
}

impl AppendWriter {
    pub async fn append(&self, record_batch: RecordBatch) -> Result<()> {
        let bucket = self.table_info.get_num_buckets();
        let table_id = self.table_info.table_id;
        let select_bucket = rand::random_range(0..bucket);
        let target_bucket = TableBucket::new(table_id, select_bucket);
        let bucket_location = self
            .metadata_updater
            .lock()
            .unwrap()
            .get_bucket_localtion(&target_bucket);

        let leader_node = bucket_location.leader();

        let mut connections_guard = self.conn.lock().unwrap();
        let con = connections_guard.get_conn(leader_node).await.unwrap();

        let produce_request = build_produce_log_request(
            table_id,
            self.table_info.schema_id,
            select_bucket,
            record_batch,
            DEFAULT_REQUIRED_ACKS as i32,
            DEFAULT_ACK_TIMEOUT_MILLS,
        );

        con.send_recieve::<ProduceLogRequest, ProduceLogResponse>(produce_request)
            .await?;
        Ok(())
    }
}
