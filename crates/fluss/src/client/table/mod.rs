use std::sync::Arc;
use crate::client::connection::FlussConnection;
use crate::client::metadata::Metadata;
use crate::client::table::append::TableAppend;
use crate::client::table::scanner::TableScan;
use crate::metadata::{TableInfo, TablePath};

use crate::error::Result;

mod append;

mod scanner;
mod writer;

pub struct FlussTable<'a> {
    conn: &'a FlussConnection,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    table_path: TablePath,
    has_primary_key: bool,
}

impl<'a> FlussTable<'a> {
    pub fn new(conn: &'a FlussConnection, metadata: Arc<Metadata>, table_info: TableInfo) -> Self {
        FlussTable {
            conn,
            table_path: table_info.table_path.clone(),
            has_primary_key: table_info.has_primary_key(),
            table_info,
            metadata,
        }
    }

    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn new_append(&self) -> Result<TableAppend> {
        Ok(TableAppend::new(
            self.table_path.clone(),
            self.table_info.clone(),
            self.conn.get_or_create_writer_client()?,
        ))
    }

    pub fn new_scan(&self) -> TableScan {
        TableScan::new(self.conn, self.table_info.clone(), self.metadata.clone())
    }
}

impl<'a> Drop for FlussTable<'a> {
    fn drop(&mut self) {
        // do-nothing now
    }
}