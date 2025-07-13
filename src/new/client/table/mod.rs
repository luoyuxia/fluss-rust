use crate::connection::FlussConnection;
use crate::metadata::{TableInfo, TablePath};
use crate::new::client::table::append::TableAppend;
use std::sync::Arc;

mod append;

mod table;
mod writer;

pub struct FlussTable {
    conn: Arc<FlussConnection>,
    table_info: TableInfo,
    table_path: TablePath,
    has_primary_key: bool,
}

impl FlussTable {
    pub fn new(conn: Arc<FlussConnection>, table_info: TableInfo) -> Self {
        FlussTable {
            conn,
            table_path: table_info.table_path.clone(),
            has_primary_key: table_info.has_primary_key(),
            table_info,
        }
    }

    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn new_append(&self) -> TableAppend {
        todo!()
    }
}

impl Drop for FlussTable {
    fn drop(&mut self) {
        // do-nothing now
    }
}
