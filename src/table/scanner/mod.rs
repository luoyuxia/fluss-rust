use std::sync::{Arc, Mutex};

use log::LogScanner;

use crate::{
    connection::Connections,
    metadata::{
        TableInfo,
        metadata_updater::MetadataUpdater,
    },
};

pub mod log;

pub struct TableScan {
    conns: Arc<Mutex<Connections>>,
    table_info: TableInfo,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
}

impl TableScan {
    pub fn new(
        conns: Arc<Mutex<Connections>>,
        table_info: TableInfo,
        metadata_updater: Arc<Mutex<MetadataUpdater>>,
    ) -> TableScan {
        Self {
            conns,
            table_info,
            metadata_updater: metadata_updater.clone(),
        }
    }

    pub fn create_log_scanner(&self) -> LogScanner {
        LogScanner::new(
            &self.table_info,
            self.metadata_updater.clone(),
            self.conns.clone(),
        )
    }
}
