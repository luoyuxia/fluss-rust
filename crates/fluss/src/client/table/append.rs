use std::sync::Arc;
use crate::client::{WriteRecord, WriterClient};
use crate::metadata::{TableInfo, TablePath};
use crate::row::GenericRow;

use crate::error::Result;

pub struct TableAppend {
    table_path: TablePath,
    table_info: TableInfo,
    writer_client: Arc<WriterClient>,
}

impl TableAppend {
    pub(super) fn new(
        table_path: TablePath,
        table_info: TableInfo,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        Self {
            table_path,
            table_info,
            writer_client,
        }
    }

    pub fn create_writer(&self) -> AppendWriter {
        AppendWriter {
            table_path: Arc::new(self.table_path.clone()),
            writer_client: self.writer_client.clone(),
        }
    }
}

pub struct AppendWriter {
    table_path: Arc<TablePath>,
    writer_client: Arc<WriterClient>,
}

impl AppendWriter {
    pub async fn append(&self, row: GenericRow<'_>) -> Result<()> {
        let record = WriteRecord::new(self.table_path.clone(), row);
        let result_handle = self.writer_client.send(&record).await?;
        let result = result_handle.wait().await?;
        result_handle.result(result)
    }

    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }
}