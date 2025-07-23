use std::sync::Arc;
use crate::client::{WriteRecord, WriterClient};
use crate::row::GenericRow;

use crate::error::Result;
use crate::metadata::{TableInfo, TablePath};

pub trait TableWriter {
    async fn flush(&self) -> Result<()>;
}

pub trait AppendWriter: TableWriter {
    async fn append(&self, row: GenericRow) -> Result<()>;
}

pub trait UpsertWriter: TableWriter {
    async fn upsert(&self, row: GenericRow) -> Result<()>;
    async fn delete(&self, row: GenericRow) -> Result<()>;
}

pub struct AbstractTableWriter {
    table_path: Arc<TablePath>,
    writer_client: Arc<WriterClient>,
    field_count: i32,
}

impl AbstractTableWriter {
    pub fn new(
        table_path: TablePath,
        table_info: &TableInfo,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        // todo: partition
        Self {
            table_path: Arc::new(table_path),
            writer_client,
            field_count: table_info.row_type().fields().len() as i32,
        }
    }

    pub async fn send(&self, write_record: &WriteRecord<'_>) -> Result<()> {
        let result_handle = self.writer_client.send(write_record).await?;
        let result = result_handle.wait().await?;
        result_handle.result(result)
    }
}

impl TableWriter for AbstractTableWriter {
    async fn flush(&self) -> Result<()> {
        todo!()
    }
}

// Append writer implementation
pub struct AppendWriterImpl {
    base: AbstractTableWriter,
}

impl AppendWriterImpl {
    pub async fn append(&self, row: GenericRow<'_>) -> Result<()> {
        let record = WriteRecord::new(self.base.table_path.clone(), row);
        self.base.send(&record).await
    }
}