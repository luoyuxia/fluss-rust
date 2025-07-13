use crate::metadata::{TableInfo, TablePath};
use crate::new::client::GenericRow;
use crate::new::client::write::accumulator::RecordAccumulator;
use crate::new::client::write::writer_client::WriterClient;
use crate::new::error::{Error, Result};
use std::sync::Arc;

pub struct TableAppend {
    table_path: TablePath,
    table_info: TableInfo,
    writer_client: Arc<WriterClient>,
}

impl TableAppend {
    pub fn create_writer() -> AppendWriter {
        todo!()
    }
}

pub struct AppendWriter {
    writer_client: Arc<WriterClient>,
}

impl AppendWriter {
    pub async fn append(&self, row: GenericRow) -> Result<()> {
        // todo: wrap it to a write record
        //  let record = WriteRecord::new(
        //      Rc::new(self.)
        //      row);
        //  let mut result_handle = self.writer_client.send(record)?;
        //  let result = result_handle.wait().await?;
        //  result_handle.result(result)
        todo!()
    }
}
