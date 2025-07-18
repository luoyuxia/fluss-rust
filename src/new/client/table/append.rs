use crate::metadata::{TableInfo, TablePath};
use crate::new::client::GenericRow;
use crate::new::client::write::WriteRecord;
use crate::new::client::write::writer_client::WriterClient;
use crate::new::error::Result;
use std::rc::Rc;
use std::sync::Arc;

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
            table_path: Rc::new(self.table_path.clone()),
            writer_client: self.writer_client.clone(),
        }
    }
}

pub struct AppendWriter {
    table_path: Rc<TablePath>,
    writer_client: Arc<WriterClient>,
}

impl AppendWriter {
    pub async fn append(&self, row: GenericRow) -> Result<()> {
        let record = WriteRecord::new(self.table_path.clone(), row);
        let mut result_handle = self.writer_client.send(&record).await?;
        let result = result_handle.wait().await?;
        result_handle.result(result)
    }
}
