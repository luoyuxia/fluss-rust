use crate::metadata::TablePath;
use crate::new::client::GenericRow;
use crate::new::client::write::broadcast::{BatchWriteResult, BroadcastOnceReceiver};
use crate::new::error::{Error, Result};
use std::rc::Rc;

pub mod accumulator;
mod batch;
pub(crate) mod broadcast;
mod bucket_assigner;
mod sender;
pub mod writer_client;

pub struct WriteRecord {
    pub row: GenericRow,
    pub table_path: Rc<TablePath>,
}

impl WriteRecord {
    pub fn new(table_path: Rc<TablePath>, row: GenericRow) -> Self {
        Self { row, table_path }
    }
}

pub struct ResultHandle {
    receiver: BroadcastOnceReceiver<BatchWriteResult>,
}

impl ResultHandle {
    pub fn new(receiver: BroadcastOnceReceiver<BatchWriteResult>) -> Self {
        ResultHandle { receiver }
    }

    pub async fn wait(&mut self) -> Result<BatchWriteResult, Error> {
        self.receiver
            .receive()
            .await
            .map_err(|e| Error::WriteError(e.to_string()))
    }

    pub fn result(&self, batch_result: BatchWriteResult) -> Result<(), Error> {
        // do nothing, just return empty result
        batch_result.map_err(|e| Error::WriteError(e.to_string()))
    }
}
