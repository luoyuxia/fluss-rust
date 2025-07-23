mod accumulator;
mod batch;

use std::sync::Arc;
pub use accumulator::*;
use crate::client::broadcast::{BatchWriteResult, BroadcastOnceReceiver};
use crate::error::Error;
use crate::metadata::TablePath;
use crate::row::GenericRow;

pub(crate) mod broadcast;
mod bucket_assigner;

mod sender;
mod writer_client;

pub use writer_client::WriterClient;

pub struct WriteRecord<'a> {
    pub row: GenericRow<'a>,
    pub table_path: Arc<TablePath>,
}

impl<'a> WriteRecord<'a> {
    pub fn new(table_path: Arc<TablePath>, row: GenericRow<'a>) -> Self {
        Self { row, table_path }
    }
}

#[derive(Debug, Clone)]
pub struct ResultHandle {
    receiver: BroadcastOnceReceiver<BatchWriteResult>,
}

impl ResultHandle {
    pub fn new(receiver: BroadcastOnceReceiver<BatchWriteResult>) -> Self {
        ResultHandle { receiver }
    }

    pub async fn wait(&self) -> Result<BatchWriteResult, Error> {
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