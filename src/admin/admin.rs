use std::sync::{Arc, Mutex};

use crate::{
    Result,
    connection::{Connections, ServerNode},
    messages::{self, GetTableInfoRequest, GetTableInfoResponse},
    metadata::TablePath,
    metadata::{TableDescriptor, TableInfo, metadata_updater::MetadataUpdater},
    rpc::{build_create_table_request, build_get_table_request, from_pb_get_table_response},
};

pub struct FlussAdmin {
    conn: Arc<Mutex<Connections>>,
    metadata_updater: Arc<Mutex<MetadataUpdater>>,
}

impl FlussAdmin {
    pub fn new(
        conns: Arc<Mutex<Connections>>,
        metadata_updater: Arc<Mutex<MetadataUpdater>>,
    ) -> Self {
        FlussAdmin {
            conn: conns,
            metadata_updater,
        }
    }

    pub async fn create_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let node = self.get_coordinator_server_node().await;
        // todo: it'll hold the lock util read the response, refactor it anyway
        let mut connections_guard = self.conn.lock().unwrap();
        let connection = connections_guard.get_conn(&node).await.unwrap();
        let create_table_request =
            build_create_table_request(table_path, table_descriptor, ignore_if_exists);
        connection
            .send_recieve::<messages::CreateTableRequest, messages::CreateTableResponse>(
                create_table_request,
            )
            .await?;
        Ok(())
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<TableInfo> {
        let node = self.get_coordinator_server_node().await;
        // todo: it'll hold the lock util read the response, refactor it anyway
        let mut connections_guard = self.conn.lock().unwrap();
        let connection = connections_guard.get_conn(&node).await.unwrap();

        let get_table_request = build_get_table_request(table_path);

        let get_table_response = connection
            .send_recieve::<GetTableInfoRequest, GetTableInfoResponse>(get_table_request)
            .await?;
        Ok(from_pb_get_table_response(table_path, get_table_response))
    }

    pub async fn get_coordinator_server_node(&self) -> ServerNode {
        self.metadata_updater
            .lock()
            .unwrap()
            .get_coordinator_server()
            .unwrap()
    }
}
