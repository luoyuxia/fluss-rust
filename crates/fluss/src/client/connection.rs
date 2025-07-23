use std::sync::Arc;
use parking_lot::RwLock;
use crate::client::admin::FlussAdmin;
use crate::client::metadata::Metadata;
use crate::client::table::FlussTable;
use crate::client::WriterClient;
use crate::config::Config;
use crate::rpc::RpcClient;

use crate::error::Result;
use crate::metadata::TablePath;

pub struct FlussConnection {
    metadata: Arc<Metadata>,
    network_connects: Arc<RpcClient>,
    args: Config,
    writer_client: RwLock<Option<Arc<WriterClient>>>,
}

impl FlussConnection {
    pub async fn new(arg: Config) -> Result<Self> {
        let connections = Arc::new(RpcClient::new());
        let metadata = Metadata::new(
            arg.bootstrap_server.as_ref().unwrap().as_str(),
            connections.clone(),
        )
            .await?;

        Ok(FlussConnection {
            metadata: Arc::new(metadata),
            network_connects: connections.clone(),
            args: arg.clone(),
            writer_client: Default::default(),
        })
    }

    pub fn get_metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }

    pub fn get_connections(&self) -> Arc<RpcClient> {
        self.network_connects.clone()
    }

    pub async fn get_admin(&self) -> Result<FlussAdmin> {
        FlussAdmin::new(self.network_connects.clone(), self.metadata.clone()).await
    }

    pub fn get_or_create_writer_client(&self) -> Result<Arc<WriterClient>> {
        if let Some(client) = self.writer_client.read().as_ref() {
            return Ok(client.clone());
        }

        // If not exists, create new one
        let client = Arc::new(WriterClient::new(self.args.clone(), self.metadata.clone())?);
        *self.writer_client.write() = Some(client.clone());
        Ok(client)
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable> {
        self.metadata.update_table_metadata(table_path).await?;
        let table_info = self.metadata.get_cluster().get_table(table_path).clone();
        Ok(FlussTable::new(self, self.metadata.clone(), table_info))
    }
}