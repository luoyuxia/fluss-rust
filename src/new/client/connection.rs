use crate::admin::admin::FlussAdmin;
use crate::metadata::TablePath;
use crate::new::args::Config;
use crate::new::client::metadata::Metadata;
use crate::new::client::table::FlussTable;
use crate::new::client::write::writer_client::WriterClient;
use crate::new::connection::connection::Connections;
use crate::new::error::Result;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct FlussConnection {
    metadata: Arc<Metadata>,
    network_connects: Arc<Connections>,
    args: Config,
    writer_client: RwLock<Option<Arc<WriterClient>>>,
}

impl FlussConnection {
    pub async fn new(arg: Config) -> Result<Self> {
        let connections = Arc::new(Connections::new());
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

    fn get_admin(&self) -> FlussAdmin {
        todo!()
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

    async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable> {
        self.metadata.update_table_metadata(table_path).await?;
        let table_info = self
            .metadata
            .get_cluster()
            .await
            .get_table(table_path)
            .clone();
        Ok(FlussTable::new(self, self.metadata.clone(), table_info))
    }
}
