use std::collections::HashSet;
use std::net::SocketAddr;
use crate::cluster::{Cluster, ServerNode, ServerType};
use crate::metadata::{TableBucket, TablePath};
use std::sync::Arc;
use parking_lot::RwLock;
use crate::rpc::{RpcClient, ServerConnection, UpdateMetadataRequest};

use crate::error::Result;
use crate::proto::MetadataResponse;

#[derive(Default)]
pub struct Metadata {
    cluster: RwLock<Arc<Cluster>>,
    connections: Arc<RpcClient>,
}

impl Metadata {
    pub async fn new(boot_strap: &str, connections: Arc<RpcClient>) -> Result<Self> {
        let custer = Self::init_cluster(boot_strap, connections.clone()).await?;
        Ok(Metadata {
            cluster: RwLock::new(Arc::new(custer)),
            connections,
        })
    }

    async fn init_cluster(boot_strap: &str, connections: Arc<RpcClient>) -> Result<Cluster> {
        let socker_addrss = boot_strap.parse::<SocketAddr>().unwrap();
        let server_node = ServerNode::new(
            -1,
            socker_addrss.ip().to_string(),
            socker_addrss.port() as u32,
            ServerType::CoordinatorServer,
        );
        let con = connections.get_connection(&server_node).await?;
        let response = con.request(UpdateMetadataRequest::new(&[])).await?;
        Cluster::from_metadata_response(response, None)
    }

    pub async fn update(&self, metadata_response: MetadataResponse) -> Result<()> {
        let origin_cluster = self.cluster.read().clone();
        let new_cluster = Cluster::from_metadata_response(
            metadata_response,
            Some(&origin_cluster),
        )?;
        let mut cluster = self.cluster.write();
        *cluster = Arc::new(new_cluster);
        Ok(())
    }

    pub async fn update_tables_metadata(&self, table_paths: &HashSet<&TablePath>) -> Result<()> {
        let server = self.cluster.read().get_one_available_server().clone();
        let conn = self.connections.get_connection(&server).await?;

        let update_table_paths: Vec<&TablePath> =
            table_paths.iter().copied().collect();
        let response = conn
            .request(UpdateMetadataRequest::new(update_table_paths.as_slice()))
            .await?;
        self.update(response).await;
        Ok(())
    }

    pub async fn update_table_metadata(&self, table_path: &TablePath) -> Result<()> {
        self.update_tables_metadata(&HashSet::from([table_path]))
            .await
    }

    pub async fn check_and_update_table_metadata(&self, table_paths: &[TablePath]) -> Result<()> {
        let cluster_binding = self.cluster.read();
        let need_update_table_paths: HashSet<&TablePath> = table_paths
            .iter()
            .filter(|table_path| cluster_binding.opt_get_table(table_path).is_none())
            .collect();
        if !need_update_table_paths.is_empty() {
            let _ = self.update_tables_metadata(&need_update_table_paths).await;
        }
        Ok(())
    }

    pub async fn get_connection(&self, server_node: &ServerNode) -> Result<ServerConnection> {
        let result = self.connections.get_connection(server_node).await?;
        Ok(result)
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        let guard = self.cluster.read();
        guard.clone()
    }

    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<&ServerNode> {
        todo!()
    }
}