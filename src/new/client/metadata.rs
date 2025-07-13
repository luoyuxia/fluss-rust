use crate::messages::MetadataResponse;
use crate::metadata::metadata_updater::BucketLocation;
use crate::metadata::{TableBucket, TableInfo, TablePath};
use crate::new::BucketId;
use crate::new::connection::connection::{Connections, ServerConnection, ServerNode};
use crate::new::error::{Error, Result};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Default)]
pub struct Cluster {}

impl Cluster {
    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<&ServerNode> {
        todo!()
    }

    pub fn get_tablet_server(&self) -> Option<ServerNode> {
        todo!()
    }

    pub fn get_table_bucket(&self, table_path: &TablePath, bucket_id: BucketId) -> &TableBucket {
        todo!()
    }

    pub fn get_bucket_locations_by_path(&self) -> &HashMap<TablePath, Vec<BucketLocation>> {
        todo!()
    }

    pub fn get_available_buckets_for_table_path(
        &self,
        table_path: &TablePath,
    ) -> Vec<BucketLocation> {
        todo!()
    }

    pub fn get_bucket_count(&self, table_path: &TablePath) -> i32 {
        todo!()
    }

    pub fn get_table(&self, table_path: &TablePath) -> &TableInfo {
        todo!()
    }
}

#[derive(Default)]
pub struct Metadata {
    cluster: RwLock<Arc<Cluster>>,
    connections: Connections,
}

impl Metadata {
    pub async fn update(&self, metadata_response: MetadataResponse) {
        let mut cluster = self.cluster.write();
        *cluster = Arc::new(Cluster {
            // todo
        })
    }

    pub async fn update_table_metadata(&self, table_paths: &HashSet<TablePath>) {
        todo!()
    }

    pub async fn get_connection(&self, server_node: &ServerNode) -> Result<ServerConnection> {
        self.connections.get_connection(server_node).await
    }

    pub async fn get_cluster(&self) -> Arc<Cluster> {
        let guard = self.cluster.read();
        guard.clone()
    }

    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<&ServerNode> {
        todo!()
    }

    pub fn get_tablet_server(&self, server_id: i32) -> Option<&ServerNode> {
        todo!()
    }
}
