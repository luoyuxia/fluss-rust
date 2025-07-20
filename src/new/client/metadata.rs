use crate::connection::{ServerNode, ServerType};
use crate::messages::{MetadataResponse, PbServerNode};
use crate::metadata::metadata_serde::JsonSerde;
use crate::metadata::metadata_updater::BucketLocation;
use crate::metadata::{TableBucket, TableDescriptor, TableInfo, TablePath};
use crate::new::BucketId;
use crate::new::connection::connection::{Connections, ServerConnection};
use crate::new::error::Result;
use crate::new::protocol::message::update_metadata::UpdateMetadataRequest;
use crate::rpc::from_pb_table_path;
use parking_lot::RwLock;
use rand::random_range;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
static EMPTY: Vec<BucketLocation> = Vec::new();

#[derive(Default)]
pub struct Cluster {
    coordinator_server: Option<ServerNode>,
    alive_tablet_servers_by_id: HashMap<i32, ServerNode>,
    alive_tablet_servers: Vec<ServerNode>,
    available_locations_by_path: HashMap<TablePath, Vec<BucketLocation>>,
    available_locations_by_bucket: HashMap<TableBucket, BucketLocation>,
    table_id_by_path: HashMap<TablePath, i64>,
    table_path_by_id: HashMap<i64, TablePath>,
    table_info_by_path: HashMap<TablePath, TableInfo>,
}

impl Cluster {
    pub fn new(
        coordinator_server: Option<ServerNode>,
        alive_tablet_servers_by_id: HashMap<i32, ServerNode>,
        available_locations_by_path: HashMap<TablePath, Vec<BucketLocation>>,
        available_locations_by_bucket: HashMap<TableBucket, BucketLocation>,
        table_id_by_path: HashMap<TablePath, i64>,
        table_info_by_path: HashMap<TablePath, TableInfo>,
    ) -> Self {
        let alive_tablet_servers = alive_tablet_servers_by_id.values().cloned().collect();
        let table_path_by_id = table_id_by_path
            .iter()
            .map(|(path, table_id)| (*table_id, path.clone()))
            .collect();
        Cluster {
            coordinator_server,
            alive_tablet_servers_by_id,
            alive_tablet_servers,
            available_locations_by_path,
            available_locations_by_bucket,
            table_id_by_path,
            table_path_by_id,
            table_info_by_path,
        }
    }

    pub fn update(&mut self, cluster: Cluster) {
        let Cluster {
            coordinator_server,
            alive_tablet_servers_by_id,
            alive_tablet_servers,
            available_locations_by_path,
            available_locations_by_bucket,
            table_id_by_path,
            table_path_by_id,
            table_info_by_path,
        } = cluster;
        self.coordinator_server = coordinator_server;
        self.alive_tablet_servers_by_id = alive_tablet_servers_by_id;
        self.alive_tablet_servers = alive_tablet_servers;
        self.available_locations_by_path = available_locations_by_path;
        self.available_locations_by_bucket = available_locations_by_bucket;
        self.table_id_by_path = table_id_by_path;
        self.table_path_by_id = table_path_by_id;
        self.table_info_by_path = table_info_by_path;
    }

    pub fn from_metadata_response(
        metadata_response: MetadataResponse,
        origin_cluster: Option<&Cluster>,
    ) -> Cluster {
        let mut servers = HashMap::with_capacity(metadata_response.tablet_servers.len());
        for pb_server in metadata_response.tablet_servers {
            let server_id = pb_server.node_id;
            let server_node = from_pb_server_node(pb_server, ServerType::TabletServer);
            servers.insert(server_id, server_node);
        }

        let coordinator_server = metadata_response
            .coordinator_server
            .map(|node| from_pb_server_node(node, ServerType::CoordinatorServer));

        let mut table_id_by_path = HashMap::new();
        let mut table_info_by_path = HashMap::new();
        if let Some(origin) = origin_cluster {
            table_info_by_path.extend(origin.get_table_info_by_path().clone());
            table_id_by_path.extend(origin.get_table_id_by_path().clone());
        }

        // Index the bucket locations by table path, and index bucket location by bucket
        let mut tmp_available_location_by_bucket = HashMap::new();
        let mut tmp_available_locations_by_path = HashMap::new();

        for table_metadata in metadata_response.table_metadata {
            let table_id = table_metadata.table_id;
            let table_path = from_pb_table_path(&table_metadata.table_path);
            let table_descriptor = TableDescriptor::deserialize_json(
                &serde_json::from_slice(table_metadata.table_json.as_slice()).unwrap(),
            );
            let table_info = TableInfo::of(
                table_path.clone(),
                table_id,
                table_metadata.schema_id,
                table_descriptor,
                table_metadata.created_time,
                table_metadata.modified_time,
            );
            table_info_by_path.insert(table_path.clone(), table_info);

            // now, get bucket matadata
            let mut found_unavailable_bucket = false;
            let mut available_bucket_for_table = vec![];
            let mut bucket_for_table = vec![];
            for bucket_metadata in table_metadata.bucket_metadata {
                let bucket_id = bucket_metadata.bucket_id;
                let bucket = TableBucket::new(table_id, bucket_id);
                let bucket_location;
                if let Some(leader_id) = bucket_metadata.leader_id
                    && let Some(server_node) = servers.get(&leader_id)
                {
                    bucket_location = BucketLocation::new(
                        bucket.clone(),
                        Some(server_node.clone()),
                        table_path.clone(),
                    );
                    available_bucket_for_table.push(bucket_location.clone());
                    tmp_available_location_by_bucket
                        .insert(bucket.clone(), bucket_location.clone());
                } else {
                    found_unavailable_bucket = true;
                    bucket_location = BucketLocation::new(bucket.clone(), None, table_path.clone());
                }
                bucket_for_table.push(bucket_location.clone());
            }

            if found_unavailable_bucket {
                tmp_available_locations_by_path
                    .insert(table_path.clone(), available_bucket_for_table.clone());
            } else {
                tmp_available_locations_by_path.insert(table_path.clone(), bucket_for_table);
            }
        }
        Cluster::new(
            coordinator_server,
            servers,
            tmp_available_locations_by_path,
            tmp_available_location_by_bucket,
            table_id_by_path,
            table_info_by_path,
        )
    }

    pub fn get_coordinator_server(&self) -> Option<&ServerNode> {
        self.coordinator_server.as_ref()
    }

    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<&ServerNode> {
        let location = self.available_locations_by_bucket.get(table_bucket);
        if let Some(location) = location {
            location.leader().as_ref()
        } else {
            None
        }
    }

    pub fn get_tablet_server(&self, id: i32) -> Option<&ServerNode> {
        self.alive_tablet_servers_by_id.get(&id)
    }

    pub fn get_table_bucket(&self, table_path: &TablePath, bucket_id: BucketId) -> TableBucket {
        let table_info = self.get_table(table_path);
        TableBucket::new(table_info.table_id, bucket_id)
    }

    pub fn get_bucket_locations_by_path(&self) -> &HashMap<TablePath, Vec<BucketLocation>> {
        &self.available_locations_by_path
    }

    pub fn get_table_info_by_path(&self) -> &HashMap<TablePath, TableInfo> {
        &self.table_info_by_path
    }

    pub fn get_table_id_by_path(&self) -> &HashMap<TablePath, i64> {
        &self.table_id_by_path
    }

    pub fn get_available_buckets_for_table_path(
        &self,
        table_path: &TablePath,
    ) -> &Vec<BucketLocation> {
        self.available_locations_by_path
            .get(table_path)
            .unwrap_or(&EMPTY)
    }

    pub fn get_one_available_server(&self) -> &ServerNode {
        assert!(
            !self.alive_tablet_servers.is_empty(),
            "no alive tablet server in cluster"
        );
        let offset = random_range(0..self.alive_tablet_servers.len());
        self.alive_tablet_servers
            .get(offset)
            .unwrap_or_else(|| panic!("can't find alive tab server by offset {offset}"))
    }

    pub fn get_bucket_count(&self, table_path: &TablePath) -> i32 {
        self.table_info_by_path
            .get(table_path)
            .unwrap_or_else(|| panic!("can't not table info by path {table_path}"))
            .num_buckets
    }

    pub fn get_table(&self, table_path: &TablePath) -> &TableInfo {
        self.table_info_by_path
            .get(table_path)
            .unwrap_or_else(|| panic!("can't find table info by path {table_path}"))
    }

    pub fn opt_get_table(&self, table_path: &TablePath) -> Option<&TableInfo> {
        self.table_info_by_path.get(table_path)
    }
}

pub fn from_pb_server_node(pb_server_node: PbServerNode, server_type: ServerType) -> ServerNode {
    ServerNode::new(
        pb_server_node.node_id,
        pb_server_node.host,
        pb_server_node.port as u32,
        server_type,
    )
}

#[derive(Default)]
pub struct Metadata {
    cluster: RwLock<Arc<Cluster>>,
    connections: Arc<Connections>,
}

impl Metadata {
    pub async fn new(boot_strap: &str, connections: Arc<Connections>) -> Result<Self> {
        let custer = Self::init_cluster(boot_strap, connections.clone()).await?;
        Ok(Metadata {
            cluster: RwLock::new(Arc::new(custer)),
            connections,
        })
    }

    async fn init_cluster(boot_strap: &str, connections: Arc<Connections>) -> Result<Cluster> {
        let socker_addrss = boot_strap.parse::<SocketAddr>().unwrap();
        let server_node = ServerNode::new(
            -1,
            socker_addrss.ip().to_string(),
            socker_addrss.port() as u32,
            ServerType::CoordinatorServer,
        );
        let con = connections.get_connection(&server_node).await?;
        let response = con.request(UpdateMetadataRequest::new(&[])).await?;
        Ok(Cluster::from_metadata_response(response, None))
    }

    pub async fn update(&self, metadata_response: MetadataResponse) {
        let origin_cluster = self.cluster.read().clone();
        let mut cluster = self.cluster.write();
        *cluster = Arc::new(Cluster::from_metadata_response(
            metadata_response,
            Some(&origin_cluster),
        ))
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
        self.connections.get_connection(server_node).await
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        let guard = self.cluster.read();
        guard.clone()
    }

    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<&ServerNode> {
        todo!()
    }
}
