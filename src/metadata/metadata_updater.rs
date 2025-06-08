use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
};

use rand::{Rng, random_range};

use crate::{
    Result,
    connection::{ConnectionConfig, Connections, ServerNode, ServerType},
    messages,
    metadata::TablePath,
    rpc::{build_metadata_request, from_pb_server_node, from_pb_table_path},
};

use super::{TableBucket, TableDescriptor, TableInfo, metadata_serde::JsonSerde};

const UNKOWN_TABLE_ID: i64 = -1;

#[derive(Debug, Default)]
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
        table_id_by_path: HashMap<TablePath, i64>,
        table_info_by_path: HashMap<TablePath, TableInfo>,
    ) -> Self {
        let alive_tablet_servers = alive_tablet_servers_by_id
            .iter()
            .map(|(_, server)| server.clone())
            .collect();
        let table_path_by_id = table_id_by_path
            .iter()
            .map(|(path, table_id)| (table_id.clone(), path.clone()))
            .collect();
        let available_locations_by_bucket: HashMap<TableBucket, BucketLocation> =
            available_locations_by_path
                .iter()
                .flat_map(|(_, bucket_location)| bucket_location)
                .map(|bucket_location| {
                    (
                        bucket_location.table_bucket.clone(),
                        bucket_location.clone(),
                    )
                })
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

    pub fn get_coordinator_server(&self) -> Option<ServerNode> {
        return self.coordinator_server.clone();
    }

    pub fn get_table_info_by_path(&self) -> &HashMap<TablePath, TableInfo> {
        return &self.table_info_by_path;
    }

    pub fn get_table_id_by_path(&self) -> &HashMap<TablePath, i64> {
        &self.table_id_by_path
    }

    pub fn get_table_id(&self, table_path: &TablePath) -> i64 {
        *self
            .table_id_by_path
            .get(table_path)
            .unwrap_or(&UNKOWN_TABLE_ID)
    }

    pub fn get_table_path_or_else_throw(&self, table_id: i64) -> &TablePath {
        self.table_path_by_id.get(&table_id).unwrap()
    }

    pub fn get_table_or_else_throw(&self, table_id: i64) -> &TableInfo {
        self.get_table(self.get_table_path_or_else_throw(table_id))
            .unwrap()
    }

    pub fn get_table(&self, table_path: &TablePath) -> Option<&TableInfo> {
        self.table_info_by_path.get(table_path)
    }

    pub fn get_one_avaiable_server(&self) -> ServerNode {
        assert!(
            self.alive_tablet_servers.len() > 0,
            "no alive tablet server in cluster"
        );
        let offset = random_range(0..self.alive_tablet_servers.len());
        self.alive_tablet_servers.get(offset).unwrap().clone()
    }
}

impl Cluster {
    pub fn from_metadata_response(
        metadata_response: messages::MetadataResponse,
        origin_cluster: Option<&Cluster>,
    ) -> Cluster {
        let mut servers = HashMap::with_capacity(metadata_response.tablet_servers.len());
        for pb_server in metadata_response.tablet_servers {
            let server_id = pb_server.node_id;
            let server_node = from_pb_server_node(pb_server, ServerType::TabletServer);
            servers.insert(server_id, server_node);
        }

        let coordinator_server = match metadata_response.coordinator_server {
            Some(node) => Some(from_pb_server_node(node, ServerType::CoordinatorServer)),
            None => None,
        };

        let mut table_id_by_path = HashMap::new();
        let mut table_info_by_path = HashMap::new();
        if let Some(origin) = origin_cluster {
            table_info_by_path.extend(origin.get_table_info_by_path().clone());
            table_id_by_path.extend(origin.get_table_id_by_path().clone());
        }

        let mut table_locations = HashMap::new();

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

            let mut locations = vec![];
            // now, get bucket matadata
            for bucket_metadata in table_metadata.bucket_metadata {
                let bucket_id = bucket_metadata.bucket_id;
                // todo: handle if leader not avaiable
                let leader_id = bucket_metadata.leader_id.unwrap();
                // what if server can't found?
                let leader_server_node = servers.get(&leader_id).unwrap().clone();
                let bucket = TableBucket::new(table_id, bucket_id);
                let bucket_location = BucketLocation::new(bucket, leader_server_node);
                locations.push(bucket_location);
            }
            table_locations.insert(table_path, locations);
        }
        Cluster::new(
            coordinator_server,
            servers,
            table_locations,
            table_id_by_path,
            table_info_by_path,
        )
    }
}

pub struct MetadataUpdater {
    connections: Arc<Mutex<Connections>>,
    cluster: RwLock<Arc<Cluster>>,
}

impl MetadataUpdater {
    pub async fn new(con_config: ConnectionConfig, connections: Arc<Mutex<Connections>>) -> Self {
        let custer = Self::init_cluster(con_config, connections.clone())
            .await
            .unwrap();
        MetadataUpdater {
            connections: connections.clone(),
            cluster: RwLock::new(Arc::new(custer)),
        }
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        self.cluster.read().unwrap().clone()
    }

    pub fn get_coordinator_server(&self) -> Option<ServerNode> {
        self.cluster.read().unwrap().get_coordinator_server()
    }

    pub fn get_tableinfo_or_throw(&self, tabe_path: &TablePath) -> TableInfo {
        self.cluster
            .read()
            .unwrap()
            .get_table(tabe_path)
            .unwrap()
            .clone()
    }

    pub fn get_bucket_localtion(&self, table_bucket: &TableBucket) -> BucketLocation {
        self.cluster
            .read()
            .unwrap()
            .available_locations_by_bucket
            .get(table_bucket)
            .unwrap()
            .clone()
    }

    pub fn get_server_node(&self, server_id: i32) -> ServerNode {
        self.cluster
            .read()
            .unwrap()
            .alive_tablet_servers_by_id
            .get(&server_id)
            .unwrap()
            .clone()
    }

    pub async fn check_and_update_table_metadta(&self, table_paths: &[TablePath]) {
        let cluster_binding = self.cluster.read().unwrap();
        let need_update_table_paths: Vec<&TablePath> = table_paths
            .iter()
            .filter(|table_path| cluster_binding.get_table(table_path).is_none())
            .collect();
        if !need_update_table_paths.is_empty() {
            let _ = self.update_table_metadata(table_paths).await;
        }
    }

    pub async fn update_cluster(&self, new_cluster: Cluster) {
        let mut cluster = self.cluster.write().unwrap();
        *cluster = Arc::new(new_cluster);
    }

    async fn init_cluster(
        con_config: ConnectionConfig,
        connections: Arc<Mutex<Connections>>,
    ) -> Result<Cluster> {
        let bootstrap_server = con_config.bootstrap_server;
        let socker_addrss = bootstrap_server.parse::<SocketAddr>().unwrap();
        let server_node = ServerNode::new(
            -1,
            socker_addrss.ip().to_string(),
            socker_addrss.port() as u32,
            ServerType::CoordinatorServer,
        );
        let mut connections_guard = connections.lock().unwrap();
        let con = connections_guard.get_conn(&server_node).await.unwrap();
        let request = build_metadata_request(&vec![]);
        let metadata_response = con
            .send_recieve::<messages::MetadataRequest, messages::MetadataResponse>(request)
            .await?;
        Ok(Cluster::from_metadata_response(metadata_response, None))
    }

    pub async fn update_table_metadata(&self, table_paths: &[TablePath]) -> Result<()> {
        let server = self.cluster.read().unwrap().get_one_avaiable_server();
        let origin_cluster = self.get_cluster();
        let mut connections_guard = self.connections.lock().unwrap();
        let con = connections_guard.get_conn(&server).await.unwrap();
        let request = build_metadata_request(table_paths);
        let metadata_response = con
            .send_recieve::<messages::MetadataRequest, messages::MetadataResponse>(request)
            .await?;
        self.update_cluster(Cluster::from_metadata_response(
            metadata_response,
            Some(&origin_cluster),
        ))
        .await;
        Ok(())
    }
}

#[derive(Debug, Clone)]

pub struct BucketLocation {
    table_bucket: TableBucket,
    leader: ServerNode,
}

impl BucketLocation {
    pub fn new(table_bucket: TableBucket, leader: ServerNode) -> BucketLocation {
        BucketLocation {
            table_bucket,
            leader,
        }
    }

    pub fn leader(&self) -> &ServerNode {
        &self.leader
    }

    pub fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }
}
