use crate::BucketId;
use crate::metadata::{TableBucket, TablePath};

mod cluster;

pub use cluster::Cluster;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerNode {
    id: i32,
    uid: String,
    host: String,
    port: u32,
    server_type: ServerType,
}

impl ServerNode {
    pub fn new(id: i32, host: String, port: u32, server_type: ServerType) -> ServerNode {
        ServerNode {
            id,
            uid: match server_type {
                ServerType::CoordinatorServer => format!("cs-{id}"),
                ServerType::TabletServer => format!("ts-{id}"),
            },
            host,
            port,
            server_type,
        }
    }

    pub fn uid(&self) -> &String {
        &self.uid
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn id(&self) -> i32 {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServerType {
    TabletServer,
    CoordinatorServer,
}

#[derive(Debug, Clone)]
pub struct BucketLocation {
    pub table_bucket: TableBucket,
    leader: Option<ServerNode>,
    pub table_path: TablePath,
}

impl BucketLocation {
    pub fn new(
        table_bucket: TableBucket,
        leader: Option<ServerNode>,
        table_path: TablePath,
    ) -> BucketLocation {
        BucketLocation {
            table_bucket,
            leader,
            table_path,
        }
    }

    pub fn leader(&self) -> &Option<ServerNode> {
        &self.leader
    }

    pub fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    pub fn bucket_id(&self) -> BucketId {
        self.table_bucket.bucket_id()
    }
}