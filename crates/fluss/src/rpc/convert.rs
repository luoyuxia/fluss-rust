use crate::cluster::{ServerNode, ServerType};
use crate::metadata::TablePath;
use crate::proto::{PbServerNode, PbTablePath};

pub fn to_table_path(table_path: &TablePath) -> PbTablePath {
    PbTablePath {
        database_name: table_path.database().to_string(),
        table_name: table_path.table().to_string(),
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

pub fn from_pb_table_path(pb_table_path: &PbTablePath) -> TablePath {
    TablePath::new(
        pb_table_path.database_name.to_string(),
        pb_table_path.table_name.to_string(),
    )
}

