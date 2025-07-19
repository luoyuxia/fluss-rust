use crate::messages::GetTableInfoResponse;
use crate::metadata::metadata_serde::JsonSerde;
use crate::metadata::{TableDescriptor, TableInfo, TablePath};
use crate::new::client::metadata::Metadata;
use crate::new::connection::connection::{Connections, ServerConnection};
use crate::new::error::Result;
use crate::new::protocol::message::create_table::CreateTableRequest;
use crate::new::protocol::message::get_table::GetTableRequest;
use std::sync::Arc;

pub struct FlussAdmin {
    admin_gateway: ServerConnection,
    metadata: Arc<Metadata>,
    connections: Arc<Connections>,
}

impl FlussAdmin {
    pub async fn new(connections: Arc<Connections>, metadata: Arc<Metadata>) -> Result<Self> {
        let admin_con = connections
            .get_connection(
                metadata
                    .get_cluster()
                    .get_coordinator_server()
                    .expect("Couldn't coordinator server"),
            )
            .await?;

        Ok(FlussAdmin {
            admin_gateway: admin_con,
            metadata,
            connections,
        })
    }

    pub async fn create_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let response = self
            .admin_gateway
            .request(CreateTableRequest::new(
                table_path,
                table_descriptor,
                ignore_if_exists,
            ))
            .await?;
        Ok(())
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<TableInfo> {
        let response = self
            .admin_gateway
            .request(GetTableRequest::new(table_path))
            .await?;
        let GetTableInfoResponse {
            table_id,
            schema_id,
            table_json,
            created_time,
            modified_time,
        } = response;
        let v: &[u8] = &table_json[..];
        let table_descriptor =
            TableDescriptor::deserialize_json(&serde_json::from_slice(v).unwrap());
        Ok(TableInfo::of(
            table_path.clone(),
            table_id,
            schema_id,
            table_descriptor,
            created_time,
            modified_time,
        ))
    }
}
