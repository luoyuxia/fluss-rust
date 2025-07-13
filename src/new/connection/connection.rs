use crate::new::connection::transport::Transport;
use crate::new::error::Error::ConnectionError;
use crate::new::error::Result;
use crate::new::messenger::Messenger;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::BufStream;
use tracing::info;

pub type MessengerTransport = Messenger<BufStream<Transport>>;

pub type ServerConnection = Arc<MessengerTransport>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServerType {
    TabletServer,
    CoordinatorServer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerNode {
    pub id: i32,
    pub uid: String,
    host: String,
    port: u32,
    server_type: ServerType,
}

impl ServerNode {
    pub fn url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

pub enum ServerRepresentation {
    Bootstrap(String),

    /// Server received from the cluster server topology
    Cluster(ServerNode),
}

impl ServerRepresentation {
    fn id(&self) -> Option<i32> {
        match self {
            Self::Bootstrap(_) => None,
            Self::Cluster(server) => Some(server.id),
        }
    }

    fn url(&self) -> String {
        match self {
            Self::Bootstrap(inner) => inner.clone(),
            Self::Cluster(server) => server.url(),
        }
    }
}

impl ServerRepresentation {
    pub async fn connect(
        &self,
        client_id: Arc<str>,
        timeout: Option<Duration>,
        max_message_size: usize,
    ) -> Result<ServerConnection> {
        let url = self.url();
        info!(
            broker = self.id(),
            url = url.as_str(),
            "Establishing new connection",
        );

        let transport = Transport::connect(&url, timeout)
            .await
            .map_err(|error| ConnectionError(error.to_string()))?;

        let messenger = Messenger::new(BufStream::new(transport), max_message_size, client_id);
        Ok(ServerConnection::new(messenger))
    }
}

#[derive(Debug, Default)]
pub struct Connections {
    connections: RwLock<HashMap<String, ServerConnection>>,
    client_id: Arc<str>,
    timeout: Option<Duration>,
    max_message_size: usize,
}

impl Connections {
    pub fn new() -> Self {
        Connections {
            connections: RwLock::new(HashMap::new()),
            client_id: Arc::from(""),
            timeout: None,
            max_message_size: usize::MAX,
        }
    }

    pub async fn get_connection(&self, server_node: &ServerNode) -> Result<ServerConnection> {
        let server_id = &server_node.uid;
        {
            let connections = self.connections.read();
            if let Some(connection) = connections.get(server_id) {
                return Ok(connection.clone());
            }
        }

        //
        let server_connection = ServerRepresentation::Cluster(server_node.clone())
            .connect(self.client_id.clone(), self.timeout, self.max_message_size)
            .await?;
        self.connections
            .write()
            .insert(server_id.clone(), server_connection.clone());
        Ok(server_connection)
    }
}
