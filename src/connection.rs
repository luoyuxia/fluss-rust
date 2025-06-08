use std::{
    collections::HashMap,
    fmt::{self},
    io::{self, Read, Write},
    mem,
    net::{Shutdown, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use byteorder::{BigEndian, WriteBytesExt};
use prost::Message;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{self},
};
use tracing::{debug, trace};

use crate::{
    Error, Result,
    admin::admin::FlussAdmin,
    args::Args,
    metadata::{TablePath, metadata_updater::MetadataUpdater},
    rpc::FlussRequest,
    table::table::Table,
};

const RESPONSE_HEADER_LENGTH: i32 = 5;
const REQUEST_HEADER_LENGTH: i32 = 8;

const SUCCESS_RESPONSE: u8 = 0;
const ERROR_RESPONSE: u8 = 1;
const SERVER_FAILURE: u8 = 2;

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub bootstrap_server: String,
    pub rw_timeout: Duration,
}

impl ConnectionConfig {
    pub fn from_args(args: Args) -> Self {
        ConnectionConfig {
            bootstrap_server: args.bootstrap_server,
            rw_timeout: args.rw_timeout,
        }
    }
}

pub struct FlussConnection {
    pub connections: Arc<Mutex<Connections>>,
    pub metadata_updater: Arc<Mutex<MetadataUpdater>>,
    pub connection_config: ConnectionConfig,
}

impl<'a> FlussConnection {
    pub async fn new(connection_config: ConnectionConfig) -> Self {
        let conections_ref = Arc::new(Mutex::new(Connections::new(connection_config.clone())));
        let _metadata_updater =
            MetadataUpdater::new(connection_config.clone(), conections_ref.clone()).await;
        let metadata_updater = Arc::new(Mutex::new(_metadata_updater));
        FlussConnection {
            connections: conections_ref.clone(),
            metadata_updater: metadata_updater,
            connection_config: connection_config,
        }
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Table {
        Table::new(
            self.connections.clone(),
            self.metadata_updater.clone(),
            table_path,
        )
        .await
    }

    pub fn get_admin(&self) -> FlussAdmin {
        FlussAdmin::new(self.connections.clone(), self.metadata_updater.clone())
    }
}

pub struct Connections {
    // server uid -> connection
    conns: HashMap<String, Connection>,
    connection_config: ConnectionConfig,
}

impl Connections {
    pub fn new(connection_config: ConnectionConfig) -> Self {
        Connections {
            conns: HashMap::new(),
            connection_config: connection_config,
        }
    }

    pub async fn get_conn(&mut self, server_node: &ServerNode) -> Result<&mut Connection> {
        let server_uid = server_node.uid.as_str();
        if let Some(conn) = self.conns.get_mut(server_uid) {
            return Ok(unsafe { mem::transmute(conn) });
        }

        let con = Connection::new(server_node, Some(self.connection_config.rw_timeout)).await?;

        self.conns.insert(server_node.uid.to_string(), con);
        Ok(self.conns.get_mut(server_uid).unwrap())
    }

    pub async fn send_request<T: Message>(
        &mut self,
        server_node: &ServerNode,
        request: FlussRequest<T>,
    ) -> Result<()> {
        let con = self.get_conn(server_node).await?;
        con.send_request(request).await
    }

    pub async fn close(&mut self) -> Result<()> {
        for (_, conn) in self.conns.iter_mut() {
            conn.shutdown().await?;
        }
        Ok(())
    }
}

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
        let uid = match server_type {
            ServerType::TabletServer => format!("cs-{}", id),
            ServerType::CoordinatorServer => format!("ts-{}", id),
        };
        ServerNode {
            id,
            uid: uid,
            host,
            port,
            server_type,
        }
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

pub struct Connection {
    // a identifier for the connection to distinguish
    // between conntions to the same host
    uid: String,
    host: String,
    port: u32,
    // the (wrapped) tcp stream
    stream: FlussStream,
    request_id: i32,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Connection {{ uid :{}, host: \"{}\", port: {} }}",
            self.uid, self.host, self.port
        )
    }
}

pub enum FlussStream {
    Pain(TcpConnectionStream),
    // may add Ssl TcpStream
}

pub struct TcpConnectionStream {
    client_address: SocketAddr,
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl TcpConnectionStream {
    pub fn new(client_address: SocketAddr, stream: TcpStream) -> TcpConnectionStream {
        let (reader, writer) = stream.into_split();
        Self {
            client_address,
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

impl TcpConnectionStream {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read_exact(buf).await
    }

    async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf).await
    }

    async fn fluss(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.writer.shutdown().await
    }
}

impl FlussStream {
    fn get_ref(&self) -> &TcpConnectionStream {
        match *self {
            FlussStream::Pain(ref s) => s,
        }
    }

    fn get_mut_ref(&mut self) -> &mut TcpConnectionStream {
        match *self {
            FlussStream::Pain(ref mut s) => s,
        }
    }

    pub async fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match *self {
            FlussStream::Pain(ref mut s) => s.shutdown().await,
        }
    }
}

impl Connection {
    pub async fn send_request<T: Message>(&mut self, request: FlussRequest<T>) -> Result<()> {
        let request_id = self.request_id;
        self.request_id += 1;
        let mut buffer: Vec<u8> = Vec::with_capacity(4);
        let request_body = request.request;
        let request_header = request.header;
        encode_request(
            &request_body,
            request_header.api_key,
            request_header.api_version,
            request_id,
            &mut buffer,
        )?;
        self.send(&buffer).await?;
        self.flush().await
    }

    pub async fn send_recieve<T: Message, R: Message + Default>(
        &mut self,
        request: FlussRequest<T>,
    ) -> Result<R> {
        self.send_request(request).await?;
        self.get_response().await
    }

    pub async fn get_response<R: Message + Default>(&mut self) -> Result<R> {
        __get_response::<R>(self).await
    }

    pub async fn send(&mut self, msg: &[u8]) -> Result<()> {
        self.stream
            .get_mut_ref()
            .write(msg)
            .await
            .map_err(From::from)
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.stream.get_mut_ref().fluss().await.map_err(From::from)
    }

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        let r = (self.stream)
            .get_mut_ref()
            .read(buf)
            .await
            .map_err(From::from);
        trace!("Read {} bytes from: {:?} => {:?}", buf.len(), self, r);
        r
    }

    pub async fn read_exact_alloc(&mut self, size: u64) -> Result<Vec<u8>> {
        let mut buffer = vec![0; size as usize];
        self.read_exact(buffer.as_mut_slice()).await?;
        Ok(buffer)
    }

    async fn shutdown(&mut self) -> Result<()> {
        let r = self.stream.shutdown(Shutdown::Both).await;
        debug!("Shut down: {:?} => {:?}", self, r);
        r.map_err(From::from)
    }

    fn from_stream(
        stream: FlussStream,
        server_node: &ServerNode,
        rw_timeout: Option<Duration>,
    ) -> Result<Connection> {
        Ok(Connection {
            uid: server_node.uid.clone(),
            host: server_node.host.clone(),
            stream: stream,
            port: server_node.port,
            request_id: 0,
        })
    }

    async fn new(server_node: &ServerNode, rw_timeout: Option<Duration>) -> Result<Connection> {
        let host = format!("{}:{}", server_node.host, server_node.port);
        let stream = TcpStream::connect(host).await?;
        let stream = FlussStream::Pain(TcpConnectionStream::new(
            stream.local_addr().unwrap(),
            stream,
        ));
        Connection::from_stream(stream, server_node, rw_timeout)
    }
}

pub async fn get_connection(config: &ConnectionConfig) -> FlussConnection {
    FlussConnection::new(config.clone()).await
}

pub fn encode_request<M, W>(
    request: &M,
    api_key: i16,
    api_version: i16,
    request_id: i32,
    buf: &mut W,
) -> Result<()>
where
    M: Message,
    W: Write,
{
    let request_size = request.encoded_len();
    let frame_length = REQUEST_HEADER_LENGTH + (request_size as i32);

    // header
    buf.write_i32::<BigEndian>(frame_length)?;
    buf.write_i16::<BigEndian>(api_key)?;
    buf.write_i16::<BigEndian>(api_version)?;
    buf.write_i32::<BigEndian>(request_id)?;

    // payload
    // todo: it'll need to convert to bytes and then write
    // consider to make it more effcient
    let mut request_buf = Vec::new();
    request_buf.reserve(request.encoded_len());
    // Unwrap is safe, since we have reserved sufficient capacity in the vector.
    request.encode(&mut request_buf).unwrap();
    buf.write_all(request_buf.as_slice()).map_err(From::from)
}

async fn __get_response<T: Message + Default>(conn: &mut Connection) -> Result<T> {
    // read frame length
    let frame_lengh = __read_int(conn).await?;
    // read response type
    // todo: echk resp_type
    let resp_type = conn.read_exact_alloc(1).await?.get(0).unwrap().clone();
    // read request id;
    let request_id = __read_int(conn).await?;
    let message_size = frame_lengh - RESPONSE_HEADER_LENGTH;
    if message_size < 0 {
        todo!("may throw exception?")
    } else {
        let mut buf = vec![0; message_size as usize];
        conn.read_exact(&mut buf).await?;
        if resp_type == SUCCESS_RESPONSE {
            Message::decode(&buf[..]).map_err(|e| e.into())
        } else {
            // current, panic directly, but need to encode it to error code
            panic!("error in process response")
        }
    }
}

async fn __read_int(conn: &mut Connection) -> Result<i32> {
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf).await?;
    Ok(i32::from_be_bytes(buf))
}
