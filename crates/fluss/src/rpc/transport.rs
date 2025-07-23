use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use crate::rpc::error::RpcError;

#[derive(Debug)]
pub enum Transport {
    Plain { inner: TcpStream },
}


impl AsyncRead for Transport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_read(cx, buf),
        }
    }
}


impl AsyncWrite for Transport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.deref_mut() {
            Self::Plain { inner } => Pin::new(inner).poll_shutdown(cx),
        }
    }
}


impl Transport {
    pub async fn connect(server: &str, timeout: Option<Duration>) -> Result<Self, RpcError> {
        let tcp_stream = Self::connect_timeout(server, timeout).await?;
        Ok(Transport::Plain { inner: tcp_stream })
    }

    async fn connect_timeout(host: &str, timeout: Option<Duration>) -> Result<TcpStream, RpcError> {
        match timeout {
            Some(timeout) => Ok(tokio::time::timeout(timeout, TcpStream::connect(host))
                .await
                .map_err(|_| RpcError::ConnectionError(format!("Timeout connecting to host {host}")))??),
            None => Ok(TcpStream::connect(host).await?),
        }
    }
}