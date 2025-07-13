use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Transport {
    Plain { inner: TcpStream },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Invalid host-port string: {0}")]
    InvalidHostPort(String),

    #[error("Invalid port: {0}")]
    InvalidPort(#[from] std::num::ParseIntError),

    #[error("Connecting to broker timed out")]
    ConnectTimeout,
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
    pub async fn connect(server: &str, timeout: Option<Duration>) -> Result<Self> {
        let tcp_stream = Self::connect_timeout(server, timeout).await?;
        Ok(Transport::Plain { inner: tcp_stream })
    }

    async fn connect_timeout(host: &str, timeout: Option<Duration>) -> Result<TcpStream> {
        match timeout {
            Some(timeout) => Ok(tokio::time::timeout(timeout, TcpStream::connect(host))
                .await
                .map_err(|_| Error::ConnectTimeout)??),
            None => Ok(TcpStream::connect(host).await?),
        }
    }
}
