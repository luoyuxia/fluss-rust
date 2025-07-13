use crate::new::protocol::api_key::ApiKey;
use crate::new::protocol::api_version::ApiVersion;
use crate::new::protocol::message::header::{ReadVersionedError, RequestHeader, ResponseHeader};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::task::Poll;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, WriteHalf};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::new::protocol::frame::{AsyncMessageRead, AsyncMessageWrite, WriteError};
use crate::new::protocol::message::{ReadVersionedType, RequestBody, WriteVersionedType};

#[derive(Debug)]
struct Response {
    #[allow(dead_code)]
    header: ResponseHeader,
    data: Cursor<Vec<u8>>,
}

#[derive(Debug)]
struct ActiveRequest {
    channel: Sender<Result<Response, RequestError>>,
}

#[derive(Debug)]
enum MessengerState {
    /// Currently active requests by correlation ID.
    ///
    /// An active request is one that got prepared or send but the response wasn't received yet.
    RequestMap(HashMap<i32, ActiveRequest>),

    /// One or our streams died and we are unable to process any more requests.
    Poison(Arc<RequestError>),
}

impl MessengerState {
    fn poison(&mut self, err: RequestError) -> Arc<RequestError> {
        match self {
            Self::RequestMap(map) => {
                let err = Arc::new(err);

                // inform all active requests
                for (_request_id, active_request) in map.drain() {
                    // it's OK if the other side is gone
                    active_request
                        .channel
                        .send(Err(RequestError::Poisoned(Arc::clone(&err))))
                        .ok();
                }
                *self = Self::Poison(Arc::clone(&err));
                err
            }
            Self::Poison(e) => {
                // already poisoned, used existing error
                Arc::clone(e)
            }
        }
    }
}

/// A connection to a single server
///
/// Note: Requests to the same [`Messenger`] will be pipelined by Fluss
///

#[derive(Debug)]
pub struct Messenger<RW> {
    /// The half of the stream that we use to send data TO the broker.
    ///
    /// This will be used by [`request`](Self::request) to queue up messages.
    stream_write: Arc<AsyncMutex<WriteHalf<RW>>>,

    client_id: Arc<str>,

    request_id: AtomicI32,

    state: Arc<Mutex<MessengerState>>,

    join_handle: JoinHandle<()>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum WriteVersionedError {
    #[error("Write error: {0}")]
    WriteError(#[from] WriteError),

    #[error("Field {field} not available in version: {version:?}")]
    FieldNotAvailable { field: String, version: ApiVersion },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RequestError {
    #[error("Cannot read framed message: {0}")]
    ReadFramedMessageError(#[from] crate::new::protocol::frame::ReadError),

    #[error("Cannot read versioned data: {0}")]
    ReadVersionedError(#[from] ReadVersionedError),

    #[error("Cannot write data: {0}")]
    WriteError(#[from] WriteVersionedError),

    #[error("Cannot read/write data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Cannot write versioned data: {0}")]
    WriteMessageError(#[from] WriteError),

    #[error("Connection is poisoned: {0}")]
    Poisoned(Arc<RequestError>),

    #[error(
        "Data left at the end of the message. Got {message_size} bytes but only read {read} bytes. api_key={api_key:?} api_version={api_version}"
    )]
    TooMuchData {
        message_size: u64,
        read: u64,
        api_key: ApiKey,
        api_version: ApiVersion,
    },
}

impl<RW> Messenger<RW>
where
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    pub fn new(stream: RW, max_message_size: usize, client_id: Arc<str>) -> Self {
        let (stream_read, stream_write) = tokio::io::split(stream);
        let state = Arc::new(Mutex::new(MessengerState::RequestMap(HashMap::default())));
        let state_captured = Arc::clone(&state);
        let join_handle = tokio::spawn(async move {
            let mut stream_read = stream_read;
            loop {
                match stream_read.read_message(max_message_size).await {
                    Ok(msg) => {
                        // message was read, so all subsequent errors should not poison the whole stream
                        let mut cursor = Cursor::new(msg);

                        let mut header =
                            match ResponseHeader::read_versioned(&mut cursor, ApiVersion(0)) {
                                Ok(header) => header,
                                Err(e) => {
                                    warn!(%e, "Cannot read message header, ignoring message");
                                    continue;
                                }
                            };

                        let active_request = match state_captured.lock().deref_mut() {
                            MessengerState::RequestMap(map) => {
                                match map.remove(&header.request_id) {
                                    Some(active_request) => active_request,
                                    _ => {
                                        warn!(
                                            request_id = header.request_id,
                                            "Got response for unknown request",
                                        );
                                        continue;
                                    }
                                }
                            }
                            MessengerState::Poison(_) => {
                                // stream is poisoned, no need to anything
                                return;
                            }
                        };

                        // we don't care if the other side is gone
                        active_request
                            .channel
                            .send(Ok(Response {
                                header,
                                data: cursor,
                            }))
                            .ok();
                    }
                    Err(e) => {
                        state_captured
                            .lock()
                            .poison(RequestError::ReadFramedMessageError(e));
                        return;
                    }
                }
            }
        });

        Self {
            stream_write: Arc::new(AsyncMutex::new(stream_write)),
            client_id,
            request_id: AtomicI32::new(0),
            state,
            join_handle,
        }
    }

    async fn send_message(&self, msg: Vec<u8>) -> Result<(), RequestError> {
        match self.send_message_inner(msg).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // need to poison the stream because message framing might be out-of-sync
                let mut state = self.state.lock();
                Err(RequestError::Poisoned(state.poison(e)))
            }
        }
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, RequestError>
    where
        R: RequestBody + Send + WriteVersionedType<Vec<u8>>,
        R::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
    {
        let request_id = self.request_id.fetch_add(1, Ordering::SeqCst);

        let header = RequestHeader {
            request_api_key: R::API_KEY,
            request_api_version: ApiVersion(0),
            request_id: request_id,
            client_id: Some(String::from(self.client_id.as_ref())),
        };

        let header_version = ApiVersion(0);

        let body_api_version = ApiVersion(0);

        let mut buf = Vec::new();
        header
            .write_versioned(&mut buf, header_version)
            .expect("Writing header to buffer should always work");
        msg.write_versioned(&mut buf, body_api_version)?;

        let (tx, rx) = channel();

        // to prevent stale data in inner state, ensure that we would remove the request again if we are cancelled while
        // sending the request
        let cleanup_on_cancel =
            CleanupRequestStateOnCancel::new(Arc::clone(&self.state), request_id);

        match self.state.lock().deref_mut() {
            MessengerState::RequestMap(map) => {
                map.insert(request_id, ActiveRequest { channel: tx });
            }
            MessengerState::Poison(e) => {
                return Err(RequestError::Poisoned(Arc::clone(e)));
            }
        }

        self.send_message(buf).await?;
        cleanup_on_cancel.message_sent();
        let mut response = rx.await.expect("Who closed this channel?!")?;
        let body = R::ResponseBody::read_versioned(&mut response.data, body_api_version)?;

        let read_bytes = response.data.position();
        let message_bytes = response.data.into_inner().len() as u64;

        if read_bytes != message_bytes {
            return Err(RequestError::TooMuchData {
                message_size: message_bytes,
                read: read_bytes,
                api_key: R::API_KEY,
                api_version: body_api_version,
            });
        }

        Ok(body)
    }

    async fn send_message_inner(&self, msg: Vec<u8>) -> Result<(), RequestError> {
        let mut stream_write = Arc::clone(&self.stream_write).lock_owned().await;
        let fut = CancellationSafeFuture::new(async move {
            stream_write.write_message(&msg).await?;
            stream_write.flush().await?;
            Ok(())
        });

        fut.await
    }
}

impl<RW> Drop for Messenger<RW> {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

struct CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
{
    /// Mark if the inner future finished. If not, we must spawn a helper task on drop.
    done: bool,

    /// Inner future.
    ///
    /// Wrapped in an `Option` so we can extract it during drop. Inside that option however we also need a pinned
    /// box because once this wrapper is polled, it will be pinned in memory -- even during drop. Now the inner
    /// future does not necessarily implement `Unpin`, so we need a heap allocation to pin it in memory even when we
    /// move it out of this option.
    inner: Option<BoxFuture<'static, F::Output>>,
}

impl<F> CancellationSafeFuture<F>
where
    F: Future + Send,
{
    fn new(fut: F) -> Self {
        Self {
            done: false,
            inner: Some(Box::pin(fut)),
        }
    }
}

impl<F> Future for CancellationSafeFuture<F>
where
    F: Future + Send,
{
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.inner.as_mut().expect("no dropped").as_mut().poll(cx) {
            Poll::Ready(res) => {
                self.done = true;
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Helper that ensures that a request is removed when a request is cancelled before it was actually sent out.
struct CleanupRequestStateOnCancel {
    state: Arc<Mutex<MessengerState>>,
    request_id: i32,
    message_sent: bool,
}

impl CleanupRequestStateOnCancel {
    /// Create new helper.
    ///
    /// You must call [`message_sent`](Self::message_sent) when the request was sent.
    fn new(state: Arc<Mutex<MessengerState>>, request_id: i32) -> Self {
        Self {
            state,
            request_id,
            message_sent: false,
        }
    }

    /// Request was sent. Do NOT clean the state any longer.
    fn message_sent(mut self) {
        self.message_sent = true;
    }
}

impl Drop for CleanupRequestStateOnCancel {
    fn drop(&mut self) {
        if !self.message_sent {
            if let MessengerState::RequestMap(map) = self.state.lock().deref_mut() {
                map.remove(&self.request_id);
            }
        }
    }
}
