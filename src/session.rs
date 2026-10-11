use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, Notify, watch};
use tokio::task::JoinHandle;

use crate::WsClient;
use crate::client::handshake::connect_socket;
use crate::error::{WsError, WsErrorKind, WsSendError, WsTermination};
use crate::message::WsMessage;
use crate::options::WsSessionLimits;
use crate::session::queue::BoundedQueue;

mod driver;
mod queue;

struct Owner {
    inbound: BoundedQueue,
    outbound: BoundedQueue,
    close: watch::Sender<bool>,
    terminal: Arc<Mutex<Option<WsTermination>>>,
    terminal_changed: Arc<Notify>,
    close_timeout: Duration,
    close_result: Mutex<Option<Option<WsErrorKind>>>,
    close_gate: AsyncMutex<()>,
    task: AsyncMutex<Option<JoinHandle<()>>>,
    abort: tokio::task::AbortHandle,
}

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.close.send(true);
        self.inbound.close();
        self.outbound.close();
        if let Some(task) = self.task.get_mut().take() {
            task.abort();
        }
    }
}

/// A connected WebSocket session with one receiving owner and cloneable senders.
pub struct WsSession {
    owner: Arc<Owner>,
    error_delivered: bool,
}

/// A cloneable handle for bounded asynchronous sends.
#[derive(Clone)]
pub struct WsSender {
    owner: Arc<Owner>,
}

impl WsClient {
    /// Opens a WebSocket using the client's default request headers.
    pub async fn connect(&self, url: &str) -> Result<WsSession, WsError> {
        self.connect_with(url, http::HeaderMap::new()).await
    }

    /// Opens a WebSocket with per-connection request headers.
    pub async fn connect_with(
        &self,
        url: &str,
        headers: http::HeaderMap,
    ) -> Result<WsSession, WsError> {
        let connected = connect_socket(url, &self.options, &headers).await?;
        let WsSessionLimits {
            inbound_messages,
            inbound_bytes,
            outbound_messages,
            outbound_bytes,
            no_progress_timeout,
            close_timeout,
            ..
        } = self.options.limits.clone();
        let inbound = BoundedQueue::new(inbound_messages, inbound_bytes);
        let outbound = BoundedQueue::new(outbound_messages, outbound_bytes);
        let terminal = Arc::new(Mutex::new(None));
        let terminal_changed = Arc::new(Notify::new());
        let (close, close_rx) = watch::channel(false);
        let task = tokio::spawn(driver::run(
            connected.stream,
            inbound.clone(),
            outbound.clone(),
            close_rx,
            terminal.clone(),
            terminal_changed.clone(),
            no_progress_timeout,
            close_timeout,
        ));
        let abort = task.abort_handle();
        Ok(WsSession {
            owner: Arc::new(Owner {
                inbound,
                outbound,
                close,
                terminal,
                terminal_changed,
                close_timeout,
                close_result: Mutex::new(None),
                close_gate: AsyncMutex::new(()),
                task: AsyncMutex::new(Some(task)),
                abort,
            }),
            error_delivered: false,
        })
    }
}

impl WsSession {
    /// Receives the next application message or the single terminal error.
    pub async fn receive(&mut self) -> Option<Result<WsMessage, WsError>> {
        loop {
            let terminal_changed = self.owner.terminal_changed.notified();
            tokio::pin!(terminal_changed);
            terminal_changed.as_mut().enable();
            if self.owner.inbound.usage().0 > 0 {
                if let Some(envelope) = self.owner.inbound.pop().await {
                    return Some(Ok(envelope.into_item()));
                }
            }
            let terminal = *self.owner.terminal.lock().expect("terminal mutex poisoned");
            if terminal.is_some() {
                if self.error_delivered {
                    return None;
                }
                self.error_delivered = true;
                return match terminal {
                    Some(WsTermination::Error { kind }) => Some(Err(WsError::new(kind))),
                    _ => None,
                };
            }
            tokio::select! {
                envelope = self.owner.inbound.pop() => {
                    if let Some(envelope) = envelope {
                        return Some(Ok(envelope.into_item()));
                    }
                }
                _ = &mut terminal_changed => {}
            }
        }
    }

    /// Returns a cloneable sender sharing this session's outbound budget.
    pub fn sender(&self) -> WsSender {
        WsSender {
            owner: self.owner.clone(),
        }
    }

    /// Enqueues a message immediately when both outbound limits permit it.
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    pub async fn send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.push(message).await
    }

    /// Performs a bounded close handshake; repeated calls are harmless.
    pub async fn close(&mut self) -> Result<(), WsError> {
        let _gate = self.owner.close_gate.lock().await;
        if let Some(result) = *self
            .owner
            .close_result
            .lock()
            .expect("close result mutex poisoned")
        {
            return result.map_or(Ok(()), |kind| Err(WsError::new(kind)));
        }
        let _ = self.owner.close.send(true);
        let join = self.owner.task.lock().await.take();
        let result = match join {
            None => Some(WsErrorKind::Internal),
            Some(join) => match tokio::time::timeout(self.owner.close_timeout, join).await {
                Ok(Ok(())) => match *self.owner.terminal.lock().expect("terminal mutex poisoned") {
                    Some(WsTermination::Error { kind }) => Some(kind),
                    _ => None,
                },
                Ok(Err(_)) => Some(WsErrorKind::Internal),
                Err(_) => {
                    self.owner.abort.abort();
                    Some(WsErrorKind::Timeout)
                }
            },
        };
        *self
            .owner
            .close_result
            .lock()
            .expect("close result mutex poisoned") = Some(result);
        result.map_or(Ok(()), |kind| Err(WsError::new(kind)))
    }

    /// Returns the immutable terminal snapshot, once the driver has stopped.
    pub fn termination(&self) -> Option<WsTermination> {
        *self.owner.terminal.lock().expect("terminal mutex poisoned")
    }
}

impl WsSender {
    /// Enqueues a message immediately when both outbound limits permit it.
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    pub async fn send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.push(message).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::{WsClient, WsMessage};
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn sends_and_receives_echo_before_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            if let Some(Ok(message)) = websocket.next().await {
                websocket.send(message).await.unwrap();
                let _ = websocket.next().await;
                let _ = websocket.flush().await;
            }
        });
        let client = WsClient::builder().build().unwrap();
        let mut session = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.connect(&format!("ws://{address}/")),
        )
        .await
        .unwrap()
        .unwrap();
        session.try_send(WsMessage::Text("hi".into())).unwrap();
        assert!(
            matches!(tokio::time::timeout(std::time::Duration::from_secs(2), session.receive()).await.unwrap(), Some(Ok(WsMessage::Text(text))) if text == "hi")
        );
        session.close().await.unwrap();
        assert!(session.receive().await.is_none());
        peer.abort();
    }
}
