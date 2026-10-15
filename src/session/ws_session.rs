// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::watch;

use super::WsSender;
use crate::client::internal::ConnectedSocket;
use crate::error::WsError;
use crate::error::WsErrorKind;
use crate::error::WsSendError;
use crate::error::WsTermination;
use crate::message::WsMessage;
use crate::options::WsSessionLimits;
use crate::session::internal::BoundedQueue;
use crate::session::internal::CloseRequest;
use crate::session::internal::Owner;

/// A connected WebSocket session with one receiving owner and cloneable
/// senders. The receiver observes queued messages first, then at most one
/// terminal error; a completed close is observed as end of stream. The session
/// owns the lifecycle task, while clones of [`WsSender`] share its send budget.
/// Dropping the session requests bounded local shutdown and closes both queues,
/// even while sender clones remain alive. Subsequent sends return
/// [`WsSendError::Closed`]. Call [`close`](Self::close) before dropping the
/// session when the close result needs to be observed.
///
/// # Examples
///
/// ```no_run
/// use qubit_websocket::{WsClient, WsTermination};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let client = WsClient::builder().build()?;
///     let mut session = client.connect("ws://127.0.0.1:9000/events").await?;
///     while let Some(event) = session.receive().await {
///         println!("event: {event:?}");
///     }
///     if let Some(WsTermination::Closed { code }) = session.termination() {
///         println!("peer close code: {code:?}; apply application policy here");
///     }
///     Ok(())
/// }
/// ```
pub struct WsSession {
    pub(super) owner: Arc<Owner>,
    pub(super) error_delivered: bool,
    pub(super) response_status_code: u16,
    pub(super) selected_subprotocol: Option<String>,
}

impl Drop for WsSession {
    fn drop(&mut self) {
        let _ = self.owner.close.send(Some(CloseRequest::Local));
        self.owner.outbound.close();
        self.owner.inbound.close();
    }
}

impl WsSession {
    /// Creates a session and starts its protocol driver from an open socket.
    pub(crate) fn from_connected(connected: ConnectedSocket, limits: WsSessionLimits) -> Self {
        let response_status_code = connected.status_code;
        let selected_subprotocol = connected.selected_subprotocol;
        let WsSessionLimits {
            inbound_messages,
            inbound_bytes,
            outbound_messages,
            outbound_bytes,
            max_inbound_frame_bytes,
            max_inbound_message_bytes,
            no_progress_timeout,
            close_timeout,
            ..
        } = limits;
        let inbound = BoundedQueue::new(inbound_messages, inbound_bytes);
        let outbound = BoundedQueue::new(outbound_messages, outbound_bytes);
        let terminal = Arc::new(Mutex::new(None));
        let terminal_changed = Arc::new(Notify::new());
        let (close, close_rx) = watch::channel(None);
        let task = tokio::spawn(super::internal::run(
            connected.stream,
            inbound.clone(),
            outbound.clone(),
            close_rx,
            close.clone(),
            terminal.clone(),
            terminal_changed.clone(),
            no_progress_timeout,
            close_timeout,
            max_inbound_frame_bytes,
            max_inbound_message_bytes,
        ));
        let abort = task.abort_handle();
        Self {
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
            response_status_code,
            selected_subprotocol,
        }
    }
}

impl WsSession {
    /// Returns the successful opening handshake's HTTP status code.
    pub fn response_status_code(&self) -> u16 {
        self.response_status_code
    }

    /// Returns the subprotocol selected by the server, if one was negotiated.
    pub fn selected_subprotocol(&self) -> Option<&str> {
        self.selected_subprotocol.as_deref()
    }

    /// Receives the next application message or the single terminal error.
    ///
    /// Once queued messages are drained, a successful close returns `None`.
    /// A terminal error is returned once as `Some(Err(_))`, then subsequent
    /// calls return `None`. A peer close code is available through
    /// [`termination`](Self::termination), not as a receive error.
    pub async fn receive(&mut self) -> Option<Result<WsMessage, WsError>> {
        loop {
            if let Some(envelope) = self.owner.inbound.try_pop() {
                return Some(Ok(envelope.into_item()));
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
            let terminal_changed = self.owner.terminal_changed.notified();
            tokio::pin!(terminal_changed);
            terminal_changed.as_mut().enable();
            tokio::select! {
                biased;
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

    /// Performs a bounded close handshake; repeated calls return its saved
    /// result.
    ///
    /// `Ok(())` means the protocol close completed, including when the peer
    /// supplied a status such as `1009`; the application must interpret that
    /// code from [`termination`](Self::termination). A write failure, task
    /// failure, or expired close deadline returns a categorized [`WsError`].
    /// The local request is published before the outbound queue is closed.
    /// Closing that queue rejects senders and discards messages not yet sent;
    /// the writer then sends a WebSocket Close frame and waits for the peer's
    /// Close response. The driver publishes the terminal result after it has
    /// combined the read and write outcomes, and this call waits for driver
    /// completion within `close_timeout`.
    pub async fn close(&mut self) -> Result<(), WsError> {
        let _gate = self.owner.close_gate.lock().await;
        if let Some(result) = *self.owner.close_result.lock().expect("close result mutex poisoned") {
            return result.map_or(Ok(()), |kind| Err(WsError::new(kind)));
        }
        let _ = self.owner.close.send(Some(CloseRequest::Local));
        self.owner.outbound.close();
        let deadline = tokio::time::Instant::now() + self.owner.close_timeout;
        let mut result = loop {
            if let Some(termination) = *self.owner.terminal.lock().expect("terminal mutex poisoned") {
                break termination_error(termination);
            }

            let terminal_changed = self.owner.terminal_changed.notified();
            tokio::pin!(terminal_changed);
            terminal_changed.as_mut().enable();
            if let Some(termination) = *self.owner.terminal.lock().expect("terminal mutex poisoned") {
                break termination_error(termination);
            }

            match tokio::time::timeout_at(deadline, &mut terminal_changed).await {
                Ok(()) => {}
                Err(_) => {
                    self.owner.abort.abort();
                    let mut terminal = self.owner.terminal.lock().expect("terminal mutex poisoned");
                    if terminal.is_none() {
                        *terminal = Some(WsTermination::Error {
                            kind: WsErrorKind::Timeout,
                        });
                    }
                    self.owner.terminal_changed.notify_waiters();
                    break Some(WsErrorKind::Timeout);
                }
            }
        };

        let mut task = self.owner.task.lock().await;
        if let Some(task) = task.as_mut() {
            match tokio::time::timeout_at(deadline, task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    if result.is_none() {
                        result = Some(WsErrorKind::Internal);
                        *self.owner.terminal.lock().expect("terminal mutex poisoned") = Some(WsTermination::Error {
                            kind: WsErrorKind::Internal,
                        });
                    }
                }
                Err(_) => {
                    self.owner.abort.abort();
                    result = Some(WsErrorKind::Timeout);
                    self.set_terminal_error_if_empty(WsErrorKind::Timeout);
                }
            }
        }
        *self.owner.close_result.lock().expect("close result mutex poisoned") = Some(result);
        result.map_or(Ok(()), |kind| Err(WsError::new(kind)))
    }

    fn set_terminal_error_if_empty(&self, kind: WsErrorKind) {
        let mut terminal = self.owner.terminal.lock().expect("terminal mutex poisoned");
        if terminal.is_none() {
            *terminal = Some(WsTermination::Error { kind });
            self.owner.terminal_changed.notify_waiters();
        }
    }

    /// Returns the immutable terminal snapshot once the driver has stopped.
    ///
    /// `Closed { code }` records the peer's raw close code, if present, and
    /// does not imply application-level success. `Error` reports a close,
    /// protocol, or transport failure. `None` means no terminal result has
    /// been recorded yet.
    pub fn termination(&self) -> Option<WsTermination> {
        *self.owner.terminal.lock().expect("terminal mutex poisoned")
    }
}

fn termination_error(termination: WsTermination) -> Option<WsErrorKind> {
    match termination {
        WsTermination::Error { kind } => Some(kind),
        WsTermination::Closed { .. } => None,
    }
}
