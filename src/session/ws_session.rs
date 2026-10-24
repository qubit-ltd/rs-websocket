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
use crate::options::WsSessionLimits;
use crate::session::internal::BoundedQueue;
use crate::session::internal::CloseRequest;
use crate::session::internal::DriverContext;
use crate::session::internal::Owner;
use crate::ws_message::WsMessage;

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
    /// Shared queues and lifecycle state owned by this session and its senders.
    pub(super) owner: Arc<Owner>,
    /// Whether the single terminal error has already been returned to the
    /// receiver.
    pub(super) error_delivered: bool,
    /// HTTP status code returned by the successful opening handshake.
    pub(super) response_status_code: u16,
    /// Subprotocol selected by the server, if negotiation succeeded.
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
    ///
    /// The queues use the configured message and payload-byte budgets. The
    /// driver context takes ownership of the remaining limits and lifecycle
    /// signals; clones of each queue share its capacity and close state.
    pub(crate) fn from_connected(connected: ConnectedSocket, limits: WsSessionLimits) -> Self {
        let response_status_code = connected.status_code;
        let selected_subprotocol = connected.selected_subprotocol;
        let close_timeout = limits.close_timeout;
        let inbound_messages = limits.inbound_messages;
        let inbound_bytes = limits.inbound_bytes;
        let outbound_messages = limits.outbound_messages;
        let outbound_bytes = limits.outbound_bytes;
        let inbound = BoundedQueue::new(inbound_messages, inbound_bytes);
        let outbound = BoundedQueue::new(outbound_messages, outbound_bytes);
        let terminal = Arc::new(Mutex::new(None));
        let terminal_changed = Arc::new(Notify::new());
        let (close, close_rx) = watch::channel(None);
        let task = tokio::spawn(super::internal::run(
            connected.stream,
            DriverContext {
                inbound: inbound.clone(),
                outbound: outbound.clone(),
                close: close_rx,
                close_tx: close.clone(),
                terminal: terminal.clone(),
                terminal_changed: terminal_changed.clone(),
                limits,
            },
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
    /// Returns the HTTP status code from the successful opening handshake.
    ///
    /// # Returns
    ///
    /// The status code received from the server, normally `101`.
    #[must_use]
    #[inline]
    pub fn response_status_code(&self) -> u16 {
        self.response_status_code
    }

    /// Returns the subprotocol selected by the server, if one was negotiated.
    ///
    /// # Returns
    ///
    /// The selected subprotocol borrowed from this session, or `None` when
    /// the server did not select one.
    #[inline]
    pub fn selected_subprotocol(&self) -> Option<&str> {
        self.selected_subprotocol.as_deref()
    }

    /// Receives the next application message or the single terminal error.
    ///
    /// Once queued messages are drained, a successful close returns `None`.
    /// A terminal error is returned once as `Some(Err(_))`, then subsequent
    /// calls return `None`. A peer close code is available through
    /// [`termination`](Self::termination), not as a receive error.
    /// [`WsErrorKind::InboundBackpressure`] means another application message
    /// arrived while one message was waiting for inbound capacity;
    /// [`WsErrorKind::Timeout`] means an operation deadline expired, including
    /// a no-progress wait with no extra application message.
    ///
    /// # Returns
    ///
    /// Returns the next message, the single terminal error, or `None` after a
    /// successful close or once the terminal error has already been delivered.
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
    ///
    /// # Returns
    ///
    /// A sender that shares this session's outbound queue and capacity limits.
    #[must_use]
    #[inline]
    pub fn sender(&self) -> WsSender {
        WsSender {
            owner: self.owner.clone(),
        }
    }

    /// Enqueues a message immediately when both outbound limits permit it.
    /// Capacity reflects admitted messages, including messages being written;
    /// pending `send()` calls do not reserve either budget.
    /// Shutdown takes priority; an oversized payload returns
    /// [`WsSendError::MessageTooLarge`] only while the session is open.
    ///
    /// # Parameters
    ///
    /// - `message`: application message to enqueue.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the message is queued; otherwise, the reason it could not
    /// be queued.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Closed`] after shutdown,
    /// [`WsSendError::MessageTooLarge`] for an oversized payload while open,
    /// or [`WsSendError::Backpressure`] when the outbound budget is full.
    pub fn try_send(&self, message: WsMessage) -> Result<(), WsSendError> {
        self.owner.outbound.try_push(message)
    }

    /// Waits until both outbound limits permit enqueueing the message.
    /// Pending calls do not reserve capacity. Concurrent waiters have no
    /// strict FIFO or starvation guarantee, and this future retains its
    /// payload while waiting.
    /// Shutdown takes priority; an oversized payload returns
    /// [`WsSendError::MessageTooLarge`] only while the session is open.
    ///
    /// # Parameters
    ///
    /// - `message`: application message to enqueue.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the message is queued; otherwise, the reason it could not
    /// be queued.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Closed`] after shutdown, or
    /// [`WsSendError::MessageTooLarge`] for an oversized payload while open.
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
    /// combined the read and write outcomes and both I/O futures have
    /// completed or been dropped. On successful completion, this call joins
    /// the driver before returning. If `close_timeout` expires, it requests
    /// driver abort and returns a `Timeout` without promising that the outer
    /// driver task has already been joined at that instant.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the close handshake completes, or an error category when
    /// the handshake fails or its deadline expires.
    ///
    /// # Errors
    ///
    /// Returns [`WsErrorKind::Timeout`] when the shared deadline expires,
    /// [`WsErrorKind::Internal`] when joining the driver fails, or the error
    /// reported by the driver.
    pub async fn close(&mut self) -> Result<(), WsError> {
        let _gate = self.owner.close_gate.lock().await;
        if let Some(result) = *self.owner.close_result.lock().expect("close result mutex poisoned") {
            return result.map_or(Ok(()), |kind| Err(WsError::new(kind)));
        }
        let _ = self.owner.close.send(Some(CloseRequest::Local));
        self.owner.outbound.close();
        let deadline = tokio::time::Instant::now() + self.owner.close_timeout;
        let termination = self.wait_for_termination(deadline).await;
        let result = self.join_driver_until(deadline, termination_error(termination)).await;
        *self.owner.close_result.lock().expect("close result mutex poisoned") = Some(result);
        result.map_or(Ok(()), |kind| Err(WsError::new(kind)))
    }

    /// Returns the immutable terminal snapshot once normal driver termination
    /// is published, or once a close timeout records an error after requesting
    /// driver abort. On normal termination, both I/O futures have completed or
    /// been dropped; a timeout does not promise that the outer task has already
    /// been joined.
    ///
    /// `Closed { code }` records the peer's raw close code, if present, and
    /// does not imply application-level success. `Error` reports a close,
    /// protocol, or transport failure. Inbound capacity overflow is reported
    /// as [`WsErrorKind::InboundBackpressure`]; expiry of a no-progress or
    /// close deadline is reported as [`WsErrorKind::Timeout`]. `None` means no
    /// terminal result has been recorded yet.
    ///
    /// # Returns
    ///
    /// The recorded terminal result, or `None` while the driver has not
    /// published one.
    #[inline]
    pub fn termination(&self) -> Option<WsTermination> {
        *self.owner.terminal.lock().expect("terminal mutex poisoned")
    }

    /// Waits for driver publication until the shared close deadline.
    ///
    /// Expiry aborts the driver and records `Timeout` only if another terminal
    /// result has not already won the race.
    async fn wait_for_termination(&self, deadline: tokio::time::Instant) -> WsTermination {
        loop {
            if let Some(termination) = *self.owner.terminal.lock().expect("terminal mutex poisoned") {
                return termination;
            }

            let terminal_changed = self.owner.terminal_changed.notified();
            tokio::pin!(terminal_changed);
            terminal_changed.as_mut().enable();
            if let Some(termination) = *self.owner.terminal.lock().expect("terminal mutex poisoned") {
                return termination;
            }

            match tokio::time::timeout_at(deadline, &mut terminal_changed).await {
                Ok(()) => {}
                Err(_) => {
                    self.owner.abort.abort();
                    let mut terminal = self.owner.terminal.lock().expect("terminal mutex poisoned");
                    if terminal.is_none() {
                        let termination = WsTermination::Error {
                            kind: WsErrorKind::Timeout,
                        };
                        *terminal = Some(termination);
                    }
                    drop(terminal);
                    self.owner.terminal_changed.notify_waiters();
                    return WsTermination::Error {
                        kind: WsErrorKind::Timeout,
                    };
                }
            }
        }
    }

    /// Joins the driver within the same deadline used for the close handshake.
    ///
    /// A join failure is `Internal` unless a previous close error already
    /// takes precedence; a join timeout aborts the driver and records timeout.
    async fn join_driver_until(
        &self,
        deadline: tokio::time::Instant,
        mut result: Option<WsErrorKind>,
    ) -> Option<WsErrorKind> {
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
        result
    }

    /// Records an error termination and wakes waiters if the driver has not
    /// already published a terminal result.
    fn set_terminal_error_if_empty(&self, kind: WsErrorKind) {
        let mut terminal = self.owner.terminal.lock().expect("terminal mutex poisoned");
        if terminal.is_none() {
            *terminal = Some(WsTermination::Error { kind });
            self.owner.terminal_changed.notify_waiters();
        }
    }
}

/// Extracts the error category from a terminal result, if it represents an
/// error rather than a completed close.
fn termination_error(termination: WsTermination) -> Option<WsErrorKind> {
    match termination {
        WsTermination::Error { kind } => Some(kind),
        WsTermination::Closed { .. } => None,
    }
}
