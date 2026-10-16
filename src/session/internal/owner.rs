// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::WsErrorKind;
use crate::error::WsTermination;
use crate::session::internal::BoundedQueue;
use crate::session::internal::CloseRequest;

/// Owns the shared queues, driver task, and close coordination for a session.
///
/// A session and its sender clones share this owner. The session controls its
/// lifetime; dropping the final owner remains a local-shutdown safeguard.
pub(crate) struct Owner {
    /// Bounded application messages received from the peer.
    pub(crate) inbound: BoundedQueue,
    /// Bounded application messages waiting to be sent to the peer.
    pub(crate) outbound: BoundedQueue,
    /// Publishes local or slow-consumer close requests to the driver.
    pub(crate) close: watch::Sender<Option<CloseRequest>>,
    /// The single terminal outcome recorded by the driver.
    pub(crate) terminal: Arc<Mutex<Option<WsTermination>>>,
    /// Wakes session operations waiting for the terminal outcome.
    pub(crate) terminal_changed: Arc<Notify>,
    /// Maximum time allowed to finish a protocol close exchange.
    pub(crate) close_timeout: Duration,
    /// Cached outcome of the first close attempt: `None` means no attempt has
    /// completed, `Some(None)` means success, and `Some(Some(kind))` stores
    /// the categorized failure returned by every later close call.
    pub(crate) close_result: Mutex<Option<Option<WsErrorKind>>>,
    /// Serializes repeated `close()` calls.
    pub(crate) close_gate: AsyncMutex<()>,
    /// Driver task joined by the first close caller.
    pub(crate) task: AsyncMutex<Option<JoinHandle<()>>>,
    /// Handle used to stop a driver whose close deadline expires.
    pub(crate) abort: tokio::task::AbortHandle,
}

impl Drop for Owner {
    /// Requests local shutdown and closes both queues when the final owner
    /// drops.
    fn drop(&mut self) {
        let _ = self.close.send(Some(CloseRequest::Local));
        self.outbound.close();
        self.inbound.close();
    }
}
