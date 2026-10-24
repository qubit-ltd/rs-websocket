// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::Notify;
use tokio::sync::watch;

use super::bounded_queue::BoundedQueue;
use super::close_request::CloseRequest;
use crate::error::WsTermination;
use crate::options::WsSessionLimits;

/// Owns the queues and lifecycle signals shared by the socket driver.
///
/// The limits travel with the session context so the socket entry points do
/// not rely on positional arguments to keep independent budgets aligned.
pub(in crate::session) struct DriverContext {
    /// Inbound application queue shared with the receiving session.
    pub(in crate::session) inbound: BoundedQueue,
    /// Outbound application queue shared with session senders.
    pub(in crate::session) outbound: BoundedQueue,
    /// Receives the first local or slow-consumer close request.
    pub(in crate::session) close: watch::Receiver<Option<CloseRequest>>,
    /// Publishes close requests for the reader and writer loops.
    pub(in crate::session) close_tx: watch::Sender<Option<CloseRequest>>,
    /// Stores the single terminal result selected by the driver.
    pub(in crate::session) terminal: Arc<Mutex<Option<WsTermination>>>,
    /// Wakes session operations waiting for terminal publication.
    pub(in crate::session) terminal_changed: Arc<Notify>,
    /// Queue, frame, and lifecycle time limits applied to this connection.
    pub(in crate::session) limits: WsSessionLimits,
}
