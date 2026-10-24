// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

use tokio::sync::Notify;

use super::queue_state::QueueState;

/// Shared queue state and capacity limits used by bounded queue handles.
pub(super) struct QueueInner {
    /// Maximum number of messages that may hold queue capacity at once.
    pub(super) max_messages: usize,
    /// Maximum reserved payload bytes, also limiting each message's size.
    pub(super) max_bytes: usize,
    /// FIFO storage and both budgets, updated under one mutex.
    pub(super) state: Mutex<QueueState>,
    /// Wakes receivers after a message is enqueued or the queue is closed.
    pub(super) readable: Notify,
    /// Wakes senders after capacity is released or the queue is closed.
    pub(super) writable: Notify,
}
