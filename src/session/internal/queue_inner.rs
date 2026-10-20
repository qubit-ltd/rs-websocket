// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use tokio::sync::Notify;

use super::envelope::Envelope;

/// Shared queue state and capacity limits used by bounded queue handles.
pub(super) struct QueueInner {
    /// Configured queue-count bound.
    pub(super) max_messages: usize,
    /// Configured byte bound, also used to reject oversized messages.
    pub(super) max_bytes: usize,
    /// FIFO storage and both budgets, updated under one mutex.
    pub(super) state: Mutex<QueueState>,
    /// Wakes receivers after a message is enqueued or the queue is closed.
    pub(super) readable: Notify,
    /// Wakes senders after capacity is released or the queue is closed.
    pub(super) writable: Notify,
}

/// Mutable queue contents, lifecycle, and atomically reserved capacity.
pub(super) struct QueueState {
    /// FIFO storage for admitted message envelopes.
    pub(super) items: VecDeque<Envelope>,
    /// Number of outstanding reservations, including active writes.
    pub(super) used_messages: usize,
    /// Payload bytes held by outstanding reservations.
    pub(super) used_bytes: usize,
    /// Whether shutdown has rejected future reservations.
    pub(super) closed: bool,
}

/// Capacity reserved for one message until its envelope is dropped.
pub(super) struct Reservation {
    inner: Weak<QueueInner>,
    bytes: usize,
}

impl Reservation {
    /// Creates a reservation after the caller updates both budgets under lock.
    pub(super) fn new(inner: Arc<QueueInner>, bytes: usize) -> Self {
        Self {
            inner: Arc::downgrade(&inner),
            bytes,
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut state = inner.state.lock().expect("queue mutex poisoned");
            state.used_messages -= 1;
            state.used_bytes -= self.bytes;
            drop(state);
            inner.writable.notify_waiters();
        }
    }
}
