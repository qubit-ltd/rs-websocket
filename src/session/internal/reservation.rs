// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Weak;

use super::queue_inner::QueueInner;

/// Capacity reserved for one message until its envelope is dropped.
#[must_use = "dropping the reservation releases queue capacity"]
pub(super) struct Reservation {
    /// Weak handle to the queue whose capacity this guard releases.
    inner: Weak<QueueInner>,
    /// Payload bytes included in the reservation.
    bytes: usize,
}

impl Reservation {
    /// Creates a reservation after the caller updates both budgets under lock.
    ///
    /// # Parameters
    ///
    /// - `inner`: Shared queue state whose reservation this guard owns.
    /// - `bytes`: Payload byte count already added to the queue's byte budget.
    ///
    /// # Returns
    ///
    /// A guard that releases the reserved message and byte capacity when
    /// dropped. Its weak reference does not keep the queue alive.
    #[inline]
    pub(super) fn new(inner: Arc<QueueInner>, bytes: usize) -> Self {
        Self {
            inner: Arc::downgrade(&inner),
            bytes,
        }
    }
}

impl Drop for Reservation {
    /// Releases this message's reserved capacity and wakes blocked senders.
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
