// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Weak;

use tokio::sync::Notify;

use super::queue_inner::QueueInner;

/// RAII registration that removes a cancelled or completed capacity waiter.
///
/// The weak queue reference avoids extending `QueueInner`'s lifetime. Dropping
/// the waiting future also drops its pending message; this guard removes the
/// waiter and gives another eligible waiter a chance to be selected.
pub(super) struct WaiterRegistration {
    /// Weak link to queue state, so the guard cannot keep the queue alive.
    inner: Weak<QueueInner>,
    /// Identifier of the entry this guard removes or resets.
    id: usize,
    /// Independent notification primitive for this waiter.
    notify: Arc<Notify>,
}

impl WaiterRegistration {
    /// Creates a guard for an already registered capacity waiter.
    pub(super) fn new(inner: Weak<QueueInner>, id: usize, notify: Arc<Notify>) -> Self {
        Self { inner, id, notify }
    }

    /// Returns this waiter's independent notification primitive.
    pub(super) fn notify(&self) -> &Notify {
        &self.notify
    }

    /// Clears this waiter's selection state and coordinates capacity wakeup.
    ///
    /// Both coordination calls are retained: resetting the marker lets this
    /// waiter be selected again, and the explicit wake pass rechecks current
    /// capacity after the preceding operation. The queue weak reference may no
    /// longer upgrade if all queue handles have been dropped.
    pub(super) fn reset(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.reset_waiter(self.id);
            inner.wake_capacity_waiter();
        }
    }
}

impl Drop for WaiterRegistration {
    /// Removes this registration when its future completes or is cancelled.
    ///
    /// If the queue still exists, removal lets the queue reconsider its other
    /// capacity waiters. A dropped queue has no registration state to update.
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.remove_waiter(self.id);
        }
    }
}
