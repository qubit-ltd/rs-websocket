// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

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
    /// Monotonic waiter identifier used by cancellation guards.
    pub(super) next_waiter_id: AtomicUsize,
}

/// One capacity waiter registered with this queue.
pub(super) struct Waiter {
    pub(super) id: usize,
    pub(super) bytes: usize,
    pub(super) notify: std::sync::Arc<Notify>,
    pub(super) notified: bool,
}

/// RAII registration that removes a cancelled or completed waiter.
pub(super) struct WaiterRegistration {
    inner: Weak<QueueInner>,
    id: usize,
    notify: std::sync::Arc<Notify>,
}

impl QueueInner {
    pub(super) fn register_waiter(self: &std::sync::Arc<Self>, bytes: usize) -> WaiterRegistration {
        let id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let notify = std::sync::Arc::new(Notify::new());
        let mut state = self.state.lock().expect("queue mutex poisoned");
        state.waiters.push(Waiter {
            id,
            bytes,
            notify: notify.clone(),
            notified: false,
        });
        self.wake_capacity_waiter_locked(&mut state);
        drop(state);
        WaiterRegistration {
            inner: std::sync::Arc::downgrade(self),
            id,
            notify,
        }
    }

    pub(super) fn wake_capacity_waiter(&self) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        self.wake_capacity_waiter_locked(&mut state);
    }

    fn wake_capacity_waiter_locked(&self, state: &mut QueueState) {
        if state.closed {
            for waiter in &state.waiters {
                waiter.notify.notify_one();
            }
            return;
        }
        let free_messages = self.max_messages - state.used_messages;
        let free_bytes = self.max_bytes - state.used_bytes;
        if free_messages == 0 {
            return;
        }
        if let Some(waiter) = state
            .waiters
            .iter_mut()
            .find(|waiter| !waiter.notified && waiter.bytes <= free_bytes)
        {
            waiter.notified = true;
            waiter.notify.notify_one();
        }
    }

    fn reset_waiter(&self, id: usize) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        if let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == id) {
            waiter.notified = false;
        }
        self.wake_capacity_waiter_locked(&mut state);
    }

    fn remove_waiter(&self, id: usize) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        state.waiters.retain(|waiter| waiter.id != id);
        self.wake_capacity_waiter_locked(&mut state);
    }
}

impl WaiterRegistration {
    pub(super) fn notify(&self) -> &Notify {
        &self.notify
    }

    pub(super) fn reset(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.reset_waiter(self.id);
            inner.wake_capacity_waiter();
        }
    }
}

impl Drop for WaiterRegistration {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.remove_waiter(self.id);
        }
    }
}
