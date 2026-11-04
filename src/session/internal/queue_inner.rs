// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;

use super::queue_state::QueueState;
use super::waiter::Waiter;
use super::waiter_registration::WaiterRegistration;

/// Shared queue state and capacity limits used by bounded queue handles.
pub(super) struct QueueInner {
    /// Maximum number of messages that may hold queue capacity at once.
    pub(super) max_messages: usize,
    /// Maximum reserved payload bytes, also limiting each message's size.
    pub(super) max_bytes: usize,
    /// FIFO items, both capacity budgets, waiters, and waiter notification
    /// state. This mutex is the coordination boundary for those values; its
    /// poison state causes queue operations to panic.
    pub(super) state: Mutex<QueueState>,
    /// Wakes receivers after a message is enqueued or the queue is closed.
    pub(super) readable: Notify,
    /// Supplies identifiers for waiter entries. This counter identifies
    /// entries; it does not synchronize capacity or queue state.
    pub(super) next_waiter_id: AtomicUsize,
}

impl QueueInner {
    /// Registers a sender waiting for the specified logical payload bytes.
    ///
    /// Registration and capacity selection are coordinated under `state`.
    /// The waiter reserves no capacity until a later enqueue succeeds. The
    /// returned guard holds only a weak reference to this queue, so it does
    /// not extend the queue's lifetime.
    pub(super) fn register_waiter(self: &Arc<Self>, bytes: usize) -> WaiterRegistration {
        let id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let notify = Arc::new(Notify::new());
        let mut state = self.state.lock().expect("queue mutex poisoned");
        state.waiters.push(Waiter {
            id,
            bytes,
            notify: notify.clone(),
            notified: false,
        });
        self.wake_capacity_waiter_locked(&mut state);
        drop(state);
        WaiterRegistration::new(Arc::downgrade(self), id, notify)
    }

    /// Selects and wakes a capacity waiter after a queue state change.
    ///
    /// For an open queue, at most one unnotified waiter whose byte request and
    /// one-message request fit the current budgets is selected. A notification
    /// does not reserve capacity, which another sender may consume first. When
    /// closed, all registered waiters are notified. Selection order follows
    /// the current waiter collection order but promises neither strict FIFO
    /// admission nor starvation freedom. This method acquires `state` itself.
    pub(super) fn wake_capacity_waiter(&self) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        self.wake_capacity_waiter_locked(&mut state);
    }

    /// Selects a waiter using state already protected by this queue's mutex.
    ///
    /// Callers must hold the lock for this `QueueInner`'s `state`. Open queues
    /// notify at most one unnotified waiter that fits both current budgets;
    /// closed queues notify every waiter. A notification is not a reservation.
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

    /// Clears a waiter's selection marker and coordinates another selection.
    ///
    /// The caller holds no `state` lock; this method acquires it and then
    /// invokes the locked selector before releasing it.
    pub(super) fn reset_waiter(&self, id: usize) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        if let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == id) {
            waiter.notified = false;
        }
        self.wake_capacity_waiter_locked(&mut state);
    }

    /// Removes a waiter's registration and coordinates another selection.
    ///
    /// The caller holds no `state` lock; this method acquires it and then
    /// invokes the locked selector before releasing it.
    pub(super) fn remove_waiter(&self, id: usize) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        state.waiters.retain(|waiter| waiter.id != id);
        self.wake_capacity_waiter_locked(&mut state);
    }
}
