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

const WAITER_INDEX_THRESHOLD: usize = 128;
const WAITER_INDEX_DEACTIVATION_THRESHOLD: usize = 64;
const WAITER_FALLBACK_PREFIX_SCAN_LIMIT: usize = 8;

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
    /// Registration is coordinated under `state` but does not change capacity
    /// or select a waiter. Callers must enable the notification and retry the
    /// enqueue after registering; capacity release selects a fitting waiter.
    /// The waiter reserves no capacity until a later enqueue succeeds. The
    /// returned guard holds only a weak reference to this queue, so it does
    /// not extend the queue's lifetime.
    ///
    /// # Parameters
    ///
    /// - `bytes`: Logical payload bytes the pending message needs to reserve.
    pub(super) fn register_waiter(self: &Arc<Self>, bytes: usize) -> WaiterRegistration {
        let id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let notify = Arc::new(Notify::new());
        let mut state = self.state.lock().expect("queue mutex poisoned");
        state.waiters.insert(
            id,
            Waiter {
                bytes,
                notify: notify.clone(),
                notified: false,
            },
        );
        if state.waiter_index_active {
            state.index_waiter(id, bytes);
        } else if state.waiters.len() > WAITER_INDEX_THRESHOLD {
            let entries = state
                .waiters
                .iter()
                .filter(|(_, waiter)| !waiter.notified)
                .map(|(&waiter_id, waiter)| (waiter_id, waiter.bytes))
                .collect::<Vec<_>>();
            for (waiter_id, waiter_bytes) in entries {
                state.index_waiter(waiter_id, waiter_bytes);
            }
            state.waiter_index_active = true;
        }
        drop(state);
        WaiterRegistration::new(Arc::downgrade(self), id, notify)
    }

    /// Selects and wakes a capacity waiter after a queue state change.
    ///
    /// For an open queue, at most one unnotified waiter whose byte request and
    /// one-message request fit the current budgets is selected. A notification
    /// does not reserve capacity, which another sender may consume first. On
    /// closed queues, the close transition already notified registered
    /// waiters. Selection follows waiter ID order but promises neither strict
    /// FIFO admission nor starvation freedom. This method acquires `state`.
    pub(super) fn wake_capacity_waiter(&self) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        self.wake_capacity_waiter_locked(&mut state);
    }

    /// Selects a waiter using state already protected by this queue's mutex.
    ///
    /// Callers must hold the lock for this `QueueInner`'s `state`. Open queues
    /// notify at most one unnotified waiter that fits both current budgets.
    /// Closed queues were broadcast at the close transition. A notification
    /// is not a reservation.
    ///
    /// # Parameters
    ///
    /// - `state`: Queue state protected by this queue's mutex.
    fn wake_capacity_waiter_locked(&self, state: &mut QueueState) {
        if state.closed {
            return;
        }
        let free_messages = self.max_messages - state.used_messages;
        let free_bytes = self.max_bytes - state.used_bytes;
        if free_messages == 0 {
            return;
        }
        if !state.waiter_index_active {
            if let Some(waiter) = state
                .waiters
                .values_mut()
                .find(|waiter| !waiter.notified && waiter.bytes <= free_bytes)
            {
                waiter.notified = true;
                waiter.notify.notify_one();
            }
            return;
        }
        let indexed_fallback = || {
            state
                .waiters_by_bytes
                .range(..=free_bytes)
                .filter_map(|(_, ids)| ids.first().copied())
                .min()
                .map(|id| {
                    let waiter = state
                        .waiters
                        .get(&id)
                        .expect("indexed waiter must be registered");
                    (id, waiter.bytes)
                })
        };
        let waiter_id = if state.waiters.len() <= WAITER_INDEX_THRESHOLD {
            state
                .waiters
                .iter()
                .find(|(_, waiter)| !waiter.notified && waiter.bytes <= free_bytes)
                .map(|(&id, waiter)| (id, waiter.bytes))
        } else if free_bytes > 0 {
            state
                .waiters
                .iter()
                .take(WAITER_FALLBACK_PREFIX_SCAN_LIMIT)
                .find(|(_, waiter)| !waiter.notified && waiter.bytes <= free_bytes)
                .map(|(&id, waiter)| (id, waiter.bytes))
                .or_else(indexed_fallback)
        } else {
            indexed_fallback()
        };
        if let Some((waiter_id, bytes)) = waiter_id {
            state.unindex_waiter(waiter_id, bytes);
            let waiter = state
                .waiters
                .get_mut(&waiter_id)
                .expect("indexed waiter must be registered");
            waiter.notified = true;
            waiter.notify.notify_one();
        }
    }

    /// Clears a waiter's selection marker and coordinates another selection.
    ///
    /// The caller holds no `state` lock; this method acquires it and then
    /// invokes the locked selector before releasing it.
    ///
    /// # Parameters
    ///
    /// - `id`: Registered waiter whose selection marker is reset.
    pub(super) fn reset_waiter(&self, id: usize) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        let (was_notified, bytes) = if let Some(waiter) = state.waiters.get_mut(&id) {
            let was_notified = waiter.notified;
            waiter.notified = false;
            (was_notified, waiter.bytes)
        } else {
            (false, 0)
        };
        if was_notified {
            if state.waiter_index_active {
                state.index_waiter(id, bytes);
            }
            self.wake_capacity_waiter_locked(&mut state);
        }
    }

    /// Removes a waiter's registration and coordinates another selection.
    ///
    /// The caller holds no `state` lock; this method acquires it and then
    /// invokes the locked selector before releasing it.
    ///
    /// # Parameters
    ///
    /// - `id`: Registered waiter whose entry is removed.
    pub(super) fn remove_waiter(&self, id: usize) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        let removed = state.waiters.remove(&id);
        let was_notified = removed.as_ref().is_some_and(|waiter| waiter.notified);
        let mut retired_index = None;
        if state.waiter_index_active {
            if state.waiters.len() <= WAITER_INDEX_DEACTIVATION_THRESHOLD {
                retired_index = Some(std::mem::take(&mut state.waiters_by_bytes));
                state.waiter_index_active = false;
            } else if let Some(waiter) = removed.filter(|waiter| !waiter.notified) {
                state.unindex_waiter(id, waiter.bytes);
            }
        }
        if was_notified {
            self.wake_capacity_waiter_locked(&mut state);
        }
        drop(state);
        // Retiring a large index can free many tree nodes. Keep that work
        // outside the queue lock so other senders are not blocked by cleanup.
        drop(retired_index);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::Notify;

    use super::super::queue_state::QueueState;
    use super::super::waiter::Waiter;
    use super::QueueInner;

    fn indexed_inner(max_bytes: usize, entries: &[(usize, usize, bool)]) -> QueueInner {
        let mut state = QueueState {
            items: VecDeque::new(),
            used_messages: 0,
            used_bytes: 0,
            closed: false,
            waiters: BTreeMap::new(),
            waiters_by_bytes: BTreeMap::new(),
            waiter_index_active: true,
        };
        for &(id, bytes, notified) in entries {
            state.waiters.insert(
                id,
                Waiter {
                    bytes,
                    notify: Arc::new(Notify::new()),
                    notified,
                },
            );
            if !notified {
                state.index_waiter(id, bytes);
            }
        }
        QueueInner {
            max_messages: 1,
            max_bytes,
            state: Mutex::new(state),
            readable: Notify::new(),
            next_waiter_id: AtomicUsize::new(entries.len()),
        }
    }

    #[test]
    fn indexed_fallback_after_notified_prefix_selects_minimum_fitting_id() {
        let mut entries = (0..8).map(|id| (id, 1, true)).collect::<Vec<_>>();
        entries.push((8, 9, false));
        entries.push((9, 8, false));
        entries.push((10, 8, false));
        entries.extend((11..129).map(|id| (id, 9, false)));
        let inner = indexed_inner(8, &entries);
        let mut state = inner.state.lock().unwrap();

        inner.wake_capacity_waiter_locked(&mut state);

        assert!(state.waiters[&9].notified);
        assert!(!state.waiters[&10].notified);
    }

    #[test]
    fn indexed_fallback_selects_zero_byte_waiter_at_zero_free_bytes() {
        let mut entries = (0..128).map(|id| (id, 0, true)).collect::<Vec<_>>();
        entries.push((128, 0, false));
        let inner = indexed_inner(0, &entries);
        let mut state = inner.state.lock().unwrap();

        inner.wake_capacity_waiter_locked(&mut state);

        assert!(state.waiters[&128].notified);
    }
}
