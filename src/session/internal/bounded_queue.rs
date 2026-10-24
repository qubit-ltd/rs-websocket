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

use tokio::sync::Notify;

use super::envelope::Envelope;
use super::queue_inner::QueueInner;
use super::queue_state::QueueState;
use super::reservation::Reservation;
use crate::error::WsSendError;
use crate::ws_message::WsMessage;

/// A FIFO queue bounded by both message count and payload bytes.
///
/// Clones share one queue and its limits. Closing it rejects new pushes, wakes
/// blocked producers, and discards queued items, releasing their reservations.
#[derive(Clone)]
pub(crate) struct BoundedQueue {
    /// Shared message storage, capacity accounting, and queue notifications.
    inner: Arc<QueueInner>,
}

impl BoundedQueue {
    /// Creates a queue with independent message-count and byte limits.
    ///
    /// # Panics
    ///
    /// Panics if either limit is zero.
    #[must_use]
    pub(crate) fn new(max_messages: usize, max_bytes: usize) -> Self {
        assert!(max_messages > 0, "message capacity must be non-zero");
        assert!(max_bytes > 0, "byte capacity must be non-zero");

        Self {
            inner: Arc::new(QueueInner {
                max_messages,
                max_bytes,
                state: Mutex::new(QueueState {
                    items: VecDeque::new(),
                    used_messages: 0,
                    used_bytes: 0,
                    closed: false,
                }),
                readable: Notify::new(),
                writable: Notify::new(),
            }),
        }
    }

    /// Attempts to enqueue without waiting for capacity.
    ///
    /// Returns `Closed` after shutdown, `MessageTooLarge` when an open queue's
    /// payload exceeds the byte limit, or `Backpressure` when either budget is
    /// full.
    pub(crate) fn try_push(&self, item: WsMessage) -> Result<(), WsSendError> {
        let bytes = item.payload_len();
        if bytes > self.inner.max_bytes {
            return Err(self.closed_or(WsSendError::MessageTooLarge));
        }
        let mut item = Some(item);
        if !self.try_enqueue(&mut item)? {
            return Err(WsSendError::Backpressure);
        }
        self.inner.readable.notify_one();
        Ok(())
    }

    /// Waits for both budgets together and enqueues, or returns when the queue
    /// closes.
    ///
    /// Cancelling this future does not retain capacity unless a complete
    /// reservation was made, in which case its RAII guard releases it.
    pub(crate) async fn push(&self, item: WsMessage) -> Result<(), WsSendError> {
        let bytes = item.payload_len();
        if bytes > self.inner.max_bytes {
            return Err(self.closed_or(WsSendError::MessageTooLarge));
        }
        let mut item = Some(item);
        loop {
            let writable = self.inner.writable.notified();
            tokio::pin!(writable);
            writable.as_mut().enable();
            if self.try_enqueue(&mut item)? {
                self.inner.readable.notify_one();
                return Ok(());
            }
            writable.await;
        }
    }

    /// Removes the next message, waiting until one is available or the queue
    /// closes.
    pub(crate) async fn pop(&self) -> Option<Envelope> {
        loop {
            let readable = self.inner.readable.notified();
            tokio::pin!(readable);
            readable.as_mut().enable();

            let (item, closed) = {
                let mut state = self.inner.state.lock().expect("queue mutex poisoned");
                (state.items.pop_front(), state.closed)
            };
            if item.is_some() {
                return item;
            }
            if closed {
                return None;
            }
            readable.await;
        }
    }

    /// Removes an available message without waiting for queue activity.
    pub(crate) fn try_pop(&self) -> Option<Envelope> {
        self.inner.state.lock().expect("queue mutex poisoned").items.pop_front()
    }

    /// Closes the queue, discards pending messages, and wakes blocked senders
    /// and receivers. Repeated calls have no additional effect.
    pub(crate) fn close(&self) {
        let items = {
            let mut state = self.inner.state.lock().expect("queue mutex poisoned");
            if state.closed {
                return;
            }
            state.closed = true;
            std::mem::take(&mut state.items)
        };
        drop(items);
        self.inner.readable.notify_waiters();
        self.inner.writable.notify_waiters();
    }

    /// Returns the currently reserved message and byte counts.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn usage(&self) -> (usize, usize) {
        let state = self.inner.state.lock().expect("queue mutex poisoned");
        (state.used_messages, state.used_bytes)
    }

    /// Atomically checks both budgets, updates accounting, and enqueues the
    /// item.
    fn try_enqueue(&self, item: &mut Option<WsMessage>) -> Result<bool, WsSendError> {
        let bytes = item.as_ref().expect("item remains pending").payload_len();
        let mut state = self.inner.state.lock().expect("queue mutex poisoned");
        if state.closed {
            return Err(WsSendError::Closed);
        }
        if state.used_messages == self.inner.max_messages || bytes > self.inner.max_bytes - state.used_bytes {
            return Ok(false);
        }
        state.used_messages += 1;
        state.used_bytes += bytes;
        state.items.push_back(Envelope {
            item: item.take().expect("item remains pending"),
            _reservation: Reservation::new(self.inner.clone(), bytes),
        });
        Ok(true)
    }

    /// Returns `Closed` if shutdown is visible; otherwise returns `error`.
    fn closed_or(&self, error: WsSendError) -> WsSendError {
        if self.inner.state.lock().expect("queue mutex poisoned").closed {
            WsSendError::Closed
        } else {
            error
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::BoundedQueue;
    use crate::ws_message::WsMessage;

    #[test]
    fn test_queued_reservation_does_not_keep_queue_alive() {
        let queue = BoundedQueue::new(1, 1);
        let inner = Arc::downgrade(&queue.inner);
        queue.try_push(WsMessage::Text("x".to_owned())).expect("message fits");

        drop(queue);

        assert!(
            inner.upgrade().is_none(),
            "queued reservation must not keep the queue alive"
        );
    }
}
