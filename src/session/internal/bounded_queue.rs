// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::Notify;

use super::envelope::Envelope;
use super::queue_inner::QueueInner;
use super::queue_state::QueueState;
use super::reservation::Reservation;
use crate::error::WsSendError;
use crate::error::WsSendFailure;
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
    /// # Parameters
    ///
    /// - `max_messages`: Maximum number of outstanding message reservations.
    /// - `max_bytes`: Maximum payload bytes held by outstanding reservations.
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
                    waiters: BTreeMap::new(),
                    waiters_by_bytes: BTreeMap::new(),
                    waiter_index_active: false,
                }),
                readable: Notify::new(),
                next_waiter_id: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    /// Attempts to enqueue without waiting for capacity.
    ///
    /// Returns `Closed` after shutdown, `MessageTooLarge` when an open queue's
    /// payload exceeds the byte limit, or `Backpressure` when either budget is
    /// full.
    ///
    /// # Parameters
    ///
    /// - `item`: Message to enqueue if both queue budgets allow it.
    pub(crate) fn try_push(&self, item: WsMessage) -> Result<(), WsSendError> {
        self.try_push_recoverable(item).map_err(|failure| failure.kind())
    }

    /// Attempts to enqueue without waiting, returning the unqueued message on
    /// failure.
    ///
    /// # Parameters
    ///
    /// - `item`: Message to enqueue or retain in the returned failure.
    pub(crate) fn try_push_recoverable(&self, item: WsMessage) -> Result<(), WsSendFailure> {
        let bytes = item.payload_len();
        if bytes > self.inner.max_bytes {
            return Err(WsSendFailure::new(self.closed_or(WsSendError::MessageTooLarge), item));
        }
        let mut item = Some(item);
        match self.try_enqueue(&mut item) {
            Ok(true) => {}
            Ok(false) => {
                return Err(WsSendFailure::new(
                    WsSendError::Backpressure,
                    item.take().expect("item remains pending"),
                ));
            }
            Err(kind) => {
                return Err(WsSendFailure::new(kind, item.take().expect("item remains pending")));
            }
        }
        self.inner.readable.notify_one();
        Ok(())
    }

    /// Waits for both budgets together and enqueues, or returns when the queue
    /// closes.
    ///
    /// Cancelling this future does not retain capacity unless a complete
    /// reservation was made, in which case its RAII guard releases it.
    ///
    /// # Parameters
    ///
    /// - `item`: Message to enqueue after capacity becomes available.
    pub(crate) async fn push(&self, item: WsMessage) -> Result<(), WsSendError> {
        self.push_recoverable(item).await.map_err(|failure| failure.kind())
    }

    /// Waits for capacity and returns the unqueued message if the queue closes
    /// before it can be admitted.
    ///
    /// # Parameters
    ///
    /// - `item`: Message to enqueue or return if admission fails.
    pub(crate) async fn push_recoverable(&self, item: WsMessage) -> Result<(), WsSendFailure> {
        let bytes = item.payload_len();
        if bytes > self.inner.max_bytes {
            return Err(WsSendFailure::new(self.closed_or(WsSendError::MessageTooLarge), item));
        }
        let mut item = Some(item);
        match self.try_enqueue(&mut item) {
            Ok(true) => {
                self.inner.readable.notify_one();
                return Ok(());
            }
            Ok(false) => {}
            Err(kind) => {
                return Err(WsSendFailure::new(kind, item.take().expect("item remains pending")));
            }
        }
        let waiter = self.inner.register_waiter(bytes);
        loop {
            let writable = waiter.notify().notified();
            tokio::pin!(writable);
            writable.as_mut().enable();
            match self.try_enqueue(&mut item) {
                Ok(true) => {
                    self.inner.readable.notify_one();
                    return Ok(());
                }
                Ok(false) => waiter.reset(),
                Err(kind) => {
                    return Err(WsSendFailure::new(kind, item.take().expect("item remains pending")));
                }
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
            for waiter in state.waiters.values() {
                waiter.notify.notify_one();
            }
            std::mem::take(&mut state.items)
        };
        drop(items);
        self.inner.readable.notify_waiters();
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
    ///
    /// # Parameters
    ///
    /// - `item`: Pending message to move into the queue if capacity is
    ///   available.
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
    ///
    /// # Parameters
    ///
    /// - `error`: Error to return while the queue remains open.
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
    use std::sync::atomic::Ordering;

    use super::BoundedQueue;
    use crate::ws_message::WsMessage;

    #[tokio::test]
    async fn test_queued_fast_path_does_not_register_waiter() {
        let queue = BoundedQueue::new(1, 64);
        let before = queue.inner.next_waiter_id.load(Ordering::Relaxed);

        queue
            .push_recoverable(WsMessage::Text("x".to_owned()))
            .await
            .expect("message fits");

        assert_eq!(queue.inner.next_waiter_id.load(Ordering::Relaxed), before);
        assert_eq!(
            queue.try_pop().expect("queued message").into_item(),
            WsMessage::Text("x".to_owned())
        );
        assert_eq!(queue.usage(), (0, 0));
    }

    #[tokio::test]
    async fn test_queued_fast_path_closed_queue_returns_message_without_registration() {
        let queue = BoundedQueue::new(1, 64);
        queue.close();
        let before = queue.inner.next_waiter_id.load(Ordering::Relaxed);
        let message = WsMessage::Text("x".to_owned());

        let failure = queue
            .push_recoverable(message.clone())
            .await
            .expect_err("closed queue rejects message");

        assert_eq!(failure.kind(), crate::error::WsSendError::Closed);
        assert_eq!(failure.into_message(), message);
        assert_eq!(queue.inner.next_waiter_id.load(Ordering::Relaxed), before);
        assert_eq!(queue.usage(), (0, 0));
    }

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

    #[tokio::test]
    async fn test_waiter_index_deactivates_after_contention_peak_recedes() {
        const HIGH_WAITER_COUNT: usize = 129;
        const RESIDUAL_WAITER_COUNT: usize = 16;

        let queue = BoundedQueue::new(1, 128);
        queue.try_push(WsMessage::Text("x".to_owned())).unwrap();
        let mut waiters = (0..HIGH_WAITER_COUNT)
            .map(|_| Box::pin(queue.push(WsMessage::Text("y".to_owned()))))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            assert!(futures_util::poll!(waiter.as_mut()).is_pending());
        }
        {
            let state = queue.inner.state.lock().expect("queue mutex poisoned");
            assert!(state.waiter_index_active);
            assert_eq!(state.waiters.len(), HIGH_WAITER_COUNT);
            assert_eq!(
                state.waiters_by_bytes.get(&1).map(|ids| ids.len()),
                Some(HIGH_WAITER_COUNT)
            );
        }

        drop(waiters.drain(..HIGH_WAITER_COUNT - RESIDUAL_WAITER_COUNT));

        {
            let state = queue.inner.state.lock().expect("queue mutex poisoned");
            assert!(!state.waiter_index_active);
            assert_eq!(state.waiters.len(), RESIDUAL_WAITER_COUNT);
            assert!(state.waiters_by_bytes.is_empty());
        }
        drop(waiters);
        drop(queue.try_pop().expect("remove occupied message"));
        assert_eq!(queue.usage(), (0, 0));
    }
}
