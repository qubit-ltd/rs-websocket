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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use super::envelope::Envelope;
use super::queue_inner::QueueInner;
use crate::error::WsSendError;
use crate::ws_message::WsMessage;

/// A FIFO queue bounded by both message count and payload bytes.
///
/// Clones share one queue and its limits. Closing it rejects new pushes, wakes
/// blocked producers, and discards queued items, releasing their permits.
#[derive(Clone)]
pub(crate) struct BoundedQueue {
    inner: Arc<QueueInner>,
}

impl BoundedQueue {
    /// Creates a queue with independent message-count and byte limits.
    ///
    /// # Panics
    ///
    /// Panics if either limit is zero or if `max_bytes` exceeds the semaphore
    /// permit range.
    pub(crate) fn new(max_messages: usize, max_bytes: usize) -> Self {
        assert!(max_messages > 0, "message capacity must be non-zero");
        assert!(max_bytes > 0, "byte capacity must be non-zero");
        assert!(max_bytes <= u32::MAX as usize, "byte capacity exceeds semaphore limit");

        Self {
            inner: Arc::new(QueueInner {
                messages: Arc::new(Semaphore::new(max_messages)),
                bytes: Arc::new(Semaphore::new(max_bytes)),
                #[cfg(test)]
                max_messages,
                max_bytes,
                closed: AtomicBool::new(false),
                items: Mutex::new(VecDeque::new()),
                readable: Notify::new(),
                writable: Notify::new(),
            }),
        }
    }

    /// Attempts to enqueue without waiting for capacity.
    ///
    /// Returns `MessageTooLarge` when the payload exceeds the byte limit,
    /// `Closed` after shutdown, or `Backpressure` when either budget is full.
    pub(crate) fn try_push(&self, item: WsMessage) -> Result<(), WsSendError> {
        let payload_len = item.payload_len();
        if payload_len > self.inner.max_bytes {
            return Err(WsSendError::MessageTooLarge);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(WsSendError::Closed);
        }

        let message_permit = self
            .inner
            .messages
            .clone()
            .try_acquire_owned()
            .map_err(|_| self.capacity_error())?;
        let byte_permit = if payload_len == 0 {
            None
        } else {
            Some(
                self.inner
                    .bytes
                    .clone()
                    .try_acquire_many_owned(payload_len as u32)
                    .map_err(|_| self.capacity_error())?,
            )
        };

        self.enqueue(item, message_permit, byte_permit)
    }

    /// Waits for capacity and enqueues, or returns when the queue closes.
    ///
    /// Returns `MessageTooLarge` when the payload exceeds the byte limit, or
    /// `Closed` if shutdown occurs before admission. Cancelling this future
    /// releases any capacity permits it acquired while waiting.
    pub(crate) async fn push(&self, item: WsMessage) -> Result<(), WsSendError> {
        let payload_len = item.payload_len();
        if payload_len > self.inner.max_bytes {
            return Err(WsSendError::MessageTooLarge);
        }

        let message_permit = self.acquire_permits(self.inner.messages.clone(), 1).await?;
        let byte_permit = if payload_len == 0 {
            None
        } else {
            Some(
                self.acquire_permits(self.inner.bytes.clone(), payload_len as u32)
                    .await?,
            )
        };
        self.enqueue(item, message_permit, byte_permit)
    }

    /// Removes the next message, waiting until one is available or the queue
    /// closes.
    pub(crate) async fn pop(&self) -> Option<Envelope> {
        loop {
            let readable = self.inner.readable.notified();
            tokio::pin!(readable);
            readable.as_mut().enable();

            if let Some(item) = self.inner.items.lock().expect("queue mutex poisoned").pop_front() {
                self.inner.writable.notify_one();
                return Some(item);
            }
            if self.inner.closed.load(Ordering::Acquire) {
                return None;
            }
            readable.await;
        }
    }

    /// Removes an available message without waiting for queue activity.
    pub(crate) fn try_pop(&self) -> Option<Envelope> {
        let item = self.inner.items.lock().expect("queue mutex poisoned").pop_front();
        if item.is_some() {
            self.inner.writable.notify_one();
        }
        item
    }

    /// Closes the queue, discards pending messages, and wakes blocked senders
    /// and receivers. Repeated calls have no additional effect.
    pub(crate) fn close(&self) {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.messages.close();
        self.inner.bytes.close();
        self.inner.items.lock().expect("queue mutex poisoned").clear();
        self.inner.readable.notify_waiters();
        self.inner.writable.notify_waiters();
    }

    /// Returns the currently reserved message and byte counts.
    #[cfg(test)]
    pub(crate) fn usage(&self) -> (usize, usize) {
        (
            self.inner.max_messages - self.inner.messages.available_permits(),
            self.inner.max_bytes - self.inner.bytes.available_permits(),
        )
    }

    /// Waits for `count` permits, waking to observe queue closure.
    ///
    /// The returned owned permit reserves capacity until it is moved into an
    /// envelope or dropped. If this future is cancelled, any permit it has
    /// acquired is released by dropping the acquisition future; callers also
    /// release any earlier permit they hold.
    async fn acquire_permits(
        &self,
        semaphore: Arc<Semaphore>,
        count: u32,
    ) -> Result<OwnedSemaphorePermit, WsSendError> {
        let acquire = semaphore.acquire_many_owned(count);
        tokio::pin!(acquire);
        loop {
            let writable = self.inner.writable.notified();
            tokio::pin!(writable);
            writable.as_mut().enable();
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(WsSendError::Closed);
            }
            tokio::select! {
                result = &mut acquire => return result.map_err(|_| WsSendError::Closed),
                _ = &mut writable => {},
            }
        }
    }

    /// Adds an item after both capacity permits have been acquired.
    ///
    /// On success, the envelope owns the permits until it is removed and
    /// dropped. If closure wins the final admission check, returns `Closed`
    /// and the supplied permits are released when this call returns.
    fn enqueue(
        &self,
        item: WsMessage,
        message_permit: OwnedSemaphorePermit,
        byte_permit: Option<OwnedSemaphorePermit>,
    ) -> Result<(), WsSendError> {
        let mut items = self.inner.items.lock().expect("queue mutex poisoned");
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(WsSendError::Closed);
        }
        items.push_back(Envelope {
            item,
            _message_permit: message_permit,
            _byte_permit: byte_permit,
        });
        drop(items);
        self.inner.readable.notify_one();
        Ok(())
    }

    /// Classifies a failed nonblocking permit acquisition against queue state.
    ///
    /// Returns `Closed` when shutdown is visible; otherwise the exhausted
    /// capacity is reported as `Backpressure`.
    fn capacity_error(&self) -> WsSendError {
        if self.inner.closed.load(Ordering::Acquire) {
            WsSendError::Closed
        } else {
            WsSendError::Backpressure
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::BoundedQueue;
    use crate::error::WsSendError;
    use crate::ws_message::WsMessage;

    fn text(payload: &str) -> WsMessage {
        WsMessage::Text(payload.to_owned())
    }

    #[tokio::test]
    async fn test_enforces_message_and_byte_limits_and_releases_permits_on_pop() {
        let queue = BoundedQueue::new(2, 5);
        queue.try_push(text("ab")).expect("2 bytes fit");
        queue.try_push(text("xyz")).expect("3 more bytes fit");
        assert_eq!(queue.try_push(text("")), Err(WsSendError::Backpressure));

        assert_eq!(queue.try_pop().expect("first item").into_item(), text("ab"));
        queue.try_push(text("q")).expect("pop releases count and byte permits");
        assert_eq!(queue.try_push(text("123456")), Err(WsSendError::MessageTooLarge));

        let empty_queue = BoundedQueue::new(1, 1);
        empty_queue.try_push(text("")).expect("empty text uses a count permit");
        assert_eq!(empty_queue.try_push(text("")), Err(WsSendError::Backpressure));
    }

    #[tokio::test]
    async fn test_async_push_waits_for_capacity_and_drop_cancels_without_leaking_permits() {
        let queue = BoundedQueue::new(1, 2);
        queue.try_push(text("ok")).expect("initial item fits");

        let waiting_queue = queue.clone();
        let waiting = tokio::spawn(async move { waiting_queue.push(text("x")).await });
        tokio::task::yield_now().await;
        assert_eq!(queue.usage(), (1, 2));

        drop(queue.pop().await.expect("pop releases capacity"));
        timeout(Duration::from_secs(1), waiting)
            .await
            .expect("push wakes after capacity is available")
            .expect("push task joins")
            .expect("push succeeds");
        assert_eq!(queue.usage(), (1, 1));

        let cancelled_queue = BoundedQueue::new(1, 1);
        cancelled_queue.try_push(text("a")).expect("fill queue");
        let pending_queue = cancelled_queue.clone();
        let pending = tokio::spawn(async move { pending_queue.push(text("b")).await });
        tokio::task::yield_now().await;
        pending.abort();
        let _ = pending.await;
        drop(cancelled_queue.pop().await.expect("release occupied slot"));
        cancelled_queue
            .try_push(text("b"))
            .expect("cancelled waiter leaked no permit");

        let partial_queue = BoundedQueue::new(2, 1);
        partial_queue.try_push(text("a")).expect("occupy byte permit");
        let pending_queue = partial_queue.clone();
        let pending = tokio::spawn(async move { pending_queue.push(text("b")).await });
        tokio::task::yield_now().await;
        assert_eq!(partial_queue.usage(), (2, 1));
        pending.abort();
        let _ = pending.await;
        assert_eq!(partial_queue.usage(), (1, 1));
        partial_queue
            .try_push(text(""))
            .expect("cancellation while waiting for bytes releases the count permit");
    }

    #[tokio::test]
    async fn test_close_releases_queued_items_and_wakes_blocked_pushers() {
        let queue = BoundedQueue::new(1, 2);
        queue.try_push(text("a")).expect("fill queue");

        let waiting_queue = queue.clone();
        let waiting = tokio::spawn(async move { waiting_queue.push(text("b")).await });
        tokio::task::yield_now().await;
        queue.close();

        assert_eq!(
            timeout(Duration::from_secs(1), waiting)
                .await
                .expect("closed queue wakes blocked pushers")
                .expect("push task joins"),
            Err(WsSendError::Closed)
        );
        assert_eq!(queue.usage(), (0, 0));
        assert_eq!(queue.try_push(text("c")), Err(WsSendError::Closed));
        assert!(
            timeout(Duration::from_secs(1), queue.pop())
                .await
                .expect("closed queue pop resolves")
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_repeated_close_and_waiter_cancellation_always_terminate() {
        for iteration in 0..32 {
            let queue = BoundedQueue::new(1, 1);
            queue.try_push(text("x")).expect("fill queue");

            let waiting_queue = queue.clone();
            let waiting = tokio::spawn(async move { waiting_queue.push(text("y")).await });
            tokio::task::yield_now().await;

            if iteration % 2 == 0 {
                waiting.abort();
                timeout(Duration::from_secs(1), waiting)
                    .await
                    .expect("cancelled waiter terminates")
                    .expect_err("aborted waiter reports cancellation");
                queue.close();
            } else {
                queue.close();
                assert_eq!(
                    timeout(Duration::from_secs(1), waiting)
                        .await
                        .expect("close wakes waiter")
                        .expect("push task joins"),
                    Err(WsSendError::Closed)
                );
            }

            assert_eq!(queue.usage(), (0, 0));
            assert!(
                timeout(Duration::from_secs(1), queue.pop())
                    .await
                    .expect("closed queue pop resolves")
                    .is_none()
            );
        }
    }
}
