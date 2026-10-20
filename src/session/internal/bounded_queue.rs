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
    /// Returns `Closed` after shutdown, `MessageTooLarge` when an open queue's
    /// payload exceeds the byte limit, or `Backpressure` when either budget is
    /// full.
    pub(crate) fn try_push(&self, item: WsMessage) -> Result<(), WsSendError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(WsSendError::Closed);
        }
        let payload_len = item.payload_len();
        if payload_len > self.inner.max_bytes {
            return Err(WsSendError::MessageTooLarge);
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
    /// Returns `Closed` after shutdown, or `MessageTooLarge` when an open
    /// queue's payload exceeds the byte limit. Cancelling this future releases
    /// any capacity permits it acquired while waiting.
    pub(crate) async fn push(&self, item: WsMessage) -> Result<(), WsSendError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(WsSendError::Closed);
        }
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
