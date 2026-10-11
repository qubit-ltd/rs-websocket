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

use crate::error::WsSendError;
use crate::message::WsMessage;

/// An application message that keeps its queue capacity reserved until dropped.
pub(crate) struct Envelope {
    pub(crate) item: WsMessage,
    pub(crate) _message_permit: OwnedSemaphorePermit,
    pub(crate) _byte_permit: Option<OwnedSemaphorePermit>,
}

impl Envelope {
    /// Returns the message and releases its queue permits.
    pub(crate) fn into_item(self) -> WsMessage {
        self.item
    }
}

struct QueueInner {
    messages: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    #[cfg(test)]
    max_messages: usize,
    max_bytes: usize,
    closed: AtomicBool,
    items: Mutex<VecDeque<Envelope>>,
    readable: Notify,
    writable: Notify,
}

/// A FIFO queue bounded by both message count and payload bytes.
#[derive(Clone)]
pub(crate) struct BoundedQueue {
    inner: Arc<QueueInner>,
}

impl BoundedQueue {
    /// Creates a queue with independent message-count and byte limits.
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
    pub(crate) async fn push(&self, item: WsMessage) -> Result<(), WsSendError> {
        let payload_len = item.payload_len();
        if payload_len > self.inner.max_bytes {
            return Err(WsSendError::MessageTooLarge);
        }

        let message_permit = self.acquire_message().await?;
        let byte_permit = if payload_len == 0 {
            None
        } else {
            Some(self.acquire_bytes(payload_len as u32).await?)
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

    /// Closes the queue, drops pending messages, and wakes blocked operations.
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

    async fn acquire_message(&self) -> Result<OwnedSemaphorePermit, WsSendError> {
        let acquire = self.inner.messages.clone().acquire_owned();
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

    async fn acquire_bytes(&self, count: u32) -> Result<OwnedSemaphorePermit, WsSendError> {
        let acquire = self.inner.bytes.clone().acquire_many_owned(count);
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

    fn capacity_error(&self) -> WsSendError {
        if self.inner.closed.load(Ordering::Acquire) {
            WsSendError::Closed
        } else {
            WsSendError::Backpressure
        }
    }
}
