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

use tokio::sync::Notify;
use tokio::sync::Semaphore;

use super::envelope::Envelope;

pub(super) struct QueueInner {
    /// Semaphore controlling the number of queued messages.
    pub(super) messages: Arc<Semaphore>,
    /// Semaphore controlling queued payload bytes.
    pub(super) bytes: Arc<Semaphore>,
    #[cfg(test)]
    /// Configured queue-count bound used by unit-test accounting assertions.
    pub(super) max_messages: usize,
    /// Configured byte bound, also used to reject oversized messages.
    pub(super) max_bytes: usize,
    /// Atomic shutdown flag checked by queue operations.
    pub(super) closed: AtomicBool,
    /// FIFO storage for admitted message envelopes.
    pub(super) items: Mutex<VecDeque<Envelope>>,
    /// Wakes receivers after a message is enqueued or the queue is closed.
    pub(super) readable: Notify,
    /// Wakes senders after capacity is released or the queue is closed.
    pub(super) writable: Notify,
}
