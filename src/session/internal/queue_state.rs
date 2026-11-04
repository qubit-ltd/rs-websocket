// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;

use super::envelope::Envelope;
use super::waiter::Waiter;

/// Mutable queue contents, lifecycle, capacity accounting, and waiters.
///
/// The containing `QueueInner::state` mutex protects the budgets, waiter
/// collection, and each waiter's notification-selection state together.
pub(super) struct QueueState {
    /// FIFO storage for admitted message envelopes.
    pub(super) items: VecDeque<Envelope>,
    /// Number of outstanding reservations, including active writes.
    pub(super) used_messages: usize,
    /// Payload bytes held by outstanding reservations.
    pub(super) used_bytes: usize,
    /// Whether shutdown has rejected future reservations.
    pub(super) closed: bool,
    /// Capacity waiters coordinated with the budgets and selection markers
    /// under the containing queue-state mutex.
    pub(super) waiters: Vec<Waiter>,
}
