// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
    pub(super) waiters: BTreeMap<usize, Waiter>,
    /// Unnotified waiter IDs grouped by required payload bytes. This index
    /// lets capacity release skip payload sizes that cannot currently fit.
    pub(super) waiters_by_bytes: BTreeMap<usize, BTreeSet<usize>>,
    /// Whether the byte index remains active after crossing its contention
    /// threshold. The index is retired only after the waiter count reaches
    /// its lower deactivation threshold; small active sets still use the
    /// original ordered selection scan.
    pub(super) waiter_index_active: bool,
}

impl QueueState {
    /// Adds one unnotified waiter to the capacity-selection index.
    ///
    /// # Parameters
    ///
    /// - `id`: Unique registration ID of the waiter.
    /// - `bytes`: Payload bytes required by the waiter.
    pub(super) fn index_waiter(&mut self, id: usize, bytes: usize) {
        self.waiters_by_bytes.entry(bytes).or_default().insert(id);
    }

    /// Removes one waiter from the capacity-selection index and prunes an
    /// empty payload-size bucket.
    ///
    /// # Parameters
    ///
    /// - `id`: Registration ID to remove from the index.
    /// - `bytes`: Payload-size bucket containing the registration ID.
    pub(super) fn unindex_waiter(&mut self, id: usize, bytes: usize) {
        let remove_bucket = self.waiters_by_bytes.get_mut(&bytes).is_some_and(|ids| {
            ids.remove(&id);
            ids.is_empty()
        });
        if remove_bucket {
            self.waiters_by_bytes.remove(&bytes);
        }
    }
}
