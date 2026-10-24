// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;

use super::envelope::Envelope;

/// Mutable queue contents, lifecycle, and atomically reserved capacity.
pub(super) struct QueueState {
    /// FIFO storage for admitted message envelopes.
    pub(super) items: VecDeque<Envelope>,
    /// Number of outstanding reservations, including active writes.
    pub(super) used_messages: usize,
    /// Payload bytes held by outstanding reservations.
    pub(super) used_bytes: usize,
    /// Whether shutdown has rejected future reservations.
    pub(super) closed: bool,
}
