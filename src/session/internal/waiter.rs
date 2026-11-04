// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use tokio::sync::Notify;

/// One sender registered while it waits for queue capacity.
pub(super) struct Waiter {
    /// Identifier used to find this registration during reset or removal.
    pub(super) id: usize,
    /// Logical payload bytes required by the pending message; no budget is
    /// reserved while the sender waits.
    pub(super) bytes: usize,
    /// Per-registration notification used to wake this sender independently.
    pub(super) notify: Arc<Notify>,
    /// Whether this registration has been selected for notification. This
    /// does not mean the message was admitted or capacity was reserved.
    pub(super) notified: bool,
}
