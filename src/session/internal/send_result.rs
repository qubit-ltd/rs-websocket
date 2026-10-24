// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::error::WsErrorKind;

/// Final outcome of writing the current message or closing the writer task.
#[must_use]
pub(super) enum SendResult {
    /// The queued application message was sent successfully.
    Sent,
    /// The writer exited with an optional categorized failure.
    Exit(Option<WsErrorKind>),
}
