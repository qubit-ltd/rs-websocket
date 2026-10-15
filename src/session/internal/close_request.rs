// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// A lifecycle request sent from the session or reader to the writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloseRequest {
    /// The receiving session or driver requested a normal close handshake.
    Local,
    /// The reader stopped due to application queue saturation.
    SlowConsumer,
}
