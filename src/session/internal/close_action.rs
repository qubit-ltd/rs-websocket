// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Close action selected from the peer and local lifecycle signals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CloseAction {
    /// A peer Close frame was observed; flush the pending acknowledgement.
    PeerClosed,
    /// The peer-close watch ended without a Close snapshot; preserve flush-only
    /// behavior.
    PeerWatchEnded,
    /// Perform the local Close handshake.
    Local,
    /// Attempt the best-effort `1013` slow-consumer Close.
    SlowConsumer,
}
