// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::close_request::CloseRequest;
use super::session_failure::SessionFailure;

/// Event selected while waiting for a Ping-triggered flush.
pub(super) enum PingFlushEvent {
    /// The peer was observed closed or its close-notification sender ended.
    PeerClosed,
    /// A peer watch update arrived without a Close frame.
    PeerUpdated,
    /// The close watch changed and reported its current request state.
    CloseChanged {
        /// `None` means there is no close request in this watch snapshot.
        request: Option<CloseRequest>,
    },
    /// The close watch sender was dropped.
    CloseWatchClosed,
    /// The flush operation completed.
    Flushed {
        /// `None` means the flush succeeded; `Some` identifies its failure.
        error: Option<SessionFailure>,
    },
}
