// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use tokio::time::error::Elapsed;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;

use super::close_request::CloseRequest;

/// Event that can interrupt a pending writer progress operation.
pub(super) enum SendEvent {
    /// A poll-ready/flush operation and its deadline result.
    Progress(
        /// Sink poll result, nested with the deadline result.
        Result<Result<(), TungsteniteError>, Elapsed>,
    ),
    /// A Ping arrived while a message write was pending.
    Ping,
    /// A local or slow-consumer close request was published.
    Close(
        /// Latest request, or `None` if the watch sender closed.
        Option<CloseRequest>,
    ),
    /// The peer's Close frame was observed.
    PeerClosed,
}
