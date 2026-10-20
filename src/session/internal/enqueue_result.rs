// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::error::WsErrorKind;

/// Result of waiting to enqueue one inbound message while servicing controls.
///
/// The reader retains at most the message currently waiting for queue capacity.
/// It continues polling control frames during that wait; additional application
/// data or expiry of the no-progress deadline takes the slow-consumer path.
#[derive(Debug)]
pub(super) enum EnqueueResult {
    /// The waiting message was admitted to the bounded inbound queue.
    Queued,
    /// The inbound queue closed before it could admit the message.
    Closed,
    /// A local close request stopped the wait.
    LocalClose,
    /// The peer sent Close while the reader was waiting for capacity.
    PeerClosed(Option<u16>),
    /// More application data arrived while the retained message awaited
    /// capacity.
    Overflow,
    /// The no-progress deadline expired while the retained message awaited
    /// capacity.
    Stalled,
    /// Reading failed; the category distinguishes transport, protocol, and
    /// capacity failures.
    Error(WsErrorKind),
}
