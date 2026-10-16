// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::error::WsErrorKind;

/// Outcome observed by the reader before the writer's final result is known.
///
/// The driver combines this with the writer result to determine the session's
/// terminal outcome.
#[derive(Clone, Copy)]
pub(super) enum ReadResult {
    /// The peer sent a Close frame with the given raw status code, if any.
    Closed(Option<u16>),
    /// The local side requested closure and the reader stopped.
    LocalClose,
    /// The reader encountered a protocol, transport, capacity, or timeout
    /// error.
    Error(WsErrorKind),
}
