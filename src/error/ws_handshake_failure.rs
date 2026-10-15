// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// A specific reason why a WebSocket opening handshake failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsHandshakeFailure {
    /// The peer returned this non-switching HTTP status.
    Status(u16),
    /// The response omitted or changed the required Upgrade header.
    Upgrade,
    /// The response omitted or changed the required Connection header.
    Connection,
    /// The response supplied an invalid WebSocket accept value.
    Accept,
    /// The response selected an unoffered or invalid subprotocol.
    Subprotocol,
}
