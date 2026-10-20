// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// A specific reason why a WebSocket opening handshake failed.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsHandshakeFailure;
///
/// let failure = WsHandshakeFailure::Status(401);
/// assert_eq!(failure, WsHandshakeFailure::Status(401));
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsHandshakeFailure {
    /// The peer returned this non-switching HTTP status, retained as its code.
    Status(u16),
    /// The response omitted or changed the required `Upgrade: websocket`
    /// header.
    Upgrade,
    /// The response omitted or changed the required `Connection: Upgrade`
    /// header.
    Connection,
    /// The response supplied an invalid `Sec-WebSocket-Accept` value.
    Accept,
    /// The response selected an unoffered, duplicated, or invalid subprotocol.
    Subprotocol,
}
