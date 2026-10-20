// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Stable, non-sensitive category for a WebSocket operation failure.
///
/// `Transport` covers DNS/TCP connection establishment and ordinary network
/// I/O while the HTTP upgrade is in progress or after it completes. TLS
/// negotiation failures and structured HTTP upgrade rejections retain their
/// own categories. A driver task join failure is `Internal`; formatting a
/// [`WsError`](crate::WsError) does not reveal its retained source.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsErrorKind;
///
/// let kind = WsErrorKind::Transport;
/// assert_eq!(kind, WsErrorKind::Transport);
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsErrorKind {
    /// Client options violate a required limit or protocol constraint.
    InvalidConfig,
    /// The URL is not a valid `ws` or `wss` endpoint.
    InvalidUrl,
    /// DNS/TCP connection setup or ordinary WebSocket transport I/O failed.
    Transport,
    /// TLS negotiation or certificate verification failed.
    Tls,
    /// The opening HTTP upgrade handshake failed.
    Handshake,
    /// A WebSocket connection violated the protocol.
    Protocol,
    /// An inbound frame exceeded the configured frame size.
    FrameTooLarge,
    /// An inbound reassembled message exceeded the configured message size.
    MessageTooLarge,
    /// An operation exceeded its configured deadline.
    Timeout,
    /// The session driver encountered an unexpected lifecycle state or its
    /// task panicked or was cancelled.
    Internal,
}
