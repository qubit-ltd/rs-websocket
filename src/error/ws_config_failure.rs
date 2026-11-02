// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// A safe, structured reason why WebSocket numeric configuration is invalid.
///
/// The variants contain no runtime configuration values. Use [`Self::field`]
/// to locate the relative configuration key associated with a failure.
///
/// # Examples
///
/// ```
/// use qubit_websocket::{WsClientOptions, WsConfigFailure};
///
/// let options = WsClientOptions::default();
/// if let Err(error) = options.validate() {
///     if let Some(failure) = error.config_failure() {
///         match failure {
///             WsConfigFailure::ZeroConnectTimeout => assert_eq!(failure.field(), "connect_timeout_ms"),
///             _ => assert!(!failure.field().is_empty()),
///         }
///     }
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WsConfigFailure {
    /// The TCP connection timeout is zero.
    ZeroConnectTimeout,
    /// The opening handshake timeout is zero.
    ZeroHandshakeTimeout,
    /// The maximum inbound frame size is zero.
    ZeroMaxInboundFrameBytes,
    /// The maximum inbound message size is zero.
    ZeroMaxInboundMessageBytes,
    /// The inbound message count budget is zero.
    ZeroInboundMessages,
    /// The inbound byte budget is zero.
    ZeroInboundBytes,
    /// The outbound message count budget is zero.
    ZeroOutboundMessages,
    /// The outbound byte budget is zero.
    ZeroOutboundBytes,
    /// The no-progress timeout is zero.
    ZeroNoProgressTimeout,
    /// The close timeout is zero.
    ZeroCloseTimeout,
    /// The frame size exceeds the maximum message size.
    FrameExceedsMessageLimit,
    /// The maximum message size exceeds the inbound byte budget.
    MessageExceedsInboundBudget,
    /// Adding WebSocket write-buffer overhead overflows `usize`.
    OutboundWriteBufferOverflow,
    /// The TCP connection deadline cannot be represented by the runtime clock.
    UnrepresentableConnectTimeout,
    /// The opening handshake deadline cannot be represented by the runtime
    /// clock.
    UnrepresentableHandshakeTimeout,
    /// The close deadline cannot be represented by the runtime clock.
    UnrepresentableCloseTimeout,
    /// The no-progress deadline cannot be represented by the runtime clock.
    UnrepresentableNoProgressTimeout,
}

impl WsConfigFailure {
    /// Returns the relative configuration field associated with this failure.
    ///
    /// For size relationships, the returned field is the limit that must be
    /// reduced; the other side of the relationship is described by the
    /// variant.
    ///
    /// # Returns
    ///
    /// A static relative key path, without any application-specific prefix.
    pub const fn field(self) -> &'static str {
        match self {
            Self::ZeroConnectTimeout | Self::UnrepresentableConnectTimeout => "connect_timeout_ms",
            Self::ZeroHandshakeTimeout | Self::UnrepresentableHandshakeTimeout => "handshake_timeout_ms",
            Self::ZeroMaxInboundFrameBytes | Self::FrameExceedsMessageLimit => "limits.max_inbound_frame_bytes",
            Self::ZeroMaxInboundMessageBytes | Self::MessageExceedsInboundBudget => "limits.max_inbound_message_bytes",
            Self::ZeroInboundMessages => "limits.inbound_messages",
            Self::ZeroInboundBytes => "limits.inbound_bytes",
            Self::ZeroOutboundMessages => "limits.outbound_messages",
            Self::ZeroOutboundBytes | Self::OutboundWriteBufferOverflow => "limits.outbound_bytes",
            Self::ZeroNoProgressTimeout | Self::UnrepresentableNoProgressTimeout => "limits.no_progress_timeout_ms",
            Self::ZeroCloseTimeout | Self::UnrepresentableCloseTimeout => "limits.close_timeout_ms",
        }
    }
}
