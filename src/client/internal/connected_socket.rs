// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

/// A socket whose opening handshake has passed tungstenite validation.
pub(crate) struct ConnectedSocket {
    /// Established WebSocket stream, with TLS present for `wss` endpoints.
    pub(crate) stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Peer-selected protocol, or `None` when no protocol was selected.
    pub(crate) selected_subprotocol: Option<String>,
    /// Successful HTTP opening response status, normally `101`.
    pub(crate) status_code: u16,
}
