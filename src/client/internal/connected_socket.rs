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
    pub(crate) stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub(crate) selected_subprotocol: Option<String>,
    pub(crate) status_code: u16,
}
