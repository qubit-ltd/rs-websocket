use bytes::Bytes;
use std::fmt;

/// An application data message exchanged with a WebSocket peer.
#[derive(Clone, Eq, PartialEq)]
pub enum WsMessage {
    /// UTF-8 text payload.
    Text(String),
    /// Opaque binary payload.
    Binary(Bytes),
}

impl fmt::Debug for WsMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => f
                .debug_struct("Text")
                .field("payload_bytes", &text.len())
                .finish(),
            Self::Binary(bytes) => f
                .debug_struct("Binary")
                .field("payload_bytes", &bytes.len())
                .finish(),
        }
    }
}

impl WsMessage {
    /// Returns the payload size in bytes.
    pub fn payload_len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Binary(bytes) => bytes.len(),
        }
    }
}
