// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Stable, non-sensitive category for a WebSocket operation failure.
///
/// `Transport` covers both connection establishment (DNS/TCP) and I/O after
/// establishment. TLS negotiation and HTTP upgrade failures retain their own
/// categories. A driver task join failure is `Internal`; formatting a
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsErrorKind {
    /// Client options violate a required limit or protocol constraint.
    InvalidConfig,
    /// The URL is not a valid `ws` or `wss` endpoint.
    InvalidUrl,
    /// DNS resolution, TCP connection, or established-connection I/O failed.
    Transport,
    /// TLS negotiation or certificate verification failed.
    Tls,
    /// The opening HTTP upgrade handshake failed.
    Handshake,
    /// An established WebSocket connection violated the protocol.
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

/// A transport or protocol error with safe formatting and an inspectable
/// source.
///
/// The category and structured handshake context are available through
/// accessors. `Display` and `Debug` omit the retained source; callers that
/// explicitly need the underlying diagnostic can inspect `Error::source`.
///
/// # Examples
///
/// ```
/// use std::error::Error as _;
/// use std::io;
/// use qubit_websocket::{WsError, WsErrorKind};
///
/// let error = WsError::with_source(WsErrorKind::Transport, io::Error::other("reset"));
/// assert_eq!(error.kind(), WsErrorKind::Transport);
/// assert!(error.source().is_some());
/// assert!(!error.to_string().contains("reset"));
/// ```
pub struct WsError {
    kind: WsErrorKind,
    status_code: Option<u16>,
    handshake_failure: Option<WsHandshakeFailure>,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl WsError {
    /// Creates an error without a source.
    pub fn new(kind: WsErrorKind) -> Self {
        Self {
            kind,
            status_code: None,
            handshake_failure: None,
            source: None,
        }
    }

    /// Creates an error retaining an underlying source for explicit diagnosis.
    pub fn with_source<E>(kind: WsErrorKind, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            status_code: None,
            handshake_failure: None,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the stable error category.
    pub fn kind(&self) -> WsErrorKind {
        self.kind
    }
    /// Returns the numeric HTTP status when a peer supplied one.
    pub fn status_code(&self) -> Option<u16> {
        self.status_code
    }
    /// Returns the structured handshake failure, if applicable.
    pub fn handshake_failure(&self) -> Option<WsHandshakeFailure> {
        self.handshake_failure
    }
    /// Adds a numeric HTTP status to this error.
    pub fn with_status_code(mut self, status_code: u16) -> Self {
        self.status_code = Some(status_code);
        self
    }
    /// Adds a structured handshake failure to this error.
    pub fn with_handshake_failure(mut self, failure: WsHandshakeFailure) -> Self {
        self.handshake_failure = Some(failure);
        self
    }
}

impl fmt::Display for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WebSocket {:?}", self.kind)?;
        if let Some(status) = self.status_code {
            write!(f, " (status {status})")?;
        }
        if let Some(failure) = self.handshake_failure {
            write!(f, ": {failure:?}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsError")
            .field("kind", &self.kind)
            .field("status_code", &self.status_code)
            .field("handshake_failure", &self.handshake_failure)
            .finish_non_exhaustive()
    }
}

impl Error for WsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn Error + 'static))
    }
}

/// Reason that an application send could not be queued.
///
/// This reports queue admission only. It does not describe a WebSocket
/// terminal failure or indicate whether the peer received a message.
///
/// # Examples
///
/// ```
/// use qubit_websocket::WsSendError;
///
/// let result: Result<(), WsSendError> = Err(WsSendError::Backpressure);
/// assert_eq!(result, Err(WsSendError::Backpressure));
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsSendError {
    /// The configured message-count or byte budget is currently full.
    Backpressure,
    /// The session no longer accepts application messages.
    Closed,
    /// The message exceeds the configured outbound byte limit.
    MessageTooLarge,
}

impl fmt::Display for WsSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Backpressure => "WebSocket send queue is full",
            Self::Closed => "WebSocket session is closed",
            Self::MessageTooLarge => "WebSocket message exceeds the outbound limit",
        };
        f.write_str(message)
    }
}

impl Error for WsSendError {}

/// Safe snapshot of a session's terminal state.
///
/// `Closed` confirms that the WebSocket close exchange completed; it does not
/// decide whether the peer's status code represents success for an application
/// protocol. `Error` means the session could not confirm a completed close or
/// encountered another failure. The snapshot contains no transport details.
///
/// # Examples
///
/// ```no_run
/// use qubit_websocket::{WsClient, WsTermination};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let client = WsClient::builder().build()?;
///     let session = client.connect("ws://127.0.0.1:9000/events").await?;
///     match session.termination() {
///         Some(WsTermination::Closed { code: Some(1000) }) => println!("peer sent 1000"),
///         Some(WsTermination::Closed { code }) => println!("peer close code: {code:?}"),
///         Some(WsTermination::Error { kind }) => println!("session failed: {kind:?}"),
///         None => println!("session is still active"),
///     }
///     Ok(())
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsTermination {
    /// The WebSocket close exchange completed; the application interprets
    /// `code`.
    Closed {
        /// The raw status supplied by the peer; `None`, `1009`, and other codes
        /// do not by themselves establish application-level success.
        code: Option<u16>,
    },
    /// The session ended because of an error category.
    Error {
        /// Stable, non-sensitive failure category.
        kind: WsErrorKind,
    },
}
