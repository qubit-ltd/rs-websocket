// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

use crate::error::WsErrorKind;
use crate::error::WsHandshakeFailure;

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
