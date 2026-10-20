// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::error::WsErrorKind;

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
#[must_use]
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
