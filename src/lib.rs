// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! WebSocket client contracts for Qubit Rust services.

mod error;
mod client;
mod message;
mod options;
mod session;

pub use error::{WsError, WsErrorKind, WsHandshakeFailure, WsSendError, WsTermination};
pub use client::{WsClient, WsClientBuilder};
pub use message::WsMessage;
pub use options::{WsClientOptions, WsSessionLimits};
