// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded Tokio WebSocket clients for Rust services.
//!
//! Build a client once, then create a session for each connection. A session
//! keeps receive ownership in one task and exposes a cloneable sender for
//! other tasks. Queue limits apply to both message count and payload bytes.
//!
//! ```no_run
//! use qubit_websocket::{WsClient, WsMessage};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = WsClient::builder().build()?;
//! let mut session = client.connect("wss://example.com/events").await?;
//! session
//!     .send(WsMessage::Text("subscribe".to_owned()))
//!     .await
//!     .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
//! while let Some(message) = session.receive().await {
//!     match message? {
//!         WsMessage::Text(text) => println!("received text ({} bytes)", text.len()),
//!         WsMessage::Binary(bytes) => println!("received binary ({} bytes)", bytes.len()),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! This crate does not reconnect, replay messages, or expose a WebSocket
//! server. `send` confirms queue admission only; it does not confirm delivery
//! or application processing.

mod client;
mod error;
mod message;
mod options;
mod session;

pub use client::WsClient;
pub use client::WsClientBuilder;
pub use error::WsError;
pub use error::WsErrorKind;
pub use error::WsHandshakeFailure;
pub use error::WsSendError;
pub use error::WsTermination;
pub use message::WsMessage;
pub use options::WsClientOptions;
pub use options::WsSessionLimits;
pub use session::WsSender;
pub use session::WsSession;
