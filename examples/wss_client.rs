// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Demonstrates connecting to a WebSocket server over TLS and exchanging a
//! message.

use std::time::Duration;

use http::HeaderMap;
use http::header::AUTHORIZATION;
use qubit_websocket::WsClient;
use qubit_websocket::WsMessage;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wss://localhost:3001/ws".to_owned());
    let mut headers = HeaderMap::new();
    if let Ok(token) = std::env::var("WS_BEARER_TOKEN") {
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
    }

    let client = WsClient::builder().subprotocols(vec!["echo.v1".to_owned()]).build()?;
    let mut session = client.connect_with(&url, headers).await?;
    session
        .send(WsMessage::Text("hello over wss".to_owned()))
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;

    loop {
        match timeout(Duration::from_secs(10), session.receive())
            .await
            .map_err(std::io::Error::other)?
        {
            Some(Ok(WsMessage::Text(reply))) => {
                println!("received text ({} bytes)", reply.len());
                session.close().await?;
            }
            Some(Ok(WsMessage::Binary(reply))) => {
                println!("received binary ({} bytes)", reply.len());
                session.close().await?;
            }
            Some(Err(error)) => {
                return Err(Box::new(error) as Box<dyn std::error::Error>);
            }
            None => {
                println!("peer closed the WebSocket");
                break;
            }
        }
    }
    Ok(())
}
