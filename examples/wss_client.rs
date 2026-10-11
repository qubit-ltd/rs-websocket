use std::time::Duration;

use http::{HeaderMap, header::AUTHORIZATION};
use qubit_websocket::{WsClient, WsMessage};
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

    let client = WsClient::builder()
        .headers(headers)
        .subprotocols(vec!["echo.v1".to_owned()])
        .build()?;
    let mut session = client.connect(&url).await?;
    session
        .send(WsMessage::Text("hello over wss".to_owned()))
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;

    let reply = timeout(Duration::from_secs(10), session.receive())
        .await?
        .ok_or_else(|| std::io::Error::other("peer ended the session without a terminal message"))?
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    match reply {
        WsMessage::Text(reply) => println!("received text ({} bytes)", reply.len()),
        WsMessage::Binary(reply) => println!("received binary ({} bytes)", reply.len()),
    }
    session.close().await?;
    Ok(())
}
