use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{Notify, watch};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::error::{WsErrorKind, WsTermination};
use crate::message::WsMessage;
use crate::session::queue::BoundedQueue;

pub(super) async fn run(
    mut socket: WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    inbound: BoundedQueue,
    outbound: BoundedQueue,
    mut close: watch::Receiver<bool>,
    terminal: Arc<Mutex<Option<WsTermination>>>,
    terminal_changed: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
) {
    let mut closing = false;
    let mut close_deadline = None;
    let (kind, code) = loop {
        if *close.borrow() && !closing {
            closing = true;
            outbound.close();
            close_deadline = Some(tokio::time::Instant::now() + close_timeout);
            match tokio::time::timeout(close_timeout, socket.close(None)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => break (Some(map_error(&error)), None),
                Err(_) => break (Some(WsErrorKind::Timeout), None),
            }
        }

        if let Some(deadline) = close_deadline {
            if tokio::time::Instant::now() >= deadline {
                break (Some(WsErrorKind::Timeout), None);
            }
        }

        tokio::select! {
            changed = close.changed(), if !closing => {
                if changed.is_err() || *close.borrow() {
                    closing = true;
                    outbound.close();
                    close_deadline = Some(tokio::time::Instant::now() + close_timeout);
                    match tokio::time::timeout(close_timeout, socket.close(None)).await {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => break (Some(map_error(&error)), None),
                        Err(_) => break (Some(WsErrorKind::Timeout), None),
                    }
                }
            }
            outgoing = outbound.pop(), if !closing => {
                let Some(envelope) = outgoing else { continue; };
                let message = match envelope.item() {
                    WsMessage::Text(text) => Message::Text(text.clone().into()),
                    WsMessage::Binary(bytes) => Message::Binary(bytes.clone()),
                };
                match tokio::time::timeout(no_progress_timeout, socket.send(message)).await {
                    Ok(Ok(())) => { drop(envelope); },
                    Ok(Err(error)) => break (Some(map_error(&error)), None),
                    Err(_) => break (Some(WsErrorKind::Timeout), None),
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let msg = WsMessage::Text(text.to_string());
                        match tokio::time::timeout(no_progress_timeout, inbound.push(msg)).await {
                            Ok(Ok(())) => {},
                            Ok(Err(_)) => break (None, None),
                            Err(_) => break (Some(WsErrorKind::Timeout), None),
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        match tokio::time::timeout(no_progress_timeout, inbound.push(WsMessage::Binary(bytes))).await {
                            Ok(Ok(())) => {},
                            Ok(Err(_)) => break (None, None),
                            Err(_) => break (Some(WsErrorKind::Timeout), None),
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {
                        if let Err(error) = tokio::time::timeout(no_progress_timeout, socket.flush()).await.unwrap_or_else(|_| Err(tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")))) {
                            break (Some(map_error(&error)), None);
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        let code = frame.map(|frame| u16::from(frame.code));
                        let _ = tokio::time::timeout(close_timeout, socket.flush()).await;
                        break (None, code);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed | tokio_tungstenite::tungstenite::Error::AlreadyClosed)) => break (None, None),
                    Some(Err(error)) => break (Some(map_error(&error)), None),
                    None => break (None, None),
                }
            }
            _ = async {
                if let Some(deadline) = close_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => break (Some(WsErrorKind::Timeout), None),
        }
    };

    outbound.close();
    let snapshot = match kind {
        Some(kind) => WsTermination::Error { kind },
        None => WsTermination::Normal { code },
    };
    let mut state = terminal.lock().expect("terminal mutex poisoned");
    if state.is_none() {
        *state = Some(snapshot);
    }
    drop(state);
    terminal_changed.notify_waiters();
}

fn map_error(error: &tokio_tungstenite::tungstenite::Error) -> WsErrorKind {
    use tokio_tungstenite::tungstenite::Error as TungsteniteError;
    match error {
        TungsteniteError::Capacity(_) => WsErrorKind::MessageTooLarge,
        TungsteniteError::Protocol(_) => WsErrorKind::Protocol,
        TungsteniteError::Io(source) if source.kind() == std::io::ErrorKind::TimedOut => {
            WsErrorKind::Timeout
        }
        _ => WsErrorKind::Connect,
    }
}
