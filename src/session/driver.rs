use futures_util::{Sink, SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

use crate::error::{WsErrorKind, WsTermination};
use crate::message::WsMessage;
use crate::session::queue::BoundedQueue;

type WsSocket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WsRead = futures_util::stream::SplitStream<WsSocket>;
type WsWrite = futures_util::stream::SplitSink<WsSocket, Message>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CloseRequest {
    Local,
    SlowConsumer,
}

pub(super) async fn run(
    socket: WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    inbound: BoundedQueue,
    outbound: BoundedQueue,
    close: watch::Receiver<Option<CloseRequest>>,
    close_tx: watch::Sender<Option<CloseRequest>>,
    terminal: Arc<Mutex<Option<WsTermination>>>,
    terminal_changed: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
) {
    let (sink, stream) = socket.split();
    let ping = Arc::new(Notify::new());
    let (peer_closed, peer_closed_rx) = watch::channel(None);
    let _peer_closed_keepalive = peer_closed.clone();
    let writer = write_loop(
        sink,
        outbound.clone(),
        close.clone(),
        peer_closed_rx,
        ping.clone(),
        no_progress_timeout,
        close_timeout,
    );
    let reader = read_loop(
        stream,
        inbound,
        close.clone(),
        close_tx,
        peer_closed,
        ping,
        no_progress_timeout,
        close_timeout,
    );
    let mut writer = tokio::spawn(writer);
    let mut reader = tokio::spawn(reader);
    let (read_result, write_result) = tokio::select! {
        biased;
        result = &mut reader => {
            let read_result = result.unwrap_or(ReadResult::Error(WsErrorKind::Connect));
            let write_result = match tokio::time::timeout(close_timeout, &mut writer).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Some(WsErrorKind::Connect),
                Err(_) => {
                    writer.abort();
                    Some(WsErrorKind::Timeout)
                }
            };
            (read_result, write_result)
        }
        result = &mut writer => {
            reader.abort();
            (
                ReadResult::LocalClose,
                result.unwrap_or(Some(WsErrorKind::Connect)),
            )
        }
    };
    outbound.close();

    let snapshot = match read_result {
        ReadResult::Error(kind) => WsTermination::Error { kind },
        ReadResult::Closed(code) => WsTermination::Normal { code },
        ReadResult::LocalClose => match write_result {
            Some(kind) => WsTermination::Error { kind },
            None => WsTermination::Normal { code: None },
        },
    };
    let mut state = terminal.lock().expect("terminal mutex poisoned");
    if state.is_none() {
        *state = Some(snapshot);
    }
    drop(state);
    terminal_changed.notify_waiters();
}

#[derive(Clone, Copy)]
enum ReadResult {
    Closed(Option<u16>),
    LocalClose,
    Error(WsErrorKind),
}

async fn read_loop(
    mut stream: WsRead,
    inbound: BoundedQueue,
    mut close: watch::Receiver<Option<CloseRequest>>,
    close_tx: watch::Sender<Option<CloseRequest>>,
    peer_closed: watch::Sender<Option<Option<u16>>>,
    ping: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
) -> ReadResult {
    let mut close_deadline = None;
    loop {
        tokio::select! {
            changed = close.changed() => {
                if changed.is_err() { return ReadResult::LocalClose; }
                if close.borrow().is_some() && close_deadline.is_none() {
                    close_deadline = Some(tokio::time::Instant::now() + close_timeout);
                }
            }
            _ = async {
                if let Some(deadline) = close_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => return ReadResult::Error(WsErrorKind::Timeout),
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let result = enqueue_while_polling_control(
                            &mut stream,
                            inbound.clone(),
                            WsMessage::Text(text.to_string()),
                            &mut close,
                            &ping,
                            no_progress_timeout,
                        ).await;
                        match result {
                            EnqueueResult::Queued => {},
                            EnqueueResult::Closed => return ReadResult::LocalClose,
                            EnqueueResult::PeerClosed(code) => {
                                let _ = peer_closed.send(Some(code));
                                return ReadResult::Closed(code);
                            }
                            EnqueueResult::SlowConsumer => {
                                let _ = close_tx.send(Some(CloseRequest::SlowConsumer));
                                return ReadResult::Error(WsErrorKind::Timeout);
                            }
                            EnqueueResult::Error(kind) => {
                                let _ = close_tx.send(Some(CloseRequest::Local));
                                return ReadResult::Error(kind);
                            }
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let result = enqueue_while_polling_control(
                            &mut stream,
                            inbound.clone(),
                            WsMessage::Binary(bytes),
                            &mut close,
                            &ping,
                            no_progress_timeout,
                        ).await;
                        match result {
                            EnqueueResult::Queued => {},
                            EnqueueResult::Closed => return ReadResult::LocalClose,
                            EnqueueResult::PeerClosed(code) => {
                                let _ = peer_closed.send(Some(code));
                                return ReadResult::Closed(code);
                            }
                            EnqueueResult::SlowConsumer => {
                                let _ = close_tx.send(Some(CloseRequest::SlowConsumer));
                                return ReadResult::Error(WsErrorKind::Timeout);
                            }
                            EnqueueResult::Error(kind) => {
                                let _ = close_tx.send(Some(CloseRequest::Local));
                                return ReadResult::Error(kind);
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_))) => { ping.notify_one(); }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        let code = frame.map(|frame| u16::from(frame.code));
                        let _ = peer_closed.send(Some(code));
                        return ReadResult::Closed(code);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed | tokio_tungstenite::tungstenite::Error::AlreadyClosed)) => {
                        let _ = peer_closed.send(Some(None));
                        return ReadResult::Closed(None);
                    }
                    Some(Err(error)) => {
                        let _ = close_tx.send(Some(CloseRequest::Local));
                        return ReadResult::Error(map_error(&error));
                    }
                    None => {
                        let _ = peer_closed.send(Some(None));
                        return ReadResult::Closed(None);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum EnqueueResult {
    Queued,
    Closed,
    PeerClosed(Option<u16>),
    SlowConsumer,
    Error(WsErrorKind),
}

async fn enqueue_while_polling_control(
    stream: &mut WsRead,
    inbound: BoundedQueue,
    message: WsMessage,
    close: &mut watch::Receiver<Option<CloseRequest>>,
    ping: &Notify,
    no_progress_timeout: Duration,
) -> EnqueueResult {
    let push = inbound.push(message);
    tokio::pin!(push);
    let deadline = tokio::time::sleep(no_progress_timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            result = &mut push => return match result {
                //
                Ok(()) => EnqueueResult::Queued,
                Err(_) => EnqueueResult::Closed,
            },
            changed = close.changed() => {
                if changed.is_err() || matches!(*close.borrow(), Some(CloseRequest::Local)) {
                    return EnqueueResult::Closed;
                }
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Ping(_))) => { ping.notify_one(); }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        return EnqueueResult::PeerClosed(frame.map(|frame| u16::from(frame.code)));
                    }
                    // At most one message is retained outside the bounded queue. If more
                    // application data arrives while it is full, terminate with 1013.
                    Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                        return EnqueueResult::SlowConsumer;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return EnqueueResult::Error(map_error(&error)),
                    None => return EnqueueResult::PeerClosed(None),
                }
            }
            _ = &mut deadline => return EnqueueResult::SlowConsumer,
        }
    }
}

async fn write_loop(
    mut sink: WsWrite,
    outbound: BoundedQueue,
    mut close: watch::Receiver<Option<CloseRequest>>,
    mut peer_closed: watch::Receiver<Option<Option<u16>>>,
    ping: Arc<Notify>,
    no_progress_timeout: Duration,
    close_timeout: Duration,
) -> Option<WsErrorKind> {
    loop {
        tokio::select! {
            biased;
            changed = peer_closed.changed() => {
                if changed.is_err() || peer_closed.borrow().is_some() {
                    return timeout_flush(&mut sink, close_timeout).await;
                }
            }
            changed = close.changed() => {
                if changed.is_err() { return None; }
                let request = *close.borrow();
                match request {
                    Some(CloseRequest::Local) => {
                        return match tokio::time::timeout(close_timeout, sink.close()).await {
                            Ok(Ok(())) => None,
                            Ok(Err(error)) => Some(map_error(&error)),
                            Err(_) => Some(WsErrorKind::Timeout),
                        };
                    }
                    Some(CloseRequest::SlowConsumer) => {
                        let close = Message::Close(Some(CloseFrame {
                            code: CloseCode::Again,
                            reason: "inbound queue full".into(),
                        }));
                        return match tokio::time::timeout(close_timeout, sink.send(close)).await {
                            Ok(Ok(())) => Some(WsErrorKind::Timeout),
                            Ok(Err(error)) => Some(map_error(&error)),
                            Err(_) => Some(WsErrorKind::Timeout),
                        };
                    }
                    None => {}
                }
            }
            _ = ping.notified() => {
                if let Some(kind) = timeout_flush(&mut sink, no_progress_timeout).await {
                    return Some(kind);
                }
            }
            outgoing = outbound.pop() => {
                let Some(envelope) = outgoing else { return None; };
                match send_envelope(
                    &mut sink,
                    envelope,
                    &mut close,
                    &mut peer_closed,
                    &ping,
                    no_progress_timeout,
                    close_timeout,
                ).await {
                    SendResult::Sent => {},
                    SendResult::Exit(kind) => return kind,
                }
            }
        }
    }
}

enum SendResult {
    Sent,
    Exit(Option<WsErrorKind>),
}

async fn send_envelope<S>(
    sink: &mut S,
    envelope: crate::session::queue::Envelope,
    close: &mut watch::Receiver<Option<CloseRequest>>,
    peer_closed: &mut watch::Receiver<Option<Option<u16>>>,
    ping: &Notify,
    no_progress_timeout: Duration,
    close_timeout: Duration,
) -> SendResult
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let crate::session::queue::Envelope {
        item,
        message_permit: _message_permit,
        byte_permit: _byte_permit,
    } = envelope;
    let mut message = Some(match item {
        WsMessage::Text(text) => Message::Text(text.into()),
        WsMessage::Binary(bytes) => Message::Binary(bytes),
    });
    let mut started = false;
    let deadline = tokio::time::Instant::now() + no_progress_timeout;

    loop {
        let event = tokio::select! {
            biased;
            changed = peer_closed.changed() => {
                if changed.is_err() || peer_closed.borrow().is_some() {
                    SendEvent::PeerClosed
                } else {
                    continue;
                }
            }
            changed = close.changed() => {
                if changed.is_err() {
                    SendEvent::Close(None)
                } else {
                    SendEvent::Close(*close.borrow())
                }
            }
            _ = ping.notified() => SendEvent::Ping,
            result = tokio::time::timeout_at(deadline, std::future::poll_fn(|context| {
                if started { sink.poll_flush_unpin(context) } else { sink.poll_ready_unpin(context) }
            })) => SendEvent::Progress(result),
        };

        match event {
            SendEvent::PeerClosed => {
                return SendResult::Exit(timeout_flush(sink, close_timeout).await);
            }
            SendEvent::Close(Some(CloseRequest::Local)) | SendEvent::Close(None) => {
                return SendResult::Exit(
                    match tokio::time::timeout(close_timeout, sink.close()).await {
                        Ok(Ok(())) => Some(WsErrorKind::Timeout),
                        Ok(Err(error)) => Some(map_error(&error)),
                        Err(_) => Some(WsErrorKind::Timeout),
                    },
                );
            }
            SendEvent::Close(Some(CloseRequest::SlowConsumer)) => {
                let close = Message::Close(Some(CloseFrame {
                    code: CloseCode::Again,
                    reason: "inbound queue full".into(),
                }));
                return SendResult::Exit(
                    match tokio::time::timeout(close_timeout, sink.send(close)).await {
                        Ok(Ok(())) => Some(WsErrorKind::Timeout),
                        Ok(Err(error)) => Some(map_error(&error)),
                        Err(_) => Some(WsErrorKind::Timeout),
                    },
                );
            }
            SendEvent::Ping => {
                if let Some(kind) = timeout_flush_until(sink, deadline).await {
                    return SendResult::Exit(Some(kind));
                }
                if started {
                    return SendResult::Sent;
                }
            }
            SendEvent::Progress(Err(_)) => return SendResult::Exit(Some(WsErrorKind::Timeout)),
            SendEvent::Progress(Ok(Err(error))) => {
                return SendResult::Exit(Some(map_error(&error)));
            }
            SendEvent::Progress(Ok(Ok(()))) if started => return SendResult::Sent,
            SendEvent::Progress(Ok(Ok(()))) => {
                if let Err(error) =
                    sink.start_send_unpin(message.take().expect("message not yet sent"))
                {
                    return SendResult::Exit(Some(map_error(&error)));
                }
                started = true;
            }
        }
    }
}

enum SendEvent {
    Progress(
        Result<Result<(), tokio_tungstenite::tungstenite::Error>, tokio::time::error::Elapsed>,
    ),
    Ping,
    Close(Option<CloseRequest>),
    PeerClosed,
}

async fn timeout_flush<S>(sink: &mut S, timeout: Duration) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(timeout, sink.flush()).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(map_error(&error)),
        Err(_) => Some(WsErrorKind::Timeout),
    }
}

async fn timeout_flush_until<S>(sink: &mut S, deadline: tokio::time::Instant) -> Option<WsErrorKind>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout_at(deadline, sink.flush()).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(map_error(&error)),
        Err(_) => Some(WsErrorKind::Timeout),
    }
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

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures_util::Sink;
    use tokio::sync::Notify;
    use tokio::sync::watch;
    use tokio_tungstenite::tungstenite::Message;

    use super::{CloseRequest, SendResult, send_envelope};
    use crate::message::WsMessage;
    use crate::session::queue::BoundedQueue;

    #[derive(Default)]
    struct SinkState {
        events: Vec<&'static str>,
        ready: bool,
    }

    struct ControlledSink {
        state: Arc<Mutex<SinkState>>,
        poll_ready_seen: Arc<Notify>,
    }

    impl Sink<Message> for ControlledSink {
        type Error = tokio_tungstenite::tungstenite::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("sink state poisoned");
            state.events.push("poll_ready");
            self.poll_ready_seen.notify_one();
            if state.ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            self.state
                .lock()
                .expect("sink state poisoned")
                .events
                .push("start_send");
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("sink state poisoned");
            state.events.push("flush");
            state.ready = true;
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.poll_flush(context)
        }
    }

    #[tokio::test]
    async fn ping_flush_interrupts_pending_business_poll_ready() {
        let queue = BoundedQueue::new(1, 16);
        queue
            .try_push(WsMessage::Text("payload".to_owned()))
            .expect("queue has capacity");
        let envelope = queue.pop().await.expect("queued envelope");

        let state = Arc::new(Mutex::new(SinkState::default()));
        let poll_ready_seen = Arc::new(Notify::new());
        let mut sink = ControlledSink {
            state: state.clone(),
            poll_ready_seen: poll_ready_seen.clone(),
        };
        let ping = Notify::new();
        let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
        let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
        let send = send_envelope(
            &mut sink,
            envelope,
            &mut close,
            &mut peer_closed,
            &ping,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        tokio::pin!(send);

        tokio::select! {
            _ = poll_ready_seen.notified() => {},
            _ = &mut send => panic!("send finished before pending poll_ready was interrupted"),
        }
        ping.notify_one();
        assert!(matches!(send.await, SendResult::Sent));

        let events = &state.lock().expect("sink state poisoned").events;
        let control_flush = events
            .iter()
            .position(|event| *event == "flush")
            .expect("Ping should trigger a flush");
        let business_send = events
            .iter()
            .position(|event| *event == "start_send")
            .expect("business message should eventually be sent");
        assert!(control_flush < business_send, "events: {events:?}");
    }
}
