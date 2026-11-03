// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::pin::Pin;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use futures_util::Sink;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use super::BoundedQueue;
use super::CapacityLimits;
use super::CloseRequest;
use super::DriverContext;
use super::ReadResult;
use super::SendResult;
use super::TungsteniteError;
use super::complete_after_reader;
use super::enqueue_while_polling_control;
use super::finish_after_writer;
use super::read_loop;
use super::resolve_termination;
use super::run_parts;
use super::send_envelope;
use super::timeout_flush;
use super::write_loop;
use crate::error::WsErrorKind;
use crate::error::WsSendError;
use crate::error::WsTermination;
use crate::options::WsSessionLimits;
use crate::session::WsSession;
use crate::session::internal::Owner;
use crate::ws_message::WsMessage;

#[tokio::test]
async fn test_idle_reader_is_not_bounded_by_no_progress_timeout() {
    let inbound = BoundedQueue::new(1, 8);
    let (close_tx, close_rx) = watch::channel(None);
    let (peer_closed, _peer_rx) = watch::channel(None);
    let reader = read_loop(
        futures_util::stream::pending(),
        inbound,
        close_rx,
        close_tx.clone(),
        peer_closed,
        Arc::new(Notify::new()),
        Duration::from_millis(10),
        Duration::from_millis(20),
        CapacityLimits::default(),
    );
    tokio::pin!(reader);
    assert!(
        tokio::time::timeout(Duration::from_millis(80), reader.as_mut())
            .await
            .is_err()
    );
    close_tx.send(Some(CloseRequest::Local)).expect("request close");
    let result = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("local close still bounds the pending reader");
    assert!(matches!(result, ReadResult::Error(WsErrorKind::Timeout)));
}

#[tokio::test]
async fn test_reader_ignores_application_data_after_local_close_but_observes_peer_close() {
    let inbound = BoundedQueue::new(1, 8);
    inbound.close();
    let (close_tx, close_rx) = watch::channel(Some(CloseRequest::Local));
    let (peer_closed, _) = watch::channel(None);
    let stream = futures_util::stream::iter([Ok(Message::Text("discarded".into())), Ok(Message::Close(None))]);

    let result = read_loop(
        stream,
        inbound,
        close_rx,
        close_tx.clone(),
        peer_closed,
        Arc::new(Notify::new()),
        Duration::from_secs(1),
        Duration::from_secs(1),
        CapacityLimits::default(),
    )
    .await;

    assert!(matches!(result, ReadResult::Closed(None)));
    drop(close_tx);
}

#[tokio::test]
async fn test_reader_reports_unexpected_inbound_queue_close_as_internal_error() {
    let inbound = BoundedQueue::new(1, 8);
    inbound.close();
    let (close_tx, close_rx) = watch::channel(None);
    let (peer_closed, _) = watch::channel(None);
    let stream = futures_util::stream::iter([Ok(Message::Text("unexpected".into()))]);

    let result = read_loop(
        stream,
        inbound,
        close_rx,
        close_tx.clone(),
        peer_closed,
        Arc::new(Notify::new()),
        Duration::from_secs(1),
        Duration::from_secs(1),
        CapacityLimits::default(),
    )
    .await;

    assert!(matches!(result, ReadResult::Error(WsErrorKind::Internal)));
    drop(close_tx);
}

#[tokio::test]
async fn test_extra_inbound_message_reports_overflow_before_deadline() {
    let inbound = BoundedQueue::new(1, 8);
    inbound
        .try_push(WsMessage::Text("full".into()))
        .expect("the single queue slot is available");
    let mut stream = futures_util::stream::iter([Ok(Message::Text("extra".into()))]);
    let (close_tx, mut close_rx) = watch::channel(None);
    let ping = Notify::new();

    let result = enqueue_while_polling_control(
        &mut stream,
        inbound,
        WsMessage::Text("waiting".into()),
        &mut close_rx,
        &ping,
        Duration::from_secs(30),
        CapacityLimits::default(),
    )
    .await;

    assert!(matches!(result, super::EnqueueResult::Overflow));
    drop(close_tx);
}

#[tokio::test]
async fn test_inbound_enqueue_reports_stalled_when_no_progress_deadline_expires() {
    let inbound = BoundedQueue::new(1, 8);
    inbound
        .try_push(WsMessage::Text("full".into()))
        .expect("the single queue slot is available");
    let mut stream = futures_util::stream::pending();
    let (close_tx, mut close_rx) = watch::channel(None);
    let ping = Notify::new();

    let result = enqueue_while_polling_control(
        &mut stream,
        inbound,
        WsMessage::Text("waiting".into()),
        &mut close_rx,
        &ping,
        Duration::from_millis(10),
        CapacityLimits::default(),
    )
    .await;

    assert!(matches!(result, super::EnqueueResult::Stalled));
    drop(close_tx);
}

#[tokio::test]
async fn test_control_frames_do_not_extend_inbound_deadline() {
    const PROBE_ENV: &str = "WS_CONTROL_FRAME_DEADLINE_PROBE";
    if std::env::var_os(PROBE_ENV).is_some() {
        let inbound = BoundedQueue::new(1, 16);
        inbound
            .try_push(WsMessage::Text("full".into()))
            .expect("the queue is full before the waiting message");
        let started = std::time::Instant::now();
        let controls = Arc::new(AtomicUsize::new(0));
        let controls_seen = controls.clone();
        let mut stream = futures_util::stream::poll_fn(move |_| {
            if started.elapsed() >= Duration::from_secs(1) {
                return Poll::Ready(Some(Err(TungsteniteError::ConnectionClosed)));
            }
            controls_seen.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Some(Ok(Message::Ping(Vec::new().into()))))
        });
        let (close_tx, mut close_rx) = watch::channel(None);

        let result = enqueue_while_polling_control(
            &mut stream,
            inbound,
            WsMessage::Text("waiting".into()),
            &mut close_rx,
            &Notify::new(),
            Duration::from_millis(20),
            CapacityLimits::default(),
        )
        .await;

        let controls = controls.load(Ordering::Relaxed);
        assert!(controls > 0, "the probe must process ready control frames");
        eprintln!(
            "controlled ready stream: controls={controls}, elapsed={:?}",
            started.elapsed()
        );
        assert!(
            matches!(result, super::EnqueueResult::Stalled),
            "control frames must not extend the enqueue deadline; result={result:?}, controls={controls}"
        );
        drop(close_tx);
        return;
    }

    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .arg("--exact")
        .arg("session::internal::driver::tests::test_control_frames_do_not_extend_inbound_deadline")
        .arg("--nocapture")
        .env(PROBE_ENV, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start control-frame deadline probe child");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll control-frame deadline child") {
            let output = child.wait_with_output().expect("collect probe output");
            assert!(
                status.success(),
                "control-frame deadline probe failed: {status}\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().expect("kill stuck control-frame deadline child");
    let output = child.wait_with_output().expect("collect stuck probe output");
    panic!(
        "control-frame deadline probe exceeded its wall-clock limit\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_send_rejects_unrepresentable_no_progress_deadline() {
    let queue = BoundedQueue::new(1, 16);
    queue
        .try_push(WsMessage::Text("payload".into()))
        .expect("queue has capacity");
    let envelope = queue.try_pop().expect("queued envelope");
    let state = Arc::new(Mutex::new(SinkState::default()));
    let mut sink = ControlledSink {
        state,
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);

    let result = send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &Notify::new(),
        Duration::MAX,
        Duration::from_secs(1),
        CapacityLimits::default(),
    )
    .await;

    assert!(matches!(result, SendResult::Exit(Some(WsErrorKind::InvalidConfig))));
    assert_eq!(queue.usage(), (0, 0), "failed send releases its reservation");
}

#[tokio::test]
async fn test_reader_rejects_unrepresentable_close_deadline() {
    let (close_tx, close_rx) = watch::channel(Some(CloseRequest::Local));
    let (peer_closed, _) = watch::channel(None);

    let result = read_loop(
        futures_util::stream::pending(),
        BoundedQueue::new(1, 16),
        close_rx,
        close_tx.clone(),
        peer_closed,
        Arc::new(Notify::new()),
        Duration::from_secs(1),
        Duration::MAX,
        CapacityLimits::default(),
    )
    .await;

    assert!(matches!(result, ReadResult::Error(WsErrorKind::InvalidConfig)));
    drop(close_tx);
}

#[tokio::test]
async fn test_inbound_enqueue_queues_waiting_message_after_capacity_recovers() {
    let inbound = BoundedQueue::new(1, 8);
    inbound
        .try_push(WsMessage::Text("full".into()))
        .expect("the single queue slot is available");
    let mut stream = futures_util::stream::pending();
    let (close_tx, mut close_rx) = watch::channel(None);
    let ping = Arc::new(Notify::new());
    let queue_for_enqueue = inbound.clone();
    let enqueue = tokio::spawn(async move {
        enqueue_while_polling_control(
            &mut stream,
            queue_for_enqueue,
            WsMessage::Text("waiting".into()),
            &mut close_rx,
            &ping,
            Duration::from_secs(1),
            CapacityLimits::default(),
        )
        .await
    });

    assert_eq!(
        inbound.try_pop().expect("the original message is queued").into_item(),
        WsMessage::Text("full".into())
    );
    assert!(matches!(
        enqueue.await.expect("enqueue task exits"),
        super::EnqueueResult::Queued
    ));
    assert_eq!(
        inbound
            .try_pop()
            .expect("the waiting message is now queued")
            .into_item(),
        WsMessage::Text("waiting".into())
    );
    drop(close_tx);
}

#[tokio::test]
async fn test_peer_close_flush_failure_reaches_session_api() {
    let inbound = BoundedQueue::new(1, 16);
    let outbound = BoundedQueue::new(1, 16);
    let (close, close_rx) = watch::channel(None);
    let (close_tx, _close_keepalive) = watch::channel(None);
    let terminal = Arc::new(Mutex::new(None));
    let terminal_changed = Arc::new(Notify::new());
    let state = Arc::new(Mutex::new(SinkState {
        flush: FlushBehavior::Error,
        ..SinkState::default()
    }));
    let sink = ControlledSink {
        state,
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let stream = futures_util::stream::iter([Ok(Message::Close(None))]);
    let driver = tokio::spawn(run_parts(
        sink,
        stream,
        DriverContext {
            inbound: inbound.clone(),
            outbound: outbound.clone(),
            close: close_rx,
            close_tx,
            terminal: terminal.clone(),
            terminal_changed: terminal_changed.clone(),
            limits: WsSessionLimits {
                max_inbound_frame_bytes: 16,
                max_inbound_message_bytes: 16,
                no_progress_timeout: Duration::from_secs(1),
                close_timeout: Duration::from_secs(1),
                ..WsSessionLimits::default()
            },
        },
    ));
    let abort = driver.abort_handle();
    let owner = Arc::new(Owner {
        inbound,
        outbound,
        close,
        terminal,
        terminal_changed,
        close_timeout: Duration::from_secs(1),
        close_result: Mutex::new(None),
        close_gate: tokio::sync::Mutex::new(()),
        task: tokio::sync::Mutex::new(Some(driver)),
        abort,
    });
    let mut session = WsSession {
        owner,
        error_delivered: false,
        response_status_code: 101,
        selected_subprotocol: None,
    };

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), session.receive())
            .await
            .expect("driver should publish the flush error"),
        Some(Err(error)) if error.kind() == WsErrorKind::Transport
    ));
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Transport
        })
    );
    assert!(session.receive().await.is_none());
    assert_eq!(
        session
            .close()
            .await
            .expect_err("failed close flush must reach close()")
            .kind(),
        WsErrorKind::Transport
    );
}

#[tokio::test]
async fn test_peer_close_rejects_sends_before_flush_completes() {
    let inbound = BoundedQueue::new(1, 16);
    let outbound = BoundedQueue::new(1, 16);
    outbound
        .try_push(WsMessage::Text("queued".into()))
        .expect("initial outbound message fits");
    let waiting_outbound = outbound.clone();
    let pending_push = tokio::spawn(async move { waiting_outbound.push(WsMessage::Text("waiting".into())).await });
    let (close_tx, close_rx) = watch::channel(None);
    let close = close_tx.clone();
    let terminal = Arc::new(Mutex::new(None));
    let terminal_changed = Arc::new(Notify::new());
    let state = Arc::new(Mutex::new(SinkState {
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let flush_seen = Arc::new(Notify::new());
    let sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: flush_seen.clone(),
    };
    let stream = futures_util::stream::iter([Ok(Message::Close(Some(
        tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
            reason: "peer close".into(),
        },
    )))]);
    let mut driver = tokio::spawn(run_parts(
        sink,
        stream,
        DriverContext {
            inbound,
            outbound: outbound.clone(),
            close: close_rx,
            close_tx,
            terminal: terminal.clone(),
            terminal_changed,
            limits: WsSessionLimits {
                max_inbound_frame_bytes: 16,
                max_inbound_message_bytes: 16,
                no_progress_timeout: Duration::from_secs(1),
                close_timeout: Duration::from_secs(5),
                ..WsSessionLimits::default()
            },
        },
    ));

    tokio::select! {
        _ = flush_seen.notified() => {},
        result = &mut driver => panic!("driver finished before gated flush: {result:?}"),
    }
    assert_eq!(
        outbound.try_push(WsMessage::Text("late".into())),
        Err(crate::error::WsSendError::Closed),
        "reader completion must close outbound before waiting for writer flush"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pending_push)
            .await
            .expect("closing outbound wakes a capacity waiter")
            .expect("pending producer task exits"),
        Err(crate::error::WsSendError::Closed)
    );

    release_flush(&state, FlushBehavior::Success);
    tokio::time::timeout(Duration::from_secs(1), driver)
        .await
        .expect("driver exits after gated flush")
        .expect("driver task succeeds");
    assert_eq!(outbound.usage(), (0, 0));
    assert_eq!(
        *terminal.lock().expect("terminal mutex poisoned"),
        Some(WsTermination::Closed { code: Some(1000) })
    );
    drop(close);
}

async fn assert_reader_failure_closes_outbound_before_writer_completion(
    stream: impl Stream<Item = Result<Message, TungsteniteError>> + Unpin + Send + 'static,
    inbound: BoundedQueue,
    expected_kind: WsErrorKind,
    wait_for_flush: bool,
) {
    let outbound = BoundedQueue::new(1, 16);
    outbound
        .try_push(WsMessage::Text("queued".into()))
        .expect("initial outbound message fits");
    let waiting_outbound = outbound.clone();
    let pending_push = tokio::spawn(async move { waiting_outbound.push(WsMessage::Text("waiting".into())).await });
    // Match WsSession::from_connected: reader and writer share one close watch.
    let (close_tx, close_rx) = watch::channel(None);
    let close = close_tx.clone();
    let terminal = Arc::new(Mutex::new(None));
    let flush_seen = Arc::new(Notify::new());
    let close_seen = Arc::new(Notify::new());
    let state = Arc::new(Mutex::new(SinkState {
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: close_seen.clone(),
        flush_seen: flush_seen.clone(),
    };
    let mut driver = tokio::spawn(run_parts(
        sink,
        stream,
        DriverContext {
            inbound: inbound.clone(),
            outbound: outbound.clone(),
            close: close_rx,
            close_tx,
            terminal: terminal.clone(),
            terminal_changed: Arc::new(Notify::new()),
            limits: WsSessionLimits {
                max_inbound_frame_bytes: 16,
                max_inbound_message_bytes: 16,
                no_progress_timeout: Duration::from_millis(20),
                close_timeout: Duration::from_millis(200),
                ..WsSessionLimits::default()
            },
        },
    ));

    tokio::select! {
        _ = async {
            if wait_for_flush {
                flush_seen.notified().await;
            } else {
                close_seen.notified().await;
            }
        } => {},
        result = &mut driver => panic!("driver finished before writer gate: {result:?}"),
    }
    assert_eq!(
        outbound.try_push(WsMessage::Text("late".into())),
        Err(crate::error::WsSendError::Closed),
        "reader failure must close outbound before waiting for the writer"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pending_push)
            .await
            .expect("closing outbound wakes a capacity waiter")
            .expect("pending producer task exits"),
        Err(crate::error::WsSendError::Closed)
    );

    if wait_for_flush {
        release_flush(&state, FlushBehavior::Success);
    }
    tokio::time::timeout(Duration::from_secs(1), driver)
        .await
        .expect("driver completes after the bounded writer wait")
        .expect("driver task succeeds");
    assert_eq!(outbound.usage(), (0, 0));
    assert_eq!(
        *terminal.lock().expect("terminal mutex poisoned"),
        Some(WsTermination::Error { kind: expected_kind })
    );
    assert_eq!(
        inbound
            .try_pop()
            .expect("previously accepted inbound message remains readable")
            .into_item(),
        WsMessage::Text("already accepted".into())
    );
    assert_eq!(inbound.usage(), (0, 0));
    drop(close);
}

#[tokio::test]
async fn test_reader_io_error_closes_outbound_and_preserves_transport_classification() {
    let inbound = BoundedQueue::new(1, 16);
    inbound
        .try_push(WsMessage::Text("already accepted".into()))
        .expect("initial inbound message fits");
    assert_reader_failure_closes_outbound_before_writer_completion(
        futures_util::stream::iter([Err(TungsteniteError::Io(std::io::Error::other(
            "controlled reader failure",
        )))]),
        inbound,
        WsErrorKind::Transport,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_inbound_overflow_closes_outbound_and_preserves_classification() {
    let inbound = BoundedQueue::new(1, 16);
    inbound
        .try_push(WsMessage::Text("already accepted".into()))
        .expect("initial inbound message fits");
    assert_reader_failure_closes_outbound_before_writer_completion(
        futures_util::stream::iter([
            Ok(Message::Text("blocked".into())),
            Ok(Message::Text("overflow".into())),
        ]),
        inbound,
        WsErrorKind::InboundBackpressure,
        true,
    )
    .await;
}

#[tokio::test]
async fn test_inbound_stall_closes_outbound_and_preserves_timeout_classification() {
    let inbound = BoundedQueue::new(1, 16);
    inbound
        .try_push(WsMessage::Text("already accepted".into()))
        .expect("initial inbound message fits");
    assert_reader_failure_closes_outbound_before_writer_completion(
        futures_util::stream::iter([Ok(Message::Text("blocked".into()))]).chain(futures_util::stream::pending()),
        inbound,
        WsErrorKind::Timeout,
        true,
    )
    .await;
}

#[tokio::test]
async fn test_closed_outbound_without_close_request_is_internal_error() {
    let outbound = BoundedQueue::new(1, 16);
    outbound.close();
    let (_close_tx, close) = watch::channel(None);
    let (_peer_closed_tx, peer_closed) = watch::channel(None);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = ControlledSink {
        state,
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };

    let result = write_loop(
        sink,
        outbound,
        close,
        peer_closed,
        Arc::new(Notify::new()),
        Duration::from_secs(1),
        Duration::from_secs(1),
        CapacityLimits::default(),
    )
    .await;

    assert_eq!(result, Some(WsErrorKind::Internal));
}

struct DropProbeStream(Arc<AtomicBool>);

impl Stream for DropProbeStream {
    type Item = Result<Message, TungsteniteError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Drop for DropProbeStream {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct PanickingStream;

impl Stream for PanickingStream {
    type Item = Result<Message, TungsteniteError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        panic!("controlled reader poll panic");
    }
}

struct DropProbeSink {
    terminal: Arc<Mutex<Option<WsTermination>>>,
    dropped: Arc<AtomicBool>,
}

impl Sink<Message> for DropProbeSink {
    type Error = TungsteniteError;

    fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
        unreachable!("the drop probe sink never becomes ready")
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

impl Drop for DropProbeSink {
    fn drop(&mut self) {
        assert!(
            self.terminal.lock().expect("terminal mutex poisoned").is_none(),
            "writer future must be dropped before terminal publication"
        );
        self.dropped.store(true, Ordering::SeqCst);
    }
}

struct TerminalDropProbeStream {
    terminal: Arc<Mutex<Option<WsTermination>>>,
    dropped: Arc<AtomicBool>,
}

impl Stream for TerminalDropProbeStream {
    type Item = Result<Message, TungsteniteError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Drop for TerminalDropProbeStream {
    fn drop(&mut self) {
        assert!(
            self.terminal.lock().expect("terminal mutex poisoned").is_none(),
            "reader future must be dropped before terminal publication"
        );
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn test_driver_drops_reader_before_publishing_writer_first_terminal() {
    let inbound = BoundedQueue::new(1, 16);
    let outbound = BoundedQueue::new(1, 16);
    outbound.close();
    let (_close_tx, close_rx) = watch::channel(None);
    let (close_tx, _close_keepalive) = watch::channel(None);
    let terminal = Arc::new(Mutex::new(None));
    let terminal_changed = Arc::new(Notify::new());
    let reader_dropped = Arc::new(AtomicBool::new(false));
    let sink = ControlledSink {
        state: Arc::new(Mutex::new(SinkState::default())),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };

    run_parts(
        sink,
        DropProbeStream(reader_dropped.clone()),
        DriverContext {
            inbound,
            outbound,
            close: close_rx,
            close_tx,
            terminal: terminal.clone(),
            terminal_changed,
            limits: WsSessionLimits {
                close_timeout: Duration::from_millis(10),
                ..WsSessionLimits::default()
            },
        },
    )
    .await;

    assert_eq!(
        *terminal.lock().expect("terminal mutex poisoned"),
        Some(WsTermination::Error {
            kind: WsErrorKind::Internal
        })
    );
    assert!(reader_dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_reader_poll_panic_is_published_as_internal_error() {
    let inbound = BoundedQueue::new(1, 16);
    let outbound = BoundedQueue::new(1, 16);
    let (_close_tx, close_rx) = watch::channel(None);
    let (close_tx, _close_keepalive) = watch::channel(None);
    let terminal = Arc::new(Mutex::new(None));
    let writer_dropped = Arc::new(AtomicBool::new(false));

    run_parts(
        DropProbeSink {
            terminal: terminal.clone(),
            dropped: writer_dropped.clone(),
        },
        PanickingStream,
        DriverContext {
            inbound,
            outbound,
            close: close_rx,
            close_tx,
            terminal: terminal.clone(),
            terminal_changed: Arc::new(Notify::new()),
            limits: WsSessionLimits {
                close_timeout: Duration::from_millis(10),
                ..WsSessionLimits::default()
            },
        },
    )
    .await;

    assert_eq!(
        *terminal.lock().expect("terminal mutex poisoned"),
        Some(WsTermination::Error {
            kind: WsErrorKind::Internal
        })
    );
    assert!(writer_dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_writer_poll_panic_is_published_as_internal_error() {
    let inbound = BoundedQueue::new(1, 16);
    let outbound = BoundedQueue::new(1, 16);
    outbound
        .try_push(WsMessage::Text("outbound".to_owned()))
        .expect("outbound queue has capacity");
    let (_close_tx, close_rx) = watch::channel(None);
    let (close_tx, _close_keepalive) = watch::channel(None);
    let terminal = Arc::new(Mutex::new(None));
    let reader_dropped = Arc::new(AtomicBool::new(false));

    run_parts(
        ControlledSink {
            state: Arc::new(Mutex::new(SinkState {
                panic_on_ready: true,
                ..SinkState::default()
            })),
            poll_ready_seen: Arc::new(Notify::new()),
            close_seen: Arc::new(Notify::new()),
            flush_seen: Arc::new(Notify::new()),
        },
        TerminalDropProbeStream {
            terminal: terminal.clone(),
            dropped: reader_dropped.clone(),
        },
        DriverContext {
            inbound,
            outbound,
            close: close_rx,
            close_tx,
            terminal: terminal.clone(),
            terminal_changed: Arc::new(Notify::new()),
            limits: WsSessionLimits {
                close_timeout: Duration::from_millis(10),
                ..WsSessionLimits::default()
            },
        },
    )
    .await;

    assert_eq!(
        *terminal.lock().expect("terminal mutex poisoned"),
        Some(WsTermination::Error {
            kind: WsErrorKind::Internal
        })
    );
    assert!(reader_dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_reader_first_timeout_drops_writer_before_terminal_publication() {
    let inbound = BoundedQueue::new(1, 16);
    let outbound = BoundedQueue::new(1, 16);
    let (_close_tx, close_rx) = watch::channel(None);
    let (close_tx, _close_keepalive) = watch::channel(None);
    let terminal = Arc::new(Mutex::new(None));
    let writer_dropped = Arc::new(AtomicBool::new(false));

    run_parts(
        DropProbeSink {
            terminal: terminal.clone(),
            dropped: writer_dropped.clone(),
        },
        futures_util::stream::iter([Ok(Message::Close(None))]),
        DriverContext {
            inbound,
            outbound,
            close: close_rx,
            close_tx,
            terminal: terminal.clone(),
            terminal_changed: Arc::new(Notify::new()),
            limits: WsSessionLimits {
                close_timeout: Duration::from_millis(10),
                ..WsSessionLimits::default()
            },
        },
    )
    .await;

    assert_eq!(
        *terminal.lock().expect("terminal mutex poisoned"),
        Some(WsTermination::Error {
            kind: WsErrorKind::Timeout
        })
    );
    assert!(writer_dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_closed_close_signal_without_request_is_internal_error() {
    let outbound = BoundedQueue::new(1, 16);
    let (close_tx, close) = watch::channel(None);
    drop(close_tx);
    let (_peer_closed_tx, peer_closed) = watch::channel(None);
    let sink = ControlledSink {
        state: Arc::new(Mutex::new(SinkState::default())),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };

    let result = write_loop(
        sink,
        outbound,
        close,
        peer_closed,
        Arc::new(Notify::new()),
        Duration::from_secs(1),
        Duration::from_secs(1),
        CapacityLimits::default(),
    )
    .await;

    assert_eq!(result, Some(WsErrorKind::Internal));
}

#[tokio::test]
async fn test_peer_close_waits_for_writer_result_before_selecting_termination() {
    let mut writer = Box::pin(async { Ok(Some(WsErrorKind::Transport)) });
    assert_eq!(
        complete_after_reader(
            ReadResult::Closed(Some(1000)),
            watch::channel(None).1,
            writer.as_mut(),
            Duration::from_secs(1)
        )
        .await,
        WsTermination::Error {
            kind: WsErrorKind::Transport
        }
    );
}

#[tokio::test]
async fn test_complete_after_reader_uses_close_snapshot_updated_during_writer_wait() {
    let (peer_closed_tx, peer_closed) = watch::channel(None);
    let (writer_release_tx, writer_release_rx) = tokio::sync::oneshot::channel();
    let (writer_started_tx, writer_started_rx) = tokio::sync::oneshot::channel();
    let mut writer = Box::pin(async move {
        writer_started_tx.send(()).expect("completion is waiting");
        let _ = writer_release_rx.await;
        Ok(None)
    });
    let completion = tokio::spawn(async move {
        complete_after_reader(
            ReadResult::LocalClose,
            peer_closed,
            writer.as_mut(),
            Duration::from_secs(1),
        )
        .await
    });

    writer_started_rx.await.expect("writer is being awaited");
    peer_closed_tx.send(Some(Some(1009))).expect("reader remains active");
    writer_release_tx.send(()).expect("writer remains active");

    assert_eq!(
        completion.await.expect("completion task exits"),
        WsTermination::Closed { code: Some(1009) }
    );
}

#[test]
fn test_writer_first_uses_peer_close_snapshot() {
    let (peer_closed_tx, peer_observer) = watch::channel(None);
    peer_closed_tx
        .send(Some(Some(1009)))
        .expect("writer observes peer Close");
    assert_eq!(
        finish_after_writer(None, &peer_observer),
        WsTermination::Closed { code: Some(1009) }
    );

    let (_peer_closed_tx, peer_observer) = watch::channel(None);
    assert_eq!(
        finish_after_writer(None, &peer_observer),
        WsTermination::Error {
            kind: WsErrorKind::Internal
        }
    );
}

#[tokio::test]
async fn test_peer_close_drops_writer_when_close_wait_expires() {
    let mut writer = Box::pin(std::future::pending::<Result<Option<WsErrorKind>, ()>>());
    assert_eq!(
        complete_after_reader(
            ReadResult::Closed(Some(1000)),
            watch::channel(None).1,
            writer.as_mut(),
            Duration::from_millis(10)
        )
        .await,
        WsTermination::Error {
            kind: WsErrorKind::Timeout
        }
    );
}

#[tokio::test]
async fn test_writer_panic_is_internal_error() {
    let mut writer = Box::pin(async { Err(()) });
    assert_eq!(
        complete_after_reader(
            ReadResult::Closed(Some(1000)),
            watch::channel(None).1,
            writer.as_mut(),
            Duration::from_secs(1)
        )
        .await,
        WsTermination::Error {
            kind: WsErrorKind::Internal
        }
    );
}

#[test]
fn test_termination_resolution_preserves_close_codes_and_error_precedence() {
    for code in [Some(1000), Some(1009), None] {
        assert_eq!(
            resolve_termination(ReadResult::Closed(code), None, Some(code)),
            WsTermination::Closed { code }
        );
    }
    assert_eq!(
        resolve_termination(
            ReadResult::Closed(Some(1000)),
            Some(WsErrorKind::Timeout),
            Some(Some(1000))
        ),
        WsTermination::Error {
            kind: WsErrorKind::Timeout
        }
    );
    assert_eq!(
        resolve_termination(ReadResult::LocalClose, None, None),
        WsTermination::Error {
            kind: WsErrorKind::Internal
        }
    );
    for code in [Some(1000), Some(1009), None] {
        assert_eq!(
            resolve_termination(ReadResult::LocalClose, None, Some(code)),
            WsTermination::Closed { code }
        );
    }
    assert_eq!(
        resolve_termination(ReadResult::LocalClose, Some(WsErrorKind::Transport), None),
        WsTermination::Error {
            kind: WsErrorKind::Transport
        }
    );
    assert_eq!(
        resolve_termination(
            ReadResult::Error(WsErrorKind::Protocol),
            Some(WsErrorKind::Timeout),
            None
        ),
        WsTermination::Error {
            kind: WsErrorKind::Protocol
        }
    );
}

#[tokio::test]
async fn test_continuous_ping_allows_outbound_progress() {
    const MAX_PINGS: usize = 32;
    let outbound = BoundedQueue::new(2, 16);
    outbound
        .try_push(WsMessage::Text("alpha".to_owned()))
        .expect("first outbound message fits");
    outbound
        .try_push(WsMessage::Text("beta".to_owned()))
        .expect("second outbound message fits");
    let ping = Arc::new(Notify::new());
    ping.notify_one();
    let (peer_closed_tx, peer_closed) = watch::channel(None::<Option<u16>>);
    let (_close_tx, close) = watch::channel(None::<CloseRequest>);
    let events = Arc::new(Mutex::new(Vec::new()));
    let ping_flushes = Arc::new(AtomicUsize::new(0));
    let business_started = Arc::new(AtomicBool::new(false));
    let sink = ContinuousPingSink {
        ping: ping.clone(),
        peer_closed: peer_closed_tx,
        events: events.clone(),
        ping_flushes: ping_flushes.clone(),
        business_started: business_started.clone(),
        queue: outbound.clone(),
        max_ping_flushes: MAX_PINGS,
    };

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        write_loop(
            sink,
            outbound.clone(),
            close,
            peer_closed,
            ping,
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        ),
    )
    .await
    .expect("writer exits after the test sink reports peer Close");

    let events = events.lock().expect("event log mutex poisoned");
    let first_send = events
        .iter()
        .position(|event| event.starts_with("start_send"))
        .expect("queued business message reaches the sink");
    let flush_count = events[..first_send]
        .iter()
        .filter(|event| **event == "ping_flush")
        .count();
    assert_eq!(result, None);
    let observed_ping_flushes = ping_flushes.load(Ordering::SeqCst);
    assert!((1..=MAX_PINGS).contains(&observed_ping_flushes));
    assert_eq!(flush_count, observed_ping_flushes);
    assert!(
        flush_count <= 1,
        "first start_send after {flush_count} Ping flushes: {events:?}"
    );
    assert_eq!(events[first_send], "start_send:alpha:2,9");
    let sent_messages = events
        .iter()
        .filter(|event| event.starts_with("start_send:"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(sent_messages, ["start_send:alpha:2,9", "start_send:beta:1,4"]);
    assert_eq!(outbound.usage(), (0, 0));
}

#[tokio::test]
async fn test_ping_precedes_next_outbound_after_outbound_only_turn() {
    let outbound = BoundedQueue::new(2, 16);
    outbound
        .try_push(WsMessage::Text("alpha".to_owned()))
        .expect("first outbound message fits");
    let ping = Arc::new(Notify::new());
    let (peer_closed_tx, peer_closed) = watch::channel(None::<Option<u16>>);
    let (_close_tx, close) = watch::channel(None::<CloseRequest>);
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = PingAfterFirstMessageSink {
        ping: ping.clone(),
        peer_closed: peer_closed_tx,
        events: events.clone(),
        queue: outbound.clone(),
        publish_pending: true,
        business_flush_pending: false,
    };

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        write_loop(
            sink,
            outbound.clone(),
            close,
            peer_closed,
            ping,
            Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityLimits::default(),
        ),
    )
    .await
    .expect("writer exits after the second business message");

    let events = events.lock().expect("event log mutex poisoned");
    let second_send = events
        .iter()
        .position(|event| *event == "start_send:beta")
        .expect("second business message is sent");
    assert_eq!(result, None);
    assert!(
        events[..second_send].contains(&"control_flush"),
        "Ping must flush before the next outbound send: {events:?}"
    );
    assert_eq!(outbound.usage(), (0, 0));
}

struct PingAfterFirstMessageSink {
    ping: Arc<Notify>,
    peer_closed: watch::Sender<Option<Option<u16>>>,
    events: Arc<Mutex<Vec<&'static str>>>,
    queue: BoundedQueue,
    publish_pending: bool,
    business_flush_pending: bool,
}

impl Sink<Message> for PingAfterFirstMessageSink {
    type Error = TungsteniteError;

    fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let Message::Text(text) = item else {
            panic!("controlled writer expects a business text message");
        };
        let event = match text.as_str() {
            "alpha" => "start_send:alpha",
            "beta" => "start_send:beta",
            _ => panic!("unexpected business message: {text}"),
        };
        self.events.lock().expect("event log mutex poisoned").push(event);
        self.business_flush_pending = true;
        if event == "start_send:beta" {
            self.peer_closed
                .send(Some(None))
                .expect("writer retains the peer Close receiver");
        }
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.business_flush_pending {
            self.business_flush_pending = false;
            self.events
                .lock()
                .expect("event log mutex poisoned")
                .push("business_flush");
            if self.publish_pending {
                self.publish_pending = false;
                self.queue
                    .try_push(WsMessage::Text("beta".to_owned()))
                    .expect("second outbound message fits");
                self.ping.notify_one();
            }
        } else {
            self.events
                .lock()
                .expect("event log mutex poisoned")
                .push("control_flush");
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

struct ContinuousPingSink {
    ping: Arc<Notify>,
    peer_closed: watch::Sender<Option<Option<u16>>>,
    events: Arc<Mutex<Vec<String>>>,
    ping_flushes: Arc<AtomicUsize>,
    business_started: Arc<AtomicBool>,
    queue: BoundedQueue,
    max_ping_flushes: usize,
}

impl Sink<Message> for ContinuousPingSink {
    type Error = TungsteniteError;

    fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let Message::Text(text) = item else {
            panic!("controlled writer expects a business text message");
        };
        let usage = self.queue.usage();
        let mut events = self.events.lock().expect("event log mutex poisoned");
        events.push(format!("start_send:{text}:{},{}", usage.0, usage.1));
        let sent_count = events.iter().filter(|event| event.starts_with("start_send:")).count();
        drop(events);
        self.business_started.store(true, Ordering::SeqCst);
        if sent_count == 2 {
            self.peer_closed
                .send(Some(None))
                .expect("writer retains the peer Close receiver");
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let is_ping_flush = !self.business_started.load(Ordering::SeqCst);
        self.events
            .lock()
            .expect("event log mutex poisoned")
            .push(if is_ping_flush { "ping_flush" } else { "flush" }.to_owned());
        if is_ping_flush {
            let count = self.ping_flushes.fetch_add(1, Ordering::SeqCst);
            if count + 1 < self.max_ping_flushes {
                self.ping.notify_one();
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct SinkState {
    events: Vec<&'static str>,
    ready: bool,
    panic_on_ready: bool,
    flush: FlushBehavior,
    flush_waker: Option<Waker>,
    close_pending: bool,
}

#[derive(Clone, Copy, Default)]
enum FlushBehavior {
    #[default]
    Success,
    Error,
    Pending,
}

struct ControlledSink {
    state: Arc<Mutex<SinkState>>,
    poll_ready_seen: Arc<Notify>,
    close_seen: Arc<Notify>,
    flush_seen: Arc<Notify>,
}

impl Sink<Message> for ControlledSink {
    type Error = TungsteniteError;

    fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self.state.lock().expect("sink state poisoned");
        state.events.push("poll_ready");
        self.poll_ready_seen.notify_one();
        assert!(!state.panic_on_ready, "controlled writer poll panic");
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

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let behavior = {
            let mut state = self.state.lock().expect("sink state poisoned");
            state.events.push("flush");
            state.ready = true;
            if matches!(state.flush, FlushBehavior::Pending) {
                state.flush_waker = Some(context.waker().clone());
            }
            state.flush
        };
        self.flush_seen.notify_one();
        match behavior {
            FlushBehavior::Success => Poll::Ready(Ok(())),
            FlushBehavior::Error => Poll::Ready(Err(TungsteniteError::Io(std::io::Error::other(
                "controlled flush failure",
            )))),
            FlushBehavior::Pending => Poll::Pending,
        }
    }

    fn poll_close(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self.state.lock().expect("sink state poisoned");
        state.events.push("close");
        self.close_seen.notify_one();
        if state.close_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

fn inflight_payload(max_messages: usize, max_bytes: usize) -> (BoundedQueue, super::Envelope) {
    let queue = BoundedQueue::new(max_messages, max_bytes);
    queue
        .try_push(WsMessage::Text("payload".to_owned()))
        .expect("payload fits the queue");
    let envelope = queue.try_pop().expect("queued envelope");
    (queue, envelope)
}

fn release_flush(state: &Arc<Mutex<SinkState>>, behavior: FlushBehavior) {
    let waker = {
        let mut state = state.lock().expect("sink state poisoned");
        state.flush = behavior;
        state.flush_waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn assert_started_pending_flush(state: &Arc<Mutex<SinkState>>) {
    let state = state.lock().expect("sink state poisoned");
    let poll_ready = state
        .events
        .iter()
        .position(|event| *event == "poll_ready")
        .expect("send polls the ready sink");
    let start_send = state
        .events
        .iter()
        .position(|event| *event == "start_send")
        .expect("ready sink receives the envelope");
    let flush = state
        .events
        .iter()
        .position(|event| *event == "flush")
        .expect("sent envelope is flushed");
    assert!(poll_ready < start_send && start_send < flush);
    assert_eq!(state.events.iter().filter(|event| **event == "start_send").count(), 1);
    assert!(state.events.iter().filter(|event| **event == "flush").count() >= 1);
}

#[tokio::test]
async fn test_inflight_message_budget_backpressure() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let flush_seen = sink.flush_seen.clone();
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(1),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(1), flush_seen.notified())
        .await
        .expect("controlled sink reports the pending flush poll");
    assert_started_pending_flush(&state);
    assert_eq!(queue.usage(), (1, 7));
    assert_eq!(
        queue.try_push(WsMessage::Text("x".to_owned())),
        Err(WsSendError::Backpressure)
    );
}

#[tokio::test]
async fn test_inflight_byte_budget_backpressure() {
    let (queue, envelope) = inflight_payload(4, 7);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(1),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_started_pending_flush(&state);
    assert_eq!(queue.usage(), (1, 7));
    assert_eq!(
        queue.try_push(WsMessage::Text("x".to_owned())),
        Err(WsSendError::Backpressure)
    );
}

#[tokio::test]
async fn test_inflight_flush_completion_releases_both_budgets() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(1),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_started_pending_flush(&state);
    assert_eq!(queue.usage(), (1, 7));
    release_flush(&state, FlushBehavior::Success);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), send.as_mut()).await,
        Ok(SendResult::Sent)
    ));
    assert_eq!(queue.usage(), (0, 0));
    queue
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("released budgets admit another message");
}

#[tokio::test]
async fn test_inflight_ping_preserves_budget_until_flush_completes() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let ping = Notify::new();
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(1),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    ping.notify_one();
    assert!(futures_util::poll!(send.as_mut()).is_pending());
    let flushes = state
        .lock()
        .expect("sink state poisoned")
        .events
        .iter()
        .filter(|event| **event == "flush")
        .count();
    assert!(
        flushes >= 2,
        "Ping must drive another flush: {:?}",
        state.lock().expect("sink state poisoned").events
    );
    assert_eq!(queue.usage(), (1, 7));
    release_flush(&state, FlushBehavior::Success);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), send.as_mut()).await,
        Ok(SendResult::Sent)
    ));
    assert_eq!(queue.usage(), (0, 0));
    assert_eq!(
        state
            .lock()
            .expect("sink state poisoned")
            .events
            .iter()
            .filter(|event| **event == "start_send")
            .count(),
        1
    );
}

async fn assert_idle_ping_flush_observes_close(local: bool) {
    let outbound = BoundedQueue::new(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let flush_seen = Arc::new(Notify::new());
    let close_seen = Arc::new(Notify::new());
    let sink = ControlledSink {
        state,
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: close_seen.clone(),
        flush_seen: flush_seen.clone(),
    };
    let (close_tx, close_rx) = watch::channel(None::<CloseRequest>);
    let (peer_tx, peer_rx) = watch::channel(None::<Option<u16>>);
    let ping = Arc::new(Notify::new());
    let writer = write_loop(
        sink,
        outbound,
        close_rx,
        peer_rx,
        ping.clone(),
        Duration::from_secs(30),
        Duration::from_millis(100),
        CapacityLimits::default(),
    );
    tokio::pin!(writer);

    ping.notify_one();
    tokio::select! {
        _ = flush_seen.notified() => {},
        result = writer.as_mut() => panic!("idle Ping writer exited early: {result:?}"),
    }
    if local {
        close_tx
            .send(Some(CloseRequest::Local))
            .expect("writer observes local close");
        tokio::select! {
            _ = close_seen.notified() => {},
            result = writer.as_mut() => panic!("local close finished before its Close was observable: {result:?}"),
        }
        peer_tx.send(Some(None)).expect("writer observes peer acknowledgement");
        assert_eq!(writer.await, None);
    } else {
        peer_tx.send(Some(Some(1000))).expect("writer observes peer Close");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), writer.as_mut())
                .await
                .expect("peer Close response is bounded by close_timeout"),
            Some(WsErrorKind::Timeout)
        );
    }
}

#[tokio::test]
async fn test_idle_ping_flush_observes_local_close() {
    assert_idle_ping_flush_observes_close(true).await;
}

#[tokio::test]
async fn test_idle_ping_flush_observes_peer_close() {
    assert_idle_ping_flush_observes_close(false).await;
}

async fn assert_inflight_ping_flush_observes_close(local: bool) {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let flush_seen = Arc::new(Notify::new());
    let close_seen = Arc::new(Notify::new());
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: close_seen.clone(),
        flush_seen: flush_seen.clone(),
    };
    let (close_tx, mut close_rx) = watch::channel(None::<CloseRequest>);
    let (peer_tx, mut peer_rx) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let send = send_envelope(
        &mut sink,
        envelope,
        &mut close_rx,
        &mut peer_rx,
        &ping,
        Duration::from_secs(30),
        Duration::from_millis(100),
        CapacityLimits::default(),
    );
    tokio::pin!(send);

    tokio::select! {
        _ = flush_seen.notified() => {},
        _ = send.as_mut() => panic!("inflight business flush exited early"),
    }
    assert_started_pending_flush(&state);
    ping.notify_one();
    tokio::select! {
        _ = flush_seen.notified() => {},
        _ = send.as_mut() => panic!("Ping flush exited early"),
    }
    assert_eq!(queue.usage(), (1, 7));
    if local {
        close_tx
            .send(Some(CloseRequest::Local))
            .expect("writer observes local close");
        tokio::select! {
            _ = close_seen.notified() => {},
            _ = send.as_mut() => panic!("local close finished before its Close was observable"),
        }
        peer_tx.send(Some(None)).expect("writer observes peer acknowledgement");
        assert!(matches!(send.await, SendResult::Exit(None)));
    } else {
        peer_tx.send(Some(Some(1000))).expect("writer observes peer Close");
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), send.as_mut())
                .await
                .expect("peer Close response is bounded by close_timeout"),
            SendResult::Exit(Some(WsErrorKind::Timeout))
        ));
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test]
async fn test_inflight_ping_flush_observes_local_close() {
    assert_inflight_ping_flush_observes_close(true).await;
}

#[tokio::test]
async fn test_inflight_ping_flush_observes_peer_close() {
    assert_inflight_ping_flush_observes_close(false).await;
}

#[tokio::test]
async fn test_inflight_ping_close_write_timeout_releases_budget_without_peer_ack() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        close_pending: true,
        ..SinkState::default()
    }));
    let flush_seen = Arc::new(Notify::new());
    let close_seen = Arc::new(Notify::new());
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: close_seen.clone(),
        flush_seen: flush_seen.clone(),
    };
    let (close_tx, mut close_rx) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_rx) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let send = send_envelope(
        &mut sink,
        envelope,
        &mut close_rx,
        &mut peer_rx,
        &ping,
        Duration::from_secs(30),
        Duration::from_millis(100),
        CapacityLimits::default(),
    );
    tokio::pin!(send);

    tokio::select! {
        _ = flush_seen.notified() => {},
        _ = send.as_mut() => panic!("inflight business flush exited early"),
    }
    ping.notify_one();
    tokio::select! {
        _ = flush_seen.notified() => {},
        _ = send.as_mut() => panic!("Ping flush exited early"),
    }
    close_tx
        .send(Some(CloseRequest::Local))
        .expect("writer observes local close");
    tokio::select! {
        _ = close_seen.notified() => {},
        _ = send.as_mut() => panic!("blocked Close write should remain pending"),
    }
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), send.as_mut())
            .await
            .expect("blocked Close write is bounded by close_timeout"),
        SendResult::Exit(Some(WsErrorKind::Timeout))
    ));
    assert_eq!(queue.usage(), (0, 0));
    let events = &state.lock().expect("sink state poisoned").events;
    assert_eq!(events.iter().filter(|event| **event == "start_send").count(), 1);
    assert!(events.contains(&"close"));
}

#[tokio::test]
async fn test_inflight_local_close_releases_budget_after_peer_ack() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let close_seen = Arc::new(Notify::new());
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: close_seen.clone(),
        flush_seen: Arc::new(Notify::new()),
    };
    let (close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(2),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_started_pending_flush(&state);
    close_tx.send(Some(CloseRequest::Local)).expect("close receiver exists");
    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_eq!(
        queue.usage(),
        (1, 7),
        "local close keeps the reservation while flushing"
    );
    release_flush(&state, FlushBehavior::Success);
    tokio::time::timeout(Duration::from_secs(2), close_seen.notified())
        .await
        .expect("Close is sent after flush");
    assert_eq!(
        queue.usage(),
        (1, 7),
        "local close keeps the reservation until peer ack"
    );
    peer_tx.send(Some(None)).expect("peer-close receiver exists");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), send.as_mut()).await,
        Ok(SendResult::Exit(None))
    ));
    assert_eq!(queue.usage(), (0, 0));
    queue
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("released budgets admit another message");
}

#[tokio::test]
async fn test_inflight_peer_close_releases_budget_after_flush_exit() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(2),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    peer_tx.send(Some(None)).expect("peer-close receiver exists");
    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_eq!(
        queue.usage(),
        (1, 7),
        "peer close does not release while flush is pending"
    );
    release_flush(&state, FlushBehavior::Success);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), send.as_mut()).await,
        Ok(SendResult::Exit(None))
    ));
    assert_eq!(queue.usage(), (0, 0));
    queue
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("released budgets admit another message");
}

#[tokio::test]
async fn test_inflight_no_progress_timeout_releases_budget() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_millis(100),
        Duration::from_secs(1),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_started_pending_flush(&state);
    assert_eq!(queue.usage(), (1, 7));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), send.as_mut()).await,
        Ok(SendResult::Exit(Some(WsErrorKind::Timeout)))
    ));
    assert_eq!(queue.usage(), (0, 0));
    queue
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("released budgets admit another message");
}

#[tokio::test]
async fn test_inflight_flush_error_releases_budget() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(1),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_started_pending_flush(&state);
    release_flush(&state, FlushBehavior::Error);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), send.as_mut()).await,
        Ok(SendResult::Exit(Some(WsErrorKind::Transport)))
    ));
    assert_eq!(queue.usage(), (0, 0));
    queue
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("released budgets admit another message");
}

#[tokio::test]
async fn test_inflight_cancel_releases_budget() {
    let (queue, envelope) = inflight_payload(1, 16);
    let state = Arc::new(Mutex::new(SinkState {
        ready: true,
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let (_close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (_peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let ping = Notify::new();
    let mut send = Box::pin(send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(5),
        Duration::from_secs(1),
        CapacityLimits::default(),
    ));

    assert!(futures_util::poll!(send.as_mut()).is_pending());
    assert_started_pending_flush(&state);
    assert_eq!(queue.usage(), (1, 7));
    drop(send);
    assert_eq!(queue.usage(), (0, 0));
    queue
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("cancellation released the reservation");
}

#[tokio::test]
async fn test_controlled_flush_failure_is_transport_error() {
    let state = Arc::new(Mutex::new(SinkState {
        flush: FlushBehavior::Error,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state,
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let mut writer =
        Box::pin(async move { Ok(timeout_flush(&mut sink, Duration::from_secs(1), CapacityLimits::default()).await) });
    assert_eq!(
        complete_after_reader(
            ReadResult::Closed(Some(1000)),
            watch::channel(None).1,
            writer.as_mut(),
            Duration::from_secs(1)
        )
        .await,
        WsTermination::Error {
            kind: WsErrorKind::Transport
        }
    );
}

#[tokio::test]
async fn test_controlled_pending_flush_is_timeout() {
    let state = Arc::new(Mutex::new(SinkState {
        flush: FlushBehavior::Pending,
        ..SinkState::default()
    }));
    let mut sink = ControlledSink {
        state,
        poll_ready_seen: Arc::new(Notify::new()),
        close_seen: Arc::new(Notify::new()),
        flush_seen: Arc::new(Notify::new()),
    };
    let mut writer =
        Box::pin(
            async move { Ok(timeout_flush(&mut sink, Duration::from_millis(10), CapacityLimits::default()).await) },
        );
    assert_eq!(
        complete_after_reader(
            ReadResult::Closed(Some(1000)),
            watch::channel(None).1,
            writer.as_mut(),
            Duration::from_secs(1)
        )
        .await,
        WsTermination::Error {
            kind: WsErrorKind::Timeout
        }
    );
}

#[tokio::test]
async fn test_ping_flush_interrupts_pending_business_poll_ready() {
    let queue = BoundedQueue::new(1, 16);
    queue
        .try_push(WsMessage::Text("payload".to_owned()))
        .expect("queue has capacity");
    let envelope = queue.pop().await.expect("queued envelope");

    let state = Arc::new(Mutex::new(SinkState::default()));
    let poll_ready_seen = Arc::new(Notify::new());
    let close_seen = Arc::new(Notify::new());
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: poll_ready_seen.clone(),
        close_seen,
        flush_seen: Arc::new(Notify::new()),
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
        CapacityLimits::default(),
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

#[tokio::test]
async fn test_local_close_during_pending_business_write_waits_for_peer_ack() {
    let queue = BoundedQueue::new(1, 16);
    queue
        .try_push(WsMessage::Text("payload".to_owned()))
        .expect("queue has capacity");
    let envelope = queue.pop().await.expect("queued envelope");

    let state = Arc::new(Mutex::new(SinkState::default()));
    let poll_ready_seen = Arc::new(Notify::new());
    let close_seen = Arc::new(Notify::new());
    let mut sink = ControlledSink {
        state: state.clone(),
        poll_ready_seen: poll_ready_seen.clone(),
        close_seen: close_seen.clone(),
        flush_seen: Arc::new(Notify::new()),
    };
    let ping = Notify::new();
    let (close_tx, mut close) = watch::channel(None::<CloseRequest>);
    let (peer_tx, mut peer_closed) = watch::channel(None::<Option<u16>>);
    let send = send_envelope(
        &mut sink,
        envelope,
        &mut close,
        &mut peer_closed,
        &ping,
        Duration::from_secs(1),
        Duration::from_secs(1),
        CapacityLimits::default(),
    );
    tokio::pin!(send);

    tokio::select! {
        _ = poll_ready_seen.notified() => {},
        _ = &mut send => panic!("business write should be pending before close"),
    }
    close_tx.send(Some(CloseRequest::Local)).expect("close receiver exists");
    tokio::select! {
        _ = close_seen.notified() => {},
        _ = &mut send => panic!("successful Close must wait for peer confirmation"),
    }
    peer_tx.send(Some(None)).expect("peer-close receiver exists");
    assert!(matches!(send.await, SendResult::Exit(None)));

    let events = &state.lock().expect("sink state poisoned").events;
    assert!(!events.contains(&"start_send"), "events: {events:?}");
}
