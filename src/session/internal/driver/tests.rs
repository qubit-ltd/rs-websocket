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
use std::time::Duration;

use futures_util::Sink;
use futures_util::Stream;
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
use crate::error::WsTermination;
use crate::options::WsSessionLimits;
use crate::session::WsSession;
use crate::session::internal::Owner;
use crate::ws_message::WsMessage;

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

#[derive(Default)]
struct SinkState {
    events: Vec<&'static str>,
    ready: bool,
    panic_on_ready: bool,
    flush: FlushBehavior,
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

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self.state.lock().expect("sink state poisoned");
        state.events.push("flush");
        state.ready = true;
        match state.flush {
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
        Poll::Ready(Ok(()))
    }
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
