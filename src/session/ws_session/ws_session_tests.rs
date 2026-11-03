// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::watch;

use super::WsSession;
use crate::error::WsErrorKind;
use crate::error::WsTermination;
use crate::session::internal::BoundedQueue;
use crate::session::internal::CloseRequest;
use crate::session::internal::Owner;
use crate::ws_message::WsMessage;

type Hook = Box<dyn FnMut() + Send>;

thread_local! {
    static HOOK: RefCell<Option<Hook>> = RefCell::new(None);
    static TIMEOUT_HOOK: RefCell<Option<Hook>> = RefCell::new(None);
}

struct HookGuard;

struct OwnerAbortGuard(Arc<Owner>);

impl Drop for OwnerAbortGuard {
    fn drop(&mut self) {
        self.0.abort.abort();
    }
}

impl HookGuard {
    fn install(hook: impl FnMut() + Send + 'static) -> Self {
        HOOK.with(|slot| assert!(slot.replace(Some(Box::new(hook))).is_none()));
        Self
    }
}

impl Drop for HookGuard {
    fn drop(&mut self) {
        HOOK.with(|slot| {
            slot.replace(None);
        });
    }
}

struct TimeoutHookGuard;

impl TimeoutHookGuard {
    fn install(hook: impl FnMut() + Send + 'static) -> Self {
        TIMEOUT_HOOK.with(|slot| assert!(slot.replace(Some(Box::new(hook))).is_none()));
        Self
    }
}

impl Drop for TimeoutHookGuard {
    fn drop(&mut self) {
        TIMEOUT_HOOK.with(|slot| {
            slot.replace(None);
        });
    }
}

fn run_hook() {
    HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook();
        }
    });
}

pub(super) fn after_first_empty() {
    run_hook();
}

pub(super) fn after_none_terminal_snapshot() {
    run_hook();
}

pub(super) fn before_timeout_terminal_lock() {
    TIMEOUT_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook();
        }
    });
}

fn session() -> (WsSession, Arc<Owner>) {
    session_with_close_timeout(Duration::from_millis(50))
}

fn session_with_close_timeout(close_timeout: Duration) -> (WsSession, Arc<Owner>) {
    session_with_driver(close_timeout, std::future::pending())
}

fn session_with_driver(
    close_timeout: Duration,
    driver: impl std::future::Future<Output = ()> + Send + 'static,
) -> (WsSession, Arc<Owner>) {
    let inbound = BoundedQueue::new(2, 64);
    let outbound = BoundedQueue::new(2, 64);
    let terminal = Arc::new(Mutex::new(None));
    let terminal_changed = Arc::new(Notify::new());
    let (close, _close_rx) = watch::channel(None::<CloseRequest>);
    let task = tokio::spawn(driver);
    let abort = task.abort_handle();
    let owner = Arc::new(Owner {
        inbound,
        outbound,
        close,
        terminal,
        terminal_changed,
        close_timeout,
        close_result: Mutex::new(None),
        close_gate: AsyncMutex::new(()),
        task: AsyncMutex::new(Some(task)),
        abort,
    });
    let session = WsSession {
        owner: owner.clone(),
        error_delivered: false,
        response_status_code: 101,
        selected_subprotocol: None,
    };
    (session, owner)
}

#[tokio::test]
async fn test_close_rejects_unrepresentable_deadline_before_publishing_close() {
    let (mut session, owner) = session_with_close_timeout(Duration::MAX);

    let error = session.close().await.expect_err("deadline cannot be represented");

    assert_eq!(error.kind(), WsErrorKind::InvalidConfig);
    assert!(owner.close.borrow().is_none());
    assert!(owner.outbound.try_push(WsMessage::Text("still open".into())).is_ok());
    owner.abort.abort();
}

#[tokio::test]
async fn test_close_caches_completed_termination() {
    let (mut session, owner) = session_with_driver(Duration::from_millis(50), async {});
    let _owner_guard = OwnerAbortGuard(owner.clone());
    publish_terminal(&owner, WsTermination::Closed { code: Some(1000) });

    tokio::task::yield_now().await;
    assert!(session.close().await.is_ok());
    assert!(session.close().await.is_ok());
    assert_eq!(session.termination(), Some(WsTermination::Closed { code: Some(1000) }));
}

#[tokio::test]
async fn test_close_caches_completed_error() {
    let (mut session, owner) = session_with_driver(Duration::from_millis(50), async {});
    let _owner_guard = OwnerAbortGuard(owner.clone());
    publish_terminal(
        &owner,
        WsTermination::Error {
            kind: WsErrorKind::Transport,
        },
    );

    tokio::task::yield_now().await;
    assert_eq!(session.close().await.unwrap_err().kind(), WsErrorKind::Transport);
    assert_eq!(session.close().await.unwrap_err().kind(), WsErrorKind::Transport);
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Transport,
        })
    );
}

#[tokio::test]
async fn test_close_caches_join_failure() {
    let (mut session, owner) = session_with_driver(Duration::from_millis(50), async { panic!("driver failed") });
    let _owner_guard = OwnerAbortGuard(owner.clone());
    publish_terminal(&owner, WsTermination::Closed { code: Some(1000) });

    tokio::task::yield_now().await;
    assert_eq!(session.close().await.unwrap_err().kind(), WsErrorKind::Internal);
    assert_eq!(session.close().await.unwrap_err().kind(), WsErrorKind::Internal);
    assert_eq!(
        session.termination(),
        Some(WsTermination::Error {
            kind: WsErrorKind::Internal,
        })
    );
}

#[tokio::test]
async fn test_close_join_timeout_remains_independent_from_termination() {
    let (mut session, owner) = session();
    let _owner_guard = OwnerAbortGuard(owner.clone());
    publish_terminal(&owner, WsTermination::Closed { code: Some(1000) });

    assert_eq!(session.close().await.unwrap_err().kind(), WsErrorKind::Timeout);
    assert_eq!(session.close().await.unwrap_err().kind(), WsErrorKind::Timeout);
    assert_eq!(session.termination(), Some(WsTermination::Closed { code: Some(1000) }));
}

async fn assert_timeout_race(expected: Option<WsTermination>) {
    let (session, owner) = session();
    let _owner_guard = OwnerAbortGuard(owner.clone());
    let _hook_guard = TimeoutHookGuard::install({
        let owner = owner.clone();
        move || {
            if let Some(termination) = expected {
                publish_terminal(&owner, termination);
            }
        }
    });

    let actual = session.wait_for_termination(tokio::time::Instant::now()).await;
    let expected = expected.unwrap_or(WsTermination::Error {
        kind: WsErrorKind::Timeout,
    });
    assert_eq!(actual, expected);
    assert_eq!(session.termination(), Some(expected));
    tokio::task::yield_now().await;
    assert_eq!(
        owner
            .task
            .try_lock()
            .expect("driver task lock")
            .as_ref()
            .unwrap()
            .is_finished(),
        expected
            == WsTermination::Error {
                kind: WsErrorKind::Timeout
            }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_wait_for_termination_timeout_keeps_published_closed() {
    assert_timeout_race(Some(WsTermination::Closed { code: Some(1000) })).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_wait_for_termination_timeout_keeps_published_error() {
    assert_timeout_race(Some(WsTermination::Error {
        kind: WsErrorKind::Transport,
    }))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_wait_for_termination_timeout_records_and_aborts_empty_state() {
    assert_timeout_race(None).await;
}

fn publish_terminal(owner: &Owner, termination: WsTermination) {
    *owner.terminal.lock().expect("terminal mutex poisoned") = Some(termination);
    owner.terminal_changed.notify_waiters();
}

async fn assert_last_message_precedes_terminal(termination: WsTermination) {
    let (mut session, owner) = session();
    let _owner_guard = OwnerAbortGuard(owner.clone());
    let (checkpoint_tx, checkpoint_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::channel();
    let hook_guard = HookGuard::install({
        move || {
            checkpoint_tx
                .send("after_first_empty")
                .expect("publisher receives checkpoint");
            release_rx
                .recv_timeout(Duration::from_millis(300))
                .expect("release after_first_empty");
        }
    });
    let publisher = std::thread::spawn({
        let owner = owner.clone();
        move || {
            checkpoint_rx
                .recv_timeout(Duration::from_millis(300))
                .expect("checkpoint after_first_empty");
            let result = owner
                .inbound
                .try_push(WsMessage::Text("last".into()))
                .map_err(|_| "last message fits");
            if result.is_ok() {
                publish_terminal(&owner, termination);
            }
            let _ = release_tx.send(());
            done_tx.send(result).expect("test receives publisher completion");
        }
    });
    let message = tokio::time::timeout(Duration::from_millis(300), session.receive())
        .await
        .expect("queued message is delivered")
        .expect("message precedes terminal")
        .expect("queued message is not an error");
    assert_eq!(message, WsMessage::Text("last".into()));
    done_rx
        .recv_timeout(Duration::from_millis(300))
        .expect("publisher completion")
        .expect("last message fits");
    publisher.join().expect("publisher exits");
    drop(hook_guard);
    match termination {
        WsTermination::Closed { .. } => assert!(session.receive().await.is_none()),
        WsTermination::Error { kind } => {
            assert_eq!(
                session
                    .receive()
                    .await
                    .expect("terminal error")
                    .expect_err("error terminal")
                    .kind(),
                kind
            );
            assert!(session.receive().await.is_none());
        }
    }
    owner.abort.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_receive_delivers_last_message_before_closed_terminal() {
    assert_last_message_precedes_terminal(WsTermination::Closed { code: Some(1000) }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_receive_delivers_last_message_before_error_terminal() {
    assert_last_message_precedes_terminal(WsTermination::Error {
        kind: WsErrorKind::Timeout,
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_receive_registers_before_terminal_snapshot_closed() {
    let (mut session, owner) = session();
    let _owner_guard = OwnerAbortGuard(owner.clone());
    let (checkpoint_tx, checkpoint_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::channel();
    let hook_guard = HookGuard::install({
        let mut calls = 0;
        move || {
            calls += 1;
            if calls == 2 {
                checkpoint_tx
                    .send("none_terminal_snapshot")
                    .expect("publisher receives checkpoint");
                release_rx
                    .recv_timeout(Duration::from_millis(300))
                    .expect("release none_terminal_snapshot");
            }
        }
    });
    let publisher = std::thread::spawn({
        let owner = owner.clone();
        move || {
            checkpoint_rx
                .recv_timeout(Duration::from_millis(300))
                .expect("checkpoint none_terminal_snapshot");
            publish_terminal(&owner, WsTermination::Closed { code: Some(1000) });
            let _ = release_tx.send(());
            done_tx.send(()).expect("test receives publisher completion");
        }
    });
    let result = tokio::time::timeout(Duration::from_millis(300), session.receive()).await;
    done_rx
        .recv_timeout(Duration::from_millis(300))
        .expect("publisher completion");
    publisher.join().expect("publisher exits");
    drop(hook_guard);
    assert!(result.expect("registered receive wakes").is_none());
    owner.abort.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_receive_registers_before_terminal_snapshot_error_once() {
    let (mut session, owner) = session();
    let _owner_guard = OwnerAbortGuard(owner.clone());
    let (checkpoint_tx, checkpoint_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::channel();
    let hook_guard = HookGuard::install({
        let mut calls = 0;
        move || {
            calls += 1;
            if calls == 2 {
                checkpoint_tx
                    .send("none_terminal_snapshot_error")
                    .expect("publisher receives checkpoint");
                release_rx
                    .recv_timeout(Duration::from_millis(300))
                    .expect("release none_terminal_snapshot_error");
            }
        }
    });
    let publisher = std::thread::spawn({
        let owner = owner.clone();
        move || {
            checkpoint_rx
                .recv_timeout(Duration::from_millis(300))
                .expect("checkpoint none_terminal_snapshot_error");
            publish_terminal(
                &owner,
                WsTermination::Error {
                    kind: WsErrorKind::Timeout,
                },
            );
            let _ = release_tx.send(());
            done_tx.send(()).expect("test receives publisher completion");
        }
    });
    let result = tokio::time::timeout(Duration::from_millis(300), session.receive()).await;
    done_rx
        .recv_timeout(Duration::from_millis(300))
        .expect("publisher completion");
    publisher.join().expect("publisher exits");
    drop(hook_guard);
    assert_eq!(
        result
            .expect("registered receive wakes")
            .expect("one terminal error")
            .unwrap_err()
            .kind(),
        WsErrorKind::Timeout
    );
    assert!(session.receive().await.is_none());
    owner.abort.abort();
}

#[tokio::test]
async fn test_receive_hook_fails_bounded_when_publisher_exits_before_checkpoint() {
    let (mut session, owner) = session();
    let _owner_guard = OwnerAbortGuard(owner);
    let (checkpoint_tx, checkpoint_rx) = mpsc::sync_channel::<&'static str>(0);
    drop(checkpoint_rx);
    let hook_guard = HookGuard::install(move || {
        checkpoint_tx
            .send("after_first_empty")
            .expect("publisher receives checkpoint after_first_empty");
    });

    let receive = tokio::spawn(async move { session.receive().await });
    let result = tokio::time::timeout(Duration::from_millis(300), receive)
        .await
        .expect("receive hook exits after publisher disconnects");
    assert!(result.expect_err("disconnected checkpoint send panics").is_panic());
    drop(hook_guard);
}

#[tokio::test]
async fn test_receive_hook_fails_bounded_when_checkpoint_is_missing() {
    let (mut session, owner) = session();
    let _owner_guard = OwnerAbortGuard(owner);
    let (_checkpoint_tx, checkpoint_rx) = mpsc::channel::<&'static str>();
    let hook_guard = HookGuard::install(move || {
        checkpoint_rx
            .recv_timeout(Duration::from_millis(300))
            .expect("checkpoint after_first_empty");
    });
    let receive = tokio::spawn(async move { session.receive().await });
    let result = tokio::time::timeout(Duration::from_millis(600), receive)
        .await
        .expect("receive hook exits when checkpoint is missing");
    assert!(result.expect_err("missing checkpoint panics").is_panic());
    drop(hook_guard);
}
