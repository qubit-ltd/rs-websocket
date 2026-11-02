// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
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
}

struct HookGuard;

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

fn session() -> (WsSession, Arc<Owner>) {
    let inbound = BoundedQueue::new(2, 64);
    let outbound = BoundedQueue::new(2, 64);
    let terminal = Arc::new(Mutex::new(None));
    let terminal_changed = Arc::new(Notify::new());
    let (close, _close_rx) = watch::channel(None::<CloseRequest>);
    let task = tokio::spawn(std::future::pending());
    let abort = task.abort_handle();
    let owner = Arc::new(Owner {
        inbound,
        outbound,
        close,
        terminal,
        terminal_changed,
        close_timeout: Duration::from_millis(50),
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

fn publish_terminal(owner: &Owner, termination: WsTermination) {
    *owner.terminal.lock().expect("terminal mutex poisoned") = Some(termination);
    owner.terminal_changed.notify_waiters();
}

async fn assert_last_message_precedes_terminal(termination: WsTermination) {
    let (mut session, owner) = session();
    let rendezvous = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let hook_guard = HookGuard::install({
        let rendezvous = rendezvous.clone();
        let release = release.clone();
        move || {
            rendezvous.wait();
            release.wait();
        }
    });
    let publisher = std::thread::spawn({
        let rendezvous = rendezvous.clone();
        let release = release.clone();
        let owner = owner.clone();
        move || {
            rendezvous.wait();
            owner
                .inbound
                .try_push(WsMessage::Text("last".into()))
                .expect("last message fits");
            publish_terminal(&owner, termination);
            release.wait();
        }
    });
    let message = tokio::time::timeout(Duration::from_millis(300), session.receive())
        .await
        .expect("queued message is delivered")
        .expect("message precedes terminal")
        .expect("queued message is not an error");
    assert_eq!(message, WsMessage::Text("last".into()));
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
    let rendezvous = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let hook_guard = HookGuard::install({
        let rendezvous = rendezvous.clone();
        let release = release.clone();
        let mut calls = 0;
        move || {
            calls += 1;
            if calls == 2 {
                rendezvous.wait();
                release.wait();
            }
        }
    });
    let publisher = std::thread::spawn({
        let rendezvous = rendezvous.clone();
        let release = release.clone();
        let owner = owner.clone();
        move || {
            rendezvous.wait();
            publish_terminal(&owner, WsTermination::Closed { code: Some(1000) });
            release.wait();
        }
    });
    let result = tokio::time::timeout(Duration::from_millis(300), session.receive()).await;
    publisher.join().expect("publisher exits");
    drop(hook_guard);
    assert!(result.expect("registered receive wakes").is_none());
    owner.abort.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_receive_registers_before_terminal_snapshot_error_once() {
    let (mut session, owner) = session();
    let rendezvous = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let hook_guard = HookGuard::install({
        let rendezvous = rendezvous.clone();
        let release = release.clone();
        let mut calls = 0;
        move || {
            calls += 1;
            if calls == 2 {
                rendezvous.wait();
                release.wait();
            }
        }
    });
    let publisher = std::thread::spawn({
        let rendezvous = rendezvous.clone();
        let release = release.clone();
        let owner = owner.clone();
        move || {
            rendezvous.wait();
            publish_terminal(
                &owner,
                WsTermination::Error {
                    kind: WsErrorKind::Timeout,
                },
            );
            release.wait();
        }
    });
    let result = tokio::time::timeout(Duration::from_millis(300), session.receive()).await;
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
