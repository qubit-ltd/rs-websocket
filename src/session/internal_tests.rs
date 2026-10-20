// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use tokio::time::timeout;

use super::super::WsMessage;
use crate::error::WsSendError;
use crate::session::internal::BoundedQueue;

fn text(payload: &str) -> WsMessage {
    WsMessage::Text(payload.to_owned())
}

#[tokio::test]
async fn test_enforces_message_and_byte_limits_and_releases_permits_on_pop() {
    let queue = BoundedQueue::new(2, 5);
    queue.try_push(text("ab")).expect("2 bytes fit");
    queue.try_push(text("xyz")).expect("3 more bytes fit");
    assert_eq!(queue.try_push(text("")), Err(WsSendError::Backpressure));

    assert_eq!(queue.try_pop().expect("first item").into_item(), text("ab"));
    queue.try_push(text("q")).expect("pop releases count and byte permits");
    assert_eq!(queue.try_push(text("123456")), Err(WsSendError::MessageTooLarge));

    let empty_queue = BoundedQueue::new(1, 1);
    empty_queue.try_push(text("")).expect("empty text uses a count permit");
    assert_eq!(empty_queue.try_push(text("")), Err(WsSendError::Backpressure));
}

#[tokio::test]
async fn test_async_push_waits_for_capacity_and_drop_cancels_without_leaking_permits() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("ok")).expect("initial item fits");

    let waiting_queue = queue.clone();
    let waiting = tokio::spawn(async move { waiting_queue.push(text("x")).await });
    tokio::task::yield_now().await;
    assert_eq!(queue.usage(), (1, 2));

    drop(queue.pop().await.expect("pop releases capacity"));
    timeout(Duration::from_secs(1), waiting)
        .await
        .expect("push wakes after capacity is available")
        .expect("push task joins")
        .expect("push succeeds");
    assert_eq!(queue.usage(), (1, 1));

    let cancelled_queue = BoundedQueue::new(1, 1);
    cancelled_queue.try_push(text("a")).expect("fill queue");
    let pending_queue = cancelled_queue.clone();
    let pending = tokio::spawn(async move { pending_queue.push(text("b")).await });
    tokio::task::yield_now().await;
    pending.abort();
    let _ = pending.await;
    drop(cancelled_queue.pop().await.expect("release occupied slot"));
    cancelled_queue
        .try_push(text("b"))
        .expect("cancelled waiter leaked no permit");

    let partial_queue = BoundedQueue::new(2, 1);
    partial_queue.try_push(text("a")).expect("occupy byte permit");
    let pending_queue = partial_queue.clone();
    let pending = tokio::spawn(async move { pending_queue.push(text("b")).await });
    tokio::task::yield_now().await;
    assert_eq!(partial_queue.usage(), (2, 1));
    pending.abort();
    let _ = pending.await;
    assert_eq!(partial_queue.usage(), (1, 1));
    partial_queue
        .try_push(text(""))
        .expect("cancellation while waiting for bytes releases the count permit");
}

#[tokio::test]
async fn test_close_releases_queued_items_and_wakes_blocked_pushers() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("a")).expect("fill queue");

    let waiting_queue = queue.clone();
    let waiting = tokio::spawn(async move { waiting_queue.push(text("b")).await });
    tokio::task::yield_now().await;
    queue.close();

    assert_eq!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .expect("closed queue wakes blocked pushers")
            .expect("push task joins"),
        Err(WsSendError::Closed)
    );
    assert_eq!(queue.usage(), (0, 0));
    assert_eq!(queue.try_push(text("c")), Err(WsSendError::Closed));
    assert!(
        timeout(Duration::from_secs(1), queue.pop())
            .await
            .expect("closed queue pop resolves")
            .is_none()
    );
}

#[tokio::test]
async fn test_repeated_close_and_waiter_cancellation_always_terminate() {
    for iteration in 0..32 {
        let queue = BoundedQueue::new(1, 1);
        queue.try_push(text("x")).expect("fill queue");

        let waiting_queue = queue.clone();
        let waiting = tokio::spawn(async move { waiting_queue.push(text("y")).await });
        tokio::task::yield_now().await;

        if iteration % 2 == 0 {
            waiting.abort();
            timeout(Duration::from_secs(1), waiting)
                .await
                .expect("cancelled waiter terminates")
                .expect_err("aborted waiter reports cancellation");
            queue.close();
        } else {
            queue.close();
            assert_eq!(
                timeout(Duration::from_secs(1), waiting)
                    .await
                    .expect("close wakes waiter")
                    .expect("push task joins"),
                Err(WsSendError::Closed)
            );
        }

        assert_eq!(queue.usage(), (0, 0));
        assert!(
            timeout(Duration::from_secs(1), queue.pop())
                .await
                .expect("closed queue pop resolves")
                .is_none()
        );
    }
}

#[test]
fn test_queue_reports_usage_and_capacity_errors() {
    let count_limited = BoundedQueue::new(1, 2);
    count_limited.try_push(WsMessage::Text("a".to_owned())).unwrap();
    assert_eq!(count_limited.usage(), (1, 1));
    assert_eq!(
        count_limited.try_push(WsMessage::Text("b".to_owned())),
        Err(WsSendError::Backpressure)
    );
    count_limited.close();
    assert_eq!(count_limited.usage(), (0, 0));
    assert_eq!(
        count_limited.try_push(WsMessage::Text("c".to_owned())),
        Err(WsSendError::Closed)
    );

    let byte_limited = BoundedQueue::new(2, 1);
    byte_limited.try_push(WsMessage::Text("x".to_owned())).unwrap();
    assert_eq!(
        byte_limited.try_push(WsMessage::Text("y".to_owned())),
        Err(WsSendError::Backpressure)
    );
}
