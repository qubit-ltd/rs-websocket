// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::poll_fn;
use futures_util::poll;
use tokio::spawn;
use tokio::sync::Barrier;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::time::timeout;

use crate::error::WsSendError;
use crate::session::internal::BoundedQueue;
use crate::ws_message::WsMessage;

fn text(payload: &str) -> WsMessage {
    WsMessage::Text(payload.to_owned())
}

#[tokio::test]
async fn test_closed_queue_takes_priority_over_oversized_payload() {
    let queue = BoundedQueue::new(1, 1);
    assert_eq!(queue.try_push(text("xx")), Err(WsSendError::MessageTooLarge));
    assert_eq!(queue.push(text("xx")).await, Err(WsSendError::MessageTooLarge));
    queue.close();
    assert_eq!(queue.try_push(text("xx")), Err(WsSendError::Closed));
    assert_eq!(queue.push(text("xx")).await, Err(WsSendError::Closed));
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
    let waiting = spawn(async move { waiting_queue.push(text("x")).await });
    yield_now().await;
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
    let pending = spawn(async move { pending_queue.push(text("b")).await });
    yield_now().await;
    pending.abort();
    let _ = pending.await;
    drop(cancelled_queue.pop().await.expect("release occupied slot"));
    cancelled_queue
        .try_push(text("b"))
        .expect("cancelled waiter leaked no permit");

    let partial_queue = BoundedQueue::new(2, 1);
    partial_queue.try_push(text("a")).expect("occupy byte permit");
    let pending_queue = partial_queue.clone();
    let pending = spawn(async move { pending_queue.push(text("b")).await });
    yield_now().await;
    assert_eq!(partial_queue.usage(), (1, 1));
    pending.abort();
    let _ = pending.await;
    assert_eq!(partial_queue.usage(), (1, 1));
    partial_queue
        .try_push(text(""))
        .expect("cancellation while waiting for bytes releases the count permit");
}

#[tokio::test]
async fn test_waiting_push_does_not_reserve_only_one_capacity_budget() {
    let queue = BoundedQueue::new(2, 1);
    queue.try_push(text("a")).expect("occupy byte budget");

    let mut waiting = Box::pin(queue.push(text("b")));
    assert!(poll!(waiting.as_mut()).is_pending());

    assert_eq!(queue.usage(), (1, 1));
    queue.try_push(text("")).expect("empty payload uses only a count slot");
    assert_eq!(queue.usage(), (2, 1));

    drop(queue.try_pop().expect("pop releases the occupied byte slot"));
    timeout(Duration::from_secs(1), waiting.as_mut())
        .await
        .expect("waiting push wakes after both budgets become available")
        .expect("waiting message is enqueued");

    assert_eq!(
        queue.try_pop().expect("empty message remains queued").into_item(),
        text("")
    );
    assert_eq!(
        queue.try_pop().expect("waiting message is queued").into_item(),
        text("b")
    );
}

#[tokio::test]
async fn test_different_sized_waiters_are_notified_when_capacity_changes() {
    let queue = BoundedQueue::new(2, 2);
    queue.try_push(text("")).expect("occupy one count slot");
    queue.try_push(text("a")).expect("occupy the remaining byte capacity");

    let big_queue = queue.clone();
    let (big_pending_tx, big_pending_rx) = oneshot::channel();
    let big = spawn(async move {
        let mut pending_signal = Some(big_pending_tx);
        let mut push = Box::pin(big_queue.push(text("bb")));
        poll_fn(|context| {
            let result = push.as_mut().poll(context);
            if result.is_pending()
                && let Some(signal) = pending_signal.take()
            {
                let _ = signal.send(());
            }
            result
        })
        .await
    });
    timeout(Duration::from_secs(1), big_pending_rx)
        .await
        .expect("big push reaches its wait state")
        .expect("big push signals its pending poll");

    let small_queue = queue.clone();
    let (small_pending_tx, small_pending_rx) = oneshot::channel();
    let small = spawn(async move {
        let mut pending_signal = Some(small_pending_tx);
        let mut push = Box::pin(small_queue.push(text("")));
        poll_fn(|context| {
            let result = push.as_mut().poll(context);
            if result.is_pending()
                && let Some(signal) = pending_signal.take()
            {
                let _ = signal.send(());
            }
            result
        })
        .await
    });
    timeout(Duration::from_secs(1), small_pending_rx)
        .await
        .expect("small push reaches its wait state")
        .expect("small push signals its pending poll");

    drop(queue.try_pop().expect("release the first count slot"));
    timeout(Duration::from_secs(1), small)
        .await
        .expect("small waiter is notified when a count slot is released")
        .expect("small push task joins")
        .expect("small message is enqueued");
    assert!(!big.is_finished(), "big waiter still lacks byte capacity");
    assert_eq!(queue.usage(), (2, 1));

    drop(queue.try_pop().expect("release the occupied byte capacity"));
    timeout(Duration::from_secs(1), big)
        .await
        .expect("big waiter is notified when byte capacity is released")
        .expect("big push task joins")
        .expect("big message is enqueued");
    assert_eq!(queue.usage(), (2, 2));
}

#[tokio::test]
async fn test_multiple_waiters_retry_after_capacity_is_released() {
    let queue = BoundedQueue::new(2, 4);
    queue.try_push(text("a")).expect("initial item fits");

    let first_queue = queue.clone();
    let first = spawn(async move { first_queue.push(text("bb")).await });
    let second_queue = queue.clone();
    let second = spawn(async move { second_queue.push(text("cc")).await });
    yield_now().await;

    drop(queue.pop().await.expect("release initial capacity"));
    drop(
        timeout(Duration::from_secs(1), queue.pop())
            .await
            .expect("a waiter enqueues after capacity is released")
            .expect("first waiting item is queued"),
    );
    drop(
        timeout(Duration::from_secs(1), queue.pop())
            .await
            .expect("the other waiter retries after capacity is released")
            .expect("second waiting item is queued"),
    );
    timeout(Duration::from_secs(1), first)
        .await
        .expect("first push task terminates")
        .expect("first push task joins")
        .expect("first push succeeds");
    timeout(Duration::from_secs(1), second)
        .await
        .expect("second push task terminates")
        .expect("second push task joins")
        .expect("second push succeeds");
}

#[tokio::test]
async fn test_close_wakes_all_blocked_pushers() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("a")).expect("fill queue");
    let mut waiters = (0..3)
        .map(|index| {
            let queue = queue.clone();
            let (pending_tx, pending_rx) = oneshot::channel();
            let waiter = spawn(async move {
                let mut pending_signal = Some(pending_tx);
                let mut push = Box::pin(queue.push(text(&index.to_string())));
                poll_fn(|context| {
                    let result = push.as_mut().poll(context);
                    if result.is_pending()
                        && let Some(signal) = pending_signal.take()
                    {
                        let _ = signal.send(());
                    }
                    result
                })
                .await
            });
            (waiter, pending_rx)
        })
        .collect::<Vec<_>>();

    for (_, pending_rx) in &mut waiters {
        timeout(Duration::from_secs(1), pending_rx)
            .await
            .expect("each push reaches its wait state")
            .expect("each push signals its pending poll");
    }

    queue.close();
    for (waiter, _) in waiters {
        assert_eq!(
            timeout(Duration::from_secs(1), waiter)
                .await
                .expect("close wakes every blocked pusher")
                .expect("push task joins"),
            Err(WsSendError::Closed)
        );
    }
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test]
async fn test_close_and_reservation_drop_can_race_without_leaking_capacity() {
    let queue = BoundedQueue::new(1, 1);
    queue.try_push(text("a")).expect("fill queue");
    let envelope = queue.try_pop().expect("pop keeps reservation alive");
    let barrier = Arc::new(Barrier::new(2));
    let drop_barrier = barrier.clone();
    let dropping = spawn(async move {
        drop_barrier.wait().await;
        drop(envelope);
    });

    barrier.wait().await;
    queue.close();
    timeout(Duration::from_secs(1), dropping)
        .await
        .expect("reservation drop completes while close races")
        .expect("drop task joins");
    assert_eq!(queue.usage(), (0, 0));
    assert_eq!(queue.try_push(text("b")), Err(WsSendError::Closed));
}

#[test]
fn test_popped_envelope_keeps_capacity_until_dropped() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("a")).expect("item fits");

    let envelope = queue.try_pop().expect("pop keeps reservation alive");
    assert_eq!(queue.usage(), (1, 1));
    assert_eq!(envelope.into_item(), text("a"));
    assert_eq!(queue.usage(), (0, 0));
}

#[tokio::test]
async fn test_close_releases_queued_items_and_wakes_blocked_pushers() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("a")).expect("fill queue");

    let waiting_queue = queue.clone();
    let waiting = spawn(async move { waiting_queue.push(text("b")).await });
    yield_now().await;
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
        let waiting = spawn(async move { waiting_queue.push(text("y")).await });
        yield_now().await;

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
    count_limited
        .try_push(WsMessage::Text("a".to_owned()))
        .expect("first item fits the count and byte limits");
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
    byte_limited
        .try_push(WsMessage::Text("x".to_owned()))
        .expect("first item fits the byte limit");
    assert_eq!(
        byte_limited.try_push(WsMessage::Text("y".to_owned())),
        Err(WsSendError::Backpressure)
    );
}
