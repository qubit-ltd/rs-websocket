mod message {
    pub(crate) use qubit_websocket::WsMessage;
}

mod error {
    pub(crate) use qubit_websocket::WsSendError;
}

#[path = "../src/session/queue.rs"]
mod queue;

use std::time::Duration;

use qubit_websocket::{WsMessage, WsSendError};
use queue::BoundedQueue;
use tokio::time::timeout;

fn text(payload: &str) -> WsMessage {
    WsMessage::Text(payload.to_owned())
}

#[tokio::test]
async fn queue_enforces_message_and_byte_limits_and_releases_permits_on_pop() {
    let queue = BoundedQueue::new(2, 5);
    queue.try_push(text("ab")).expect("2 bytes fit");
    queue.try_push(text("xyz")).expect("3 more bytes fit");
    assert_eq!(queue.try_push(text("")), Err(WsSendError::Backpressure));

    assert_eq!(queue.pop().await.expect("first item").item(), &text("ab"));
    queue
        .try_push(text("q"))
        .expect("pop releases count and byte permits");
    assert_eq!(
        queue.try_push(text("123456")),
        Err(WsSendError::MessageTooLarge)
    );

    let empty_queue = BoundedQueue::new(1, 1);
    empty_queue
        .try_push(text(""))
        .expect("empty text uses a count permit");
    assert_eq!(
        empty_queue.try_push(text("")),
        Err(WsSendError::Backpressure)
    );
}

#[tokio::test]
async fn async_push_waits_for_capacity_and_drop_cancels_without_leaking_permits() {
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
    partial_queue
        .try_push(text("a"))
        .expect("occupy byte permit");
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
async fn close_releases_queued_items_and_wakes_blocked_pushers() {
    let queue = BoundedQueue::new(1, 2);
    queue.try_push(text("a")).expect("fill queue");

    let waiting_queue = queue.clone();
    let waiting = tokio::spawn(async move { waiting_queue.push(text("b")).await });
    tokio::task::yield_now().await;
    queue.close();

    assert_eq!(
        waiting.await.expect("push task joins"),
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
