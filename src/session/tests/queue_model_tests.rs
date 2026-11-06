// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use bytes::Bytes;

use crate::error::WsSendError;
use crate::session::internal::BoundedQueue;
use crate::ws_message::WsMessage;

const MESSAGE_LIMIT: usize = 2;
const BYTE_LIMIT: usize = 16;
type PendingSender<'a> = Pin<Box<dyn Future<Output = Result<(), WsSendError>> + 'a>>;

#[derive(Clone, Debug)]
enum QueueOp {
    TryPush { id: usize, bytes: usize },
    StartSend { id: usize, bytes: usize },
    PollSend { id: usize },
    CancelSend { id: usize },
    PopHold,
    DropEnvelope { id: usize },
    Close,
}

#[derive(Default)]
struct NoopWake {
    calls: AtomicUsize,
}

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

fn message(id: usize, bytes: usize) -> WsMessage {
    assert!(bytes >= 3);
    let prefix = format!("{id:03}");
    WsMessage::Binary(Bytes::from(format!("{prefix}{}", "x".repeat(bytes - 3))))
}

fn message_id(message: &WsMessage) -> usize {
    let WsMessage::Binary(bytes) = message else {
        panic!("model queue stores binary payloads")
    };
    std::str::from_utf8(&bytes[..3]).unwrap().parse().unwrap()
}

fn can_admit(used_messages: usize, used_bytes: usize, bytes: usize) -> bool {
    used_messages < MESSAGE_LIMIT && bytes <= BYTE_LIMIT - used_bytes
}

fn expected_error(closed: bool, bytes: usize, used_messages: usize, used_bytes: usize) -> Option<WsSendError> {
    if closed {
        Some(WsSendError::Closed)
    } else if bytes > BYTE_LIMIT {
        Some(WsSendError::MessageTooLarge)
    } else if !can_admit(used_messages, used_bytes, bytes) {
        Some(WsSendError::Backpressure)
    } else {
        None
    }
}

fn assert_usage(queue: &BoundedQueue, live: &BTreeMap<usize, usize>, context: &str) {
    let expected = (live.len(), live.values().sum());
    assert_eq!(queue.usage(), expected, "{context}");
}

fn track(trace: &mut Vec<QueueOp>, op: QueueOp) {
    match &op {
        QueueOp::TryPush { id, bytes } | QueueOp::StartSend { id, bytes } => {
            assert!(*id < 1_000_000 && *bytes <= 1_000);
        }
        QueueOp::PollSend { id } | QueueOp::CancelSend { id } | QueueOp::DropEnvelope { id } => {
            assert!(*id < 1_000_000);
        }
        QueueOp::PopHold | QueueOp::Close => {}
    }
    trace.push(op);
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn generated_message(id: usize, bytes: usize) -> WsMessage {
    assert!(bytes >= 6);
    let prefix = format!("{id:06}");
    WsMessage::Binary(Bytes::from(format!("{prefix}{}", "y".repeat(bytes - 6))))
}

fn generated_message_id(message: &WsMessage) -> usize {
    let WsMessage::Binary(bytes) = message else {
        panic!("generated model queue stores binary payloads")
    };
    std::str::from_utf8(&bytes[..6]).unwrap().parse().unwrap()
}

fn generated_can_admit(live: &BTreeMap<usize, usize>, bytes: usize, max_messages: usize, max_bytes: usize) -> bool {
    live.len() < max_messages && bytes <= max_bytes - live.values().sum::<usize>()
}

fn generated_expected_error(
    live: &BTreeMap<usize, usize>,
    bytes: usize,
    closed: bool,
    max_messages: usize,
    max_bytes: usize,
) -> Option<WsSendError> {
    if closed {
        Some(WsSendError::Closed)
    } else if bytes > max_bytes {
        Some(WsSendError::MessageTooLarge)
    } else if !generated_can_admit(live, bytes, max_messages, max_bytes) {
        Some(WsSendError::Backpressure)
    } else {
        None
    }
}

#[test]
fn test_queue_model_real_queue_traces() {
    let seeds = [1_u64, 0x9847_0bb0, 0xbbfe_461c, 0x9e37_79b9_7f4a_7c15];
    for seed in seeds {
        let queue = BoundedQueue::new(MESSAGE_LIMIT, BYTE_LIMIT);
        let waker = Waker::from(Arc::new(NoopWake::default()));
        let mut context = Context::from_waker(&waker);
        let mut fifo = VecDeque::new();
        let mut held = BTreeMap::new();
        let mut live = BTreeMap::new();
        let mut senders: BTreeMap<usize, PendingSender<'_>> = BTreeMap::new();
        let mut trace = Vec::new();
        let closed = false;

        for (id, bytes) in [(0, 3), (1, 5)] {
            let op = QueueOp::TryPush { id, bytes };
            assert_eq!(expected_error(closed, bytes, live.len(), live.values().sum()), None);
            queue.try_push(message(id, bytes)).expect("initial capacity fits");
            live.insert(id, bytes);
            fifo.push_back(id);
            track(&mut trace, op);
            assert_usage(&queue, &live, &format!("seed={seed} trace={trace:?}"));
        }

        // Register more senders than the selector threshold while both budgets are
        // occupied.
        for offset in 0..129 {
            let id = 100 + offset;
            let bytes = 3 + ((seed.wrapping_add(offset as u64 * 17)) as usize % 14);
            let op = QueueOp::StartSend { id, bytes };
            let mut future = Box::pin(queue.push(message(id, bytes)));
            assert!(
                future.as_mut().poll(&mut context).is_pending(),
                "sender {id} starts pending"
            );
            senders.insert(id, future);
            track(&mut trace, op);
            assert_usage(&queue, &live, &format!("seed={seed} start sender {id}"));
            assert_eq!(queue.waiter_snapshot_for_test().len(), offset + 1);
        }
        let registered = queue.waiter_snapshot_for_test();
        assert_eq!(registered.len(), 129, "seed={seed}");
        assert!(registered.iter().all(|(_, _, notified)| !notified));
        assert_usage(&queue, &live, &format!("seed={seed} trace={trace:?}"));
        let canceled_id = 100 + ((seed as usize) % 129);
        track(&mut trace, QueueOp::CancelSend { id: canceled_id });
        senders.remove(&canceled_id);
        assert_eq!(
            queue.waiter_snapshot_for_test().len(),
            128,
            "pending cancellation unregisters sender"
        );
        assert_usage(&queue, &live, &format!("seed={seed} after cancel"));
        let mut random = seed.max(1);
        for _ in 0..24 {
            let sender_ids = senders.keys().copied().collect::<Vec<_>>();
            let id = sender_ids[(next_random(&mut random) as usize) % sender_ids.len()];
            track(&mut trace, QueueOp::PollSend { id });
            assert_eq!(senders.get_mut(&id).unwrap().as_mut().poll(&mut context), Poll::Pending);
            assert_usage(&queue, &live, &format!("seed={seed} generated poll trace={trace:?}"));
        }
        trace.push(QueueOp::TryPush { id: 301, bytes: 3 });
        assert_eq!(
            expected_error(closed, 3, live.len(), live.values().sum()),
            Some(WsSendError::Backpressure)
        );
        assert_eq!(queue.try_push(message(301, 3)), Err(WsSendError::Backpressure));
        assert_usage(&queue, &live, &format!("seed={seed} full try push"));

        // Pop does not release the reservation while the caller retains its envelope.
        trace.push(QueueOp::PopHold);
        let first = queue.try_pop().expect("first queued envelope");
        let first_id = fifo.pop_front().unwrap();
        assert_eq!(message_id(first.item_for_test()), first_id);
        held.insert(first_id, first);
        assert_usage(&queue, &live, &format!("seed={seed} after pop hold"));

        trace.push(QueueOp::DropEnvelope { id: first_id });
        drop(held.remove(&first_id).unwrap());
        live.remove(&first_id);
        assert_usage(&queue, &live, &format!("seed={seed} after envelope drop"));

        // A competing immediate sender consumes the released capacity before the
        // notified sender is polled.
        let competitor = 300;
        let competitor_bytes = 3;
        trace.push(QueueOp::TryPush {
            id: competitor,
            bytes: competitor_bytes,
        });
        assert!(can_admit(live.len(), live.values().sum(), competitor_bytes));
        queue
            .try_push(message(competitor, competitor_bytes))
            .expect("competitor wins capacity");
        live.insert(competitor, competitor_bytes);
        fifo.push_back(competitor);
        assert_usage(&queue, &live, &format!("seed={seed} competitor"));

        let notified_registration_id = queue
            .waiter_snapshot_for_test()
            .into_iter()
            .find(|(_, _, notified)| *notified)
            .map(|(id, _, _)| id)
            .expect("release selected a sender");
        let notified = 100 + notified_registration_id;
        trace.push(QueueOp::PollSend { id: notified });
        let pending = senders.get_mut(&notified).unwrap().as_mut().poll(&mut context);
        assert_eq!(
            pending,
            Poll::Pending,
            "notification does not reserve capacity; trace={trace:?}"
        );
        assert!(
            queue
                .waiter_snapshot_for_test()
                .iter()
                .all(|(_, _, selected)| !selected)
        );

        // Drain in actual FIFO order and drop each envelope to feed the next sender.
        while let Some(expected_id) = fifo.front().copied() {
            trace.push(QueueOp::PopHold);
            let envelope = queue.try_pop().expect("reference FIFO item has a real envelope");
            let actual_id = message_id(envelope.item_for_test());
            assert_eq!(actual_id, expected_id, "seed={seed} trace={trace:?}");
            fifo.pop_front();
            held.insert(actual_id, envelope);
            assert_usage(&queue, &live, &format!("seed={seed} held id={actual_id}"));
            if actual_id == competitor {
                trace.push(QueueOp::DropEnvelope { id: actual_id });
                drop(held.remove(&actual_id).unwrap());
                live.remove(&actual_id);
                assert_usage(&queue, &live, &format!("seed={seed} competitor released"));
            } else if live.len() == MESSAGE_LIMIT {
                // Release one held envelope to trigger another selection.
                let release_id = *held.keys().next().unwrap();
                trace.push(QueueOp::DropEnvelope { id: release_id });
                drop(held.remove(&release_id).unwrap());
                live.remove(&release_id);
                assert_usage(&queue, &live, &format!("seed={seed} release id={release_id}"));
            }
            if let Some((registration_id, _, _)) = queue
                .waiter_snapshot_for_test()
                .into_iter()
                .find(|(_, _, notified)| *notified)
            {
                let send_id = 100 + registration_id;
                trace.push(QueueOp::PollSend { id: send_id });
                let can_fit = can_admit(live.len(), live.values().sum(), senders_payload_size(send_id, seed));
                let result = senders.get_mut(&send_id).unwrap().as_mut().poll(&mut context);
                if can_fit {
                    assert_eq!(
                        result,
                        Poll::Ready(Ok(())),
                        "selected sender fits; seed={seed} trace={trace:?}"
                    );
                    let bytes = senders_payload_size(send_id, seed);
                    live.insert(send_id, bytes);
                    fifo.push_back(send_id);
                    senders.remove(&send_id);
                } else {
                    assert_eq!(
                        result,
                        Poll::Pending,
                        "selected sender may lose capacity race; seed={seed} trace={trace:?}"
                    );
                }
            }
            assert_usage(&queue, &live, &format!("seed={seed} trace={trace:?}"));
        }

        // Close broadcasts to every remaining pending future. Completed futures are
        // removed below.
        track(&mut trace, QueueOp::Close);
        queue.close();
        queue.close();
        let cancel_id = *senders.keys().next().expect("pending senders remain");
        track(&mut trace, QueueOp::CancelSend { id: cancel_id });
        let before_cancel = queue.waiter_snapshot_for_test().len();
        senders.remove(&cancel_id);
        assert_eq!(
            queue.waiter_snapshot_for_test().len(),
            before_cancel - 1,
            "cancellation unregisters its waiter"
        );
        for (&id, future) in &mut senders {
            assert_eq!(
                future.as_mut().poll(&mut context),
                Poll::Ready(Err(WsSendError::Closed)),
                "close resolves waiter id={id} seed={seed} trace={trace:?}"
            );
        }
        // The queue drops its queued envelopes on close; caller-held envelopes retain
        // their budgets.
        for id in fifo.drain(..) {
            live.remove(&id);
        }
        assert_usage(&queue, &live, &format!("seed={seed} after close"));
        drop(senders);
        for (_, envelope) in std::mem::take(&mut held) {
            drop(envelope);
        }
        live.clear();
        assert_eq!(queue.usage(), (0, 0), "seed={seed} cleanup trace={trace:?}");
    }

    let byte_queue = BoundedQueue::new(2, 6);
    byte_queue.try_push(message(401, 6)).unwrap();
    assert_eq!(
        byte_queue.usage(),
        (1, 6),
        "byte budget exhausted while message budget remains"
    );
    assert_eq!(byte_queue.try_push(message(402, 3)), Err(WsSendError::Backpressure));
    let byte_hold = byte_queue.try_pop().unwrap();
    assert_eq!(byte_queue.usage(), (1, 6));
    drop(byte_hold);
    assert_eq!(byte_queue.usage(), (0, 0));

    let close_queue = BoundedQueue::new(2, BYTE_LIMIT);
    close_queue.try_push(message(401, 3)).unwrap();
    let held_across_close = close_queue.try_pop().unwrap();
    close_queue.try_push(message(402, 5)).unwrap();
    close_queue.close();
    assert_eq!(
        close_queue.usage(),
        (1, 3),
        "close drops queued item but retains external envelope"
    );
    drop(held_across_close);
    assert_eq!(close_queue.usage(), (0, 0));

    let zero_queue = BoundedQueue::new(1, 1);
    zero_queue.try_push(WsMessage::Text(String::new())).unwrap();
    assert_eq!(zero_queue.usage(), (1, 0));
    assert_eq!(
        zero_queue.try_push(WsMessage::Text(String::new())),
        Err(WsSendError::Backpressure)
    );
    let held = zero_queue.try_pop().unwrap();
    assert_eq!(
        zero_queue.usage(),
        (1, 0),
        "zero-byte envelope still uses message capacity"
    );
    drop(held);
    assert_eq!(zero_queue.usage(), (0, 0));
    zero_queue.try_push(WsMessage::Text(String::new())).unwrap();
    zero_queue.close();
    assert_eq!(zero_queue.usage(), (0, 0));

    let precedence_queue = BoundedQueue::new(1, 1);
    assert_eq!(
        precedence_queue.try_push(message(403, 3)),
        Err(WsSendError::MessageTooLarge)
    );
    precedence_queue.close();
    assert_eq!(precedence_queue.try_push(message(404, 3)), Err(WsSendError::Closed));
}

#[test]
fn test_queue_model_real_queue_generated_traces() {
    let seed_override = std::env::var("QUBIT_WS_MODEL_SEED")
        .ok()
        .map(|value| value.parse::<u64>().expect("invalid QUBIT_WS_MODEL_SEED"));
    let steps = std::env::var("QUBIT_WS_MODEL_STEPS")
        .ok()
        .map(|value| value.parse::<usize>().expect("invalid QUBIT_WS_MODEL_STEPS"))
        .unwrap_or(2048);
    let default_seeds = [1_u64, 0x9847_0bb0, 0xbbfe_461c, 0x9e37_79b9_7f4a_7c15];
    let seeds = seed_override.map_or_else(|| default_seeds.to_vec(), |seed| vec![seed]);
    const MAX_MESSAGES: usize = 4;
    const MAX_BYTES: usize = 32;
    let prefix = [
        QueueOp::TryPush { id: 0, bytes: 8 },
        QueueOp::TryPush { id: 1, bytes: 8 },
        QueueOp::TryPush { id: 2, bytes: 8 },
        QueueOp::TryPush { id: 3, bytes: 8 },
        QueueOp::StartSend { id: 4, bytes: 7 },
        QueueOp::PollSend { id: 4 },
        QueueOp::CancelSend { id: 4 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 0 },
        QueueOp::StartSend { id: 5, bytes: 6 },
        QueueOp::TryPush { id: 6, bytes: 6 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 1 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 2 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 3 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 5 },
        QueueOp::TryPush { id: 7, bytes: 32 },
        QueueOp::TryPush { id: 8, bytes: 6 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 7 },
        QueueOp::TryPush { id: 9, bytes: 6 },
        QueueOp::TryPush { id: 10, bytes: 6 },
        QueueOp::TryPush { id: 11, bytes: 6 },
        QueueOp::TryPush { id: 12, bytes: 6 },
        QueueOp::TryPush { id: 13, bytes: 6 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 9 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 10 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 11 },
        QueueOp::PopHold,
        QueueOp::DropEnvelope { id: 12 },
    ];
    assert!(
        steps > prefix.len(),
        "QUBIT_WS_MODEL_STEPS must exceed the deterministic prefix"
    );
    assert!(
        steps < 1_000_000,
        "QUBIT_WS_MODEL_STEPS must keep generated IDs within six digits"
    );

    for seed in seeds {
        let queue = BoundedQueue::new(MAX_MESSAGES, MAX_BYTES);
        let waker = Waker::from(Arc::new(NoopWake::default()));
        let mut context = Context::from_waker(&waker);
        let mut rng = seed.max(1);
        let mut trace = Vec::with_capacity(steps);
        let mut counts = [0_usize; 7];
        let mut fifo = VecDeque::new();
        let mut held = BTreeMap::new();
        let mut live = BTreeMap::<usize, usize>::new();
        let mut pending = BTreeMap::<usize, PendingSender<'_>>::new();
        let mut sender_bytes = BTreeMap::<usize, usize>::new();
        let mut registrations = BTreeMap::<usize, usize>::new();
        let mut next_id = prefix.len() + 10;
        let mut closed = false;
        let mut saw_message_capacity_full = false;
        let mut saw_byte_capacity_full = false;
        let mut saw_pending_sender = false;
        let mut saw_held_envelope = false;

        for step in 0..steps {
            let op = if step < prefix.len() {
                prefix[step].clone()
            } else if step + 1 == steps {
                QueueOp::Close
            } else {
                let random = next_random(&mut rng);
                match random % 6 {
                    0 => {
                        let id = next_id;
                        next_id += 1;
                        QueueOp::TryPush {
                            id,
                            bytes: 6 + (next_random(&mut rng) as usize % 35),
                        }
                    }
                    1 => {
                        let id = next_id;
                        next_id += 1;
                        QueueOp::StartSend {
                            id,
                            bytes: 6 + (next_random(&mut rng) as usize % 35),
                        }
                    }
                    2 if !pending.is_empty() => {
                        let keys = pending.keys().copied().collect::<Vec<_>>();
                        QueueOp::PollSend {
                            id: keys[next_random(&mut rng) as usize % keys.len()],
                        }
                    }
                    3 if !pending.is_empty() => {
                        let keys = pending.keys().copied().collect::<Vec<_>>();
                        QueueOp::CancelSend {
                            id: keys[next_random(&mut rng) as usize % keys.len()],
                        }
                    }
                    4 => QueueOp::PopHold,
                    5 if !held.is_empty() => {
                        let keys = held.keys().copied().collect::<Vec<_>>();
                        QueueOp::DropEnvelope {
                            id: keys[next_random(&mut rng) as usize % keys.len()],
                        }
                    }
                    _ => {
                        let id = next_id;
                        next_id += 1;
                        QueueOp::TryPush {
                            id,
                            bytes: 6 + (next_random(&mut rng) as usize % 35),
                        }
                    }
                }
            };
            if matches!(op, QueueOp::Close) {
                for id in pending.keys().copied().collect::<Vec<_>>() {
                    counts[3] += 1;
                    track(&mut trace, QueueOp::CancelSend { id });
                    pending.remove(&id);
                    sender_bytes.remove(&id);
                    registrations.remove(&id);
                }
                while let Some(expected_id) = fifo.pop_front() {
                    counts[4] += 1;
                    track(&mut trace, QueueOp::PopHold);
                    let envelope = queue
                        .try_pop()
                        .expect("model FIFO predicts a queue item before close tail");
                    assert_eq!(
                        generated_message_id(envelope.item_for_test()),
                        expected_id,
                        "seed={seed} step={step} trace={trace:?}"
                    );
                    held.insert(expected_id, envelope);
                }
                for id in held.keys().copied().collect::<Vec<_>>() {
                    counts[5] += 1;
                    track(&mut trace, QueueOp::DropEnvelope { id });
                    drop(held.remove(&id).unwrap());
                    live.remove(&id);
                }
                assert_eq!(
                    queue.usage(),
                    (0, 0),
                    "seed={seed} step={step} pre-close cleanup trace={trace:?}"
                );
                for _ in 0..MAX_MESSAGES {
                    let id = next_id;
                    next_id += 1;
                    counts[0] += 1;
                    track(&mut trace, QueueOp::TryPush { id, bytes: 8 });
                    queue
                        .try_push(generated_message(id, 8))
                        .expect("close tail fills queue");
                    live.insert(id, 8);
                    fifo.push_back(id);
                }
                let id = next_id;
                next_id += 1;
                counts[1] += 1;
                track(&mut trace, QueueOp::StartSend { id, bytes: 7 });
                let mut future = Box::pin(queue.push(generated_message(id, 7)));
                assert!(
                    future.as_mut().poll(&mut context).is_pending(),
                    "close tail sender remains pending"
                );
                pending.insert(id, future);
                sender_bytes.insert(id, 7);
                let registration_id = queue
                    .waiter_snapshot_for_test()
                    .last()
                    .expect("close tail waiter registered")
                    .0;
                registrations.insert(id, registration_id);
                counts[4] += 1;
                track(&mut trace, QueueOp::PopHold);
                let expected_id = fifo.pop_front().unwrap();
                let envelope = queue.try_pop().expect("close tail holds one envelope");
                assert_eq!(generated_message_id(envelope.item_for_test()), expected_id);
                held.insert(expected_id, envelope);
                assert_eq!(
                    queue.usage(),
                    (MAX_MESSAGES, MAX_BYTES),
                    "seed={seed} step={step} close fixture trace={trace:?}"
                );
            }
            let op_kind = match op {
                QueueOp::TryPush { .. } => 0,
                QueueOp::StartSend { .. } => 1,
                QueueOp::PollSend { .. } => 2,
                QueueOp::CancelSend { .. } => 3,
                QueueOp::PopHold => 4,
                QueueOp::DropEnvelope { .. } => 5,
                QueueOp::Close => 6,
            };
            counts[op_kind] += 1;
            track(&mut trace, op.clone());
            match op.clone() {
                QueueOp::TryPush { id, bytes } => {
                    let expected = generated_expected_error(&live, bytes, closed, MAX_MESSAGES, MAX_BYTES);
                    let result = queue.try_push(generated_message(id, bytes));
                    assert_eq!(
                        result,
                        expected.map_or(Ok(()), Err),
                        "seed={seed} step={step} capacity=({MAX_MESSAGES}, {MAX_BYTES}) trace={trace:?} model={:?} actual_usage={:?}",
                        (
                            fifo.iter().copied().collect::<Vec<_>>(),
                            held.keys().copied().collect::<Vec<_>>(),
                            live.clone()
                        ),
                        queue.usage()
                    );
                    if result.is_ok() {
                        live.insert(id, bytes);
                        fifo.push_back(id);
                    }
                }
                QueueOp::StartSend { id, bytes } => {
                    let expected = generated_expected_error(&live, bytes, closed, MAX_MESSAGES, MAX_BYTES);
                    let prior_waiters = queue
                        .waiter_snapshot_for_test()
                        .into_iter()
                        .map(|(waiter_id, _, _)| waiter_id)
                        .collect::<std::collections::BTreeSet<_>>();
                    let mut future = Box::pin(queue.push(generated_message(id, bytes)));
                    let result = future.as_mut().poll(&mut context);
                    match expected {
                        None => {
                            assert_eq!(result, Poll::Ready(Ok(())), "seed={seed} step={step} trace={trace:?}");
                            live.insert(id, bytes);
                            fifo.push_back(id);
                        }
                        Some(WsSendError::Backpressure) => {
                            assert_eq!(result, Poll::Pending, "seed={seed} step={step} trace={trace:?}");
                            pending.insert(id, future);
                            sender_bytes.insert(id, bytes);
                            saw_pending_sender = true;
                            let registration_id = queue
                                .waiter_snapshot_for_test()
                                .into_iter()
                                .map(|(waiter_id, _, _)| waiter_id)
                                .find(|waiter_id| !prior_waiters.contains(waiter_id))
                                .expect("pending send registers a waiter");
                            registrations.insert(id, registration_id);
                        }
                        Some(error) => assert_eq!(
                            result,
                            Poll::Ready(Err(error)),
                            "seed={seed} step={step} trace={trace:?}"
                        ),
                    }
                }
                QueueOp::PollSend { id } => {
                    if let Some(future) = pending.get_mut(&id) {
                        let selected = registrations.get(&id).is_some_and(|registration_id| {
                            queue
                                .waiter_snapshot_for_test()
                                .iter()
                                .any(|(waiter_id, _, notified)| waiter_id == registration_id && *notified)
                        });
                        let expected_fit =
                            generated_can_admit(&live, sender_bytes[&id], MAX_MESSAGES, MAX_BYTES) && selected;
                        let result = future.as_mut().poll(&mut context);
                        if closed {
                            assert_eq!(
                                result,
                                Poll::Ready(Err(WsSendError::Closed)),
                                "seed={seed} step={step} trace={trace:?}"
                            );
                            pending.remove(&id);
                            sender_bytes.remove(&id);
                            registrations.remove(&id);
                        } else if expected_fit {
                            assert_eq!(result, Poll::Ready(Ok(())), "seed={seed} step={step} trace={trace:?}");
                            let bytes = sender_bytes.remove(&id).unwrap();
                            pending.remove(&id);
                            registrations.remove(&id);
                            live.insert(id, bytes);
                            fifo.push_back(id);
                        } else {
                            assert_eq!(result, Poll::Pending, "seed={seed} step={step} trace={trace:?}");
                        }
                    } else {
                        panic!("generated PollSend references no pending future: seed={seed} step={step} op={op:?}");
                    }
                }
                QueueOp::CancelSend { id } => {
                    assert!(
                        pending.remove(&id).is_some(),
                        "cancel references pending future: seed={seed} step={step} op={op:?}"
                    );
                    sender_bytes.remove(&id);
                    registrations.remove(&id);
                }
                QueueOp::PopHold => {
                    if let Some(expected_id) = fifo.pop_front() {
                        let envelope = queue.try_pop().expect("model FIFO predicts a queue item");
                        let actual_id = generated_message_id(envelope.item_for_test());
                        assert_eq!(actual_id, expected_id, "seed={seed} step={step} trace={trace:?}");
                        held.insert(actual_id, envelope);
                        saw_held_envelope = true;
                    } else {
                        assert!(
                            queue.try_pop().is_none(),
                            "seed={seed} step={step} empty FIFO disagrees with queue"
                        );
                    }
                }
                QueueOp::DropEnvelope { id } => {
                    let envelope = held.remove(&id).expect("generated DropEnvelope references held ID");
                    drop(envelope);
                    live.remove(&id);
                }
                QueueOp::Close => {
                    assert!(!closed, "generated trace closes exactly once");
                    assert!(!pending.is_empty(), "seed={seed} close must exercise pending futures");
                    assert!(
                        !held.is_empty(),
                        "seed={seed} close must retain at least one external Envelope"
                    );
                    queue.close();
                    closed = true;
                    for id in fifo.drain(..) {
                        live.remove(&id);
                    }
                }
            }
            let model_snapshot = format!(
                "fifo={fifo:?} held={:?} live={live:?} pending={:?} closed={closed}",
                held.keys().collect::<Vec<_>>(),
                pending.keys().collect::<Vec<_>>()
            );
            let actual_usage = queue.usage();
            let expected_usage = (live.len(), live.values().sum::<usize>());
            saw_message_capacity_full |= expected_usage.0 == MAX_MESSAGES;
            saw_byte_capacity_full |= expected_usage.1 == MAX_BYTES;
            let actual_waiters = queue.waiter_snapshot_for_test();
            assert_eq!(
                actual_usage, expected_usage,
                "seed={seed} step={step} capacity=({MAX_MESSAGES}, {MAX_BYTES}) trace={trace:?} model={model_snapshot} actual=(usage={actual_usage:?}, waiters={actual_waiters:?})"
            );
            assert_eq!(
                actual_waiters.len(),
                pending.len(),
                "seed={seed} step={step} capacity=({MAX_MESSAGES}, {MAX_BYTES}) trace={trace:?} model={model_snapshot} actual=(usage={actual_usage:?}, waiters={actual_waiters:?})"
            );
        }

        assert!(closed);
        for (&id, future) in &mut pending {
            assert_eq!(
                future.as_mut().poll(&mut context),
                Poll::Ready(Err(WsSendError::Closed)),
                "seed={seed} close cleanup pending id={id} trace={trace:?}"
            );
        }
        pending.clear();
        sender_bytes.clear();
        assert_eq!(
            queue.waiter_snapshot_for_test().len(),
            0,
            "seed={seed} all closed waiters removed"
        );
        for (_, envelope) in std::mem::take(&mut held) {
            drop(envelope);
        }
        live.clear();
        assert_eq!(
            queue.usage(),
            (0, 0),
            "seed={seed} final usage; steps={steps} counts={counts:?} trace={trace:?}"
        );
        assert!(
            counts.iter().all(|count| *count > 0),
            "seed={seed} operation coverage counts={counts:?}"
        );
        assert!(
            saw_message_capacity_full && saw_byte_capacity_full && saw_pending_sender && saw_held_envelope,
            "seed={seed} state coverage: messages_full={saw_message_capacity_full} bytes_full={saw_byte_capacity_full} pending={saw_pending_sender} held={saw_held_envelope}"
        );
    }
}

fn senders_payload_size(id: usize, seed: u64) -> usize {
    3 + ((seed.wrapping_add((id - 100) as u64 * 17)) as usize % 14)
}
