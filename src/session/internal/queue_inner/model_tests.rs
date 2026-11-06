// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use tokio::sync::Notify;

use super::super::queue_state::QueueState;
use super::super::reservation::Reservation;
use super::QueueInner;

#[derive(Clone, Debug)]
enum Op {
    Register { bytes: usize },
    Wake,
    Reset { id: usize },
    Remove { id: usize },
    Reserve { bytes: usize },
    Release { reservation_id: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WaiterModel {
    id: usize,
    bytes: usize,
    notified: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Snapshot {
    waiters: Vec<WaiterModel>,
    used_messages: usize,
    used_bytes: usize,
    index_active: bool,
    index: BTreeMap<usize, BTreeSet<usize>>,
}

#[derive(Debug, PartialEq, Eq)]
struct Difference(String);

fn compare(expected: &Snapshot, actual: &Snapshot) -> Result<(), Difference> {
    if expected.waiters != actual.waiters {
        return Err(Difference(format!(
            "waiters expected={:?} actual={:?}",
            expected.waiters, actual.waiters
        )));
    }
    if expected.used_messages != actual.used_messages || expected.used_bytes != actual.used_bytes {
        return Err(Difference(format!(
            "accounting expected=({}, {}) actual=({}, {})",
            expected.used_messages, expected.used_bytes, actual.used_messages, actual.used_bytes
        )));
    }
    if expected.index_active != actual.index_active || expected.index != actual.index {
        return Err(Difference(format!(
            "index expected_active={} expected={:?} actual_active={} actual={:?}",
            expected.index_active, expected.index, actual.index_active, actual.index
        )));
    }
    Ok(())
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

struct Harness {
    inner: Arc<QueueInner>,
    waiters: BTreeMap<usize, (usize, super::super::waiter_registration::WaiterRegistration)>,
    reservations: BTreeMap<usize, Reservation>,
    reservation_bytes: BTreeMap<usize, usize>,
    expected_waiters: Vec<WaiterModel>,
    next_reservation_id: usize,
    expected_messages: usize,
    expected_bytes: usize,
    expected_index_active: bool,
}

impl Harness {
    fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(QueueInner {
                max_messages,
                max_bytes,
                state: Mutex::new(QueueState {
                    items: VecDeque::new(),
                    used_messages: 0,
                    used_bytes: 0,
                    closed: false,
                    waiters: BTreeMap::new(),
                    waiters_by_bytes: BTreeMap::new(),
                    waiter_index_active: false,
                }),
                readable: Notify::new(),
                next_waiter_id: AtomicUsize::new(0),
            }),
            waiters: BTreeMap::new(),
            reservations: BTreeMap::new(),
            reservation_bytes: BTreeMap::new(),
            expected_waiters: Vec::new(),
            next_reservation_id: 0,
            expected_messages: 0,
            expected_bytes: 0,
            expected_index_active: false,
        }
    }

    fn wake_expected(&mut self) {
        let free_messages = self.inner.max_messages - self.expected_messages;
        let free_bytes = self.inner.max_bytes - self.expected_bytes;
        if free_messages == 0 {
            return;
        }
        if let Some(waiter) = self
            .expected_waiters
            .iter_mut()
            .find(|waiter| !waiter.notified && waiter.bytes <= free_bytes)
        {
            waiter.notified = true;
        }
    }

    fn apply(&mut self, op: &Op) -> Result<(), String> {
        match *op {
            Op::Register { bytes } => {
                let guard = self.inner.register_waiter(bytes);
                let id = self.inner.next_waiter_id.load(std::sync::atomic::Ordering::Relaxed) - 1;
                self.waiters.insert(id, (bytes, guard));
                self.expected_waiters.push(WaiterModel {
                    id,
                    bytes,
                    notified: false,
                });
                if !self.expected_index_active && self.expected_waiters.len() > 128 {
                    self.expected_index_active = true;
                }
            }
            Op::Wake => {
                self.wake_expected();
                self.inner.wake_capacity_waiter();
            }
            Op::Reset { id } => {
                let Some(position) = self.expected_waiters.iter().position(|w| w.id == id) else {
                    return Err(format!("reset unknown waiter {id}"));
                };
                let was_notified = self.expected_waiters[position].notified;
                self.expected_waiters[position].notified = false;
                self.waiters.get(&id).expect("registered guard").1.reset();
                if was_notified {
                    self.wake_expected();
                }
            }
            Op::Remove { id } => {
                let Some(position) = self.expected_waiters.iter().position(|w| w.id == id) else {
                    return Err(format!("remove unknown waiter {id}"));
                };
                let removed = self.expected_waiters.remove(position);
                self.waiters.remove(&id);
                if self.expected_index_active && self.expected_waiters.len() <= 64 {
                    self.expected_index_active = false;
                }
                if removed.notified {
                    self.wake_expected();
                }
            }
            Op::Reserve { bytes } => {
                if self.expected_messages >= self.inner.max_messages
                    || bytes > self.inner.max_bytes - self.expected_bytes
                {
                    return Err(format!("illegal reserve of {bytes}"));
                }
                self.expected_messages += 1;
                self.expected_bytes += bytes;
                let mut state = self.inner.state.lock().unwrap();
                state.used_messages += 1;
                state.used_bytes += bytes;
                drop(state);
                let id = self.next_reservation_id;
                self.next_reservation_id += 1;
                self.reservations
                    .insert(id, Reservation::new(self.inner.clone(), bytes));
                self.reservation_bytes.insert(id, bytes);
            }
            Op::Release { reservation_id } => {
                let bytes = self
                    .reservation_bytes
                    .remove(&reservation_id)
                    .ok_or_else(|| format!("release unknown reservation {reservation_id}"))?;
                let reservation = self
                    .reservations
                    .remove(&reservation_id)
                    .ok_or_else(|| format!("release unknown reservation {reservation_id}"))?;
                self.expected_messages -= 1;
                self.expected_bytes -= bytes;
                self.wake_expected();
                drop(reservation);
            }
        }
        Ok(())
    }

    fn actual_snapshot(&self) -> Snapshot {
        let state = self.inner.state.lock().unwrap();
        let actual_waiters = state
            .waiters
            .iter()
            .map(|(&id, waiter)| WaiterModel {
                id,
                bytes: waiter.bytes,
                notified: waiter.notified,
            })
            .collect::<Vec<_>>();
        Snapshot {
            waiters: actual_waiters,
            used_messages: state.used_messages,
            used_bytes: state.used_bytes,
            index_active: state.waiter_index_active,
            index: state.waiters_by_bytes.clone(),
        }
    }
}

fn apply_and_compare(harness: &mut Harness, op: Op, seed: u64, step: usize, trace: &mut Vec<Op>) {
    harness
        .apply(&op)
        .unwrap_or_else(|error| panic!("seed={seed} step={step} op={op:?} error={error} trace={trace:?}"));
    let expected = expected_snapshot(harness);
    let actual = harness.actual_snapshot();
    if let Err(difference) = compare(&expected, &actual) {
        panic!("seed={seed} step={step} ops={trace:?} next={op:?} difference={difference:?}");
    }
    trace.push(op);
}

fn trace_is_legal(ops: &[Op]) -> bool {
    let mut waiters = BTreeSet::new();
    let mut reservations = BTreeMap::new();
    let mut next_waiter = 0;
    let mut next_reservation = 0;
    let mut used_messages = 0;
    let mut used_bytes = 0;
    for op in ops {
        match *op {
            Op::Register { .. } => {
                waiters.insert(next_waiter);
                next_waiter += 1;
            }
            Op::Reset { id } | Op::Remove { id } if !waiters.contains(&id) => return false,
            Op::Remove { id } => {
                waiters.remove(&id);
            }
            Op::Release { reservation_id } => {
                let Some(bytes) = reservations.remove(&reservation_id) else {
                    return false;
                };
                used_messages -= 1;
                used_bytes -= bytes;
            }
            Op::Reserve { bytes } => {
                if used_messages >= 8 || bytes > 128 - used_bytes {
                    return false;
                }
                reservations.insert(next_reservation, bytes);
                next_reservation += 1;
                used_messages += 1;
                used_bytes += bytes;
            }
            _ => {}
        }
    }
    true
}

fn shrink_trace(mut trace: Vec<Op>, fails: impl Fn(&[Op]) -> bool) -> Vec<Op> {
    let mut chunk = trace.len().max(1) / 2;
    while chunk > 0 {
        let mut start = 0;
        let mut changed = false;
        while start < trace.len() {
            let end = (start + chunk).min(trace.len());
            let mut candidate = trace.clone();
            candidate.drain(start..end);
            if trace_is_legal(&candidate) && fails(&candidate) {
                trace = candidate;
                changed = true;
            } else {
                start = end;
            }
        }
        if !changed {
            chunk /= 2;
        }
    }
    trace
}

fn trace_fails_in_class(ops: &[Op], expected_class: &str) -> bool {
    let mut harness = Harness::new(8, 128);
    for op in ops {
        if harness.apply(op).is_err() {
            return true;
        }
        if let Err(Difference(detail)) = compare(&expected_snapshot(&harness), &harness.actual_snapshot()) {
            return detail.split_whitespace().next() == Some(expected_class);
        }
    }
    false
}

fn synthetic_wrong_selection_fails(ops: &[Op]) -> bool {
    let mut waiters = Vec::<(usize, usize, bool)>::new();
    let mut next_id = 0;
    for op in ops {
        match *op {
            Op::Register { bytes } => {
                waiters.push((next_id, bytes, false));
                next_id += 1;
            }
            Op::Wake => {
                let expected = waiters
                    .iter()
                    .find(|(_, bytes, notified)| !notified && *bytes <= 128)
                    .map(|(id, _, _)| *id);
                let wrong = waiters
                    .iter()
                    .rev()
                    .find(|(_, bytes, notified)| !notified && *bytes <= 128)
                    .map(|(id, _, _)| *id);
                if expected != wrong {
                    return true;
                }
                if let Some(id) = expected {
                    waiters.iter_mut().find(|(waiter_id, _, _)| *waiter_id == id).unwrap().2 = true;
                }
            }
            Op::Reset { id } => {
                if let Some(waiter) = waiters.iter_mut().find(|(waiter_id, _, _)| *waiter_id == id) {
                    waiter.2 = false;
                }
            }
            Op::Remove { id } => waiters.retain(|(waiter_id, _, _)| *waiter_id != id),
            Op::Reserve { .. } | Op::Release { .. } => {}
        }
    }
    false
}

#[test]
fn test_queue_model_oracle_rejects_wrong_selection() {
    let expected = Snapshot {
        waiters: vec![WaiterModel {
            id: 0,
            bytes: 1,
            notified: true,
        }],
        used_messages: 0,
        used_bytes: 0,
        index_active: false,
        index: BTreeMap::new(),
    };
    let mut actual = expected.clone();
    actual.waiters[0].notified = false;
    assert!(compare(&expected, &actual).is_err());
}

#[test]
fn test_queue_model_oracle_rejects_stale_index() {
    let expected = Snapshot {
        waiters: Vec::new(),
        used_messages: 0,
        used_bytes: 0,
        index_active: true,
        index: BTreeMap::new(),
    };
    let mut actual = expected.clone();
    actual.index.insert(4, BTreeSet::from([7]));
    assert!(compare(&expected, &actual).is_err());
}

#[test]
fn test_queue_model_oracle_rejects_accounting_mismatch() {
    let expected = Snapshot {
        waiters: Vec::new(),
        used_messages: 1,
        used_bytes: 4,
        index_active: false,
        index: BTreeMap::new(),
    };
    let mut actual = expected.clone();
    actual.used_bytes = 3;
    assert!(compare(&expected, &actual).is_err());
}

#[test]
fn test_queue_model_shrinker_preserves_wrong_selection_failure() {
    let trace = vec![
        Op::Register { bytes: 99 },
        Op::Wake,
        Op::Register { bytes: 3 },
        Op::Register { bytes: 1 },
        Op::Wake,
        Op::Register { bytes: 7 },
    ];
    assert!(trace_is_legal(&trace));
    assert!(synthetic_wrong_selection_fails(&trace));
    let shrunk = shrink_trace(trace.clone(), synthetic_wrong_selection_fails);
    assert!(trace_is_legal(&shrunk));
    assert!(synthetic_wrong_selection_fails(&shrunk));
    assert!(
        shrunk.len() < trace.len(),
        "shrinker should remove irrelevant operations: {shrunk:?}"
    );
}

#[test]
fn test_queue_model_selector_traces() {
    let seeds = [1, 0x9847_0bb0, 0xbbfe_461c, 0x9e37_79b9_7f4a_7c15];
    let seed_override = std::env::var("QUBIT_WS_MODEL_SEED")
        .ok()
        .map(|s| s.parse::<u64>().expect("invalid QUBIT_WS_MODEL_SEED"));
    let steps = std::env::var("QUBIT_WS_MODEL_STEPS")
        .ok()
        .map(|s| s.parse::<usize>().expect("invalid QUBIT_WS_MODEL_STEPS"))
        .unwrap_or(2048);
    let seeds = seed_override.map_or_else(|| seeds.to_vec(), |seed| vec![seed]);
    for seed in seeds {
        let mut competition = Harness::new(8, 128);
        let mut competition_trace = Vec::new();
        apply_and_compare(
            &mut competition,
            Op::Reserve { bytes: 128 },
            seed,
            0,
            &mut competition_trace,
        );
        apply_and_compare(
            &mut competition,
            Op::Register { bytes: 1 },
            seed,
            1,
            &mut competition_trace,
        );
        apply_and_compare(
            &mut competition,
            Op::Register { bytes: 1 },
            seed,
            2,
            &mut competition_trace,
        );
        apply_and_compare(
            &mut competition,
            Op::Release { reservation_id: 0 },
            seed,
            3,
            &mut competition_trace,
        );
        let selected = competition.expected_waiters.iter().find(|w| w.notified).unwrap().id;
        apply_and_compare(
            &mut competition,
            Op::Reserve { bytes: 128 },
            seed,
            4,
            &mut competition_trace,
        );
        apply_and_compare(
            &mut competition,
            Op::Reset { id: selected },
            seed,
            5,
            &mut competition_trace,
        );
        assert!(competition.expected_waiters.iter().all(|w| !w.notified));
        apply_and_compare(
            &mut competition,
            Op::Release { reservation_id: 1 },
            seed,
            6,
            &mut competition_trace,
        );
        assert!(competition.expected_waiters.iter().any(|w| w.notified));
        for id in competition.expected_waiters.iter().map(|w| w.id).collect::<Vec<_>>() {
            apply_and_compare(&mut competition, Op::Remove { id }, seed, 7, &mut competition_trace);
        }
        let mut zero_byte = Harness::new(8, 128);
        let mut zero_trace = Vec::new();
        apply_and_compare(&mut zero_byte, Op::Reserve { bytes: 128 }, seed, 0, &mut zero_trace);
        apply_and_compare(&mut zero_byte, Op::Register { bytes: 0 }, seed, 1, &mut zero_trace);
        apply_and_compare(
            &mut zero_byte,
            Op::Release { reservation_id: 0 },
            seed,
            2,
            &mut zero_trace,
        );
        assert!(
            zero_byte.expected_waiters[0].notified,
            "zero-byte waiter fits with no free bytes"
        );
        apply_and_compare(&mut zero_byte, Op::Remove { id: 0 }, seed, 3, &mut zero_trace);
        let mut harness = Harness::new(8, 128);
        let mut state = seed.max(1);
        let mut ops = Vec::new();
        for index in 0..129 {
            apply_and_compare(
                &mut harness,
                Op::Register {
                    bytes: if index == 0 { 0 } else { index % 129 },
                },
                seed,
                index,
                &mut ops,
            );
            if [63, 64, 65, 127, 128, 129].contains(&(index + 1)) {
                assert_eq!(harness.expected_waiters.len(), index + 1);
                assert_eq!(harness.expected_index_active, index + 1 > 128);
            }
        }
        apply_and_compare(&mut harness, Op::Wake, seed, 129, &mut ops);
        let notified = harness
            .expected_waiters
            .iter()
            .find(|w| w.notified)
            .expect("prefix wake selects a waiter")
            .id;
        apply_and_compare(&mut harness, Op::Reset { id: notified }, seed, 130, &mut ops);
        for index in 0..65 {
            let id = harness.expected_waiters[0].id;
            apply_and_compare(&mut harness, Op::Remove { id }, seed, 131 + index, &mut ops);
        }
        assert!(!harness.expected_index_active, "index retires at 64 waiters");
        for index in 0..65 {
            apply_and_compare(
                &mut harness,
                Op::Register { bytes: index % 65 },
                seed,
                196 + index,
                &mut ops,
            );
        }
        assert!(harness.expected_index_active, "index reactivates above 128 waiters");
        for id in harness.expected_waiters.iter().map(|w| w.id).collect::<Vec<_>>() {
            apply_and_compare(&mut harness, Op::Remove { id }, seed, 261, &mut ops);
        }
        ops.clear();
        for index in 0..steps {
            let choice = next_random(&mut state) % 6;
            let op = match choice {
                0 if harness.expected_waiters.len() < 256 => Op::Register {
                    bytes: (next_random(&mut state) % 129) as usize,
                },
                1 => Op::Wake,
                2 if !harness.expected_waiters.is_empty() => Op::Reset {
                    id: harness.expected_waiters[(next_random(&mut state) as usize) % harness.expected_waiters.len()]
                        .id,
                },
                3 if !harness.expected_waiters.is_empty() => Op::Remove {
                    id: harness.expected_waiters[(next_random(&mut state) as usize) % harness.expected_waiters.len()]
                        .id,
                },
                4 if harness.expected_messages < 8 && harness.expected_bytes < 128 => Op::Reserve {
                    bytes: (next_random(&mut state) as usize) % (128 - harness.expected_bytes + 1),
                },
                _ if !harness.reservations.is_empty() => Op::Release {
                    reservation_id: *harness.reservations.keys().next().unwrap(),
                },
                _ => Op::Wake,
            };
            if let Err(error) = harness.apply(&op) {
                panic!("seed={seed} step={index} op={op:?} error={error} trace={ops:?}");
            }
            let expected = expected_snapshot(&harness);
            let actual = harness.actual_snapshot();
            if let Err(difference) = compare(&expected, &actual) {
                let failure_class = difference.0.split_whitespace().next().unwrap_or_default().to_owned();
                let mut failing_trace = ops.clone();
                failing_trace.push(op.clone());
                let minimized = shrink_trace(failing_trace, |candidate| {
                    trace_fails_in_class(candidate, &failure_class)
                });
                panic!(
                    "seed={seed} step={index} capacity=({}, {}) ops={ops:?} minimized={minimized:?} next={op:?} expected={expected:?} actual={actual:?} difference={difference:?}",
                    harness.inner.max_messages, harness.inner.max_bytes
                );
            }
            ops.push(op);
        }
        for id in harness.waiters.keys().copied().collect::<Vec<_>>() {
            apply_and_compare(&mut harness, Op::Remove { id }, seed, steps, &mut ops);
        }
        for reservation_id in harness.reservations.keys().copied().collect::<Vec<_>>() {
            apply_and_compare(&mut harness, Op::Release { reservation_id }, seed, steps, &mut ops);
        }
        assert_eq!(
            compare(&expected_snapshot(&harness), &harness.actual_snapshot()),
            Ok(())
        );
    }
}

fn expected_snapshot(harness: &Harness) -> Snapshot {
    let mut index = BTreeMap::<usize, BTreeSet<usize>>::new();
    if harness.expected_index_active {
        for waiter in harness.expected_waiters.iter().filter(|w| !w.notified) {
            index.entry(waiter.bytes).or_default().insert(waiter.id);
        }
    }
    Snapshot {
        waiters: harness.expected_waiters.clone(),
        used_messages: harness.expected_messages,
        used_bytes: harness.expected_bytes,
        index_active: harness.expected_index_active,
        index,
    }
}
