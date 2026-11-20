// Project:   scalo
// File:      tests/delivery_invariants.rs
// Purpose:   Delivery invariants every approach must keep, on shaped senders in virtual time
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The six delivery invariants, checked for each registered approach on shaped
//! senders under tokio's paused clock:
//!
//! 1. No record is released as delivered before its destination confirms.
//! 2. A held answer never says taken before every destination it was routed to
//!    confirms.
//! 3. A held batch with a record for a sick destination is refused whole, and
//!    another caller's batch that touches no sick destination is neither
//!    refused nor delayed by it.
//! 4. A cancelled request releases its memory lease.
//! 5. A refused record maps to the right caller and the right record.
//! 6. Kafka per-key order survives a full producer queue, and a batch larger
//!    than the queue drains through it.
//!
//! Each invariant is a `check_*` function and one test per approach. Where an
//! approach does not keep an invariant, its test asserts what it does instead
//! and is named for it.

#![cfg(feature = "transport-test-support")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;

use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::test_support::delivery::{
    Answer, AnyApproach, CallerBatch, DeliveryApproach, DestinationId, Destinations, Outcome,
    RoutedRecord, Shape, approach,
};
use scalo::transport::test_support::shaped::{
    Down, Faults, OutcomeMode, ShapedConfig, ShapedSender,
};
use scalo::transport::{PayloadFormat, Record, RecordMeta};

const INLINE: &str = "inline_sequential";
const PER_RECORD: &str = "per_record_futures";
const PARALLEL: &str = "parallel_fanout";
const PER_DESTINATION: &str = "batch_per_destination";
const MERGE_CHANNEL: &str = "merge_channel";
const MERGE_SWAP: &str = "merge_swap";

/// The approaches that send each destination one block per caller batch, or
/// merge those blocks: each keeps every invariant.
const BLOCK_APPROACHES: [&str; 4] = [PARALLEL, PER_DESTINATION, MERGE_CHANNEL, MERGE_SWAP];

fn named(name: &str) -> AnyApproach {
    approach(name).unwrap_or_else(|| panic!("{name} is registered"))
}

fn destinations(senders: &[(&str, &ShapedSender)]) -> Arc<Destinations<ShapedSender>> {
    let guard = Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1 << 40,
        ..MemoryGuardConfig::default()
    }));
    let mut set = Destinations::new(guard);
    for (name, sender) in senders {
        // Kafka-shaped senders are named from "k".
        let shape = if name.starts_with('k') {
            Shape::PerRecord
        } else {
            Shape::Batch
        };
        set.add_shaped(name, (*sender).clone(), shape);
    }
    Arc::new(set)
}

fn record(payload: impl Into<Bytes>, routes: &[DestinationId]) -> RoutedRecord {
    RoutedRecord {
        record: Record {
            payload: payload.into(),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
        routes: Arc::from(routes),
    }
}

fn kafka(window_ms: u64) -> ShapedConfig {
    ShapedConfig {
        call_latency: Duration::from_millis(2),
        confirm_window: Some(Duration::from_millis(window_ms)),
        outcome: OutcomeMode::PerRecord,
        ..ShapedConfig::default()
    }
}

fn grpc(latency_ms: u64) -> ShapedConfig {
    ShapedConfig {
        call_latency: Duration::from_millis(latency_ms),
        ..ShapedConfig::default()
    }
}

fn down() -> ShapedConfig {
    ShapedConfig {
        faults: Faults {
            down: Some(Down {
                after: Duration::from_millis(1),
            }),
            ..Faults::default()
        },
        ..ShapedConfig::default()
    }
}

/// Spawn a delivery so its progress can be read while it runs.
fn spawn(
    approach: AnyApproach,
    set: &Arc<Destinations<ShapedSender>>,
    records: Vec<RoutedRecord>,
    answer: Answer,
) -> tokio::task::JoinHandle<Vec<Outcome>> {
    let set = Arc::clone(set);
    tokio::spawn(async move {
        approach
            .deliver(&set, CallerBatch { records, answer })
            .await
    })
}

// 1. No record is released as delivered before its destination confirms.

async fn check_no_release_before_confirm(name: &str) {
    let k = ShapedSender::new("k", kafka(50));
    let g = ShapedSender::new("g", grpc(30));
    let set = destinations(&[("k", &k), ("g", &g)]);
    let (kid, gid) = (DestinationId(0), DestinationId(1));
    let records = vec![
        record("a", &[kid]),
        record("b", &[gid]),
        record("c", &[kid, gid]),
    ];
    let handle = spawn(named(name), &set, records, Answer::PerRecord);

    // The Kafka-shaped window closes at 50 ms and confirms 2 ms later.
    tokio::time::sleep(Duration::from_millis(51)).await;
    assert!(
        !handle.is_finished(),
        "{name} answered before kafka confirmed"
    );
    let outcomes = handle.await.expect("delivery task");
    assert_eq!(outcomes, [Outcome::Sent; 3], "{name}");
    assert_eq!(
        k.stats().records_confirmed,
        2,
        "{name}: kafka confirmed a and c"
    );
    assert_eq!(
        g.stats().records_confirmed,
        2,
        "{name}: grpc confirmed b and c"
    );
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_releases_nothing_before_its_destination_confirms() {
    check_no_release_before_confirm(INLINE).await;
}

#[tokio::test(start_paused = true)]
async fn per_record_futures_releases_nothing_before_its_destination_confirms() {
    check_no_release_before_confirm(PER_RECORD).await;
}

// 2. A held answer never says taken before every destination confirms.

async fn check_held_answer_waits_for_every_destination(name: &str) {
    let k = ShapedSender::new("k", kafka(10));
    let slow = ShapedSender::new("slow", grpc(100));
    let set = destinations(&[("k", &k), ("slow", &slow)]);
    let routes = [DestinationId(0), DestinationId(1)];
    let records: Vec<RoutedRecord> = (0..4).map(|i| record(format!("{i}"), &routes)).collect();
    let handle = spawn(named(name), &set, records.clone(), Answer::Held);
    tokio::time::sleep(Duration::from_millis(99)).await;
    assert!(
        !handle.is_finished(),
        "{name} answered before the slow destination confirmed"
    );
    assert_eq!(
        handle.await.expect("delivery task"),
        [Outcome::Sent; 4],
        "{name}"
    );

    let k = ShapedSender::new("k", kafka(10));
    let gone = ShapedSender::new("gone", down());
    let set = destinations(&[("k", &k), ("gone", &gone)]);
    let outcomes = named(name)
        .deliver(
            &set,
            CallerBatch {
                records,
                answer: Answer::Held,
            },
        )
        .await;
    assert!(
        outcomes.iter().all(|o| *o != Outcome::Sent),
        "{name}: a held batch with a destination down answered {outcomes:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_holds_its_answer_until_every_destination_confirms() {
    check_held_answer_waits_for_every_destination(INLINE).await;
}

#[tokio::test(start_paused = true)]
async fn per_record_futures_holds_its_answer_until_every_destination_confirms() {
    check_held_answer_waits_for_every_destination(PER_RECORD).await;
}

// 3. A sick destination refuses a held batch whole, and no other caller.

/// A caller can only be told to send the whole request again, so a held batch
/// with one record for a sick destination is refused whole.
async fn check_held_batch_refused_whole(name: &str) {
    let well = ShapedSender::new("well", grpc(5));
    let sick = ShapedSender::new("sick", down());
    let set = destinations(&[("sick", &sick), ("well", &well)]);
    let outcomes = named(name)
        .deliver(
            &set,
            CallerBatch {
                records: vec![
                    record("s", &[DestinationId(0)]),
                    record("w", &[DestinationId(1)]),
                ],
                answer: Answer::Held,
            },
        )
        .await;
    assert!(
        outcomes.iter().all(|o| o.is_undelivered()),
        "{name}: a held batch with a record for a down destination answered {outcomes:?}"
    );
}

/// Another caller's batch that touches no sick destination is answered in its
/// own time, whether the sick one is slow or down.
async fn check_other_caller_untouched(name: &str) {
    let slow = ShapedConfig {
        faults: Faults {
            extra_latency: Duration::from_secs(1),
            ..Faults::default()
        },
        ..grpc(5)
    };
    for (label, sick_config) in [("slow", slow), ("down", down())] {
        let well = ShapedSender::new("well", grpc(5));
        let sick = ShapedSender::new("sick", sick_config);
        let set = destinations(&[("sick", &sick), ("well", &well)]);
        let to_sick = spawn(
            named(name),
            &set,
            vec![
                record("s", &[DestinationId(0)]),
                record("sw", &[DestinationId(0), DestinationId(1)]),
            ],
            Answer::Held,
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
        let start = Instant::now();
        let outcomes = named(name)
            .deliver(
                &set,
                CallerBatch {
                    records: vec![
                        record("w1", &[DestinationId(1)]),
                        record("w2", &[DestinationId(1)]),
                    ],
                    answer: Answer::Held,
                },
            )
            .await;
        assert_eq!(outcomes, [Outcome::Sent; 2], "{name}, sick {label}");
        assert!(
            start.elapsed() <= Duration::from_millis(10),
            "{name}, sick {label}: the other caller waited {:?}",
            start.elapsed()
        );
        to_sick.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_refuses_a_held_batch_with_a_sick_destination_whole() {
    check_held_batch_refused_whole(INLINE).await;
}

#[tokio::test(start_paused = true)]
async fn per_record_futures_refuses_a_held_batch_with_a_sick_destination_whole() {
    check_held_batch_refused_whole(PER_RECORD).await;
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_leaves_another_callers_batch_untouched_by_a_sick_destination() {
    check_other_caller_untouched(INLINE).await;
}

#[tokio::test(start_paused = true)]
async fn per_record_futures_leaves_another_callers_batch_untouched_by_a_sick_destination() {
    check_other_caller_untouched(PER_RECORD).await;
}

// 4. A cancelled request releases its memory lease.

async fn check_cancel_releases_lease(name: &str) {
    let slow = ShapedSender::new("slow", grpc(1000));
    let k = ShapedSender::new("k", kafka(1000));
    let set = destinations(&[("slow", &slow), ("k", &k)]);
    let routes = [DestinationId(0), DestinationId(1)];
    let records: Vec<RoutedRecord> = (0..10)
        .map(|i| record(vec![b'x'; 100 + i], &routes))
        .collect();
    let handle = spawn(named(name), &set, records, Answer::Held);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        set.ledger().held_bytes() > 0,
        "{name}: no lease while delivering"
    );
    handle.abort();
    assert!(handle.await.is_err_and(|e| e.is_cancelled()));
    assert_eq!(
        set.ledger().held_bytes(),
        0,
        "{name}: lease kept after cancel"
    );
    assert_eq!(
        set.ledger().guard_reserved_bytes(),
        0,
        "{name}: guard kept the bytes"
    );
    assert_eq!(
        slow.bytes_in_flight() + k.bytes_in_flight(),
        0,
        "{name}: queue kept the bytes"
    );
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_releases_its_lease_when_cancelled() {
    check_cancel_releases_lease(INLINE).await;
}

#[tokio::test(start_paused = true)]
async fn per_record_futures_releases_its_lease_when_cancelled() {
    check_cancel_releases_lease(PER_RECORD).await;
}

// 5. A refused record maps to the right caller and the right record.

async fn check_refusal_maps_to_its_record(name: &str) {
    let refusing = ShapedSender::new(
        "refusing",
        ShapedConfig {
            faults: Faults {
                refuse: Some(|p| p.first() == Some(&b'!')),
                ..Faults::default()
            },
            ..grpc(5)
        },
    );
    let k = ShapedSender::new("k", kafka(10));
    let set = destinations(&[("k", &k), ("refusing", &refusing)]);
    let routes = [DestinationId(0), DestinationId(1)];
    let caller_a: Vec<RoutedRecord> = (0..5)
        .map(|i| {
            record(
                if i == 3 {
                    "!a3".to_string()
                } else {
                    format!("a{i}")
                },
                &routes,
            )
        })
        .collect();
    let caller_b: Vec<RoutedRecord> = (0..5).map(|i| record(format!("b{i}"), &routes)).collect();
    let a = spawn(named(name), &set, caller_a, Answer::Held);
    let b = spawn(named(name), &set, caller_b, Answer::Held);
    let mut expected_a = [Outcome::Sent; 5];
    expected_a[3] = Outcome::DeadLettered;
    assert_eq!(a.await.expect("caller a"), expected_a, "{name}: caller a");
    assert_eq!(
        b.await.expect("caller b"),
        [Outcome::Sent; 5],
        "{name}: caller b"
    );
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_maps_a_refusal_to_its_caller_and_record() {
    check_refusal_maps_to_its_record(INLINE).await;
}

#[tokio::test(start_paused = true)]
async fn per_record_futures_maps_a_refusal_to_its_caller_and_record() {
    check_refusal_maps_to_its_record(PER_RECORD).await;
}

// 6. Kafka per-key order survives, and a batch larger than the queue drains
//    through it.

/// A Kafka-shaped queue of 6 bytes that waits up to 100 ms for room and logs
/// the order records enter it.
fn small_kafka_queue() -> ShapedSender {
    ShapedSender::new(
        "k",
        ShapedConfig {
            queue_cap_bytes: 6,
            queue_full_wait: Duration::from_millis(100),
            log_order: true,
            ..kafka(20)
        },
    )
}

/// Ten records, each one byte holding its index, except record 4, which is
/// three bytes: it does not fit beside records 0 to 3, and record 5 does.
fn mixed_sizes(route: DestinationId) -> Vec<RoutedRecord> {
    (0..10u8)
        .map(|i| {
            let payload = if i == 4 { vec![i; 3] } else { vec![i] };
            record(payload, &[route])
        })
        .collect()
}

/// Deliver the mixed batch through a small queue and return the outcomes and
/// the order the records entered the queue.
async fn through_a_full_queue(name: &str) -> (Vec<Outcome>, Vec<u8>) {
    let k = small_kafka_queue();
    let set = destinations(&[("k", &k)]);
    let outcomes = named(name)
        .deliver(
            &set,
            CallerBatch {
                records: mixed_sizes(DestinationId(0)),
                answer: Answer::PerRecord,
            },
        )
        .await;
    let order: Vec<u8> = k.queued_payloads().iter().map(|p| p[0]).collect();
    assert_eq!(k.bytes_in_flight(), 0, "{name}: the queue drained");
    (outcomes, order)
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_keeps_kafka_order_through_a_full_queue() {
    let (outcomes, order) = through_a_full_queue(INLINE).await;
    assert_eq!(outcomes, [Outcome::Sent; 10]);
    assert_eq!(order, (0..10).collect::<Vec<u8>>());
}

/// The real Kafka sender does the same: `delivery_path --tier 2 --order-check`.
#[tokio::test(start_paused = true)]
async fn per_record_futures_lets_a_later_record_pass_an_earlier_one_at_a_full_kafka_queue() {
    let (outcomes, order) = through_a_full_queue(PER_RECORD).await;
    assert_eq!(outcomes, [Outcome::Sent; 10]);
    assert_eq!(order, ORDER_PER_RECORD);
}

/// The order `per_record_futures` puts the mixed batch into the small queue.
const ORDER_PER_RECORD: [u8; 10] = [0, 1, 2, 3, 5, 6, 4, 7, 8, 9];

async fn check_batch_larger_than_queue_completes(name: &str) {
    let k = ShapedSender::new(
        "k",
        ShapedConfig {
            queue_cap_bytes: 4,
            queue_full_wait: Duration::from_millis(100),
            ..kafka(20)
        },
    );
    let set = destinations(&[("k", &k)]);
    let records: Vec<RoutedRecord> = (0..20u8)
        .map(|i| record(vec![i], &[DestinationId(0)]))
        .collect();
    let outcomes = named(name)
        .deliver(
            &set,
            CallerBatch {
                records,
                answer: Answer::Held,
            },
        )
        .await;
    assert_eq!(outcomes, [Outcome::Sent; 20], "{name}");
    let stats = k.stats();
    assert_eq!(stats.records_confirmed, 20, "{name}");
    assert_eq!(stats.queue_full_refusals, 0, "{name}");
    assert!(stats.queue_full_waits > 0, "{name}: the queue never filled");
}

#[tokio::test(start_paused = true)]
async fn inline_sequential_drains_a_batch_larger_than_the_queue() {
    check_batch_larger_than_queue_completes(INLINE).await;
}

#[tokio::test(start_paused = true)]
async fn per_record_futures_drains_a_batch_larger_than_the_queue() {
    check_batch_larger_than_queue_completes(PER_RECORD).await;
}

#[test]
fn every_registered_approach_has_its_invariant_tests() {
    let names: Vec<&str> = scalo::transport::test_support::delivery::REGISTRY
        .iter()
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        names,
        [
            INLINE,
            PER_RECORD,
            PARALLEL,
            PER_DESTINATION,
            MERGE_CHANNEL,
            MERGE_SWAP
        ],
        "add the new approach's invariant tests here"
    );
}

// Every invariant for the block and merge approaches, one test each.

#[tokio::test(start_paused = true)]
async fn block_approaches_release_nothing_before_their_destination_confirms() {
    for name in BLOCK_APPROACHES {
        check_no_release_before_confirm(name).await;
    }
}

#[tokio::test(start_paused = true)]
async fn block_approaches_hold_their_answer_until_every_destination_confirms() {
    for name in BLOCK_APPROACHES {
        check_held_answer_waits_for_every_destination(name).await;
    }
}

#[tokio::test(start_paused = true)]
async fn block_approaches_refuse_a_held_batch_with_a_sick_destination_whole() {
    for name in BLOCK_APPROACHES {
        check_held_batch_refused_whole(name).await;
    }
}

#[tokio::test(start_paused = true)]
async fn block_approaches_leave_another_callers_batch_untouched_by_a_sick_destination() {
    for name in BLOCK_APPROACHES {
        check_other_caller_untouched(name).await;
    }
}

#[tokio::test(start_paused = true)]
async fn block_approaches_release_their_lease_when_cancelled() {
    for name in BLOCK_APPROACHES {
        check_cancel_releases_lease(name).await;
    }
}

#[tokio::test(start_paused = true)]
async fn block_approaches_map_a_refusal_to_its_caller_and_record() {
    for name in BLOCK_APPROACHES {
        check_refusal_maps_to_its_record(name).await;
    }
}

#[tokio::test(start_paused = true)]
async fn block_approaches_keep_kafka_order_through_a_full_queue() {
    for name in BLOCK_APPROACHES {
        let (outcomes, order) = through_a_full_queue(name).await;
        assert_eq!(outcomes, [Outcome::Sent; 10], "{name}");
        assert_eq!(order, (0..10).collect::<Vec<u8>>(), "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn block_approaches_drain_a_batch_larger_than_the_queue() {
    for name in BLOCK_APPROACHES {
        check_batch_larger_than_queue_completes(name).await;
    }
}

/// Two callers' blocks for one gRPC-shaped destination go out as one call when
/// they arrive within the merge window, and each caller gets its own answer.
#[tokio::test(start_paused = true)]
async fn merge_approaches_send_two_callers_blocks_as_one_call() {
    for name in [MERGE_CHANNEL, MERGE_SWAP] {
        let g = ShapedSender::new("g", grpc(5));
        let set = destinations(&[("g", &g)]);
        let merge = approach(&format!("{name}/w2/b65536")).expect("a merge spec parses");
        let a = {
            let set = Arc::clone(&set);
            let merge = merge.clone();
            tokio::spawn(async move {
                merge
                    .deliver(
                        &set,
                        CallerBatch {
                            records: vec![record("a", &[DestinationId(0)])],
                            answer: Answer::Held,
                        },
                    )
                    .await
            })
        };
        let b = merge
            .deliver(
                &set,
                CallerBatch {
                    records: vec![
                        record("b1", &[DestinationId(0)]),
                        record("b2", &[DestinationId(0)]),
                    ],
                    answer: Answer::Held,
                },
            )
            .await;
        assert_eq!(b, [Outcome::Sent; 2], "{name}");
        assert_eq!(a.await.expect("caller a"), [Outcome::Sent], "{name}");
        let stats = g.stats();
        assert_eq!(stats.calls, 1, "{name}: two callers, one call");
        assert_eq!(stats.max_batch, 3, "{name}");
        assert!(merge.peak_queued_bytes().is_some_and(|b| b >= 5), "{name}");
    }
}

/// Many callers on one merge stage: every block is answered, none is dropped
/// by a caller that stops waiting once its own answer arrives.
#[tokio::test(start_paused = true)]
async fn merge_approaches_answer_every_caller() {
    for name in [MERGE_CHANNEL, MERGE_SWAP] {
        let g = ShapedSender::new(
            "g",
            ShapedConfig {
                call_latency: Duration::from_millis(5),
                max_calls_in_flight: 4,
                ..ShapedConfig::default()
            },
        );
        let set = destinations(&[("g", &g)]);
        let merge = approach(&format!("{name}/w1/b4096")).expect("a merge spec parses");
        let mut callers = tokio::task::JoinSet::new();
        for caller in 0..64u8 {
            let set = Arc::clone(&set);
            let merge = merge.clone();
            callers.spawn(async move {
                let mut answers = Vec::new();
                for round in 0..5u8 {
                    // Staggered arrivals, so blocks are queued while an earlier drain is in flight.
                    tokio::time::sleep(Duration::from_millis(u64::from(caller % 7))).await;
                    let outcomes = merge
                        .deliver(
                            &set,
                            CallerBatch {
                                records: vec![record(vec![caller, round], &[DestinationId(0)])],
                                answer: Answer::Held,
                            },
                        )
                        .await;
                    answers.extend(outcomes);
                }
                answers
            });
        }
        let answers: Vec<Outcome> = callers.join_all().await.into_iter().flatten().collect();
        assert_eq!(answers.len(), 320, "{name}");
        assert!(
            answers.iter().all(|o| *o == Outcome::Sent),
            "{name}: {answers:?}"
        );
        assert_eq!(g.stats().records_confirmed, 320, "{name}");
    }
}
