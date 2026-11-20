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
    RoutedRecord, approach,
};
use scalo::transport::test_support::shaped::{
    Down, Faults, OutcomeMode, ShapedConfig, ShapedSender,
};
use scalo::transport::{PayloadFormat, Record, RecordMeta};

const INLINE: &str = "inline_sequential";
const PER_RECORD: &str = "per_record_futures";

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
        set.add(name, (*sender).clone());
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
            start.elapsed() <= Duration::from_millis(5),
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
        [INLINE, PER_RECORD],
        "add the new approach's invariant tests here"
    );
}
