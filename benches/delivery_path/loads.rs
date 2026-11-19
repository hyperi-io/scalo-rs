// Project:   scalo
// File:      benches/delivery_path/loads.rs
// Purpose:   The named loads W1 to W7: caller groups and destination faults
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;
use std::time::Duration;

use scalo::transport::test_support::delivery::{Answer, DestinationId, Destinations, RoutedRecord};
use scalo::transport::test_support::shaped::{Down, ShapedConfig, Stall, grpc_like, kafka_like};
use scalo::transport::{PayloadFormat, Record, RecordMeta, TransportSender};

/// The Kafka-shaped destination every load routes to.
pub const KAFKA: &str = "kafka";
/// The gRPC-shaped destination both groups route to.
pub const GRPC_A: &str = "grpc-a";
/// The gRPC-shaped destination only the bulk group routes to, and the one a
/// fault makes sick.
pub const GRPC_B: &str = "grpc-b";

/// The wire key every record carries.
const TOPIC: &str = "bench-events";

/// First payload byte of a record the refusing destination dead-letters.
const REFUSED_MARK: u8 = b'!';

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Load {
    W1,
    W2,
    W3,
    W4,
    W5,
    W6,
    W7,
}

impl Load {
    pub const ALL: [Self; 7] = [
        Self::W1,
        Self::W2,
        Self::W3,
        Self::W4,
        Self::W5,
        Self::W6,
        Self::W7,
    ];

    pub fn parse(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|l| l.id() == id)
    }

    pub fn id(self) -> &'static str {
        match self {
            Self::W1 => "W1",
            Self::W2 => "W2",
            Self::W3 => "W3",
            Self::W4 => "W4",
            Self::W5 => "W5",
            Self::W6 => "W6",
            Self::W7 => "W7",
        }
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::W1 => "512 callers x 1 record x 200 B to kafka+grpc-a",
            Self::W2 => "8 callers x 1000 records x 1 KiB to kafka+grpc-a+grpc-b",
            Self::W3 => "W1 and W2 together",
            Self::W4 => "W3 with grpc-b 100 ms slow",
            Self::W5 => "W3 with grpc-b down",
            Self::W6 => "W3 with kafka confirmations stalling 500 ms every 2 s",
            Self::W7 => "W2 with one record per batch refused by grpc-a",
        }
    }

    /// Whether the load injects a fault, which only tier 1 can.
    #[cfg_attr(
        not(all(feature = "testcontainers", feature = "transport-grpc", unix)),
        allow(dead_code)
    )]
    pub fn has_fault(self) -> bool {
        matches!(self, Self::W4 | Self::W5 | Self::W6 | Self::W7)
    }

    /// The destination a fault makes sick, whose isolation is measured.
    pub fn sick(self) -> Option<&'static str> {
        match self {
            Self::W4 | Self::W5 => Some(GRPC_B),
            _ => None,
        }
    }

    fn small(self) -> bool {
        !matches!(self, Self::W2 | Self::W7)
    }

    fn bulk(self) -> bool {
        self != Self::W1
    }
}

/// The three destinations and their shapes under `load`.
pub fn topology(load: Load, seed: u64) -> [(&'static str, ShapedConfig); 3] {
    let mut kafka = ShapedConfig {
        seed,
        ..kafka_like()
    };
    let grpc_a_seed = seed.wrapping_add(1);
    let mut grpc_a = ShapedConfig {
        seed: grpc_a_seed,
        ..grpc_like()
    };
    let mut grpc_b = ShapedConfig {
        seed: seed.wrapping_add(2),
        ..grpc_like()
    };
    match load {
        Load::W4 => grpc_b.faults.extra_latency = Duration::from_millis(100),
        Load::W5 => {
            grpc_b.faults.down = Some(Down {
                after: grpc_b.call_latency,
            });
        }
        Load::W6 => {
            kafka.faults.stall = Some(Stall {
                period: Duration::from_secs(2),
                duration: Duration::from_millis(500),
            });
        }
        Load::W7 => grpc_a.faults.refuse = Some(|p| p.first() == Some(&REFUSED_MARK)),
        Load::W1 | Load::W2 | Load::W3 => {}
    }
    [(KAFKA, kafka), (GRPC_A, grpc_a), (GRPC_B, grpc_b)]
}

/// A set of identical callers.
pub struct Group {
    pub label: &'static str,
    pub routes: Vec<&'static str>,
    /// Each caller's batch, built once and offered again on every round.
    pub batches: Vec<Arc<[RoutedRecord]>>,
    pub answer: Answer,
}

impl Group {
    /// Whether the group's records go to `destination`.
    pub fn touches(&self, destination: &str) -> bool {
        self.routes.contains(&destination)
    }
}

/// The caller groups of `load`, routed in `destinations`.
pub fn groups<S: TransportSender>(load: Load, destinations: &Destinations<S>) -> Vec<Group> {
    let mut groups = Vec::new();
    if load.small() {
        groups.push(group(
            destinations,
            "small",
            512,
            1,
            200,
            &[KAFKA, GRPC_A],
            0,
        ));
    }
    if load.bulk() {
        let refused = usize::from(load == Load::W7);
        groups.push(group(
            destinations,
            "bulk",
            8,
            1000,
            1024,
            &[KAFKA, GRPC_A, GRPC_B],
            refused,
        ));
    }
    groups
}

fn group<S: TransportSender>(
    destinations: &Destinations<S>,
    label: &'static str,
    callers: usize,
    records: usize,
    size: usize,
    routes: &[&'static str],
    refused_per_batch: usize,
) -> Group {
    let ids: Arc<[DestinationId]> = routes
        .iter()
        .filter_map(|name| destinations.id(name))
        .collect();
    let topic: Arc<str> = Arc::from(TOPIC);
    let batches = (0..callers)
        .map(|caller| {
            (0..records)
                .map(|i| RoutedRecord {
                    record: Record {
                        payload: payload(size, caller, i, i < refused_per_batch),
                        key: Some(Arc::clone(&topic)),
                        headers: Vec::new(),
                        metadata: RecordMeta {
                            timestamp_ms: None,
                            format: PayloadFormat::Json,
                        },
                    },
                    routes: Arc::clone(&ids),
                })
                .collect()
        })
        .collect();
    Group {
        label,
        routes: routes.to_vec(),
        batches,
        answer: Answer::Held,
    }
}

/// A `size`-byte payload unique to the caller and record.
fn payload(size: usize, caller: usize, record: usize, refused: bool) -> bytes::Bytes {
    let head = format!(
        "{}{{\"caller\":{caller},\"record\":{record},\"pad\":\"",
        if refused { "!" } else { "" }
    );
    let mut text = head.into_bytes();
    text.resize(size.saturating_sub(2).max(text.len()), b'x');
    text.extend_from_slice(b"\"}");
    bytes::Bytes::from(text)
}
