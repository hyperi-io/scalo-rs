// Project:   scalo
// File:      src/transport/kafka/lease.rs
// Purpose:   A Kafka consumer's claim on one partition, for records a caller buffers
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A consumer's claim on one partition, from the assignment that gave it to
//! the revoke, or the consumer rebuild, that ends it.
//!
//! A caller that holds records before it writes them -- a buffer that fills
//! for seconds, a retry queue -- takes each record's lease when `recv` hands
//! it over, and asks [`KafkaTransport::holds`](super::KafkaTransport::holds)
//! before the write and before the commit. Once the lease has ended, the
//! partition's next owner, another member or this one under a new lease,
//! reads the record again from the committed offset, so the buffered copy is
//! a duplicate: discard it and commit nothing for it.

/// This consumer's claim on one partition. See [`KafkaTransport::lease`](super::KafkaTransport::lease).
///
/// Leases compare equal only when they are the same claim. A partition
/// revoked and given back, or held by a rebuilt consumer, is under a new
/// lease, even when the same member holds it. The value carries nothing else
/// a caller can read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionLease {
    /// Which of the transport's consumer clients holds it: a rebuild is a new client.
    client: u64,
    /// The number of the assignment that gave the client the partition.
    assignment: u64,
}

impl PartitionLease {
    pub(super) const fn new(client: u64, assignment: u64) -> Self {
        Self { client, assignment }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_is_one_client_and_one_assignment() {
        let lease = PartitionLease::new(0, 3);
        assert_eq!(lease, PartitionLease::new(0, 3));
        assert_ne!(lease, PartitionLease::new(0, 4), "assigned again");
        assert_ne!(lease, PartitionLease::new(1, 3), "a rebuilt client");
    }
}
