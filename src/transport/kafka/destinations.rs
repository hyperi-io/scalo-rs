// Project:   scalo
// File:      src/transport/kafka/destinations.rs
// Purpose:   Which destination topics a Kafka sender found missing
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A sender's destination topics, split into those last seen missing and
//! those last seen delivering, so one missing topic degrades the transport
//! instead of failing it.

use std::collections::HashSet;

/// How a sender's destination topics stand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DestinationHealth {
    /// No destination is missing.
    AllDelivering,
    /// Some destinations are missing and at least one other delivers.
    SomeMissing,
    /// Every destination the sender has written to is missing.
    AllMissing,
}

/// The destination topics a send found missing, and those a send has
/// delivered to since. A topic is in one set or neither.
#[derive(Debug, Default)]
pub(super) struct Destinations {
    missing: HashSet<String>,
    delivering: HashSet<String>,
}

impl Destinations {
    /// Record `topic` as missing. True when it was not missing before.
    pub(super) fn missing(&mut self, topic: &str) -> bool {
        self.delivering.remove(topic);
        self.missing.insert(topic.to_string())
    }

    /// Record a delivery to `topic`. True when it was missing before.
    pub(super) fn delivered(&mut self, topic: &str) -> bool {
        let was_missing = self.missing.remove(topic);
        if was_missing || !self.delivering.contains(topic) {
            self.delivering.insert(topic.to_string());
        }
        was_missing
    }

    /// Whether any destination is missing.
    pub(super) fn any_missing(&self) -> bool {
        !self.missing.is_empty()
    }

    /// How the destinations stand.
    pub(super) fn health(&self) -> DestinationHealth {
        match (self.missing.is_empty(), self.delivering.is_empty()) {
            (true, _) => DestinationHealth::AllDelivering,
            (false, false) => DestinationHealth::SomeMissing,
            (false, true) => DestinationHealth::AllMissing,
        }
    }

    /// The missing topics, sorted.
    pub(super) fn missing_topics(&self) -> Vec<String> {
        let mut topics: Vec<String> = self.missing.iter().cloned().collect();
        topics.sort_unstable();
        topics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each sequence of missing and delivered reports leaves the health and
    /// the missing list it should.
    #[test]
    fn reports_move_each_topic_between_missing_and_delivering() {
        type Step = (&'static str, bool);
        let cases: [(&str, &[Step], DestinationHealth, &[&str]); 6] = [
            ("nothing sent", &[], DestinationHealth::AllDelivering, &[]),
            (
                "the only topic missing",
                &[("a", false)],
                DestinationHealth::AllMissing,
                &["a"],
            ),
            (
                "one of two missing",
                &[("a", true), ("b", false)],
                DestinationHealth::SomeMissing,
                &["b"],
            ),
            (
                "a delivering topic goes missing",
                &[("a", true), ("a", false)],
                DestinationHealth::AllMissing,
                &["a"],
            ),
            (
                "a missing topic delivers again",
                &[("a", true), ("b", false), ("b", true)],
                DestinationHealth::AllDelivering,
                &[],
            ),
            (
                "both missing after delivering",
                &[("a", true), ("b", true), ("a", false), ("b", false)],
                DestinationHealth::AllMissing,
                &["a", "b"],
            ),
        ];
        for (case, steps, health, missing) in cases {
            let mut destinations = Destinations::default();
            for &(topic, delivered) in steps {
                if delivered {
                    destinations.delivered(topic);
                } else {
                    destinations.missing(topic);
                }
            }
            assert_eq!(destinations.health(), health, "{case}");
            assert_eq!(destinations.missing_topics(), missing, "{case}");
            assert_eq!(destinations.any_missing(), !missing.is_empty(), "{case}");
        }
    }

    /// Only the first missing report of a run, and the delivery that ends it,
    /// say so.
    #[test]
    fn a_report_says_whether_it_changed_the_topic() {
        let mut destinations = Destinations::default();
        assert!(destinations.missing("a"));
        assert!(!destinations.missing("a"));
        assert!(destinations.delivered("a"));
        assert!(!destinations.delivered("a"));
    }
}
