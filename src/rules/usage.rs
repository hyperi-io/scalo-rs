// Project:   scalo
// File:      src/rules/usage.rs
// Purpose:   Per-rule hit and clamp counters
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Two views of one count: in-process atomics a caller reads with
//! [`RuleSet::usage`](super::RuleSet::usage), and with the `metrics` feature the
//! `rules_hits_total` and `rules_clamped_total` series, labelled `rule_set` and
//! `rule`. The atomics start at zero for each compiled set, while a series keeps
//! counting across reloads because its labels name the same row.

use std::sync::atomic::{AtomicU64, Ordering};

/// The `rule` label of a clamp on a value the record carried, which no row set.
/// Not a valid rule id, so it never names a row.
pub const RECORD_LABEL: &str = "(record)";

pub(crate) struct Usage {
    hits: Box<[AtomicU64]>,
    /// One per rule, then one for values the record carried.
    clamped: Box<[AtomicU64]>,
    #[cfg(feature = "metrics")]
    hit_series: Box<[metrics::Counter]>,
    #[cfg(feature = "metrics")]
    clamp_series: Box<[metrics::Counter]>,
}

impl Usage {
    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub(crate) fn new(set: &str, ids: &[&str]) -> Self {
        let zeros = |n: usize| (0..n).map(|_| AtomicU64::new(0)).collect();
        Self {
            hits: zeros(ids.len()),
            clamped: zeros(ids.len() + 1),
            #[cfg(feature = "metrics")]
            hit_series: ids
                .iter()
                .map(|id| {
                    metrics::counter!("rules_hits_total", "rule_set" => set.to_string(), "rule" => (*id).to_string())
                })
                .collect(),
            #[cfg(feature = "metrics")]
            clamp_series: ids
                .iter()
                .copied()
                .chain(std::iter::once(RECORD_LABEL))
                .map(|id| {
                    metrics::counter!("rules_clamped_total", "rule_set" => set.to_string(), "rule" => id.to_string())
                })
                .collect(),
        }
    }

    pub(crate) fn add_hits(&self, rule: usize, n: u64) {
        self.hits[rule].fetch_add(n, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        self.hit_series[rule].increment(n);
    }

    /// `rule` is `None` for a value the record carried.
    pub(crate) fn add_clamped(&self, rule: Option<usize>, n: u64) {
        let slot = rule.unwrap_or(self.hits.len());
        self.clamped[slot].fetch_add(n, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        self.clamp_series[slot].increment(n);
    }

    pub(crate) fn hits(&self, rule: usize) -> u64 {
        self.hits[rule].load(Ordering::Relaxed)
    }

    pub(crate) fn clamped(&self, rule: Option<usize>) -> u64 {
        self.clamped[rule.unwrap_or(self.hits.len())].load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hits_and_clamps_count_per_row_and_for_the_record() {
        let usage = Usage::new("t", &["a", "b"]);
        usage.add_hits(1, 3);
        usage.add_clamped(Some(0), 2);
        usage.add_clamped(None, 5);
        assert_eq!((usage.hits(0), usage.hits(1)), (0, 3));
        assert_eq!(usage.clamped(Some(0)), 2);
        assert_eq!(usage.clamped(Some(1)), 0);
        assert_eq!(usage.clamped(None), 5);
    }
}
