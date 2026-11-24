// Project:   scalo
// File:      src/transport/filter/mod.rs
// Purpose:   Transport-level message filtering engine
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # Transport Filter Engine
//!
//! Provides transport-level message filtering using CEL syntax with SIMD
//! fast-path for simple patterns. Embedded in every transport -- filters are
//! configured via the config cascade and applied automatically.
//!
//! ## Performance Tiers
//!
//! - **Tier 1** -- SIMD field extraction (~50-100ns/msg). Always enabled.
//! - **Tier 2** -- Pre-compiled CEL (~500ns-1us/msg). Requires `allow_cel_filters_in/out`.
//! - **Tier 3** -- Complex CEL with regex/iteration (~5-50us/msg). Requires `allow_complex_filters_in/out`.
//!
//! ## Usage
//!
//! Transports construct `TransportFilterEngine` from config at creation time.
//! The engine is a no-op when no filters are configured (zero overhead).
//!
//! ```yaml
//! kafka:
//!   filters_in:
//!     - expression: 'has(_internal)'
//!       action: drop
//!     - expression: 'status == "poison"'
//!       action: dlq
//! ```

pub mod budget;
pub mod classify;
pub mod compiled;
pub mod config;
pub mod metrics;

pub use budget::{BudgetError, FilterBudget};
pub use config::{
    FilterAction, FilterDirection, FilterRule, FilterTier, TransportFilterTierConfig,
};

use compiled::CompiledFilter;
use metrics::FilterMetrics;

use crate::transport::error::TransportError;
use crate::transport::types::{BlockOutcome, SendResult};

/// Result of evaluating a filter against a message payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDisposition {
    /// Message passes all filters -- continue processing.
    Pass,
    /// Message matched a filter with `action: drop` -- discard silently.
    Drop,
    /// Message matched a filter with `action: dlq` -- route to dead-letter queue.
    Dlq,
}

/// A DLQ entry produced by inbound filtering.
///
/// The transport does NOT send to DLQ directly -- it returns these entries
/// alongside passing messages. The caller handles DLQ routing using its
/// own `Dlq` handle.
#[derive(Debug, Clone)]
pub struct FilteredDlqEntry {
    /// Raw message payload.
    pub payload: Vec<u8>,
    /// Routing key (Kafka topic, gRPC metadata, etc.).
    pub key: Option<std::sync::Arc<str>>,
    /// Human-readable reason for DLQ routing (filter expression text).
    pub reason: String,
}

/// Result of partitioning a batch of messages through inbound filters.
///
/// Drop and DLQ messages are removed from `messages`. Their commit tokens are
/// preserved in `filtered_tokens`. The engine does NOT send to DLQ directly
/// (transports hold no DLQ handle) -- the caller routes `dlq_entries`.
#[derive(Debug)]
pub struct FilteredBatch<T, K> {
    /// Messages that passed all filters.
    pub messages: Vec<T>,
    /// Messages matched by filters with `action: dlq`. Caller routes to DLQ.
    pub dlq_entries: Vec<FilteredDlqEntry>,
    /// Count of messages dropped (matched by `action: drop` filters).
    pub drop_count: u64,
    /// Commit tokens of the messages removed by `drop`/`dlq` filters.
    ///
    /// A filtered record WAS handled, so its source ack must still commit.
    /// Dropping these tokens stalls the Kafka offset behind any all-filtered
    /// stretch (phantom KEDA lag, then a replay storm with duplicate
    /// dead-letters if the consumer restarts inside it). The caller carries
    /// them into `WorkBatch.commit_tokens` so the block commit covers them
    /// once inbound-DLQ routing succeeds.
    pub filtered_tokens: Vec<K>,
}

impl<T, K> FilteredBatch<T, K> {
    /// Create a `FilteredBatch` containing only passing messages (no filtering).
    /// Used when there are no inbound filters configured.
    #[must_use]
    pub fn passthrough(messages: Vec<T>) -> Self {
        Self {
            messages,
            dlq_entries: Vec::new(),
            drop_count: 0,
            filtered_tokens: Vec::new(),
        }
    }
}

/// Transport-level message filter engine.
///
/// Embedded in every transport. Compiled from config at construction time.
/// Zero-cost when no filters are configured (`filters_in` and `filters_out`
/// are empty vecs -> `has_inbound_filters()` returns false, branch predicted).
pub struct TransportFilterEngine {
    filters_in: Vec<CompiledFilter>,
    filters_out: Vec<CompiledFilter>,
    metrics: FilterMetrics,
}

impl std::fmt::Debug for TransportFilterEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportFilterEngine")
            .field("filters_in_count", &self.filters_in.len())
            .field("filters_out_count", &self.filters_out.len())
            .field("metrics", &"FilterMetrics")
            .finish()
    }
}

/// Threshold for logging a warning about filter count.
const FILTER_COUNT_WARNING_THRESHOLD: usize = 20;

impl TransportFilterEngine {
    /// Construct a filter engine from config rules.
    ///
    /// Parses each expression, classifies its tier, rejects expressions above
    /// the enabled tier, and compiles Tier 2/3 expressions to CEL programs.
    ///
    /// # Errors
    ///
    /// Returns `TransportError::Config` if:
    /// - An expression is invalid (syntax error)
    /// - An expression's tier exceeds what's enabled by `tier_config`
    pub fn new(
        filters_in: &[FilterRule],
        filters_out: &[FilterRule],
        tier_config: &TransportFilterTierConfig,
    ) -> Result<Self, TransportError> {
        let compiled_in = Self::compile_rules(filters_in, FilterDirection::In, tier_config)?;
        let compiled_out = Self::compile_rules(filters_out, FilterDirection::Out, tier_config)?;

        // Log startup info
        if !compiled_in.is_empty() || !compiled_out.is_empty() {
            for (idx, filter) in compiled_in.iter().enumerate() {
                tracing::info!(
                    index = idx,
                    tier = %filter.tier(),
                    expression = filter.expression_text(),
                    action = ?filter.action(),
                    direction = "in",
                    "Transport filter configured"
                );
            }
            for (idx, filter) in compiled_out.iter().enumerate() {
                tracing::info!(
                    index = idx,
                    tier = %filter.tier(),
                    expression = filter.expression_text(),
                    action = ?filter.action(),
                    direction = "out",
                    "Transport filter configured"
                );
            }

            // Warn about ordering (higher tier before lower tier)
            Self::warn_suboptimal_ordering(&compiled_in, "in");
            Self::warn_suboptimal_ordering(&compiled_out, "out");

            // Warn about large filter counts
            if compiled_in.len() > FILTER_COUNT_WARNING_THRESHOLD {
                tracing::warn!(
                    count = compiled_in.len(),
                    direction = "in",
                    "Large number of inbound filters -- may impact throughput"
                );
            }
            if compiled_out.len() > FILTER_COUNT_WARNING_THRESHOLD {
                tracing::warn!(
                    count = compiled_out.len(),
                    direction = "out",
                    "Large number of outbound filters -- may impact throughput"
                );
            }
        }

        Ok(Self {
            filters_in: compiled_in,
            filters_out: compiled_out,
            metrics: FilterMetrics::new(),
        })
    }

    /// Create an empty filter engine (no filters, zero overhead).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            filters_in: Vec::new(),
            filters_out: Vec::new(),
            metrics: FilterMetrics::new(),
        }
    }

    /// Check if any inbound filters are configured.
    #[inline]
    #[must_use]
    pub fn has_inbound_filters(&self) -> bool {
        !self.filters_in.is_empty()
    }

    /// Check if any outbound filters are configured.
    #[inline]
    #[must_use]
    pub fn has_outbound_filters(&self) -> bool {
        !self.filters_out.is_empty()
    }

    /// Check if any inbound filter uses `action: dlq`.
    #[must_use]
    pub fn has_dlq_filters_in(&self) -> bool {
        self.filters_in
            .iter()
            .any(|f| f.action() == FilterAction::Dlq)
    }

    /// Check if any outbound filter uses `action: dlq`.
    #[must_use]
    pub fn has_dlq_filters_out(&self) -> bool {
        self.filters_out
            .iter()
            .any(|f| f.action() == FilterAction::Dlq)
    }

    /// Evaluate inbound filters against a raw payload. First-match-wins.
    ///
    /// Returns `Pass` if no filter matches (or no filters configured).
    /// MsgPack payloads always pass (SIMD extraction is JSON-only).
    #[inline]
    #[must_use]
    pub fn apply_inbound(&self, payload: &[u8]) -> FilterDisposition {
        self.apply_filters(payload, &self.filters_in, FilterDirection::In)
    }

    /// Partition a batch of messages through inbound filters.
    ///
    /// The recommended transport API: returns a `FilteredBatch` with passing
    /// messages AND DLQ entries (the caller routes DLQ via its own handle).
    /// Drop and DLQ messages are removed from `messages`. Never silently lost.
    ///
    /// `T` is the message type, `K` its commit token. The closures extract
    /// payload/key/token, avoiding coupling to a specific message struct.
    /// `get_token` runs only for removed messages, so their source acks survive
    /// into [`FilteredBatch::filtered_tokens`].
    pub fn partition_batch<T, K>(
        &self,
        messages: Vec<T>,
        get_payload: impl Fn(&T) -> &[u8],
        get_key: impl Fn(&T) -> Option<std::sync::Arc<str>>,
        get_token: impl Fn(&T) -> K,
    ) -> FilteredBatch<T, K> {
        if !self.has_inbound_filters() {
            return FilteredBatch::passthrough(messages);
        }

        let mut passing = Vec::with_capacity(messages.len());
        let mut dlq_entries: Vec<FilteredDlqEntry> = Vec::new();
        let mut filtered_tokens: Vec<K> = Vec::new();
        let mut drop_count: u64 = 0;

        for msg in messages {
            let payload = get_payload(&msg);
            match self.apply_inbound(payload) {
                FilterDisposition::Pass => passing.push(msg),
                FilterDisposition::Drop => {
                    // Intentionally discarded, but it WAS handled: keep its ack
                    // so the source offset advances past it.
                    filtered_tokens.push(get_token(&msg));
                    drop_count += 1;
                }
                FilterDisposition::Dlq => {
                    let key = get_key(&msg);
                    dlq_entries.push(FilteredDlqEntry {
                        payload: payload.to_vec(),
                        key,
                        reason: "transport filter".to_string(),
                    });
                    // Routed to DLQ: keep its ack so the block commit covers it
                    // once DLQ routing succeeds (a routing failure is terminal
                    // and skips the whole commit, so re-delivery still holds).
                    filtered_tokens.push(get_token(&msg));
                }
            }
        }

        FilteredBatch {
            messages: passing,
            dlq_entries,
            drop_count,
            filtered_tokens,
        }
    }

    /// Whether an outbound `dlq` filter takes `payload`: the answer a sender's
    /// `dead_letter_reason` gives for a filter match.
    ///
    /// Counted in `transport_filtered_total` and
    /// `transport_filter_cel_payload_skip_total` only when it answers `true`,
    /// as the record is then taken out before the send. Any other outcome is
    /// the send's to count when it filters the record.
    #[must_use]
    pub fn outbound_dead_letter(&self, payload: &[u8]) -> bool {
        if !self.has_dlq_filters_out() || is_likely_msgpack(payload) {
            return false;
        }
        let (matched, skips) = compiled::holding_skips(|| first_match(payload, &self.filters_out));
        let dead_lettered = matched == Some(FilterAction::Dlq);
        if dead_lettered {
            self.metrics.record(FilterDirection::Out, FilterAction::Dlq);
            compiled::count_cel_skips(skips);
        }
        dead_lettered
    }

    /// Evaluate outbound filters against a raw payload. First-match-wins.
    #[inline]
    #[must_use]
    pub fn apply_outbound(&self, payload: &[u8]) -> FilterDisposition {
        self.apply_filters(payload, &self.filters_out, FilterDirection::Out)
    }

    /// A sender's answer for a payload the outbound filters keep off the wire:
    /// `Ok` for a `drop` match, `FilteredDlq` for a `dlq` match. `None` when it
    /// passes and is to be sent.
    #[inline]
    #[must_use]
    pub fn outbound_one(&self, payload: &[u8]) -> Option<SendResult> {
        if !self.has_outbound_filters() {
            return None;
        }
        match self.apply_outbound(payload) {
            FilterDisposition::Pass => None,
            FilterDisposition::Drop => Some(SendResult::Ok),
            FilterDisposition::Dlq => Some(SendResult::FilteredDlq),
        }
    }

    /// The items the outbound filters pass, in order, and a sender's answer
    /// for the block once they are sent: `FilteredDlq` when a `dlq` filter
    /// matched one, else `Ok`. Items are kept as given, so a borrowed record
    /// is never copied.
    pub fn partition_outbound<T>(
        &self,
        items: impl IntoIterator<Item = T>,
        payload: impl Fn(&T) -> &[u8],
    ) -> (Vec<T>, SendResult) {
        if !self.has_outbound_filters() {
            return (items.into_iter().collect(), SendResult::Ok);
        }
        let mut outcome = BlockOutcome::default();
        let kept: Vec<T> = items
            .into_iter()
            .filter(|item| match self.outbound_one(payload(item)) {
                None => true,
                Some(settled) => {
                    let _ = outcome.add(settled);
                    false
                }
            })
            .collect();
        (kept, outcome.finish())
    }

    fn apply_filters(
        &self,
        payload: &[u8],
        filters: &[CompiledFilter],
        direction: FilterDirection,
    ) -> FilterDisposition {
        if filters.is_empty() {
            return FilterDisposition::Pass;
        }

        // MsgPack bypasses filters (SIMD extraction is JSON-only).
        // Warn once per direction so operators don't silently pass
        // every MsgPack message. Emit a metric every time so the
        // bypass rate is visible.
        if is_likely_msgpack(payload) {
            #[cfg(feature = "metrics")]
            ::metrics::counter!(
                "transport_filter_msgpack_bypass_total",
                "direction" => direction.to_string()
            )
            .increment(1);
            warn_msgpack_bypass_once(direction);
            return FilterDisposition::Pass;
        }

        match first_match(payload, filters) {
            None => FilterDisposition::Pass,
            Some(action) => {
                self.metrics.record(direction, action);
                match action {
                    FilterAction::Drop => FilterDisposition::Drop,
                    FilterAction::Dlq => FilterDisposition::Dlq,
                }
            }
        }
    }

    fn compile_rules(
        rules: &[FilterRule],
        direction: FilterDirection,
        tier_config: &TransportFilterTierConfig,
    ) -> Result<Vec<CompiledFilter>, TransportError> {
        let mut compiled = Vec::with_capacity(rules.len());

        for (idx, rule) in rules.iter().enumerate() {
            let filter = CompiledFilter::from_expression(
                &rule.expression,
                rule.action,
                direction,
                tier_config,
            )
            .map_err(|e| {
                TransportError::Config(format!(
                    "filter_{direction}[{idx}]: '{expr}' -- {e}",
                    expr = rule.expression
                ))
            })?;
            compiled.push(filter);
        }

        Ok(compiled)
    }

    fn warn_suboptimal_ordering(filters: &[CompiledFilter], direction: &str) {
        let mut lowest_seen = FilterTier::Tier3;
        for (idx, filter) in filters.iter().enumerate() {
            let tier = filter.tier();
            if (tier as u8) > (lowest_seen as u8) {
                tracing::warn!(
                    direction,
                    index = idx,
                    tier = %tier,
                    expression = filter.expression_text(),
                    "Higher-tier filter precedes lower-tier filter -- consider reordering for better performance"
                );
            }
            if (tier as u8) < (lowest_seen as u8) {
                lowest_seen = tier;
            }
        }
    }
}

/// The action of the first of `filters` that `payload` matches, uncounted.
fn first_match(payload: &[u8], filters: &[CompiledFilter]) -> Option<FilterAction> {
    filters.iter().find_map(|filter| filter.evaluate(payload))
}

/// Log a one-shot warning per direction when filters skip MsgPack.
/// Counter increments every call. The log fires once per process.
fn warn_msgpack_bypass_once(direction: FilterDirection) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static WARNED_IN: AtomicBool = AtomicBool::new(false);
    static WARNED_OUT: AtomicBool = AtomicBool::new(false);
    let flag = match direction {
        FilterDirection::In => &WARNED_IN,
        FilterDirection::Out => &WARNED_OUT,
    };
    if !flag.swap(true, Ordering::AcqRel) {
        tracing::warn!(
            target: "scalo::transport::filter",
            direction = %direction,
            "transport filters skipped a MsgPack payload -- \
             filters are JSON-only. Scrape \
             transport_filter_msgpack_bypass_total for the rate."
        );
    }
}

/// Quick heuristic: is this payload likely MsgPack (not JSON)?
///
/// Checks the first byte for MsgPack markers. Same heuristic as
/// `PayloadFormat::detect()` in `types.rs`.
#[inline]
fn is_likely_msgpack(payload: &[u8]) -> bool {
    match payload.first() {
        Some(b) => matches!(
            b,
            0x80..=0x8f | 0xde..=0xdf | 0x90..=0x9f | 0xdc..=0xdd
        ),
        None => false, // empty payload is not MsgPack
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_no_filters_always_passes() {
        let engine =
            TransportFilterEngine::new(&[], &[], &TransportFilterTierConfig::default()).unwrap();
        assert!(!engine.has_inbound_filters());
        assert!(!engine.has_outbound_filters());
        assert_eq!(
            engine.apply_inbound(br#"{"any":"thing"}"#),
            FilterDisposition::Pass
        );
    }

    #[test]
    fn engine_tier1_drop_filter() {
        let rules = vec![FilterRule {
            expression: r#"status == "poison""#.into(),
            action: FilterAction::Drop,
        }];
        let engine =
            TransportFilterEngine::new(&rules, &[], &TransportFilterTierConfig::default()).unwrap();
        assert!(engine.has_inbound_filters());

        assert_eq!(
            engine.apply_inbound(br#"{"status":"poison","data":"x"}"#),
            FilterDisposition::Drop
        );
        assert_eq!(
            engine.apply_inbound(br#"{"status":"ok","data":"x"}"#),
            FilterDisposition::Pass
        );
    }

    #[test]
    fn partition_batch_carries_filtered_tokens() {
        // Dropped AND DLQ'd messages must surface their commit tokens in
        // filtered_tokens, so the caller can still advance the source past
        // them (no stalled Kafka offset on filtered records).
        struct M {
            payload: Vec<u8>,
            token: u64,
        }
        let rules = vec![
            FilterRule {
                expression: r#"status == "drop_me""#.into(),
                action: FilterAction::Drop,
            },
            FilterRule {
                expression: r#"status == "dlq_me""#.into(),
                action: FilterAction::Dlq,
            },
        ];
        let engine =
            TransportFilterEngine::new(&rules, &[], &TransportFilterTierConfig::default()).unwrap();

        let messages = vec![
            M {
                payload: br#"{"status":"ok"}"#.to_vec(),
                token: 1,
            },
            M {
                payload: br#"{"status":"drop_me"}"#.to_vec(),
                token: 2,
            },
            M {
                payload: br#"{"status":"dlq_me"}"#.to_vec(),
                token: 3,
            },
            M {
                payload: br#"{"status":"ok"}"#.to_vec(),
                token: 4,
            },
        ];

        let batch =
            engine.partition_batch(messages, |m| m.payload.as_slice(), |_m| None, |m| m.token);

        // Passing messages keep tokens 1 and 4.
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].token, 1);
        assert_eq!(batch.messages[1].token, 4);
        assert_eq!(batch.drop_count, 1);
        assert_eq!(batch.dlq_entries.len(), 1);
        // Dropped (2) and DLQ'd (3) tokens are preserved for the block commit.
        let mut filtered = batch.filtered_tokens.clone();
        filtered.sort_unstable();
        assert_eq!(filtered, vec![2, 3]);
    }

    #[test]
    fn partition_batch_passthrough_has_no_filtered_tokens() {
        // No inbound filters -> every message passes, nothing filtered.
        struct M {
            payload: Vec<u8>,
            token: u64,
        }
        let engine =
            TransportFilterEngine::new(&[], &[], &TransportFilterTierConfig::default()).unwrap();
        let messages = vec![M {
            payload: br#"{"status":"ok"}"#.to_vec(),
            token: 1,
        }];
        let batch =
            engine.partition_batch(messages, |m| m.payload.as_slice(), |_m| None, |m| m.token);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.filtered_tokens, [] as [u64; 0]);
        assert_eq!(batch.drop_count, 0);
    }

    #[test]
    fn engine_first_match_wins() {
        let rules = vec![
            FilterRule {
                expression: r#"status == "drop_me""#.into(),
                action: FilterAction::Drop,
            },
            FilterRule {
                expression: r#"status == "drop_me""#.into(),
                action: FilterAction::Dlq,
            },
        ];
        let engine =
            TransportFilterEngine::new(&rules, &[], &TransportFilterTierConfig::default()).unwrap();
        // First filter matches -- Drop, not Dlq
        assert_eq!(
            engine.apply_inbound(br#"{"status":"drop_me"}"#),
            FilterDisposition::Drop
        );
    }

    #[test]
    fn engine_tier2_rejected_without_opt_in() {
        let rules = vec![FilterRule {
            expression: r#"severity > 3 && source != "internal""#.into(),
            action: FilterAction::Drop,
        }];
        let result = TransportFilterEngine::new(&rules, &[], &TransportFilterTierConfig::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Tier 2"), "Error should mention tier: {err}");
    }

    #[test]
    fn engine_tier3_rejected_without_complex_opt_in() {
        let tier_config = TransportFilterTierConfig {
            allow_cel_filters_in: true,
            ..Default::default()
        };
        let rules = vec![FilterRule {
            expression: r#"field.matches("^prod-.*")"#.into(),
            action: FilterAction::Drop,
        }];
        let result = TransportFilterEngine::new(&rules, &[], &tier_config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Tier 3"), "Error should mention tier: {err}");
    }

    #[test]
    fn engine_invalid_expression_errors() {
        let rules = vec![FilterRule {
            expression: "this is not valid ((( CEL".into(),
            action: FilterAction::Drop,
        }];
        let result = TransportFilterEngine::new(&rules, &[], &TransportFilterTierConfig::default());
        assert!(result.is_err());
    }

    #[test]
    fn engine_has_dlq_filters_detection() {
        let rules = vec![FilterRule {
            expression: "has(field)".into(),
            action: FilterAction::Dlq,
        }];
        let engine =
            TransportFilterEngine::new(&rules, &[], &TransportFilterTierConfig::default()).unwrap();
        assert!(engine.has_dlq_filters_in());
        assert!(!engine.has_dlq_filters_out());
    }

    #[test]
    fn engine_msgpack_payload_passes_through() {
        let rules = vec![FilterRule {
            expression: "has(_table)".into(),
            action: FilterAction::Drop,
        }];
        let engine =
            TransportFilterEngine::new(&rules, &[], &TransportFilterTierConfig::default()).unwrap();
        // MsgPack fixmap header (0x81)
        let msgpack = &[
            0x81, 0xa6, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0xa6, 0x65, 0x76, 0x65, 0x6e, 0x74,
            0x73,
        ];
        assert_eq!(engine.apply_inbound(msgpack), FilterDisposition::Pass);
    }

    #[test]
    fn engine_outbound_filter_independent() {
        let in_rules = vec![FilterRule {
            expression: "has(drop_in)".into(),
            action: FilterAction::Drop,
        }];
        let out_rules = vec![FilterRule {
            expression: "has(drop_out)".into(),
            action: FilterAction::Drop,
        }];
        let engine = TransportFilterEngine::new(
            &in_rules,
            &out_rules,
            &TransportFilterTierConfig::default(),
        )
        .unwrap();

        let payload_in = br#"{"drop_in":true}"#;
        assert_eq!(engine.apply_inbound(payload_in), FilterDisposition::Drop);
        assert_eq!(engine.apply_outbound(payload_in), FilterDisposition::Pass);

        let payload_out = br#"{"drop_out":true}"#;
        assert_eq!(engine.apply_inbound(payload_out), FilterDisposition::Pass);
        assert_eq!(engine.apply_outbound(payload_out), FilterDisposition::Drop);
    }

    /// Every tier resolves a dotted path through the record's nested objects. A path with
    /// no value, or a value of the wrong type, can't match and the record passes.
    #[cfg(feature = "expression")]
    #[test]
    fn engine_resolves_nested_fields_in_every_tier() {
        let tiers = TransportFilterTierConfig {
            allow_cel_filters_in: true,
            allow_complex_filters_in: true,
            ..Default::default()
        };
        let src = br#"{"source":{"type":"file","name":"dns"},"event":{"message":"a2","n":7,"fields":{"level":"debug"},"tags":["x","y"]}}"#;
        let other = br#"{"source":{"type":"file","name":"app"},"event":{"message":"b1","n":3,"fields":{"level":"info"},"tags":["z"]}}"#;
        let no_event = br#"{"source":{"type":"file","name":"dns"}}"#;
        let empty_event = br#"{"source":{"type":"file","name":"dns"},"event":{}}"#;
        let wrong_type =
            br#"{"source":{"type":"file","name":"dns"},"event":{"message":5,"n":"seven"}}"#;
        // (expression, payload, dropped)
        let cases: [(&str, &[u8], bool); 37] = [
            // Tier 1: raw-byte field extraction.
            (r#"event.message == "a2""#, src, true),
            (r#"event.message == "a2""#, other, false),
            (r#"event.message != "a2""#, other, true),
            (r#"event.message.startsWith("a")"#, src, true),
            (r#"event.message.startsWith("a")"#, other, false),
            ("has(event.message)", src, true),
            ("has(event.message)", empty_event, false),
            ("has(event.message)", no_event, false),
            ("!has(event.message)", no_event, true),
            (r#"event.fields.level == "debug""#, src, true),
            (r#"event.fields.level == "debug""#, other, false),
            // Tier 2: pre-compiled CEL, no regex.
            (r#"source.name == "dns" && event.n > 5"#, src, true),
            (r#"source.name == "dns" && event.n > 5"#, other, false),
            (r#"source.name == "dns" && event.n > 5"#, empty_event, false),
            (r#"source.name == "dns" && event.n > 5"#, no_event, false),
            (r#"source.name == "dns" && event.n > 5"#, wrong_type, false),
            ("event.n > 5 && event.n < 10", src, true),
            ("event.n > 5 && event.n < 10", other, false),
            (r#"event.message == "a2" || event.n > 100"#, src, true),
            (r#"event.message == "a2" || event.n > 100"#, other, false),
            (r"has(event.message) && event.n > 5", src, true),
            (r"has(event.message) && event.n > 5", empty_event, false),
            (r#"event.message.contains("2") && event.n > 5"#, src, true),
            (
                r#"event.fields.level == "debug" && source.type == "file""#,
                src,
                true,
            ),
            (
                r#"event.fields.level == "debug" && source.type == "file""#,
                other,
                false,
            ),
            (r"event.tags.size() == 2 && event.n > 5", src, true),
            (r"event.tags.size() == 2 && event.n > 5", other, false),
            // Tier 3: regex and iteration.
            (r#"event.message.matches("^a")"#, src, true),
            (r#"event.message.matches("^a")"#, other, false),
            (r#"event.message.matches("^a")"#, empty_event, false),
            (r#"event.message.matches("^a")"#, no_event, false),
            (r#"event.message.matches("^a")"#, wrong_type, false),
            (
                r#"source.name == "dns" && event.message.matches("^a[0-9]$")"#,
                src,
                true,
            ),
            (
                r#"source.name == "dns" && event.message.matches("^a[0-9]$")"#,
                other,
                false,
            ),
            (r#"event.fields.level.matches("^deb")"#, src, true),
            (r#"event.tags.exists(t, t == "y")"#, src, true),
            (r#"event.tags.exists(t, t == "y")"#, other, false),
        ];
        for (expression, payload, dropped) in cases {
            let rules = vec![FilterRule {
                expression: expression.into(),
                action: FilterAction::Drop,
            }];
            let engine = TransportFilterEngine::new(&rules, &[], &tiers)
                .unwrap_or_else(|e| panic!("{expression} must compile: {e}"));
            let expected = if dropped {
                FilterDisposition::Drop
            } else {
                FilterDisposition::Pass
            };
            assert_eq!(
                engine.apply_inbound(payload),
                expected,
                "{expression} on {}",
                String::from_utf8_lossy(payload)
            );
        }
    }

    #[test]
    fn is_likely_msgpack_detection() {
        assert!(is_likely_msgpack(&[0x81])); // fixmap
        assert!(is_likely_msgpack(&[0x90])); // fixarray
        assert!(is_likely_msgpack(&[0xde])); // map16
        assert!(!is_likely_msgpack(b"{")); // JSON object
        assert!(!is_likely_msgpack(b"[")); // JSON array
        assert!(!is_likely_msgpack(b"")); // empty
    }
}
