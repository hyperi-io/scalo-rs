// Project:   scalo
// File:      src/worker/engine/pre_route.rs
// Purpose:   Zero-copy pre-route field extraction and filter evaluation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pre-route phase: each filter extracts the field it names from raw JSON
//! bytes using `sonic_rs::get_from_slice` (SIMD-accelerated) and decides
//! whether the message continues, is dropped, or goes to DLQ.
//!
//! Hot path: ~50-100 ns per message.

use sonic_rs::JsonValueTrait as _;

use super::config::PreRouteFilterConfig;

/// Result of extracting the routing field from raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreRouteExtraction {
    /// Field found with this string value.
    Found(String),
    /// Field not present in the JSON object.
    Missing,
    /// Payload is not valid JSON.
    ParseError(String),
}

/// Outcome after applying filters to a pre-route extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreRouteOutcome {
    /// Message passes -- proceed to parse + transform.
    Continue,
    /// Message filtered out -- skip parse, include in commit.
    Filtered,
    /// Message routes to DLQ with reason.
    Dlq(String),
}

/// Runtime filter derived from [`PreRouteFilterConfig`].
#[derive(Debug, Clone)]
pub enum PreRouteFilter {
    /// Drop (filter) the message when the named field is absent.
    DropFieldMissing(String),
    /// Route to DLQ when the named field equals a specific value.
    DlqFieldValue { field: String, value: String },
}

/// Extract a routing field from raw JSON bytes using SIMD.
///
/// Uses `sonic_rs::get_from_slice` for zero-copy extraction. The error path
/// (distinguishing a missing field from invalid JSON) requires a full
/// validity check and is intentionally cold.
///
/// # Behaviour
/// - `Ok` string value -> `Found(string)`
/// - `Ok` non-string value -> `Found(raw_str)` (raw JSON)
/// - `Err` with `is_not_found()` -> `Missing`
/// - `Err` other -> `ParseError`
#[inline]
pub fn extract_routing_field(payload: &[u8], field_name: &str) -> PreRouteExtraction {
    match sonic_rs::get_from_slice(payload, &[field_name]) {
        Ok(lv) => {
            // Extract the string value if it is a JSON string, otherwise
            // fall back to the raw representation (e.g. a number or bool
            // used as a routing key).
            let value = lv
                .as_str()
                .map_or_else(|| lv.as_raw_str().to_owned(), str::to_owned);
            PreRouteExtraction::Found(value)
        }
        Err(e) if e.is_not_found() => PreRouteExtraction::Missing,
        Err(e) => PreRouteExtraction::ParseError(e.to_string()),
    }
}

/// Apply `filters` to `payload`, each reading the field it names.
///
/// Filters are evaluated in order -- first match wins. If no filter matches
/// the message continues. A field two filters in a row name is extracted
/// once.
#[must_use]
pub fn apply_filters(payload: &[u8], filters: &[PreRouteFilter]) -> PreRouteOutcome {
    let mut last: Option<(&str, PreRouteExtraction)> = None;
    for filter in filters {
        let field = match filter {
            PreRouteFilter::DropFieldMissing(field)
            | PreRouteFilter::DlqFieldValue { field, .. } => field.as_str(),
        };
        let extraction = match last.take() {
            Some((seen, extraction)) if seen == field => extraction,
            _ => extract_routing_field(payload, field),
        };
        match (filter, &extraction) {
            (PreRouteFilter::DropFieldMissing(_), PreRouteExtraction::Missing) => {
                return PreRouteOutcome::Filtered;
            }
            (
                PreRouteFilter::DlqFieldValue {
                    value: expected, ..
                },
                PreRouteExtraction::Found(actual),
            ) if actual == expected => {
                return PreRouteOutcome::Dlq(format!(
                    "field '{field}' value '{actual}' matches DLQ rule"
                ));
            }
            _ => {}
        }
        last = Some((field, extraction));
    }

    // A parse error with no filters still results in Continue -- the parse
    // phase will detect and handle the invalid payload.
    PreRouteOutcome::Continue
}

/// Convert config-layer filter definitions to runtime filters.
#[must_use]
pub fn filters_from_config(configs: &[PreRouteFilterConfig]) -> Vec<PreRouteFilter> {
    configs
        .iter()
        .map(|c| match c {
            PreRouteFilterConfig::DropFieldMissing { field } => {
                PreRouteFilter::DropFieldMissing(field.clone())
            }
            PreRouteFilterConfig::DlqFieldValue { field, value } => PreRouteFilter::DlqFieldValue {
                field: field.clone(),
                value: value.clone(),
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- extraction tests ---------------------------------------------------

    #[test]
    fn extract_routing_field_found() {
        let payload = br#"{"_table": "events", "host": "web1"}"#;
        let result = extract_routing_field(payload, "_table");
        assert_eq!(result, PreRouteExtraction::Found("events".to_string()));
    }

    #[test]
    fn extract_routing_field_missing() {
        let payload = br#"{"host": "web1"}"#;
        let result = extract_routing_field(payload, "_table");
        assert_eq!(result, PreRouteExtraction::Missing);
    }

    #[test]
    fn extract_routing_field_invalid_json() {
        let payload = b"not json at all {{{";
        let result = extract_routing_field(payload, "_table");
        assert!(
            matches!(result, PreRouteExtraction::ParseError(_)),
            "expected ParseError, got {result:?}"
        );
    }

    #[test]
    fn extract_routing_field_numeric_value_returns_raw() {
        // Routing fields are sometimes integers in practice (e.g. a table ID).
        // The extractor should return the raw representation rather than panic.
        let payload = br#"{"_table": 42}"#;
        let result = extract_routing_field(payload, "_table");
        assert_eq!(result, PreRouteExtraction::Found("42".to_string()));
    }

    #[test]
    fn extract_routing_field_nested_object() {
        let payload = br#"{"meta": {"source": "kafka"}, "_table": "logs"}"#;
        let result = extract_routing_field(payload, "_table");
        assert_eq!(result, PreRouteExtraction::Found("logs".to_string()));
    }

    // ---- filter tests -------------------------------------------------------

    /// Each filter reads its own field; first match wins; a parse error or no
    /// filter continues.
    #[test]
    fn filters_read_their_own_field() {
        let drop_missing = |f: &str| PreRouteFilter::DropFieldMissing(f.to_string());
        let dlq_on = |f: &str, v: &str| PreRouteFilter::DlqFieldValue {
            field: f.to_string(),
            value: v.to_string(),
        };
        let cases: Vec<(&str, Vec<PreRouteFilter>, &[u8], PreRouteOutcome)> = vec![
            (
                "drop when absent",
                vec![drop_missing("_table")],
                br#"{"host":"web1"}"#,
                PreRouteOutcome::Filtered,
            ),
            (
                "drop keeps present",
                vec![drop_missing("_table")],
                br#"{"_table":"events"}"#,
                PreRouteOutcome::Continue,
            ),
            (
                "drop reads its field, not another",
                vec![drop_missing("host")],
                br#"{"_table":"events"}"#,
                PreRouteOutcome::Filtered,
            ),
            (
                "dlq on value",
                vec![dlq_on("_table", "poison")],
                br#"{"_table":"poison"}"#,
                PreRouteOutcome::Dlq("field '_table' value 'poison' matches DLQ rule".into()),
            ),
            (
                "dlq other value",
                vec![dlq_on("_table", "poison")],
                br#"{"_table":"events"}"#,
                PreRouteOutcome::Continue,
            ),
            (
                "dlq reads its field, not another",
                vec![dlq_on("status", "bad")],
                br#"{"_table":"bad","status":"ok"}"#,
                PreRouteOutcome::Continue,
            ),
            (
                "two fields, second matches",
                vec![drop_missing("_table"), dlq_on("status", "bad")],
                br#"{"_table":"t","status":"bad"}"#,
                PreRouteOutcome::Dlq("field 'status' value 'bad' matches DLQ rule".into()),
            ),
            (
                "first match wins",
                vec![drop_missing("_table"), dlq_on("_table", "anything")],
                br#"{"host":"web1"}"#,
                PreRouteOutcome::Filtered,
            ),
            (
                "no filters",
                vec![],
                br#"{"_table":"x"}"#,
                PreRouteOutcome::Continue,
            ),
            (
                "parse error with a dlq filter continues",
                vec![dlq_on("_table", "x")],
                b"not json {{{",
                PreRouteOutcome::Continue,
            ),
        ];
        for (name, filters, payload, want) in cases {
            assert_eq!(apply_filters(payload, &filters), want, "{name}");
        }
    }

    #[test]
    fn filters_from_config_roundtrip() {
        let configs = vec![
            PreRouteFilterConfig::DropFieldMissing {
                field: "_table".to_string(),
            },
            PreRouteFilterConfig::DlqFieldValue {
                field: "status".to_string(),
                value: "error".to_string(),
            },
        ];
        let filters = filters_from_config(&configs);
        assert_eq!(filters.len(), 2);
        assert!(matches!(filters[0], PreRouteFilter::DropFieldMissing(_)));
        assert!(matches!(filters[1], PreRouteFilter::DlqFieldValue { .. }));
    }
}
