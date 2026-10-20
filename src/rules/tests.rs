// Project:   scalo
// File:      src/rules/tests.rs
// Purpose:   Evaluation tests for the rules module
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::HashSet;

use super::*;
use crate::expression::ProfileConfig;

fn compile(config: &RuleSetConfig) -> RuleSet {
    RuleSet::compile_with(config, &CompileOptions::default()).unwrap_or_else(|e| panic!("{e}"))
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap()
}

fn two_targets() -> RuleSetConfig {
    RuleSetConfig::new("t", ["_source", "_org_id"])
}

#[test]
fn the_first_matching_row_in_declared_order_wins() {
    let set = compile(
        &two_targets()
            .rule(
                RuleConfig::new("first", "_source", SetAction::value("a"))
                    .matching(MatchConfig::starts_with("host", "web")),
            )
            .rule(
                RuleConfig::new("second", "_source", SetAction::value("b"))
                    .matching(MatchConfig::starts_with("host", "web-")),
            ),
    );
    let source = set.target("_source").unwrap();
    let record = json(r#"{"host":"web-1"}"#);
    let outcome = set.evaluate(&record);
    let assigned = outcome.get(source).unwrap();
    assert_eq!(assigned.value(), "a");
    assert_eq!(assigned.rule().map(|r| r.id()), Some("first"));
}

#[test]
fn no_matching_row_leaves_the_target_unassigned() {
    let set = compile(
        &two_targets().rule(
            RuleConfig::new("web", "_source", SetAction::value("web"))
                .matching(MatchConfig::equals("host", "web")),
        ),
    );
    let record = json(r#"{"host":"db"}"#);
    let outcome = set.evaluate(&record);
    assert!(outcome.is_empty());
    assert_eq!(outcome.clamp(), None);
}

/// `_source` is evaluated before `_org_id`, so an org row can test the source
/// the same pass chose rather than the record's own.
#[test]
fn a_later_target_reads_an_earlier_targets_new_value() {
    let set = compile(
        &two_targets()
            .rule(
                RuleConfig::new("org-for-web", "_org_id", SetAction::value("web-org"))
                    .matching(MatchConfig::equals("_source", "web")),
            )
            .rule(
                RuleConfig::new("web", "_source", SetAction::value("web"))
                    .matching(MatchConfig::starts_with("host", "web-")),
            ),
    );
    let org = set.target("_org_id").unwrap();
    let record = json(r#"{"host":"web-1","_source":"sender-said"}"#);
    assert_eq!(set.evaluate(&record).value(org), Some("web-org"));

    // With no `_source` row matching, the record's own value is what the org row reads.
    let unmatched = json(r#"{"host":"db-1","_source":"web"}"#);
    assert_eq!(set.evaluate(&unmatched).value(org), Some("web-org"));
}

/// An earlier target cannot read a later one's new value: it does not exist yet.
#[test]
fn an_earlier_target_reads_the_records_value_of_a_later_one() {
    let set = compile(
        &two_targets()
            .rule(
                RuleConfig::new("src", "_source", SetAction::from("_org_id"))
                    .matching(MatchConfig::exists("_org_id")),
            )
            .rule(RuleConfig::new("org", "_org_id", SetAction::value("new"))),
    );
    let source = set.target("_source").unwrap();
    let record = json(r#"{"_org_id":"old"}"#);
    let outcome = set.evaluate(&record);
    assert_eq!(outcome.value(source), Some("old"));
}

#[test]
fn a_cel_condition_reads_an_earlier_targets_new_value() {
    let set = compile(
        &two_targets()
            .rule(RuleConfig::new("web", "_source", SetAction::value("web")))
            .rule(
                RuleConfig::new("cel", "_org_id", SetAction::value("o"))
                    .when(r#"_source == "web" && size(host) > 2"#),
            ),
    );
    let org = set.target("_org_id").unwrap();
    assert_eq!(
        set.evaluate(&json(r#"{"host":"abc"}"#)).value(org),
        Some("o")
    );
    assert_eq!(set.evaluate(&json(r#"{"host":"ab"}"#)).value(org), None);
    // A condition naming a missing field is false, not an error.
    assert_eq!(set.evaluate(&json("{}")).value(org), None);
}

#[test]
fn a_from_row_that_finds_nothing_falls_through_to_the_next() {
    let set = compile(
        &two_targets()
            .rule(RuleConfig::new(
                "tags",
                "_org_id",
                SetAction::from("tags.event.org_id"),
            ))
            .rule(RuleConfig::new(
                "flat",
                "_org_id",
                SetAction::from("org_id"),
            ))
            .rule(RuleConfig::new(
                "fallback",
                "_org_id",
                SetAction::value("default"),
            )),
    );
    let org = set.target("_org_id").unwrap();
    let cases = [
        (
            r#"{"tags":{"event":{"org_id":"t1"}},"org_id":"f1"}"#,
            "t1",
            "tags",
        ),
        (
            r#"{"tags":{"event":{"org_id":""}},"org_id":"f1"}"#,
            "f1",
            "flat",
        ),
        (
            r#"{"tags":{"event":{"org_id":null}},"org_id":"f1"}"#,
            "f1",
            "flat",
        ),
        (
            r#"{"tags":{"event":{"org_id":{"x":1}}},"org_id":7}"#,
            "7",
            "flat",
        ),
        (r#"{"other":1}"#, "default", "fallback"),
    ];
    for (record, value, rule) in cases {
        let parsed = json(record);
        let outcome = set.evaluate(&parsed);
        let assigned = outcome.get(org).unwrap();
        assert_eq!(assigned.value(), value, "{record}");
        assert_eq!(assigned.rule().map(|r| r.id()), Some(rule), "{record}");
    }
}

/// A value the lookup table does not hold is no match, so the next row runs.
#[test]
fn an_unknown_lookup_key_misses_and_falls_through() {
    let set = compile(
        &two_targets()
            .lookup(
                "aliases",
                [("logs_syslog", "syslog"), ("logs_beats_winlog", "windows")],
            )
            .rule(RuleConfig::new(
                "alias",
                "_source",
                SetAction::lookup("aliases", "tags.event.category"),
            ))
            .rule(RuleConfig::new("main", "_source", SetAction::value("main"))),
    );
    let source = set.target("_source").unwrap();
    let known = json(r#"{"tags":{"event":{"category":"logs_syslog"}}}"#);
    let unknown = json(r#"{"tags":{"event":{"category":"logs_unheard_of"}}}"#);
    let absent = json("{}");
    assert_eq!(set.evaluate(&known).value(source), Some("syslog"));
    assert_eq!(
        set.evaluate(&unknown)
            .get(source)
            .and_then(|a| a.rule())
            .map(|r| r.id()),
        Some("main")
    );
    assert_eq!(set.evaluate(&absent).value(source), Some("main"));
}

#[test]
fn every_native_test_reads_the_fields_text() {
    let cases: [(MatchConfig, &str, bool); 16] = [
        (MatchConfig::equals("h", "web"), "web", true),
        (MatchConfig::equals("h", "web"), "webx", false),
        (MatchConfig::starts_with("h", "we"), "web", true),
        (MatchConfig::ends_with("h", "eb"), "web", true),
        (MatchConfig::ends_with("h", "we"), "web", false),
        (MatchConfig::contains("h", "e"), "web", true),
        (MatchConfig::contains("h", "x"), "web", false),
        (MatchConfig::prefix_and_suffix("h", "w", "b"), "web", true),
        (MatchConfig::prefix_and_suffix("h", "w", "x"), "web", false),
        (MatchConfig::length("h", Some(3), Some(3)), "web", true),
        (MatchConfig::length("h", Some(4), None), "web", false),
        (MatchConfig::length("h", None, Some(2)), "web", false),
        (
            MatchConfig::length("h", Some(2), Some(2)),
            "\u{e9}\u{e9}",
            true,
        ),
        (MatchConfig::exists("h"), "web", true),
        (MatchConfig::exists("h"), "", false),
        (MatchConfig::contains("h", "internal").negate(), "web", true),
    ];
    for (matcher, value, expected) in cases {
        let set = compile(&two_targets().rule(
            RuleConfig::new("r", "_source", SetAction::value("hit")).matching(matcher.clone()),
        ));
        let record = serde_json::json!({ "h": value });
        let hit = !set.evaluate(&record).is_empty();
        assert_eq!(hit, expected, "{matcher:?} on {value:?}");
    }
}

#[test]
fn a_test_on_a_missing_null_or_composite_field_fails_and_its_negation_holds() {
    let plain = compile(
        &two_targets().rule(
            RuleConfig::new("r", "_source", SetAction::value("hit"))
                .matching(MatchConfig::contains("h", "x")),
        ),
    );
    let negated = compile(
        &two_targets().rule(
            RuleConfig::new("r", "_source", SetAction::value("hit"))
                .matching(MatchConfig::contains("h", "x").negate()),
        ),
    );
    for record in [r"{}", r#"{"h":null}"#, r#"{"h":{"x":1}}"#] {
        let record = json(record);
        assert!(plain.evaluate(&record).is_empty(), "{record}");
        assert!(!negated.evaluate(&record).is_empty(), "{record}");
    }
}

#[test]
fn exists_accepts_an_object_and_refuses_null_and_empty() {
    let set = compile(&two_targets().rule(
        RuleConfig::new("r", "_source", SetAction::value("hit")).matching(MatchConfig::exists("h")),
    ));
    assert!(!set.evaluate(&json(r#"{"h":{"a":1}}"#)).is_empty());
    assert!(!set.evaluate(&json(r#"{"h":0}"#)).is_empty());
    for absent in [r"{}", r#"{"h":null}"#, r#"{"h":""}"#] {
        assert!(set.evaluate(&json(absent)).is_empty(), "{absent}");
    }
}

#[test]
fn all_needs_every_part_and_negate_inverts_it() {
    let both = MatchConfig::all(vec![
        MatchConfig::starts_with("h", "web-"),
        MatchConfig::ends_with("h", ".prod"),
    ]);
    let set = compile(
        &two_targets()
            .rule(
                RuleConfig::new("both", "_source", SetAction::value("prod")).matching(both.clone()),
            )
            .rule(
                RuleConfig::new("not-both", "_source", SetAction::value("other"))
                    .matching(both.negate()),
            ),
    );
    let source = set.target("_source").unwrap();
    assert_eq!(
        set.evaluate(&json(r#"{"h":"web-1.prod"}"#)).value(source),
        Some("prod")
    );
    assert_eq!(
        set.evaluate(&json(r#"{"h":"web-1.dev"}"#)).value(source),
        Some("other")
    );
}

#[test]
fn a_regex_test_runs_where_the_profile_allows_it() {
    let options = CompileOptions::default().with_profile(ProfileConfig {
        allow_regex: true,
        ..ProfileConfig::default()
    });
    let set = RuleSet::compile_with(
        &two_targets().rule(
            RuleConfig::new("r", "_source", SetAction::value("hit"))
                .matching(MatchConfig::regex("h", "^web-[0-9]+$")),
        ),
        &options,
    )
    .unwrap();
    assert!(!set.evaluate(&json(r#"{"h":"web-12"}"#)).is_empty());
    assert!(set.evaluate(&json(r#"{"h":"web-x"}"#)).is_empty());
}

#[test]
fn bytes_and_a_parsed_value_give_the_same_outcome() {
    let set = compile(
        &two_targets()
            .lookup("t", [("a", "alpha")])
            .rule(RuleConfig::new("l", "_source", SetAction::lookup("t", "k")))
            .rule(
                RuleConfig::new("o", "_org_id", SetAction::from("org"))
                    .matching(MatchConfig::equals("_source", "alpha")),
            ),
    );
    let text = r#"{"k":"a","org":"acme"}"#;
    let bytes = JsonPayload::new(text.as_bytes()).unwrap();
    let value = json(text);
    let from_bytes: Vec<(String, String)> = set
        .evaluate(&bytes)
        .iter()
        .map(|(f, a)| (f.to_string(), a.value().to_string()))
        .collect();
    let from_value: Vec<(String, String)> = set
        .evaluate(&value)
        .iter()
        .map(|(f, a)| (f.to_string(), a.value().to_string()))
        .collect();
    assert_eq!(from_bytes, from_value);
    assert_eq!(
        from_bytes,
        [
            ("_source".to_string(), "alpha".to_string()),
            ("_org_id".to_string(), "acme".to_string())
        ]
    );
}

fn org_rules() -> RuleSet {
    compile(
        &two_targets()
            .rule(RuleConfig::new(
                "sender",
                "_org_id",
                SetAction::from("tenant"),
            ))
            .rule(
                RuleConfig::new("web", "_org_id", SetAction::value("web-org"))
                    .matching(MatchConfig::starts_with("host", "web")),
            ),
    )
}

fn allowed() -> HashSet<String> {
    ["acme".to_string(), "default".to_string()].into()
}

/// A final value outside the allowed set is replaced by the default, and the
/// row that produced it is named.
#[test]
fn the_clamp_replaces_a_value_outside_the_allowed_set() {
    let set = org_rules();
    let org = set.target("_org_id").unwrap();
    let allowed = allowed();
    let clamp = Clamp::new(org, &allowed, "default");

    let record = json(r#"{"tenant":"other-corp"}"#);
    let outcome = set.evaluate_clamped(&record, &clamp);
    assert_eq!(outcome.value(org), Some("default"));
    assert_eq!(outcome.get(org).unwrap().rule(), None);
    match outcome.clamp() {
        Some(ClampOutcome::Replaced { was, rule }) => {
            assert_eq!(was, "other-corp");
            assert_eq!(rule.map(|r| r.id()), Some("sender"));
        }
        other => panic!("expected Replaced, got {other:?}"),
    }

    let acme = json(r#"{"tenant":"acme"}"#);
    let inside = set.evaluate_clamped(&acme, &clamp);
    assert_eq!(inside.value(org), Some("acme"));
    assert_eq!(inside.clamp(), Some(&ClampOutcome::Kept));
}

/// With no row setting the target, the record's own value is the final one,
/// and it is held to the set like any other.
#[test]
fn the_clamp_holds_the_records_own_value_too() {
    let set = org_rules();
    let org = set.target("_org_id").unwrap();
    let allowed = allowed();
    let clamp = Clamp::new(org, &allowed, "default");

    let other = json(r#"{"_org_id":"other-corp"}"#);
    let foreign = set.evaluate_clamped(&other, &clamp);
    assert_eq!(foreign.value(org), Some("default"));
    assert!(matches!(
        foreign.clamp(),
        Some(ClampOutcome::Replaced { rule: None, .. })
    ));

    let acme = json(r#"{"_org_id":"acme"}"#);
    let own = set.evaluate_clamped(&acme, &clamp);
    assert_eq!(
        own.get(org),
        None,
        "an allowed record value is left in place"
    );
    assert_eq!(own.clamp(), Some(&ClampOutcome::Kept));

    let empty = json(r#"{"_org_id":""}"#);
    let none = set.evaluate_clamped(&empty, &clamp);
    assert_eq!(none.value(org), Some("default"));
    assert_eq!(none.clamp(), Some(&ClampOutcome::Defaulted));
}

#[test]
fn hits_count_per_row_and_clamps_per_row_and_record() {
    let set = org_rules();
    let org = set.target("_org_id").unwrap();
    let allowed = allowed();
    let clamp = Clamp::new(org, &allowed, "default");
    for record in [
        r#"{"tenant":"acme"}"#,
        r#"{"tenant":"evil"}"#,
        r#"{"host":"web-1"}"#,
        r#"{"_org_id":"evil"}"#,
        r"{}",
    ] {
        let _ = set.evaluate_clamped(&json(record), &clamp);
    }
    let usage = set.usage();
    let counts: Vec<(&str, u64, u64)> = usage.iter().map(|u| (u.rule, u.hits, u.clamped)).collect();
    assert_eq!(counts, [("sender", 2, 1), ("web", 1, 1)]);
    assert_eq!(set.record_clamped(), 1);
}

#[test]
fn a_batch_adds_its_counts_on_flush_and_on_drop() {
    let set = org_rules();
    {
        let mut batch = set.batch();
        for _ in 0..3 {
            let _ = batch.evaluate(&json(r#"{"tenant":"acme"}"#));
        }
        assert_eq!(
            set.usage()[0].hits,
            0,
            "a batch holds its counts until it flushes"
        );
        batch.flush();
        assert_eq!(set.usage()[0].hits, 3);
        let _ = batch.evaluate(&json(r#"{"tenant":"acme"}"#));
    }
    assert_eq!(
        set.usage()[0].hits,
        4,
        "dropping the batch flushed the last count"
    );
}

#[test]
fn a_clamp_on_a_target_of_another_set_does_nothing() {
    let wide = compile(&RuleSetConfig::new("w", ["a", "b", "c"]));
    let set = org_rules();
    let foreign = wide.target("c").unwrap();
    let allowed = allowed();
    let record = json("{}");
    let outcome = set.evaluate_clamped(&record, &Clamp::new(foreign, &allowed, "default"));
    assert_eq!(outcome.clamp(), None);
    assert!(outcome.is_empty());
}

#[test]
fn outcome_iteration_follows_target_order() {
    let set = compile(
        &two_targets()
            .rule(RuleConfig::new("o", "_org_id", SetAction::value("o")))
            .rule(RuleConfig::new("s", "_source", SetAction::value("s"))),
    );
    let record = json("{}");
    let fields: Vec<&str> = set.evaluate(&record).iter().map(|(f, _)| f).collect();
    assert_eq!(fields, ["_source", "_org_id"]);
    assert_eq!(
        set.target_field(set.target("_org_id").unwrap()),
        Some("_org_id")
    );
    assert_eq!(set.target("nope"), None);
}
