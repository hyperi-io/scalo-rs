// Project:   scalo
// File:      src/yaml/tests.rs
// Purpose:   Behaviour tests for the scalo YAML parse rules, budgets and emitter
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Debug, Deserialize)]
struct Flag {
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct Text {
    s: String,
}

#[derive(Debug, Deserialize)]
struct MaybeText {
    s: Option<String>,
}

fn config<T: DeserializeOwned>(text: &str) -> Result<T, YamlError> {
    from_str(text, "test.yaml", Profile::Config)
}

fn value(text: &str) -> Value {
    to_value(text, "test.yaml", Profile::Data).unwrap()
}

#[test]
fn only_true_and_false_are_booleans() {
    assert!(config::<Flag>("enabled: true\n").unwrap().enabled);
    assert!(config::<Flag>("enabled: True\n").unwrap().enabled);
    for word in ["yes", "no", "on", "off", "y", "n"] {
        let err = config::<Flag>(&format!("enabled: {word}\n")).unwrap_err();
        assert_eq!(err.line, Some(1), "{word}: {err}");
        assert_eq!(config::<Text>(&format!("s: {word}\n")).unwrap().s, word);
        assert_eq!(value(&format!("v: {word}\n"))["v"], json!(word));
    }
}

/// A null into a plain `String` is a located error. `Option` and the opt-in
/// helper read it as absent or empty.
#[test]
fn null_into_string_is_a_located_error() {
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Two {
        a: u8,
        s: String,
    }
    #[derive(Deserialize)]
    struct Opted {
        #[serde(default, deserialize_with = "de::null_as_empty")]
        s: String,
        #[serde(default, deserialize_with = "de::null_as_empty")]
        missing: String,
    }

    for text in ["a: 1\ns:\n", "a: 1\ns: ~\n", "a: 1\ns: null\n"] {
        let err = config::<Two>(text).unwrap_err();
        assert_eq!(err.line, Some(2), "{text:?}: {err}");
        assert!(config::<MaybeText>(&text[5..]).unwrap().s.is_none());
    }

    let opted: Opted = config("s:\n").unwrap();
    assert_eq!(opted.s, "");
    assert_eq!(opted.missing, "");
    let opted: Opted = config("s: kept\n").unwrap();
    assert_eq!(opted.s, "kept");
}

#[test]
fn sensitive_string_reads_null_as_empty() {
    #[derive(Deserialize)]
    struct Secret {
        password: crate::SensitiveString,
    }
    assert!(config::<Secret>("password:\n").unwrap().password.is_empty());
    assert_eq!(
        config::<Secret>("password: hunter2\n")
            .unwrap()
            .password
            .expose(),
        "hunter2"
    );
}

#[test]
fn duplicate_keys_are_refused_in_structs_maps_and_values() {
    assert_eq!(config::<Text>("s: a\ns: b\n").unwrap_err().line, Some(2));
    assert!(config::<HashMap<String, String>>("k: a\nk: b\n").is_err());
    assert!(to_value("k: a\nk: b\n", "t", Profile::Data).is_err());
}

#[test]
fn merge_keys_are_merged() {
    #[derive(Deserialize)]
    struct Pair {
        a: u8,
        b: u8,
    }
    #[derive(Deserialize)]
    struct Doc {
        base: Pair,
        child: Pair,
    }
    let doc: Doc = config("base: &base\n  a: 1\n  b: 2\nchild:\n  <<: *base\n  b: 3\n").unwrap();
    assert_eq!((doc.base.a, doc.base.b), (1, 2));
    assert_eq!((doc.child.a, doc.child.b), (1, 3));
}

#[test]
fn non_finite_floats_are_refused_untyped_and_read_typed() {
    #[derive(Deserialize)]
    struct Float {
        v: f64,
    }
    assert!(to_value("v: .inf\n", "t", Profile::Data).is_err());
    assert!(to_value("v: .nan\n", "t", Profile::Data).is_err());
    assert!(config::<Float>("v: .inf\n").unwrap().v.is_infinite());
}

#[test]
fn binary_tag_into_string_keeps_the_text() {
    assert_eq!(
        config::<Text>("s: !!binary aGVsbG8=\n").unwrap().s,
        "aGVsbG8="
    );
}

/// The number shapes the design lints: numbers untyped, text typed.
#[test]
fn leading_zero_and_underscore_numbers_by_target() {
    let v = value("a: 0123\nb: 1_000\nc: '0123'\n");
    assert_eq!(v["a"].as_f64(), Some(123.0));
    assert_eq!(v["b"].as_i64(), Some(1000));
    assert_eq!(v["c"], json!("0123"));
    assert_eq!(config::<Text>("s: 0123\n").unwrap().s, "0123");
    assert_eq!(config::<Text>("s: 1_000\n").unwrap().s, "1_000");
}

#[test]
fn more_than_one_document_is_refused() {
    assert!(config::<Text>("s: a\n---\ns: b\n").is_err());
    assert!(to_value("s: a\n---\ns: b\n", "t", Profile::Data).is_err());
}

#[test]
fn empty_and_comment_only_documents_read_as_default() {
    #[derive(Debug, Default, Deserialize, PartialEq)]
    #[serde(default)]
    struct Defaults {
        n: u8,
    }
    assert_eq!(config::<Defaults>("").unwrap(), Defaults::default());
    assert_eq!(
        config::<Defaults>("# nothing\n").unwrap(),
        Defaults::default()
    );
    assert_eq!(
        config::<Option<BTreeMap<String, String>>>("").unwrap(),
        None
    );
}

#[test]
fn config_budget_refuses_deep_nesting_and_big_input() {
    let mut deep = String::new();
    for depth in 0..40 {
        deep.push_str(&"  ".repeat(depth));
        deep.push_str("a:\n");
    }
    deep.push_str(&"  ".repeat(40));
    deep.push_str("a: 1\n");
    assert!(
        config::<Value>(&deep).is_err(),
        "40 levels is over the config depth"
    );
    assert!(from_str::<Value>(&deep, "t", Profile::Data).is_ok());

    let big = format!("s: {}\n", "x".repeat(CONFIG_MAX_INPUT_BYTES));
    let err = config::<Text>(&big).unwrap_err();
    assert!(err.message().contains("limit"), "{err}");
    assert!(err.line.is_none());
}

/// Enrichment-table shape, five keys a row: 40k rows breach the config and
/// default node caps and pass the data profile.
#[test]
fn data_budget_takes_a_40k_row_table() {
    use std::fmt::Write as _;

    let mut text = String::with_capacity(40_000 * 120);
    for row in 0..40_000 {
        let _ = write!(
            text,
            "- ip: 10.0.{}.{}\n  host: host-{row}\n  site: syd\n  owner: ops\n  tier: {}\n",
            row / 256 % 256,
            row % 256,
            row % 3
        );
    }
    let rows: Vec<Value> = from_str(&text, "table.yaml", Profile::Data).unwrap();
    assert_eq!(rows.len(), 40_000);
    assert!(from_str::<Vec<Value>>(&text, "table.yaml", Profile::Config).is_err());
}

#[test]
fn trusted_profile_has_no_byte_limit() {
    let big = format!("s: {}\n", "x".repeat(CONFIG_MAX_INPUT_BYTES + 1));
    assert_eq!(
        from_str::<Text>(&big, "t", Profile::Trusted)
            .unwrap()
            .s
            .len(),
        CONFIG_MAX_INPUT_BYTES + 1
    );
}

#[test]
fn errors_carry_origin_line_and_column_and_no_neighbour_value() {
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Db {
        password: String,
        port: u16,
    }
    let err = from_str::<Db>(
        "password: hunter2\nport: notanumber\n",
        "db.yaml",
        Profile::Config,
    )
    .unwrap_err();
    assert_eq!(err.origin, "db.yaml");
    assert_eq!(err.line, Some(2));
    assert_eq!(err.column, Some(7));
    let shown = err.to_string();
    assert!(shown.starts_with("db.yaml: "), "{shown}");
    assert!(!shown.contains("hunter2"), "{shown}");
    assert!(std::error::Error::source(&err).is_some());
}

#[test]
fn render_snippet_shows_one_line_and_masks_secrets() {
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Db {
        password: u16,
        port: u16,
    }
    let source = "port: 1\npassword: hunter2\n";
    let err = from_str::<Db>(source, "db.yaml", Profile::Config).unwrap_err();
    let snippet = err.render_snippet(source);
    assert!(snippet.contains("db.yaml:2"), "{snippet}");
    assert!(snippet.contains("password: ***REDACTED***"), "{snippet}");
    assert!(!snippet.contains("hunter2"), "{snippet}");
    assert!(
        !snippet.contains("port: 1"),
        "only the failing line: {snippet}"
    );

    let source = "port: notanumber\n";
    let err = from_str::<Db>(source, "db.yaml", Profile::Config).unwrap_err();
    assert!(err.render_snippet(source).contains("port: notanumber"));
}

/// A value under a secret parent key is masked though its own key is plain.
#[test]
fn render_snippet_masks_under_a_secret_parent() {
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Outer {
        credentials: Inner,
    }
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Inner {
        user: String,
        value: u16,
    }
    let source = "credentials:\n  user: svc\n  value: s3cr3t-text\n";
    let err = from_str::<Outer>(source, "c.yaml", Profile::Config).unwrap_err();
    assert_eq!(err.line, Some(3));
    let snippet = err.render_snippet(source);
    assert!(!snippet.contains("s3cr3t"), "{snippet}");
    assert!(snippet.contains("***REDACTED***"), "{snippet}");
}

#[test]
fn render_snippet_is_empty_without_a_line() {
    let err = from_path::<Text>("/nonexistent/x.yaml", Profile::Config).unwrap_err();
    assert!(err.line.is_none());
    assert_eq!(err.render_snippet("anything"), "");
}

#[test]
fn from_path_names_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.yaml");
    std::fs::write(&path, "s: [1\n").unwrap();
    let err = from_path::<Text>(&path, Profile::Config).unwrap_err();
    assert_eq!(err.origin, path.display().to_string());

    std::fs::write(&path, "s: ok\n").unwrap();
    assert_eq!(from_path::<Text>(&path, Profile::Config).unwrap().s, "ok");
}

#[test]
fn reporting_lists_unknown_keys_and_lints() {
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct App {
        name: String,
        nested: Nested,
    }
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Nested {
        enabled: bool,
    }
    let text = "name: a\nenabeld: true\nnested:\n   enabled: true\n   typo: 1\nzip: 0800\n";
    let loaded: Loaded<App> = from_str_reporting(text, "app.yaml", Profile::Config).unwrap();
    assert_eq!(loaded.unknown_keys, ["enabeld", "nested.typo", "zip"]);
    let lint_lines: Vec<u64> = loaded.lints.iter().map(|l| l.line).collect();
    assert_eq!(lint_lines, [4, 6], "{:?}", loaded.lints);

    let data: Loaded<Value> = from_str_reporting(text, "app.yaml", Profile::Data).unwrap();
    assert_eq!(data.unknown_keys, [] as [String; 0]);
    assert_eq!(data.lints.len(), 1, "data skips the indentation pass");
}

#[test]
fn to_string_reads_back_and_empties_whitespace_lines() {
    let original = json!({
        "name": "svc",
        "flag": "yes",
        "zero": "0123",
        "text": "first line\n\nthird line after a blank\n",
        "long": format!("{}end", "word ".repeat(40)),
        "list": [1, 2.5, true, null],
        "empty": {},
    });
    let text = to_string(&original).unwrap();
    assert!(
        !text
            .lines()
            .any(|line| !line.is_empty() && line.trim().is_empty()),
        "a whitespace-only line survived:\n{text}"
    );
    let back: Value = from_str(&text, "t", Profile::Trusted).unwrap();
    assert_eq!(back, original);
}

#[test]
fn to_string_accepts_f32_rounding() {
    #[derive(serde::Serialize)]
    struct Ratio {
        r: f32,
    }
    let text = to_string(&Ratio { r: 0.1 }).unwrap();
    assert!(text.contains("0.1"), "{text}");
}

/// A value whose YAML would not read back is refused rather than written.
#[test]
fn to_string_refuses_what_does_not_read_back() {
    assert!(to_string(&f64::NAN).is_err());
}

#[test]
fn errors_and_loaded_values_cross_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<YamlError>();
    assert_send_sync::<Loaded<Value>>();
    assert_send_sync::<Lint>();
}
