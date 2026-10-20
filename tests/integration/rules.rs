// Project:   scalo
// File:      tests/integration/rules.rs
// Purpose:   The shared rules corpus, and a live reload that keeps the last good set
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `tests/fixtures/rules_corpus.json` is the contract scalo-py checks its own
//! validator and evaluator against, so every finding here carries its exact
//! location and span, and every outcome its row id.

use std::collections::{BTreeMap, HashSet};

use scalo::expression::ProfileConfig;
use scalo::rules::{
    ClampOutcome, CompileOptions, JsonPayload, Outcome, RuleDiagnostic, RuleSet, RuleSetConfig,
    Severity,
};
use serde::Deserialize;
use serde_json::Value;

const CORPUS: &str = "tests/fixtures/rules_corpus.json";

#[derive(Deserialize)]
struct Corpus {
    compile: Vec<CompileCase>,
    evaluate: Vec<EvaluateCase>,
}

#[derive(Deserialize)]
struct CompileCase {
    name: String,
    #[serde(default)]
    allow_regex: bool,
    config: RuleSetConfig,
    findings: Vec<Finding>,
}

#[derive(Debug, Deserialize)]
struct Finding {
    severity: String,
    location: String,
    rule: Option<String>,
    span: Option<[usize; 2]>,
    contains: String,
}

#[derive(Deserialize)]
struct EvaluateCase {
    name: String,
    config: RuleSetConfig,
    clamp: Option<ClampSpec>,
    cases: Vec<RecordCase>,
}

#[derive(Deserialize)]
struct ClampSpec {
    target: String,
    allowed: Vec<String>,
    default: String,
}

#[derive(Deserialize)]
struct RecordCase {
    record: Value,
    expect: BTreeMap<String, Expected>,
    clamp: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Expected {
    value: String,
    rule: Option<String>,
}

fn corpus() -> Corpus {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(CORPUS);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {CORPUS}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {CORPUS}: {e}"))
}

fn options(allow_regex: bool) -> CompileOptions {
    CompileOptions::default().with_profile(ProfileConfig {
        allow_regex,
        ..ProfileConfig::default()
    })
}

fn severity(d: &RuleDiagnostic) -> &'static str {
    match d.severity {
        Severity::Error => "error",
        Severity::Warning => "warning",
        _ => "other",
    }
}

#[test]
fn every_compile_case_finds_exactly_its_findings() {
    for case in corpus().compile {
        let found = RuleSet::check(&case.config, &options(case.allow_regex));
        let got: Vec<(&str, &str, Option<&str>, Option<[usize; 2]>)> = found
            .iter()
            .map(|d| {
                (
                    severity(d),
                    d.location.as_str(),
                    d.rule.as_deref(),
                    d.span.map(|s| [s.start, s.end]),
                )
            })
            .collect();
        let want: Vec<(&str, &str, Option<&str>, Option<[usize; 2]>)> = case
            .findings
            .iter()
            .map(|f| {
                (
                    f.severity.as_str(),
                    f.location.as_str(),
                    f.rule.as_deref(),
                    f.span,
                )
            })
            .collect();
        assert_eq!(got, want, "{}: {found:#?}", case.name);
        for (d, f) in found.iter().zip(&case.findings) {
            assert!(
                d.message.contains(&f.contains),
                "{}: {:?} does not contain {:?}",
                case.name,
                d.message,
                f.contains
            );
        }
        let compiled = RuleSet::compile_with(&case.config, &options(case.allow_regex));
        let has_error = case.findings.iter().any(|f| f.severity == "error");
        assert_eq!(compiled.is_err(), has_error, "{}", case.name);
    }
}

fn assignments(outcome: &Outcome<'_>) -> BTreeMap<String, Expected> {
    outcome
        .iter()
        .map(|(field, a)| {
            (
                field.to_string(),
                Expected {
                    value: a.value().to_string(),
                    rule: a.rule().map(|r| r.id().to_string()),
                },
            )
        })
        .collect()
}

fn clamp_name(outcome: &Outcome<'_>) -> Option<&'static str> {
    outcome.clamp().map(|c| match c {
        ClampOutcome::Kept => "kept",
        ClampOutcome::Replaced { .. } => "replaced",
        ClampOutcome::Defaulted => "defaulted",
        _ => "other",
    })
}

/// Raw JSON bytes and a parsed value give the same outcome for every case.
#[test]
fn every_evaluate_case_gives_its_outcome_from_bytes_and_from_a_value() {
    for case in corpus().evaluate {
        let set = RuleSet::compile_with(&case.config, &options(false))
            .unwrap_or_else(|e| panic!("{}: {e}", case.name));
        let allowed: HashSet<String> = case
            .clamp
            .as_ref()
            .map(|c| c.allowed.iter().cloned().collect())
            .unwrap_or_default();
        let clamp = case.clamp.as_ref().map(|c| {
            let target = set
                .target(&c.target)
                .unwrap_or_else(|| panic!("{}: no target {}", case.name, c.target));
            scalo::rules::Clamp::new(target, &allowed, c.default.as_str())
        });
        for (index, rc) in case.cases.iter().enumerate() {
            let bytes = serde_json::to_vec(&rc.record).unwrap();
            let payload = JsonPayload::new(&bytes).unwrap();
            let outcomes = match &clamp {
                Some(clamp) => [
                    set.evaluate_clamped(&rc.record, clamp),
                    set.evaluate_clamped(&payload, clamp),
                ],
                None => [set.evaluate(&rc.record), set.evaluate(&payload)],
            };
            for (source, outcome) in ["value", "bytes"].iter().zip(&outcomes) {
                assert_eq!(
                    assignments(outcome),
                    rc.expect,
                    "{} case {index} from {source}",
                    case.name
                );
                assert_eq!(
                    clamp_name(outcome),
                    rc.clamp.as_deref(),
                    "{} case {index} from {source}",
                    case.name
                );
            }
        }
    }
}

#[cfg(feature = "config-reload")]
mod reload {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use scalo::config::reloader::ReloaderConfig;
    use scalo::config::shared::SharedConfig;
    use scalo::rules::{CompileOptions, RuleSet, reloader};

    fn rules_file(id: &str, op: &str, value: &str) -> String {
        format!(
            "log_level: info\nrules:\n  targets: [_source]\n  rules:\n    - id: {id}\n      \
             match: {{ field: host, op: {op}, value: web- }}\n      \
             set: {{ _source: {{ value: {value} }} }}\n"
        )
    }

    /// Write `text`, then stamp an mtime `ahead` of now, so the poll sees a change
    /// however coarse the filesystem clock is.
    fn write_ahead(path: &std::path::Path, text: &str, ahead: Duration) {
        std::fs::write(path, text).unwrap();
        let file = std::fs::File::options().write(true).open(path).unwrap();
        file.set_modified(SystemTime::now() + ahead).unwrap();
    }

    fn source_of(set: &RuleSet, host: &str) -> Option<String> {
        let record = serde_json::json!({ "host": host });
        let target = set.target("_source")?;
        set.evaluate(&record).value(target).map(str::to_string)
    }

    /// A file that stops compiling leaves the running set in place, and the next
    /// good file replaces it, all without restarting anything.
    #[tokio::test]
    async fn a_reload_keeps_the_last_good_set_until_a_good_one_compiles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        write_ahead(
            &path,
            &rules_file("web", "starts_with", "web"),
            Duration::ZERO,
        );
        let options = CompileOptions::default();
        let first = RuleSet::from_file_section(&path, "rules", &options).unwrap();
        let shared = SharedConfig::new(Arc::new(first));
        let mut changes = shared.subscribe();

        let watcher = reloader(
            ReloaderConfig {
                config_path: Some(path.clone()),
                poll_interval: Duration::from_millis(20),
                periodic_interval: Duration::ZERO,
                debounce: Duration::ZERO,
                enable_sighup: false,
            },
            shared.clone(),
            "rules",
            options,
        )
        .unwrap()
        .start();
        // The reloader reads the starting mtime once it runs.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // `regex` is off in the default profile, so this file does not compile.
        write_ahead(
            &path,
            &rules_file("web", "regex", "broken"),
            Duration::from_secs(10),
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            shared.version(),
            0,
            "a set that failed to compile was published"
        );
        assert_eq!(source_of(&shared.get(), "web-1").as_deref(), Some("web"));

        write_ahead(
            &path,
            &rules_file("web-2", "starts_with", "web2"),
            Duration::from_secs(20),
        );
        tokio::time::timeout(Duration::from_secs(5), changes.changed())
            .await
            .expect("the good file was not reloaded within 5 s")
            .unwrap();
        let current = shared.get();
        assert_eq!(source_of(&current, "web-1").as_deref(), Some("web2"));
        assert_eq!(current.usage()[0].rule, "web-2");
        watcher.abort();
    }
}
