// Project:   scalo
// File:      tests/integration/credential_masking.rs
// Purpose:   Credential masking against the shared cross-language corpus
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Credential-name matching, text masking and URL masking, checked against
//! `tests/fixtures/credential_masking_parity.json`, the corpus scalo-py's logger
//! is checked against too, and through a real `tracing` subscriber.

use std::collections::HashSet;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use proptest::prelude::*;
use serde_json::Value;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;

use scalo::logger::{
    MaskingWriter, default_sensitive_fields, is_credential_name, mask_sensitive_string,
    mask_url_userinfo,
};

const MASK: &str = "[REDACTED]";

/// The value every `{secret}` in the corpus stands for, built here so no credential sits in source.
fn secret() -> String {
    format!("FAKE-{}-{}", "credname", "X".repeat(12))
}

/// `text` with the corpus placeholders filled in.
fn fill(text: &str, mask: &str) -> String {
    let secret = secret();
    text.replace("{pw}", &format!("p@{secret}"))
        .replace("{secret}", &secret)
        .replace("{mask}", mask)
}

fn corpus() -> Value {
    let raw = std::fs::read_to_string("tests/fixtures/credential_masking_parity.json")
        .expect("read the corpus");
    serde_json::from_str(&raw).expect("parse the corpus")
}

fn cases<'a>(corpus: &'a Value, section: &str) -> &'a [Value] {
    let cases = corpus[section].as_array().expect("corpus section");
    assert!(!cases.is_empty(), "corpus section {section} is empty");
    cases
}

fn names<'a>(corpus: &'a Value, list: &str) -> Vec<&'a str> {
    corpus["names"][list]
        .as_array()
        .expect("corpus name list")
        .iter()
        .map(|name| name.as_str().expect("name"))
        .collect()
}

/// The text masker the logger applies, with the default fields.
fn mask_text(text: &str) -> String {
    let fields = default_sensitive_fields();
    let patterns: Vec<&str> = fields.iter().map(String::as_str).collect();
    mask_sensitive_string(text, &patterns)
}

fn text_field<'a>(case: &'a Value, field: &str) -> &'a str {
    case[field]
        .as_str()
        .unwrap_or_else(|| panic!("{field} in {case}"))
}

#[test]
fn credential_names_match() {
    let corpus = corpus();
    for list in [
        "credential",
        "spellings_of_api_key",
        "previous_scalo_rs_defaults",
    ] {
        for name in names(&corpus, list) {
            assert!(is_credential_name(name, ""), "{list}: {name}");
        }
    }
}

#[test]
fn setting_names_do_not_match() {
    let corpus = corpus();
    for list in ["shown", "no_term"] {
        for name in names(&corpus, list) {
            assert!(!is_credential_name(name, ""), "{list}: {name:?}");
        }
    }
}

#[test]
fn every_previous_default_is_still_a_default() {
    let corpus = corpus();
    let defaults: HashSet<String> = default_sensitive_fields().into_iter().collect();
    for name in names(&corpus, "previous_scalo_rs_defaults") {
        assert!(defaults.contains(name), "{name} left the default list");
    }
}

#[test]
fn a_parent_qualifies_only_a_bare_key_or_value() {
    let corpus = corpus();
    for case in cases(&corpus, "parent_cases") {
        let expected = case["credential"].as_bool().expect("credential");
        let actual = is_credential_name(text_field(case, "name"), text_field(case, "parent"));
        assert_eq!(actual, expected, "case {}", case["id"]);
    }
}

#[test]
fn text_cases_match_the_corpus() {
    let corpus = corpus();
    for case in cases(&corpus, "text_cases") {
        let input = fill(text_field(case, "input"), MASK);
        let expected = fill(text_field(case, "expected"), MASK);
        assert_eq!(mask_text(&input), expected, "case {}", case["id"]);
    }
}

#[test]
fn url_cases_match_the_corpus() {
    let corpus = corpus();
    for case in cases(&corpus, "url_cases") {
        let mask = fill(text_field(case, "mask"), MASK);
        let input = fill(text_field(case, "input"), MASK);
        let keep_user = case["keep_user"].as_bool().expect("keep_user");
        let expected = fill(text_field(case, "expected"), &mask);
        assert_eq!(
            mask_url_userinfo(&input, &mask, keep_user),
            expected,
            "case {}",
            case["id"]
        );
    }
}

#[test]
fn coarse_cases_match_the_corpus() {
    let corpus = corpus();
    for case in cases(&corpus, "coarse_cases") {
        let input = fill(text_field(case, "input"), MASK);
        let should_mask = case["should_mask"].as_bool().expect("should_mask");
        assert_eq!(
            mask_text(&input) != input,
            should_mask,
            "case {}",
            case["id"]
        );
    }
}

/// Masks URL userinfo by reading the grammar one step at a time, as a check on the scanner.
///
/// A URL starts at `://`. The user holds no white space, `:`, `/` or `@` and ends
/// at a `:`. The password runs to the last `@` a host follows, never past white
/// space or the next `://`. The next URL is looked for from that `@`.
fn reference_mask(text: &str, mask: &str, keep_user: bool) -> String {
    let chars: Vec<char> = text.chars().collect();
    let starts_head = |i: usize| chars.get(i..i + 3) == Some(&[':', '/', '/'][..]);
    let host_follows = |at: usize| {
        let mut end = at + 1;
        while end < chars.len() && !chars[end].is_whitespace() && !"@/?#".contains(chars[end]) {
            end += 1;
        }
        end == chars.len() || chars[end].is_whitespace() || "/?#".contains(chars[end])
    };
    let find_head = |from: usize| (from..chars.len()).find(|&i| starts_head(i));
    let mut out = String::new();
    let mut copied = 0;
    let mut head = find_head(0);
    while let Some(h) = head {
        let mut colon = h + 3;
        while colon < chars.len() && !chars[colon].is_whitespace() && !":/@".contains(chars[colon])
        {
            colon += 1;
        }
        if colon < chars.len() && chars[colon] == ':' {
            let mut run_end = colon + 1;
            while run_end < chars.len() && !chars[run_end].is_whitespace() && !starts_head(run_end)
            {
                run_end += 1;
            }
            let ats: Vec<usize> = (colon + 1..run_end)
                .filter(|&i| chars[i] == '@' && host_follows(i))
                .collect();
            if let Some(&at) = ats.last() {
                if !keep_user {
                    out.extend(&chars[copied..h]);
                    out.push_str("://");
                    out.push_str(mask);
                } else if at > colon + 1 {
                    out.extend(&chars[copied..=colon]);
                    out.push_str(mask);
                } else {
                    out.extend(&chars[copied..at]);
                }
                copied = at;
                head = find_head(at);
                continue;
            }
        }
        head = find_head(h + 1);
    }
    out.extend(&chars[copied..]);
    out
}

const ALPHABET: [&str; 16] = [
    "a", "B", "1", ".", ",", ":", ":", "/", "/", "@", "@", "?", "#", " ", "\\", "://",
];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20_000))]

    #[test]
    fn the_url_masker_reads_random_text_as_the_grammar_does(
        parts in proptest::collection::vec(proptest::sample::select(ALPHABET.to_vec()), 0..24)
    ) {
        let text = parts.concat();
        prop_assert_eq!(mask_url_userinfo(&text, MASK, true), reference_mask(&text, MASK, true));
        prop_assert_eq!(mask_url_userinfo(&text, "***", false), reference_mask(&text, "***", false));
    }
}

#[test]
fn a_long_line_is_masked_in_linear_time() {
    let lines = [
        "A".repeat(200_000) + " a://x",
        "a://:".to_string() + &"A".repeat(200_000),
        "1a".repeat(100_000) + " b://x",
        "a://:".to_string() + &"@".repeat(200_000),
        "a://:".to_string() + &"@a".repeat(100_000),
        "a://:@".to_string() + &"a".repeat(200_000) + "@",
        "://a:".repeat(50_000),
        "://".to_string() + &"a".repeat(200_000),
        "password: f(".repeat(20_000),
        "password: \"".repeat(20_000),
        "token=".repeat(40_000),
        "bearer ".repeat(30_000),
    ];
    for line in &lines {
        let started = Instant::now();
        let _ = mask_url_userinfo(line, MASK, true);
        let _ = mask_url_userinfo(line, MASK, false);
        let _ = mask_text(line);
        // A quadratic scan of 200,000 characters takes minutes, so the bound sits far above a linear one.
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{} took {:?}",
            &line[..20],
            started.elapsed()
        );
    }
}

// ---------------------------------------------------------------------------
// Through a real subscriber
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Captured(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    /// Emit `log` through a subscriber whose writer masks with the default fields.
    fn run(&self, json: bool, log: impl FnOnce()) {
        let fields = Arc::new(
            default_sensitive_fields()
                .into_iter()
                .collect::<HashSet<_>>(),
        );
        let shared = Arc::clone(&self.0);
        let make_writer = move || {
            MaskingWriter::new(
                CapturedWriter(Arc::clone(&shared)),
                Arc::clone(&fields),
                json,
            )
        };
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_file(false)
            .with_line_number(false)
            .with_writer(make_writer);
        if json {
            let subscriber = tracing_subscriber::registry()
                .with(EnvFilter::new("info"))
                .with(layer.json());
            tracing::subscriber::with_default(subscriber, log);
        } else {
            let subscriber = tracing_subscriber::registry()
                .with(EnvFilter::new("info"))
                .with(layer);
            tracing::subscriber::with_default(subscriber, log);
        }
    }
}

#[test]
fn a_json_line_masks_fields_in_any_spelling_and_url_passwords_in_text() {
    let secret = secret();
    let captured = Captured::new();
    captured.run(true, || {
        tracing::warn!(
            secretAccessKey = %secret,
            sasl_password = %secret,
            username = "svc",
            "connecting to postgres://svc:ab@{secret}/ef@db:5432/app"
        );
    });
    let output = captured.text();
    assert!(!output.contains(&secret), "{output}");
    let line: Value = serde_json::from_str(output.trim()).expect("one JSON line");
    let fields = &line["fields"];
    assert_eq!(fields["secretAccessKey"], MASK);
    assert_eq!(fields["sasl_password"], MASK);
    assert_eq!(fields["username"], "svc");
    assert_eq!(
        fields["message"],
        format!("connecting to postgres://svc:{MASK}@db:5432/app")
    );
}

#[test]
fn a_text_line_masks_key_colon_value_and_keeps_its_target() {
    let secret = secret();
    let captured = Captured::new();
    captured.run(false, || {
        tracing::warn!(target: "app::auth", apiKey = %secret, "retrying, x-auth-token: {secret}");
    });
    let output = captured.text();
    assert!(!output.contains(&secret), "{output}");
    assert!(
        output.contains("app::auth: retrying, x-auth-token: [REDACTED]"),
        "{output}"
    );
    assert!(output.contains("apiKey=[REDACTED]"), "{output}");
}
