// Project:   scalo
// File:      tests/integration/docs_rs_features.rs
// Purpose:   Keep the hand-listed docs.rs feature set naming real features
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `[package.metadata.docs.rs].features` is listed by hand, because
//! `all-features` needs system libraries the docs.rs sandbox lacks. docs.rs
//! refuses the whole build on a name the crate does not have, so a renamed or
//! removed feature has to fail here rather than at the next publish.

use std::collections::BTreeSet;
use std::path::Path;

/// Lines of the TOML table `header`, up to the next table header.
fn table<'a>(manifest: &'a str, header: &str) -> Vec<&'a str> {
    manifest
        .lines()
        .skip_while(|line| line.trim() != header)
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .collect()
}

/// The key a `key = value` line sets, or `None` for a blank or comment line.
fn key(line: &str) -> Option<&str> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    line.split_once('=').map(|(key, _)| key.trim())
}

/// Every quoted string in `text`.
fn quoted(text: &str) -> Vec<&str> {
    text.split('"').skip(1).step_by(2).collect()
}

/// The docs.rs feature names `manifest` lists that are not features of it.
fn unknown_docs_rs_features(manifest: &str) -> Vec<String> {
    let docs_rs = table(manifest, "[package.metadata.docs.rs]").join("\n");
    let list = docs_rs
        .split_once("features = [")
        .and_then(|(_, rest)| rest.split_once(']'))
        .map(|(list, _)| list)
        .expect("[package.metadata.docs.rs] has no features list");
    let listed: Vec<&str> = list
        .lines()
        .map(|line| line.split('#').next().unwrap_or(""))
        .flat_map(quoted)
        .collect();
    assert!(!listed.is_empty(), "the docs.rs features list is empty");

    // An optional dependency is an implicit feature, so it is a valid name too.
    let mut known: BTreeSet<&str> = table(manifest, "[features]")
        .into_iter()
        .filter_map(key)
        .collect();
    known.extend(
        table(manifest, "[dependencies]")
            .into_iter()
            .filter(|line| line.contains("optional = true"))
            .filter_map(key),
    );

    listed
        .into_iter()
        .filter(|name| !known.contains(name))
        .map(str::to_string)
        .collect()
}

#[test]
fn every_docs_rs_feature_is_a_feature_of_the_crate() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));

    let unknown = unknown_docs_rs_features(&manifest);
    assert!(
        unknown.is_empty(),
        "[package.metadata.docs.rs].features names features the crate does not have, \
         so docs.rs would refuse the build: {unknown:?}"
    );
}

#[test]
fn a_renamed_feature_left_in_the_docs_rs_list_is_caught() {
    let manifest = r#"
[package.metadata.docs.rs]
features = [
    "metrics", "metrics-prometheus",  # renamed away
    "tokio",
]

[features]
metrics = ["dep:metrics"]

[dependencies]
tokio = { version = "1", optional = true }
"#;
    assert_eq!(unknown_docs_rs_features(manifest), ["metrics-prometheus"]);
}
