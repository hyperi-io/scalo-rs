// Project:   scalo
// File:      tests/integration/chart_fixtures.rs
// Purpose:   Hold the scalo-service fixture charts to the contract schema and dial rules
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The thin charts under `charts/scalo-service/tests/fixtures/` stand for what
//! an assembler writes. Each one's contract must load in scalo, pass
//! `validate`, and pass the committed contract schema; and its
//! `values.schema.json` must carry, under `config`, exactly the dials
//! [`dials`] finds in that contract, nested by path. That last check is the
//! README's assembly algorithm, run against the committed output.

use std::path::{Path, PathBuf};

use scalo::deployment::{
    CONTRACT_SCHEMA_VERSION, DeploymentContract, contract_schema_file_name, dials,
};
use serde_json::{Map, Value, json};

fn chart_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("charts/scalo-service")
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn validator() -> jsonschema::Validator {
    let schema = read_json(
        &chart_dir()
            .join("schema")
            .join(contract_schema_file_name(CONTRACT_SCHEMA_VERSION)),
    );
    jsonschema::validator_for(&schema).expect("the committed contract schema compiles")
}

fn fixtures() -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(chart_dir().join("tests/fixtures"))
        .expect("read the fixture directory")
        .map(|entry| entry.expect("fixture entry").path())
        .filter(|path| path.join("Chart.yaml").is_file())
        .collect();
    found.sort();
    assert!(!found.is_empty(), "no fixture charts found");
    found
}

/// The `config` subtree the README's algorithm derives from a contract's dials.
fn derived_config(contract: &Value) -> Value {
    let mut config = json!({ "type": "object" });
    let Some(schema) = contract.get("config_schema").filter(|s| !s.is_null()) else {
        return config;
    };
    for (path, leaf) in dials(schema).expect("the fixture's dials read") {
        let parts: Vec<&str> = path.split('.').collect();
        let (name, parents) = parts.split_last().expect("a dial path has a name");
        let mut node = &mut config;
        for part in parents {
            let properties = node
                .as_object_mut()
                .expect("an object node")
                .entry("properties")
                .or_insert_with(|| Value::Object(Map::new()));
            node = properties
                .as_object_mut()
                .expect("properties is an object")
                .entry((*part).to_string())
                .or_insert_with(|| json!({ "type": "object" }));
        }
        node.as_object_mut()
            .expect("an object node")
            .entry("properties")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("properties is an object")
            .insert((*name).to_string(), leaf);
    }
    config
}

#[test]
fn every_fixture_contract_loads_validates_and_passes_the_schema() {
    let validator = validator();
    for fixture in fixtures() {
        let raw = read_json(&fixture.join("files/contract.json"));
        let errors: Vec<String> = validator.iter_errors(&raw).map(|e| e.to_string()).collect();
        assert_eq!(errors, Vec::<String>::new(), "{}", fixture.display());
        let contract: DeploymentContract =
            serde_json::from_value(raw).unwrap_or_else(|e| panic!("{}: {e}", fixture.display()));
        contract
            .validate()
            .unwrap_or_else(|e| panic!("{}: {e}", fixture.display()));
    }
}

#[test]
fn every_fixture_values_schema_carries_exactly_its_contracts_dials() {
    for fixture in fixtures() {
        let contract = read_json(&fixture.join("files/contract.json"));
        let values_schema = read_json(&fixture.join("values.schema.json"));
        pretty_assertions::assert_eq!(
            values_schema["properties"]["config"],
            derived_config(&contract),
            "{}",
            fixture.display()
        );
        jsonschema::validator_for(&values_schema)
            .unwrap_or_else(|e| panic!("{}: {e}", fixture.display()));
    }
}

/// An assembler adds `config` itself, and refuses a fragment that already has it.
#[test]
fn the_skeleton_values_schema_compiles_and_leaves_config_to_the_dials() {
    let skeleton = read_json(&chart_dir().join("skeleton/values.schema.json"));
    assert!(skeleton["properties"].get("config").is_none());
    jsonschema::validator_for(&skeleton).expect("the skeleton fragment compiles");
}

/// The expected-fail contracts that drop a required field, name another version
/// or name an unknown service account fail the schema as well as the chart.
#[test]
fn expected_fail_contracts_the_schema_also_refuses() {
    let validator = validator();
    for case in [
        "missing-metrics-port",
        "unsupported-schema-version",
        "service-account-unknown",
    ] {
        let contract = read_json(
            &chart_dir()
                .join("tests/expected-fail")
                .join(case)
                .join("contract.json"),
        );
        assert!(!validator.is_valid(&contract), "{case} passed the schema");
    }
}
