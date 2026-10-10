// Project:   scalo
// File:      src/deployment/contract_schema.rs
// Purpose:   The deployment contract's JSON Schema, derived from its Rust types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The JSON Schema every reader of a `deployment-contract.json` validates
//! against.
//!
//! It is derived from [`DeploymentContract`] by schemars and committed as
//! `charts/scalo-service/schema/deployment-contract.v<N>.schema.json`, where a
//! chart assembler outside Rust reads it. A test fails when the committed file
//! and the derivation differ.

use std::borrow::Cow;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde_json::Value;

use super::contract::{CONTRACT_SCHEMA_VERSION, DeploymentContract};
use super::dials::{DIAL_KEYWORD, DIAL_TIERS};

/// The JSON Schema of a contract at [`CONTRACT_SCHEMA_VERSION`].
///
/// `schema_version` is pinned to that version and required, because a reader
/// picks the schema file by it.
#[must_use]
pub fn contract_json_schema() -> Value {
    let mut schema =
        serde_json::to_value(schemars::schema_for!(DeploymentContract)).unwrap_or(Value::Null);
    if let Some(version) = schema.pointer_mut("/properties/schema_version") {
        version["const"] = Value::from(CONTRACT_SCHEMA_VERSION);
        version["default"] = Value::from(CONTRACT_SCHEMA_VERSION);
    }
    if let Some(Value::Array(required)) = schema.get_mut("required") {
        let pinned = Value::from("schema_version");
        if !required.contains(&pinned) {
            required.insert(0, pinned);
        }
    }
    schema
}

/// The committed file name of the contract schema for `version`.
#[must_use]
pub fn contract_schema_file_name(version: u32) -> String {
    format!("deployment-contract.v{version}.schema.json")
}

/// One node of an app's config JSON Schema, as the contract carries it.
///
/// The contract does not re-validate the app's schema, but wherever the dial
/// or secret marker appears it must hold a value a reader understands.
pub(crate) struct ConfigSchemaNode;

impl JsonSchema for ConfigSchemaNode {
    fn schema_name() -> Cow<'static, str> {
        "ConfigSchemaNode".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let node = generator.subschema_for::<Self>();
        json_schema!({
            "description": "A node of the app's config JSON Schema. Wherever `x-scalo-dial` appears it is `big` or `small`, and `x-scalo-secret` is a boolean.",
            "properties": {
                DIAL_KEYWORD: { "type": "string", "enum": DIAL_TIERS },
                "x-scalo-secret": { "type": "boolean" }
            },
            "additionalProperties": node,
            "items": node
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::breaking_changes;

    /// Where the committed schema lives, relative to the crate root.
    const SCHEMA_DIR: &str = "charts/scalo-service/schema";

    fn committed_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(SCHEMA_DIR)
            .join(contract_schema_file_name(CONTRACT_SCHEMA_VERSION))
    }

    /// Regenerate with `env SCALO_WRITE_CONTRACT_SCHEMA=1 cargo nextest run
    /// --all-features -E 'test(committed_contract_schema)'`.
    #[test]
    fn committed_contract_schema_matches_the_rust_types() {
        let generated = format!(
            "{}\n",
            serde_json::to_string_pretty(&contract_json_schema()).unwrap()
        );
        let path = committed_path();
        if std::env::var_os("SCALO_WRITE_CONTRACT_SCHEMA").is_some() {
            std::fs::write(&path, &generated).unwrap();
            return;
        }
        let committed = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        pretty_assertions::assert_eq!(
            committed,
            generated,
            "{} differs from the schema the contract types derive; regenerate it (see this test's docs)",
            path.display()
        );
    }

    #[test]
    fn the_schema_pins_its_version_and_requires_it() {
        let schema = contract_json_schema();
        assert_eq!(schema["properties"]["schema_version"]["const"], 4);
        assert_eq!(schema["required"][0], "schema_version");
        assert_eq!(
            schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
    }

    #[test]
    fn the_schema_names_no_vendor_or_product() {
        let text = contract_json_schema().to_string().to_ascii_lowercase();
        for term in ["dfe", "hyperi", "x-dfe-secret"] {
            assert!(
                !text.contains(term),
                "the contract schema mentions {term:?}"
            );
        }
        assert!(text.is_ascii());
    }

    #[test]
    fn the_schema_carries_every_v4_field() {
        let schema = contract_json_schema();
        for pointer in [
            "/properties/writable_paths",
            "/properties/termination_grace_seconds",
            "/properties/resources",
            "/properties/security",
            "/properties/singleton",
            "/properties/service_account",
            "/properties/config_mount_path",
            "/$defs/HealthContract/properties/startup_budget_seconds",
            "/$defs/PortContract/properties/public",
            "/$defs/PortContract/properties/app_protocol",
            "/$defs/SecretGroupContract/properties/optional",
            "/$defs/ConfigSchemaNode/properties/x-scalo-dial",
        ] {
            assert!(schema.pointer(pointer).is_some(), "{pointer} missing");
        }
        assert_eq!(
            schema["properties"]["termination_grace_seconds"]["default"],
            45
        );
        assert_eq!(
            schema["$defs"]["HealthContract"]["properties"]["startup_budget_seconds"]["minimum"],
            1
        );
        assert_eq!(schema["properties"]["service_account"]["default"], "own");
        assert!(
            !schema["required"]
                .as_array()
                .unwrap()
                .contains(&"service_account".into())
        );
    }

    /// The no-op baseline every breaking-change run starts from.
    #[test]
    fn the_schema_has_no_breaking_change_against_itself() {
        let schema = contract_json_schema();
        assert_eq!(breaking_changes(&schema, &schema), vec![]);
    }

    /// Dropping a field from the derived schema is caught as a removal.
    #[test]
    fn dropping_a_contract_field_is_breaking() {
        let old = contract_json_schema();
        let mut new = old.clone();
        new["properties"]
            .as_object_mut()
            .unwrap()
            .remove("termination_grace_seconds");
        let breaks = breaking_changes(&old, &new);
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert_eq!(breaks[0].path, "termination_grace_seconds");
    }
}
