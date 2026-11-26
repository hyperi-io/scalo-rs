// Project:   scalo
// File:      src/deployment/emit.rs
// Purpose:   Emit + drift-check config artefacts. Guard declared listeners
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Emission of the reflectable config artefacts.
//!
//! Given a [`DeploymentContract`] carrying `config_schema` and/or
//! `capabilities`, write the four artefact files. Filenames follow scalo's
//! existing emitted-artefact convention (kebab-case `<domain>-<kind>`, cf.
//! `metrics-manifest.json`, `deployment-contract.json`) so they never collide
//! with app source:
//!
//! | File | From |
//! |------|------|
//! | `config-schema.json`     | `contract.config_schema` (if present) |
//! | `config-schema.yaml`     | same, YAML encoding |
//! | `capability-catalog.json` | `contract.capabilities` (if non-empty) |
//! | `capability-catalog.yaml` | same, YAML encoding |
//!
//! Output is deterministic (stable field order, no timestamps) so a committed
//! copy can be drift-checked against a fresh regeneration -- see
//! [`assert_no_config_artifact_drift`]. The same check fails when the
//! catalogue and the schema disagree on an option's address or values -- see
//! [`check_catalogue_against_schema`]. See `docs/reflectable-config-shape.md`
//! for the cross-language shape shared with scalo-py.
//!
//! [`assert_listeners_declared`] checks the contract's listeners against its
//! ports.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::dials::pointer;
use super::error::DeploymentError;
use super::{Capability, DeploymentContract, FieldSpec};

/// Derive a JSON Schema (draft 2020-12) for a config type via schemars.
///
/// Apps call this to fill [`DeploymentContract::config_schema`], typically
/// `config_schema_json::<MyConfig>()`. Secret fields (scalo
/// [`SensitiveString`](crate::SensitiveString)) carry the `x-scalo-secret`
/// marker automatically.
#[cfg(feature = "config-schema")]
#[must_use]
pub fn config_schema_json<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).unwrap_or(serde_json::Value::Null)
}

/// One artefact: its filename and rendered content.
struct Artefact {
    name: &'static str,
    content: String,
}

/// Render the config artefacts a contract would emit, as (filename, content)
/// pairs. Shared by [`emit_config_artifacts`] and the drift check so the two
/// never diverge. Only emits schema files when `config_schema` is present, and
/// capability files when `capabilities` is non-empty.
fn render(contract: &DeploymentContract) -> Result<Vec<Artefact>, DeploymentError> {
    let mut out = Vec::new();

    if let Some(schema) = &contract.config_schema {
        out.push(Artefact {
            name: "config-schema.json",
            content: to_json(schema, "config-schema.json")?,
        });
        out.push(Artefact {
            name: "config-schema.yaml",
            content: to_yaml(schema, "config-schema.yaml")?,
        });
    }

    if !contract.capabilities.is_empty() {
        out.push(Artefact {
            name: "capability-catalog.json",
            content: to_json(&contract.capabilities, "capability-catalog.json")?,
        });
        out.push(Artefact {
            name: "capability-catalog.yaml",
            content: to_yaml(&contract.capabilities, "capability-catalog.yaml")?,
        });
    }

    Ok(out)
}

/// Pretty JSON with a single trailing newline (POSIX text file).
fn to_json<T: serde::Serialize>(value: &T, what: &str) -> Result<String, DeploymentError> {
    let mut s = serde_json::to_string_pretty(value).map_err(|e| DeploymentError::Serialise {
        what: what.to_string(),
        message: e.to_string(),
    })?;
    s.push('\n');
    Ok(s)
}

/// YAML with a single trailing newline, checked to read back to `value`.
fn to_yaml<T: serde::Serialize>(value: &T, what: &str) -> Result<String, DeploymentError> {
    crate::yaml::to_string(value).map_err(|e| DeploymentError::Serialise {
        what: what.to_string(),
        message: e.message().to_string(),
    })
}

/// Emit the reflectable config artefacts for a contract into `dir`.
///
/// Creates `dir` if needed. Returns the paths written (0, 2, or 4 files
/// depending on which of `config_schema` / `capabilities` the contract carries).
///
/// # Errors
///
/// Returns [`DeploymentError`] if the directory cannot be created, an artefact
/// cannot be serialised, or a file cannot be written.
pub fn emit_config_artifacts(
    contract: &DeploymentContract,
    dir: impl AsRef<Path>,
) -> Result<Vec<PathBuf>, DeploymentError> {
    let dir = dir.as_ref();
    let artefacts = render(contract)?;
    if artefacts.is_empty() {
        return Ok(Vec::new());
    }

    std::fs::create_dir_all(dir).map_err(|e| DeploymentError::CreateDir {
        path: dir.display().to_string(),
        source: e,
    })?;

    let mut written = Vec::with_capacity(artefacts.len());
    for artefact in artefacts {
        let path = dir.join(artefact.name);
        std::fs::write(&path, artefact.content.as_bytes()).map_err(|e| {
            DeploymentError::WriteFile {
                path: path.display().to_string(),
                source: e,
            }
        })?;
        written.push(path);
    }
    Ok(written)
}

/// Check that the committed config artefacts under `dir` match a fresh
/// regeneration from `contract` (no drift), and that the contract's catalogue
/// agrees with its schema.
///
/// Use in a downstream app's test suite so the normal `test` job fails if the
/// checked-in `config-schema.*` / `capability-catalog.*` drift from the app's current
/// `Config` / catalog. On drift, returns [`DeploymentError::Drift`] naming the
/// file and how to regenerate.
///
/// # Errors
///
/// Returns [`DeploymentError::Drift`] if [`check_catalogue_against_schema`]
/// finds anything, or a committed file is missing or its bytes differ. Other
/// [`DeploymentError`] variants on read/serialise failure.
pub fn check_config_artifact_drift(
    contract: &DeploymentContract,
    dir: impl AsRef<Path>,
) -> Result<(), DeploymentError> {
    let dir = dir.as_ref();
    let findings = check_catalogue_against_schema(contract);
    if !findings.is_empty() {
        let lines: Vec<String> = findings.iter().map(|f| format!("  {f}")).collect();
        return Err(DeploymentError::Drift {
            path: dir.join("capability-catalog.json").display().to_string(),
            detail: format!(
                "the capability catalogue and the config schema disagree. Set each field's \
                 schema_path to the dotted path of the config option it describes, and make \
                 a closed-set option an enum in the Config so the schema allows exactly the \
                 field's enum_values. Then run `<app> config-schema --dir {}` and commit the \
                 result.\n{}",
                dir.display(),
                lines.join("\n")
            ),
        });
    }
    let artefacts = render(contract)?;
    for artefact in artefacts {
        let path = dir.join(artefact.name);
        let committed = std::fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                DeploymentError::Drift {
                    path: path.display().to_string(),
                    detail: format!(
                        "artefact is missing -- run `<app> config-schema --dir {}` \
                         (or generate-artefacts) and commit the output",
                        dir.display()
                    ),
                }
            } else {
                DeploymentError::ReadFile {
                    path: path.display().to_string(),
                    source: e,
                }
            }
        })?;
        if committed != artefact.content {
            return Err(DeploymentError::Drift {
                path: path.display().to_string(),
                detail: format!(
                    "committed content differs from the generated output ({} vs {} bytes) -- \
                     the Config/catalog changed. Run `<app> config-schema --dir {}` \
                     (or generate-artefacts) and commit the result.",
                    committed.len(),
                    artefact.content.len(),
                    dir.display()
                ),
            });
        }
    }
    Ok(())
}

/// Panic if the committed config artefacts under `dir` drift from a fresh
/// regeneration from `contract`. Convenience wrapper over
/// [`check_config_artifact_drift`] for use directly in a `#[test]`.
///
/// # Panics
///
/// Panics with a remediation message if any artefact is missing or drifted.
pub fn assert_no_config_artifact_drift(contract: &DeploymentContract, dir: impl AsRef<Path>) {
    if let Err(e) = check_config_artifact_drift(contract, dir) {
        panic!("{e}");
    }
}

/// How many `$ref` and `allOf` / `anyOf` / `oneOf` steps one lookup takes
/// before it gives up, so definitions that refer to each other can't loop.
const MAX_SCHEMA_DEPTH: usize = 32;

/// Check every catalogue field against the config option it describes.
///
/// A field's [`FieldSpec::schema_path`] must name an option in
/// `contract.config_schema`. A field with [`FieldSpec::enum_values`] needs a
/// `schema_path`, and that option must allow the same set of values, as
/// `enum` or `const`. A field with neither isn't checked. Walks every
/// capability and its children. `docs/reflectable-config-shape.md` has the
/// rules a path resolves by.
///
/// Returns one line per finding, in catalogue order. Empty when the two agree.
#[must_use]
pub fn check_catalogue_against_schema(contract: &DeploymentContract) -> Vec<String> {
    let mut findings = Vec::new();
    for capability in &contract.capabilities {
        check_capability(
            contract.config_schema.as_ref(),
            capability,
            "",
            &mut findings,
        );
    }
    findings
}

/// Push a finding for each field of `capability` and its children that
/// disagrees with `schema`. `parent` is the slash-joined names above it.
fn check_capability(
    schema: Option<&Value>,
    capability: &Capability,
    parent: &str,
    findings: &mut Vec<String>,
) {
    let location = if parent.is_empty() {
        capability.name.clone()
    } else {
        format!("{parent}/{}", capability.name)
    };
    for field in &capability.fields {
        if let Some(finding) = check_field(schema, field) {
            findings.push(format!(
                "capability {location}, field {}: {finding}",
                field.name
            ));
        }
    }
    for child in &capability.children {
        check_capability(schema, child, &location, findings);
    }
}

/// What disagrees between `field` and `schema`, if anything.
fn check_field(schema: Option<&Value>, field: &FieldSpec) -> Option<String> {
    let want: BTreeSet<String> = field.enum_values.iter().cloned().collect();
    let Some(path) = field.schema_path.as_deref() else {
        return (!want.is_empty()).then(|| {
            format!(
                "enum_values {} but no schema_path, so the schema can't be checked to allow them",
                listed(&want)
            )
        });
    };
    let Some(root) = schema else {
        return Some(format!(
            "schema_path \"{path}\" is set but the contract has no config_schema"
        ));
    };
    let node = match resolve_path(root, path) {
        Ok(node) => node,
        Err(reason) => {
            return Some(format!(
                "schema_path \"{path}\" names no option in the config schema ({reason})"
            ));
        }
    };
    if want.is_empty() {
        return None;
    }
    match closed_set(root, node, 0) {
        None => Some(format!(
            "enum_values {} but \"{path}\" in the schema has no enum",
            listed(&want)
        )),
        Some(have) if have == want => None,
        Some(have) => Some(format!(
            "enum_values {} but \"{path}\" in the schema allows {}",
            listed(&want),
            listed(&have)
        )),
    }
}

/// `values` as a bracketed list of quoted strings.
fn listed(values: &BTreeSet<String>) -> String {
    let values: Vec<&String> = values.iter().collect();
    format!("{values:?}")
}

/// A step a path segment takes after its name.
#[derive(Clone, Copy)]
enum Step {
    /// `[]`: into an array's `items`.
    Items,
    /// `{}`: into a map's value schema.
    Values,
}

impl Step {
    fn token(self) -> &'static str {
        match self {
            Step::Items => "[]",
            Step::Values => "{}",
        }
    }
}

/// A segment's name and the `[]` / `{}` steps after it, or `None` when it
/// is not a name followed only by those.
fn parse_segment(segment: &str) -> Option<(&str, Vec<Step>)> {
    let (name, mut rest) = segment.split_at(segment.find(['[', '{']).unwrap_or(segment.len()));
    if name.is_empty() {
        return None;
    }
    let mut steps = Vec::new();
    while !rest.is_empty() {
        let step = [Step::Items, Step::Values]
            .into_iter()
            .find(|step| rest.starts_with(step.token()))?;
        rest = &rest[step.token().len()..];
        steps.push(step);
    }
    Some((name, steps))
}

/// The schema node at dotted `path` under `root`, or why there is none.
fn resolve_path<'a>(root: &'a Value, path: &str) -> Result<&'a Value, String> {
    let mut node = root;
    let mut walked = String::new();
    for segment in path.split('.') {
        if segment.is_empty() {
            return Err("the path has an empty segment".to_string());
        }
        let Some((name, steps)) = parse_segment(segment) else {
            return Err(format!(
                "segment \"{segment}\" is not a name followed by any of [] and {{}}"
            ));
        };
        let parent = node;
        node = find(root, parent, &|n| n.get("properties")?.get(name), 0).ok_or_else(|| {
            if walked.is_empty() {
                format!("no \"{name}\" at the top level")
            } else {
                format!("no \"{name}\" under \"{walked}\"")
            }
        })?;
        if !walked.is_empty() {
            walked.push('.');
        }
        walked.push_str(name);
        for step in steps {
            let parent = node;
            node = take_step(root, parent, step).ok_or_else(|| match step {
                Step::Items if find(root, parent, &|n| n.get("prefixItems"), 0).is_some() => {
                    format!("\"{walked}\" is a tuple (prefixItems), which [] can't step into (segment \"{segment}\")")
                }
                Step::Items => format!("\"{walked}\" is not an array (segment \"{segment}\")"),
                Step::Values => format!("\"{walked}\" is not a map (segment \"{segment}\")"),
            })?;
            walked.push_str(step.token());
        }
    }
    Ok(node)
}

/// The schema `step` leads to from `node`: an array's object `items`, or a
/// map's object `additionalProperties`, else its one `patternProperties` value.
fn take_step<'a>(root: &'a Value, node: &'a Value, step: Step) -> Option<&'a Value> {
    match step {
        Step::Items => find(root, node, &|n| n.get("items").filter(|i| i.is_object()), 0),
        Step::Values => find(
            root,
            node,
            &|n| {
                n.get("additionalProperties")
                    .filter(|v| v.is_object())
                    .or_else(|| {
                        n.get("patternProperties")?
                            .as_object()
                            .filter(|patterns| patterns.len() == 1)?
                            .values()
                            .next()
                    })
            },
            0,
        ),
    }
}

/// The first subschema `select` picks from `node`, looked up through `$ref`
/// and every `allOf` / `anyOf` / `oneOf` arm.
fn find<'a>(
    root: &'a Value,
    node: &'a Value,
    select: &dyn Fn(&'a Value) -> Option<&'a Value>,
    depth: usize,
) -> Option<&'a Value> {
    if depth > MAX_SCHEMA_DEPTH {
        return None;
    }
    if let Some(found) = select(node) {
        return Some(found);
    }
    if let Some(found) =
        follow_ref(root, node).and_then(|target| find(root, target, select, depth + 1))
    {
        return Some(found);
    }
    ["allOf", "anyOf", "oneOf"]
        .iter()
        .filter_map(|key| node.get(*key).and_then(Value::as_array))
        .flatten()
        .find_map(|arm| find(root, arm, select, depth + 1))
}

/// The closed set of values `node` allows, from `enum` or `const`, through
/// `$ref`, the union of `anyOf` / `oneOf` arms and the intersection of
/// `allOf` arms. `null` is left out. `None` when the set is open.
fn closed_set(root: &Value, node: &Value, depth: usize) -> Option<BTreeSet<String>> {
    if depth > MAX_SCHEMA_DEPTH {
        return None;
    }
    if let Some(values) = node.get("enum").and_then(Value::as_array) {
        return Some(values.iter().filter_map(value_text).collect());
    }
    if let Some(value) = node.get("const") {
        return Some(value_text(value).into_iter().collect());
    }
    if node.get("$ref").is_some() {
        return follow_ref(root, node).and_then(|target| closed_set(root, target, depth + 1));
    }
    if let Some(arms) = node.get("allOf").and_then(Value::as_array) {
        let mut sets = arms
            .iter()
            .filter_map(|arm| closed_set(root, arm, depth + 1));
        let first = sets.next()?;
        return Some(sets.fold(first, |all, set| &all & &set));
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(arms) = node.get(key).and_then(Value::as_array) {
            let mut all = BTreeSet::new();
            for arm in arms {
                if arm.get("type").and_then(Value::as_str) != Some("null") {
                    all.extend(closed_set(root, arm, depth + 1)?);
                }
            }
            return Some(all);
        }
    }
    None
}

/// An enum value as the catalogue writes it, or `None` for `null`.
fn value_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// The node `node`'s `$ref` points at, when it is local to the schema.
fn follow_ref<'a>(root: &'a Value, node: &Value) -> Option<&'a Value> {
    pointer(root, node.get("$ref")?.as_str()?).ok()
}

/// Panic if `default_config` has a listen address no port declares, or one
/// that binds a different port than the port claiming it. Wraps
/// [`DeploymentContract::undeclared_listeners`] for use directly in a `#[test]`.
///
/// # Panics
///
/// Panics listing every finding when there is at least one.
pub fn assert_listeners_declared(contract: &DeploymentContract) {
    let findings = contract.undeclared_listeners();
    if !findings.is_empty() {
        let lines: Vec<String> = findings.iter().map(|m| format!("  {m}")).collect();
        panic!(
            "{app}: listeners and declared ports disagree -- add a port with \
             `bound_from`, fix its number, or list a send-only path in \
             `unbound_listen_paths`:\n{lines}",
            app = contract.app_name,
            lines = lines.join("\n"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::{
        Capability, FieldSpec, HealthContract, ImageProfile, NativeDepsContract, OciLabels,
    };

    fn contract_with_catalog() -> DeploymentContract {
        DeploymentContract {
            app_name: "demo".into(),
            binary_name: String::new(),
            description: String::new(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "DEMO".into(),
            metric_prefix: "demo".into(),
            config_mount_path: "/etc/demo/demo.yaml".into(),
            image_registry: "registry.example.com/team".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: None,
            depends_on: vec![],
            keda: None,
            base_image: "debian:trixie-slim".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::Production,
            oci_labels: OciLabels::default(),
            schema_version: 3,
            config_schema: Some(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": { "region": { "type": "string" } }
            })),
            capabilities: vec![
                Capability::source("aws")
                    .maturity("stable")
                    .field(FieldSpec::string("id").required())
                    .field(FieldSpec::secret("secret_access_key")),
            ],
            writable_paths: vec![],
            termination_grace_seconds: 45,
            resources: crate::deployment::ResourcesContract::default(),
            security: crate::deployment::SecurityContract::default(),
            singleton: false,
            service_account: crate::deployment::ServiceAccount::Own,
            hot_paths: vec![],
            openssl_conf: crate::deployment::DEFAULT_OPENSSL_CONF.to_string(),
            memory: None,
        }
    }

    #[test]
    fn emit_writes_four_files_when_both_present() {
        let dir = tempfile::tempdir().unwrap();
        let written = emit_config_artifacts(&contract_with_catalog(), dir.path()).unwrap();
        assert_eq!(written.len(), 4);
        for name in [
            "config-schema.json",
            "config-schema.yaml",
            "capability-catalog.json",
            "capability-catalog.yaml",
        ] {
            assert!(dir.path().join(name).exists(), "missing {name}");
        }
    }

    #[test]
    fn emit_is_deterministic() {
        let contract = contract_with_catalog();
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        emit_config_artifacts(&contract, a.path()).unwrap();
        emit_config_artifacts(&contract, b.path()).unwrap();
        for name in ["config-schema.json", "capability-catalog.json"] {
            let av = std::fs::read_to_string(a.path().join(name)).unwrap();
            let bv = std::fs::read_to_string(b.path().join(name)).unwrap();
            assert_eq!(av, bv, "{name} not deterministic");
        }
    }

    #[test]
    fn emit_nothing_when_contract_bare() {
        let mut contract = contract_with_catalog();
        contract.config_schema = None;
        contract.capabilities = vec![];
        let dir = tempfile::tempdir().unwrap();
        let written = emit_config_artifacts(&contract, dir.path()).unwrap();
        assert_eq!(written, [] as [std::path::PathBuf; 0]);
    }

    #[test]
    fn drift_check_passes_on_fresh_emit() {
        let contract = contract_with_catalog();
        let dir = tempfile::tempdir().unwrap();
        emit_config_artifacts(&contract, dir.path()).unwrap();
        assert!(check_config_artifact_drift(&contract, dir.path()).is_ok());
    }

    #[test]
    fn drift_check_catches_planted_drift() {
        let contract = contract_with_catalog();
        let dir = tempfile::tempdir().unwrap();
        emit_config_artifacts(&contract, dir.path()).unwrap();
        // Corrupt one committed file.
        std::fs::write(dir.path().join("capability-catalog.json"), "[]\n").unwrap();
        let err = check_config_artifact_drift(&contract, dir.path()).unwrap_err();
        assert!(matches!(err, DeploymentError::Drift { .. }), "got {err:?}");
    }

    #[test]
    fn drift_check_catches_missing_file() {
        let contract = contract_with_catalog();
        let dir = tempfile::tempdir().unwrap();
        // Never emitted -> files missing -> drift.
        let err = check_config_artifact_drift(&contract, dir.path()).unwrap_err();
        assert!(matches!(err, DeploymentError::Drift { .. }), "got {err:?}");
    }

    /// A contract carrying `schema` and one `store` capability holding `field`.
    fn joined(schema: Option<serde_json::Value>, field: FieldSpec) -> DeploymentContract {
        let mut contract = contract_with_catalog();
        contract.config_schema = schema;
        contract.capabilities = vec![Capability::sink("store").field(field)];
        contract
    }

    /// One row of a catalogue check table: the schema, the field's schema_path
    /// and enum_values, and a piece of the one finding expected, or `None` for
    /// no finding.
    struct Case<'a> {
        name: &'a str,
        schema: Option<&'a serde_json::Value>,
        path: Option<&'a str>,
        values: &'a [&'a str],
        want: Option<&'a str>,
    }

    const BOTH: &[&str] = &["http", "native"];

    /// The closed set every table schema gives its `protocol` option.
    fn protocol_enum() -> serde_json::Value {
        serde_json::json!({ "type": "string", "enum": ["http", "native"] })
    }

    /// Check a `protocol` field built from each row against the row's schema.
    fn check_cases(cases: &[Case<'_>]) {
        for case in cases {
            let mut field = FieldSpec::string("protocol");
            field.enum_values = case.values.iter().map(|v| (*v).to_string()).collect();
            field.schema_path = case.path.map(str::to_string);
            let findings = check_catalogue_against_schema(&joined(case.schema.cloned(), field));
            let name = case.name;
            match case.want {
                None => assert!(findings.is_empty(), "{name}: {findings:?}"),
                Some(want) => {
                    assert_eq!(findings.len(), 1, "{name}: {findings:?}");
                    assert!(
                        findings[0].starts_with("capability store, field protocol: "),
                        "{name}: {}",
                        findings[0]
                    );
                    assert!(findings[0].contains(want), "{name}: {}", findings[0]);
                }
            }
        }
    }

    #[test]
    fn catalogue_check_follows_each_schema_shape() {
        use serde_json::json;

        const AT: Option<&str> = Some("store.protocol");

        let protocol = protocol_enum();
        let plain = json!({ "properties": { "protocol": protocol } });
        let nested = json!({
            "properties": { "store": { "type": "object", "properties": { "protocol": protocol } } }
        });
        let behind_ref = json!({
            "properties": { "store": { "$ref": "#/$defs/Store" } },
            "$defs": {
                "Store": {
                    "type": "object",
                    "properties": { "protocol": { "description": "Wire protocol.", "$ref": "#/$defs/Protocol" } }
                },
                "Protocol": protocol
            }
        });
        let option = json!({
            "properties": { "store": { "properties": {
                "protocol": { "anyOf": [{ "$ref": "#/$defs/Protocol" }, { "type": "null" }] }
            } } },
            "$defs": { "Protocol": protocol }
        });
        let option_inline = json!({ "properties": { "store": { "properties": {
            "protocol": { "type": ["string", "null"], "enum": ["http", "native", null] }
        } } } });
        let one_of_const = json!({ "properties": { "store": { "properties": { "protocol": { "oneOf": [
            { "type": "string", "const": "http", "description": "Over HTTP." },
            { "type": "string", "const": "native", "description": "The native protocol." }
        ] } } } } });
        let one_of_mixed = json!({ "properties": { "store": { "properties": { "protocol": { "oneOf": [
            { "type": "string", "enum": ["http"] },
            { "type": "string", "const": "native", "description": "The native protocol." }
        ] } } } } });
        let one_of_open = json!({ "properties": { "store": { "properties": { "protocol": { "oneOf": [
            { "type": "string", "enum": ["http", "native"] },
            { "type": "object", "properties": { "custom": { "type": "string" } } }
        ] } } } } });
        let draft7 = json!({
            "properties": { "store": { "allOf": [{ "$ref": "#/definitions/Store" }] } },
            "definitions": {
                "Store": { "properties": { "protocol": { "$ref": "#/definitions/Protocol" } } },
                "Protocol": protocol
            }
        });
        let escaped = json!({
            "properties": { "store": { "$ref": "#/$defs/store~1v2" } },
            "$defs": { "store/v2": { "properties": { "protocol": protocol } } }
        });
        let foreign = json!({
            "properties": { "store": { "$ref": "https://example.com/store.json" } }
        });
        let open = json!({ "properties": { "store": { "properties": {
            "protocol": { "type": "string" }
        } } } });

        #[rustfmt::skip]
        let cases = [
            Case { name: "plain property", schema: Some(&plain), path: Some("protocol"), values: BOTH, want: None },
            Case { name: "nested struct", schema: Some(&nested), path: AT, values: BOTH, want: None },
            Case { name: "behind $ref", schema: Some(&behind_ref), path: AT, values: BOTH, want: None },
            Case { name: "Option behind anyOf", schema: Some(&option), path: AT, values: BOTH, want: None },
            Case { name: "Option of an inline enum", schema: Some(&option_inline), path: AT, values: BOTH, want: None },
            Case { name: "enum via oneOf/const", schema: Some(&one_of_const), path: AT, values: BOTH, want: None },
            Case { name: "oneOf of enum and const arms", schema: Some(&one_of_mixed), path: AT, values: BOTH, want: None },
            Case { name: "draft-07 definitions behind allOf", schema: Some(&draft7), path: AT, values: BOTH, want: None },
            Case { name: "JSON-pointer-escaped $ref", schema: Some(&escaped), path: AT, values: BOTH, want: None },
            Case { name: "catalogue order is not compared", schema: Some(&nested), path: AT, values: &["native", "http"], want: None },
            Case { name: "path without enum_values", schema: Some(&open), path: AT, values: &[], want: None },
            Case { name: "no schema_path, no enum_values", schema: Some(&nested), path: None, values: &[], want: None },
            Case { name: "path missing", schema: Some(&nested), path: Some("store.proto"), values: BOTH, want: Some("names no option in the config schema (no \"proto\" under \"store\")") },
            Case { name: "path missing at the top", schema: Some(&nested), path: Some("sink.protocol"), values: BOTH, want: Some("(no \"sink\" at the top level)") },
            Case { name: "empty segment", schema: Some(&nested), path: Some("store..protocol"), values: BOTH, want: Some("(the path has an empty segment)") },
            Case { name: "$ref outside the document", schema: Some(&foreign), path: AT, values: BOTH, want: Some("(no \"protocol\" under \"store\")") },
            Case { name: "enum set differs", schema: Some(&behind_ref), path: AT, values: &["http"], want: Some("enum_values [\"http\"] but \"store.protocol\" in the schema allows [\"http\", \"native\"]") },
            Case { name: "leaf has no enum", schema: Some(&open), path: AT, values: BOTH, want: Some("enum_values [\"http\", \"native\"] but \"store.protocol\" in the schema has no enum") },
            Case { name: "open oneOf has no enum", schema: Some(&one_of_open), path: AT, values: BOTH, want: Some("in the schema has no enum") },
            Case { name: "enum_values without schema_path", schema: Some(&nested), path: None, values: BOTH, want: Some("enum_values [\"http\", \"native\"] but no schema_path") },
            Case { name: "schema absent", schema: None, path: AT, values: BOTH, want: Some("schema_path \"store.protocol\" is set but the contract has no config_schema") },
        ];
        check_cases(&cases);
    }

    #[test]
    fn catalogue_check_steps_into_arrays_and_maps() {
        use serde_json::json;

        let protocol = protocol_enum();
        let nested = json!({
            "properties": { "store": { "type": "object", "properties": { "protocol": protocol } } }
        });
        let array_of_objects = json!({
            "properties": { "tables": { "$ref": "#/$defs/Tables" } },
            "$defs": {
                "Tables": { "type": ["array", "null"], "items": { "$ref": "#/$defs/Table" } },
                "Table": { "properties": { "source": { "properties": { "protocol": protocol } } } }
            }
        });
        let array_of_enum = json!({ "properties": {
            "protocols": { "type": "array", "items": protocol }
        } });
        let array_of_arrays = json!({ "properties": {
            "matrix": { "type": "array", "items": { "type": "array", "items": protocol } }
        } });
        let tuple = json!({ "properties": {
            "pair": { "type": "array", "prefixItems": [protocol, protocol], "items": false }
        } });
        let map_of_objects = json!({ "properties": { "stores": {
            "type": "object",
            "additionalProperties": { "anyOf": [{ "$ref": "#/$defs/Store" }, { "type": "null" }] }
        } }, "$defs": { "Store": { "properties": { "protocol": protocol } } } });
        let map_of_arrays = json!({ "properties": { "routes": {
            "type": "object",
            "additionalProperties": { "type": "array", "items": protocol }
        } } });
        let pattern_map = json!({ "properties": { "stores": {
            "type": "object",
            "additionalProperties": false,
            "patternProperties": { "^[a-z]+$": { "properties": { "protocol": protocol } } }
        } } });
        let two_patterns = json!({ "properties": { "stores": {
            "type": "object",
            "patternProperties": {
                "^a": { "properties": { "protocol": protocol } },
                "^b": { "properties": { "protocol": protocol } }
            }
        } } });

        #[rustfmt::skip]
        let cases = [
            Case { name: "array of objects behind $ref", schema: Some(&array_of_objects), path: Some("tables[].source.protocol"), values: BOTH, want: None },
            Case { name: "array of enum", schema: Some(&array_of_enum), path: Some("protocols[]"), values: BOTH, want: None },
            Case { name: "array of arrays", schema: Some(&array_of_arrays), path: Some("matrix[][]"), values: BOTH, want: None },
            Case { name: "map of objects behind anyOf", schema: Some(&map_of_objects), path: Some("stores{}.protocol"), values: BOTH, want: None },
            Case { name: "map of arrays", schema: Some(&map_of_arrays), path: Some("routes{}[]"), values: BOTH, want: None },
            Case { name: "map by one patternProperties", schema: Some(&pattern_map), path: Some("stores{}.protocol"), values: BOTH, want: None },
            Case { name: "an array without [] has no enum", schema: Some(&array_of_enum), path: Some("protocols"), values: BOTH, want: Some("\"protocols\" in the schema has no enum") },
            Case { name: "name missing after []", schema: Some(&array_of_objects), path: Some("tables[].sink"), values: &[], want: Some("(no \"sink\" under \"tables[]\")") },
            Case { name: "[] on an object", schema: Some(&nested), path: Some("store[].protocol"), values: BOTH, want: Some("(\"store\" is not an array (segment \"store[]\"))") },
            Case { name: "{} on an array", schema: Some(&array_of_enum), path: Some("protocols{}"), values: BOTH, want: Some("(\"protocols\" is not a map (segment \"protocols{}\"))") },
            Case { name: "second step on the wrong kind", schema: Some(&map_of_arrays), path: Some("routes{}{}"), values: BOTH, want: Some("(\"routes{}\" is not a map (segment \"routes{}{}\"))") },
            Case { name: "{} on several patternProperties", schema: Some(&two_patterns), path: Some("stores{}.protocol"), values: BOTH, want: Some("(\"stores\" is not a map (segment \"stores{}\"))") },
            Case { name: "[] on a tuple", schema: Some(&tuple), path: Some("pair[]"), values: BOTH, want: Some("(\"pair\" is a tuple (prefixItems), which [] can't step into (segment \"pair[]\"))") },
            Case { name: "segment with an index", schema: Some(&array_of_enum), path: Some("protocols[0]"), values: BOTH, want: Some("(segment \"protocols[0]\" is not a name followed by any of [] and {})") },
            Case { name: "segment with no name", schema: Some(&array_of_enum), path: Some("[]"), values: BOTH, want: Some("(segment \"[]\" is not a name followed by any of [] and {})") },
        ];
        check_cases(&cases);
    }

    #[test]
    fn catalogue_check_walks_children_in_order() {
        let schema = serde_json::json!({ "properties": { "store": { "properties": {
            "host": { "type": "string" }
        } } } });
        let mut contract = contract_with_catalog();
        contract.config_schema = Some(schema);
        contract.capabilities = vec![
            Capability::sink("store")
                .field(FieldSpec::string("host").schema_path("store.host"))
                .field(FieldSpec::string("port").schema_path("store.port"))
                .child(
                    Capability::service("tls")
                        .field(FieldSpec::enumeration("mode", ["off", "verify"])),
                ),
        ];
        let findings = check_catalogue_against_schema(&contract);
        assert_eq!(findings.len(), 2, "{findings:?}");
        assert!(findings[0].starts_with("capability store, field port: "));
        assert!(findings[1].starts_with("capability store/tls, field mode: "));
    }

    #[test]
    fn drift_check_fails_when_the_catalogue_and_schema_disagree() {
        let contract = joined(
            Some(serde_json::json!({ "properties": { "protocol": {
                "type": "string", "enum": ["http", "native"]
            } } })),
            FieldSpec::enumeration("protocol", ["http"]).schema_path("protocol"),
        );
        let dir = tempfile::tempdir().unwrap();
        emit_config_artifacts(&contract, dir.path()).unwrap();
        let err = check_config_artifact_drift(&contract, dir.path()).unwrap_err();
        let DeploymentError::Drift { path, detail } = &err else {
            panic!("got {err:?}");
        };
        assert!(path.ends_with("capability-catalog.json"), "{path}");
        assert!(
            detail.contains("capability store, field protocol: enum_values [\"http\"]"),
            "{detail}"
        );
    }

    /// Where the reflectable-config fixture lives, relative to the crate root.
    #[cfg(feature = "config-schema")]
    const REFLECTABLE_FIXTURE: &str = "tests/fixtures/reflectable-config";

    /// The small app config the reflectable-config fixture describes.
    #[cfg(feature = "config-schema")]
    #[allow(dead_code, reason = "only the schema of these types is read")]
    mod example_config {
        /// Example app configuration.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        pub struct ExampleConfig {
            /// The record store the app writes to.
            pub store: StoreConfig,
            /// Lookup tables loaded at startup.
            pub tables: Vec<TableConfig>,
            /// Outputs, by name.
            pub outputs: std::collections::BTreeMap<String, OutputConfig>,
        }

        /// One lookup table.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        pub struct TableConfig {
            /// Path of the table file.
            pub path: String,
            /// File format of the table.
            pub format: TableFormat,
        }

        /// Table file format.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum TableFormat {
            Csv,
            Json,
        }

        /// One output.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        pub struct OutputConfig {
            /// Log levels sent to this output.
            pub levels: Vec<Level>,
        }

        /// Log level.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum Level {
            Debug,
            Info,
            Warn,
        }

        /// How the app reaches its store.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        pub struct StoreConfig {
            /// Store host name.
            pub host: String,
            /// Wire protocol to the store.
            pub protocol: Protocol,
            /// Codec for each batch. Unset sends batches uncompressed.
            pub codec: Option<Codec>,
            /// TLS to the store.
            pub tls: TlsConfig,
        }

        /// Wire protocol.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum Protocol {
            Http,
            Native,
        }

        /// Batch codec.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum Codec {
            Gzip,
            Zstd,
        }

        /// TLS settings.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        pub struct TlsConfig {
            /// Whether the server certificate is checked.
            pub mode: TlsMode,
        }

        /// TLS mode.
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum TlsMode {
            /// Plain TCP.
            Off,
            /// TLS with the server certificate checked.
            Verify,
        }
    }

    /// The contract the reflectable-config fixture is emitted from.
    #[cfg(feature = "config-schema")]
    fn reflectable_contract() -> DeploymentContract {
        let mut contract = contract_with_catalog();
        contract.config_schema = Some(config_schema_json::<example_config::ExampleConfig>());
        contract.capabilities = vec![
            Capability::sink("store")
                .description("Writes records to a store.")
                .maturity("stable")
                .field(
                    FieldSpec::string("host")
                        .required()
                        .description("Store host name.")
                        .schema_path("store.host"),
                )
                .field(
                    FieldSpec::enumeration("protocol", ["http", "native"])
                        .required()
                        .description("Wire protocol to the store.")
                        .schema_path("store.protocol"),
                )
                .field(
                    FieldSpec::enumeration("codec", ["gzip", "zstd"])
                        .description("Codec for each batch. Unset sends batches uncompressed.")
                        .schema_path("store.codec"),
                )
                .child(
                    Capability::service("tls")
                        .description("TLS to the store.")
                        .field(
                            FieldSpec::enumeration("mode", ["off", "verify"])
                                .required()
                                .description("Whether the server certificate is checked.")
                                .schema_path("store.tls.mode"),
                        ),
                ),
            Capability::source("lookup_table")
                .description("A lookup table loaded from a file at startup.")
                .field(
                    FieldSpec::string("path")
                        .required()
                        .description("Path of the table file.")
                        .schema_path("tables[].path"),
                )
                .field(
                    FieldSpec::enumeration("format", ["csv", "json"])
                        .required()
                        .description("File format of the table.")
                        .schema_path("tables[].format"),
                ),
            Capability::sink("output")
                .description("A named output.")
                .field(
                    FieldSpec::enumeration("levels", ["debug", "info", "warn"])
                        .required()
                        .description("Log levels sent to this output.")
                        .schema_path("outputs{}.levels[]"),
                ),
        ];
        contract
    }

    /// The committed fixture is what `emit_config_artifacts` writes for
    /// [`reflectable_contract`], and its catalogue agrees with its schema.
    /// `env SCALO_WRITE_REFLECTABLE_CONFIG=1` rewrites it after a deliberate
    /// change.
    #[cfg(feature = "config-schema")]
    #[test]
    fn reflectable_config_fixture_matches_and_joins() {
        let contract = reflectable_contract();
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(REFLECTABLE_FIXTURE);
        if std::env::var_os("SCALO_WRITE_REFLECTABLE_CONFIG").is_some() {
            emit_config_artifacts(&contract, &dir).unwrap();
        }
        assert_eq!(
            check_catalogue_against_schema(&contract),
            Vec::<String>::new()
        );
        if let Err(e) = check_config_artifact_drift(&contract, &dir) {
            panic!("{e}");
        }
        let schema = contract.config_schema.as_ref().unwrap();
        for pointer in [
            "/$defs/Protocol/enum",
            "/$defs/TlsMode/oneOf/0/const",
            "/$defs/StoreConfig/properties/codec/anyOf/1/type",
            "/properties/tables/items/$ref",
            "/properties/outputs/additionalProperties/$ref",
            "/$defs/OutputConfig/properties/levels/items/$ref",
        ] {
            assert!(
                schema.pointer(pointer).is_some(),
                "{pointer} missing, so the fixture no longer covers that schema shape"
            );
        }
    }

    #[test]
    fn listeners_declared_passes_when_every_listener_has_a_port() {
        let mut contract = contract_with_catalog();
        contract.default_config = Some(serde_json::json!({ "grpc": { "listen": ":6000" } }));
        contract.extra_ports =
            vec![crate::deployment::PortContract::tcp("grpc", 6000).bound_from("grpc.listen")];
        assert_listeners_declared(&contract);
    }

    #[test]
    #[should_panic(expected = "listener grpc.listen")]
    fn listeners_declared_panics_on_an_undeclared_listener() {
        let mut contract = contract_with_catalog();
        contract.default_config = Some(serde_json::json!({ "grpc": { "listen": ":6000" } }));
        assert_listeners_declared(&contract);
    }

    /// Panic, naming `T` and the first offending char in context, if the
    /// schema derived for `T` is not pure ASCII.
    #[cfg(feature = "config-schema")]
    fn assert_schema_is_ascii<T: schemars::JsonSchema>() {
        let name = std::any::type_name::<T>();
        let schema = config_schema_json::<T>();
        assert!(!schema.is_null(), "schema for {name} failed to serialise");
        let json = serde_json::to_string(&schema).unwrap();
        let chars: Vec<char> = json.chars().collect();
        if let Some(pos) = chars.iter().position(|c| !c.is_ascii()) {
            let context: String = chars[pos.saturating_sub(40)..(pos + 40).min(chars.len())]
                .iter()
                .collect();
            panic!(
                "schema for {name} carries non-ASCII {:?} (U+{:04X}) near: {context}",
                chars[pos],
                u32::from(chars[pos]),
            );
        }
    }

    /// Doc comments on config types become schema descriptions in every
    /// consumer's committed artefact, so each root schema must be ASCII.
    #[cfg(feature = "config-schema")]
    #[test]
    fn generated_config_schemas_are_ascii() {
        assert_schema_is_ascii::<DeploymentContract>();
        #[cfg(feature = "dlq")]
        assert_schema_is_ascii::<crate::dlq::DlqConfig>();
        #[cfg(feature = "dlq-kafka")]
        assert_schema_is_ascii::<crate::dlq::KafkaDlqConfig>();
        #[cfg(feature = "dlq-kafka")]
        assert_schema_is_ascii::<crate::dlq::DlqRouting>();
        #[cfg(feature = "dlq-http")]
        assert_schema_is_ascii::<crate::dlq::HttpDlqConfig>();
        #[cfg(feature = "geoip-download")]
        assert_schema_is_ascii::<crate::geoip_download::GeoIpConfig>();
        #[cfg(feature = "memory")]
        assert_schema_is_ascii::<crate::memory::MemoryGuardConfig>();
        #[cfg(feature = "scaling")]
        assert_schema_is_ascii::<crate::scaling::ScalingPressureConfig>();
        #[cfg(feature = "secrets")]
        assert_schema_is_ascii::<crate::secrets::CacheConfig>();
        #[cfg(feature = "tiered-sink")]
        assert_schema_is_ascii::<crate::tiered_sink::TieredSinkConfig>();
        #[cfg(any(feature = "spool", feature = "tiered-sink"))]
        assert_schema_is_ascii::<crate::spool_codec::CorruptionPolicy>();
        #[cfg(feature = "io")]
        assert_schema_is_ascii::<crate::io::RotationPeriod>();
        #[cfg(feature = "transport")]
        assert_schema_is_ascii::<crate::transport::filter::FilterRule>();
        #[cfg(feature = "transport-grpc")]
        assert_schema_is_ascii::<crate::transport::grpc::GrpcConfig>();
        #[cfg(feature = "transport-kafka")]
        assert_schema_is_ascii::<crate::transport::kafka::KafkaConfig>();
        #[cfg(feature = "transport-memory")]
        assert_schema_is_ascii::<crate::transport::memory::MemoryConfig>();
        #[cfg(feature = "worker-batch")]
        assert_schema_is_ascii::<crate::worker::engine::BatchProcessingConfig>();
        #[cfg(feature = "worker-batch")]
        assert_schema_is_ascii::<crate::worker::engine::PreRouteFilterConfig>();
        #[cfg(feature = "worker-batch")]
        assert_schema_is_ascii::<crate::worker::engine::types::PayloadFormat>();
    }
}
