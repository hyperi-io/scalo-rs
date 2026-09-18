// Project:   scalo
// File:      src/deployment/emit.rs
// Purpose:   Emit + drift-check reflectable config artefacts (scalo-rs#6)
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
//! [`assert_no_config_artifact_drift`]. See `docs/reflectable-config-shape.md`
//! for the cross-language shape shared with scalo-py.

use std::path::{Path, PathBuf};

use super::DeploymentContract;
use super::error::DeploymentError;

/// Derive a JSON Schema (draft 2020-12) for a config type via schemars.
///
/// Apps call this to fill [`DeploymentContract::config_schema`], typically
/// `config_schema_json::<MyConfig>()`. Secret fields (scalo
/// [`SensitiveString`](crate::SensitiveString)) carry the `x-dfe-secret`
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

/// YAML with a single trailing newline (serde_yaml_ng already appends one).
fn to_yaml<T: serde::Serialize>(value: &T, what: &str) -> Result<String, DeploymentError> {
    let s = serde_yaml_ng::to_string(value).map_err(|e| DeploymentError::Serialise {
        what: what.to_string(),
        message: e.to_string(),
    })?;
    Ok(s)
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
/// regeneration from `contract` (no drift).
///
/// Use in a downstream app's test suite so the normal `test` job fails if the
/// checked-in `config-schema.*` / `capability-catalog.*` drift from the app's current
/// `Config` / catalog. On drift, returns [`DeploymentError::Drift`] naming the
/// file and how to regenerate.
///
/// # Errors
///
/// Returns [`DeploymentError::Drift`] if a committed file is missing or its
/// bytes differ; other [`DeploymentError`] variants on read/serialise failure.
pub fn check_config_artifact_drift(
    contract: &DeploymentContract,
    dir: impl AsRef<Path>,
) -> Result<(), DeploymentError> {
    let dir = dir.as_ref();
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
            image_registry: "ghcr.io/hyperi-io".into(),
            extra_ports: vec![],
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
        assert!(written.is_empty());
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
        #[cfg(feature = "dlq")]
        assert_schema_is_ascii::<crate::dlq::DlqConfig>();
        #[cfg(feature = "dlq-kafka")]
        assert_schema_is_ascii::<crate::dlq::KafkaDlqConfig>();
        #[cfg(feature = "dlq-kafka")]
        assert_schema_is_ascii::<crate::dlq::DlqRouting>();
        #[cfg(feature = "dlq-http")]
        assert_schema_is_ascii::<crate::dlq::HttpDlqConfig>();
        #[cfg(feature = "dlq-redis")]
        assert_schema_is_ascii::<crate::dlq::RedisDlqConfig>();
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
