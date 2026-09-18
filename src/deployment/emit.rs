// Project:   scalo
// File:      src/deployment/emit.rs
// Purpose:   Emit + drift-check config artefacts; guard committed charts and listeners
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
//!
//! [`check_chart_drift`] guards a committed Helm chart the same way, with a
//! [`ChartPatch`] pinning each hand fix, and [`assert_listeners_declared`]
//! checks the contract's listeners against its ports.

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

/// A hand edit a committed chart file carries on purpose: text the generator
/// writes (`from`) replaced by what the file holds instead (`to`).
///
/// [`check_chart_drift`] applies each patch to the freshly generated file
/// before comparing, and fails once the generator stops writing `from`, so the
/// hand fix is pinned to the content it replaces rather than exempted. `from`
/// must occur exactly once in that file, so one patch pins one edit.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChartPatch {
    /// Path under the chart root, e.g. `templates/keda-scaledobject.yaml`.
    pub file: String,
    /// Text the generator writes that the hand fix replaces.
    pub from: String,
    /// What the committed file holds in its place.
    pub to: String,
}

impl ChartPatch {
    /// A hand fix replacing `from`, which the generator writes once into
    /// `file`, with `to`.
    #[must_use]
    pub fn new(file: impl Into<String>, from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            file: file.into(),
            from: from.into(),
            to: to.into(),
        }
    }
}

/// Check that the committed chart under `chart_dir` is exactly what
/// [`generate_chart`](super::generate_chart) writes for `contract`, with each
/// of `patches` applied.
///
/// Every generated file must be on disk and equal the fresh output after its
/// patches, each patch's `from` must occur exactly once in the file it names,
/// and every file in the chart root or `templates/` must be one the generator
/// writes. The chart is rendered without identity annotations, as a committed
/// chart is.
///
/// # Errors
///
/// Returns [`DeploymentError::Drift`] listing every problem found,
/// [`DeploymentError::InvalidContract`] when the contract cannot generate a
/// chart, or [`DeploymentError::ReadFile`] when a chart file cannot be read.
pub fn check_chart_drift(
    contract: &DeploymentContract,
    chart_dir: &Path,
    patches: &[ChartPatch],
) -> Result<(), DeploymentError> {
    let fresh = super::generate::chart_files(contract, None)?;
    let mut problems = Vec::new();

    for patch in patches {
        if patch.from.is_empty() {
            problems.push(format!(
                "{}: a patch has an empty `from`, which pins nothing",
                patch.file
            ));
        } else if !fresh.iter().any(|(name, _)| *name == patch.file) {
            problems.push(format!(
                "{}: patch no longer applies -- the generator does not write this file; drop it",
                patch.file
            ));
        }
    }

    for (name, generated) in &fresh {
        let mut expected = generated.clone();
        let mut patches_apply = true;
        for patch in patches
            .iter()
            .filter(|p| p.file == *name && !p.from.is_empty())
        {
            match generated.matches(&patch.from).count() {
                1 => expected = expected.replacen(&patch.from, &patch.to, 1),
                0 => {
                    patches_apply = false;
                    problems.push(format!(
                        "{name}: patch no longer applies -- drop it (the generator no longer \
                         writes {:?})",
                        patch.from.lines().next().unwrap_or_default()
                    ));
                }
                count => {
                    patches_apply = false;
                    problems.push(format!(
                        "{name}: a patch's `from` occurs {count} times in the generated file, \
                         so it pins no one edit -- lengthen it until it is unique ({:?})",
                        patch.from.lines().next().unwrap_or_default()
                    ));
                }
            }
        }
        let path = chart_dir.join(name);
        match std::fs::read_to_string(&path) {
            // A patch that does not apply is the problem to report for this file.
            Ok(_) if !patches_apply => {}
            Ok(committed) if committed == expected => {}
            Ok(committed) => problems.push(format!(
                "{name}: {}",
                describe_difference(&expected, &committed)
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                problems.push(format!("{name}: generated but missing from the chart"));
            }
            Err(e) => {
                return Err(DeploymentError::ReadFile {
                    path: path.display().to_string(),
                    source: e,
                });
            }
        }
    }

    for name in files_in_chart(chart_dir)? {
        if !fresh.iter().any(|(generated, _)| *generated == name) {
            problems.push(format!(
                "{name}: in the chart, but the generator does not write it"
            ));
        }
    }

    if problems.is_empty() {
        return Ok(());
    }
    Err(DeploymentError::Drift {
        path: chart_dir.display().to_string(),
        detail: format!(
            "{}\nRegenerate the chart from the contract, and keep a hand edit only as a \
             ChartPatch.",
            problems.join("\n")
        ),
    })
}

/// Panic if the committed chart under `chart_dir` drifts from the generator's
/// output with `patches` applied. Wraps [`check_chart_drift`] for use directly
/// in a `#[test]`.
///
/// # Panics
///
/// Panics listing every problem when the chart drifted, or when the check
/// itself cannot run.
pub fn assert_no_chart_drift(
    contract: &DeploymentContract,
    chart_dir: &Path,
    patches: &[ChartPatch],
) {
    if let Err(e) = check_chart_drift(contract, chart_dir, patches) {
        panic!("{e}");
    }
}

/// How a committed file differs from the expected content: in its line endings
/// alone, in its trailing newlines alone, or from a given line.
///
/// The first two are named outright because a line-by-line comparison reads
/// past both and would point at a line beyond the end of the file.
fn describe_difference(expected: &str, committed: &str) -> String {
    if committed.contains('\r') && committed.replace("\r\n", "\n") == expected {
        return "differs only in line endings -- the committed file has CRLF where the \
                generator writes LF"
            .to_string();
    }
    let trailing = |text: &str| text.len() - text.trim_end_matches('\n').len();
    if expected.trim_end_matches('\n') == committed.trim_end_matches('\n') {
        return format!(
            "differs only in its trailing newlines -- the generator writes {}, the committed \
             file has {}",
            trailing(expected),
            trailing(committed)
        );
    }
    format!(
        "differs from the generated chart at line {}",
        first_differing_line(expected, committed)
    )
}

/// The 1-based line where `a` and `b` first differ.
fn first_differing_line(a: &str, b: &str) -> usize {
    let (mut a_lines, mut b_lines) = (a.lines(), b.lines());
    let mut line = 1;
    loop {
        match (a_lines.next(), b_lines.next()) {
            (Some(x), Some(y)) if x == y => line += 1,
            _ => return line,
        }
    }
}

/// Paths, relative to the chart root, of the files in the two directories the
/// generator writes to: the root and `templates/`.
fn files_in_chart(chart_dir: &Path) -> Result<Vec<String>, DeploymentError> {
    let mut found = Vec::new();
    for sub in ["", "templates"] {
        let dir = chart_dir.join(sub);
        let read_error = |e: std::io::Error| DeploymentError::ReadFile {
            path: dir.display().to_string(),
            source: e,
        };
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(read_error(e)),
        };
        for entry in entries {
            let entry = entry.map_err(read_error)?;
            if entry.path().is_file() {
                let file = entry.file_name().to_string_lossy().into_owned();
                found.push(if sub.is_empty() {
                    file
                } else {
                    format!("{sub}/{file}")
                });
            }
        }
    }
    found.sort();
    Ok(found)
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
            image_registry: "ghcr.io/hyperi-io".into(),
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

    /// A chart generated into a temp dir, standing in for a committed one.
    fn committed_chart() -> (DeploymentContract, tempfile::TempDir) {
        let contract = contract_with_catalog();
        let dir = tempfile::tempdir().unwrap();
        crate::deployment::generate_chart(&contract, dir.path(), None).unwrap();
        (contract, dir)
    }

    /// Replace `from` with `to` in a committed chart file, as a hand fix would.
    fn hand_edit(dir: &Path, file: &str, from: &str, to: &str) {
        let path = dir.join(file);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains(from), "{file} lacks {from:?}");
        std::fs::write(&path, text.replace(from, to)).unwrap();
    }

    fn drift_detail(result: Result<(), DeploymentError>) -> String {
        match result {
            Err(DeploymentError::Drift { detail, .. }) => detail,
            other => panic!("expected drift, got {other:?}"),
        }
    }

    const REPLICAS: &str = "  replicas: {{ .Values.replicaCount }}\n";

    fn pin_replicas() -> ChartPatch {
        ChartPatch {
            file: "templates/deployment.yaml".into(),
            from: REPLICAS.into(),
            to: "  replicas: 2\n".into(),
        }
    }

    #[test]
    fn chart_drift_passes_on_a_fresh_chart() {
        let (contract, dir) = committed_chart();
        check_chart_drift(&contract, dir.path(), &[]).unwrap();
        assert_no_chart_drift(&contract, dir.path(), &[]);
    }

    #[test]
    fn chart_drift_passes_a_hand_fix_its_patch_pins() {
        let (contract, dir) = committed_chart();
        hand_edit(
            dir.path(),
            "templates/deployment.yaml",
            REPLICAS,
            "  replicas: 2\n",
        );

        check_chart_drift(&contract, dir.path(), &[pin_replicas()]).unwrap();
        // Without the patch the same hand fix is drift.
        let detail = drift_detail(check_chart_drift(&contract, dir.path(), &[]));
        assert!(
            detail
                .contains("templates/deployment.yaml: differs from the generated chart at line 9"),
            "{detail}"
        );
    }

    /// A patch pins one edit; any other edit to the same file, or to another
    /// file, is still drift.
    #[test]
    fn chart_drift_catches_an_edit_outside_the_patch() {
        let (contract, dir) = committed_chart();
        hand_edit(
            dir.path(),
            "templates/deployment.yaml",
            REPLICAS,
            "  replicas: 2\n",
        );
        hand_edit(
            dir.path(),
            "templates/deployment.yaml",
            "periodSeconds: 10",
            "periodSeconds: 20",
        );
        hand_edit(
            dir.path(),
            "values.yaml",
            "replicaCount: 1",
            "replicaCount: 4",
        );

        let detail = drift_detail(check_chart_drift(&contract, dir.path(), &[pin_replicas()]));
        assert!(
            detail.contains("templates/deployment.yaml: differs from the generated chart"),
            "{detail}"
        );
        assert!(
            detail.contains("values.yaml: differs from the generated chart"),
            "{detail}"
        );
    }

    #[test]
    fn chart_drift_fails_a_patch_the_generator_has_moved_past() {
        let (contract, dir) = committed_chart();
        let stale = ChartPatch {
            file: "templates/deployment.yaml".into(),
            from: "  replicas: 3\n".into(),
            to: "  replicas: 2\n".into(),
        };
        let detail = drift_detail(check_chart_drift(&contract, dir.path(), &[stale]));
        assert!(
            detail.contains("templates/deployment.yaml: patch no longer applies -- drop it"),
            "{detail}"
        );

        for (file, from) in [("templates/pdb.yaml", "x"), ("values.yaml", "")] {
            let patch = ChartPatch {
                file: file.into(),
                from: from.into(),
                to: "y".into(),
            };
            let detail = drift_detail(check_chart_drift(&contract, dir.path(), &[patch]));
            assert!(detail.starts_with(&format!("{file}: ")), "{detail}");
        }
    }

    /// A `from` that occurs more than once pins no one edit, so it is refused
    /// with the count rather than applied everywhere it matches.
    #[test]
    fn chart_drift_refuses_a_patch_whose_from_is_not_unique() {
        let (contract, dir) = committed_chart();
        let generated =
            std::fs::read_to_string(dir.path().join("templates/deployment.yaml")).unwrap();
        let from = "{{- end }}\n";
        let count = generated.matches(from).count();
        assert!(count > 1, "the fixture needs a repeated line");

        let patch = ChartPatch {
            file: "templates/deployment.yaml".into(),
            from: from.into(),
            to: "{{- end -}}\n".into(),
        };
        hand_edit(
            dir.path(),
            "templates/deployment.yaml",
            from,
            "{{- end -}}\n",
        );

        let detail = drift_detail(check_chart_drift(&contract, dir.path(), &[patch]));
        assert!(
            detail.contains(&format!(
                "templates/deployment.yaml: a patch's `from` occurs {count} times"
            )),
            "{detail}"
        );
    }

    /// A file that differs only in its line endings or trailing newlines says
    /// so, rather than naming a line past the end of the file.
    #[test]
    fn chart_drift_names_a_line_ending_or_trailing_newline_difference() {
        let (contract, dir) = committed_chart();
        let values = dir.path().join("values.yaml");
        let text = std::fs::read_to_string(&values).unwrap();
        std::fs::write(&values, text.replace('\n', "\r\n")).unwrap();
        let chart = dir.path().join("Chart.yaml");
        let text = std::fs::read_to_string(&chart).unwrap();
        std::fs::write(&chart, format!("{text}\n")).unwrap();

        let detail = drift_detail(check_chart_drift(&contract, dir.path(), &[]));
        assert!(
            detail.contains("values.yaml: differs only in line endings"),
            "{detail}"
        );
        assert!(
            detail.contains("Chart.yaml: differs only in its trailing newlines"),
            "{detail}"
        );
    }

    /// A chart file that cannot be read for any reason but absence is an error
    /// of its own, not a drift finding.
    #[test]
    fn chart_drift_hands_back_a_read_error_other_than_absence() {
        let (contract, dir) = committed_chart();
        let path = dir.path().join("templates/deployment.yaml");
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        let err = check_chart_drift(&contract, dir.path(), &[]).unwrap_err();
        assert!(
            matches!(err, DeploymentError::ReadFile { ref path, .. } if path.ends_with("templates/deployment.yaml")),
            "{err:?}"
        );
    }

    #[test]
    fn chart_drift_fails_on_a_missing_or_extra_file() {
        let (contract, dir) = committed_chart();
        std::fs::remove_file(dir.path().join("templates/hpa.yaml")).unwrap();
        std::fs::write(
            dir.path().join("templates/keda-scaledobject.yaml"),
            "stale\n",
        )
        .unwrap();

        let detail = drift_detail(check_chart_drift(&contract, dir.path(), &[]));
        assert!(
            detail.contains("templates/hpa.yaml: generated but missing from the chart"),
            "{detail}"
        );
        assert!(
            detail.contains(
                "templates/keda-scaledobject.yaml: in the chart, but the generator does not write it"
            ),
            "{detail}"
        );
    }

    #[test]
    #[should_panic(expected = "values.yaml: differs from the generated chart")]
    fn assert_no_chart_drift_panics_on_drift() {
        let (contract, dir) = committed_chart();
        hand_edit(
            dir.path(),
            "values.yaml",
            "replicaCount: 1",
            "replicaCount: 4",
        );
        assert_no_chart_drift(&contract, dir.path(), &[]);
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
