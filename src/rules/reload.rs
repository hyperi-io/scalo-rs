// Project:   scalo
// File:      src/rules/reload.rs
// Purpose:   Reload a rule set from its section of the config file
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Rules live in a section of the app's config file and reload through the
//! config [`ConfigReloader`]: on each change the section is read and compiled,
//! and the new set replaces the old only when it compiles. A file that fails to
//! read, parse or compile leaves the last good set in place and counts in
//! `config_reloads_total{result="error"}`.
//!
//! Declare the section in the deployment contract's `hot_paths`, so the
//! scalo-service chart leaves it out of the pod's config checksum and an edit
//! reaches the running pod through the mounted file instead of a rollout.

use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;

use super::config::RuleSetConfig;
use super::error::RulesError;
use super::{CompileOptions, RuleSet};
use crate::config::reloader::{ConfigReloader, ReloaderConfig};
use crate::config::shared::SharedConfig;

impl RuleSet {
    /// Compile the rule set at dotted `key` of the YAML file at `path`.
    ///
    /// # Errors
    ///
    /// [`RulesError::Load`] when the file cannot be read or parsed, or holds
    /// nothing at `key`. [`RulesError::Invalid`] when the section does not compile.
    pub fn from_file_section(
        path: impl AsRef<Path>,
        key: &str,
        options: &CompileOptions,
    ) -> Result<Self, RulesError> {
        from_file_section(path.as_ref(), key, options)
    }
}

fn from_file_section(
    path: &Path,
    key: &str,
    options: &CompileOptions,
) -> Result<RuleSet, RulesError> {
    let origin = format!("{} at {key}", path.display());
    let load = |message: String| RulesError::Load {
        origin: origin.clone(),
        message,
    };
    let document: serde_json::Value = crate::yaml::from_path(path, crate::yaml::Profile::Config)
        .map_err(|e| load(e.to_string()))?;
    let pointer = format!("/{}", key.replace('.', "/"));
    let section = document
        .pointer(&pointer)
        .ok_or_else(|| load("the file has no such section".to_string()))?;
    let config = RuleSetConfig::deserialize(section).map_err(|e| load(e.to_string()))?;
    RuleSet::compile_with(&config, options)
}

/// A reloader that recompiles the set at dotted `key` of
/// `config.config_path` on every change and publishes it to `shared`.
///
/// The set in `shared` stays until a new one compiles. Warnings a reload
/// compiles with are logged.
///
/// # Errors
///
/// [`RulesError::Load`] when `config` watches no file.
pub fn reloader(
    config: ReloaderConfig,
    shared: SharedConfig<Arc<RuleSet>>,
    key: impl Into<String>,
    options: CompileOptions,
) -> Result<ConfigReloader<Arc<RuleSet>>, RulesError> {
    let key = key.into();
    let Some(path) = config.config_path.clone() else {
        return Err(RulesError::Load {
            origin: format!("section {key}"),
            message: "the reloader config names no file to watch".to_string(),
        });
    };
    Ok(ConfigReloader::new(
        config,
        shared,
        move || {
            let set = from_file_section(&path, &key, &options)?;
            for warning in set.warnings() {
                tracing::warn!(rule_set = set.name(), %warning, "rule set reloaded with a warning");
            }
            Ok(Arc::new(set))
        },
        |_| Ok(()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &tempfile::TempDir, text: &str) -> std::path::PathBuf {
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, text).unwrap();
        path
    }

    const GOOD: &str = r"
log_level: info
routing:
  rules:
    name: ingest
    targets: [_source]
    rules:
      - id: web
        match: { field: host, op: starts_with, value: web- }
        set: { _source: { value: web } }
";

    #[test]
    fn a_section_of_the_file_compiles() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, GOOD);
        let set =
            RuleSet::from_file_section(&path, "routing.rules", &CompileOptions::default()).unwrap();
        assert_eq!(set.name(), "ingest");
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn a_missing_section_or_file_is_a_load_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, GOOD);
        let options = CompileOptions::default();
        assert!(matches!(
            RuleSet::from_file_section(&path, "routing.absent", &options),
            Err(RulesError::Load { .. })
        ));
        assert!(matches!(
            RuleSet::from_file_section(dir.path().join("none.yaml"), "routing.rules", &options),
            Err(RulesError::Load { .. })
        ));
    }

    #[test]
    fn an_invalid_section_reports_its_findings() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, &GOOD.replace("starts_with", "regex"));
        let err = RuleSet::from_file_section(&path, "routing.rules", &CompileOptions::default())
            .unwrap_err();
        assert_eq!(err.diagnostics()[0].location, "rules[0].match.op");
    }

    #[test]
    fn a_reloader_needs_a_file() {
        let shared = SharedConfig::new(Arc::new(
            RuleSet::compile_with(&RuleSetConfig::default(), &CompileOptions::default()).unwrap(),
        ));
        let result = reloader(
            ReloaderConfig::default(),
            shared,
            "rules",
            CompileOptions::default(),
        );
        assert!(matches!(result, Err(RulesError::Load { .. })));
    }
}
