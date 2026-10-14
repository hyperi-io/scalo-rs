// Project:   scalo
// File:      src/config/yaml_file.rs
// Purpose:   figment provider that reads one cascade YAML file through crate::yaml
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The cascade's YAML layer: one file, parsed with [`crate::yaml`]'s config
//! profile so the cascade reads YAML by the same rules as every other parse.

use std::path::PathBuf;

use figment::value::{Dict, Map};
use figment::{Error, Metadata, Profile, Provider};

/// One YAML file merged into the cascade. A missing file contributes nothing.
pub(crate) struct YamlFile {
    path: PathBuf,
}

impl YamlFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Provider for YamlFile {
    fn metadata(&self) -> Metadata {
        Metadata::from("YAML file", self.path.as_path())
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        if !self.path.is_file() {
            return Ok(Map::new());
        }
        // An empty or comment-only file is null, which is an empty layer.
        let dict: Option<Dict> = crate::yaml::from_path(&self.path, crate::yaml::Profile::Config)
            .map_err(|e| Error::from(e.to_string()))?;
        Ok(Profile::Default.collect(dict.unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests {
    use figment::Figment;

    use super::*;

    fn layer(text: &str) -> (tempfile::TempDir, YamlFile) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.yaml");
        std::fs::write(&path, text).unwrap();
        (dir, YamlFile::new(path))
    }

    #[test]
    fn reads_nested_keys_and_types() {
        let (_dir, file) = layer("kafka:\n  brokers: [a, b]\n  port: 9092\n  enabled: true\n");
        let figment = Figment::from(file);
        assert_eq!(
            figment
                .extract_inner::<Vec<String>>("kafka.brokers")
                .unwrap(),
            ["a", "b"]
        );
        assert_eq!(figment.extract_inner::<u16>("kafka.port").unwrap(), 9092);
        assert!(figment.extract_inner::<bool>("kafka.enabled").unwrap());
    }

    /// `yes` is text in the cascade, as it is in every other scalo parse.
    #[test]
    fn yaml_11_booleans_stay_text() {
        let (_dir, file) = layer("flag: yes\n");
        let figment = Figment::from(file);
        assert_eq!(figment.extract_inner::<String>("flag").unwrap(), "yes");
        assert!(figment.extract_inner::<bool>("flag").is_err());
    }

    #[test]
    fn empty_and_missing_files_are_empty_layers() {
        let (_dir, empty) = layer("# nothing set\n");
        assert!(Figment::from(empty).extract::<Dict>().unwrap().is_empty());

        let missing = YamlFile::new(PathBuf::from("/nonexistent/settings.yaml"));
        assert!(Figment::from(missing).extract::<Dict>().unwrap().is_empty());
    }

    /// A malformed file fails the extract with its path and line, not silently.
    #[test]
    fn malformed_file_fails_with_its_location() {
        let (_dir, file) = layer("a: 1\na: 2\n");
        let err = Figment::from(file)
            .extract::<Dict>()
            .unwrap_err()
            .to_string();
        assert!(err.contains("settings.yaml"), "{err}");
        assert!(err.contains("line 2"), "{err}");
    }
}
