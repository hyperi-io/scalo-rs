// Project:   scalo
// File:      src/deployment/validate.rs
// Purpose:   Validate a Dockerfile against the deployment contract
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Validate deployment artifacts against the app contract.
//!
//! [`validate_dockerfile`] checks `Dockerfile` for port, healthcheck, and config path.

use std::path::Path;

use super::contract::DeploymentContract;
use super::error::{ContractMismatch, DeploymentError};

/// Validate a Dockerfile against the deployment contract.
///
/// Checks EXPOSE port, HEALTHCHECK path, and config mount path.
///
/// Returns a list of mismatches (empty = all good).
///
/// # Errors
///
/// Returns `DeploymentError` if the Dockerfile cannot be read.
pub fn validate_dockerfile(
    contract: &DeploymentContract,
    dockerfile_path: impl AsRef<Path>,
) -> Result<Vec<ContractMismatch>, DeploymentError> {
    let dockerfile_path = dockerfile_path.as_ref();
    let content = read_text(dockerfile_path)?;
    let mut mismatches = Vec::new();

    // EXPOSE port
    if !exposes_tcp_port(&content, contract.metrics_port) {
        mismatches.push(ContractMismatch {
            field: "Dockerfile EXPOSE".into(),
            expected: format!("EXPOSE {}", contract.metrics_port),
            actual: extract_line_containing(&content, "EXPOSE"),
        });
    }

    // HEALTHCHECK path
    if !content.contains(&contract.health.liveness_path) {
        mismatches.push(ContractMismatch {
            field: "Dockerfile HEALTHCHECK path".into(),
            expected: contract.health.liveness_path.clone(),
            actual: extract_line_containing(&content, "HEALTHCHECK"),
        });
    }

    // HEALTHCHECK port: an HTTP client's URL, or the bash `/dev/tcp` form the
    // generator writes.
    let port_str = format!("localhost:{}", contract.metrics_port);
    let tcp_str = format!("/dev/tcp/localhost/{}", contract.metrics_port);
    if !content.contains(&port_str) && !content.contains(&tcp_str) {
        mismatches.push(ContractMismatch {
            field: "Dockerfile HEALTHCHECK port".into(),
            expected: port_str,
            actual: extract_line_containing(&content, "HEALTHCHECK"),
        });
    }

    // Config mount path
    if !content.contains(&contract.config_mount_path) {
        mismatches.push(ContractMismatch {
            field: "Dockerfile config path".into(),
            expected: contract.config_mount_path.clone(),
            actual: extract_line_containing(&content, "CMD"),
        });
    }

    Ok(mismatches)
}

// ============================================================================
// Internal helpers
// ============================================================================

fn read_text(path: &Path) -> Result<String, DeploymentError> {
    if !path.exists() {
        return Err(DeploymentError::NotFound(path.display().to_string()));
    }
    std::fs::read_to_string(path).map_err(|e| DeploymentError::ReadFile {
        path: path.display().to_string(),
        source: e,
    })
}

/// True when an `EXPOSE` instruction lists `port` as a whole token, bare or as
/// `<port>/tcp`, so `EXPOSE 90901` does not pass for 9090.
fn exposes_tcp_port(dockerfile: &str, port: u16) -> bool {
    let bare = port.to_string();
    let tcp = format!("{port}/tcp");
    dockerfile.lines().any(|line| {
        let mut tokens = line.split_whitespace();
        tokens
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("EXPOSE"))
            && tokens.any(|token| token == bare || token.eq_ignore_ascii_case(&tcp))
    })
}

/// The first non-comment line carrying `keyword`, so a lint pragma or note
/// above an instruction is never reported in its place.
fn extract_line_containing(content: &str, keyword: &str) -> String {
    content
        .lines()
        .find(|line| !line.trim_start().starts_with('#') && line.contains(keyword))
        .unwrap_or("(not found)")
        .trim()
        .to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::keda::KedaContract;

    fn test_contract() -> DeploymentContract {
        DeploymentContract {
            app_name: "test-app".into(),
            binary_name: "test-app".into(),
            description: "Test application".into(),
            metrics_port: 9090,
            health: super::super::HealthContract::default(),
            env_prefix: "TEST_APP".into(),
            metric_prefix: "test".into(),
            config_mount_path: "/etc/test/config.yaml".into(),
            image_registry: "ghcr.io/hyperi-io".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
            entrypoint_args: vec!["--config".into(), "/etc/test/config.yaml".into()],
            secrets: vec![],
            default_config: Some(serde_json::json!({
                "kafka": { "brokers": ["kafka:9092"], "group_id": "test-app", "topics": ["events"] }
            })),
            depends_on: vec![],
            keda: Some(KedaContract::default()),
            base_image: "ubuntu:24.04".into(),
            native_deps: super::super::NativeDepsContract::default(),
            image_profile: super::super::ImageProfile::default(),
            schema_version: 3,
            oci_labels: super::super::OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
            writable_paths: vec![],
            termination_grace_seconds: 45,
            resources: super::super::ResourcesContract::default(),
            security: super::super::SecurityContract::default(),
            singleton: false,
            service_account: super::super::ServiceAccount::Own,
            hot_paths: vec![],
        }
    }

    #[test]
    fn test_validate_dockerfile_not_found() {
        let contract = test_contract();
        let result = validate_dockerfile(&contract, "/nonexistent/Dockerfile");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_dockerfile_with_tempfile() {
        let dir = tempfile::tempdir().unwrap();
        let dockerfile = dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile,
            "FROM ubuntu:24.04\n\
             EXPOSE 9090\n\
             HEALTHCHECK CMD curl -sf http://localhost:9090/livez\n\
             CMD [\"--config\", \"/etc/test/config.yaml\"]\n",
        )
        .unwrap();

        let contract = test_contract();
        let mismatches = validate_dockerfile(&contract, &dockerfile).unwrap();
        assert!(
            mismatches.is_empty(),
            "Unexpected mismatches: {mismatches:?}"
        );
    }

    /// The generator's own output validates clean, and a mismatch names the
    /// instruction itself rather than the comment lines above it.
    #[test]
    fn test_validate_generated_dockerfile_reports_instructions_not_comments() {
        let dir = tempfile::tempdir().unwrap();
        let dockerfile = dir.path().join("Dockerfile");
        let mut contract = test_contract();
        contract.native_deps = super::super::NativeDepsContract::for_scalo_features(
            &["directory-config-git"],
            &contract.base_image,
        );
        std::fs::write(
            &dockerfile,
            super::super::generate_dockerfile(&contract, None).unwrap(),
        )
        .unwrap();
        let mismatches = validate_dockerfile(&contract, &dockerfile).unwrap();
        assert!(mismatches.is_empty(), "{mismatches:?}");

        contract.metrics_port = 9191;
        let mismatches = validate_dockerfile(&contract, &dockerfile).unwrap();
        let port = mismatches
            .iter()
            .find(|m| m.field == "Dockerfile HEALTHCHECK port")
            .expect("port mismatch reported");
        assert!(port.actual.starts_with("HEALTHCHECK "), "{port:?}");
    }

    #[test]
    fn test_validate_dockerfile_wrong_port() {
        let dir = tempfile::tempdir().unwrap();
        let dockerfile = dir.path().join("Dockerfile");
        std::fs::write(
            &dockerfile,
            "FROM ubuntu:24.04\n\
             EXPOSE 8080\n\
             HEALTHCHECK CMD curl -sf http://localhost:8080/livez\n\
             CMD [\"--config\", \"/etc/test/config.yaml\"]\n",
        )
        .unwrap();

        let contract = test_contract();
        let mismatches = validate_dockerfile(&contract, &dockerfile).unwrap();
        assert!(!mismatches.is_empty());
        assert!(mismatches.iter().any(|m| m.field.contains("EXPOSE")));
    }

    /// The metrics port must be a whole token on an EXPOSE line, bare or `/tcp`,
    /// wherever it sits in the list.
    #[test]
    fn test_validate_dockerfile_matches_the_expose_port_as_a_whole_token() {
        let expose_mismatch = |expose: &str| {
            let dir = tempfile::tempdir().unwrap();
            let dockerfile = dir.path().join("Dockerfile");
            std::fs::write(
                &dockerfile,
                format!(
                    "FROM ubuntu:24.04\n\
                     {expose}\n\
                     HEALTHCHECK CMD curl -sf http://localhost:9090/livez\n\
                     CMD [\"--config\", \"/etc/test/config.yaml\"]\n"
                ),
            )
            .unwrap();
            validate_dockerfile(&test_contract(), &dockerfile)
                .unwrap()
                .iter()
                .any(|m| m.field == "Dockerfile EXPOSE")
        };

        for wrong in [
            "EXPOSE 90901",
            "EXPOSE 19090",
            "EXPOSE 8080 9090/udp",
            "# EXPOSE 9090",
            "LABEL note=\"EXPOSE 9090\"",
        ] {
            assert!(expose_mismatch(wrong), "{wrong:?} passed for 9090");
        }
        for right in [
            "EXPOSE 9090",
            "EXPOSE 8080 9090 514/udp",
            "EXPOSE 9090/tcp",
            "expose 9090",
        ] {
            assert!(!expose_mismatch(right), "{right:?} failed for 9090");
        }
    }

    #[test]
    fn test_contract_mismatch_display() {
        let m = ContractMismatch {
            field: "service.port".into(),
            expected: "9090".into(),
            actual: "8080".into(),
        };
        assert_eq!(m.to_string(), "service.port: expected '9090', got '8080'");
    }
}
