// Project:   scalo
// File:      src/deployment/generate/manifest.rs
// Purpose:   Container manifest (CI-consumable JSON) generation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use crate::deployment::contract::{DeploymentContract, ImageProfile, PortContract};

use super::common::udp_port_suffix;

// ============================================================================
// Container Manifest (CI-consumable JSON)
// ============================================================================

/// Generate a container manifest JSON for CI consumption.
///
/// This is the minimal subset of the deployment contract that CI needs to
/// build the container image. No secrets, no K8s-specific config.
///
/// # Errors
///
/// Returns an error string naming the field at fault if the contract fails
/// [`DeploymentContract::validate`], or if JSON serialisation fails.
pub fn generate_container_manifest(contract: &DeploymentContract) -> Result<String, String> {
    contract.validate().map_err(|e| e.to_string())?;
    let binary = contract.binary();

    let apt_repos: Vec<serde_json::Value> = contract
        .native_deps
        .apt_repos
        .iter()
        .map(|r| {
            serde_json::json!({
                "key_url": r.key_url,
                "keyring": r.keyring,
                "url": r.url,
                "codename": r.codename,
                "packages": r.packages,
            })
        })
        .collect();

    // A port with a `when` condition is not exposed, because an image cannot
    // know whether that listener is on; it is listed with its condition instead.
    let mut expose_ports = vec![serde_json::Value::from(contract.metrics_port)];
    let mut conditional_ports = Vec::new();
    for p in &contract.extra_ports {
        match &p.when {
            None => expose_ports.push(expose_entry(p)),
            Some(when) => conditional_ports.push(serde_json::json!({
                "name": p.name,
                "port": p.port,
                "protocol": p.protocol,
                "when": when,
            })),
        }
    }

    let profile_str = match contract.image_profile {
        ImageProfile::Production => "production",
        ImageProfile::Development => "development",
    };

    let title = if contract.oci_labels.title.is_empty() {
        &contract.app_name
    } else {
        &contract.oci_labels.title
    };

    let mut manifest = serde_json::json!({
        "schema_version": "1",
        "app_name": contract.app_name,
        "binary_name": binary,
        "base_image": contract.base_image,
        "image_registry": contract.image_registry,
        "image_profile": profile_str,
        "runtime_packages": {
            "apt_repos": apt_repos,
            "apt_packages": contract.native_deps.apt_packages,
            // Which release those package names are valid for, and whether it
            // had to be assumed. A consumer that composes an image from this
            // manifest rather than from the generated Dockerfile would
            // otherwise get the assumed names with no signal at all -- the
            // Dockerfile's warning comment does not reach it.
            "distro": contract.native_deps.distro.map(|d| d.as_str()),
            "unresolved_base_image": contract.native_deps.unresolved_base_image,
        },
        "expose_ports": expose_ports,
        "healthcheck": {
            "path": contract.health.liveness_path,
            "port": contract.metrics_port,
            "interval": "30s",
            "timeout": "3s",
            "start_period": "5s",
            "retries": 3,
        },
        "entrypoint": [binary],
        "cmd": contract.entrypoint_args,
        "user": "appuser",
        "uid": 1000,
        "labels": {
            "io.scalo.profile": profile_str,
            "io.scalo.app": contract.app_name,
            "io.scalo.metrics_port": contract.metrics_port.to_string(),
            "org.opencontainers.image.title": title,
            "org.opencontainers.image.description": contract.oci_labels.description,
            "org.opencontainers.image.vendor": contract.oci_labels.vendor,
            "org.opencontainers.image.licenses": contract.oci_labels.licenses,
        },
    });
    // Only present when a port is gated, so an ungated contract's manifest is unchanged.
    if !conditional_ports.is_empty()
        && let Some(fields) = manifest.as_object_mut()
    {
        fields.insert(
            "conditional_ports".to_string(),
            serde_json::Value::Array(conditional_ports),
        );
    }

    serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("container manifest JSON failed: {e}"))
}

/// A port as `expose_ports` lists it: a bare number for TCP and `<port>/udp`
/// for UDP, the form the Dockerfile `EXPOSE` line uses, since a consumer reads
/// a bare number as TCP.
fn expose_entry(port: &PortContract) -> serde_json::Value {
    match udp_port_suffix(&port.protocol) {
        "" => port.port.into(),
        suffix => format!("{}{suffix}", port.port).into(),
    }
}
