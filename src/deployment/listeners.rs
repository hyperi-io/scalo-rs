// Project:   scalo
// File:      src/deployment/listeners.rs
// Purpose:   Check each listen address in default_config against the declared ports
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Listener coverage -- the other direction from a port gate.
//!
//! A [`PortCondition`](super::PortCondition) stops the artefacts publishing a
//! port nothing binds. This checks the reverse: a listener the app binds that
//! no port declares, or declares with a different number.

use std::collections::BTreeMap;

use serde_json::Value;

use super::contract::{DeploymentContract, value_at};
use super::error::ContractMismatch;

/// The `default_config` path of the metrics listener, which `metrics_port` serves.
const METRICS_ADDRESS: &str = "metrics.address";

/// A port that says it serves a listen address.
struct Claim {
    /// The contract field making the claim, e.g. `extra_ports[grpc]`.
    by: String,
    port: u16,
}

impl DeploymentContract {
    /// Listen addresses in `default_config` that no port declares or that bind
    /// a different port, and `bound_from` paths that name no listen address.
    ///
    /// A listen address is a leaf named `listen`, `bind_address` or ending in
    /// `_bind_address` whose value is a string or null, plus `metrics.address`
    /// and any path a port's `bound_from` names. A port claims the path its
    /// `bound_from` names, `metrics_port` claims `metrics.address`, and a path
    /// in `unbound_listen_paths` needs no claim. A `host:port` value must
    /// carry the claiming port's number; a null or host-only value is not
    /// compared. Paths are dotted and relative to `default_config`. An empty
    /// result means every listener the app binds is declared.
    #[must_use]
    pub fn undeclared_listeners(&self) -> Vec<ContractMismatch> {
        let mut listeners = BTreeMap::new();
        if let Some(config) = &self.default_config {
            collect_listeners(config, "", &mut listeners);
            if let Some(metrics) = value_at(config, METRICS_ADDRESS)
                && is_listen_value(metrics)
            {
                listeners.insert(METRICS_ADDRESS.to_string(), metrics);
            }
        }

        let mut claims: BTreeMap<&str, Vec<Claim>> = BTreeMap::new();
        claims.entry(METRICS_ADDRESS).or_default().push(Claim {
            by: "metrics_port".to_string(),
            port: self.metrics_port,
        });

        let mut unknown_paths = Vec::new();
        for port in &self.extra_ports {
            let Some(path) = port.bound_from.as_deref() else {
                continue;
            };
            let by = format!("extra_ports[{}]", port.name);
            match self.default_config.as_ref().and_then(|c| value_at(c, path)) {
                Some(value) if is_listen_value(value) || value.is_u64() => {
                    listeners.entry(path.to_string()).or_insert(value);
                    claims.entry(path).or_default().push(Claim {
                        by,
                        port: port.port,
                    });
                }
                found => unknown_paths.push(ContractMismatch {
                    field: format!("{by}.bound_from"),
                    expected: "a listen address in default_config".to_string(),
                    actual: match found {
                        Some(value) => format!("{path} is {value}, not an address"),
                        None => format!("{path} (not in default_config)"),
                    },
                }),
            }
        }

        let mut findings = Vec::new();
        for (path, value) in &listeners {
            let claimed = claims.get(path.as_str()).map_or(&[][..], Vec::as_slice);
            if claimed.is_empty() && !self.unbound_listen_paths.iter().any(|p| p == path) {
                findings.push(ContractMismatch {
                    field: format!("listener {path}"),
                    expected: "a port whose bound_from names it, or an unbound_listen_paths entry"
                        .to_string(),
                    actual: format!("no port declared for {value}"),
                });
            }
            if let Some(bound) = bound_port(value) {
                for claim in claimed.iter().filter(|claim| claim.port != bound) {
                    findings.push(ContractMismatch {
                        field: format!("listener {path}"),
                        expected: format!("port {} ({})", claim.port, claim.by),
                        actual: format!("{value} binds port {bound}"),
                    });
                }
            }
        }
        findings.extend(unknown_paths);
        findings
    }
}

/// Record every listen address under `node`, keyed by its dotted path.
fn collect_listeners<'a>(node: &'a Value, path: &str, out: &mut BTreeMap<String, &'a Value>) {
    let join = |key: &str| {
        if path.is_empty() {
            key.to_string()
        } else {
            format!("{path}.{key}")
        }
    };
    match node {
        Value::Object(map) => {
            for (key, child) in map {
                if is_listen_key(key) && is_listen_value(child) {
                    out.insert(join(key), child);
                } else {
                    collect_listeners(child, &join(key), out);
                }
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_listeners(child, &join(&index.to_string()), out);
            }
        }
        _ => {}
    }
}

/// True for a key naming the address a listener binds.
fn is_listen_key(key: &str) -> bool {
    key == "listen" || key == "bind_address" || key.ends_with("_bind_address")
}

/// True for a value a listen address can hold: an address string, or null for none.
fn is_listen_value(value: &Value) -> bool {
    value.is_string() || value.is_null()
}

/// The port a listen address binds, from `host:port`, `[v6]:port`, a URL or a
/// bare number; `None` for null or a host with no port.
fn bound_port(value: &Value) -> Option<u16> {
    match value {
        Value::String(address) => {
            let authority = address
                .split_once("://")
                .map_or(address.as_str(), |(_, rest)| rest);
            let authority = authority.split_once('/').map_or(authority, |(a, _)| a);
            let (host, port) = authority.rsplit_once(':')?;
            // A colon left in an unbracketed host means the value is a bare IPv6 address.
            if host.contains(':') && !host.ends_with(']') {
                return None;
            }
            port.parse().ok()
        }
        Value::Number(number) => number.as_u64().and_then(|n| u16::try_from(n).ok()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::{
        HealthContract, ImageProfile, NativeDepsContract, OciLabels, PortContract,
    };

    fn contract(default_config: Value, extra_ports: Vec<PortContract>) -> DeploymentContract {
        DeploymentContract {
            schema_version: 3,
            app_name: "app".into(),
            binary_name: String::new(),
            description: String::new(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "APP".into(),
            metric_prefix: "app".into(),
            config_mount_path: "/etc/app/config.yaml".into(),
            image_registry: "ghcr.io/example".into(),
            extra_ports,
            unbound_listen_paths: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: Some(default_config),
            depends_on: vec![],
            keda: None,
            base_image: "debian:trixie-slim".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::Production,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        }
    }

    fn fields(contract: &DeploymentContract) -> Vec<String> {
        contract
            .undeclared_listeners()
            .into_iter()
            .map(|m| m.field)
            .collect()
    }

    /// A push listener bound only on the grpc transport, with a null default
    /// address, must still be declared.
    #[test]
    fn test_a_push_listener_gated_on_a_transport_must_be_declared() {
        let config = serde_json::json!({ "transport": "kafka", "grpc": { "listen": null } });
        let bare = contract(config.clone(), vec![]);
        let findings = bare.undeclared_listeners();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].field, "listener grpc.listen");

        let declared = contract(
            config,
            vec![
                PortContract::tcp("push", 50051)
                    .when_equals("config.transport", "grpc")
                    .bound_from("grpc.listen"),
            ],
        );
        assert!(declared.undeclared_listeners().is_empty());
    }

    #[test]
    fn test_the_metrics_address_must_match_metrics_port() {
        let wrong = contract(
            serde_json::json!({ "metrics": { "address": "0.0.0.0:9091" } }),
            vec![],
        );
        let findings = wrong.undeclared_listeners();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].field, "listener metrics.address");
        assert_eq!(findings[0].expected, "port 9090 (metrics_port)");
        assert!(findings[0].actual.contains("9091"), "{:?}", findings[0]);

        let right = contract(
            serde_json::json!({ "metrics": { "address": "0.0.0.0:9090" } }),
            vec![],
        );
        assert!(right.undeclared_listeners().is_empty());
    }

    #[test]
    fn test_a_declared_port_must_match_the_address_it_serves() {
        let mismatched = contract(
            serde_json::json!({
                "source": { "transport": "bus", "grpc": { "listen": "0.0.0.0:6000" } }
            }),
            vec![
                PortContract::tcp("push", 50051)
                    .when_one_of("config.source.transport", ["direct", "grpc"])
                    .bound_from("source.grpc.listen"),
            ],
        );
        let findings = mismatched.undeclared_listeners();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].field, "listener source.grpc.listen");
        assert_eq!(findings[0].expected, "port 50051 (extra_ports[push])");
        assert!(
            findings[0].actual.contains("binds port 6000"),
            "{:?}",
            findings[0]
        );
    }

    /// Syslog binds 514 on TCP and on UDP from two keys; declaring only the
    /// TCP port leaves the UDP listener undeclared.
    #[test]
    fn test_a_tcp_and_udp_listener_pair_needs_both_ports() {
        let config = serde_json::json!({
            "syslog": {
                "enabled": false,
                "tcp_bind_address": "0.0.0.0:514",
                "udp_bind_address": "0.0.0.0:514",
            }
        });
        let tcp_only = contract(
            config.clone(),
            vec![
                PortContract::tcp("syslog", 514)
                    .when_enabled("config.syslog.enabled")
                    .bound_from("syslog.tcp_bind_address"),
            ],
        );
        assert_eq!(fields(&tcp_only), vec!["listener syslog.udp_bind_address"]);

        let both = contract(
            config,
            vec![
                PortContract::tcp("syslog", 514).bound_from("syslog.tcp_bind_address"),
                PortContract::udp("syslog-udp", 514).bound_from("syslog.udp_bind_address"),
            ],
        );
        assert!(both.undeclared_listeners().is_empty());
    }

    /// One host-only address serves three UDP ports, so no port number is compared.
    #[test]
    fn test_one_host_only_address_can_serve_several_ports() {
        let flow = contract(
            serde_json::json!({ "flow": { "enabled": false, "bind_address": "0.0.0.0" } }),
            vec![
                PortContract::udp("netflow", 2055).bound_from("flow.bind_address"),
                PortContract::udp("ipfix", 4739).bound_from("flow.bind_address"),
                PortContract::udp("sflow", 6343).bound_from("flow.bind_address"),
            ],
        );
        assert!(flow.undeclared_listeners().is_empty());
    }

    #[test]
    fn test_a_send_only_address_can_be_waived() {
        let mut sender = contract(
            serde_json::json!({ "sink": { "bind_address": "0.0.0.0" } }),
            vec![],
        );
        assert_eq!(fields(&sender), vec!["listener sink.bind_address"]);
        sender.unbound_listen_paths = vec!["sink.bind_address".into()];
        assert!(sender.undeclared_listeners().is_empty());
    }

    #[test]
    fn test_bound_from_must_name_a_listen_address() {
        let missing = contract(
            serde_json::json!({ "grpc": { "enabled": true, "tls": {} } }),
            vec![
                PortContract::tcp("push", 6000).bound_from("grpc.listen"),
                PortContract::tcp("tls", 6001).bound_from("grpc.tls"),
            ],
        );
        let findings = missing.undeclared_listeners();
        assert_eq!(
            findings
                .iter()
                .map(|m| m.field.as_str())
                .collect::<Vec<_>>(),
            vec![
                "extra_ports[push].bound_from",
                "extra_ports[tls].bound_from"
            ]
        );
        assert_eq!(findings[0].actual, "grpc.listen (not in default_config)");
    }

    /// A key named nothing like a listener still counts once a port names it,
    /// and a list entry is addressed by its index.
    #[test]
    fn test_bound_from_can_name_any_address_key() {
        let custom = contract(
            serde_json::json!({
                "admin": { "port": 7000 },
                "servers": [ { "listen": "0.0.0.0:7001" } ],
            }),
            vec![
                PortContract::tcp("admin", 7000).bound_from("admin.port"),
                PortContract::tcp("server", 7002).bound_from("servers.0.listen"),
            ],
        );
        let findings = custom.undeclared_listeners();
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].field, "listener servers.0.listen");
        assert_eq!(findings[0].expected, "port 7002 (extra_ports[server])");
    }

    #[test]
    fn test_no_default_config_means_no_listeners() {
        let mut none = contract(Value::Null, vec![]);
        none.default_config = None;
        assert!(none.undeclared_listeners().is_empty());
    }

    #[test]
    fn test_bound_port_reads_each_address_form() {
        let port = |v: Value| bound_port(&v);
        assert_eq!(port(serde_json::json!("0.0.0.0:6000")), Some(6000));
        assert_eq!(port(serde_json::json!(":6000")), Some(6000));
        assert_eq!(port(serde_json::json!("[::]:6000")), Some(6000));
        assert_eq!(
            port(serde_json::json!("http://0.0.0.0:4317/v1")),
            Some(4317)
        );
        assert_eq!(port(serde_json::json!(6000)), Some(6000));
        assert_eq!(port(serde_json::json!("0.0.0.0")), None);
        assert_eq!(port(serde_json::json!("::1")), None);
        assert_eq!(port(serde_json::json!("0.0.0.0:http")), None);
        assert_eq!(port(serde_json::json!(70000)), None);
        assert_eq!(port(Value::Null), None);
    }
}
