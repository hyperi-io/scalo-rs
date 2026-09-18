// Project:   scalo
// File:      src/deployment/checks.rs
// Purpose:   The contract check every generator that can refuse runs first
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The one check a contract passes before an artefact is generated from it.

use super::contract::{DeploymentContract, PortCondition, PortContract};
use super::error::DeploymentError;

/// The protocols Kubernetes takes on a container or Service port.
const KUBERNETES_PROTOCOLS: [&str; 3] = ["TCP", "UDP", "SCTP"];

/// The longest port name Kubernetes takes (an RFC 6335 `IANA_SVC_NAME`).
const MAX_PORT_NAME_LEN: usize = 15;

impl DeploymentContract {
    /// Check that every artefact generated from this contract comes out valid.
    ///
    /// [`generate_chart`](super::generate_chart),
    /// [`generate_container_manifest`](super::generate_container_manifest),
    /// [`check_chart_drift`](super::check_chart_drift) and the
    /// `generate-artefacts` subcommand run it before writing anything, and
    /// [`validate_helm_values`](super::validate_helm_values) reports it.
    ///
    /// Each extra port needs a name Kubernetes takes -- 1 to 15 lowercase
    /// letters, digits and single inner hyphens, with at least one letter --
    /// and a protocol of TCP, UDP or SCTP in any case. No `when` path or value
    /// and no `bound_from` may hold a control character, because the generators
    /// print them onto one line and a newline there starts an instruction or a
    /// key of its own. KEDA left on needs a trigger to scale on, and a
    /// ScaledObject whose only trigger is CPU needs `min_replicas` of at least
    /// one, because KEDA's CPU scaler cannot wake a workload from zero.
    ///
    /// # Errors
    ///
    /// [`DeploymentError::InvalidContract`] naming the first field at fault.
    pub fn validate(&self) -> Result<(), DeploymentError> {
        for (index, port) in self.extra_ports.iter().enumerate() {
            check_port(index, port)?;
        }
        self.check_keda()
    }

    /// The KEDA half of [`validate`](Self::validate).
    fn check_keda(&self) -> Result<(), DeploymentError> {
        let Some(keda) = self.enabled_keda() else {
            return Ok(());
        };
        if keda.kafka_trigger.enabled {
            return Ok(());
        }
        if !keda.cpu_enabled {
            return Err(invalid(
                "keda".to_string(),
                "the Kafka lag trigger and the CPU trigger are both off, so KEDA would have \
                 nothing to scale on; set `keda: None` to turn autoscaling off"
                    .to_string(),
            ));
        }
        if keda.min_replicas == 0 {
            return Err(invalid(
                "keda.min_replicas".to_string(),
                "CPU is the only trigger, and KEDA's CPU scaler cannot wake a workload from \
                 zero; set min_replicas to 1 or more, or keep the Kafka lag trigger"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Check one extra port, naming it by index until its name is known to be printable.
fn check_port(index: usize, port: &PortContract) -> Result<(), DeploymentError> {
    if let Some(fault) = port_name_fault(&port.name) {
        return Err(invalid(
            format!("extra_ports[{index}].name"),
            format!(
                "{:?} {fault}, and Kubernetes takes a port name of 1 to {MAX_PORT_NAME_LEN} \
                 lowercase letters, digits and single inner hyphens, with at least one letter",
                port.name
            ),
        ));
    }
    let field = |part: &str| format!("extra_ports[{}].{part}", port.name);
    if !KUBERNETES_PROTOCOLS
        .iter()
        .any(|known| port.protocol.eq_ignore_ascii_case(known))
    {
        return Err(invalid(
            field("protocol"),
            format!(
                "{:?} is not a protocol Kubernetes takes -- use TCP, UDP or SCTP",
                port.protocol
            ),
        ));
    }
    if let Some(text) = port
        .when
        .as_ref()
        .and_then(|when| condition_texts(when).find(|text| has_control(text)))
    {
        return Err(invalid(field("when"), holds_a_control_character(text)));
    }
    if let Some(path) = port.bound_from.as_deref().filter(|path| has_control(path)) {
        return Err(invalid(
            field("bound_from"),
            holds_a_control_character(path),
        ));
    }
    Ok(())
}

/// Why `name` is not a port name Kubernetes takes, or `None` when it is one.
fn port_name_fault(name: &str) -> Option<&'static str> {
    if name.is_empty() || name.len() > MAX_PORT_NAME_LEN {
        return Some("is not 1 to 15 characters long");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Some("holds a character other than a lowercase letter, a digit or '-'");
    }
    if !name.bytes().any(|b| b.is_ascii_lowercase()) {
        return Some("has no letter");
    }
    if name.starts_with('-') || name.ends_with('-') || name.contains("--") {
        return Some("starts or ends with '-', or has two in a row");
    }
    None
}

/// Every string a port condition carries: its path, then its value or values.
fn condition_texts(condition: &PortCondition) -> impl Iterator<Item = &str> {
    let values: &[String] = match condition {
        PortCondition::Enabled { .. } => &[],
        PortCondition::Equals { value, .. } => std::slice::from_ref(value),
        PortCondition::OneOf { values, .. } => values,
    };
    std::iter::once(condition.path()).chain(values.iter().map(String::as_str))
}

fn has_control(text: &str) -> bool {
    text.chars().any(char::is_control)
}

fn holds_a_control_character(text: &str) -> String {
    format!(
        "{text:?} holds a control character, which would break the line a generator prints \
         it on"
    )
}

fn invalid(field: String, reason: String) -> DeploymentError {
    DeploymentError::InvalidContract { field, reason }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_port_name_is_checked_as_kubernetes_checks_it() {
        for good in [
            "http",
            "grpc",
            "syslog-udp",
            "a",
            "h2c",
            "web-api-2",
            "abcdefghijklmno",
        ] {
            assert_eq!(port_name_fault(good), None, "{good:?}");
        }
        for bad in [
            "",
            "abcdefghijklmnop",
            "Web",
            "web_api",
            "web.api",
            "8080",
            "-web",
            "web-",
            "we--b",
            "web\n",
            "web api",
        ] {
            assert!(port_name_fault(bad).is_some(), "{bad:?} passed");
        }
    }

    #[test]
    fn every_string_a_condition_carries_is_checked() {
        fn texts(condition: &PortCondition) -> Vec<&str> {
            condition_texts(condition).collect()
        }
        assert_eq!(
            texts(&PortCondition::Enabled {
                path: "config.a".into()
            }),
            ["config.a"]
        );
        assert_eq!(
            texts(&PortCondition::Equals {
                path: "config.a".into(),
                value: "on".into()
            }),
            ["config.a", "on"]
        );
        assert_eq!(
            texts(&PortCondition::OneOf {
                path: "config.a".into(),
                values: vec!["x".into(), "y".into()]
            }),
            ["config.a", "x", "y"]
        );
    }
}
