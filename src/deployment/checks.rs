// Project:   scalo
// File:      src/deployment/checks.rs
// Purpose:   The contract check every generator that can refuse runs first
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The one check a contract passes before an artefact is generated from it.

use std::collections::BTreeSet;

use super::contract::{DeploymentContract, PortCondition, PortContract, WritablePath};
use super::error::DeploymentError;

/// The protocols Kubernetes takes on a container or Service port.
const KUBERNETES_PROTOCOLS: [&str; 3] = ["TCP", "UDP", "SCTP"];

/// The longest port name Kubernetes takes (an RFC 6335 `IANA_SVC_NAME`).
const MAX_PORT_NAME_LEN: usize = 15;

/// The longest Kubernetes Service name (an RFC 1035 label).
const MAX_APP_NAME_LEN: usize = 63;

/// The longest writable path name, so `writable-<name>` stays a 63-character volume name.
const MAX_WRITABLE_NAME_LEN: usize = 50;

impl DeploymentContract {
    /// Check that every artefact generated from this contract comes out valid.
    ///
    /// [`generate_chart`](super::generate_chart),
    /// [`generate_container_manifest`](super::generate_container_manifest),
    /// [`check_chart_drift`](super::check_chart_drift) and the
    /// `generate-artefacts` subcommand run it before writing anything, and
    /// [`validate_helm_values`](super::validate_helm_values) reports it.
    ///
    /// `app_name` is a Kubernetes Service name -- 1 to 63 lowercase letters,
    /// digits and inner hyphens, starting with a letter -- because every object
    /// the chart renders is named after it.
    ///
    /// `image_registry` must be set: there is no default, and an image named
    /// without one resolves to Docker Hub's library namespace.
    ///
    /// Each extra port needs a name Kubernetes takes -- 1 to 15 lowercase
    /// letters, digits and single inner hyphens, with at least one letter --
    /// and a protocol of TCP, UDP or SCTP in any case. No `when` path or value
    /// and no `bound_from` may hold a control character, because the generators
    /// print them onto one line and a newline there starts an instruction or a
    /// key of its own. KEDA left on needs a trigger to scale on, and a
    /// ScaledObject whose only trigger is CPU needs `min_replicas` of at least
    /// one, because KEDA's CPU scaler cannot wake a workload from zero. A
    /// `singleton` runs one pod, so it cannot leave KEDA on.
    ///
    /// Each writable path needs a unique name of 1 to 50 lowercase letters,
    /// digits and inner hyphens, a unique absolute path, and a claim size when
    /// it is persistent. Each added
    /// capability is an upper-case Linux capability name such as `NET_ADMIN`.
    /// Every `x-scalo-dial` marker in `config_schema` is `big` or `small`, and
    /// every `$ref` the dial search follows resolves.
    ///
    /// # Errors
    ///
    /// [`DeploymentError::InvalidContract`] naming the first field at fault.
    pub fn validate(&self) -> Result<(), DeploymentError> {
        if let Some(fault) = app_name_fault(&self.app_name) {
            return Err(invalid(
                "app_name".to_string(),
                format!(
                    "{:?} {fault}, and every object the chart renders is named after it, so it \
                     must be a Kubernetes Service name: 1 to {MAX_APP_NAME_LEN} lowercase \
                     letters, digits and inner hyphens, starting with a letter",
                    self.app_name
                ),
            ));
        }
        if self.image_registry.trim().is_empty() {
            return Err(invalid(
                "image_registry".to_string(),
                "no registry is set, so the chart, compose file and container manifest would \
                 name an image nothing pushed. Set `image_registry` in the contract, or \
                 `deployment.image_registry` in the config cascade"
                    .to_string(),
            ));
        }
        for (index, port) in self.extra_ports.iter().enumerate() {
            check_port(index, port)?;
        }
        self.check_writable_paths()?;
        self.check_security()?;
        if let Some(schema) = &self.config_schema {
            super::dials(schema)
                .map_err(|e| invalid("config_schema".to_string(), e.to_string()))?;
        }
        if self.singleton && self.enabled_keda().is_some() {
            return Err(invalid(
                "keda".to_string(),
                "a singleton runs exactly one pod, so KEDA must be off; set `keda: None`"
                    .to_string(),
            ));
        }
        self.check_keda()
    }

    /// The `writable_paths` half of [`validate`](Self::validate).
    fn check_writable_paths(&self) -> Result<(), DeploymentError> {
        let mut names = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for (index, writable) in self.writable_paths.iter().enumerate() {
            check_writable_path(index, writable)?;
            let field = |part: &str| format!("writable_paths[{}].{part}", writable.name);
            if !names.insert(writable.name.as_str()) {
                return Err(invalid(field("name"), "is declared twice".to_string()));
            }
            if !paths.insert(writable.path.trim_end_matches('/')) {
                return Err(invalid(
                    field("path"),
                    format!("{:?} is mounted by another writable path", writable.path),
                ));
            }
        }
        Ok(())
    }

    /// The `security` half of [`validate`](Self::validate).
    fn check_security(&self) -> Result<(), DeploymentError> {
        for (index, capability) in self.security.capabilities_add.iter().enumerate() {
            let named = !capability.is_empty()
                && capability
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
            if !named {
                return Err(invalid(
                    format!("security.capabilities_add[{index}]"),
                    format!(
                        "{capability:?} is not a Linux capability name; write it upper case \
                         without the CAP_ prefix, e.g. NET_ADMIN"
                    ),
                ));
            }
        }
        Ok(())
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
    if has_control(&port.app_protocol) {
        return Err(invalid(
            field("app_protocol"),
            holds_a_control_character(&port.app_protocol),
        ));
    }
    Ok(())
}

/// Check one writable path, naming it by index until its name is known to be printable.
fn check_writable_path(index: usize, writable: &WritablePath) -> Result<(), DeploymentError> {
    if let Some(fault) = writable_name_fault(&writable.name) {
        return Err(invalid(
            format!("writable_paths[{index}].name"),
            format!(
                "{:?} {fault}, and a writable path name is 1 to {MAX_WRITABLE_NAME_LEN} \
                 lowercase letters, digits and inner hyphens",
                writable.name
            ),
        ));
    }
    let field = |part: &str| format!("writable_paths[{}].{part}", writable.name);
    if !writable.path.starts_with('/') || has_control(&writable.path) {
        return Err(invalid(
            field("path"),
            format!("{:?} is not an absolute path on one line", writable.path),
        ));
    }
    if writable.persistent && writable.size.trim().is_empty() {
        return Err(invalid(
            field("size"),
            "a persistent path needs a size for its claim, e.g. \"1Gi\"".to_string(),
        ));
    }
    for text in [&writable.size, &writable.size_limit] {
        if has_control(text) {
            return Err(invalid(field("size"), holds_a_control_character(text)));
        }
    }
    if let Some(text) = writable
        .when
        .as_ref()
        .and_then(|when| condition_texts(when).find(|text| has_control(text)))
    {
        return Err(invalid(field("when"), holds_a_control_character(text)));
    }
    Ok(())
}

/// Why `name` is not a writable path name, or `None` when it is one.
fn writable_name_fault(name: &str) -> Option<&'static str> {
    if name.is_empty() || name.len() > MAX_WRITABLE_NAME_LEN {
        return Some("is not 1 to 50 characters long");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Some("holds a character other than a lowercase letter, a digit or '-'");
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Some("starts or ends with '-'");
    }
    None
}

/// Why `name` is not a Kubernetes Service name, or `None` when it is one.
fn app_name_fault(name: &str) -> Option<&'static str> {
    if name.is_empty() || name.len() > MAX_APP_NAME_LEN {
        return Some("is not 1 to 63 characters long");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Some("holds a character other than a lowercase letter, a digit or '-'");
    }
    if !name.starts_with(|c: char| c.is_ascii_lowercase()) {
        return Some("does not start with a lowercase letter");
    }
    if name.ends_with('-') {
        return Some("ends with '-'");
    }
    None
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
    use crate::deployment::{
        HealthContract, ImageProfile, KedaContract, NativeDepsContract, OciLabels,
        ResourcesContract, SecurityContract,
    };

    fn contract() -> DeploymentContract {
        DeploymentContract {
            schema_version: crate::deployment::CONTRACT_SCHEMA_VERSION,
            app_name: "app".into(),
            binary_name: String::new(),
            description: String::new(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "APP".into(),
            metric_prefix: "app".into(),
            config_mount_path: String::new(),
            image_registry: "registry.example.com".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: None,
            depends_on: vec![],
            keda: None,
            base_image: "debian:trixie-slim".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::default(),
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
            writable_paths: vec![],
            termination_grace_seconds: 45,
            resources: ResourcesContract::default(),
            security: SecurityContract::default(),
            singleton: false,
        }
    }

    fn refused_field(contract: &DeploymentContract) -> String {
        match contract.validate() {
            Err(DeploymentError::InvalidContract { field, .. }) => field,
            other => panic!("the contract passed: {other:?}"),
        }
    }

    #[test]
    fn writable_paths_with_good_names_and_paths_pass() {
        let mut c = contract();
        c.writable_paths = vec![
            WritablePath::new("spool", "/var/lib/app/spool").size_limit("2Gi"),
            WritablePath::new("state-2", "/var/lib/app/state").persistent("5Gi"),
        ];
        c.validate().unwrap();
    }

    #[test]
    fn a_bad_writable_path_is_refused_with_its_field() {
        let cases = [
            (
                WritablePath::new("Spool", "/spool"),
                "writable_paths[0].name",
            ),
            (
                WritablePath::new("-spool", "/spool"),
                "writable_paths[0].name",
            ),
            (WritablePath::new("", "/spool"), "writable_paths[0].name"),
            (
                WritablePath::new("a".repeat(51), "/spool"),
                "writable_paths[0].name",
            ),
            (
                WritablePath::new("spool", "spool"),
                "writable_paths[spool].path",
            ),
            (
                WritablePath::new("spool", "/sp\nool"),
                "writable_paths[spool].path",
            ),
            (
                WritablePath::new("spool", "/spool").persistent(""),
                "writable_paths[spool].size",
            ),
            (
                WritablePath::new("spool", "/spool").when(PortCondition::Enabled {
                    path: "config.a\nb".into(),
                }),
                "writable_paths[spool].when",
            ),
        ];
        for (writable, field) in cases {
            let mut c = contract();
            c.writable_paths = vec![writable.clone()];
            assert_eq!(refused_field(&c), field, "{writable:?}");
        }
    }

    #[test]
    fn a_writable_name_or_path_declared_twice_is_refused() {
        let mut c = contract();
        c.writable_paths = vec![
            WritablePath::new("spool", "/a"),
            WritablePath::new("spool", "/b"),
        ];
        assert_eq!(refused_field(&c), "writable_paths[spool].name");
        c.writable_paths = vec![
            WritablePath::new("one", "/data/"),
            WritablePath::new("two", "/data"),
        ];
        assert_eq!(refused_field(&c), "writable_paths[two].path");
    }

    #[test]
    fn a_singleton_cannot_leave_keda_on() {
        let mut c = contract();
        c.singleton = true;
        c.keda = Some(KedaContract::default());
        assert_eq!(refused_field(&c), "keda");
        c.keda = None;
        c.validate().unwrap();
    }

    #[test]
    fn an_added_capability_must_be_a_capability_name() {
        let mut c = contract();
        c.security.capabilities_add = vec!["NET_ADMIN".into()];
        c.validate().unwrap();
        for bad in ["net_admin", "CAP NET", ""] {
            c.security.capabilities_add = vec![bad.into()];
            assert_eq!(refused_field(&c), "security.capabilities_add[0]", "{bad:?}");
        }
    }

    #[test]
    fn a_dial_marker_outside_the_tiers_is_refused() {
        let mut c = contract();
        c.config_schema = Some(serde_json::json!({
            "properties": { "rows": { "type": "integer", "x-scalo-dial": "big" } }
        }));
        c.validate().unwrap();
        c.config_schema = Some(serde_json::json!({
            "properties": { "rows": { "type": "integer", "x-scalo-dial": "huge" } }
        }));
        assert_eq!(refused_field(&c), "config_schema");
    }

    #[test]
    fn a_control_character_in_an_app_protocol_is_refused() {
        let mut c = contract();
        c.extra_ports = vec![PortContract::tcp("grpc", 6000).app_protocol("h2c\nx")];
        assert_eq!(refused_field(&c), "extra_ports[grpc].app_protocol");
    }

    #[test]
    fn an_app_name_is_checked_as_a_service_name() {
        for good in ["app", "my-app", "a", "dfe-receiver", "app2", "a--b", &"a".repeat(63)] {
            assert_eq!(app_name_fault(good), None, "{good:?}");
        }
        for bad in [
            "",
            &"a".repeat(64),
            "My-app",
            "my_app",
            "my.app",
            "1app",
            "-app",
            "app-",
            "app\n",
            "my app",
        ] {
            assert!(app_name_fault(bad).is_some(), "{bad:?} passed");
        }
    }

    #[test]
    fn a_contract_whose_app_name_is_not_a_service_name_is_refused() {
        let mut c = contract();
        c.app_name = "Event_Stage".into();
        assert_eq!(refused_field(&c), "app_name");
        c.app_name = "event-stage".into();
        c.validate().unwrap();
    }

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
