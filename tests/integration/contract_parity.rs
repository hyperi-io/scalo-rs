// Project:   scalo
// File:      tests/integration/contract_parity.rs
// Purpose:   Golden deployment contract shared with scalo-py
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The contract scalo emits, pinned as
//! `tests/fixtures/contract-parity/deployment-contract.json`.
//!
//! scalo-py keeps a copy of the file and parses it with its own models, so a
//! field scalo adds here fails scalo-py's suite until its models carry it. The
//! sample sets every optional field, so `skip_serializing_if` leaves none of
//! them out of the fixture.
//!
//! Regenerate after a deliberate contract change, then copy the file to
//! scalo-py's `tests/fixtures/contract-parity/`:
//!
//! ```text
//! env SCALO_WRITE_PARITY_FIXTURE=1 cargo nextest run --all-features -E 'test(contract_parity)'
//! ```

use std::path::PathBuf;

use scalo::SensitiveString;
use scalo::deployment::{
    BaseDistro, Capability, DeploymentContract, FieldSpec, HealthContract, ImageProfile,
    KafkaLagTrigger, KedaConfig, KedaContract, NativeDepsContract, OciLabels, PortContract,
    SecretEnvContract, SecretGroupContract, config_schema_json,
};
use scalo::schemars;

const FIXTURE: &str = "tests/fixtures/contract-parity/deployment-contract.json";

/// Sample app configuration.
#[derive(schemars::JsonSchema)]
#[schemars(crate = "scalo::schemars")]
#[allow(dead_code)]
struct ParityConfig {
    /// HTTP intake.
    http: HttpSection,
    /// Upstream the app consumes from.
    source: SourceSection,
}

#[derive(schemars::JsonSchema)]
#[schemars(crate = "scalo::schemars")]
#[allow(dead_code)]
struct HttpSection {
    /// Listen address.
    bind_address: String,
}

#[derive(schemars::JsonSchema)]
#[schemars(crate = "scalo::schemars")]
#[allow(dead_code)]
struct SourceSection {
    /// `kafka` or `direct`.
    transport: String,
    /// Broker list.
    brokers: Vec<String>,
    /// Consumer group.
    group_id: String,
    /// Topics to consume.
    topics: Vec<String>,
    /// Broker password.
    password: SensitiveString,
}

fn parity_contract() -> DeploymentContract {
    let mut native_deps = NativeDepsContract::for_features(
        &["transport-kafka", "spool", "directory-config-git"],
        BaseDistro::Trixie,
    );
    native_deps.unresolved_base_image = Some("registry.example.com/base@sha256:0123".into());
    native_deps.contradicted_base_image = Some("debian:bookworm-slim".into());

    let keda = KedaContract::from_config(&KedaConfig {
        min_replicas: 2,
        max_replicas: 8,
        cooldown_period: 120,
        ..KedaConfig::default()
    })
    .with_kafka_trigger(KafkaLagTrigger::under("config.source"));

    DeploymentContract {
        schema_version: 3,
        app_name: "parity-app".into(),
        binary_name: "parity-app-bin".into(),
        description: "Sample app covering every contract field".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/healthz".into(),
            readiness_path: "/ready".into(),
            metrics_path: "/metrics".into(),
        },
        env_prefix: "PARITY_APP".into(),
        metric_prefix: "parity".into(),
        config_mount_path: "/etc/parity-app/config.yaml".into(),
        image_registry: "registry.example.com/team".into(),
        extra_ports: vec![
            PortContract::tcp("http", 8080).bound_from("http.bind_address"),
            PortContract::tcp("grpc", 6000)
                .when_enabled("config.grpc.enabled")
                .bound_from("grpc.bind_address"),
            PortContract::tcp("push", 6001).when_equals("config.source.transport", "direct"),
            PortContract::udp("syslog", 5514)
                .when_one_of("config.syslog.protocol", ["udp", "both"])
                .bound_from("syslog.bind_address"),
        ],
        unbound_listen_paths: vec!["client.bind_address".into()],
        entrypoint_args: vec!["--config".into(), "/etc/parity-app/config.yaml".into()],
        secrets: vec![SecretGroupContract {
            group_name: "source".into(),
            env_vars: vec![SecretEnvContract {
                env_var: "PARITY_APP__SOURCE__PASSWORD".into(),
                key_name: "password".into(),
                secret_key: "source-password".into(),
            }],
        }],
        default_config: Some(serde_json::json!({
            "http": { "bind_address": "0.0.0.0:8080" },
            "grpc": { "enabled": false, "bind_address": "0.0.0.0:6000" },
            "source": {
                "transport": "kafka",
                "brokers": ["broker:9092"],
                "group_id": "parity-app",
                "topics": ["events"],
            },
            "syslog": { "protocol": "udp", "bind_address": "0.0.0.0:5514" },
            "client": { "bind_address": "0.0.0.0:0" },
        })),
        depends_on: vec!["kafka".into()],
        keda: Some(keda),
        base_image: "debian:trixie-slim".into(),
        native_deps,
        image_profile: ImageProfile::Development,
        oci_labels: OciLabels {
            title: "parity-app".into(),
            description: "Sample app covering every contract field".into(),
            vendor: "Example Ltd".into(),
            label_namespace: "com.example".into(),
            licenses: "Apache-2.0".into(),
            copyright: "(c) 2026 Example Ltd".into(),
        },
        config_schema: Some(config_schema_json::<ParityConfig>()),
        capabilities: vec![
            Capability::source("kafka")
                .description("Consume from a Kafka topic.")
                .maturity("stable")
                .field(
                    FieldSpec::string("brokers")
                        .required()
                        .description("Broker list.")
                        .example(serde_json::json!(["broker:9092"])),
                )
                .field(FieldSpec::secret("password").description("Broker password."))
                .field(
                    FieldSpec::enumeration("transport", ["kafka", "direct"]).default_value("kafka"),
                )
                .child(
                    Capability::service("events")
                        .field(FieldSpec::int("partitions").default_value(3)),
                ),
        ],
    }
}

/// The contract as `generate-artefacts` writes it: pretty JSON and one newline.
fn emitted() -> String {
    format!("{}\n", parity_contract().to_json())
}

#[test]
fn contract_parity_fixture_matches_the_emitted_contract() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let emitted = emitted();
    if std::env::var_os("SCALO_WRITE_PARITY_FIXTURE").is_some() {
        std::fs::write(&path, &emitted).expect("write the parity fixture");
        return;
    }
    let committed = std::fs::read_to_string(&path).expect("read the parity fixture");
    pretty_assertions::assert_eq!(
        committed,
        emitted,
        "the contract changed: regenerate {FIXTURE} (see the module docs) and copy it to scalo-py"
    );
}

#[test]
fn contract_parity_fixture_carries_every_optional_key() {
    let value: serde_json::Value = serde_json::from_str(&emitted()).expect("contract JSON");
    let present = |pointer: &str| value.pointer(pointer).is_some();
    for pointer in [
        "/unbound_listen_paths",
        "/config_schema",
        "/capabilities",
        "/extra_ports/0/bound_from",
        "/extra_ports/1/when",
        "/keda/enabled",
        "/keda/kafka_trigger",
        "/native_deps/distro",
        "/native_deps/unresolved_base_image",
        "/native_deps/contradicted_base_image",
        "/capabilities/0/maturity",
        "/capabilities/0/children",
        "/capabilities/0/fields/0/example",
        "/capabilities/0/fields/1/secret",
        "/capabilities/0/fields/2/default",
        "/capabilities/0/fields/2/enum_values",
    ] {
        assert!(
            present(pointer),
            "{pointer} missing from the parity contract"
        );
    }
    let kinds: Vec<&str> = value["extra_ports"]
        .as_array()
        .expect("extra_ports")
        .iter()
        .filter_map(|port| {
            port.pointer("/when/kind")
                .and_then(serde_json::Value::as_str)
        })
        .collect();
    assert_eq!(kinds, ["enabled", "equals", "one_of"]);
}

#[test]
fn contract_parity_fixture_round_trips() {
    let parsed: DeploymentContract = serde_json::from_str(&emitted()).expect("contract JSON");
    assert_eq!(format!("{}\n", parsed.to_json()), emitted());
}
