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
    BaseDistro, CONTRACT_SCHEMA_VERSION, Capability, DeploymentContract, FieldSpec, HealthContract,
    ImageProfile, KafkaLagTrigger, KedaConfig, KedaContract, NativeDepsContract, OciLabels,
    PortCondition, PortContract, ResourceList, ResourcesContract, SecretEnvContract,
    SecretGroupContract, SecurityContract, ServiceAccount, WritablePath, config_schema_json,
    contract_schema_file_name,
};
use scalo::schemars;

const FIXTURE: &str = "tests/fixtures/contract-parity/deployment-contract.json";

/// Where the committed contract JSON Schema lives, relative to the crate root.
const SCHEMA_DIR: &str = "charts/scalo-service/schema";

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
    /// Records fetched per poll.
    #[schemars(extend("x-scalo-dial" = "big"), range(min = 1, max = 100_000))]
    batch_size: u32,
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
        schema_version: CONTRACT_SCHEMA_VERSION,
        app_name: "parity-app".into(),
        binary_name: "parity-app-bin".into(),
        description: "Sample app covering every contract field".into(),
        metrics_port: 9090,
        health: HealthContract {
            liveness_path: "/healthz".into(),
            readiness_path: "/ready".into(),
            metrics_path: "/metrics".into(),
            startup_budget_seconds: 120,
        },
        env_prefix: "PARITY_APP".into(),
        metric_prefix: "parity".into(),
        config_mount_path: "/etc/parity-app/config.yaml".into(),
        image_registry: "registry.example.com/team".into(),
        extra_ports: vec![
            PortContract::tcp("http", 8080)
                .bound_from("http.bind_address")
                .public(),
            PortContract::tcp("grpc", 6000)
                .when_enabled("config.grpc.enabled")
                .bound_from("grpc.bind_address")
                .app_protocol("kubernetes.io/h2c"),
            PortContract::tcp("push", 6001).when_equals("config.source.transport", "direct"),
            PortContract::udp("syslog", 5514)
                .when_one_of("config.syslog.protocol", ["udp", "both"])
                .bound_from("syslog.bind_address"),
        ],
        unbound_listen_paths: vec!["client.bind_address".into()],
        entrypoint_args: vec!["--config".into(), "/etc/parity-app/config.yaml".into()],
        secrets: vec![
            SecretGroupContract::new(
                "source",
                vec![SecretEnvContract {
                    env_var: "PARITY_APP__SOURCE__PASSWORD".into(),
                    key_name: "password".into(),
                    secret_key: "source-password".into(),
                }],
            )
            .optional(),
        ],
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
        writable_paths: vec![
            WritablePath::new("spool", "/var/lib/parity-app/spool")
                .size_limit("2Gi")
                .when(PortCondition::Enabled {
                    path: "config.spool.enabled".into(),
                }),
            WritablePath::new("state", "/var/lib/parity-app/state").persistent("5Gi"),
        ],
        termination_grace_seconds: 60,
        resources: ResourcesContract {
            requests: ResourceList {
                cpu: "250m".into(),
                memory: "256Mi".into(),
            },
            limits: ResourceList {
                cpu: "2".into(),
                memory: "1Gi".into(),
            },
        },
        security: SecurityContract {
            run_as_user: 1001,
            run_as_group: 1002,
            fs_group: 1003,
            read_only_root_filesystem: true,
            capabilities_add: vec!["NET_BIND_SERVICE".into()],
        },
        singleton: false,
        service_account: ServiceAccount::None,
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
        "/health/startup_budget_seconds",
        "/extra_ports/0/public",
        "/extra_ports/1/app_protocol",
        "/secrets/0/optional",
        "/writable_paths/0/size_limit",
        "/writable_paths/0/when",
        "/writable_paths/1/persistent",
        "/writable_paths/1/size",
        "/termination_grace_seconds",
        "/resources/requests/cpu",
        "/resources/limits/memory",
        "/security/capabilities_add",
        "/singleton",
        "/service_account",
        "/config_schema/$defs/SourceSection/properties/batch_size/x-scalo-dial",
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

/// The committed contract JSON Schema for the current version, as a validator.
fn committed_schema_validator() -> jsonschema::Validator {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(SCHEMA_DIR)
        .join(contract_schema_file_name(CONTRACT_SCHEMA_VERSION));
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let schema: serde_json::Value = serde_json::from_str(&text).expect("schema JSON");
    jsonschema::validator_for(&schema).expect("the committed schema is a valid JSON Schema")
}

fn schema_errors(validator: &jsonschema::Validator, contract: &serde_json::Value) -> Vec<String> {
    validator
        .iter_errors(contract)
        .map(|e| e.to_string())
        .collect()
}

/// Every key scalo emits passes the schema a chart assembler validates against.
#[test]
fn contract_parity_fixture_passes_the_committed_schema() {
    let validator = committed_schema_validator();
    let contract: serde_json::Value = serde_json::from_str(&emitted()).expect("contract JSON");
    assert_eq!(schema_errors(&validator, &contract), Vec::<String>::new());
}

/// Each contract here breaks one rule the committed schema states, and fails it.
#[test]
fn contracts_the_committed_schema_refuses() {
    let validator = committed_schema_validator();
    let base: serde_json::Value = serde_json::from_str(&emitted()).expect("contract JSON");
    let broken: [(&str, fn(&mut serde_json::Value)); 8] = [
        ("app_name missing", |c| {
            c.as_object_mut().unwrap().remove("app_name");
        }),
        ("schema_version missing", |c| {
            c.as_object_mut().unwrap().remove("schema_version");
        }),
        ("schema_version 3", |c| {
            c["schema_version"] = 3.into();
        }),
        ("metrics_port out of range", |c| {
            c["metrics_port"] = 70000.into();
        }),
        ("dial tier unknown", |c| {
            c["config_schema"]["$defs"]["SourceSection"]["properties"]["batch_size"]["x-scalo-dial"] =
                "huge".into();
        }),
        ("writable path name invalid", |c| {
            c["writable_paths"][0]["name"] = "Spool_Dir".into();
        }),
        ("startup budget zero", |c| {
            c["health"]["startup_budget_seconds"] = 0.into();
        }),
        ("service account unknown", |c| {
            c["service_account"] = "external".into();
        }),
    ];
    for (case, mutate) in broken {
        let mut contract = base.clone();
        mutate(&mut contract);
        assert!(
            !validator.is_valid(&contract),
            "{case}: the schema accepted it"
        );
    }
}
