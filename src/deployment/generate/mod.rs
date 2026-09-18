// Project:   scalo
// File:      src/deployment/generate/mod.rs
// Purpose:   Generate deployment artifacts from DeploymentContract
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Generate deployment artifacts (Dockerfile, Helm chart, Compose fragment,
//! container manifest, ArgoCD Application) from a
//! [`DeploymentContract`](crate::deployment::DeploymentContract).
//!
//! Apps provide ~20% customisation (ports, secrets, config); this module
//! generates ~80% boilerplate. Split by artefact kind into submodules;
//! the public surface is unchanged (re-exported here).

mod argocd;
mod common;
mod compose;
mod dockerfile;
mod helm;
mod manifest;

pub use argocd::{ArgocdConfig, generate_argocd_application};
pub use compose::generate_compose_fragment;
pub use dockerfile::{generate_dockerfile, generate_runtime_stage};
pub(crate) use helm::chart_files;
pub use helm::generate_chart;
pub use manifest::generate_container_manifest;

// Tests call these private helpers + contract types directly via `use super::*`.
#[cfg(test)]
use crate::deployment::contract::{DeploymentContract, ImageProfile};
#[cfg(test)]
use common::{is_go_identifier, safe_template_lookup, to_camel_suffix};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::contract::{
        OciLabels, PortContract, SecretEnvContract, SecretGroupContract,
    };
    use crate::deployment::error::DeploymentError;
    use crate::deployment::keda::{KafkaLagTrigger, KedaConfig, KedaContract};
    use crate::deployment::native_deps::NativeDepsContract;

    fn test_contract() -> DeploymentContract {
        DeploymentContract {
            app_name: "dfe-loader".into(),
            binary_name: "dfe-loader".into(),
            description: "High-performance Kafka to ClickHouse data loader".into(),
            metrics_port: 9090,
            health: super::super::HealthContract::default(),
            env_prefix: "DFE_LOADER".into(),
            metric_prefix: "loader".into(),
            config_mount_path: "/etc/dfe/loader.yaml".into(),
            image_registry: "ghcr.io/hyperi-io".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
            entrypoint_args: vec!["--config".into(), "/etc/dfe/loader.yaml".into()],
            secrets: vec![
                SecretGroupContract {
                    group_name: "kafka".into(),
                    env_vars: vec![
                        SecretEnvContract {
                            env_var: "DFE_LOADER__KAFKA__USERNAME".into(),
                            key_name: "username".into(),
                            secret_key: "kafka-username".into(),
                        },
                        SecretEnvContract {
                            env_var: "DFE_LOADER__KAFKA__PASSWORD".into(),
                            key_name: "password".into(),
                            secret_key: "kafka-password".into(),
                        },
                    ],
                },
                SecretGroupContract {
                    group_name: "clickhouse".into(),
                    env_vars: vec![SecretEnvContract {
                        env_var: "DFE_LOADER__CLICKHOUSE__PASSWORD".into(),
                        key_name: "password".into(),
                        secret_key: "clickhouse-password".into(),
                    }],
                },
            ],
            default_config: None,
            depends_on: vec!["kafka".into(), "clickhouse".into()],
            keda: Some(KedaContract::default()),
            base_image: "ubuntu:24.04".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::default(),
            schema_version: 3,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        }
    }

    #[test]
    fn test_generate_dockerfile() {
        let contract = test_contract();
        let dockerfile = generate_dockerfile(&contract, None);

        assert!(dockerfile.contains("FROM ubuntu:24.04"));
        assert!(dockerfile.contains("COPY dfe-loader /usr/local/bin/dfe-loader"));
        assert!(dockerfile.contains("EXPOSE 9090"));
        assert!(dockerfile.contains("localhost:9090/livez"));
        assert!(dockerfile.contains("ENTRYPOINT [\"dfe-loader\"]"));
        assert!(dockerfile.contains("CMD [\"--config\", \"/etc/dfe/loader.yaml\"]"));
    }

    #[test]
    fn dockerfile_header_defaults_to_scalo_licence_and_copyright() {
        // Default contract (OciLabels::default) must keep scalo's own header --
        // guards the default-preserving fix for #4 (no golden-output change).
        let contract = test_contract();
        let dockerfile = generate_dockerfile(&contract, None);
        assert!(
            dockerfile.contains("# License:   Apache-2.0"),
            "default header must carry scalo's Apache-2.0 licence"
        );
        assert!(
            dockerfile.contains("# Copyright: (c) 2026 HYPERI PTY LIMITED"),
            "default header must carry scalo's copyright"
        );
    }

    #[test]
    fn dockerfile_header_uses_consumer_licence_and_copyright() {
        // A non-Apache consumer (e.g. a BUSL-1.1 app) must get ITS licence +
        // copyright in the generated header, not scalo's (issue #4).
        let mut contract = test_contract();
        contract.oci_labels = OciLabels {
            licenses: "BUSL-1.1".into(),
            copyright: "(c) 2026 Acme Corp".into(),
            ..OciLabels::default()
        };
        let dockerfile = generate_dockerfile(&contract, None);
        assert!(
            dockerfile.contains("# License:   BUSL-1.1"),
            "consumer licence must flow into the header"
        );
        assert!(
            dockerfile.contains("# Copyright: (c) 2026 Acme Corp"),
            "consumer copyright must flow into the header"
        );
        // And scalo's defaults must NOT leak in.
        assert!(!dockerfile.contains("Apache-2.0"));
        assert!(!dockerfile.contains("HYPERI PTY LIMITED"));
    }

    #[test]
    fn test_generate_dockerfile_with_native_deps() {
        let mut contract = test_contract();
        contract.native_deps = NativeDepsContract::for_scalo_features(
            &["transport-kafka", "spool", "tiered-sink"],
            "ubuntu:24.04",
        );

        let dockerfile = generate_dockerfile(&contract, None);

        // Should contain Confluent APT repo setup
        assert!(dockerfile.contains("packages.confluent.io"));
        assert!(dockerfile.contains("confluent-clients.gpg"));
        // Should contain runtime packages
        assert!(dockerfile.contains("librdkafka1"));
        assert!(dockerfile.contains("libssl3"));
        assert!(dockerfile.contains("libzstd1"));
        // Should include gnupg for key import
        assert!(dockerfile.contains("gnupg"));
    }

    #[test]
    fn test_generate_dockerfile_no_native_deps() {
        let mut contract = test_contract();
        contract.native_deps = NativeDepsContract::for_scalo_features(
            &["cli", "deployment", "logger"],
            "ubuntu:24.04",
        );

        let dockerfile = generate_dockerfile(&contract, None);

        // No Confluent repo, no runtime packages
        assert!(!dockerfile.contains("confluent"));
        assert!(!dockerfile.contains("librdkafka1"));
        assert!(!dockerfile.contains("gnupg"));
    }

    #[test]
    fn test_generate_dockerfile_bookworm_codename() {
        let mut contract = test_contract();
        contract.base_image = "debian:bookworm-slim".into();
        contract.native_deps =
            NativeDepsContract::for_scalo_features(&["transport-kafka"], "debian:bookworm-slim");

        let dockerfile = generate_dockerfile(&contract, None);
        assert!(dockerfile.contains("bookworm main"));
    }

    #[test]
    fn test_generate_dockerfile_production_profile() {
        let contract = test_contract();
        let dockerfile = generate_dockerfile(&contract, None);

        assert!(dockerfile.contains("Purpose:   production container image"));
        assert!(dockerfile.contains("io.hyperi.profile=\"production\""));
        assert!(!dockerfile.contains("strace"));
        assert!(!dockerfile.contains("tcpdump"));
    }

    #[test]
    fn test_generate_dockerfile_dev_profile() {
        let contract = test_contract().with_dev_profile();
        let dockerfile = generate_dockerfile(&contract, None);

        assert!(dockerfile.contains("Purpose:   development container image"));
        assert!(dockerfile.contains("io.hyperi.profile=\"development\""));
        assert!(dockerfile.contains("strace"));
        assert!(dockerfile.contains("tcpdump"));
        assert!(dockerfile.contains("procps"));
        assert!(dockerfile.contains("bash"));
        assert!(dockerfile.contains("jq"));
    }

    #[test]
    fn test_generate_dockerfile_dev_with_native_deps() {
        let mut contract = test_contract();
        contract.native_deps =
            NativeDepsContract::for_scalo_features(&["transport-kafka", "spool"], "ubuntu:24.04");
        let dev = contract.with_dev_profile();
        let dockerfile = generate_dockerfile(&dev, None);

        // Dev tools present alongside native deps
        assert!(dockerfile.contains("strace"));
        assert!(dockerfile.contains("librdkafka1"));
        assert!(dockerfile.contains("libzstd1"));
        assert!(dockerfile.contains("io.hyperi.profile=\"development\""));
    }

    #[test]
    fn test_with_dev_profile_preserves_contract() {
        let contract = test_contract();
        let dev = contract.with_dev_profile();

        assert_eq!(dev.app_name, contract.app_name);
        assert_eq!(dev.metrics_port, contract.metrics_port);
        assert_eq!(dev.image_profile, ImageProfile::Development);
        assert_eq!(contract.image_profile, ImageProfile::Production);
    }

    #[test]
    fn test_generate_dockerfile_extra_ports() {
        let mut contract = test_contract();
        contract.extra_ports = vec![PortContract::tcp("http", 8080)];

        let dockerfile = generate_dockerfile(&contract, None);
        assert!(dockerfile.contains("EXPOSE 9090 8080"));
    }

    /// Mixed TCP and UDP extra ports, the UDP ones spelt in both cases.
    fn mixed_protocol_ports() -> Vec<PortContract> {
        vec![
            PortContract::tcp("http", 8080),
            PortContract::udp("syslog", 514),
            PortContract {
                protocol: "udp".into(),
                ..PortContract::udp("netflow", 2055)
            },
        ]
    }

    /// EXPOSE without a protocol means TCP, so a UDP port has to say so.
    #[test]
    fn test_dockerfile_exposes_udp_ports_as_udp() {
        let mut contract = test_contract();
        contract.extra_ports = mixed_protocol_ports();

        let dockerfile = generate_dockerfile(&contract, None);
        assert!(
            dockerfile.contains("\nEXPOSE 9090 8080 514/udp 2055/udp\n"),
            "UDP ports exposed as TCP:\n{dockerfile}"
        );

        let runtime = generate_runtime_stage(&contract);
        assert!(
            runtime.contains("\nEXPOSE 9090 8080 514/udp 2055/udp\n"),
            "UDP ports exposed as TCP in the runtime stage:\n{runtime}"
        );
    }

    /// A compose port with no protocol publishes TCP only, so a UDP listener
    /// would be unreachable from the host.
    #[test]
    fn test_compose_publishes_udp_ports_as_udp() {
        let mut contract = test_contract();
        contract.extra_ports = mixed_protocol_ports();

        let compose = generate_compose_fragment(&contract);
        assert!(
            compose.contains(
                "    ports:\n      - \"9090:9090\"\n      - \"8080:8080\"\n      \
                 - \"514:514/udp\"\n      - \"2055:2055/udp\"\n"
            ),
            "UDP ports published as TCP:\n{compose}"
        );
    }

    /// A manifest consumer reads a bare port number as TCP, so a UDP port is
    /// listed in the form the Dockerfile EXPOSE line uses.
    #[test]
    fn test_container_manifest_lists_udp_ports_as_udp() {
        let mut contract = test_contract();
        contract.extra_ports = mixed_protocol_ports();

        let manifest: serde_json::Value =
            serde_json::from_str(&generate_container_manifest(&contract).unwrap()).unwrap();
        assert_eq!(
            manifest["expose_ports"],
            serde_json::json!([9090, 8080, "514/udp", "2055/udp"])
        );
    }

    /// One always-on port, then one gated port per kind of condition.
    fn gated_ports() -> Vec<PortContract> {
        vec![
            PortContract::tcp("http", 8080),
            PortContract::tcp("grpc", 6000).when_enabled("config.grpc.enabled"),
            PortContract::tcp("push", 6001).when_equals("config.source.transport", "direct"),
            PortContract::udp("relay", 6002)
                .when_one_of("config.source.transport", ["direct", "grpc"]),
        ]
    }

    /// A port without `when` must render exactly as it did before gates existed.
    #[test]
    fn test_ungated_ports_render_unchanged() {
        let mut contract = test_contract();
        contract.extra_ports = vec![
            PortContract::tcp("http", 8080),
            PortContract::udp("syslog", 514),
        ];
        let files = render_chart(&contract);

        assert!(files["templates/deployment.yaml"].contains(
            "          ports:\n\
             \x20           - name: metrics\n\
             \x20             containerPort: {{ .Values.service.port }}\n\
             \x20             protocol: TCP\n\
             \x20           - name: http\n\
             \x20             containerPort: 8080\n\
             \x20             protocol: TCP\n\
             \x20           - name: syslog\n\
             \x20             containerPort: 514\n\
             \x20             protocol: UDP\n\
             \x20         env:\n"
        ));
        assert!(files["templates/service.yaml"].contains(
            "      name: metrics\n\
             \x20   - port: 8080\n\
             \x20     targetPort: 8080\n\
             \x20     protocol: TCP\n\
             \x20     name: http\n\
             \x20   - port: 514\n\
             \x20     targetPort: 514\n\
             \x20     protocol: UDP\n\
             \x20     name: syslog\n\
             \x20 selector:\n"
        ));
        for (name, text) in [
            ("dockerfile", generate_dockerfile(&contract, None)),
            ("runtime stage", generate_runtime_stage(&contract)),
        ] {
            assert!(
                text.contains("\nEXPOSE 9090 8080 514/udp\n\nHEALTHCHECK"),
                "{name} EXPOSE changed:\n{text}"
            );
            assert!(!text.contains("Conditional listeners"), "{name}:\n{text}");
        }
        assert!(generate_compose_fragment(&contract).contains(
            "    ports:\n      - \"9090:9090\"\n      - \"8080:8080\"\n      \
             - \"514:514/udp\"\n    volumes:\n"
        ));
        let manifest: serde_json::Value =
            serde_json::from_str(&generate_container_manifest(&contract).unwrap()).unwrap();
        assert_eq!(
            manifest["expose_ports"],
            serde_json::json!([9090, 8080, "514/udp"])
        );
        assert!(manifest.get("conditional_ports").is_none());
    }

    #[test]
    fn test_chart_gates_each_kind_of_condition() {
        let mut contract = test_contract();
        contract.extra_ports = gated_ports();
        let files = render_chart(&contract);

        let deployment = &files["templates/deployment.yaml"];
        assert!(
            deployment.contains(
                "            - name: http\n\
                 \x20             containerPort: 8080\n\
                 \x20             protocol: TCP\n\
                 \x20           {{- if ((.Values.config).grpc).enabled }}\n\
                 \x20           - name: grpc\n\
                 \x20             containerPort: 6000\n\
                 \x20             protocol: TCP\n\
                 \x20           {{- end }}\n\
                 \x20           {{- if eq (toString ((.Values.config).source).transport) \"direct\" }}\n\
                 \x20           - name: push\n\
                 \x20             containerPort: 6001\n\
                 \x20             protocol: TCP\n\
                 \x20           {{- end }}\n\
                 \x20           {{- if has (toString ((.Values.config).source).transport) (list \"direct\" \"grpc\") }}\n\
                 \x20           - name: relay\n\
                 \x20             containerPort: 6002\n\
                 \x20             protocol: UDP\n\
                 \x20           {{- end }}\n\
                 \x20         env:\n"
            ),
            "deployment ports not gated:\n{deployment}"
        );

        let service = &files["templates/service.yaml"];
        assert!(
            service.contains(
                "    - port: 8080\n\
                 \x20     targetPort: 8080\n\
                 \x20     protocol: TCP\n\
                 \x20     name: http\n\
                 \x20   {{- if ((.Values.config).grpc).enabled }}\n\
                 \x20   - port: 6000\n\
                 \x20     targetPort: 6000\n\
                 \x20     protocol: TCP\n\
                 \x20     name: grpc\n\
                 \x20   {{- end }}\n\
                 \x20   {{- if eq (toString ((.Values.config).source).transport) \"direct\" }}\n\
                 \x20   - port: 6001\n\
                 \x20     targetPort: 6001\n\
                 \x20     protocol: TCP\n\
                 \x20     name: push\n\
                 \x20   {{- end }}\n\
                 \x20   {{- if has (toString ((.Values.config).source).transport) (list \"direct\" \"grpc\") }}\n\
                 \x20   - port: 6002\n\
                 \x20     targetPort: 6002\n\
                 \x20     protocol: UDP\n\
                 \x20     name: relay\n\
                 \x20   {{- end }}\n\
                 \x20 selector:\n"
            ),
            "service ports not gated:\n{service}"
        );
    }

    /// A quote in a gate value must not end the template string early.
    #[test]
    fn test_chart_gate_value_is_quoted_for_the_template() {
        let mut contract = test_contract();
        contract.extra_ports =
            vec![PortContract::tcp("odd", 7000).when_equals("config.mode", r#"a"b\c"#)];
        let files = render_chart(&contract);
        assert!(
            files["templates/service.yaml"]
                .contains(r#"{{- if eq (toString (.Values.config).mode) "a\"b\\c" }}"#),
            "{}",
            files["templates/service.yaml"]
        );
    }

    #[test]
    fn test_chart_rejects_a_gate_it_cannot_render() {
        for (port, field) in [
            (
                PortContract::tcp("grpc", 6000).when_enabled("config.my-grpc.enabled"),
                "extra_ports[grpc].when",
            ),
            (
                PortContract::tcp("push", 6001)
                    .when_one_of("config.source.transport", Vec::<String>::new()),
                "extra_ports[push].when",
            ),
        ] {
            let mut contract = test_contract();
            contract.extra_ports = vec![port];
            let dir = tempfile::tempdir().unwrap();
            let err = generate_chart(&contract, dir.path(), None).unwrap_err();
            assert!(
                matches!(err, DeploymentError::InvalidContract { field: ref f, .. } if f == field),
                "unexpected error: {err}"
            );
            assert!(
                std::fs::read_dir(dir.path()).unwrap().next().is_none(),
                "a rejected contract left files behind"
            );
        }
    }

    /// A gated port leaves EXPOSE, since the image cannot know whether its
    /// listener is on, and is listed in a comment right after the line.
    #[test]
    fn test_dockerfile_lists_gated_ports_instead_of_exposing_them() {
        let mut contract = test_contract();
        contract.extra_ports = gated_ports();
        let expected = "\nEXPOSE 9090 8080\n\
             # Conditional listeners, not EXPOSEd -- publish explicitly when enabled:\n\
             #   6000/tcp grpc -- when config.grpc.enabled is true\n\
             #   6001/tcp push -- when config.source.transport is \"direct\"\n\
             #   6002/udp relay -- when config.source.transport is one of \"direct\", \"grpc\"\n\
             \nHEALTHCHECK";
        for (name, text) in [
            ("dockerfile", generate_dockerfile(&contract, None)),
            ("runtime stage", generate_runtime_stage(&contract)),
        ] {
            assert!(text.contains(expected), "{name}:\n{text}");
        }
    }

    #[test]
    fn test_manifest_lists_gated_ports_apart_from_exposed_ones() {
        let mut contract = test_contract();
        contract.extra_ports = gated_ports();
        let manifest: serde_json::Value =
            serde_json::from_str(&generate_container_manifest(&contract).unwrap()).unwrap();
        assert_eq!(manifest["expose_ports"], serde_json::json!([9090, 8080]));
        assert_eq!(
            manifest["conditional_ports"],
            serde_json::json!([
                { "name": "grpc", "port": 6000, "protocol": "TCP",
                  "when": { "kind": "enabled", "path": "config.grpc.enabled" } },
                { "name": "push", "port": 6001, "protocol": "TCP",
                  "when": { "kind": "equals", "path": "config.source.transport", "value": "direct" } },
                { "name": "relay", "port": 6002, "protocol": "UDP",
                  "when": { "kind": "one_of", "path": "config.source.transport",
                            "values": ["direct", "grpc"] } },
            ])
        );
    }

    /// Compose publishes a gated port only when `default_config` turns its
    /// listener on; off, or not decidable from the config, leaves a comment.
    #[test]
    fn test_compose_publishes_a_gated_port_only_when_its_listener_is_on() {
        let mut contract = test_contract();
        contract.extra_ports = vec![
            PortContract::tcp("grpc", 6000).when_enabled("config.grpc.enabled"),
            PortContract::tcp("push", 6001).when_equals("config.source.transport", "direct"),
            PortContract::tcp("otlp", 4317).when_enabled("config.otlp.enabled"),
            PortContract::udp("flow", 2055).when_enabled("config.flow.enabled"),
        ];
        contract.default_config = Some(serde_json::json!({
            "grpc": { "enabled": true },
            "source": { "transport": "bus" },
            "flow": { "enabled": null },
        }));

        let compose = generate_compose_fragment(&contract);
        assert!(
            compose.contains(
                "    ports:\n      - \"9090:9090\"\n      - \"6000:6000\"\n      \
                 # - \"6001:6001\"  # only when config.source.transport is \"direct\"; \
                 uncomment to publish\n      \
                 # - \"4317:4317\"  # only when config.otlp.enabled is true; uncomment to publish\n      \
                 # - \"2055:2055/udp\"  # only when config.flow.enabled is true; \
                 uncomment to publish\n    volumes:\n"
            ),
            "{compose}"
        );

        // With no default config nothing is decidable, so every gated port is a comment.
        contract.default_config = None;
        let compose = generate_compose_fragment(&contract);
        assert!(!compose.contains("      - \"6000:6000\""), "{compose}");
        assert!(
            compose.contains("      # - \"6000:6000\"  # only when"),
            "{compose}"
        );
    }

    /// `bound_from` and `unbound_listen_paths` feed only the listener check,
    /// so no artefact may change with them.
    #[test]
    fn test_listener_metadata_changes_no_artefact() {
        let mut plain = test_contract();
        plain.extra_ports = gated_ports();
        plain.default_config = Some(serde_json::json!({
            "grpc": { "enabled": true, "listen": "0.0.0.0:6000" },
            "kafka": { "brokers": ["k:9092"], "group_id": "g", "topics": ["t"] },
        }));
        let mut annotated = plain.clone();
        for port in &mut annotated.extra_ports {
            port.bound_from = Some(format!("{}.listen", port.name));
        }
        annotated.unbound_listen_paths = vec!["sink.bind_address".into()];

        assert_eq!(render_chart(&plain), render_chart(&annotated));
        assert_eq!(
            generate_dockerfile(&plain, None),
            generate_dockerfile(&annotated, None)
        );
        assert_eq!(
            generate_runtime_stage(&plain),
            generate_runtime_stage(&annotated)
        );
        assert_eq!(
            generate_compose_fragment(&plain),
            generate_compose_fragment(&annotated)
        );
        assert_eq!(
            generate_container_manifest(&plain).unwrap(),
            generate_container_manifest(&annotated).unwrap()
        );
    }

    #[test]
    fn test_generate_compose_fragment() {
        let contract = test_contract();
        let compose = generate_compose_fragment(&contract);

        assert!(compose.contains("dfe-loader:"));
        assert!(compose.contains("ghcr.io/hyperi-io/dfe-loader"));
        assert!(compose.contains("kafka:"));
        assert!(compose.contains("clickhouse:"));
        assert!(compose.contains("condition: service_healthy"));
        assert!(compose.contains("\"9090:9090\""));
        assert!(compose.contains("loader.yaml:/etc/dfe/loader.yaml:ro"));
        assert!(compose.contains("deploy:"));
        assert!(compose.contains("resources:"));
        assert!(compose.contains("limits:"));
        // Defaulted, but overridable from the environment -- nobody should have
        // to hand-edit an AUTOGENERATED file to give a local run more headroom.
        assert!(compose.contains("cpus: \"${DFE_LOADER_CPU_LIMIT:-2}\""));
        assert!(compose.contains("memory: ${DFE_LOADER_MEM_LIMIT:-1G}"));
    }

    #[test]
    fn test_generate_chart() {
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();

        generate_chart(&contract, dir.path(), None).unwrap();

        // Verify files exist
        assert!(dir.path().join("Chart.yaml").exists());
        assert!(dir.path().join("values.yaml").exists());
        assert!(dir.path().join("templates/_helpers.tpl").exists());
        assert!(dir.path().join("templates/deployment.yaml").exists());
        assert!(dir.path().join("templates/service.yaml").exists());
        assert!(dir.path().join("templates/serviceaccount.yaml").exists());
        assert!(dir.path().join("templates/configmap.yaml").exists());
        assert!(dir.path().join("templates/secret.yaml").exists());
        assert!(dir.path().join("templates/hpa.yaml").exists());
        assert!(dir.path().join("templates/keda-scaledobject.yaml").exists());
        assert!(dir.path().join("templates/keda-triggerauth.yaml").exists());
        assert!(dir.path().join("templates/NOTES.txt").exists());
    }

    #[test]
    fn test_chart_yaml_content() {
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let content = std::fs::read_to_string(dir.path().join("Chart.yaml")).unwrap();
        assert!(content.contains("name: dfe-loader"));
        assert!(content.contains("description: High-performance Kafka to ClickHouse data loader"));
    }

    #[test]
    fn test_values_yaml_content() {
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let content = std::fs::read_to_string(dir.path().join("values.yaml")).unwrap();
        assert!(content.contains("port: 9090"));
        assert!(content.contains("prometheus.io/port: \"9090\""));
        assert!(content.contains("prometheus.io/path: \"/metrics\""));
        assert!(content.contains("lagThreshold: \"1000\""));
        assert!(content.contains("kafka-username"));
        assert!(content.contains("kafka-password"));
        assert!(content.contains("clickhouse-password"));
    }

    #[test]
    fn test_helpers_contain_secret_helpers() {
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let content = std::fs::read_to_string(dir.path().join("templates/_helpers.tpl")).unwrap();
        assert!(content.contains("kafkaSecretName"));
        assert!(content.contains("clickhouseSecretName"));
    }

    #[test]
    fn test_deployment_contains_env_vars() {
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let content =
            std::fs::read_to_string(dir.path().join("templates/deployment.yaml")).unwrap();
        assert!(content.contains("DFE_LOADER__KAFKA__USERNAME"));
        assert!(content.contains("DFE_LOADER__KAFKA__PASSWORD"));
        assert!(content.contains("DFE_LOADER__CLICKHOUSE__PASSWORD"));
        assert!(content.contains("path: /livez"));
        assert!(content.contains("path: /readyz"));
        assert!(content.contains("/etc/dfe"));
        // Observability identity: OTel service.name + k8s downward-API resource
        // attrs. Per-pod differentiation comes from these, not metric names.
        assert!(content.contains("name: OTEL_SERVICE_NAME"));
        assert!(content.contains("name: OTEL_RESOURCE_ATTRIBUTES"));
        assert!(content.contains("fieldPath: metadata.uid"));
        assert!(content.contains("k8s.pod.uid=$(POD_UID)"));
    }

    #[test]
    fn test_deployment_env_present_without_secrets() {
        // Even with NO secret env, the observability env block must be emitted.
        let mut contract = test_contract();
        contract.secrets.clear();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();
        let content =
            std::fs::read_to_string(dir.path().join("templates/deployment.yaml")).unwrap();
        assert!(content.contains("          env:\n"));
        assert!(content.contains("name: OTEL_SERVICE_NAME"));
    }

    #[test]
    fn test_otlp_export_is_wired_and_opt_in() {
        // The scrape annotations get metrics to Prometheus; this is the other
        // pathway. Off unless an endpoint is set, so a chart installed without
        // a collector does not sit there retrying.
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let values = std::fs::read_to_string(dir.path().join("values.yaml")).unwrap();
        assert!(values.contains("otel:"));
        assert!(values.contains("endpoint: \"\""));
        assert!(values.contains("protocol: grpc"));

        let deployment =
            std::fs::read_to_string(dir.path().join("templates/deployment.yaml")).unwrap();
        // Parenthesised so `otel: null` yields no env rather than a Helm
        // nil-pointer error.
        assert!(deployment.contains("{{- if (.Values.otel).endpoint }}"));
        assert!(deployment.contains("name: OTEL_EXPORTER_OTLP_ENDPOINT"));
        assert!(deployment.contains("name: OTEL_EXPORTER_OTLP_PROTOCOL"));
    }

    #[test]
    fn test_chart_hardens_the_pod_and_container() {
        // Without these the chart admits a container that can escalate
        // privilege and keeps the full default capability set. Values-driven
        // so an app with a real need can opt back out, but the DEFAULT has to
        // be the hardened one -- a default nobody sets is the one that ships.
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let values = std::fs::read_to_string(dir.path().join("values.yaml")).unwrap();
        assert!(values.contains("runAsNonRoot: true"));
        assert!(values.contains("type: RuntimeDefault"));
        assert!(values.contains("allowPrivilegeEscalation: false"));
        assert!(values.contains("- ALL"));
        // The image creates and switches to appuser uid 1000; the chart must
        // assert the same uid, not a different one that would fail to start.
        assert!(values.contains("runAsUser: 1000"));
        // Deliberately false: the spool and DLQ write to the container
        // filesystem and the only volume mounted is the read-only config map.
        assert!(values.contains("readOnlyRootFilesystem: false"));

        let deployment =
            std::fs::read_to_string(dir.path().join("templates/deployment.yaml")).unwrap();
        assert!(deployment.contains("{{- with .Values.podSecurityContext }}"));
        assert!(deployment.contains("{{- with .Values.securityContext }}"));
    }

    #[test]
    fn test_dockerfile_warns_when_the_release_was_assumed() {
        // A digest-pinned base carries no codename, so the package names are an
        // assumption. Say so in the artefact rather than let it ship quietly.
        let mut contract = test_contract();
        contract.base_image = "debian@sha256:abc123".into();
        contract.native_deps = super::super::NativeDepsContract::for_scalo_features(
            &["transport-kafka"],
            &contract.base_image,
        );

        let dockerfile = generate_dockerfile(&contract, None);
        assert!(dockerfile.contains("# WARNING: could not derive the distro release"));
        assert!(dockerfile.contains("debian@sha256:abc123"));
        assert!(dockerfile.contains("deployment.base_distro"));
    }

    #[test]
    fn test_dockerfile_pins_the_repo_signing_key() {
        // Without the assertion the key is trust-on-first-use, re-fetched every
        // build: a compromised mirror serves its own key, `signed-by` validates
        // the attacker's repo, and we install their librdkafka1. Verified by
        // build in both directions -- the right fingerprint passes, and a
        // one-digit change fails the RUN.
        let mut contract = test_contract();
        contract.native_deps = super::super::NativeDepsContract::for_features(
            &["transport-kafka"],
            super::super::BaseDistro::Trixie,
        );

        let dockerfile = generate_dockerfile(&contract, None);
        assert!(
            dockerfile.contains("gpg --show-keys --with-colons --with-fingerprint"),
            "the signing key must be checked before it is trusted"
        );
        assert!(
            dockerfile.contains("^fpr:::::::::CBBB821E8FAF364F79835C438B1DA6120C2BF624:"),
            "the pinned Confluent fingerprint must be asserted"
        );
        // The assertion has to precede the dearmor that installs the keyring.
        let check = dockerfile.find("--with-fingerprint").unwrap();
        let install = dockerfile.find("gpg --dearmor").unwrap();
        assert!(check < install, "key is trusted before it is verified");
    }

    #[test]
    fn test_dockerfile_has_no_warning_for_a_recognised_release() {
        // Must carry native deps: with none, build_apt_block early-returns
        // before the warning block is even reached, so an empty contract would
        // pass this test no matter what the warning logic did.
        let mut contract = test_contract();
        contract.base_image = "debian:trixie-slim".into();
        contract.native_deps = super::super::NativeDepsContract::for_scalo_features(
            &["transport-kafka"],
            &contract.base_image,
        );

        let dockerfile = generate_dockerfile(&contract, None);
        assert!(
            dockerfile.contains("apt-get install"),
            "apt block was skipped"
        );
        assert!(!dockerfile.contains("# WARNING:"));
    }

    #[test]
    fn test_is_go_identifier() {
        // Valid Go identifiers
        assert!(is_go_identifier("foo"));
        assert!(is_go_identifier("FOO"));
        assert!(is_go_identifier("foo_bar"));
        assert!(is_go_identifier("_underscore_start"));
        assert!(is_go_identifier("foo123"));
        assert!(is_go_identifier("a"));

        // Invalid -- would break Go templates
        assert!(!is_go_identifier("bearer-tokens")); // hyphen
        assert!(!is_go_identifier("foo.bar")); // dot
        assert!(!is_go_identifier("123foo")); // digit-leading
        assert!(!is_go_identifier("")); // empty
        assert!(!is_go_identifier("foo bar")); // space
        assert!(!is_go_identifier("foo:bar")); // colon
    }

    #[test]
    fn test_safe_template_lookup_chooses_form() {
        assert_eq!(
            safe_template_lookup(".Values.auth", "username"),
            ".Values.auth.username"
        );
        assert_eq!(
            safe_template_lookup(".Values.auth", "bearer-tokens"),
            "(index .Values.auth \"bearer-tokens\")"
        );
        assert_eq!(
            safe_template_lookup(".Values.kafka.secretKeys", "kafka-username"),
            "(index .Values.kafka.secretKeys \"kafka-username\")"
        );
    }

    /// `helm lint` rejects `default (index .Values.config.kafka.topics 0)` with
    /// `index of untyped nil`, because Sprig's `default` evaluates both
    /// operands, so the topic lookup is an `if/else if/else` block, and it takes
    /// the first topic by splitting a joined string because `index` on a string
    /// topic yields a byte rather than the topic.
    #[test]
    fn test_keda_scaledobject_topic_lookup_is_lint_safe() {
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let keda_yaml =
            std::fs::read_to_string(dir.path().join("templates/keda-scaledobject.yaml")).unwrap();

        // Old broken form must not appear
        assert!(
            !keda_yaml.contains(
                ".Values.keda.kafka.topic | default (index .Values.config.kafka.topics 0)"
            ),
            "keda-scaledobject.yaml still uses the eagerly-evaluated `default (index ...)` form:\n{keda_yaml}"
        );
        assert!(
            !keda_yaml.contains("(index .Values.config.kafka.topics 0)"),
            "keda-scaledobject.yaml still indexes the topics value, which breaks on a string:\n{keda_yaml}"
        );

        // New conditional form must appear
        assert!(
            keda_yaml.contains("if .Values.keda.kafka.topic"),
            "keda-scaledobject.yaml missing if/else guard for topic lookup:\n{keda_yaml}"
        );
        assert!(
            keda_yaml.contains(r#"{{- $topics := join "," ((.Values.config).kafka).topics }}"#),
            "keda-scaledobject.yaml does not join the topics value nil-safely:\n{keda_yaml}"
        );
        assert!(
            keda_yaml.contains("else if $topics"),
            "keda-scaledobject.yaml missing fallback branch for the configured topics:\n{keda_yaml}"
        );
        assert!(
            keda_yaml.contains(r#"topic: {{ splitList "," $topics | first | quote }}"#),
            "keda-scaledobject.yaml does not take the first configured topic:\n{keda_yaml}"
        );
    }

    /// Every file `generate_chart` wrote, keyed by its path under the chart root.
    fn render_chart(contract: &DeploymentContract) -> std::collections::BTreeMap<String, String> {
        let dir = tempfile::tempdir().unwrap();
        generate_chart(contract, dir.path(), None).unwrap();
        let mut files = std::collections::BTreeMap::new();
        for sub in ["", "templates"] {
            for entry in std::fs::read_dir(dir.path().join(sub)).unwrap() {
                let path = entry.unwrap().path();
                if path.is_file() {
                    let rel = path.strip_prefix(dir.path()).unwrap();
                    files.insert(
                        rel.display().to_string(),
                        std::fs::read_to_string(&path).unwrap(),
                    );
                }
            }
        }
        files
    }

    /// `brokers` is a list, and `quote` on a list renders `"[a b]"`, which KEDA
    /// reads as one host named `[a`, so the list has to be joined first.
    #[test]
    fn test_keda_brokers_are_joined_before_quoting() {
        let files = render_chart(&test_contract());
        let scaled = &files["templates/keda-scaledobject.yaml"];
        assert!(
            scaled.contains(
                r#"bootstrapServers: {{ join "," ((.Values.config).kafka).brokers | quote }}"#
            ),
            "broker list is not joined before quoting:\n{scaled}"
        );
        assert!(
            !scaled.contains(".Values.config.kafka.brokers | quote"),
            "broker list is still quoted as a list:\n{scaled}"
        );
    }

    /// With no kafka secret group there is no TriggerAuthentication, so telling
    /// KEDA to do SCRAM would leave it authenticating with no credentials.
    #[test]
    fn test_keda_sasl_only_with_a_kafka_secret_group() {
        let with_secret = render_chart(&test_contract());
        assert!(
            with_secret["templates/keda-scaledobject.yaml"].contains("sasl: scram_sha512"),
            "a kafka secret group must still set the SASL mechanism"
        );

        let mut contract = test_contract();
        contract.secrets.retain(|g| g.group_name != "kafka");
        let without_secret = render_chart(&contract);
        let scaled = &without_secret["templates/keda-scaledobject.yaml"];
        assert!(
            !scaled.contains("authenticationRef:"),
            "no kafka secret group, yet an authenticationRef was emitted:\n{scaled}"
        );
        assert!(
            !scaled.contains("sasl:"),
            "SASL mechanism emitted with no credentials to go with it:\n{scaled}"
        );
        assert!(
            scaled.contains("tls: disable"),
            "the tls line is out of scope here and must be left as it was:\n{scaled}"
        );
    }

    /// `KedaConfig::enabled` is the documented off switch, so turning it off must
    /// produce exactly what an absent KEDA contract does.
    #[test]
    fn test_keda_config_disabled_generates_what_no_keda_does() {
        let mut off = test_contract();
        off.keda = Some(KedaContract::from_config(&KedaConfig {
            enabled: false,
            ..KedaConfig::default()
        }));
        let mut absent = test_contract();
        absent.keda = None;

        assert_eq!(render_chart(&off), render_chart(&absent));
    }

    /// An app whose Kafka settings live under `config.source` has no
    /// `config.kafka` key at all, so the trigger must not address one.
    #[test]
    fn test_keda_trigger_under_source_never_addresses_config_kafka() {
        let mut contract = test_contract();
        contract.keda = Some(
            KedaContract::default().with_kafka_trigger(KafkaLagTrigger::under("config.source")),
        );
        let files = render_chart(&contract);
        let scaled = &files["templates/keda-scaledobject.yaml"];

        assert!(
            !scaled.contains("config.kafka") && !scaled.contains("(.Values.config).kafka"),
            "trigger still addresses the kafka section:\n{scaled}"
        );
        assert!(scaled.contains(
            r#"bootstrapServers: {{ join "," ((.Values.config).source).brokers | quote }}"#
        ));
        assert!(scaled.contains(
            "consumerGroup: {{ .Values.keda.kafka.consumerGroup | default ((.Values.config).source).group_id | quote }}"
        ));
        assert!(scaled.contains(r#"{{- $topics := join "," ((.Values.config).source).topics }}"#));
    }

    /// With the Kafka lag trigger off, CPU is the only scaler: no kafka trigger,
    /// no kafka values, and no ScaledObject unless CPU scaling is on.
    #[test]
    fn test_keda_trigger_disabled_scales_on_cpu_only() {
        let mut contract = test_contract();
        contract.keda =
            Some(KedaContract::default().with_kafka_trigger(KafkaLagTrigger::disabled()));
        let files = render_chart(&contract);

        let scaled = &files["templates/keda-scaledobject.yaml"];
        assert!(
            !scaled.contains("type: kafka"),
            "kafka trigger emitted:\n{scaled}"
        );
        assert!(
            scaled.contains("- type: cpu"),
            "cpu trigger missing:\n{scaled}"
        );
        assert!(
            scaled.starts_with("{{- if and .Values.keda.enabled .Values.keda.cpu.enabled }}\n"),
            "ScaledObject is not gated on CPU scaling:\n{scaled}"
        );

        // Kept as a stub so the chart's file set is the same either way.
        let auth = &files["templates/keda-triggerauth.yaml"];
        assert!(
            auth.lines().all(|line| line.starts_with('#')),
            "TriggerAuthentication generated with no Kafka lag trigger:\n{auth}"
        );

        let values = &files["values.yaml"];
        assert!(values.contains("keda:\n  enabled: true\n"));
        assert!(
            !values.contains("lagThreshold") && !values.contains("  kafka:\n    #"),
            "keda.kafka offered with no trigger to read it:\n{values}"
        );
        assert!(values.contains("  cpu:\n    enabled: true\n"));
    }

    /// Kubernetes accepts only TCP, UDP and SCTP in upper case, so a contract
    /// spelling the protocol in lower case must still render a valid manifest.
    #[test]
    fn test_chart_writes_port_protocols_in_upper_case() {
        let mut contract = test_contract();
        contract.extra_ports = vec![
            PortContract {
                protocol: "tcp".into(),
                ..PortContract::tcp("http", 8080)
            },
            PortContract {
                protocol: "udp".into(),
                ..PortContract::udp("netflow", 2055)
            },
            PortContract {
                protocol: "Sctp".into(),
                ..PortContract::tcp("diameter", 3868)
            },
        ];
        let files = render_chart(&contract);
        for name in ["templates/deployment.yaml", "templates/service.yaml"] {
            let text = &files[name];
            let protocols: Vec<&str> = text
                .lines()
                .filter_map(|line| line.trim().strip_prefix("protocol: "))
                .collect();
            assert_eq!(protocols, ["TCP", "TCP", "UDP", "SCTP"], "{name}:\n{text}");
        }
    }

    /// A protocol Kubernetes does not take would render a Deployment and
    /// Service that fail to apply, so the chart is refused before any file.
    #[test]
    fn test_chart_rejects_a_protocol_kubernetes_does_not_take() {
        for protocol in ["http", "grpc", ""] {
            let mut contract = test_contract();
            contract.extra_ports = vec![PortContract {
                protocol: protocol.into(),
                ..PortContract::tcp("web", 8080)
            }];
            let dir = tempfile::tempdir().unwrap();
            let err = generate_chart(&contract, dir.path(), None)
                .expect_err("a protocol other than TCP, UDP or SCTP is refused");
            assert!(
                matches!(
                    err,
                    DeploymentError::InvalidContract { ref field, .. }
                        if field == "extra_ports[web].protocol"
                ),
                "unexpected error for {protocol:?}: {err}"
            );
            assert!(
                std::fs::read_dir(dir.path()).unwrap().next().is_none(),
                "a rejected contract left files behind"
            );
        }
    }

    /// One extra port per fault the contract check names, with the field it names.
    fn faulty_ports() -> Vec<(PortContract, &'static str)> {
        let name = "extra_ports[0].name";
        vec![
            (PortContract::tcp("Web", 8080), name),
            (PortContract::tcp("web_api", 8080), name),
            (PortContract::tcp("a-port-name-too-long", 8080), name),
            (PortContract::tcp("8080", 8080), name),
            (PortContract::tcp("-web", 8080), name),
            (PortContract::tcp("web-", 8080), name),
            (PortContract::tcp("we--b", 8080), name),
            (PortContract::tcp("", 8080), name),
            (PortContract::tcp("web\nEXPOSE 22", 8080), name),
            (
                PortContract {
                    protocol: "http".into(),
                    ..PortContract::tcp("web", 8080)
                },
                "extra_ports[web].protocol",
            ),
            (
                PortContract::tcp("web", 8080).when_equals("config.mode\nRUN id", "on"),
                "extra_ports[web].when",
            ),
            (
                PortContract::tcp("web", 8080).when_equals("config.mode", "on\nRUN id"),
                "extra_ports[web].when",
            ),
            (
                PortContract::tcp("web", 8080).when_one_of("config.mode", ["on", "off\ny: 1"]),
                "extra_ports[web].when",
            ),
            (
                PortContract::tcp("web", 8080).bound_from("web.listen\r"),
                "extra_ports[web].bound_from",
            ),
        ]
    }

    /// Every generator that can refuse a contract refuses each fault the one
    /// contract check names, before it writes anything.
    #[test]
    fn test_every_refusing_generator_refuses_a_faulty_port() {
        for (port, field) in faulty_ports() {
            let mut contract = test_contract();
            contract.extra_ports = vec![port];

            let dir = tempfile::tempdir().unwrap();
            let err = generate_chart(&contract, dir.path(), None).expect_err(field);
            assert!(
                matches!(err, DeploymentError::InvalidContract { field: ref f, .. } if f == field),
                "chart, {field}: {err}"
            );
            assert!(
                std::fs::read_dir(dir.path()).unwrap().next().is_none(),
                "a rejected contract left files behind"
            );

            let err = generate_container_manifest(&contract).expect_err(field);
            assert!(err.contains(field), "container manifest, {field}: {err}");

            let err =
                crate::deployment::check_chart_drift(&contract, dir.path(), &[]).expect_err(field);
            assert!(
                matches!(err, DeploymentError::InvalidContract { field: ref f, .. } if f == field),
                "chart drift check, {field}: {err}"
            );
        }
    }

    /// The Dockerfile and Compose generators return text rather than a Result,
    /// so a control character reaching them is written as its escape and
    /// cannot start an instruction or key of its own.
    #[test]
    fn test_text_generators_keep_a_control_character_on_its_comment_line() {
        let mut contract = test_contract();
        contract.extra_ports = vec![
            PortContract::tcp("web\nEXPOSE 22", 8080).when_equals("config.mode", "on\nRUN id"),
        ];
        for (name, text) in [
            ("dockerfile", generate_dockerfile(&contract, None)),
            ("runtime stage", generate_runtime_stage(&contract)),
            ("compose", generate_compose_fragment(&contract)),
        ] {
            assert!(
                !text
                    .lines()
                    .any(|line| line.starts_with("EXPOSE 22") || line.starts_with("RUN id")),
                "{name} carries an injected line:\n{text}"
            );
            assert!(text.contains(r#""on\nRUN id""#), "{name}:\n{text}");
        }
    }

    /// KEDA's CPU scaler cannot wake a workload from zero on its own, so a
    /// ScaledObject whose only trigger is CPU is refused a minimum of zero.
    #[test]
    fn test_a_cpu_only_scaled_object_cannot_scale_to_zero() {
        let mut contract = test_contract();
        let mut keda = KedaContract::default().with_kafka_trigger(KafkaLagTrigger::disabled());
        keda.min_replicas = 0;
        contract.keda = Some(keda);

        let dir = tempfile::tempdir().unwrap();
        let err = generate_chart(&contract, dir.path(), None).unwrap_err();
        assert!(
            matches!(err, DeploymentError::InvalidContract { ref field, .. } if field == "keda.min_replicas"),
            "unexpected error: {err}"
        );
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "a rejected contract left files behind"
        );

        // With the Kafka lag trigger on, lag wakes the workload from zero.
        let mut lag = test_contract();
        if let Some(keda) = lag.keda.as_mut() {
            keda.min_replicas = 0;
        }
        generate_chart(&lag, tempfile::tempdir().unwrap().path(), None)
            .expect("the lag trigger can scale from zero");
    }

    /// Whatever renders owns the replica count -- the ScaledObject, else the
    /// HPA -- and the Deployment sets `replicas` exactly when neither does,
    /// since a Deployment without it runs one pod.
    #[test]
    fn test_deployment_replicas_gate_is_the_inverse_of_every_scaler() {
        let replicas_gate = |contract: &DeploymentContract| {
            let files = render_chart(contract);
            files["templates/deployment.yaml"]
                .lines()
                .take_while(|line| !line.contains("replicas:"))
                .last()
                .unwrap_or_default()
                .trim()
                .to_string()
        };

        // The Kafka lag trigger renders the ScaledObject whenever KEDA is on,
        // and the HPA only while it is off, so the gate folds to this.
        assert_eq!(
            replicas_gate(&test_contract()),
            "{{- if not (or .Values.keda.enabled .Values.autoscaling.enabled) }}"
        );

        let mut cpu_only = test_contract();
        cpu_only.keda =
            Some(KedaContract::default().with_kafka_trigger(KafkaLagTrigger::disabled()));
        assert_eq!(
            replicas_gate(&cpu_only),
            "{{- if not (or (and .Values.keda.enabled .Values.keda.cpu.enabled) \
             (and .Values.autoscaling.enabled (not .Values.keda.enabled))) }}"
        );

        let mut no_keda = test_contract();
        no_keda.keda = None;
        assert_eq!(
            replicas_gate(&no_keda),
            "{{- if not (and .Values.autoscaling.enabled (not .Values.keda.enabled)) }}"
        );
    }

    #[test]
    fn test_keda_with_no_trigger_at_all_is_rejected() {
        let mut contract = test_contract();
        let mut keda = KedaContract::default().with_kafka_trigger(KafkaLagTrigger::disabled());
        keda.cpu_enabled = false;
        contract.keda = Some(keda);

        let dir = tempfile::tempdir().unwrap();
        let err = generate_chart(&contract, dir.path(), None).unwrap_err();
        assert!(
            matches!(err, DeploymentError::InvalidContract { ref field, .. } if field == "keda"),
            "unexpected error: {err}"
        );
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "a rejected contract left files behind"
        );
    }

    #[test]
    fn test_keda_trigger_rejects_a_path_the_chart_cannot_address() {
        let mut contract = test_contract();
        contract.keda = Some(
            KedaContract::default().with_kafka_trigger(KafkaLagTrigger::under("config.my-source")),
        );

        let dir = tempfile::tempdir().unwrap();
        let err = generate_chart(&contract, dir.path(), None).unwrap_err();
        assert!(
            matches!(
                err,
                DeploymentError::InvalidContract { ref field, .. }
                    if field == "keda.kafka_trigger.brokers_path"
            ),
            "unexpected error: {err}"
        );
        assert!(err.to_string().contains("config.my-source.brokers"));
    }

    /// KEDA's Kafka scaler takes `sasl` (the MECHANISM), `username` and
    /// `password`, and ignores `saslType`, so the TriggerAuthentication binds
    /// the username to `username` and the trigger names the mechanism under
    /// `sasl`; wired any other way, authentication fails and the scaler never
    /// reads lag.
    #[test]
    fn test_keda_kafka_auth_uses_the_parameters_keda_recognises() {
        let contract = test_contract();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let auth =
            std::fs::read_to_string(dir.path().join("templates/keda-triggerauth.yaml")).unwrap();
        assert!(
            auth.contains("- parameter: username"),
            "TriggerAuthentication supplies no `username`, so SASL cannot authenticate:\n{auth}"
        );
        assert!(
            auth.contains("- parameter: password"),
            "TriggerAuthentication supplies no `password`:\n{auth}"
        );
        assert!(
            !auth.contains("- parameter: sasl\n"),
            "TriggerAuthentication still binds `sasl` to a credential; `sasl` is the \
             mechanism, not the username:\n{auth}"
        );

        let scaled =
            std::fs::read_to_string(dir.path().join("templates/keda-scaledobject.yaml")).unwrap();
        assert!(
            !scaled.contains("saslType:"),
            "keda-scaledobject.yaml still uses `saslType`, which the kafka trigger does not \
             recognise (the key is `sasl`):\n{scaled}"
        );
        assert!(
            scaled.contains("sasl: scram_sha512"),
            "keda-scaledobject.yaml supplies no SASL mechanism:\n{scaled}"
        );
    }

    /// Go templates reject a dot-walked `.Values.x.bearer-tokens` ("bad
    /// character U+002D '-'"), so a hyphenated key renders in the
    /// `(index .Values.x "bearer-tokens")` form.
    #[test]
    fn test_secret_yaml_handles_hyphenated_key_names() {
        let mut contract = test_contract();
        // A hyphenated key_name, as a token group carries.
        contract.secrets.push(SecretGroupContract {
            group_name: "auth".into(),
            env_vars: vec![SecretEnvContract {
                env_var: "MY_APP__AUTH__BEARER_TOKENS".into(),
                key_name: "bearer-tokens".into(),
                secret_key: "bearer-tokens".into(),
            }],
        });

        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        let secret_yaml =
            std::fs::read_to_string(dir.path().join("templates/secret.yaml")).unwrap();
        let deployment_yaml =
            std::fs::read_to_string(dir.path().join("templates/deployment.yaml")).unwrap();

        // Old broken form must not appear anywhere
        assert!(
            !secret_yaml.contains(".Values.auth.bearer-tokens"),
            "secret.yaml still uses broken dot-walked form for hyphenated key:\n{secret_yaml}"
        );
        assert!(
            !secret_yaml.contains(".Values.auth.secretKeys.bearer-tokens"),
            "secret.yaml still uses broken dot-walked form for hyphenated secretKeys lookup:\n{secret_yaml}"
        );
        assert!(
            !deployment_yaml.contains(".Values.auth.secretKeys.bearer-tokens"),
            "deployment.yaml still uses broken dot-walked form for hyphenated secretKeys lookup:\n{deployment_yaml}"
        );

        // Safe index form must appear
        assert!(
            secret_yaml.contains("(index .Values.auth.secretKeys \"bearer-tokens\")"),
            "secret.yaml missing index-form lookup for secretKeys.bearer-tokens:\n{secret_yaml}"
        );
        assert!(
            secret_yaml.contains("(index .Values.auth \"bearer-tokens\")"),
            "secret.yaml missing index-form lookup for value bearer-tokens:\n{secret_yaml}"
        );
        assert!(
            deployment_yaml.contains("(index .Values.auth.secretKeys \"bearer-tokens\")"),
            "deployment.yaml missing index-form lookup for secretKeys.bearer-tokens:\n{deployment_yaml}"
        );

        // Sanity: Go-safe keys (e.g. existing kafka.username) still use dot form
        assert!(
            secret_yaml.contains(".Values.kafka.secretKeys.username"),
            "Go-safe key 'username' should still use dot-walked form:\n{secret_yaml}"
        );
    }

    #[test]
    fn test_generate_argocd_application_default() {
        let contract = test_contract();
        let argo = ArgocdConfig {
            repo_url: "https://github.com/hyperi-io/dfe-loader".into(),
            ..Default::default()
        };
        let yaml = generate_argocd_application(&contract, &argo, None);

        assert!(yaml.contains("apiVersion: argoproj.io/v1alpha1"));
        assert!(yaml.contains("kind: Application"));
        assert!(yaml.contains("name: dfe-loader"));
        assert!(yaml.contains("namespace: argocd"));
        assert!(yaml.contains("repoURL: https://github.com/hyperi-io/dfe-loader"));
        assert!(yaml.contains("targetRevision: main"));
        assert!(yaml.contains("path: chart"));
        assert!(yaml.contains("CreateNamespace=true"));
        assert!(yaml.contains("Schema version: "));
    }

    #[test]
    fn test_generate_argocd_custom_namespace_and_path() {
        let contract = test_contract();
        let argo = ArgocdConfig {
            repo_url: "https://github.com/hyperi-io/dfe-loader".into(),
            dest_namespace: "production".into(),
            chart_path: "deploy/chart".into(),
            target_revision: "v1.0.0".into(),
            sync_wave: 5,
            ..Default::default()
        };
        let yaml = generate_argocd_application(&contract, &argo, None);
        assert!(yaml.contains("namespace: production"));
        assert!(yaml.contains("path: deploy/chart"));
        assert!(yaml.contains("targetRevision: v1.0.0"));
        assert!(yaml.contains("sync-wave: \"5\""));
    }

    #[test]
    fn argocd_config_default_uses_wave_apps() {
        let cfg = ArgocdConfig::default();
        assert_eq!(cfg.sync_wave, crate::deployment::WAVE_APPS);
    }

    #[test]
    fn argocd_config_default_has_no_extra_ignore_differences() {
        let cfg = ArgocdConfig::default();
        assert!(cfg.extra_ignore_differences.is_empty());
    }

    #[test]
    fn generate_argocd_application_emits_default_ignore_differences() {
        let contract = test_contract();
        let argo = ArgocdConfig {
            repo_url: "https://github.com/hyperi-io/dfe-loader".into(),
            ..Default::default()
        };
        let yaml = generate_argocd_application(&contract, &argo, None);
        assert!(yaml.contains("ignoreDifferences:"));
        assert!(yaml.contains("/spec/replicas"));
        assert!(yaml.contains("/spec/clusterIP"));
        assert!(yaml.contains(".webhooks[].clientConfig.caBundle"));
    }

    #[test]
    fn generate_argocd_application_appends_extra_ignore_differences() {
        let contract = test_contract();
        let argo = ArgocdConfig {
            repo_url: "https://github.com/hyperi-io/dfe-loader".into(),
            extra_ignore_differences: vec![
                "- group: apps\n  kind: Deployment\n  jsonPointers:\n    - /spec/template/spec/containers/0/image".into(),
            ],
            ..Default::default()
        };
        let yaml = generate_argocd_application(&contract, &argo, None);
        assert!(yaml.contains("/spec/template/spec/containers/0/image"));
    }

    #[test]
    fn generate_argocd_application_sync_wave_annotation_uses_config_value() {
        let contract = test_contract();
        let argo = ArgocdConfig {
            repo_url: "https://github.com/hyperi-io/dfe-loader".into(),
            sync_wave: crate::deployment::WAVE_TOPICS,
            ..Default::default()
        };
        let yaml = generate_argocd_application(&contract, &argo, None);
        assert!(yaml.contains(r#"argocd.argoproj.io/sync-wave: "-5""#));
    }

    #[test]
    fn test_no_keda_files_when_disabled() {
        let mut contract = test_contract();
        contract.keda = None;

        let dir = tempfile::tempdir().unwrap();
        generate_chart(&contract, dir.path(), None).unwrap();

        assert!(!dir.path().join("templates/keda-scaledobject.yaml").exists());
        assert!(!dir.path().join("templates/keda-triggerauth.yaml").exists());
    }

    #[test]
    fn test_to_camel_suffix() {
        assert_eq!(to_camel_suffix("kafka"), "kafka");
        assert_eq!(to_camel_suffix("clickhouse"), "clickhouse");
        assert_eq!(to_camel_suffix("click_house"), "clickHouse");
        assert_eq!(to_camel_suffix("my-service"), "myService");
    }

    // ============================================================================
    // Contract Identity Annotation Scheme v1 -- end-to-end wiring tests.
    // The unit tests for ContractIdentity itself live in
    // src/deployment/contract_identity.rs; these verify the three
    // generators each emit the three keys in the right surface.
    // ============================================================================

    fn test_identity() -> crate::deployment::ContractIdentity {
        crate::deployment::ContractIdentity::new(
            "0123456789abcdef0123456789abcdef01234567",
            "ghcr.io/hyperi-io/dfe-loader:v2.7.2",
        )
        .expect("test fixture must be valid")
    }

    #[test]
    fn dockerfile_omits_identity_block_when_none() {
        let dockerfile = generate_dockerfile(&test_contract(), None);
        assert!(!dockerfile.contains("io.hyperi.contract"));
    }

    #[test]
    fn dockerfile_emits_three_identity_labels_when_some() {
        let id = test_identity();
        let dockerfile = generate_dockerfile(&test_contract(), Some(&id));
        assert!(dockerfile.contains("LABEL io.hyperi.contract.version=\"v1\""));
        assert!(dockerfile.contains(
            "LABEL io.hyperi.contract.source-commit=\"0123456789abcdef0123456789abcdef01234567\""
        ));
        assert!(dockerfile.contains(
            "LABEL io.hyperi.contract.image-ref=\"ghcr.io/hyperi-io/dfe-loader:v2.7.2\""
        ));
        // The existing io.hyperi.profile label is unaffected.
        assert!(dockerfile.contains("LABEL io.hyperi.profile=\"production\""));
    }

    #[test]
    fn chart_yaml_omits_identity_block_when_none() {
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&test_contract(), dir.path(), None).unwrap();
        let chart = std::fs::read_to_string(dir.path().join("Chart.yaml")).unwrap();
        assert!(!chart.contains("io.hyperi.contract"));
    }

    #[test]
    fn chart_yaml_emits_three_identity_annotations_when_some() {
        let id = test_identity();
        let dir = tempfile::tempdir().unwrap();
        generate_chart(&test_contract(), dir.path(), Some(&id)).unwrap();
        let chart = std::fs::read_to_string(dir.path().join("Chart.yaml")).unwrap();
        // Top-level annotations block present.
        assert!(chart.contains("\nannotations:\n"));
        assert!(chart.contains("io.hyperi.contract.version: \"v1\""));
        assert!(chart.contains(
            "io.hyperi.contract.source-commit: \"0123456789abcdef0123456789abcdef01234567\""
        ));
        assert!(
            chart.contains("io.hyperi.contract.image-ref: \"ghcr.io/hyperi-io/dfe-loader:v2.7.2\"")
        );
    }

    #[test]
    fn argocd_application_omits_identity_block_when_none() {
        let argo = ArgocdConfig::default();
        let yaml = generate_argocd_application(&test_contract(), &argo, None);
        assert!(!yaml.contains("io.hyperi.contract"));
        // sync-wave is unaffected.
        assert!(yaml.contains("argocd.argoproj.io/sync-wave:"));
    }

    #[test]
    fn argocd_application_emits_three_identity_annotations_when_some() {
        let id = test_identity();
        let argo = ArgocdConfig::default();
        let yaml = generate_argocd_application(&test_contract(), &argo, Some(&id));
        // Both the existing sync-wave AND the three identity keys must appear
        // under the same metadata.annotations block.
        assert!(yaml.contains("argocd.argoproj.io/sync-wave:"));
        assert!(yaml.contains("io.hyperi.contract.version: \"v1\""));
        assert!(yaml.contains(
            "io.hyperi.contract.source-commit: \"0123456789abcdef0123456789abcdef01234567\""
        ));
        assert!(
            yaml.contains("io.hyperi.contract.image-ref: \"ghcr.io/hyperi-io/dfe-loader:v2.7.2\"")
        );
    }

    #[test]
    fn all_three_surfaces_share_the_same_key_prefix() {
        let id = test_identity();
        let argo = ArgocdConfig::default();
        let dir = tempfile::tempdir().unwrap();

        let dockerfile = generate_dockerfile(&test_contract(), Some(&id));
        generate_chart(&test_contract(), dir.path(), Some(&id)).unwrap();
        let chart = std::fs::read_to_string(dir.path().join("Chart.yaml")).unwrap();
        let app = generate_argocd_application(&test_contract(), &argo, Some(&id));

        // The documented grep payoff: every surface mentions the prefix
        // exactly three times (once per key).
        assert_eq!(dockerfile.matches("io.hyperi.contract").count(), 3);
        assert_eq!(chart.matches("io.hyperi.contract").count(), 3);
        assert_eq!(app.matches("io.hyperi.contract").count(), 3);
    }
}
