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
        contract.extra_ports = vec![PortContract {
            name: "http".into(),
            port: 8080,
            protocol: "TCP".into(),
        }];

        let dockerfile = generate_dockerfile(&contract, None);
        assert!(dockerfile.contains("EXPOSE 9090 8080"));
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

    /// Regression for the dfe-receiver canary 2026-05-25 finding:
    /// keda-scaledobject.yaml previously used
    /// `default (index .Values.config.kafka.topics 0)` which `helm lint`
    /// rejects with `error calling index: index of untyped nil` because
    /// Sprig's `default` evaluates both operands. The render must now
    /// use a conditional `if/else if/else` block instead, and take the first
    /// topic by splitting a joined string, because `index` on a string topic
    /// yields a byte rather than the topic.
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
    /// `password`. The generator used to hand the username to `sasl` and supply
    /// no `username` at all, while the trigger set `saslType`, which is not a
    /// recognised key -- so the mechanism arrived nowhere, no username arrived,
    /// and `password` was the only correctly wired parameter of the three.
    /// Authentication could not succeed, so the scaler never read lag and the
    /// app never scaled. Every DFE app that ships a generated chart had this.
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

    /// Regression for the dfe-receiver canary 2026-05-25 finding:
    /// secret.yaml previously emitted `.Values.x.bearer-tokens` which
    /// Go templates reject ("bad character U+002D '-'"). The render
    /// must now use the `(index .Values.x "bearer-tokens")` form.
    #[test]
    fn test_secret_yaml_handles_hyphenated_key_names() {
        let mut contract = test_contract();
        // dfe-receiver-style hyphenated key_name (token group)
        contract.secrets.push(SecretGroupContract {
            group_name: "auth".into(),
            env_vars: vec![SecretEnvContract {
                env_var: "DFE_RECEIVER__AUTH__BEARER_TOKENS".into(),
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
