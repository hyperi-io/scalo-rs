// Project:   scalo
// File:      src/deployment/generate/mod.rs
// Purpose:   Generate deployment artifacts from DeploymentContract
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Generate deployment artifacts (Dockerfile, runtime stage, Compose fragment,
//! container manifest) from a
//! [`DeploymentContract`](crate::deployment::DeploymentContract).
//!
//! The Kubernetes objects come from the scalo-service library chart, which
//! renders the serialised contract itself (`charts/scalo-service`).

mod common;
mod compose;
mod dockerfile;
mod manifest;

pub use compose::generate_compose_fragment;
pub use dockerfile::{generate_dockerfile, generate_runtime_stage};
pub use manifest::generate_container_manifest;

// Tests call these contract types directly via `use super::*`.
#[cfg(test)]
use crate::deployment::contract::{DeploymentContract, ImageProfile};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::contract::{
        OciLabels, PortContract, SecretEnvContract, SecretGroupContract,
    };
    use crate::deployment::keda::KedaContract;
    use crate::deployment::native_deps::{AptRepoContract, NativeDepsContract};

    const VENDOR_FINGERPRINT: &str = "0123456789ABCDEF0123456789ABCDEF01234567";

    /// A distro package plus a signed vendor repo with no codename of its own,
    /// which takes the apt block through every step.
    fn deps_with_repo(base_image: &str) -> NativeDepsContract {
        let mut deps =
            NativeDepsContract::for_scalo_features(&["directory-config-git"], base_image);
        deps.apt_repos.push(AptRepoContract {
            key_url: "https://apt.vendor.example/archive.key".into(),
            keyring: "/usr/share/keyrings/vendor.gpg".into(),
            url: "https://apt.vendor.example/deb".into(),
            codename: String::new(),
            key_fingerprint: VENDOR_FINGERPRINT.into(),
            packages: vec!["vendor-runtime".into()],
        });
        deps
    }

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
                    optional: false,
                },
                SecretGroupContract {
                    group_name: "clickhouse".into(),
                    env_vars: vec![SecretEnvContract {
                        env_var: "DFE_LOADER__CLICKHOUSE__PASSWORD".into(),
                        key_name: "password".into(),
                        secret_key: "clickhouse-password".into(),
                    }],
                    optional: false,
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
            writable_paths: vec![],
            termination_grace_seconds: 45,
            resources: crate::deployment::ResourcesContract::default(),
            security: crate::deployment::SecurityContract::default(),
            singleton: false,
            service_account: crate::deployment::ServiceAccount::Own,
            hot_paths: vec![],
        }
    }

    #[test]
    fn test_generate_dockerfile() {
        let contract = test_contract();
        let dockerfile = generate_dockerfile(&contract, None).unwrap();

        assert!(dockerfile.contains("FROM ubuntu:24.04"));
        assert!(dockerfile.contains("COPY dfe-loader /usr/local/bin/dfe-loader"));
        assert!(dockerfile.contains("EXPOSE 9090"));
        assert!(dockerfile.contains("/dev/tcp/localhost/9090"));
        assert!(dockerfile.contains("GET /livez HTTP/1.0"));
        assert!(dockerfile.contains("ENTRYPOINT [\"dfe-loader\"]"));
        assert!(dockerfile.contains("CMD [\"--config\", \"/etc/dfe/loader.yaml\"]"));
    }

    /// A production image has no HTTP client, so the compose healthcheck runs
    /// the image's own bash check, with `$` doubled for compose.
    #[test]
    fn compose_healthcheck_runs_the_images_own_check() {
        let contract = test_contract();
        let compose = generate_compose_fragment(&contract).unwrap();
        let dockerfile = generate_dockerfile(&contract, None).unwrap();

        assert!(!dockerfile.contains("curl"), "{dockerfile}");
        assert!(!compose.contains("curl"), "{compose}");
        let check = dockerfile::liveness_check(&contract);
        assert!(
            dockerfile.contains(&format!("CMD [\"bash\", \"-c\", {check}]")),
            "{dockerfile}"
        );
        assert!(
            compose.contains(&format!(
                "test: [\"CMD\", \"bash\", \"-c\", {}]",
                check.replace('$', "$$")
            )),
            "{compose}"
        );
        assert!(compose.contains("\\\"$$code\\\""), "{compose}");
    }

    /// The header lines every generated Dockerfile starts with, before the
    /// licence and copyright.
    const HEADER_TOP: &str = "# Project:   dfe-loader\n\
                              # File:      Dockerfile\n\
                              # Purpose:   production container image\n\
                              #\n";

    /// A contract that names no licence or copyright gets no `# License` or
    /// `# Copyright` line -- not an empty one -- and no second `#`.
    #[test]
    fn dockerfile_header_omits_an_unset_licence_and_copyright() {
        let dockerfile = generate_dockerfile(&test_contract(), None).unwrap();
        assert!(
            dockerfile.starts_with(&format!(
                "{HEADER_TOP}# AUTOGENERATED -- do not edit by hand.\n"
            )),
            "{dockerfile}"
        );
        assert!(!dockerfile.contains("# License"), "{dockerfile}");
        assert!(!dockerfile.contains("# Copyright"), "{dockerfile}");
    }

    /// A licence and copyright that are set are written as they always were:
    /// both lines, in order, closed by `#`.
    #[test]
    fn dockerfile_header_writes_a_set_licence_and_copyright_as_before() {
        let mut contract = test_contract();
        contract.oci_labels = OciLabels {
            licenses: "BUSL-1.1".into(),
            copyright: "(c) 2026 Acme Corp".into(),
            ..OciLabels::default()
        };
        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(
            dockerfile.starts_with(&format!(
                "{HEADER_TOP}# License:   BUSL-1.1\n\
                 # Copyright: (c) 2026 Acme Corp\n\
                 #\n\
                 # AUTOGENERATED -- do not edit by hand.\n"
            )),
            "{dockerfile}"
        );
    }

    /// Either line is written on its own when only its field is set.
    #[test]
    fn dockerfile_header_writes_only_the_line_that_is_set() {
        let mut contract = test_contract();
        contract.oci_labels.licenses = "MIT".into();
        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(
            dockerfile.starts_with(&format!("{HEADER_TOP}# License:   MIT\n#\n# AUTOGENERATED")),
            "{dockerfile}"
        );

        let mut contract = test_contract();
        contract.oci_labels.copyright = "(c) 2026 Acme Corp".into();
        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(
            dockerfile.starts_with(&format!(
                "{HEADER_TOP}# Copyright: (c) 2026 Acme Corp\n#\n# AUTOGENERATED"
            )),
            "{dockerfile}"
        );
    }

    #[test]
    fn test_generate_dockerfile_with_native_deps() {
        let mut contract = test_contract();
        contract.native_deps = deps_with_repo("ubuntu:24.04");

        let dockerfile = generate_dockerfile(&contract, None).unwrap();

        assert!(dockerfile.contains("https://apt.vendor.example/deb"));
        assert!(dockerfile.contains("signed-by=/usr/share/keyrings/vendor.gpg"));
        assert!(dockerfile.contains("vendor-runtime"));
        assert!(dockerfile.contains("libgit2-1.7"));
        // The key import needs gnupg.
        assert!(dockerfile.contains("gnupg"));
    }

    #[test]
    fn test_generate_dockerfile_no_native_deps() {
        let mut contract = test_contract();
        // The Kafka transport and the spool link their C libraries statically.
        contract.native_deps = NativeDepsContract::for_scalo_features(
            &["cli", "deployment", "logger", "transport-kafka", "spool"],
            "ubuntu:24.04",
        );

        // The static OpenSSL reads the CA store under /etc/ssl/certs, which
        // ca-certificates provides.
        for image in [
            generate_dockerfile(&contract, None).unwrap(),
            generate_runtime_stage(&contract).unwrap(),
        ] {
            for absent in [
                "librdkafka",
                "libssl",
                "zlib1g",
                "libzstd",
                "signed-by",
                "gnupg",
            ] {
                assert!(!image.contains(absent), "{absent}:\n{image}");
            }
            assert!(image.contains(" ca-certificates"), "{image}");
        }
    }

    /// A repo with no codename takes the base image's release, and one with a
    /// codename keeps it.
    #[test]
    fn test_generate_dockerfile_bookworm_codename() {
        for (codename, line) in [("", "deb bookworm main"), ("noble", "deb noble main")] {
            let mut contract = test_contract();
            contract.base_image = "debian:bookworm-slim".into();
            contract.native_deps = deps_with_repo("debian:bookworm-slim");
            contract.native_deps.apt_repos[0].codename = codename.into();

            let dockerfile = generate_dockerfile(&contract, None).unwrap();
            assert!(dockerfile.contains(line), "{codename:?}:\n{dockerfile}");
        }
    }

    #[test]
    fn test_generate_dockerfile_production_profile() {
        let contract = test_contract();
        let dockerfile = generate_dockerfile(&contract, None).unwrap();

        assert!(dockerfile.contains("Purpose:   production container image"));
        assert!(dockerfile.contains("LABEL io.scalo.profile=\"production\""));
        assert!(!dockerfile.contains("strace"));
        assert!(!dockerfile.contains("tcpdump"));
    }

    #[test]
    fn test_generate_dockerfile_dev_profile() {
        let contract = test_contract().with_dev_profile();
        let dockerfile = generate_dockerfile(&contract, None).unwrap();

        assert!(dockerfile.contains("Purpose:   development container image"));
        assert!(dockerfile.contains("LABEL io.scalo.profile=\"development\""));
        assert!(dockerfile.contains("strace"));
        assert!(dockerfile.contains("tcpdump"));
        assert!(dockerfile.contains("procps"));
        assert!(dockerfile.contains("bash"));
        assert!(dockerfile.contains("jq"));
    }

    #[test]
    fn test_generate_dockerfile_dev_with_native_deps() {
        let mut contract = test_contract();
        contract.native_deps = deps_with_repo("ubuntu:24.04");
        let dev = contract.with_dev_profile();
        let dockerfile = generate_dockerfile(&dev, None).unwrap();

        // Dev tools present alongside native deps
        assert!(dockerfile.contains("strace"));
        assert!(dockerfile.contains("vendor-runtime"));
        assert!(dockerfile.contains("libgit2-1.7"));
        assert!(dockerfile.contains("LABEL io.scalo.profile=\"development\""));
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

        let dockerfile = generate_dockerfile(&contract, None).unwrap();
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

        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(
            dockerfile.contains("\nEXPOSE 9090 8080 514/udp 2055/udp\n"),
            "UDP ports exposed as TCP:\n{dockerfile}"
        );

        let runtime = generate_runtime_stage(&contract).unwrap();
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

        let compose = generate_compose_fragment(&contract).unwrap();
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
        for (name, text) in [
            ("dockerfile", generate_dockerfile(&contract, None).unwrap()),
            ("runtime stage", generate_runtime_stage(&contract).unwrap()),
        ] {
            assert!(
                text.contains("\nEXPOSE 9090 8080 514/udp\n\n# bash builtins"),
                "{name} EXPOSE changed:\n{text}"
            );
            assert!(!text.contains("Conditional listeners"), "{name}:\n{text}");
        }
        assert!(generate_compose_fragment(&contract).unwrap().contains(
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
             \n# bash builtins";
        for (name, text) in [
            ("dockerfile", generate_dockerfile(&contract, None).unwrap()),
            ("runtime stage", generate_runtime_stage(&contract).unwrap()),
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

        let compose = generate_compose_fragment(&contract).unwrap();
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
        let compose = generate_compose_fragment(&contract).unwrap();
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

        assert_eq!(
            generate_dockerfile(&plain, None).unwrap(),
            generate_dockerfile(&annotated, None).unwrap()
        );
        assert_eq!(
            generate_runtime_stage(&plain).unwrap(),
            generate_runtime_stage(&annotated).unwrap()
        );
        assert_eq!(
            generate_compose_fragment(&plain).unwrap(),
            generate_compose_fragment(&annotated).unwrap()
        );
        assert_eq!(
            generate_container_manifest(&plain).unwrap(),
            generate_container_manifest(&annotated).unwrap()
        );
    }

    #[test]
    fn test_generate_compose_fragment() {
        let contract = test_contract();
        let compose = generate_compose_fragment(&contract).unwrap();

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

    /// Compose's `command` replaces the image's CMD and is appended to its
    /// ENTRYPOINT, which already names the binary, so it carries only the args.
    #[test]
    fn compose_command_is_the_entrypoint_args_alone() {
        let contract = test_contract();
        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(
            dockerfile.contains(&format!("ENTRYPOINT [\"{}\"]", contract.binary())),
            "{dockerfile}"
        );
        let compose = generate_compose_fragment(&contract).unwrap();
        assert!(
            compose.contains("    command: [\"--config\", \"/etc/dfe/loader.yaml\"]\n"),
            "{compose}"
        );
    }

    /// The image creates and switches to appuser uid 1000, the uid the
    /// scalo-service chart runs the pod as by default.
    #[test]
    fn test_image_runs_as_uid_1000() {
        let contract = test_contract();
        for image in [
            generate_dockerfile(&contract, None).unwrap(),
            generate_runtime_stage(&contract).unwrap(),
        ] {
            assert!(image.contains("useradd --create-home --uid 1000 appuser\n"));
            assert!(image.contains("\nUSER 1000\n"), "{image}");
        }
    }

    #[test]
    fn test_dockerfile_warns_when_the_release_was_assumed() {
        // A digest-pinned base carries no codename, so the package names are an
        // assumption. Say so in the artefact rather than let it ship quietly.
        let mut contract = test_contract();
        contract.base_image = "debian@sha256:abc123".into();
        contract.native_deps = super::super::NativeDepsContract::for_scalo_features(
            &["directory-config-git"],
            &contract.base_image,
        );

        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(dockerfile.contains("# WARNING: could not derive the distro release"));
        assert!(dockerfile.contains("debian@sha256:abc123"));
        assert!(dockerfile.contains("deployment.base_distro"));
    }

    #[test]
    fn test_dockerfile_pins_the_repo_signing_key() {
        // Without the assertion the key is trust-on-first-use, re-fetched every
        // build: a compromised mirror serves its own key, `signed-by` validates
        // the attacker's repo, and the image installs their packages.
        let mut contract = test_contract();
        contract.native_deps = deps_with_repo("debian:trixie-slim");

        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(
            dockerfile.contains("gpg --show-keys --with-colons --with-fingerprint"),
            "the signing key must be checked before it is trusted"
        );
        assert!(
            dockerfile.contains(&format!("^fpr:::::::::{VENDOR_FINGERPRINT}:")),
            "the pinned fingerprint must be asserted"
        );
        // The assertion has to precede the dearmor that installs the keyring.
        let check = dockerfile.find("--with-fingerprint").unwrap();
        let install = dockerfile.find("gpg --dearmor").unwrap();
        assert!(check < install, "key is trusted before it is verified");
    }

    /// Every line that directly follows a `marker` line in `text`.
    fn lines_after<'a>(text: &'a str, marker: &str) -> Vec<&'a str> {
        text.lines()
            .collect::<Vec<_>>()
            .windows(2)
            .filter(|pair| pair[0] == marker)
            .map(|pair| pair[1])
            .collect()
    }

    /// The user's home is the working directory, and every writable path is
    /// created and handed to appuser while the build still runs as root.
    #[test]
    fn appuser_gets_a_workdir_and_owns_every_writable_path() {
        let mut contract = test_contract();
        contract.writable_paths = vec![
            crate::deployment::WritablePath::new("spool", "/var/spool/dfe/archiver"),
            crate::deployment::WritablePath::new("archive", "/var/data/it's here")
                .persistent("5Gi"),
        ];
        let mkdir = "RUN mkdir -p '/var/spool/dfe/archiver' '/var/data/it'\\''s here' \
                     && chown 1000:1000 '/var/spool/dfe/archiver' '/var/data/it'\\''s here'";
        for (name, text) in [
            ("dockerfile", generate_dockerfile(&contract, None).unwrap()),
            ("runtime stage", generate_runtime_stage(&contract).unwrap()),
        ] {
            let order: Vec<&str> = text
                .lines()
                .filter(|l| {
                    l.starts_with("RUN mkdir") || l.starts_with("WORKDIR") || l.starts_with("USER")
                })
                .collect();
            assert_eq!(
                order,
                [mkdir, "WORKDIR /home/appuser", "USER 1000"],
                "{name}:\n{text}"
            );
        }

        contract.writable_paths.clear();
        let text = generate_dockerfile(&contract, None).unwrap();
        assert!(
            !text.contains("RUN mkdir"),
            "no writable path, no mkdir:\n{text}"
        );
        assert!(text.contains("WORKDIR /home/appuser"), "{text}");
    }

    /// A DLQ that writes files gets its directory in every render: the
    /// chart's contract JSON, the Dockerfile and the runtime stage. One that
    /// writes none, or whose directory is already writable, adds nothing.
    #[cfg(feature = "dlq")]
    #[test]
    fn a_file_dlq_renders_its_directory_writable() {
        use crate::deployment::WritablePath;
        use crate::dlq::{DlqConfig, DlqMode};

        let dlq = |enabled: bool, file: bool, mode: DlqMode, path: &str| {
            let mut c = DlqConfig {
                enabled,
                mode,
                ..DlqConfig::default()
            };
            c.file.enabled = file;
            c.file.path = path.into();
            c
        };
        let fleet = "/var/spool/dfe/dlq";
        // (case, existing writable paths, dlq config, the paths after)
        let cases: [(&str, Vec<WritablePath>, DlqConfig, Vec<&str>); 8] = [
            (
                "scalo's default",
                vec![],
                DlqConfig::default(),
                vec!["/var/spool/scalo/dlq"],
            ),
            (
                "cascade",
                vec![],
                dlq(true, true, DlqMode::Cascade, fleet),
                vec![fleet],
            ),
            (
                "fan out",
                vec![],
                dlq(true, true, DlqMode::FanOut, fleet),
                vec![fleet],
            ),
            (
                "file only",
                vec![],
                dlq(true, true, DlqMode::FileOnly, fleet),
                vec![fleet],
            ),
            (
                "kafka only",
                vec![],
                dlq(true, true, DlqMode::KafkaOnly, fleet),
                vec![],
            ),
            (
                "dlq off",
                vec![],
                dlq(false, true, DlqMode::Cascade, fleet),
                vec![],
            ),
            (
                "file backend off",
                vec![],
                dlq(true, false, DlqMode::Cascade, fleet),
                vec![],
            ),
            (
                "already under a writable path",
                vec![WritablePath::new("spool", "/var/spool/dfe")],
                dlq(true, true, DlqMode::Cascade, fleet),
                vec!["/var/spool/dfe"],
            ),
        ];
        for (case, existing, config, want) in cases {
            let mut contract = test_contract();
            contract.writable_paths = existing;
            let contract = contract.with_dlq(&config);
            let paths: Vec<&str> = contract
                .writable_paths
                .iter()
                .map(|w| w.path.as_str())
                .collect();
            assert_eq!(paths, want, "{case}");
            contract
                .validate()
                .unwrap_or_else(|e| panic!("{case}: {e}"));

            let json: serde_json::Value =
                serde_json::from_str(&contract.to_json().unwrap()).unwrap();
            let rendered: Vec<&str> = json["writable_paths"]
                .as_array()
                .map(|a| a.iter().filter_map(|w| w["path"].as_str()).collect())
                .unwrap_or_default();
            assert_eq!(rendered, want, "{case}: the chart's contract JSON");
            for (name, text) in [
                ("dockerfile", generate_dockerfile(&contract, None).unwrap()),
                ("runtime stage", generate_runtime_stage(&contract).unwrap()),
            ] {
                for path in &want {
                    assert!(
                        text.contains(&format!("mkdir -p '{path}'")),
                        "{case}: the {name} does not create {path}:\n{text}"
                    );
                }
            }
        }
    }

    /// A production image keeps its repo's packages and drops every tool it
    /// needed to fetch the key, and the healthcheck needs no HTTP client.
    #[test]
    fn a_production_image_carries_no_fetch_or_network_tools() {
        let mut contract = test_contract();
        contract.native_deps = deps_with_repo("debian:trixie-slim");
        let cases = [
            ("production with a repo", contract.clone(), true),
            ("production, no deps", test_contract(), false),
        ];
        for (name, contract, purges) in cases {
            let text = generate_dockerfile(&contract, None).unwrap();
            for tool in ["netcat-openbsd", "iputils-ping"] {
                assert!(!text.contains(tool), "{name} installs {tool}:\n{text}");
            }
            assert!(
                !text.contains("CMD curl"),
                "{name} healthcheck uses curl:\n{text}"
            );
            assert_eq!(
                text.contains("apt-get purge -y --auto-remove curl gnupg"),
                purges,
                "{name}:\n{text}"
            );
        }

        let dev = contract.with_dev_profile();
        let text = generate_dockerfile(&dev, None).unwrap();
        for tool in ["curl", "netcat-openbsd", "iputils-ping", "gnupg"] {
            assert!(
                text.contains(tool),
                "a development image keeps {tool}:\n{text}"
            );
        }
        assert!(!text.contains("apt-get purge"), "{text}");
    }

    /// hadolint reads a pragma only from the line directly above the
    /// instruction it names, so each one is pinned to that instruction, in both
    /// apt branches and both generators.
    #[test]
    fn test_hadolint_pragmas_sit_directly_above_their_instructions() {
        let plain = test_contract();
        let mut with_repo = test_contract();
        with_repo.native_deps = deps_with_repo("ubuntu:24.04");

        for contract in [&plain, &with_repo] {
            let dockerfile = generate_dockerfile(contract, None).unwrap();
            let runtime = generate_runtime_stage(contract).unwrap();
            for (name, text) in [("dockerfile", &dockerfile), ("runtime stage", &runtime)] {
                assert_eq!(
                    lines_after(text, "# hadolint ignore=DL3008"),
                    ["RUN apt-get update && apt-get install -y --no-install-recommends \\"],
                    "{name}:\n{text}"
                );
                assert!(
                    !text.contains("DL3025"),
                    "exec-form HEALTHCHECK needs no pragma:\n{text}"
                );
                let users: Vec<&str> = text.lines().filter(|l| l.starts_with("USER ")).collect();
                assert_eq!(users, ["USER 1000"], "{name}:\n{text}");
            }
            assert_eq!(
                lines_after(&runtime, "# hadolint ignore=DL3022"),
                ["COPY --from=builder /app/target/release/dfe-loader /usr/local/bin/dfe-loader"],
                "{runtime}"
            );
            assert!(!dockerfile.contains("DL3022"), "{dockerfile}");
        }
    }

    #[test]
    fn test_dockerfile_has_no_warning_for_a_recognised_release() {
        // Must carry native deps: with none, build_apt_block early-returns
        // before the warning block is even reached, so an empty contract would
        // pass this test no matter what the warning logic did.
        let mut contract = test_contract();
        contract.base_image = "debian:trixie-slim".into();
        contract.native_deps = super::super::NativeDepsContract::for_scalo_features(
            &["directory-config-git"],
            &contract.base_image,
        );

        let dockerfile = generate_dockerfile(&contract, None).unwrap();
        assert!(
            dockerfile.contains("apt-get install"),
            "apt block was skipped"
        );
        assert!(!dockerfile.contains("# WARNING:"));
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

    /// Every generator refuses each fault the one contract check names, so a
    /// contract the chart refuses renders nothing anywhere. That includes a
    /// control character that would otherwise start an instruction or a key.
    #[test]
    fn every_generator_refuses_a_faulty_port() {
        type Generator =
            fn(&DeploymentContract) -> Result<String, crate::deployment::DeploymentError>;
        let generators: [(&str, Generator); 4] = [
            ("container manifest", generate_container_manifest),
            ("dockerfile", |c| generate_dockerfile(c, None)),
            ("runtime stage", generate_runtime_stage),
            ("compose", generate_compose_fragment),
        ];
        for (port, field) in faulty_ports() {
            let mut contract = test_contract();
            contract.extra_ports = vec![port];
            for (name, generate) in generators {
                let err = generate(&contract).expect_err(field).to_string();
                assert!(err.contains(field), "{name}, {field}: {err}");
            }
        }
    }

    // ============================================================================
    // Contract Identity Annotation Scheme v1 -- end-to-end wiring tests.
    // The unit tests for ContractIdentity itself live in
    // src/deployment/contract_identity.rs; these verify the Dockerfile
    // generator emits the three keys as image labels.
    // ============================================================================

    fn test_identity() -> crate::deployment::ContractIdentity {
        crate::deployment::ContractIdentity::new(
            "0123456789abcdef0123456789abcdef01234567",
            "registry.example.com/event-loader:v2.7.2",
        )
        .expect("test fixture must be valid")
    }

    #[test]
    fn dockerfile_omits_identity_block_when_none() {
        let dockerfile = generate_dockerfile(&test_contract(), None).unwrap();
        assert!(!dockerfile.contains("io.scalo.contract"));
    }

    #[test]
    fn dockerfile_emits_three_identity_labels_when_some() {
        let id = test_identity();
        let dockerfile = generate_dockerfile(&test_contract(), Some(&id)).unwrap();
        assert!(dockerfile.contains("LABEL io.scalo.contract.version=\"v1\""));
        assert!(dockerfile.contains(
            "LABEL io.scalo.contract.source-commit=\"0123456789abcdef0123456789abcdef01234567\""
        ));
        assert!(dockerfile.contains(
            "LABEL io.scalo.contract.image-ref=\"registry.example.com/event-loader:v2.7.2\""
        ));
        // The profile label is unaffected.
        assert!(dockerfile.contains("LABEL io.scalo.profile=\"production\""));
    }

    /// The documented grep payoff: the prefix appears exactly once per key.
    #[test]
    fn dockerfile_carries_each_identity_key_once() {
        let dockerfile = generate_dockerfile(&test_contract(), Some(&test_identity())).unwrap();
        assert_eq!(dockerfile.matches("io.scalo.contract").count(), 3);
    }

    /// A contract with its own name, label namespace, vendor and licence.
    fn namespaced_contract() -> DeploymentContract {
        let mut contract = test_contract();
        contract.app_name = "event-loader".into();
        contract.binary_name = "event-loader".into();
        contract.oci_labels = OciLabels {
            vendor: "Example Ltd".into(),
            label_namespace: "com.example".into(),
            licenses: "MIT".into(),
            ..OciLabels::default()
        };
        contract
    }

    /// Every key scalo stamps itself moves with `label_namespace`, on every
    /// surface, and no default key is left behind.
    #[test]
    fn every_scalo_key_follows_the_label_namespace() {
        let id = test_identity();
        let contract = namespaced_contract();

        let dockerfile = generate_dockerfile(&contract, Some(&id)).unwrap();
        let runtime = generate_runtime_stage(&contract).unwrap();
        let manifest = generate_container_manifest(&contract).unwrap();

        assert!(dockerfile.contains("LABEL com.example.profile=\"production\""));
        assert!(runtime.contains("LABEL com.example.profile=\"production\""));
        assert_eq!(dockerfile.matches("com.example.contract.").count(), 3);
        let labels = &serde_json::from_str::<serde_json::Value>(&manifest).unwrap()["labels"];
        assert_eq!(labels["com.example.profile"], "production");
        assert_eq!(labels["com.example.app"], "event-loader");
        assert_eq!(labels["com.example.metrics_port"], "9090");
        for (name, out) in [
            ("dockerfile", &dockerfile),
            ("runtime stage", &runtime),
            ("manifest", &manifest),
        ] {
            assert!(
                !out.contains("io.scalo"),
                "{name} kept a default key:\n{out}"
            );
        }
    }

    /// The default contract names no vendor or licence, so neither label is
    /// written -- not one with an empty value.
    #[test]
    fn an_unset_vendor_or_licence_writes_no_label() {
        let contract = test_contract();
        let runtime = generate_runtime_stage(&contract).unwrap();
        assert!(
            runtime.contains(
                "LABEL org.opencontainers.image.description=\"\"\n\
                 LABEL io.scalo.profile=\"production\"\n"
            ),
            "{runtime}"
        );
        let manifest: serde_json::Value =
            serde_json::from_str(&generate_container_manifest(&contract).unwrap()).unwrap();
        for key in [
            "org.opencontainers.image.vendor",
            "org.opencontainers.image.licenses",
        ] {
            assert!(!runtime.contains(key), "{key} in:\n{runtime}");
            assert!(
                manifest["labels"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .all(|k| k != key),
                "{key} in:\n{manifest}"
            );
        }
    }

    /// A vendor and licence that are set are written where they always were:
    /// the runtime stage's label block keeps its lines and their order, and the
    /// manifest carries both.
    #[test]
    fn a_set_vendor_and_licence_are_written_in_place() {
        let contract = namespaced_contract();
        let runtime = generate_runtime_stage(&contract).unwrap();
        assert!(
            runtime.contains(
                "# Static OCI labels (from contract)\n\
                 LABEL org.opencontainers.image.title=\"event-loader\"\n\
                 LABEL org.opencontainers.image.description=\"\"\n\
                 LABEL org.opencontainers.image.vendor=\"Example Ltd\"\n\
                 LABEL org.opencontainers.image.licenses=\"MIT\"\n\
                 LABEL com.example.profile=\"production\"\n\n"
            ),
            "{runtime}"
        );
        let manifest: serde_json::Value =
            serde_json::from_str(&generate_container_manifest(&contract).unwrap()).unwrap();
        assert_eq!(
            manifest["labels"]["org.opencontainers.image.vendor"],
            "Example Ltd"
        );
        assert_eq!(
            manifest["labels"]["org.opencontainers.image.licenses"],
            "MIT"
        );
    }
}
