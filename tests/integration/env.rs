// Project:   scalo
// File:      tests/integration/env.rs
// Purpose:   Integration tests for environment variable loading
// Language:  Rust

// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for environment variable loading and .env cascade.
//!
//! These tests verify that:
//! - Standard ENV variable names are correctly loaded
//! - Legacy ENV aliases work with deprecation warnings
//! - The .env file cascade (home + project) works correctly

// =============================================================================
// Kafka ENV Loading Tests
// =============================================================================

#[cfg(feature = "transport-kafka")]
mod kafka_env {
    use scalo::transport::kafka::KafkaConfig;

    #[test]
    fn test_kafka_from_env_standard_names() {
        temp_env::with_vars(
            [
                ("KAFKA_BOOTSTRAP_SERVERS", Some("broker1:9092,broker2:9092")),
                ("KAFKA_SASL_USERNAME", Some("testuser")),
                ("KAFKA_SASL_PASSWORD", Some("testpass")),
                ("KAFKA_SECURITY_PROTOCOL", Some("SASL_SSL")),
                ("KAFKA_SASL_MECHANISM", Some("SCRAM-SHA-512")),
                ("KAFKA_GROUP_ID", Some("test-group")),
                ("KAFKA_TOPICS", Some("topic1,topic2")),
            ],
            || {
                let config = KafkaConfig::from_env_standard();

                assert_eq!(config.brokers, vec!["broker1:9092", "broker2:9092"]);
                assert_eq!(config.sasl_username, Some("testuser".to_string()));
                assert_eq!(
                    config.sasl_password.as_ref().map(|p| p.expose()),
                    Some("testpass")
                );
                assert_eq!(config.security_protocol, "SASL_SSL");
                assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-512".to_string()));
                assert_eq!(config.group, "test-group");
                assert_eq!(config.topics, vec!["topic1", "topic2"]);
            },
        );
    }

    #[test]
    fn test_kafka_from_env_provider() {
        temp_env::with_var("KAFKA_PROVIDER", Some("confluent-cloud"), || {
            // from_env reads the provider NAME; the mechanism derivation happens later at
            // KafkaTransport::new (apply_provider), so here we assert the field is set.
            assert_eq!(
                KafkaConfig::from_env_standard().provider,
                Some("confluent-cloud".to_string())
            );
        });
    }

    #[test]
    fn test_kafka_from_env_group_instance_id() {
        // Set; canonically the pod name.
        temp_env::with_var("KAFKA_GROUP_INSTANCE_ID", Some("pod-7"), || {
            assert_eq!(
                KafkaConfig::from_env_standard().group_instance_id,
                Some("pod-7".to_string())
            );
        });
    }

    #[test]
    fn test_kafka_group_instance_id_unset_by_default() {
        temp_env::with_var("KAFKA_BOOTSTRAP_SERVERS", Some("b:9092"), || {
            // Opt-in: absent env -> dynamic membership.
            assert_eq!(KafkaConfig::from_env_standard().group_instance_id, None);
        });
    }

    #[test]
    fn test_kafka_from_env_legacy_brokers() {
        temp_env::with_var(
            "KAFKA_BROKERS",
            Some("legacy-broker:9092"), // Legacy name
            || {
                let config = KafkaConfig::from_env_standard();

                // Should fall back to legacy KAFKA_BROKERS
                assert_eq!(config.brokers, vec!["legacy-broker:9092"]);
            },
        );
    }

    #[test]
    fn test_kafka_from_env_legacy_sasl_user() {
        temp_env::with_var(
            "KAFKA_SASL_USER",
            Some("legacy-user"), // Legacy name
            || {
                let config = KafkaConfig::from_env_standard();

                // Should fall back to legacy KAFKA_SASL_USER
                assert_eq!(config.sasl_username, Some("legacy-user".to_string()));
            },
        );
    }

    #[test]
    fn test_kafka_from_env_standard_wins_over_legacy() {
        temp_env::with_vars(
            [
                ("KAFKA_BOOTSTRAP_SERVERS", Some("standard:9092")),
                ("KAFKA_BROKERS", Some("legacy:9092")), // Should be ignored
            ],
            || {
                let config = KafkaConfig::from_env_standard();

                // Standard name should win
                assert_eq!(config.brokers, vec!["standard:9092"]);
            },
        );
    }

    #[test]
    fn test_kafka_from_env_with_prefix() {
        temp_env::with_vars(
            [
                ("MYAPP_BOOTSTRAP_SERVERS", Some("prefixed:9092")),
                ("MYAPP_GROUP_ID", Some("prefixed-group")),
            ],
            || {
                let config = KafkaConfig::from_env("MYAPP");

                assert_eq!(config.brokers, vec!["prefixed:9092"]);
                assert_eq!(config.group, "prefixed-group");
            },
        );
    }

    #[test]
    fn test_kafka_from_env_ssl_skip_verify() {
        temp_env::with_var("KAFKA_SSL_SKIP_VERIFY", Some("true"), || {
            let config = KafkaConfig::from_env_standard();

            assert!(config.ssl_skip_verify);
        });
    }

    #[test]
    fn test_kafka_from_env_profile() {
        temp_env::with_var("KAFKA_PROFILE", Some("devtest"), || {
            let config = KafkaConfig::from_env_standard();

            assert_eq!(
                config.profile,
                scalo::transport::kafka::KafkaProfile::DevTest
            );
            // DevTest should auto-enable ssl_skip_verify
            assert!(config.ssl_skip_verify);
        });
    }
}

// =============================================================================
// Vault/OpenBao ENV Loading Tests
// =============================================================================

#[cfg(feature = "secrets-vault")]
mod vault_env {
    use scalo::secrets::{OpenBaoAuth, OpenBaoConfig};

    /// All vault/openbao env vars that could interfere with tests.
    const VAULT_ENV_VARS: &[&str] = &[
        "VAULT_ADDR",
        "VAULT_TOKEN",
        "VAULT_SKIP_VERIFY",
        "VAULT_NAMESPACE",
        "VAULT_ROLE_ID",
        "VAULT_SECRET_ID",
        "VAULT_K8S_ROLE",
        "VAULT_K8S_MOUNT",
        "OPENBAO_ADDR",
        "OPENBAO_TOKEN",
        "BAO_ADDR",
        "BAO_TOKEN",
        "OPENBAO_ROOT_TOKEN",
    ];

    /// Every vault/openbao var unset except `overrides` -- so a leftover value
    /// from the host environment never leaks into a test.
    fn vault_env(overrides: &[(&str, &str)]) -> Vec<(&'static str, Option<String>)> {
        VAULT_ENV_VARS
            .iter()
            .map(|&name| {
                let value = overrides
                    .iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| (*v).to_string());
                (name, value)
            })
            .collect()
    }

    #[test]
    fn test_vault_from_env_token_auth() {
        temp_env::with_vars(
            vault_env(&[
                ("VAULT_ADDR", "https://vault.example.com:8200"),
                ("VAULT_TOKEN", "s.test-token"),
            ]),
            || {
                let config = OpenBaoConfig::from_env().expect("Should load from env");

                assert_eq!(config.address, "https://vault.example.com:8200");
                assert!(
                    matches!(config.auth, OpenBaoAuth::Token { token } if token.expose() == "s.test-token")
                );
            },
        );
    }

    #[test]
    fn test_vault_from_env_approle_auth() {
        temp_env::with_vars(
            vault_env(&[
                ("VAULT_ADDR", "https://vault.example.com:8200"),
                ("VAULT_ROLE_ID", "role-123"),
                ("VAULT_SECRET_ID", "secret-456"),
            ]),
            || {
                let config = OpenBaoConfig::from_env().expect("Should load from env");

                assert!(matches!(
                    config.auth,
                    OpenBaoAuth::AppRole {
                        role_id,
                        secret_id,
                        ..
                    } if role_id == "role-123" && secret_id.expose() == "secret-456"
                ));
            },
        );
    }

    #[test]
    fn test_vault_from_env_k8s_auth() {
        temp_env::with_vars(
            vault_env(&[
                ("VAULT_ADDR", "https://vault.example.com:8200"),
                ("VAULT_K8S_ROLE", "my-k8s-role"),
            ]),
            || {
                let config = OpenBaoConfig::from_env().expect("Should load from env");

                assert!(matches!(
                    config.auth,
                    OpenBaoAuth::Kubernetes { role, .. } if role == "my-k8s-role"
                ));
            },
        );
    }

    #[test]
    fn test_vault_from_env_openbao_fallback() {
        temp_env::with_vars(
            vault_env(&[
                ("OPENBAO_ADDR", "https://openbao:8200"), // Legacy name
                ("OPENBAO_TOKEN", "s.openbao-token"),     // Legacy name
            ]),
            || {
                let config = OpenBaoConfig::from_env().expect("Should load from env");

                assert_eq!(config.address, "https://openbao:8200");
                assert!(
                    matches!(config.auth, OpenBaoAuth::Token { token } if token.expose() == "s.openbao-token")
                );
            },
        );
    }

    #[test]
    fn test_vault_from_env_vault_wins_over_openbao() {
        temp_env::with_vars(
            vault_env(&[
                ("VAULT_ADDR", "https://vault-wins:8200"),
                ("OPENBAO_ADDR", "https://openbao-loses:8200"),
                ("VAULT_TOKEN", "vault-token"),
                ("OPENBAO_TOKEN", "openbao-token"),
            ]),
            || {
                let config = OpenBaoConfig::from_env().expect("Should load from env");

                // VAULT_* should win
                assert_eq!(config.address, "https://vault-wins:8200");
                assert!(
                    matches!(config.auth, OpenBaoAuth::Token { token } if token.expose() == "vault-token")
                );
            },
        );
    }

    #[test]
    fn test_vault_from_env_skip_verify() {
        temp_env::with_vars(
            vault_env(&[
                ("VAULT_ADDR", "https://vault:8200"),
                ("VAULT_TOKEN", "test"),
                ("VAULT_SKIP_VERIFY", "true"),
            ]),
            || {
                let config = OpenBaoConfig::from_env().expect("Should load from env");

                assert!(config.skip_verify);
            },
        );
    }

    #[test]
    fn test_vault_from_env_namespace() {
        temp_env::with_vars(
            vault_env(&[
                ("VAULT_ADDR", "https://vault:8200"),
                ("VAULT_TOKEN", "test"),
                ("VAULT_NAMESPACE", "myorg"),
            ]),
            || {
                let config = OpenBaoConfig::from_env().expect("Should load from env");

                assert_eq!(config.namespace, Some("myorg".to_string()));
            },
        );
    }

    #[test]
    fn test_vault_from_env_missing_addr() {
        temp_env::with_vars(
            vault_env(&[("VAULT_TOKEN", "test")]), // No VAULT_ADDR
            || {
                let config = OpenBaoConfig::from_env();

                assert!(config.is_none());
            },
        );
    }

    #[test]
    fn test_vault_from_env_missing_auth() {
        temp_env::with_vars(
            vault_env(&[("VAULT_ADDR", "https://vault:8200")]), // No auth
            || {
                let config = OpenBaoConfig::from_env();

                assert!(config.is_none());
            },
        );
    }
}

// =============================================================================
// AWS ENV Loading Tests
// =============================================================================

#[cfg(feature = "secrets-aws")]
mod aws_env {
    use scalo::secrets::AwsConfig;

    #[test]
    fn test_aws_from_env_region() {
        temp_env::with_var("AWS_DEFAULT_REGION", Some("eu-west-1"), || {
            let config = AwsConfig::from_env();

            assert_eq!(config.region.as_deref(), Some("eu-west-1"));
        });
    }

    #[test]
    fn test_aws_from_env_legacy_region() {
        temp_env::with_var("AWS_REGION", Some("ap-southeast-2"), || {
            // Legacy
            let config = AwsConfig::from_env();

            assert_eq!(config.region.as_deref(), Some("ap-southeast-2"));
        });
    }

    #[test]
    fn test_aws_from_env_endpoint() {
        temp_env::with_var("AWS_ENDPOINT_URL", Some("http://localhost:4566"), || {
            let config = AwsConfig::from_env();

            assert_eq!(
                config.endpoint_url,
                Some("http://localhost:4566".to_string())
            );
        });
    }

    /// With no region in the environment the config carries none, so the AWS
    /// SDK chain (profile, IMDS) resolves it instead of a pinned default.
    #[test]
    fn test_aws_from_env_leaves_an_unset_region_unset() {
        temp_env::with_vars(
            [
                ("AWS_DEFAULT_REGION", None::<&str>),
                ("AWS_REGION", None::<&str>),
            ],
            || {
                let config = AwsConfig::from_env();

                assert!(
                    config.region.is_none(),
                    "an unset region must stay unset, got {:?}",
                    config.region
                );
            },
        );
    }
}

// =============================================================================
// env_compat Module Tests
// =============================================================================

#[cfg(feature = "config")]
mod env_compat_tests {
    use scalo::config::env_compat::{self, EnvVar};

    #[test]
    fn test_postgres_standard_env() {
        temp_env::with_vars(
            [
                ("PGHOST", Some("pg-standard")),
                ("PGPORT", Some("5432")),
                ("PGUSER", Some("postgres")),
                ("PGDATABASE", Some("mydb")),
            ],
            || {
                assert_eq!(
                    env_compat::postgres::host().get(),
                    Some("pg-standard".to_string())
                );
                assert_eq!(env_compat::postgres::port().get(), Some("5432".to_string()));
                assert_eq!(
                    env_compat::postgres::user().get(),
                    Some("postgres".to_string())
                );
                assert_eq!(
                    env_compat::postgres::database().get(),
                    Some("mydb".to_string())
                );
            },
        );
    }

    #[test]
    fn test_postgres_legacy_fallback() {
        temp_env::with_vars(
            [
                ("POSTGRESQL_HOST", Some("pg-legacy")),
                ("POSTGRESQL_PORT", Some("5433")),
            ],
            || {
                assert_eq!(
                    env_compat::postgres::host().get(),
                    Some("pg-legacy".to_string())
                );
                assert_eq!(env_compat::postgres::port().get(), Some("5433".to_string()));
            },
        );
    }

    #[test]
    fn test_clickhouse_env() {
        temp_env::with_vars(
            [
                ("CLICKHOUSE_HOST", Some("clickhouse.local")),
                ("CLICKHOUSE_DATABASE", Some("events")),
            ],
            || {
                assert_eq!(
                    env_compat::clickhouse::host().get(),
                    Some("clickhouse.local".to_string())
                );
                assert_eq!(
                    env_compat::clickhouse::database().get(),
                    Some("events".to_string())
                );
            },
        );
    }

    #[test]
    fn test_env_var_get_bool_variants() {
        temp_env::with_vars(
            [
                ("TEST_BOOL_1", Some("true")),
                ("TEST_BOOL_2", Some("1")),
                ("TEST_BOOL_3", Some("YES")),
                ("TEST_BOOL_4", Some("on")),
                ("TEST_BOOL_5", Some("false")),
            ],
            || {
                assert_eq!(EnvVar::new("TEST_BOOL_1").get_bool(), Some(true));
                assert_eq!(EnvVar::new("TEST_BOOL_2").get_bool(), Some(true));
                assert_eq!(EnvVar::new("TEST_BOOL_3").get_bool(), Some(true));
                assert_eq!(EnvVar::new("TEST_BOOL_4").get_bool(), Some(true));
                assert_eq!(EnvVar::new("TEST_BOOL_5").get_bool(), Some(false));
            },
        );
    }

    #[test]
    fn test_env_var_get_list() {
        temp_env::with_var("TEST_LIST", Some("a, b, c, d"), || {
            let result = EnvVar::new("TEST_LIST").get_list();
            assert_eq!(
                result,
                Some(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string()
                ])
            );
        });
    }

    #[test]
    fn test_env_var_which_name_used() {
        temp_env::with_var("LEGACY_VAR_TEST", Some("value"), || {
            // Set only legacy
            let var = EnvVar::new("STANDARD_VAR_TEST").with_legacy("LEGACY_VAR_TEST");
            assert_eq!(var.which_name_used(), Some("LEGACY_VAR_TEST"));
        });

        temp_env::with_var("STANDARD_VAR_TEST", Some("value"), || {
            // Set standard
            let var = EnvVar::new("STANDARD_VAR_TEST").with_legacy("LEGACY_VAR_TEST");
            assert_eq!(var.which_name_used(), Some("STANDARD_VAR_TEST"));
        });
    }
}
