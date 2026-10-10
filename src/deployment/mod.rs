// Project:   scalo
// File:      src/deployment/mod.rs
// Purpose:   Deployment contract validation and generation for Helm charts and Dockerfiles
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract validation and generation for Kubernetes/Helm/Docker.
//!
//! Apps provide ~20% customisation via [`DeploymentContract`]; this module
//! generates ~80% boilerplate (Dockerfile, Helm chart, Compose fragment) and
//! validates existing artifacts against the contract.
//!
//! # Architecture
//!
//! ```text
//! App Config::default()  ->  DeploymentContract  ->  generate_chart("chart/")
//!                                                 ->  generate_dockerfile()
//!                                                 ->  generate_compose_fragment()
//!                                                 ->  validate_helm_values("chart/")
//!                                                 ->  validate_dockerfile("Dockerfile")
//! ```
//!
//! The config cascade (figment) is the SSoT for app defaults. The contract
//! captures the deployment-facing subset. Generation creates artifacts from
//! scratch; validation asserts that existing artifacts match.
//!
//! # Example
//!
//! ```rust,no_run
//! use scalo::deployment::{
//!     CONTRACT_SCHEMA_VERSION, DEFAULT_BASE_DISTRO, DeploymentContract, HealthContract,
//!     ImageProfile, KedaContract, NativeDepsContract, ResourcesContract, SecurityContract,
//!     ServiceAccount, WritablePath, base_image_from_cascade, generate_dockerfile,
//!     generate_chart, generate_compose_fragment,
//! };
//!
//! let contract = DeploymentContract {
//!     app_name: "my-app".into(),
//!     binary_name: "my-app".into(),
//!     description: "High-performance data loader".into(),
//!     metrics_port: 9090,
//!     health: HealthContract::default(),
//!     env_prefix: "MY_APP".into(),
//!     metric_prefix: "loader".into(),
//!     config_mount_path: "/etc/my-app/config.yaml".into(),
//!     image_registry: "ghcr.io/example-org".into(),
//!     extra_ports: vec![],
//!     unbound_listen_paths: vec![],
//!     entrypoint_args: vec!["--config".into(), "/etc/my-app/config.yaml".into()],
//!     secrets: vec![],
//!     default_config: None,
//!     depends_on: vec!["kafka".into(), "clickhouse".into()],
//!     keda: Some(KedaContract::default()),
//!     base_image: base_image_from_cascade(),
//!     native_deps: NativeDepsContract::for_features(
//!         &["transport-kafka", "spool", "tiered-sink"],
//!         DEFAULT_BASE_DISTRO,
//!     ),
//!     image_profile: ImageProfile::Production,
//!     oci_labels: Default::default(),
//!     schema_version: CONTRACT_SCHEMA_VERSION,
//!     config_schema: None,
//!     capabilities: vec![],
//!     writable_paths: vec![WritablePath::new("spool", "/var/lib/my-app/spool").size_limit("2Gi")],
//!     termination_grace_seconds: 45,
//!     resources: ResourcesContract::default(),
//!     security: SecurityContract::default(),
//!     singleton: false,
//!     service_account: ServiceAccount::Own,
//! };
//!
//! // Generate production Dockerfile (without identity annotations -- Phase 1
//! // backwards-compat. New callers should pass `Some(&identity)`; see
//! // `ContractIdentity::new` and `ContractIdentity::detect`.)
//! let dockerfile = generate_dockerfile(&contract, None);
//!
//! // Generate development Dockerfile (same binary, adds debug tools)
//! let dev_dockerfile = generate_dockerfile(&contract.with_dev_profile(), None);
//!
//! // Generate Helm chart directory
//! // generate_chart(&contract, "chart/").unwrap();
//!
//! // Generate Docker Compose service fragment
//! let compose = generate_compose_fragment(&contract);
//! ```

pub mod app_project;
mod capability;
mod checks;
mod contract;
pub mod contract_identity;
#[cfg(feature = "config-schema")]
mod contract_schema;
mod dials;
mod emit;
mod error;
pub mod generate;
mod keda;
mod listeners;
mod native_deps;
mod registry;
mod schema_compat;
#[cfg(feature = "deployment-smoke")]
pub mod smoke;
#[cfg(feature = "deployment-test-support")]
pub mod test_support;
mod validate;
pub mod waves;

pub use app_project::{AppProjectContract, AppProjectDestination, generate_argocd_app_project};
pub use capability::{Capability, FieldSpec, FieldType};
pub use contract::{
    CONTRACT_SCHEMA_VERSION, DEFAULT_LABEL_NAMESPACE, DeploymentContract, HealthContract,
    ImageProfile, OciLabels, PortCondition, PortContract, ResourceList, ResourcesContract,
    SecretEnvContract, SecretGroupContract, SecurityContract, ServiceAccount, WritablePath,
};
pub use contract_identity::{ContractIdentity, IdentityError, KEY_SEGMENT, VERSION};
#[cfg(feature = "config-schema")]
pub use contract_schema::{contract_json_schema, contract_schema_file_name};
pub use dials::{DIAL_KEYWORD, DIAL_TIERS, DialError, dials};
#[cfg(feature = "config-schema")]
pub use emit::config_schema_json;
pub use emit::{
    ChartPatch, assert_listeners_declared, assert_no_chart_drift, assert_no_config_artifact_drift,
    check_chart_drift, check_config_artifact_drift, emit_config_artifacts,
};
pub use error::{ContractMismatch, DeploymentError};
pub use generate::{
    ArgocdConfig, generate_argocd_application, generate_chart, generate_compose_fragment,
    generate_container_manifest, generate_dockerfile, generate_runtime_stage,
};
pub use keda::{KafkaLagTrigger, KedaConfig, KedaContract};
pub use native_deps::{AptRepoContract, BaseDistro, NativeDepsContract};
pub use registry::{
    DEFAULT_BASE_DISTRO, DEFAULT_BASE_IMAGE, argocd_dest_namespace_from_cascade,
    argocd_repo_url_from_cascade, base_distro_from_cascade, base_image_from_cascade,
    image_registry_from_cascade, resolve_base_distro,
};
pub use schema_compat::{SchemaBreak, SchemaBreakKind, breaking_changes};
pub use validate::{validate_dockerfile, validate_helm_values};
pub use waves::{WAVE_APPS, WAVE_CRDS, WAVE_OPERATORS, WAVE_POST, WAVE_TOPICS};

#[cfg(test)]
pub(crate) mod env_test_lock {
    use std::sync::{Mutex, MutexGuard};

    /// `DEPLOYMENT__BASE_DISTRO` is process-wide state and the test harness runs
    /// tests as threads, so every test that sets OR reads it must serialise.
    static LOCK: Mutex<()> = Mutex::new(());

    pub(crate) fn guard() -> MutexGuard<'static, ()> {
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
