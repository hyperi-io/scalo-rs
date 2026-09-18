// Project:   scalo
// File:      src/cli/app.rs
// Purpose:   ServiceApp trait and standard lifecycle runner
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Application trait and lifecycle runner for data-plane services.
//!
//! Provides the standard startup sequence: parse -> log -> config -> dispatch.
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::cli::{CommonArgs, ServiceApp, CliError, VersionInfo, run_app};
//!
//! struct MyApp { common: CommonArgs }
//!
//! impl ServiceApp for MyApp {
//!     type Config = MyConfig;
//!
//!     fn name(&self) -> &str { "my-service" }
//!     fn env_prefix(&self) -> &str { "MY_SERVICE" }
//!     fn version_info(&self) -> VersionInfo {
//!         VersionInfo::new("my-service", env!("CARGO_PKG_VERSION"))
//!     }
//!     fn common_args(&self) -> &CommonArgs { &self.common }
//!     fn load_config(&self, path: Option<&str>) -> Result<MyConfig, CliError> { todo!() }
//!     async fn run_service(&self, config: MyConfig) -> Result<(), CliError> { todo!() }
//! }
//! ```

use std::fmt::Debug;

use serde::de::DeserializeOwned;

use super::error::CliError;
use super::version::VersionInfo;
use super::{CommonArgs, StandardCommand, output};

/// The commit the service reports in its app info and manifest, from the
/// build's `GIT_COMMIT`.
const BUILD_COMMIT: &str = match option_env!("GIT_COMMIT") {
    Some(commit) => commit,
    None => "unknown",
};

/// Trait for data-plane service applications.
///
/// Implement this trait to get the standard CLI lifecycle for free.
/// The 80% common behaviour (logging, config, metrics, version) is handled
/// by `run_app()`. Your app provides the 20% (config type, service logic).
pub trait ServiceApp: Sized {
    /// Application-specific configuration type.
    type Config: DeserializeOwned + Debug + Send + Sync;

    /// Service name (e.g. "dfe-loader").
    fn name(&self) -> &str;

    /// Environment variable prefix for config cascade (e.g. "DFE_LOADER").
    fn env_prefix(&self) -> &str;

    /// Version information for this service.
    fn version_info(&self) -> VersionInfo;

    /// Access the common CLI arguments.
    fn common_args(&self) -> &CommonArgs;

    /// Resolve the active subcommand.
    ///
    /// Returns `None` to default to `StandardCommand::Run`.
    fn command(&self) -> Option<&StandardCommand> {
        None
    }

    /// Load application configuration from the given path (or defaults).
    ///
    /// # Errors
    ///
    /// Returns `CliError` if configuration cannot be loaded or parsed.
    fn load_config(&self, path: Option<&str>) -> Result<Self::Config, CliError>;

    /// Run the main service loop.
    ///
    /// Called after logging, config, and [`ServiceRuntime`](super::ServiceRuntime)
    /// are initialised. The runtime contains all common infrastructure (metrics,
    /// memory guard, shutdown token, scaling pressure, and the worker pool when
    /// `worker-pool` is on). Apps just use it -- no boilerplate needed.
    ///
    /// # Errors
    ///
    /// Returns `CliError` if the service encounters a fatal error.
    fn run_service(
        &self,
        config: Self::Config,
        runtime: super::ServiceRuntime,
    ) -> impl std::future::Future<Output = Result<(), CliError>> + Send;

    /// Does this configuration give the service work to do?
    ///
    /// The app's EMPTINESS PREDICATE, and nothing else: `load_config` still
    /// refuses a structurally invalid config loudly, but a config that is valid
    /// and simply empty of work -- no enabled sources, no topics, no
    /// destination -- returns [`WorkState::Idle`](crate::lifecycle::WorkState::Idle)
    /// with the operator-facing reason.
    ///
    /// [`run_app`] then keeps the service Ready with no transports open until a
    /// config change gives it work; see [`crate::lifecycle`] for what idle looks
    /// like from outside. The default is
    /// [`Active`](crate::lifecycle::WorkState::Active), so an app that does not
    /// override this behaves exactly as before.
    ///
    /// ```rust,ignore
    /// fn work_state(&self, config: &Self::Config) -> WorkState {
    ///     WorkState::idle_if(config.sources.enabled().next().is_none(), "no enabled sources")
    /// }
    /// ```
    #[cfg(feature = "lifecycle")]
    fn work_state(&self, _config: &Self::Config) -> crate::lifecycle::WorkState {
        crate::lifecycle::WorkState::Active
    }

    /// Provide scaling pressure components for KEDA autoscaling.
    ///
    /// Override to register app-specific scaling signals (buffer depth,
    /// consumer lag, error rate, etc.). The default returns an empty vec.
    #[cfg(feature = "scaling")]
    fn scaling_components(&self, _config: &Self::Config) -> Vec<crate::ScalingComponent> {
        vec![]
    }

    /// Describe this service's own metrics: its metric groups and anything
    /// else it emits itself.
    ///
    /// Called by the `metrics-manifest` and `generate-artefacts` subcommands to
    /// capture the full metric catalogue without starting the service. The
    /// scalo runtime set (`ServiceMetrics`, app info, and the worker pool and
    /// batch engine sets when compiled in) is always included, so an override
    /// describes only what the app adds. The default is a no-op.
    #[cfg(any(feature = "metrics", feature = "otel-metrics"))]
    fn register_metrics(&self, _manager: &crate::metrics::MetricsManager) {}

    /// Build the deployment contract for this service.
    ///
    /// Called by `generate-artefacts` to produce container specs, health
    /// endpoints, KEDA config, and metrics manifest. The default returns
    /// `None`. Override to provide a contract.
    #[cfg(feature = "deployment")]
    fn deployment_contract(&self) -> Option<crate::deployment::DeploymentContract> {
        None
    }

    /// Default version-check configuration for this service.
    ///
    /// The runtime overlays the `version_check` config cascade on this, so
    /// any key a deployment sets -- an explicit `enabled: false` included
    /// -- wins. Override to supply the service's releases endpoint; the
    /// default has none, which leaves the check inert.
    #[cfg(feature = "version-check")]
    fn version_check_defaults(&self) -> crate::VersionCheckConfig {
        crate::VersionCheckConfig::default()
    }
}

/// Drive the standard data-plane service lifecycle.
///
/// Handles subcommand dispatch:
/// - `run` (default): init logger -> load config -> run service
/// - `version`: print version info and exit
/// - `config-check`: load config, validate, print summary
///
/// # Errors
///
/// Returns `CliError` if any lifecycle step fails.
pub async fn run_app<A: ServiceApp>(app: A) -> Result<(), CliError> {
    let command = app.command().cloned().unwrap_or(StandardCommand::Run);
    let args = app.common_args();

    match command {
        StandardCommand::Version => {
            let info = app.version_info();
            println!("{info}");
            Ok(())
        }

        StandardCommand::ConfigCheck => {
            // Same order as `run`: the level this command REPORTS is resolved
            // partly from config, so a logger built first would report the
            // default rather than what the cascade actually yields.
            let config_path = args.config.as_deref();
            let loaded = app.load_config(config_path);
            init_logger(args)?;

            match loaded {
                Ok(config) => {
                    output::print_success("configuration is valid");
                    if !args.quiet {
                        eprintln!();
                        output::print_kv("service", &app.name());
                        output::print_kv("config", &config_path.unwrap_or("(defaults)"));
                        output::print_kv("log_level", &args.effective_log_level());
                        output::print_kv("log_format", &args.effective_log_format());
                        output::print_kv("metrics_addr", &args.effective_metrics_addr());
                        eprintln!();
                        // Mask the Debug dump before printing: configs hold
                        // ENV-sourced secrets in plain `String` fields, so
                        // `{config:#?}` would print them in clear text.
                        // Without the `logger` feature the consumer has opted
                        // out of every sensitive-field defence anyway, so an
                        // unmasked print here is consistent.
                        let raw = format!("{config:#?}");
                        #[cfg(feature = "logger")]
                        let masked = {
                            let default_fields = crate::logger::default_sensitive_fields();
                            let patterns: Vec<&str> =
                                default_fields.iter().map(String::as_str).collect();
                            crate::logger::mask_sensitive_string(&raw, &patterns)
                        };
                        #[cfg(not(feature = "logger"))]
                        let masked = raw;
                        eprintln!("  config: {masked}");
                    }
                    Ok(())
                }
                Err(e) => {
                    output::print_error(&format!("configuration invalid: {e}"));
                    Err(e)
                }
            }
        }

        #[cfg(any(feature = "metrics", feature = "otel-metrics"))]
        StandardCommand::MetricsManifest => {
            let manifest = build_metrics_manifest(&app, &manifest_manager(&app));
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest)
                    .map_err(|e| CliError::Service(format!("JSON serialisation failed: {e}")))?
            );
            Ok(())
        }
        #[cfg(not(any(feature = "metrics", feature = "otel-metrics")))]
        StandardCommand::MetricsManifest => {
            output::print_error("metrics feature not enabled -- no manifest available");
            Err(CliError::Service("metrics feature not enabled".into()))
        }

        StandardCommand::GenerateArtefacts(ref artefact_args) => {
            generate_artefacts(&app, artefact_args)?;
            Ok(())
        }

        StandardCommand::ConfigSchema(ref cfg_args) => {
            #[cfg(feature = "deployment")]
            {
                emit_config_schema(&app, &cfg_args.dir)?;
                Ok(())
            }
            #[cfg(not(feature = "deployment"))]
            {
                let _ = cfg_args;
                output::print_error(
                    "deployment feature not enabled -- no config artefacts available",
                );
                Err(CliError::Service("deployment feature not enabled".into()))
            }
        }

        StandardCommand::Run => {
            let version_info = app.version_info();
            let config_path = args.config.as_deref();

            // Config loads before the logger: logger setup composes the OTLP
            // span exporter, whose settings live in the cascade. The cascade
            // has no subscriber while it loads, so its own log lines are lost.
            let loaded = app.load_config(config_path);
            init_logger_for_service(args, app.name(), &version_info.version)?;
            let config = loaded?;

            tracing::info!(
                service = app.name(),
                version = version_info.version,
                config = config_path.unwrap_or("(defaults)"),
                "starting service"
            );

            tracing::debug!(?config, "configuration loaded");

            // Build ServiceRuntime -- all common infrastructure for free
            let runtime = super::ServiceRuntime::build(
                app.name(),
                app.env_prefix(),
                &args.effective_metrics_addr(),
                &version_info.version,
                BUILD_COMMIT,
                #[cfg(feature = "scaling")]
                app.scaling_components(&config),
                #[cfg(feature = "version-check")]
                app.version_check_defaults(),
            )
            .await?;

            // Idle until configured. Evaluated HERE -- after the runtime, so
            // /livez and /readyz are already serving -- and not before, or an
            // app with nothing to do would crash-loop with no probe surface.
            #[cfg(feature = "lifecycle")]
            let result = match wait_for_work(&app, config, config_path).await {
                Some(config) => app.run_service(config, runtime).await,
                None => Ok(()),
            };
            #[cfg(not(feature = "lifecycle"))]
            let result = app.run_service(config, runtime).await;

            // Flush what is queued before the process goes away. Both calls
            // are bounded and safe when nothing was ever wired up.
            #[cfg(feature = "otel-metrics")]
            crate::metrics::shutdown_otel_export();
            #[cfg(feature = "otel-tracing")]
            crate::otel_tracing::shutdown();

            result
        }

        #[cfg(feature = "top")]
        StandardCommand::Top(ref top_args) => {
            let top_config = crate::top::TopConfig::from_args(top_args);
            crate::top::run_top(&top_config).map_err(|e| CliError::Service(e.to_string()))
        }
    }
}

/// Hold the service at the idle gate until its configuration gives it work.
///
/// Returns the config to run with, or `None` when shutdown arrived while the
/// service was still idle (exit cleanly -- never start work on the way out).
/// Every wake re-reads the config through the app's own `load_config`, so the
/// predicate sees exactly what a fresh start would see; a load that fails while
/// idle is logged and the previous config kept, matching the reloader.
#[cfg(feature = "lifecycle")]
async fn wait_for_work<A: ServiceApp>(
    app: &A,
    mut config: A::Config,
    config_path: Option<&str>,
) -> Option<A::Config> {
    use crate::lifecycle::{GateWake, IdleGate, WorkState, wait_for_config_change};

    let mut gate = IdleGate::new();
    loop {
        match app.work_state(&config) {
            WorkState::Active => {
                gate.leave_idle();
                return Some(config);
            }
            WorkState::Idle(reason) => gate.enter_idle(&reason),
        }

        if wait_for_config_change(config_path.map(std::path::Path::new)).await
            == GateWake::ShuttingDown
        {
            tracing::info!("shutting down while idle -- no work was ever configured");
            return None;
        }

        match app.load_config(config_path) {
            Ok(reloaded) => config = reloaded,
            Err(e) => {
                tracing::warn!(error = %e, "config reload while idle failed, keeping the current config");
            }
        }
    }
}

/// Initialise the logger from CLI arguments.
#[cfg(feature = "logger")]
fn init_logger(args: &CommonArgs) -> Result<(), CliError> {
    let opts = args.to_logger_options()?;
    crate::logger::setup(opts)?;
    Ok(())
}

/// Initialise the logger with service name and version injected into JSON output.
#[cfg(feature = "logger")]
fn init_logger_for_service(
    args: &CommonArgs,
    service_name: &str,
    service_version: &str,
) -> Result<(), CliError> {
    let opts = args.to_logger_options()?;
    crate::logger::setup(crate::logger::LoggerOptions {
        service_name: Some(service_name.to_string()),
        service_version: Some(service_version.to_string()),
        ..opts
    })?;
    Ok(())
}

/// Initialise the logger from CLI arguments (no-op without logger feature).
#[cfg(not(feature = "logger"))]
fn init_logger(_args: &CommonArgs) -> Result<(), CliError> {
    Ok(())
}

/// Initialise the logger with service name and version (no-op without logger feature).
#[cfg(not(feature = "logger"))]
fn init_logger_for_service(
    _args: &CommonArgs,
    _service_name: &str,
    _service_version: &str,
) -> Result<(), CliError> {
    Ok(())
}

/// The manager a manifest subcommand describes into: offline, under the
/// namespace the running service takes from the same config.
///
/// The app's config is loaded first, best-effort, because the namespace lives
/// in the cascade that load populates. A load that fails is reported on stderr
/// and the default namespace taken, so stdout stays pure JSON.
#[cfg(any(feature = "metrics", feature = "otel-metrics"))]
fn manifest_manager<A: ServiceApp>(app: &A) -> crate::metrics::MetricsManager {
    if let Err(e) = app.load_config(app.common_args().config.as_deref()) {
        output::print_warn(&format!(
            "config did not load, so the manifest takes the default metrics namespace: {e}"
        ));
    }
    let namespace = crate::metrics::MetricsSettings::from_cascade().namespace;
    crate::metrics::MetricsManager::with_config(crate::metrics::MetricsConfig::offline(&namespace))
}

/// The manifest a service's subcommands publish: the scalo runtime set, then
/// whatever the app's [`ServiceApp::register_metrics`] adds.
///
/// An app that adds nothing is warned on stderr and the command still
/// succeeds, because the runtime set alone is a true manifest.
#[cfg(any(feature = "metrics", feature = "otel-metrics"))]
fn build_metrics_manifest<A: ServiceApp>(
    app: &A,
    mgr: &crate::metrics::MetricsManager,
) -> crate::metrics::ManifestResponse {
    let registry = mgr.registry();
    registry.set_app_name(app.name());
    let _service =
        super::runtime::register_runtime_metrics(mgr, &app.version_info().version, BUILD_COMMIT);
    let scalo_owned = registry.manifest().metrics.len();

    app.register_metrics(mgr);

    let manifest = registry.manifest();
    if manifest.metrics.len() == scalo_owned {
        output::print_warn(&format!(
            "`{}` describes no metrics of its own -- the manifest lists the scalo runtime set \
             only. Override ServiceApp::register_metrics to add the app's.",
            app.name()
        ));
    }
    manifest
}

/// Pretty JSON ending in one newline, the form every artefact scalo writes
/// takes, so a file regenerated by either command is byte-identical.
fn artefact_json<T: serde::Serialize>(value: &T, what: &str) -> Result<String, CliError> {
    serde_json::to_string_pretty(value)
        .map(ending_in_newline)
        .map_err(|e| CliError::Service(format!("{what} JSON failed: {e}")))
}

/// `text` with one trailing newline, added only when it lacks one.
fn ending_in_newline(mut text: String) -> String {
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text
}

/// Refuse a contract whose `default_config` binds a listener that no port
/// declares, or declares with another number, because the artefacts would
/// ship a pod whose listener nothing routes to.
#[cfg(feature = "deployment")]
fn refuse_undeclared_listeners(
    contract: &crate::deployment::DeploymentContract,
) -> Result<(), CliError> {
    let findings = contract.undeclared_listeners();
    if findings.is_empty() {
        return Ok(());
    }
    let lines: Vec<String> = findings.iter().map(|m| format!("  {m}")).collect();
    Err(CliError::Service(format!(
        "{app}: listeners and declared ports disagree, so no artefacts were written -- \
         declare a port with `bound_from` naming the listen path (with the port it binds), \
         or add a send-only path to `unbound_listen_paths`:\n{lines}",
        app = contract.app_name,
        lines = lines.join("\n"),
    )))
}

/// Generate all CI artefacts for this service.
///
/// Produces metrics manifest, deployment contract, and container spec
/// in the output directory. Files are deterministic -- running twice produces
/// identical output (no timestamps that change between runs). A contract with
/// an undeclared listener is refused before anything is written.
fn generate_artefacts<A: ServiceApp>(
    app: &A,
    args: &super::commands::GenerateArtefactsArgs,
) -> Result<(), CliError> {
    #[cfg(feature = "deployment")]
    let deployment_contract = app.deployment_contract();
    #[cfg(feature = "deployment")]
    if let Some(contract) = &deployment_contract {
        refuse_undeclared_listeners(contract)?;
    }

    let output_dir = std::path::Path::new(&args.output_dir);
    std::fs::create_dir_all(output_dir)
        .map_err(|e| CliError::Service(format!("failed to create output dir: {e}")))?;

    let mut generated: Vec<String> = Vec::new();

    // Metrics manifest
    #[cfg(any(feature = "metrics", feature = "otel-metrics"))]
    {
        let manifest = build_metrics_manifest(app, &manifest_manager(app));
        let path = output_dir.join("metrics-manifest.json");
        let json = artefact_json(&manifest, "metrics manifest")?;
        std::fs::write(&path, &json)
            .map_err(|e| CliError::Service(format!("failed to write {}: {e}", path.display())))?;
        generated.push(format!(
            "metrics-manifest.json ({} metrics)",
            manifest.metrics.len()
        ));
    }

    // Deployment contract + container manifest
    #[cfg(feature = "deployment")]
    if deployment_contract.is_none() {
        output::print_warn(&format!(
            "ServiceApp::deployment_contract() returned None for `{}` -- \
             only metrics-manifest.json will be generated. \
             Implement the trait hook to emit deployment-contract.json, \
             container-manifest.json, and Dockerfile.runtime.",
            app.name()
        ));
    }
    #[cfg(feature = "deployment")]
    if let Some(contract) = deployment_contract {
        // Full deployment contract (secrets, KEDA, Helm, everything)
        let path = output_dir.join("deployment-contract.json");
        let json = artefact_json(&contract, "deployment contract")?;
        std::fs::write(&path, &json)
            .map_err(|e| CliError::Service(format!("failed to write {}: {e}", path.display())))?;
        generated.push("deployment-contract.json".to_string());

        // Container manifest (minimal subset for CI image builds)
        let cm_path = output_dir.join("container-manifest.json");
        let cm_json = crate::deployment::generate::generate_container_manifest(&contract)
            .map(ending_in_newline)
            .map_err(|e| CliError::Service(format!("container manifest failed: {e}")))?;
        std::fs::write(&cm_path, &cm_json).map_err(|e| {
            CliError::Service(format!("failed to write {}: {e}", cm_path.display()))
        })?;
        generated.push("container-manifest.json".to_string());

        // Runtime stage Dockerfile fragment (for CI composition)
        let rt_path = output_dir.join("Dockerfile.runtime");
        let rt_content = crate::deployment::generate::generate_runtime_stage(&contract);
        std::fs::write(&rt_path, &rt_content).map_err(|e| {
            CliError::Service(format!("failed to write {}: {e}", rt_path.display()))
        })?;
        generated.push("Dockerfile.runtime".to_string());

        // ArgoCD Application CR (default generation -- ArgoCD is the
        // standard CD tool across the fleet).
        let argo_path = output_dir.join("argocd-application.yaml");
        let argo_cfg = crate::deployment::ArgocdConfig {
            repo_url: crate::deployment::argocd_repo_url_from_cascade(&contract.app_name),
            ..Default::default()
        };
        let argo_content =
            crate::deployment::generate::generate_argocd_application(&contract, &argo_cfg, None);
        std::fs::write(&argo_path, &argo_content).map_err(|e| {
            CliError::Service(format!("failed to write {}: {e}", argo_path.display()))
        })?;
        generated.push("argocd-application.yaml".to_string());

        // Reflectable config artefacts (config-schema.{json,yaml} +
        // capability-catalog.{json,yaml}). Emitted only when the contract carries a
        // config_schema and/or capabilities (scalo-rs#6). Same output as the
        // standalone `config-schema` subcommand, so the drift test is stable
        // whichever produced the committed copy.
        let cfg_written = crate::deployment::emit_config_artifacts(&contract, output_dir)
            .map_err(|e| CliError::Service(format!("config artefacts failed: {e}")))?;
        for p in &cfg_written {
            let name = p
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or("config-artefact");
            generated.push(name.to_string());
        }
    }

    if generated.is_empty() {
        output::print_warn("no artefacts generated (no metrics or deployment features enabled)");
    } else {
        output::print_success(&format!(
            "generated {} artefact(s) in {}",
            generated.len(),
            output_dir.display()
        ));
        for name in &generated {
            output::print_kv("  wrote", name);
        }
    }

    Ok(())
}

/// Emit just the reflectable config artefacts (`config-schema.*`,
/// `capability-catalog.*`) for the `config-schema` subcommand.
#[cfg(feature = "deployment")]
fn emit_config_schema<A: ServiceApp>(app: &A, dir: &str) -> Result<(), CliError> {
    let Some(contract) = app.deployment_contract() else {
        output::print_warn(&format!(
            "ServiceApp::deployment_contract() returned None for `{}` -- no config artefacts",
            app.name()
        ));
        return Ok(());
    };
    let written = crate::deployment::emit_config_artifacts(&contract, dir)
        .map_err(|e| CliError::Service(format!("config artefacts failed: {e}")))?;
    if written.is_empty() {
        output::print_warn(&format!(
            "contract for `{}` carries no config_schema or capabilities -- nothing emitted. \
             Populate `config_schema` + `capabilities` in the app's deployment_contract().",
            app.name()
        ));
    } else {
        output::print_success(&format!(
            "wrote {} config artefact(s) to {dir}",
            written.len()
        ));
        for p in &written {
            output::print_kv("  wrote", &p.display().to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ServiceRuntime;
    use crate::metrics::{ManifestResponse, MetricsManager, ServiceMetrics};

    #[test]
    fn test_standard_command_default_is_run() {
        // When command() returns None, run_app defaults to Run
        let cmd = StandardCommand::Run;
        assert!(matches!(cmd, StandardCommand::Run));
    }

    fn common() -> CommonArgs {
        CommonArgs {
            config: None,
            log_level: None,
            log_format: None,
            metrics_addr: None,
            verbose: false,
            quiet: false,
        }
    }

    /// A service that describes nothing of its own.
    struct BareApp {
        common: CommonArgs,
    }

    impl ServiceApp for BareApp {
        type Config = ();

        fn name(&self) -> &'static str {
            "bare-app"
        }
        fn env_prefix(&self) -> &'static str {
            "BARE_APP"
        }
        fn version_info(&self) -> VersionInfo {
            VersionInfo::new("bare-app", "1.2.3")
        }
        fn common_args(&self) -> &CommonArgs {
            &self.common
        }
        fn load_config(&self, _path: Option<&str>) -> Result<(), CliError> {
            Ok(())
        }
        fn run_service(
            &self,
            _config: (),
            _runtime: ServiceRuntime,
        ) -> impl std::future::Future<Output = Result<(), CliError>> + Send {
            std::future::ready(Ok(()))
        }
    }

    /// A service that describes a counter of its own and, as some consumers
    /// do, the service set a second time.
    struct ExtraApp {
        common: CommonArgs,
    }

    impl ServiceApp for ExtraApp {
        type Config = ();

        fn name(&self) -> &'static str {
            "extra-app"
        }
        fn env_prefix(&self) -> &'static str {
            "EXTRA_APP"
        }
        fn version_info(&self) -> VersionInfo {
            VersionInfo::new("extra-app", "1.2.3")
        }
        fn common_args(&self) -> &CommonArgs {
            &self.common
        }
        fn load_config(&self, _path: Option<&str>) -> Result<(), CliError> {
            Ok(())
        }
        fn run_service(
            &self,
            _config: (),
            _runtime: ServiceRuntime,
        ) -> impl std::future::Future<Output = Result<(), CliError>> + Send {
            std::future::ready(Ok(()))
        }
        fn register_metrics(&self, manager: &MetricsManager) {
            let _ = manager.counter("extra_widgets_total", "Widgets the app made");
            let _ = ServiceMetrics::register(manager);
        }
    }

    fn names(manifest: &ManifestResponse) -> Vec<&str> {
        manifest.metrics.iter().map(|m| m.name.as_str()).collect()
    }

    #[test]
    fn a_manifest_carries_the_runtime_set_when_the_app_adds_nothing() {
        let mgr = MetricsManager::new_for_test("");
        let manifest = build_metrics_manifest(&BareApp { common: common() }, &mgr);
        let names = names(&manifest);

        for expected in [
            "transport_sent_total",
            "pipeline_ready",
            "records_dlq_total",
        ] {
            assert!(names.contains(&expected), "{expected} missing: {names:?}");
        }
        #[cfg(feature = "worker-pool")]
        assert!(
            names.contains(&"worker_pool_active_threads"),
            "the pool set: {names:?}"
        );
        #[cfg(feature = "worker-batch")]
        assert!(
            names.contains(&"batch_engine_messages_received_total"),
            "the engine set: {names:?}"
        );
        assert_eq!(manifest.app, "bare-app");
        #[cfg(feature = "service-metrics")]
        assert_eq!(manifest.version, "1.2.3", "the app info set carries it");
    }

    #[test]
    fn a_manifest_lists_the_apps_own_metrics_and_every_name_once() {
        let mgr = MetricsManager::new_for_test("");
        let manifest = build_metrics_manifest(&ExtraApp { common: common() }, &mgr);
        let names = names(&manifest);

        assert!(names.contains(&"extra_widgets_total"), "{names:?}");
        assert!(names.contains(&"transport_sent_total"), "{names:?}");
        let mut once = names.clone();
        once.sort_unstable();
        once.dedup();
        assert_eq!(
            once.len(),
            names.len(),
            "a name described twice is listed once: {names:?}"
        );
    }

    #[test]
    fn every_artefact_ends_in_one_newline() {
        let json = artefact_json(&serde_json::json!({"a": 1}), "probe").unwrap();
        assert!(json.ends_with("}\n") && !json.ends_with("\n\n"), "{json:?}");
        assert_eq!(ending_in_newline("x\n".to_owned()), "x\n");
    }

    /// A service whose deployment contract is fixed when it is built.
    #[cfg(feature = "deployment")]
    struct ContractApp {
        common: CommonArgs,
        contract: crate::deployment::DeploymentContract,
    }

    #[cfg(feature = "deployment")]
    impl ServiceApp for ContractApp {
        type Config = ();

        fn name(&self) -> &'static str {
            "contract-app"
        }
        fn env_prefix(&self) -> &'static str {
            "CONTRACT_APP"
        }
        fn version_info(&self) -> VersionInfo {
            VersionInfo::new("contract-app", "1.2.3")
        }
        fn common_args(&self) -> &CommonArgs {
            &self.common
        }
        fn load_config(&self, _path: Option<&str>) -> Result<(), CliError> {
            Ok(())
        }
        fn run_service(
            &self,
            _config: (),
            _runtime: ServiceRuntime,
        ) -> impl std::future::Future<Output = Result<(), CliError>> + Send {
            std::future::ready(Ok(()))
        }
        fn deployment_contract(&self) -> Option<crate::deployment::DeploymentContract> {
            Some(self.contract.clone())
        }
    }

    /// An archiver-shaped service: a push listener that binds only on the grpc
    /// transport, with a null default address, and `extra_ports` beside metrics.
    #[cfg(feature = "deployment")]
    fn archiver_app(extra_ports: Vec<crate::deployment::PortContract>) -> ContractApp {
        use crate::deployment::{
            DeploymentContract, HealthContract, ImageProfile, NativeDepsContract, OciLabels,
        };
        ContractApp {
            common: common(),
            contract: DeploymentContract {
                schema_version: 3,
                app_name: "contract-app".into(),
                binary_name: String::new(),
                description: String::new(),
                metrics_port: 9090,
                health: HealthContract::default(),
                env_prefix: "CONTRACT_APP".into(),
                metric_prefix: "contract_app".into(),
                config_mount_path: "/etc/contract-app/config.yaml".into(),
                image_registry: "ghcr.io/hyperi-io".into(),
                extra_ports,
                unbound_listen_paths: vec![],
                entrypoint_args: vec![],
                secrets: vec![],
                default_config: Some(serde_json::json!({
                    "transport": "kafka",
                    "grpc": { "listen": null },
                })),
                depends_on: vec![],
                keda: None,
                base_image: "debian:trixie-slim".into(),
                native_deps: NativeDepsContract::default(),
                image_profile: ImageProfile::Production,
                oci_labels: OciLabels::default(),
                config_schema: None,
                capabilities: vec![],
            },
        }
    }

    #[cfg(feature = "deployment")]
    fn generate_into(app: &ContractApp, dir: &std::path::Path) -> Result<(), CliError> {
        let args = crate::cli::commands::GenerateArtefactsArgs {
            output_dir: dir.to_str().expect("a UTF-8 tempdir").to_owned(),
        };
        generate_artefacts(app, &args)
    }

    #[cfg(feature = "deployment")]
    #[test]
    fn generate_artefacts_refuses_an_undeclared_listener_before_writing() {
        let out = tempfile::tempdir().unwrap();
        let err = generate_into(&archiver_app(vec![]), out.path())
            .expect_err("an undeclared listener is refused");

        assert!(matches!(err, CliError::Service(_)), "{err:?}");
        let message = err.to_string();
        for part in [
            "listener grpc.listen",
            "a port whose bound_from names it",
            "no port declared for null",
            "unbound_listen_paths",
        ] {
            assert!(message.contains(part), "{part:?} missing from: {message}");
        }
        let written: Vec<_> = std::fs::read_dir(out.path()).unwrap().collect();
        assert!(
            written.is_empty(),
            "written before the refusal: {written:?}"
        );
    }

    #[cfg(feature = "deployment")]
    #[test]
    fn generate_artefacts_writes_once_the_listener_is_declared() {
        let push = crate::deployment::PortContract::tcp("push", 50051)
            .when_equals("config.transport", "grpc")
            .bound_from("grpc.listen");
        let out = tempfile::tempdir().unwrap();
        generate_into(&archiver_app(vec![push]), out.path()).expect("a declared listener passes");
        assert!(out.path().join("deployment-contract.json").is_file());
    }
}
