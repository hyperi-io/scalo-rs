// Project:   scalo
// File:      src/cli/runtime.rs
// Purpose:   ServiceRuntime -- pre-built infrastructure for data-plane service apps
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pre-built service infrastructure for data-plane pipeline applications.
//!
//! [`ServiceRuntime`] is created by [`super::run_app`] before calling
//! [`ServiceApp::run_service`]. Apps receive it fully wired -- eliminates
//! ~50 lines of identical boilerplate per consumer service.
//!
//! ## What's included (always)
//!
//! - [`MetricsManager`] -- started, serving `/metrics`, `/livez`, `/readyz`
//!   and the runtime's `/scaling/pressure`
//! - [`ServiceMetrics`] -- the platform data-plane metrics, registered under
//!   bare names or the `metrics.namespace` prefix
//! - [`MemoryGuard`] -- cgroup-aware, auto-detected from env prefix
//! - [`CancellationToken`] -- signal handler installed with K8s pre-stop delay
//! - [`RuntimeContext`] -- K8s/Docker/BareMetal metadata
//!
//! ## What's included (when features enabled)
//!
//! - [`AdaptiveWorkerPool`] -- rayon + tokio hybrid (`worker-pool` feature,
//!   which `worker-batch` includes)
//! - [`ScalingPressure`] -- KEDA signals (`scaling` feature)
//!
//! ## What stays app-specific
//!
//! - Readiness check criteria (each app defines "ready" differently)
//! - Config hot-reload (optional, app-specific reload logic)
//! - Pipeline creation (100% domain-specific)
//! - DLQ setup (varies per app)
//! - App-specific metric groups (ConsumerMetrics, BufferMetrics, etc.)

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::env::{RuntimeContext, runtime_context};
#[cfg(feature = "memory")]
use crate::memory::{MemoryGuard, MemoryGuardConfig};
use crate::metrics::MetricsManager;

use super::error::CliError;

/// Pre-built service infrastructure. Created by `run_app()` before `run_service()`.
///
/// Apps receive this fully wired -- they just use the fields. No boilerplate needed.
///
/// On bare metal, K8s-specific features (pre-stop delay, pod metadata in logs)
/// are automatically disabled. On K8s, they're automatically enabled.
pub struct ServiceRuntime {
    /// Metrics manager -- already started, serving endpoints.
    /// Use for registering app-specific metrics and metric groups.
    pub metrics: MetricsManager,

    /// Platform data-plane metrics (transport, pipeline, records, scaling,
    /// spool), under bare names or the `metrics.namespace` prefix. Already
    /// registered.
    pub dfe: Arc<crate::metrics::ServiceMetrics>,

    /// Cgroup-aware memory guard. Tracks memory usage for backpressure.
    /// Auto-detected from env prefix + cgroup limits.
    #[cfg(feature = "memory")]
    pub memory_guard: Arc<MemoryGuard>,

    /// Shutdown token. Cancelled on SIGTERM/SIGINT (with K8s pre-stop delay).
    /// Clone and pass to your pipeline loops.
    pub shutdown: CancellationToken,

    /// Runtime context -- K8s/Docker/BareMetal metadata (pod_name, namespace, etc.).
    pub context: &'static RuntimeContext,

    /// Adaptive worker pool for parallel batch processing (`worker-pool`
    /// feature, which `worker-batch` includes). `None` if the pool could not be
    /// built from its config.
    #[cfg(feature = "worker-pool")]
    pub worker_pool: Option<Arc<crate::worker::AdaptiveWorkerPool>>,

    /// Batch processing engine with SIMD parsing and pre-route filtering
    /// (`worker-batch` feature). `None` if `worker-batch` is not enabled
    /// or worker pool creation failed.
    #[cfg(feature = "worker-batch")]
    pub batch_engine: Option<Arc<crate::worker::BatchEngine>>,

    /// Scaling pressure calculator for KEDA autoscaling (`scaling` feature).
    /// `None` if the `scaling` feature is not enabled.
    #[cfg(feature = "scaling")]
    pub scaling: Option<Arc<crate::ScalingPressure>>,

    /// Self-regulation governor (`governor` feature). Default-ON, opt-out via
    /// `self_regulation.enabled = false`. `None` when disabled -- nothing is
    /// constructed and the data path is byte-identical to pre-governor.
    ///
    /// Thread [`pressure`](crate::SelfRegulationGovernor::pressure) into your
    /// receive transports' inbound gate / `with_pressure` hooks. The
    /// [`budget`](crate::SelfRegulationGovernor::budget) is already wired into
    /// the [`batch_engine`](Self::batch_engine) governed run path.
    #[cfg(feature = "governor")]
    pub governor: Option<crate::SelfRegulationGovernor>,
}

impl ServiceRuntime {
    /// Build the service runtime from app configuration.
    ///
    /// This is called by `run_app()` -- apps don't call it directly.
    ///
    /// # Errors
    ///
    /// Returns `CliError` if the metrics server fails to start.
    pub(crate) async fn build(
        app_name: &str,
        env_prefix: &str,
        metrics_addr: &str,
        version: &str,
        commit: &str,
        #[cfg(feature = "scaling")] scaling_components: Vec<crate::ScalingComponent>,
        #[cfg(feature = "version-check")] version_check_defaults: crate::VersionCheckConfig,
    ) -> Result<Self, CliError> {
        let ctx = runtime_context();

        // --- Metrics ---
        // Namespace is DECOUPLED from app_name: sourced from the `metrics`
        // config section (bare by default), NOT forced to the app name. Metric
        // names are bare unless the consumer opts into a `{namespace}_` prefix
        // via `metrics.namespace`; services are differentiated by platform
        // labels (Prometheus job/instance, OTel service.name), not the name.
        //
        // The whole `metrics` section is carried through, not just the
        // namespace: `metrics.otel.*` configures the OTLP push, and the app
        // name supplies `service.name` when config leaves it unset.
        let metrics_config = crate::metrics::MetricsSettings::from_cascade().into_config(app_name);
        let mut metrics = MetricsManager::with_config(metrics_config);
        metrics.registry().set_app_name(app_name);
        let dfe = Arc::new(register_runtime_metrics(&metrics, version, commit));

        // --- Memory guard ---
        #[cfg(feature = "memory")]
        let memory_guard = Arc::new(MemoryGuard::new(MemoryGuardConfig::from_env(env_prefix)));

        // --- Self-regulation governor (default-ON, opt-out) ---
        //
        // Constructed HERE -- before the worker pool, batch engine, and the
        // transports the app builds in run_service() -- so the shared pressure
        // and byte budget can be threaded into all of them. When
        // `self_regulation.enabled = false`, `build` returns None and nothing
        // is constructed: every downstream Option stays None and the data path
        // is byte-identical to pre-governor behaviour.
        #[cfg(feature = "governor")]
        let governor = crate::SelfRegulationConfig::from_cascade().build(Arc::clone(&memory_guard));

        // --- Scaling pressure ---
        #[cfg(feature = "scaling")]
        let scaling = {
            let config = crate::ScalingPressureConfig::from_cascade();
            let pressure = Arc::new(crate::ScalingPressure::new(config, scaling_components));
            metrics.set_scaling_pressure(Arc::clone(&pressure));
            Some(pressure)
        };

        // --- Worker pool ---
        #[cfg(feature = "worker-pool")]
        let worker_pool = {
            match crate::worker::AdaptiveWorkerPool::from_cascade("worker_pool") {
                Ok(pool) => {
                    let pool = Arc::new(pool);
                    pool.register_metrics(&metrics);
                    #[cfg(feature = "memory")]
                    pool.set_memory_guard(Arc::clone(&memory_guard));
                    #[cfg(feature = "scaling")]
                    if let Some(ref sp) = scaling {
                        pool.set_scaling_pressure(Arc::clone(sp));
                    }
                    tracing::info!(
                        max_threads = pool.max_threads(),
                        "Adaptive worker pool enabled"
                    );
                    Some(pool)
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Worker pool not configured, falling back to sequential"
                    );
                    None
                }
            }
        };

        // --- Batch engine (worker-batch tier only) ---
        #[cfg(feature = "worker-batch")]
        let batch_engine = {
            if let Some(ref pool) = worker_pool {
                let config =
                    crate::worker::engine::BatchProcessingConfig::from_cascade("batch_processing")
                        .unwrap_or_default();
                let mut engine = crate::worker::BatchEngine::with_pool(Arc::clone(pool), config);
                engine.auto_wire(
                    &metrics,
                    #[cfg(feature = "memory")]
                    Some(&memory_guard),
                );
                // Wire the governor's byte-budget lever so the engine's governed
                // run path streams in budget-sized sub-blocks. None (governor
                // off) leaves the engine on the whole-batch loop.
                #[cfg(feature = "governor")]
                if let Some(ref gov) = governor {
                    engine.set_byte_budget(gov.budget());
                }
                Some(Arc::new(engine))
            } else {
                None
            }
        };

        // --- Shutdown ---
        let shutdown = crate::shutdown::install_signal_handler();

        // Start worker pool scaling loop after shutdown token exists
        #[cfg(feature = "worker-pool")]
        if let Some(ref pool) = worker_pool {
            pool.start_scaling_loop(shutdown.clone());
        }

        // --- Start metrics server ---
        if let Err(e) = metrics.start_server(metrics_addr).await {
            tracing::error!(error = %e, addr = metrics_addr, "Failed to start metrics server");
        }

        // --- Version check (fire-and-forget) ---
        // The app's defaults (its releases endpoint) sit UNDER the cascade,
        // so a deployment's explicit version_check.* keys always win.
        #[cfg(feature = "version-check")]
        {
            crate::VersionCheck::new(crate::VersionCheckConfig::from_cascade_or(
                app_name,
                version,
                version_check_defaults,
            ))
            .check_on_startup();
        }

        // Log runtime context
        tracing::info!(
            environment = %ctx.environment,
            pod_name = ?ctx.pod_name,
            namespace = ?ctx.namespace,
            "Service runtime initialised"
        );

        Ok(Self {
            metrics,
            dfe,
            #[cfg(feature = "memory")]
            memory_guard,
            shutdown,
            context: ctx,
            #[cfg(feature = "worker-pool")]
            worker_pool,
            #[cfg(feature = "worker-batch")]
            batch_engine,
            #[cfg(feature = "scaling")]
            scaling,
            #[cfg(feature = "governor")]
            governor,
        })
    }

    /// Set the readiness check callback.
    ///
    /// Each app defines its own readiness criteria. Call this in `run_service()`
    /// once you know what "ready" means for your app: the metrics listener is
    /// already serving by then, and the callback is picked up regardless.
    pub fn set_readiness_check<F: Fn() -> bool + Send + Sync + 'static>(&mut self, check: F) {
        self.metrics.set_readiness_check(check);
    }

    /// Return the batch processing engine, if the `worker-batch` feature is
    /// enabled and the worker pool was successfully created.
    #[cfg(feature = "worker-batch")]
    #[must_use]
    pub fn batch_engine(&self) -> Option<&Arc<crate::worker::BatchEngine>> {
        self.batch_engine.as_ref()
    }

    /// Build a governed receive transport from config in ONE call
    /// (`governor` + `transport` features).
    ///
    /// Reads the transport config at `key` and threads the runtime's
    /// [`governor`](Self::governor) pressure into the receiver's inbound brake
    /// (Kafka pause-partitions gate, HTTP/gRPC 503/`unavailable` shed) so apps
    /// skip the `gate_actuator -> InboundGate -> with_inbound_gate` dance.
    ///
    /// When the governor is disabled (`self_regulation.enabled = false`,
    /// [`governor`](Self::governor) is `None`) this falls back to the plain
    /// [`AnyReceiver::from_config`](crate::transport::factory::AnyReceiver::from_config)
    /// -- data path stays byte-identical to pre-governor.
    ///
    /// # Errors
    ///
    /// Returns the underlying transport error if the config is missing/invalid
    /// or the backend fails to construct.
    #[cfg(all(feature = "governor", feature = "transport"))]
    pub async fn governed_receiver(
        &self,
        key: &str,
    ) -> Result<crate::transport::factory::AnyReceiver, crate::transport::TransportError> {
        use crate::transport::factory::AnyReceiver;
        match self.governor {
            Some(ref gov) => AnyReceiver::from_config_with_governor(key, gov).await,
            None => AnyReceiver::from_config(key).await,
        }
    }

    /// Build the OUTBOUND sender wrapped in a [`SinkStack`](crate::sink_stack::SinkStack)
    /// -- the symmetric, default-on companion to [`governed_receiver`](Self::governed_receiver).
    ///
    /// This is how an app gets retry/backoff + per-attempt timeout (and,
    /// opt-in via config, adaptive concurrency + rate-limit) on its outbound send
    /// for free, instead of hand-rolling them. The
    /// [`SinkStackConfig`](crate::sink_stack::SinkStackConfig) is read
    /// from the cascade at `cfg_key` (e.g. `"sink_stack"`); the stack's defaults
    /// preserve at-least-once (whole-batch retry on a transient failure, never an
    /// ack before the sink confirms). Use the returned stack as the driver's sink
    /// via [`SinkStack::send_workbatch`](crate::sink_stack::SinkStack::send_workbatch).
    ///
    /// To opt OUT, build a bare [`AnySender::from_config`](crate::transport::factory::AnySender::from_config)
    /// directly, or set `max_retries = 0` (one attempt, no controls).
    ///
    /// # Errors
    /// Returns the transport error if the sender config is missing/invalid or the
    /// backend fails to construct.
    #[cfg(all(feature = "sink-stack", feature = "transport"))]
    pub async fn outbound_sink_stack(
        &self,
        sender_key: &str,
        cfg_key: &str,
    ) -> Result<crate::sink_stack::SinkStack, crate::transport::TransportError> {
        use crate::sink_stack::{SinkStack, SinkStackConfig};
        use crate::transport::factory::AnySender;
        let sender = std::sync::Arc::new(AnySender::from_config(sender_key).await?);
        let cfg = SinkStackConfig::from_cascade_key(cfg_key);
        Ok(SinkStack::new(sender, &cfg))
    }
}

/// Describe every metric the scalo runtime emits into `manager`: the service
/// set, the app info set, and the worker pool and batch engine sets when their
/// features are compiled in.
///
/// The one list the running service and the `metrics-manifest` and
/// `generate-artefacts` subcommands all describe, so a manifest lists the
/// runtime set whether or not the app describes anything of its own. The
/// process, container, HTTP client and memory guard gauges are served but not
/// described here, so they are not in the manifest (scalo-rs#137).
#[must_use]
pub(crate) fn register_runtime_metrics(
    manager: &MetricsManager,
    #[cfg_attr(not(feature = "service-metrics"), allow(unused_variables))] version: &str,
    #[cfg_attr(not(feature = "service-metrics"), allow(unused_variables))] commit: &str,
) -> crate::metrics::ServiceMetrics {
    let service = crate::metrics::ServiceMetrics::register(manager);

    #[cfg(feature = "service-metrics")]
    {
        let _app_metrics = crate::metrics::groups::AppMetrics::new(manager, version, commit);
    }

    #[cfg(feature = "worker-pool")]
    crate::worker::metrics::describe(manager);

    #[cfg(feature = "worker-batch")]
    crate::worker::engine::metrics::describe(manager);

    service
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The runtime builds under whichever pool features are compiled in, and
    /// its pool metrics are described exactly when those features are on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn service_runtime_builds_under_current_features() {
        let runtime = ServiceRuntime::build(
            "runtime-build-probe",
            "RUNTIME_BUILD_PROBE",
            "127.0.0.1:0",
            "1.2.3",
            "probe",
            #[cfg(feature = "scaling")]
            Vec::new(),
            #[cfg(feature = "version-check")]
            crate::VersionCheckConfig::default(),
        )
        .await
        .expect("the runtime builds");

        let manifest = runtime.metrics.registry().manifest();
        let names: Vec<&str> = manifest.metrics.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"transport_sent_total"), "{names:?}");
        assert_eq!(
            names.contains(&"worker_pool_active_threads"),
            cfg!(feature = "worker-pool"),
            "pool metrics follow the worker-pool feature: {names:?}"
        );
        assert_eq!(
            names.contains(&"batch_engine_messages_received_total"),
            cfg!(feature = "worker-batch"),
            "engine metrics follow the worker-batch feature: {names:?}"
        );
        #[cfg(feature = "worker-pool")]
        assert!(
            runtime.worker_pool.is_some(),
            "default config builds a pool"
        );
        #[cfg(feature = "worker-batch")]
        assert!(runtime.batch_engine.is_some(), "and an engine on it");

        runtime.shutdown.cancel();
    }

    /// KEDA reads `/scaling/pressure` from the metrics listener `build` starts,
    /// so the runtime's own pressure has to answer a real request on it.
    #[cfg(feature = "scaling")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metrics_listener_serves_the_runtime_scaling_pressure() {
        let addr = free_local_addr();
        let runtime = ServiceRuntime::build(
            "scaling-route-probe",
            "SCALING_ROUTE_PROBE",
            &addr,
            "1.2.3",
            "probe",
            vec![crate::ScalingComponent::new("probe", 1.0, 100.0)],
            #[cfg(feature = "version-check")]
            crate::VersionCheckConfig::default(),
        )
        .await
        .expect("the runtime builds");

        runtime
            .scaling
            .as_ref()
            .expect("the scaling feature builds a pressure")
            .set_component("probe", 42.0);

        let (status, body) = get(&addr, "/scaling/pressure").await;
        assert_eq!(
            status, 200,
            "KEDA's signal must be reachable, got body {body:?}"
        );
        assert_eq!(body, "42.00");

        runtime.shutdown.cancel();
    }

    /// A loopback address with a port nothing is bound to.
    #[cfg(feature = "scaling")]
    fn free_local_addr() -> String {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
        probe.local_addr().expect("the bound address").to_string()
    }

    /// One HTTP/1.1 GET, returning the status code and the body.
    #[cfg(feature = "scaling")]
    async fn get(addr: &str, path: &str) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to the metrics listener");
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("send the request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("read the response");

        let (head, body) = response.split_once("\r\n\r\n").expect("a head and a body");
        let status = head
            .split(' ')
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("a status code");
        (status, body.to_string())
    }
}
