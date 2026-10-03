// Project:   scalo
// File:      src/http_server/server.rs
// Purpose:   HTTP server implementation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! HTTP server implementation using axum.

use crate::http_server::{HttpServerConfig, HttpServerError, Result};
use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpListener;
#[cfg(not(feature = "shutdown"))]
use tokio::signal;
use tokio::sync::watch;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

/// High-performance HTTP server built on axum.
///
/// Provides graceful shutdown, health endpoints, and Tower middleware support.
pub struct HttpServer {
    config: HttpServerConfig,
    ready: Arc<AtomicBool>,
}

impl HttpServer {
    /// Create a new HTTP server with the given configuration.
    #[must_use]
    pub fn new(config: HttpServerConfig) -> Self {
        let ready = Arc::new(AtomicBool::new(true));

        #[cfg(feature = "health")]
        {
            let r = Arc::clone(&ready);
            crate::health::HealthRegistry::register("http_server", move || {
                if r.load(Ordering::Relaxed) {
                    crate::health::HealthStatus::Healthy
                } else {
                    crate::health::HealthStatus::Unhealthy
                }
            });
        }

        Self { config, ready }
    }

    /// Create a new HTTP server bound to the specified address.
    #[must_use]
    pub fn bind(address: impl Into<String>) -> Self {
        Self::new(HttpServerConfig::new(address))
    }

    /// Set the readiness state for the /readyz endpoint.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    /// Get the current readiness state.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Get a clone of the readiness flag for use in application state.
    #[must_use]
    pub fn ready_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.ready)
    }

    /// Serve the given router until SIGTERM/SIGINT, then drain gracefully.
    ///
    /// # Errors
    ///
    /// Returns an error if binding fails or the server encounters an error.
    pub async fn serve(self, app: Router) -> Result<()> {
        #[cfg(feature = "shutdown")]
        {
            let token = crate::shutdown::install_signal_handler();
            self.serve_with_shutdown(app, token.cancelled_owned()).await
        }
        #[cfg(not(feature = "shutdown"))]
        {
            self.serve_with_shutdown(app, shutdown_signal()).await
        }
    }

    /// Serve with a custom shutdown signal.
    ///
    /// This is useful for testing or when you need custom shutdown logic.
    ///
    /// # Errors
    ///
    /// Returns an error if binding fails or the server encounters an error.
    pub async fn serve_with_shutdown<F>(self, app: Router, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        // Enforce config validity before binding: a
        // config requesting unsupported in-process TLS must fail loudly here,
        // not bind cleartext while is_tls_enabled() reports true.
        self.config.validate().map_err(HttpServerError::TlsConfig)?;
        let shutdown_timeout = self.config.shutdown_timeout();
        let app = self.build_router(app);

        let addr: SocketAddr =
            self.config
                .bind_address
                .parse()
                .map_err(|e| HttpServerError::Bind {
                    address: self.config.bind_address.clone(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                })?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| HttpServerError::Bind {
                address: self.config.bind_address.clone(),
                source: e,
            })?;

        #[cfg(feature = "logger")]
        tracing::info!(address = %addr, "HTTP server listening");

        // Two-phase: unbounded wait for shutdown, then bounded drain.
        // If drain exceeds shutdown_timeout, drop the serve future
        // so K8s terminationGracePeriodSeconds isn't blown.
        //
        // Flip ready -> false BEFORE notifying drain start so
        // /readyz returns 503 the moment shutdown is signalled.
        // K8s endpoint controller catches the 503 and stops routing
        // before in-flight requests finish draining.
        let (drain_started_tx, drain_started_rx) = tokio::sync::oneshot::channel();
        let drain_started_tx = std::sync::Mutex::new(Some(drain_started_tx));
        let ready_for_signal = Arc::clone(&self.ready);
        let signal = async move {
            shutdown.await;
            ready_for_signal.store(false, Ordering::SeqCst);
            if let Some(tx) = drain_started_tx.lock().ok().and_then(|mut g| g.take()) {
                let _ = tx.send(());
            }
        };
        // WithGracefulShutdown is IntoFuture, not Future.
        let serve = axum::serve(listener, app)
            .with_graceful_shutdown(signal)
            .into_future();
        tokio::pin!(serve);

        tokio::select! {
            result = &mut serve => result.map_err(HttpServerError::Io)?,
            () = async {
                let _ = drain_started_rx.await;
                tokio::time::sleep(shutdown_timeout).await;
            } => {
                #[cfg(feature = "logger")]
                tracing::warn!(
                    timeout_ms = u64::try_from(shutdown_timeout.as_millis()).unwrap_or(u64::MAX),
                    "HTTP server graceful drain timed out -- forcing exit"
                );
            }
        }

        #[cfg(feature = "logger")]
        tracing::info!("HTTP server shut down gracefully");

        Ok(())
    }

    /// Serve and return a handle for programmatic shutdown.
    ///
    /// Returns a `ShutdownHandle` that can be used to trigger shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if binding fails.
    pub async fn serve_with_handle(self, app: Router) -> Result<(ShutdownHandle, ServerFuture)> {
        // Same validation gate as serve_with_shutdown.
        self.config.validate().map_err(HttpServerError::TlsConfig)?;
        let (tx, rx) = watch::channel(());
        let handle = ShutdownHandle { sender: tx };

        let shutdown_timeout = self.config.shutdown_timeout();
        let app = self.build_router(app);

        let addr: SocketAddr =
            self.config
                .bind_address
                .parse()
                .map_err(|e| HttpServerError::Bind {
                    address: self.config.bind_address.clone(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                })?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| HttpServerError::Bind {
                address: self.config.bind_address.clone(),
                source: e,
            })?;

        #[cfg(feature = "logger")]
        tracing::info!(address = %addr, "HTTP server listening");

        // Two-phase shutdown, matching `serve_with_shutdown`.
        // Flip ready -> false before notifying drain start.
        let (drain_started_tx, drain_started_rx) = tokio::sync::oneshot::channel();
        let drain_started_tx = std::sync::Mutex::new(Some(drain_started_tx));
        let ready_for_signal = Arc::clone(&self.ready);
        let signal = async move {
            let _ = rx.clone().changed().await;
            ready_for_signal.store(false, Ordering::SeqCst);
            if let Some(tx) = drain_started_tx.lock().ok().and_then(|mut g| g.take()) {
                let _ = tx.send(());
            }
        };

        let future = ServerFuture {
            inner: Box::pin(async move {
                let serve = axum::serve(listener, app)
                    .with_graceful_shutdown(signal)
                    .into_future();
                tokio::pin!(serve);

                tokio::select! {
                    result = &mut serve => result.map_err(HttpServerError::Io)?,
                    () = async {
                        let _ = drain_started_rx.await;
                        tokio::time::sleep(shutdown_timeout).await;
                    } => {
                        #[cfg(feature = "logger")]
                        tracing::warn!(
                            timeout_ms = u64::try_from(shutdown_timeout.as_millis()).unwrap_or(u64::MAX),
                            "HTTP server graceful drain timed out -- forcing exit"
                        );
                    }
                }

                Ok(())
            }),
        };

        Ok((handle, future))
    }

    /// Build the final router with optional health endpoints + middleware.
    ///
    /// Applies `tower_http::TraceLayer` (per-request `tracing` spans, default
    /// DEBUG level so probe traffic doesn't flood at INFO) and
    /// `tower_http::TimeoutLayer` (per-request deadline from
    /// [`HttpServerConfig::request_timeout`]). The layers wrap both the
    /// user-supplied router and the framework probe/admin routes.
    fn build_router(&self, app: Router) -> Router {
        let mut router = app;

        if self.config.enable_health_endpoints {
            // /livez and /readyz are the whole surface -- no aliases. A second
            // path meaning the same thing eventually stops meaning the same
            // thing, and an alias that keeps answering 200 hides a probe still
            // aimed at a retired name. A startupProbe targets /livez:
            // Kubernetes suspends liveness until it passes, so one path gives
            // both a generous boot budget and a tight liveness period.
            let ready = Arc::clone(&self.ready);
            router = router
                .route("/livez", get(health_live))
                .route("/readyz", get(move || health_ready(Arc::clone(&ready))));
        }

        #[cfg(all(feature = "health", feature = "serde_json"))]
        if self.config.enable_health_endpoints {
            router = router.route("/health/detailed", get(health_detailed));
        }

        #[cfg(feature = "config")]
        if self.config.enable_config_endpoint {
            router = router.route("/config", get(config_dump));
        }

        // Every route, extractor or not, gets the cap, so a custom extractor or
        // a streamed body cannot buffer past it.
        if self.config.max_body_bytes > 0 {
            router = router.layer(RequestBodyLimitLayer::new(self.config.max_body_bytes));
        }

        router
            .layer(TraceLayer::new_for_http())
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                self.config.request_timeout(),
            ))
            // Cap in-flight requests. Queue fills under load;
            // upstream should see backpressure via send-timeout.
            .layer(ConcurrencyLimitLayer::new(self.config.max_connections))
    }
}

/// Handle for triggering server shutdown.
#[derive(Clone)]
pub struct ShutdownHandle {
    sender: watch::Sender<()>,
}

impl ShutdownHandle {
    /// Trigger graceful shutdown.
    pub fn shutdown(self) {
        let _ = self.sender.send(());
    }
}

/// Future representing the running server.
pub struct ServerFuture {
    inner: std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>,
}

impl std::future::Future for ServerFuture {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

/// Liveness endpoint handler.
async fn health_live() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Readiness endpoint handler.
///
/// Checks the local ready flag AND (when the `health` feature is enabled)
/// the global [`HealthRegistry`](crate::health::HealthRegistry). Both must
/// be true for a 200 response; otherwise 503.
async fn health_ready(ready: Arc<AtomicBool>) -> impl IntoResponse {
    let locally_ready = ready.load(Ordering::SeqCst);

    #[cfg(feature = "health")]
    let registry_ready = crate::health::HealthRegistry::is_ready();
    #[cfg(not(feature = "health"))]
    let registry_ready = true;

    if locally_ready && registry_ready {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT READY")
    }
}

/// Detailed health endpoint returning per-component status as JSON.
///
/// Returns the output of [`HealthRegistry::to_json()`](crate::health::HealthRegistry::to_json),
/// which includes overall status and each registered component's state.
#[cfg(all(feature = "health", feature = "serde_json"))]
async fn health_detailed() -> impl IntoResponse {
    let json = crate::health::HealthRegistry::to_json();
    axum::Json(json)
}

/// Config registry dump endpoint handler (redacted).
#[cfg(feature = "config")]
async fn config_dump() -> impl IntoResponse {
    let effective = crate::config::registry::dump_effective();
    let defaults = crate::config::registry::dump_defaults();

    let body = serde_json::json!({
        "effective": effective,
        "defaults": defaults,
        "sections": crate::config::registry::sections()
            .iter()
            .map(|s| serde_json::json!({
                "key": s.key,
                "type": s.type_name,
            }))
            .collect::<Vec<_>>(),
    });

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&body).unwrap_or_default(),
    )
}

/// Wait for a shutdown signal (SIGTERM or SIGINT).
///
/// Used as fallback when the `shutdown` feature is not enabled.
#[cfg(not(feature = "shutdown"))]
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }

    #[cfg(feature = "logger")]
    tracing::info!("Shutdown signal received, starting graceful shutdown");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// A route that records whether it ran, behind a server with this body cap.
    fn capped_ingest(max_body_bytes: usize) -> (Router, Arc<AtomicBool>) {
        let ran = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&ran);
        let handler = move |body: axum::body::Bytes| {
            seen.store(true, Ordering::SeqCst);
            async move { body.len().to_string() }
        };
        let server = HttpServer::new(HttpServerConfig {
            max_body_bytes,
            ..HttpServerConfig::default()
        });
        let app = server.build_router(Router::new().route("/ingest", axum::routing::post(handler)));
        (app, ran)
    }

    fn post_ingest(len: usize) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/ingest")
            .header("content-length", len)
            .body(Body::from(vec![b'x'; len]))
            .unwrap()
    }

    #[tokio::test]
    async fn a_body_over_the_limit_is_refused_before_the_handler_runs() {
        let (app, ran) = capped_ingest(16);

        let over = app.clone().oneshot(post_ingest(32)).await.unwrap();
        assert_eq!(over.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            !ran.load(Ordering::SeqCst),
            "the handler ran for a refused body"
        );

        let under = app.oneshot(post_ingest(16)).await.unwrap();
        assert_eq!(under.status(), StatusCode::OK);
        assert!(ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_zero_limit_turns_the_cap_off() {
        let (app, ran) = capped_ingest(0);
        let response = app.oneshot(post_ingest(64 * 1024)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_health_live() {
        let config = HttpServerConfig::default();
        let server = HttpServer::new(config);
        let app = server.build_router(Router::new());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_ready_when_ready() {
        #[cfg(feature = "health")]
        crate::health::HealthRegistry::reset();

        let config = HttpServerConfig::default();
        let server = HttpServer::new(config);
        server.set_ready(true);
        let app = server.build_router(Router::new());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_ready_when_not_ready() {
        let config = HttpServerConfig::default();
        let server = HttpServer::new(config);
        server.set_ready(false);
        let app = server.build_router(Router::new());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_server_with_handle() {
        // Test the handle API works with an actual server
        let config = HttpServerConfig::new("127.0.0.1:0");
        let server = HttpServer::new(config);

        let app = Router::new().route("/", get(|| async { "Hello" }));

        // Test the handle API compiles and works
        let (handle, future) = server.serve_with_handle(app).await.unwrap();

        // Shutdown immediately
        handle.shutdown();

        // Wait for server to finish
        future.await.unwrap();
    }

    /// Kaz #38: the K8s-standard `/livez` and `/readyz` paths must be mounted.
    #[tokio::test]
    async fn k8s_standard_health_paths_are_mounted() {
        let config = HttpServerConfig::default();
        let server = HttpServer::new(config);
        let app = server.build_router(Router::new());

        for path in &["/livez", "/readyz"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(*path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "path={path}");
        }
    }

    /// Retired paths are GONE, not quietly still answering.
    ///
    /// An alias that keeps returning 200 hides a probe still aimed at the old
    /// name, which is how a chart and an app drift apart unnoticed.
    #[tokio::test]
    async fn retired_health_paths_are_not_mounted() {
        let config = HttpServerConfig::default();
        let server = HttpServer::new(config);
        let app = server.build_router(Router::new());

        for path in &[
            "/healthz",
            "/health/live",
            "/health/ready",
            "/health/startup",
            "/startupz",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(*path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "path={path}");
        }
    }

    /// Regression: shutdown signal flips the readiness
    /// flag so /readyz returns 503 before the drain window
    /// completes. K8s endpoint controller stops routing on the 503.
    #[tokio::test]
    async fn shutdown_signal_flips_ready_before_drain() {
        let config = HttpServerConfig::new("127.0.0.1:0");
        let server = HttpServer::new(config);
        let ready = server.ready_flag();
        assert!(ready.load(Ordering::SeqCst), "ready starts true");

        let app = Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                "done"
            }),
        );

        let (handle, future) = server.serve_with_handle(app).await.unwrap();
        let server_task = tokio::spawn(future);

        // Trigger shutdown -- handle.shutdown() drops the watch sender.
        handle.shutdown();

        // Allow the signal future to observe + flip readiness.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !ready.load(Ordering::SeqCst),
            "ready must flip to false post-shutdown",
        );

        let _ = server_task.await;
    }

    #[test]
    fn test_ready_flag() {
        let config = HttpServerConfig::default();
        let server = HttpServer::new(config);

        assert!(server.is_ready());
        server.set_ready(false);
        assert!(!server.is_ready());
        server.set_ready(true);
        assert!(server.is_ready());
    }
}
