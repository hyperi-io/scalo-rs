// Project:   scalo
// File:      tests/proxy_refusal.rs
// Purpose:   Every proxied transport against a real Squid that refuses CONNECT
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A proxy that refuses a CONNECT holds the records.
//!
//! NO mocks: the run's Squid container opens tunnels to port 443 only (403) or
//! asks for credentials the sender lacks or gets wrong (407). Each transport sends to a
//! target on port 8443 through it, which Squid refuses before it resolves the
//! name, so no target runs. The records come back `Backpressured` instead of
//! `Reject`, which a caller would dead-letter. The refusal counts in
//! `transport_proxy_refused_total` and fails readiness, and the log carries the
//! proxy's status once.
//!
//! They need a Docker daemon on the machine running them: without one they skip
//! on a developer's machine and fail when `CI` is set.
//!
//! ```text
//! cargo nextest run --features transport-proxy-test-support,metrics,health,transport-grpc,transport-http,transport-otlp,transport-syslog --test proxy_refusal --no-capture
//! ```

#![cfg(all(
    unix,
    feature = "transport-proxy-test-support",
    feature = "metrics",
    feature = "health",
    feature = "transport-grpc",
    feature = "transport-http",
    feature = "transport-otlp",
    feature = "transport-syslog"
))]

use bytes::Bytes;
use scalo::health::{HealthRegistry, HealthStatus};
use scalo::sensitive::SensitiveString;
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::http::{HttpTransport, HttpTransportConfig};
use scalo::transport::otlp::{OtlpConfig, OtlpProtocol, OtlpTransport};
use scalo::transport::proxy::Scheme;
use scalo::transport::proxy::squid::{Squid, TARGET_HOST, USER};
use scalo::transport::syslog::{SyslogConfig, SyslogProtocol, SyslogTransport};
use scalo::transport::{
    PayloadFormat, ProxyConfig, ProxyError, Record, RecordMeta, SendResult, TransportSender,
};

/// The port every transport sends to, which the proxy refuses a tunnel to.
const TARGET_PORT: u16 = 8443;

/// Why the proxy refuses the CONNECT.
#[derive(Debug, Clone, Copy)]
enum Refusal {
    /// A tunnel to port 443 only, so any other port is 403.
    PortOnly,
    /// Credentials wanted and none sent, 407.
    AuthRequired,
    /// Credentials sent with the wrong password, 407.
    WrongPassword,
}

impl Refusal {
    const ALL: [Self; 3] = [Self::PortOnly, Self::AuthRequired, Self::WrongPassword];

    fn status(self) -> u16 {
        match self {
            Self::PortOnly => 403,
            Self::AuthRequired | Self::WrongPassword => 407,
        }
    }

    fn proxy(self, squid: &Squid) -> String {
        match self {
            Self::PortOnly => squid.port_only(),
            Self::AuthRequired => squid.auth(),
            Self::WrongPassword => squid.auth_with(USER, &format!("{}-wrong", squid.password())),
        }
    }
}

/// A transport under test, and the name its readiness check registers under, if any.
#[derive(Debug, Clone, Copy)]
enum Case {
    Grpc,
    Http,
    OtlpGrpc,
    OtlpHttp,
    Syslog(SyslogProtocol),
}

impl Case {
    const ALL: [Self; 6] = [
        Self::Grpc,
        Self::Http,
        Self::OtlpGrpc,
        Self::OtlpHttp,
        Self::Syslog(SyslogProtocol::Tcp),
        Self::Syslog(SyslogProtocol::Relp),
    ];

    /// The `transport` label its metrics carry.
    fn label(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Http => "http",
            Self::OtlpGrpc | Self::OtlpHttp => "otlp",
            Self::Syslog(_) => "syslog",
        }
    }

    /// The component its readiness check registers in the health registry.
    fn component(self) -> Option<&'static str> {
        match self {
            Self::Grpc => Some("transport:grpc"),
            Self::Http => Some("transport:http"),
            Self::OtlpGrpc | Self::OtlpHttp => Some("transport:otlp"),
            Self::Syslog(_) => None,
        }
    }
}

/// What a transport answered to two sends through a proxy, and its readiness after them.
struct Outcome {
    single: SendResult,
    batch: SendResult,
    ready: bool,
    registered: Option<HealthStatus>,
}

fn record() -> Record {
    Record {
        payload: Bytes::from_static(b"{\"seq\":1}"),
        key: None,
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

/// A send and a batch through `proxy`, then the transport's readiness, closing it after.
async fn send_through(case: Case, proxy: &str) -> Outcome {
    let proxy = ProxyConfig::Url(SensitiveString::new(proxy));
    let target = format!("{TARGET_HOST}:{TARGET_PORT}");
    match case {
        Case::Grpc => {
            let mut config = GrpcConfig::client(&format!("http://{target}"));
            config.proxy = proxy;
            config.send_timeout_ms = 10_000;
            let transport = GrpcTransport::new(&config).await.expect("gRPC transport");
            outcome(case, &transport).await
        }
        Case::Http => {
            let config = HttpTransportConfig {
                proxy,
                send_timeout_ms: 10_000,
                ..HttpTransportConfig::sender(&format!("https://{target}"))
            };
            let transport = HttpTransport::new(&config).await.expect("HTTP transport");
            outcome(case, &transport).await
        }
        Case::OtlpGrpc | Case::OtlpHttp => {
            let (protocol, scheme) = if matches!(case, Case::OtlpGrpc) {
                (OtlpProtocol::Grpc, "http")
            } else {
                (OtlpProtocol::Http, "https")
            };
            let config = OtlpConfig {
                endpoint: format!("{scheme}://{target}"),
                protocol,
                timeout_ms: 10_000,
                proxy,
                ..OtlpConfig::default()
            };
            let transport = OtlpTransport::new(&config).expect("OTLP transport");
            outcome(case, &transport).await
        }
        Case::Syslog(protocol) => {
            let config = SyslogConfig {
                connect_timeout_ms: 10_000,
                send_timeout_ms: 10_000,
                proxy,
                ..SyslogConfig::new(target, protocol)
            };
            let transport = SyslogTransport::new(&config).expect("syslog transport");
            outcome(case, &transport).await
        }
    }
}

async fn outcome<T: TransportSender>(case: Case, transport: &T) -> Outcome {
    let single = transport
        .send("events", Bytes::from_static(b"{\"seq\":1}"))
        .await;
    let batch = transport.send_batch(&[record()]).await;
    let registered = case.component().and_then(|name| {
        HealthRegistry::components()
            .into_iter()
            .find_map(|(component, status)| (component == name).then_some(status))
    });
    let ready = transport.is_healthy();
    let _ = transport.close().await;
    Outcome {
        single,
        batch,
        ready,
        registered,
    }
}

/// `transport_proxy_refused_total` for `transport`, from a Prometheus rendering.
fn refused_total(rendered: &str, transport: &str) -> f64 {
    rendered
        .lines()
        .filter(|line| line.starts_with("transport_proxy_refused_total{"))
        .filter(|line| line.contains(&format!("transport=\"{transport}\"")))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// The proxy answers the status the row expects, read straight off a CONNECT,
/// so a refusal below means the proxy said it and not the transport.
async fn assert_proxy_refuses(proxy: &str, refusal: Refusal) {
    let dialer = ProxyConfig::Url(SensitiveString::new(proxy))
        .dialer()
        .expect("proxy dialer");
    let error = dialer
        .connect(Scheme::Tls, TARGET_HOST, TARGET_PORT)
        .await
        .expect_err("the proxy refuses a tunnel to this port");
    let ProxyError::Refused(refused) = error else {
        panic!("{refusal:?}: not a refusal: {error}");
    };
    assert_eq!(refused.status, refusal.status(), "{refusal:?}: {refused}");
    assert_eq!(refused.port, TARGET_PORT, "{refusal:?}: {refused}");
}

/// A refused CONNECT is `Backpressured` on every transport and send path, counted
/// and failing readiness, whether the proxy refuses the port or asks for credentials.
#[tokio::test]
async fn a_refused_connect_holds_the_records() {
    let Some(squid) = Squid::shared().await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    // A current-thread runtime runs every send on this thread, which sees the recorder.
    let _local = metrics::set_default_local_recorder(&recorder);

    for refusal in Refusal::ALL {
        let proxy = refusal.proxy(squid);
        assert_proxy_refuses(&proxy, refusal).await;

        for case in Case::ALL {
            let name = format!("{refusal:?} {case:?}");
            let before = refused_total(&handle.render(), case.label());
            let got = send_through(case, &proxy).await;

            assert!(
                matches!(got.single, SendResult::Backpressured),
                "{name}: send is {:?}",
                got.single
            );
            assert!(
                matches!(got.batch, SendResult::Backpressured),
                "{name}: send_batch is {:?}",
                got.batch
            );
            assert!(!got.ready, "{name}: a refused proxy fails readiness");
            if case.component().is_some() {
                assert_eq!(
                    got.registered,
                    Some(HealthStatus::Unhealthy),
                    "{name}: the health registry"
                );
            }
            let after = refused_total(&handle.render(), case.label());
            assert!(
                after >= before + 2.0,
                "{name}: transport_proxy_refused_total went {before} -> {after}"
            );
        }
    }
}
