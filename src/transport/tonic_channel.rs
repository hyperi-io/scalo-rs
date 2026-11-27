// Project:   scalo
// File:      src/transport/tonic_channel.rs
// Purpose:   The lazily dialled tonic channel every gRPC client shares
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The tonic channel the gRPC, Vector and OTLP clients dial, with the dial
//! bounded and dead connections dropped.
//!
//! TLS is scalo's rustls client, run by [`TlsConnector`] over each dialled
//! stream, so a gRPC client takes the same profile, trust, CA files and key
//! pins as every other client. tonic's own TLS is not used: it builds its
//! config from PEM bytes on rustls's defaults.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tonic::codec::CompressionEncoding;
use tonic::codegen::Service;

use crate::compression::Codec;

/// ALPN for HTTP/2, which every gRPC server agrees to.
const ALPN_H2: &[u8] = b"h2";

/// Default send limit of a gRPC client, and its HTTP/2 PING period when it
/// has none.
pub(crate) const DEFAULT_SEND_TIMEOUT_MS: u64 = 30_000;

/// Every encoding a tonic server or client here reads. tonic's zstd, and the
/// zstd C library under it, comes only with the `compression` feature.
#[cfg(feature = "compression")]
pub(crate) const ACCEPTED_ENCODINGS: &[CompressionEncoding] =
    &[CompressionEncoding::Gzip, CompressionEncoding::Zstd];
#[cfg(not(feature = "compression"))]
pub(crate) const ACCEPTED_ENCODINGS: &[CompressionEncoding] = &[CompressionEncoding::Gzip];

/// The codecs a gRPC or Vector-compat client sends with: zstd only with `compression`.
#[cfg(all(feature = "transport-grpc", feature = "compression"))]
pub(crate) const TONIC_CODECS: &[Codec] = &[Codec::None, Codec::Gzip, Codec::Zstd];
#[cfg(all(feature = "transport-grpc", not(feature = "compression")))]
pub(crate) const TONIC_CODECS: &[Codec] = &[Codec::None, Codec::Gzip];

/// tonic's encoding for `codec`, which fixes the level. `None` for
/// [`Codec::None`] and for a codec tonic lacks, which an `Accepts` check
/// refuses first.
pub(crate) fn encoding(codec: Codec) -> Option<CompressionEncoding> {
    match codec {
        Codec::Gzip => Some(CompressionEncoding::Gzip),
        #[cfg(feature = "compression")]
        Codec::Zstd => Some(CompressionEncoding::Zstd),
        _ => None,
    }
}

/// Client TLS a channel runs over each dialled stream: a rustls config
/// offering ALPN `h2`, and the name the server's certificate is checked
/// against.
#[derive(Clone)]
pub(crate) struct ChannelTls {
    connector: tokio_rustls::TlsConnector,
    server_name: ServerName<'static>,
}

impl ChannelTls {
    /// `config` with ALPN `h2`, checking the server against `server_name`, a
    /// DNS name or an IP address.
    ///
    /// # Errors
    ///
    /// When `server_name` is neither.
    pub(crate) fn new(config: &rustls::ClientConfig, server_name: &str) -> Result<Self, String> {
        let name = server_name.trim_matches(['[', ']']).to_string();
        let server_name = ServerName::try_from(name)
            .map_err(|e| format!("TLS server name {server_name:?}: {e}"))?;
        let mut config = config.clone();
        config.alpn_protocols = vec![ALPN_H2.to_vec()];
        Ok(Self {
            connector: tokio_rustls::TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }
}

/// The endpoint for calls naming `origin`, dialling `dial` when set and
/// `origin` otherwise.
///
/// With `tls`, `origin` must be `https://`. The dial URI is then written as
/// `http://` with its port spelled out, so tonic hands the stream to
/// [`TlsConnector`] rather than looking for TLS of its own, and calls still
/// name `origin`'s scheme and authority.
///
/// # Errors
///
/// An invalid URI, or a scheme that does not match `tls`.
pub(crate) fn endpoint(
    origin: &str,
    dial: Option<&str>,
    tls: bool,
) -> Result<tonic::transport::Endpoint, String> {
    let invalid = |e: &dyn std::fmt::Display| format!("invalid endpoint: {e}");
    let origin_uri: http::Uri = origin.parse().map_err(|e| invalid(&e))?;
    let is_https = origin_uri.scheme_str() == Some("https");
    if tls != is_https {
        return Err(if tls {
            "a tls section needs an https:// endpoint".to_string()
        } else {
            "an https:// endpoint needs TLS".to_string()
        });
    }
    let dial_uri: http::Uri = dial.unwrap_or(origin).parse().map_err(|e| invalid(&e))?;
    if !tls {
        let endpoint = tonic::transport::Endpoint::from_shared(dial_uri.to_string())
            .map_err(|e| invalid(&e))?;
        return Ok(if dial.is_some() {
            endpoint.origin(origin_uri)
        } else {
            endpoint
        });
    }
    let authority = dial_uri
        .authority()
        .ok_or_else(|| "invalid endpoint: no host".to_string())?;
    let port = authority.port_u16().unwrap_or(443);
    let path = dial_uri.path_and_query().map_or("", |p| p.as_str());
    let plain = format!("http://{}:{port}{path}", authority.host());
    tonic::transport::Endpoint::from_shared(plain)
        .map(|endpoint| endpoint.origin(origin_uri))
        .map_err(|e| invalid(&e))
}

/// The host `origin` names, without brackets, the default TLS server name.
///
/// # Errors
///
/// An invalid URI, or one with no host.
pub(crate) fn origin_host(origin: &str) -> Result<String, String> {
    let uri: http::Uri = origin
        .parse()
        .map_err(|e| format!("invalid endpoint: {e}"))?;
    uri.host()
        .map(|host| host.trim_matches(['[', ']']).to_string())
        .ok_or_else(|| "invalid endpoint: no host".to_string())
}

/// tonic's connector for a TLS channel: dials through the proxy connector,
/// then runs the client handshake and requires ALPN `h2`.
#[derive(Clone)]
pub(crate) struct TlsConnector {
    dial: crate::transport::proxy::Connector,
    tls: ChannelTls,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl Service<http::Uri> for TlsConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.dial.poll_ready(cx)
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let dial = self.dial.call(uri);
        let tls = self.tls.clone();
        Box::pin(async move {
            let tcp = dial.await?.into_inner();
            let stream = tls.connector.connect(tls.server_name, tcp).await?;
            if stream.get_ref().1.alpn_protocol() != Some(ALPN_H2) {
                return Err(
                    "TLS: the server did not agree to HTTP/2 (ALPN h2), which gRPC needs".into(),
                );
            }
            Ok(TokioIo::new(stream))
        })
    }
}

/// A lazily dialled channel to `ep`, direct or through `proxy`, with `tls`
/// run over each stream when set.
///
/// A dial whose DNS lookup, TCP connect or TLS handshake is unfinished at nine
/// tenths of `send_timeout_ms` is abandoned. A connection that has read nothing
/// for `send_timeout_ms` is sent an HTTP/2 PING and closed if the PING goes
/// unanswered for as long again, so a peer that stays connected but stops
/// answering is dropped and the next call dials afresh. `0` leaves the dial
/// unbounded and PINGs at [`DEFAULT_SEND_TIMEOUT_MS`].
pub(crate) fn lazy_channel(
    ep: tonic::transport::Endpoint,
    send_timeout_ms: u64,
    proxy: crate::transport::ProxyDialer,
    tls: Option<ChannelTls>,
) -> tonic::transport::Channel {
    let dial = crate::transport::proxy::Connector::new(proxy, plain_tcp());
    bounded_channel(ep, send_timeout_ms, dial, tls)
}

/// [`lazy_channel`] dialling where `resolved` says, with no proxy.
#[cfg(feature = "transport-grpc")]
pub(crate) fn resolved_channel(
    ep: tonic::transport::Endpoint,
    send_timeout_ms: u64,
    resolved: crate::transport::resolver::ResolvedDial,
    tls: Option<ChannelTls>,
) -> tonic::transport::Channel {
    let dial = crate::transport::proxy::Connector::new(
        crate::transport::ProxyDialer::direct(),
        plain_tcp(),
    )
    .resolved_by(resolved);
    bounded_channel(ep, send_timeout_ms, dial, tls)
}

fn plain_tcp() -> hyper_util::client::legacy::connect::HttpConnector {
    let mut tcp = hyper_util::client::legacy::connect::HttpConnector::new();
    tcp.enforce_http(false);
    tcp.set_nodelay(true);
    tcp
}

fn bounded_channel(
    mut ep: tonic::transport::Endpoint,
    send_timeout_ms: u64,
    dial: crate::transport::proxy::Connector,
    tls: Option<ChannelTls>,
) -> tonic::transport::Channel {
    // A dial left running when its send gives up hands its failure to the
    // next send, so the dial ends at nine tenths of the limit.
    if send_timeout_ms > 0 {
        ep = ep.connect_timeout(Duration::from_millis(send_timeout_ms) * 9 / 10);
    }

    let keep_alive = Duration::from_millis(if send_timeout_ms > 0 {
        send_timeout_ms
    } else {
        DEFAULT_SEND_TIMEOUT_MS
    });
    ep = ep
        .http2_keep_alive_interval(keep_alive)
        .keep_alive_timeout(keep_alive)
        .keep_alive_while_idle(true);

    // Given the connector, tonic abandons a dial whose DNS, TCP connect or TLS
    // handshake outruns the connect timeout, where connect_lazy() bounds the
    // TCP connect alone and the next send queues behind it.
    match tls {
        Some(tls) => ep.connect_with_connector_lazy(TlsConnector {
            dial: dial.for_tls(),
            tls,
        }),
        None => ep.connect_with_connector_lazy(dial),
    }
}

#[cfg(all(
    test,
    feature = "transport-grpc-vector-compat",
    feature = "transport-http"
))]
mod tests {
    use std::net::SocketAddr;
    use std::time::Instant;

    use super::*;
    use crate::transport::grpc::{GrpcConfig, GrpcTransport};
    use crate::transport::proxy::test_front::tls_front;
    use crate::transport::vector_compat::proto::vector;

    /// A Vector-compat server behind a TLS front offering `alpn`, the front's
    /// address and the CA file to trust.
    async fn tls_server(alpn: &[&[u8]]) -> (GrpcTransport, SocketAddr, tempfile::NamedTempFile) {
        let server = GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0").with_vector_compat())
            .await
            .unwrap();
        let (front, ca) = tls_front(server.local_addr().unwrap(), alpn).await;
        let ca_file = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(ca_file.path(), ca).await.unwrap();
        (server, front, ca_file)
    }

    fn channel_tls(ca: &std::path::Path, name: &str) -> ChannelTls {
        let tls =
            crate::tls::ClientTls::pinned(ca).with_profiles(crate::tls::TlsProfiles::default());
        let config = crate::tls::build_client_tls(&tls).unwrap();
        ChannelTls::new(&config, name).unwrap()
    }

    async fn health(
        origin: &str,
        timeout_ms: u64,
        proxy: crate::transport::ProxyDialer,
        tls: ChannelTls,
    ) -> Result<(), tonic::Status> {
        let ep = endpoint(origin, None, true).unwrap();
        let channel = lazy_channel(ep, timeout_ms, proxy, Some(tls));
        vector::vector_client::VectorClient::new(channel)
            .health_check(vector::HealthCheckRequest {})
            .await
            .map(drop)
    }

    /// A call over TLS reaches the server, and one to a server that does not
    /// agree to `h2` fails naming ALPN.
    #[tokio::test]
    async fn a_tls_channel_calls_through_the_connector_and_needs_h2() {
        let (_server, front, ca) = tls_server(&[b"h2"]).await;
        let origin = format!("https://localhost:{}", front.port());
        let direct = crate::transport::ProxyDialer::direct();
        let name = origin_host(&origin).unwrap();
        health(
            &origin,
            5_000,
            direct.clone(),
            channel_tls(ca.path(), &name),
        )
        .await
        .expect("a health check over TLS");

        // `platform` trust verifies through the OS verifier, with the CA file beside it.
        let platform = crate::tls::ClientTls::new(crate::tls::TrustChoice::Platform)
            .with_ca(ca.path())
            .with_profiles(crate::tls::TlsProfiles::default());
        let config = crate::tls::build_client_tls(&platform).unwrap();
        health(
            &origin,
            5_000,
            direct.clone(),
            ChannelTls::new(&config, &name).unwrap(),
        )
        .await
        .expect("a health check over TLS with platform trust");

        // A server with no ALPN at all completes the handshake without one.
        let (_server, front, ca) = tls_server(&[]).await;
        let origin = format!("https://localhost:{}", front.port());
        let error = health(&origin, 5_000, direct, channel_tls(ca.path(), "localhost"))
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("ALPN h2"), "{error:?}");
    }

    /// A server that takes the TCP connection and never answers the
    /// ClientHello is abandoned at the dial limit.
    #[tokio::test]
    async fn a_stalled_tls_handshake_ends_at_the_dial_limit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        let ca = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(ca.path(), crate::tls::test_pki::TestCa::new("ca").pem())
            .await
            .unwrap();
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            health(
                &format!("https://localhost:{}", addr.port()),
                300,
                crate::transport::ProxyDialer::direct(),
                channel_tls(ca.path(), "localhost"),
            ),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(matches!(result, Ok(Err(_))), "{result:?}");
        assert!(
            elapsed < Duration::from_secs(1),
            "a 270 ms dial took {elapsed:?}"
        );
    }

    /// A TLS channel reaches its server through a real Squid's CONNECT tunnel.
    /// The target name resolves only inside Squid.
    #[cfg(all(feature = "transport-proxy-test-support", unix))]
    #[tokio::test]
    async fn a_tls_channel_goes_through_a_connect_proxy() {
        use crate::sensitive::SensitiveString;
        use crate::transport::ProxyConfig;
        use crate::transport::proxy::squid::{Squid, TARGET_HOST};

        let Some(squid) = Squid::shared().await else {
            return;
        };
        let (_server, front, ca) = tls_server(&[b"h2"]).await;
        let proxy = ProxyConfig::Url(SensitiveString::new(squid.open()))
            .dialer()
            .unwrap();
        health(
            &format!("https://{TARGET_HOST}:{}", front.port()),
            10_000,
            proxy,
            channel_tls(ca.path(), TARGET_HOST),
        )
        .await
        .expect("a health check through the proxy");
    }
}
