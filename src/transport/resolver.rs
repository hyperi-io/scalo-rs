// Project:   scalo
// File:      src/transport/resolver.rs
// Purpose:   A caller's answer to where a transport dials an endpoint's host
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`ConnectResolver`]: the address a transport dials for an endpoint's host,
//! asked at every connect in place of DNS. The HTTP and gRPC transports take
//! one through their builders.

use std::net::SocketAddr;

/// Where a transport dials an endpoint's host, asked at every connect in
/// place of DNS.
///
/// The endpoint URL's host stays the name TLS sends and checks the
/// certificate against, and the `Host` or `:authority` a request names. An
/// endpoint that names its port is dialled at that port. One that doesn't is
/// dialled at the answer's port, or at the scheme's default when the answer's
/// port is 0. An endpoint whose host is an IP address is dialled as written,
/// without asking.
///
/// A transport given a resolver keeps no connection between sends, so a
/// changed answer takes effect at the next send rather than whenever a kept
/// connection drops. Each send pays a TCP and TLS handshake for that. A
/// `proxy` other than `none` is refused beside a resolver, since the proxy
/// looks the name up itself.
pub trait ConnectResolver: Send + Sync {
    /// The address to dial for `host`, the endpoint's host name in lowercase.
    /// `None` when this resolver doesn't answer for `host`: that connect
    /// fails, and the send is retried.
    fn resolve(&self, host: &str) -> Option<SocketAddr>;
}

#[cfg(any(feature = "transport-http", feature = "transport-grpc"))]
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The connect error for a host the resolver has no answer for.
#[cfg(any(feature = "transport-http", feature = "transport-grpc"))]
fn unanswered(host: &str) -> BoxError {
    format!("the connect resolver has no address for {host}").into()
}

/// Refuse `proxy` beside a resolver, at build.
///
/// # Errors
///
/// `TransportError::Config` naming `transport` unless `proxy` is `none`.
#[cfg(any(feature = "transport-http", feature = "transport-grpc"))]
pub(crate) fn refuse_proxy(
    proxy: &crate::transport::ProxyConfig,
    transport: &str,
) -> crate::transport::TransportResult<()> {
    match proxy {
        crate::transport::ProxyConfig::None => Ok(()),
        _ => Err(crate::transport::TransportError::Config(format!(
            "{transport}: a connect resolver is set beside proxy {}, which looks the \
             endpoint's name up itself; set proxy: none",
            proxy.kind()
        ))),
    }
}

/// A [`ConnectResolver`] as reqwest's resolver, which applies the port rule.
#[cfg(feature = "transport-http")]
pub(crate) struct ReqwestResolver(pub(crate) std::sync::Arc<dyn ConnectResolver>);

#[cfg(feature = "transport-http")]
impl reqwest::dns::Resolve for ReqwestResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let answer = self
            .0
            .resolve(name.as_str())
            .map(|addr| -> reqwest::dns::Addrs { Box::new(std::iter::once(addr)) })
            .ok_or_else(|| unanswered(name.as_str()));
        Box::pin(std::future::ready(answer))
    }
}

/// A [`ConnectResolver`] where a gRPC channel dials, with the port rule
/// applied.
#[cfg(feature = "transport-grpc")]
#[derive(Clone)]
pub(crate) struct ResolvedDial {
    resolver: std::sync::Arc<dyn ConnectResolver>,
    /// The endpoint's port, when it names one other than its scheme's default.
    named_port: Option<u16>,
}

#[cfg(feature = "transport-grpc")]
impl ResolvedDial {
    /// Dials for `endpoint`, the URL calls name.
    ///
    /// # Errors
    ///
    /// When `endpoint` is not a URI.
    pub(crate) fn new(
        resolver: std::sync::Arc<dyn ConnectResolver>,
        endpoint: &str,
    ) -> Result<Self, String> {
        let uri: http::Uri = endpoint
            .parse()
            .map_err(|e| format!("invalid endpoint: {e}"))?;
        let default = default_port(&uri);
        Ok(Self {
            resolver,
            named_port: uri.port_u16().filter(|port| *port != default),
        })
    }

    /// The address a dial to `uri` goes to: an IP host as written, a name
    /// where the resolver says.
    fn address(&self, uri: &http::Uri) -> Result<SocketAddr, BoxError> {
        let host = uri
            .host()
            .ok_or("the dial has no host")?
            .trim_matches(['[', ']']);
        let port = uri.port_u16().unwrap_or_else(|| default_port(uri));
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            return Ok(SocketAddr::new(ip, port));
        }
        let mut addr = self
            .resolver
            .resolve(&host.to_ascii_lowercase())
            .ok_or_else(|| unanswered(host))?;
        match self.named_port {
            Some(named) => addr.set_port(named),
            None if addr.port() == 0 => addr.set_port(port),
            None => {}
        }
        Ok(addr)
    }

    /// A TCP connection, with `TCP_NODELAY` on, to where `uri` resolves.
    pub(crate) async fn connect(&self, uri: &http::Uri) -> Result<tokio::net::TcpStream, BoxError> {
        let addr = self.address(uri)?;
        let stream = tokio::net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}

#[cfg(feature = "transport-grpc")]
fn default_port(uri: &http::Uri) -> u16 {
    if uri.scheme_str() == Some("https") {
        443
    } else {
        80
    }
}

#[cfg(all(test, any(feature = "transport-http", feature = "transport-grpc")))]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::ConnectResolver;
    use crate::tls::test_pki::TestCa;

    /// The address one host is proven at, moved by the test between sends.
    struct Slot {
        host: &'static str,
        addr: std::sync::Mutex<Option<SocketAddr>>,
    }

    impl Slot {
        fn new(host: &'static str, addr: SocketAddr) -> Arc<Self> {
            Arc::new(Self {
                host,
                addr: std::sync::Mutex::new(Some(addr)),
            })
        }

        fn point(&self, addr: Option<SocketAddr>) {
            *self.addr.lock().unwrap() = addr;
        }
    }

    impl ConnectResolver for Slot {
        fn resolve(&self, host: &str) -> Option<SocketAddr> {
            (host == self.host)
                .then(|| *self.addr.lock().unwrap())
                .flatten()
        }
    }

    /// A TLS listener in front of `upstream`, counting the connections it
    /// took and those still open.
    struct Front {
        addr: SocketAddr,
        taken: Arc<AtomicUsize>,
        open: Arc<AtomicUsize>,
    }

    impl Front {
        /// Serves a certificate for `host` from `ca`, offering `alpn`, and
        /// relays each connection's plain bytes to `upstream`.
        async fn start(ca: &TestCa, host: &str, upstream: SocketAddr, alpn: &[&[u8]]) -> Self {
            use rustls_pki_types::PrivateKeyDer;
            use rustls_pki_types::pem::PemObject;

            let leaf = ca.server_for("resolver front", host);
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let mut config = rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![leaf.der.clone()],
                    PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).unwrap(),
                )
                .unwrap();
            config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (taken, open) = (Arc::<AtomicUsize>::default(), Arc::<AtomicUsize>::default());
            let (counted, held) = (Arc::clone(&taken), Arc::clone(&open));
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    counted.fetch_add(1, Ordering::SeqCst);
                    held.fetch_add(1, Ordering::SeqCst);
                    let (acceptor, held) = (acceptor.clone(), Arc::clone(&held));
                    tokio::spawn(async move {
                        if let Ok(mut tls) = acceptor.accept(stream).await
                            && let Ok(mut plain) = tokio::net::TcpStream::connect(upstream).await
                        {
                            let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
                        }
                        held.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            });
            Self { addr, taken, open }
        }

        fn taken(&self) -> usize {
            self.taken.load(Ordering::SeqCst)
        }

        fn open(&self) -> usize {
            self.open.load(Ordering::SeqCst)
        }
    }

    /// Waits up to 5 s for every connection to `fronts` to close.
    async fn all_closed(fronts: &[&Front]) -> bool {
        let open = || fronts.iter().map(|f| f.open()).sum::<usize>();
        tokio::time::timeout(Duration::from_secs(5), async {
            while open() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    }

    /// `ca` written to a file, to trust.
    fn ca_file(ca: &TestCa) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), ca.pem()).unwrap();
        file
    }

    fn tls(ca: &tempfile::NamedTempFile) -> crate::tls::ClientTlsConfig {
        crate::tls::ClientTlsConfig {
            trust: crate::tls::TrustChoice::Bundle,
            ca_paths: vec![ca.path().to_path_buf()],
            ..crate::tls::ClientTlsConfig::default()
        }
    }

    /// A loopback HTTP/1.1 server that keeps each connection open and answers
    /// every request on it 200, so only the client decides when one closes.
    #[cfg(feature = "transport-http")]
    async fn keep_alive_server() -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    loop {
                        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                            continue;
                        };
                        let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                        let length: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse().ok())
                            .unwrap_or(0);
                        while buf.len() < end + 4 + length {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        }
                        buf.drain(..end + 4 + length);
                        let ok = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
                        if stream.write_all(ok).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    /// A send to `https://named.example` dials where the resolver says, checks
    /// the certificate against `named.example`, opens a connection per send,
    /// and follows a moved answer at the next send. An endpoint's own port
    /// wins over the answer's, a name the resolver doesn't answer for is
    /// retried, and a proxy beside a resolver fails the build.
    #[cfg(feature = "transport-http")]
    #[tokio::test]
    async fn http_dials_where_the_resolver_says_and_keeps_the_name() {
        use crate::transport::http::{HttpTransport, HttpTransportConfig};
        use crate::transport::{ProxyConfig, SendResult, TransportSender};

        let ca = TestCa::new("resolver test CA");
        let trusted = ca_file(&ca);
        let upstream = keep_alive_server().await;
        let alpn: &[&[u8]] = &[b"http/1.1"];
        let a = Front::start(&ca, "named.example", upstream, alpn).await;
        let b = Front::start(&ca, "named.example", upstream, alpn).await;
        let wrong = Front::start(&ca, "other.example", upstream, alpn).await;
        let slot = Slot::new("named.example", a.addr);
        let config = |endpoint: &str| HttpTransportConfig {
            tls: tls(&trusted),
            ..HttpTransportConfig::sender(endpoint)
        };
        let resolver: Arc<dyn ConnectResolver> = slot.clone();
        let built = HttpTransport::builder(&config("https://named.example"))
            .resolver(Arc::clone(&resolver))
            .start()
            .await
            .unwrap();
        let send = || built.send("ingest", bytes::Bytes::from_static(b"{}"));

        assert!(send().await.is_ok());
        assert!(send().await.is_ok());
        assert_eq!(a.taken(), 2, "a connection per send, none kept");

        slot.point(Some(b.addr));
        assert!(send().await.is_ok());
        assert_eq!(b.taken(), 1, "the moved answer takes the next send");
        assert!(all_closed(&[&a, &b]).await, "each send's connection closes");

        slot.point(None);
        let unanswered = send().await;
        assert!(
            matches!(unanswered, SendResult::Backpressured),
            "{unanswered:?}"
        );

        slot.point(Some(wrong.addr));
        let refused = send().await;
        assert!(
            matches!(&refused, SendResult::Fatal(e) if e.to_string().contains("TLS")),
            "the certificate is checked against named.example: {refused:?}"
        );

        // The endpoint's port wins: the answer's IP at b's port reaches b.
        slot.point(Some(a.addr));
        let named_port =
            HttpTransport::builder(&config(&format!("https://named.example:{}", b.addr.port())))
                .resolver(Arc::clone(&resolver))
                .start()
                .await
                .unwrap();
        let reached = named_port
            .send("ingest", bytes::Bytes::from_static(b"{}"))
            .await;
        assert!(reached.is_ok(), "{reached:?}");
        assert_eq!(b.taken(), 2, "dialled at the endpoint's port");

        let proxied = HttpTransportConfig {
            proxy: ProxyConfig::System,
            ..config("https://named.example")
        };
        let error = HttpTransport::builder(&proxied)
            .resolver(resolver)
            .start()
            .await
            .err()
            .map(|e| e.to_string());
        assert!(
            error
                .as_deref()
                .is_some_and(|e| e.contains("proxy system") && e.contains("proxy: none")),
            "{error:?}"
        );
    }

    /// As the HTTP test, for gRPC: each `RouteBatch` call dials its own
    /// connection where the resolver says and closes it after, and a moved
    /// answer takes the next call. TLS checks `named.example`.
    #[cfg(feature = "transport-grpc")]
    #[tokio::test]
    async fn grpc_dials_where_the_resolver_says_and_keeps_the_name() {
        use crate::transport::grpc::{GrpcConfig, GrpcTransport};
        use crate::transport::{
            PayloadFormat, ProxyConfig, Record, RecordMeta, SendResult, TransportReceiver,
            TransportSender,
        };

        let ca = TestCa::new("resolver test CA");
        let trusted = ca_file(&ca);
        let server = GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0"))
            .await
            .unwrap();
        let upstream = server.local_addr().unwrap();
        let alpn: &[&[u8]] = &[b"h2"];
        let a = Front::start(&ca, "named.example", upstream, alpn).await;
        let b = Front::start(&ca, "named.example", upstream, alpn).await;
        let wrong = Front::start(&ca, "other.example", upstream, alpn).await;
        let slot = Slot::new("named.example", a.addr);
        let resolver: Arc<dyn ConnectResolver> = slot.clone();
        let mut config = GrpcConfig::client("https://named.example");
        config.tls = Some(tls(&trusted));
        let client = GrpcTransport::builder(&config)
            .resolver(Arc::clone(&resolver))
            .start()
            .await
            .unwrap();
        let block = [Record {
            payload: bytes::Bytes::from_static(b"{}"),
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }];

        assert!(client.send_batch(&block).await.is_ok());
        assert!(client.send_batch(&block).await.is_ok());
        assert_eq!(a.taken(), 2, "a connection per call, none kept");

        slot.point(Some(b.addr));
        assert!(client.send_batch(&block).await.is_ok());
        assert_eq!(b.taken(), 1, "the moved answer takes the next call");
        assert!(all_closed(&[&a, &b]).await, "each call's connection closes");
        let received = server.recv(10).await.unwrap();
        assert_eq!(received.len(), 3);

        slot.point(None);
        let unanswered = client.send_batch(&block).await;
        assert!(
            matches!(unanswered, SendResult::Backpressured),
            "{unanswered:?}"
        );

        slot.point(Some(wrong.addr));
        let refused = client.send_batch(&block).await;
        assert!(
            matches!(&refused, SendResult::Fatal(e) if e.to_string().contains("TLS")),
            "the certificate is checked against named.example: {refused:?}"
        );

        config.proxy = ProxyConfig::System;
        let error = GrpcTransport::builder(&config)
            .resolver(resolver)
            .start()
            .await
            .err()
            .map(|e| e.to_string());
        assert!(
            error
                .as_deref()
                .is_some_and(|e| e.contains("proxy system") && e.contains("proxy: none")),
            "{error:?}"
        );
    }
}
