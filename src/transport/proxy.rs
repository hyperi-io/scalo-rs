// Project:   scalo
// File:      src/transport/proxy.rs
// Purpose:   One egress proxy setting and HTTP CONNECT tunnel for the stream transports
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The egress proxy every TCP and TLS transport dials through.
//!
//! [`ProxyConfig`] is the one `proxy` setting: `none` (the default), `system`
//! or a proxy URL. [`ProxyConfig::dialer`] resolves it once into a
//! [`ProxyDialer`], which opens each connection directly or through an HTTP
//! CONNECT tunnel. TLS runs over the tunnel with the target's name for SNI, as
//! it would over a direct connection.

mod system;
#[cfg(test)]
pub(crate) mod test_proxy;
#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;

use hyper_util::client::proxy::matcher::Matcher;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
#[cfg(feature = "transport-grpc")]
use tonic::codegen::Service;

use crate::sensitive::SensitiveString;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::types::SendResult;

/// Longest CONNECT response head read before the proxy is taken as broken.
const MAX_RESPONSE_HEAD: usize = 8 * 1024;

/// Which proxy a transport's connections go through.
///
/// `system` is the machine's setting: the `HTTPS_PROXY`, `HTTP_PROXY`,
/// `ALL_PROXY` and `NO_PROXY` environment variables first, then on Windows
/// the machine WinHTTP proxy (`netsh winhttp set proxy`), which a service
/// reads where it has no user proxy, and on macOS the System Configuration
/// proxies. A proxy URL is `http://host:port`, and `user:password@` in it is
/// basic proxy auth, so the setting serialises as a secret.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum ProxyConfig {
    /// Connect directly, whatever the machine names.
    #[default]
    None,
    /// The proxy the machine names.
    System,
    /// This proxy.
    Url(SensitiveString),
}

impl ProxyConfig {
    /// The kind, for logs.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::System => "system",
            Self::Url(_) => "url",
        }
    }

    /// Resolve the setting into a dialer, reading the machine's proxy once
    /// for `system`.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the proxy URL, or one the machine names,
    /// is not an `http://` proxy with a host.
    pub fn dialer(&self) -> TransportResult<ProxyDialer> {
        let settings = match self {
            Self::None => return Ok(ProxyDialer::direct()),
            Self::System => system::settings(),
            Self::Url(url) => system::Settings {
                http: url.expose().trim().to_string(),
                https: url.expose().trim().to_string(),
                ..system::Settings::default()
            },
        };
        let routes = Routes::new(&settings)?;
        tracing::info!(
            proxy = self.kind(),
            http = routes.http.as_ref().map_or("direct", |e| e.shown.as_str()),
            https = routes.https.as_ref().map_or("direct", |e| e.shown.as_str()),
            bypass = %settings.no,
            "egress proxy resolved"
        );
        Ok(ProxyDialer {
            routes: Some(Arc::new(routes)),
        })
    }

    /// Refuse a proxy on a datagram transport, at config load.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` naming `transport` unless the setting is `none`.
    pub fn refuse_for_udp(&self, transport: &str) -> TransportResult<()> {
        match self {
            Self::None => Ok(()),
            _ => Err(TransportError::Config(format!(
                "{transport}: proxy {} is set, and UDP can't go through an HTTP CONNECT \
                 proxy; use TCP or TLS, or set proxy: none",
                self.kind()
            ))),
        }
    }
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::System => f.write_str("System"),
            Self::Url(url) => f
                .debug_tuple("Url")
                .field(&shown(url.expose()).unwrap_or_else(|| "<not a URL>".into()))
                .finish(),
        }
    }
}

impl Serialize for ProxyConfig {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::None => serializer.serialize_str("none"),
            Self::System => serializer.serialize_str("system"),
            Self::Url(url) => url.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ProxyConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
        Ok(match text.trim() {
            "" | "none" => Self::None,
            "system" => Self::System,
            url => Self::Url(SensitiveString::new(url)),
        })
    }
}

/// A secret string: the value may be a URL holding a password.
#[cfg(feature = "config-schema")]
impl schemars::JsonSchema for ProxyConfig {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ProxyConfig".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        SensitiveString::json_schema(generator)
    }
}

/// Whether a target is reached over TLS, which picks the `https` proxy entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// Plain TCP, routed by the `http` entry.
    Plain,
    /// TLS, routed by the `https` entry.
    Tls,
}

impl Scheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "http",
            Self::Tls => "https",
        }
    }
}

/// Opens a transport's connections, directly or through the proxy its
/// [`ProxyConfig`] resolved to. Cheap to clone.
#[derive(Clone, Default)]
pub struct ProxyDialer {
    routes: Option<Arc<Routes>>,
}

impl fmt::Debug for ProxyDialer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entry = |e: &Option<Entry>| e.as_ref().map(|e| e.shown.clone());
        match &self.routes {
            None => f.write_str("ProxyDialer(direct)"),
            Some(routes) => f
                .debug_struct("ProxyDialer")
                .field("http", &entry(&routes.http))
                .field("https", &entry(&routes.https))
                .finish_non_exhaustive(),
        }
    }
}

impl ProxyDialer {
    /// A dialer that always connects directly.
    #[must_use]
    pub fn direct() -> Self {
        Self::default()
    }

    /// Whether a connection to `host:port` goes through a proxy.
    #[must_use]
    pub fn proxies(&self, scheme: Scheme, host: &str, port: u16) -> bool {
        self.route(scheme, host, port).is_some()
    }

    /// A TCP connection to `host:port`, through a CONNECT tunnel when the
    /// proxy covers it. Run TLS over the stream as over a direct one.
    ///
    /// # Errors
    ///
    /// [`ProxyError::Refused`] when the proxy answers CONNECT with anything
    /// but 2xx, and the other variants when the proxy or the target can't be
    /// reached.
    pub async fn connect(
        &self,
        scheme: Scheme,
        host: &str,
        port: u16,
    ) -> Result<TcpStream, ProxyError> {
        match self.route(scheme, host, port) {
            Some(route) => route.tunnel(host, port).await,
            None => TcpStream::connect((unbracketed(host), port))
                .await
                .map_err(ProxyError::Target),
        }
    }

    fn route(&self, scheme: Scheme, host: &str, port: u16) -> Option<Route> {
        self.routes.as_ref()?.route(scheme, host, port)
    }

    /// The proxy URL, credentials included, for a client library that dials
    /// its own proxy, such as reqwest.
    pub(crate) fn proxy_url(&self, scheme: Scheme, host: &str, port: u16) -> Option<String> {
        let routes = self.routes.as_ref()?;
        routes.route(scheme, host, port)?;
        let entry = match scheme {
            Scheme::Plain => routes.http.as_ref(),
            Scheme::Tls => routes.https.as_ref(),
        }?;
        Some(entry.url.expose().to_string())
    }
}

/// Why a connection through the proxy failed.
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    /// The proxy answered CONNECT with a refusal.
    #[error(transparent)]
    Refused(#[from] ProxyRefused),
    /// The proxy could not be reached, or closed the connection mid-answer.
    #[error("proxy {proxy}: {source}")]
    Proxy {
        /// The proxy's host and port.
        proxy: String,
        /// The I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The proxy's answer to CONNECT is not HTTP/1.x.
    #[error("proxy {proxy} answered CONNECT with something other than HTTP/1.x")]
    NotHttp {
        /// The proxy's host and port.
        proxy: String,
    },
    /// A direct connection to the target failed.
    #[error("connect: {0}")]
    Target(#[source] std::io::Error),
}

/// The proxy refused to open a tunnel to the target.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("proxy {proxy} refused CONNECT to {host}:{port}: HTTP {status}")]
pub struct ProxyRefused {
    /// The proxy's host and port.
    pub proxy: String,
    /// The target host.
    pub host: String,
    /// The target port.
    pub port: u16,
    /// The status the proxy answered with.
    pub status: u16,
}

/// The [`ProxyRefused`] in `error` or its causes, through boxes and
/// `std::io::Error`.
#[must_use]
pub fn refusal<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a ProxyRefused> {
    let mut next = Some(error);
    while let Some(error) = next {
        if let Some(refused) = error.downcast_ref::<ProxyRefused>() {
            return Some(refused);
        }
        if let Some(ProxyError::Refused(refused)) = error.downcast_ref::<ProxyError>() {
            return Some(refused);
        }
        if let Some(inner) = error
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            && let Some(refused) = refusal(inner)
        {
            return Some(refused);
        }
        next = error.source();
    }
    None
}

/// The send result for a refused CONNECT: `Reject`, logged once with the
/// proxy's status and the target port.
#[must_use]
pub fn rejected(refused: &ProxyRefused, transport: &str) -> SendResult {
    tracing::warn!(
        transport,
        proxy = %refused.proxy,
        target = %format!("{}:{}", refused.host, refused.port),
        status = refused.status,
        "proxy refused CONNECT; many proxies allow CONNECT to port 443 only"
    );
    SendResult::Reject(TransportError::Connection(refused.to_string()))
}

/// The resolved proxy entries and the hosts that bypass them.
struct Routes {
    matcher: Matcher,
    http: Option<Entry>,
    https: Option<Entry>,
    /// Hosts without a dot bypass the proxy: WinHTTP's `<local>`, macOS's
    /// `ExcludeSimpleHostnames`.
    bypass_simple: bool,
}

/// One proxy entry, checked.
struct Entry {
    /// `http://` URL, credentials included.
    url: SensitiveString,
    /// `host:port`, for logs and errors.
    shown: String,
}

impl Entry {
    fn parse(raw: &str) -> TransportResult<Option<Self>> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Ok(None);
        }
        let url = if raw.contains("://") {
            raw.to_string()
        } else {
            format!("http://{raw}")
        };
        let Some(shown) = shown(&url) else {
            return Err(TransportError::Config(
                "proxy: the URL has no host, or is not a URL".into(),
            ));
        };
        let scheme = url.split("://").next().unwrap_or_default();
        if !scheme.eq_ignore_ascii_case("http") {
            return Err(TransportError::Config(format!(
                "proxy {shown}: the scheme is {scheme}, and only an http:// proxy \
                 that answers CONNECT is supported"
            )));
        }
        Ok(Some(Self {
            url: SensitiveString::new(url),
            shown,
        }))
    }
}

impl Routes {
    fn new(settings: &system::Settings) -> TransportResult<Self> {
        let http = Entry::parse(&settings.http)?;
        let https = Entry::parse(&settings.https)?;
        let raw = |e: &Option<Entry>| e.as_ref().map(|e| e.url.expose().to_string());
        let matcher = Matcher::builder()
            .http(raw(&http).unwrap_or_default())
            .https(raw(&https).unwrap_or_default())
            .no(settings.no.as_str())
            .build();
        Ok(Self {
            matcher,
            http,
            https,
            bypass_simple: settings.bypass_simple,
        })
    }

    fn route(&self, scheme: Scheme, host: &str, port: u16) -> Option<Route> {
        let bare = unbracketed(host);
        if self.bypass_simple && !bare.contains('.') && bare.parse::<std::net::IpAddr>().is_err() {
            return None;
        }
        let target: http::Uri = format!("{}://{}:{port}", scheme.as_str(), bracketed(host))
            .parse()
            .ok()?;
        let intercept = self.matcher.intercept(&target)?;
        let proxy = intercept.uri();
        let proxy_port = proxy.port_u16().unwrap_or(80);
        let proxy_host = unbracketed(proxy.host()?).to_string();
        Some(Route {
            shown: format!("{}:{proxy_port}", bracketed(&proxy_host)),
            host: proxy_host,
            port: proxy_port,
            auth: intercept
                .basic_auth()
                .and_then(|value| value.to_str().ok())
                .map(SensitiveString::new),
        })
    }
}

/// The proxy one connection goes through.
struct Route {
    host: String,
    port: u16,
    shown: String,
    /// The `Proxy-Authorization` value.
    auth: Option<SensitiveString>,
}

impl Route {
    async fn tunnel(&self, host: &str, port: u16) -> Result<TcpStream, ProxyError> {
        let broken = |source| ProxyError::Proxy {
            proxy: self.shown.clone(),
            source,
        };
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(broken)?;
        let authority = format!("{}:{port}", bracketed(host));
        let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
        if let Some(auth) = &self.auth {
            request.push_str("Proxy-Authorization: ");
            request.push_str(auth.expose());
            request.push_str("\r\n");
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await.map_err(broken)?;

        let status = self.read_status(&mut stream).await?;
        if (200..300).contains(&status) {
            tracing::debug!(proxy = %self.shown, target = %authority, "proxy tunnel open");
            return Ok(stream);
        }
        Err(ProxyRefused {
            proxy: self.shown.clone(),
            host: unbracketed(host).to_string(),
            port,
            status,
        }
        .into())
    }

    /// Read the response head a byte at a time, so no byte of the tunnel is
    /// taken with it, and return its status.
    async fn read_status(&self, stream: &mut TcpStream) -> Result<u16, ProxyError> {
        let mut head = Vec::with_capacity(128);
        while !head.ends_with(b"\r\n\r\n") {
            if head.len() >= MAX_RESPONSE_HEAD {
                return Err(ProxyError::NotHttp {
                    proxy: self.shown.clone(),
                });
            }
            let byte = stream.read_u8().await.map_err(|source| ProxyError::Proxy {
                proxy: self.shown.clone(),
                source,
            })?;
            head.push(byte);
        }
        status_of(&head).ok_or_else(|| ProxyError::NotHttp {
            proxy: self.shown.clone(),
        })
    }
}

/// The status in an HTTP/1.x response head.
fn status_of(head: &[u8]) -> Option<u16> {
    let line = head.split(|b| *b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?;
    let mut parts = line.split_ascii_whitespace();
    if !parts.next()?.starts_with("HTTP/1.") {
        return None;
    }
    parts.next()?.parse().ok()
}

/// `host:port` of a proxy URL, without credentials, or `None` when it has no
/// host.
fn shown(url: &str) -> Option<String> {
    let uri: http::Uri = url.parse().ok()?;
    let host = uri.host().filter(|h| !h.is_empty())?;
    Some(format!("{host}:{}", uri.port_u16().unwrap_or(80)))
}

fn unbracketed(host: &str) -> &str {
    host.trim_start_matches('[').trim_end_matches(']')
}

/// `host` with brackets when it is an IPv6 address, as an authority needs.
fn bracketed(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

/// tonic's connector: each dial goes through [`ProxyDialer`], and tonic runs
/// its TLS over the stream.
#[cfg(feature = "transport-grpc")]
#[derive(Clone)]
pub(crate) struct Connector {
    dialer: ProxyDialer,
    direct: hyper_util::client::legacy::connect::HttpConnector,
}

#[cfg(feature = "transport-grpc")]
impl Connector {
    /// `direct` dials whatever the proxy does not cover.
    pub(crate) fn new(
        dialer: ProxyDialer,
        direct: hyper_util::client::legacy::connect::HttpConnector,
    ) -> Self {
        Self { dialer, direct }
    }
}

#[cfg(feature = "transport-grpc")]
impl Service<http::Uri> for Connector {
    type Response = hyper_util::rt::TokioIo<TcpStream>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.direct.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let scheme = if uri.scheme_str() == Some("https") {
            Scheme::Tls
        } else {
            Scheme::Plain
        };
        let port = uri
            .port_u16()
            .unwrap_or(if scheme == Scheme::Tls { 443 } else { 80 });
        let host = uri.host().unwrap_or_default().to_string();
        let Some(route) = self.dialer.route(scheme, &host, port) else {
            let dial = self.direct.call(uri);
            return Box::pin(async move { dial.await.map_err(Into::into) });
        };
        Box::pin(async move {
            let stream = route.tunnel(&host, port).await?;
            stream.set_nodelay(true)?;
            Ok(hyper_util::rt::TokioIo::new(stream))
        })
    }
}
