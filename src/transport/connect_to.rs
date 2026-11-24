// Project:   scalo
// File:      src/transport/connect_to.rs
// Purpose:   Dial another address while TLS and requests keep the endpoint's name
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A connect-to override: dial an IP address, and another port when one is
//! given, in place of the endpoint's host and port. TLS still checks the
//! endpoint's host name, and requests name the endpoint's host and port in
//! `Host` or `:authority`. The OTLP transport takes it.

use std::net::{IpAddr, SocketAddr};

/// The address a transport dials in place of its endpoint's host and port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectTo {
    ip: IpAddr,
    /// Unset, the endpoint's port.
    port: Option<u16>,
}

impl ConnectTo {
    /// `IP` or `IP:port`, an IPv6 address in brackets when a port follows.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if let Ok(addr) = text.parse::<SocketAddr>() {
            return Ok(Self {
                ip: addr.ip(),
                port: Some(addr.port()),
            });
        }
        let bare = text
            .strip_prefix('[')
            .and_then(|t| t.strip_suffix(']'))
            .unwrap_or(text);
        bare.parse::<IpAddr>()
            .map(|ip| Self { ip, port: None })
            .map_err(|_| format!("connect_to {text:?} is not an IP address or IP:port"))
    }

    /// The endpoint's host name, which TLS and requests keep. An IP address
    /// is refused: there is no name to keep, and reqwest dials one without a
    /// lookup to override.
    fn named_host(url: &reqwest::Url) -> Result<&str, String> {
        url.domain()
            .ok_or_else(|| "connect_to needs an endpoint that names its host".to_string())
    }

    /// `url` with this port in place of its own, when one is set.
    fn with_port(self, url: &reqwest::Url) -> Result<reqwest::Url, String> {
        let mut moved = url.clone();
        if let Some(port) = self.port {
            moved
                .set_port(Some(port))
                .map_err(|()| "connect_to: the endpoint cannot take a port".to_string())?;
        }
        Ok(moved)
    }

    /// A tonic endpoint for `url` that dials this address, with `tls` checking
    /// `url`'s host name and calls naming `url` as `:authority`.
    pub(crate) fn tonic_endpoint(
        self,
        url: &reqwest::Url,
        tls: Option<tonic::transport::ClientTlsConfig>,
    ) -> Result<tonic::transport::Endpoint, String> {
        let host = Self::named_host(url)?;
        let mut dial = self.with_port(url)?;
        dial.set_ip_host(self.ip)
            .map_err(|()| "connect_to: the endpoint cannot take an address".to_string())?;
        let origin: tonic::transport::Uri = url
            .as_str()
            .parse()
            .map_err(|e| format!("invalid endpoint: {e}"))?;
        let mut endpoint = tonic::transport::Endpoint::from_shared(dial.to_string())
            .map_err(|e| format!("invalid endpoint: {e}"))?
            .origin(origin);
        if let Some(tls) = tls {
            endpoint = endpoint
                .tls_config(tls.domain_name(host))
                .map_err(|e| format!("TLS: {e}"))?;
        }
        Ok(endpoint)
    }

    /// `builder` resolving `url`'s host to this address.
    pub(crate) fn resolve(
        self,
        builder: reqwest::ClientBuilder,
        url: &reqwest::Url,
    ) -> Result<reqwest::ClientBuilder, String> {
        // Port 0: reqwest dials the URL's port, which `request_url` sets.
        Ok(builder.resolve(Self::named_host(url)?, SocketAddr::new(self.ip, 0)))
    }

    /// The URL reqwest requests for `url`: this port in place of `url`'s when
    /// one is set, since reqwest dials the URL's port. The host, which TLS
    /// checks, stays.
    pub(crate) fn request_url(self, url: &reqwest::Url) -> Result<String, String> {
        self.with_port(url).map(String::from)
    }

    /// The `Host` header naming `url`'s host and port, as the endpoint gives them.
    pub(crate) fn host_header(url: &reqwest::Url) -> Result<reqwest::header::HeaderValue, String> {
        let host = Self::named_host(url)?;
        let value = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        reqwest::header::HeaderValue::from_str(&value).map_err(|e| format!("Host header: {e}"))
    }
}

impl std::fmt::Display for ConnectTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.port {
            Some(port) => SocketAddr::new(self.ip, port).fmt(f),
            None => self.ip.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The URL reqwest requests for each `connect_to` and the `Host` it sends,
    /// and the settings refused.
    #[test]
    fn connect_to_parses_and_keeps_the_endpoint_name() {
        // (connect_to, endpoint, request URL and Host, or the error text)
        let cases: [(&str, &str, Result<(&str, &str), &str>); 6] = [
            (
                "10.0.0.5:9000",
                "https://collector.example:4318/v1/logs",
                Ok((
                    "https://collector.example:9000/v1/logs",
                    "collector.example:4318",
                )),
            ),
            (
                "10.0.0.5",
                "https://collector.example/v1/logs",
                Ok(("https://collector.example/v1/logs", "collector.example")),
            ),
            (
                "[::1]:4317",
                "http://collector.example:4317",
                Ok(("http://collector.example:4317/", "collector.example:4317")),
            ),
            (
                "collector-2:4317",
                "http://collector.example:4317",
                Err("is not an IP address"),
            ),
            (
                "10.0.0.5",
                "http://192.168.1.9:4318",
                Err("needs an endpoint that names its host"),
            ),
            (
                "",
                "http://collector.example:4318",
                Err("is not an IP address"),
            ),
        ];
        for (connect_to, endpoint, want) in cases {
            let url = reqwest::Url::parse(endpoint).unwrap();
            let got = ConnectTo::parse(connect_to).and_then(|to| {
                let request = to.request_url(&url)?;
                let host = ConnectTo::host_header(&url)?;
                Ok((request, host))
            });
            match (got, want) {
                (Ok((request, host)), Ok((want_request, want_host))) => {
                    assert_eq!(request, want_request, "{connect_to}");
                    assert_eq!(host.to_str().unwrap(), want_host, "{connect_to}");
                }
                (Err(e), Err(text)) => assert!(e.contains(text), "{connect_to}: {e}"),
                (got, want) => panic!("{connect_to}: got {got:?}, want {want:?}"),
            }
        }
        assert_eq!(
            ConnectTo::parse("[::1]:4317").unwrap().to_string(),
            "[::1]:4317"
        );
        assert_eq!(
            ConnectTo::parse(" 10.0.0.5 ").unwrap().to_string(),
            "10.0.0.5"
        );
    }
}
