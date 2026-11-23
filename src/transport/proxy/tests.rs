// Project:   scalo
// File:      src/transport/proxy/tests.rs
// Purpose:   Proxy setting, machine settings and the CONNECT tunnel against a local proxy
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::system::{Settings, from_winhttp, wildcard_to_rule};
use super::test_proxy::{ConnectProxy, Policy, echo};
use super::{ProxyConfig, ProxyDialer, ProxyError, ProxyRefused, Scheme, refusal, rejected};
use crate::sensitive::SensitiveString;
use crate::transport::error::TransportError;
use crate::transport::types::SendResult;

fn url(text: &str) -> ProxyConfig {
    ProxyConfig::Url(SensitiveString::new(text))
}

/// The proxy environment variables, all unset but `set`.
fn proxy_env(set: &[(&'static str, &'static str)]) -> Vec<(&'static str, Option<&'static str>)> {
    [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
        "REQUEST_METHOD",
    ]
    .into_iter()
    .map(|name| {
        let value = set.iter().find(|(n, _)| *n == name).map(|(_, v)| *v);
        (name, value)
    })
    .collect()
}

#[test]
fn the_setting_reads_and_writes_as_one_string() {
    // (json, variant, kind)
    let rows = [
        (r#""none""#, ProxyConfig::None, "none"),
        (r#""""#, ProxyConfig::None, "none"),
        ("null", ProxyConfig::None, "none"),
        (r#""system""#, ProxyConfig::System, "system"),
        (
            r#""http://u:pw-secret@proxy.example:3128""#,
            url("http://u:pw-secret@proxy.example:3128"),
            "url",
        ),
    ];
    for (json, want, kind) in rows {
        let read: ProxyConfig = serde_json::from_str(json).unwrap();
        assert_eq!(read, want, "{json}");
        assert_eq!(read.kind(), kind, "{json}");
        let shown = format!("{read:?} {}", serde_json::to_string(&read).unwrap());
        assert!(!shown.contains("pw-secret"), "{json}: {shown}");
    }
    assert_eq!(ProxyConfig::default(), ProxyConfig::None);
}

#[test]
fn a_proxy_that_is_not_an_http_connect_proxy_fails_at_load() {
    // (setting, error contains)
    let rows = [
        ("socks5://u:pw-secret@proxy.example:1080", "socks5"),
        ("https://u:pw-secret@proxy.example:443", "https"),
        ("not a url pw-secret", "proxy"),
        ("http://u:pw-secret@", "no host"),
    ];
    for (setting, wanted) in rows {
        let error = url(setting).dialer().unwrap_err().to_string();
        assert!(error.contains(wanted), "{setting}: {error}");
        assert!(!error.contains("pw-secret"), "{setting}: {error}");
    }
    assert!(
        url("proxy.example:3128").dialer().is_ok(),
        "no scheme is http"
    );
}

#[test]
fn udp_refuses_any_proxy() {
    for (setting, ok) in [
        (ProxyConfig::None, true),
        (ProxyConfig::System, false),
        (url("http://proxy.example:3128"), false),
    ] {
        let result = setting.refuse_for_udp("syslog udp");
        assert_eq!(result.is_ok(), ok, "{setting:?}");
        if let Err(error) = result {
            assert!(matches!(error, TransportError::Config(_)), "{error}");
            assert!(error.to_string().contains("syslog udp"), "{error}");
        }
    }
}

#[test]
fn winhttp_settings_parse_to_proxy_entries() {
    // (proxy, bypass, http, https, no, bypass_simple)
    let rows = [
        (
            "proxy.corp:8080",
            "",
            "proxy.corp:8080",
            "proxy.corp:8080",
            "",
            false,
        ),
        (
            "http=web.corp:80;https=tls.corp:443",
            "<local>;*.corp.example 10.*;192.168.*.*",
            "web.corp:80",
            "tls.corp:443",
            ".corp.example,10.0.0.0/8,192.168.0.0/16",
            true,
        ),
        ("https=tls.corp:443", "", "", "tls.corp:443", "", false),
        ("", "", "", "", "", false),
    ];
    for (proxy, bypass, http, https, no, simple) in rows {
        let want = Settings {
            http: http.into(),
            https: https.into(),
            no: no.into(),
            bypass_simple: simple,
        };
        assert_eq!(from_winhttp(proxy, bypass), want, "{proxy} {bypass}");
    }
    for (entry, rule) in [
        ("*.example.com", ".example.com"),
        ("10.1.*.*", "10.1.0.0/16"),
        ("10.*", "10.0.0.0/8"),
        ("*", "*"),
        ("host.example", "host.example"),
        ("a.*", "a.*"),
    ] {
        assert_eq!(wildcard_to_rule(entry), rule, "{entry}");
    }
}

#[tokio::test]
async fn system_reads_the_environment_and_its_bypass_list() {
    // (environment, scheme, host, proxied)
    let rows: [(&[(&str, &str)], Scheme, &str, bool); 7] = [
        (&[], Scheme::Tls, "a.example", false),
        (
            &[("HTTPS_PROXY", "http://p:3128")],
            Scheme::Tls,
            "a.example",
            true,
        ),
        (
            &[("HTTPS_PROXY", "http://p:3128")],
            Scheme::Plain,
            "a.example",
            false,
        ),
        (&[("https_proxy", "p:3128")], Scheme::Tls, "a.example", true),
        (
            &[("ALL_PROXY", "http://p:3128")],
            Scheme::Plain,
            "a.example",
            true,
        ),
        (
            &[
                ("HTTPS_PROXY", "http://p:3128"),
                ("NO_PROXY", ".example,10.0.0.0/8"),
            ],
            Scheme::Tls,
            "a.example",
            false,
        ),
        (
            &[("HTTPS_PROXY", "http://p:3128"), ("REQUEST_METHOD", "GET")],
            Scheme::Tls,
            "a.example",
            false,
        ),
    ];
    for (env, scheme, host, proxied) in rows {
        let dialer = temp_env::with_vars(proxy_env(env), || ProxyConfig::System.dialer()).unwrap();
        // The OS store may name a proxy on a developer's machine.
        if cfg!(any(windows, target_os = "macos")) && !proxied {
            continue;
        }
        assert_eq!(
            dialer.proxies(scheme, host, 443),
            proxied,
            "{env:?} {scheme:?}"
        );
    }
}

/// The OS store reads without failing on the machine the tests run on.
#[cfg(any(windows, target_os = "macos"))]
#[test]
fn the_os_proxy_store_reads() {
    let settings = super::system::settings();
    let dialer = temp_env::with_vars(proxy_env(&[]), || ProxyConfig::System.dialer());
    assert!(dialer.is_ok(), "{settings:?}: {:?}", dialer.err());
}

/// The tunnel opens for an allowed CONNECT, with the configured auth, and a
/// refusal comes back with the proxy's status and the target port.
#[tokio::test]
async fn connect_through_the_proxy() {
    let target = echo().await;
    // (policy, credentials, wanted: Ok or the refusal status)
    let rows: [(Policy, Option<(&str, &str)>, Result<(), u16>); 6] = [
        (Policy::Allow, None, Ok(())),
        (Policy::Refuse(403), None, Err(403)),
        (Policy::Auth("Basic dTpwdw==".into()), None, Err(407)),
        (
            Policy::Auth("Basic dTpwdw==".into()),
            Some(("u", "pw")),
            Ok(()),
        ),
        (
            Policy::Auth("Basic dTpwdw==".into()),
            Some(("u", "wrong")),
            Err(407),
        ),
        (Policy::OnlyPort(443), None, Err(403)),
    ];
    for (policy, credentials, wanted) in rows {
        let name = format!("{policy:?} {credentials:?}");
        let proxy = ConnectProxy::start(policy).await;
        let setting = match credentials {
            Some((user, password)) => url(&proxy.url_with(user, password)),
            None => url(&proxy.url()),
        };
        let dialer = setting.dialer().unwrap();
        let connected = dialer
            .connect(Scheme::Plain, "127.0.0.1", target.port())
            .await;
        let seen = proxy.seen();
        assert_eq!(seen.len(), 1, "{name}");
        assert_eq!(seen[0].target, target.to_string(), "{name}");
        match (connected, wanted) {
            (Ok(mut stream), Ok(())) => {
                stream.write_all(b"through").await.unwrap();
                let mut back = [0_u8; 7];
                stream.read_exact(&mut back).await.unwrap();
                assert_eq!(&back, b"through", "{name}");
            }
            (Err(ProxyError::Refused(refused)), Err(status)) => {
                assert_eq!(
                    refused,
                    ProxyRefused {
                        proxy: proxy.addr.to_string(),
                        host: "127.0.0.1".into(),
                        port: target.port(),
                        status,
                    },
                    "{name}"
                );
                let text = refused.to_string();
                assert!(text.contains(&status.to_string()), "{name}: {text}");
                assert!(!text.contains("pw"), "{name}: {text}");
            }
            (got, _) => panic!("{name}: got {got:?}"),
        }
    }
}

#[tokio::test]
async fn none_and_an_unreachable_proxy() {
    let target = echo().await;
    let mut direct = ProxyDialer::direct()
        .connect(Scheme::Plain, "127.0.0.1", target.port())
        .await
        .unwrap();
    direct.write_all(b"x").await.unwrap();
    assert_eq!(direct.read_u8().await.unwrap(), b'x');

    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let error = url(&format!("http://u:pw-secret@{closed}"))
        .dialer()
        .unwrap()
        .connect(Scheme::Tls, "127.0.0.1", target.port())
        .await
        .unwrap_err();
    assert!(matches!(error, ProxyError::Proxy { .. }), "{error:?}");
    assert!(refusal(&error).is_none());
    assert!(!error.to_string().contains("pw-secret"), "{error}");
}

/// TLS runs over the tunnel with the target's name for SNI.
#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_through_the_tunnel() {
    let (front, ca) = super::test_proxy::tls_front(echo().await, &[]).await;
    let proxy = ConnectProxy::start(Policy::Allow).await;
    let stream = url(&proxy.url())
        .dialer()
        .unwrap()
        .connect(Scheme::Tls, "localhost", front.port())
        .await
        .unwrap();
    let connector = crate::tls::test_pki::connector(&ca, None, &[]);
    let mut tls = connector
        .connect(
            rustls_pki_types::ServerName::try_from("localhost").unwrap(),
            stream,
        )
        .await
        .unwrap();
    tls.write_all(b"sealed").await.unwrap();
    let mut back = [0_u8; 6];
    tls.read_exact(&mut back).await.unwrap();
    assert_eq!(&back, b"sealed");
    assert_eq!(
        proxy.seen()[0].target,
        format!("localhost:{}", front.port())
    );
}

#[test]
fn a_refusal_is_found_in_a_cause_chain_and_rejected() {
    let refused = ProxyRefused {
        proxy: "p:3128".into(),
        host: "t.example".into(),
        port: 6514,
        status: 403,
    };
    let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(refused.clone());
    let wrapped = std::io::Error::other(ProxyError::Refused(refused.clone()));
    let chained = std::io::Error::other(wrapped);
    assert_eq!(refusal(boxed.as_ref()), Some(&refused));
    assert_eq!(refusal(&chained), Some(&refused));
    assert_eq!(refusal(&std::io::Error::other("other")), None);

    let SendResult::Reject(TransportError::Connection(text)) = rejected(&refused, "test") else {
        panic!("a refusal is not Reject");
    };
    assert!(text.contains("403") && text.contains("6514"), "{text}");
}
