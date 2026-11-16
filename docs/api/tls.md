# TLS

The `tls` feature covers both ends of a TLS connection. Client trust (`TlsTrust`, `build_client_config`) is described in the `scalo::tls` rustdoc. This page is the server side: `ServerTls`, a certificate that reloads without a restart, and one handshake helper for every kind of listener.

`ServerTls` reads a certificate, its key and an optional client CA, and builds a rustls `ServerConfig` with the aws-lc-rs provider on a `CryptoProfile`. Each of the three comes from a file or from a secret spec, never both:

```yaml
tls:
  cert_file: /etc/tls/tls.crt         # or cert_secret: vault:kv/app/tls:cert
  key_file: /etc/tls/tls.key          # or key_secret
  ca_file: /etc/tls/ca.crt            # or ca_secret, read only with client_auth
  client_auth: none                   # none | optional | required
  reload_interval_secs: 30            # 0 turns reload off
  handshake_timeout_ms: 10000
```

A secret spec is anything `scalo::secrets::resolve` reads (`vault:`, `file:`, `aws:`, `env:`), so a `*_secret` field needs the `secrets` feature as well. Without it the load fails with `TlsError::SecretsUnsupported`.

```rust
use scalo::tls::{ServerTls, ServerTlsConfig, ServerTlsOptions};

let tls = ServerTls::load_with(&config.tls, ServerTlsOptions::http()).await?;
```

`load` uses the default options: the `Prod` profile and no ALPN. `ServerTlsOptions::http()` offers `h2` then `http/1.1`. `with_profile(CryptoProfile::HighSec)` serves TLS 1.3 with hybrid post-quantum key exchange only.

Startup refuses, and nothing binds, when:

- the certificate or key has no source, or one has both a file and a secret
- `client_auth` is `optional` or `required` with no CA
- a file cannot be read, a PEM holds no certificate or key, or the key does not match the certificate

A CA set with `client_auth: none` checks nothing. It is logged and not read.

---

## Reload

The certificate in service sits in a `CertResolver`, the rustls `ResolvesServerCert` that every handshake asks. A reload swaps it in place, so every listener built from the same `ServerTls` serves the new certificate from its next handshake. Open connections keep the certificate they started with.

A task re-reads the sources every `reload_interval_secs`. Polling catches a Kubernetes Secret volume's symlink swap and a rotated OpenBao field the same way. The task holds the `ServerTls` weakly and stops once the last clone is dropped.

| Change | Result |
| --- | --- |
| certificate and key replaced | new certificate from the next handshake |
| client CA replaced | new config, used by the next `accept` |
| unreadable file, bad PEM, key that does not match | the certificate in service stays, one warning |
| nothing changed | nothing |

A certificate written before its key fails the key match for one poll and lands on the next. The failure is logged once and again only when it changes, with a `tls.event` security event. Every outcome is counted on `tls_certificate_reloads_total{result}`.

`ServerTls::reload()` runs the same reload on demand, for a caller with its own trigger such as a secrets rotation event. It returns `Reloaded { certificate, client_ca }`.

---

## Accepting connections

`ServerTls::accept(io, peer)` runs the server handshake on any `AsyncRead + AsyncWrite` stream, bounded by `handshake_timeout_ms`. A TCP listener that is not HTTP calls it per connection:

```rust
let (stream, peer) = listener.accept().await?;
let tls = tls.clone();
tokio::spawn(async move {
    let Ok(stream) = tls.accept(stream, Some(peer)).await else {
        return; // counted and logged by accept
    };
    // read frames from `stream`
});
```

| Metric | Labels |
| --- | --- |
| `tls_handshakes_total` | `protocol` (`1.2`, `1.3`), `key_exchange` (`hybrid`, `classical`) |
| `tls_handshake_failures_total` | `reason` (`error`, `timeout`) |
| `tls_certificate_reloads_total` | `result` (`success`, `failure`) |

A failed handshake also logs at debug and emits a `tls.event` security event with the peer address.

For axum, `scalo::http_server::TlsListener` wraps a `TcpListener` and a `ServerTls`, runs each handshake in its own task, and hands axum the finished streams. `HttpServer` uses it, see [http-server.md](http-server.md#tls).

`ServerTls::acceptor()` returns a `tokio_rustls::TlsAcceptor` on the config in service, for a caller driving its own handshake. Take one per connection, because a client CA change replaces the config.
