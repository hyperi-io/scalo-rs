# TLS

The `tls` feature covers both ends of a TLS connection. Every client reads one config section, `ClientTlsConfig`, and builds through `build_client_tls`. Every server reads `ServerTlsConfig` and builds a `ServerTls`, a certificate that reloads without a restart, with one handshake helper for every kind of listener. Both sections name a profile: [tls-profiles.md](../tls-profiles.md) says what each profile offers and connects to.

## Client trust

```yaml
tls:
  profile: strong            # strong | commercial | cnsa2
  trust: native              # native | platform | bundle
  ca_paths: [/etc/tls/ca.pem]
  client_cert_path: /etc/tls/client.pem
  client_key_path: /etc/tls/client.key
  pin_sha256: ["47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="]
  server_name: logs.example.net
  key_share: profile         # profile | classical
```

The HTTP, gRPC, OTLP (both protocols), syslog, Vector-compat and Kafka transports all take this section under `tls`. A CA list takes a single path in place of the list. An unknown key fails the load, so the old one-file `ca_path` is refused by name.

| `trust` | Accepts a server certificate from | CA files |
| --- | --- | --- |
| `native` (default) | the OS root store, read through `rustls-native-certs` and checked by rustls | trusted beside the OS store |
| `platform` | the OS verifier through `rustls-platform-verifier`, which applies Windows and macOS policy and revocation | trusted beside the OS store |
| `bundle` | the CA files only, the exclusive pin | required |

```rust
use scalo::tls::{ClientTls, TrustChoice, build_client_tls};

let pinned = ClientTls::pinned("/etc/tls/ca.pem")
    .with_identity("/etc/tls/client.pem", "/etc/tls/client.key");
let os = ClientTls::new(TrustChoice::Platform);
let config = build_client_tls(&pinned)?;
```

`ClientTlsConfig::client_tls()` maps a section onto `ClientTls` and reads the `tls.profiles` settings from the cascade then. A `ClientTls` built in code reads them when the config is built, or takes them from `with_profiles`. The identity is a PEM certificate chain and its key, and both paths may name one file holding the pair. Every file is read when the config is built, so the build fails when a file is missing, holds no certificate or key, or the key does not match the certificate. `bundle` with no CA file trusts nothing and is refused.

`pin_sha256` pins the server's public key under any trust choice: the chain must still verify, then the leaf's key must match one pin. A certificate re-issued on the same key keeps working, and a re-keyed one is refused and logged at warn with its pin, so list both across a rotation. A pin is the SHA-256 of the SubjectPublicKeyInfo in base64 or 64 hex digits. Other text fails the load with `TlsError::InvalidPin`.

```bash
openssl x509 -in server.pem -pubkey -noout | openssl pkey -pubin -outform der \
  | openssl dgst -sha256 -binary | base64
```

`server_name` is the name the certificate is checked against and sent as SNI, in place of the endpoint's host. gRPC, OTLP over gRPC, Vector compat and syslog take it. HTTP and OTLP over HTTP check the endpoint's host, and refuse it. Kafka refuses pins, `server_name` and a classical `key_share`, takes `platform` on Windows only, and maps the rest onto librdkafka's properties ([tls-profiles.md](../tls-profiles.md#fips-and-kafka)).

The gRPC transports run this config over each stream they dial, so gRPC takes every trust choice, pins and the profile like the rest. The channel asks for ALPN `h2` and refuses a server that does not agree to it. scalo enables none of tonic's own TLS features.

The build logs the profile, whether TLS 1.2 is on, the trust choice, the number of CA files and whether mutual TLS is on at info, each file read at debug, and a native root that could not be read at warn. Paths are logged, never key material.

## Failures

`TlsFailure::find` takes an error chain, reqwest's and tonic's included, and the profile with its settings, and says why the handshake failed: `VersionMismatch`, `NoSharedParameters`, `UntrustedChain`, `NameMismatch`, `Expired`, `NotYetValid`, `PinMismatch` or `ClientCertRejected`, each worded to name the fix. A transport whose handshake fails reports that text.

---

On the server side, `ServerTls` reads a certificate, its key and an optional client CA, and builds a rustls `ServerConfig` on the section's profile. The certificate and key each come from a file or a secret spec, never both, and the client CA from `ca_files` or `ca_secret`:

```yaml
tls:
  profile: strong
  cert_file: /etc/tls/tls.crt         # or cert_secret: vault:kv/app/tls:cert
  key_file: /etc/tls/tls.key          # or key_secret
  ca_files: /etc/tls/ca.crt           # or a list, or ca_secret; read only with client_auth
  client_auth: none                   # none | optional | required
  reload_interval_secs: 30            # 0 turns reload off
  handshake_timeout_ms: 10000
```

A secret spec is anything `scalo::secrets::resolve` reads (`vault:`, `file:`, `aws:`, `env:`), so a `*_secret` field needs the `secrets` feature as well. Without it the load fails with `TlsError::SecretsUnsupported`.

```rust
use scalo::tls::{ServerTls, ServerTlsConfig, ServerTlsOptions};

let tls = ServerTls::load_with(&config.tls, ServerTlsOptions::http()).await?;
```

`load` uses the default options: no ALPN, and the `tls.profiles` settings from the cascade. `ServerTlsOptions::http()` offers `h2` then `http/1.1`, and `with_profiles` takes the settings from code.

Startup rejects, and nothing binds, when:

- the certificate or key has no source, or one has both a file and a secret
- `client_auth` is `optional` or `required` with no CA, or the CA is set by both `ca_files` and `ca_secret`
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
| `tls_handshakes_total` | `protocol` (`1.2`, `1.3`), `key_exchange` (`post_quantum`, `classical`) |
| `tls_handshake_failures_total` | `reason` (`error`, `timeout`) |
| `tls_certificate_reloads_total` | `result` (`success`, `failure`) |
| `tls_tls12_handshakes_total` | `role` (`client`, `server`), `profile` |

A failed handshake logs at debug and emits a `tls.event` security event with the peer address. A TLS 1.2 handshake, on either side, logs at warn.

For axum, `scalo::http_server::TlsListener` wraps a `TcpListener` and a `ServerTls`, runs each handshake in its own task, and hands axum the finished streams. `HttpServer` uses it, see [http-server.md](http-server.md#tls).

`ServerTls::acceptor()` returns a `tokio_rustls::TlsAcceptor` on the config in service, for a caller driving its own handshake. Take one per connection, because a client CA change replaces the config.
