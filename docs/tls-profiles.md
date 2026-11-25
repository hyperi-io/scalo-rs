# TLS profiles

Every TLS client and server scalo builds names a profile, and the profile sets the protocol versions, cipher suites, key-exchange groups and signature schemes. A connection never builds its own cipher or curve list. There are three profiles, and `strong` is the default. Clients inside other libraries keep those libraries' TLS: Vault through `vaultrs`, AWS Secrets Manager through the AWS SDK, and the OpenTelemetry exporters.

```yaml
transport:
  otlp:
    endpoint: https://collector.example:4317
    tls:
      profile: strong        # strong | commercial | cnsa2
```

Settings that belong to a profile, not a connection, sit once in the cascade and apply to every connection that names it:

```yaml
tls:
  profiles:
    strong:
      allow_tls12: true
```

The connection keys themselves (trust, CA files, client certificate, pins) are in [api/tls.md](api/tls.md).

## The profiles

| | `strong` (default) | `commercial` | `cnsa2` |
| --- | --- | --- | --- |
| TLS | 1.3. 1.2 only with `allow_tls12` | 1.3 and 1.2 | 1.3 only |
| TLS 1.3 suites, in order | AES-256-GCM, ChaCha20-Poly1305, AES-128-GCM | AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305 | AES-256-GCM |
| TLS 1.2 suites | ECDHE-ECDSA and ECDHE-RSA with AES-256-GCM | all six ECDHE AEAD suites | none |
| Groups, in order | X25519MLKEM768, SecP256r1MLKEM768, P-384, X25519, P-256 | X25519MLKEM768, X25519, P-256, P-384 | ML-KEM-1024, P-384 |
| Signatures verified | ECDSA P-256, P-384, P-521, Ed25519, RSA-PSS and RSA PKCS#1 (2048 to 8192 bits), ML-DSA-44, -65, -87 | as `strong` | ECDSA P-384 with SHA-384, ML-DSA-87 |
| Default `allow_tls12` | false | true | refused |

A client sends a key share for its first group. A server that prefers another group the client lists asks for it in one more round trip, so every pair of profiles agrees on something:

| client \ server | `strong` | `commercial` | `cnsa2` |
| --- | --- | --- | --- |
| `strong` | X25519MLKEM768, AES-256 | X25519MLKEM768, AES-256 | P-384, AES-256 |
| `commercial` | X25519MLKEM768, AES-128 | X25519MLKEM768, AES-128 | P-384, AES-256 |
| `cnsa2` | P-384, AES-256 | P-384, AES-256 | ML-KEM-1024, AES-256 |

`src/tls/matrix_tests.rs` runs that table through the one builder on each side.

## Why `strong` is the default

`strong` follows culvert's stance: TLS 1.3 only, AES-256-GCM first, P-384 certificates ([culvert README, "Cryptography and CNSA 2.0"](https://github.com/hyperi-io/culvert#cryptography-and-cnsa-20)). It goes further on key exchange: culvert's OpenSSL base had no ML-KEM, and `strong` leads with the X25519MLKEM768 hybrid, so a peer that has it gets harvest-now-decrypt-later protection and one that lacks it falls back to a classical group. AES-128-GCM sits last because RFC 8446 section 9.1 makes it mandatory to implement.

TLS 1.2 is off by default because what it costs depends on the whole set of peers a deployment talks to, not on one link. Turn it on for the profile when a peer needs it, and every connection on that profile can then fall back. With it on, `strong` still offers only its own AES-256-GCM suites and classical groups.

`commercial` offers every ECDHE AEAD suite with TLS 1.2 on, for the widest reach that is still current. NIST deprecates classical key exchange after 2030, which is when this profile ages out. `cnsa2` is the CNSA 2.0 shape, the same as AWS's CNSA2-INTEROP1 policy.

No profile goes below TLS 1.2 or offers a CBC or RSA key-exchange suite: rustls has none. A peer that needs one gets a named error, and a TLS-terminating proxy in front of it is the fix.

## What each profile connects to (2026-11)

Read from each peer's published capabilities, not from live handshakes:

| Profile | Connects to | Fails with |
| --- | --- | --- |
| `strong` | another scalo, librdkafka on OpenSSL 3.5 to JDK 17 and 21 brokers (classical groups), ClickHouse (P-256, one extra round trip), Postgres, Go 1.26 collectors and Beats, Envoy, Vector, rsyslog, NXLog, the cloud load balancers. Go, Envoy and the load balancers agree the hybrid group | a TLS 1.2-only peer, and a Windows Server 2012 R2 receiver on an RSA certificate, which has no ECDHE RSA GCM suite (an ECDSA certificate or `commercial` fixes it) |
| `strong` with `allow_tls12` | the above, plus Windows Server 2016 and 2019 receivers, rsyslog's gtls driver on RHEL 7, syslog-ng on OpenSSL 1.0.2 and Java 8 SIEMs, each on an ECDHE AES-256-GCM suite | the 2012 R2 RSA case |
| `cnsa2` | another scalo on `cnsa2`, and P-384 peers | ClickHouse's and Postgres's default configs, Envoy and Windows Schannel |

ClickHouse's server takes only P-256 by default and its `ecdhCurve` names no hybrid group, so a connection to it is never post-quantum, whatever the profile.

## TLS 1.2 and the key share

Every TLS 1.2 handshake logs a warning and counts `tls_tls12_handshakes_total{role, profile}`, on clients and servers, Kafka's excepted (below). A client that meets a TLS 1.2-only peer on a profile without `allow_tls12` fails with:

```text
peer supports only TLS 1.2: enable allow_tls12 on profile strong
```

There is no per-connection version setting. A connection chooses a profile by name, and the profile decides.

The hybrid key share is about 1.2 KB, which some middleboxes drop. A client behind one sets `key_share: classical` in its `tls` section: it sends its profile's first classical group and lists the post-quantum ones, so a server that prefers them still gets them, one round trip later.

## FIPS and Kafka

FIPS 140-3 is a build mode, not a profile. No scalo build has it yet: rustls 0.23.46 needs AWS-LC-FIPS 4.x, which NIST has not validated. `profile: fips` fails at config load, naming the reason, and never falls back to another profile.

librdkafka owns its TLS stack on OpenSSL, so for Kafka the profile becomes OpenSSL lists: `ssl.cipher.suites` gets the profile's TLS 1.2 suites, and `cnsa2` also sets `ssl.curves.list` and `ssl.sigalgs.list`. The other profiles leave groups to OpenSSL, because an OpenSSL before 3.5 refuses a list that names an ML-KEM group. A raw `librdkafka_overrides` key wins over all of these.

librdkafka has no setting for a version floor, and `ssl.cipher.suites` reaches TLS 1.2 only, so both go through OpenSSL's own config. Before the first Kafka TLS client, scalo writes `MinProtocol` (TLSv1.3, or TLSv1.2 with `allow_tls12`) and the profile's TLS 1.3 `Ciphersuites` into the file `OPENSSL_CONF` names, when that file is missing or one scalo wrote. OpenSSL reads it once, at its first TLS context, so every Kafka client in a process takes the first one's settings, and a later client on another profile logs a warning. An operator's own file is left alone.

The file sets OpenSSL's system default, so it reaches every OpenSSL user in the process, and any child process that inherits `OPENSSL_CONF`, not Kafka alone. Set `OPENSSL_CONF` to a writable path in the deployment, or call `scalo::transport::kafka::set_openssl_conf()` first thing in `main`, before the runtime starts. With it unset, the first Kafka TLS client warns that the 1.3 floor is not enforced, and a TLS 1.2 broker is reached on the profile's TLS 1.2 suites. librdkafka does not report the version a broker agrees, so Kafka's TLS 1.2 handshakes are not counted: a Kafka profile with `allow_tls12` logs one warning at start instead.

## Failures

A failed handshake reads as one of these, from the alert or error rustls raised, through reqwest, tonic and a raw stream alike (`scalo::tls::TlsFailure`):

| Failure | Message |
| --- | --- |
| `VersionMismatch` | peer supports only TLS 1.2: enable allow_tls12 on profile `<name>` |
| `NoSharedParameters` | the peer refused every suite and group profile `<name>` offers: try profile `<next>` |
| `UntrustedChain` | the peer's certificate does not chain to a trusted CA |
| `NameMismatch` | the peer's certificate does not name the host it was reached at |
| `Expired` | the peer's certificate has expired, or ours has, as the peer reported |
| `NotYetValid` | the peer's certificate is not valid yet |
| `PinMismatch` | the peer's public key matches no configured pin_sha256 |
| `ClientCertRejected` | the peer refused our client certificate, or wanted one and got none |

A client never sees the server's offer, only the alert it sent, so `NoSharedParameters` names the next profile to try rather than what the server holds.
