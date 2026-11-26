// Project:   scalo
// File:      src/tls_config.rs
// Purpose:   The one client and one server TLS config section every transport reads
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The `tls:` sections, as plain data.
//!
//! Every TLS client reads a [`ClientTlsConfig`] and every TLS server a
//! [`ServerTlsConfig`]. They compile without rustls, so a build that parses
//! Kafka or HTTP-server config with no TLS stack still reads and refuses the
//! same keys. `scalo::tls` re-exports them and builds the configs.
//!
//! ```yaml
//! tls:
//!   profile: strong            # strong | commercial | cnsa2
//!   trust: native              # native | platform | bundle
//!   ca_paths: [/etc/tls/ca.pem]
//!   client_cert_path: /etc/tls/client.pem
//!   client_key_path: /etc/tls/client.key
//!   pin_sha256: ["47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="]
//!   server_name: logs.example.net
//!   key_share: profile         # profile | classical
//! ```
//!
//! A CA list takes a single path in place of the list.

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize};

use crate::crypto::CryptoProfile;
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
use crate::crypto::KeyShare;

/// A removed config key. Any value for it fails the load, naming the key that
/// replaced it, so a config written for the old shape can't load with its TLS
/// settings dropped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Removed;

/// One private deserialiser per removed key, each failing with the key's
/// replacement: `name: "old_key" => "what to set";`.
macro_rules! removed_keys {
    ($($name:ident: $old:literal => $new:literal;)*) => {$(
        fn $name<'de, D: serde::Deserializer<'de>>(
            _: D,
        ) -> Result<$crate::tls_config::Removed, D::Error> {
            Err(serde::de::Error::custom(concat!(
                "`", $old, "` is removed: set ", $new, " (docs/migrations.md)"
            )))
        }
    )*};
}
pub(crate) use removed_keys;

/// One path or a list of them.
#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(PathBuf),
    Many(Vec<PathBuf>),
}

/// A CA list read from a single path or a list.
fn one_or_many<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<PathBuf>, D::Error> {
    Ok(match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(path) => vec![path],
        OneOrMany::Many(paths) => paths,
    })
}

#[cfg(feature = "config-schema")]
fn one_or_many_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "anyOf": [
            { "type": "string" },
            { "type": "array", "items": { "type": "string" } }
        ],
        "description": "A PEM file path, or a list of them."
    })
}

/// Which certificates a client accepts a server's certificate from.
///
/// Every choice trusts the CA files given beside it, and `bundle` trusts
/// nothing else.
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum TrustChoice {
    /// The OS root store, read through `rustls-native-certs` and checked by
    /// rustls.
    #[default]
    Native,
    /// The OS verifier, through `rustls-platform-verifier`, so Windows and
    /// macOS apply their own policy and revocation checks.
    Platform,
    /// Only the CA files given: an exclusive pin, with no OS root trusted.
    Bundle,
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl TrustChoice {
    /// The name config uses for this choice.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Platform => "platform",
            Self::Bundle => "bundle",
        }
    }
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl fmt::Display for TrustChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A key pin that is not a SHA-256 in base64 or hex.
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("TLS: key pin {pin:?} is not a SHA-256 in base64 or 64 hex digits")]
pub struct InvalidPin {
    /// The pin as written. It hashes a public key, so it is not secret.
    pub pin: String,
}

/// The SHA-256 of a certificate's SubjectPublicKeyInfo: a pin on the key, so a
/// certificate re-issued on the same key keeps it.
///
/// Written in base64 or as 64 hex digits. `docs/api/tls.md` has the openssl
/// line that prints a certificate's pin.
///
/// ```rust
/// let pin: scalo::tls_config::SpkiPin = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".parse()?;
/// assert_eq!(pin.to_string(), "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
/// # Ok::<(), scalo::tls_config::InvalidPin>(())
/// ```
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpkiPin(pub(crate) [u8; 32]);

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl std::str::FromStr for SpkiPin {
    type Err = InvalidPin;

    fn from_str(text: &str) -> Result<Self, InvalidPin> {
        use base64::Engine as _;

        let bytes: Option<Vec<u8>> =
            if text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit()) {
                text.as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| {
                        std::str::from_utf8(pair)
                            .ok()
                            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                    })
                    .collect()
            } else {
                base64::engine::general_purpose::STANDARD.decode(text).ok()
            };
        bytes
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .map(Self)
            .ok_or_else(|| InvalidPin {
                pin: text.to_string(),
            })
    }
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl fmt::Display for SpkiPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use base64::Engine as _;

        f.write_str(&base64::engine::general_purpose::STANDARD.encode(self.0))
    }
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl fmt::Debug for SpkiPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SpkiPin").field(&self.to_string()).finish()
    }
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl Serialize for SpkiPin {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl<'de> Deserialize<'de> for SpkiPin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(all(
    feature = "config-schema",
    any(feature = "tls", feature = "transport-kafka-config")
))]
impl schemars::JsonSchema for SpkiPin {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SpkiPin".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^([0-9A-Fa-f]{64}|[A-Za-z0-9+/]{43}=)$",
            "description": "SHA-256 of a server certificate's SubjectPublicKeyInfo, in base64 or 64 hex digits."
        })
    }
}

/// The `tls:` section every TLS client reads: a profile, a trust choice, CA
/// files, an optional client certificate, key pins, the name the server's
/// certificate is checked against, and the key share sent first.
///
/// A transport that cannot honour a key says so at config load: Kafka takes no
/// pin, server name or key share, and `platform` trust only on Windows.
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default, deny_unknown_fields)]
pub struct ClientTlsConfig {
    /// `strong` (default), `commercial` or `cnsa2`. Per-profile settings, such
    /// as `allow_tls12`, live in the `tls.profiles` cascade section.
    pub profile: CryptoProfile,
    /// `native` (default), `platform` or `bundle`, as in [`TrustChoice`].
    pub trust: TrustChoice,
    /// CA PEM files, trusted beside the OS store under `native` and
    /// `platform`, and alone under `bundle`. One path or a list.
    #[serde(deserialize_with = "one_or_many")]
    #[cfg_attr(
        feature = "config-schema",
        schemars(schema_with = "one_or_many_schema")
    )]
    pub ca_paths: Vec<PathBuf>,
    /// Client certificate PEM for mutual TLS, with `client_key_path`.
    pub client_cert_path: Option<PathBuf>,
    /// Client private key PEM for mutual TLS, with `client_cert_path`. May
    /// name the same file when it holds both.
    pub client_key_path: Option<PathBuf>,
    /// SHA-256 pins on the server's public key, as in [`SpkiPin`]. Once the
    /// chain verifies, the key must match one. Empty checks no key.
    pub pin_sha256: Vec<SpkiPin>,
    /// The name checked against the server's certificate and sent as SNI.
    /// Default the endpoint's host.
    pub server_name: Option<String>,
    /// `profile` (default) sends the profile's first group, a post-quantum
    /// one. `classical` sends a classical share first.
    pub key_share: KeyShare,
    /// Removed: `ca_paths`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_ca_path")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub ca_path: Removed,
    /// Removed, from the Vector compat section: `trust`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_native_roots")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub native_roots: Removed,
    /// Removed, from the Vector compat section: `ca_paths`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_extra_roots")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub extra_roots: Removed,
    /// Removed, from the Vector compat section: `trust: bundle`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_exclusive")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub exclusive: Removed,
    /// Removed, from the Vector compat section: `server_name`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_domain")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub domain: Removed,
    /// Removed, from the Vector compat section: `client_cert_path`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_client_cert")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub client_cert: Removed,
    /// Removed, from the Vector compat section: `client_key_path`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_client_key")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub client_key: Removed,
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
crate::tls_config::removed_keys! {
    removed_ca_path: "ca_path" => "ca_paths, one path or a list";
    removed_native_roots: "native_roots" => "trust: native, or trust: bundle to leave the OS store out";
    removed_extra_roots: "extra_roots" => "ca_paths";
    removed_exclusive: "exclusive" => "trust: bundle";
    removed_domain: "domain" => "server_name";
    removed_client_cert: "client_cert" => "client_cert_path";
    removed_client_key: "client_key" => "client_key_path";
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl ClientTlsConfig {
    /// The client certificate and key, both or neither.
    ///
    /// # Errors
    ///
    /// When only one is set.
    pub fn identity_paths(&self) -> Result<Option<(&PathBuf, &PathBuf)>, String> {
        match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert), Some(key)) => Ok(Some((cert, key))),
            (None, None) => Ok(None),
            _ => Err("mutual TLS needs both client_cert_path and client_key_path".to_string()),
        }
    }
}

/// Whose certificate a failure is about.
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Whose {
    /// The certificate the other end presented.
    Peer,
    /// The certificate this end presented, refused by the other end.
    Ours,
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl fmt::Display for Whose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Peer => "the peer's",
            Self::Ours => "our",
        })
    }
}

/// Why a TLS handshake failed, worded to name the fix. `scalo::tls` reads it
/// from a rustls error chain, and the Kafka transport from librdkafka's
/// OpenSSL text.
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TlsFailure {
    /// The peer has no TLS version the profile offers.
    #[error("{}", version_hint(*profile, *tls12_offered))]
    VersionMismatch {
        /// The profile in use.
        profile: CryptoProfile,
        /// Whether TLS 1.2 was offered beside TLS 1.3.
        tls12_offered: bool,
    },
    /// The peer refused every suite, group and signature scheme the profile
    /// offers.
    #[error(
        "the peer refused every suite and group profile `{profile}` offers{}",
        wider.map_or_default(|p| format!(": try profile `{p}`"))
    )]
    NoSharedParameters {
        /// The profile in use.
        profile: CryptoProfile,
        /// The next profile to try, when a wider one exists.
        wider: Option<CryptoProfile>,
    },
    /// The peer's certificate does not chain to a trusted CA, or a signature
    /// in the chain does not verify.
    #[error("the peer's certificate does not chain to a trusted CA")]
    UntrustedChain,
    /// The peer's certificate does not name the host it was reached at.
    #[error("the peer's certificate does not name the host it was reached at")]
    NameMismatch,
    /// A certificate is past its `notAfter` time.
    #[error("{whose} certificate has expired")]
    Expired {
        /// The peer's, or ours as the peer reported.
        whose: Whose,
    },
    /// A certificate is before its `notBefore` time.
    #[error("{whose} certificate is not valid yet")]
    NotYetValid {
        /// The peer's.
        whose: Whose,
    },
    /// The peer's key matches no configured `pin_sha256`.
    #[error("the peer's public key matches no configured pin_sha256")]
    PinMismatch,
    /// The peer refused our client certificate, or wanted one and got none.
    #[error("the peer refused our client certificate, or wanted one and got none")]
    ClientCertRejected,
}

/// The version mismatch message: the fix when TLS 1.2 was held back, else
/// the versions the peer lacks.
#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
fn version_hint(profile: CryptoProfile, tls12_offered: bool) -> String {
    if tls12_offered {
        format!("the peer has neither TLS 1.3 nor 1.2, which profile {profile} offers")
    } else if profile.has_tls12() {
        format!("peer supports only TLS 1.2: enable allow_tls12 on profile {profile}")
    } else {
        format!("the peer has no TLS 1.3, and profile {profile} has no TLS 1.2")
    }
}

#[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
impl TlsFailure {
    /// The version mismatch in OpenSSL's error text `detail`, for a
    /// connection on `profile`. `None` for any other failure.
    #[must_use]
    pub fn from_openssl(detail: &str, profile: CryptoProfile, tls12_offered: bool) -> Option<Self> {
        let detail = detail.to_ascii_lowercase();
        ["alert protocol version", "unsupported protocol"]
            .iter()
            .any(|marker| detail.contains(marker))
            .then_some(Self::VersionMismatch {
                profile,
                tls12_offered,
            })
    }
}

/// Whether a server asks clients for a certificate.
#[cfg(any(feature = "tls", feature = "http-server"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum ClientAuth {
    /// No client certificate is asked for.
    #[default]
    None,
    /// A client certificate is checked against the CA when one is presented.
    #[serde(alias = "request")]
    Optional,
    /// Every client presents a certificate that chains to the CA.
    #[serde(alias = "require")]
    Required,
}

#[cfg(any(feature = "tls", feature = "http-server"))]
impl std::fmt::Display for ClientAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Optional => "optional",
            Self::Required => "required",
        })
    }
}

/// The `tls:` section every TLS server reads: a profile, and where its
/// certificate, key and client CA come from.
///
/// The certificate and key each take a `*_file` path or a `*_secret` spec,
/// never both, and the client CA takes `ca_files` or `ca_secret`. A spec is
/// anything `scalo::secrets::resolve` reads (`vault:`, `file:`, `aws:`,
/// `env:`) and needs the `secrets` feature.
///
/// ```yaml
/// tls:
///   profile: strong
///   cert_file: /etc/tls/tls.crt       # or cert_secret: vault:kv/app/tls:cert
///   key_file: /etc/tls/tls.key        # or key_secret
///   ca_files: /etc/tls/ca.crt         # or a list, or ca_secret
///   client_auth: none                 # none | optional | required
///   reload_interval_secs: 30
///   handshake_timeout_ms: 10000
/// ```
#[cfg(any(feature = "tls", feature = "http-server"))]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default, deny_unknown_fields)]
pub struct ServerTlsConfig {
    /// `strong` (default), `commercial` or `cnsa2`.
    pub profile: CryptoProfile,
    /// PEM certificate chain, leaf first.
    pub cert_file: Option<PathBuf>,
    /// Secret spec for the PEM certificate chain.
    pub cert_secret: Option<String>,
    /// PEM private key.
    pub key_file: Option<PathBuf>,
    /// Secret spec for the PEM private key.
    pub key_secret: Option<String>,
    /// PEM CA files client certificates must chain to. One path or a list.
    #[serde(deserialize_with = "one_or_many")]
    #[cfg_attr(
        feature = "config-schema",
        schemars(schema_with = "one_or_many_schema")
    )]
    pub ca_files: Vec<PathBuf>,
    /// Secret spec for the PEM CA bundle.
    pub ca_secret: Option<String>,
    /// Whether clients are asked for a certificate. Default `none`.
    pub client_auth: ClientAuth,
    /// How often the sources are re-read, in seconds. `0` turns reload off.
    /// Default 30.
    pub reload_interval_secs: u64,
    /// How long a client has to finish its handshake, in milliseconds.
    /// Default 10,000.
    pub handshake_timeout_ms: u64,
    /// Removed: `ca_files`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_ca_file")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub ca_file: Removed,
}

#[cfg(any(feature = "tls", feature = "http-server"))]
crate::tls_config::removed_keys! {
    removed_ca_file: "ca_file" => "ca_files, one path or a list";
}

#[cfg(any(feature = "tls", feature = "http-server"))]
impl Default for ServerTlsConfig {
    fn default() -> Self {
        Self {
            profile: CryptoProfile::default(),
            cert_file: None,
            cert_secret: None,
            key_file: None,
            key_secret: None,
            ca_files: Vec::new(),
            ca_secret: None,
            client_auth: ClientAuth::None,
            reload_interval_secs: 30,
            handshake_timeout_ms: 10_000,
            ca_file: Removed,
        }
    }
}

#[cfg(any(feature = "tls", feature = "http-server"))]
impl std::fmt::Debug for ServerTlsConfig {
    /// Secret specs print as set or unset: a literal spec is the secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set = |spec: &Option<String>| spec.as_ref().map(|_| "<set>");
        f.debug_struct("ServerTlsConfig")
            .field("profile", &self.profile)
            .field("cert_file", &self.cert_file)
            .field("cert_secret", &set(&self.cert_secret))
            .field("key_file", &self.key_file)
            .field("key_secret", &set(&self.key_secret))
            .field("ca_files", &self.ca_files)
            .field("ca_secret", &set(&self.ca_secret))
            .field("client_auth", &self.client_auth)
            .field("reload_interval_secs", &self.reload_interval_secs)
            .field("handshake_timeout_ms", &self.handshake_timeout_ms)
            .finish_non_exhaustive()
    }
}

#[cfg(any(feature = "tls", feature = "http-server"))]
impl ServerTlsConfig {
    /// A config serving the certificate and key at these paths.
    #[must_use]
    pub fn from_files(cert_file: impl Into<PathBuf>, key_file: impl Into<PathBuf>) -> Self {
        Self {
            cert_file: Some(cert_file.into()),
            key_file: Some(key_file.into()),
            ..Self::default()
        }
    }
}

/// Each `(document, replacement)` row fails to load into `T`, through the
/// YAML parser and through figment, with an error naming the replacement.
/// Each document is a JSON object, which is YAML too.
#[cfg(all(test, feature = "yaml", feature = "figment"))]
pub(crate) fn assert_removed<T: serde::de::DeserializeOwned>(rows: &[(&str, &str)]) {
    for (document, replacement) in rows {
        let errors = [
            crate::yaml::from_str::<T>(document, "removed", crate::yaml::Profile::Config)
                .err()
                .map(|e| e.to_string()),
            figment::Figment::from(
                <figment::providers::Json as figment::providers::Format>::string(document),
            )
            .extract::<T>()
            .err()
            .map(|e| e.to_string()),
        ];
        for (reader, error) in ["yaml", "figment"].iter().zip(errors) {
            let Some(error) = error else {
                panic!("{reader} loaded {document}");
            };
            assert!(
                error.contains("is removed") && error.contains(replacement),
                "{reader}, {document}: {error}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::de::value::{Error, StrDeserializer};

    use super::*;

    /// A removed key in a `tls` section fails the load, naming its
    /// replacement, where it was dropped.
    #[cfg(all(feature = "yaml", feature = "figment"))]
    #[test]
    fn a_removed_tls_key_fails_the_load() {
        #[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
        assert_removed::<ClientTlsConfig>(&[
            (r#"{"ca_path": "/ca.pem"}"#, "ca_paths"),
            (r#"{"native_roots": false}"#, "trust: bundle"),
            (r#"{"extra_roots": ["/ca.pem"]}"#, "ca_paths"),
            (r#"{"exclusive": true}"#, "trust: bundle"),
            (r#"{"domain": "source"}"#, "server_name"),
            (r#"{"client_cert": "/c.pem"}"#, "client_cert_path"),
            (r#"{"client_key": "/k.pem"}"#, "client_key_path"),
        ]);
        #[cfg(any(feature = "tls", feature = "http-server"))]
        assert_removed::<ServerTlsConfig>(&[(r#"{"ca_file": "/ca.pem"}"#, "ca_files")]);
    }

    /// Base64 and hex of the same 32 bytes read as one pin, which prints as
    /// base64. Any other text fails to parse, from text and from config.
    #[cfg(any(feature = "tls", feature = "transport-kafka-config"))]
    #[test]
    fn a_pin_reads_base64_or_hex_and_nothing_else() {
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD;

        let base64 = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        let hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let from_base64: SpkiPin = base64.parse().unwrap();
        for text in [hex, hex.to_uppercase().as_str()] {
            assert_eq!(text.parse::<SpkiPin>().unwrap(), from_base64, "{text}");
        }
        assert_eq!(from_base64.to_string(), base64);

        let bad = [
            ("empty", String::new()),
            (
                "base64 without its padding",
                base64.trim_end_matches('=').to_string(),
            ),
            ("base64 of 31 bytes", STANDARD.encode([0u8; 31])),
            // 48 zero bytes are 64 `A`s, which read as hex, so use 0xff.
            (
                "base64 of 48 bytes, a SHA-384",
                STANDARD.encode([0xffu8; 48]),
            ),
            ("63 hex digits", hex[1..].to_string()),
            ("64 characters, one not hex", format!("{}g", &hex[1..])),
            ("hex with a leading plus", format!("+{}", &hex[1..])),
            ("a pin with spaces around it", format!(" {base64} ")),
        ];
        for (name, text) in bad {
            let error = text.parse::<SpkiPin>().unwrap_err().to_string();
            assert!(
                error.contains("is not a SHA-256 in base64 or 64 hex digits"),
                "{name}: {error}"
            );
            let from_config = SpkiPin::deserialize(StrDeserializer::<Error>::new(&text));
            assert!(from_config.is_err(), "{name} must not load from config");
        }
    }

    /// A CA list reads from one path or a list, and is empty when absent.
    #[test]
    fn a_ca_list_reads_from_one_path_or_a_list() {
        use serde::de::value::SeqDeserializer;

        let one = one_or_many(StrDeserializer::<Error>::new("/etc/tls/ca.pem")).unwrap();
        assert_eq!(one, [PathBuf::from("/etc/tls/ca.pem")]);
        let many = SeqDeserializer::<_, Error>::new(vec!["/a.pem", "/b.pem"].into_iter());
        assert_eq!(
            one_or_many(many).unwrap(),
            [PathBuf::from("/a.pem"), PathBuf::from("/b.pem")]
        );
    }
}
