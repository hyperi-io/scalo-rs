// Project:   scalo
// File:      src/transport/kafka/openssl_floor.rs
// Purpose:   The TLS version floor for librdkafka's OpenSSL, set through OPENSSL_CONF
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! librdkafka has no property for a TLS version, so the profile's floor
//! reaches its OpenSSL through the config file `OPENSSL_CONF` names. OpenSSL
//! reads `MinProtocol` from it when it creates its first TLS context, once per
//! process, so every Kafka client in a process takes the first one's floor.
//!
//! scalo writes that file before its first Kafka TLS client. It treats the
//! file as its own when it does not exist yet or starts with [`FIRST_LINE`],
//! and leaves an operator's file alone. With `OPENSSL_CONF` unset the floor
//! is not set: the deployment sets the variable, or a binary calls
//! [`set_openssl_conf`] first thing in `main`.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::crypto::CryptoProfile;
use crate::tls_config::TlsFailure;

/// The first line of every file scalo writes, which marks it as scalo's.
const FIRST_LINE: &str = "# scalo: the TLS version floor for Kafka clients, rewritten at start";

/// What the first Kafka TLS client found.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Held {
    /// The file `OPENSSL_CONF` names holds this floor.
    Written(&'static str),
    /// `OPENSSL_CONF` names the operator's own file.
    Operator(PathBuf),
    /// `OPENSSL_CONF` is unset, so OpenSSL keeps its own floor.
    Unset,
    /// The file could not be written.
    Failed(String),
}

/// The posture the process's Kafka clients run on, and what held.
struct Floor {
    profile: CryptoProfile,
    tls12: bool,
}

static FLOOR: OnceLock<Floor> = OnceLock::new();

/// OpenSSL's name for the lowest version `tls12` allows.
fn min_protocol(tls12: bool) -> &'static str {
    if tls12 { "TLSv1.2" } else { "TLSv1.3" }
}

/// An OpenSSL config setting only the floor, which OpenSSL applies to every
/// context it creates.
fn config_text(min: &str) -> String {
    format!(
        "{FIRST_LINE}\n\
         openssl_conf = scalo_openssl\n\n\
         [scalo_openssl]\n\
         ssl_conf = scalo_ssl\n\n\
         [scalo_ssl]\n\
         system_default = scalo_tls\n\n\
         [scalo_tls]\n\
         MinProtocol = {min}\n"
    )
}

/// Write `text` to `path` in one rename, so OpenSSL never reads half of it.
fn write_whole(path: &Path, text: &str) -> std::io::Result<()> {
    let staged = path.with_extension("staged");
    std::fs::write(&staged, text)?;
    std::fs::rename(&staged, path)
}

/// Whether `path` is free for scalo to write: missing, or scalo's.
fn is_ours(path: &Path) -> bool {
    match std::fs::read_to_string(path) {
        Ok(text) => text.lines().next() == Some(FIRST_LINE),
        Err(e) => e.kind() == std::io::ErrorKind::NotFound,
    }
}

/// Set the floor for `profile` before the first Kafka TLS client, and warn
/// when it cannot be set or differs from the one already set.
pub(super) fn apply(profile: CryptoProfile, tls12: bool) {
    let floor = FLOOR.get_or_init(|| {
        let held = match std::env::var_os("OPENSSL_CONF").map(PathBuf::from) {
            None => Held::Unset,
            Some(path) if !is_ours(&path) => Held::Operator(path),
            Some(path) => {
                let min = min_protocol(tls12);
                match write_whole(&path, &config_text(min)) {
                    Ok(()) => Held::Written(min),
                    Err(e) => Held::Failed(format!("{}: {e}", path.display())),
                }
            }
        };
        report(profile, tls12, &held);
        Floor { profile, tls12 }
    });
    if (floor.profile, floor.tls12) != (profile, tls12) {
        tracing::warn!(
            profile = profile.as_str(),
            allow_tls12 = tls12,
            first_profile = floor.profile.as_str(),
            first_allow_tls12 = floor.tls12,
            "kafka: OpenSSL takes one TLS floor per process, so this client keeps the first \
             Kafka client's"
        );
    }
}

/// Log what the first Kafka TLS client found, once.
fn report(profile: CryptoProfile, tls12: bool, held: &Held) {
    match held {
        Held::Written(min) => {
            tracing::info!(
                profile = profile.as_str(),
                min,
                "kafka: OpenSSL TLS floor set"
            );
        }
        Held::Operator(path) => tracing::info!(
            path = %path.display(),
            "kafka: OPENSSL_CONF names a file scalo did not write, so its TLS floor is the \
             operator's"
        ),
        Held::Unset if !tls12 => tracing::warn!(
            profile = profile.as_str(),
            "kafka: the TLS 1.3 floor is not enforced: OPENSSL_CONF is unset, so librdkafka's \
             OpenSSL also takes TLS 1.2, on the profile's TLS 1.2 suites. Set OPENSSL_CONF to a \
             writable path in the deployment, or call \
             scalo::transport::kafka::set_openssl_conf first thing in main"
        ),
        Held::Unset => {}
        Held::Failed(why) => tracing::warn!(
            error = %why,
            "kafka: the OpenSSL TLS floor file could not be written, so the floor is not \
             enforced"
        ),
    }
}

/// The version mismatch in librdkafka's error text `detail`, named on the
/// process's Kafka profile. `None` for any other failure, or before the first
/// TLS client.
pub(super) fn version_failure(detail: &str) -> Option<TlsFailure> {
    let floor = FLOOR.get()?;
    TlsFailure::from_openssl(detail, floor.profile, floor.tls12)
}

/// Point `OPENSSL_CONF` at a file scalo writes the Kafka TLS floor into, for
/// a binary whose deployment does not set it. Returns the path it names.
///
/// Call it first thing in `main`, before the async runtime or any other
/// thread starts: it changes the process environment. A variable already set
/// is kept and its path returned.
///
/// # Errors
///
/// More than one thread runs, or the thread count cannot be read (any OS but
/// Linux), or the file cannot be written.
pub fn set_openssl_conf() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("OPENSSL_CONF") {
        return Ok(PathBuf::from(path));
    }
    if !single_threaded() {
        return Err(
            "set_openssl_conf runs only while the process has one thread: call it first in \
             main, or set OPENSSL_CONF in the deployment"
                .to_string(),
        );
    }
    let path = std::env::temp_dir().join(format!("scalo-openssl-{}.cnf", std::process::id()));
    write_whole(&path, &config_text(min_protocol(false)))
        .map_err(|e| format!("{}: {e}", path.display()))?;
    // SAFETY: the process has one thread, checked above, so nothing reads the
    // environment while it changes.
    #[allow(unsafe_code, reason = "a one-thread process, checked above")]
    unsafe {
        std::env::set_var("OPENSSL_CONF", &path);
    }
    Ok(path)
}

/// Whether this process runs one thread.
fn single_threaded() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/task").is_ok_and(|tasks| tasks.count() == 1)
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file sets the floor through OpenSSL's system default section, and
    /// only a missing file or one scalo wrote is taken as scalo's.
    #[test]
    fn the_floor_file_is_scalos_or_left_alone() {
        let text = config_text("TLSv1.3");
        assert!(text.starts_with(FIRST_LINE));
        assert!(text.contains("system_default = scalo_tls"));
        assert!(text.ends_with("MinProtocol = TLSv1.3\n"));
        assert_eq!(min_protocol(true), "TLSv1.2");

        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.cnf");
        let ours = dir.path().join("ours.cnf");
        let theirs = dir.path().join("theirs.cnf");
        std::fs::write(&ours, config_text("TLSv1.2")).unwrap();
        std::fs::write(&theirs, "openssl_conf = site\n").unwrap();
        assert!(is_ours(&missing));
        assert!(is_ours(&ours));
        assert!(!is_ours(&theirs));
        write_whole(&ours, &config_text("TLSv1.3")).unwrap();
        assert!(
            std::fs::read_to_string(&ours)
                .unwrap()
                .ends_with("TLSv1.3\n")
        );
    }

    /// librdkafka's text for a peer with no version the floor allows maps to
    /// the version mismatch, and other failures to nothing.
    #[test]
    fn openssl_version_text_maps_to_the_mismatch() {
        let strong = |detail| TlsFailure::from_openssl(detail, CryptoProfile::Strong, false);
        let refused = strong(
            "ssl://127.0.0.1:9093/bootstrap: SSL handshake failed: error:0A00042E:SSL \
             routines::tlsv1 alert protocol version",
        );
        assert_eq!(
            refused.map(|f| f.to_string()).as_deref(),
            Some("peer supports only TLS 1.2: enable allow_tls12 on profile strong")
        );
        assert!(strong("SSL handshake failed: certificate verify failed").is_none());
    }
}
