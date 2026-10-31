// Project:   scalo
// File:      src/deployment/test_support.rs
// Purpose:   Reusable test helpers for contract-artefact e2e tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Test helpers for contract-artefact end-to-end tests, shared by scalo
//! itself and every downstream consumer service (loaders, receivers,
//! archivers, fetchers, and transform apps).
//!
//! # What this module provides
//!
//! * **Tool probes** -- `OnceLock`-cached wrappers (slow probe runs at most
//!   once per test binary): [`docker_available`], [`helm_available`],
//!   [`kubeconform_available`], [`hadolint_available`], [`kind_available`],
//!   [`kubectl_available`].
//! * **Skip emission** -- [`skip`] writes the canonical
//!   `HYPERCI-SKIP[contract-e2e][<tier>]: <test>: <reason>` line to stderr
//!   AND a side-channel log the CI runner greps + counts for a summary.
//! * **Tier-B gate** -- [`tier_b_enabled`] (true iff `E2E_CLUSTER=1`).
//! * **Kind cluster lifecycle** -- [`KindClusterGuard`] +
//!   [`ensure_kind_cluster`]: per-test uniquely-named cluster, torn down on
//!   Drop so parallel runs never collide on cluster name.
//! * **Memory budget** -- [`assert_peak_memory`]: the test's peak resident
//!   set fails over the contract's memory limit and warns over its request,
//!   so no test carries a budget of its own.
//!
//! # Usage from a consumer's `tests/e2e/`
//!
//! Add `scalo` with the `deployment` feature in dev-dependencies:
//!
//! ```ignore
//! use scalo::deployment::test_support::{
//!     docker_available, ensure_kind_cluster, skip, tier_b_enabled,
//! };
//!
//! #[test]
//! fn my_consumer_dockerfile_actually_starts() {
//!     if !docker_available() {
//!         skip(
//!             "tier-a",
//!             "my_consumer_dockerfile_actually_starts",
//!             "docker daemon not reachable",
//!         );
//!         return;
//!     }
//!     // ... build + run image, etc.
//! }
//! ```
//!
//! # Why no `tempfile` dependency here?
//!
//! scalo `src/` stays std-only; `tempfile` is a dev-dep not pulled into the
//! runtime crate. Callers needing a tempdir pass one in via
//! [`ensure_kind_cluster_in`]. The shorthand [`ensure_kind_cluster`] uses
//! `~/.cache/scalo/contract-test/<cluster>/` and cleans it on Drop.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

#[cfg(all(feature = "transport", feature = "worker-batch"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "transport", feature = "worker-batch"))))]
pub mod conformance;

// ============================================================================
// Tool probes -- thin, cached, no panic.
// ============================================================================

/// Returns true iff `docker info` succeeds. Cached for the test-binary
/// lifetime.
#[must_use]
pub fn docker_available() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        Command::new("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// The daemon endpoint of docker's active context, when one resolves.
///
/// A test that points `DOCKER_CONFIG` at a throwaway directory -- the usual way
/// to stop docker invoking a credential helper the host may not have -- also
/// discards the CONTEXT STORE, which lives in that same directory. Docker then
/// falls back to `unix:///var/run/docker.sock`. That is the right socket on
/// Linux CI and the wrong one anywhere the daemon listens elsewhere: Docker
/// Desktop puts it under the user's home directory, so the build fails to
/// connect on a developer machine while passing in CI.
///
/// Resolve the endpoint from the REAL config up front and hand it back through
/// `DOCKER_HOST`, so the throwaway config strips credentials and nothing else.
/// Returns `None` when no context resolves, leaving docker's own default.
#[must_use]
pub fn docker_host() -> Option<&'static str> {
    static HOST: OnceLock<Option<String>> = OnceLock::new();
    HOST.get_or_init(|| {
        // An explicit DOCKER_HOST already applies to every docker invocation,
        // context store or not, so honour it rather than re-deriving.
        if let Ok(explicit) = std::env::var("DOCKER_HOST")
            && !explicit.is_empty()
        {
            return Some(explicit);
        }
        let out = Command::new("docker")
            .args([
                "context",
                "inspect",
                "--format",
                "{{.Endpoints.docker.Host}}",
            ])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let host = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        (!host.is_empty()).then_some(host)
    })
    .as_deref()
}

/// Returns true iff `helm version` succeeds.
#[must_use]
pub fn helm_available() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        Command::new("helm")
            .arg("version")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// Returns true iff `kubeconform -v` succeeds.
#[must_use]
pub fn kubeconform_available() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        Command::new("kubeconform")
            .arg("-v")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// Returns true iff `hadolint --version` succeeds.
#[must_use]
pub fn hadolint_available() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        Command::new("hadolint")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// Returns true iff `kind version` succeeds.
#[must_use]
pub fn kind_available() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        Command::new("kind")
            .arg("version")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// Returns true iff `kubectl version --client=true` succeeds.
#[must_use]
pub fn kubectl_available() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        Command::new("kubectl")
            .args(["version", "--client=true"])
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// Returns true iff the `E2E_CLUSTER` env var is set to `1` or `true`.
///
/// Cluster-based (Tier B) tests must check this before bringing up kind
/// because cluster spin-up is slow (60-120 s typical) and the harness
/// shouldn't run by default in `cargo test`.
#[must_use]
pub fn tier_b_enabled() -> bool {
    std::env::var("E2E_CLUSTER").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

// ============================================================================
// Skip emission with canonical prefix.
// ============================================================================

/// Write a skip notice with the canonical
/// `HYPERCI-SKIP[contract-e2e][<tier>]:` prefix to stderr AND to a
/// side-channel log file the test runner can grep after the test stage.
///
/// `tier` is `"tier-a"` or `"tier-b"`. Anything else works but the test
/// runner's grep won't match.
///
/// # Side-channel path
///
/// Tries `$CARGO_TARGET_TMPDIR/contract-e2e-skips.log` first (cargo's
/// official integration-test scratch dir). Falls back to
/// `$HOME/.cache/scalo/contract-e2e-skips.log` if the env var is
/// unset (e.g. when the test binary is invoked outside `cargo test`).
/// File-write failures are silently ignored -- the test must not fail
/// because the skip-log infrastructure had a hiccup.
pub fn skip(tier: &str, test_name: &str, reason: &str) {
    let line = format!("HYPERCI-SKIP[contract-e2e][{tier}]: {test_name}: {reason}");
    eprintln!("{line}");
    if let Some(path) = skip_log_path() {
        let _ = append_line(&path, &line);
    }
}

fn skip_log_path() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("CARGO_TARGET_TMPDIR") {
        return Some(Path::new(&dir).join("contract-e2e-skips.log"));
    }
    // Fallback: ~/.cache/scalo/. Never /tmp per AGENT-RULES Rule 4.
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()?;
    Some(Path::new(&home).join(".cache/scalo/contract-e2e-skips.log"))
}

fn append_line(path: &Path, line: &str) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{line}")
}

// ============================================================================
// Docker config helper -- empty creds JSON to avoid credential-helper lookup.
// ============================================================================

/// Empty-creds JSON suitable for `<docker_config_dir>/config.json`.
///
/// Set `DOCKER_CONFIG=<dir>` and write this to `<dir>/config.json` to
/// prevent docker from invoking credential helpers like
/// `docker-credential-secretservice` that may not be on the host. Public
/// registry pulls (Docker Hub, ghcr.io public images) don't need auth, so
/// the empty config is sufficient for build + run tests.
#[must_use]
pub fn docker_empty_creds_json() -> &'static str {
    r#"{"auths": {}}"#
}

// ============================================================================
// Kind cluster lifecycle -- per-test cluster, dropped on test exit.
// ============================================================================

/// Owns a kind cluster + its kubeconfig path. Dropping the guard deletes
/// the cluster and (if owned) removes the kubeconfig parent directory.
#[derive(Debug)]
pub struct KindClusterGuard {
    /// Kind cluster name (e.g. `hctest-deadbeef`).
    pub name: String,
    /// Path to the kubeconfig file. Tests pass this via `KUBECONFIG` env
    /// var so they don't disturb the user's default context.
    pub kubeconfig: PathBuf,
    /// Directory that holds `kubeconfig`. When the guard owns it (`true`),
    /// the directory is removed on Drop. When `false`, the caller manages
    /// the lifetime (e.g. via their own `tempfile::TempDir`).
    cleanup_dir: bool,
}

impl Drop for KindClusterGuard {
    fn drop(&mut self) {
        // Best-effort: delete the cluster.
        let _ = Command::new("kind")
            .args(["delete", "cluster", "--name", &self.name])
            .output();
        // Best-effort: remove the kubeconfig directory if we own it.
        if self.cleanup_dir
            && let Some(parent) = self.kubeconfig.parent()
        {
            let _ = std::fs::remove_dir_all(parent);
        }
    }
}

/// Bring up a uniquely-named kind cluster for the caller, storing
/// `kubeconfig` under `kubeconfig_dir` (which the caller manages, e.g.
/// via their own `tempfile::TempDir`).
///
/// Returns `None` if any prerequisite is missing (kind/kubectl/docker)
/// or cluster creation fails -- the underlying reason is emitted via
/// [`skip`] so the test runner sees it.
///
/// The cluster name is derived from `test_name` via a small hash so each
/// test in a parallel suite gets its own cluster.
#[must_use]
pub fn ensure_kind_cluster_in(test_name: &str, kubeconfig_dir: &Path) -> Option<KindClusterGuard> {
    let cluster = prepare_cluster_or_skip(test_name)?;
    let kubeconfig = kubeconfig_dir.join("kubeconfig");
    let kc = Command::new("kind")
        .args(["get", "kubeconfig", "--name", &cluster])
        .output()
        .ok()?;
    std::fs::write(&kubeconfig, &kc.stdout).ok()?;
    Some(KindClusterGuard {
        name: cluster,
        kubeconfig,
        cleanup_dir: false,
    })
}

/// Convenience: like [`ensure_kind_cluster_in`] but uses an owned
/// directory under `~/.cache/scalo/contract-test/<cluster>/` so the
/// caller doesn't need `tempfile`. The directory is removed on Drop.
#[must_use]
pub fn ensure_kind_cluster(test_name: &str) -> Option<KindClusterGuard> {
    let cluster = prepare_cluster_or_skip(test_name)?;
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()?;
    let dir = Path::new(&home)
        .join(".cache/scalo/contract-test")
        .join(&cluster);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        skip(
            "tier-b",
            test_name,
            &format!(
                "could not create cluster scratch dir {}: {e}",
                dir.display()
            ),
        );
        // Best-effort cleanup of the cluster we just created.
        let _ = Command::new("kind")
            .args(["delete", "cluster", "--name", &cluster])
            .output();
        return None;
    }
    let kubeconfig = dir.join("kubeconfig");
    let kc = Command::new("kind")
        .args(["get", "kubeconfig", "--name", &cluster])
        .output()
        .ok()?;
    std::fs::write(&kubeconfig, &kc.stdout).ok()?;
    Some(KindClusterGuard {
        name: cluster,
        kubeconfig,
        cleanup_dir: true,
    })
}

fn prepare_cluster_or_skip(test_name: &str) -> Option<String> {
    if !kind_available() {
        skip(
            "tier-b",
            test_name,
            "kind CLI not on PATH (install: https://kind.sigs.k8s.io/)",
        );
        return None;
    }
    if !kubectl_available() {
        skip("tier-b", test_name, "kubectl not on PATH");
        return None;
    }
    if !docker_available() {
        skip(
            "tier-b",
            test_name,
            "docker daemon not reachable (kind requires docker)",
        );
        return None;
    }

    let suffix = test_name.bytes().fold(0u32, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(u32::from(b))
    });
    let cluster = format!("hctest-{suffix:08x}");

    let create = Command::new("kind")
        .args(["create", "cluster", "--name", &cluster, "--wait", "120s"])
        .output()
        .ok()?;
    if !create.status.success() {
        skip(
            "tier-b",
            test_name,
            &format!(
                "kind create cluster failed: {}",
                String::from_utf8_lossy(&create.stderr).trim()
            ),
        );
        return None;
    }
    Some(cluster)
}

// ============================================================================
// Wait helpers -- usable from cluster tests for "wait until X happens".
// ============================================================================

/// Poll `f` every `interval` until it returns `true` or `deadline` elapses.
/// Returns `true` if `f` returned `true` before deadline, `false` otherwise.
///
/// Used by cluster tests to wait for pods/deployments to become Ready.
pub fn wait_until(deadline: Duration, interval: Duration, mut f: impl FnMut() -> bool) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(interval);
    }
    f()
}

// ============================================================================
// Memory budget -- a test held to the contract's memory request and limit.
// ============================================================================

/// The peak resident set of this process (`VmHWM`), in bytes. `None` where
/// `/proc/self/status` does not report it, which is anything but Linux.
#[must_use]
pub fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    kib.checked_mul(1024)
}

/// The bytes in a Kubernetes memory quantity: `512Mi`, `1G`, `1.5Gi`,
/// `129e6` or a plain `134217728`. A fraction of a byte rounds up, as
/// Kubernetes rounds it. `None` for anything else, a negative included.
#[must_use]
pub fn memory_quantity_bytes(quantity: &str) -> Option<u64> {
    let quantity = quantity.trim();
    let split = quantity
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(quantity.len());
    let (number, suffix) = quantity.split_at(split);
    let number: f64 = number.parse().ok()?;
    let binary = |power: i32| 1024_f64.powi(power);
    let scale = match suffix {
        "" => 1.0,
        "Ki" => binary(1),
        "Mi" => binary(2),
        "Gi" => binary(3),
        "Ti" => binary(4),
        "Pi" => binary(5),
        "Ei" => binary(6),
        "m" => 1e-3,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        exponent if exponent.starts_with(['e', 'E']) => {
            10_f64.powi(exponent[1..].parse::<i32>().ok()?)
        }
        _ => return None,
    };
    let bytes = (number * scale).ceil();
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "range-checked against u64 just before the cast"
    )]
    let in_range = bytes.is_finite() && bytes >= 0.0 && bytes <= u64::MAX as f64;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "range-checked on the line above"
    )]
    in_range.then_some(bytes as u64)
}

/// How a peak resident set stands against a contract's memory request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryVerdict {
    /// At or under the request, or no request is declared.
    Within,
    /// Over the request and under the limit: the pod runs, but the
    /// scheduler reserves less than it uses.
    OverRequest {
        /// The peak resident set, in bytes.
        peak: u64,
        /// The contract's memory request, in bytes.
        request: u64,
    },
}

/// Judge `peak` bytes against `resources`. `Err` is why the test fails: a
/// peak over `limits.memory`, no memory limit declared, or a quantity that
/// does not parse.
///
/// # Errors
///
/// As above, with the figures in the message.
pub fn judge_peak_memory(
    peak: u64,
    resources: &super::ResourcesContract,
) -> Result<MemoryVerdict, String> {
    let quantity = |name: &str, value: &str| {
        memory_quantity_bytes(value)
            .ok_or_else(|| format!("the contract's memory {name} {value:?} is not a quantity"))
    };
    if resources.limits.memory.is_empty() {
        return Err("the contract declares no memory limit to hold the test to".into());
    }
    let limit = quantity("limit", &resources.limits.memory)?;
    if peak > limit {
        return Err(format!(
            "peak resident set {} over the contract's memory limit {} ({}): the pod would be OOM-killed",
            mib(peak),
            mib(limit),
            resources.limits.memory
        ));
    }
    if resources.requests.memory.is_empty() {
        return Ok(MemoryVerdict::Within);
    }
    let request = quantity("request", &resources.requests.memory)?;
    Ok(if peak > request {
        MemoryVerdict::OverRequest { peak, request }
    } else {
        MemoryVerdict::Within
    })
}

/// Hold this process's peak resident set to the contract: panic over
/// `limits.memory`, and warn over `requests.memory`. Returns the peak, or
/// `None` after a [`skip`] line where the peak is not reported.
///
/// Call it at the end of the test. The peak is the process's, so the test
/// needs a process of its own: nextest gives every test one, and under
/// `cargo test` the test goes in an integration test file by itself.
///
/// The warning goes to stderr, to stdout as a GitHub `::warning::`
/// annotation, and to `$GITHUB_STEP_SUMMARY` when set. nextest hides a
/// passing test's output, so the step summary is where CI shows it.
///
/// # Panics
///
/// Over the limit, with no memory limit declared, or on a quantity that
/// does not parse.
#[allow(
    clippy::must_use_candidate,
    reason = "a test asserts and moves on; the peak is for one that also reports it"
)]
pub fn assert_peak_memory(test_name: &str, resources: &super::ResourcesContract) -> Option<u64> {
    let Some(peak) = peak_rss_bytes() else {
        skip(
            "memory",
            test_name,
            "this platform reports no peak resident set",
        );
        return None;
    };
    assert_peak_memory_of(test_name, peak, resources);
    Some(peak)
}

/// Hold a peak of `peak_bytes` to `resources`, for a test that measured the
/// peak itself: panic over `limits.memory`, and warn over `requests.memory`.
///
/// Use it where the peak belongs to a child process, which
/// [`assert_peak_memory`] cannot see. The warning goes to stderr, to stdout
/// as a GitHub `::warning::` annotation, and to `$GITHUB_STEP_SUMMARY` when
/// set, exactly as [`assert_peak_memory`] reports it.
///
/// # Panics
///
/// Over the limit, with no memory limit declared, or on a quantity that
/// does not parse.
pub fn assert_peak_memory_of(
    test_name: &str,
    peak_bytes: u64,
    resources: &super::ResourcesContract,
) {
    let summary = std::env::var("GITHUB_STEP_SUMMARY").ok();
    if let Err(reason) = report_peak(
        test_name,
        peak_bytes,
        resources,
        summary.as_deref().map(Path::new),
    ) {
        panic!("{test_name}: {reason}");
    }
}

/// Judge `peak_bytes` and report an over-request warning to the given sinks.
/// `Err` is the failure reason, and nothing is reported for it.
fn report_peak(
    test_name: &str,
    peak_bytes: u64,
    resources: &super::ResourcesContract,
    summary: Option<&Path>,
) -> Result<MemoryVerdict, String> {
    let verdict = judge_peak_memory(peak_bytes, resources)?;
    if let MemoryVerdict::OverRequest { peak, request } = verdict {
        let message = format!(
            "{test_name}: peak resident set {} over the contract's memory request {} ({})",
            mib(peak),
            mib(request),
            resources.requests.memory
        );
        eprintln!("warning: {message}");
        println!("::warning title=memory over request::{message}");
        if let Some(summary) = summary {
            let _ = append_line(summary, &format!("- WARNING: {message}"));
        }
    }
    Ok(verdict)
}

/// `bytes` in MiB, to one decimal place.
fn mib(bytes: u64) -> String {
    #[allow(
        clippy::cast_precision_loss,
        reason = "a figure in a message; a rounding past 2^52 bytes does not matter"
    )]
    let mib = bytes as f64 / 1_048_576.0;
    format!("{mib:.1} MiB")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::{ResourceList, ResourcesContract};

    #[test]
    fn memory_quantities_read_as_kubernetes_reads_them() {
        let cases = [
            ("134217728", Some(134_217_728)),
            ("128Mi", Some(134_217_728)),
            ("1.5Gi", Some(1_610_612_736)),
            ("512Ki", Some(524_288)),
            ("1G", Some(1_000_000_000)),
            ("129e6", Some(129_000_000)),
            (" 64Mi ", Some(67_108_864)),
            ("1500m", Some(2)),
            ("", None),
            ("Mi", None),
            ("-1Mi", None),
            ("1.2.3Mi", None),
            ("12MB", None),
            ("1e999", None),
        ];
        for (quantity, want) in cases {
            assert_eq!(memory_quantity_bytes(quantity), want, "{quantity:?}");
        }
    }

    fn resources(request: &str, limit: &str) -> ResourcesContract {
        ResourcesContract {
            requests: ResourceList {
                memory: request.into(),
                ..ResourceList::default()
            },
            limits: ResourceList {
                memory: limit.into(),
                ..ResourceList::default()
            },
        }
    }

    #[test]
    fn a_peak_fails_over_the_limit_and_warns_over_the_request() {
        const MIB: u64 = 1_048_576;
        let over_request = MemoryVerdict::OverRequest {
            peak: 200 * MIB,
            request: 128 * MIB,
        };
        // (case, peak, request, limit, verdict, or a word the failure carries)
        let cases: [(&str, u64, &str, &str, Result<MemoryVerdict, &str>); 7] = [
            (
                "under the request",
                100 * MIB,
                "128Mi",
                "512Mi",
                Ok(MemoryVerdict::Within),
            ),
            (
                "at the request",
                128 * MIB,
                "128Mi",
                "512Mi",
                Ok(MemoryVerdict::Within),
            ),
            (
                "over the request",
                200 * MIB,
                "128Mi",
                "512Mi",
                Ok(over_request),
            ),
            (
                "no request",
                200 * MIB,
                "",
                "512Mi",
                Ok(MemoryVerdict::Within),
            ),
            (
                "over the limit",
                513 * MIB,
                "128Mi",
                "512Mi",
                Err("OOM-killed"),
            ),
            ("no limit", 1, "128Mi", "", Err("no memory limit")),
            ("a bad quantity", 1, "lots", "512Mi", Err("not a quantity")),
        ];
        for (name, peak, request, limit, want) in cases {
            let got = report_peak(name, peak, &resources(request, limit), None);
            match (got, want) {
                (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
                (Err(reason), Err(word)) => assert!(reason.contains(word), "{name}: {reason}"),
                (got, _) => panic!("{name}: {got:?}"),
            }
        }
    }

    #[test]
    fn an_over_request_peak_appends_a_step_summary_line() {
        const MIB: u64 = 1_048_576;
        let dir = tempfile::tempdir().expect("a temp dir");
        let summary = dir.path().join("summary.md");
        let got = report_peak(
            "summarised",
            200 * MIB,
            &resources("128Mi", "512Mi"),
            Some(&summary),
        );
        assert!(
            matches!(got, Ok(MemoryVerdict::OverRequest { .. })),
            "{got:?}"
        );
        let text = std::fs::read_to_string(&summary).expect("the summary was written");
        assert!(
            text.starts_with("- WARNING: summarised: peak resident set 200.0 MiB"),
            "{text}"
        );
    }

    #[test]
    #[should_panic(expected = "over the contract's memory limit")]
    fn assert_peak_memory_of_fails_over_the_limit() {
        const MIB: u64 = 1_048_576;
        assert_peak_memory_of("oom", 513 * MIB, &resources("128Mi", "512Mi"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_reports_a_peak_and_a_roomy_contract_passes_it() {
        let before = peak_rss_bytes().expect("VmHWM on Linux");
        assert!(before > 0);
        // A high-water mark only rises, so the helper reads at least as much.
        let held = assert_peak_memory("roomy", &resources("1Ei", "1Ei")).expect("a peak");
        assert!(held >= before, "{held} < {before}");
    }
}
