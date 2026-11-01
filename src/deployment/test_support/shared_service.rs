// Project:   scalo
// File:      src/deployment/test_support/shared_service.rs
// Purpose:   One container per backing service per test run, owned by one process
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One container per backing service per test run, shared by every test process.
//!
//! nextest runs each test in a process of its own, so whichever process asks
//! first creates the container under a run-scoped name and the rest join it by
//! that name. The creator also starts a detached watcher that removes the
//! container once the run's nextest process exits, whether it passed, failed
//! or was interrupted. [`start_alone`] starts a container one test holds by
//! itself, removed when the holder drops or its process exits. Nothing here
//! names a repo, image or port: the caller passes a [`Suite`] and a
//! [`ServiceSpec`].
//!
//! Ports publish on the daemon host's loopback, never a wildcard. A daemon
//! reached over ssh gets an `ssh -L` tunnel per port, on the same port number
//! at both ends. A remote `tcp://` daemon needs `HYPERI_TEST_DOCKER_BIND`.
//!
//! Every container carries the `io.hyperi.test.*` labels of [`labels`]. A
//! create first sweeps the suite's containers whose owner has gone
//! ([`sweep_removes`]): on the owner's host and PID namespace once the owner
//! process has exited, anywhere else once its [`LEASE`] has lapsed.
//!
//! The pet rung lives here too: [`lookup`] reads `HYPERI_TEST_*` settings from
//! the process environment, then the project `.env`, then the file named by
//! `HYPERI_TEST_ENV_FILE`, and [`pets_allowed`] says when a run may use them.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use scalo::deployment::test_support::shared_service::{
//!     Ready, ServiceSpec, Suite, acquire,
//! };
//!
//! const SUITE: Suite = Suite {
//!     repo: "my-app",
//!     manifest_dir: env!("CARGO_MANIFEST_DIR"),
//!     tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
//! };
//!
//! let spec = ServiceSpec {
//!     ports: vec![6379],
//!     ready: Ready::Log("Ready to accept connections"),
//!     ..ServiceSpec::new("redis", "redis:8.2.2@sha256:<digest>", Duration::from_secs(60))
//! };
//! // Blocking: run it off the async runtime, e.g. in `spawn_blocking`.
//! let redis = acquire(&SUITE, &spec);
//! let address = redis.addr(6379);
//! ```

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Interval between readiness probes and state checks.
const POLL: Duration = Duration::from_millis(250);

/// How long a container stays another host's after its owner labels it: past
/// the longest full run, so a sweep from elsewhere never takes a live one.
pub const LEASE: Duration = Duration::from_secs(4 * 3600);

/// The repo whose suite starts the containers, for names, labels, the run log
/// and the `HYPERI_TEST_*` lookup.
#[derive(Debug, Clone, Copy)]
pub struct Suite {
    /// Repo name, e.g. the crate name.
    pub repo: &'static str,
    /// Directory holding the project `.env`: `env!("CARGO_MANIFEST_DIR")`.
    pub manifest_dir: &'static str,
    /// Where the run log goes: `option_env!("CARGO_TARGET_TMPDIR")`, else the
    /// system temp directory.
    pub tmp_dir: Option<&'static str>,
}

impl Suite {
    /// Value of the `io.hyperi.test.suite` label: `<repo>-integration`.
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}-integration", self.repo)
    }

    /// Container name prefix: `HYPERI_TEST_CONTAINER_PREFIX`, else `<repo>-test`.
    #[must_use]
    pub fn prefix(&self) -> String {
        lookup(self, "HYPERI_TEST_CONTAINER_PREFIX")
            .unwrap_or_else(|| format!("{}-test", self.repo))
    }

    /// The run log every creator and watcher appends to,
    /// `shared-services-<run>.log` under [`tmp_dir`](Self::tmp_dir).
    #[must_use]
    pub fn log_path(&self) -> PathBuf {
        let dir = self.tmp_dir.map_or_else(std::env::temp_dir, PathBuf::from);
        dir.join(format!("shared-services-{}.log", run_id()))
    }

    /// Write `line` to stderr and to the [run log](Self::log_path).
    pub fn record(&self, line: &str) {
        eprintln!("shared-service: {line}");
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())
        {
            let _ = writeln!(file, "{line}");
        }
    }
}

/// How a started container shows it is ready to serve.
#[derive(Debug, Clone)]
pub enum Ready {
    /// `GET path` on the container port answers 200 with this body, trimmed.
    Http {
        /// Container port the request goes to.
        port: u16,
        /// Request path, e.g. `/ping`.
        path: &'static str,
        /// Expected body, compared trimmed.
        body: &'static str,
    },
    /// The container log carries this line.
    Log(&'static str),
}

/// One backing service the suite starts.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    /// Service name, used in the container name and the service label.
    pub service: &'static str,
    /// Image reference, pinned by tag and digest.
    pub image: String,
    /// Container ports to publish, each to a free host port.
    pub ports: Vec<u16>,
    /// Container ports to publish on a given host port instead, as (container, host).
    pub pinned_ports: Vec<(u16, u16)>,
    /// Environment for the container.
    pub env: Vec<(String, String)>,
    /// Entrypoint override, when the image's own cannot be used.
    pub entrypoint: Option<String>,
    /// Arguments after the image.
    pub args: Vec<String>,
    /// Files copied in before start, built from the endpoint host name. A path
    /// ending in `/` creates that directory, ahead of the files that go in it.
    pub files: fn(&str) -> Vec<(String, Vec<u8>)>,
    /// Runs once the ports are known and before the readiness wait.
    pub after_start: fn(&SharedContainer),
    /// The readiness signal.
    pub ready: Ready,
    /// Bound on create, start and readiness together.
    pub startup_timeout: Duration,
}

impl ServiceSpec {
    /// A spec for `service` from `image` with nothing published, no files and
    /// no readiness signal beyond the ports answering: set `ports` and `ready`
    /// with struct update syntax.
    #[must_use]
    pub fn new(service: &'static str, image: impl Into<String>, startup_timeout: Duration) -> Self {
        Self {
            service,
            image: image.into(),
            ports: Vec::new(),
            pinned_ports: Vec::new(),
            env: Vec::new(),
            entrypoint: None,
            args: Vec::new(),
            files: |_| Vec::new(),
            after_start: |_| {},
            ready: Ready::Log(""),
            startup_timeout,
        }
    }

    fn all_ports(&self) -> Vec<u16> {
        let mut ports = self.ports.clone();
        ports.extend(self.pinned_ports.iter().map(|&(c, _)| c));
        ports
    }
}

/// A running container and where its ports are reachable.
#[derive(Debug, Clone)]
pub struct SharedContainer {
    /// Container name.
    pub name: String,
    /// Host the ports are reachable on from this machine.
    pub host: String,
    ports: Vec<(u16, u16)>,
}

impl SharedContainer {
    /// Host port that `container_port` is published on.
    ///
    /// # Panics
    ///
    /// When the spec did not publish `container_port`.
    #[must_use]
    pub fn port(&self, container_port: u16) -> u16 {
        self.ports
            .iter()
            .find_map(|&(c, h)| (c == container_port).then_some(h))
            .unwrap_or_else(|| panic!("{} does not publish port {container_port}", self.name))
    }

    /// `host:port` for `container_port`.
    ///
    /// # Panics
    ///
    /// As [`port`](Self::port).
    #[must_use]
    pub fn addr(&self, container_port: u16) -> String {
        format!("{}:{}", self.host, self.port(container_port))
    }

    /// Bytes of a file inside the container.
    ///
    /// # Panics
    ///
    /// When the file cannot be read.
    #[must_use]
    pub fn read_file(&self, path: &str) -> Vec<u8> {
        let out = docker(&["exec", &self.name, "cat", path]);
        assert!(
            out.status.success(),
            "read {path} from {}: {}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    /// Run a command inside the container and return its stdout.
    ///
    /// # Panics
    ///
    /// When the command exits non-zero.
    #[must_use]
    pub fn exec(&self, argv: &[&str]) -> String {
        let mut command = vec!["exec", self.name.as_str()];
        command.extend_from_slice(argv);
        let out = docker(&command);
        assert!(
            out.status.success(),
            "{argv:?} in {}: {}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

/// A container one test holds alone, removed with its tunnel when dropped.
#[derive(Debug)]
pub struct OwnedContainer {
    /// Where it is reachable.
    pub container: SharedContainer,
    tunnel: Option<u32>,
}

impl Drop for OwnedContainer {
    fn drop(&mut self) {
        let _ = docker(&["rm", "--force", "--volumes", &self.container.name]);
        if let Some(pid) = self.tunnel {
            let _ = Command::new("kill").arg(pid.to_string()).status();
        }
    }
}

/// Where the Docker daemon is, which decides how its ports are reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Daemon {
    /// A local socket, or `tcp://` on loopback.
    Local,
    /// Over ssh: ports bind to the daemon host's loopback and arrive here
    /// through a tunnel.
    Ssh {
        /// `user@host` or `host`.
        target: String,
        /// The ssh port, when the endpoint names one.
        port: Option<u16>,
    },
    /// `tcp://` to another host.
    Tcp {
        /// The daemon's host.
        host: String,
    },
}

impl Daemon {
    /// The daemon `DOCKER_HOST` names, else the current docker context's.
    #[must_use]
    pub fn current() -> Self {
        let endpoint = std::env::var("DOCKER_HOST")
            .ok()
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| {
                let out = docker(&[
                    "context",
                    "inspect",
                    "--format",
                    "{{.Endpoints.docker.Host}}",
                ]);
                String::from_utf8_lossy(&out.stdout).trim().to_string()
            });
        Self::parse(&endpoint)
    }

    /// The daemon a Docker endpoint URL names.
    #[must_use]
    pub fn parse(endpoint: &str) -> Self {
        let Some((scheme, rest)) = endpoint.split_once("://") else {
            return Self::Local;
        };
        let authority = rest.split('/').next().unwrap_or(rest);
        let (user, host_port) = match authority.rsplit_once('@') {
            Some((user, host_port)) => (Some(user), host_port),
            None => (None, authority),
        };
        let (host, port) = match host_port.rsplit_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().ok()),
            None => (host_port, None),
        };
        match scheme {
            "ssh" => Self::Ssh {
                target: user.map_or_else(|| host.to_string(), |u| format!("{u}@{host}")),
                port,
            },
            "tcp" | "http" | "https" if !is_loopback(host) => Self::Tcp {
                host: host.to_string(),
            },
            _ => Self::Local,
        }
    }

    /// The address ports bind to on the daemon host, and the host clients on
    /// this machine connect to.
    ///
    /// # Panics
    ///
    /// For a remote `tcp://` daemon without `HYPERI_TEST_DOCKER_BIND`, or with
    /// a wildcard there: the services carry test credentials.
    #[must_use]
    pub fn binding(&self, suite: &Suite) -> (String, String) {
        match self {
            Self::Local | Self::Ssh { .. } => ("127.0.0.1".into(), "127.0.0.1".into()),
            Self::Tcp { host } => {
                let bind = lookup(suite, "HYPERI_TEST_DOCKER_BIND").unwrap_or_else(|| {
                    panic!(
                        "Docker at tcp://{host} would publish the test services to its network. \
                         Set HYPERI_TEST_DOCKER_BIND to a private address on that host, or \
                         reach the daemon over ssh."
                    )
                });
                assert!(
                    !matches!(bind.as_str(), "0.0.0.0" | "::" | "[::]" | "*"),
                    "HYPERI_TEST_DOCKER_BIND must name one address, not {bind}"
                );
                (bind.clone(), bind)
            }
        }
    }
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
}

/// The run's container for `spec`, created if no process has yet.
///
/// Named `<prefix>-integration-<service>-r<run>`. Blocking: call it from a
/// plain thread or `spawn_blocking`, never directly on an async runtime thread.
///
/// # Panics
///
/// When Docker is unreachable, the container cannot be created, or it is not
/// ready within `spec.startup_timeout`.
#[must_use]
pub fn acquire(suite: &Suite, spec: &ServiceSpec) -> SharedContainer {
    let name = format!(
        "{}-integration-{}-r{}",
        suite.prefix(),
        spec.service,
        run_id()
    );
    let deadline = Instant::now() + spec.startup_timeout;
    let daemon = Daemon::current();
    let (_, host) = daemon.binding(suite);
    loop {
        match state(&name) {
            State::Running => {
                let container = published(&name, &host, &spec.all_ports(), deadline);
                wait_ready(&container, &spec.ready, deadline);
                wait_reachable(&container, deadline);
                return container;
            }
            State::Absent => {
                if create(suite, spec, &name, &daemon, run_owner_pid()) {
                    let (container, _) = start_owned(
                        suite,
                        spec,
                        &name,
                        &daemon,
                        &host,
                        run_owner_pid(),
                        deadline,
                    );
                    suite.record(&format!(
                        "started {} container {name} for run {} (owner pid {}): 1 {} container this run",
                        spec.service,
                        run_id(),
                        run_owner_pid(),
                        spec.service
                    ));
                    return container;
                }
            }
            // A peer created it and is still copying files in or starting it.
            State::Other(status) => assert!(
                Instant::now() < deadline,
                "shared {} container {name} stuck in state {status}",
                spec.service
            ),
        }
        std::thread::sleep(POLL);
    }
}

/// A container named `name` that this test process holds alone. Name it with
/// [`container_name`] so two tests never ask for the same one.
///
/// # Panics
///
/// As [`acquire`], and when `name` is already taken.
#[must_use]
pub fn start_alone(suite: &Suite, spec: &ServiceSpec, name: &str) -> OwnedContainer {
    let deadline = Instant::now() + spec.startup_timeout;
    let daemon = Daemon::current();
    let (_, host) = daemon.binding(suite);
    let owner = std::process::id();
    assert!(
        create(suite, spec, name, &daemon, owner),
        "{name} already exists: a second container for one test"
    );
    let (container, tunnel) = start_owned(suite, spec, name, &daemon, &host, owner, deadline);
    OwnedContainer { container, tunnel }
}

/// Container name for a container one test starts for itself:
/// `<prefix>-integration-<test>-<service>-p<pid>`, or without `<test>` for one
/// started once for a whole test binary.
///
/// Lowercase, with every character Docker refuses collapsed to `-`. The pid
/// keeps two runs of one test on one daemon from asking for the same name.
#[must_use]
pub fn container_name(suite: &Suite, test: Option<&str>, service: &str) -> String {
    let pid = std::process::id();
    let prefix = suite.prefix();
    test.map_or_else(
        || format!("{prefix}-integration-{}-p{pid}", slug(service)),
        |t| format!("{prefix}-integration-{}-{}-p{pid}", slug(t), slug(service)),
    )
}

fn slug(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

/// The name of the test this expands inside, for naming its containers.
///
/// Rust cannot read the current test's name, and a literal per call site
/// drifts when a test is renamed. `type_name` of a function declared here
/// reports the path it is nested in, the calling test, so this has to be a
/// macro: expanded in a helper it would report the helper. An `async fn`'s
/// generated `::{{closure}}` segment is trimmed.
///
/// ```
/// fn my_test() -> &'static str {
///     scalo::test_name!()
/// }
/// assert_eq!(my_test(), "my_test");
/// ```
#[macro_export]
macro_rules! test_name {
    () => {{
        fn probe() {}
        fn path_of<T>(_: T) -> &'static str {
            ::std::any::type_name::<T>()
        }
        let mut name = path_of(probe);
        name = name.strip_suffix("::probe").unwrap_or(name);
        name = name.strip_suffix("::{{closure}}").unwrap_or(name);
        name.rsplit("::").next().unwrap_or(name)
    }};
}

/// Copy the files in, start the container, open its tunnel and hand both to a
/// watcher that ends them when `owner` exits.
fn start_owned(
    suite: &Suite,
    spec: &ServiceSpec,
    name: &str,
    daemon: &Daemon,
    host: &str,
    owner: u32,
    deadline: Instant,
) -> (SharedContainer, Option<u32>) {
    spawn_watcher(suite, owner, Some(name), None);
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("staging directory: {e}"));
    for (index, (target, bytes)) in (spec.files)(host).into_iter().enumerate() {
        let staged = dir.path().join(index.to_string());
        if target.ends_with('/') {
            use std::os::unix::fs::PermissionsExt;
            std::fs::create_dir(&staged).unwrap_or_else(|e| panic!("stage {target}: {e}"));
            std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
                .unwrap_or_else(|e| panic!("make {target} readable: {e}"));
        } else {
            std::fs::write(&staged, bytes).unwrap_or_else(|e| panic!("stage {target}: {e}"));
        }
        let target = target.trim_end_matches('/');
        let copied = docker(&["cp", &staged.to_string_lossy(), &format!("{name}:{target}")]);
        assert!(
            copied.status.success(),
            "copy {target} into {name}: {}",
            String::from_utf8_lossy(&copied.stderr)
        );
    }
    let started = docker(&["start", name]);
    assert!(
        started.status.success(),
        "start {name}: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    let container = published(name, host, &spec.all_ports(), deadline);
    let tunnel = match daemon {
        Daemon::Ssh { target, port } => {
            let pid = open_tunnel(target, *port, &container);
            spawn_watcher(suite, owner, None, Some(pid));
            Some(pid)
        }
        Daemon::Local | Daemon::Tcp { .. } => None,
    };
    (spec.after_start)(&container);
    wait_ready(&container, &spec.ready, deadline);
    wait_reachable(&container, deadline);
    (container, tunnel)
}

/// Forward each published port from this machine to the daemon host's
/// loopback, on the same number at both ends, so an address a service
/// advertises resolves here too. Returns the ssh process id.
fn open_tunnel(target: &str, port: Option<u16>, container: &SharedContainer) -> u32 {
    let mut args: Vec<String> = vec![
        "-N".into(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ExitOnForwardFailure=yes".into(),
    ];
    if let Some(port) = port {
        args.extend(["-p".into(), port.to_string()]);
    }
    for &(_, host_port) in &container.ports {
        args.extend([
            "-L".into(),
            format!("127.0.0.1:{host_port}:127.0.0.1:{host_port}"),
        ]);
    }
    args.push(target.to_string());
    let out = Command::new("sh")
        .args([
            "-c",
            r#"ssh "$@" </dev/null >/dev/null 2>&1 & echo $!"#,
            "shared-service-tunnel",
        ])
        .args(&args)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .unwrap_or_else(|e| panic!("start the ssh tunnel to {target}: {e}"));
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("no pid for the ssh tunnel to {target}"))
}

/// Create the container. False when another process created it first.
fn create(suite: &Suite, spec: &ServiceSpec, name: &str, daemon: &Daemon, owner: u32) -> bool {
    reap_dead_owners(suite);
    let (bind, _) = daemon.binding(suite);
    let mut args: Vec<String> = vec!["create".into(), "--name".into(), name.into()];
    for (key, value) in labels(suite, spec.service, owner) {
        args.extend(["--label".into(), format!("{key}={value}")]);
    }
    for port in &spec.ports {
        args.extend(["--publish".into(), format!("{bind}::{port}")]);
    }
    for (port, host_port) in &spec.pinned_ports {
        args.extend(["--publish".into(), format!("{bind}:{host_port}:{port}")]);
    }
    for (key, value) in &spec.env {
        args.extend(["--env".into(), format!("{key}={value}")]);
    }
    if let Some(entrypoint) = &spec.entrypoint {
        args.extend(["--entrypoint".into(), entrypoint.clone()]);
    }
    args.push(spec.image.clone());
    args.extend(spec.args.iter().cloned());
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = docker(&refs);
    if out.status.success() {
        return true;
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("already in use") || stderr.contains("Conflict"),
        "create {name}: {stderr}"
    );
    false
}

/// Who owns a container: the machine, the PID namespace, and the process with
/// its start time, so a reused pid does not pass for the owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    /// Hostname and machine id.
    pub host: String,
    /// The PID namespace, `/proc/self/ns/pid`.
    pub pid_ns: String,
    /// Process id.
    pub pid: u32,
    /// The process's start time, `/proc/<pid>/stat` field 22.
    pub start: String,
}

impl Owner {
    /// The identity of local process `pid`.
    #[must_use]
    pub fn of(pid: u32) -> Self {
        Self {
            host: host_id(),
            pid_ns: pid_namespace(),
            pid,
            start: process_start(pid).unwrap_or_default(),
        }
    }
}

/// Hostname and machine id, so two machines with one hostname still differ.
#[must_use]
pub fn host_id() -> String {
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| {
            Command::new("hostname")
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        })
        .unwrap_or_default();
    let machine = std::fs::read_to_string("/etc/machine-id").unwrap_or_default();
    format!("{}/{}", hostname.trim(), machine.trim())
}

fn pid_namespace() -> String {
    std::fs::read_link("/proc/self/ns/pid")
        .map_or_else(|_| "none".into(), |p| p.to_string_lossy().into_owned())
}

/// Start time of `pid`, or `None` when no such process exists.
#[must_use]
pub fn process_start(pid: u32) -> Option<String> {
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        // Field 22 counts from the pid, and the command name before it is
        // parenthesised and may hold spaces.
        let after_comm = stat.rsplit_once(')')?.1;
        return after_comm.split_whitespace().nth(19).map(str::to_string);
    }
    if Path::new("/proc/self").exists() {
        return None;
    }
    let out = Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let start = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!start.is_empty()).then_some(start)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Labels on every container: suite, repo, service, run, owner and lease.
#[must_use]
pub fn labels(suite: &Suite, service: &str, owner_pid: u32) -> Vec<(String, String)> {
    let owner = Owner::of(owner_pid);
    vec![
        ("io.hyperi.test.suite".into(), suite.label()),
        ("io.hyperi.test.repo".into(), suite.repo.into()),
        ("io.hyperi.test.service".into(), service.into()),
        ("io.hyperi.test.run".into(), run_id()),
        ("io.hyperi.test.owner-host".into(), owner.host),
        ("io.hyperi.test.owner-pidns".into(), owner.pid_ns),
        ("io.hyperi.test.owner-pid".into(), owner.pid.to_string()),
        ("io.hyperi.test.owner-start".into(), owner.start),
        (
            "io.hyperi.test.lease-until".into(),
            (now_secs() + LEASE.as_secs()).to_string(),
        ),
    ]
}

/// The owner labels a container carries, as read back for a sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Labelled {
    /// Container name.
    pub name: String,
    /// `owner-host`.
    pub host: String,
    /// `owner-pidns`.
    pub pid_ns: String,
    /// `owner-pid`.
    pub pid: Option<u32>,
    /// `owner-start`.
    pub start: String,
    /// `lease-until`, epoch seconds.
    pub lease_until: Option<u64>,
}

/// Whether a sweep from `here_host` and `here_pid_ns` removes `container`, at
/// `now` epoch seconds, with `start_of` giving a local process's start time.
///
/// On the owner's own host and PID namespace it goes once the owner process is
/// gone or its start time no longer matches. Anywhere else the owner cannot be
/// seen, so it goes only once its lease has lapsed. A container with no owner
/// labels is never taken.
#[must_use]
pub fn sweep_removes(
    container: &Labelled,
    here_host: &str,
    here_pid_ns: &str,
    now: u64,
    start_of: impl Fn(u32) -> Option<String>,
) -> bool {
    if container.host == here_host && container.pid_ns == here_pid_ns {
        return container
            .pid
            .is_some_and(|pid| start_of(pid).is_none_or(|start| start != container.start));
    }
    container.lease_until.is_some_and(|until| now > until)
}

/// Remove this suite's containers whose owner has gone, by [`sweep_removes`].
pub fn reap_dead_owners(suite: &Suite) {
    let filter = format!("label=io.hyperi.test.suite={}", suite.label());
    let format = "{{.Names}}\t{{.Label \"io.hyperi.test.owner-host\"}}\t\
                  {{.Label \"io.hyperi.test.owner-pidns\"}}\t{{.Label \"io.hyperi.test.owner-pid\"}}\t\
                  {{.Label \"io.hyperi.test.owner-start\"}}\t{{.Label \"io.hyperi.test.lease-until\"}}";
    let out = docker(&["ps", "--all", "--filter", &filter, "--format", format]);
    let (host, pid_ns, now) = (host_id(), pid_namespace(), now_secs());
    for container in parse_listing(&String::from_utf8_lossy(&out.stdout)) {
        if sweep_removes(&container, &host, &pid_ns, now, process_start) {
            let _ = docker(&["rm", "--force", "--volumes", &container.name]);
        }
    }
}

/// The tab-separated listing [`reap_dead_owners`] asks Docker for.
#[must_use]
pub fn parse_listing(listing: &str) -> Vec<Labelled> {
    listing
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            let name = fields.next().filter(|n| !n.is_empty())?.to_string();
            let mut next = || fields.next().unwrap_or("").trim().to_string();
            Some(Labelled {
                name,
                host: next(),
                pid_ns: next(),
                pid: next().parse().ok(),
                start: next(),
                lease_until: next().parse().ok(),
            })
        })
        .collect()
}

/// Start a detached process that, once `owner` exits, removes `container` and
/// ends `process`, and logs the removal to the suite's run log.
///
/// The shell backgrounds the watcher and exits, so the watcher is reparented
/// rather than left for this process to reap. It sits in a process group of
/// its own so a Ctrl-C to the run does not reach it, and holds none of the
/// test's pipes, which nextest would report as a leak.
///
/// # Panics
///
/// When `sh` cannot be run.
pub fn spawn_watcher(suite: &Suite, owner: u32, container: Option<&str>, process: Option<u32>) {
    use std::os::unix::process::CommandExt;
    let script = r#"(
while kill -0 "$1" 2>/dev/null; do sleep 1; done
if [ -n "$4" ]; then kill "$4" 2>/dev/null; fi
if [ -n "$2" ]; then
  docker rm --force --volumes "$2" >/dev/null 2>&1
  printf '%s removed %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$2" >> "$3"
fi
) </dev/null >/dev/null 2>&1 &"#;
    let started = Command::new("sh")
        .args(["-c", script, "shared-service-watcher"])
        .arg(owner.to_string())
        .arg(container.unwrap_or(""))
        .arg(suite.log_path())
        .arg(process.map(|p| p.to_string()).unwrap_or_default())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .status()
        .unwrap_or_else(|e| panic!("start the shared-container watcher: {e}"));
    assert!(
        started.success(),
        "the shared-container watcher did not start: {started}"
    );
}

enum State {
    Absent,
    Running,
    Other(String),
}

fn state(name: &str) -> State {
    let out = docker(&["inspect", "--format", "{{.State.Status}}", name]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        let lower = stderr.to_ascii_lowercase();
        assert!(
            lower.contains("no such object") || lower.contains("no such container"),
            "Docker is needed for the shared test containers: {stderr}"
        );
        return State::Absent;
    }
    match String::from_utf8_lossy(&out.stdout).trim() {
        "running" => State::Running,
        other => State::Other(other.to_string()),
    }
}

/// The published host ports, waiting for Docker to report every one.
fn published(name: &str, host: &str, ports: &[u16], deadline: Instant) -> SharedContainer {
    let mut mapped = Vec::with_capacity(ports.len());
    for &port in ports {
        loop {
            let out = docker(&["port", name, &format!("{port}/tcp")]);
            let text = String::from_utf8_lossy(&out.stdout);
            if let Some(host_port) = text
                .lines()
                .find_map(|l| l.rsplit(':').next()?.trim().parse().ok())
            {
                mapped.push((port, host_port));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{name} never published port {port}"
            );
            std::thread::sleep(POLL);
        }
    }
    SharedContainer {
        name: name.to_string(),
        host: host.to_string(),
        ports: mapped,
    }
}

/// The addresses each published port of container `name` is bound to on the
/// daemon host.
#[must_use]
pub fn bound_addresses(name: &str) -> Vec<String> {
    let out = docker(&["port", name]);
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let (_, bound) = line.split_once(" -> ")?;
            Some(bound.rsplit_once(':')?.0.to_string())
        })
        .collect()
}

fn wait_ready(container: &SharedContainer, ready: &Ready, deadline: Instant) {
    loop {
        let up = match ready {
            Ready::Http { port, path, body } => {
                http_get(&container.host, container.port(*port), path)
                    .is_some_and(|(status, text)| status == 200 && text.trim() == *body)
            }
            Ready::Log(line) => {
                let out = docker(&["logs", &container.name]);
                String::from_utf8_lossy(&out.stdout).contains(line)
                    || String::from_utf8_lossy(&out.stderr).contains(line)
            }
        };
        if up {
            return;
        }
        if Instant::now() >= deadline {
            let logs = docker(&["logs", "--tail", "40", &container.name]);
            panic!(
                "{} not ready by its deadline. Last log lines:\n{}{}",
                container.name,
                String::from_utf8_lossy(&logs.stdout),
                String::from_utf8_lossy(&logs.stderr)
            );
        }
        std::thread::sleep(POLL);
    }
}

/// Wait until every published port accepts a connection from this machine,
/// which through a tunnel can lag the container's own readiness.
fn wait_reachable(container: &SharedContainer, deadline: Instant) {
    for &(_, port) in &container.ports {
        loop {
            if connect_any(resolve(&container.host, port), Duration::from_secs(1)).is_some() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{} port {port} is not reachable at {}: a tunnel that did not open, \
                 or a local port already in use",
                container.name,
                container.host
            );
            std::thread::sleep(POLL);
        }
    }
}

/// Every address `host` resolves to on `port`, in resolver order.
fn resolve(host: &str, port: u16) -> Vec<SocketAddr> {
    (host, port)
        .to_socket_addrs()
        .map(Iterator::collect)
        .unwrap_or_default()
}

/// The first of `addrs` that accepts a connection, tried in order, so a name
/// that resolves to ::1 first still reaches a service on 127.0.0.1.
fn connect_any(
    addrs: impl IntoIterator<Item = SocketAddr>,
    timeout: Duration,
) -> Option<TcpStream> {
    addrs
        .into_iter()
        .find_map(|addr| TcpStream::connect_timeout(&addr, timeout).ok())
}

/// `GET path` over plain HTTP/1.0: the status code and the body.
#[must_use]
pub fn http_get(host: &str, port: u16, path: &str) -> Option<(u16, String)> {
    http_get_at(resolve(host, port), host, path)
}

/// `GET path` on the first of `addrs` that accepts, naming `host` in the request.
fn http_get_at(addrs: Vec<SocketAddr>, host: &str, path: &str) -> Option<(u16, String)> {
    let mut stream = connect_any(addrs, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    write!(stream, "GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n").ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let (head, body) = response.split_once("\r\n\r\n")?;
    let status: u16 = head.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, body.to_string()))
}

/// Short id of this test run: the start of nextest's run id, else this process.
#[must_use]
pub fn run_id() -> String {
    std::env::var("NEXTEST_RUN_ID").map_or_else(
        |_| format!("p{}", std::process::id()),
        |id| {
            id.chars()
                .filter(char::is_ascii_alphanumeric)
                .take(8)
                .collect()
        },
    )
}

/// The process whose exit ends the run: nextest when it is the runner.
#[must_use]
pub fn run_owner_pid() -> u32 {
    if std::env::var_os("NEXTEST_RUN_ID").is_some() {
        std::os::unix::process::parent_id()
    } else {
        std::process::id()
    }
}

/// A `HYPERI_TEST_*` setting: the process environment, then the project
/// `.env`, then the file named by `HYPERI_TEST_ENV_FILE`. Empty counts as unset.
#[must_use]
pub fn lookup(suite: &Suite, key: &str) -> Option<String> {
    let project = Path::new(suite.manifest_dir).join(".env");
    let in_project = |name: &str| file_value(&project, name);
    std::env::var(key)
        .ok()
        .or_else(|| in_project(key))
        .or_else(|| {
            let user = std::env::var("HYPERI_TEST_ENV_FILE")
                .ok()
                .or_else(|| in_project("HYPERI_TEST_ENV_FILE"))?;
            file_value(Path::new(&user), key)
        })
        .filter(|v| !v.is_empty())
}

/// `key` from a dotenv-format file, without touching the process environment.
fn file_value(path: &Path, key: &str) -> Option<String> {
    dotenvy::from_path_iter(path)
        .ok()?
        .filter_map(Result::ok)
        .find_map(|(k, v)| (k == key).then_some(v))
}

/// Whether this run may use a pet: never in CI, never under the full tier
/// (`HYPERCI_TEST_TIER=full`).
#[must_use]
pub fn pets_allowed() -> bool {
    std::env::var_os("CI").is_none()
        && !std::env::var("HYPERCI_TEST_TIER").is_ok_and(|t| t.eq_ignore_ascii_case("full"))
}

/// Run the docker CLI and return what it printed.
///
/// # Panics
///
/// When `docker` cannot be run at all.
#[must_use]
pub fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|e| panic!("run docker {args:?}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUITE: Suite = Suite {
        repo: "r",
        manifest_dir: "/nonexistent",
        tmp_dir: None,
    };

    /// A resolver that answers ::1 before 127.0.0.1 still reaches a service
    /// bound to 127.0.0.1 alone, for both the reachability probe and `GET`.
    #[test]
    fn the_probes_try_every_resolved_address() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        let server = std::thread::spawn(move || {
            for stream in listener.incoming().take(2) {
                let mut stream = stream.expect("accept");
                let mut request = [0u8; 256];
                let _ = stream.read(&mut request);
                let _ = stream.write_all(b"HTTP/1.0 200 OK\r\n\r\nOk.");
            }
        });
        let v6_first: Vec<SocketAddr> = vec![
            format!("[::1]:{port}").parse().expect("v6 addr"),
            format!("127.0.0.1:{port}").parse().expect("v4 addr"),
        ];
        let timeout = Duration::from_secs(1);
        assert!(
            connect_any(v6_first.clone(), timeout).is_some(),
            "the reachability probe stopped at ::1"
        );
        assert_eq!(
            http_get_at(v6_first, "localhost", "/ping"),
            Some((200, "Ok.".to_string())),
            "the readiness GET stopped at ::1"
        );
        server.join().expect("test server");
    }

    #[test]
    fn daemons_and_where_their_ports_bind() {
        let local = ("127.0.0.1".to_string(), "127.0.0.1".to_string());
        for (endpoint, want, binding) in [
            ("unix:///var/run/docker.sock", Daemon::Local, Some(&local)),
            ("", Daemon::Local, Some(&local)),
            ("tcp://127.0.0.1:2375", Daemon::Local, Some(&local)),
            ("tcp://localhost:2375", Daemon::Local, Some(&local)),
            (
                "ssh://derek@build.example:2222",
                Daemon::Ssh {
                    target: "derek@build.example".into(),
                    port: Some(2222),
                },
                Some(&local),
            ),
            (
                "ssh://build.example",
                Daemon::Ssh {
                    target: "build.example".into(),
                    port: None,
                },
                Some(&local),
            ),
            (
                "tcp://10.0.0.5:2376",
                Daemon::Tcp {
                    host: "10.0.0.5".into(),
                },
                None,
            ),
        ] {
            let daemon = Daemon::parse(endpoint);
            assert_eq!(daemon, want, "{endpoint}");
            let bound = std::panic::catch_unwind(|| daemon.binding(&SUITE));
            match binding {
                Some(b) => assert_eq!(bound.as_ref().ok(), Some(b), "{endpoint}"),
                None => assert!(
                    bound.is_err(),
                    "{endpoint}: a remote tcp daemon must refuse"
                ),
            }
        }
    }

    #[test]
    fn a_sweep_takes_only_containers_whose_owner_is_gone() {
        let here = ("h1/m1", "pid:[1]");
        let container = |host: &str, ns: &str, pid: u32, start: &str, lease: u64| Labelled {
            name: "c".into(),
            host: host.into(),
            pid_ns: ns.into(),
            pid: Some(pid),
            start: start.into(),
            lease_until: Some(lease),
        };
        // Process 10, started at 500, is the only one alive here.
        let start_of = |pid: u32| (pid == 10).then(|| "500".to_string());
        let now = 1_000;
        for (label, c, removed) in [
            (
                "ours, owner alive",
                container("h1/m1", "pid:[1]", 10, "500", 2_000),
                false,
            ),
            (
                "ours, owner gone",
                container("h1/m1", "pid:[1]", 11, "500", 2_000),
                true,
            ),
            (
                "ours, pid reused",
                container("h1/m1", "pid:[1]", 10, "400", 2_000),
                true,
            ),
            (
                "another host, lease live",
                container("h2/m2", "pid:[1]", 11, "1", 2_000),
                false,
            ),
            (
                "another host, lease lapsed",
                container("h2/m2", "pid:[1]", 11, "1", 900),
                true,
            ),
            (
                "another namespace, lease live",
                container("h1/m1", "pid:[9]", 11, "1", 2_000),
                false,
            ),
        ] {
            assert_eq!(
                sweep_removes(&c, here.0, here.1, now, start_of),
                removed,
                "{label}"
            );
        }
        let unlabelled = Labelled {
            name: "old".into(),
            ..Labelled::default()
        };
        assert!(
            !sweep_removes(&unlabelled, here.0, here.1, now, start_of),
            "a container with no owner labels is never taken"
        );
    }

    #[test]
    fn a_listing_reads_back_into_owner_labels() {
        let listing = "a\th\tns\t42\t900\t1234\nb\t\t\t\t\t\n\n";
        let got = parse_listing(listing);
        assert_eq!(got.len(), 2);
        assert_eq!(
            got[0],
            Labelled {
                name: "a".into(),
                host: "h".into(),
                pid_ns: "ns".into(),
                pid: Some(42),
                start: "900".into(),
                lease_until: Some(1234),
            }
        );
        assert_eq!((got[1].pid, got[1].lease_until), (None, None));
    }

    /// Names carry the suite prefix, the test and the pid, collapse what
    /// Docker refuses, and differ between two tests wanting one service.
    #[test]
    fn container_names_are_legal_and_per_test() {
        let pid = std::process::id();
        let base = "r-test-integration";
        for (test, service, want) in [
            (
                Some("inserter_basic"),
                "clickhouse",
                format!("{base}-inserter-basic-clickhouse-p{pid}"),
            ),
            (None, "kafka", format!("{base}-kafka-p{pid}")),
            (
                Some("e2e::kafka::test_x"),
                "Kafka",
                format!("{base}-e2e--kafka--test-x-kafka-p{pid}"),
            ),
        ] {
            let name = temp_env::with_var_unset("HYPERI_TEST_CONTAINER_PREFIX", || {
                container_name(&SUITE, test, service)
            });
            assert_eq!(name, want);
            let mut chars = name.chars();
            assert!(
                chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
                    && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')),
                "{name} is not a legal Docker container name"
            );
        }
        assert_ne!(
            container_name(&SUITE, Some("first"), "kafka"),
            container_name(&SUITE, Some("second"), "kafka"),
        );
    }

    #[test]
    fn test_name_reports_the_calling_test() {
        assert_eq!(crate::test_name!(), "test_name_reports_the_calling_test");
    }

    /// An `async fn` body compiles to a generated future, so the raw path
    /// carries `::{{closure}}`, which the macro trims.
    #[tokio::test]
    async fn test_name_reports_the_calling_test_when_async() {
        assert_eq!(
            crate::test_name!(),
            "test_name_reports_the_calling_test_when_async"
        );
    }

    /// The prefix setting wins over `<repo>-test`, from the environment or
    /// the file `HYPERI_TEST_ENV_FILE` names, and an empty value is unset.
    #[test]
    fn the_prefix_follows_the_lookup_ladder() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.env");
        std::fs::write(&file, "HYPERI_TEST_CONTAINER_PREFIX=from-file\n").unwrap();
        let file = file.to_string_lossy().into_owned();
        for (case, env, env_file, want) in [
            ("unset", None, None, "r-test"),
            ("process env", Some("ci-123"), None, "ci-123"),
            ("empty is unset", Some(""), None, "r-test"),
            ("user file", None, Some(file.as_str()), "from-file"),
            (
                "env beats the file",
                Some("ci-123"),
                Some(file.as_str()),
                "ci-123",
            ),
        ] {
            temp_env::with_vars(
                [
                    ("HYPERI_TEST_CONTAINER_PREFIX", env),
                    ("HYPERI_TEST_ENV_FILE", env_file),
                ],
                || assert_eq!(SUITE.prefix(), want, "{case}"),
            );
        }
    }
}
