// Project:   scalo
// File:      src/transport/proxy/squid.rs
// Purpose:   One real Squid per test run for the proxy tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A real Squid for tests that go through an HTTP CONNECT proxy, behind the
//! `transport-proxy-test-support` feature.
//!
//! [`Squid::shared`] is the run's one container, started by the first test
//! process to ask and removed when the run ends, so every test binary uses the
//! same proxy. One container serves four behaviours on four ports, picked by
//! the URL a test takes from it:
//!
//! | URL | CONNECT |
//! |---|---|
//! | [`open`](Squid::open) | any target |
//! | [`port_only`](Squid::port_only) | port 443 only, 403 for the rest |
//! | [`auth`](Squid::auth) | 407 without credentials |
//! | [`auth_with`](Squid::auth_with) | any target for [`USER`] and [`password`](Squid::password), 407 for others |
//!
//! [`TARGET_HOST`] resolves only inside Squid, to the machine running the
//! tests, so a connection to it that arrives went through the proxy. Put a
//! listener on `0.0.0.0` and reach it as `TARGET_HOST:port`. The Docker daemon
//! has to run on the machine running the tests, since Squid dials back to it
//! across the daemon's bridge.

use std::time::Duration;

use tokio::sync::OnceCell;

use crate::deployment::test_support::shared_service::{
    self, Daemon, Ready, ServiceSpec, SharedContainer, Suite,
};

/// Canonical's Squid, as `image:tag@digest`.
///
/// renovate: datasource=docker depName=ubuntu/squid
pub const SQUID_IMAGE: &str = "docker.io/ubuntu/squid:7.2-26.04_edge@sha256:0f1f416ce714f39f439532fb32cc5cd45cf285d56824662a0bd0ea3d6b8e3317";

/// The name only Squid resolves, to the machine running the tests.
pub const TARGET_HOST: &str = "target.test";

/// The proxy user [`Squid::auth_with`] takes.
pub const USER: &str = "scalo";

/// Where the run's random proxy password sits in the container.
const PASSWORD_FILE: &str = "/etc/squid/proxy-password";

const OPEN_PORT: u16 = 3128;
const PORT_ONLY_PORT: u16 = 3129;
const AUTH_PORT: u16 = 3130;

const SUITE: Suite = Suite {
    repo: "scalo-rs",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

/// The run's Squid.
#[derive(Debug)]
pub struct Squid {
    container: SharedContainer,
    password: String,
}

impl Squid {
    /// The run's Squid, started by the first caller. `None` when no Docker
    /// daemon runs on this machine, which fails the test when `CI` is set.
    ///
    /// # Panics
    ///
    /// When the container can't be started.
    pub async fn shared() -> Option<&'static Self> {
        static SHARED: OnceCell<Squid> = OnceCell::const_new();
        if !crate::deployment::test_support::docker_available() {
            skip_or_fail("no Docker daemon answers");
            return None;
        }
        if Daemon::current() != Daemon::Local {
            skip_or_fail("the Docker daemon is not on this machine, so Squid can't dial back");
            return None;
        }
        Some(
            SHARED
                .get_or_init(|| async {
                    let container =
                        tokio::task::spawn_blocking(|| shared_service::acquire(&SUITE, &spec()))
                            .await
                            .unwrap_or_else(|e| panic!("start Squid: {e}"));
                    let password = String::from_utf8_lossy(&container.read_file(PASSWORD_FILE))
                        .trim()
                        .to_string();
                    Self {
                        container,
                        password,
                    }
                })
                .await,
        )
    }

    /// A proxy that opens a tunnel to any target.
    #[must_use]
    pub fn open(&self) -> String {
        self.url(OPEN_PORT, None)
    }

    /// A proxy that opens a tunnel to port 443 only and answers 403 to the rest.
    #[must_use]
    pub fn port_only(&self) -> String {
        self.url(PORT_ONLY_PORT, None)
    }

    /// A proxy that wants credentials: 407 without them.
    #[must_use]
    pub fn auth(&self) -> String {
        self.url(AUTH_PORT, None)
    }

    /// The password of [`USER`] for this run: random, written into the
    /// container when it starts.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }

    /// The same proxy with `user:password@` in its URL. [`USER`] and
    /// [`password`](Self::password) open a tunnel to any target, and any other
    /// pair is 407.
    #[must_use]
    pub fn auth_with(&self, user: &str, password: &str) -> String {
        self.url(AUTH_PORT, Some((user, password)))
    }

    fn url(&self, port: u16, credentials: Option<(&str, &str)>) -> String {
        let address = self.container.addr(port);
        match credentials {
            Some((user, password)) => format!("http://{user}:{password}@{address}"),
            None => format!("http://{address}"),
        }
    }
}

fn skip_or_fail(reason: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "{reason} in CI, which provides a container runtime. This test must RUN here."
    );
    eprintln!("skipping: {reason}");
}

fn spec() -> ServiceSpec {
    ServiceSpec {
        ports: vec![OPEN_PORT, PORT_ONLY_PORT, AUTH_PORT],
        files,
        after_start: hash_password,
        // The image's supervisor logs this once Squid has stayed up for a second.
        ready: Ready::Log("Started default services"),
        ..ServiceSpec::new("squid", SQUID_IMAGE, Duration::from_secs(120))
    }
}

/// Hashes the random password into the file `basic_ncsa_auth` reads, with the
/// image's own `openssl`, in `/tmp` because the exec user can't write `/etc`.
fn hash_password(container: &SharedContainer) {
    let script = format!(
        "printf '{USER}:' > /tmp/squid-passwd && openssl passwd -6 -in {PASSWORD_FILE} >> /tmp/squid-passwd"
    );
    let _ = container.exec(&["sh", "-c", &script]);
}

/// The config, password and hosts file copied in before Squid starts.
fn files(_host: &str) -> Vec<(String, Vec<u8>)> {
    let out = shared_service::docker(&[
        "network",
        "inspect",
        "bridge",
        "--format",
        "{{(index .IPAM.Config 0).Gateway}}",
    ]);
    let gateway = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(
        !gateway.is_empty(),
        "no gateway on Docker's bridge network: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let conf = format!(
        "\
http_port {OPEN_PORT} name=open
http_port {PORT_ONLY_PORT} name=port_only
http_port {AUTH_PORT} name=auth
hosts_file /etc/squid/hosts
auth_param basic program /usr/lib/squid/basic_ncsa_auth /tmp/squid-passwd
auth_param basic realm scalo-test
acl via_open myportname open
acl via_port_only myportname port_only
acl via_auth myportname auth
acl tunnel method CONNECT
acl tls_port port 443
acl known proxy_auth REQUIRED
http_access allow tunnel via_open
http_access allow tunnel via_port_only tls_port
http_access allow tunnel via_auth known
http_access deny all
"
    );
    vec![
        ("/etc/squid/squid.conf".into(), conf.into_bytes()),
        (
            PASSWORD_FILE.into(),
            uuid::Uuid::new_v4().simple().to_string().into_bytes(),
        ),
        (
            "/etc/squid/hosts".into(),
            format!("{gateway} {TARGET_HOST}\n").into_bytes(),
        ),
    ]
}
