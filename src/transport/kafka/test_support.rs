// Project:   scalo
// File:      src/transport/kafka/test_support.rs
// Purpose:   Kafka test fixtures for consumer suites: pinned images and a TLS broker in Docker
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka test fixtures, behind the `transport-kafka-test-support` feature.
//!
//! An app's integration tests depend on this instead of copying it, so the
//! broker image is pinned in [`KAFKA_IMAGE`] and nowhere else, and a fix to the
//! fixture reaches every suite on its next scalo bump. It starts containers, so
//! the test host needs a Docker daemon. [`require_docker`] skips a test without
//! one on a developer's machine and fails it when `CI` is set.
//!
//! [`shared_kafka`] is the run's one PLAINTEXT broker, shared by every test
//! process through [`shared_service`], and [`kafka_spec`] is its spec, for a
//! test that needs a broker to itself
//! ([`start_alone`](shared_service::start_alone)).
//!
//! [`TlsBroker`] is one single-node KRaft broker with three secured listeners,
//! and the certificates for it.
//!
//! The broker is Apache Kafka's own image, driven by the docker CLI so the
//! certificates and `server.properties` are in place before it starts. It runs
//! on the host network, so the addresses it advertises are the ones the client
//! under test dials.
//!
//! | Listener   | Transport | Client must present                     |
//! |------------|-----------|-----------------------------------------|
//! | `SSL`      | TLS       | nothing                                 |
//! | `SASL_SSL` | TLS       | SCRAM-SHA-512 credentials               |
//! | `SSL_MTLS` | TLS       | a certificate signed by the test CA     |
//!
//! The certificates come from `openssl` in a pinned container, so the host needs
//! no `openssl` and the keys are a different implementation from the one under
//! test (librdkafka links OpenSSL, the broker is a JVM).

use std::fs::Permissions;
use std::path::Path;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::process::Command;

use crate::deployment::test_support::shared_service::{
    self, Ready, ServiceSpec, SharedContainer, Suite,
};

/// Makes the throwaway certificates.
///
/// renovate: datasource=docker depName=alpine/openssl
const OPENSSL_IMAGE: &str =
    "alpine/openssl:3.5.9@sha256:1c361a204e9791bec6202bccbd1065eafaa65cb5a1a9a06ae000238569fba6a2";

/// The broker image every Kafka test starts, as `image:tag@digest`.
///
/// The JVM image: `apache/kafka-native` before 4.4.0 segfaults in `getpwuid` on ~2% of starts.
/// The line the DFE stack deploys.
///
/// renovate: datasource=docker depName=apache/kafka
pub const KAFKA_IMAGE: &str =
    "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

/// [`KAFKA_IMAGE`] without the image name, as `tag@digest`.
///
/// What `testcontainers-modules`' `with_tag` takes, with the image set by `with_jvm_image`.
#[must_use]
pub fn kafka_tag() -> &'static str {
    KAFKA_IMAGE
        .split_once(':')
        .map_or(KAFKA_IMAGE, |(_, tag)| tag)
}

/// The PLAINTEXT listener inside a [`kafka_spec`] broker.
pub const KAFKA_PORT: u16 = 9092;

/// A JVM broker under CI container contention can take most of a minute.
const SHARED_STARTUP_TIMEOUT: Duration = Duration::from_secs(180);

/// Where the broker's entrypoint waits for its start script.
const KAFKA_START_SCRIPT: &str = "/opt/kafka/hyperi_start.sh";

/// One single-node KRaft broker, PLAINTEXT on [`KAFKA_PORT`], advertising
/// the host port Docker publishes it on, or `host_port` when given.
///
/// Pin `host_port` for a client configured before the broker exists, or for a
/// test that stops and starts the broker, so the address stays the same. The
/// entrypoint waits for a start script written once the port is known, so
/// the advertised address is the one a client here dials.
#[must_use]
pub fn kafka_spec(host_port: Option<u16>) -> ServiceSpec {
    let wait = format!(
        "while [ ! -f {KAFKA_START_SCRIPT} ]; do sleep 0.1; done; \
         chmod 755 {KAFKA_START_SCRIPT} && {KAFKA_START_SCRIPT}"
    );
    let env = [
        (
            "KAFKA_LISTENERS",
            "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
        ),
        ("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw"),
        ("KAFKA_PROCESS_ROLES", "broker,controller"),
        ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
        (
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        ),
        ("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
        ("KAFKA_NODE_ID", "1"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094"),
    ];
    let (ports, pinned_ports) = match host_port {
        Some(host) => (Vec::new(), vec![(KAFKA_PORT, host)]),
        None => (vec![KAFKA_PORT], Vec::new()),
    };
    ServiceSpec {
        ports,
        pinned_ports,
        env: env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
        entrypoint: Some("bash".into()),
        args: vec!["-c".into(), wait],
        after_start: write_kafka_start_script,
        ready: Ready::Log("Kafka Server started"),
        ..ServiceSpec::new("kafka", KAFKA_IMAGE, SHARED_STARTUP_TIMEOUT)
    }
}

/// The broker advertises the published port, which only exists once the
/// container runs, so the entrypoint waits for this script.
fn write_kafka_start_script(container: &SharedContainer) {
    let host = &container.host;
    let port = container.port(KAFKA_PORT);
    let script = format!(
        "printf '#!/usr/bin/env bash\\nexport \
         KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{host}:{port},BROKER://localhost:9093\\n\
         /etc/kafka/docker/run\\n' > {KAFKA_START_SCRIPT}"
    );
    let _ = container.exec(&["sh", "-c", &script]);
}

/// The run's shared broker, started by the first test process to ask and
/// removed when the run ends. Its bootstrap address is
/// `container.addr(KAFKA_PORT)`.
///
/// Tests on it keep to their own topics and consumer groups. Call
/// [`require_docker`] first, so a machine with no Docker daemon skips rather
/// than panics, and CI still fails.
///
/// # Panics
///
/// When no Docker daemon answers, or the broker cannot be started or joined
/// (see [`shared_service::acquire`]).
pub async fn shared_kafka(suite: Suite) -> SharedContainer {
    static SHARED: OnceLock<SharedContainer> = OnceLock::new();
    if let Some(container) = SHARED.get() {
        return container.clone();
    }
    tokio::task::spawn_blocking(move || {
        SHARED
            .get_or_init(|| shared_service::acquire(&suite, &kafka_spec(None)))
            .clone()
    })
    .await
    .unwrap_or_else(|e| panic!("acquire the shared Kafka broker: {e}"))
}

/// Whether a Docker daemon answers `docker info`, asked once per test binary.
#[must_use]
pub fn docker_answers() -> bool {
    static ANSWERS: OnceLock<bool> = OnceLock::new();
    *ANSWERS.get_or_init(|| {
        std::process::Command::new("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

/// Fail in CI rather than skip: a skipped container test reports green while
/// exercising nothing, and CI does provide a container runtime.
///
/// # Panics
///
/// When the `CI` environment variable is set.
pub fn skip_or_fail(reason: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "{reason} in CI, which provides a container runtime. This test must RUN here."
    );
    eprintln!("skipping: {reason}");
}

/// Whether a test that needs Docker can run. Without a daemon it skips on a
/// developer's machine (`false`, with the reason printed) and fails when `CI`
/// is set.
///
/// ```rust,ignore
/// #[tokio::test]
/// async fn against_a_real_broker() {
///     if !require_docker() {
///         return;
///     }
///     // start a broker
/// }
/// ```
///
/// # Panics
///
/// When no Docker daemon answers and the `CI` environment variable is set.
#[must_use]
pub fn require_docker() -> bool {
    if docker_answers() {
        return true;
    }
    skip_or_fail("no Docker daemon answers");
    false
}

/// Directory inside the broker container the test's files are copied to.
const BROKER_DIR: &str = "/dfe-test";

/// A JVM broker takes 5-12 s to become ready, a good deal longer on a busy runner.
const START_DEADLINE: Duration = Duration::from_secs(240);

/// How long an image may take to pull.
const PULL_DEADLINE: Duration = Duration::from_secs(300);

/// The SCRAM-SHA-512 user the broker is formatted with.
pub const SCRAM_USER: &str = "dfe";

/// Its password.
pub const SCRAM_PASSWORD: &str = "dfe-secret";

/// Fixed cluster id: KRaft storage must be formatted with one.
const CLUSTER_ID: &str = "MkU3OEVBNTcwNTJENDM2Qk";

/// Certificates for the broker and its clients.
///
/// `ca.crt` signs `server` (valid for `localhost` and `127.0.0.1`) and `client`.
/// `rogue-ca.crt` is an unrelated CA that signs `rogue-client`.
const PKI_SCRIPT: &str = r#"set -e
mkdir /pki && cd /pki
key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1"; }
ca() {
  key "$1.key"
  openssl req -x509 -new -key "$1.key" -subj "/CN=$1" -days 2 \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$1.crt"
}
leaf() {
  key "$1.key"
  openssl req -new -key "$1.key" -subj "/CN=$1" -out "$1.csr"
  printf '%s\n' "basicConstraints=CA:FALSE" "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA "$2.crt" -CAkey "$2.key" -CAcreateserial -days 2 -extfile "$1.ext" -out "$1.crt"
}
ca ca
ca rogue-ca
leaf server ca "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
leaf client ca "extendedKeyUsage=clientAuth"
leaf rogue-client rogue-ca "extendedKeyUsage=clientAuth"
cat server.key server.crt > server.pem
rm -f *.csr *.ext *.srl
chmod 644 *
tar -cf - .
"#;

async fn docker(args: &[&str]) -> Result<std::process::Output, String> {
    Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| format!("docker could not run: {e}"))
}

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

/// First port of the range the broker's ports come from, below the kernel's ephemeral range.
const PORT_BASE: u32 = 20_000;

/// Ports each test process draws from, and how many such windows fit between [`PORT_BASE`] and the ephemeral range.
const PORT_WINDOW: u32 = 50;
const PORT_WINDOWS: u32 = 255;

/// Ports already handed out by this process.
static PORTS_TAKEN: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// A loopback port nothing is bound to at the moment of the call.
///
/// The broker binds its own listeners, so the ports have to be written into its
/// config before it starts. nextest runs tests as concurrent processes, and two
/// of them asking the kernel for a free port can be given the same one, so each
/// process draws from a window picked by its pid.
fn free_port() -> u16 {
    use std::sync::atomic::Ordering;

    let window = PORT_BASE + (std::process::id() % PORT_WINDOWS) * PORT_WINDOW;
    loop {
        let taken = PORTS_TAKEN.fetch_add(1, Ordering::Relaxed);
        if taken >= PORT_WINDOW {
            break;
        }
        let port = u16::try_from(window + taken).unwrap_or(0);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|e| panic!("no loopback port could be bound: {e}"));
    listener
        .local_addr()
        .unwrap_or_else(|e| panic!("no local address: {e}"))
        .port()
}

/// `docker create` or `docker run` arguments naming a container `test` owns
/// for `service`, labelled for `suite` with this process as its owner, after
/// sweeping the suite's containers whose owner has gone.
fn owned_container_args(suite: &Suite, test: &str, service: &str) -> (String, Vec<String>) {
    shared_service::reap_dead_owners(suite);
    owned_container_flags(suite, test, service)
}

/// The name and the `--name` and `--label` flags of [`owned_container_args`].
fn owned_container_flags(suite: &Suite, test: &str, service: &str) -> (String, Vec<String>) {
    let name = shared_service::container_name(suite, Some(test), service);
    let mut args = vec!["--name".to_string(), name.clone()];
    for (key, value) in shared_service::labels(suite, service, std::process::id()) {
        args.extend(["--label".to_string(), format!("{key}={value}")]);
    }
    (name, args)
}

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

/// The generated certificates, in a directory removed on drop.
pub struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    /// Generate the certificates in a container named and labelled as one
    /// `test` owns in `suite` (see [`shared_service::container_name`]).
    ///
    /// # Panics
    ///
    /// When Docker or the openssl image fails: a TLS test that cannot make its
    /// certificates has nothing to test.
    pub async fn generate(suite: &Suite, test: &str) -> Self {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("temp dir: {e}"));
        let (name, owned) = owned_container_args(suite, test, "openssl");
        shared_service::spawn_watcher(suite, std::process::id(), Some(&name), None);
        let mut run: Vec<&str> = vec!["run", "--rm"];
        run.extend(owned.iter().map(String::as_str));
        run.extend(["--entrypoint", "sh", OPENSSL_IMAGE, "-c", PKI_SCRIPT]);
        let out = docker(&run).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(
            out.status.success(),
            "the openssl container could not make certificates: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let tar = dir.path().join("pki.tar");
        tokio::fs::write(&tar, &out.stdout)
            .await
            .unwrap_or_else(|e| panic!("write pki.tar: {e}"));
        let status = Command::new("tar")
            .arg("-xf")
            .arg(&tar)
            .arg("-C")
            .arg(dir.path())
            .status()
            .await
            .unwrap_or_else(|e| panic!("tar could not run: {e}"));
        assert!(status.success(), "tar could not unpack the certificates");
        tokio::fs::remove_file(&tar)
            .await
            .unwrap_or_else(|e| panic!("remove pki.tar: {e}"));
        Self { dir }
    }

    /// Path of a generated file on the host, such as `ca.crt` or `client.key`.
    #[must_use]
    pub fn file(&self, name: &str) -> String {
        self.dir.path().join(name).to_string_lossy().into_owned()
    }

    fn dir(&self) -> &Path {
        self.dir.path()
    }
}

// ---------------------------------------------------------------------------
// Broker
// ---------------------------------------------------------------------------

/// A running broker, removed when dropped.
pub struct TlsBroker {
    name: String,
    /// The certificates the broker and the test's clients use.
    pub pki: Pki,
    /// `SSL` listener: TLS, no client certificate asked for.
    pub tls: String,
    /// `SASL_SSL` listener: TLS plus SCRAM-SHA-512.
    pub sasl_ssl: String,
    /// `SSL_MTLS` listener: TLS, a client certificate required.
    pub mtls: String,
    scratch: tempfile::TempDir,
}

fn server_properties(controller: u16, tls: u16, sasl_ssl: u16, mtls: u16) -> String {
    format!(
        "process.roles=broker,controller\n\
         node.id=1\n\
         controller.quorum.voters=1@127.0.0.1:{controller}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://127.0.0.1:{controller},SSL://127.0.0.1:{tls},\
         SASL_SSL://127.0.0.1:{sasl_ssl},SSL_MTLS://127.0.0.1:{mtls}\n\
         advertised.listeners=SSL://127.0.0.1:{tls},SASL_SSL://127.0.0.1:{sasl_ssl},\
         SSL_MTLS://127.0.0.1:{mtls}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL,SSL_MTLS:SSL\n\
         inter.broker.listener.name=SSL\n\
         ssl.keystore.type=PEM\n\
         ssl.keystore.location={BROKER_DIR}/server.pem\n\
         ssl.truststore.type=PEM\n\
         ssl.truststore.location={BROKER_DIR}/ca.crt\n\
         ssl.client.auth=none\n\
         listener.name.ssl_mtls.ssl.client.auth=required\n\
         sasl.enabled.mechanisms=SCRAM-SHA-512\n\
         listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=\
         org.apache.kafka.common.security.scram.ScramLoginModule required;\n\
         offsets.topic.replication.factor=1\n\
         transaction.state.log.replication.factor=1\n\
         transaction.state.log.min.isr=1\n\
         group.initial.rebalance.delay.ms=0\n\
         num.partitions=1\n\
         log.dirs=/tmp/kraft-logs\n"
    )
}

/// Formats the KRaft storage with the SCRAM user, then runs the broker.
fn start_script() -> String {
    format!(
        "set -e\n\
         cd /opt/kafka\n\
         bin/kafka-storage.sh format -t {CLUSTER_ID} -c {BROKER_DIR}/server.properties \
         --add-scram 'SCRAM-SHA-512=[name={SCRAM_USER},password={SCRAM_PASSWORD}]' --ignore-formatted\n\
         exec bin/kafka-server-start.sh {BROKER_DIR}/server.properties\n"
    )
}

impl TlsBroker {
    /// Generate the certificates, start the broker and wait until it serves.
    ///
    /// The broker and the openssl container are named for `test` under the
    /// suite's prefix (`HYPERI_TEST_CONTAINER_PREFIX`, else `<repo>-test`), as
    /// [`shared_service::container_name`] names them, and carry the suite's
    /// owner labels, so a leftover says whose it is and a later run's sweep
    /// removes it once this process has gone.
    ///
    /// # Errors
    ///
    /// The docker CLI's complaint when an image will not pull, the files will
    /// not copy in, or the broker does not come up.
    pub async fn start(suite: &Suite, test: &str) -> Result<Self, String> {
        let pki = Pki::generate(suite, test).await;
        let controller = free_port();
        let (tls, sasl_ssl, mtls) = (free_port(), free_port(), free_port());

        let scratch = tempfile::tempdir().map_err(|e| format!("temp dir: {e}"))?;
        for name in ["ca.crt", "server.pem"] {
            tokio::fs::copy(pki.dir().join(name), scratch.path().join(name))
                .await
                .map_err(|e| format!("stage {name}: {e}"))?;
        }
        write(
            &scratch.path().join("server.properties"),
            &server_properties(controller, tls, sasl_ssl, mtls),
        )
        .await?;
        write(&scratch.path().join("start.sh"), &start_script()).await?;
        // The broker runs as its image's user, whose uid need not match this process's.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(scratch.path(), Permissions::from_mode(0o755))
                .await
                .map_err(|e| format!("chmod scratch dir: {e}"))?;
        }

        let (name, owned) = owned_container_args(suite, test, "kafka");
        shared_service::spawn_watcher(suite, std::process::id(), Some(&name), None);
        let mut create: Vec<String> = vec!["create".into()];
        create.extend(owned);
        create.extend(
            [
                "--network",
                "host",
                "--env",
                "KAFKA_HEAP_OPTS=-Xms256m -Xmx768m",
                "--entrypoint",
                "sh",
                KAFKA_IMAGE,
            ]
            .map(String::from),
        );
        create.push(format!("{BROKER_DIR}/start.sh"));
        let refs: Vec<&str> = create.iter().map(String::as_str).collect();
        let created = tokio::time::timeout(PULL_DEADLINE, docker(&refs))
            .await
            .map_err(|_| format!("{KAFKA_IMAGE} did not pull within {PULL_DEADLINE:?}"))??;
        if !created.status.success() {
            return Err(format!(
                "docker create {KAFKA_IMAGE} failed: {}",
                String::from_utf8_lossy(&created.stderr)
            ));
        }
        // From here the Drop impl removes the container on any early return.
        let broker = Self {
            name,
            pki,
            tls: format!("127.0.0.1:{tls}"),
            sasl_ssl: format!("127.0.0.1:{sasl_ssl}"),
            mtls: format!("127.0.0.1:{mtls}"),
            scratch,
        };

        let src = format!("{}/.", broker.scratch.path().display());
        let dest = format!("{}:{BROKER_DIR}", broker.name);
        let copied = docker(&["cp", &src, &dest]).await?;
        if !copied.status.success() {
            return Err(format!(
                "docker cp into {} failed: {}",
                broker.name,
                String::from_utf8_lossy(&copied.stderr)
            ));
        }
        let started = docker(&["start", &broker.name]).await?;
        if !started.status.success() {
            return Err(format!(
                "docker start {} failed: {}",
                broker.name,
                String::from_utf8_lossy(&started.stderr)
            ));
        }
        broker
            .wait_for_log("Kafka Server started", START_DEADLINE)
            .await?;
        Ok(broker)
    }

    /// Everything the broker has written to stdout and stderr.
    pub async fn logs(&self) -> String {
        match docker(&["logs", &self.name]).await {
            Ok(out) => {
                let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&out.stderr));
                text
            }
            Err(e) => e,
        }
    }

    async fn running(&self) -> bool {
        docker(&["inspect", "--format", "{{.State.Running}}", &self.name])
            .await
            .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "true")
    }

    async fn wait_for_log(&self, needle: &str, within: Duration) -> Result<(), String> {
        let deadline = Instant::now() + within;
        loop {
            let logs = self.logs().await;
            if logs.contains(needle) {
                return Ok(());
            }
            let running = self.running().await;
            if !running || Instant::now() > deadline {
                return Err(format!(
                    "{} never logged {needle:?} (running: {running}). Log:\n{logs}",
                    self.name
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn write(path: &Path, text: &str) -> Result<(), String> {
    tokio::fs::write(path, text)
        .await
        .map_err(|e| format!("write {}: {e}", path.display()))
}

impl Drop for TlsBroker {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", "--volumes", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUITE: Suite = Suite {
        repo: "r",
        manifest_dir: "/nonexistent",
        tmp_dir: None,
    };

    /// The broker and the openssl container are named under the prefix
    /// setting and carry the suite's owner labels, so neither starts unnamed.
    #[test]
    fn tls_broker_containers_take_the_suite_prefix_and_owner_labels() {
        let pid = std::process::id();
        for (prefix, base) in [(None, "r-test"), (Some("ci-42"), "ci-42")] {
            temp_env::with_var("HYPERI_TEST_CONTAINER_PREFIX", prefix, || {
                for service in ["kafka", "openssl"] {
                    let (name, flags) = owned_container_flags(&SUITE, "kafka_tls", service);
                    assert_eq!(
                        name,
                        format!("{base}-integration-kafka-tls-{service}-p{pid}"),
                        "prefix {prefix:?}"
                    );
                    assert_eq!(flags[..2], ["--name".to_string(), name.clone()]);
                    for label in [
                        "io.hyperi.test.suite=r-integration".to_string(),
                        format!("io.hyperi.test.service={service}"),
                        format!("io.hyperi.test.owner-pid={pid}"),
                    ] {
                        assert!(
                            flags.windows(2).any(|w| w[0] == "--label" && w[1] == label),
                            "{service}: no {label} in {flags:?}"
                        );
                    }
                }
            });
        }
    }
}
