// Project:   scalo
// File:      tests/common/tls_broker.rs
// Purpose:   A real Apache Kafka broker with TLS, mTLS and SASL_SSL SCRAM listeners, in Docker
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One single-node KRaft broker with three secured listeners, and the
//! certificates for it.
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

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command;

/// Makes the throwaway certificates.
///
/// renovate: datasource=docker depName=alpine/openssl
const OPENSSL_IMAGE: &str =
    "alpine/openssl:3.5.9@sha256:1c361a204e9791bec6202bccbd1065eafaa65cb5a1a9a06ae000238569fba6a2";

/// The JVM image: `apache/kafka-native` before 4.4.0 segfaults in `getpwuid` on ~2% of starts.
///
/// The line the DFE stack deploys.
///
/// renovate: datasource=docker depName=apache/kafka
const KAFKA_IMAGE: &str =
    "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

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

// ---------------------------------------------------------------------------
// Container naming
// ---------------------------------------------------------------------------

/// Label marking every container this suite starts, for bulk cleanup.
const SUITE_LABEL: (&str, &str) = ("io.hyperi.test.suite", "scalo-rs-integration");

/// Container name: `scalo-rs-test-integration-<test>-<service>`.
fn container_name(test: &str, service: &str) -> String {
    let slug = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    };
    format!("scalo-rs-test-integration-{}-{}", slug(test), slug(service))
}

/// Whether process `pid` is alive on this host. `ps` failing to run counts as
/// alive, so a container is never removed on a guess.
fn process_alive(pid: u32) -> bool {
    std::process::Command::new("ps")
        .args(["-p", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_or(true, |s| s.success())
}

/// The name to start a container under: `name`, or `name-<pid>` when a live
/// run on this host holds it.
///
/// A holder is removed only when its `io.hyperi.test.owner-pid` label names an
/// exited process, so a concurrent run's container is never touched.
fn claim_container_name(name: &str) -> String {
    let own = format!("{name}-{}", std::process::id());
    for candidate in [name, own.as_str()] {
        let inspect = std::process::Command::new("docker")
            .args([
                "inspect",
                "--type",
                "container",
                "--format",
                r#"{{index .Config.Labels "io.hyperi.test.owner-pid"}}"#,
                candidate,
            ])
            .output();
        let Ok(out) = inspect else {
            return candidate.to_string();
        };
        if !out.status.success() {
            return candidate.to_string();
        }
        let leaked = match String::from_utf8_lossy(&out.stdout).trim().parse::<u32>() {
            Ok(pid) => pid == std::process::id() || !process_alive(pid),
            Err(_) => false,
        };
        if leaked {
            let _ = std::process::Command::new("docker")
                .args(["rm", "--force", "--volumes", candidate])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            return candidate.to_string();
        }
    }
    own
}

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

/// The generated certificates, in a directory removed on drop.
pub struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    /// Generate the certificates in a container.
    ///
    /// # Panics
    ///
    /// When Docker or the openssl image fails: a TLS test that cannot make its
    /// certificates has nothing to test.
    pub async fn generate() -> Self {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("temp dir: {e}"));
        let out = docker(&[
            "run",
            "--rm",
            "--entrypoint",
            "sh",
            OPENSSL_IMAGE,
            "-c",
            PKI_SCRIPT,
        ])
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            out.status.success(),
            "the openssl container could not make certificates: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let tar = dir.path().join("pki.tar");
        std::fs::write(&tar, &out.stdout).unwrap_or_else(|e| panic!("write pki.tar: {e}"));
        let status = std::process::Command::new("tar")
            .arg("-xf")
            .arg(&tar)
            .arg("-C")
            .arg(dir.path())
            .status()
            .unwrap_or_else(|e| panic!("tar could not run: {e}"));
        assert!(status.success(), "tar could not unpack the certificates");
        std::fs::remove_file(&tar).unwrap_or_else(|e| panic!("remove pki.tar: {e}"));
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
    /// `test` names the owning test in the container name.
    ///
    /// # Errors
    ///
    /// The docker CLI's complaint when an image will not pull, the files will
    /// not copy in, or the broker does not come up.
    pub async fn start(test: &str) -> Result<Self, String> {
        let pki = Pki::generate().await;
        let controller = free_port();
        let (tls, sasl_ssl, mtls) = (free_port(), free_port(), free_port());

        let scratch = tempfile::tempdir().map_err(|e| format!("temp dir: {e}"))?;
        for name in ["ca.crt", "server.pem"] {
            std::fs::copy(pki.dir().join(name), scratch.path().join(name))
                .map_err(|e| format!("stage {name}: {e}"))?;
        }
        write(
            &scratch.path().join("server.properties"),
            &server_properties(controller, tls, sasl_ssl, mtls),
        )?;
        write(&scratch.path().join("start.sh"), &start_script())?;
        // The broker runs as its image's user, whose uid need not match this process's.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(scratch.path(), std::fs::Permissions::from_mode(0o755))
                .map_err(|e| format!("chmod scratch dir: {e}"))?;
        }

        let name = claim_container_name(&container_name(test, "kafka"));
        let mut create: Vec<String> = [
            "create",
            "--name",
            &name,
            "--network",
            "host",
            "--env",
            "KAFKA_HEAP_OPTS=-Xms256m -Xmx768m",
            "--label",
            &format!("{}={}", SUITE_LABEL.0, SUITE_LABEL.1),
            "--label",
            "io.hyperi.test.repo=scalo-rs",
            "--label",
            "io.hyperi.test.service=kafka",
            "--label",
            &format!("io.hyperi.test.owner-pid={}", std::process::id()),
            "--entrypoint",
            "sh",
            KAFKA_IMAGE,
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
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

fn write(path: &Path, text: &str) -> Result<(), String> {
    std::fs::write(path, text).map_err(|e| format!("write {}: {e}", path.display()))
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
