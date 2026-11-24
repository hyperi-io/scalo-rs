// Project:   scalo
// File:      tests/common/squid.rs
// Purpose:   A real Squid proxy in a container, set to refuse CONNECT
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A Squid proxy one test holds alone, for the transports' refused-CONNECT tests.

use std::time::Duration;

use scalo::deployment::test_support::shared_service::{self, OwnedContainer, Ready, ServiceSpec};

use super::SUITE;

/// Canonical's Squid, as `image:tag@digest`.
///
/// renovate: datasource=docker depName=ubuntu/squid
pub const SQUID_IMAGE: &str = "docker.io/ubuntu/squid:7.2-26.04_edge@sha256:42decc7f79b4e1044f4fe9b3a855af2cbfc76652f3191694726e21058443edc9";

/// The port Squid listens on inside the container. The image's supervisor
/// logs `Started default services` once Squid has stayed up for a second.
const SQUID_PORT: u16 = 3128;

/// Where Squid reads its config and password file.
const CONF_PATH: &str = "/etc/squid/squid.conf";
const PASSWD_PATH: &str = "/etc/squid/passwd";

/// The only port `PortOnly` opens a tunnel to.
pub const ALLOWED_PORT: u16 = 443;

/// Why the proxy refuses a CONNECT.
#[derive(Debug, Clone, Copy)]
pub enum Refusal {
    /// A tunnel to port [`ALLOWED_PORT`] only, so any other port is answered 403.
    PortOnly,
    /// Basic auth with no user in the password file, so every CONNECT is answered 407.
    AuthRequired,
}

impl Refusal {
    /// The status Squid answers a CONNECT to port 8443 with.
    pub fn status(self) -> u16 {
        match self {
            Self::PortOnly => 403,
            Self::AuthRequired => 407,
        }
    }
}

const PORT_ONLY_CONF: &str = "\
http_port 3128
acl tunnel method CONNECT
acl tunnel_port port 443
http_access allow tunnel tunnel_port
http_access deny all
";

const AUTH_REQUIRED_CONF: &str = "\
http_port 3128
auth_param basic program /usr/lib/squid/basic_ncsa_auth /etc/squid/passwd
auth_param basic realm scalo-test
acl known proxy_auth REQUIRED
http_access allow known
http_access deny all
";

/// A running Squid, removed when this drops or the test process exits.
pub struct Squid {
    _container: OwnedContainer,
    /// The proxy URL, `http://host:port`.
    pub url: String,
}

impl Squid {
    /// Start Squid for `test`, refusing every CONNECT the way `refusal` says.
    pub async fn start(test: &str, refusal: Refusal) -> Self {
        let conf: fn(&str) -> Vec<(String, Vec<u8>)> = match refusal {
            Refusal::PortOnly => |_| vec![(CONF_PATH.into(), PORT_ONLY_CONF.into())],
            Refusal::AuthRequired => |_| {
                vec![
                    (CONF_PATH.into(), AUTH_REQUIRED_CONF.into()),
                    (PASSWD_PATH.into(), Vec::new()),
                ]
            },
        };
        let spec = ServiceSpec {
            ports: vec![SQUID_PORT],
            files: conf,
            ready: Ready::Log("Started default services"),
            ..ServiceSpec::new("squid", SQUID_IMAGE, Duration::from_secs(120))
        };
        let name = shared_service::container_name(&SUITE, Some(test), "squid");
        let container =
            tokio::task::spawn_blocking(move || shared_service::start_alone(&SUITE, &spec, &name))
                .await
                .expect("start a Squid of the test's own");
        let url = format!("http://{}", container.container.addr(SQUID_PORT));
        Self {
            _container: container,
            url,
        }
    }
}
