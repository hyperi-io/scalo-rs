// Project:   scalo
// File:      src/cli/test_http.rs
// Purpose:   Test helpers that reach the runtime's metrics listener over real HTTP
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Test helpers that reach the runtime's metrics listener over real HTTP.

/// A free loopback address that no other test in this run is given.
///
/// Ports come from 20000-29999, below the ephemeral range on Linux and macOS,
/// so a `bind` to port 0 elsewhere can't take one before the server binds it.
/// A `create_new` marker file claims each port across nextest's processes.
pub(crate) fn free_local_addr() -> String {
    use std::hash::BuildHasher;

    const LOW: u16 = 20_000;
    const SPAN: u16 = 10_000;
    let run = std::env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| std::process::id().to_string());
    let claims = std::env::temp_dir().join(format!("scalo-test-ports-{run}"));
    std::fs::create_dir_all(&claims).expect("create the port claim directory");
    let start = std::hash::RandomState::new().hash_one(std::process::id()) % u64::from(SPAN);
    for step in 0..u64::from(SPAN) {
        let offset = u16::try_from((start + step) % u64::from(SPAN)).expect("below SPAN");
        let port = LOW + offset;
        let claimed = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(claims.join(port.to_string()))
            .is_ok();
        if claimed && std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return format!("127.0.0.1:{port}");
        }
    }
    panic!("no free loopback port in {LOW}-{}", LOW + SPAN - 1);
}

/// One HTTP/1.1 GET, returning the status code and the body.
pub(crate) async fn get(addr: &str, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to the metrics listener");
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send the request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read the response");

    let (head, body) = response.split_once("\r\n\r\n").expect("a head and a body");
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status code");
    (status, body.to_string())
}
