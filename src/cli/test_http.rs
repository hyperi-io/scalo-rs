// Project:   scalo
// File:      src/cli/test_http.rs
// Purpose:   Test helpers that reach the runtime's metrics listener over real HTTP
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Test helpers that reach the runtime's metrics listener over real HTTP.

/// A loopback address that was free a moment ago.
pub(crate) fn free_local_addr() -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    probe.local_addr().expect("the bound address").to_string()
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
