// Project:   scalo
// File:      src/dlq/http.rs
// Purpose:   HTTP POST DLQ backend variant
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! HTTP backend variant for the DLQ enum.
//!
//! POSTs failed messages as NDJSON to a configured HTTP endpoint via
//! reqwest. The reqwest client is async-native so no `spawn_blocking`
//! is needed.

use serde::{Deserialize, Serialize};

use super::entry::DlqEntry;
use super::error::DlqError;
use crate::framing::{Framing, Ndjson};

/// Configuration for the HTTP DLQ backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct HttpDlqConfig {
    /// Enable the HTTP backend.
    pub enabled: bool,

    /// Endpoint URL to POST failed messages to.
    pub endpoint: String,

    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

impl Default for HttpDlqConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            timeout_secs: 30,
        }
    }
}

/// HTTP backend -- internal variant carried by [`super::DlqBackend::Http`].
#[derive(Debug)]
pub struct HttpDlqInner {
    client: reqwest::Client,
    endpoint: String,
}

impl HttpDlqInner {
    /// Build the HTTP backend.
    ///
    /// # Errors
    ///
    /// Returns an error if `endpoint` is empty or the reqwest client
    /// cannot be built.
    pub fn new(config: &HttpDlqConfig) -> Result<Self, DlqError> {
        if config.endpoint.is_empty() {
            return Err(DlqError::BackendError("HTTP DLQ endpoint is empty".into()));
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| DlqError::BackendError(format!("failed to build HTTP DLQ client: {e}")))?;

        Ok(Self {
            client,
            endpoint: config.endpoint.clone(),
        })
    }

    /// Send a batch as `application/x-ndjson`.
    pub async fn send_batch(&mut self, batch: &[DlqEntry]) -> Result<(), DlqError> {
        if batch.is_empty() {
            return Ok(());
        }

        let body = DlqEntry::ndjson(batch).map_err(|e| DlqError::Serialization(e.to_string()))?;

        let resp = self
            .client
            .post(&self.endpoint)
            .header("content-type", Ndjson.content_type())
            .body(body)
            .send()
            .await
            .map_err(|e| DlqError::BackendError(format!("HTTP DLQ batch send failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(DlqError::BackendError(format!(
                "HTTP DLQ endpoint returned {}",
                resp.status()
            )));
        }

        #[cfg(feature = "metrics")]
        metrics::counter!("dlq_sent_total", "backend" => "http").increment(batch.len() as u64);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The body is each entry's JSON followed by one newline, sent as NDJSON.
    #[tokio::test]
    async fn send_batch_posts_one_json_line_per_entry() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let body_at = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..n]);
                if let Some(at) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    break at + 4;
                }
            };
            let head = String::from_utf8_lossy(&request[..body_at]).to_ascii_lowercase();
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .and_then(|value| value.trim().parse().ok())
                .unwrap();
            while request.len() < body_at + length {
                let n = socket.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..n]);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            (head, request[body_at..].to_vec())
        });

        let mut backend = HttpDlqInner::new(&HttpDlqConfig {
            enabled: true,
            endpoint: format!("http://{address}/dlq"),
            timeout_secs: 5,
        })
        .unwrap();
        let entries = [
            DlqEntry::new("svc", "parse_error", b"a".to_vec()),
            DlqEntry::new("svc", "too_large", b"b\n".to_vec()),
        ];
        backend.send_batch(&entries).await.unwrap();

        let (head, body) = server.await.unwrap();
        let mut expected = Vec::new();
        for entry in &entries {
            expected.extend(serde_json::to_vec(entry).unwrap());
            expected.push(b'\n');
        }
        assert_eq!(body, expected);
        assert!(
            head.contains("content-type: application/x-ndjson"),
            "{head}"
        );
    }

    #[test]
    fn config_defaults() {
        let config = HttpDlqConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "");
        assert_eq!(config.timeout_secs, 30);
    }

    #[test]
    fn config_deserialise() {
        let json =
            r#"{"enabled":true,"endpoint":"http://dlq.example.com/ingest","timeout_secs":10}"#;
        let config: HttpDlqConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.endpoint, "http://dlq.example.com/ingest");
        assert_eq!(config.timeout_secs, 10);
    }

    #[test]
    fn rejects_empty_endpoint() {
        let config = HttpDlqConfig::default();
        assert!(HttpDlqInner::new(&config).is_err());
    }
}
