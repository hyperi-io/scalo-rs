// Project:   scalo
// File:      src/transport/routed.rs
// Purpose:   Per-key routing transport for data originators (receiver, fetcher)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Per-key routing transport for data originators -- the NAMED SINK SET.
//!
//! Routes `send(key, payload)` to different transport backends based on the
//! key. Used by data-originator services (receiver, fetcher) where data-based
//! routing determines the destination (topic, endpoint, stream), and by any
//! stage whose sink list is config-driven (a transform sending on to the
//! loader, a matched record fanning out to loader AND archiver).
//!
//! # Config
//!
//! ```yaml
//! transport:
//!   output:
//!     type: routed
//!     default: kafka
//!     routes:
//!       events.land:
//!         type: grpc
//!         grpc:
//!           endpoint: "http://loader-land:6000"
//!       events.load:
//!         type: kafka
//!       audit.land:
//!         type: grpc
//!         grpc:
//!           endpoint: "http://archiver:6000"
//!     kafka:
//!       brokers: ["kafka:9092"]
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! let sender = RoutedSender::from_config("transport.output").await?;
//! // Routes to different backends based on key
//! sender.send("events.land", payload).await;  // -> gRPC to loader-land
//! sender.send("events.load", payload).await;  // -> Kafka topic
//! sender.send("audit.land", payload).await;   // -> gRPC to archiver
//! sender.send("unknown", payload).await;      // -> default (Kafka)
//! ```
//!
//! # Named destinations, and the wire key
//!
//! [`send`](TransportSender::send) uses ONE string for both the route lookup
//! and the backend's wire destination, which fits a routing table keyed by
//! topic. When the destination NAME is not the wire key -- a destination
//! called `loader` whose Kafka topic is `<source>_land`, computed per record
//! --  use [`send_to`](RoutedSender::send_to), which takes the two separately,
//! or [`send_fanout`](RoutedSender::send_fanout) to reach a LIST of named
//! destinations with one payload.
//!
//! ```rust,ignore
//! sender.send_to("loader", "orders_land", payload).await;
//! sender.send_fanout(&["loader", "archiver"], "orders_land", payload).await;
//! ```
//!
//! # Backpressure
//!
//! A routed send NEVER retries and NEVER routes to a DLQ: it returns the
//! chosen backend's [`SendResult`] unchanged, so the caller applies its own
//! policy (the fetcher holds the batch and stalls its scheduler; the receiver
//! back-pressures its ingest). Bounded retry with backoff is
//! [`SinkStack`](crate::sink_stack::SinkStack)'s job and composes on top --
//! `RoutedSender` implements [`TransportSender`], so a stack wraps it.

use std::collections::HashMap;

use super::error::{TransportError, TransportResult};
use super::factory::AnySender;
use super::traits::{TransportBase, TransportSender};
use super::types::SendResult;

/// A routing transport that dispatches `send()` to different backends
/// based on the key parameter.
///
/// Used by data-originator services (receiver, fetcher) where
/// data-based routing determines the destination.
pub struct RoutedSender {
    /// Per-key route overrides.
    routes: HashMap<String, AnySender>,
    /// Default sender for keys not in the routes map.
    default: Option<AnySender>,
    closed: std::sync::atomic::AtomicBool,
}

impl RoutedSender {
    /// Create a new routed sender with explicit routes and optional default.
    #[must_use]
    pub fn new(routes: HashMap<String, AnySender>, default: Option<AnySender>) -> Self {
        Self {
            routes,
            default,
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create from a map of key -> `TransportConfig` plus a default config.
    ///
    /// Each route gets its own `AnySender` created from the corresponding config.
    pub async fn from_route_configs(
        routes: HashMap<String, super::TransportConfig>,
        default_config: Option<super::TransportConfig>,
    ) -> TransportResult<Self> {
        let mut senders = HashMap::with_capacity(routes.len());
        for (key, config) in routes {
            let sender = AnySender::from_transport_config(&config).await?;
            senders.insert(key, sender);
        }

        let default = match default_config {
            Some(cfg) => Some(AnySender::from_transport_config(&cfg).await?),
            None => None,
        };

        Ok(Self::new(senders, default))
    }

    /// Get the list of configured route keys.
    #[must_use]
    pub fn route_keys(&self) -> Vec<&str> {
        self.routes.keys().map(String::as_str).collect()
    }

    /// Check if a specific route key is configured.
    #[must_use]
    pub fn has_route(&self, key: &str) -> bool {
        self.routes.contains_key(key)
    }

    /// Check if a default fallback sender is configured.
    #[must_use]
    pub fn has_default(&self) -> bool {
        self.default.is_some()
    }

    /// Per-destination health: the configured route name and whether its
    /// sender is currently healthy. The default sender, when configured,
    /// appears as `"default"`.
    #[must_use]
    pub fn destination_health(&self) -> Vec<(&str, bool)> {
        let mut out: Vec<(&str, bool)> = self
            .routes
            .iter()
            .map(|(name, sender)| (name.as_str(), sender.is_healthy()))
            .collect();
        if let Some(ref default) = self.default {
            out.push(("default", default.is_healthy()));
        }
        out
    }

    /// Whether the sender resolving `destination` is healthy. An unknown
    /// destination reports the default sender's health, or `false` when there
    /// is no default -- the same resolution [`send_to`](Self::send_to) uses.
    #[must_use]
    pub fn is_destination_healthy(&self, destination: &str) -> bool {
        self.resolve(destination)
            .is_some_and(|(_, sender)| sender.is_healthy())
    }

    /// Whether AT LEAST ONE configured sender is healthy. Readiness gates that
    /// must not stall a whole originator on one sick destination use this;
    /// [`is_healthy`](TransportBase::is_healthy) is the all-of form.
    #[must_use]
    pub fn any_healthy(&self) -> bool {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        self.routes.values().any(AnySender::is_healthy)
            || self.default.as_ref().is_some_and(AnySender::is_healthy)
    }

    /// Send to ONE named destination, with the wire key supplied separately.
    ///
    /// `destination` selects the route (falling back to the default);
    /// `key` is what the chosen backend receives as its destination -- the
    /// Kafka topic, the gRPC metadata routing key, the Redis stream. Use this
    /// wherever the destination NAME and the wire key differ; the bare
    /// [`send`](TransportSender::send) is this call with the two equal.
    pub async fn send_to(&self, destination: &str, key: &str, payload: bytes::Bytes) -> SendResult {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }

        let Some((route_name, sender)) = self.resolve(destination) else {
            return SendResult::Fatal(TransportError::Config(format!(
                "no route configured for destination '{destination}' and no default sender"
            )));
        };
        record_route_send(route_name, payload.len());
        sender.send(key, payload).await
    }

    /// Fan one payload out to EVERY named destination in `destinations`.
    ///
    /// Acknowledged only when every destination has accepted: the first
    /// `Backpressured`/`Fatal` short-circuits and is returned, so the caller
    /// retries the whole fan-out. That re-delivers to the destinations that
    /// already accepted -- at-least-once, duplicates never loss, the same
    /// contract as [`TransportSender::send_batch`]'s per-record fallback. An
    /// empty destination list is `Ok` (nothing to deliver).
    pub async fn send_fanout(
        &self,
        destinations: &[&str],
        key: &str,
        payload: bytes::Bytes,
    ) -> SendResult {
        for destination in destinations {
            // Bytes clone is a refcount bump, not a payload copy.
            match self.send_to(destination, key, payload.clone()).await {
                SendResult::Ok | SendResult::FilteredDlq => {}
                other => return other,
            }
        }
        SendResult::Ok
    }

    /// Resolve which route + sender handles a given key. Returns the
    /// configured route name (or `"default"` for the fallback) so
    /// metrics can label by route, not by per-message key (F7).
    fn resolve(&self, key: &str) -> Option<(&str, &AnySender)> {
        if let Some((name, sender)) = self.routes.get_key_value(key) {
            Some((name.as_str(), sender))
        } else {
            self.default.as_ref().map(|s| ("default", s))
        }
    }
}

impl TransportBase for RoutedSender {
    async fn close(&self) -> TransportResult<()> {
        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // Close all route senders
        for sender in self.routes.values() {
            sender.close().await?;
        }
        if let Some(ref default) = self.default {
            default.close().await?;
        }
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        // Healthy if all configured senders are healthy
        let routes_healthy = self.routes.values().all(|s| s.is_healthy());
        let default_healthy = self.default.as_ref().is_none_or(|s| s.is_healthy());
        routes_healthy && default_healthy
    }

    fn name(&self) -> &'static str {
        "routed"
    }
}

/// Count one routed send. The route label is the CONFIGURED route name (or
/// `"default"`), never the per-message key: cardinality is bounded by the
/// routing table size, not by message count.
fn record_route_send(route_name: &str, payload_len: usize) {
    #[cfg(feature = "metrics")]
    {
        metrics::counter!(
            "transport_sent_total",
            "transport" => "routed",
            "route" => route_name.to_string()
        )
        .increment(1);
        metrics::counter!(
            "transport_sent_bytes_total",
            "transport" => "routed",
            "route" => route_name.to_string()
        )
        .increment(payload_len as u64);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (route_name, payload_len);
}

impl TransportSender for RoutedSender {
    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        self.send_to(destination, destination, payload).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "transport-memory")]
    use crate::transport::memory::{MemoryConfig, MemoryTransport};

    #[cfg(feature = "transport-memory")]
    fn make_memory_sender() -> AnySender {
        AnySender::Memory(
            MemoryTransport::new(&MemoryConfig::default())
                .expect("memory transport with valid config must construct"),
        )
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn routes_to_correct_sender() {
        let mut route_map = HashMap::new();
        route_map.insert("events.land".into(), make_memory_sender());
        route_map.insert("events.load".into(), make_memory_sender());

        let sender = RoutedSender::new(route_map, Some(make_memory_sender()));

        let result_land = sender
            .send("events.land", bytes::Bytes::from_static(b"land-payload"))
            .await;
        assert!(result_land.is_ok());

        let result_load = sender
            .send("events.load", bytes::Bytes::from_static(b"load-payload"))
            .await;
        assert!(result_load.is_ok());

        // Unknown key falls through to default
        let result_default = sender
            .send("unknown.key", bytes::Bytes::from_static(b"default-payload"))
            .await;
        assert!(result_default.is_ok());

        assert!(sender.is_healthy());
        assert_eq!(sender.name(), "routed");
    }

    #[tokio::test]
    async fn no_route_no_default_returns_fatal() {
        let sender = RoutedSender::new(HashMap::new(), None);

        let result = sender
            .send("unknown", bytes::Bytes::from_static(b"payload"))
            .await;
        assert!(result.is_fatal());
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn close_propagates_to_all_senders() {
        let mut route_map = HashMap::new();
        route_map.insert("a".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, Some(make_memory_sender()));

        assert!(sender.is_healthy());
        sender.close().await.unwrap();
        assert!(!sender.is_healthy());
    }

    #[test]
    fn route_keys_and_has_route() {
        let sender = RoutedSender::new(HashMap::new(), None);
        assert!(sender.route_keys().is_empty());
        assert!(!sender.has_route("anything"));
        assert!(!sender.has_default());
    }

    #[tokio::test]
    async fn send_after_close_returns_fatal() {
        let sender = RoutedSender::new(HashMap::new(), None);
        sender.close().await.unwrap();

        let result = sender
            .send("key", bytes::Bytes::from_static(b"payload"))
            .await;
        assert!(result.is_fatal());
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn send_to_routes_by_name_and_carries_the_wire_key() {
        let mut route_map = HashMap::new();
        route_map.insert("loader".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, None);

        // Name and wire key differ: the name resolves the route, the key
        // reaches the backend.
        let result = sender
            .send_to("loader", "orders_land", bytes::Bytes::from_static(b"p"))
            .await;
        assert!(result.is_ok());

        // An unknown name with no default is still fatal.
        let result = sender
            .send_to("nowhere", "orders_land", bytes::Bytes::from_static(b"p"))
            .await;
        assert!(result.is_fatal());
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn fanout_delivers_to_every_destination() {
        let mut route_map = HashMap::new();
        route_map.insert("loader".into(), make_memory_sender());
        route_map.insert("archiver".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, None);

        let result = sender
            .send_fanout(
                &["loader", "archiver"],
                "orders_land",
                bytes::Bytes::from_static(b"p"),
            )
            .await;
        assert!(result.is_ok());

        // Empty list is a no-op, not an error.
        assert!(
            sender
                .send_fanout(&[], "orders_land", bytes::Bytes::from_static(b"p"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn fanout_short_circuits_on_an_unroutable_destination() {
        let mut route_map = HashMap::new();
        route_map.insert("loader".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, None);

        let result = sender
            .send_fanout(
                &["loader", "missing"],
                "orders_land",
                bytes::Bytes::from_static(b"p"),
            )
            .await;
        assert!(
            result.is_fatal(),
            "a fan-out is acknowledged only when EVERY destination accepts"
        );
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn destination_health_reports_each_route_and_the_default() {
        let mut route_map = HashMap::new();
        route_map.insert("loader".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, Some(make_memory_sender()));

        let mut health = sender.destination_health();
        health.sort_unstable();
        assert_eq!(health, vec![("default", true), ("loader", true)]);
        assert!(sender.is_destination_healthy("loader"));
        // Unknown name resolves through the default.
        assert!(sender.is_destination_healthy("anything-else"));
        assert!(sender.any_healthy());

        sender.close().await.unwrap();
        assert!(!sender.any_healthy(), "closed set is healthy nowhere");
    }

    #[test]
    fn any_healthy_is_false_with_no_senders() {
        let sender = RoutedSender::new(HashMap::new(), None);
        assert!(!sender.any_healthy());
        assert!(!sender.is_destination_healthy("loader"));
    }

    /// Regression: `resolve` returns the configured route
    /// name (or `"default"`), not the per-message key. Metric labels
    /// stay bounded by the routing table size, not by message count.
    #[test]
    #[cfg(feature = "transport-memory")]
    fn resolve_returns_route_name_not_message_key() {
        let mut route_map = HashMap::new();
        route_map.insert("events.land".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, Some(make_memory_sender()));

        // Match: route name equals the configured key.
        let (name, _) = sender.resolve("events.land").unwrap();
        assert_eq!(name, "events.land");

        // Miss: falls through to "default" -- bounded label, not the
        // arbitrary inbound key.
        let (name, _) = sender.resolve("arbitrary-user-key-12345").unwrap();
        assert_eq!(name, "default");
    }
}
