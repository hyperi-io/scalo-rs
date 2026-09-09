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
//! # Blocks
//!
//! [`send_batch`](TransportSender::send_batch) groups a block by the route each
//! record's key resolves to and sends each group in ONE call, so a routed block
//! keeps the backend's native batch instead of degrading to one send per
//! record. [`send_batch_fanout`](RoutedSender::send_batch_fanout) is the batch
//! form of `send_fanout`.
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
use super::work_batch::Record;

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
            return unroutable(destination);
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

    /// Fan a whole BLOCK out to every named destination in `destinations`.
    ///
    /// The batch form of [`send_fanout`](Self::send_fanout): each destination
    /// receives the WHOLE block in ONE
    /// [`send_batch`](TransportSender::send_batch) call, with each record's own
    /// `key` as the wire destination. Acknowledged only when every destination
    /// has accepted; the first `Backpressured`/`Fatal` short-circuits and is
    /// returned, so the caller retries the whole fan-out (at-least-once --
    /// duplicates, never loss). An empty destination list or an empty block is
    /// `Ok`.
    pub async fn send_batch_fanout(&self, destinations: &[&str], records: &[Record]) -> SendResult {
        if destinations.is_empty() || records.is_empty() {
            return SendResult::Ok;
        }
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }

        for destination in destinations {
            let Some((route_name, sender)) = self.resolve(destination) else {
                return unroutable(destination);
            };
            for record in records {
                record_route_send(route_name, record.payload.len());
            }
            match sender.send_batch(records).await {
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

/// The `Fatal` a destination with no route and no default produces.
fn unroutable(destination: &str) -> SendResult {
    SendResult::Fatal(TransportError::Config(format!(
        "no route configured for destination '{destination}' and no default sender"
    )))
}

/// Send a block by destination GROUP: one `send_batch` per resolved route,
/// never one send per record.
///
/// `resolve` maps a record's wire key to `(route name, sender)` -- the same
/// resolution [`RoutedSender::send`] uses, passed in so the grouping is
/// testable against any sender.
///
/// Every record is resolved BEFORE anything reaches a sender: an unroutable
/// record fails the whole block with nothing sent, so a retry cannot re-deliver
/// a prefix that the deterministic routing will reject again. Groups are then
/// sent in first-appearance order and the first `Backpressured`/`Fatal`
/// short-circuits, leaving the later groups unsent for the caller's retry --
/// the trait's own short-circuit contract, at group granularity instead of
/// record granularity. A block that resolves to ONE route is handed to that
/// sender as the caller's own slice, with no per-group copy.
async fn send_grouped<'a, S, F>(records: &[Record], resolve: F) -> SendResult
where
    S: TransportSender + 'a,
    F: Fn(&str) -> Option<(&'a str, &'a S)>,
{
    // Grouping is by SENDER identity, not by route name: an unknown key and a
    // route literally named `default` share the label but are different sinks.
    let mut one_route: Option<(&'a str, &'a S)> = None;
    let mut mixed = false;
    for record in records {
        let destination = record.key.as_deref().unwrap_or("");
        let Some(resolved) = resolve(destination) else {
            return unroutable(destination);
        };
        match one_route {
            None => one_route = Some(resolved),
            Some((_, sender)) => mixed |= !std::ptr::eq(sender, resolved.1),
        }
    }

    let Some((first_route, first_sender)) = one_route else {
        return SendResult::Ok; // empty block
    };
    if !mixed {
        for record in records {
            record_route_send(first_route, record.payload.len());
        }
        // A whole block the sink filtered to DLQ is HANDLED, not failed --
        // the same normalisation the multi-group loop below performs.
        return match first_sender.send_batch(records).await {
            SendResult::FilteredDlq => SendResult::Ok,
            other => other,
        };
    }

    // Mixed block: one Vec per sink, input order preserved within each group.
    // Record::clone bumps the payload refcount rather than copying it.
    let mut groups: Vec<(&'a str, &'a S, Vec<Record>)> = Vec::new();
    for record in records {
        let destination = record.key.as_deref().unwrap_or("");
        let Some((route_name, sender)) = resolve(destination) else {
            return unroutable(destination);
        };
        if let Some((_, _, group)) = groups.iter_mut().find(|(_, s, _)| std::ptr::eq(*s, sender)) {
            group.push(record.clone());
        } else {
            groups.push((route_name, sender, vec![record.clone()]));
        }
    }

    for (route_name, sender, group) in groups {
        for record in &group {
            record_route_send(route_name, record.payload.len());
        }
        match sender.send_batch(&group).await {
            SendResult::Ok | SendResult::FilteredDlq => {}
            other => return other,
        }
    }
    SendResult::Ok
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

    /// Send a block by destination GROUP -- one underlying `send_batch` per
    /// resolved route.
    ///
    /// Without this override the trait's default would degrade a routed block
    /// to one `send` per record, throwing away every backend's native batch RPC
    /// (gRPC's single `RouteBatch`) and its all-or-nothing acceptance. Records
    /// are grouped by the route their `key` resolves to -- the same resolution
    /// [`send`](TransportSender::send) uses -- and each group is handed to its
    /// sender in ONE call, keeping input order within the group.
    ///
    /// The result is the trait's: `Ok` once every group is accepted, otherwise
    /// the first `Backpressured`/`Fatal` in first-appearance order of the
    /// groups, which is the failure belonging to the earliest record that could
    /// have produced one. Groups after it are NOT sent, so the caller retries
    /// the whole block (at-least-once -- duplicates, never loss). An empty block
    /// is `Ok`; a block holding an unroutable record is `Fatal` with nothing
    /// sent at all.
    async fn send_batch(&self, records: &[Record]) -> SendResult {
        if records.is_empty() {
            return SendResult::Ok;
        }
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }
        send_grouped(records, |destination| self.resolve(destination)).await
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

    // ---- send_batch: grouping, order, and short-circuit -------------------

    use super::super::types::PayloadFormat;
    use super::super::work_batch::RecordMeta;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A sender that records what reached it: one entry per `send_batch` call
    /// holding that call's payloads in order, plus a count of per-record `send`
    /// calls (which must stay 0 when the batch path is taken).
    #[derive(Default)]
    struct CountingSender {
        batches: Mutex<Vec<Vec<Vec<u8>>>>,
        sends: AtomicUsize,
        backpressure: bool,
    }

    impl CountingSender {
        fn backpressured() -> Self {
            Self {
                backpressure: true,
                ..Self::default()
            }
        }

        /// The payloads of each `send_batch` call, as UTF-8, in call order.
        fn calls(&self) -> Vec<Vec<String>> {
            self.batches
                .lock()
                .expect("test mutex is never poisoned")
                .iter()
                .map(|batch| {
                    batch
                        .iter()
                        .map(|p| String::from_utf8_lossy(p).into_owned())
                        .collect()
                })
                .collect()
        }
    }

    impl TransportBase for CountingSender {
        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "counting"
        }
    }

    impl TransportSender for CountingSender {
        async fn send(&self, _destination: &str, _payload: bytes::Bytes) -> SendResult {
            let _ = self.sends.fetch_add(1, Ordering::Relaxed);
            SendResult::Ok
        }

        async fn send_batch(&self, records: &[Record]) -> SendResult {
            self.batches
                .lock()
                .expect("test mutex is never poisoned")
                .push(records.iter().map(|r| r.payload.to_vec()).collect());
            if self.backpressure {
                SendResult::Backpressured
            } else {
                SendResult::Ok
            }
        }
    }

    fn rec(key: Option<&str>, payload: &'static [u8]) -> Record {
        Record {
            payload: bytes::Bytes::from_static(payload),
            key: key.map(std::sync::Arc::from),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }
    }

    /// The same resolution `RoutedSender::resolve` performs, over fakes.
    fn resolve_fake<'a>(
        routes: &'a HashMap<String, CountingSender>,
        default: Option<&'a CountingSender>,
        key: &str,
    ) -> Option<(&'a str, &'a CountingSender)> {
        routes.get_key_value(key).map_or_else(
            || default.map(|s| ("default", s)),
            |(name, sender)| Some((name.as_str(), sender)),
        )
    }

    #[tokio::test]
    async fn send_batch_reaches_each_destination_in_one_call() {
        let mut routes = HashMap::new();
        routes.insert("loader".to_string(), CountingSender::default());
        routes.insert("archiver".to_string(), CountingSender::default());

        // Interleaved so grouping, not input adjacency, does the work.
        let records = vec![
            rec(Some("loader"), b"l1"),
            rec(Some("archiver"), b"a1"),
            rec(Some("loader"), b"l2"),
            rec(Some("archiver"), b"a2"),
        ];

        let result = send_grouped(&records, |k| resolve_fake(&routes, None, k)).await;
        assert!(result.is_ok(), "every group accepted: {result:?}");

        let loader = &routes["loader"];
        let archiver = &routes["archiver"];
        assert_eq!(
            loader.calls(),
            vec![vec!["l1".to_string(), "l2".to_string()]],
            "one send_batch call carrying both loader records in input order"
        );
        assert_eq!(
            archiver.calls(),
            vec![vec!["a1".to_string(), "a2".to_string()]],
            "one send_batch call carrying both archiver records in input order"
        );
        assert_eq!(
            loader.sends.load(Ordering::Relaxed) + archiver.sends.load(Ordering::Relaxed),
            0,
            "the batch path must never degrade to per-record send"
        );
    }

    #[tokio::test]
    async fn send_batch_single_destination_sends_the_block_untouched() {
        let mut routes = HashMap::new();
        routes.insert("loader".to_string(), CountingSender::default());

        let records = vec![rec(Some("loader"), b"one"), rec(Some("loader"), b"two")];
        let result = send_grouped(&records, |k| resolve_fake(&routes, None, k)).await;

        assert!(result.is_ok());
        assert_eq!(
            routes["loader"].calls(),
            vec![vec!["one".to_string(), "two".to_string()]]
        );
    }

    #[tokio::test]
    async fn send_batch_backpressure_surfaces_for_that_group_only() {
        let mut routes = HashMap::new();
        routes.insert("loader".to_string(), CountingSender::default());
        routes.insert("archiver".to_string(), CountingSender::backpressured());
        routes.insert("audit".to_string(), CountingSender::default());

        // Group order is first appearance: loader, archiver, audit.
        let records = vec![
            rec(Some("loader"), b"l1"),
            rec(Some("archiver"), b"a1"),
            rec(Some("audit"), b"x1"),
            rec(Some("archiver"), b"a2"),
        ];

        let result = send_grouped(&records, |k| resolve_fake(&routes, None, k)).await;
        assert!(
            result.is_backpressured(),
            "the failing destination's result is the block's result: {result:?}"
        );
        assert_eq!(
            routes["loader"].calls(),
            vec![vec!["l1".to_string()]],
            "the group before the failure was sent"
        );
        assert_eq!(
            routes["archiver"].calls(),
            vec![vec!["a1".to_string(), "a2".to_string()]],
            "the backpressure covers exactly the failing destination's records"
        );
        assert!(
            routes["audit"].calls().is_empty(),
            "a group after the failure stays unsent for the caller's retry"
        );
    }

    #[tokio::test]
    async fn send_batch_unroutable_record_sends_nothing_at_all() {
        let mut routes = HashMap::new();
        routes.insert("loader".to_string(), CountingSender::default());

        // The routable record comes FIRST: the pre-flight resolve must still
        // stop it reaching the wire, so a retry cannot re-deliver a prefix.
        let records = vec![rec(Some("loader"), b"l1"), rec(Some("nowhere"), b"n1")];
        let result = send_grouped(&records, |k| resolve_fake(&routes, None, k)).await;

        assert!(result.is_fatal(), "unroutable block is fatal: {result:?}");
        assert!(
            routes["loader"].calls().is_empty(),
            "nothing is sent when any record in the block is unroutable"
        );
    }

    #[tokio::test]
    async fn send_batch_keyless_records_go_to_the_default() {
        let routes = HashMap::new();
        let default = CountingSender::default();

        let records = vec![rec(None, b"d1"), rec(Some("unknown"), b"d2")];
        let result = send_grouped(&records, |k| resolve_fake(&routes, Some(&default), k)).await;

        assert!(result.is_ok());
        assert_eq!(
            default.calls(),
            vec![vec!["d1".to_string(), "d2".to_string()]],
            "both fall through to the default as ONE group"
        );
    }

    /// A route literally NAMED `default` and the fallback share the metric
    /// label but are different sinks, so grouping must split them.
    #[tokio::test]
    async fn send_batch_splits_a_default_named_route_from_the_fallback() {
        let mut routes = HashMap::new();
        routes.insert("default".to_string(), CountingSender::default());
        let fallback = CountingSender::default();

        let records = vec![
            rec(Some("default"), b"named"),
            rec(Some("unknown"), b"fell"),
        ];
        let result = send_grouped(&records, |k| resolve_fake(&routes, Some(&fallback), k)).await;

        assert!(result.is_ok());
        assert_eq!(routes["default"].calls(), vec![vec!["named".to_string()]]);
        assert_eq!(fallback.calls(), vec![vec!["fell".to_string()]]);
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn send_batch_empty_block_is_a_no_op() {
        let mut route_map = HashMap::new();
        route_map.insert("loader".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, None);

        assert!(sender.send_batch(&[]).await.is_ok());

        // Still Ok after close -- an empty block never reaches a sender.
        sender.close().await.unwrap();
        assert!(sender.send_batch(&[]).await.is_ok());
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn send_batch_surfaces_the_backpressured_destination() {
        // A one-slot channel for "slow", so a single record fills it.
        let slow = AnySender::Memory(
            MemoryTransport::new(&MemoryConfig {
                buffer_size: 1,
                ..MemoryConfig::default()
            })
            .expect("memory transport with valid config must construct"),
        );
        let mut route_map = HashMap::new();
        route_map.insert("slow".to_string(), slow);
        route_map.insert("fast".to_string(), make_memory_sender());
        let sender = RoutedSender::new(route_map, None);

        // Fill "slow" so its next accept backpressures.
        assert!(
            sender
                .send("slow", bytes::Bytes::from_static(b"fill"))
                .await
                .is_ok()
        );

        let records = vec![rec(Some("fast"), b"f1"), rec(Some("slow"), b"s1")];
        assert!(
            sender.send_batch(&records).await.is_backpressured(),
            "the routed block carries the failing destination's result up"
        );
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn send_batch_after_close_is_fatal() {
        let mut route_map = HashMap::new();
        route_map.insert("loader".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, None);
        sender.close().await.unwrap();

        let records = vec![rec(Some("loader"), b"l1")];
        assert!(sender.send_batch(&records).await.is_fatal());
    }

    #[tokio::test]
    #[cfg(feature = "transport-memory")]
    async fn batch_fanout_delivers_the_whole_block_to_every_destination() {
        let mut route_map = HashMap::new();
        route_map.insert("loader".into(), make_memory_sender());
        route_map.insert("archiver".into(), make_memory_sender());
        let sender = RoutedSender::new(route_map, None);

        let records = vec![
            rec(Some("orders_land"), b"r1"),
            rec(Some("orders_land"), b"r2"),
        ];
        assert!(
            sender
                .send_batch_fanout(&["loader", "archiver"], &records)
                .await
                .is_ok()
        );

        // Empty list and empty block are both no-ops, not errors.
        assert!(sender.send_batch_fanout(&[], &records).await.is_ok());
        assert!(sender.send_batch_fanout(&["loader"], &[]).await.is_ok());

        // One unroutable name fails the whole fan-out.
        assert!(
            sender
                .send_batch_fanout(&["loader", "missing"], &records)
                .await
                .is_fatal()
        );
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
