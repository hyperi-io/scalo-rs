// Project:   scalo
// File:      src/transport/vector_compat/source.rs
// Purpose:   Vector gRPC source compatibility wrapper
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector-compatible gRPC source.
//!
//! Implements the `vector.Vector` gRPC service so that legacy Vector sinks
//! can push events to a data-plane service. Incoming `EventWrapper` messages are
//! converted to JSON and fed into the same receive channel as native traffic.

use super::convert::event_wrapper_to_json;
use super::proto::vector;
use crate::transport::grpc::{GrpcToken, receiver_closed};
use crate::transport::types::{Message, PayloadFormat};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

/// gRPC service that accepts `PushEvents` RPCs from Vector sinks.
///
/// Converts Vector's protobuf events to JSON and forwards them into
/// the transport's receive channel alongside native messages.
///
/// A request is queued whole or not at all: its events are converted first,
/// then room for all of them is reserved, waiting while the queue is full,
/// before any is queued. A receiver closed under the request refuses it with
/// `Unavailable`, which Vector retries, and none of its events were queued. A
/// request with more events than the receive queue holds cannot be reserved at
/// once and is queued event by event, so if the receiver closes part-way the
/// events already queued arrive again when Vector retries.
///
/// Queued events count in `transport_received_*{transport="grpc"}`. A
/// `GrpcTransport` built with a pressure governor refuses these requests with
/// `Unavailable` while the governor holds intake, as it refuses a native push.
pub struct VectorCompatService {
    sender: mpsc::Sender<Message<GrpcToken>>,
    sequence: Arc<AtomicU64>,
}

impl VectorCompatService {
    /// Create a new Vector compat service.
    ///
    /// Uses the same sender/sequence as the native transport server so
    /// both native and Vector-compat events arrive in the same channel.
    pub fn new(sender: mpsc::Sender<Message<GrpcToken>>, sequence: Arc<AtomicU64>) -> Self {
        Self { sender, sequence }
    }

    /// Wrap one converted event for the receive queue, taking the next sequence.
    fn message(&self, payload: bytes::Bytes) -> Message<GrpcToken> {
        Message {
            key: None, // Vector events don't carry a topic key
            payload,
            token: GrpcToken::new(self.sequence.fetch_add(1, Ordering::Relaxed)),
            timestamp_ms: None,
            format: PayloadFormat::Json,
        }
    }
}

#[tonic::async_trait]
impl vector::vector_server::Vector for VectorCompatService {
    async fn push_events(
        &self,
        request: Request<vector::PushEventsRequest>,
    ) -> Result<Response<vector::PushEventsResponse>, Status> {
        // Shed before doing any work while the server's governor holds intake.
        #[cfg(feature = "governor")]
        crate::transport::grpc::shed_if_held(
            request
                .extensions()
                .get::<crate::transport::grpc::InboundGate>()
                .map(|gate| &gate.0),
        )?;

        let req = request.into_inner();

        // Convert every event before queueing any, so a failure queues nothing.
        let mut payloads = Vec::with_capacity(req.events.len());
        for event_wrapper in &req.events {
            // Convert Vector event to JSON (skip metrics)
            let Some(json_value) = event_wrapper_to_json(event_wrapper) else {
                continue;
            };
            let payload = serde_json::to_vec(&json_value)
                .map_err(|e| Status::internal(format!("json serialise failed: {e}")))?;
            payloads.push(bytes::Bytes::from(payload));
        }
        if payloads.is_empty() {
            return Ok(Response::new(vector::PushEventsResponse {}));
        }

        if payloads.len() > self.sender.max_capacity() {
            // Too many to reserve at once: queue as room frees up.
            for payload in payloads {
                #[cfg(feature = "metrics")]
                let bytes = payload.len();
                self.sender
                    .send(self.message(payload))
                    .await
                    .map_err(|_| receiver_closed())?;
                #[cfg(feature = "metrics")]
                crate::transport::grpc::count_received(1, bytes);
            }
        } else {
            #[cfg(feature = "metrics")]
            let (events, bytes) = (
                payloads.len() as u64,
                payloads.iter().map(bytes::Bytes::len).sum::<usize>(),
            );
            let permits = self
                .sender
                .reserve_many(payloads.len())
                .await
                .map_err(|_| receiver_closed())?;
            // Room is held for every event, so queueing cannot fail part-way.
            for (permit, payload) in permits.zip(payloads) {
                permit.send(self.message(payload));
            }
            #[cfg(feature = "metrics")]
            crate::transport::grpc::count_received(events, bytes);
        }

        Ok(Response::new(vector::PushEventsResponse {}))
    }

    async fn health_check(
        &self,
        _request: Request<vector::HealthCheckRequest>,
    ) -> Result<Response<vector::HealthCheckResponse>, Status> {
        Ok(Response::new(vector::HealthCheckResponse {
            status: vector::ServingStatus::Serving.into(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::convert::json_to_event_wrapper;
    use super::*;
    use crate::transport::vector_compat::proto::vector::vector_server::Vector as _;

    /// A `PushEvents` request carrying `n` log events.
    fn push_request(n: usize) -> Request<vector::PushEventsRequest> {
        let events = (0..n)
            .map(|seq| json_to_event_wrapper(&serde_json::json!({ "seq": seq })))
            .collect();
        Request::new(vector::PushEventsRequest { events })
    }

    /// A message already in the receive queue ahead of the request.
    fn queued(seq: u64) -> Message<GrpcToken> {
        Message {
            key: None,
            payload: bytes::Bytes::from_static(b"{}"),
            token: GrpcToken::new(seq),
            timestamp_ms: None,
            format: PayloadFormat::Json,
        }
    }

    /// A request the receiver closes under while it waits for buffer space is
    /// refused whole, naming the close, with none of its events queued -- so
    /// Vector's retry of the whole request duplicates nothing.
    #[tokio::test]
    async fn a_request_the_receiver_closes_under_queues_none_of_its_events() {
        let (tx, mut rx) = mpsc::channel(4);
        // Two slots taken, so a three-event request cannot fit yet.
        for seq in 0..2 {
            tx.try_send(queued(seq)).expect("room for the queued pair");
        }
        let service = Arc::new(VectorCompatService::new(tx, Arc::new(AtomicU64::new(0))));
        let pushing = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.push_events(push_request(3)).await }
        });
        // Let the push take what room there is and wait on the rest.
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }

        rx.close();
        let refused = pushing
            .await
            .expect("push task")
            .expect_err("a closed receiver refuses the push");
        let mut in_queue = 0;
        while rx.recv().await.is_some() {
            in_queue += 1;
        }

        assert_eq!(refused.code(), tonic::Code::Unavailable);
        assert_eq!(refused.message(), "receiver closed");
        assert_eq!(
            in_queue, 2,
            "only the two messages queued before the request; none of its three events"
        );
    }

    /// A request larger than the whole receive buffer still gets through,
    /// queued as room frees up.
    #[tokio::test]
    async fn a_request_larger_than_the_buffer_is_delivered() {
        let (tx, mut rx) = mpsc::channel(2);
        let service = Arc::new(VectorCompatService::new(tx, Arc::new(AtomicU64::new(0))));
        let pushing = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.push_events(push_request(5)).await }
        });

        let mut received = 0;
        while received < 5 {
            let message = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("the push keeps queueing")
                .expect("queue open");
            assert_eq!(message.format, PayloadFormat::Json);
            received += 1;
        }

        assert!(pushing.await.expect("push task").is_ok());
    }

    /// With the governor holding intake, a `PushEvents` is refused with
    /// `Unavailable`, which Vector retries, and none of its events are queued.
    #[cfg(feature = "governor")]
    #[tokio::test]
    async fn a_push_while_the_governor_holds_is_refused_and_queues_nothing() {
        use crate::governor::{Hysteresis, MemoryPressureSource, PressureSource, UnifiedPressure};
        use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
        use crate::transport::error::TransportError;
        use crate::transport::grpc::{GrpcConfig, GrpcTransport};
        use crate::transport::vector_compat::VectorCompatClient;
        use crate::transport::{TransportBase, TransportReceiver};

        // Pinned to the reservation counter so 950/1000 is the ratio, not the
        // host's own memory usage.
        let guard = Arc::new(MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: 1000,
                pressure_threshold: 0.80,
                ..Default::default()
            },
            UsageSource::Reservations,
        ));
        guard.add_bytes(950);
        let pressure = Arc::new(UnifiedPressure::new(
            vec![Arc::new(MemoryPressureSource::new(Arc::clone(&guard))) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("valid band"),
        ));
        assert!(pressure.should_hold(), "pinned-high governor must hold");

        let server = GrpcTransport::with_pressure(
            &GrpcConfig::server("127.0.0.1:0").with_vector_compat(),
            Some(pressure),
        )
        .await
        .expect("Vector-compat server");
        let addr = server.local_addr().expect("server bound");
        let client = VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("client");

        let pushed = client.send_events(&[serde_json::json!({ "seq": 1 })]).await;
        assert!(
            matches!(pushed, Err(TransportError::Send(ref message)) if message.contains("under pressure")),
            "a push under pressure must be refused as held, got {pushed:?}"
        );
        assert!(
            server.recv(10).await.expect("recv").records.is_empty(),
            "a refused push queues nothing"
        );
        let _ = server.close().await;
    }

    /// Events a `PushEvents` queues count as received, as a native push does.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn queued_events_count_as_received() {
        use crate::transport::grpc::{GrpcConfig, GrpcTransport};
        use crate::transport::vector_compat::VectorCompatClient;
        use crate::transport::{TransportBase, TransportReceiver};

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        // Current-thread runtime: the server's tasks run on this thread and see it.
        let _local = metrics::set_default_local_recorder(&recorder);

        let server = GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0").with_vector_compat())
            .await
            .expect("Vector-compat server");
        let addr = server.local_addr().expect("server bound");
        let client = VectorCompatClient::connect_lazy(&format!("http://{addr}")).expect("client");
        let events: Vec<_> = (0..3)
            .map(|seq| serde_json::json!({ "seq": seq }))
            .collect();
        client.send_events(&events).await.expect("push");
        let records = server.recv(10).await.expect("recv").records;
        assert_eq!(records.len(), 3);
        let bytes: usize = records.iter().map(|r| r.payload.len()).sum();

        let rendered = handle.render();
        let value = |name: &str| -> Option<f64> {
            let series = format!("{name}{{transport=\"grpc\"}} ");
            rendered
                .lines()
                .find_map(|line| line.strip_prefix(series.as_str())?.parse().ok())
        };
        assert_eq!(
            value("transport_received_events_total"),
            Some(3.0),
            "three events were queued:\n{rendered}"
        );
        assert_eq!(
            value("transport_received_bytes_total"),
            Some(f64::from(u32::try_from(bytes).expect("small"))),
            "the queued payload bytes:\n{rendered}"
        );
        let _ = server.close().await;
    }
}
