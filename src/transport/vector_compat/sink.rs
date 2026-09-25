// Project:   scalo
// File:      src/transport/vector_compat/sink.rs
// Purpose:   Vector gRPC sink compatibility wrapper
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Vector-compatible gRPC sink.
//!
//! Sends events to a Vector source via the `vector.Vector/PushEvents` RPC.
//! Use this to push events to a downstream Vector pipeline.

use std::time::Duration;

use super::convert::json_to_event_wrapper;
use super::proto::vector;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::grpc::{GrpcConfig, lazy_channel};

/// Client that sends events to a Vector source via gRPC.
///
/// Converts JSON values to Vector's protobuf `EventWrapper` format
/// and sends them via `PushEvents`.
///
/// Bounded by the gRPC transport's default `send_timeout_ms` (30 s), or the
/// limit given to [`connect_lazy_within`](Self::connect_lazy_within): a dial
/// whose DNS lookup or TCP connect is unfinished at nine tenths of it is
/// abandoned, so the call that started it fails and the next dials afresh, and
/// `health_check` gives up at it. `send_events` has no limit once connected: a
/// source that acknowledges end to end holds the RPC until its own sink
/// delivers, and cutting that off would push the same events again while the
/// first push may still land. A connection that has read nothing for 30 s is
/// sent an HTTP/2 PING and closed if the PING goes unanswered for 30 s more,
/// so a send to a peer that stops answering ends with an error and the next
/// dials afresh.
pub struct VectorCompatClient {
    client: vector::vector_client::VectorClient<tonic::transport::Channel>,

    /// Dial and health-check limit (milliseconds, 0 = none).
    send_timeout_ms: u64,
}

impl VectorCompatClient {
    /// Connect to a Vector source endpoint.
    ///
    /// Uses lazy connection -- the actual TCP connection is established
    /// on the first RPC call.
    ///
    /// # Errors
    ///
    /// Returns error if the endpoint URI is invalid.
    pub fn connect_lazy(endpoint: &str) -> TransportResult<Self> {
        Self::connect_lazy_within(endpoint, GrpcConfig::default().send_timeout_ms)
    }

    /// [`connect_lazy`](Self::connect_lazy) with the dial and health-check limit
    /// set to `send_timeout_ms` (0 = none).
    ///
    /// A caller holding its own source's answer sets this below that hold, so
    /// a stalled dial fails while the source can still answer its sender.
    ///
    /// # Errors
    ///
    /// Returns error if the endpoint URI is invalid.
    pub fn connect_lazy_within(endpoint: &str, send_timeout_ms: u64) -> TransportResult<Self> {
        // Bound the response decode size. `usize::MAX` contradicts the never-OOM
        // doctrine -- even a (trusted) Vector server response should not be able
        // to drive an unbounded allocation. 64 MiB is far above any real ack
        // response and matches the generous end of the transport size envelope.
        const MAX_DECODE_BYTES: usize = 64 * 1024 * 1024;

        let ep = tonic::transport::Channel::from_shared(endpoint.to_string())
            .map_err(|e| TransportError::Config(format!("invalid Vector endpoint: {e}")))?;
        let channel = lazy_channel(ep, send_timeout_ms);

        let client = vector::vector_client::VectorClient::new(channel)
            .max_decoding_message_size(MAX_DECODE_BYTES)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip);

        Ok(Self {
            client,
            send_timeout_ms,
        })
    }

    /// Send JSON values as Vector log events.
    ///
    /// Each JSON value is wrapped as a Vector `Log` event inside an `EventWrapper`.
    /// Waits as long as the source holds the RPC and still answers HTTP/2
    /// PINGs; only the dial is limited.
    ///
    /// # Errors
    ///
    /// Returns error if the gRPC call fails, including a dial abandoned at the
    /// dial limit and a connection closed for an unanswered PING. The error
    /// does not say whether a resend can succeed: a caller that needs to know
    /// uses [`send_events_status`](Self::send_events_status).
    pub async fn send_events(&self, values: &[serde_json::Value]) -> TransportResult<()> {
        self.send_events_status(values)
            .await
            .map_err(|e| TransportError::Send(format!("Vector PushEvents failed: {e}")))
    }

    /// [`send_events`](Self::send_events), failing with the gRPC status, so a
    /// caller can tell a refusal no resend clears from a failure that can
    /// clear, with [`is_permanent_rejection`](Self::is_permanent_rejection).
    ///
    /// # Errors
    ///
    /// The status the source answered with, or the client's own failure (a
    /// dial abandoned at the dial limit, a connection closed for an unanswered
    /// PING) as a status carrying its source error.
    pub async fn send_events_status(
        &self,
        values: &[serde_json::Value],
    ) -> Result<(), tonic::Status> {
        let events: Vec<_> = values.iter().map(json_to_event_wrapper).collect();
        self.client
            .clone()
            .push_events(vector::PushEventsRequest { events })
            .await?;
        Ok(())
    }

    /// Whether a failed push is a refusal that no resend of the same events
    /// can clear.
    ///
    /// Permanent:
    ///
    /// - `DataLoss`: Vector's `vector` source answers it when a sink it feeds
    ///   rejected the events.
    /// - `InvalidArgument`: the source cannot use the request.
    /// - `OutOfRange`: the request is over the source's message-size limit.
    ///
    /// Every other code can clear, so the caller holds the events and sends
    /// them again: `Unavailable` and `ResourceExhausted` (the source is down,
    /// busy or shutting down), `DeadlineExceeded` and `Cancelled` (the send
    /// ran out of time, and the source may still take it), and the codes that
    /// name a configuration fault rather than these events (`Unimplemented`,
    /// `PermissionDenied`, `Unauthenticated`), whose events are lost if they
    /// are dropped. A status carrying a source error is the client's own
    /// connection failing, never a refusal.
    #[must_use]
    pub fn is_permanent_rejection(status: &tonic::Status) -> bool {
        std::error::Error::source(status).is_none()
            && matches!(
                status.code(),
                tonic::Code::DataLoss | tonic::Code::InvalidArgument | tonic::Code::OutOfRange
            )
    }

    /// Check if the remote Vector source is healthy.
    ///
    /// A short probe, so it gives up at the send limit, dial included; a
    /// source that does not answer by then is not healthy.
    ///
    /// # Errors
    ///
    /// Returns error if the health check RPC fails, or gets no answer within
    /// the send limit.
    pub async fn health_check(&self) -> TransportResult<bool> {
        let mut request = tonic::Request::new(vector::HealthCheckRequest {});
        // The grpc-timeout header tells the server the deadline.
        if self.send_timeout_ms > 0 {
            request.set_timeout(Duration::from_millis(self.send_timeout_ms));
        }
        let mut client = self.client.clone();
        let probe = client.health_check(request);

        let answered = if self.send_timeout_ms == 0 {
            probe.await
        } else {
            tokio::time::timeout(Duration::from_millis(self.send_timeout_ms), probe)
                .await
                .unwrap_or_else(|_elapsed| {
                    Err(tonic::Status::deadline_exceeded(format!(
                        "no answer within send_timeout_ms ({} ms)",
                        self.send_timeout_ms
                    )))
                })
        };
        let response = answered
            .map_err(|e| TransportError::Connection(format!("Vector health check failed: {e}")))?;

        Ok(response.into_inner().status == vector::ServingStatus::Serving as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A listener that completes TCP but never speaks, holding every connection,
    /// and the number of connections it has accepted.
    async fn silent_listener() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                held.push(stream);
            }
        });
        (addr, accepts)
    }

    /// A listener whose accept queue is full, so the kernel drops every SYN and
    /// a dial to it stalls in the TCP connect. Sending on the returned channel
    /// frees the queue; from then on it accepts silently, counting accepts
    /// (the connection that filled the queue included).
    async fn stalled_connect_listener() -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        Arc<AtomicUsize>,
    ) {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket
            .bind("127.0.0.1:0".parse().expect("addr"))
            .expect("bind");
        // Backlog 0 holds one completed connection; the filler takes that slot.
        let listener = socket.listen(0).expect("listen");
        let addr = listener.local_addr().expect("addr");
        let filler = tokio::net::TcpStream::connect(addr)
            .await
            .expect("filler connects");

        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        tokio::spawn(async move {
            let _filler = filler;
            let _ = released.await;
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                held.push(stream);
            }
        });
        (addr, release, accepts)
    }

    /// A send to a peer whose TCP connect never completes ends when the dial is
    /// abandoned, at nine tenths of the send limit.
    #[tokio::test]
    async fn a_send_to_a_server_that_never_completes_the_connect_ends_at_the_dial_limit() {
        let (addr, _release, _accepts) = stalled_connect_listener().await;
        let client = VectorCompatClient::connect_lazy_within(&format!("http://{addr}"), 300)
            .expect("client");

        let started = std::time::Instant::now();
        let sent = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_events(&[serde_json::json!({ "seq": 1 })]),
        )
        .await;
        let elapsed = started.elapsed();

        assert!(
            matches!(sent, Ok(Err(TransportError::Send(_)))),
            "send_events must end at the dial limit as an error, got {sent:?}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "a dial limited to 270 ms took {elapsed:?}"
        );
    }

    /// A health check to a server that accepts and never answers ends at the
    /// send limit: it is a short probe, so a silent peer is a failed probe.
    #[tokio::test]
    async fn a_health_check_to_a_server_that_never_answers_ends_at_send_timeout() {
        let (addr, _accepts) = silent_listener().await;
        let client = VectorCompatClient::connect_lazy_within(&format!("http://{addr}"), 300)
            .expect("client");

        let started = std::time::Instant::now();
        let health = tokio::time::timeout(Duration::from_secs(5), client.health_check()).await;
        let elapsed = started.elapsed();

        assert!(
            matches!(health, Ok(Err(TransportError::Connection(_)))),
            "health_check must end at the send limit as an error, got {health:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "a probe at a 300 ms limit took {elapsed:?}"
        );
    }

    /// A send the receiver holds open while it waits for queue room is not cut
    /// at the send limit: it completes once room frees. Cutting it would make
    /// the caller push the same events again while the first push may still
    /// land.
    #[tokio::test]
    async fn a_send_the_receiver_holds_is_not_cut_at_the_send_limit() {
        use crate::transport::grpc::GrpcTransport;
        use crate::transport::{TransportBase, TransportReceiver};

        let mut server_config = GrpcConfig::server("127.0.0.1:0").with_vector_compat();
        server_config.recv_buffer_size = 1;
        server_config.recv_timeout_ms = 100;
        let server = GrpcTransport::new(&server_config)
            .await
            .expect("Vector-compat server");
        let addr = server.local_addr().expect("server bound");
        let client = Arc::new(
            VectorCompatClient::connect_lazy_within(&format!("http://{addr}"), 300)
                .expect("client"),
        );

        // One event fills the one-slot queue, so the next push waits for room.
        client
            .send_events(&[serde_json::json!({ "seq": 1 })])
            .await
            .expect("the first event fits");
        let held = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.send_events(&[serde_json::json!({ "seq": 2 })]).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !held.is_finished(),
            "the held send ended before the receiver had room, 700 ms past its 300 ms limit"
        );

        let first = server.recv(10).await.expect("recv").records.len();
        let result = tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .expect("the held send completes once room frees")
            .expect("send task");
        let second = server.recv(10).await.expect("recv").records.len();

        assert!(result.is_ok(), "the held send must succeed, got {result:?}");
        assert_eq!(
            (first, second),
            (1, 1),
            "each event reaches the receiver once"
        );
        let _ = server.close().await;
    }

    /// A send on a connection whose peer stops answering at the HTTP/2 level
    /// ends once a PING goes unanswered, and the next send dials afresh. A
    /// source that holds the RPC while it still answers PINGs is not cut.
    #[tokio::test]
    async fn a_send_to_a_peer_that_stops_answering_ends_and_the_next_redials() {
        use crate::transport::grpc::GrpcTransport;
        use crate::transport::grpc::test_peers::FreezingProxy;
        use crate::transport::{TransportBase, TransportReceiver};

        let server = GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0").with_vector_compat())
            .await
            .expect("Vector-compat server");
        let proxy = FreezingProxy::start(server.local_addr().expect("server bound")).await;
        let client =
            VectorCompatClient::connect_lazy_within(&format!("http://{}", proxy.addr), 300)
                .expect("client");

        client
            .send_events(&[serde_json::json!({ "seq": 1 })])
            .await
            .expect("before the freeze");
        proxy.freeze();
        let stalled = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_events(&[serde_json::json!({ "seq": 2 })]),
        )
        .await;
        assert!(
            matches!(stalled, Ok(Err(TransportError::Send(_)))),
            "a send on a connection whose peer stopped answering must end as an error, \
             got {stalled:?}"
        );

        let fresh = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_events(&[serde_json::json!({ "seq": 3 })]),
        )
        .await;
        assert!(
            matches!(fresh, Ok(Ok(()))),
            "the next send dials afresh, got {fresh:?}"
        );
        assert_eq!(
            proxy.accepted(),
            2,
            "one connection before the freeze, one after"
        );
        assert_eq!(server.recv(10).await.expect("recv").records.len(), 2);
        let _ = server.close().await;
    }

    /// A Vector source that answers every push with a status of this code.
    struct Refusing(tonic::Code);

    #[tonic::async_trait]
    impl vector::vector_server::Vector for Refusing {
        async fn push_events(
            &self,
            _request: tonic::Request<vector::PushEventsRequest>,
        ) -> Result<tonic::Response<vector::PushEventsResponse>, tonic::Status> {
            Err(tonic::Status::new(self.0, "refused"))
        }

        async fn health_check(
            &self,
            _request: tonic::Request<vector::HealthCheckRequest>,
        ) -> Result<tonic::Response<vector::HealthCheckResponse>, tonic::Status> {
            Ok(tonic::Response::new(vector::HealthCheckResponse {
                status: vector::ServingStatus::Serving.into(),
            }))
        }
    }

    /// Serve [`Refusing`] with `code` on a loopback port.
    async fn refusing_source(code: tonic::Code) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        // The client sends gzip, as Vector's source accepts it.
        let service = vector::vector_server::VectorServer::new(Refusing(code))
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        addr
    }

    /// A refusal no resend clears is told apart from a failure that can clear,
    /// so a caller holding its source can drop or dead-letter the one and
    /// resend the other.
    #[tokio::test]
    async fn a_push_refused_for_good_is_told_from_one_that_can_clear() {
        for (code, permanent) in [
            (tonic::Code::DataLoss, true),
            (tonic::Code::InvalidArgument, true),
            (tonic::Code::OutOfRange, true),
            (tonic::Code::Unavailable, false),
            (tonic::Code::ResourceExhausted, false),
            (tonic::Code::Unimplemented, false),
        ] {
            let addr = refusing_source(code).await;
            let client = VectorCompatClient::connect_lazy_within(&format!("http://{addr}"), 5_000)
                .expect("client");
            let status = client
                .send_events_status(&[serde_json::json!({ "seq": 1 })])
                .await
                .expect_err("the source refuses every push");
            assert_eq!(status.code(), code, "{status:?}");
            assert_eq!(
                VectorCompatClient::is_permanent_rejection(&status),
                permanent,
                "{code:?}"
            );
            assert!(
                matches!(
                    client.send_events(&[serde_json::json!({ "seq": 2 })]).await,
                    Err(TransportError::Send(_))
                ),
                "send_events keeps its error for every code"
            );
        }
    }

    /// The client's own connection failing is never a refusal, whatever code
    /// tonic gives it.
    #[tokio::test]
    async fn a_push_that_never_reached_a_source_is_not_a_refusal() {
        let (addr, _accepts) = silent_listener().await;
        let client = VectorCompatClient::connect_lazy_within(&format!("https://{addr}"), 5_000)
            .expect("client");
        let status = tokio::time::timeout(
            Duration::from_secs(2),
            client.send_events_status(&[serde_json::json!({ "seq": 1 })]),
        )
        .await
        .expect("the TLS refusal is immediate")
        .expect_err("no TLS on this client");
        assert!(
            !VectorCompatClient::is_permanent_rejection(&status),
            "{status:?}"
        );
    }

    /// The client carries no TLS, so tonic refuses an `https` endpoint once the
    /// TCP connect completes, before any RPC is sent: the send fails at once.
    #[tokio::test]
    async fn an_https_endpoint_is_refused() {
        let (addr, _accepts) = silent_listener().await;
        let client = VectorCompatClient::connect_lazy_within(&format!("https://{addr}"), 5_000)
            .expect("client");

        let sent = tokio::time::timeout(
            Duration::from_secs(2),
            client.send_events(&[serde_json::json!({ "seq": 1 })]),
        )
        .await;

        assert!(
            matches!(sent, Ok(Err(TransportError::Send(_)))),
            "got {sent:?}"
        );
    }

    /// A TCP connect the server never completes is abandoned inside the send
    /// limit, so the next send dials afresh the moment the server accepts,
    /// rather than queueing behind the stalled dial's SYN retries.
    ///
    /// The kernel retries a SYN at 1 s and again at 3 s. With a 1.5 s limit the
    /// stalled dial ends at 1.35 s, after the first retry, and a dial left
    /// running would next reach the server at 3 s, well past the 500 ms the
    /// fresh dial is given.
    #[tokio::test]
    async fn each_send_after_a_stalled_connect_dials_afresh() {
        let (addr, release, accepts) = stalled_connect_listener().await;
        let client = Arc::new(
            VectorCompatClient::connect_lazy_within(&format!("http://{addr}"), 1_500)
                .expect("client"),
        );

        let first = tokio::time::timeout(
            Duration::from_secs(5),
            client.send_events(&[serde_json::json!({ "seq": 1 })]),
        )
        .await;
        assert!(
            matches!(first, Ok(Err(_))),
            "a send to a server that never completes the connect must end at the send \
             limit as an error, got {first:?}"
        );

        release.send(()).expect("listener task alive");
        // The server never answers, so this send stays open; only its dial is
        // under test.
        let second = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.send_events(&[serde_json::json!({ "seq": 2 })]).await }
        });

        let started = std::time::Instant::now();
        while accepts.load(Ordering::SeqCst) < 2 && started.elapsed() < Duration::from_millis(500) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let reached = accepts.load(Ordering::SeqCst);
        second.abort();

        assert_eq!(
            reached, 2,
            "the second send's dial should reach the server within 500 ms of it accepting \
             (the filler plus one dial)"
        );
    }
}
