// Project:   scalo
// File:      benches/delivery_path/listener.rs
// Purpose:   In-process gRPC listeners for tier 2: scalo's own, a slow one, and none
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use scalo::transport::grpc::proto::transport_server::{Transport, TransportServer};
use scalo::transport::grpc::proto::{
    Batch, BatchAck, HealthCheckRequest, HealthCheckResponse, PushRequest, PushResponse,
    ServingStatus,
};
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{TransportBase, TransportReceiver};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

/// A gRPC endpoint a cell sends to, and what keeps it serving.
pub enum Listener {
    /// scalo's own receive server, drained as fast as it fills.
    Scalo {
        server: Arc<GrpcTransport>,
        stop: Arc<AtomicBool>,
        drain: JoinHandle<()>,
    },
    /// A server that answers every call after a fixed delay.
    Slow { serve: JoinHandle<()> },
    /// Nothing listens: the port was bound once and released.
    Down,
}

impl Listener {
    /// scalo's receive server on a free loopback port.
    pub async fn scalo() -> Option<(String, Self)> {
        let addr = free_port().await?;
        let server = Arc::new(GrpcTransport::new(&GrpcConfig::server(&addr)).await.ok()?);
        let stop = Arc::new(AtomicBool::new(false));
        let drain = tokio::spawn(drain(Arc::clone(&server), Arc::clone(&stop)));
        Some((
            format!("http://{addr}"),
            Self::Scalo {
                server,
                stop,
                drain,
            },
        ))
    }

    /// A server that waits `delay` before it answers each call.
    pub async fn slow(delay: Duration) -> Option<(String, Self)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
        let addr = listener.local_addr().ok()?;
        let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
        let service = TransportServer::new(Slow { delay }).max_decoding_message_size(usize::MAX);
        let serve = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await;
        });
        Some((format!("http://{addr}"), Self::Slow { serve }))
    }

    /// An endpoint nothing listens on.
    pub async fn down() -> Option<(String, Self)> {
        Some((format!("http://{}", free_port().await?), Self::Down))
    }

    pub async fn close(self) {
        match self {
            Self::Scalo {
                server,
                stop,
                drain,
            } => {
                stop.store(true, Ordering::Relaxed);
                drain.abort();
                let _ = server.close().await;
            }
            Self::Slow { serve } => serve.abort(),
            Self::Down => {}
        }
    }
}

/// A loopback address that was free a moment ago.
async fn free_port() -> Option<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
    Some(listener.local_addr().ok()?.to_string())
}

/// Take what a listener received as fast as it arrives.
async fn drain(server: Arc<GrpcTransport>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        match server.recv(10_000).await {
            Ok(batch) if batch.records.is_empty() => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(batch) => {
                let _ = server.commit(&batch.commit_tokens).await;
            }
            Err(_) => break,
        }
    }
}

struct Slow {
    delay: Duration,
}

#[tonic::async_trait]
impl Transport for Slow {
    async fn push(&self, _request: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        tokio::time::sleep(self.delay).await;
        Ok(Response::new(PushResponse { accepted: 1 }))
    }

    async fn route_batch(&self, request: Request<Batch>) -> Result<Response<BatchAck>, Status> {
        let accepted = request.into_inner().records.len() as u64;
        tokio::time::sleep(self.delay).await;
        Ok(Response::new(BatchAck { accepted }))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving.into(),
        }))
    }
}
