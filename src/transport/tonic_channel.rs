// Project:   scalo
// File:      src/transport/tonic_channel.rs
// Purpose:   The lazily dialled tonic channel every gRPC client shares
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The tonic channel the gRPC, Vector and OTLP clients dial, with the dial
//! bounded and dead connections dropped.

use std::time::Duration;

use tonic::codec::CompressionEncoding;

use crate::compression::Codec;

/// Default send limit of a gRPC client, and its HTTP/2 PING period when it
/// has none.
pub(crate) const DEFAULT_SEND_TIMEOUT_MS: u64 = 30_000;

/// Every encoding a tonic server or client here reads. tonic's zstd, and the
/// zstd C library under it, comes only with the `compression` feature.
#[cfg(feature = "compression")]
pub(crate) const ACCEPTED_ENCODINGS: &[CompressionEncoding] =
    &[CompressionEncoding::Gzip, CompressionEncoding::Zstd];
#[cfg(not(feature = "compression"))]
pub(crate) const ACCEPTED_ENCODINGS: &[CompressionEncoding] = &[CompressionEncoding::Gzip];

/// The codecs a gRPC or Vector-compat client sends with: zstd only with `compression`.
#[cfg(all(feature = "transport-grpc", feature = "compression"))]
pub(crate) const TONIC_CODECS: &[Codec] = &[Codec::None, Codec::Gzip, Codec::Zstd];
#[cfg(all(feature = "transport-grpc", not(feature = "compression")))]
pub(crate) const TONIC_CODECS: &[Codec] = &[Codec::None, Codec::Gzip];

/// tonic's encoding for `codec`, which fixes the level. `None` for
/// [`Codec::None`] and for a codec tonic lacks, which an `Accepts` check
/// refuses first.
pub(crate) fn encoding(codec: Codec) -> Option<CompressionEncoding> {
    match codec {
        Codec::Gzip => Some(CompressionEncoding::Gzip),
        #[cfg(feature = "compression")]
        Codec::Zstd => Some(CompressionEncoding::Zstd),
        _ => None,
    }
}

/// A lazily dialled channel to `ep`, direct or through `proxy`.
///
/// A dial whose DNS lookup, TCP connect or TLS handshake is unfinished at nine
/// tenths of `send_timeout_ms` is abandoned. A connection that has read nothing
/// for `send_timeout_ms` is sent an HTTP/2 PING and closed if the PING goes
/// unanswered for as long again, so a peer that stays connected but stops
/// answering is dropped and the next call dials afresh. `0` leaves the dial
/// unbounded and PINGs at [`DEFAULT_SEND_TIMEOUT_MS`].
pub(crate) fn lazy_channel(
    mut ep: tonic::transport::Endpoint,
    send_timeout_ms: u64,
    proxy: crate::transport::ProxyDialer,
) -> tonic::transport::Channel {
    // A dial left running when its send gives up hands its failure to the
    // next send, so the dial ends at nine tenths of the limit.
    if send_timeout_ms > 0 {
        ep = ep.connect_timeout(Duration::from_millis(send_timeout_ms) * 9 / 10);
    }

    let keep_alive = Duration::from_millis(if send_timeout_ms > 0 {
        send_timeout_ms
    } else {
        DEFAULT_SEND_TIMEOUT_MS
    });
    ep = ep
        .http2_keep_alive_interval(keep_alive)
        .keep_alive_timeout(keep_alive)
        .keep_alive_while_idle(true);

    // Given the connector, tonic abandons a dial whose DNS, TCP connect or TLS
    // handshake outruns the connect timeout, where connect_lazy() bounds the
    // TCP connect alone and the next send queues behind it.
    let mut tcp = hyper_util::client::legacy::connect::HttpConnector::new();
    // tonic's own settings: an https URI passes through to its TLS layer.
    tcp.enforce_http(false);
    tcp.set_nodelay(true);
    ep.connect_with_connector_lazy(crate::transport::proxy::Connector::new(proxy, tcp))
}
