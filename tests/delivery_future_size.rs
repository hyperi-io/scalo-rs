// Project:   scalo
// File:      tests/delivery_future_size.rs
// Purpose:   Size of one destination's send future for the delivery bench's tier 2 sender
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One destination's send future is large, and an approach that collects them
//! by value copies each one whole. Run with `--nocapture` to read the size.

#![cfg(all(
    feature = "transport-test-support",
    feature = "testcontainers",
    feature = "transport-grpc"
))]

use std::future::Future;
use std::mem::size_of;
use std::pin::Pin;

use scalo::transport::test_support::delivery::destination_send;
use scalo::transport::{AnySender, Record};

// The bench's own sender wrapper, so the size is the one the bench sends with.
#[allow(dead_code, reason = "only the type is measured")]
#[path = "../benches/delivery_path/observe.rs"]
mod observe;

use observe::Observed;

/// Size of the future `make` returns, without building one.
fn size_of_returned<A, B, C, F: Future>(_make: impl Fn(A, B, C) -> F) -> usize {
    size_of::<F>()
}

#[test]
fn a_send_future_is_larger_than_the_pointer_a_pinned_one_moves() {
    let future = size_of_returned(destination_send::<Observed<AnySender>>);
    let boxed = size_of::<Pin<Box<u8>>>();
    eprintln!("send future: {future} bytes, boxed handle: {boxed} bytes");
    assert!(future > size_of::<Vec<Record>>() + size_of::<Vec<usize>>());
}
