// Project:   scalo
// File:      examples/cpu_floor.rs
// Purpose:   A binary carrying the x86_64 CPU floor check
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Built with `RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"`,
//! this prints `cpu floor met` on a CPU at the floor. Under
//! `qemu-x86_64 -cpu Westmere` it prints the refusal and exits with status 1.

scalo::require_cpu_floor!();

fn main() {
    println!("cpu floor met");
}
