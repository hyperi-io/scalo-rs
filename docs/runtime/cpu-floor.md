# CPU floor check

A binary built with `-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq` dies with a bare SIGILL on an older CPU, or on a VM whose CPU type masks AVX2. `scalo::require_cpu_floor!()` makes it refuse with a message instead.

```rust
scalo::require_cpu_floor!();

fn main() {
    // ...
}
```

Put it once, at item level, in the binary crate's `main.rs`. It has to expand in the binary: an `.init_array` entry inside the scalo rlib is linked only when something references its object file.

It expands only for x86_64 Linux builds with AVX2 enabled. A baseline build, or an arm64 one, carries nothing, so a crate built both ways can call it unconditionally.

The check runs from `.init_array`, before `main` and the Rust runtime. It is assembly, because a v3 build lets the compiler use BMI2, MOVBE or AVX in any Rust code, the check included.

It checks the CPUID range, then leaf 1 ECX, XCR0, leaf 7 EBX and leaf 0x80000001 ECX, in that order. That covers every x86-64-v3 feature plus PCLMULQDQ. The leaf range is checked before each leaf is read, because a VM that caps the range answers a higher leaf with another leaf's data.

On failure it writes two lines to stderr and exits with status 1:

```text
error: this CPU lacks one of AVX2, BMI1 or BMI2.
This build needs an x86-64-v3 CPU with PCLMULQDQ: Intel Haswell (2013) or later, AMD Excavator (2015) or EPYC 7001 or later. On a VM, set the CPU type to host, x86-64-v3 or a Haswell EVC baseline.
```

The first line names the feature group that failed.

## Testing a build

`examples/cpu_floor.rs` carries the check. Build it the way a release is built, then run it under `qemu-x86_64`:

```sh
env RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq" \
  CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
  cargo build --release --example cpu_floor
qemu-x86_64 -cpu Westmere target/release/examples/cpu_floor   # refuses, exit 1
qemu-x86_64 -cpu Haswell target/release/examples/cpu_floor    # cpu floor met
```

`-cpu Haswell,-avx2`, `-bmi2`, `-pclmulqdq` or `-abm` (LZCNT) each refuse with their own group. A BOLT pass keeps the check: BOLT leaves the original copy in `.bolt.org.text`, where `.init_array` still points.
