# Baseline benches

`benches/baseline.rs` is a criterion bench of two hot-path primitives, run on fixed records so an app's own benches can measure the same bytes. Each runs on a small (~200 B) and a large (~4 KiB) JSON log record the bench builds with fixed field values.

| Group | What it measures | Throughput against |
| --- | --- | --- |
| `baseline/record_codec` | `Record::encode` and `Record::decode`, the serialisation on the spill cache's cold path | payload bytes for encode, framed bytes for decode |
| `baseline/ingest_parse` | `transport::parse` of the payload as JSON, the ingest decode | payload bytes |

```sh
cargo bench --bench baseline --features transport
```

The bench needs the `transport` feature (`required-features` in `Cargo.toml`). App-level comparisons on the same records live in the app repos.
