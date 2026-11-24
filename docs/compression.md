# Compression

Every scalo surface that compresses reads one key shape, and runs one engine where scalo does the compressing.

```yaml
compression: {codec: zstd, level: 3}
```

- `codec` is `none`, `zstd`, `lz4`, `gzip` or `snappy`. It is required.
- `level` is optional. Unset, the codec's default: zstd 3 (range 1-22), gzip 6 (range 1-9). LZ4, snappy and none take no level.
- An unknown key, such as `levl`, fails the load.

The types are `scalo::compression::{Codec, Compression, Accepts}`, always compiled, so a config struct can hold a setting a dependency applies.

## What each surface takes

Each surface lists its codecs in an `Accepts`, and refuses anything else when it is built, naming the key, the codecs it takes and the level range. The config schema narrows the `codec` enum and the `level` range the same way.

| Surface | Key | Codecs | Level | Default |
| --- | --- | --- | --- | --- |
| Tiered sink spill | `compression` | none, zstd, lz4, snappy (with `compression-snappy`) | yes | zstd 3 |
| Rotated files: DLQ, file output, `io::FileWriterConfig` | `compress_rotated` | none, gzip, zstd | yes | gzip 6 |
| Rotated files: file transport | `compress_rotated` | none, gzip, zstd | yes | none |
| HTTP send | `transport.http.compression` | none, gzip, zstd | yes | none |
| OTLP | `transport.otlp.compression` | none, gzip | no | none |
| gRPC client | `transport.grpc.compression` | none, gzip, zstd | no | none |
| Vector-compat client | `VectorCompatClient::with_compression` | none, gzip, zstd | no | gzip |
| Kafka producer | `kafka.sizing.producer.compression` | all five | yes | zstd 3 |

- HTTP, OTLP and gRPC default to none because the peer must decode the codec. HTTP and OTLP over HTTP send it as `Content-Encoding`. scalo's HTTP receive side decodes gzip and zstd bodies, to at most `max_body_bytes`, and answers any other coding 415.
- gRPC and OTLP over gRPC map the codec onto tonic's `CompressionEncoding`, which fixes the level, so a level is refused. A gRPC server reads gzip and zstd on every connection, whatever its own `compression` says.
- Kafka maps onto librdkafka's `compression.type` and `compression.level`, so snappy needs no scalo feature there. The raw `producer_librdkafka` and `librdkafka_overrides` maps stay strings and win: a codec named there takes that codec's default level, unless a raw map names a level too. See [kafka-path.md](kafka-path.md#wire-compression).
- Rotated files past the six newest get the codec's suffix, `.gz` or `.zst`. Retention reads every suffix, so changing the codec keeps old files in the count.
- GeoIP downloads are gzip because the provider packages them that way, read through flate2 directly and never from config.
- The tiered sink keeps no codec per record. Its drain holds, committing nothing, at an entry the current codec cannot read, and resumes when the sink reopens under the codec that wrote it ([pipeline/tiered-sink.md](pipeline/tiered-sink.md#changing-the-codec)).

The engines need features. The `compression` feature compiles the zstd, LZ4 and gzip engines, and tonic's gzip and zstd. `compression-snappy` adds snappy. `tiered-sink`, `io` and `transport-http` each pull `compression`. Where a dependency applies a fixed codec, the feature pulls only that codec: `transport-grpc` turns on tonic's gzip and zstd, `transport-otlp` tonic's gzip and `flate2`, and `geoip-download` `flate2`.

| API | For |
| --- | --- |
| `Compressor::new(compression)` | Blocks in a loop. `compress_into(src, dst)` and `decompress_into(src, dst, limit)` reuse the codec context and `dst`, so a loop allocates only what `dst` grows by |
| `CompressorPool::new(compression)` | Callers compressing at once, such as concurrent sends. Each call takes an idle `Compressor` or makes one, and returns it, so the pool holds as many as were ever busy together |
| `Compression::compress(src)`, `decompress(src, limit)` | One block, with a context made for it |
| `Compression::encoder(writer)`, `decoder(reader)` | Streams. `Encoder::finish` writes the trailer. Wrap a decoder in `Read::take` for untrusted input |
| `Codec::tag()`, `Codec::from_tag(byte)` | One stable byte per codec, for a store scalo owns |
| `Codec::extension()` | The file suffix of what an `Encoder` writes |

zstd and gzip blocks and streams are one format. For LZ4 and snappy the block is the raw block (LZ4 with a 4-byte little-endian length in front, as `lz4_flex::compress_prepend_size` writes it) and the stream is the framed format, so each is read by the form that wrote it. `decompress_into` answers bytes that are not the codec's with `InvalidData`, and output past `limit` with `FileTooLarge`.

```rust
use scalo::compression::{Codec, Compression, Compressor};

let mut compressor = Compressor::new(Compression::new(Codec::Zstd))?;
let (mut packed, mut unpacked) = (Vec::new(), Vec::new());
for record in [&b"first"[..], b"second"] {
    compressor.compress_into(record, &mut packed)?;
    compressor.decompress_into(&packed, &mut unpacked, 1 << 20)?;
    assert_eq!(unpacked, record);
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

A surface of an app's own checks its key with an `Accepts`:

```rust
use scalo::compression::{Accepts, Codec};

const SINK: Accepts = Accepts::new("sink.compression", &[Codec::None, Codec::Zstd]);
```

`Accepts::without_level()` refuses a level, and `Accepts::external()` takes a codec a dependency applies without scalo's engine. `Accepts::schema()` (with `config-schema`) gives the narrowed schema for a `schema_with` field.

Source: [../src/compression/](../src/compression/).
