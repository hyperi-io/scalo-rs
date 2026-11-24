# Custom transports

An app that sends somewhere scalo has no backend for writes its own sender. It implements `TransportSender`, registers a constructor under a type name, and config naming that type builds it through the factory, `RoutedSender` and `TieredSink` like a built-in. Custom transports send only.

Two traits do the work. `TransportSender` is the one you implement. `DynTransportSender` is its boxed-future twin, which every `TransportSender` gets for free, so `AnySender::Custom` can hold yours behind an `Arc`.

---

## Delivery guarantee

Every transport states what its `Ok` proves, through `TransportBase::delivery_guarantee`. There is no default, so a new sender says what it keeps.

| Guarantee | `Ok` means | Built-ins |
| --- | --- | --- |
| `Acknowledged` | the destination answered | Kafka, gRPC, HTTP, OTLP, memory |
| `Written` | written and flushed here, no answer | file, pipe |
| `FireAndForget` | handed off, no flush, no answer | Kafka with `acks=0` |

The config's `delivery` key is the weakest guarantee the factory accepts: `ack` (the default), `flush` or `none`. A sender below it is refused at build, and the error names the setting that would accept it:

```text
file transport writes and flushes but has no acknowledgement: set transport.output.delivery: flush to accept that (now ack)
```

---

## Example 1 -- a minimal sender

A sender that hands each record to a channel the app drains. It needs no settings, so its constructor ignores its config.

```rust
use scalo::transport::{
    AnySender, DeliveryGuarantee, SendResult, TransportBase, TransportResult,
    TransportSender, register_sender,
};
use tokio::sync::mpsc;

struct ChannelSender(mpsc::Sender<bytes::Bytes>);

impl TransportBase for ChannelSender {
    async fn close(&self) -> TransportResult<()> { Ok(()) }
    fn is_healthy(&self) -> bool { !self.0.is_closed() }
    fn name(&self) -> &'static str { "channel" }
    // The receiving task took the record: that is the answer.
    fn delivery_guarantee(&self) -> DeliveryGuarantee { DeliveryGuarantee::Acknowledged }
}

impl TransportSender for ChannelSender {
    async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
        match self.0.try_send(payload) {
            Ok(()) => SendResult::Ok,
            Err(mpsc::error::TrySendError::Full(_)) => SendResult::Backpressured,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                SendResult::Fatal(scalo::transport::TransportError::Closed)
            }
        }
    }
}

let (tx, mut rx) = mpsc::channel(1024);
register_sender("channel", move |_config| {
    let tx = tx.clone();
    async move { Ok(ChannelSender(tx)) }
})?;
```

```yaml
transport:
  output:
    type: channel
```

`AnySender::from_config("transport.output").await?` now builds it. The default `send_batch` sends each record through `send`, and `send_batch_by` bounds that by a deadline. Override either only when the destination does better in one call.

An app that builds its sender itself skips the registry: `AnySender::custom(ChannelSender(tx))`.

---

## Example 2 -- a sender with its own config

A sender that writes NDJSON lines to a TCP peer. Its settings sit in a section named after its type, and its constructor reads them with `config.section()`. An absent section reads as an empty map, so serde defaults apply.

```rust
use std::time::Duration;
use scalo::transport::{
    CustomSenderConfig, DeliveryGuarantee, SendResult, TransportBase, TransportError,
    TransportResult, TransportSender, register_sender,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[derive(serde::Deserialize)]
struct LineSettings {
    address: String,
    #[serde(default = "default_timeout_ms")]
    connect_timeout_ms: u64,
}

fn default_timeout_ms() -> u64 { 5_000 }

struct LineSender {
    stream: Mutex<tokio::net::TcpStream>,
}

impl LineSender {
    async fn connect(config: CustomSenderConfig) -> TransportResult<Self> {
        let settings: LineSettings = config.section()?;
        let wait = Duration::from_millis(settings.connect_timeout_ms);
        let stream = tokio::time::timeout(wait, tokio::net::TcpStream::connect(&settings.address))
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        Ok(Self { stream: Mutex::new(stream) })
    }
}

impl TransportBase for LineSender {
    async fn close(&self) -> TransportResult<()> {
        self.stream.lock().await.shutdown().await.map_err(|e| TransportError::Send(e.to_string()))
    }
    fn is_healthy(&self) -> bool { true }
    fn name(&self) -> &'static str { "line_tcp" }
    // TCP takes the bytes and the peer says nothing back.
    fn delivery_guarantee(&self) -> DeliveryGuarantee { DeliveryGuarantee::Written }
}

impl TransportSender for LineSender {
    async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
        let mut stream = self.stream.lock().await;
        let written = async {
            stream.write_all(&payload).await?;
            stream.write_all(b"\n").await?;
            stream.flush().await
        };
        match written.await {
            Ok(()) => SendResult::Ok,
            // The peer went away: the caller keeps the record and sends it again.
            Err(_) => SendResult::Backpressured,
        }
    }
}

register_sender("line_tcp", |config| LineSender::connect(config))?;
```

```yaml
transport:
  output:
    type: line_tcp
    delivery: flush
    line_tcp:
      address: "collector:5170"
      connect_timeout_ms: 2000
```

`delivery: flush` is needed because the sender states `Written`. Without it the build fails naming `transport.output.delivery: flush`. Each key takes cascade env overrides as any other, `<PREFIX>__TRANSPORT__OUTPUT__LINE_TCP__ADDRESS` for one. That is why a type name is lowercase letters, digits and `_`.

---

## What a send answers

| Answer | Means | The caller |
| --- | --- | --- |
| `Ok` | sent, to the guarantee stated | releases the records |
| `Backpressured` | busy, try again | sends them again later |
| `RetryAfter(wait)` | busy, and the destination named the wait | sends them again after `wait` |
| `Reject(error)` | refused for good | dead-letters them, never retries |
| `FilteredDlq` | scalo's filter or size cap took a record | dead-letters what `dead_letter_reason` names |
| `Fatal(error)` | the sender itself cannot go on | stops, or keeps the records until it is fixed |

`SinkStack` waits out a `RetryAfter`, capped at `max_backoff_ms`, and dead-letters a rejected record. A rejected batch names no record, so it sends the batch again one record at a time to find it. A deadline that passes in `send_batch_by` answers `Backpressured`, because the records may or may not have landed.

`scalo::backoff::Backoff` is the schedule the in-tree retry loops use. `next_delay(failures, hint)` takes the destination's wait in place of its own, up to the ceiling, `delays()` yields the waits as an endless iterator for a loop such as `backon`'s, and `parse_retry_after` reads an HTTP `Retry-After` header.

The trait architecture and the factory are in [README.md](README.md), the built-in backends in [backends.md](backends.md), custom routes in [routing.md](routing.md), and retries and dead letters around a sender in [../pipeline/sink-stack.md](../pipeline/sink-stack.md).
