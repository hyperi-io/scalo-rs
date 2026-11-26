# Syslog

The `transport-syslog` feature sends records to a syslog receiver as `type: syslog`. Each record's payload is the message body, after an RFC 5424 or RFC 3164 header. A `listen` section receives syslog as well: see [Receiving](#receiving).

| `protocol` | Wire | Guarantee | `delivery` it needs |
| --- | --- | --- | --- |
| `tcp` (default) | octet counting or LF lines, RFC 6587 | `written` | `flush` |
| `tls` | the same inside TLS, RFC 5425 | `written` | `flush` |
| `udp` | one message per datagram, RFC 5426 | `fire_and_forget` | `none` |
| `relp` | RELP, each message answered, TLS when `tls` is set | `acknowledged` | `ack` (default) |

The factory refuses a sender below the config's `delivery` key, so a TCP output says `delivery: flush` and a UDP one `delivery: none`. See [custom.md](custom.md#delivery-guarantee).

```yaml
transport:
  output:
    type: syslog
    delivery: flush
    syslog:
      address: logs.example.net:6514
      protocol: tls
      facility: local0
      app_name: billing
      tls:
        trust: bundle
        ca_paths: [/etc/tls/ca.pem]
        client_cert_path: /etc/tls/client.pem
        client_key_path: /etc/tls/client.key
```

An agent forwarding records from many hosts sets HOSTNAME and MSGID per record from headers it puts on each record, with PROCID nil:

```yaml
    syslog:
      address: logs.example.net:6514
      protocol: tls
      app_name: agent
      proc_id: "-"
      hostname_header: host.name    # record header holding the source host
      msg_id_header: source.kind    # record header holding the source kind
```

---

## Messages

RFC 5424, the default:

```text
<134>1 2026-10-11T22:14:15.003000Z web01 billing 4242 login - {"user":"ann"}
```

RFC 3164:

```text
<134>Oct 11 22:14:15 web01 billing[4242]: {"user":"ann"}
```

- PRI is `facility * 8 + severity`, from config for every message.
- The time is the record's timestamp, else the send time. RFC 5424 writes it in UTC to the microsecond, RFC 3164 in the host's zone to the second. A record's timestamp is in milliseconds, so the last three digits are `000`.
- HOSTNAME, APP-NAME and PROCID come from config. Unset, they are the machine's host name, the executable's name and the process id. RFC 3164 keeps letters, digits, `-`, `_` and `.` in the tag, up to 32.
- MSGID is the record's key, else `msg_id`, else `-`. `send(destination, payload)` uses the destination.
- `hostname_header` and `msg_id_header` take HOSTNAME and MSGID from each record instead: the value of the record header they name. A record without that header gets `-`, never the configured value or the machine's host name. RFC 3164 has no MSGID, so it reads `hostname_header` only.
- Every header field keeps its printable ASCII, no spaces, cut to the RFC 5424 maximum: 255 for HOSTNAME, 48 for APP-NAME, 128 for PROCID, 32 for MSGID. A field that leaves nothing is `-`.
- STRUCTURED-DATA is `structured_data` from config, the same on every message, with `"`, `\` and `]` escaped in values.
- One trailing `\n` or `\r\n` is taken off the payload. An empty payload sends the header alone.
- With `passthrough: true`, a payload that already starts with a `<PRI>` (1 to 3 digits, at most 191) is sent as it is. Anything else gets a header.

TCP and TLS frame each message with `scalo::framing` ([framing.md](framing.md)):

- `octet_counting`, the default: `<length> <message>`. A message may hold any byte.
- `lf`: the message then `\n`. A `\n` inside the message is sent as `#012`, the octal escape receivers commonly print, so it cannot end the frame early.

`max_message_bytes` bounds a message, header included. Unset, it is 2048 for UDP and 8192 otherwise, and it must be at least 480. UDP cuts a longer message at that size, never inside a UTF-8 character, sends it, and counts it in `transport_truncated_total`. Every other protocol leaves it out of the send, counts it in `transport_message_too_large_total`, sends the rest and answers `FilteredDlq`. `dead_letter_reason` names it as `TooLarge`, so a caller that screens the block first routes it to its DLQ.

---

## Connections and results

One connection is held and reused, behind a lock, so concurrent sends go one after another. It opens on the first send or `healthcheck`, which a factory boot probe runs.

| Event | Result |
| --- | --- |
| Batch written and flushed (`tcp`, `tls`), sent (`udp`), every message answered `200` (`relp`) | `Ok`, or `FilteredDlq` when a record was left out |
| Receiver closed the connection while it was idle | reconnect before the batch, no loss |
| Connect refused or timed out, DNS failed, the connect resolver has no answer | `Backpressured` |
| The connect resolver's answer changed since the last batch | the old connection ended, a new one dialled before the batch |
| Write broke off mid-batch, or the deadline passed | `Backpressured`, the connection dropped, never `Ok` |
| TLS refused the receiver's certificate or ours, or found no version, suite or group in common | `Fatal(Config)`, naming the `TlsFailure` |
| The proxy refused CONNECT | `Backpressured`, with the proxy's status and the receiver's port logged once per run and `is_healthy()` and the `transport:syslog` health check false until a send goes through: many proxies allow CONNECT to port 443 only |
| RELP session lost mid-batch | reconnect once in the same call and send only the unanswered messages, then `Backpressured` if that fails too |
| RELP answer other than `200` for a message | `Backpressured`, the session kept |
| RELP receiver refuses the session, or offers no `syslog` command | `Fatal(Config)` |
| Sender closed | `Fatal(Closed)` |

A send dropped at its deadline takes its connection with it, so a later batch never follows half a frame. A send with no deadline of its own is bounded by `send_timeout_ms`.

TCP and TLS have no answer. A receiver that takes the bytes and fails before handling them loses them, and so does one that closes between the idle check and the write. Use `relp` where that loss matters.

RELP has no answer that means a message can never be taken: a receiver answers `500` for a full queue as well as a bad message. So a `500` is a retry of the whole block, and the messages answered `200` arrive twice. A receiver that refuses one message every time holds the block until the caller's retry limit gives up on it.

The client opens a session with `relp_version=0` and `commands=syslog`, then sends each message as a `syslog` command with its own transaction number. Up to `relp_window` (default 128) go out before an answer is awaited. `close` sends the `close` command and waits a second for the answer. A `serverclose` from the receiver ends the session, and the next batch opens another.

There is no maintained Rust RELP crate. The client is in `src/transport/syslog/relp.rs`.

### Connect resolver

`SyslogTransport::builder(&config).resolver(Arc::new(routes)).build()` takes a `ConnectResolver`, asked before every batch where to dial the address's host in place of DNS. It's code, not config, because the address is the app's runtime state. The rules match the HTTP and gRPC transports ([http.md](http.md#connect-resolver)):

- The address always names its port, so the dial goes to the answer's IP at that port. An IP address host is dialled as written.
- TLS still sends and checks the address's host name, or `tls.server_name`.
- A host the resolver has no answer for is `Backpressured`, and ends the kept connection.
- The connection is kept while the answer holds. When the answer changes, the next batch ends the old connection (a RELP session with `close`) and dials the new address. UDP binds a socket to the new address.
- A `proxy` other than `none` fails the build, since the proxy looks the name up itself.

Source: [../../src/transport/syslog/stream.rs](../../src/transport/syslog/stream.rs), [../../src/transport/resolver.rs](../../src/transport/resolver.rs).

---

## Receiving

`SyslogReceiver` listens on TCP, TLS and UDP and hands each message over as one record, its bytes as they came. The PRI and header are the app's to parse. `type: syslog` builds it when the `syslog` section has a `listen` section:

```yaml
transport:
  input:
    type: syslog
    syslog:
      listen:
        tcp_address: 0.0.0.0:514
        udp_address: 0.0.0.0:514
        tls_address: 0.0.0.0:6514
        carriage_returns: keep
        tls:
          cert_file: /etc/tls/syslog.pem
          key_file: /etc/tls/syslog.key
```

A TCP or TLS connection carries octet-counted frames and LF lines mixed, read by the `octet_counting` framing ([framing.md](framing.md)). `carriage_returns: keep` leaves the CR before an LF in the record, as receivers that pass the raw line on do. The default `strip` drops it. A message over `max_message_bytes` is skipped by its count, or up to its `\n`, counted in `transport_message_too_large_total`, and the connection carries on. Each record is a slice of the buffer it was read into, so taking one copies nothing. UDP takes one datagram as one record, whatever its size, and an empty datagram as none, counted in `transport_dropped_total{reason="empty"}`.

Records wait in a queue of `recv_buffer_size` for `recv`. A full queue stops the TCP and TLS reads, so the sender's writes back up. A datagram the full queue cannot take is dropped and counted in `transport_dropped_total{reason="queue_full"}`. No protocol here answers the sender, so the guarantee is `written`, or `fire_and_forget` with a UDP listener, and `commit` does nothing. `close` stops the listeners and closes every connection, and `recv` hands over what is queued, then `Closed`.

A connection closes after `idle_timeout_ms` with no frame, and when a frame begun has not ended `frame_timeout_ms` after its first byte, so a sender trickling an oversize frame loses its connection. The frame clock runs only while the socket is awaited, never while a full queue holds the reader. Both closes count in `transport_connections_closed_total` with `reason` `idle` or `frame_timeout`.

`transport.syslog.listen`, or the `listen` section of a `syslog` section. Every key goes through the cascade, YAML and environment alike.

| Key | Default | Meaning |
| --- | --- | --- |
| `tcp_address`, `udp_address` | unset | `ip:port` to listen on. Unset, no listener |
| `tls_address` | unset | `ip:port` for TLS (RFC 5425), with `tls` |
| `tls` | none | server TLS: `profile`, `cert_file` or `cert_secret`, `key_file` or `key_secret`, `ca_files`, `client_auth`, `reload_interval_secs`, `handshake_timeout_ms` ([../api/tls.md](../api/tls.md)) |
| `max_message_bytes` | 65536 | largest TCP or TLS message |
| `max_frame_bytes` | 262144 | largest octet count trusted. A larger count is read as an LF line. At least `max_message_bytes` |
| `carriage_returns` | `strip` | `keep` leaves the `\r`s before an LF line's `\n` in the record, and a line of one `\r` is a record |
| `tcp_keepalive_secs` | 60 | keepalive probes after this long quiet, then every this long. 0 is off |
| `idle_timeout_ms` | 600000 | close a connection with no frame for this long. 0 keeps it |
| `frame_timeout_ms` | 30000 | close a connection whose frame has not ended this long after its first byte. 0 waits for ever |
| `first_byte_timeout_ms` | 30000 | close a connection that sends nothing this long after it is accepted. 0 waits for ever |
| `max_connections` | 10000 | connections the TCP listener holds open, and the TLS listener the same again. 0 holds no limit |
| `ip_filter` | `disabled` | `mode` (`disabled`, `allowlist`, `denylist`) and `cidrs`, the senders TCP, TLS and UDP take traffic from |
| `udp_recv_buffer_bytes` | 8388608 | `SO_RCVBUF`. A smaller grant is logged: raise `net.core.rmem_max` |
| `udp_batch_size` | 64 | datagrams read per wake |
| `recv_buffer_size` | 10000 | records queued for `recv` |
| `recv_timeout_ms` | 100 | how long `recv` waits for a first record |
| `filters_in` | none | inbound filters on each record ([filter-engine.md](filter-engine.md)) |

A config with no address, `tls_address` without `tls` or the other way round, an address that is not `ip:port`, a zero `max_message_bytes`, `recv_buffer_size` or `udp_batch_size`, an `ip_filter` entry that is not a CIDR, or an `allowlist` with no `cidrs` is refused at build. Each listener binds before any serves, so a failed bind leaves none.

### Who gets in

The receiver is often the edge facing senders it does not control, so each listener checks a connection before it reads a byte of it:

- `ip_filter` takes senders by CIDR, IPv4 and IPv6. `mode: allowlist` takes only those in `cidrs`, `denylist` all but those, and `disabled`, the default, everyone. A dual-stack listener sees an IPv4 sender as `::ffff:a.b.c.d` and checks it as `a.b.c.d`. A refused TCP or TLS connection is closed as it is accepted, before the TLS handshake, and counted with `reason="ip_filter"`. A refused datagram is dropped and counted in `transport_dropped_total{reason="ip_filter"}`. Refusals log at most once per 5 s per listener.
- `max_connections` caps the connections the TCP listener holds open, and the TLS listener the same again. One past it is closed as it is accepted, `reason="over_limit"`. There is no per-sender cap.
- `first_byte_timeout_ms` closes a connection that sends nothing that long after it is accepted, `reason="no_first_byte"`, so a sender cannot hold a place under the cap by opening connections and going quiet. On TLS the handshake then has `tls.handshake_timeout_ms`.

### UDP throughput and loss

Each wake reads every datagram ready, up to `udp_batch_size`, with `recvmmsg` on Linux and a read per datagram elsewhere. Each is copied once into a shared block that its record is a slice of. Equal datagrams from one sender that the kernel joined by receive offload are split back into their datagrams.

A datagram the kernel drops because the socket's receive buffer is full is lost before any read sees it. On Linux the receiver reads the socket's own count from `/proc/net/udp` and `/proc/net/udp6` every 10 s into `transport_dropped_total{reason="kernel"}`. A rising count wants a larger `udp_recv_buffer_bytes`, a larger `net.core.rmem_max`, or a faster `recv`. Elsewhere nothing reads it.

`tests/syslog_udp_rate.rs` measures the datagrams a second `recv` hands over on loopback, ignored by default:

```sh
cargo nextest run --release --features transport-syslog --run-ignored only --no-capture -E 'test(syslog_udp_rate)'
```

---

## Config

`transport.syslog`, or the `syslog` section of a transport config. Every key goes through the cascade, YAML and environment alike.

| Key | Default | Meaning |
| --- | --- | --- |
| `address` | required | `host:port` or `[v6]:port`, resolved on each connect, or by the [connect resolver](#connect-resolver) |
| `protocol` | `tcp` | `tcp`, `tls`, `udp` or `relp` |
| `framing` | `octet_counting` | `octet_counting` or `lf`, for `tcp` and `tls` |
| `format` | `rfc5424` | `rfc5424` or `rfc3164` |
| `passthrough` | `false` | send a payload starting with a `<PRI>` as it is |
| `facility` | `user` | `kern`, `user`, `mail`, `daemon`, `auth`, `syslog`, `lpr`, `news`, `uucp`, `cron`, `authpriv`, `ftp`, `ntp`, `audit`, `alert`, `clock`, `local0` to `local7` |
| `severity` | `info` | `emerg`, `alert`, `crit`, `err`, `warning`, `notice`, `info`, `debug` |
| `hostname` | host name | HOSTNAME |
| `hostname_header` | none | record header whose value is HOSTNAME, `-` when a record lacks it |
| `app_name` | executable name | APP-NAME, the RFC 3164 tag |
| `proc_id` | process id | PROCID |
| `msg_id` | `-` | MSGID for a record with no key |
| `msg_id_header` | none | record header whose value is MSGID, in place of the key, `-` when a record lacks it |
| `structured_data` | none | SD-ID to its parameters, RFC 5424 only |
| `max_message_bytes` | 2048 UDP, 8192 otherwise | see above |
| `connect_timeout_ms` | 5000 | TCP connect, TLS handshake and RELP open |
| `send_timeout_ms` | 30000 | one send with no deadline of its own |
| `relp_window` | 128 | RELP messages in flight |
| `proxy` | `none` | `none`, `system` or an `http://` proxy URL, for `tcp`, `tls` and `relp`: an HTTP CONNECT tunnel with TLS inside it ([http.md](http.md#proxy-redirects-and-logs)) |
| `tls.profile` | `strong` | `strong`, `commercial` or `cnsa2` ([../tls-profiles.md](../tls-profiles.md)) |
| `tls.trust` | `native` | `native`, `platform` or `bundle` ([../api/tls.md](../api/tls.md)) |
| `tls.ca_paths` | none | CA PEM files, one path or a list |
| `tls.client_cert_path`, `tls.client_key_path` | none | client certificate for mutual TLS, both or neither |
| `tls.pin_sha256` | none | SHA-256 pins on the receiver's public key, checked after its chain ([key pins](../api/tls.md#client-trust)) |
| `tls.server_name` | host of `address` | the name the receiver's certificate is checked against |
| `tls.key_share` | `profile` | `classical` sends a classical key share first, for a receiver behind a middlebox that drops the larger one |
| `filters_out` | none | outbound filters on each payload ([filter-engine.md](filter-engine.md)) |
| `listen` | none | the listeners, see [Receiving](#receiving) |

A `tls` section with `protocol: tcp` or `udp` is refused at build, and so are a `proxy` other than `none` with `udp` or beside a resolver, an address with no port, a `max_message_bytes` under 480 or, for UDP, over 65507, a `relp_window` of 0, and an empty `hostname_header` or `msg_id_header`.

All carry `transport="syslog"`: `transport_sent_total` (messages), `transport_sent_bytes_total` (message bytes before framing), `transport_send_errors_total`, `transport_backpressured_total`, `transport_send_duration_seconds`, `transport_message_too_large_total`, `transport_truncated_total`, and `transport_redelivered_total` with `reason` `reconnect` (a RELP resend in the call) or `deadline`.
