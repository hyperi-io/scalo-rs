# Syslog

The `transport-syslog` feature sends records to a syslog receiver as `type: syslog`. Each record's payload is the message body, after an RFC 5424 or RFC 3164 header. It sends only.

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
- The time is the record's timestamp, else the send time. RFC 5424 writes it in UTC to the microsecond, RFC 3164 in the host's zone to the second.
- HOSTNAME, APP-NAME and PROCID come from config. Unset, they are the machine's host name, the executable's name and the process id. RFC 3164 keeps letters, digits, `-`, `_` and `.` in the tag, up to 32.
- MSGID is the record's key, else `msg_id`, else `-`. `send(destination, payload)` uses the destination.
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
| Connect refused or timed out, DNS failed | `Backpressured` |
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

---

## Config

`transport.syslog`, or the `syslog` section of a transport config. Every key goes through the cascade, YAML and environment alike.

| Key | Default | Meaning |
| --- | --- | --- |
| `address` | required | `host:port` or `[v6]:port`, resolved on each connect |
| `protocol` | `tcp` | `tcp`, `tls`, `udp` or `relp` |
| `framing` | `octet_counting` | `octet_counting` or `lf`, for `tcp` and `tls` |
| `format` | `rfc5424` | `rfc5424` or `rfc3164` |
| `passthrough` | `false` | send a payload starting with a `<PRI>` as it is |
| `facility` | `user` | `kern`, `user`, `mail`, `daemon`, `auth`, `syslog`, `lpr`, `news`, `uucp`, `cron`, `authpriv`, `ftp`, `ntp`, `audit`, `alert`, `clock`, `local0` to `local7` |
| `severity` | `info` | `emerg`, `alert`, `crit`, `err`, `warning`, `notice`, `info`, `debug` |
| `hostname` | host name | HOSTNAME |
| `app_name` | executable name | APP-NAME, the RFC 3164 tag |
| `proc_id` | process id | PROCID |
| `msg_id` | `-` | MSGID for a record with no key |
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

A `tls` section with `protocol: tcp` or `udp` is refused at build, and so are a `proxy` other than `none` with `udp`, an address with no port, a `max_message_bytes` under 480 or, for UDP, over 65507, and a `relp_window` of 0.

All carry `transport="syslog"`: `transport_sent_total` (messages), `transport_sent_bytes_total` (message bytes before framing), `transport_send_errors_total`, `transport_backpressured_total`, `transport_send_duration_seconds`, `transport_message_too_large_total`, `transport_truncated_total`, and `transport_redelivered_total` with `reason` `reconnect` (a RELP resend in the call) or `deadline`.
