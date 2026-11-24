// Project:   scalo
// File:      src/transport/otlp/record.rs
// Purpose:   A Record as an OTLP LogRecord, and the request around a block
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema;

use super::config::OtlpConfig;
use crate::transport::work_batch::Record;

/// The record header whose value becomes the log record's severity, instead
/// of an attribute.
pub const SEVERITY_HEADER: &str = "severity";

/// The record header whose value, decimal nanoseconds since the epoch, becomes
/// the log record's time in place of `metadata.timestamp_ms`, instead of an
/// attribute.
pub const TIME_UNIX_NANO_HEADER: &str = "time_unix_nano";

/// The resource every log export carries, and its schema URL.
#[derive(Debug, Clone)]
pub(super) struct OtlpResource {
    resource: Resource,
    schema_url: String,
}

impl OtlpResource {
    /// `service.name`, the replica, host and version identity,
    /// `OTEL_RESOURCE_ATTRIBUTES` and the SDK's own attributes, with
    /// `config.resource_attributes` set last so they win.
    pub(super) fn new(config: &OtlpConfig) -> Self {
        let mut builder = opentelemetry_sdk::Resource::builder();
        if let Some(name) = config.service_name.as_deref().filter(|n| !n.is_empty()) {
            builder = builder.with_service_name(name.to_string());
        }
        builder = builder.with_attributes(crate::otel_resource::instance_attributes());
        for (key, value) in &config.resource_attributes {
            builder =
                builder.with_attribute(opentelemetry::KeyValue::new(key.clone(), value.clone()));
        }
        let converted = ResourceAttributesWithSchema::from(&builder.build());
        Self {
            resource: Resource {
                attributes: converted.attributes.0,
                ..Resource::default()
            },
            schema_url: converted.schema_url.unwrap_or_default(),
        }
    }

    /// One export request holding `records` under this resource.
    pub(super) fn request<'a>(
        &self,
        records: impl IntoIterator<Item = &'a Record>,
        observed_unix_nano: u64,
    ) -> ExportLogsServiceRequest {
        let log_records = records
            .into_iter()
            .map(|record| log_record(record, observed_unix_nano))
            .collect();
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(self.resource.clone()),
                scope_logs: vec![ScopeLogs {
                    log_records,
                    ..ScopeLogs::default()
                }],
                schema_url: self.schema_url.clone(),
            }],
        }
    }
}

/// `record` as a log record: the payload as the body, the
/// [`TIME_UNIX_NANO_HEADER`] or else the timestamp as the time, the
/// [`SEVERITY_HEADER`] as the severity and every other header as an
/// attribute. The key is not sent.
pub(super) fn log_record(record: &Record, observed_unix_nano: u64) -> LogRecord {
    let mut log = LogRecord {
        time_unix_nano: record.metadata.timestamp_ms.map_or(0, unix_nano),
        observed_time_unix_nano: observed_unix_nano,
        body: Some(any_value(&record.payload)),
        ..LogRecord::default()
    };
    for (name, value) in &record.headers {
        if name == SEVERITY_HEADER
            && let Ok(text) = std::str::from_utf8(value)
        {
            log.severity_number = i32::from(severity(text));
            log.severity_text = text.to_string();
            continue;
        }
        if name == TIME_UNIX_NANO_HEADER
            && let Some(nanos) = std::str::from_utf8(value)
                .ok()
                .and_then(|text| text.trim().parse::<u64>().ok())
        {
            log.time_unix_nano = nanos;
            continue;
        }
        log.attributes.push(KeyValue {
            key: name.clone(),
            value: Some(any_value(value)),
            ..KeyValue::default()
        });
    }
    tracing::trace!(
        bytes = record.payload.len(),
        attributes = log.attributes.len(),
        severity = log.severity_number,
        "OTLP log record built"
    );
    log
}

/// A string value for UTF-8, bytes otherwise.
fn any_value(bytes: &[u8]) -> AnyValue {
    let value = match std::str::from_utf8(bytes) {
        Ok(text) => any_value::Value::StringValue(text.to_string()),
        Err(_) => any_value::Value::BytesValue(bytes.to_vec()),
    };
    AnyValue { value: Some(value) }
}

/// Milliseconds since the epoch as nanoseconds, 0 (unknown) before it.
fn unix_nano(timestamp_ms: i64) -> u64 {
    u64::try_from(timestamp_ms).map_or(0, |ms| ms.saturating_mul(1_000_000))
}

/// Nanoseconds since the epoch now, the observed time of every record sent.
pub(super) fn now_unix_nano() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// The severity number for a level name, as the OpenTelemetry log data
/// model maps syslog levels, or for a number from 1 to 24 as given.
fn severity(text: &str) -> SeverityNumber {
    let text = text.trim();
    if let Ok(number) = text.parse::<i32>() {
        return SeverityNumber::try_from(number)
            .ok()
            .filter(|n| *n != SeverityNumber::Unspecified)
            .unwrap_or(SeverityNumber::Unspecified);
    }
    match text.to_ascii_lowercase().as_str() {
        "trace" => SeverityNumber::Trace,
        "debug" => SeverityNumber::Debug,
        "info" | "information" | "informational" => SeverityNumber::Info,
        "notice" => SeverityNumber::Info2,
        "warn" | "warning" => SeverityNumber::Warn,
        "error" | "err" => SeverityNumber::Error,
        "crit" | "critical" => SeverityNumber::Error2,
        "alert" => SeverityNumber::Error3,
        "fatal" | "emerg" | "emergency" => SeverityNumber::Fatal,
        _ => SeverityNumber::Unspecified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{PayloadFormat, RecordMeta};

    fn record(payload: &'static [u8], headers: &[(&str, &[u8])], ts: Option<i64>) -> Record {
        Record {
            payload: bytes::Bytes::from_static(payload),
            key: Some("routing-key".into()),
            headers: headers
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.to_vec()))
                .collect(),
            metadata: RecordMeta {
                timestamp_ms: ts,
                format: PayloadFormat::Json,
            },
        }
    }

    fn text(value: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }
    }

    fn raw(value: &[u8]) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::BytesValue(value.to_vec())),
        }
    }

    /// (case, record, body, time, severity number and text, attributes)
    type Case = (
        &'static str,
        Record,
        AnyValue,
        u64,
        (i32, &'static str),
        Vec<(&'static str, AnyValue)>,
    );

    #[test]
    fn a_record_maps_onto_a_log_record() {
        let observed = 42;
        let cases: Vec<Case> = vec![
            (
                "utf-8 body, timestamp, headers",
                record(
                    br#"{"msg":"hi"}"#,
                    &[("tenant", b"blue"), ("blob", &[0xff, 0x00])],
                    Some(1_700_000_000_123),
                ),
                text(r#"{"msg":"hi"}"#),
                1_700_000_000_123_000_000,
                (0, ""),
                vec![("tenant", text("blue")), ("blob", raw(&[0xff, 0x00]))],
            ),
            (
                "bytes body, no timestamp",
                record(&[0x80, 0x81], &[], None),
                raw(&[0x80, 0x81]),
                0,
                (0, ""),
                vec![],
            ),
            (
                "a time before the epoch is unknown",
                record(b"x", &[], Some(-5)),
                text("x"),
                0,
                (0, ""),
                vec![],
            ),
            (
                "severity by name is not an attribute",
                record(b"x", &[("severity", b"Warning"), ("a", b"b")], None),
                text("x"),
                0,
                (13, "Warning"),
                vec![("a", text("b"))],
            ),
            (
                "severity by number",
                record(b"x", &[("severity", b"18")], None),
                text("x"),
                0,
                (18, "18"),
                vec![],
            ),
            (
                "an unknown severity keeps its text",
                record(b"x", &[("severity", b"loud")], None),
                text("x"),
                0,
                (0, "loud"),
                vec![],
            ),
            (
                "a number out of range is unspecified",
                record(b"x", &[("severity", b"25")], None),
                text("x"),
                0,
                (0, "25"),
                vec![],
            ),
            (
                "a severity that is not UTF-8 stays an attribute",
                record(b"x", &[("severity", &[0xff])], None),
                text("x"),
                0,
                (0, ""),
                vec![("severity", raw(&[0xff]))],
            ),
            (
                "nanoseconds win over the millisecond timestamp",
                record(
                    b"x",
                    &[("time_unix_nano", b"1700000000123456789")],
                    Some(1_700_000_000_123),
                ),
                text("x"),
                1_700_000_000_123_456_789,
                (0, ""),
                vec![],
            ),
            (
                "nanoseconds that are not a number stay an attribute",
                record(b"x", &[("time_unix_nano", b"-1")], Some(5)),
                text("x"),
                5_000_000,
                (0, ""),
                vec![("time_unix_nano", text("-1"))],
            ),
        ];
        for (case, record, body, time, (number, severity_text), attributes) in cases {
            let log = log_record(&record, observed);
            assert_eq!(log.body, Some(body), "{case}: body");
            assert_eq!(log.time_unix_nano, time, "{case}: time");
            assert_eq!(log.observed_time_unix_nano, observed, "{case}: observed");
            assert_eq!(
                (log.severity_number, log.severity_text.as_str()),
                (number, severity_text),
                "{case}: severity"
            );
            let got: Vec<(&str, Option<AnyValue>)> = log
                .attributes
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.clone()))
                .collect();
            let want: Vec<(&str, Option<AnyValue>)> =
                attributes.into_iter().map(|(k, v)| (k, Some(v))).collect();
            assert_eq!(got, want, "{case}: attributes");
        }
    }

    #[test]
    fn each_level_name_has_its_severity() {
        use SeverityNumber as S;
        for (names, want) in [
            (&["trace", "TRACE"][..], S::Trace),
            (&["debug"], S::Debug),
            (&["info", "Information", "informational"], S::Info),
            (&["notice"], S::Info2),
            (&["warn", "warning"], S::Warn),
            (&["error", "err"], S::Error),
            (&["crit", "critical"], S::Error2),
            (&["alert"], S::Error3),
            (&["fatal", "emerg", "emergency"], S::Fatal),
            (&["", "0", "verbose"], S::Unspecified),
        ] {
            for name in names {
                assert_eq!(severity(name), want, "{name}");
            }
        }
    }

    /// The resource carries the configured attributes over the replica
    /// identity, and every record of a block under one scope.
    #[test]
    fn a_request_carries_the_resource_and_every_record() {
        let config = OtlpConfig {
            service_name: Some("shipper".into()),
            resource_attributes: [("service.instance.id".to_string(), "pinned".to_string())]
                .into_iter()
                .collect(),
            ..OtlpConfig::default()
        };
        let resource = OtlpResource::new(&config);
        let records = [record(b"a", &[], None), record(b"b", &[], None)];
        let request = resource.request(&records, 7);

        assert_eq!(request.resource_logs.len(), 1);
        let logs = &request.resource_logs[0];
        let attribute = |key: &str| {
            logs.resource
                .as_ref()
                .and_then(|r| r.attributes.iter().find(|kv| kv.key == key))
                .and_then(|kv| kv.value.clone())
        };
        assert_eq!(attribute("service.name"), Some(text("shipper")));
        assert_eq!(
            attribute("service.instance.id"),
            Some(text("pinned")),
            "a configured attribute wins over the replica identity"
        );
        assert_eq!(logs.scope_logs.len(), 1);
        let bodies: Vec<Option<AnyValue>> = logs.scope_logs[0]
            .log_records
            .iter()
            .map(|l| l.body.clone())
            .collect();
        assert_eq!(bodies, vec![Some(text("a")), Some(text("b"))]);
    }
}
