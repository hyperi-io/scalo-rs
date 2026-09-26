// Project:   scalo
// File:      benches/parse_guard.rs
// Purpose:   JSON depth guard cost against the byte-at-a-time guard it replaced
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! JSON depth guard benchmarks.
//!
//! Every input runs through the byte-at-a-time guard the block scan replaced,
//! through [`json_depth_within`], and through a `sonic_rs::Value` parse of the
//! same bytes for scale. Each iteration guards every record of the input once.
//!
//! - `identity_events`: 24 generated records shaped like an identity
//!   provider's audit log -- nested actor, client and request objects, about
//!   1.8 KB each.
//! - `string_heavy`: one record whose three message fields hold 12 KiB of
//!   text with escaped quotes, backslashes and brackets.
//! - `deep_60`: one record nested 60 levels, legal under the default bound.
//! - `corpus`: each non-empty line of the NDJSON file named by
//!   `SCALO_BENCH_JSON_CORPUS`, when that is set.
//!
//! ```text
//! cargo bench --bench parse_guard --features transport
//! env SCALO_BENCH_JSON_CORPUS=/path/to/events.ndjson cargo bench --bench parse_guard --features transport
//! ```

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use scalo::parse_guard::{MAX_PARSE_DEPTH, json_depth_within};

/// Records in the generated audit-log input.
const IDENTITY_EVENTS: usize = 24;

/// Nesting of the deep input, four levels inside the default bound.
const DEEP: usize = 60;

/// The byte-at-a-time guard the block scan replaced, the baseline it is measured against.
#[inline(never)]
fn byte_guard(payload: &[u8], max: usize) -> bool {
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &b in payload {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

/// One audit-log record; `i` varies the identifiers, addresses and times.
fn identity_event(i: usize) -> Vec<u8> {
    let ip = i % 254 + 1;
    let minute = i % 60;
    let second = (i * 7) % 60;
    format!(
        concat!(
            r#"{{"actor":{{"alternateId":"user{i}@example.com","detailEntry":null,"displayName":"User {i}","#,
            r#""id":"00u{i:017}","type":"User"}},"authenticationContext":{{"authenticationProvider":null,"#,
            r#""authenticationStep":0,"credentialProvider":null,"credentialType":null,"#,
            r#""externalSessionId":"102{i:022}","interface":null,"issuer":null}},"client":{{"device":"Computer","#,
            r#""geographicalContext":{{"city":"Springfield","country":"Exampleland","geolocation":{{"lat":37.7201,"#,
            r#""lon":-121.919}},"postalCode":"9{i:04}","state":"Region"}},"id":null,"ipAddress":"198.51.100.{ip}","#,
            r#""userAgent":{{"browser":"FIREFOX","os":"Linux","#,
            r#""rawUserAgent":"Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"}},"#,
            r#""zone":"null"}},"debugContext":{{"debugData":{{"requestId":"req{i:024}","#,
            r#""requestUri":"/login/signout","threatSuspected":"false","#,
            r#""url":"/login/signout?message=session_expired&next=%2Fapp%2F{i}"}}}},"#,
            r#""displayMessage":"User logout","eventType":"user.session.end","#,
            r#""legacyEventType":"core.user_auth.logout_success","outcome":{{"reason":null,"result":"SUCCESS"}},"#,
            r#""published":"2026-09-26T12:{minute:02}:{second:02}.843Z","request":{{"ipChain":[{{"#,
            r#""geographicalContext":{{"city":"Springfield","country":"Exampleland","geolocation":{{"lat":37.7201,"#,
            r#""lon":-121.919}},"postalCode":"9{i:04}","state":"Region"}},"ip":"198.51.100.{ip}","source":null,"#,
            r#""version":"V4"}}]}},"securityContext":{{"asNumber":null,"asOrg":null,"domain":null,"isProxy":null,"#,
            r#""isp":null}},"severity":"INFO","target":[{{"alternateId":"app{i}","displayName":"App \"{i}\"","#,
            r#""id":"0oa{i:017}","type":"AppInstance"}}],"transaction":{{"detail":{{}},"id":"tx{i:025}","#,
            r#""type":"WEB"}},"uuid":"{i:08x}-4f77-11ea-97fb-5925e98228bd","version":"0"}}"#,
        ),
        i = i,
        ip = ip,
        minute = minute,
        second = second,
    )
    .into_bytes()
}

/// One record with three 4 KiB message fields full of escapes and brackets.
fn string_heavy() -> Vec<u8> {
    let line = r#"GET /api/v1/items?filter={\"tags\":[\"a\",\"b\"]} 200 in \\\\share\\logs [ok] {\"ms\":12}\n"#;
    let field = line.repeat(4096 / line.len() + 1);
    format!(
        r#"{{"host":"web-01","level":"info","message":"{field}","stack_trace":"{field}","raw":"{field}"}}"#
    )
    .into_bytes()
}

/// One record nested `levels` deep, objects and arrays alternating, a string at each object.
fn deep(levels: usize) -> Vec<u8> {
    let mut record = String::new();
    for level in 0..levels {
        if level % 2 == 0 {
            record.push_str(r#"{"name":"level "#);
            record.push_str(&level.to_string());
            record.push_str(r#"","child":"#);
        } else {
            record.push('[');
        }
    }
    record.push_str(r#""leaf""#);
    for level in (0..levels).rev() {
        record.push(if level % 2 == 0 { '}' } else { ']' });
    }
    record.into_bytes()
}

/// Each non-empty line of the NDJSON file named by `SCALO_BENCH_JSON_CORPUS`.
fn corpus() -> Option<Vec<Vec<u8>>> {
    let path = std::env::var_os("SCALO_BENCH_JSON_CORPUS")?;
    let raw = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let records: Vec<Vec<u8>> = raw
        .split(|&b| b == b'\n')
        .filter(|line| line.iter().any(|b| !b.is_ascii_whitespace()))
        .map(<[u8]>::to_vec)
        .collect();
    assert!(!records.is_empty(), "no records in {}", path.display());
    Some(records)
}

fn bench_input(c: &mut Criterion, name: &str, records: &[Vec<u8>]) {
    let bytes: usize = records.iter().map(Vec::len).sum();
    for record in records {
        // Every record is legal, so each guard reads it to the end rather than stopping early.
        assert!(
            byte_guard(record, MAX_PARSE_DEPTH),
            "{name} nests past the bound"
        );
        assert!(
            json_depth_within(record, MAX_PARSE_DEPTH),
            "{name} disagrees with the byte guard"
        );
    }
    eprintln!(
        "{name}: {} records, {bytes} bytes, mean {} bytes",
        records.len(),
        bytes / records.len()
    );

    let mut group = c.benchmark_group(format!("parse_guard/{name}"));
    group.throughput(Throughput::Elements(records.len() as u64));
    group.bench_function("byte_guard", |b| {
        b.iter(|| {
            records
                .iter()
                .filter(|r| byte_guard(black_box(r), MAX_PARSE_DEPTH))
                .count()
        });
    });
    group.bench_function("block_guard", |b| {
        b.iter(|| {
            records
                .iter()
                .filter(|r| json_depth_within(black_box(r), MAX_PARSE_DEPTH))
                .count()
        });
    });
    group.bench_function("sonic_rs_value", |b| {
        b.iter(|| {
            records
                .iter()
                .filter(|r| sonic_rs::from_slice::<sonic_rs::Value>(black_box(r)).is_ok())
                .count()
        });
    });
    group.finish();
}

fn benches(c: &mut Criterion) {
    let deep_record = deep(DEEP);
    assert!(json_depth_within(&deep_record, DEEP));
    assert!(!json_depth_within(&deep_record, DEEP - 1));

    let identity: Vec<Vec<u8>> = (0..IDENTITY_EVENTS).map(identity_event).collect();
    bench_input(c, "identity_events", &identity);
    bench_input(c, "string_heavy", &[string_heavy()]);
    bench_input(c, "deep_60", &[deep_record]);
    if let Some(records) = corpus() {
        bench_input(c, "corpus", &records);
    }
}

criterion_group!(parse_guard, benches);
criterion_main!(parse_guard);
