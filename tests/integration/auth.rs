// Project:   scalo
// File:      tests/integration/auth.rs
// Purpose:   Credential acquisition, caching and placement against a real server
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The acquisition and placement halves of `auth`, against a real token
//! endpoint.
//!
//! The fixture is an axum router on an ephemeral port, holding the listener it
//! was bound on rather than dropping it to hand the port over. It records every
//! exchange, every form it was posted and every header it was called with, so
//! an assertion is what went over the wire.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use reqwest::header::HeaderName;
use scalo::auth::{
    BasicPlacement, Cached, ClientCredentials, CredentialSource, HeaderPlacement, MetadataServer,
    QueryPlacement, Static, TokenPost, TokenReading,
};
use scalo::http_client::{HttpClient, HttpClientConfig};
use scalo::sensitive::SensitiveString;

/// One request the fixture saw on the API route.
#[derive(Debug, Clone, Default)]
struct Seen {
    headers: HashMap<String, String>,
    query: String,
}

/// What the fixture saw.
#[derive(Debug, Default)]
struct Recorded {
    token_exchanges: u32,
    /// The form of every token exchange, in order.
    forms: Vec<Vec<(String, String)>>,
    metadata_hits: u32,
    /// The `metadata-flavor` header of every metadata call, in order.
    metadata_flavour: Vec<String>,
    api: Vec<Seen>,
}

type Shared = Arc<Mutex<Recorded>>;

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// Decode one `application/x-www-form-urlencoded` body into its pairs.
fn parse_form(body: &[u8]) -> Vec<(String, String)> {
    String::from_utf8_lossy(body)
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(name), percent_decode(value))
        })
        .collect()
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        let byte = bytes[at];
        if byte == b'+' {
            out.push(b' ');
            at += 1;
        } else if byte == b'%'
            && at + 3 <= bytes.len()
            && let Ok(decoded) = u8::from_str_radix(&raw[at + 1..at + 3], 16)
        {
            out.push(decoded);
            at += 3;
        } else {
            out.push(byte);
            at += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The token endpoint. The recipe in the path picks the response shape:
/// `long` a full hour, `due-once` a first token that is immediately due,
/// `string` an `expires_in` sent as a numeric string, `no-expiry` a response
/// that omits it, `tokenless` one carrying no token at all, `refused` a 400.
async fn token(
    State(state): State<Shared>,
    Path(recipe): Path<String>,
    body: Bytes,
) -> (StatusCode, String) {
    let nth = {
        let mut recorded = state.lock().unwrap();
        recorded.token_exchanges += 1;
        recorded.forms.push(parse_form(&body));
        recorded.token_exchanges
    };

    match recipe.as_str() {
        "refused" => {
            return (
                StatusCode::BAD_REQUEST,
                "{\"error\":\"invalid_client\"}".to_owned(),
            );
        }
        "tokenless" => {
            return (StatusCode::OK, "{\"token_type\":\"Bearer\"}".to_owned());
        }
        _ => {}
    }

    let expiry = match recipe.as_str() {
        "due-once" if nth == 1 => ",\"expires_in\":0".to_owned(),
        "string" => ",\"expires_in\":\"3600\"".to_owned(),
        "no-expiry" => String::new(),
        _ => ",\"expires_in\":3600".to_owned(),
    };
    (
        StatusCode::OK,
        format!(
            "{{\"access_token\":\"tok-{nth}\",\"token_type\":\"Bearer\"{expiry},\"instance_url\":\"https://shard-{nth}.example\"}}"
        ),
    )
}

/// A cloud metadata server: a plain GET behind a flavour header.
async fn metadata(State(state): State<Shared>, headers: HeaderMap) -> Response {
    let nth = {
        let mut recorded = state.lock().unwrap();
        recorded.metadata_hits += 1;
        recorded
            .metadata_flavour
            .push(header_value(&headers, "metadata-flavor"));
        recorded.metadata_hits
    };
    (
        StatusCode::OK,
        format!("{{\"access_token\":\"metadata-tok-{nth}\",\"expires_in\":3599}}"),
    )
        .into_response()
}

/// The protected resource: records what it was called with.
async fn api(
    State(state): State<Shared>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> StatusCode {
    let seen = Seen {
        headers: headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect(),
        query: query.unwrap_or_default(),
    };
    state.lock().unwrap().api.push(seen);
    StatusCode::OK
}

/// Bind, hold the listener, serve. The address is only handed out after the
/// server owns the socket.
async fn fixture() -> (SocketAddr, Shared) {
    let state: Shared = Arc::new(Mutex::new(Recorded::default()));
    let app = Router::new()
        .route("/token/{recipe}", post(token))
        .route("/metadata", get(metadata))
        .route("/api", get(api))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, state)
}

fn client() -> Arc<HttpClient> {
    Arc::new(
        HttpClient::new(HttpClientConfig {
            min_retry_interval_ms: 1,
            max_retry_interval_ms: 20,
            ..Default::default()
        })
        .unwrap(),
    )
}

fn client_credentials(addr: SocketAddr, recipe: &str) -> ClientCredentials {
    ClientCredentials::new(
        client(),
        format!("http://{addr}/token/{recipe}"),
        "client-42",
        SensitiveString::new("s3cr3t-do-not-print"),
    )
}

/// A cold source hit by many callers at once mints once: the renewal lock is a
/// single-flight gate, not one exchange per caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_exchange_serves_concurrent_callers() {
    let (addr, state) = fixture().await;
    let source = Arc::new(Cached::new(client_credentials(addr, "long")));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let source = Arc::clone(&source);
        tasks.spawn(async move {
            source
                .credential()
                .await
                .map(|c| c.secret.expose().to_owned())
        });
    }

    let mut secrets = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        secrets.push(joined.unwrap().unwrap());
    }

    assert_eq!(secrets.len(), 32);
    assert!(
        secrets.iter().all(|s| s == "tok-1"),
        "every caller got the one minted token: {secrets:?}"
    );
    assert_eq!(
        state.lock().unwrap().token_exchanges,
        1,
        "32 concurrent cold callers, one exchange"
    );
}

/// A hit is a pointer clone of the held credential, not a rebuilt one. An
/// implementation that returns a fresh `Credential` per call fails this.
#[tokio::test]
async fn a_cache_hit_hands_back_the_same_pointer() {
    let (addr, state) = fixture().await;
    let source = Cached::new(client_credentials(addr, "long"));

    let first = source.credential().await.unwrap();
    let second = source.credential().await.unwrap();

    assert!(
        Arc::ptr_eq(&first, &second),
        "the second call handed back the held credential"
    );
    assert_eq!(state.lock().unwrap().token_exchanges, 1);
}

/// A credential that is due is exchanged again and the new one is swapped in
/// whole -- once, not once per caller -- and the next call hits the new one.
#[tokio::test]
async fn a_due_credential_is_renewed_once_and_swapped() {
    let (addr, state) = fixture().await;
    let source = Cached::new(client_credentials(addr, "due-once"));

    // The first token is already past its renewal point when it arrives.
    let first = source.credential().await.unwrap();
    assert_eq!(first.secret.expose(), "tok-1");

    let renewed = source.credential().await.unwrap();
    assert_eq!(renewed.secret.expose(), "tok-2");
    assert!(
        !Arc::ptr_eq(&first, &renewed),
        "the renewal swapped the held pointer"
    );

    let after = source.credential().await.unwrap();
    assert!(
        Arc::ptr_eq(&renewed, &after),
        "and the fresh one is now the hit"
    );
    assert_eq!(state.lock().unwrap().token_exchanges, 2);
}

/// `scope` is absent from the form when the consumer did not set one: an empty
/// `scope=` is a request for no scopes at all, which some providers refuse
/// (RFC 6749 s4.4.2 makes the parameter optional).
#[tokio::test]
async fn scope_is_absent_when_unset() {
    let (addr, state) = fixture().await;

    let unscoped = Cached::new(client_credentials(addr, "long"));
    unscoped.credential().await.unwrap();

    let scoped = Cached::new(client_credentials(addr, "long").with_scope("read:events"));
    scoped.credential().await.unwrap();

    let forms = state.lock().unwrap().forms.clone();
    assert_eq!(forms.len(), 2);
    assert!(
        !forms[0].iter().any(|(name, _)| name == "scope"),
        "no scope key at all, not an empty one: {:?}",
        forms[0]
    );
    assert!(
        forms[0].contains(&("grant_type".to_owned(), "client_credentials".to_owned())),
        "{:?}",
        forms[0]
    );
    assert!(
        forms[1].contains(&("scope".to_owned(), "read:events".to_owned())),
        "{:?}",
        forms[1]
    );
}

/// Providers send `expires_in` as a JSON string as readily as a number; both
/// are read, and the exposed fields ride along in `extra`.
#[tokio::test]
async fn expires_in_as_a_string_is_accepted() {
    let (addr, _state) = fixture().await;
    let source = Cached::new(
        client_credentials(addr, "string")
            .with_reading(TokenReading::default().expose_field("instance_url")),
    );

    let credential = source.credential().await.unwrap();

    // An hour out, less the renewal margin: far enough ahead that the next
    // call is a hit, which an unparsed string expiry would not be.
    assert!(credential.renew_at > std::time::Instant::now() + Duration::from_secs(3000));
    let extra = credential
        .extra
        .as_deref()
        .expect("the exposed field is there");
    assert_eq!(extra["instance_url"], "https://shard-1.example");
    assert!(
        extra.get("access_token").is_none(),
        "only the named fields are exposed: {extra}"
    );
}

/// A response with no expiry at all takes the configured fallback, less the
/// renewal margin -- not zero, which would re-exchange on every call.
#[tokio::test]
async fn a_response_without_an_expiry_uses_the_fallback() {
    let (addr, _state) = fixture().await;
    let source = Cached::new(
        client_credentials(addr, "no-expiry").with_reading(
            TokenReading::default()
                .with_expires_in_fallback(Duration::from_secs(600))
                .with_renew_margin(Duration::from_secs(60)),
        ),
    );

    let credential = source.credential().await.unwrap();
    let now = std::time::Instant::now();

    assert!(credential.renew_at > now + Duration::from_secs(400));
    assert!(credential.renew_at <= now + Duration::from_secs(540));
}

/// A 2xx that carries no token is a malformed response, named as such rather
/// than cached as an empty credential.
#[tokio::test]
async fn a_response_without_a_token_is_refused() {
    let (addr, _state) = fixture().await;
    let source = Cached::new(client_credentials(addr, "tokenless"));

    let error = source.credential().await.expect_err("no access_token");

    assert!(
        error.to_string().contains("access_token"),
        "the failure names what was missing: {error}"
    );
}

/// #88's done-when: two placements over one shared source land on one request
/// and cost one exchange.
#[tokio::test]
async fn two_placements_over_one_source_land_on_one_request() {
    let (addr, state) = fixture().await;
    let source = Arc::new(Cached::new(client_credentials(addr, "long")));
    let signer = (
        HeaderPlacement::new(
            HeaderName::from_static("dd-api-key"),
            "",
            Arc::clone(&source),
        ),
        HeaderPlacement::new(
            HeaderName::from_static("dd-application-key"),
            "",
            Arc::clone(&source),
        ),
    );

    let http = client();
    let response = http
        .get_signed(&format!("http://{addr}/api"), &signer)
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let recorded = state.lock().unwrap();
    assert_eq!(recorded.api.len(), 1, "both headers rode one request");
    let seen = &recorded.api[0];
    assert_eq!(
        seen.headers.get("dd-api-key").map(String::as_str),
        Some("tok-1")
    );
    assert_eq!(
        seen.headers.get("dd-application-key").map(String::as_str),
        Some("tok-1")
    );
    assert_eq!(
        recorded.token_exchanges, 1,
        "two headers, one exchange -- the source is shared"
    );
}

/// The bearer, query and basic placements put the credential where they say,
/// over a source that does no I/O at all.
#[tokio::test]
async fn each_placement_puts_the_credential_where_it_says() {
    let (addr, state) = fixture().await;
    let http = client();
    let url = format!("http://{addr}/api");

    let bearer = HeaderPlacement::bearer(Cached::new(Static::new("static-token")));
    http.get_signed(&url, &bearer).await.unwrap();

    let query = QueryPlacement::new("token", Cached::new(Static::new("static-token")));
    http.get_signed(&url, &query).await.unwrap();

    let basic = BasicPlacement::new("account-1234", Cached::new(Static::new("licence-key")));
    http.get_signed(&url, &basic).await.unwrap();

    let recorded = state.lock().unwrap();
    assert_eq!(recorded.api.len(), 3);
    assert_eq!(
        recorded.api[0]
            .headers
            .get("authorization")
            .map(String::as_str),
        Some("Bearer static-token")
    );
    assert_eq!(recorded.api[1].query, "token=static-token");
    // account-1234:licence-key
    assert_eq!(
        recorded.api[2]
            .headers
            .get("authorization")
            .map(String::as_str),
        Some("Basic YWNjb3VudC0xMjM0OmxpY2VuY2Uta2V5")
    );
    assert_eq!(
        recorded.token_exchanges, 0,
        "a static credential exchanges nothing"
    );
}

/// A metadata server is a GET behind a header, read through the same parser.
#[tokio::test]
async fn a_metadata_server_credential_carries_its_flavour_header() {
    let (addr, state) = fixture().await;
    let source = Cached::new(
        MetadataServer::new(client(), format!("http://{addr}/metadata"))
            .with_header(HeaderName::from_static("metadata-flavor"), "Google"),
    );

    let credential = source.credential().await.unwrap();

    assert_eq!(credential.secret.expose(), "metadata-tok-1");
    let recorded = state.lock().unwrap();
    assert_eq!(recorded.metadata_hits, 1);
    assert_eq!(recorded.metadata_flavour, ["Google"]);
}

/// The generic form POST is how a client assertion reaches a token endpoint:
/// the consumer mints and signs it, scalo only posts what it is handed.
#[tokio::test]
async fn a_token_post_sends_the_rendered_form() {
    let (addr, state) = fixture().await;
    let source = Cached::new(
        TokenPost::new(client(), format!("http://{addr}/token/long"))
            .with_form_field("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
            .with_form_field("assertion", "header.payload.signature"),
    );

    let credential = source.credential().await.unwrap();

    assert_eq!(credential.secret.expose(), "tok-1");
    let forms = state.lock().unwrap().forms.clone();
    assert_eq!(forms.len(), 1);
    assert!(
        forms[0].contains(&(
            "grant_type".to_owned(),
            "urn:ietf:params:oauth:grant-type:jwt-bearer".to_owned()
        )),
        "{:?}",
        forms[0]
    );
    assert!(
        forms[0].contains(&(
            "assertion".to_owned(),
            "header.payload.signature".to_owned()
        )),
        "{:?}",
        forms[0]
    );
}

/// Nothing carries the secret out: not a debug render of the credential, not a
/// refusal from the token endpoint, not an unreachable one. Only this test
/// stops a later change putting it back.
#[tokio::test]
async fn a_credential_never_reaches_a_debug_render_or_an_error() {
    const SECRET: &str = "s3cr3t-do-not-print";
    let (addr, _state) = fixture().await;

    let held = Cached::new(Static::new(SECRET));
    let credential = held.credential().await.unwrap();
    let rendered = format!("{credential:?}");
    assert!(!rendered.contains(SECRET), "{rendered}");
    assert!(rendered.contains("REDACTED"), "{rendered}");

    let refused = Cached::new(client_credentials(addr, "refused"))
        .credential()
        .await
        .expect_err("the endpoint answered 400");
    assert!(!format!("{refused}").contains(SECRET), "{refused}");
    assert!(!format!("{refused:?}").contains(SECRET), "{refused:?}");
    assert!(
        format!("{refused}").contains("invalid_client"),
        "the refusal body is still diagnosable: {refused}"
    );

    // A port that was free and is now closed, so the connection is refused
    // rather than answered: the transport-error arm rather than a status.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = listener.local_addr().unwrap();
    drop(listener);
    let unreachable = Cached::new(client_credentials(dead, "long"))
        .credential()
        .await
        .expect_err("nothing is listening");
    assert!(!format!("{unreachable}").contains(SECRET), "{unreachable}");
    assert!(
        !format!("{unreachable:?}").contains(SECRET),
        "{unreachable:?}"
    );
}
