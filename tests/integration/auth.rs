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
use std::sync::atomic::{AtomicU64, Ordering};
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
    AnyCredentialSource, AuthError, BasicPlacement, Cached, ClientCredentials, Credential,
    CredentialSource, Exchange, HeaderPlacement, MetadataServer, Placement, QueryPlacement, Static,
    TokenPost, TokenReading,
};
use scalo::http_client::{HttpClient, HttpClientConfig, HttpError, SignError};
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
    /// Hits on the route that refuses once, in order.
    flaky_api_hits: u32,
    /// Where the `redirect` recipe sends the caller, set once both fixtures
    /// are bound.
    redirect_to: Option<String>,
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

/// The field names of a recorded form. An assertion message is printed when it
/// fails, and the values carry the client secret.
fn field_names(form: &[(String, String)]) -> Vec<&str> {
    form.iter().map(|(name, _)| name.as_str()).collect()
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
/// `long` a full hour, `short` half a minute, `due-once` a first token that is
/// immediately due, `string` an `expires_in` sent as a numeric string,
/// `no-expiry` a response that omits it, `absurd` one that will not fit an
/// `Instant`, `tokenless` one carrying no token at all, `refused` a 400,
/// `unauthorised` a 401,
/// `echo` a 400 that hands the posted form straight back, `flaky` a 503 before
/// the token, `gated` a first exchange slow enough for every other caller to
/// arrive during it, `slow` one that never answers in time, `redirect` a 307 to
/// a second listener.
async fn token(State(state): State<Shared>, Path(recipe): Path<String>, body: Bytes) -> Response {
    let (nth, redirect_to) = {
        let mut recorded = state.lock().unwrap();
        recorded.token_exchanges += 1;
        recorded.forms.push(parse_form(&body));
        (recorded.token_exchanges, recorded.redirect_to.clone())
    };

    match recipe.as_str() {
        "refused" => {
            return (StatusCode::BAD_REQUEST, "{\"error\":\"invalid_client\"}").into_response();
        }
        "unauthorised" => {
            return (StatusCode::UNAUTHORIZED, "{\"error\":\"invalid_client\"}").into_response();
        }
        "echo" => {
            return (
                StatusCode::BAD_REQUEST,
                String::from_utf8_lossy(&body).into_owned(),
            )
                .into_response();
        }
        "tokenless" => {
            return (StatusCode::OK, "{\"token_type\":\"Bearer\"}").into_response();
        }
        "redirect" => {
            let target = redirect_to.expect("the test names the second listener");
            return (
                StatusCode::TEMPORARY_REDIRECT,
                [(axum::http::header::LOCATION, target)],
            )
                .into_response();
        }
        "flaky" if nth == 1 => {
            return (StatusCode::SERVICE_UNAVAILABLE, "come back").into_response();
        }
        "slow" => tokio::time::sleep(Duration::from_secs(5)).await,
        "gated" if nth == 1 => tokio::time::sleep(Duration::from_millis(50)).await,
        _ => {}
    }

    let expiry = match recipe.as_str() {
        "due-once" if nth == 1 => ",\"expires_in\":0",
        "string" => ",\"expires_in\":\"3600\"",
        "no-expiry" => "",
        "short" => ",\"expires_in\":30",
        "absurd" => ",\"expires_in\":18446744073709551615",
        _ => ",\"expires_in\":3600",
    };
    (
        StatusCode::OK,
        format!(
            "{{\"access_token\":\"tok-{nth}\",\"token_type\":\"Bearer\"{expiry},\"instance_url\":\"https://shard-{nth}.example\"}}"
        ),
    )
        .into_response()
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

/// The protected resource again, refusing once before it serves, so a test can
/// see what a retried attempt carried.
async fn flaky_api(
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
    let mut recorded = state.lock().unwrap();
    recorded.flaky_api_hits += 1;
    recorded.api.push(seen);
    if recorded.flaky_api_hits == 1 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

fn router(state: &Shared) -> Router {
    Router::new()
        .route("/token/{recipe}", post(token))
        .route("/metadata", get(metadata))
        .route("/api", get(api))
        .route("/api-flaky", get(flaky_api))
        .with_state(Arc::clone(state))
}

/// Bind, hold the listener, serve. The address is only handed out after the
/// server owns the socket.
async fn fixture() -> (SocketAddr, Shared) {
    let state: Shared = Arc::new(Mutex::new(Recorded::default()));
    let app = router(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, state)
}

/// The same fixture, with the first connection accepted and dropped rather than
/// answered: a transport failure that the port then stops producing, which no
/// closed port can express.
async fn fixture_refusing_one_connection() -> (SocketAddr, Shared) {
    let state: Shared = Arc::new(Mutex::new(Recorded::default()));
    let app = router(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (refused, _) = listener.accept().await.unwrap();
        drop(refused);
        axum::serve(listener, app).await.unwrap();
    });
    (addr, state)
}

/// The credential every fixture exchange hands out, and the one no error,
/// debug render or exposed field is allowed to carry.
const SECRET: &str = "s3cr3t-do-not-print";

fn client() -> HttpClient {
    HttpClient::new(HttpClientConfig {
        min_retry_interval_ms: 1,
        max_retry_interval_ms: 20,
        ..Default::default()
    })
    .unwrap()
}

fn client_credentials(addr: SocketAddr, recipe: &str) -> ClientCredentials {
    ClientCredentials::new(
        &client(),
        format!("http://{addr}/token/{recipe}"),
        "client-42",
        SensitiveString::new(SECRET),
    )
    .expect("a loopback token endpoint")
}

/// A cold source hit by many callers at once mints once: the renewal lock is a
/// single-flight gate, not one exchange per caller.
///
/// The first exchange is slow enough that every other caller is inside
/// `credential()` while it runs, so an ungated implementation records more than
/// one exchange rather than winning the race by being quick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_exchange_serves_concurrent_callers() {
    let (addr, state) = fixture().await;
    let source = Arc::new(Cached::new(client_credentials(addr, "gated")));

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
        field_names(&forms[0])
    );
    assert!(
        forms[0].contains(&("grant_type".to_owned(), "client_credentials".to_owned())),
        "{:?}",
        field_names(&forms[0])
    );
    assert!(
        forms[1].contains(&("scope".to_owned(), "read:events".to_owned())),
        "{:?}",
        field_names(&forms[1])
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

/// Two placements over one shared source land on one request and cost one
/// exchange, which is what a provider wanting two headers needs.
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
        MetadataServer::new(&client(), format!("http://{addr}/metadata"))
            .expect("a metadata endpoint")
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
        TokenPost::new(&client(), format!("http://{addr}/token/long"))
            .expect("a loopback token endpoint")
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
        field_names(&forms[0])
    );
    assert!(
        forms[0].contains(&(
            "assertion".to_owned(),
            "header.payload.signature".to_owned()
        )),
        "{:?}",
        field_names(&forms[0])
    );
}

/// A minted post renders its form on every exchange, so a renewal carries a
/// fresh single-use assertion rather than the one the endpoint already spent.
#[tokio::test]
async fn a_minted_token_post_sends_a_fresh_assertion_on_every_renewal() {
    let (addr, state) = fixture().await;
    let minted = AtomicU64::new(0);
    let source = Cached::new(
        TokenPost::minted(
            &client(),
            format!("http://{addr}/token/due-once"),
            move || {
                let jti = minted.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(vec![
                    (
                        "grant_type".to_owned(),
                        "urn:ietf:params:oauth:grant-type:jwt-bearer".to_owned(),
                    ),
                    (
                        "assertion".to_owned(),
                        format!("header.jti-{jti}.signature"),
                    ),
                ])
            },
        )
        .expect("a loopback token endpoint"),
    );

    // The first token is already past its renewal point when it arrives, so
    // the second call is a renewal through the cache.
    let first = source.credential().await.unwrap();
    let renewed = source.credential().await.unwrap();

    assert_eq!(first.secret.expose(), "tok-1");
    assert_eq!(renewed.secret.expose(), "tok-2");
    let assertions: Vec<String> = state
        .lock()
        .unwrap()
        .forms
        .iter()
        .filter_map(|form| {
            form.iter()
                .find(|(name, _)| name == "assertion")
                .map(|(_, value)| value.clone())
        })
        .collect();
    assert_eq!(
        assertions,
        ["header.jti-1.signature", "header.jti-2.signature"],
        "each exchange posted its own assertion"
    );
}

/// A form the consumer cannot mint is handed back as the consumer's own
/// failure, and nothing reaches the endpoint.
#[tokio::test]
async fn a_form_that_cannot_be_minted_is_an_error_and_posts_nothing() {
    let (addr, state) = fixture().await;
    let source = Cached::new(
        TokenPost::minted(&client(), format!("http://{addr}/token/long"), || {
            Err(AuthError::Unavailable {
                reason: "signing key not readable".to_owned(),
            })
        })
        .expect("a loopback token endpoint"),
    );

    let error = source.credential().await.expect_err("nothing to post");

    assert!(
        matches!(error, AuthError::Unavailable { .. }),
        "the render's own failure: {error:?}"
    );
    assert!(!error.is_transient());
    assert_eq!(
        state.lock().unwrap().token_exchanges,
        0,
        "nothing was posted"
    );
}

// Every channel a credential could leave by, one test each.

/// Channel: a debug render of the credential itself.
#[tokio::test]
async fn a_debug_render_of_a_credential_carries_neither_secret_nor_extra() {
    let credential = Cached::new(Static::new(SECRET)).credential().await.unwrap();

    let rendered = format!("{credential:?}");

    assert!(!rendered.contains(SECRET), "{rendered}");
    assert!(rendered.contains("REDACTED"), "{rendered}");
}

/// Channel: the error text of a refusal, from an endpoint that hands the form
/// it was posted straight back.
#[tokio::test]
async fn a_refusal_that_echoes_the_form_keeps_the_secret_out_of_the_error() {
    let (addr, _state) = fixture().await;

    let error = Cached::new(client_credentials(addr, "echo"))
        .credential()
        .await
        .expect_err("the endpoint answered 400");

    assert!(!format!("{error}").contains(SECRET), "{error}");
    assert!(!format!("{error:?}").contains(SECRET), "{error:?}");
    assert!(
        format!("{error}").contains("400"),
        "the status is still named: {error}"
    );
}

/// Channel: a transport error on a call whose credential is in the query, which
/// is where reqwest's own copy of the request URL puts it.
#[tokio::test]
async fn a_transport_error_on_a_query_signed_call_carries_no_secret() {
    // A port that was free and is now closed, so the connection is refused
    // rather than answered: the transport-error arm rather than a status.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = listener.local_addr().unwrap();
    drop(listener);

    let placement = QueryPlacement::new("token", Cached::new(Static::new(SECRET)));
    let error = client()
        .get_signed(&format!("http://{dead}/api"), &placement)
        .await
        .expect_err("nothing is listening");

    assert!(!format!("{error}").contains(SECRET), "{error}");
    assert!(!format!("{error:?}").contains(SECRET), "{error:?}");
}

/// Channel: the endpoint the error variant names, when the token URL itself
/// carries the credential in its query.
#[tokio::test]
async fn the_endpoint_an_error_names_carries_no_query() {
    let (addr, _state) = fixture().await;
    let source = Cached::new(
        ClientCredentials::new(
            &client(),
            format!("http://{addr}/token/refused?wrapping={SECRET}"),
            "client-42",
            SensitiveString::new(SECRET),
        )
        .unwrap(),
    );

    let error = source.credential().await.expect_err("the endpoint refused");

    assert!(!format!("{error}").contains(SECRET), "{error}");
    assert!(!format!("{error:?}").contains(SECRET), "{error:?}");
    assert!(
        format!("{error}").contains("/token/refused"),
        "the endpoint is still named by host and path: {error}"
    );
}

/// Channel: `Credential::extra`, which a consumer reads and renders.
#[tokio::test]
async fn a_credential_field_is_never_carried_in_extra() {
    let (addr, _state) = fixture().await;
    let source = Cached::new(
        client_credentials(addr, "long")
            .with_reading(TokenReading::default().expose_field("access_token")),
    );

    let credential = source.credential().await.unwrap();

    let rendered = credential
        .extra
        .as_deref()
        .map(ToString::to_string)
        .unwrap_or_default();
    assert!(
        !rendered.contains("tok-1"),
        "the token is not exposable: {rendered}"
    );
}

/// Channel: the error from a credential that cannot be rendered as a header
/// value, which is the one place the value reaches a formatter.
#[tokio::test]
async fn a_credential_that_is_not_a_header_value_is_refused_without_naming_it() {
    let (addr, _state) = fixture().await;
    let smuggled = format!("{SECRET}\r\nx-injected: 1");
    let placement = HeaderPlacement::bearer(Cached::new(Static::new(smuggled)));

    let error = client()
        .get_signed(&format!("http://{addr}/api"), &placement)
        .await
        .expect_err("a header value cannot carry CRLF");

    assert!(!format!("{error}").contains(SECRET), "{error}");
    assert!(!format!("{error:?}").contains(SECRET), "{error:?}");
}

/// A failed acquisition is one failure shared with every caller that waited on
/// it, and the endpoint is not hit again until the backoff has passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_acquisition_is_shared_with_every_waiter() {
    let (addr, state) = fixture().await;
    let source = Arc::new(Cached::new(client_credentials(addr, "refused")));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let source = Arc::clone(&source);
        tasks.spawn(async move {
            source
                .credential()
                .await
                .map_err(|e| e.to_string())
                .expect_err("the endpoint refuses every caller")
        });
    }

    let mut failures = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        failures.push(joined.unwrap());
    }

    assert_eq!(
        state.lock().unwrap().token_exchanges,
        1,
        "one refusal, not one per caller"
    );
    assert!(
        failures.iter().all(|failure| *failure == failures[0]),
        "every waiter was handed the same failure: {failures:?}"
    );
}

/// A hung endpoint is bounded by the exchange's own timeout and the failure is
/// then shared, so callers do not queue up one timeout each -- and every one of
/// them is handed a failure worth retrying, not a refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_endpoint_does_not_park_every_caller_in_turn() {
    let (addr, _state) = fixture().await;
    let http = HttpClient::new(HttpClientConfig {
        timeout_secs: 1,
        max_retries: 0,
        ..Default::default()
    })
    .unwrap();
    let source = Arc::new(Cached::new(
        ClientCredentials::new(
            &http,
            format!("http://{addr}/token/slow"),
            "client-42",
            SensitiveString::new(SECRET),
        )
        .unwrap(),
    ));

    let started = std::time::Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let source = Arc::clone(&source);
        tasks.spawn(async move { source.credential().await.err() });
    }
    let mut failures = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        failures.push(joined.unwrap().expect("the endpoint never answers"));
    }
    let elapsed = started.elapsed();

    assert_eq!(failures.len(), 8);
    assert!(
        elapsed < Duration::from_secs(4),
        "eight callers cost one timeout between them, not eight: {elapsed:?}"
    );
    assert!(
        failures.iter().all(AuthError::is_transient),
        "a waiter on a timeout retries the same way the runner does: {failures:?}"
    );
}

/// The `expires_in` a token endpoint can send is any `u64`, and one near the
/// top of the range is held under the renewal lock: a lifetime that overflowed
/// an `Instant` there would take the process down on the provider's say-so.
#[tokio::test]
async fn an_expiry_that_will_not_fit_an_instant_is_a_credential_not_a_panic() {
    let (addr, state) = fixture().await;
    let source = Cached::new(client_credentials(addr, "absurd"));

    let credential = source.credential().await.unwrap();
    let again = source.credential().await.unwrap();

    assert_eq!(credential.secret.expose(), "tok-1");
    assert!(Arc::ptr_eq(&credential, &again), "held, not renewed");
    assert_eq!(state.lock().unwrap().token_exchanges, 1);
}

/// A renew margin at or over the lifetime holds half the lifetime rather than
/// making every request its own exchange.
#[tokio::test]
async fn a_lifetime_shorter_than_the_margin_is_still_held() {
    let (addr, state) = fixture().await;
    let source = Cached::new(client_credentials(addr, "short"));

    for _ in 0..5 {
        source.credential().await.unwrap();
    }

    let exchanges = state.lock().unwrap().token_exchanges;
    assert!(
        matches!(exchanges, 1 | 2),
        "five calls over a 30s credential cost one exchange, not five: {exchanges}"
    );
}

/// A token endpoint that 307s elsewhere does not get the form reposted to the
/// host it names: the exchange's client refuses redirects.
#[tokio::test]
async fn a_redirected_token_exchange_does_not_repost_the_form() {
    let (addr, state) = fixture().await;
    let (elsewhere, elsewhere_state) = fixture().await;
    state.lock().unwrap().redirect_to = Some(format!("http://{elsewhere}/token/long"));

    let error = Cached::new(client_credentials(addr, "redirect"))
        .credential()
        .await
        .expect_err("a redirect is not a token");

    assert!(
        elsewhere_state.lock().unwrap().forms.is_empty(),
        "the second listener was never posted the form: {error}"
    );
}

/// A client-credentials POST is retried on the exchange's own client whatever
/// the template's non-idempotent retry flag says, because a client secret is
/// safe to resend -- and the template's flag is off here.
#[tokio::test]
async fn the_exchange_retries_its_own_token_post() {
    let (addr, state) = fixture().await;
    assert!(
        !client().config().retry_non_idempotent,
        "the shared client does not replay a POST"
    );

    let credential = Cached::new(client_credentials(addr, "flaky"))
        .credential()
        .await
        .unwrap();

    assert_eq!(credential.secret.expose(), "tok-2");
    assert_eq!(state.lock().unwrap().token_exchanges, 2);
}

/// A rendered token POST can carry a single-use assertion, so with the
/// template's retry flag off it is posted once and the refusal handed back.
#[tokio::test]
async fn a_token_post_is_not_replayed_when_the_template_says_not_to() {
    let (addr, state) = fixture().await;
    let http = client();
    assert!(
        !http.config().retry_non_idempotent,
        "the template does not replay a POST"
    );

    let result = Cached::new(
        TokenPost::new(&http, format!("http://{addr}/token/flaky"))
            .expect("a loopback token endpoint")
            .with_form_field("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
            .with_form_field("assertion", "header.payload.signature"),
    )
    .credential()
    .await;

    assert_eq!(
        state.lock().unwrap().token_exchanges,
        1,
        "the assertion was posted once, not replayed"
    );
    let error = result.expect_err("the only answer was a 503");
    assert!(
        matches!(error, AuthError::Refused { status: 503, .. }),
        "{error:?}"
    );
}

/// A consumer whose rendered form is safe to resend opts in on the template,
/// and the exchange then rides out a 503 like any retried call.
#[tokio::test]
async fn a_token_post_retries_when_the_template_opts_in() {
    let (addr, state) = fixture().await;
    let http = HttpClient::new(HttpClientConfig {
        retry_non_idempotent: true,
        min_retry_interval_ms: 1,
        max_retry_interval_ms: 20,
        ..Default::default()
    })
    .unwrap();

    let credential = Cached::new(
        TokenPost::new(&http, format!("http://{addr}/token/flaky"))
            .expect("a loopback token endpoint")
            .with_form_field("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
            .with_form_field("assertion", "header.payload.signature"),
    )
    .credential()
    .await
    .unwrap();

    assert_eq!(credential.secret.expose(), "tok-2");
    assert_eq!(state.lock().unwrap().token_exchanges, 2);
}

/// An endpoint that could not be reached is a transient signing failure, so the
/// call that needed the credential retries rather than failing outright -- and
/// that retry reaches the endpoint, rather than being handed the failure it is
/// retrying from.
#[tokio::test]
async fn an_unreachable_token_endpoint_is_retried_by_the_outer_call() {
    let (addr, state) = fixture_refusing_one_connection().await;
    let source = Cached::new(client_credentials(addr, "long"));

    let response = client()
        .get_signed(
            &format!("http://{addr}/api"),
            &HeaderPlacement::bearer(source),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let recorded = state.lock().unwrap();
    assert_eq!(
        recorded.token_exchanges, 1,
        "the dropped connection was not an exchange"
    );
    assert_eq!(recorded.api.len(), 1);
}

/// A query placement appends its parameter to the request built for this
/// attempt, so a retried call carries one copy of it and not two.
#[tokio::test]
async fn a_query_placement_appends_once_across_a_retry() {
    let (addr, state) = fixture().await;
    let placement = QueryPlacement::new("token", Cached::new(Static::new("static-token")));

    let response = client()
        .get_signed(&format!("http://{addr}/api-flaky?page=2"), &placement)
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let recorded = state.lock().unwrap();
    assert_eq!(recorded.api.len(), 2, "the 503 must have been retried");
    assert_eq!(recorded.api[1].query, "page=2&token=static-token");
}

/// A consumer that learns the provider has revoked the credential drops the
/// held one, and the next call mints a new one rather than re-serving it.
#[tokio::test]
async fn an_invalidated_credential_is_exchanged_again() {
    let (addr, state) = fixture().await;
    let source = Cached::new(client_credentials(addr, "long"));

    let first = source.credential().await.unwrap();
    source.invalidate();
    let second = source.credential().await.unwrap();

    assert_eq!(first.secret.expose(), "tok-1");
    assert_eq!(second.secret.expose(), "tok-2");
    assert_eq!(state.lock().unwrap().token_exchanges, 2);
}

/// An exchange a consumer writes gets the response reading scalo already has:
/// the required token, both `expires_in` forms, the fallback, the margin and
/// the exposed fields, off the public surface.
struct BespokeExchange {
    http: HttpClient,
    url: String,
    reading: TokenReading,
}

impl Exchange for BespokeExchange {
    async fn acquire(&self) -> Result<Credential, AuthError> {
        let response = self
            .http
            .get(&self.url)
            .await
            .map_err(|e| AuthError::Unreachable {
                url: self.url.clone(),
                source: Box::new(e),
            })?;
        let body: serde_json::Value = response.json().await.map_err(|e| AuthError::Malformed {
            url: self.url.clone(),
            reason: e.without_url().to_string(),
        })?;
        self.reading.read(&self.url, &body)
    }
}

#[tokio::test]
async fn a_consumer_written_exchange_reads_a_token_response_through_the_shared_parser() {
    let (addr, state) = fixture().await;
    let reading = TokenReading::default().with_renew_margin(Duration::from_secs(60));
    let source = Cached::new(BespokeExchange {
        http: client(),
        url: format!("http://{addr}/metadata"),
        reading: reading.clone(),
    });

    let credential = source.credential().await.unwrap();

    assert_eq!(credential.secret.expose(), "metadata-tok-1");
    assert_eq!(state.lock().unwrap().metadata_hits, 1);
    assert!(
        reading.renew_at(Duration::from_secs(3600)) > std::time::Instant::now(),
        "the renewal point is the parser's, not the consumer's arithmetic"
    );
}

/// Sources of different kinds held in one map under a name, and a list of
/// placements whose length is only known at run time, both signing one request.
#[tokio::test]
async fn a_registry_of_sources_signs_from_a_runtime_recipe() {
    let (addr, state) = fixture().await;

    let mut registry: HashMap<String, Arc<AnyCredentialSource>> = HashMap::new();
    for (name, recipe) in [("licence", "static"), ("events", "long")] {
        let source = match recipe {
            "static" => AnyCredentialSource::Static(Cached::new(Static::new("static-token"))),
            token => {
                AnyCredentialSource::ClientCredentials(Cached::new(client_credentials(addr, token)))
            }
        };
        registry.insert(name.to_owned(), Arc::new(source));
    }

    let placements = vec![
        Placement::Header(HeaderPlacement::new(
            HeaderName::from_static("dd-api-key"),
            "",
            Arc::clone(&registry["events"]),
        )),
        Placement::Query(QueryPlacement::new(
            "token",
            Arc::clone(&registry["licence"]),
        )),
    ];

    let response = client()
        .get_signed(&format!("http://{addr}/api"), &placements)
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let recorded = state.lock().unwrap();
    let seen = &recorded.api[0];
    assert_eq!(
        seen.headers.get("dd-api-key").map(String::as_str),
        Some("tok-1")
    );
    assert_eq!(seen.query, "token=static-token");
}

/// A value with a query's own separators in it arrives as one parameter, not as
/// the extra parameters it would be if it were formatted into the URL.
#[tokio::test]
async fn a_query_credential_is_encoded_rather_than_appended_raw() {
    let (addr, state) = fixture().await;
    let placement = QueryPlacement::new("token", Cached::new(Static::new("a&b=c")));

    client()
        .get_signed(&format!("http://{addr}/api"), &placement)
        .await
        .unwrap();

    assert_eq!(state.lock().unwrap().api[0].query, "token=a%26b%3Dc");
}

/// A refused exchange reaches the caller of the signed request with its status
/// intact, so a consumer can map a 401 from the token endpoint onto its own
/// error rather than parsing a message.
#[tokio::test]
async fn a_refused_exchange_reaches_the_signed_call_with_its_status() {
    let (addr, state) = fixture().await;
    let placement = HeaderPlacement::bearer(Cached::new(client_credentials(addr, "unauthorised")));

    let error = client()
        .get_signed(&format!("http://{addr}/api"), &placement)
        .await
        .expect_err("the token endpoint refused");

    assert!(
        matches!(
            error,
            HttpError::Sign(SignError::Auth(AuthError::Refused { status: 401, .. }))
        ),
        "{error:?}"
    );
    let recorded = state.lock().unwrap();
    assert_eq!(recorded.token_exchanges, 1, "a refusal is not retried");
    assert!(recorded.api.is_empty(), "the request never left");
}

/// Channel: the log line. Everything the acquisition and the signing emit,
/// captured at trace level, with the secret in the token URL's query, in the
/// form, in a refusal that echoes the form, and on a query-signed call that
/// fails in transport.
#[cfg(feature = "logger")]
#[tokio::test]
async fn no_log_line_carries_the_secret() {
    use tracing_subscriber::layer::SubscriberExt as _;

    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("trace"))
        .with(tracing_subscriber::fmt::layer().with_writer(move || Capture(Arc::clone(&sink))));
    let _guard = tracing::subscriber::set_default(subscriber);

    let (addr, _state) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = listener.local_addr().unwrap();
    drop(listener);
    let exchange = |recipe: &str| {
        ClientCredentials::new(
            &client(),
            format!("http://{addr}/token/{recipe}?wrapping={SECRET}"),
            "client-42",
            SensitiveString::new(SECRET),
        )
        .unwrap()
        .with_form_field("audience", SECRET)
        .with_reading(
            TokenReading::default()
                .with_renew_margin(Duration::from_secs(3600))
                .expose_field("access_token"),
        )
    };

    let held = Cached::new(exchange("short")).credential().await.unwrap();
    let refused = Cached::new(exchange("echo"))
        .credential()
        .await
        .unwrap_err();
    let unsent = client()
        .get_signed(
            &format!("http://{dead}/api"),
            &QueryPlacement::new("token", Cached::new(Static::new(SECRET))),
        )
        .await
        .unwrap_err();
    tracing::warn!(?held, %refused, %unsent, "what a consumer would log");

    let log = String::from_utf8_lossy(&captured.lock().unwrap()).into_owned();
    assert!(
        log.contains("credential acquired"),
        "the channel was exercised: {log}"
    );
    assert!(!log.contains(SECRET), "{log}");
}
