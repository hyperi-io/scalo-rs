// Project:   scalo
// File:      src/auth/source.rs
// Purpose:   Credential acquisition: one exchange, cached and renewed
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Where a credential comes from, and how long it is held.
//!
//! The two halves are deliberately separate. An [`Exchange`] knows one
//! protocol and obtains one credential, once, with no state. [`Cached`] wraps
//! any exchange and owns the caching, the renewal point and the single-flight
//! gate, so no protocol has to implement them and every protocol gets them.
//!
//! A [`CredentialSource`] is the read side that a placement calls per request:
//! on a hit it is one atomic load and a pointer clone, and it never parks.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use reqwest::Method;
use reqwest::header::HeaderName;
use serde_json::{Map, Value};

use super::error::AuthError;
use crate::http_client::{HttpClient, HttpError, Unsigned};
use crate::sensitive::SensitiveString;

/// Assumed lifetime of a token response that carries no `expires_in`.
const DEFAULT_EXPIRES_IN_FALLBACK: Duration = Duration::from_secs(3600);

/// How far ahead of expiry a credential is renewed, so a request that is signed
/// now and arrives in a moment is not signed with an expired credential.
const DEFAULT_RENEW_MARGIN: Duration = Duration::from_secs(60);

/// How long a credential that needs no exchange is held for.
const STATIC_LIFETIME: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Longest refusal body kept in an error, so a provider that answers with a
/// page cannot fill the log with it.
const MAX_REFUSAL_BODY: usize = 512;

/// One acquired credential, held whole and swapped whole.
#[non_exhaustive]
pub struct Credential {
    /// The secret a placement puts on the request.
    pub secret: SensitiveString,
    /// Already margin-adjusted: renew when `now >= this`.
    pub renew_at: Instant,
    /// What else the exchange returned that a consumer reads; `None` for a
    /// source that returns only a secret, so the common case costs nothing.
    pub extra: Option<Arc<Value>>,
}

impl Credential {
    /// A credential that is held until `renew_at`.
    #[must_use]
    pub fn new(secret: SensitiveString, renew_at: Instant) -> Self {
        Self {
            secret,
            renew_at,
            extra: None,
        }
    }

    /// Carry the rest of what the exchange returned alongside the secret.
    #[must_use]
    pub fn with_extra(mut self, extra: Arc<Value>) -> Self {
        self.extra = Some(extra);
        self
    }

    /// Whether the credential has reached its renewal point.
    #[must_use]
    pub fn is_due(&self) -> bool {
        Instant::now() >= self.renew_at
    }
}

/// Hand-written so neither the secret nor an exposed field can reach a trace or
/// an error report that formats the credential.
impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("secret", &self.secret)
            .field("renew_at", &self.renew_at)
            .field("extra", &self.extra.as_ref().map(|_| "***REDACTED***"))
            .finish_non_exhaustive()
    }
}

/// The read side: the credential to use for this request.
pub trait CredentialSource: Send + Sync {
    /// The current credential, acquiring or renewing one if needed.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when a credential has to be acquired and the
    /// exchange fails.
    fn credential(&self) -> impl Future<Output = Result<Arc<Credential>, AuthError>> + Send;
}

impl<T: CredentialSource + ?Sized> CredentialSource for Arc<T> {
    fn credential(&self) -> impl Future<Output = Result<Arc<Credential>, AuthError>> + Send {
        T::credential(self)
    }
}

impl<T: CredentialSource + ?Sized> CredentialSource for &T {
    fn credential(&self) -> impl Future<Output = Result<Arc<Credential>, AuthError>> + Send {
        T::credential(self)
    }
}

/// The per-protocol half: how one credential is obtained, once.
///
/// An implementation does no caching and holds no state -- [`Cached`] owns
/// that. A signing scheme that needs its own crypto (a JWT client assertion, a
/// provider-specific signature) mints its inputs in the consumer and hands them
/// to [`TokenPost`], or implements this trait itself.
pub trait Exchange: Send + Sync {
    /// Obtain one credential now.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the endpoint cannot be reached, refuses, or
    /// answers with something that is not a credential.
    fn acquire(&self) -> impl Future<Output = Result<Credential, AuthError>> + Send;
}

/// Caching, renewal and single-flight over any [`Exchange`].
///
/// A hit loads the held pointer and returns a clone of it. A miss takes the
/// renewal lock, re-checks (another task may have renewed while this one
/// waited), and only then exchanges -- so a cold source hit by a hundred
/// callers mints once, not a hundred times.
#[derive(Debug)]
pub struct Cached<E> {
    current: ArcSwapOption<Credential>,
    renewing: tokio::sync::Mutex<()>,
    exchange: E,
}

impl<E> Cached<E> {
    /// Wrap an exchange, holding nothing until the first call.
    #[must_use]
    pub fn new(exchange: E) -> Self {
        Self {
            current: ArcSwapOption::empty(),
            renewing: tokio::sync::Mutex::new(()),
            exchange,
        }
    }

    /// The held credential, whether or not it is still current. `None` before
    /// the first acquisition.
    #[must_use]
    pub fn held(&self) -> Option<Arc<Credential>> {
        self.current.load_full()
    }
}

impl<E: Exchange> CredentialSource for Cached<E> {
    async fn credential(&self) -> Result<Arc<Credential>, AuthError> {
        if let Some(held) = self.current.load_full()
            && !held.is_due()
        {
            return Ok(held);
        }

        // The one await under a guard in this module, and the reason the mutex
        // is the async one: the exchange is I/O and the wait is the gate.
        let _renewing = self.renewing.lock().await;
        if let Some(held) = self.current.load_full()
            && !held.is_due()
        {
            return Ok(held);
        }

        let fresh = Arc::new(self.exchange.acquire().await?);
        self.current.store(Some(Arc::clone(&fresh)));
        tracing::debug!(
            renew_in_secs = fresh
                .renew_at
                .saturating_duration_since(Instant::now())
                .as_secs(),
            "credential acquired"
        );
        Ok(fresh)
    }
}

/// How a token response is read: what to assume when it omits an expiry, how
/// far ahead of expiry to renew, and which of its other fields to carry.
#[derive(Debug, Clone)]
pub struct TokenReading {
    expires_in_fallback: Duration,
    renew_margin: Duration,
    expose: Vec<String>,
}

impl Default for TokenReading {
    fn default() -> Self {
        Self {
            expires_in_fallback: DEFAULT_EXPIRES_IN_FALLBACK,
            renew_margin: DEFAULT_RENEW_MARGIN,
            expose: Vec::new(),
        }
    }
}

impl TokenReading {
    /// The lifetime to assume when the response carries no `expires_in`.
    #[must_use]
    pub fn with_expires_in_fallback(mut self, fallback: Duration) -> Self {
        self.expires_in_fallback = fallback;
        self
    }

    /// How far ahead of expiry to renew.
    #[must_use]
    pub fn with_renew_margin(mut self, margin: Duration) -> Self {
        self.renew_margin = margin;
        self
    }

    /// Carry this top-level response field, when present, in
    /// [`Credential::extra`].
    #[must_use]
    pub fn expose_field(mut self, name: impl Into<String>) -> Self {
        self.expose.push(name.into());
        self
    }
}

/// A credential that needs no exchange: a key read from config or a secrets
/// backend, already resolved by the consumer.
#[derive(Debug, Clone)]
pub struct Static {
    secret: SensitiveString,
}

impl Static {
    /// Hold this secret as the credential.
    #[must_use]
    pub fn new(secret: impl Into<SensitiveString>) -> Self {
        Self {
            secret: secret.into(),
        }
    }
}

impl Exchange for Static {
    async fn acquire(&self) -> Result<Credential, AuthError> {
        Ok(Credential::new(self.secret.clone(), far_future()))
    }
}

/// An OAuth2 client-credentials exchange (RFC 6749 s4.4).
///
/// Every value is already rendered: templating, secret resolution and any
/// per-deployment substitution belong to the consumer.
pub struct ClientCredentials {
    http: Arc<HttpClient>,
    token_url: String,
    client_id: String,
    client_secret: SensitiveString,
    scope: Option<String>,
    extra_form: Vec<(String, String)>,
    reading: TokenReading,
}

impl ClientCredentials {
    /// Exchange these client credentials at `token_url`.
    #[must_use]
    pub fn new(
        http: Arc<HttpClient>,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: SensitiveString,
    ) -> Self {
        Self {
            http,
            token_url: token_url.into(),
            client_id: client_id.into(),
            client_secret,
            scope: None,
            extra_form: Vec::new(),
            reading: TokenReading::default(),
        }
    }

    /// Ask for these scopes. Unset means the parameter is absent from the
    /// form: `scope=` is a request for no scopes, which some providers refuse.
    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Add a provider-specific form field (an `audience`, a `resource`).
    #[must_use]
    pub fn with_form_field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_form.push((name.into(), value.into()));
        self
    }

    /// How to read the token response.
    #[must_use]
    pub fn with_reading(mut self, reading: TokenReading) -> Self {
        self.reading = reading;
        self
    }
}

impl Exchange for ClientCredentials {
    async fn acquire(&self) -> Result<Credential, AuthError> {
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "client_credentials"),
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.expose()),
        ];
        if let Some(ref scope) = self.scope {
            form.push(("scope", scope.as_str()));
        }
        for (name, value) in &self.extra_form {
            form.push((name.as_str(), value.as_str()));
        }

        let body = post_form(&self.http, &self.token_url, &form).await?;
        credential_from_token_response(&self.token_url, &body, &self.reading)
    }
}

/// A form POST to a token endpoint, exactly as the consumer renders it.
///
/// This is how a signed client assertion (RFC 7523) or a session login reaches
/// an endpoint: the consumer mints and signs the assertion and hands it in as a
/// form value, so no key format or JWT library enters scalo.
pub struct TokenPost {
    http: Arc<HttpClient>,
    token_url: String,
    form: Vec<(String, String)>,
    reading: TokenReading,
}

impl TokenPost {
    /// Post to `token_url`. The form starts empty.
    #[must_use]
    pub fn new(http: Arc<HttpClient>, token_url: impl Into<String>) -> Self {
        Self {
            http,
            token_url: token_url.into(),
            form: Vec::new(),
            reading: TokenReading::default(),
        }
    }

    /// Add a rendered form field.
    #[must_use]
    pub fn with_form_field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.form.push((name.into(), value.into()));
        self
    }

    /// How to read the token response.
    #[must_use]
    pub fn with_reading(mut self, reading: TokenReading) -> Self {
        self.reading = reading;
        self
    }
}

impl Exchange for TokenPost {
    async fn acquire(&self) -> Result<Credential, AuthError> {
        let form: Vec<(&str, &str)> = self
            .form
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let body = post_form(&self.http, &self.token_url, &form).await?;
        credential_from_token_response(&self.token_url, &body, &self.reading)
    }
}

/// A cloud instance metadata server: a GET, usually behind a header that proves
/// the call was not made by a browser or a confused proxy.
pub struct MetadataServer {
    http: Arc<HttpClient>,
    url: String,
    headers: Vec<(HeaderName, String)>,
    reading: TokenReading,
}

impl MetadataServer {
    /// Read a credential from `url`.
    #[must_use]
    pub fn new(http: Arc<HttpClient>, url: impl Into<String>) -> Self {
        Self {
            http,
            url: url.into(),
            headers: Vec::new(),
            reading: TokenReading::default(),
        }
    }

    /// Send this header with the request (`Metadata-Flavor: Google`,
    /// `Metadata: true`).
    #[must_use]
    pub fn with_header(mut self, name: HeaderName, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    /// How to read the token response.
    #[must_use]
    pub fn with_reading(mut self, reading: TokenReading) -> Self {
        self.reading = reading;
        self
    }
}

impl Exchange for MetadataServer {
    async fn acquire(&self) -> Result<Credential, AuthError> {
        let response = self
            .http
            .send_signed(
                Method::GET,
                &self.url,
                None,
                |mut builder| {
                    for (name, value) in &self.headers {
                        builder = builder.header(name.clone(), value);
                    }
                    builder
                },
                &Unsigned,
            )
            .await
            .map_err(|e| AuthError::Unreachable {
                url: self.url.clone(),
                source: strip_url(e),
            })?;
        let body = json_body(response, &self.url).await?;
        credential_from_token_response(&self.url, &body, &self.reading)
    }
}

/// The credential in a token endpoint's 2xx response.
///
/// `access_token` is required. `expires_in` is read whether the provider sent
/// it as a number or as a numeric string, and falls back when it is absent
/// altogether; the renewal point is that lifetime less the margin. The named
/// fields that are present are carried in [`Credential::extra`].
fn credential_from_token_response(
    url: &str,
    body: &Value,
    reading: &TokenReading,
) -> Result<Credential, AuthError> {
    let secret = body
        .get("access_token")
        .and_then(Value::as_str)
        .ok_or_else(|| AuthError::Malformed {
            url: url.to_owned(),
            reason: "no access_token in the response".to_owned(),
        })?;

    let expires_in = body
        .get("expires_in")
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        })
        .map_or(reading.expires_in_fallback, Duration::from_secs);

    let extra: Map<String, Value> = reading
        .expose
        .iter()
        .filter_map(|name| body.get(name).map(|value| (name.clone(), value.clone())))
        .collect();

    let renew_at = Instant::now() + expires_in.saturating_sub(reading.renew_margin);
    let credential = Credential::new(SensitiveString::from(secret), renew_at);
    Ok(if extra.is_empty() {
        credential
    } else {
        credential.with_extra(Arc::new(Value::Object(extra)))
    })
}

/// POST a rendered form through the shared client, so a token exchange gets the
/// same timeouts, connection pool and retry policy as every other call.
async fn post_form(
    http: &HttpClient,
    url: &str,
    form: &[(&str, &str)],
) -> Result<Value, AuthError> {
    let response = http
        .send_signed(
            Method::POST,
            url,
            None,
            |builder| builder.form(form),
            &Unsigned,
        )
        .await
        .map_err(|e| AuthError::Unreachable {
            url: url.to_owned(),
            source: strip_url(e),
        })?;
    json_body(response, url).await
}

/// The response body as JSON, or the refusal named.
async fn json_body(response: reqwest::Response, url: &str) -> Result<Value, AuthError> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AuthError::Refused {
            url: url.to_owned(),
            status: status.as_u16(),
            body: body.chars().take(MAX_REFUSAL_BODY).collect(),
        });
    }
    response.json().await.map_err(|e| AuthError::Malformed {
        url: url.to_owned(),
        reason: e.without_url().to_string(),
    })
}

/// reqwest's `Display` appends the request URL, and a credential placed in a
/// query would ride out with it, so the URL is dropped before the error is
/// reported. The endpoint is named by the variant instead.
fn strip_url(error: HttpError) -> HttpError {
    match error {
        HttpError::Transport(e) => HttpError::Transport(e.without_url()),
        other => other,
    }
}

/// Far enough out that a credential with no expiry is never renewed in a
/// process's lifetime, without risking an `Instant` overflow.
fn far_future() -> Instant {
    let now = Instant::now();
    now.checked_add(STATIC_LIFETIME).unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(raw: &str) -> Value {
        serde_json::from_str(raw).unwrap()
    }

    #[test]
    fn expires_in_is_read_as_a_number_or_a_string() {
        let reading = TokenReading::default().with_renew_margin(Duration::ZERO);
        let numeric = credential_from_token_response(
            "https://idp.example/token",
            &response("{\"access_token\":\"a\",\"expires_in\":120}"),
            &reading,
        )
        .unwrap();
        let stringly = credential_from_token_response(
            "https://idp.example/token",
            &response("{\"access_token\":\"a\",\"expires_in\":\"120\"}"),
            &reading,
        )
        .unwrap();

        let floor = Instant::now() + Duration::from_secs(60);
        assert!(numeric.renew_at > floor);
        assert!(stringly.renew_at > floor);
    }

    #[test]
    fn a_missing_expiry_takes_the_fallback_less_the_margin() {
        let reading = TokenReading::default()
            .with_expires_in_fallback(Duration::from_secs(600))
            .with_renew_margin(Duration::from_secs(60));
        let credential = credential_from_token_response(
            "https://idp.example/token",
            &response("{\"access_token\":\"a\"}"),
            &reading,
        )
        .unwrap();

        let now = Instant::now();
        assert!(credential.renew_at > now + Duration::from_secs(400));
        assert!(credential.renew_at <= now + Duration::from_secs(540));
    }

    #[test]
    fn a_margin_longer_than_the_lifetime_is_due_immediately() {
        let reading = TokenReading::default().with_renew_margin(Duration::from_secs(600));
        let credential = credential_from_token_response(
            "https://idp.example/token",
            &response("{\"access_token\":\"a\",\"expires_in\":30}"),
            &reading,
        )
        .unwrap();

        assert!(
            credential.is_due(),
            "saturating, not a panic and not an hour in the past"
        );
    }

    #[test]
    fn only_the_named_fields_are_exposed() {
        let bare = credential_from_token_response(
            "https://idp.example/token",
            &response("{\"access_token\":\"a\",\"instance_url\":\"https://shard\"}"),
            &TokenReading::default(),
        )
        .unwrap();
        assert!(bare.extra.is_none(), "nothing asked for, nothing carried");

        let exposed = credential_from_token_response(
            "https://idp.example/token",
            &response("{\"access_token\":\"a\",\"instance_url\":\"https://shard\"}"),
            &TokenReading::default().expose_field("instance_url"),
        )
        .unwrap();
        let extra = exposed.extra.as_deref().unwrap();
        assert_eq!(extra["instance_url"], "https://shard");
        assert!(extra.get("access_token").is_none());
    }

    #[test]
    fn a_response_with_no_token_is_malformed() {
        let error = credential_from_token_response(
            "https://idp.example/token",
            &response("{\"token_type\":\"Bearer\"}"),
            &TokenReading::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("access_token"), "{error}");
    }

    #[tokio::test]
    async fn a_static_credential_is_never_due() {
        let held = Cached::new(Static::new("static-token"));
        let credential = held.credential().await.unwrap();
        assert_eq!(credential.secret.expose(), "static-token");
        assert!(!credential.is_due());
        assert!(held.held().is_some());
    }

    #[test]
    fn a_credential_debug_shows_no_secret() {
        let credential = Credential::new(SensitiveString::new("hunter2"), Instant::now())
            .with_extra(Arc::new(response("{\"refresh_token\":\"r3fr3sh\"}")));
        let rendered = format!("{credential:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("r3fr3sh"), "{rendered}");
        assert!(rendered.contains("REDACTED"), "{rendered}");
    }
}
