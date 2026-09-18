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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use reqwest::Method;
use reqwest::header::HeaderName;
use serde_json::{Map, Value};

use super::error::{AuthError, endpoint_name};
use crate::http_client::{HttpClient, HttpClientConfig, Unsigned};
use crate::sensitive::SensitiveString;

/// Assumed lifetime of a token response that carries no `expires_in`.
const DEFAULT_EXPIRES_IN_FALLBACK: Duration = Duration::from_secs(3600);

/// How far ahead of expiry a credential is renewed, so a request that is signed
/// now and arrives in a moment is not signed with an expired credential.
const DEFAULT_RENEW_MARGIN: Duration = Duration::from_secs(60);

/// How long a credential that needs no exchange is held for.
const STATIC_LIFETIME: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Longest lifetime taken from a token response. A provider that advertises
/// more than a month has either made a mistake or been tampered with, and an
/// `expires_in` near `u64::MAX` overflows the arithmetic outright.
const MAX_CREDENTIAL_LIFETIME: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Longest refusal detail kept in an error, so a provider that answers with a
/// page cannot fill the log with it.
const MAX_REFUSAL_DETAIL: usize = 512;

/// How long one acquisition may take when the exchange names no bound of its
/// own.
const DEFAULT_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long after a refusal the endpoint is left alone, and every caller is
/// handed that one failure instead.
const DEFAULT_FAILURE_BACKOFF: Duration = Duration::from_secs(1);

/// Response fields that are themselves credentials, and so are never carried in
/// [`Credential::extra`].
const NEVER_EXPOSED: [&str; 4] = ["access_token", "refresh_token", "id_token", "client_secret"];

/// What a refusal says when its body carried nothing diagnosable.
const NO_REFUSAL_DETAIL: &str = "the body named no error";

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
/// to [`TokenPost::minted`], or implements this trait itself.
pub trait Exchange: Send + Sync {
    /// Obtain one credential now.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the endpoint cannot be reached, refuses, or
    /// answers with something that is not a credential.
    fn acquire(&self) -> impl Future<Output = Result<Credential, AuthError>> + Send;

    /// How long one acquisition may take before it is abandoned.
    ///
    /// [`Cached`] holds the renewal gate for the whole acquisition, so an
    /// endpoint that accepts the connection and then says nothing would park
    /// every caller behind it. The HTTP exchanges answer with their own
    /// client's timeout over every attempt it may make.
    fn timeout(&self) -> Duration {
        DEFAULT_EXCHANGE_TIMEOUT
    }
}

/// The last acquisition failure, held so that callers waiting on one in-flight
/// acquisition report it rather than each hitting the endpoint in turn.
#[derive(Debug)]
struct Failure {
    /// How many acquisitions had finished once this one had, so a caller that
    /// entered before that count can tell it waited on this failure.
    ordinal: u64,
    at: Instant,
    message: String,
    transient: bool,
}

/// Caching, renewal and single-flight over any [`Exchange`].
///
/// A hit loads the held pointer and returns a clone of it. A miss takes the
/// renewal lock, re-checks (another task may have renewed while this one
/// waited), and only then exchanges -- so a cold source hit by a hundred
/// callers mints once, not a hundred times.
///
/// A failure is shared the same way: the caller that ran the acquisition and
/// every caller that waited on it report the same failure. A failure the
/// endpoint answered with is then held for the failure backoff, so a refused
/// credential is not posted again by every caller in turn, while an endpoint
/// that could not be reached or ran out of time is tried again by the next
/// caller, because that is what a retry is for.
#[derive(Debug)]
pub struct Cached<E> {
    current: ArcSwapOption<Credential>,
    renewal: tokio::sync::Mutex<Option<Failure>>,
    /// Acquisitions finished so far, read before a caller queues on the lock.
    finished: AtomicU64,
    exchange: E,
    failure_backoff: Duration,
}

impl<E> Cached<E> {
    /// Wrap an exchange, holding nothing until the first call.
    #[must_use]
    pub fn new(exchange: E) -> Self {
        Self {
            current: ArcSwapOption::empty(),
            renewal: tokio::sync::Mutex::new(None),
            finished: AtomicU64::new(0),
            exchange,
            failure_backoff: DEFAULT_FAILURE_BACKOFF,
        }
    }

    /// How long a refusal, or a response that was not a credential, is reported
    /// to every caller before the endpoint is tried again. Zero exchanges on
    /// every miss. An endpoint that was unreachable or out of time is not held
    /// at all.
    #[must_use]
    pub fn with_failure_backoff(mut self, backoff: Duration) -> Self {
        self.failure_backoff = backoff;
        self
    }

    /// The held credential, whether or not it is still current. `None` before
    /// the first acquisition.
    #[must_use]
    pub fn held(&self) -> Option<Arc<Credential>> {
        self.current.load_full()
    }

    /// Drop the held credential, so the next call exchanges.
    ///
    /// For the consumer that learns from a 401 that the provider has revoked
    /// the credential before its advertised expiry.
    pub fn invalidate(&self) {
        self.current.store(None);
    }
}

impl<E: Exchange> CredentialSource for Cached<E> {
    async fn credential(&self) -> Result<Arc<Credential>, AuthError> {
        if let Some(held) = self.current.load_full()
            && !held.is_due()
        {
            return Ok(held);
        }

        // Read before queueing, so a failure that finishes while this caller
        // waits is one it waited on; the lock orders the count against the
        // failure it describes.
        let entered = self.finished.load(Ordering::Relaxed);
        // The one await under a guard in this module, and the reason the mutex
        // is the async one: the exchange is I/O and the wait is the gate.
        let mut renewal = self.renewal.lock().await;
        if let Some(held) = self.current.load_full()
            && !held.is_due()
        {
            return Ok(held);
        }
        if let Some(failure) = renewal.as_ref()
            && failure.stands_for(entered, self.failure_backoff)
        {
            return Err(AuthError::Shared {
                message: failure.message.clone(),
                transient: failure.transient,
            });
        }

        let deadline = self.exchange.timeout();
        let outcome = tokio::time::timeout(deadline, self.exchange.acquire()).await;
        let ordinal = self.finished.fetch_add(1, Ordering::Relaxed) + 1;
        let fresh = match outcome {
            Ok(Ok(credential)) => credential,
            Ok(Err(error)) => {
                *renewal = Some(Failure::of(&error, ordinal));
                return Err(error);
            }
            Err(_elapsed) => {
                let error = AuthError::TimedOut {
                    secs: deadline.as_secs(),
                };
                *renewal = Some(Failure::of(&error, ordinal));
                return Err(error);
            }
        };

        *renewal = None;
        let fresh = Arc::new(fresh);
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

impl Failure {
    fn of(error: &AuthError, ordinal: u64) -> Self {
        Self {
            ordinal,
            at: Instant::now(),
            message: error.to_string(),
            transient: error.is_transient(),
        }
    }

    /// Whether a caller that read `entered` finished acquisitions before
    /// queueing is handed this failure: it waited on the acquisition that
    /// produced it, or the endpoint answered with it inside `backoff` ago.
    fn stands_for(&self, entered: u64, backoff: Duration) -> bool {
        self.ordinal > entered || (!self.transient && self.at.elapsed() < backoff)
    }
}

/// A credential source chosen at run time, for a consumer that reads which kind
/// to build out of config and holds them all in one collection.
///
/// One variant per exchange, dispatched by a match: no vtable, no boxed future,
/// and the same shape the DLQ backends use.
#[non_exhaustive]
#[derive(Debug)]
pub enum AnyCredentialSource {
    /// A key the consumer already resolved.
    Static(Cached<Static>),
    /// An OAuth2 client-credentials exchange.
    ClientCredentials(Cached<ClientCredentials>),
    /// A form POST of exactly the fields it was handed.
    TokenPost(Cached<TokenPost>),
    /// A cloud instance metadata server.
    MetadataServer(Cached<MetadataServer>),
}

impl AnyCredentialSource {
    /// The exchange kind, for a log or metric label.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Static(_) => "static",
            Self::ClientCredentials(_) => "client_credentials",
            Self::TokenPost(_) => "token_post",
            Self::MetadataServer(_) => "metadata_server",
        }
    }

    /// Drop the held credential, so the next call exchanges.
    pub fn invalidate(&self) {
        match self {
            Self::Static(source) => source.invalidate(),
            Self::ClientCredentials(source) => source.invalidate(),
            Self::TokenPost(source) => source.invalidate(),
            Self::MetadataServer(source) => source.invalidate(),
        }
    }
}

impl CredentialSource for AnyCredentialSource {
    async fn credential(&self) -> Result<Arc<Credential>, AuthError> {
        match self {
            Self::Static(source) => source.credential().await,
            Self::ClientCredentials(source) => source.credential().await,
            Self::TokenPost(source) => source.credential().await,
            Self::MetadataServer(source) => source.credential().await,
        }
    }
}

/// How a token response is read: what to assume when it omits an expiry, how
/// far ahead of expiry to renew, and which of its other fields to carry.
///
/// A consumer writing its own [`Exchange`] reads its response through
/// [`Self::read`] rather than re-deriving the same rules.
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
    ///
    /// A field that is itself a credential -- `access_token`, `refresh_token`,
    /// `id_token`, `client_secret` -- is dropped with a warning: `extra` is
    /// read and rendered by consumers, and a secret copied into it is a second
    /// place to leak from.
    #[must_use]
    pub fn expose_field(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        if NEVER_EXPOSED.contains(&name.as_str()) {
            tracing::warn!(field = %name, "refusing to expose a credential field");
            return self;
        }
        self.expose.push(name);
        self
    }

    /// Read a token endpoint's 2xx response into a credential.
    ///
    /// `access_token` is required. `expires_in` is read whether the provider
    /// sent it as a number or as a numeric string, and falls back when it is
    /// absent or unreadable. The exposed fields that are present are carried in
    /// [`Credential::extra`].
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Malformed`] when the response carries no
    /// `access_token`.
    pub fn read(&self, url: &str, body: &Value) -> Result<Credential, AuthError> {
        let endpoint = endpoint_name(url);
        let secret = body
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| AuthError::Malformed {
                url: endpoint.clone(),
                reason: "no access_token in the response".to_owned(),
            })?;

        let extra: Map<String, Value> = self
            .expose
            .iter()
            .filter_map(|name| body.get(name).map(|value| (name.clone(), value.clone())))
            .collect();

        let credential = Credential::new(
            SensitiveString::from(secret),
            self.renew_at(self.lifetime_of(body, &endpoint)),
        );
        Ok(if extra.is_empty() {
            credential
        } else {
            credential.with_extra(Arc::new(Value::Object(extra)))
        })
    }

    /// When a credential of this lifetime is due for renewal.
    ///
    /// The lifetime is clamped to a month, because an absurd `expires_in`
    /// overflows the arithmetic outright. The margin is then taken off it, but
    /// never below half the lifetime: a margin at or over the lifetime would
    /// otherwise make every request its own serialised exchange.
    #[must_use]
    pub fn renew_at(&self, lifetime: Duration) -> Instant {
        let lifetime = if lifetime > MAX_CREDENTIAL_LIFETIME {
            tracing::warn!(
                advertised_secs = lifetime.as_secs(),
                ceiling_secs = MAX_CREDENTIAL_LIFETIME.as_secs(),
                "credential lifetime clamped to the ceiling"
            );
            MAX_CREDENTIAL_LIFETIME
        } else {
            lifetime
        };
        if self.renew_margin >= lifetime {
            tracing::warn!(
                margin_secs = self.renew_margin.as_secs(),
                lifetime_secs = lifetime.as_secs(),
                "renew margin is not shorter than the lifetime, holding half of it"
            );
        }
        let hold = lifetime.saturating_sub(self.renew_margin).max(lifetime / 2);
        let now = Instant::now();
        now.checked_add(hold).unwrap_or_else(far_future)
    }

    /// The lifetime the response advertises, or the fallback when it advertises
    /// none or advertises something that is not a number of seconds.
    fn lifetime_of(&self, body: &Value, endpoint: &str) -> Duration {
        let Some(raw) = body.get("expires_in") else {
            return self.expires_in_fallback;
        };
        let Some(seconds) = raw
            .as_u64()
            .or_else(|| raw.as_str().and_then(|seconds| seconds.parse().ok()))
        else {
            tracing::warn!(
                endpoint,
                "expires_in is not a number of seconds, taking the fallback lifetime"
            );
            return self.expires_in_fallback;
        };
        Duration::from_secs(seconds)
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
    http: HttpClient,
    token_url: String,
    client_id: String,
    client_secret: SensitiveString,
    scope: Option<String>,
    extra_form: Vec<(String, String)>,
    reading: TokenReading,
}

impl ClientCredentials {
    /// Exchange these client credentials at `token_url`.
    ///
    /// `http` is the settings the exchange takes: it builds its own client from
    /// them, because this exchange refuses redirects and replays its own POST
    /// whatever the shared client does, a client-secret POST being safe to resend.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Endpoint`] when `token_url` is not an https URL or
    /// a loopback address, and [`AuthError::Client`] when the exchange's own
    /// client cannot be built.
    pub fn new(
        http: &HttpClient,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: SensitiveString,
    ) -> Result<Self, AuthError> {
        let token_url = token_url.into();
        require_secure_endpoint(&token_url)?;
        Ok(Self {
            http: exchange_client(http, true)?,
            token_url,
            client_id: client_id.into(),
            client_secret,
            scope: None,
            extra_form: Vec::new(),
            reading: TokenReading::default(),
        })
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

/// Hand-written: the endpoint alone, named as an error names it, so a render
/// of the exchange says which one it is and nothing it will post.
impl fmt::Debug for ClientCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCredentials")
            .field("token_url", &endpoint_name(&self.token_url))
            .finish_non_exhaustive()
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
        self.reading.read(&self.token_url, &body)
    }

    fn timeout(&self) -> Duration {
        exchange_deadline(&self.http)
    }
}

/// A form POST to a token endpoint, exactly as the consumer renders it.
///
/// [`Self::new`] posts a form rendered once, the same on every exchange, which
/// suits a form that is safe to resend: a session login, a client secret.
/// [`Self::minted`] renders the form afresh for every exchange, which is how a
/// signed client assertion (RFC 7523) reaches an endpoint: the consumer mints
/// and signs a new assertion each time, so no key format or JWT library enters
/// scalo.
///
/// A single-use assertion is refused on a second sight, so the POST is not
/// retried inside one exchange unless the template's `retry_non_idempotent`
/// opts in.
pub struct TokenPost {
    http: HttpClient,
    token_url: String,
    form: Vec<(String, String)>,
    render: Option<Arc<RenderForm>>,
    reading: TokenReading,
}

/// A consumer's closure that renders a token post's form for one exchange.
type RenderForm = dyn Fn() -> Result<Vec<(String, String)>, AuthError> + Send + Sync;

impl TokenPost {
    /// Post to `token_url` a form that is safe to resend: a session login, a
    /// client secret. The form starts empty and is posted as rendered on every
    /// exchange, renewals included.
    ///
    /// `http` is the settings the exchange takes, with redirects refused as for
    /// [`ClientCredentials::new`] and its `retry_non_idempotent` deciding
    /// whether the POST is retried.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Endpoint`] when `token_url` is not an https URL or
    /// a loopback address, and [`AuthError::Client`] when the exchange's own
    /// client cannot be built.
    pub fn new(http: &HttpClient, token_url: impl Into<String>) -> Result<Self, AuthError> {
        let token_url = token_url.into();
        require_secure_endpoint(&token_url)?;
        Ok(Self {
            http: exchange_client(http, http.config().retry_non_idempotent)?,
            token_url,
            form: Vec::new(),
            render: None,
            reading: TokenReading::default(),
        })
    }

    /// Post to `token_url` a form that `render` mints afresh for every
    /// exchange: the RFC 7523 client assertion path, where a signed assertion
    /// carries a single-use `jti` and a short `exp` and so cannot be resent.
    ///
    /// `render` runs once per acquisition, renewals included, and its fields
    /// are posted after any added with [`Self::with_form_field`]. A render that
    /// cannot mint (a key it cannot read, a signer that failed) returns an
    /// [`AuthError`], usually [`AuthError::Unavailable`], which the exchange
    /// hands back without posting anything. `http` is taken as for
    /// [`Self::new`], so the POST is not retried inside one exchange unless the
    /// template's `retry_non_idempotent` opts in: a retry would replay the same
    /// assertion.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Endpoint`] when `token_url` is not an https URL or
    /// a loopback address, and [`AuthError::Client`] when the exchange's own
    /// client cannot be built.
    pub fn minted<F>(
        http: &HttpClient,
        token_url: impl Into<String>,
        render: F,
    ) -> Result<Self, AuthError>
    where
        F: Fn() -> Result<Vec<(String, String)>, AuthError> + Send + Sync + 'static,
    {
        let mut post = Self::new(http, token_url)?;
        post.render = Some(Arc::new(render));
        Ok(post)
    }

    /// Add a rendered form field, posted unchanged on every exchange.
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

/// Hand-written: the endpoint alone, as for [`ClientCredentials`]; the form
/// carries the assertion this exchange exists to send.
impl fmt::Debug for TokenPost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenPost")
            .field("token_url", &endpoint_name(&self.token_url))
            .finish_non_exhaustive()
    }
}

impl Exchange for TokenPost {
    async fn acquire(&self) -> Result<Credential, AuthError> {
        let minted = match self.render {
            Some(ref render) => render()?,
            None => Vec::new(),
        };
        let form: Vec<(&str, &str)> = self
            .form
            .iter()
            .chain(&minted)
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let body = post_form(&self.http, &self.token_url, &form).await?;
        self.reading.read(&self.token_url, &body)
    }

    fn timeout(&self) -> Duration {
        exchange_deadline(&self.http)
    }
}

/// A cloud instance metadata server: a GET, usually behind a header that proves
/// the call was not made by a browser or a confused proxy.
///
/// The endpoint is not held to the https rule the token exchanges are: every
/// cloud serves its metadata over plaintext on a link-local address, and the
/// credential comes back over a hop that never leaves the instance.
pub struct MetadataServer {
    http: HttpClient,
    url: String,
    headers: Vec<(HeaderName, String)>,
    reading: TokenReading,
}

impl MetadataServer {
    /// Read a credential from `url`.
    ///
    /// `http` is the settings the exchange takes, as for
    /// [`ClientCredentials::new`].
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Client`] when the exchange's own client cannot be
    /// built.
    pub fn new(http: &HttpClient, url: impl Into<String>) -> Result<Self, AuthError> {
        Ok(Self {
            http: exchange_client(http, http.config().retry_non_idempotent)?,
            url: url.into(),
            headers: Vec::new(),
            reading: TokenReading::default(),
        })
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

/// Hand-written: the endpoint alone, as for [`ClientCredentials`]; a metadata
/// server can want a header whose value is itself a credential.
impl fmt::Debug for MetadataServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataServer")
            .field("url", &endpoint_name(&self.url))
            .finish_non_exhaustive()
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
                url: endpoint_name(&self.url),
                source: Box::new(e.without_url()),
            })?;
        let body = json_body(response, &self.url).await?;
        self.reading.read(&self.url, &body)
    }

    fn timeout(&self) -> Duration {
        exchange_deadline(&self.http)
    }
}

/// The client a token exchange uses: the caller's own settings, with redirects
/// refused and the POST retried only when `retry_post` says it is safe to resend.
///
/// Redirects are refused because reqwest carries the form and any custom header
/// across a cross-origin hop, which hands the credential to whatever host the
/// endpoint names. `retry_post` is the exchange's own call, because only the
/// exchange knows whether its request carries anything single-use.
fn exchange_client(template: &HttpClient, retry_post: bool) -> Result<HttpClient, AuthError> {
    let config = HttpClientConfig {
        retry_non_idempotent: retry_post,
        ..template.config().clone()
    };
    HttpClient::with_redirect_policy(config, reqwest::redirect::Policy::none())
        .map_err(|source| AuthError::Client { source })
}

/// The bound on one acquisition: the client's own per-request timeout over
/// every attempt it may make, so the deadline cannot fire while a retry the
/// client itself scheduled is still in flight.
fn exchange_deadline(http: &HttpClient) -> Duration {
    let config = http.config();
    let attempts = config.max_retries.saturating_add(1);
    Duration::from_secs(config.timeout_secs).saturating_mul(attempts)
        + Duration::from_millis(config.max_retry_interval_ms).saturating_mul(config.max_retries)
}

/// A token endpoint carries the client secret in the form it is posted, so a
/// plaintext hop hands that secret to anyone on the path. Loopback is allowed
/// so a test fixture needs no certificate.
fn require_secure_endpoint(url: &str) -> Result<(), AuthError> {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return Err(AuthError::Endpoint {
            url: endpoint_name(url),
            reason: "not a URL".to_owned(),
        });
    };
    if parsed.scheme() == "https" || is_loopback(&parsed) {
        return Ok(());
    }
    Err(AuthError::Endpoint {
        url: endpoint_name(url),
        reason: "must be https, or a loopback address".to_owned(),
    })
}

/// Whether the host is this machine, by name or by either address family.
fn is_loopback(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    // An IPv6 host comes back in the brackets the URL wrote it in.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

/// POST a rendered form through the exchange's client.
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
            url: endpoint_name(url),
            source: Box::new(e.without_url()),
        })?;
    json_body(response, url).await
}

/// The response body as JSON, or the refusal named.
async fn json_body(response: reqwest::Response, url: &str) -> Result<Value, AuthError> {
    let endpoint = endpoint_name(url);
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AuthError::Refused {
            url: endpoint,
            status: status.as_u16(),
            detail: refusal_detail(&body),
        });
    }
    response.json().await.map_err(|e| AuthError::Malformed {
        url: endpoint,
        reason: e.without_url().to_string(),
    })
}

/// What a refusal is allowed to carry: the error fields RFC 6749 s5.2 names,
/// and nothing else.
///
/// The body is never kept whole. A token endpoint that echoes the form it was
/// posted -- or names the failing field and quotes its value -- would otherwise
/// put the client secret in the error text, and an error text is the one thing
/// every consumer logs.
fn refusal_detail(body: &str) -> String {
    let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(body) else {
        return NO_REFUSAL_DETAIL.to_owned();
    };
    let named: Vec<String> = ["error", "error_description"]
        .into_iter()
        .filter_map(|name| {
            fields
                .get(name)
                .and_then(Value::as_str)
                .map(|value| format!("{name}={value}"))
        })
        .collect();
    if named.is_empty() {
        return NO_REFUSAL_DETAIL.to_owned();
    }
    named.join(", ").chars().take(MAX_REFUSAL_DETAIL).collect()
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

    fn read(raw: &str, reading: &TokenReading) -> Result<Credential, AuthError> {
        reading.read("https://idp.example/token", &response(raw))
    }

    #[test]
    fn expires_in_is_read_as_a_number_or_a_string() {
        let reading = TokenReading::default().with_renew_margin(Duration::ZERO);
        let numeric = read("{\"access_token\":\"a\",\"expires_in\":120}", &reading).unwrap();
        let stringly = read("{\"access_token\":\"a\",\"expires_in\":\"120\"}", &reading).unwrap();

        let floor = Instant::now() + Duration::from_secs(60);
        assert!(numeric.renew_at > floor);
        assert!(stringly.renew_at > floor);
    }

    #[test]
    fn an_expiry_that_is_not_a_number_of_seconds_takes_the_fallback() {
        let reading = TokenReading::default()
            .with_expires_in_fallback(Duration::from_secs(600))
            .with_renew_margin(Duration::from_secs(60));

        let credential =
            read("{\"access_token\":\"a\",\"expires_in\":\"soon\"}", &reading).unwrap();

        let now = Instant::now();
        assert!(credential.renew_at > now + Duration::from_secs(400));
        assert!(credential.renew_at <= now + Duration::from_secs(540));
    }

    #[test]
    fn an_absurd_expiry_is_clamped_rather_than_overflowing() {
        let reading = TokenReading::default();
        let ceiling = Instant::now() + MAX_CREDENTIAL_LIFETIME;

        for raw in [
            "{\"access_token\":\"a\",\"expires_in\":18446744073709551615}",
            "{\"access_token\":\"a\",\"expires_in\":\"18446744073709551615\"}",
        ] {
            let credential = read(raw, &reading).expect("still a credential");
            assert!(
                credential.renew_at <= ceiling,
                "a wire value cannot outrun the ceiling"
            );
        }
    }

    #[test]
    fn a_missing_expiry_takes_the_fallback_less_the_margin() {
        let reading = TokenReading::default()
            .with_expires_in_fallback(Duration::from_secs(600))
            .with_renew_margin(Duration::from_secs(60));
        let credential = read("{\"access_token\":\"a\"}", &reading).unwrap();

        let now = Instant::now();
        assert!(credential.renew_at > now + Duration::from_secs(400));
        assert!(credential.renew_at <= now + Duration::from_secs(540));
    }

    #[test]
    fn a_margin_longer_than_the_lifetime_still_holds_half_of_it() {
        let reading = TokenReading::default().with_renew_margin(Duration::from_secs(600));
        let credential = read("{\"access_token\":\"a\",\"expires_in\":30}", &reading).unwrap();

        let now = Instant::now();
        assert!(
            !credential.is_due(),
            "a margin over the lifetime cannot make every request an exchange"
        );
        assert!(credential.renew_at <= now + Duration::from_secs(15));
    }

    #[test]
    fn only_the_named_fields_are_exposed() {
        let bare = read(
            "{\"access_token\":\"a\",\"instance_url\":\"https://shard\"}",
            &TokenReading::default(),
        )
        .unwrap();
        assert!(bare.extra.is_none(), "nothing asked for, nothing carried");

        let exposed = read(
            "{\"access_token\":\"a\",\"instance_url\":\"https://shard\"}",
            &TokenReading::default().expose_field("instance_url"),
        )
        .unwrap();
        let extra = exposed.extra.as_deref().unwrap();
        assert_eq!(extra["instance_url"], "https://shard");
        assert!(extra.get("access_token").is_none());
    }

    #[test]
    fn a_credential_field_is_never_exposable() {
        for name in NEVER_EXPOSED {
            let reading = TokenReading::default().expose_field(name);
            let credential = read(
                "{\"access_token\":\"a\",\"refresh_token\":\"r\",\"id_token\":\"i\",\"client_secret\":\"c\"}",
                &reading,
            )
            .unwrap();
            assert!(
                credential.extra.is_none(),
                "{name} is a credential, not a field to carry"
            );
        }
    }

    #[test]
    fn a_response_with_no_token_is_malformed() {
        let error = read("{\"token_type\":\"Bearer\"}", &TokenReading::default()).unwrap_err();
        assert!(error.to_string().contains("access_token"), "{error}");
    }

    #[test]
    fn a_refusal_keeps_the_error_fields_and_nothing_else() {
        assert_eq!(
            refusal_detail("{\"error\":\"invalid_client\",\"error_description\":\"bad id\"}"),
            "error=invalid_client, error_description=bad id"
        );
        assert_eq!(
            refusal_detail("client_id=a&client_secret=s3cr3t-do-not-print"),
            NO_REFUSAL_DETAIL,
            "an echoed form is not JSON and is not kept"
        );
        assert_eq!(
            refusal_detail("{\"client_secret\":\"s3cr3t-do-not-print\"}"),
            NO_REFUSAL_DETAIL,
            "JSON is kept field by field, not whole"
        );
    }

    #[test]
    fn a_plaintext_token_endpoint_is_refused() {
        let error =
            require_secure_endpoint("http://idp.example/token").expect_err("plaintext refused");
        assert!(error.to_string().contains("https"), "{error}");

        require_secure_endpoint("https://idp.example/token").expect("https is the point");
        require_secure_endpoint("http://127.0.0.1:8080/token").expect("a loopback fixture");
        require_secure_endpoint("http://localhost:8080/token").expect("a loopback fixture");
        require_secure_endpoint("http://[::1]:8080/token").expect("a loopback fixture");
    }

    #[tokio::test]
    async fn a_static_credential_is_never_due() {
        let held = Cached::new(Static::new("static-token"));
        let credential = held.credential().await.unwrap();
        assert_eq!(credential.secret.expose(), "static-token");
        assert!(!credential.is_due());
        assert!(held.held().is_some());
    }

    /// An exchange that never answers is abandoned at its own deadline, so a
    /// hung acquisition cannot hold the renewal lock for ever.
    #[tokio::test]
    async fn a_hung_exchange_is_abandoned_at_its_own_deadline() {
        struct Hung;
        impl Exchange for Hung {
            async fn acquire(&self) -> Result<Credential, AuthError> {
                std::future::pending().await
            }
            fn timeout(&self) -> Duration {
                Duration::from_millis(50)
            }
        }
        let source = Cached::new(Hung);

        let error = source.credential().await.expect_err("nothing ever answers");

        assert!(matches!(error, AuthError::TimedOut { .. }), "{error}");
        assert!(error.is_transient());
        assert!(source.held().is_none());
    }

    #[tokio::test]
    async fn an_invalidated_source_holds_nothing() {
        let held = Cached::new(Static::new("static-token"));
        held.credential().await.unwrap();

        held.invalidate();

        assert!(held.held().is_none());
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

    #[test]
    fn an_exchange_debug_names_the_endpoint_and_nothing_it_posts() {
        let http = HttpClient::new(HttpClientConfig::default()).unwrap();
        let exchange = ClientCredentials::new(
            &http,
            "https://idp.example/token?wrapping=s3cr3t-do-not-print",
            "client-42",
            SensitiveString::new("s3cr3t-do-not-print"),
        )
        .unwrap()
        .with_form_field("audience", "s3cr3t-do-not-print");

        let rendered = format!("{exchange:?}");

        assert!(!rendered.contains("s3cr3t-do-not-print"), "{rendered}");
        assert!(!rendered.contains("client-42"), "{rendered}");
        assert!(!rendered.contains("audience"), "{rendered}");
        assert!(rendered.contains("https://idp.example/token"), "{rendered}");
    }

    #[test]
    fn a_failure_stands_for_its_waiters_and_a_refusal_for_the_backoff() {
        let refused = Failure::of(
            &AuthError::Refused {
                url: "https://idp.example/token".to_owned(),
                status: 401,
                detail: "error=invalid_client".to_owned(),
            },
            3,
        );
        assert!(
            refused.stands_for(2, Duration::ZERO),
            "queued before it finished"
        );
        assert!(
            refused.stands_for(3, Duration::from_secs(60)),
            "inside the backoff"
        );
        assert!(
            !refused.stands_for(3, Duration::ZERO),
            "no backoff, not a waiter"
        );

        let timed_out = Failure::of(&AuthError::TimedOut { secs: 30 }, 3);
        assert!(
            timed_out.stands_for(2, Duration::ZERO),
            "queued before it finished"
        );
        assert!(
            !timed_out.stands_for(3, Duration::from_secs(60)),
            "a transient failure is not held: the next caller tries again"
        );
    }

    #[test]
    fn an_exchange_is_bounded_by_its_own_clients_attempts() {
        let http = HttpClient::new(HttpClientConfig {
            timeout_secs: 2,
            max_retries: 0,
            ..Default::default()
        })
        .unwrap();
        let exchange = TokenPost::new(&http, "https://idp.example/token").unwrap();

        assert_eq!(exchange.timeout(), Duration::from_secs(2));
    }
}
