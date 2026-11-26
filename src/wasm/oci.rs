// Project:   scalo
// File:      src/wasm/oci.rs
// Purpose:   Pulls a plugin artefact from an OCI registry for the loader
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pulls a plugin from an OCI registry and returns the bytes
//! [`WasmEngine::load`](super::WasmEngine::load) takes.
//!
//! The pull checks the plugin's contents against its manifest digest and
//! nothing else. The loader still runs the digest pin, the signature check
//! and every limit on the bytes it is given.

use std::time::Duration;

use futures_util::TryStreamExt;
use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::errors::OciDistributionError;
use oci_client::manifest::{OciDescriptor, OciManifest};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, ParseError, Reference};
use sha2::{Digest, Sha256};

use super::engine::hex;
use super::trust::parse_sha256;
use crate::sensitive::SensitiveString;

/// The layer holding the `.wasm`.
pub const WASM_MEDIA_TYPE: &str = "application/wasm";

/// The layer holding a keyless Sigstore bundle, JSON.
pub const BUNDLE_MEDIA_TYPE: &str = "application/vnd.dev.sigstore.bundle.v0.3+json";

/// The layer holding a key signature, base64 text, the bytes of a `name.wasm.sig` file.
pub const SIGNATURE_MEDIA_TYPE: &str = "application/vnd.scalo.wasm.signature.v1";

/// A connection that takes longer than this to open fails.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A read that stalls for this long fails.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Credentials for the registry.
#[derive(Debug, Clone)]
pub enum OciAuth {
    /// No credentials.
    Anonymous,
    /// HTTP Basic credentials. The password is redacted in `Debug` and logs.
    Basic {
        /// The user name.
        username: String,
        /// The password.
        password: SensitiveString,
    },
}

/// The layers of one plugin artefact.
#[derive(Debug)]
pub struct Pulled {
    /// The `.wasm`.
    pub wasm: Vec<u8>,
    /// The keyless Sigstore bundle, JSON, if the artefact carries one.
    pub bundle: Option<Vec<u8>>,
    /// The key signature, base64 text, if the artefact carries one.
    pub signature: Option<Vec<u8>>,
}

/// Why a pull returned no plugin.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PullError {
    /// The reference does not parse.
    #[error("{reference} is not an OCI reference: {source}")]
    Reference {
        /// The reference as given.
        reference: String,
        /// The parse error.
        #[source]
        source: ParseError,
    },
    /// The registry refused the request, or its reply is malformed.
    #[error("registry: {0}")]
    Registry(#[source] OciDistributionError),
    /// The registry returned an image index where one artefact manifest was expected.
    #[error("the registry returned an image index, not an artefact manifest")]
    NotAnImage,
    /// The manifest has no `application/wasm` layer.
    #[error("the manifest has no application/wasm layer")]
    NoWasmLayer,
    /// The manifest has more layers of one media type than a plugin takes.
    #[error("the manifest has {count} {media_type} layers, and at most one is allowed")]
    ExtraLayers {
        /// The media type that repeats.
        media_type: &'static str,
        /// How many layers carry it.
        count: usize,
    },
    /// A layer's descriptor cannot be used.
    #[error("the {media_type} layer's descriptor is unusable: {detail}")]
    BadDescriptor {
        /// The layer's media type.
        media_type: String,
        /// What is wrong with it.
        detail: String,
    },
    /// A layer is over the `max_bytes` cap.
    #[error("the {media_type} layer is over the {limit}-byte cap")]
    TooLarge {
        /// The layer's media type.
        media_type: String,
        /// The cap.
        limit: usize,
    },
    /// A layer's bytes do not hash to the digest its manifest gives.
    #[error("the {media_type} layer hashes to {actual}, not the manifest's {expected}")]
    DigestMismatch {
        /// The layer's media type.
        media_type: String,
        /// The digest the manifest gives, hex.
        expected: String,
        /// The digest of the bytes, hex.
        actual: String,
    },
    /// Reading a layer failed partway through.
    #[error("reading the {media_type} layer: {source}")]
    Download {
        /// The layer's media type.
        media_type: String,
        /// The read error.
        #[source]
        source: std::io::Error,
    },
}

/// Pulls the plugin at `reference` over HTTPS and returns its layers.
///
/// `max_bytes` caps each layer while it downloads. Pull by digest
/// (`name@sha256:...`) or sign the plugin: a tag can move to other bytes.
/// Pass the result to [`WasmEngine::load`](super::WasmEngine::load), which
/// checks it.
///
/// # Errors
///
/// A [`PullError`] for an unparsable reference, a registry refusal, a
/// manifest without exactly one `application/wasm` layer, a layer over
/// `max_bytes`, or a layer whose bytes do not match its digest.
pub async fn pull(reference: &str, auth: OciAuth, max_bytes: usize) -> Result<Pulled, PullError> {
    pull_with(reference, auth, max_bytes, ClientProtocol::Https).await
}

/// [`pull`] over plain HTTP, for a local registry or a test. Logs a warning on every call.
///
/// # Errors
///
/// The same as [`pull`].
pub async fn pull_insecure_http(
    reference: &str,
    auth: OciAuth,
    max_bytes: usize,
) -> Result<Pulled, PullError> {
    tracing::warn!(reference = %reference, "wasm: pulling a plugin over plain HTTP");
    pull_with(reference, auth, max_bytes, ClientProtocol::Http).await
}

async fn pull_with(
    reference: &str,
    auth: OciAuth,
    max_bytes: usize,
    protocol: ClientProtocol,
) -> Result<Pulled, PullError> {
    let parsed: Reference = reference.parse().map_err(|source| PullError::Reference {
        reference: reference.to_string(),
        source,
    })?;
    let auth = match auth {
        OciAuth::Anonymous => RegistryAuth::Anonymous,
        OciAuth::Basic { username, password } => {
            RegistryAuth::Basic(username, password.expose().to_string())
        }
    };
    // One client per pull: a client keeps the first credentials it sees for a
    // registry, so a shared one would send this caller's to the next.
    let client = Client::new(ClientConfig {
        protocol,
        connect_timeout: Some(CONNECT_TIMEOUT),
        read_timeout: Some(READ_TIMEOUT),
        ..ClientConfig::default()
    });
    let (manifest, _digest) = client
        .pull_manifest(&parsed, &auth)
        .await
        .map_err(PullError::Registry)?;
    let OciManifest::Image(manifest) = manifest else {
        return Err(PullError::NotAnImage);
    };
    let layers = choose_layers(&manifest.layers)?;
    let wasm = fetch(&client, &parsed, layers.wasm, max_bytes).await?;
    let bundle = fetch_optional(&client, &parsed, layers.bundle, max_bytes).await?;
    let signature = fetch_optional(&client, &parsed, layers.signature, max_bytes).await?;
    Ok(Pulled {
        wasm,
        bundle,
        signature,
    })
}

/// The layers a plugin artefact carries. Layers of other media types are ignored.
#[derive(Debug)]
struct Layers<'a> {
    wasm: &'a OciDescriptor,
    bundle: Option<&'a OciDescriptor>,
    signature: Option<&'a OciDescriptor>,
}

fn choose_layers(layers: &[OciDescriptor]) -> Result<Layers<'_>, PullError> {
    let wasm = at_most_one(layers, WASM_MEDIA_TYPE)?.ok_or(PullError::NoWasmLayer)?;
    Ok(Layers {
        wasm,
        bundle: at_most_one(layers, BUNDLE_MEDIA_TYPE)?,
        signature: at_most_one(layers, SIGNATURE_MEDIA_TYPE)?,
    })
}

/// The one layer of `media_type`, `None` when there is none, or an error for more than one.
fn at_most_one<'a>(
    layers: &'a [OciDescriptor],
    media_type: &'static str,
) -> Result<Option<&'a OciDescriptor>, PullError> {
    let found: Vec<&OciDescriptor> = layers
        .iter()
        .filter(|layer| layer.media_type == media_type)
        .collect();
    match found.as_slice() {
        [] => Ok(None),
        [layer] => Ok(Some(*layer)),
        many => Err(PullError::ExtraLayers {
            media_type,
            count: many.len(),
        }),
    }
}

async fn fetch_optional(
    client: &Client,
    reference: &Reference,
    layer: Option<&OciDescriptor>,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, PullError> {
    match layer {
        Some(layer) => fetch(client, reference, layer, max_bytes).await.map(Some),
        None => Ok(None),
    }
}

/// Downloads one layer. Refuses past `max_bytes` while the bytes stream in,
/// then checks them against the digest the manifest gives.
async fn fetch(
    client: &Client,
    reference: &Reference,
    layer: &OciDescriptor,
    max_bytes: usize,
) -> Result<Vec<u8>, PullError> {
    let media_type = layer.media_type.clone();
    let expected = expected_digest(layer)?;
    let too_large = || PullError::TooLarge {
        media_type: media_type.clone(),
        limit: max_bytes,
    };
    let declared = usize::try_from(layer.size).map_err(|_| PullError::BadDescriptor {
        media_type: media_type.clone(),
        detail: format!("its size {} is negative", layer.size),
    })?;
    if declared > max_bytes {
        return Err(too_large());
    }

    let sized = client
        .pull_blob_stream(reference, layer)
        .await
        .map_err(PullError::Registry)?;
    collect_checked(sized.stream, &media_type, max_bytes, expected).await
}

/// The bytes of `stream`, refused once they pass `max_bytes` and checked
/// against `expected` at the end.
async fn collect_checked<S, B>(
    mut stream: S,
    media_type: &str,
    max_bytes: usize,
    expected: [u8; 32],
) -> Result<Vec<u8>, PullError>
where
    S: futures_util::Stream<Item = Result<B, std::io::Error>> + Unpin,
    B: AsRef<[u8]>,
{
    let mut bytes = Vec::new();
    let mut hasher = Sha256::new();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|source| PullError::Download {
            media_type: media_type.to_string(),
            source,
        })?
    {
        let chunk = chunk.as_ref();
        if chunk.len() > max_bytes - bytes.len() {
            return Err(PullError::TooLarge {
                media_type: media_type.to_string(),
                limit: max_bytes,
            });
        }
        hasher.update(chunk);
        bytes.extend_from_slice(chunk);
    }

    let actual: [u8; 32] = hasher.finalize().into();
    if actual != expected {
        return Err(PullError::DigestMismatch {
            media_type: media_type.to_string(),
            expected: hex(&expected),
            actual: hex(&actual),
        });
    }
    Ok(bytes)
}

/// The sha256 the layer's manifest gives, as 32 bytes.
fn expected_digest(layer: &OciDescriptor) -> Result<[u8; 32], PullError> {
    let bad = |detail: &str| PullError::BadDescriptor {
        media_type: layer.media_type.clone(),
        detail: detail.to_string(),
    };
    let hex = layer
        .digest
        .strip_prefix("sha256:")
        .ok_or_else(|| bad("its digest is not sha256"))?;
    parse_sha256(hex).ok_or_else(|| bad("its digest is not 64 hex digits"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    fn layer(media_type: &str) -> OciDescriptor {
        OciDescriptor {
            media_type: media_type.to_string(),
            digest: DIGEST.to_string(),
            size: 1,
            urls: None,
            annotations: None,
            artifact_type: None,
        }
    }

    #[tokio::test]
    async fn a_layer_is_refused_past_the_cap_mid_stream_or_on_a_digest_mismatch() {
        let chunks = || vec![b"abc".to_vec(), b"defg".to_vec()];
        let digest: [u8; 32] = Sha256::digest(b"abcdefg").into();
        let collect = |max: usize, expected: [u8; 32]| {
            let stream =
                futures_util::stream::iter(chunks().into_iter().map(Ok::<_, std::io::Error>));
            collect_checked(stream, WASM_MEDIA_TYPE, max, expected)
        };
        assert_eq!(collect(7, digest).await.unwrap(), b"abcdefg");
        assert!(matches!(
            collect(6, digest).await,
            Err(PullError::TooLarge { limit: 6, .. })
        ));
        assert!(matches!(
            collect(7, [0; 32]).await,
            Err(PullError::DigestMismatch { .. })
        ));
    }

    fn label(error: &PullError) -> &'static str {
        match error {
            PullError::NoWasmLayer => "no wasm",
            PullError::ExtraLayers { .. } => "extra",
            _ => "other",
        }
    }

    #[test]
    fn the_manifest_layers_pick_the_wasm_and_its_proofs() {
        let wasm = layer(WASM_MEDIA_TYPE);
        let bundle = layer(BUNDLE_MEDIA_TYPE);
        let signature = layer(SIGNATURE_MEDIA_TYPE);
        let other = layer("application/octet-stream");
        // (case, layers, want: Ok((bundle present, signature present)) or the label of the refusal)
        let cases: [(&str, Vec<OciDescriptor>, Result<(bool, bool), &str>); 7] = [
            ("one wasm layer", vec![wasm.clone()], Ok((false, false))),
            (
                "a bundle and a signature beside it",
                vec![wasm.clone(), bundle.clone(), signature.clone()],
                Ok((true, true)),
            ),
            (
                "a bundle alone",
                vec![wasm.clone(), bundle.clone()],
                Ok((true, false)),
            ),
            ("no wasm layer", vec![bundle.clone()], Err("no wasm")),
            (
                "two wasm layers",
                vec![wasm.clone(), wasm.clone()],
                Err("extra"),
            ),
            (
                "two bundles",
                vec![wasm.clone(), bundle.clone(), bundle.clone()],
                Err("extra"),
            ),
            (
                "a layer of another media type is ignored",
                vec![wasm.clone(), other],
                Ok((false, false)),
            ),
        ];
        for (case, layers, want) in cases {
            let got = choose_layers(&layers)
                .map(|chosen| (chosen.bundle.is_some(), chosen.signature.is_some()))
                .map_err(|error| label(&error));
            assert_eq!(got, want, "{case}");
        }
    }

    #[test]
    fn a_layer_digest_must_be_sha256_with_64_hex_digits() {
        let good = format!("sha256:{}", "ab".repeat(32));
        let cases: [(&str, &str, bool); 4] = [
            ("sha256 with 64 hex digits", &good, true),
            ("sha512", "sha512:00", false),
            ("sha256 but short", "sha256:00ff", false),
            ("no algorithm", "00ff", false),
        ];
        for (case, digest, parses) in cases {
            let mut descriptor = layer(WASM_MEDIA_TYPE);
            descriptor.digest = digest.to_string();
            assert_eq!(expected_digest(&descriptor).is_ok(), parses, "{case}");
        }
    }

    #[test]
    fn a_password_never_appears_in_debug_output() {
        let auth = OciAuth::Basic {
            username: "ci".to_string(),
            password: "hunter2".into(),
        };
        let shown = format!("{auth:?}");
        assert!(shown.contains("ci"), "{shown}");
        assert!(!shown.contains("hunter2"), "{shown}");
    }
}
