// Project:   scalo
// File:      src/response_body.rs
// Purpose:   Read an HTTP response body under a byte cap
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A peer's response body read under a cap, so a broken or hostile server
//! can't make a client buffer more than the caller expects.

/// Why a capped body read stopped.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CappedReadError {
    /// The body declared or ran past the cap.
    #[error("response body over {limit} bytes")]
    TooLarge {
        /// The cap, in bytes.
        limit: usize,
    },
    /// The body could not be read.
    #[error("cannot read response body: {0}")]
    Read(reqwest::Error),
}

/// `response`'s body, refused once it declares or reads more than `limit`
/// bytes.
pub(crate) async fn read_capped(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, CappedReadError> {
    let too_large = CappedReadError::TooLarge { limit };
    let declared = response.content_length().unwrap_or(0);
    if declared > limit as u64 {
        return Err(too_large);
    }
    let mut body = Vec::with_capacity(usize::try_from(declared).unwrap_or(0));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| CappedReadError::Read(e.without_url()))?
    {
        if body.len() + chunk.len() > limit {
            return Err(too_large);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
