// Project:   scalo
// File:      src/accept.rs
// Purpose:   Accept-error handling shared by the crate's TCP listeners
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Accept-error handling shared by the crate's TCP listeners.

use std::io;
use std::time::Duration;

/// How long a listener waits after an accept error that is not the client's.
const PAUSE: Duration = Duration::from_secs(1);

/// The pause after a failed accept. A connection-level error is the client's
/// and costs nothing. Anything else, out of file descriptors say, comes back
/// on the next accept, so the loop would spin.
fn pause_for(kind: io::ErrorKind) -> Option<Duration> {
    match kind {
        io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset => None,
        _ => Some(PAUSE),
    }
}

/// Log a failed accept on `listener` and back off unless it was the client's.
pub(crate) async fn pause_after_error(listener: &'static str, e: &io::Error) {
    let Some(pause) = pause_for(e.kind()) else {
        return;
    };
    tracing::error!(error = %e, listener, "listener accept failed");
    tokio::time::sleep(pause).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_client_error_skips_the_pause() {
        for (kind, pause) in [
            (io::ErrorKind::ConnectionRefused, None),
            (io::ErrorKind::ConnectionAborted, None),
            (io::ErrorKind::ConnectionReset, None),
            (io::ErrorKind::Other, Some(PAUSE)),
            (io::ErrorKind::OutOfMemory, Some(PAUSE)),
            (io::ErrorKind::PermissionDenied, Some(PAUSE)),
        ] {
            assert_eq!(pause_for(kind), pause, "{kind:?}");
        }
    }

    /// EMFILE has no `ErrorKind` of its own, so it lands in the pausing arm.
    #[cfg(unix)]
    #[test]
    fn out_of_file_descriptors_pauses() {
        let emfile = io::Error::from_raw_os_error(24);
        assert_eq!(pause_for(emfile.kind()), Some(PAUSE));
    }
}
