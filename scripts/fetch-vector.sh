#!/usr/bin/env bash
# Project:   scalo
# File:      scripts/fetch-vector.sh
# Purpose:   Download and cache Vector binary for integration tests
# Language:  Bash
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   ./scripts/fetch-vector.sh              # ensure the pinned version, print path
#   VECTOR_VERSION=0.43.0 ./scripts/fetch-vector.sh  # override the pin
#
# Prints the absolute path to the vector binary on stdout (last line). Status
# messages go to stderr.
#
# SAFE TO RUN CONCURRENTLY. Every test in tests/e2e/vector_compat.rs calls this:
# the OnceLock that caches the result is per-PROCESS and nextest runs one process
# per test, so N tests invoke it at once. Two things make that safe:
#
#   - The cache directory is keyed by version, so no run ever deletes a binary
#     another run is about to exec. The previous "rm -rf bin, then rebuild it in
#     place" produced `ETXTBSY` ("Text file busy") when one test exec'd the
#     binary while another still held it open for writing.
#   - Download and extraction happen in a private temp directory, promoted with
#     one rename. A reader therefore sees either no directory or a complete one,
#     never a half-extracted binary. A mkdir lock keeps concurrent runs from
#     downloading the same tarball N times; losing the lock race means waiting
#     for the winner, not failing.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${REPO_ROOT}/.tmp/vector"
ARCH="$(uname -m)"

# Pinned. A floating "latest" made the harness retarget on every upstream
# release, so a break landed with nothing in the diff to explain it -- and it
# needed network plus gh/jq on every single test run. Bump deliberately; Vector
# minor releases change CLI flags and config schema.
# renovate: datasource=github-releases depName=vectordotdev/vector
DEFAULT_VECTOR_VERSION="0.56.0"

WANT_VERSION="${VECTOR_VERSION:-$DEFAULT_VECTOR_VERSION}"

# Version-keyed, so a fetch for one version cannot disturb another.
VERSION_DIR="${CACHE_DIR}/${WANT_VERSION}-${ARCH}"
BINARY="${VERSION_DIR}/vector"
LOCK_DIR="${CACHE_DIR}/.lock-${WANT_VERSION}-${ARCH}"

if [[ -x "$BINARY" ]]; then
    echo "Vector ${WANT_VERSION} already cached" >&2
    echo "$BINARY"
    exit 0
fi

mkdir -p "${CACHE_DIR}"

# mkdir is atomic on POSIX: exactly one concurrent run creates the lock and
# downloads; the rest wait for the binary to appear.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Another run is fetching Vector ${WANT_VERSION}; waiting..." >&2
    for _ in $(seq 1 300); do
        if [[ -x "$BINARY" ]]; then
            echo "$BINARY"
            exit 0
        fi
        sleep 1
    done
    echo "ERROR: timed out waiting for another run to cache Vector ${WANT_VERSION}" >&2
    echo "If no fetch is running, remove the stale lock: ${LOCK_DIR}" >&2
    exit 1
fi

# Release the lock however we leave, so a failed download does not wedge every
# later run behind a lock nobody holds.
cleanup() {
    rm -rf "$LOCK_DIR" "${WORK_DIR:-}"
}
trap cleanup EXIT

WORK_DIR="$(mktemp -d "${CACHE_DIR}/.fetch-XXXXXX")"

echo "Downloading Vector ${WANT_VERSION} for ${ARCH}..." >&2
TARBALL_NAME="vector-${WANT_VERSION}-${ARCH}-unknown-linux-gnu.tar.gz"
DOWNLOAD_URL="https://github.com/vectordotdev/vector/releases/download/v${WANT_VERSION}/${TARBALL_NAME}"

curl -fSL --progress-bar -o "${WORK_DIR}/${TARBALL_NAME}" "$DOWNLOAD_URL"

echo "Extracting..." >&2
tar xzf "${WORK_DIR}/${TARBALL_NAME}" -C "${WORK_DIR}"

# The tarball contains vector-{ARCH}-unknown-linux-gnu/bin/vector.
EXTRACTED_BIN="${WORK_DIR}/vector-${ARCH}-unknown-linux-gnu/bin"
if [[ ! -x "${EXTRACTED_BIN}/vector" ]]; then
    echo "ERROR: no vector binary in ${TARBALL_NAME} at the expected path" >&2
    exit 1
fi

# One rename publishes the whole directory. `mv` into an existing target would
# nest it, so check first -- the target existing means a concurrent run beat us,
# which is a success, not a conflict.
if [[ -x "$BINARY" ]]; then
    echo "Vector ${WANT_VERSION} cached by a concurrent run" >&2
else
    mv "$EXTRACTED_BIN" "$VERSION_DIR"
fi

if [[ ! -x "$BINARY" ]]; then
    echo "ERROR: Vector binary not found at ${BINARY} after extraction" >&2
    exit 1
fi

echo "Vector ${WANT_VERSION} cached at ${BINARY}" >&2
echo "$BINARY"
