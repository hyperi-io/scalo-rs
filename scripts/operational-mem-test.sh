#!/usr/bin/env bash
# Project:   scalo
# File:      scripts/operational-mem-test.sh
# Purpose:   Cgroup-confined memory backpressure operational test (black box)
# Language:  Bash
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Proves the MemoryGuard cap ACTUALLY works in operation, not just in a unit
# test, with a with/without control (the property most often skipped):
#
#   cap=on  under --memory=$MEM  -> survives (exit 0) AND backpressures
#                                   (rejected > 0). Held memory plateaus below
#                                   the limit; the kernel never OOM-kills it.
#   cap=off under --memory=$MEM  -> OOM-killed (non-zero exit, 137). The control
#                                   proves the limit is real and the load
#                                   genuinely over-subscribes -- if cap=off also
#                                   survived, the test would be proving nothing.
#
# This is BLACK BOX: it asserts on the kernel outcome (exit/OOM) and the
# harness's stdout backpressure counters, NOT on scalo internals.
#
# Usage:  scripts/operational-mem-test.sh [MEM] [DURATION_SECS]
#   MEM            docker --memory value           (default 512m)
#   DURATION_SECS  harness run length for cap=on    (default 15)
#
# Requires docker. Exit 0 = both legs pass.

set -euo pipefail

MEM="${1:-512m}"
DURATION="${2:-15}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="scalo-mem-loadgen:optest"
DOCKERFILE="$REPO_ROOT/scripts/operational/Dockerfile.mem_loadgen"

if ! command -v docker >/dev/null 2>&1; then
    echo "SKIP: docker not available" >&2
    exit 2
fi

# Avoid host credential helpers (e.g. docker-credential-secretservice) that are
# absent in headless/CI contexts -- the public base images need no auth.
# Mirrors scalo's contract-artefact e2e (docker_empty_creds_json).
DOCKER_CONFIG_DIR="$(mktemp -d)"
printf '{"auths":{}}\n' >"$DOCKER_CONFIG_DIR/config.json"
export DOCKER_CONFIG="$DOCKER_CONFIG_DIR"
trap 'rm -rf "$DOCKER_CONFIG_DIR"' EXIT

echo "== building harness image (memory feature, pure Rust -- light) =="
docker build -f "$DOCKERFILE" -t "$IMAGE" "$REPO_ROOT"

# Over-subscribe: 1 MiB payloads held 4s, far faster than they drain.
PAYLOAD_BYTES=1048576
COMMON_ENV=(
    -e HARNESS_PAYLOAD_BYTES="$PAYLOAD_BYTES"
    -e HARNESS_RATE_HZ=50000
    -e HARNESS_HOLD_MS=4000
    # Fixed format, and no colour: the init-log assertion below matches on the
    # field text, which ANSI escapes would sit inside.
    -e LOG_FORMAT=text
    -e NO_COLOR=1
)

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== leg 1/2: cap=ON under --memory=$MEM (expect survive + backpressure) =="
on_out="$(docker run --rm --memory="$MEM" --memory-swap="$MEM" \
    "${COMMON_ENV[@]}" -e HARNESS_CAP=on -e HARNESS_DURATION_SECS="$DURATION" \
    "$IMAGE" 2>&1)" && on_rc=0 || on_rc=$?
echo "$on_out"
[ "$on_rc" -eq 0 ] || fail "cap=on was killed (rc=$on_rc) -- backpressure did not hold memory under $MEM"
final_rej="$(printf '%s\n' "$on_out" | sed -n 's/.*rejected=\([0-9]\+\).*/\1/p' | tail -1)"
[ -n "$final_rej" ] && [ "$final_rej" -gt 0 ] \
    || fail "cap=on did not backpressure (rejected=$final_rej); load may not have over-subscribed -- raise rate or lower MEM"

# The guard must be reading the kernel's own accounting. A reservation counter
# sees only what callers hand it, which is what let the brake sit idle while
# the process held 189 MiB.
printf '%s\n' "$on_out" | grep -q "memory guard initialised" \
    || fail "no guard init log: the harness cannot show which usage source the guard resolved"
printf '%s\n' "$on_out" | grep -Eq 'usage_source["=:]+"?cgroup-v[12]' \
    || fail "the guard did not resolve a cgroup usage source: $(printf '%s\n' "$on_out" | grep -o 'usage_source[^ ]*' | head -1)"

# Held memory must plateau at the configured limit, not merely below the
# kernel's cap: admission is bounded to one payload over, and the kernel's
# charge drifts between samples inside the default 15% headroom.
# Anchored to the harness line: the cgroup module logs the raw cap under the
# same field name before the guard applies its headroom.
limit="$(printf '%s\n' "$on_out" | sed -n 's/^mem_loadgen start .*limit_bytes=\([0-9]\+\).*/\1/p' | head -1)"
peak="$(printf '%s\n' "$on_out" | sed -n 's/.*usage_bytes=\([0-9]\+\).*/\1/p' | sort -n | tail -1)"
[ -n "$limit" ] && [ -n "$peak" ] \
    || fail "could not read limit_bytes/usage_bytes from the harness output"
ceiling=$((limit + limit / 20))
[ "$peak" -le "$ceiling" ] \
    || fail "usage peaked at $peak, more than 5% over limit_bytes=$limit (ceiling $ceiling)"
echo "PASS leg 1: survived, rejected=$final_rej (backpressure engaged), peak usage $peak <= $ceiling"

echo "== leg 2/2: cap=OFF under --memory=$MEM (control: expect OOM-kill) =="
# Cap on a short duration; it should OOM well before this.
off_rc=0
docker run --rm --memory="$MEM" --memory-swap="$MEM" \
    "${COMMON_ENV[@]}" -e HARNESS_CAP=off -e HARNESS_DURATION_SECS=30 \
    "$IMAGE" >/dev/null 2>&1 || off_rc=$?
if [ "$off_rc" -eq 0 ]; then
    fail "cap=off SURVIVED under $MEM -- the control failed: either the limit is not enforced or the load did not over-subscribe. The test proves nothing if the control passes."
fi
echo "PASS leg 2: control OOM-killed (rc=$off_rc)"

echo "== OPERATIONAL MEMORY TEST PASSED (cap bounds memory; control OOMs) =="
