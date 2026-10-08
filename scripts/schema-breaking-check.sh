#!/usr/bin/env bash
# Project:   scalo
# File:      scripts/schema-breaking-check.sh
# Purpose:   Fail when a published schema refuses what its last release accepted
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage: scripts/schema-breaking-check.sh [REF]
#
# REF defaults to the newest v* tag reachable from HEAD. Every schema file REF
# carries is compared with the working tree by `breaking_changes`:
#
#   charts/scalo-service/schema/deployment-contract.v<N>.schema.json
#     A released version changes compatibly or not at all. A breaking change
#     goes into a new v<N+1> file, with CONTRACT_SCHEMA_VERSION at N+1.
#   charts/scalo-service/skeleton/values.schema.json
#     A breaking change passes only beside that schema_version bump, the
#     handshake that tells every consumer to move.
#
# Needs the full git history and tags (fetch-depth: 0) and cargo.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [[ "$(git rev-parse --is-shallow-repository)" == "true" ]]; then
  echo "schema-breaking-check: this clone is shallow, so the last release cannot be read; fetch with full history and tags" >&2
  exit 2
fi

ref="${1:-}"
if [[ -z "$ref" ]]; then
  ref="$(git describe --tags --abbrev=0 --match 'v[0-9]*' HEAD 2>/dev/null || true)"
fi
if [[ -z "$ref" ]]; then
  echo "schema-breaking-check: no v* tag is reachable from HEAD, so there is no release to compare against" >&2
  exit 2
fi

schema_dir="charts/scalo-service/schema"
skeleton="charts/scalo-service/skeleton/values.schema.json"
pattern='^deployment-contract\.v([0-9]+)\.schema\.json$'

# The highest contract schema version among file names read on stdin.
highest_version() {
  local highest=0 name
  while read -r name; do
    if [[ "$(basename "$name")" =~ $pattern ]] && (( BASH_REMATCH[1] > highest )); then
      highest="${BASH_REMATCH[1]}"
    fi
  done
  echo "$highest"
}

released_files="$(git ls-tree --name-only "$ref" -- "$schema_dir/" || true)"
released_version="$(highest_version <<< "$released_files")"
current_version="$(find "$schema_dir" -maxdepth 1 -name 'deployment-contract.v*.schema.json' | highest_version)"

cargo build --quiet --example schema_breaking --no-default-features --features deployment
compare="${CARGO_TARGET_DIR:-$root/target}/debug/examples/schema_breaking"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failures=0

for path in $released_files; do
  [[ "$(basename "$path")" =~ $pattern ]] || continue
  if [[ ! -f "$path" ]]; then
    echo "FAIL: $path was released in $ref and is gone; a released schema version stays, frozen" >&2
    failures=$((failures + 1))
    continue
  fi
  git show "$ref:$path" > "$work/old.json"
  "$compare" "$work/old.json" "$path" || failures=$((failures + 1))
done

if git cat-file -e "$ref:$skeleton" 2>/dev/null; then
  git show "$ref:$skeleton" > "$work/skeleton.json"
  if ! "$compare" "$work/skeleton.json" "$skeleton"; then
    if (( current_version > released_version )); then
      echo "schema-breaking-check: $skeleton breaks, beside a schema_version bump from $released_version to $current_version"
    else
      echo "FAIL: $skeleton breaks without a schema_version bump; release $ref is at $released_version" >&2
      failures=$((failures + 1))
    fi
  fi
else
  echo "schema-breaking-check: $ref carries no $skeleton, so there is nothing to compare it with"
fi

if [[ -z "$released_files" ]]; then
  echo "schema-breaking-check: $ref carries no $schema_dir, so v$current_version has no released copy yet"
fi

if (( failures > 0 )); then
  echo "schema-breaking-check: $failures breaking change set(s) against $ref" >&2
  exit 1
fi
echo "schema-breaking-check: no breaking change against $ref"
