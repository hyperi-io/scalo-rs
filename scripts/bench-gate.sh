#!/usr/bin/env bash
# Project:   scalo
# File:      scripts/bench-gate.sh
# Purpose:   Fail when a criterion benchmark's median regresses past a bound
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage: scripts/bench-gate.sh [CRITERION_DIR] [MAX_REGRESSION]
#
# Reads every <bench>/change/estimates.json criterion wrote when a run compared
# itself with a saved baseline, and fails when any median is more than
# MAX_REGRESSION slower (default 0.05, five percent). CRITERION_DIR defaults to
# $CARGO_TARGET_DIR/criterion, else target/criterion.
#
#   git switch <base>   && cargo bench --features rules --bench rules_benchmark -- --save-baseline base
#   git switch <change> && cargo bench --features rules --bench rules_benchmark -- --baseline base
#   scripts/bench-gate.sh
#
# Needs jq.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dir="${1:-${CARGO_TARGET_DIR:-$root/target}/criterion}"
max="${2:-0.05}"

if ! command -v jq >/dev/null; then
  echo "bench-gate: jq is not on PATH" >&2
  exit 2
fi

mapfile -t changes < <(find "$dir" -path '*/change/estimates.json' -print | sort)
if (( ${#changes[@]} == 0 )); then
  echo "bench-gate: no change/estimates.json under $dir; run the bench with --baseline first" >&2
  exit 2
fi

failures=0
for file in "${changes[@]}"; do
  bench="${file#"$dir"/}"
  bench="${bench%/change/estimates.json}"
  median="$(jq -r '.median.point_estimate' "$file")"
  if jq -e --argjson max "$max" '.median.point_estimate > $max' "$file" >/dev/null; then
    echo "FAIL: $bench median changed by $median, over the bound of $max" >&2
    failures=$((failures + 1))
  else
    echo "ok: $bench median changed by $median"
  fi
done

if (( failures > 0 )); then
  echo "bench-gate: $failures benchmark(s) regressed past $max" >&2
  exit 1
fi
echo "bench-gate: ${#changes[@]} benchmark(s) within $max of the baseline"
