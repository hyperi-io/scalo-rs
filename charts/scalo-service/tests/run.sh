#!/usr/bin/env bash
# Project:   scalo
# File:      charts/scalo-service/tests/run.sh
# Purpose:   Test the scalo-service library chart through its fixture thin charts
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage: charts/scalo-service/tests/run.sh
#
# Runs the pinned helm, helm-unittest and kubeconform from SCALO_CHART_TOOLS
# (default: ~/.cache/scalo-rs/chart-tools), installing them there through
# install-tools.sh when they are missing or a different version, so a local run
# and CI run the same binaries. Needs jq. Works on a copy of the chart, so the
# checkout is never written.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
library="$(dirname "$here")"
crd_catalog='https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

tools="${SCALO_CHART_TOOLS:-${XDG_CACHE_HOME:-$HOME/.cache}/scalo-rs/chart-tools}"
if [[ "$(cat "$tools/versions" 2>/dev/null || true)" != "$("$here/install-tools.sh" --versions)" ]]; then
  "$here/install-tools.sh" "$tools"
fi
export PATH="$tools/bin:$PATH"
export HELM_PLUGINS="$tools/helm-plugins"
if ! command -v jq >/dev/null; then
  echo "run.sh: jq is not on PATH" >&2
  exit 2
fi

stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
cp -R "$library" "$stage/scalo-service"
fixtures="$stage/scalo-service/tests/fixtures"
failures=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# A fixture is a thin chart an assembler could have written: the skeleton's
# templates verbatim, and its values schema plus `config` only.
check_assembled() {
  local fixture="$1" name="$2"
  diff -r "$library/skeleton/templates" "$fixture/templates" || fail "$name: templates/ differs from skeleton/templates/"
  jq -S . "$library/skeleton/values.schema.json" > "$stage/skeleton.json"
  jq -S 'del(.properties.config)' "$fixture/values.schema.json" > "$stage/fixture.json"
  diff "$stage/skeleton.json" "$stage/fixture.json" || fail "$name: values.schema.json is not skeleton/values.schema.json plus config"
}

render_and_validate() {
  local fixture="$1" label="$2"
  shift 2
  if ! helm template release "$fixture" --namespace apps "$@" > "$stage/render.yaml"; then
    fail "$label: helm template"
    return
  fi
  kubeconform -strict -summary -ignore-missing-schemas \
    -schema-location default -schema-location "$crd_catalog" \
    "$stage/render.yaml" || fail "$label: kubeconform"
}

for fixture in "$fixtures"/*/; do
  fixture="${fixture%/}"
  name="$(basename "$fixture")"
  echo "==> fixture $name"
  check_assembled "$fixture" "$name"
  helm dependency build --skip-refresh "$fixture" >/dev/null
  helm lint --strict "$fixture" || fail "$name: helm lint"
  helm unittest --strict "$fixture" || fail "$name: helm unittest"
  render_and_validate "$fixture" "$name [defaults]"
  for values in "$fixture"/ci/*-values.yaml; do
    [[ -e "$values" ]] || continue
    render_and_validate "$fixture" "$name [$(basename "$values")]" -f "$values"
  done
done

# Each expected-fail case starts from a fixture, swaps in its own contract or
# values, and must be refused with the message its `case` file names.
for case_dir in "$library"/tests/expected-fail/*/; do
  case_dir="${case_dir%/}"
  case_name="$(basename "$case_dir")"
  base="$(sed -n 's/^fixture=//p' "$case_dir/case")"
  expected="$(sed -n 's/^error=//p' "$case_dir/case")"
  if [[ -z "$base" || -z "$expected" || ! -d "$fixtures/$base" ]]; then
    fail "$case_name: case file needs fixture= naming a fixture and error="
    continue
  fi
  chart="$fixtures/expected-fail-$case_name"
  cp -R "$fixtures/$base" "$chart"
  if grep -qx 'remove-contract=true' "$case_dir/case"; then
    rm -f "$chart/files/contract.json"
  fi
  if [[ -f "$case_dir/contract.json" ]]; then
    cp "$case_dir/contract.json" "$chart/files/contract.json"
  fi
  args=()
  if [[ -f "$case_dir/values.yaml" ]]; then
    args=(-f "$case_dir/values.yaml")
  fi
  if helm template release "$chart" --namespace apps "${args[@]}" > "$stage/render.yaml" 2> "$stage/error.txt"; then
    fail "$case_name: rendered, and it must be refused"
  elif ! grep -qF -- "$expected" "$stage/error.txt"; then
    fail "$case_name: refused without \"$expected\":"
    cat "$stage/error.txt" >&2
  else
    echo "ok: $case_name refused"
  fi
  rm -rf "$chart"
done

if (( failures > 0 )); then
  echo "run.sh: $failures failure(s)" >&2
  exit 1
fi
echo "run.sh: every fixture and expected-fail case passed"
