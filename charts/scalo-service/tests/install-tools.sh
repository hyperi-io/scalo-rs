#!/usr/bin/env bash
# Project:   scalo
# File:      charts/scalo-service/tests/install-tools.sh
# Purpose:   Install the pinned helm, helm-unittest and kubeconform the chart tests run
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage: charts/scalo-service/tests/install-tools.sh DIR
#        charts/scalo-service/tests/install-tools.sh --versions
#
# Writes DIR/bin/helm, DIR/bin/kubeconform and DIR/helm-plugins/unittest, each
# checked against the sha256 its project publishes, and DIR/versions naming
# what it installed. Then put DIR/bin on PATH and set
# HELM_PLUGINS=DIR/helm-plugins. Linux amd64 and arm64. --versions prints the
# pinned versions in the form DIR/versions holds.

set -euo pipefail

# helm v4.3.0 (2026-09-09), helm-unittest v1.2.1 (2026-10-03), kubeconform v0.8.0 (2026-06-04).
HELM_VERSION="v4.3.0"
UNITTEST_VERSION="1.2.1"
KUBECONFORM_VERSION="v0.8.0"
versions="helm=$HELM_VERSION helm-unittest=$UNITTEST_VERSION kubeconform=$KUBECONFORM_VERSION"

if [[ "${1:-}" == "--versions" ]]; then
  echo "$versions"
  exit 0
fi

dir="${1:?usage: install-tools.sh DIR}"
case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "install-tools.sh: no pinned build for $(uname -m)" >&2; exit 2 ;;
esac
if [[ "$(uname -s)" != Linux ]]; then
  echo "install-tools.sh: Linux only; elsewhere install helm 4, helm-unittest and kubeconform yourself" >&2
  exit 2
fi

declare -A sums=(
  [helm-amd64]=86584a54def73570558f66f5111cc53dfed56689637ae32c1201205d494f54fb
  [helm-arm64]=31c5794dd55c66a51e6b7d2e2ac7a114ae8b1de41ff1d9ba51748ac973b06a08
  [unittest-amd64]=48e86ecc1467b009630e5954375e5d8494139f73b466df5fe8c04fa45b634939
  [unittest-arm64]=c66062713cd3cbb1a80d7098bd1d4a48db8438f56b43fb6377df65447f93f8de
  [kubeconform-amd64]=9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883
  [kubeconform-arm64]=1f53fc8e81258197a35e8603054162a5af1de8c5af13746c71ab680d9534ed87
)

downloads="$(mktemp -d)"
trap 'rm -rf "$downloads"' EXIT

fetch() {
  local url="$1" file="$2" sum="$3"
  curl -sSfL --retry 3 -o "$downloads/$file" "$url"
  echo "$sum  $downloads/$file" | sha256sum --check --quiet
}

mkdir -p "$dir/bin" "$dir/helm-plugins/unittest"

fetch "https://get.helm.sh/helm-$HELM_VERSION-linux-$arch.tar.gz" helm.tgz "${sums[helm-$arch]}"
tar -xzf "$downloads/helm.tgz" -C "$downloads" "linux-$arch/helm"
install -m 0755 "$downloads/linux-$arch/helm" "$dir/bin/helm"

fetch "https://github.com/yannh/kubeconform/releases/download/$KUBECONFORM_VERSION/kubeconform-linux-$arch.tar.gz" kubeconform.tgz "${sums[kubeconform-$arch]}"
tar -xzf "$downloads/kubeconform.tgz" -C "$downloads" kubeconform
install -m 0755 "$downloads/kubeconform" "$dir/bin/kubeconform"

fetch "https://github.com/helm-unittest/helm-unittest/releases/download/v$UNITTEST_VERSION/helm-unittest-linux-$arch-$UNITTEST_VERSION.tgz" unittest.tgz "${sums[unittest-$arch]}"
tar -xzf "$downloads/unittest.tgz" -C "$dir/helm-plugins/unittest"

echo "$versions" > "$dir/versions"
echo "installed $versions into $dir"
