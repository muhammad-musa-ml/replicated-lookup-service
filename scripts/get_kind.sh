#!/usr/bin/env bash
# Download kind and kubectl into .tools/ and verify each against the published
# sha256 before it is made executable. Used for the Kubernetes check in deploy/k8s.
#   scripts/get_kind.sh [kind-version] [kubectl-version]
set -euo pipefail

cd "$(dirname "$0")/.."
KIND_VERSION=${1:-v0.33.0}
KUBECTL_VERSION=${2:-v1.34.0}
case "$(uname -s)" in
  Linux*) OS=linux; EXT="" ;;
  Darwin*) OS=darwin; EXT="" ;;
  *) OS=windows; EXT=".exe" ;;
esac
ARCH=amd64
mkdir -p .tools

fetch_checked() {  # url, sha256-url, destination
  local url=$1 sum_url=$2 dest=$3 tmp
  tmp=$(mktemp)
  curl -fsSL -o "$tmp" "$url"
  local want got
  want=$(curl -fsSL "$sum_url" | awk '{print $1}')
  got=$(sha256sum "$tmp" | awk '{print $1}')
  if [ "$want" != "$got" ]; then
    echo "checksum mismatch for $url: want $want, got $got" >&2
    rm -f "$tmp"
    exit 1
  fi
  mv "$tmp" "$dest"
  chmod +x "$dest"
  echo "$dest  sha256 $got"
}

fetch_checked \
  "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${OS}-${ARCH}" \
  "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${OS}-${ARCH}.sha256sum" \
  ".tools/kind${EXT}"

fetch_checked \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/${OS}/${ARCH}/kubectl${EXT}" \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/${OS}/${ARCH}/kubectl${EXT}.sha256" \
  ".tools/kubectl${EXT}"
