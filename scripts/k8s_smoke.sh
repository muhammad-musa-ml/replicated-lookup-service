#!/usr/bin/env bash
# Create a kind cluster, load the images, apply deploy/k8s, check the cache tier
# answers, delete one dataset pod while requests keep coming, then remove the
# cluster. Needs Docker and the tools from scripts/get_kind.sh.
#   scripts/k8s_smoke.sh [--keep]     --keep leaves the cluster running
set -euo pipefail

cd "$(dirname "$0")/.."
KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1
EXT=""
[ -f .tools/kind.exe ] && EXT=".exe"
KIND=".tools/kind$EXT"
KUBECTL=".tools/kubectl$EXT"
CLUSTER=rls
export KUBECONFIG="$PWD/.tools/kubeconfig"
BASE=http://localhost:8250

cleanup() {
  [ "$KEEP" = 1 ] || "$KIND" delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose build data-init dataset-go-1 > /dev/null   # rls-python and rls-go
"$KIND" create cluster --name "$CLUSTER" --config deploy/k8s/kind.yaml --wait 120s
"$KIND" load docker-image rls-python:latest rls-go:latest --name "$CLUSTER"

"$KUBECTL" kustomize --load-restrictor LoadRestrictionsNone deploy/k8s | "$KUBECTL" apply -f -
"$KUBECTL" rollout status statefulset/dataset --timeout=180s
"$KUBECTL" rollout status deployment/cache --timeout=180s
"$KUBECTL" rollout status deployment/table --timeout=180s
"$KUBECTL" get pods -o wide

echo "cache tier through the NodePort:"
for _ in $(seq 1 30); do curl -fsS "$BASE/healthz" > /dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/healthz"; echo
parcels=($(curl -fsS "$BASE/zip/53703?limit=200" | grep -oE '"parcel": ?"[0-9]{12}"' | grep -oE '[0-9]{12}' | sort -u))
ask() {
  local body out
  body=$(mktemp)
  out=$(curl -s -o "$body" -w '%{http_code} %{time_total}' "$BASE/parcel/${parcels[$1]}" || true)
  printf '  %s http %s  %ss  source=%s attempts=%s\n' "${parcels[$1]}" "${out% *}" "${out#* }" \
    "$(grep -oE '"source": ?"[^"]*"' "$body" | cut -d'"' -f4)" \
    "$(grep -oE '"attempts": ?\[[^]]*\]' "$body" | sed -E 's/"attempts": ?//')"
  rm -f "$body"
}
for i in 0 1 2 3; do ask "$i"; done

echo "deleting pod dataset-0"
"$KUBECTL" delete pod dataset-0 --wait=false > /dev/null
for i in 4 5 6 7 8 9 10 11; do ask "$i"; done
"$KUBECTL" rollout status statefulset/dataset --timeout=120s
echo "after the pod is back (cooldown first):"
sleep 6
for i in 12 13 14 15 16 17; do ask "$i"; done
