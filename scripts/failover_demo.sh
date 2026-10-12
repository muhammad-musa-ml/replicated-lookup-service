#!/usr/bin/env bash
# Stop one dataset replica while requests keep flowing, then bring it back, and
# print which replica answered each request.
#
#   scripts/failover_demo.sh [replica-service]     default: dataset-$DATASET_IMPL-1
#
# Needs the stack up (docker compose up -d). Every request asks for a different
# parcel so the cache never answers; that is the point of the demo.
set -euo pipefail

cd "$(dirname "$0")/.."
BASE=${BASE:-http://localhost:8200}
IMPL=${DATASET_IMPL:-$(grep -E '^DATASET_IMPL=' .env | cut -d= -f2)}
VICTIM=${1:-dataset-${IMPL:-go}-1}
COOLDOWN=${COOLDOWN:-5}

parcels=($(curl -fsS "$BASE/zip/53703?limit=400" | grep -oE '"parcel": ?"[0-9]{12}"' | grep -oE '[0-9]{12}' | sort -u))
if [ "${#parcels[@]}" -lt 30 ]; then
  echo "need at least 30 distinct parcels from /zip/53703, got ${#parcels[@]}" >&2
  exit 1
fi

n=0
ask() {
  local parcel=${parcels[$n]} body out code t
  n=$((n + 1))
  body=$(mktemp)
  out=$(curl -s -o "$body" -w '%{http_code} %{time_total}' "$BASE/parcel/$parcel" || true)
  code=${out% *}
  t=${out#* }
  printf '  %-12s http %s  %ss  source=%s attempts=%s\n' "$parcel" "$code" "$t" \
    "$(grep -oE '"source": ?"[^"]*"' "$body" | cut -d'"' -f4)" \
    "$(grep -oE '"attempts": ?\[[^]]*\]' "$body" | sed -E 's/"attempts": ?//; s/[[:space:]]//g')"
  rm -f "$body"
}

echo "healthy, both replicas up:"
for _ in 1 2 3 4 5 6; do ask; done

echo "stopping $VICTIM"
docker compose stop -t 1 "$VICTIM" > /dev/null 2>&1
echo "replica down, traffic keeps flowing:"
for _ in 1 2 3 4 5 6 7 8; do ask; done

echo "starting $VICTIM again"
docker compose start "$VICTIM" > /dev/null 2>&1
for _ in $(seq 1 60); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' "replicated-lookup-service-$VICTIM-1" 2>/dev/null)" = healthy ] && break
  sleep 1
done
sleep "$((COOLDOWN + 1))"
echo "replica back, rotation resumes:"
for _ in 1 2 3 4 5 6; do ask; done
