#!/usr/bin/env bash
# The whole measurement pass, in order. Run it from the repository root on a quiet
# machine (nothing else benchmarking on the Docker engine); it takes a bit under
# two hours.
#
#   bench/run_all.sh
#
# Needs: Docker, the repo's venv active (pip install -e "python[cache,dev]"),
# and a free-threaded CPython: FT_PYTHON=<path to python3.13t> or
# `uv python install 3.13t`. The first compose start builds data/madison.csv.gz.
# Results land in bench/results/ with a .meta.json per experiment.
set -euo pipefail

cd "$(dirname "$0")/.."
export DATASET_IMPL=go
FT_PYTHON=${FT_PYTHON:-$(uv python find 3.13t)}

wait_healthy() {  # wait_healthy N: until N services report healthy
  for _ in $(seq 1 120); do
    [ "$(docker compose --profile all ps --format '{{.Health}}' | grep -c '^healthy$')" -ge "$1" ] && return 0
    sleep 2
  done
  echo "stack did not become healthy" >&2
  exit 1
}

python bench/machine_state.py before

docker compose up -d --build
wait_healthy 4                                # cache, table and the two Go replicas
python bench/run_hitrate.py                   # (a) hit rate against cache size
python bench/run_latency.py                   # (b) latency with and without the server cache
python bench/run_failover.py                  # (c) replica stopped
python bench/run_failover.py --how pause --out failover_pause   # (c) replica hung

docker compose --profile all up -d
wait_healthy 8                                # plus the Python and Java pairs
SECONDS=0
python -m lookupsvc.conformance --dataset data/madison.csv.gz \
    --target python=localhost:8210 --target java=localhost:8212 --target go=localhost:8214 \
    | tee bench/results/conformance.txt
echo "elapsed seconds: $SECONDS" >> bench/results/conformance.txt
python bench/run_impls.py                     # (f) Python, Java and Go replicas, cache tier cache off

docker compose --profile all down -v -t 1     # the next two need no containers
python bench/run_formats.py                   # (d) CSV against Parquet
python bench/run_threads.py --ft-python "$FT_PYTHON"            # (e) GIL against free-threaded
python bench/plot.py
python bench/summarize.py
python bench/machine_state.py after
