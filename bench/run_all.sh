#!/usr/bin/env bash
# The whole measurement pass, in order. Run it from the repository root on a quiet
# machine (nothing else on the Docker engine); it takes about an hour.
#
#   bench/run_all.sh
#
# Needs: Docker, the repo's venv active (pip install -e "python[cache,dev]"),
# data/madison.csv.gz (the first compose start builds it), and a free-threaded
# CPython: FT_PYTHON=<path to python3.13t> or `uv python install 3.13t`.
# Results land in bench/results/ with a .meta.json per experiment.
set -euo pipefail

cd "$(dirname "$0")/.."
export DATASET_IMPL=go
FT_PYTHON=${FT_PYTHON:-$(uv python find 3.13t)}

docker compose up -d --build
for _ in $(seq 1 90); do
  [ "$(docker compose ps --format '{{.Health}}' | grep -c '^healthy$')" -ge 4 ] && break
  sleep 2
done
docker compose ps

python bench/run_hitrate.py                   # (a) hit rate against cache size
python bench/run_latency.py                   # (b) latency with and without the server cache
python bench/run_failover.py                  # (c) replica stopped
python bench/run_failover.py --how pause --out failover_pause   # (c) replica hung
docker compose down -v -t 1                   # the next two need no containers
python bench/run_formats.py                   # (d) CSV against Parquet
python bench/run_threads.py --ft-python "$FT_PYTHON"            # (e) GIL against free-threaded
python bench/plot.py
