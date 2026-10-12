# replicated-lookup-service

A property lookup service for Madison, Wisconsin: two replicas of a gRPC dataset server sit behind an HTTP cache tier that load balances between them, keeps an LRU cache, and keeps answering when a replica dies. The dataset tier exists three times (Python, Java, Go) and a conformance tool checks that all three give byte-identical answers for every key in the data.

This started as a project in CS 544 (Big Data Systems) at UW-Madison. The course version had a Flask cache in front of two Java gRPC servers, a port of the Java server to Python, lookup by zip code, and a protobuf change from a failure flag to an error string. A later project in the same course had a thread-safe CSV/Parquet upload server and a performance study of a multi-threaded client with its own cache, run with and without the GIL. I rebuilt all of it from scratch as one repository and added what was missing to run it for real: a fetch script for the city's public data, a Go implementation next to the Java and Python ones, the conformance tool, passive health tracking and failover timing in the cache tier, Prometheus metrics, compose health checks and memory limits, a failure-injection script, Kubernetes manifests that I ran on kind, CI, and a benchmark harness that records its results and the machine they came from.

## Layout

    proto/           lookup.proto (current contract), table.proto (upload service)
    proto-v1/        the first lookup contract, frozen, used by the compatibility tests
    python/          lookupsvc: dataset server, cache tier, table service, fetch, conformance tool
    java/            Gradle project, dataset server
    go/              dataset server
    bench/           load client, experiment scripts, plots
    deploy/          Prometheus config, Kubernetes manifests (kustomize)
    scripts/         proto generation, failover demo, kind download, k8s smoke test
    testdata/        a 252-row synthetic dataset in the same shape as the real one

## The data

`python -m lookupsvc.fetch` downloads two tables from the City of Madison open data portal, the tax parcels and the address points, and joins them on the parcel id: the address table supplies parcel, street address and zip code, the tax table supplies the assessed values, lot size, year built and living area. The result is `data/madison.csv.gz` plus a Parquet copy. Neither is committed. When I ran it the join gave 154,639 rows; 433 address points had no matching tax parcel and 5 were repeats of an address on the same parcel, and I dropped those. The city publishes the data "for informational purposes" under its data policy with no warranty, which is the only licence statement I found.

A parcel with many units (an apartment building) has one row per address, so a parcel lookup can return many addresses.

## Run it

You need Docker with Compose v2 (I used Docker 28.4 and Compose 2.39 on Windows 11). Nothing else, for the stack.

    git clone <this repo> && cd replicated-lookup-service
    docker compose up -d --build

The first start downloads about 77 MB from the city, builds the dataset, and then starts two Go dataset replicas, the cache tier on port 8200 and the upload service on 8201. `docker compose ps` should show everything healthy. Then:

    curl localhost:8200/parcel/070922106137
    curl "localhost:8200/zip/53706?limit=3"
    curl "localhost:8200/address?q=1308%20W%20Dayton%20St"
    curl "localhost:8200/street?q=W%20Dayton%20St&limit=5"
    curl localhost:8200/healthz
    curl localhost:8200/metrics

Every response carries `source` (`cache`, or the replica that answered), `attempts` (replicas that were unreachable before it) and `total` (matches before the `limit`). Errors are plain: 404 for no match, 400 for a bad key, 503 when no replica answers, 501 when a replica does not support the lookup.

To use the Java or Python replicas instead, set `DATASET_IMPL` and `COMPOSE_PROFILES` in `.env` (both to `java` or `py`). `docker compose --profile all up -d` starts all three pairs, with host ports 8210-8215 for the replicas, which is what the conformance tool needs. `--profile monitoring` adds a Prometheus on 8220 that scrapes the cache tier.

Upload service (CSV uploads accumulate into one table, stored as CSV and Parquet, aggregates from either):

    python -m lookupsvc.table.client upload part1.csv part2.csv
    python -m lookupsvc.table.client aggregate assessed_total sum --format csv
    python -m lookupsvc.table.client aggregate assessed_total sum --format parquet

Stop a replica and watch traffic move:

    scripts/failover_demo.sh

Tear everything down with `docker compose --profile all --profile monitoring down -v`.

### Conformance

With all three implementations up:

    python -m lookupsvc.conformance --dataset data/madison.csv.gz \
        --target python=localhost:8210 --target java=localhost:8212 --target go=localhost:8214

It replays every parcel, address, street and zip in the file (plus altered spellings and keys that match nothing) against each target and exits 1 if any reply differs or any call fails.

### Kubernetes

`scripts/get_kind.sh` downloads kind and kubectl into `.tools/` and checks each against its published sha256. `scripts/k8s_smoke.sh` creates a kind cluster, loads the images, applies `deploy/k8s`, deletes a dataset pod while requests flow and removes the cluster. The pods serve the synthetic fixture, not the Madison data.

## Tests

    python -m venv .venv && . .venv/bin/activate          # .venv\Scripts\activate on Windows
    pip install -e "python[cache,dev]"
    python scripts/gen_proto.py
    python -m pytest python/tests bench                    # Python unit tests, benchmark harness tests
    RLS_INTEGRATION=1 python -m pytest python/tests/integration   # compose stack on the fixture, includes conformance
    docker run --rm -v "$PWD":/src -v rls-gocache:/go/pkg golang:1.26-bookworm sh /src/go/test_in_docker.sh
    docker run --rm -v "$PWD":/src -v rls-gradle:/home/gradle/.gradle gradle:9.8.1-jdk21 sh /src/java/test_in_docker.sh

With Go 1.25+ or JDK 21 installed locally, `go/gen.sh` followed by `go test ./...` in `go/`, and `./gradlew test` in `java/`, do the same. The integration test needs ports 8210-8215, 18200 and 18201 free. The workflow in `.github/workflows/ci.yml` runs the same steps.

## Benchmarks

`bench/` has a multi-threaded load client (`loadgen.py`, standard library only, with its own cache: one lock that is never held during a request, O(1) eviction) and one script per experiment. Each writes its CSV and a `.meta.json` with the command, commit and machine to `bench/results/`, and `bench/plot.py` draws the SVGs.

    python bench/run_hitrate.py   # (a) hit rate against cache size, Zipf keys; server LRU, client LRU, client FIFO
    python bench/run_latency.py   # (b) request latency percentiles with and without the server-side cache
    python bench/run_failover.py  # (c) what the client sees when a replica is stopped (or hung, with --how pause)
    python bench/run_formats.py   # (d) CSV against Parquet, loading and aggregating
    python bench/run_threads.py --ft-python <python3.13t>   # (e) GIL against free-threaded CPython at 1/2/4/8 threads
    python bench/plot.py

(a), (b) and (c) need the stack running; they start throwaway cache-tier containers on ports 8290 and up. (e) needs a free-threaded build: `uv python install 3.13t`, or `bench/Dockerfile.freethreaded`.

## Results

TODO (measurement phase): fill in from `bench/results/` with the exact commands, machine and repetition counts.

- hit rate against cache size, plot, and the Zipf parameters used
- latency percentiles with and without the server cache
- failover: stop and pause cases, trial count, what the numbers mean
- CSV against Parquet: load and aggregate times, file sizes
- GIL against free-threaded at 1/2/4/8 threads, and what the workload parameters (`--cache`, `--passes`) do to the answer
- time to start the whole stack on a clean machine, if I decide to quote it

## Limitations

TODO (measurement phase): state these once the numbers are in. Things I already know belong here:

- a hung replica (as opposed to a dead one) is not failed over: only UNAVAILABLE, or a deadline on a channel that never connected, counts as unreachable, so DEADLINE_EXCEEDED on a live connection returns a 502 (`run_failover.py --how pause` shows it)
- the cache tier runs one gunicorn worker with 16 threads, so one cache per container and the GIL applies; replicating the cache tier means separate caches
- the upload service is a single process with an in-process lock, not something to scale out; aggregate sums are float64
- address matching is exact after lower-casing and collapsing whitespace, no fuzzy matching, and key normalisation is ASCII only
- the Kubernetes manifests were run on kind with the synthetic fixture only
