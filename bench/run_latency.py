"""(b) End-to-end request latency with and without the server-side cache.

For each cache-tier configuration a fresh container serves the same warmed-up
Zipf workload from a load client that has no cache of its own, so every request
is a real HTTP round trip. Percentiles are taken over all requests of a run.

    python bench/run_latency.py        # defaults: server cache 0 and 5000, 20000 requests, 5 repetitions
"""
import argparse
import sys

import loadgen
from common import DATASET, BenchCache, stack_ready, write_csv, write_meta


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--server-caches", default="0,5000", help="cache-tier CACHE_SIZE values to compare")
    ap.add_argument("--requests", type=int, default=20000)
    ap.add_argument("--warmup", type=int, default=2000)
    ap.add_argument("--keys", type=int, default=0)
    ap.add_argument("--skew", type=float, default=1.1)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default="latency")
    args = ap.parse_args(argv)

    stack_ready()
    parcels = loadgen.load_parcels(DATASET, args.keys or None)
    rows = []
    for size in [int(s) for s in args.server_caches.split(",")]:
        for rep in range(args.reps):
            seed = args.seed + rep
            work = loadgen.zipf_workload(parcels, args.warmup + args.requests, args.skew, seed)
            with BenchCache(size) as cache:
                make_fetch = loadgen.http_source(cache.url)
                no_cache = loadgen.BoundedCache(0)
                loadgen.run_load(make_fetch, work[: args.warmup], args.threads, no_cache)
                before = cache.stats()
                res = loadgen.run_load(make_fetch, work[args.warmup:], args.threads, no_cache)
                after = cache.stats()
            hits = after["hits"] - before["hits"]
            misses = after["misses"] - before["misses"]
            rows.append({"server_cache": size, "rep": rep, "seed": seed, "threads": args.threads,
                         "server_hit_rate": round(hits / max(1, hits + misses), 4), **res.summary()})
            r = rows[-1]
            print(f"server_cache={size} rep={rep} p50={r['p50_ms']}ms p99={r['p99_ms']}ms rps={r['rps']}", flush=True)
    write_csv(args.out, rows)
    write_meta(args.out, sys.argv[0:1] + (argv if argv is not None else sys.argv[1:]), {"parameters": vars(args)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
