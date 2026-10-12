"""(a) Cache hit rate against cache size on a skewed key distribution.

Three series over the same Zipf workload of parcel ids:
  server-lru   the cache tier's LRU, one throwaway container per size, read
               from its /stats counters (the load client has no cache)
  client-lru   the load client's own cache, no network involved
  client-fifo  same, FIFO eviction

    python bench/run_hitrate.py        # defaults: sizes 10..10000, 30000 requests, 5 repetitions
"""
import argparse
import sys

import loadgen
from common import DATASET, BenchCache, stack_ready, write_csv, write_meta


def client_series(sizes, parcels, args, rows):
    for policy in ("lru", "fifo"):
        for size in sizes:
            for rep in range(args.reps):
                seed = args.seed + rep
                work = loadgen.zipf_workload(parcels, args.requests, args.skew, seed)
                res = loadgen.run_load(loadgen.local_source(DATASET, parcels), work, 1,
                                       loadgen.BoundedCache(size, policy))
                rows.append(row(f"client-{policy}", size, rep, seed, args, res.hit_rate))
                print(f"client-{policy} size={size} rep={rep} hit={res.hit_rate:.4f}", flush=True)


def server_series(sizes, parcels, args, rows):
    stack_ready()
    for size in sizes:
        for rep in range(args.reps):
            seed = args.seed + rep
            work = loadgen.zipf_workload(parcels, args.requests, args.skew, seed)
            with BenchCache(size) as cache:
                res = loadgen.run_load(loadgen.http_source(cache.url), work, args.threads, loadgen.BoundedCache(0))
                stats = cache.stats()
            total = stats["hits"] + stats["misses"]
            hit = stats["hits"] / total if total else 0.0
            assert res.errors == 0 and total == args.requests, (res.errors, total)
            rows.append(row("server-lru", size, rep, seed, args, hit))
            print(f"server-lru size={size} rep={rep} hit={hit:.4f}", flush=True)


def row(series, size, rep, seed, args, hit):
    return {"series": series, "cache_size": size, "rep": rep, "seed": seed, "requests": args.requests,
            "keys": args.keys, "skew": args.skew, "threads": args.threads if series == "server-lru" else 1,
            "hit_rate": round(hit, 5)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sizes", default="10,100,1000,3000,10000")
    ap.add_argument("--requests", type=int, default=30000)
    ap.add_argument("--keys", type=int, default=0, help="distinct parcels (0 = all)")
    ap.add_argument("--skew", type=float, default=1.1)
    ap.add_argument("--threads", type=int, default=4, help="load threads for the server series")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--skip-server", action="store_true")
    ap.add_argument("--out", default="hitrate")
    args = ap.parse_args(argv)

    sizes = [int(s) for s in args.sizes.split(",")]
    parcels = loadgen.load_parcels(DATASET, args.keys or None)
    args.keys = len(parcels)
    rows = []
    client_series(sizes, parcels, args, rows)
    if not args.skip_server:
        server_series(sizes, parcels, args, rows)
    write_csv(args.out, rows)
    write_meta(args.out, sys.argv[0:1] + (argv if argv is not None else sys.argv[1:]), {"parameters": vars(args)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
