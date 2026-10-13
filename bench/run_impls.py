"""(f) Does the dataset server's language matter for request latency?

The Python, Java and Go replica pairs serve the same Zipf workload through the
cache tier with its cache switched off (CACHE_SIZE=0), so every request is a
gRPC round trip to a replica. Repetitions are interleaved (py, java, go, py, ...)
so drift in the machine does not line up with one implementation.

Needs all three pairs running: docker compose --profile all up -d

    python bench/run_impls.py
"""
import argparse
import os
import sys

import loadgen
from common import DATASET, BenchCache, stack_ready, write_csv, write_meta


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--impls", default="py,java,go")
    ap.add_argument("--requests", type=int, default=20000)
    ap.add_argument("--warmup", type=int, default=2000)
    ap.add_argument("--keys", type=int, default=0)
    ap.add_argument("--skew", type=float, default=1.1)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default="impls")
    args = ap.parse_args(argv)

    impls = args.impls.split(",")
    parcels = loadgen.load_parcels(DATASET, args.keys or None)
    rows = []
    for rep in range(args.reps):
        seed = args.seed + rep
        work = loadgen.zipf_workload(parcels, args.warmup + args.requests, args.skew, seed)
        for impl in impls:
            os.environ["DATASET_IMPL"] = impl
            stack_ready()
            with BenchCache(0) as cache:
                make_fetch = loadgen.http_source(cache.url)
                no_cache = loadgen.BoundedCache(0)
                loadgen.run_load(make_fetch, work[: args.warmup], args.threads, no_cache)
                res = loadgen.run_load(make_fetch, work[args.warmup:], args.threads, no_cache)
            rows.append({"impl": impl, "rep": rep, "seed": seed, "threads": args.threads, **res.summary()})
            r = rows[-1]
            print(f"{impl:<5} rep={rep} p50={r['p50_ms']}ms p99={r['p99_ms']}ms rps={r['rps']} err={r['errors']}",
                  flush=True)
    write_csv(args.out, rows)
    write_meta(args.out, sys.argv[0:1] + (argv if argv is not None else sys.argv[1:]), {"parameters": vars(args)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
