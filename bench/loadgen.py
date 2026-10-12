"""Multi-threaded load client with its own bounded cache.

Standard library only (requests is imported when the HTTP source is used), so
the same file runs on a free-threaded CPython build without extra wheels.

    python loadgen.py --mode http  --url http://localhost:8200 --data ../data/madison.csv.gz \\
        --requests 20000 --threads 4 --cache 1000
    python loadgen.py --mode local --data ../data/madison.csv.gz --requests 200000 --threads 8

Keys are drawn from the dataset's parcel ids with a Zipf-like skew. The cache
has one lock that is never held while a request is in flight, and evicts in O(1).
"""
import argparse
import bisect
import csv
import gzip
import itertools
import json
import random
import statistics
import sys
import sysconfig
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field


class BoundedCache:
    """Fixed-size cache shared by all worker threads.

    policy "lru" refreshes an entry on every hit, "fifo" evicts in insertion
    order. Both are O(1) through OrderedDict. The lock covers only dictionary
    operations; callers must not hold it (and this class never does) while
    waiting on the network. Capacity 0 disables the cache."""

    def __init__(self, capacity: int, policy: str = "lru"):
        if policy not in ("lru", "fifo"):
            raise ValueError("policy must be lru or fifo")
        self.capacity = capacity
        self.policy = policy
        self._data: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key not in self._data:
                return False, None
            if self.policy == "lru":
                self._data.move_to_end(key)
            return True, self._data[key]

    def put(self, key, value) -> None:
        if self.capacity <= 0:
            return
        with self._lock:
            if key in self._data:
                self._data[key] = value
                if self.policy == "lru":
                    self._data.move_to_end(key)
                return
            self._data[key] = value
            if len(self._data) > self.capacity:
                self._data.popitem(last=False)

    def __len__(self):
        with self._lock:
            return len(self._data)


def load_parcels(path, limit=None) -> list[str]:
    """Distinct parcel ids of the dataset, in file order."""
    opener = gzip.open if str(path).endswith(".gz") else open
    seen, out = set(), []
    with opener(path, "rt", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            p = row["parcel"]
            if p not in seen:
                seen.add(p)
                out.append(p)
                if limit and len(out) >= limit:
                    break
    return out


def zipf_workload(keys: list[str], requests: int, skew: float = 1.1, seed: int = 1) -> list[str]:
    """`requests` keys; key i has weight 1/(i+1)**skew. Fixed seed, so every
    experiment replays the same sequence."""
    weights = [1.0 / (i + 1) ** skew for i in range(len(keys))]
    cum = list(itertools.accumulate(weights))
    rng = random.Random(seed)
    total = cum[-1]
    return [keys[bisect.bisect_left(cum, rng.random() * total)] for _ in range(requests)]


def ranges(n: int, parts: int) -> list[tuple[int, int]]:
    """Split range(n) into `parts` nearly even half-open ranges."""
    base, extra = divmod(n, parts)
    out, start = [], 0
    for i in range(parts):
        end = start + base + (1 if i < extra else 0)
        out.append((start, end))
        start = end
    return out


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return float("nan")
    i = min(len(sorted_values) - 1, max(0, round(q / 100 * (len(sorted_values) - 1))))
    return sorted_values[i]


@dataclass
class RunResult:
    requests: int = 0
    hits: int = 0
    errors: int = 0
    seconds: float = 0.0
    checksum: int = 0
    latencies: list = field(default_factory=list)  # seconds, requests that went to the source
    events: list = field(default_factory=list)  # (t, latency, ok) when record_events

    @property
    def hit_rate(self) -> float:
        return self.hits / self.requests if self.requests else 0.0

    def summary(self) -> dict:
        lat = sorted(x * 1000 for x in self.latencies)
        return {
            "requests": self.requests,
            "hit_rate": round(self.hit_rate, 4),
            "errors": self.errors,
            "seconds": round(self.seconds, 4),
            "rps": round(self.requests / self.seconds, 1) if self.seconds else 0,
            "source_calls": len(lat),
            "p50_ms": round(percentile(lat, 50), 3),
            "p90_ms": round(percentile(lat, 90), 3),
            "p99_ms": round(percentile(lat, 99), 3),
            "max_ms": round(lat[-1], 3) if lat else float("nan"),
            "mean_ms": round(statistics.fmean(lat), 3) if lat else float("nan"),
        }


def consume(payload: dict) -> int:
    """The work a caller does with a result, so threads have something to
    compute besides waiting: total assessed value of the properties."""
    return sum(p["assessed_total"] for p in payload["properties"])


def run_load(make_fetch, workload: list[str], threads: int, cache: BoundedCache,
             record_events: bool = False, t0: float | None = None) -> RunResult:
    """make_fetch() is called once per thread and returns fetch(key) -> payload
    dict (it may keep a per-thread HTTP session). Errors count, they do not stop
    the run."""
    parts = ranges(len(workload), threads)
    results = [RunResult() for _ in parts]
    ready = threading.Barrier(threads + 1)
    t0 = t0 if t0 is not None else time.perf_counter()

    def work(idx, lo, hi):
        res = results[idx]
        fetch = make_fetch()
        ready.wait()
        for key in workload[lo:hi]:
            res.requests += 1
            hit, payload = cache.get(key)
            if hit:
                res.hits += 1
                res.checksum += consume(payload)
                continue
            started = time.perf_counter()
            try:
                payload = fetch(key)  # no lock held here
            except Exception:  # noqa: BLE001 - counted, the run goes on
                res.errors += 1
                if record_events:
                    res.events.append((started - t0, time.perf_counter() - started, False))
                continue
            elapsed = time.perf_counter() - started
            res.latencies.append(elapsed)
            if record_events:
                res.events.append((started - t0, elapsed, True))
            cache.put(key, payload)
            res.checksum += consume(payload)

    pool = [threading.Thread(target=work, args=(i, lo, hi)) for i, (lo, hi) in enumerate(parts)]
    for t in pool:
        t.start()
    ready.wait()
    start = time.perf_counter()
    for t in pool:
        t.join()
    total = RunResult(seconds=time.perf_counter() - start)
    for r in results:
        total.requests += r.requests
        total.hits += r.hits
        total.errors += r.errors
        total.checksum += r.checksum
        total.latencies.extend(r.latencies)
        total.events.extend(r.events)
    return total


# --- sources ---------------------------------------------------------------


def http_source(base_url: str):
    import requests

    def make_fetch():
        session = requests.Session()

        def fetch(key):
            r = session.get(f"{base_url}/parcel/{key}", timeout=10)
            r.raise_for_status()
            return r.json()

        return fetch

    return make_fetch


def local_source(path, parcels: list[str]):
    """No network: answers come from an in-memory table of JSON documents, so a
    run measures the client (cache, lock, JSON, consume) and nothing else."""
    wanted = set(parcels)
    docs: dict[str, list] = {}
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row["parcel"] in wanted:
                docs.setdefault(row["parcel"], []).append(
                    {"parcel": row["parcel"], "address": row["address"], "zip": row["zip"],
                     "assessed_total": int(row["assessed_total"])}
                )
    encoded = {k: json.dumps({"properties": v, "total": len(v), "error": ""}) for k, v in docs.items()}

    def make_fetch():
        return lambda key: json.loads(encoded[key])

    return make_fetch


def interpreter_info() -> dict:
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    return {
        "python": sys.version.split()[0],
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled": bool(gil),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mode", choices=["http", "local"], required=True)
    ap.add_argument("--url", default="http://localhost:8200")
    ap.add_argument("--data", required=True, help="dataset csv.gz, used for the key universe")
    ap.add_argument("--keys", type=int, default=0, help="use only the first N parcels (0 = all)")
    ap.add_argument("--requests", type=int, default=10000)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--cache", type=int, default=0, help="client cache entries")
    ap.add_argument("--policy", choices=["lru", "fifo"], default="lru")
    ap.add_argument("--skew", type=float, default=1.1)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--warmup", type=int, default=0, help="extra requests first, not measured")
    args = ap.parse_args(argv)

    parcels = load_parcels(args.data, args.keys or None)
    workload = zipf_workload(parcels, args.requests + args.warmup, args.skew, args.seed)
    make_fetch = http_source(args.url) if args.mode == "http" else local_source(args.data, parcels)

    cache = BoundedCache(args.cache, args.policy)
    if args.warmup:
        run_load(make_fetch, workload[: args.warmup], args.threads, cache)
    result = run_load(make_fetch, workload[args.warmup:], args.threads, cache)

    out = {"mode": args.mode, "threads": args.threads, "cache": args.cache, "policy": args.policy,
           "skew": args.skew, "keys": len(parcels), **interpreter_info(), **result.summary(),
           "checksum": result.checksum}
    print(json.dumps(out))
    return 0 if result.errors == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
