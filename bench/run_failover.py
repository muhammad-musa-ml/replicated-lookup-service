"""(c) What a client sees when one dataset replica is killed mid-run.

A closed-loop load client (no client cache, cache tier with CACHE_SIZE=0 so every
request reaches a replica) runs for --duration seconds. At --kill-at seconds
the first replica of the chosen implementation is stopped with
`docker compose stop -t 0`. Per request (start offset, latency, ok) goes to
failover_events.csv; one summary row per trial goes to failover.csv.

Recovery is measured as the time from the kill until the last request slower
than --slow-factor times the pre-kill p99.

    python bench/run_failover.py --trials 5 --duration 20 --kill-at 8
"""
import argparse
import sys
import threading
import time

import loadgen
from common import DATASET, BenchCache, compose, impl, sh, stack_ready, write_csv, write_meta, PROJECT


def restore(victim: str, how: str) -> None:
    compose("unpause" if how == "pause" else "start", victim)
    name = f"{PROJECT}-{victim}-1"
    for _ in range(90):
        if sh(["docker", "inspect", "-f", "{{.State.Health.Status}}", name]).stdout.strip() == "healthy":
            return
        time.sleep(1)
    raise SystemExit(f"{victim} did not come back")


def one_trial(args, parcels, trial, victim):
    work = loadgen.zipf_workload(parcels, 400000, args.skew, args.seed + trial)
    stop = threading.Event()
    kill = {}

    def killer():
        time.sleep(args.kill_at)
        kill["start"] = time.perf_counter()
        out = compose("pause", victim) if args.how == "pause" else compose("stop", "-t", "0", victim)
        kill["end"] = time.perf_counter()
        kill["rc"] = out.returncode

    with BenchCache(0) as cache:
        make_fetch = loadgen.http_source(cache.url)
        loadgen.run_load(make_fetch, work[:500], args.threads, loadgen.BoundedCache(0))  # warm connections
        t0 = time.perf_counter()
        threading.Thread(target=killer, daemon=True).start()
        threading.Timer(args.duration, stop.set).start()
        res = loadgen.run_load(make_fetch, work[500:], args.threads, loadgen.BoundedCache(0),
                               record_events=True, t0=t0, stop=stop)
    restore(victim, args.how)
    time.sleep(args.settle)

    assert kill.get("rc") == 0, "docker compose stop failed"
    kill_t = kill["start"] - t0
    events = sorted(res.events)
    before = sorted(lat for t, lat, ok in events if ok and t + lat < kill_t)
    base_p99 = loadgen.percentile(before, 99) * 1000
    slow = [(t, lat) for t, lat, ok in events if t >= kill_t and lat * 1000 > args.slow_factor * base_p99]
    failed = [e for e in events if not e[2]]
    summary = {
        "trial": trial, "victim": victim, "how": args.how, "threads": args.threads, "duration_s": args.duration,
        "kill_at_s": round(kill_t, 3), "stop_command_s": round(kill["end"] - kill["start"], 3),
        "requests": res.requests, "errors": res.errors,
        "baseline_p50_ms": round(loadgen.percentile(before, 50) * 1000, 3),
        "baseline_p99_ms": round(base_p99, 3),
        "max_latency_ms": round(max((lat for _, lat, _ in events), default=0) * 1000, 3),
        "slow_requests": len(slow),
        "recovery_s": round(max((t + lat for t, lat in slow), default=kill_t) - kill_t, 3),
        "first_error_after_kill_s": round(failed[0][0] - kill_t, 3) if failed else "",
    }
    return summary, [(trial, round(t, 4), round(lat * 1000, 3), int(ok)) for t, lat, ok in events]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--kill-at", type=float, default=8)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--keys", type=int, default=0)
    ap.add_argument("--skew", type=float, default=0.8, help="low skew so cache-free requests are spread out")
    ap.add_argument("--slow-factor", type=float, default=3.0)
    ap.add_argument("--settle", type=float, default=8, help="seconds to wait after the replica is back")
    ap.add_argument("--how", choices=["stop", "pause"], default="stop",
                    help="stop: the replica process goes away; pause: it hangs, connections stay open")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default="failover")
    args = ap.parse_args(argv)

    stack_ready()
    parcels = loadgen.load_parcels(DATASET, args.keys or None)
    victim = f"dataset-{impl()}-1"
    summaries, events = [], []
    for trial in range(args.trials):
        s, e = one_trial(args, parcels, trial, victim)
        summaries.append(s)
        events += e
        print(f"trial {trial}: errors={s['errors']} max={s['max_latency_ms']}ms recovery={s['recovery_s']}s "
              f"baseline p99={s['baseline_p99_ms']}ms", flush=True)
    write_csv(args.out, summaries)
    write_csv(args.out + "_events", [dict(zip(("trial", "t_s", "latency_ms", "ok"), e)) for e in events])
    write_meta(args.out, sys.argv[0:1] + (argv if argv is not None else sys.argv[1:]), {"parameters": vars(args)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
