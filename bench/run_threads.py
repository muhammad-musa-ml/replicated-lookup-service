"""(e) The load client under CPython with the GIL and under free-threaded CPython.

Each run is a separate interpreter process executing loadgen.py in local mode
(no network, so the time is client work: cache lock, JSON decoding, summing).
Three modes:
  cpython-gil          a regular CPython build
  freethreaded-gil     a free-threaded build with the GIL switched on  (-X gil=1)
  freethreaded-nogil   the same build with the GIL off                 (-X gil=0)

The interpreters are checked against what loadgen reports about itself, and every
run must produce the same checksum, so a mode cannot "win" by doing less work.

    python bench/run_threads.py --standard-python python --ft-python <path to python3.13t> \\
        --requests 400000 --reps 5

A free-threaded build comes from `uv python install 3.13t` (then `uv python find 3.13t`)
or from bench/Dockerfile.freethreaded.
"""
import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

from common import DATASET, write_csv, write_meta

LOADGEN = Path(__file__).resolve().parent / "loadgen.py"


def run_once(exe, flags, args, threads):
    cmd = [exe, *flags, str(LOADGEN), "--mode", "local", "--data", str(DATASET), "--keys", str(args.keys),
           "--requests", str(args.requests), "--threads", str(threads), "--cache", str(args.cache),
           "--skew", str(args.skew), "--seed", str(args.seed), "--passes", str(args.passes)]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if out.returncode != 0:
        raise SystemExit(f"{' '.join(cmd)} failed ({out.returncode}): {out.stderr[-500:]}")
    return json.loads(out.stdout.strip().splitlines()[-1])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--standard-python", default=sys.executable)
    ap.add_argument("--ft-python", required=True)
    ap.add_argument("--threads", default="1,2,4,8")
    ap.add_argument("--requests", type=int, default=100000)
    ap.add_argument("--cache", type=int, default=200)
    ap.add_argument("--keys", type=int, default=20000)
    ap.add_argument("--skew", type=float, default=1.1)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--passes", type=int, default=50, help="JSON decode passes per fetched document")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--out", default="threads")
    args = ap.parse_args(argv)
    # a relative path with forward slashes is not found by CreateProcess on Windows
    args.standard_python = str(Path(shutil.which(args.standard_python) or args.standard_python).resolve())
    args.ft_python = str(Path(shutil.which(args.ft_python) or args.ft_python).resolve())

    modes = [
        ("cpython-gil", args.standard_python, [], dict(free_threaded_build=False, gil_enabled=True)),
        ("freethreaded-gil", args.ft_python, ["-X", "gil=1"], dict(free_threaded_build=True, gil_enabled=True)),
        ("freethreaded-nogil", args.ft_python, ["-X", "gil=0"], dict(free_threaded_build=True, gil_enabled=False)),
    ]
    rows, checksums, versions = [], set(), {}
    for rep in range(args.reps):
        for threads in [int(t) for t in args.threads.split(",")]:
            for mode, exe, flags, expect in modes:
                r = run_once(exe, flags, args, threads)
                got = {k: r[k] for k in expect}
                if got != expect:
                    raise SystemExit(f"{mode}: interpreter reports {got}, expected {expect}")
                checksums.add(r["checksum"])
                versions[mode] = r["python"]
                rows.append({"mode": mode, "threads": threads, "rep": rep, "python": r["python"],
                             "requests": r["requests"], "seconds": r["seconds"], "rps": r["rps"],
                             "hit_rate": r["hit_rate"], "p99_ms": r["p99_ms"]})
                print(f"{mode:<20} threads={threads} rep={rep} {r['seconds']}s {r['rps']} req/s", flush=True)
    if len(checksums) != 1:
        raise SystemExit(f"runs disagree on the checksum: {checksums}")
    write_csv(args.out, rows)
    write_meta(args.out, sys.argv[0:1] + (argv if argv is not None else sys.argv[1:]),
               {"parameters": vars(args), "interpreters": versions, "checksum": checksums.pop()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
