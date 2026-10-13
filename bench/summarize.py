"""Write bench/results/summary.md from the raw CSVs: median with min-max for
every reported figure. The README quotes this file and nothing else.

    python bench/summarize.py
"""
from collections import defaultdict

from common import RESULTS, read_csv, summarize


def exists(name):
    return (RESULTS / f"{name}.csv").exists()


def cell(values, digits=3):
    s = summarize([float(v) for v in values])
    f = f"{{:.{digits}f}}"
    return f"{f.format(s['median'])} ({f.format(s['min'])}-{f.format(s['max'])})"


def table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return out


def by(rows, key):
    out = defaultdict(list)
    for r in rows:
        out[key(r)].append(r)
    return out


def hitrate():
    rows = read_csv("hitrate")
    out = ["## Hit rate against cache size (median, min-max over repetitions)", ""]
    sizes = sorted({int(r["cache_size"]) for r in rows})
    series = list(dict.fromkeys(r["series"] for r in rows))
    g = by(rows, lambda r: (r["series"], int(r["cache_size"])))
    out += table(["cache size"] + series, [[s] + [cell([r["hit_rate"] for r in g[(x, s)]], 4) for x in series] for s in sizes])
    first = rows[0]
    out += ["", f"{first['requests']} requests per run, {first['keys']} distinct parcels, Zipf skew {first['skew']}, "
                f"{len({r['rep'] for r in rows})} repetitions (a different seed each).", ""]
    return out


def latency_like(name, title, key, label):
    rows = read_csv(name)
    out = [f"## {title} (median, min-max over repetitions)", ""]
    g = by(rows, key)
    hdr = [label, "p50 ms", "p90 ms", "p99 ms", "max ms", "requests/s", "errors"]
    if "server_hit_rate" in rows[0]:
        hdr.append("server hit rate")
    body = []
    for k, rs in g.items():
        line = [k] + [cell([r[c] for r in rs], 2) for c in ("p50_ms", "p90_ms", "p99_ms", "max_ms", "rps")]
        line.append(sum(int(r["errors"]) for r in rs))
        if "server_hit_rate" in rows[0]:
            line.append(cell([r["server_hit_rate"] for r in rs], 3))
        body.append(line)
    out += table(hdr, body)
    first = rows[0]
    out += ["", f"{first['requests']} requests per run after warm-up, {first['threads']} client threads, "
                f"{len(g[next(iter(g))])} repetitions.", ""]
    return out


def failover(name, title):
    rows = read_csv(name)
    out = [f"## {title}", ""]
    out += table(
        ["trials", "errors (all trials)", "requests (median)", "baseline p99 ms", "max latency ms", "slow requests", "recovery s",
         "victim errors seen by cache tier", "answered by victim", "answered by other"],
        [[len(rows), sum(int(r["errors"]) for r in rows), cell([r["requests"] for r in rows], 0),
          cell([r["baseline_p99_ms"] for r in rows], 1), cell([r["max_latency_ms"] for r in rows], 1),
          cell([r["slow_requests"] for r in rows], 0), cell([r["recovery_s"] for r in rows], 2),
          cell([r["victim_errors_seen"] for r in rows], 0), cell([r["answered_by_victim"] for r in rows], 0),
          cell([r["answered_by_other"] for r in rows], 0)]])
    out += ["", f"victim {rows[0]['victim']}, {rows[0]['threads']} client threads, {rows[0]['duration_s']} s per trial, "
                f"injected at {rows[0]['kill_at_s']} s (median). Slow means over 3x the pre-failure p99; "
                "recovery is the time from the failure to the last slow request.", ""]
    return out


def formats():
    rows = read_csv("formats")
    out = ["## CSV against Parquet (seconds, median and min-max)", ""]
    g = by(rows, lambda r: (r["task"], r["format"]))
    tasks = list(dict.fromkeys(r["task"] for r in rows))
    body = []
    for t in tasks:
        for fmt in dict.fromkeys(r["format"] for r in rows if r["task"] == t):
            rs = g[(t, fmt)]
            body.append([t, fmt, cell([r["seconds"] for r in rs], 4), rs[0]["bytes"]])
    out += table(["task", "format", "seconds", "bytes read or written"], body)
    out.append("")
    return out


def threads():
    rows = read_csv("threads")
    out = ["## GIL against free-threaded CPython (seconds for the whole workload, median and min-max)", ""]
    g = by(rows, lambda r: (r["mode"], int(r["threads"])))
    modes = list(dict.fromkeys(r["mode"] for r in rows))
    counts = sorted({int(r["threads"]) for r in rows})
    body = []
    for t in counts:
        body.append([t] + [cell([r["seconds"] for r in g[(m, t)]], 2) for m in modes])
    out += table(["threads"] + modes, body)
    out += ["", "Speedup of the median against one thread of the same mode:", ""]
    base = {m: summarize([float(r["seconds"]) for r in g[(m, 1)]])["median"] for m in modes}
    out += table(["threads"] + modes, [[t] + [f"{base[m] / summarize([float(r['seconds']) for r in g[(m, t)]])['median']:.2f}x" for m in modes] for t in counts])
    out += ["", "Interpreters: " + ", ".join(f"{m} {g[(m, 1)][0]['python']}" for m in modes) + ".", ""]
    return out


def main():
    out = ["# Summary of bench/results (generated by bench/summarize.py; do not edit)", ""]
    if exists("hitrate"):
        out += hitrate()
    if exists("latency"):
        out += latency_like("latency", "Latency, cache tier with and without its cache", lambda r: f"server cache {r['server_cache']}", "configuration")
    if exists("impls"):
        out += latency_like("impls", "Latency by dataset implementation, cache tier cache off", lambda r: r["impl"], "implementation")
    if exists("failover"):
        out += failover("failover", "Failover, replica stopped")
    if exists("failover_pause"):
        out += failover("failover_pause", "Failover, replica paused (hung)")
    if exists("formats"):
        out += formats()
    if exists("threads"):
        out += threads()
    (RESULTS / "summary.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print("wrote", RESULTS / "summary.md")


if __name__ == "__main__":
    main()
