"""Draw SVG plots from the CSVs in bench/results.

    python bench/plot.py            # every plot whose CSV exists
    python bench/plot.py hitrate threads
"""
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from common import RESULTS, read_csv, summarize  # noqa: E402

matplotlib.rcParams.update({"svg.hashsalt": "rls", "svg.fonttype": "none", "axes.spines.top": False,
                            "axes.spines.right": False, "figure.figsize": (6.4, 4.2)})


def save(fig, name):
    path = RESULTS / f"{name}.svg"
    fig.tight_layout()
    fig.savefig(path, format="svg", metadata={"Date": None})
    plt.close(fig)
    print("wrote", path)


def grouped(rows, key, value):
    out = defaultdict(list)
    for r in rows:
        out[key(r)].append(float(r[value]))
    return out


def line_with_band(ax, xs, groups, label, **kw):
    stats = [summarize(groups[x]) for x in xs]
    ax.plot(xs, [s["median"] for s in stats], marker="o", label=label, **kw)
    ax.fill_between(xs, [s["min"] for s in stats], [s["max"] for s in stats], alpha=0.15)


def hitrate():
    rows = read_csv("hitrate")
    fig, ax = plt.subplots()
    for series in dict.fromkeys(r["series"] for r in rows):
        groups = grouped([r for r in rows if r["series"] == series], lambda r: int(r["cache_size"]), "hit_rate")
        line_with_band(ax, sorted(groups), groups, series)
    ax.set_xscale("log")
    ax.set_xlabel("cache size (entries)")
    ax.set_ylabel("hit rate")
    ax.set_ylim(0, 1)
    ax.set_title("Hit rate against cache size, Zipf workload\nmedian and min-max over repetitions", fontsize=10)
    ax.legend()
    save(fig, "hitrate")


def latency():
    rows = read_csv("latency")
    sizes = sorted({int(r["server_cache"]) for r in rows})
    fig, ax = plt.subplots()
    width = 0.8 / len(sizes)
    for i, size in enumerate(sizes):
        sel = [r for r in rows if int(r["server_cache"]) == size]
        med = [summarize([float(r[p]) for r in sel])["median"] for p in ("p50_ms", "p90_ms", "p99_ms")]
        label = "server cache off" if size == 0 else f"server cache {size}"
        ax.bar([x + i * width for x in range(3)], med, width, label=label)
    ax.set_xticks([x + width * (len(sizes) - 1) / 2 for x in range(3)], ["p50", "p90", "p99"])
    ax.set_ylabel("request latency (ms)")
    ax.set_yscale("log")
    ax.set_title("End-to-end latency over HTTP, client without a cache\nmedian over repetitions", fontsize=10)
    ax.legend()
    save(fig, "latency")


def failover():
    ev = read_csv("failover_events")
    summary = read_csv("failover")
    fig, ax = plt.subplots()
    trials = sorted({int(r["trial"]) for r in ev})
    for trial in trials:
        kill = float(next(s for s in summary if int(s["trial"]) == trial)["kill_at_s"])
        worst = {}  # slowest request in each 250 ms window
        for r in ev:
            if int(r["trial"]) == trial:
                bucket = round((float(r["t_s"]) - kill) * 4) / 4
                worst[bucket] = max(worst.get(bucket, 0.0), float(r["latency_ms"]))
        xs = sorted(worst)
        ax.plot(xs, [worst[x] for x in xs], linewidth=0.9, alpha=0.8, label=f"trial {trial}")
    ax.axvline(0, color="k", linewidth=1, linestyle="--")
    ax.set_yscale("log")
    ax.set_xlabel("seconds since the replica was stopped")
    ax.set_ylabel("slowest request per 250 ms (ms)")
    ax.set_title("Request latency around a replica failure", fontsize=10)
    if len(trials) <= 6:
        ax.legend(fontsize=8)
    save(fig, "failover")


def formats():
    rows = read_csv("formats")
    tasks = [t for t in dict.fromkeys(r["task"] for r in rows) if t != "upload_all_parts"]
    fig, ax = plt.subplots()
    width = 0.38
    for i, fmt in enumerate(("csv", "parquet")):
        med = [summarize([float(r["seconds"]) for r in rows if r["task"] == t and r["format"] == fmt])["median"]
               for t in tasks]
        ax.bar([x + i * width for x in range(len(tasks))], med, width, label=fmt)
    ax.set_xticks([x + width / 2 for x in range(len(tasks))], tasks, rotation=25, ha="right")
    ax.set_ylabel("seconds")
    ax.set_yscale("log")
    ax.set_title("Loading and aggregating, CSV against Parquet\nmedian over repetitions", fontsize=10)
    ax.legend()
    save(fig, "formats")


def threads():
    rows = read_csv("threads")
    fig, ax = plt.subplots()
    for mode in dict.fromkeys(r["mode"] for r in rows):
        groups = grouped([r for r in rows if r["mode"] == mode], lambda r: int(r["threads"]), "seconds")
        line_with_band(ax, sorted(groups), groups, mode)
    ax.set_xscale("log", base=2)
    xs = sorted({int(r["threads"]) for r in rows})
    ax.set_xticks(xs, [str(x) for x in xs])
    ax.set_xlabel("threads")
    ax.set_ylabel("seconds for the whole workload")
    ax.set_title("Load client run time by interpreter mode\nmedian and min-max over repetitions", fontsize=10)
    ax.legend()
    save(fig, "threads")


PLOTS = {"hitrate": hitrate, "latency": latency, "failover": failover, "formats": formats, "threads": threads}

if __name__ == "__main__":
    wanted = sys.argv[1:] or [n for n in PLOTS if (RESULTS / f"{n}.csv").exists()]
    for name in wanted:
        PLOTS[name]()
