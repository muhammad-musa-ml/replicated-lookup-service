"""Shared helpers for the benchmark scripts: result files, run metadata and the
few docker compose calls they need."""
import csv
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(os.environ.get("RLS_RESULTS") or Path(__file__).resolve().parent / "results")
DATASET = ROOT / "data" / "madison.csv.gz"
PARQUET = ROOT / "data" / "madison.parquet"
PROJECT = "replicated-lookup-service"


def sh(args, **kw):
    try:
        return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
                              errors="replace", **kw)
    except FileNotFoundError:  # git or docker missing, e.g. inside the free-threaded image
        return subprocess.CompletedProcess(args, 127, "", f"{args[0]} not found")


def git_commit() -> str:
    out = sh(["git", "rev-parse", "HEAD"])
    dirty = sh(["git", "status", "--porcelain", "--", "python", "bench", "proto", "go", "java"]).stdout.strip()
    return out.stdout.strip() + ("+dirty" if dirty else "")


def docker_info() -> dict:
    out = sh(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}} {{.ServerVersion}} {{.OperatingSystem}}"])
    parts = out.stdout.strip().split(" ", 3)
    if len(parts) < 4:
        return {}
    return {"docker_cpus": int(parts[0]), "docker_mem_bytes": int(parts[1]),
            "docker_version": parts[2], "docker_os": parts[3]}


def machine_info() -> dict:
    return {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "python": sys.version.split()[0],
        **docker_info(),
    }


def write_meta(name: str, argv: list[str], extra: dict | None = None) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    meta = {
        "experiment": name,
        "command": "python " + " ".join(argv),
        "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "commit": git_commit(),
        "machine": machine_info(),
        **(extra or {}),
    }
    (RESULTS / f"{name}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")


def write_csv(name: str, rows: list[dict]) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return path


def read_csv(name: str) -> list[dict]:
    with open(RESULTS / f"{name}.csv", newline="") as f:
        return list(csv.DictReader(f))


def summarize(values: list[float]) -> dict:
    """median plus the spread the README quotes: min, max and the interquartile range."""
    v = sorted(values)
    if len(v) >= 4:
        q = statistics.quantiles(v, n=4, method="inclusive")
        q1, q3 = q[0], q[2]
    else:
        q1, q3 = v[0], v[-1]
    return {"median": statistics.median(v), "min": v[0], "max": v[-1], "q1": q1, "q3": q3, "n": len(v)}


# --- docker compose --------------------------------------------------------

def impl() -> str:
    return os.environ.get("DATASET_IMPL", "go")


def compose(*args, **kw):
    env = {**os.environ, "DATASET_IMPL": impl()}
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "--profile", "all", *args],
        cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace", **kw,
    )


def stack_ready(timeout=30) -> None:
    """The bench cache containers need the replicas of the chosen implementation."""
    names = [f"{PROJECT}-dataset-{impl()}-{i}-1" for i in (1, 2)]
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = sh(["docker", "inspect", "-f", "{{.State.Health.Status}}", *names]).stdout.split()
        if out == ["healthy", "healthy"]:
            return
        time.sleep(1)
    raise SystemExit(f"start the replicas first: docker compose up -d ({names} are not healthy)")


class BenchCache:
    """A throwaway cache-tier container on the compose network, published on a
    host port in the 8290s, with its own CACHE_SIZE."""

    def __init__(self, size: int, port: int = 8290, name: str = "rls-bench-cache"):
        self.size, self.port, self.name = size, port, name
        self.url = f"http://localhost:{port}"

    def __enter__(self):
        sh(["docker", "rm", "-f", self.name])
        out = compose("run", "-d", "--no-deps", "--name", self.name, "-p", f"{self.port}:8000",
                      "-e", f"CACHE_SIZE={self.size}", "cache")
        if out.returncode != 0:
            raise SystemExit(f"could not start the bench cache: {out.stderr}")
        for _ in range(60):
            try:
                if requests.get(self.url + "/healthz", timeout=2).status_code == 200:
                    return self
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise SystemExit("bench cache did not become healthy")

    def stats(self) -> dict:
        return requests.get(self.url + "/stats", timeout=5).json()

    def __exit__(self, *exc):
        sh(["docker", "rm", "-f", self.name])
