"""Append a snapshot of the machine to bench/results/machine_state.txt.

    python bench/machine_state.py before
    python bench/machine_state.py after

Records CPU busy percent (five samples, two seconds apart), free memory, the
Docker VM's memory, and how many running containers belong to other projects.
"""
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone

from common import PROJECT, RESULTS, sh

MINE = (PROJECT, "rls-bench")


def powershell(expr: str) -> str | None:
    try:
        out = subprocess.run(["powershell", "-NoProfile", "-Command", expr], capture_output=True,
                             text=True, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None


def cpu_model() -> str:
    if platform.system() == "Windows":
        return powershell("(Get-CimInstance Win32_Processor | Select-Object -First 1).Name") or "unknown"
    try:
        for line in open("/proc/cpuinfo", encoding="utf-8"):
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def memory_gib() -> tuple[float | None, float | None]:
    """(total, free) in GiB."""
    if platform.system() == "Windows":
        raw = powershell("$o = Get-CimInstance Win32_OperatingSystem; "
                         "\"$($o.TotalVisibleMemorySize) $($o.FreePhysicalMemory)\"")
        if raw:
            total, free = (int(x) for x in raw.split())
            return round(total / 2**20, 1), round(free / 2**20, 1)
        return None, None
    info = dict(line.split(":", 1) for line in open("/proc/meminfo", encoding="utf-8"))
    kb = lambda k: int(info[k].split()[0])  # noqa: E731
    return round(kb("MemTotal") / 2**20, 1), round(kb("MemAvailable") / 2**20, 1)


def cpu_busy_samples(n=5, gap=2.0) -> list[int]:
    out = []
    for i in range(n):
        if platform.system() == "Windows":
            raw = powershell("(Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average).Average")
            out.append(int(float(raw)) if raw else -1)
        else:
            a = open("/proc/stat").readline().split()[1:]
            time.sleep(gap)
            b = open("/proc/stat").readline().split()[1:]
            d = [int(y) - int(x) for x, y in zip(a, b)]
            out.append(round(100 * (1 - d[3] / sum(d))))
            continue
        if i < n - 1:
            time.sleep(gap)
    return out


def other_containers(names: list[str]) -> int:
    return sum(1 for n in names if n and not n.startswith(MINE))


def main(label: str) -> None:
    total, free = memory_gib()
    names = sh(["docker", "ps", "--format", "{{.Names}}"]).stdout.split()
    vm = sh(["docker", "info", "--format", "{{.MemTotal}} {{.NCPU}}"]).stdout.split()
    lines = [
        f"{label} snapshot, UTC {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}",
        f"os: {platform.system()} {platform.release()} {platform.machine()}",
        f"cpu: {cpu_model()}, logical cpus: {os.cpu_count()}",
        f"total memory GiB: {total}, free GiB: {free}",
        f"docker VM memory GiB: {round(int(vm[0]) / 2**30, 1) if vm else 'unknown'}, "
        f"docker VM cpus: {vm[1] if len(vm) > 1 else 'unknown'}",
        f"cpu busy percent, five samples two seconds apart: {', '.join(str(x) for x in cpu_busy_samples())}",
        f"docker containers running (other projects, not mine): {other_containers(names)}",
        f"docker containers running (mine): {len(names) - other_containers(names)}",
        "",
    ]
    RESULTS.mkdir(parents=True, exist_ok=True)
    with open(RESULTS / "machine_state.txt", "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "manual")
