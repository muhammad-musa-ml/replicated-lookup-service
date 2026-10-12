import collections
import json
import threading
from pathlib import Path

import pytest

import loadgen
from loadgen import BoundedCache, load_parcels, local_source, ranges, run_load, zipf_workload

SAMPLE = Path(__file__).resolve().parents[2] / "testdata" / "sample.csv.gz"


def test_lru_refreshes_on_hit_fifo_does_not():
    lru, fifo = BoundedCache(2, "lru"), BoundedCache(2, "fifo")
    for c in (lru, fifo):
        c.put("a", 1)
        c.put("b", 2)
        c.get("a")
        c.put("c", 3)
    assert lru.get("a")[0] and not lru.get("b")[0]
    assert not fifo.get("a")[0] and fifo.get("b")[0]


def test_capacity_zero_and_bad_policy():
    c = BoundedCache(0)
    c.put("a", 1)
    assert len(c) == 0 and c.get("a") == (False, None)
    with pytest.raises(ValueError):
        BoundedCache(1, "random")


def test_overwrite_keeps_size():
    c = BoundedCache(2, "fifo")
    for v in range(10):
        c.put("a", v)
    assert len(c) == 1 and c.get("a") == (True, 9)


def test_ranges_cover_everything_evenly():
    for n, parts in [(10, 3), (3, 8), (0, 4), (1000, 7)]:
        rs = ranges(n, parts)
        assert rs[0][0] == 0 and rs[-1][1] == n
        assert all(a[1] == b[0] for a, b in zip(rs, rs[1:]))
        sizes = [hi - lo for lo, hi in rs]
        assert max(sizes) - min(sizes) <= 1


def test_workload_is_reproducible_and_skewed():
    keys = [f"k{i}" for i in range(500)]
    a = zipf_workload(keys, 20000, 1.1, seed=3)
    assert a == zipf_workload(keys, 20000, 1.1, seed=3)
    assert a != zipf_workload(keys, 20000, 1.1, seed=4)
    counts = collections.Counter(a)
    assert counts.most_common(1)[0][0] == "k0"
    assert counts["k0"] > 10 * counts["k300"]
    flat = collections.Counter(zipf_workload(keys, 20000, 0.0, seed=3))
    assert flat.most_common(1)[0][1] < 100, "skew 0 is close to uniform"


def payload(key):
    return {"properties": [{"assessed_total": len(key)}], "total": 1, "error": ""}


def test_single_thread_hit_accounting():
    res = run_load(lambda: payload, ["a", "bb", "a", "bb", "ccc"], 1, BoundedCache(2))
    assert (res.requests, res.hits, res.errors, len(res.latencies)) == (5, 2, 0, 3)
    assert res.checksum == 1 + 2 + 1 + 2 + 3


def test_the_cache_lock_is_never_held_during_the_request():
    cache = BoundedCache(100)
    held = []

    def make_fetch():
        def fetch(key):
            held.append(cache._lock.locked())
            return payload(key)

        return fetch

    run_load(make_fetch, [f"k{i % 40}" for i in range(2000)], 8, cache)
    assert held and not any(held)


def test_a_cache_that_locks_around_io_would_be_caught_by_that_check():
    # the check above has teeth: a get-then-fetch-under-lock pattern trips it
    cache = BoundedCache(100)
    seen = []

    def make_fetch():
        def fetch(key):
            with cache._lock:
                seen.append(cache._lock.locked())
            return payload(key)

        return fetch

    run_load(make_fetch, ["a"], 1, cache)
    assert seen == [True]


def test_concurrent_run_accounts_for_every_request():
    workload = zipf_workload([f"k{i}" for i in range(200)], 20000, 1.0, seed=1)
    res = run_load(lambda: payload, workload, 8, BoundedCache(50))
    assert res.requests == 20000
    assert res.hits + len(res.latencies) + res.errors == res.requests
    assert res.checksum == sum(len(k) for k in workload)


def test_errors_are_counted_and_the_run_continues():
    def make_fetch():
        def fetch(key):
            if key == "bad":
                raise RuntimeError("boom")
            return payload(key)

        return fetch

    res = run_load(make_fetch, ["a", "bad", "b", "bad", "a"], 1, BoundedCache(10))
    assert (res.errors, res.requests, res.hits) == (2, 5, 1)


def test_events_are_recorded_with_start_offsets():
    res = run_load(lambda: payload, ["a", "b", "c"], 1, BoundedCache(0), record_events=True)
    assert [ok for _, _, ok in res.events] == [True] * 3
    assert [t for t, _, _ in res.events] == sorted(t for t, _, _ in res.events)


def test_summary_percentiles():
    r = loadgen.RunResult(requests=100, seconds=2.0, latencies=[i / 1000 for i in range(1, 101)])
    s = r.summary()
    assert s["rps"] == 50.0 and s["p50_ms"] == pytest.approx(50, abs=1.5) and s["max_ms"] == 100


def test_local_source_serves_the_dataset_documents():
    parcels = load_parcels(SAMPLE, 5)
    fetch = local_source(SAMPLE, parcels)()
    doc = fetch(parcels[0])
    assert doc["properties"] and doc["properties"][0]["parcel"] == parcels[0]


def test_cli_local_mode_prints_one_json_line(capsys):
    code = loadgen.main(
        ["--mode", "local", "--data", str(SAMPLE), "--requests", "500", "--threads", "3", "--cache", "20"]
    )
    out = json.loads(capsys.readouterr().out)
    assert code == 0
    assert out["requests"] == 500 and out["threads"] == 3 and 0 < out["hit_rate"] < 1
    assert {"python", "gil_enabled", "free_threaded_build", "p99_ms", "rps"} <= set(out)


def test_thread_safety_of_the_cache_under_contention():
    cache = BoundedCache(64)

    def hammer(seed):
        for i in range(5000):
            k = (seed * 31 + i * 7) % 300
            hit, v = cache.get(k)
            if hit:
                assert v == k
            else:
                cache.put(k, k)

    ts = [threading.Thread(target=hammer, args=(s,)) for s in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(cache) <= 64


def test_stop_event_ends_the_run_early():
    stop = threading.Event()
    calls = []

    def make_fetch():
        def fetch(key):
            calls.append(key)
            if len(calls) >= 50:
                stop.set()
            return payload(key)

        return fetch

    res = run_load(make_fetch, [f"k{i}" for i in range(100000)], 1, BoundedCache(0), stop=stop)
    assert 50 <= res.requests < 100


def test_local_source_passes_decode_more_but_return_the_same_document():
    parcels = load_parcels(SAMPLE, 5)
    one = local_source(SAMPLE, parcels, passes=1)()(parcels[0])
    many = local_source(SAMPLE, parcels, passes=7)()(parcels[0])
    assert one == many


def test_lru_overwrite_counts_as_use():
    c = BoundedCache(2, "lru")
    c.put("a", 1)
    c.put("b", 2)
    c.put("a", 3)  # a is now the freshest
    c.put("c", 4)  # evicts b
    assert c.get("a") == (True, 3) and not c.get("b")[0]
