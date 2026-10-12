"""Brings up the compose stack on the synthetic fixture and checks it from the
outside: HTTP through the cache tier, failover when a replica is stopped, the
table service, and all three dataset implementations against each other.

Skipped unless RLS_INTEGRATION=1 (it builds three images and needs Docker, and
ports 8210-8215 plus CACHE_PORT/TABLE_PORT free). Run it from the repo root:

    RLS_INTEGRATION=1 pytest python/tests/integration -s
"""
import os
import sys
import subprocess
import time
from pathlib import Path

import pytest
import requests

pytestmark = pytest.mark.skipif(os.environ.get("RLS_INTEGRATION") != "1", reason="set RLS_INTEGRATION=1")

ROOT = Path(__file__).resolve().parents[3]
CACHE_PORT, TABLE_PORT = 18200, 18201
COMPOSE = [
    "docker", "compose", "-p", "rls-it",
    "-f", "docker-compose.yml", "-f", "docker-compose.fixture.yml",
    "--profile", "all",
]
ENV = {**os.environ, "CACHE_PORT": str(CACHE_PORT), "TABLE_PORT": str(TABLE_PORT), "DATASET_IMPL": "go",
       "COMPOSE_PROFILES": ""}
BASE = f"http://localhost:{CACHE_PORT}"


def compose(*args, check=True):
    return subprocess.run([*COMPOSE, *args], cwd=ROOT, env=ENV, check=check, capture_output=True, text=True, encoding="utf-8", errors="replace")


def wait_for(predicate, what, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise AssertionError(f"timed out waiting for {what}")


def all_healthy():
    out = compose("ps", "--format", "{{.Name}} {{.Health}}", check=False).stdout.split("\n")
    rows = [line.split() for line in out if line.strip()]
    return len(rows) >= 8 and all(len(r) == 2 and r[1] == "healthy" for r in rows)


@pytest.fixture(scope="module")
def stack():
    compose("up", "-d", "--build")
    try:
        wait_for(all_healthy, "every service to report healthy")
        yield
    finally:
        compose("down", "-v", "-t", "1", check=False)


def get(path, **kw):
    return requests.get(BASE + path, timeout=10, **kw)


def test_lookup_through_the_cache_and_repeat_from_cache(stack):
    first = get("/zip/53703?limit=5").json()
    assert first["returned"] == 5 and first["source"].startswith("dataset-go-")
    parcel = first["properties"][0]["parcel"]

    a = get(f"/parcel/{parcel}").json()
    b = get(f"/parcel/{parcel}").json()
    assert a["source"].startswith("dataset-go-") and b["source"] == "cache"
    assert a["properties"] == b["properties"]
    assert get("/parcel/000").status_code == 404


def test_metrics_are_exposed(stack):
    get("/zip/53703")
    text = get("/metrics").text
    assert "rls_cache_hits_total" in text and "rls_http_request_seconds_bucket" in text


def test_all_three_implementations_agree(stack):
    out = subprocess.run(
        [
            sys.executable, "-m", "lookupsvc.conformance", "--dataset", "testdata/sample.csv.gz",
            "--target", "python=localhost:8210", "--target", "java=localhost:8212",
            "--target", "go=localhost:8214",
        ],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert "0 differences, 0 errors" in out.stdout


def test_traffic_survives_a_stopped_replica(stack):
    parcels = []
    for p in get("/zip/53704?limit=200").json()["properties"]:
        if p["parcel"] not in parcels:
            parcels.append(p["parcel"])
    assert len(parcels) >= 12

    compose("stop", "-t", "1", "dataset-go-1")
    try:
        results = [get(f"/parcel/{p}") for p in parcels[:12]]
        assert [r.status_code for r in results] == [200] * 12
        assert {r.json()["source"] for r in results} == {"dataset-go-2"}
        assert any(r.json()["attempts"] == ["dataset-go-1"] for r in results)
    finally:
        compose("start", "dataset-go-1")
        wait_for(all_healthy, "dataset-go-1 to come back")


def test_table_service_accumulates_uploads(stack):
    import grpc

    from lookupsvc.gen import table_pb2 as pb
    from lookupsvc.gen import table_pb2_grpc as pb_grpc

    with grpc.insecure_channel(f"localhost:{TABLE_PORT}") as ch:
        stub = pb_grpc.TableStub(ch)
        stub.Upload(pb.UploadRequest(csv=b"a,b\n1,2.5\n2,3.5\n"))
        r = stub.Upload(pb.UploadRequest(csv=b"a,b\n3,4.0\n"))
        assert r.total_rows == 3
        for fmt in (pb.CSV, pb.PARQUET):
            agg = stub.Aggregate(pb.AggregateRequest(column="b", op=pb.SUM, format=fmt))
            assert agg.value == 10.0 and agg.uploads == 2
