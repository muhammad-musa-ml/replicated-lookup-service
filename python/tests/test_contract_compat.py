"""The cache tier and the servers across the v1 -> v2 contract change.

v2 replaced the `failed` flag with an `error` string (and added ByZip and a
total). These tests run the current code against peers built from the frozen
v1 proto in both directions."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

from lookupsvc.cache_tier.app import Config, create_app
from lookupsvc.dataset_server import build_server

HERE = Path(__file__).resolve().parent


@pytest.fixture(scope="module")
def v1_server(sample_path):
    if not (HERE / "legacy_gen").exists():
        pytest.skip("run scripts/gen_proto.py first")
    proc = subprocess.Popen(
        [sys.executable, str(HERE / "legacy_server.py"), "--data", str(sample_path)],
        stdout=subprocess.PIPE,
        text=True,
    )
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT="), line
    yield f"127.0.0.1:{line.split('=')[1]}"
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture()
def client(v1_server):
    app = create_app(Config(replicas=[("legacy", v1_server)], cache_size=8, upstream_timeout=3))
    return app.test_client()


def test_cache_reads_a_v1_servers_replies(client, dataset):
    row = dataset.rows[0]
    body = client.get(f"/parcel/{row.parcel}").get_json()
    assert body["source"] == "legacy" and body["error"] == ""
    assert [p["address"] for p in body["properties"]] == [r.address for r in dataset.by_parcel(row.parcel)]
    assert body["total"] == len(body["properties"]), "total falls back to the count when the server sends none"


def test_v1_failure_flag_becomes_not_found_and_is_not_cached(client):
    for _ in range(2):
        r = client.get("/parcel/nope")
        assert r.status_code == 404 and r.get_json()["error"] == "no addresses found"
    assert client.application.config["CACHE"].keys() == []


def test_v1_server_without_zip_gives_501_not_a_retry_storm(client):
    r = client.get("/zip/53703")
    assert r.status_code == 501
    assert "does not support" in r.get_json()["error"]


def test_street_limit_is_applied_by_the_cache_when_the_server_ignores_it(client, dataset):
    street = max({r.street for r in dataset.rows}, key=lambda s: len(dataset.by_street(s)))
    body = client.get("/street", query_string={"q": street, "limit": 2}).get_json()
    assert body["returned"] == 2 and body["total"] == len(dataset.by_street(street))


def test_v1_client_still_works_against_a_v2_server(dataset):
    if not (HERE / "legacy_gen").exists():
        pytest.skip("run scripts/gen_proto.py first")
    server, port = build_server(dataset, 0, "v2", workers=2)
    server.start()
    try:
        def ask(parcel):
            out = subprocess.run(
                [sys.executable, str(HERE / "legacy_client.py"), f"127.0.0.1:{port}", parcel],
                capture_output=True, text=True, timeout=30, check=True,
            ).stdout
            return json.loads(out)

        row = dataset.rows[0]
        found = ask(row.parcel)
        assert found["failed"] is False
        assert found["addresses"] == [r.address for r in dataset.by_parcel(row.parcel)]

        missing = ask("nope")
        assert missing["failed"] is True, "v2 servers keep setting the deprecated flag for old clients"
        assert missing["addresses"] == []
    finally:
        server.stop(0)
