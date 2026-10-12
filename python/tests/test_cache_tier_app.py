import re
import time

import pytest

from lookupsvc.cache_tier.app import Config, create_app
from lookupsvc.dataset_server import LookupServicer, build_server


class Counting(LookupServicer):
    def __init__(self, dataset, name):
        super().__init__(dataset, name)
        self.calls = 0

    def ByParcel(self, request, context):
        self.calls += 1
        return super().ByParcel(request, context)

    def ByZip(self, request, context):
        self.calls += 1
        return super().ByZip(request, context)


@pytest.fixture()
def cluster(dataset):
    """Two live replicas and a cache tier app in front of them."""
    servicers = [Counting(dataset, "one"), Counting(dataset, "two")]
    servers, replicas = [], []
    for s in servicers:
        server, port = build_server(dataset, 0, s.replica, workers=4, servicer=s)
        server.start()
        servers.append(server)
        replicas.append((s.replica, f"127.0.0.1:{port}"))

    def app(cache_size=4, **kw):
        return create_app(Config(replicas=replicas, cache_size=cache_size, **{"upstream_timeout": 2, **kw}))

    class Cluster:
        pass

    c = Cluster()
    c.servicers, c.servers, c.app = servicers, servers, app
    yield c
    for s in servers:
        s.stop(0)


def parcels(dataset, n):
    out = []
    for r in dataset.rows:
        if r.parcel not in out:
            out.append(r.parcel)
        if len(out) == n:
            return out


def test_misses_alternate_between_replicas(cluster, dataset):
    client = cluster.app().test_client()
    sources = [client.get(f"/parcel/{p}").get_json()["source"] for p in parcels(dataset, 4)]
    assert sources == ["one", "two", "one", "two"]


def test_repeat_is_served_from_cache_without_an_upstream_call(cluster, dataset):
    client = cluster.app().test_client()
    p = parcels(dataset, 1)[0]
    first = client.get(f"/parcel/{p}").get_json()
    calls = sum(s.calls for s in cluster.servicers)
    again = client.get(f"/parcel/{p}").get_json()
    assert first["source"] in ("one", "two") and again["source"] == "cache"
    assert again["properties"] == first["properties"]
    assert sum(s.calls for s in cluster.servicers) == calls


def test_errors_are_never_cached(cluster):
    client = cluster.app().test_client()
    for _ in range(3):
        r = client.get("/parcel/does-not-exist")
        assert r.status_code == 404
        body = r.get_json()
        assert body["error"] == "no addresses found" and body["source"] in ("one", "two")
    assert sum(s.calls for s in cluster.servicers) == 3, "every miss reached a replica"
    assert client.application.config["CACHE"].keys() == []


def test_lru_eviction_is_visible_through_the_api(cluster, dataset):
    client = cluster.app(cache_size=2).test_client()
    a, b, c = parcels(dataset, 3)
    for p in (a, b):
        client.get(f"/parcel/{p}")
    client.get(f"/parcel/{a}")  # a is fresh, b is the oldest
    client.get(f"/parcel/{c}")  # evicts b
    assert client.get(f"/parcel/{a}").get_json()["source"] == "cache"
    assert client.get(f"/parcel/{b}").get_json()["source"] != "cache"


def test_failover_reports_the_dead_replica_and_keeps_serving(cluster, dataset):
    client = cluster.app(cache_size=0, cooldown=60, upstream_timeout=0.5).test_client()
    ps = parcels(dataset, 8)
    cluster.servers[0].stop(0).wait()

    first = client.get(f"/parcel/{ps[0]}")
    assert first.status_code == 200
    body = first.get_json()
    assert body["source"] == "two" and body["attempts"] == ["one"]

    later = [client.get(f"/parcel/{p}").get_json() for p in ps[1:]]
    assert {b["source"] for b in later} == {"two"}
    assert all(b["attempts"] == [] for b in later), "dead replica is skipped during the cooldown"
    assert client.get("/stats").get_json()["replicas"] == {"one": "down", "two": "up"}


def test_all_replicas_down_is_a_503_not_a_crash(cluster, dataset):
    client = cluster.app(upstream_timeout=0.5).test_client()
    for s in cluster.servers:
        s.stop(0).wait()
    r = client.get(f"/parcel/{parcels(dataset, 1)[0]}")
    assert r.status_code == 503
    body = r.get_json()
    assert body["error"] == "all replicas unavailable" and sorted(body["attempts"]) == ["one", "two"]
    assert client.get("/healthz").status_code == 503


def test_cached_answers_survive_losing_every_replica(cluster, dataset):
    client = cluster.app().test_client()
    p = parcels(dataset, 1)[0]
    client.get(f"/parcel/{p}")
    for s in cluster.servers:
        s.stop(0).wait()
    r = client.get(f"/parcel/{p}")
    assert r.status_code == 200 and r.get_json()["source"] == "cache"


def test_zip_endpoint_end_to_end(cluster, dataset):
    client = cluster.app().test_client()
    zip_code = dataset.rows[0].zip
    expected = len(dataset.by_zip(zip_code))

    body = client.get(f"/zip/{zip_code}?limit=3").get_json()
    assert body["total"] == expected and body["returned"] == min(3, expected)
    assert all(p["zip"] == zip_code for p in body["properties"])
    assert client.get(f"/zip/{zip_code}?limit=3").get_json()["source"] == "cache"

    # a different limit is a different cache entry
    assert client.get(f"/zip/{zip_code}?limit=4").get_json()["source"] != "cache"
    assert client.get("/zip/00000").status_code == 404


@pytest.mark.parametrize(
    "path",
    ["/zip/abc", "/zip/1234", "/zip/53703?limit=0", "/zip/53703?limit=x", "/street", "/address", "/address?q=%20"],
)
def test_bad_input_is_a_400(cluster, path):
    r = cluster.app().test_client().get(path)
    assert r.status_code == 400 and r.get_json()["error"]


def test_address_and_street_lookups(cluster, dataset):
    client = cluster.app().test_client()
    row = dataset.rows[0]
    a = client.get("/address", query_string={"q": row.address.upper()}).get_json()
    assert a["error"] == "" and any(p["address"] == row.address for p in a["properties"])
    s = client.get("/street", query_string={"q": row.street, "limit": 2}).get_json()
    assert s["returned"] <= 2 and s["total"] == len(dataset.by_street(row.street))


def test_metrics_and_stats(cluster, dataset):
    client = cluster.app().test_client()
    p = parcels(dataset, 1)[0]
    client.get(f"/parcel/{p}")
    client.get(f"/parcel/{p}")
    client.get("/parcel/missing")
    text = client.get("/metrics").get_data(as_text=True)
    assert "rls_cache_hits_total 1.0" in text
    assert "rls_cache_misses_total 2.0" in text
    assert 'rls_http_request_seconds_bucket{endpoint="/parcel/<parcel>"' in text
    assert 'rls_http_requests_total{endpoint="/parcel/<parcel>",status="404"} 1.0' in text
    stats = client.get("/stats").get_json()
    assert (stats["hits"], stats["misses"], stats["entries"]) == (1, 2, 1)


def test_upstream_errors_are_counted(cluster, dataset):
    client = cluster.app(cache_size=0, cooldown=60, upstream_timeout=0.5).test_client()
    cluster.servers[0].stop(0).wait()
    client.get(f"/parcel/{parcels(dataset, 1)[0]}")
    text = client.get("/metrics").get_data(as_text=True)
    # a refused connection is UNAVAILABLE on Linux; some Windows loopbacks only
    # ever report the deadline
    assert re.search(r'rls_upstream_errors_total\{code="(UNAVAILABLE|DEADLINE_EXCEEDED)",replica="one"\} 1.0', text)


def test_healthz_reports_each_replica(cluster):
    body = cluster.app().test_client().get("/healthz").get_json()
    assert body["ok"] and set(body["replicas"]) == {"one", "two"}
    assert body["replicas"]["one"]["implementation"] == "python"


def test_config_from_env():
    c = Config.from_env({"REPLICAS": "a=h1:1, h2:2", "CACHE_SIZE": "9", "COOLDOWN": "1.5"})
    assert c.replicas == [("a", "h1:1"), ("h2", "h2:2")]
    assert (c.cache_size, c.cooldown, c.default_limit) == (9, 1.5, 100)


class Hung(LookupServicer):
    """Accepts the call and never answers in time."""

    def ByParcel(self, request, context):
        time.sleep(1.5)
        return super().ByParcel(request, context)


def test_a_hung_replica_is_failed_over_and_then_left_alone(dataset):
    started = []
    replicas = []
    for name, servicer in (("hung", Hung(dataset, "hung")), ("ok", LookupServicer(dataset, "ok"))):
        server, port = build_server(dataset, 0, name, workers=8, servicer=servicer)
        server.start()
        started.append(server)
        replicas.append((name, f"127.0.0.1:{port}"))
    try:
        app = create_app(Config(replicas=replicas, cache_size=0, upstream_timeout=0.4, cooldown=60))
        client = app.test_client()
        ps = parcels(dataset, 6)

        t0 = time.perf_counter()
        first = client.get(f"/parcel/{ps[0]}")
        slow = time.perf_counter() - t0
        assert first.status_code == 200 and first.get_json()["source"] == "ok"
        assert first.get_json()["attempts"] == ["hung"]
        assert 0.35 < slow < 1.4, "one short deadline was spent, not the replica's 1.5 s"

        t0 = time.perf_counter()
        rest = [client.get(f"/parcel/{p}").get_json() for p in ps[1:]]
        assert time.perf_counter() - t0 < 1.0
        assert {r["source"] for r in rest} == {"ok"} and all(r["attempts"] == [] for r in rest)
        assert client.get("/stats").get_json()["replicas"] == {"hung": "down", "ok": "up"}
        text = client.get("/metrics").get_data(as_text=True)
        assert 'rls_upstream_errors_total{code="DEADLINE_EXCEEDED",replica="hung"} 1.0' in text
    finally:
        for s in started:
            s.stop(0)
