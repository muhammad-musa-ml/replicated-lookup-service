"""HTTP/JSON cache tier in front of the dataset replicas.

Environment (see Config.from_env): REPLICAS ("name=host:port,..."), CACHE_SIZE,
UPSTREAM_TIMEOUT, COOLDOWN, PROBE_TIMEOUT, DEFAULT_LIMIT.
"""
import os
import re
import time
from dataclasses import dataclass

import grpc
from flask import Flask, jsonify, request
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from ..data import EMPTY_KEY, NOT_FOUND
from ..gen import lookup_pb2 as pb
from .balancer import AllReplicasDown, Balancer, UpstreamError, connect
from .lru import LRUCache

PROPERTY_FIELDS = [f.name for f in pb.Property.DESCRIPTOR.fields]
ZIP_RE = re.compile(r"^\d{5}$")
MAX_LIMIT = 5000


@dataclass
class Config:
    replicas: list  # [(name, target)]
    cache_size: int = 256
    upstream_timeout: float = 2.0
    cooldown: float = 5.0
    probe_timeout: float = 0.5
    default_limit: int = 100

    @classmethod
    def from_env(cls, env=os.environ) -> "Config":
        replicas = []
        for item in env["REPLICAS"].split(","):
            item = item.strip()
            if not item:
                continue
            name, _, target = item.partition("=")
            replicas.append((name, target) if target else (name.split(":")[0], name))
        return cls(
            replicas=replicas,
            cache_size=int(env.get("CACHE_SIZE", "256")),
            upstream_timeout=float(env.get("UPSTREAM_TIMEOUT", "2.0")),
            cooldown=float(env.get("COOLDOWN", "5.0")),
            probe_timeout=float(env.get("PROBE_TIMEOUT", "0.5")),
            default_limit=int(env.get("DEFAULT_LIMIT", "100")),
        )


def property_dict(p) -> dict:
    return {name: getattr(p, name) for name in PROPERTY_FIELDS}


def reply_error(reply) -> str:
    """The error of a reply under either contract. A v1 server sets only the
    failed flag, so an empty failed reply reads as 'not found'."""
    if reply.error:
        return reply.error
    if reply.failed or not reply.properties:
        return NOT_FOUND
    return ""


class Metrics:
    def __init__(self, cache: LRUCache):
        self.registry = CollectorRegistry()
        self.hits = Counter("rls_cache_hits_total", "Lookups answered from the cache", registry=self.registry)
        self.misses = Counter("rls_cache_misses_total", "Lookups that went to a replica", registry=self.registry)
        self.requests = Counter(
            "rls_http_requests_total", "HTTP requests", ["endpoint", "status"], registry=self.registry
        )
        self.latency = Histogram(
            "rls_http_request_seconds",
            "HTTP request latency",
            ["endpoint"],
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
            registry=self.registry,
        )
        self.upstream_errors = Counter(
            "rls_upstream_errors_total", "gRPC errors from replicas", ["replica", "code"], registry=self.registry
        )
        self.upstream_calls = Counter(
            "rls_upstream_answers_total", "Lookups answered by each replica", ["replica"], registry=self.registry
        )
        Gauge("rls_cache_entries", "Entries in the cache", registry=self.registry).set_function(cache.__len__)


def counter_value(counter: Counter) -> int:
    return int(next(s.value for s in counter.collect()[0].samples if s.name.endswith("_total")))


def create_app(config: Config, balancer: Balancer | None = None) -> Flask:
    app = Flask(__name__)
    cache = LRUCache(config.cache_size)
    metrics = Metrics(cache)
    if balancer is None:
        balancer = Balancer(
            [connect(n, t) for n, t in config.replicas],
            cooldown=config.cooldown,
            probe_timeout=config.probe_timeout,
            on_error=lambda replica, code: metrics.upstream_errors.labels(replica, code.name).inc(),
        )
    app.config.update(CACHE=cache, METRICS=metrics, BALANCER=balancer)

    def respond(properties, total, source, attempts, status=200, error=""):
        body = {
            "properties": properties,
            "returned": len(properties),
            "total": total,
            "error": error,
            "source": source,
            "attempts": attempts,
        }
        return jsonify(body), status

    def lookup(kind: str, key: str, build_request, limit: int):
        """Cache-aside lookup shared by all four endpoints."""
        cache_key = (kind, key, limit)
        hit, value = cache.get(cache_key)
        if hit:
            metrics.hits.inc()
            return respond(value[0], value[1], "cache", [])
        metrics.misses.inc()

        try:
            answer = balancer.call(kind, build_request(), config.upstream_timeout)
        except AllReplicasDown as e:
            return respond([], 0, "none", e.attempts, 503, "all replicas unavailable")
        except UpstreamError as e:
            status = 501 if e.code == grpc.StatusCode.UNIMPLEMENTED else 502
            msg = "upstream does not support this lookup" if status == 501 else f"grpc error: {e.code.name}"
            return respond([], 0, e.replica, [], status, msg)

        metrics.upstream_calls.labels(answer.replica).inc()
        reply = answer.reply
        err = reply_error(reply)
        if err:
            status = 400 if err == EMPTY_KEY else 404 if err == NOT_FOUND else 502
            return respond([], 0, answer.replica, answer.attempts, status, err)

        properties = [property_dict(p) for p in reply.properties]
        total = reply.total or len(properties)
        if limit and len(properties) > limit:  # a v1 server ignores the limit
            properties = properties[:limit]
        cache.put(cache_key, (properties, total))
        return respond(properties, total, answer.replica, answer.attempts)

    def limit_arg() -> int | None:
        raw = request.args.get("limit")
        if raw is None:
            return config.default_limit
        if not raw.isdigit() or not (0 < int(raw) <= MAX_LIMIT):
            return None
        return int(raw)

    def bad(message):
        return respond([], 0, "none", [], 400, message)

    @app.get("/parcel/<parcel>")
    def by_parcel(parcel):
        parcel = parcel.strip()
        if not parcel or len(parcel) > 64:
            return bad("invalid parcel")
        return lookup("ByParcel", parcel, lambda: pb.ParcelRequest(parcel=parcel), 0)

    @app.get("/address")
    def by_address():
        q = " ".join(request.args.get("q", "").split())
        if not q or len(q) > 200:
            return bad("q must be a street address")
        return lookup("ByAddress", q.lower(), lambda: pb.AddressRequest(address=q), 0)

    @app.get("/street")
    def by_street():
        q = " ".join(request.args.get("q", "").split())
        limit = limit_arg()
        if not q or len(q) > 200:
            return bad("q must be a street name")
        if limit is None:
            return bad(f"limit must be between 1 and {MAX_LIMIT}")
        return lookup("ByStreet", q.lower(), lambda: pb.StreetRequest(street=q, limit=limit), limit)

    @app.get("/zip/<zip_code>")
    def by_zip(zip_code):
        limit = limit_arg()
        if not ZIP_RE.match(zip_code):
            return bad("zip must be five digits")
        if limit is None:
            return bad(f"limit must be between 1 and {MAX_LIMIT}")
        return lookup("ByZip", zip_code, lambda: pb.ZipRequest(zip=zip_code, limit=limit), limit)

    @app.get("/healthz")
    def healthz():
        states = {}
        for r in balancer.replicas:
            try:
                h = r.stub.Health(pb.HealthRequest(), timeout=1)
                states[r.name] = {"serving": h.serving, "implementation": h.implementation, "rows": h.rows}
            except grpc.RpcError as e:
                states[r.name] = {"serving": False, "error": e.code().name}
        ok = any(s["serving"] for s in states.values())
        return jsonify({"ok": ok, "replicas": states}), 200 if ok else 503

    @app.get("/stats")
    def stats():
        return jsonify(
            {
                "cache_size": cache.capacity,
                "entries": len(cache),
                "hits": counter_value(metrics.hits),
                "misses": counter_value(metrics.misses),
                "replicas": balancer.states(),
            }
        )

    @app.get("/metrics")
    def prom():
        return generate_latest(metrics.registry), 200, {"Content-Type": CONTENT_TYPE_LATEST}

    @app.before_request
    def start_timer():
        request.environ["rls.t0"] = time.perf_counter()

    @app.after_request
    def observe(response):
        endpoint = request.url_rule.rule if request.url_rule else "unmatched"
        if endpoint != "/metrics":
            metrics.latency.labels(endpoint).observe(time.perf_counter() - request.environ["rls.t0"])
            metrics.requests.labels(endpoint, str(response.status_code)).inc()
        return response

    return app
