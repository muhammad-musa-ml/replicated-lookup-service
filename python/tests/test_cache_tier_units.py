import threading

import grpc
import pytest

from lookupsvc.cache_tier.balancer import AllReplicasDown, Balancer, Replica, UpstreamError
from lookupsvc.cache_tier.lru import LRUCache


def test_lru_evicts_least_recently_used():
    c = LRUCache(3)
    for k in "abc":
        c.put(k, k.upper())
    assert c.get("a") == (True, "A")  # a is now the freshest
    c.put("d", "D")  # evicts b
    assert c.get("b") == (False, None)
    assert c.keys() == ["c", "a", "d"]


def test_lru_overwrite_does_not_grow_or_evict_others():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)
    c.put("a", 3)
    assert len(c) == 2 and c.get("a") == (True, 3) and c.get("b") == (True, 2)


def test_lru_capacity_zero_stores_nothing():
    c = LRUCache(0)
    c.put("a", 1)
    assert c.get("a") == (False, None) and len(c) == 0


def test_lru_none_is_a_cacheable_value():
    c = LRUCache(2)
    c.put("a", None)
    assert c.get("a") == (True, None)


def test_lru_negative_capacity_rejected():
    with pytest.raises(ValueError):
        LRUCache(-1)


def test_lru_survives_concurrent_use():
    c = LRUCache(50)

    def work(seed):
        for i in range(3000):
            k = (seed * 7 + i) % 200
            hit, _ = c.get(k)
            if not hit:
                c.put(k, k)

    threads = [threading.Thread(target=work, args=(s,)) for s in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(c) <= 50
    assert all(c.get(k) == (True, k) for k in c.keys())


class FakeRpcError(grpc.RpcError):
    def __init__(self, code, details="boom"):
        self._code, self._details = code, details

    def code(self):
        return self._code

    def details(self):
        return self._details


class FakeStub:
    def __init__(self, fail_with=None):
        self.fail_with = fail_with
        self.calls = 0
        self.timeouts = []

    def ByParcel(self, request, timeout):
        self.calls += 1
        self.timeouts.append(timeout)
        if self.fail_with:
            raise FakeRpcError(self.fail_with)
        return "reply"


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def make(*stubs, cooldown=5.0, clock=None, on_error=None):
    replicas = [Replica(f"r{i + 1}", f"t{i + 1}", s) for i, s in enumerate(stubs)]
    return Balancer(replicas, cooldown=cooldown, clock=clock or Clock(), on_error=on_error)


def test_round_robin_alternates():
    lb = make(FakeStub(), FakeStub(), FakeStub())
    assert [lb.call("ByParcel", None, 1).replica for _ in range(6)] == ["r1", "r2", "r3", "r1", "r2", "r3"]


def test_unavailable_fails_over_and_reports_the_attempt():
    a, b = FakeStub(grpc.StatusCode.UNAVAILABLE), FakeStub()
    lb = make(a, b)
    answer = lb.call("ByParcel", None, 1)
    assert (answer.replica, answer.attempts) == ("r2", ["r1"])
    assert a.calls == 1 and b.calls == 1


def test_all_unavailable_raises_with_every_attempt():
    lb = make(FakeStub(grpc.StatusCode.UNAVAILABLE), FakeStub(grpc.StatusCode.UNAVAILABLE))
    with pytest.raises(AllReplicasDown) as e:
        lb.call("ByParcel", None, 1)
    assert sorted(e.value.attempts) == ["r1", "r2"]


@pytest.mark.parametrize(
    "code",
    [grpc.StatusCode.INTERNAL, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNIMPLEMENTED],
)
def test_other_errors_are_not_retried(code):
    a, b = FakeStub(code), FakeStub()
    lb = make(a, b)
    with pytest.raises(UpstreamError) as e:
        lb.call("ByParcel", None, 1)
    assert e.value.code == code and e.value.replica == "r1"
    assert b.calls == 0


def test_deadline_on_a_channel_that_never_connected_counts_as_unreachable():
    a, b = FakeStub(grpc.StatusCode.DEADLINE_EXCEEDED), FakeStub()
    lb = make(a, b)
    lb.replicas[0].state = grpc.ChannelConnectivity.TRANSIENT_FAILURE
    answer = lb.call("ByParcel", None, 1)
    assert (answer.replica, answer.attempts) == ("r2", ["r1"])


def test_suspect_replica_is_probed_with_a_short_deadline():
    clock = Clock()
    a, b = FakeStub(grpc.StatusCode.UNAVAILABLE), FakeStub()
    lb = make(a, b, cooldown=5.0, clock=clock)
    lb.probe_timeout = 0.25
    lb.call("ByParcel", None, 3.0)
    assert a.timeouts == [3.0]
    clock.now += 6
    lb.call("ByParcel", None, 3.0)  # cooldown over: a is retried, but with the probe deadline
    lb.call("ByParcel", None, 3.0)
    assert a.timeouts[1] == 0.25
    a.fail_with = None
    clock.now += 6
    for _ in range(4):
        lb.call("ByParcel", None, 3.0)
    assert a.timeouts[-2:] == [0.25, 3.0], "one probe succeeds, then a gets the normal deadline again"


def test_down_replica_is_skipped_until_cooldown_ends():
    clock = Clock()
    a, b = FakeStub(grpc.StatusCode.UNAVAILABLE), FakeStub()
    lb = make(a, b, cooldown=5.0, clock=clock)
    lb.call("ByParcel", None, 1)  # a fails, marked down
    for _ in range(4):
        assert lb.call("ByParcel", None, 1).attempts == []
    assert a.calls == 1, "a is not even tried during the cooldown"
    assert lb.states() == {"r1": "down", "r2": "up"}

    clock.now += 6
    a.fail_with = None  # replica came back
    seen = {lb.call("ByParcel", None, 1).replica for _ in range(4)}
    assert seen == {"r1", "r2"}
    assert lb.states() == {"r1": "up", "r2": "up"}


def test_all_marked_down_still_tries_everyone():
    clock = Clock()
    a, b = FakeStub(grpc.StatusCode.UNAVAILABLE), FakeStub(grpc.StatusCode.UNAVAILABLE)
    lb = make(a, b, clock=clock)
    with pytest.raises(AllReplicasDown):
        lb.call("ByParcel", None, 1)
    b.fail_with = None
    assert lb.call("ByParcel", None, 1).replica == "r2"


def test_error_hook_sees_every_failure():
    seen = []
    lb = make(FakeStub(grpc.StatusCode.UNAVAILABLE), FakeStub(), on_error=lambda r, c: seen.append((r, c.name)))
    lb.call("ByParcel", None, 1)
    assert seen == [("r1", "UNAVAILABLE")]


def test_no_replicas_is_a_config_error():
    with pytest.raises(ValueError):
        Balancer([])
