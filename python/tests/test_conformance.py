import grpc
import pytest

from lookupsvc import conformance
from lookupsvc.data import load_rows
from lookupsvc.dataset_server import LookupServicer, build_server
from lookupsvc.gen import lookup_pb2 as pb


class Tampered(LookupServicer):
    """Right answers everywhere except one address."""

    def __init__(self, dataset, victim):
        super().__init__(dataset, "tampered")
        self.victim = victim

    def ByAddress(self, request, context):
        reply = super().ByAddress(request, context)
        if request.address == self.victim:
            reply.properties[0].assessed_total += 1
        return reply


class Broken(LookupServicer):
    def ByZip(self, request, context):
        context.abort(grpc.StatusCode.INTERNAL, "boom")


@pytest.fixture()
def rows(sample_path):
    return load_rows(sample_path)


@pytest.fixture()
def start(dataset):
    started = []

    def make(servicer):
        server, port = build_server(dataset, 0, "x", workers=4, servicer=servicer)
        server.start()
        started.append(server)
        return f"127.0.0.1:{port}"

    yield make
    for s in started:
        s.stop(0)


def test_identical_replicas_agree(dataset, rows, start):
    a, b = start(LookupServicer(dataset, "a")), start(LookupServicer(dataset, "b"))
    report = conformance.replay({"a": a, "b": b}, rows, workers=4)
    assert report.ok, report.diffs[:3] + report.errors[:3]
    assert report.checked > len(rows)


def test_one_wrong_field_is_caught(dataset, rows, start):
    victim = rows[7].address
    good, bad = start(LookupServicer(dataset)), start(Tampered(dataset, victim))
    report = conformance.replay({"good": good, "bad": bad}, rows, workers=4)
    assert not report.ok
    assert any(victim in d and "bad differs from good" in d for d in report.diffs)


def test_a_failing_rpc_is_an_error_not_a_pass(dataset, rows, start):
    good, bad = start(LookupServicer(dataset)), start(Broken(dataset))
    report = conformance.replay({"good": good, "bad": bad}, rows, workers=4)
    assert report.errors and "INTERNAL" in report.errors[0]


def test_row_count_mismatch_is_reported(dataset, rows, start):
    addr = start(LookupServicer(dataset))
    report = conformance.replay({"a": addr, "b": addr}, rows[:-1], workers=2)
    assert any("dataset file has" in d for d in report.diffs)


def test_request_set_includes_the_awkward_keys(rows):
    seen = list(conformance.build_requests(rows))
    assert ("ByParcel", pb.ParcelRequest(parcel="")) in seen
    assert any(rpc == "ByZip" and req.limit == 2 for rpc, req in seen)
    assert any(rpc == "ByAddress" and req.address == req.address.upper() for rpc, req in seen)
