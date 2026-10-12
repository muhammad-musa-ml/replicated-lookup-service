from pathlib import Path

import grpc
import pytest

from lookupsvc.data import Dataset
from lookupsvc.dataset_server import build_server
from lookupsvc.gen import lookup_pb2_grpc as pb_grpc

SAMPLE = Path(__file__).resolve().parents[2] / "testdata" / "sample.csv.gz"


@pytest.fixture(scope="session")
def sample_path():
    return SAMPLE


@pytest.fixture(scope="session")
def dataset():
    return Dataset.from_file(SAMPLE)


@pytest.fixture()
def replica(dataset):
    """A live in-process dataset replica; yields (target, server)."""
    server, port = build_server(dataset, 0, "test-1", workers=4)
    server.start()
    yield f"127.0.0.1:{port}", server
    server.stop(0).wait()


@pytest.fixture()
def stub(replica):
    target, _ = replica
    with grpc.insecure_channel(target) as ch:
        yield pb_grpc.LookupStub(ch)
