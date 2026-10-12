"""gRPC dataset replica, Python implementation.

Environment: DATA_PATH (gzipped CSV), PORT (default 50051), REPLICA_ID.
`--healthcheck` dials the local port, calls Health and exits 0 or 1; compose
uses it as the container healthcheck.
"""
import argparse
import os
import signal
import sys
from concurrent import futures

import grpc

from .data import EMPTY_KEY, NOT_FOUND, Dataset, Row
from .gen import lookup_pb2 as pb
from .gen import lookup_pb2_grpc as pb_grpc

MAX_MESSAGE = 32 * 1024 * 1024


def to_proto(r: Row) -> pb.Property:
    return pb.Property(
        parcel=r.parcel,
        address=r.address,
        street=r.street,
        zip=r.zip,
        assessed_total=r.assessed_total,
        assessed_land=r.assessed_land,
        assessed_improvement=r.assessed_improvement,
        lot_size=r.lot_size,
        year_built=r.year_built,
        living_area=r.living_area,
    )


def reply_for(rows: list[Row], key: str, limit: int = 0) -> pb.LookupReply:
    if not key.strip():
        return pb.LookupReply(error=EMPTY_KEY, failed=True)
    if not rows:
        return pb.LookupReply(error=NOT_FOUND, failed=True)
    shown = rows[:limit] if limit > 0 else rows
    return pb.LookupReply(properties=[to_proto(r) for r in shown], total=len(rows))


class LookupServicer(pb_grpc.LookupServicer):
    def __init__(self, dataset: Dataset, replica: str = "python-1"):
        self.ds = dataset
        self.replica = replica

    def ByParcel(self, request, context):
        return reply_for(self.ds.by_parcel(request.parcel), request.parcel)

    def ByAddress(self, request, context):
        return reply_for(self.ds.by_address(request.address), request.address)

    def ByStreet(self, request, context):
        return reply_for(self.ds.by_street(request.street), request.street, request.limit)

    def ByZip(self, request, context):
        return reply_for(self.ds.by_zip(request.zip), request.zip, request.limit)

    def Health(self, request, context):
        return pb.HealthReply(
            serving=True, implementation="python", replica=self.replica, rows=len(self.ds)
        )


def build_server(
    dataset: Dataset, port: int, replica: str, workers: int = 16, servicer=None
) -> tuple[grpc.Server, int]:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=workers),
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE),
            ("grpc.max_receive_message_length", MAX_MESSAGE),
        ],
    )
    pb_grpc.add_LookupServicer_to_server(servicer or LookupServicer(dataset, replica), server)
    bound = server.add_insecure_port(f"[::]:{port}")
    return server, bound


def healthcheck(port: int) -> int:
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as ch:
            reply = pb_grpc.LookupStub(ch).Health(pb.HealthRequest(), timeout=2)
        return 0 if reply.serving else 1
    except grpc.RpcError:
        return 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--healthcheck", action="store_true")
    args = ap.parse_args(argv)
    port = int(os.environ.get("PORT", "50051"))
    if args.healthcheck:
        return healthcheck(port)

    replica = os.environ.get("REPLICA_ID", "python-1")
    dataset = Dataset.from_file(os.environ.get("DATA_PATH", "/data/madison.csv.gz"))
    server, port = build_server(dataset, port, replica)
    server.start()
    print(f"{replica}: {len(dataset)} rows, listening on {port}", flush=True)

    signal.signal(signal.SIGTERM, lambda *_: server.stop(2))
    server.wait_for_termination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
