"""gRPC front for TableStore. Environment: DATA_DIR, PORT (default 50061)."""
import argparse
import os
import signal
import sys
from concurrent import futures

import grpc

from ..gen import table_pb2 as pb
from ..gen import table_pb2_grpc as pb_grpc
from .store import FORMATS, OPS, InvalidQuery, InvalidUpload, TableStore

MAX_MESSAGE = 128 * 1024 * 1024
OP_NAMES = {pb.SUM: "sum", pb.MEAN: "mean", pb.COUNT: "count"}
FORMAT_NAMES = {pb.CSV: "csv", pb.PARQUET: "parquet"}


class TableServicer(pb_grpc.TableServicer):
    def __init__(self, store: TableStore):
        self.store = store

    def Upload(self, request, context):
        try:
            entry = self.store.upload(request.csv)
        except InvalidUpload as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        _, _, total = self.store.info()
        return pb.UploadReply(upload_id=entry.id, rows=entry.rows, total_rows=total)

    def Aggregate(self, request, context):
        op, fmt = OP_NAMES.get(request.op), FORMAT_NAMES.get(request.format)
        if op is None or fmt is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"op must be one of {OPS}, format one of {FORMATS}")
        try:
            result = self.store.aggregate(request.column, op, fmt)
        except InvalidQuery as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        return pb.AggregateReply(value=result.value, count=result.count, uploads=result.uploads)

    def Info(self, request, context):
        columns, uploads, rows = self.store.info()
        return pb.InfoReply(columns=columns, uploads=uploads, total_rows=rows)


def build_server(store: TableStore, port: int, workers: int = 16):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=workers),
        options=[
            ("grpc.max_receive_message_length", MAX_MESSAGE),
            ("grpc.max_send_message_length", MAX_MESSAGE),
        ],
    )
    pb_grpc.add_TableServicer_to_server(TableServicer(store), server)
    return server, server.add_insecure_port(f"[::]:{port}")


def healthcheck(port: int) -> int:
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as ch:
            pb_grpc.TableStub(ch).Info(pb.InfoRequest(), timeout=2)
        return 0
    except grpc.RpcError:
        return 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--healthcheck", action="store_true")
    args = ap.parse_args(argv)
    port = int(os.environ.get("PORT", "50061"))
    if args.healthcheck:
        return healthcheck(port)

    store = TableStore(os.environ.get("DATA_DIR", "/var/lib/tablestore"))
    server, port = build_server(store, port)
    server.start()
    _, uploads, rows = store.info()
    print(f"table store: {uploads} uploads, {rows} rows, listening on {port}", flush=True)
    signal.signal(signal.SIGTERM, lambda *_: server.stop(2))
    server.wait_for_termination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
