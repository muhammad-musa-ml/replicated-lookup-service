"""Small command line client for the table service.

    python -m lookupsvc.table.client upload part1.csv part2.csv
    python -m lookupsvc.table.client aggregate assessed_total sum --format parquet
    python -m lookupsvc.table.client info
"""
import argparse
import sys
from pathlib import Path

import grpc

from ..gen import table_pb2 as pb
from ..gen import table_pb2_grpc as pb_grpc

MAX_MESSAGE = 128 * 1024 * 1024


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="localhost:8201")
    sub = ap.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("upload")
    up.add_argument("files", nargs="+", type=Path)
    ag = sub.add_parser("aggregate")
    ag.add_argument("column")
    ag.add_argument("op", choices=["sum", "mean", "count"])
    ag.add_argument("--format", choices=["csv", "parquet"], default="parquet")
    sub.add_parser("info")
    args = ap.parse_args(argv)

    options = [("grpc.max_send_message_length", MAX_MESSAGE), ("grpc.max_receive_message_length", MAX_MESSAGE)]
    with grpc.insecure_channel(args.target, options=options) as ch:
        stub = pb_grpc.TableStub(ch)
        try:
            if args.cmd == "upload":
                for f in args.files:
                    r = stub.Upload(pb.UploadRequest(csv=f.read_bytes()))
                    print(f"{f.name}: upload {r.upload_id}, {r.rows} rows, table now {r.total_rows}")
            elif args.cmd == "aggregate":
                r = stub.Aggregate(
                    pb.AggregateRequest(
                        column=args.column,
                        op=getattr(pb, args.op.upper()),
                        format=getattr(pb, args.format.upper()),
                    )
                )
                print(f"{args.op}({args.column}) = {r.value:g} over {r.count} values in {r.uploads} uploads")
            else:
                r = stub.Info(pb.InfoRequest())
                print(f"{r.uploads} uploads, {r.total_rows} rows, columns: {', '.join(r.columns)}")
        except grpc.RpcError as e:
            print(f"{e.code().name}: {e.details()}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
