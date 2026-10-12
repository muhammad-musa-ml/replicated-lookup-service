"""A dataset server built from the frozen first contract (proto-v1/lookup_v1.proto).

It runs as a subprocess because both contracts use the package name `lookup`,
and two files defining the same message names cannot live in one Python process.

    python legacy_server.py --data ../../testdata/sample.csv.gz   # prints PORT=<n>
"""
import argparse
import sys
from concurrent import futures

import grpc
from legacy_gen import lookup_v1_pb2 as pb
from legacy_gen import lookup_v1_pb2_grpc as pb_grpc

from lookupsvc.data import Dataset


def prop(r):
    return pb.Property(
        parcel=r.parcel, address=r.address, street=r.street, zip=r.zip,
        assessed_total=r.assessed_total, assessed_land=r.assessed_land,
        assessed_improvement=r.assessed_improvement, lot_size=r.lot_size,
        year_built=r.year_built, living_area=r.living_area,
    )


class V1(pb_grpc.LookupServicer):
    def __init__(self, ds):
        self.ds = ds

    def _reply(self, rows):
        if not rows:
            return pb.LookupReply(failed=True)
        return pb.LookupReply(properties=[prop(r) for r in rows])

    def ByParcel(self, request, context):
        return self._reply(self.ds.by_parcel(request.parcel))

    def ByAddress(self, request, context):
        return self._reply(self.ds.by_address(request.address))

    def ByStreet(self, request, context):
        return self._reply(self.ds.by_street(request.street))

    def Health(self, request, context):
        return pb.HealthReply(serving=True, implementation="python-v1", replica="legacy", rows=len(self.ds))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    args = ap.parse_args()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_LookupServicer_to_server(V1(Dataset.from_file(args.data)), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    print(f"PORT={port}", flush=True)
    server.wait_for_termination()
    sys.exit(0)
