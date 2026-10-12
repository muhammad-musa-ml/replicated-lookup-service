"""A client built from the frozen first contract. Asks a server for one parcel and
prints what it understood as JSON. Subprocess only, see legacy_server.py."""
import json
import sys

import grpc
from legacy_gen import lookup_v1_pb2 as pb
from legacy_gen import lookup_v1_pb2_grpc as pb_grpc

target, parcel = sys.argv[1], sys.argv[2]
with grpc.insecure_channel(target) as ch:
    reply = pb_grpc.LookupStub(ch).ByParcel(pb.ParcelRequest(parcel=parcel), timeout=5)
print(json.dumps({"failed": reply.failed, "addresses": [p.address for p in reply.properties]}))
