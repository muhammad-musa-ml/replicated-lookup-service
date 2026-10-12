"""Replay every key of the dataset against several dataset replicas and fail on
any difference between their replies.

    python -m lookupsvc.conformance --dataset data/madison.csv.gz \\
        --target python=localhost:50051 --target java=localhost:50052 --target go=localhost:50053

Keys: every parcel, address, street and zip in the dataset, plus altered
spellings (case, padding) and keys that match nothing. The first --target is the
reference; the others are compared against its serialized replies. Exit status 1
if anything differs or any call fails.
"""
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from itertools import islice

import grpc

from .data import load_rows
from .gen import lookup_pb2 as pb
from .gen import lookup_pb2_grpc as pb_grpc

MAX_MESSAGE = 32 * 1024 * 1024

# (name, request class, request field, limit field present)
CALLS = {
    "ByParcel": (pb.ParcelRequest, "parcel", False),
    "ByAddress": (pb.AddressRequest, "address", False),
    "ByStreet": (pb.StreetRequest, "street", True),
    "ByZip": (pb.ZipRequest, "zip", True),
}


@dataclass
class Report:
    checked: int = 0
    diffs: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.diffs and not self.errors


def build_requests(rows):
    """Yield (rpc, request) for every key worth replaying."""
    parcels = sorted({r.parcel for r in rows})
    addresses = sorted({r.address for r in rows})
    streets = sorted({r.street for r in rows})
    zips = sorted({r.zip for r in rows})

    for p in parcels:
        yield "ByParcel", pb.ParcelRequest(parcel=p)
    for a in addresses:
        yield "ByAddress", pb.AddressRequest(address=a)
    for s in streets:
        yield "ByStreet", pb.StreetRequest(street=s)
        yield "ByStreet", pb.StreetRequest(street=s, limit=1)
    for z in zips:
        yield "ByZip", pb.ZipRequest(zip=z)
        yield "ByZip", pb.ZipRequest(zip=z, limit=2)

    # spellings that should normalise (or not) the same way everywhere
    for a in addresses[:: max(1, len(addresses) // 500)]:
        yield "ByAddress", pb.AddressRequest(address=a.upper())
        yield "ByAddress", pb.AddressRequest(address="  " + a.replace(" ", "   ") + "  ")
    for s in streets[:50]:
        yield "ByStreet", pb.StreetRequest(street=s.lower())
    for p in parcels[:50]:
        yield "ByParcel", pb.ParcelRequest(parcel=" " + p + " ")

    for rpc, (req, name, has_limit) in CALLS.items():
        for key in ("", "   ", "no-such-key", "0", "000000000000", "53703 53704"):
            yield rpc, req(**{name: key})


def replay(targets: dict[str, str], rows, workers: int = 16, progress=None) -> Report:
    """targets maps a label to host:port; the first label is the reference."""
    labels = list(targets)
    channels = {
        label: grpc.insecure_channel(
            addr,
            options=[
                ("grpc.max_receive_message_length", MAX_MESSAGE),
                ("grpc.max_send_message_length", MAX_MESSAGE),
            ],
        )
        for label, addr in targets.items()
    }
    stubs = {label: pb_grpc.LookupStub(ch) for label, ch in channels.items()}
    report = Report()

    health = {}
    for label in labels:
        try:
            health[label] = stubs[label].Health(pb.HealthRequest(), timeout=10)
        except grpc.RpcError as e:
            report.errors.append(f"{label}: Health failed: {e.code().name}")
    ref_rows = {h.rows for h in health.values()}
    if len(ref_rows) > 1:
        report.diffs.append(f"row counts differ: { {k: h.rows for k, h in health.items()} }")
    if health and next(iter(ref_rows)) != len(rows):
        report.diffs.append(f"servers hold {next(iter(ref_rows))} rows, dataset file has {len(rows)}")

    def one(item):
        rpc, request = item
        replies = {}
        for label in labels:
            try:
                replies[label] = getattr(stubs[label], rpc)(request, timeout=30).SerializeToString(
                    deterministic=True
                )
            except grpc.RpcError as e:
                return rpc, request, None, f"{label}: {e.code().name} {e.details()}"
        return rpc, request, replies, None

    requests = iter(build_requests(rows))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while batch := list(islice(requests, 4000)):
            for rpc, request, replies, err in pool.map(one, batch):
                report.checked += 1
                key = f"{rpc}({str(request).strip().replace(chr(10), ' ')})"
                if err:
                    report.errors.append(f"{key}: {err}")
                    continue
                ref = replies[labels[0]]
                for label in labels[1:]:
                    if replies[label] != ref:
                        report.diffs.append(f"{key}: {label} differs from {labels[0]}")
            if progress:
                progress(report.checked)

    for ch in channels.values():
        ch.close()
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="replay a dataset against several replicas and compare")
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--target", action="append", required=True, metavar="LABEL=HOST:PORT")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--show", type=int, default=20, help="how many differences to print")
    args = ap.parse_args(argv)

    targets = dict(t.split("=", 1) for t in args.target)
    if len(targets) < 2:
        ap.error("need at least two targets")
    rows = load_rows(args.dataset)
    def progress(n):
        if n % 20000 == 0:
            print(f"  {n} requests", flush=True)

    report = replay(targets, rows, args.workers, progress=progress)

    print(f"{report.checked} requests x {len(targets)} implementations: "
          f"{len(report.diffs)} differences, {len(report.errors)} errors")
    for line in (report.diffs + report.errors)[: args.show]:
        print("  ", line)
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
