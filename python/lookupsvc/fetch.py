"""Download the City of Madison tax parcel and address point tables, join them on
the parcel id and write the lookup dataset (gzipped CSV plus a Parquet copy).

    python -m lookupsvc.fetch --out data

The raw downloads stay under <out>/raw and are not committed anywhere.
"""
import argparse
import csv
import gzip
import io
import sys
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .data import COLUMNS

TAX_URL = (
    "https://data-cityofmadison.opendata.arcgis.com/api/download/v1/items/"
    "0338b0638e4749c395f8d38b39a5c466/csv?layers=0"
)
ADDRESS_URL = (
    "https://data-cityofmadison.opendata.arcgis.com/api/download/v1/items/"
    "a72d02a4fda34327ae68dd0c2fd07455/csv?layers=20"
)


def _num(text: str, kind=int):
    text = (text or "").strip()
    if not text:
        return kind(0)
    return kind(float(text))


def join_rows(tax_rows, address_rows):
    """Address points supply parcel/address/zip, tax parcels the numbers.
    Address points whose parcel is not in the tax table are dropped, and so are
    repeated (parcel, address) pairs. Returns (rows, dropped_unmatched, dropped_dupes)."""
    tax = {}
    for r in tax_rows:
        tax.setdefault(r["Parcel"].strip(), r)

    seen = set()
    out = []
    unmatched = dupes = 0
    for a in address_rows:
        parcel = a["Parcel"].strip()
        address = " ".join(a["Address"].split())
        if parcel not in tax or not address:
            unmatched += 1
            continue
        if (parcel, address) in seen:
            dupes += 1
            continue
        seen.add((parcel, address))
        t = tax[parcel]
        street = " ".join(p for p in (a["StreetDir"], a["StreetName"], a["StreetType"]) if p.strip())
        out.append(
            {
                "parcel": parcel,
                "address": address,
                "street": " ".join(street.split()),
                "zip": a["ZipCode"].strip()[:5],
                "assessed_total": _num(t["CurrentTotal"]),
                "assessed_land": _num(t["CurrentLand"]),
                "assessed_improvement": _num(t["CurrentImpr"]),
                "lot_size": _num(t["LotSize"], float),
                "year_built": _num(t["YearBuilt"]),
                "living_area": _num(t["TotalLivingArea"]),
            }
        )
    out.sort(key=lambda r: (r["parcel"], r["address"]))
    return out, unmatched, dupes


def write_csv_gz(rows, path: Path) -> None:
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    # no name and mtime=0 in the header, so identical data gives identical bytes
    with open(path, "wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz:
        gz.write(buf.getvalue().encode("utf-8"))


def write_parquet(rows, path: Path) -> None:
    schema = pa.schema(
        [
            ("parcel", pa.string()),
            ("address", pa.string()),
            ("street", pa.string()),
            ("zip", pa.string()),
            ("assessed_total", pa.int64()),
            ("assessed_land", pa.int64()),
            ("assessed_improvement", pa.int64()),
            ("lot_size", pa.float64()),
            ("year_built", pa.int32()),
            ("living_area", pa.int32()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")


def _download(url: str, dest: Path) -> None:
    print("downloading", dest.name, flush=True)
    req = urllib.request.Request(url, headers={"User-Agent": "replicated-lookup-service"})
    tmp = dest.with_suffix(".part")
    with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as f:
        while chunk := resp.read(1 << 20):
            f.write(chunk)
    tmp.replace(dest)


def _read_csv(path: Path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default="data", type=Path)
    ap.add_argument("--force", action="store_true", help="download again and rebuild")
    args = ap.parse_args(argv)

    out = args.out
    csv_out = out / "madison.csv.gz"
    parquet_out = out / "madison.parquet"
    if csv_out.exists() and parquet_out.exists() and not args.force:
        print("dataset already built:", csv_out)
        return 0

    raw = out / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    tax_path, addr_path = raw / "tax_parcels.csv", raw / "address_points.csv"
    for url, path in ((TAX_URL, tax_path), (ADDRESS_URL, addr_path)):
        if args.force or not path.exists():
            _download(url, path)

    rows, unmatched, dupes = join_rows(_read_csv(tax_path), _read_csv(addr_path))
    write_csv_gz(rows, csv_out)
    write_parquet(rows, parquet_out)
    print(f"{len(rows)} rows ({unmatched} address points without a tax parcel, {dupes} repeated addresses dropped)")
    print("wrote", csv_out, "and", parquet_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
