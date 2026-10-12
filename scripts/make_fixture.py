"""Write testdata/sample.csv.gz: a small synthetic dataset in the same shape as the
Madison one, used by unit tests, the conformance run in CI and the compose smoke
test. Fixed seed, so rerunning gives byte-identical output."""
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "python"))

from lookupsvc.fetch import write_csv_gz  # noqa: E402

STREETS = [
    ("", "State", "St"),
    ("W", "Dayton", "St"),
    ("N", "Lake Mendota", "Dr"),
    ("E", "Washington", "Ave"),
    ("", "Mifflin", "St"),
    ("S", "Park", "St"),
    ("", "Hawks Landing", "Cir"),
    ("W", "Johnson", "St"),
    ("", "Monroe", "St"),
    ("", "Odana", "Rd"),
    ("N", "Sherman", "Ave"),
    ("", "Atwood", "Ave"),
]
ZIPS = ["53703", "53704", "53705", "53706", "53711", "53719"]


def build(seed=544, parcels=140):
    rng = random.Random(seed)
    rows = []
    for i in range(parcels):
        parcel = f"07{rng.randrange(10**9, 10**10 - 1):010d}"
        direction, name, kind = rng.choice(STREETS)
        zip_code = rng.choice(ZIPS)
        number = rng.randrange(1, 4000)
        street = " ".join(p for p in (direction, name, kind) if p)
        if i % 9 == 0:
            units = [f" Unit {u}" for u in range(101, 101 + rng.randrange(3, 14))]
        else:
            units = [""]
        total = 0 if i % 31 == 0 else rng.randrange(80_000, 2_500_000)
        land = int(total * 0.3)
        for unit in units:
            rows.append(
                {
                    "parcel": parcel,
                    "address": f"{number} {street}{unit}",
                    "street": street,
                    "zip": zip_code,
                    "assessed_total": total,
                    "assessed_land": land,
                    "assessed_improvement": total - land,
                    "lot_size": 0.0 if total == 0 else round(rng.uniform(1500, 22000), 2),
                    "year_built": 0 if total == 0 else rng.randrange(1890, 2024),
                    "living_area": 0 if total == 0 else rng.randrange(400, 6000),
                }
            )
    rows.sort(key=lambda r: (r["parcel"], r["address"]))
    return rows


if __name__ == "__main__":
    out = ROOT / "testdata"
    out.mkdir(exist_ok=True)
    rows = build()
    write_csv_gz(rows, out / "sample.csv.gz")
    print(len(rows), "rows ->", out / "sample.csv.gz")
