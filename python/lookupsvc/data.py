import csv
import gzip
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

COLUMNS = [
    "parcel",
    "address",
    "street",
    "zip",
    "assessed_total",
    "assessed_land",
    "assessed_improvement",
    "lot_size",
    "year_built",
    "living_area",
]

NOT_FOUND = "no addresses found"
EMPTY_KEY = "empty key"


@dataclass(frozen=True)
class Row:
    parcel: str
    address: str
    street: str
    zip: str
    assessed_total: int
    assessed_land: int
    assessed_improvement: int
    lot_size: float
    year_built: int
    living_area: int


def norm(text: str) -> str:
    """Key normalisation shared with the Java and Go servers: collapse
    whitespace, ASCII lowercase."""
    return " ".join(text.split()).lower()


def _open_text(path: Path):
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return open(path, encoding="utf-8", newline="")


def load_rows(path) -> list[Row]:
    path = Path(path)
    with _open_text(path) as f:
        reader = csv.DictReader(f)
        missing = [c for c in COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{path} is missing columns: {', '.join(missing)}")
        return [
            Row(
                parcel=r["parcel"],
                address=r["address"],
                street=r["street"],
                zip=r["zip"],
                assessed_total=int(r["assessed_total"]),
                assessed_land=int(r["assessed_land"]),
                assessed_improvement=int(r["assessed_improvement"]),
                lot_size=float(r["lot_size"]),
                year_built=int(r["year_built"]),
                living_area=int(r["living_area"]),
            )
            for r in reader
        ]


class Dataset:
    """Rows in file order plus one index per lookup key."""

    def __init__(self, rows: list[Row]):
        self.rows = rows
        self._parcel: dict[str, list[int]] = defaultdict(list)
        self._address: dict[str, list[int]] = defaultdict(list)
        self._street: dict[str, list[int]] = defaultdict(list)
        self._zip: dict[str, list[int]] = defaultdict(list)
        for i, r in enumerate(rows):
            self._parcel[r.parcel].append(i)
            self._address[norm(r.address)].append(i)
            self._street[norm(r.street)].append(i)
            self._zip[r.zip].append(i)

    @classmethod
    def from_file(cls, path) -> "Dataset":
        return cls(load_rows(path))

    def _rows(self, index: dict, key: str) -> list[Row]:
        return [self.rows[i] for i in index.get(key, ())]

    def by_parcel(self, parcel: str) -> list[Row]:
        return self._rows(self._parcel, parcel.strip())

    def by_address(self, address: str) -> list[Row]:
        return self._rows(self._address, norm(address))

    def by_street(self, street: str) -> list[Row]:
        return self._rows(self._street, norm(street))

    def by_zip(self, zip_code: str) -> list[Row]:
        return self._rows(self._zip, zip_code.strip())

    def __len__(self) -> int:
        return len(self.rows)
