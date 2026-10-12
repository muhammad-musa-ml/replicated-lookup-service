import gzip

import pyarrow.parquet as pq

from lookupsvc.data import COLUMNS, Dataset
from lookupsvc.fetch import join_rows, write_csv_gz, write_parquet


def tax(parcel, total="100", land="40", impr="60", lot="1000.5", year="1990", area="900"):
    return {
        "Parcel": parcel, "CurrentTotal": total, "CurrentLand": land, "CurrentImpr": impr,
        "LotSize": lot, "YearBuilt": year, "TotalLivingArea": area,
    }


def addr(parcel, address, zip_code="53703-1234", d="W", n="Dayton", t="St"):
    return {
        "Parcel": parcel, "Address": address, "ZipCode": zip_code,
        "StreetDir": d, "StreetName": n, "StreetType": t,
    }


def test_join_builds_street_zip_and_numbers():
    rows, unmatched, dupes = join_rows(
        [tax("A", total="", year="")],
        [addr("A", "1308  W Dayton St"), addr("A", "1308 W Dayton St"), addr("B", "9 Nowhere Rd")],
    )
    assert (unmatched, dupes) == (1, 1)
    (r,) = rows
    assert r["address"] == "1308 W Dayton St"
    assert r["street"] == "W Dayton St"
    assert r["zip"] == "53703"
    assert r["assessed_total"] == 0 and r["year_built"] == 0
    assert r["lot_size"] == 1000.5


def test_street_without_direction():
    rows, _, _ = join_rows([tax("A")], [addr("A", "5 State St", d="", n="State")])
    assert rows[0]["street"] == "State St"


def test_outputs_roundtrip(tmp_path):
    rows, _, _ = join_rows(
        [tax("B"), tax("A", total="1.0e5")],
        [addr("B", "2 Mifflin St", n="Mifflin"), addr("A", "1 State St", n="State")],
    )
    assert [r["parcel"] for r in rows] == ["A", "B"], "output is sorted"
    csv_path, pq_path = tmp_path / "d.csv.gz", tmp_path / "d.parquet"
    write_csv_gz(rows, csv_path)
    write_parquet(rows, pq_path)
    assert rows[0]["assessed_total"] == 100000

    ds = Dataset.from_file(csv_path)
    assert len(ds) == 2
    table = pq.read_table(pq_path)
    assert table.column_names == COLUMNS
    assert table.to_pylist() == rows


def test_csv_output_is_deterministic(tmp_path):
    rows, _, _ = join_rows([tax("A")], [addr("A", "1 State St", n="State")])
    a, b = tmp_path / "a.csv.gz", tmp_path / "b.csv.gz"
    write_csv_gz(rows, a)
    write_csv_gz(rows, b)
    assert a.read_bytes() == b.read_bytes()
    assert gzip.open(a, "rt").readline().strip().split(",") == COLUMNS
