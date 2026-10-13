import gzip

import pytest

from lookupsvc.data import COLUMNS, load_rows, norm
from lookupsvc.gen import lookup_pb2 as pb


def test_loads_by_column_name_not_position(tmp_path):
    # same data, columns shuffled and an extra column in the middle
    order = ["living_area", "zip", "extra", *[c for c in COLUMNS if c not in ("living_area", "zip")]]
    values = {
        "parcel": "1", "address": "1 Main St", "street": "Main St", "zip": "53703",
        "assessed_total": "10", "assessed_land": "4", "assessed_improvement": "6",
        "lot_size": "12.5", "year_built": "1999", "living_area": "800", "extra": "x",
    }
    path = tmp_path / "shuffled.csv.gz"
    with gzip.open(path, "wt", newline="") as f:
        f.write(",".join(order) + "\n" + ",".join(values[c] for c in order) + "\n")
    (row,) = load_rows(path)
    assert (row.parcel, row.zip, row.living_area, row.lot_size) == ("1", "53703", 800, 12.5)


def test_missing_column_is_named(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("parcel,address\n1,2\n")
    with pytest.raises(ValueError, match="street"):
        load_rows(path)


def test_lookups_cover_every_row(dataset):
    for row in dataset.rows:
        assert row in dataset.by_parcel(row.parcel)
        assert row in dataset.by_address(row.address)
        assert row in dataset.by_street(row.street)
        assert row in dataset.by_zip(row.zip)


def test_address_and_street_ignore_case_and_spacing(dataset):
    row = dataset.rows[0]
    messy = "  " + row.address.upper().replace(" ", "   ") + " "
    assert dataset.by_address(messy) == dataset.by_address(row.address)
    assert dataset.by_street(row.street.lower()) == dataset.by_street(row.street)
    assert norm(" A  b ") == "a b"


def test_parcel_with_several_addresses_keeps_file_order(dataset):
    multi = next(p for p in {r.parcel for r in dataset.rows} if len(dataset.by_parcel(p)) > 3)
    rows = dataset.by_parcel(multi)
    assert [r.address for r in rows] == sorted(r.address for r in rows)


def test_unknown_keys_find_nothing(dataset):
    assert dataset.by_parcel("999") == []
    assert dataset.by_zip("00000") == []
    assert dataset.by_address("") == []


def test_server_reply_shapes(stub, dataset):
    row = dataset.rows[0]
    reply = stub.ByParcel(pb.ParcelRequest(parcel=row.parcel))
    assert reply.error == "" and not reply.failed
    assert reply.total == len(reply.properties) == len(dataset.by_parcel(row.parcel))
    assert reply.properties[0].address == row.address

    missing = stub.ByParcel(pb.ParcelRequest(parcel="nope"))
    assert missing.error == "no addresses found"
    assert missing.failed, "v2 servers still set the deprecated flag for v1 clients"
    assert len(missing.properties) == 0

    assert stub.ByZip(pb.ZipRequest(zip="  ")).error == "empty key"


def test_zip_limit_truncates_but_reports_total(stub, dataset):
    zip_code = max({r.zip for r in dataset.rows}, key=lambda z: len(dataset.by_zip(z)))
    full = len(dataset.by_zip(zip_code))
    reply = stub.ByZip(pb.ZipRequest(zip=zip_code, limit=3))
    assert len(reply.properties) == 3 and reply.total == full
    assert len(stub.ByZip(pb.ZipRequest(zip=zip_code)).properties) == full


def test_health(stub, dataset):
    h = stub.Health(pb.HealthRequest())
    assert h.serving and h.implementation == "python" and h.rows == len(dataset)
