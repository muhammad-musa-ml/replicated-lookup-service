import threading
import time

import grpc
import pytest

from lookupsvc.gen import table_pb2 as pb
from lookupsvc.gen import table_pb2_grpc as pb_grpc
from lookupsvc.table.server import build_server
from lookupsvc.table.store import InvalidQuery, InvalidUpload, Sequence, TableStore


def csv(rows, header="id,price,city"):
    return (header + "\n" + "\n".join(rows) + "\n").encode()


@pytest.fixture()
def store(tmp_path):
    return TableStore(tmp_path / "t")


def test_uploads_accumulate_and_both_formats_agree(store):
    store.upload(csv(["1,10.5,Madison", "2,20.0,Verona"]))
    store.upload(csv(["3,4.5,Madison", "4,,Fitchburg"]))  # one missing price

    columns, uploads, rows = store.info()
    assert (columns, uploads, rows) == (["id", "price", "city"], 2, 4)
    for fmt in ("csv", "parquet"):
        assert store.aggregate("price", "sum", fmt).value == 35.0
        assert store.aggregate("price", "count", fmt).value == 3, "empty cells are not counted"
        mean = store.aggregate("price", "mean", fmt)
        assert mean.value == pytest.approx(35.0 / 3) and mean.count == 3 and mean.uploads == 2
        assert store.aggregate("id", "sum", fmt).value == 10
        assert store.aggregate("city", "count", fmt).value == 4


def test_each_upload_is_stored_as_csv_and_parquet(store):
    e = store.upload(csv(["1,1.0,a"]))
    assert (store.root / e.csv).read_bytes() == csv(["1,1.0,a"])
    assert (store.root / e.parquet).stat().st_size > 0


def test_second_upload_may_widen_integers_into_a_float_column(store):
    store.upload(csv(["1,1.5,a"]))
    store.upload(csv(["2,3,b"]))  # 3 reads as an integer, the table says float
    assert store.aggregate("price", "sum", "parquet").value == 4.5
    assert store.aggregate("price", "sum", "csv").value == 4.5


@pytest.mark.parametrize(
    "bad",
    [
        csv(["1,1.0,a"], header="id,price,town"),
        csv(["1,1.0"], header="id,price"),
        csv(["x,1.0,a"]),  # id was an integer column
        csv(["1,1.0,a"], header="id,city,price"),
        b"",
    ],
)
def test_incompatible_uploads_are_rejected_and_change_nothing(store, bad):
    store.upload(csv(["1,1.0,a"]))
    with pytest.raises(InvalidUpload):
        store.upload(bad)
    assert store.info()[1:] == (1, 1)
    assert store.aggregate("price", "sum", "csv").value == 1.0


def test_float_values_do_not_silently_truncate_into_an_integer_column(store):
    store.upload(csv(["1,1.0,a"]))
    with pytest.raises(InvalidUpload):
        store.upload(csv(["1.5,1.0,a"]))


@pytest.mark.parametrize(
    "args",
    [("nope", "sum", "csv"), ("city", "sum", "csv"), ("price", "median", "csv"), ("price", "sum", "xlsx")],
)
def test_bad_queries(store, args):
    store.upload(csv(["1,1.0,a"]))
    with pytest.raises(InvalidQuery):
        store.aggregate(*args)


def test_query_before_any_upload(store):
    with pytest.raises(InvalidQuery):
        store.aggregate("price", "sum", "csv")
    assert store.info() == ([], 0, 0)


def test_mean_of_an_all_null_column_is_an_error(store):
    store.upload(csv(["1,,a"], header="id,price,city"))
    with pytest.raises(InvalidQuery):
        store.aggregate("price", "mean", "parquet")


def test_state_survives_a_restart_and_ids_continue(tmp_path):
    root = tmp_path / "t"
    a = TableStore(root)
    a.upload(csv(["1,1.0,a"]))
    a.upload(csv(["2,2.0,b"]))
    b = TableStore(root)
    assert b.info()[1:] == (2, 2)
    assert b.aggregate("price", "sum", "csv").value == 3.0
    assert b.upload(csv(["3,3.0,c"])).id == 2
    assert b.aggregate("price", "sum", "parquet").value == 6.0


# --- concurrency -----------------------------------------------------------


class NoLock:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class SlowSequence(Sequence):
    """Same counter, but with a gap between reading and writing the value, which
    is where an unprotected read-modify-write loses updates."""

    def next(self):
        v = self.value
        time.sleep(0.02)
        self.value = v + 1
        return v


ROWS = 5


def hammer(root, lock, uploaders=8):
    store = TableStore(root, lock=lock, sequence=SlowSequence())
    store.upload(csv([f"0,0.0,seed"] * ROWS))
    start = threading.Barrier(uploaders)

    errors = []

    def go(i):
        start.wait()
        try:
            store.upload(csv([f"{i},{float(i)},t{i}"] * ROWS))
        except OSError as e:  # unguarded commits can also trip over each other's temp file
            errors.append(e)

    threads = [threading.Thread(target=go, args=(i,)) for i in range(1, uploaders + 1)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return store, errors


def test_concurrent_uploads_all_land_exactly_once(tmp_path):
    n = 8
    store, errors = hammer(tmp_path / "t", threading.Lock(), n)
    assert not errors
    entries, _ = store.snapshot()
    assert sorted(e.id for e in entries) == list(range(n + 1))
    expected_sum = ROWS * sum(range(1, n + 1))
    for fmt in ("csv", "parquet"):
        agg = store.aggregate("price", "sum", fmt)
        assert agg.value == expected_sum and agg.count == ROWS * (n + 1) and agg.uploads == n + 1
    files = {p.name for p in store.root.iterdir()}
    assert len([f for f in files if f.endswith(".parquet")]) == n + 1


def test_the_same_scenario_corrupts_the_table_without_the_lock(tmp_path):
    n = 8
    store, _ = hammer(tmp_path / "t", NoLock(), n)
    entries, _ = store.snapshot()
    ids = [e.id for e in entries]
    assert len(set(ids)) < len(ids), "without the lock, uploads were handed the same id"
    expected_sum = ROWS * sum(range(1, n + 1))
    assert store.aggregate("price", "sum", "parquet").value != expected_sum, "and overwrote each other's files"


def test_readers_only_ever_see_whole_uploads(tmp_path):
    store = TableStore(tmp_path / "t")
    store.upload(csv([f"0,0.0,seed"] * ROWS))
    stop = threading.Event()
    seen, errors = [], []

    def reader():
        while not stop.is_set():
            try:
                agg = store.aggregate("id", "count", "csv" if len(seen) % 2 else "parquet")
                seen.append(agg.count)
            except Exception as e:  # noqa: BLE001 - any failure here is the finding
                errors.append(e)

    r = threading.Thread(target=reader)
    r.start()
    writers = [
        threading.Thread(target=lambda i=i: [store.upload(csv([f"{i},1.0,x"] * ROWS)) for _ in range(5)])
        for i in range(4)
    ]
    [w.start() for w in writers]
    [w.join() for w in writers]
    stop.set()
    r.join()

    assert not errors, errors[:1]
    assert seen and all(c % ROWS == 0 for c in seen)
    assert seen == sorted(seen), "a committed upload never disappears"
    assert store.info()[2] == ROWS * 21


# --- gRPC ------------------------------------------------------------------


@pytest.fixture()
def stub(tmp_path):
    server, port = build_server(TableStore(tmp_path / "t"), 0, workers=4)
    server.start()
    with grpc.insecure_channel(f"127.0.0.1:{port}") as ch:
        yield pb_grpc.TableStub(ch)
    server.stop(0)


def test_grpc_round_trip(stub):
    r = stub.Upload(pb.UploadRequest(csv=csv(["1,2.5,a", "2,3.5,b"])))
    assert (r.upload_id, r.rows, r.total_rows) == (0, 2, 2)
    stub.Upload(pb.UploadRequest(csv=csv(["3,4.0,c"])))
    for fmt in (pb.CSV, pb.PARQUET):
        a = stub.Aggregate(pb.AggregateRequest(column="price", op=pb.SUM, format=fmt))
        assert (a.value, a.count, a.uploads) == (10.0, 3, 2)
    info = stub.Info(pb.InfoRequest())
    assert list(info.columns) == ["id", "price", "city"] and info.total_rows == 3


def test_grpc_errors_are_invalid_argument(stub):
    stub.Upload(pb.UploadRequest(csv=csv(["1,2.5,a"])))
    with pytest.raises(grpc.RpcError) as e:
        stub.Upload(pb.UploadRequest(csv=csv(["1,2.5,a"], header="a,b,c")))
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as e:
        stub.Aggregate(pb.AggregateRequest(column="price"))  # op and format unset
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as e:
        stub.Aggregate(pb.AggregateRequest(column="zzz", op=pb.SUM, format=pb.CSV))
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
