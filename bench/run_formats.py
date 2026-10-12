"""(d) CSV against Parquet: loading the dataset, and accumulating uploads.

Two parts, all in one process on the local disk (no network), file cache warm:
  load       read the whole dataset, or one column, from the gzipped CSV and
             from the Parquet copy
  uploads    split the dataset into --parts CSV uploads and send them to a fresh
             TableStore (each upload is parsed and stored as CSV and Parquet),
             then answer aggregates from the CSV copies and from the Parquet copies

    python bench/run_formats.py --parts 8 --reps 5
"""
import argparse
import io
import math
import sys
import tempfile
import time
from pathlib import Path

import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from common import DATASET, PARQUET, write_csv, write_meta
from lookupsvc.table.store import TableStore


def timed(fn):
    start = time.perf_counter()
    out = fn()
    return time.perf_counter() - start, out


def split_uploads(table, parts):
    n = table.num_rows
    step = -(-n // parts)
    chunks = []
    for lo in range(0, n, step):
        buf = io.BytesIO()
        pacsv.write_csv(table.slice(lo, step), buf)
        chunks.append(buf.getvalue())
    return chunks


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--parts", type=int, default=8)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--out", default="formats")
    args = ap.parse_args(argv)

    rows = []

    def add(task, fmt, rep, seconds, size=""):
        rows.append({"task": task, "format": fmt, "rep": rep, "seconds": round(seconds, 5), "bytes": size})
        print(f"{task:<18}{fmt:<8}rep={rep} {seconds:.4f}s", flush=True)

    csv_bytes, parquet_bytes = DATASET.stat().st_size, PARQUET.stat().st_size
    for rep in range(args.reps):
        s, table = timed(lambda: pacsv.read_csv(DATASET))
        add("load_all_columns", "csv", rep, s, csv_bytes)
        s, ptable = timed(lambda: pq.read_table(PARQUET))
        add("load_all_columns", "parquet", rep, s, parquet_bytes)
        assert table.num_rows == ptable.num_rows
        opts = pacsv.ConvertOptions(include_columns=["assessed_total"])
        s, _ = timed(lambda: pacsv.read_csv(DATASET, convert_options=opts))
        add("load_one_column", "csv", rep, s, csv_bytes)
        s, _ = timed(lambda: pq.read_table(PARQUET, columns=["assessed_total"]))
        add("load_one_column", "parquet", rep, s, parquet_bytes)

    chunks = split_uploads(pq.read_table(PARQUET), args.parts)
    for rep in range(args.reps):
        with tempfile.TemporaryDirectory() as tmp:
            store = TableStore(Path(tmp) / "t")
            s, _ = timed(lambda: [store.upload(c) for c in chunks])
            add("upload_all_parts", "csv+parquet", rep, s, sum(len(c) for c in chunks))
            entries, _ = store.snapshot()
            csv_total = sum((store.root / e.csv).stat().st_size for e in entries)
            pq_total = sum((store.root / e.parquet).stat().st_size for e in entries)
            for op, col in (("sum", "assessed_total"), ("mean", "lot_size"), ("count", "street")):
                results = {}
                for fmt, size in (("csv", csv_total), ("parquet", pq_total)):
                    s, agg = timed(lambda: store.aggregate(col, op, fmt))
                    results[fmt] = agg
                    add(f"aggregate_{op}", fmt, rep, s, size)
                # chunking differs between the readers, so float sums can differ in the last bits
                assert math.isclose(results["csv"].value, results["parquet"].value, rel_tol=1e-9), (op, results)
                assert results["csv"].count == results["parquet"].count
    write_csv(args.out, rows)
    write_meta(args.out, sys.argv[0:1] + (argv if argv is not None else sys.argv[1:]),
               {"parameters": vars(args), "rows": ptable.num_rows, "csv_gz_bytes": csv_bytes,
                "parquet_bytes": parquet_bytes})
    return 0


if __name__ == "__main__":
    sys.exit(main())
