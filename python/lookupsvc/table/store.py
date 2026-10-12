"""A table that grows by CSV upload.

Every upload is kept twice, as the CSV that arrived and as Parquet, and an
aggregate can be answered from either copy. Uploads are numbered, become
visible all at once when they commit, and the first upload fixes the schema.

Locking: one lock guards the schema, the id counter and the list of committed
uploads. Parsing, file writes and column scans happen outside it, so a long scan
never blocks an upload and an upload never blocks a query for more than a list
copy.
"""
import json
import os
import threading
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

OPS = ("sum", "mean", "count")
FORMATS = ("csv", "parquet")


class InvalidUpload(ValueError):
    pass


class InvalidQuery(ValueError):
    pass


class Sequence:
    """Upload id source. Only ever called with the store lock held."""

    def __init__(self, start: int = 0):
        self.value = start

    def next(self) -> int:
        v = self.value
        self.value = v + 1
        return v


@dataclass(frozen=True)
class Entry:
    id: int
    rows: int
    csv: str
    parquet: str


@dataclass(frozen=True)
class Aggregate:
    value: float
    count: int
    uploads: int


class TableStore:
    def __init__(self, root, lock=None, sequence: Sequence | None = None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = lock if lock is not None else threading.Lock()
        self._seq = sequence if sequence is not None else Sequence()
        self._entries: list[Entry] = []
        self._schema: pa.Schema | None = None
        self._load()

    # --- persistence -------------------------------------------------------

    @property
    def _manifest(self) -> Path:
        return self.root / "manifest.json"

    def _load(self) -> None:
        if not self._manifest.exists():
            return
        data = json.loads(self._manifest.read_text())
        self._entries = [Entry(**e) for e in data["uploads"]]
        if self._entries:
            self._schema = pq.read_schema(self.root / self._entries[0].parquet)
            self._seq.value = max(e.id for e in self._entries) + 1

    def _save_manifest(self) -> None:
        # called with the lock held, so two commits never share the temp file
        tmp = self._manifest.with_suffix(".tmp")
        tmp.write_text(json.dumps({"uploads": [e.__dict__ for e in self._entries]}))
        os.replace(tmp, self._manifest)

    # --- uploads -----------------------------------------------------------

    @staticmethod
    def _conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
        if table.column_names != schema.names:
            raise InvalidUpload(f"columns {table.column_names} do not match the table's {schema.names}")
        try:
            return table.cast(schema)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
            raise InvalidUpload(f"values do not fit the table's column types: {e}") from e

    def upload(self, data: bytes) -> Entry:
        try:
            table = pacsv.read_csv(BytesIO(data))
        except pa.ArrowInvalid as e:
            raise InvalidUpload(f"not a readable CSV: {e}") from e
        if table.num_columns == 0:
            raise InvalidUpload("no columns")

        while True:
            with self._lock:
                schema = self._schema
            candidate = self._conform(table, schema) if schema is not None else table
            with self._lock:
                if self._schema is schema:  # nobody fixed or changed the schema meanwhile
                    if schema is None:
                        self._schema = candidate.schema
                    upload_id = self._seq.next()
                    break

        entry = Entry(
            id=upload_id,
            rows=candidate.num_rows,
            csv=f"upload-{upload_id:06d}.csv",
            parquet=f"upload-{upload_id:06d}.parquet",
        )
        (self.root / entry.csv).write_bytes(data)
        pq.write_table(candidate, self.root / entry.parquet)

        with self._lock:
            self._entries.append(entry)
            self._save_manifest()
        return entry

    # --- queries -----------------------------------------------------------

    def snapshot(self) -> tuple[list[Entry], pa.Schema | None]:
        with self._lock:
            return list(self._entries), self._schema

    def info(self) -> tuple[list[str], int, int]:
        entries, schema = self.snapshot()
        return (schema.names if schema else []), len(entries), sum(e.rows for e in entries)

    def _column(self, entry: Entry, column: str, fmt: str, schema: pa.Schema) -> pa.ChunkedArray:
        if fmt == "parquet":
            return pq.read_table(self.root / entry.parquet, columns=[column]).column(column)
        opts = pacsv.ConvertOptions(
            include_columns=[column], column_types={column: schema.field(column).type}
        )
        return pacsv.read_csv(self.root / entry.csv, convert_options=opts).column(column)

    def aggregate(self, column: str, op: str, fmt: str) -> Aggregate:
        if op not in OPS:
            raise InvalidQuery(f"op must be one of {OPS}")
        if fmt not in FORMATS:
            raise InvalidQuery(f"format must be one of {FORMATS}")
        entries, schema = self.snapshot()
        if schema is None:
            raise InvalidQuery("nothing has been uploaded yet")
        if column not in schema.names:
            raise InvalidQuery(f"no column {column!r}; have {schema.names}")
        numeric = pa.types.is_integer(schema.field(column).type) or pa.types.is_floating(
            schema.field(column).type
        )
        if op != "count" and not numeric:
            raise InvalidQuery(f"{op} needs a numeric column, {column!r} is {schema.field(column).type}")

        total, count = 0, 0
        for entry in entries:
            col = self._column(entry, column, fmt, schema)
            count += pc.count(col).as_py()
            if op != "count":
                total += pc.sum(col).as_py() or 0
        if op == "count":
            return Aggregate(float(count), count, len(entries))
        if op == "sum":
            return Aggregate(float(total), count, len(entries))
        if count == 0:
            raise InvalidQuery("no non-null values to average")
        return Aggregate(total / count, count, len(entries))
