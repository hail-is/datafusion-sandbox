"""Dataset fixtures written under pytest's `tmp_path`, laid out as ADR 0018 describes.

The mixed dataset holds three input tables: the multi-sample file `g0` of two samples' rows,
beside its sample annotation table, and the single-sample tables `s=HG02230/` and `s=NA18534/`,
each of four files. Every sample holds the same eight rows, in locus-then-alleles order.

They are on disk, which is unavoidable: the suites test `hail_df` through its production backend,
whose object-store resolver serves local paths and `gs://` URLs only, and the wheel offers no
in-memory store the backend could reach.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import vortex as vx

FORMATS = ["parquet", "vortex"]

INPUT_TABLES = ["g0", "s=HG02230", "s=NA18534"]
SAMPLE_SETS = [("HG00308", "HG00592"), ("HG02230",), ("NA18534",)]

# File stem and its rows as contig, position, alleles. Stems sort in reverse locus order.
FILES = [
    ("d", [("chr01", 1, "A,G"), ("chr01", 2, "A,C")]),
    ("c", [("chr01", 2, "A,G"), ("chr01", 3, "A,C")]),
    ("b", [("chr01", 4, "A,G"), ("chr02", 1, "A,C")]),
    ("a", [("chr02", 2, "A,G"), ("chr02", 3, "A,C")]),
]
ROWS = [row for _, rows in FILES for row in rows]

ROW_FIELDS = [
    pa.field("contig", pa.string(), nullable=False),
    pa.field("position", pa.int32(), nullable=False),
    pa.field("alleles", pa.string(), nullable=False),
]
SAMPLE_FIELD = pa.field("s", pa.string(), nullable=False)


def write_table(table: pa.Table, path: Path, input_format: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if input_format == "parquet":
        pq.write_table(table, path)
    else:
        vx.io.write(table, str(path))


def rows_table(rows, samples=None) -> pa.Table:
    columns = [list(column) for column in zip(*rows, strict=True)]
    fields = list(ROW_FIELDS)
    if samples is not None:
        columns.append(samples)
        fields.append(SAMPLE_FIELD)
    return pa.table(columns, schema=pa.schema(fields))


def write_mixed_dataset(root: Path, input_format: str) -> Path:
    """Writes the mixed dataset under `root/samples` and returns that path."""
    dataset = root / "samples"
    for sample in ["HG02230", "NA18534"]:
        for stem, rows in FILES:
            write_table(
                rows_table(rows), dataset / f"s={sample}" / f"{stem}.{input_format}", input_format
            )
    g0_rows = [(row, sample) for row in ROWS for sample in SAMPLE_SETS[0]]
    write_table(
        rows_table([row for row, _ in g0_rows], [sample for _, sample in g0_rows]),
        dataset / f"g0.{input_format}",
        input_format,
    )
    write_table(
        pa.table([list(SAMPLE_SETS[0])], schema=pa.schema([SAMPLE_FIELD])),
        dataset / f"g0.samples.{input_format}",
        input_format,
    )
    return dataset


@pytest.fixture(params=FORMATS)
def input_format(request) -> str:
    return request.param


@pytest.fixture
def mixed_dataset(tmp_path, input_format) -> Path:
    return write_mixed_dataset(tmp_path, input_format)
