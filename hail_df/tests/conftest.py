"""Dataset fixtures written under pytest's `tmp_path`, laid out as ADR 0018 describes.

The mixed dataset holds three input tables: the multi-sample file `g0` of two samples' rows,
beside its sample annotation table, and the single-sample tables `s=HG02230/` and `s=NA18534/`,
each of four files. Every sample holds the same eight rows, in locus-then-alleles order.

They are on disk, which is unavoidable: the suites test `hail_df` through its production backend,
whose object-store resolver serves local paths and `gs://` URLs only, and the wheel offers no
in-memory store the backend could reach.
"""

from functools import partial
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import vortex as vx
from hail_df import Backend, formulations

FORMATS = ["parquet", "vortex"]

# Every formulation with a built-in plan, at group counts below, equal to and above the mixed
# dataset's three input tables, as #321's matrix has them.
FORMULATIONS = {
    "union": formulations.union,
    **{
        f"grouped-merge-{groups}": partial(formulations.grouped_merge, groups=groups)
        for groups in [1, 2, 3, 8]
    },
}

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


def write_single_sample_dataset(root: Path, input_format: str, samples: list[str]) -> Path:
    """Writes a dataset of one single-sample input table per sample, `s=<sample>/`, each of one file
    of the eight rows, under `root/samples`, and returns that path."""
    dataset = root / "samples"
    for sample in samples:
        write_table(rows_table(ROWS), dataset / f"s={sample}" / f"a.{input_format}", input_format)
    return dataset


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


def plan_sections(text: str) -> dict[str, str]:
    """The sections of an Outcome's plan text by name, each line without the table's padding, which
    the widest line of any section sets."""
    lines = text.splitlines()
    name_width = lines[0].index("+", 1)
    sections: dict[str, list[str]] = {}
    current: list[str] = []
    for line in lines[3:-1]:
        name, plan = line[1:name_width].strip(), line[name_width + 2 : -1].rstrip()
        if name:
            current = sections.setdefault(name, [])
        current.append(plan)
    return {name: "\n".join(plan) for name, plan in sections.items()}


def write_large_dataset(root: Path, rows_per_table: int) -> Path:
    """Writes a Parquet dataset of two input tables of `rows_per_table` rows each under `root`, for
    a test that needs a run to last, and returns its path."""
    rows = pa.table(
        [
            pa.repeat("chr01", rows_per_table),
            pa.array(range(1, rows_per_table + 1), pa.int32()),
            pa.repeat("A,G", rows_per_table),
        ],
        schema=pa.schema(ROW_FIELDS),
    )
    write_table(rows, root / "s=HG02230" / "a.parquet", "parquet")
    write_table(rows, root / "s=NA18534" / "a.parquet", "parquet")
    return root


@pytest.fixture(params=FORMATS)
def input_format(request) -> str:
    return request.param


@pytest.fixture(params=FORMULATIONS)
def formulation(request):
    return FORMULATIONS[request.param]


@pytest.fixture
def mixed_dataset(tmp_path, input_format) -> Path:
    return write_mixed_dataset(tmp_path, input_format)


@pytest.fixture(scope="module")
def backend() -> Backend:
    return Backend()


@pytest.fixture
def dataset(backend, mixed_dataset, input_format):
    return backend.open_dataset(str(mixed_dataset), input_format, "locus,alleles")
