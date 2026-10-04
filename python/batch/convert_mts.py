# /// script
# requires-python = ">=3.13"
# dependencies = ["hail==0.2.139"]
# ///
"""Convert the ten-sample production MatrixTables into multi-sample input tables.

Each `gs://.../mts_prod_500/NN.mt` becomes input tables in one of two layouts:
- `multi-file` (issue #245): the table `tNNN` in six datasets, one per format, locus
  representation and Vortex compression, each table a directory of per-contig files.
- `single-file` (issue #253): the same six forms as one file per table, suffixed `_1f`, at ten
  samples per table (`tNNN`) and at five (`t{2·NN}` from the first five columns, `t{2·NN+1}` from
  the last five). It also writes each of those tables as a MatrixTable, with the source's
  all-missing rows filtered out, for Hail's side of the comparison.

Run from the host with `uv run python/batch/convert_mts.py submit --layout LAYOUT`, which submits
one Batch job per `--per-job` MatrixTables. Each job writes this file into its container and runs
its `convert` subcommand, which builds every form on local disk and uploads it with `hailtop.fs`.
`summary` adds up the per-MatrixTable reports the jobs write.

The conversion logic is copied from `vds_to_reference` and `ht_to_reference` in
`hailtools/cli.py`, not imported, so that a job needs nothing but this file.
"""

import argparse
import base64
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
import json
from pathlib import Path
import shutil
import subprocess
import time

import hailtop.fs as hfs

BUCKET = "gs://hail-pschultz/combiner-bench"
SOURCE = f"{BUCKET}/datasets/mts_prod_500"
DATASETS = f"{BUCKET}/datasets"

MTS = [f"{index:02d}" for index in range(50)]
CONTIGS = range(1, 9)
# Columns per source MatrixTable, which the single-file layout splits in half by position.
SOURCE_SAMPLES = 10

IMAGE = "hailgenetics/hail:0.2.139-py3.13"
REGION = "us-central1"
# Pinned exactly: `vortex-data` to the Rust reader's Vortex release, since a newer writer can use
# encodings the reader doesn't know, and `pyarrow` to `uv.lock`'s. A dataset must not mix writer
# versions, because the writers' choices are part of what the benchmark measures.
PIP_PACKAGES = ["vortex-data==0.86.1", "pyarrow==25.0.1"]
# Chosen for the first job, which times one MatrixTable.
JOB_CPU = 8
JOB_MEMORY = "standard"
JOB_STORAGE = "50Gi"
# Spark runs in local mode, so its driver holds everything; leave room for pyarrow.
SPARK_DRIVER_MEMORY = f"{JOB_CPU * 2}g"

WORK = Path("/io/convert_mts")
# DataFusion's default `max_row_group_size`.
ROW_GROUP_SIZE = 1024 * 1024


@dataclass(frozen=True)
class Form:
    """One dataset's form of an input table."""

    dataset: str
    packed: bool
    # `None` for Parquet, else the `vx convert` strategy.
    vortex_strategy: str | None = None
    # One file per table, else a directory of per-contig files.
    single_file: bool = False

    @property
    def extension(self) -> str:
        return "parquet" if self.vortex_strategy is None else "vortex"

    def data(self, root: str, table: str) -> str:
        stem = f"{root}/{self.dataset}/{table}"
        return f"{stem}.{self.extension}" if self.single_file else stem

    def annotation(self, root: str, table: str) -> str:
        return f"{root}/{self.dataset}/{table}.samples.{self.extension}"


def forms(samples: int, single_file: bool) -> list[Form]:
    """The six forms at `samples` per table, Parquet first, since the Vortex forms convert them."""

    def form(format: str, packed: bool, strategy: str | None = None) -> Form:
        name = "_".join(
            [
                f"{format}_prod_{samples}s",
                *(["packed"] if packed else []),
                *(["compact"] if strategy == "compact" else []),
                *(["1f"] if single_file else []),
            ]
        )
        return Form(name, packed, strategy, single_file)

    return [
        form("parquets", packed=False),
        form("parquets", packed=True),
        form("vortices", packed=False, strategy="default"),
        form("vortices", packed=True, strategy="default"),
        form("vortices", packed=False, strategy="compact"),
        form("vortices", packed=True, strategy="compact"),
    ]


@dataclass(frozen=True)
class Output:
    """An input table written in each of `forms` from the source columns at `columns`."""

    table: str
    columns: range
    forms: tuple[Form, ...]
    # Whether each Parquet form must hold exactly the rows of its multi-file form.
    check_counterpart: bool = False


@dataclass(frozen=True)
class MatrixTableOutput:
    """A MatrixTable of the source columns at `columns`, without the rows that have no defined
    entry there."""

    path: str
    columns: range


@dataclass(frozen=True)
class Layout:
    # Outside `datasets/`, because a dataset root may hold only input tables.
    reports: str
    outputs: list[Output]
    matrix_tables: list[MatrixTableOutput]


def layout(name: str, mt: str) -> Layout:
    """What `NN.mt` becomes in the layout `name`."""
    index = int(mt)
    every = range(SOURCE_SAMPLES)
    halves = [range(0, SOURCE_SAMPLES // 2), range(SOURCE_SAMPLES // 2, SOURCE_SAMPLES)]
    if name == "multi-file":
        return Layout(
            reports=f"{BUCKET}/convert_mts",
            outputs=[Output(table_name(index), every, tuple(forms(10, single_file=False)))],
            matrix_tables=[],
        )
    tables = [(10, table_name(index), every)] + [
        (5, table_name(2 * index + half), columns) for half, columns in enumerate(halves)
    ]
    return Layout(
        reports=f"{BUCKET}/convert_mts_1f",
        outputs=[
            Output(
                table,
                columns,
                tuple(forms(samples, single_file=True)),
                check_counterpart=samples == 10,
            )
            for samples, table, columns in tables
        ],
        # The first takes every column, so the others can be read from it.
        matrix_tables=[
            MatrixTableOutput(f"{DATASETS}/mts_prod_{samples}s/{table}.mt", columns)
            for samples, table, columns in tables
        ],
    )


LAYOUTS = ["multi-file", "single-file"]


def table_name(index: int) -> str:
    return f"t{index:03d}"


def contig_name(ordinal: int) -> str:
    """Name a contig by its ordinal padded to two digits, as Rust's `Locus::contig_name` does."""
    return f"chr{ordinal:02d}"


def report_path(reports: str, mt: str) -> str:
    return f"{reports}/{table_name(int(mt))}.json"


# Submit, on the host.


def job_commands(layout_name: str, mts: list[str]) -> list[str]:
    source = base64.b64encode(Path(__file__).read_bytes()).decode()
    return [
        "set -euo pipefail",
        f"echo {source} | base64 -d > convert_mts.py",
        f"python3 -m pip install --quiet {' '.join(PIP_PACKAGES)}",
        f"python3 convert_mts.py convert --layout {layout_name} --mts {' '.join(mts)}",
    ]


def submit(layout_name: str, mts: list[str], per_job: int, dry_run: bool) -> None:
    chunks = [mts[start : start + per_job] for start in range(0, len(mts), per_job)]
    if dry_run:
        for chunk in chunks:
            print(f"job convert {' '.join(chunk)}: {JOB_CPU} cpu, {JOB_MEMORY}, {JOB_STORAGE}")
            for command in job_commands(layout_name, chunk)[2:]:
                print(f"  {command}")
        return

    import hailtop.batch as hb

    # The billing project and remote_tmpdir come from `hailctl config`.
    batch = hb.Batch(
        name=f"convert-mts-{layout_name}",
        backend=hb.ServiceBackend(),
        default_image=IMAGE,
        default_regions=[REGION],
    )
    for chunk in chunks:
        job = batch.new_bash_job(name=f"convert {' '.join(chunk)}")
        job.cpu(JOB_CPU)
        job.memory(JOB_MEMORY)
        job.storage(JOB_STORAGE)
        for command in job_commands(layout_name, chunk):
            job.command(command)
    submitted = batch.run(wait=False)
    print(f"submitted batch {submitted.id}; follow it with `hailctl batch wait {submitted.id}`")


# Convert, inside a job.


class Timings:
    """Seconds spent in each phase, summed over the phase's spans."""

    def __init__(self) -> None:
        self.seconds: dict[str, float] = {}

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        start = time.monotonic()
        try:
            yield
        finally:
            self.seconds[name] = self.seconds.get(name, 0.0) + time.monotonic() - start


def pending_forms(output: Output) -> list[Form]:
    """The forms of `output` still to upload. A form with its annotation table is complete and
    never touched; one with data but no annotation table is deleted, to be redone."""
    pending = []
    for form in output.forms:
        if hfs.exists(form.annotation(DATASETS, output.table)):
            continue
        data = form.data(DATASETS, output.table)
        if form.single_file and hfs.exists(data):
            print(f"{data}: no annotation table, deleting")
            hfs.remove(data)
        elif not form.single_file and hfs.is_dir(data):
            print(f"{data}: no annotation table, deleting")
            hfs.rmtree(data)
        pending.append(form)
    return pending


def complete(matrix_table: MatrixTableOutput) -> bool:
    return hfs.exists(f"{matrix_table.path}/_SUCCESS")


def convert(layout_name: str, mts: list[str]) -> None:
    import hail as hl

    WORK.mkdir(parents=True, exist_ok=True)
    hl.init(
        backend="spark",
        master=f"local[{JOB_CPU}]",
        tmp_dir=f"file://{WORK}/hail-tmp",
        local_tmpdir=f"file://{WORK}/hail-tmp",
        spark_conf={"spark.driver.memory": SPARK_DRIVER_MEMORY},
        quiet=True,
    )
    for mt in mts:
        plan = layout(layout_name, mt)
        report = report_path(plan.reports, mt)
        pending = {output: pending_forms(output) for output in plan.outputs}
        done = not any(pending.values()) and all(map(complete, plan.matrix_tables))
        if done and hfs.exists(report):
            print(f"{mt}.mt: complete, skipping")
            continue
        result = convert_mt(mt, plan, pending)
        with hfs.open(report, "w") as file:
            json.dump(result, file, indent=2)
        print(json.dumps(result, indent=2))


def convert_mt(mt: str, plan: Layout, pending: dict[Output, list[Form]]) -> dict:
    """Builds every form of every output of `NN.mt` on local disk, uploads the pending ones, and
    writes the MatrixTables not yet complete."""
    import hail as hl
    import pyarrow as pa

    timings = Timings()
    work = WORK / mt
    shutil.rmtree(work, ignore_errors=True)
    entries_dir = work / "entries"

    with timings.phase("read"):
        source = hl.read_matrix_table(f"{SOURCE}/{mt}.mt")
        samples = source.s.collect()
        source = source.select_globals()
        source = source.select_entries("DP", "GQ", "LEN", ploidy=source.LGT.ploidy)
        # One row per defined entry, sorted by (locus, s): the column key is `s`.
        df = (
            source.entries()
            .to_spark()
            .withColumnRenamed("locus.contig", "contig")
            .withColumnRenamed("locus.position", "position")
        )
        df.write.parquet(str(entries_dir), compression="zstd")
    if len(set(samples)) != len(samples):
        raise AssertionError(f"{mt}.mt has duplicate samples")
    if len(samples) != SOURCE_SAMPLES:
        raise AssertionError(f"{mt}.mt has {len(samples)} samples, expected {SOURCE_SAMPLES}")

    plain_schema, packed_schema, annotation_schema = schemas()
    with timings.phase("split and check"):
        contig_tables = split_entries(entries_dir, plain_schema, packed_schema)
    shutil.rmtree(entries_dir)

    tables = []
    for output in plan.outputs:
        output_samples = [samples[column] for column in output.columns]
        annotation = pa.table({"s": sorted(output_samples)}, schema=annotation_schema)
        output_tables = select_samples(contig_tables, output_samples)
        rows = {name: plain.num_rows for name, (plain, _) in output_tables.items()}
        parquet_forms = [form for form in output.forms if form.vortex_strategy is None]
        vortex_forms = [form for form in output.forms if form.vortex_strategy is not None]

        with timings.phase("parquet"):
            for form in parquet_forms:
                write_parquet_form(form, output.table, work, output_tables, annotation)
        del output_tables
        if output.check_counterpart:
            with timings.phase("compare"):
                for form in parquet_forms:
                    check_counterpart(form, output.table, work)
        with timings.phase("vortex"):
            for form in vortex_forms:
                [source_form] = [f for f in parquet_forms if f.packed == form.packed]
                write_vortex_form(form, source_form, output.table, work)
        with timings.phase("upload"):
            for form in pending[output]:
                upload_form(form, output.table, work)
        tables.append(
            {
                "table": output.table,
                "datasets": [form.dataset for form in output.forms],
                "rows": rows,
                "total_rows": sum(rows.values()),
                "samples": sorted(output_samples),
                "uploaded": [form.dataset for form in pending[output]],
            }
        )
        # A job may convert several MatrixTables, so free this output's disk before the next.
        shutil.rmtree(work / "forms")

    matrix_tables = []
    if plan.matrix_tables:
        with timings.phase("matrix tables"):
            matrix_tables = write_matrix_tables(mt, plan, samples, tables)
    shutil.rmtree(work)

    return {
        "source": f"{SOURCE}/{mt}.mt",
        "tables": tables,
        "matrix_tables": matrix_tables,
        "seconds": timings.seconds,
        "versions": versions(),
    }


def schemas():
    """The plain, packed and sample annotation schemas, with `s` last as the combiner writes it."""
    import pyarrow as pa

    sample = pa.field("s", pa.string(), nullable=False)
    values = [pa.field(name, pa.int32()) for name in ("DP", "GQ", "LEN", "ploidy")]
    plain = pa.schema(
        [
            pa.field("contig", pa.string(), nullable=False),
            pa.field("position", pa.int32(), nullable=False),
            *values,
            sample,
        ]
    )
    packed = pa.schema([pa.field("locus", pa.int64(), nullable=False), *values, sample])
    return plain, packed, pa.schema([sample])


def split_entries(entries_dir: Path, plain_schema, packed_schema):
    """Reads Spark's part files in partition order and splits them into one table per contig,
    in both representations, in contig order."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    parts = sorted(entries_dir.glob("part-*.parquet"))
    entries = pa.concat_tables(pq.read_table(part) for part in parts)

    for name in ("contig", "position", "s", "LEN"):
        if entries[name].null_count:
            raise AssertionError(f"{entries[name].null_count} rows with a missing {name}")
    expected = {f"chr{ordinal}" for ordinal in CONTIGS}
    contigs = set(pc.unique(entries["contig"]).to_pylist())
    if contigs != expected:
        raise AssertionError(f"source contigs {sorted(contigs)}, expected {sorted(expected)}")

    contig_tables = {}
    for ordinal in CONTIGS:
        name = contig_name(ordinal)
        contig = entries.filter(pc.equal(entries["contig"], f"chr{ordinal}"))
        if contig.num_rows == 0:
            raise AssertionError(f"{name} is empty")
        position = contig["position"].combine_chunks()
        values = [contig[field].combine_chunks() for field in ("DP", "GQ", "LEN", "ploidy")]
        sample = contig["s"].combine_chunks()
        plain = pa.Table.from_arrays(
            [pa.repeat(pa.scalar(name), contig.num_rows), position, *values, sample],
            schema=plain_schema,
        )
        locus = pc.bit_wise_or(
            pc.cast(position, pa.int64()), pa.scalar(ordinal << 32, type=pa.int64())
        )
        packed = pa.Table.from_arrays([locus, *values, sample], schema=packed_schema)
        contig_tables[name] = (plain, packed)
    return contig_tables


def select_samples(contig_tables, samples: list[str]):
    """The contig tables with only the rows of `samples`, which keeps their order."""
    if len(samples) == SOURCE_SAMPLES:
        return contig_tables
    return {
        name: tuple(table.filter(sample_mask(table, samples)) for table in pair)
        for name, pair in contig_tables.items()
    }


def sample_mask(table, samples: list[str]):
    import pyarrow as pa
    import pyarrow.compute as pc

    return pc.is_in(table["s"], value_set=pa.array(samples))


def check_strictly_increasing(table, keys: list[str], where: str) -> None:
    """Asserts that `(*keys, s)` strictly increases down `table`, comparing strings bytewise as
    Arrow does. Strict order also proves that no `(*keys, s)` repeats."""
    import pyarrow.compute as pc

    columns = [table[key].combine_chunks() for key in [*keys, "s"]]
    n = table.num_rows - 1
    increasing = tied = None
    for column in columns:
        left, right = column.slice(0, n), column.slice(1)
        less, equal = pc.less(left, right), pc.equal(left, right)
        if tied is None:
            increasing, tied = less, equal
        else:
            increasing = pc.or_(increasing, pc.and_(tied, less))
            tied = pc.and_(tied, equal)
    if not pc.all(increasing).as_py():
        index = pc.index(increasing, False).as_py()
        row = tuple(column[index + 1].as_py() for column in columns)
        previous = tuple(column[index].as_py() for column in columns)
        raise AssertionError(f"{where}: row {index + 1} {row} does not follow {previous}")


def write_parquet_form(form: Form, table: str, work: Path, contig_tables, annotation) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = str(work / "forms")
    keys = ["locus"] if form.packed else ["contig", "position"]
    contigs = {
        name: packed if form.packed else plain for name, (plain, packed) in contig_tables.items()
    }
    data = Path(form.data(root, table))
    if form.single_file:
        data.parent.mkdir(parents=True, exist_ok=True)
        # Rows run in `(locus, s)` order across every contig.
        whole = pa.concat_tables(contigs.values())
        check_strictly_increasing(whole, keys, str(data))
        pq.write_table(whole, data, compression="zstd", row_group_size=ROW_GROUP_SIZE)
    else:
        data.mkdir(parents=True)
        for name, contig in contigs.items():
            check_strictly_increasing(contig, keys, f"{data}/{name}")
            pq.write_table(
                contig, data / f"{name}.parquet", compression="zstd", row_group_size=ROW_GROUP_SIZE
            )
        written = sorted(path.stem for path in data.iterdir())
        expected = [contig_name(ordinal) for ordinal in CONTIGS]
        if written != expected:
            raise AssertionError(f"{data} holds {written}, expected {expected}")
    pq.write_table(annotation, form.annotation(root, table), compression="zstd")


def check_counterpart(form: Form, table: str, work: Path) -> None:
    """Asserts that the single-file Parquet form of `table` holds exactly the rows, and its
    annotation table the samples, of the multi-file form already in GCS."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    counterpart = replace(form, dataset=form.dataset.removesuffix("_1f"), single_file=False)
    local = work / "counterparts" / counterpart.dataset
    local.mkdir(parents=True)
    for entry in hfs.ls(counterpart.data(DATASETS, table)):
        hfs.copy(entry.path, str(local / Path(entry.path).name))
    annotation = local.with_suffix(".samples.parquet")
    hfs.copy(counterpart.annotation(DATASETS, table), str(annotation))

    root = str(work / "forms")
    expected = pa.concat_tables(pq.read_table(path) for path in sorted(local.iterdir()))
    if not pq.read_table(form.data(root, table)).equals(expected):
        raise AssertionError(f"{form.dataset}/{table} differs from {counterpart.dataset}/{table}")
    if not pq.read_table(form.annotation(root, table)).equals(pq.read_table(annotation)):
        raise AssertionError(f"{form.dataset}/{table} has other samples than {counterpart.dataset}")
    shutil.rmtree(work / "counterparts")


def write_vortex_form(form: Form, source: Form, table: str, work: Path) -> None:
    """Converts the matching Parquet form's files, checking that each holds the same rows and
    keeps its columns' names and nullability."""
    root = str(work / "forms")
    data = Path(form.data(root, table))
    if form.single_file:
        data.parent.mkdir(parents=True, exist_ok=True)
        pairs = [(Path(source.data(root, table)), data)]
    else:
        data.mkdir(parents=True)
        pairs = [
            (parquet, data / f"{parquet.stem}.vortex")
            for parquet in sorted(Path(source.data(root, table)).iterdir())
        ]
    pairs.append((Path(source.annotation(root, table)), Path(form.annotation(root, table))))
    strategy = [] if form.vortex_strategy == "default" else ["-s", form.vortex_strategy]
    for parquet, destination in pairs:
        subprocess.check_call(["vx", "convert", *strategy, str(parquet)])
        parquet.with_suffix(".vortex").replace(destination)
        check_same_rows(destination, parquet)


def check_same_rows(vortex_path: Path, parquet_path: Path) -> None:
    import pyarrow.parquet as pq
    import vortex

    expected = pq.read_table(parquet_path)
    file = vortex.open(str(vortex_path))
    names = [(f.name, f.nullable) for f in expected.schema]
    actual_names = [(f.name, f.nullable) for f in file.dtype.to_arrow_schema()]
    if actual_names != names:
        raise AssertionError(f"{vortex_path} has columns {actual_names}, expected {names}")
    # Vortex reads strings back as `string_view`.
    actual = file.scan().read_all().to_arrow_table()
    if actual.num_rows != expected.num_rows or not all(
        actual[f.name].cast(f.type).equals(expected[f.name]) for f in expected.schema
    ):
        raise AssertionError(f"{vortex_path} holds other rows than {parquet_path}")


def upload_form(form: Form, table: str, work: Path) -> None:
    """Uploads the data, then the annotation table last, so that a job that dies partway leaves a
    table discovery rejects rather than one that is silently short."""
    root = str(work / "forms")
    data = Path(form.data(root, table))
    if form.single_file:
        hfs.copy(str(data), form.data(DATASETS, table))
    else:
        for path in sorted(data.iterdir()):
            hfs.copy(str(path), f"{form.data(DATASETS, table)}/{path.name}")
    hfs.copy(form.annotation(root, table), form.annotation(DATASETS, table))


def write_matrix_tables(
    mt: str, plan: Layout, samples: list[str], tables: list[dict]
) -> list[dict]:
    """Writes each MatrixTable not yet complete straight to GCS, keeping the source's entry fields
    and globals, then checks each against its input table's rows."""
    import hail as hl

    first = plan.matrix_tables[0]
    if first.columns != range(len(samples)):
        raise AssertionError(f"{first.path} must take every column")
    results = []
    for matrix_table, output, table in zip(plan.matrix_tables, plan.outputs, tables, strict=True):
        columns = [samples[column] for column in matrix_table.columns]
        if output.columns != matrix_table.columns:
            raise AssertionError(f"{matrix_table.path} and {output.table} take other columns")
        was_complete = complete(matrix_table)
        if not was_complete:
            # The first is filtered from the source; the rest from the first, read back.
            base = f"{SOURCE}/{mt}.mt" if matrix_table is first else first.path
            subset = hl.read_matrix_table(base)
            if matrix_table is not first:
                subset = subset.filter_cols(hl.literal(set(columns)).contains(subset.s))
            subset = subset.filter_rows(hl.agg.any(hl.is_defined(subset.entry)))
            subset.write(matrix_table.path, overwrite=True)

        result = hl.read_matrix_table(matrix_table.path)
        if result.s.collect() != columns:
            raise AssertionError(f"{matrix_table.path} has other columns than {columns}")
        counted = result.annotate_rows(defined=hl.agg.count_where(hl.is_defined(result.entry)))
        rows, defined, empty_rows = counted.aggregate_rows(
            (hl.agg.count(), hl.agg.sum(counted.defined), hl.agg.count_where(counted.defined == 0))
        )
        if empty_rows:
            raise AssertionError(f"{matrix_table.path} has {empty_rows} rows with no defined entry")
        if defined != table["total_rows"]:
            raise AssertionError(
                f"{matrix_table.path} has {defined} defined entries, "
                f"but {output.table} has {table['total_rows']} rows"
            )
        results.append(
            {
                "path": matrix_table.path,
                "table": output.table,
                "rows": rows,
                "defined_entries": defined,
                "written": not was_complete,
            }
        )
    return results


def versions() -> dict[str, str]:
    from importlib.metadata import version

    import hail as hl

    return {
        "vortex-data": version("vortex-data"),
        "pyarrow": version("pyarrow"),
        "hail": hl.version(),
    }


# Summary, on the host.


def summary(layout_name: str) -> None:
    reports_dir = layout(layout_name, MTS[0]).reports
    reports = []
    for entry in sorted(hfs.ls(reports_dir), key=lambda entry: entry.path):
        if entry.path.endswith(".json"):
            with hfs.open(entry.path) as file:
                reports.append(json.load(file))
    if not reports:
        print(f"no reports in {reports_dir}")
        return

    # Reports of #245's first run hold their one table at the top level.
    tables = [table for report in reports for table in report.get("tables", [report])]
    seconds: dict[str, float] = {}
    for report in reports:
        for phase, spent in report["seconds"].items():
            seconds[phase] = seconds.get(phase, 0.0) + spent
        phases = ", ".join(f"{phase} {spent:.0f}s" for phase, spent in report["seconds"].items())
        print(f"{report['source'].rsplit('/', 1)[-1]}: {phases}")

    for per_table in sorted({len(table["samples"]) for table in tables}, reverse=True):
        group = [table for table in tables if len(table["samples"]) == per_table]
        rows: dict[str, int] = {}
        for table in group:
            for contig, count in table["rows"].items():
                rows[contig] = rows.get(contig, 0) + count
        total = sum(rows.values())
        samples = [sample for table in group for sample in table["samples"]]
        print(
            f"\n{per_table} samples per table: {len(group)} tables, "
            f"{len(set(samples))} distinct samples, {total:,} rows"
        )
        if len(set(samples)) != len(samples):
            print("warning: a sample is in more than one table")
        for contig in sorted(rows):
            print(f"  {contig}: {rows[contig]:,}")
        print(f"{total / 1.0e9:.3f} of the estimate of 1.0B rows")

    matrix_tables = [mt for report in reports for mt in report.get("matrix_tables", [])]
    for directory in sorted({mt["path"].rsplit("/", 1)[0] for mt in matrix_tables}):
        group = [mt for mt in matrix_tables if mt["path"].startswith(f"{directory}/")]
        rows = sum(mt["rows"] for mt in group)
        defined = sum(mt["defined_entries"] for mt in group)
        print(f"\n{directory}: {len(group)} MatrixTables, {rows:,} rows")
        print(f"{defined:,} defined entries")

    print()
    for phase, spent in seconds.items():
        print(f"  {phase}: {spent:.0f}s total, {spent / len(reports):.0f}s per MatrixTable")
    for package in reports[0]["versions"]:
        found = sorted({report["versions"][package] for report in reports})
        mixed = "  MIXED" if len(found) > 1 else ""
        print(f"{package}: {', '.join(found)}{mixed}")


def mt_index(value: str) -> str:
    if value not in MTS:
        raise argparse.ArgumentTypeError(f"not a MatrixTable index 00-49: {value}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    submit_parser = commands.add_parser("submit", help="Submit a Batch of conversion jobs.")
    submit_parser.add_argument("--mts", nargs="+", type=mt_index, default=MTS)
    submit_parser.add_argument("--per-job", type=int, default=1)
    submit_parser.add_argument("--dry-run", action="store_true")
    convert_parser = commands.add_parser("convert", help="Convert MatrixTables, inside a job.")
    convert_parser.add_argument("--mts", nargs="+", type=mt_index, required=True)
    summary_parser = commands.add_parser("summary", help="Add up the jobs' reports.")
    for subparser in (submit_parser, convert_parser, summary_parser):
        subparser.add_argument("--layout", choices=LAYOUTS, required=True)
    args = parser.parse_args()

    if args.command == "submit":
        submit(args.layout, args.mts, args.per_job, args.dry_run)
    elif args.command == "convert":
        convert(args.layout, args.mts)
    else:
        summary(args.layout)


if __name__ == "__main__":
    main()
