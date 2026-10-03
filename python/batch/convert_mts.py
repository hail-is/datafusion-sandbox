# /// script
# requires-python = ">=3.13"
# dependencies = ["hail==0.2.139"]
# ///
"""Convert the ten-sample production MatrixTables into multi-sample input tables.

`gs://.../mts_prod_500/NN.mt` becomes the input table `tNNN` in six datasets, one per format,
locus representation and Vortex compression. See issue #245. Run from the host with
`uv run python/batch/convert_mts.py submit`, which submits one Batch job per `--per-job`
MatrixTables. Each job writes this file into its container and runs its `convert` subcommand,
which builds every form on local disk and uploads it with `hailtop.fs`. `summary` adds up the
per-table reports the jobs write.

The conversion logic is copied from `vds_to_reference` and `ht_to_reference` in
`hailtools/cli.py`, not imported, so that a job needs nothing but this file.
"""

import argparse
import base64
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import subprocess
import time

import hailtop.fs as hfs

BUCKET = "gs://hail-pschultz/combiner-bench"
SOURCE = f"{BUCKET}/datasets/mts_prod_500"
DATASETS = f"{BUCKET}/datasets"
# Outside `datasets/`, because a dataset root may hold only input tables.
REPORTS = f"{BUCKET}/convert_mts"

MTS = [f"{index:02d}" for index in range(50)]
CONTIGS = range(1, 9)

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

    @property
    def extension(self) -> str:
        return "parquet" if self.vortex_strategy is None else "vortex"

    def data(self, root: str, table: str) -> str:
        return f"{root}/{self.dataset}/{table}"

    def annotation(self, root: str, table: str) -> str:
        return f"{root}/{self.dataset}/{table}.samples.{self.extension}"


PARQUET_FORMS = [
    Form("parquets_prod_10s", packed=False),
    Form("parquets_prod_10s_packed", packed=True),
]
VORTEX_FORMS = [
    Form("vortices_prod_10s", packed=False, vortex_strategy="default"),
    Form("vortices_prod_10s_packed", packed=True, vortex_strategy="default"),
    Form("vortices_prod_10s_compact", packed=False, vortex_strategy="compact"),
    Form("vortices_prod_10s_packed_compact", packed=True, vortex_strategy="compact"),
]
FORMS = PARQUET_FORMS + VORTEX_FORMS


def table_name(mt: str) -> str:
    return f"t{int(mt):03d}"


def contig_name(ordinal: int) -> str:
    """Name a contig by its ordinal padded to two digits, as Rust's `Locus::contig_name` does."""
    return f"chr{ordinal:02d}"


def report_path(table: str) -> str:
    return f"{REPORTS}/{table}.json"


# Submit, on the host.


def job_commands(mts: list[str]) -> list[str]:
    source = base64.b64encode(Path(__file__).read_bytes()).decode()
    return [
        "set -euo pipefail",
        f"echo {source} | base64 -d > convert_mts.py",
        f"python3 -m pip install --quiet {' '.join(PIP_PACKAGES)}",
        f"python3 convert_mts.py convert --mts {' '.join(mts)}",
    ]


def submit(mts: list[str], per_job: int, dry_run: bool) -> None:
    chunks = [mts[start : start + per_job] for start in range(0, len(mts), per_job)]
    if dry_run:
        for chunk in chunks:
            print(f"job convert {' '.join(chunk)}: {JOB_CPU} cpu, {JOB_MEMORY}, {JOB_STORAGE}")
            for command in job_commands(chunk)[2:]:
                print(f"  {command}")
        return

    import hailtop.batch as hb

    # The billing project and remote_tmpdir come from `hailctl config`.
    batch = hb.Batch(
        name="convert-mts",
        backend=hb.ServiceBackend(),
        default_image=IMAGE,
        default_regions=[REGION],
    )
    for chunk in chunks:
        job = batch.new_bash_job(name=f"convert {' '.join(chunk)}")
        job.cpu(JOB_CPU)
        job.memory(JOB_MEMORY)
        job.storage(JOB_STORAGE)
        for command in job_commands(chunk):
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


def pending_forms(table: str) -> list[Form]:
    """The forms of `table` still to upload. A form with its annotation table is complete and
    never touched; one with data but no annotation table is deleted, to be redone."""
    pending = []
    for form in FORMS:
        if hfs.exists(form.annotation(DATASETS, table)):
            continue
        data = form.data(DATASETS, table)
        if hfs.is_dir(data):
            print(f"{data}: no annotation table, deleting")
            hfs.rmtree(data)
        pending.append(form)
    return pending


def convert(mts: list[str]) -> None:
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
        table = table_name(mt)
        pending = pending_forms(table)
        if not pending and hfs.exists(report_path(table)):
            print(f"{table}: complete, skipping")
            continue
        report = convert_mt(mt, table, pending)
        with hfs.open(report_path(table), "w") as file:
            json.dump(report, file, indent=2)
        print(json.dumps(report, indent=2))


def convert_mt(mt: str, table: str, pending: list[Form]) -> dict:
    """Builds every form of `table` from `NN.mt` on local disk and uploads the pending ones."""
    import hail as hl
    import pyarrow as pa
    import vortex

    timings = Timings()
    work = WORK / table
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

    plain_schema, packed_schema, annotation_schema = schemas()
    with timings.phase("split and check"):
        rows, contig_tables = split_entries(entries_dir, plain_schema, packed_schema)
    if len(set(samples)) != len(samples):
        raise AssertionError(f"{mt}.mt has duplicate samples")
    annotation = pa.table({"s": sorted(samples)}, schema=annotation_schema)

    with timings.phase("parquet"):
        for form in PARQUET_FORMS:
            write_parquet_form(form, table, work, contig_tables, annotation, timings)

    with timings.phase("vortex"):
        for form in VORTEX_FORMS:
            write_vortex_form(form, table, work, vortex)

    with timings.phase("upload"):
        for form in pending:
            upload_form(form, table, work)
    # A job may convert several tables, so free this one's disk before the next.
    shutil.rmtree(work)

    return {
        "table": table,
        "source": f"{SOURCE}/{mt}.mt",
        "rows": rows,
        "total_rows": sum(rows.values()),
        "samples": sorted(samples),
        "uploaded": [form.dataset for form in pending],
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
    in both representations. Returns the row count per contig file and the tables."""
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

    rows = {}
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
        rows[name] = contig.num_rows
        contig_tables[name] = (plain, packed)
    return rows, contig_tables


def check_strictly_increasing(table, key: str, where: str) -> None:
    """Asserts that `(key, s)` strictly increases down `table`, comparing `s` bytewise as Arrow
    does. Strict order also proves that no `(key, s)` repeats."""
    import pyarrow.compute as pc

    keys = table[key].combine_chunks()
    samples = table["s"].combine_chunks()
    n = table.num_rows - 1
    left, right = keys.slice(0, n), keys.slice(1)
    increasing = pc.or_(
        pc.less(left, right),
        pc.and_(pc.equal(left, right), pc.less(samples.slice(0, n), samples.slice(1))),
    )
    if not pc.all(increasing).as_py():
        index = pc.index(increasing, False).as_py()
        raise AssertionError(
            f"{where}: row {index + 1} ({right[index]}, {samples[index + 1]}) does not follow "
            f"({left[index]}, {samples[index]})"
        )


def write_parquet_form(form: Form, table: str, work: Path, contig_tables, annotation, timings):
    import pyarrow.parquet as pq

    data = Path(form.data(str(work), table))
    data.mkdir(parents=True)
    for name, (plain, packed) in contig_tables.items():
        contig = packed if form.packed else plain
        check_strictly_increasing(contig, "locus" if form.packed else "position", f"{data}/{name}")
        pq.write_table(
            contig, data / f"{name}.parquet", compression="zstd", row_group_size=ROW_GROUP_SIZE
        )
    written = sorted(path.stem for path in data.iterdir())
    expected = [contig_name(ordinal) for ordinal in CONTIGS]
    if written != expected:
        raise AssertionError(f"{data} holds {written}, expected {expected}")
    pq.write_table(annotation, form.annotation(str(work), table), compression="zstd")


def write_vortex_form(form: Form, table: str, work: Path, vortex) -> None:
    """Converts the matching Parquet form's files, checking that each keeps its columns' names
    and nullability."""
    import pyarrow.parquet as pq

    [source] = [f for f in PARQUET_FORMS if f.packed == form.packed]
    data = Path(form.data(str(work), table))
    data.mkdir(parents=True)
    pairs = [
        (parquet, data / f"{parquet.stem}.vortex")
        for parquet in sorted(Path(source.data(str(work), table)).iterdir())
    ]
    pairs.append(
        (Path(source.annotation(str(work), table)), Path(form.annotation(str(work), table)))
    )
    strategy = [] if form.vortex_strategy == "default" else ["-s", form.vortex_strategy]
    for parquet, destination in pairs:
        subprocess.check_call(["vx", "convert", *strategy, str(parquet)])
        parquet.with_suffix(".vortex").replace(destination)
        expected = [(f.name, f.nullable) for f in pq.read_schema(parquet)]
        actual = [(f.name, f.nullable) for f in vortex.open(str(destination)).dtype.to_arrow_schema()]
        if actual != expected:
            raise AssertionError(f"{destination} has columns {actual}, expected {expected}")


def upload_form(form: Form, table: str, work: Path) -> None:
    """Uploads the contig files, then the annotation table last, so that a job that dies partway
    leaves a table discovery rejects rather than one that is silently short."""
    for path in sorted(Path(form.data(str(work), table)).iterdir()):
        hfs.copy(str(path), f"{form.data(DATASETS, table)}/{path.name}")
    hfs.copy(form.annotation(str(work), table), form.annotation(DATASETS, table))


def versions() -> dict[str, str]:
    from importlib.metadata import version

    import hail as hl

    return {
        "vortex-data": version("vortex-data"),
        "pyarrow": version("pyarrow"),
        "hail": hl.version(),
    }


# Summary, on the host.


def summary() -> None:
    reports = []
    for entry in sorted(hfs.ls(REPORTS), key=lambda entry: entry.path):
        if entry.path.endswith(".json"):
            with hfs.open(entry.path) as file:
                reports.append(json.load(file))
    if not reports:
        print(f"no reports in {REPORTS}")
        return

    rows: dict[str, int] = {}
    seconds: dict[str, float] = {}
    for report in reports:
        for contig, count in report["rows"].items():
            rows[contig] = rows.get(contig, 0) + count
        for phase, spent in report["seconds"].items():
            seconds[phase] = seconds.get(phase, 0.0) + spent
        phases = ", ".join(f"{phase} {spent:.0f}s" for phase, spent in report["seconds"].items())
        print(f"{report['table']}: {report['total_rows']:,} rows; {phases}")

    total = sum(rows.values())
    samples = [sample for report in reports for sample in report["samples"]]
    print(f"\n{len(reports)} tables, {len(samples)} samples, {total:,} rows")
    if len(set(samples)) != len(samples):
        print("warning: a sample is in more than one table")
    for contig in sorted(rows):
        print(f"  {contig}: {rows[contig]:,}")
    print(f"{total / 1.0e9:.3f} of the estimate of 1.0B rows")
    for phase, spent in seconds.items():
        print(f"  {phase}: {spent:.0f}s total, {spent / len(reports):.0f}s per table")
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
    commands.add_parser("summary", help="Add up the jobs' reports.")
    args = parser.parse_args()

    if args.command == "submit":
        submit(args.mts, args.per_job, args.dry_run)
    elif args.command == "convert":
        convert(args.mts)
    else:
        summary()


if __name__ == "__main__":
    main()
