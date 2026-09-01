"""Command-line interface for hailtools.

A collection of utilities for creating parquet and vortex files from existing
Hail data. Commands are defined directly on the Typer app below; if this file
grows unwieldy we can split them into a `commands/` subpackage later.
"""

from collections.abc import Iterator
from contextlib import ExitStack
import logging
from pathlib import Path, PurePath
import re
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import typer

import hail as hl
from hail.vds.combiner import transform_gvcf
from hail.vds.combiner.combine import combine_references

from . import gce

app = typer.Typer(
    help="Utilities for creating parquet and vortex files from Hail data.",
    no_args_is_help=True,
)

# `hailtools gce ...`. The implementation lives in gce.py and depends on nothing outside
# the standard library, so it can also be run directly on a build machine that has a rust
# toolchain but no hail install:
#     python3 python/src/hailtools/gce.py list
gce_app = typer.Typer(
    help="Pick rustc CPU flags for a GCE machine family.",
    no_args_is_help=True,
)
app.add_typer(gce_app, name="gce")


def _gce_run(fn, *args) -> None:
    try:
        print(fn(*args))
    except gce.GceCpuError as e:
        typer.echo(f"error: {e}", err=True)
        raise typer.Exit(1) from None


@gce_app.command("list")
def gce_list() -> None:
    """Show every family: which target-cpu is safe, and what it costs."""
    _gce_run(gce.cmd_list)


@gce_app.command("flags")
def gce_flags(family: str) -> None:
    """Print the rustc flags to build for FAMILY, e.g. `hailtools gce flags n2`."""
    _gce_run(gce.family_flags, family)


@gce_app.command("features")
def gce_features(family: str) -> None:
    """Print the CPU features common to every platform in FAMILY, one per line."""
    _gce_run(lambda f: "\n".join(sorted(gce.family_features(f))), family)


@gce_app.command("verify")
def gce_verify(family: str | None = None) -> None:
    """Run on a GCE VM: check the instance really has the features we computed."""
    _gce_run(gce.cmd_verify, family)


@app.command()
def setup() -> None:
    download()
    convert_gvcfs(Path('gvcfs_chr22'), Path('vdss_chr22'))
    convert_vdss(
        Path('vdss_chr22'),
        Path('parquets_chr22'),
        Path('parquets_alleles_chr22'),
    )
    scale_parquets(Path('parquets_chr22'))
    scale_parquets(Path('parquets_alleles_chr22'))
    pack_loci(Path('parquets_chr22'), Path('parquets_packed_chr22'))
    pack_loci(
        Path('parquets_alleles_chr22'),
        Path('parquets_alleles_packed_chr22'),
    )
    convert_parquets(Path('parquets_chr22'), Path('vortices_chr22'))
    convert_parquets(Path('parquets_alleles_chr22'), Path('vortices_alleles_chr22'))
    convert_parquets(Path('parquets_packed_chr22'), Path('vortices_packed_chr22'))
    convert_parquets(
        Path('parquets_alleles_packed_chr22'),
        Path('vortices_alleles_packed_chr22'),
    )


gs_curl_root = PurePath('https://storage.googleapis.com/hail-common/benchmark')
data_dir = Path('../data/')


def resolve_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    else:
        return data_dir / path


def __download(data_dir: Path, filename: str) -> None:
    url = gs_curl_root / filename
    dest = data_dir / filename
    logging.info(f'downloading: {filename}')
    subprocess.check_call(['curl', url, '-Lfs', '-m', '200', '--output', dest])
    logging.info(f'done: {filename}')


@app.command()
def download() -> None:
    gvcfs_tar = '1kg_chr22.tar'
    __download(data_dir, gvcfs_tar)
    subprocess.check_call(['tar', '-xf', data_dir / gvcfs_tar, '-C', data_dir])
    subprocess.check_call(['rm', data_dir / gvcfs_tar])


@app.command()
def convert_gvcf(path: Path, dest: Path) -> None:
    assert(dest.is_dir())

    mt = hl.import_vcf(str(path), reference_genome='GRCh38', force=True)
    name = path.name.removesuffix('.vcf.gz')

    vds = transform_gvcf(mt, ['DP', 'MIN_DP', 'GQ', 'GT'])
    vds_path = dest.with_name(f'{name}.vds')
    vds.write(vds_path)


@app.command()
def convert_gvcfs(path: Path, dest: Path) -> None:
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert(path.is_dir())
    dest.mkdir(exist_ok=True)

    for gvcf in path.glob('*.vcf.gz'):
        name = gvcf.stem
        sample_id = name.split('.')[0]
        part_dir = dest / f's={sample_id}'
        part_dir.mkdir()
        convert_gvcf(gvcf, part_dir)

def spark_df_to_parquet(df, dest: Path, filename: str) -> None:
    tmp_dir = dest / 'tmp'
    df.write.parquet(str(tmp_dir), compression='zstd')
    [tmp] = tmp_dir.glob('*.parquet')
    tmp.rename(dest / f'{filename}.zstd.parquet')
    for file in tmp_dir.iterdir():
        file.unlink()
    tmp_dir.rmdir()

def vds_to_reference(vds: hl.vds.VariantDataset, name: str, dest: Path) -> None:
    ref_mt = vds.reference_data
    ref_mt = ref_mt.transmute_entries(ploidy=ref_mt.LGT.ploidy)
    df = (
        ref_mt.entries()
        .key_by('locus')
        .drop('s', 'END')
        .to_spark()
        .withColumnRenamed('locus.contig', 'contig')
        .withColumnRenamed('locus.position', 'position')
    )
    spark_df_to_parquet(df, dest, f'{name}.reference')

def vds_to_alleles(vds: hl.vds.VariantDataset, name: str, dest: Path) -> None:
    var_mt = vds.variant_data
    df = (
        var_mt.rows()
        .drop('rsid')
        .key_by('locus')
        .explode('alleles')
        .to_spark()
        .withColumnRenamed('locus.contig', 'contig')
        .withColumnRenamed('locus.position', 'position')
    )
    spark_df_to_parquet(df, dest, f'{name}.alleles')

@app.command()
def convert_vds(path: Path, dest: Path, alleles_dest: Path | None = None) -> None:
    assert(dest.is_dir())
    name = path.stem
    vds = hl.vds.read_vds(path)
    vds_to_reference(vds, name, dest)
    if alleles_dest is not None:
        vds_to_alleles(vds, name, alleles_dest)

@app.command()
def convert_vdss(path: Path, dest: Path, alleles_dest: Path | None = None) -> None:
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert(path.is_dir())
    dest.mkdir(exist_ok=True)
    if alleles_dest is not None:
        alleles_dest = resolve_path(alleles_dest)
        alleles_dest.mkdir(exist_ok=True)

    for gvcf in path.glob('*.vds'):
        name = gvcf.stem
        sample_id = name.split('.')[0]
        ref_dir = dest / f's={sample_id}'
        ref_dir.mkdir(parents=True)

        alleles_dir = None
        if alleles_dest is not None:
            alleles_dir = alleles_dest / f's={sample_id}'
            alleles_dir.mkdir(parents=True)

        convert_vds(gvcf, ref_dir, alleles_dir)


def _iter_row_group_batches(parquet: pq.ParquetFile) -> Iterator[pa.RecordBatch]:
    for row_group in range(parquet.metadata.num_row_groups):
        yield from parquet.iter_batches(row_groups=[row_group])


def _write_scaled_parquets(source: Path, contigs: list[str]) -> None:
    parquet = pq.ParquetFile(source)
    contig_index = parquet.schema_arrow.get_field_index('contig')
    if contig_index == -1:
        raise ValueError(f"{source} has no contig column")

    contig_field = parquet.schema_arrow.field(contig_index)
    with ExitStack() as stack:
        writers = [
            (
                contig,
                stack.enter_context(
                    pq.ParquetWriter(
                        source.with_name(_parquet_name_with_contig(source, contig)),
                        parquet.schema_arrow,
                        compression='zstd',
                    )
                ),
            )
            for contig in contigs
        ]
        for batch in _iter_row_group_batches(parquet):
            for contig, writer in writers:
                repeated_contig = pa.repeat(
                    pa.scalar(contig, type=contig_field.type), batch.num_rows
                )
                writer.write_batch(
                    batch.set_column(contig_index, contig_field, repeated_contig)
                )


def _contig_ordinal(contig: str) -> int:
    match = re.fullmatch(r"chr([0-9]+)", contig)
    if match is None:
        raise ValueError(f"invalid contig name: {contig}")
    return int(match.group(1))


def _pack_locus(contig_ordinal: int, positions: pa.Array) -> pa.Array:
    ordinal_bits = pa.scalar(contig_ordinal << 32, type=pa.int64())
    return pc.bit_wise_or(pc.cast(positions, pa.int64()), ordinal_bits)


def _required_non_null_schema(schema: pa.Schema) -> pa.Schema:
    required = {"contig", "position", "alleles"}
    fields = [
        field.with_nullable(False) if field.name in required else field
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _packed_schema(schema: pa.Schema) -> pa.Schema:
    fields = []
    for field in schema:
        if field.name == "contig":
            fields.append(pa.field("locus", pa.int64(), nullable=False))
        elif field.name != "position":
            fields.append(field)
    return pa.schema(fields, metadata=schema.metadata)


def _pack_batch(
    batch: pa.RecordBatch, schema: pa.Schema, contig_ordinal: int
) -> pa.RecordBatch:
    position_index = batch.schema.get_field_index("position")
    arrays = []
    for index, field in enumerate(batch.schema):
        if field.name == "contig":
            arrays.append(_pack_locus(contig_ordinal, batch.column(position_index)))
        elif field.name != "position":
            arrays.append(batch.column(index))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _pack_parquet(source: Path, destination: Path) -> str:
    parquet = pq.ParquetFile(source)
    normalized_schema = _required_non_null_schema(parquet.schema_arrow)
    for name in ("contig", "position"):
        if normalized_schema.get_field_index(name) == -1:
            raise ValueError(f"{source} has no {name} column")
    packed_schema = _packed_schema(normalized_schema)
    destination.parent.mkdir(parents=True, exist_ok=True)
    normalized_path = None
    if not parquet.schema_arrow.equals(normalized_schema):
        with tempfile.NamedTemporaryFile(
            dir=source.parent,
            prefix=f".{source.name}.",
            suffix=".parquet",
            delete=False,
        ) as temporary:
            normalized_path = Path(temporary.name)
    with tempfile.NamedTemporaryFile(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".parquet",
        delete=False,
    ) as temporary:
        packed_path = Path(temporary.name)
    try:
        observed_contigs = set()
        contig_ordinal = None
        with ExitStack() as stack:
            source_writer = (
                stack.enter_context(
                    pq.ParquetWriter(
                        normalized_path, normalized_schema, compression="zstd"
                    )
                )
                if normalized_path is not None
                else None
            )
            packed_writer = stack.enter_context(
                pq.ParquetWriter(packed_path, packed_schema, compression="zstd")
            )
            for batch in _iter_row_group_batches(parquet):
                for field in normalized_schema:
                    if not field.nullable:
                        column = batch.column(batch.schema.get_field_index(field.name))
                        if column.null_count:
                            raise ValueError(f"{source} has null values in {field.name}")
                contig_index = batch.schema.get_field_index("contig")
                observed_contigs.update(
                    pc.unique(batch.column(contig_index)).to_pylist()
                )
                if len(observed_contigs) != 1:
                    raise ValueError(
                        f"{source} contains contigs: {sorted(observed_contigs)}"
                    )
                if contig_ordinal is None:
                    contig_ordinal = _contig_ordinal(next(iter(observed_contigs)))
                normalized = pa.RecordBatch.from_arrays(
                    batch.columns, schema=normalized_schema
                )
                if source_writer is not None:
                    source_writer.write_batch(normalized)
                packed_writer.write_batch(
                    _pack_batch(normalized, packed_schema, contig_ordinal)
                )
        if len(observed_contigs) != 1:
            raise ValueError(f"{source} contains contigs: {sorted(observed_contigs)}")
        if normalized_path is not None:
            normalized_path.replace(source)
        packed_path.replace(destination)
        return observed_contigs.pop()
    finally:
        if normalized_path is not None:
            normalized_path.unlink(missing_ok=True)
        packed_path.unlink(missing_ok=True)


def _parquet_name_with_contig(source: Path, contig: str) -> str:
    suffix = ".zstd.parquet" if source.name.endswith(".zstd.parquet") else ".parquet"
    stem = source.name.removesuffix(suffix)
    return f"{stem}.{contig}{suffix}"


@app.command("pack-loci")
def pack_loci(path: Path, dest: Path) -> None:
    """Normalize a dataset and write a copy with packed loci."""
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert path.is_dir()
    dest.parent.mkdir(parents=True, exist_ok=True)
    parquets = sorted(path.rglob("*.parquet"))
    with tempfile.TemporaryDirectory(
        dir=dest.parent, prefix=f".{dest.name}."
    ) as temporary:
        staging = Path(temporary)
        for parquet in parquets:
            relative = parquet.relative_to(path)
            staged = staging / relative
            contig = _pack_parquet(parquet, staged)
            packed_name = (
                parquet.name
                if f".{contig}." in parquet.name
                else _parquet_name_with_contig(parquet, contig)
            )
            staged.replace(staged.with_name(packed_name))

        if not dest.exists():
            staging.replace(dest)
        else:
            for packed in staging.rglob("*.parquet"):
                destination = dest / packed.relative_to(staging)
                destination.parent.mkdir(parents=True, exist_ok=True)
                packed.replace(destination)


def parquet_to_vortex(source: Path, destination: Path, compact: bool) -> None:
    if compact:
        subprocess.check_call(['uv', 'run', 'vx', 'convert', '-s', 'compact', source])
    else:
        subprocess.check_call(['uv', 'run', 'vx', 'convert', source])
    source.with_suffix('.vortex').replace(destination)


def _scale_parquets(path: Path, scale_factor: int) -> None:
    originals = [
        parquet
        for parquet in path.rglob("*.parquet")
        if re.search(r"\.chr[0-9]{2}(?:\.zstd)?\.parquet$", parquet.name) is None
    ]
    if scale_factor == 1:
        return
    for parquet in originals:
        contigs = [f"chr{22 - offset:02d}" for offset in range(1, scale_factor)]
        _write_scaled_parquets(parquet, contigs)


@app.command("scale-parquets")
def scale_parquets(path: Path, scale_factor: int = 10) -> None:
    """Persist synthetic contigs in an existing parquet dataset."""
    if scale_factor > 23:
        raise ValueError(
            "scale factor cannot exceed 23 because contig naming must stay fixed width"
        )
    path = resolve_path(path)
    assert path.is_dir()
    _scale_parquets(path, scale_factor)


@app.command()
def convert_parquets(path: Path, dest: Path, compact: bool = False) -> None:
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert(path.is_dir())
    dest.mkdir(exist_ok=True)

    for parquet in path.rglob('*.parquet'):
        tmp = parquet.with_suffix('')
        if tmp.suffix == '.zstd':
            tmp = tmp.with_suffix('')
        name = tmp.name
        part_path = (dest / parquet.relative_to(path)).with_name(f'{name}.vortex')
        part_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_to_vortex(parquet, part_path, compact)


@app.command()
def combine_vdss(path: Path, dest: Path) -> None:
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert(path.is_dir())

    mts = [hl.vds.read_vds(vds).reference_data for vds in path.rglob('*.vds')]
    combine_references(mts).write(str(dest))

if __name__ == "__main__":
    app()
