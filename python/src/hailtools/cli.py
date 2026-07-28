"""Command-line interface for hailtools.

A collection of utilities for creating parquet and vortex files from existing
Hail data. Commands are defined directly on the Typer app below; if this file
grows unwieldy we can split them into a `commands/` subpackage later.
"""

import logging
import subprocess
from pathlib import Path, PurePath
import shutil

import hail as hl
import typer
from hail.vds.combiner import transform_gvcf
from hail.vds.combiner.combine import combine_references

app = typer.Typer(
    help="Utilities for creating parquet and vortex files from Hail data.",
    no_args_is_help=True,
)


@app.command()
def setup() -> None:
    download()
    convert_gvcfs(Path('gvcfs_chr22'), Path('vdss_chr22'))
    convert_vdss(Path('vdss_chr22'), Path('parquets_chr22'))
    convert_parquets(Path('parquets_chr22'), Path('vortices_chr22'))


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


@app.command()
def convert_vds(path: Path, dest: Path) -> None:
    assert(dest.is_dir())

    name = path.stem
    vds = hl.vds.read_vds(path)
    tmp_dir = dest / f'{name}.reference'
    ref_mt = vds.reference_data
    ref_mt = ref_mt.transmute_entries(ploidy=ref_mt.LGT.ploidy)
    ref_mt.entries().key_by('locus').drop('s', 'END').to_spark().drop('locus.contig').withColumnRenamed('locus.position', 'position').write.parquet(str(tmp_dir), compression='zstd')
    [tmp] = tmp_dir.glob('*.parquet')
    tmp.rename(dest / f'{name}.reference.zstd.parquet')
    for file in tmp_dir.iterdir():
        file.unlink()
    tmp_dir.rmdir()


@app.command()
def convert_vdss(path: Path, dest: Path) -> None:
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert(path.is_dir())
    dest.mkdir(exist_ok=True)
    (dest / 'contig=chr22').mkdir()

    for gvcf in path.glob('*.vds'):
        name = gvcf.stem
        sample_id = name.split('.')[0]
        part_dir = dest / 'contig=chr22' / f's={sample_id}'
        part_dir.mkdir()
        convert_vds(gvcf, part_dir)


@app.command()
def convert_parquets(path: Path, dest: Path, compact: bool = False, scale_factor: int = 1) -> None:
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert(path.is_dir())
    dest.mkdir(exist_ok=True)

    (dest / "contig=chr22").mkdir()

    for parquet in path.rglob('*.parquet'):
        tmp = parquet.with_suffix('')
        if tmp.suffix == '.zstd':
            tmp = tmp.with_suffix('')
        name = tmp.name
        part_path = (dest / parquet.relative_to(path)).with_name(f'{name}.vortex')
        part_path.parent.mkdir(parents=True, exist_ok=True)
        if compact:
            subprocess.check_call(['uv', 'run', 'vx', 'convert', '-s', 'compact', parquet])
        else:
            subprocess.check_call(['uv', 'run', 'vx', 'convert', parquet])
        parquet.with_suffix('.vortex').rename(part_path)

    contigs = [f'contig=chr{22-i}' for i in range(1, scale_factor)]
    for contig in contigs:
        shutil.copytree(dest / "contig=chr22", dest / contig)


@app.command()
def combine_vdss(path: Path, dest: Path) -> None:
    path = resolve_path(path)
    dest = resolve_path(dest)

    assert(path.is_dir())

    mts = [hl.vds.read_vds(vds).reference_data for vds in path.rglob('*.vds')]
    combine_references(mts).write(str(dest))

if __name__ == "__main__":
    app()
