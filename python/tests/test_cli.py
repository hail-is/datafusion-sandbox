from pathlib import Path
from types import SimpleNamespace

import hail as hl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import vortex

from hailtools import cli


@pytest.fixture(scope="module")
def hail_context(tmp_path_factory):
    hl.init(
        master="local[2]",
        quiet=True,
        tmp_dir=str(tmp_path_factory.mktemp("hail")),
    )
    yield
    hl.stop()


def two_locus_matrix_table():
    matrix = hl.utils.range_matrix_table(2, 1)
    matrix = matrix.key_rows_by(
        locus=hl.locus(
            "chr22",
            matrix.row_idx + 1,
            reference_genome="GRCh38",
        )
    )
    return matrix.naive_coalesce(1)


def test_reference_conversion_stores_the_contig(tmp_path, hail_context):
    reference = two_locus_matrix_table()
    reference = reference.key_cols_by(s=hl.str(reference.col_idx))
    reference = reference.annotate_entries(
        LGT=hl.call(0, 0),
        END=reference.locus.position,
    )
    vds = SimpleNamespace(reference_data=reference)
    destination = tmp_path / "reference"
    destination.mkdir()

    cli.vds_to_reference(vds, "sample", destination)

    table = pq.read_table(destination / "sample.reference.zstd.parquet")
    assert table.column("contig").to_pylist() == ["chr22", "chr22"]
    assert table.column("position").to_pylist() == [1, 2]


def test_allele_conversion_stores_the_contig(tmp_path, hail_context):
    variants = two_locus_matrix_table()
    variants = variants.annotate_rows(alleles=["A", "C"], rsid="rs-test")
    vds = SimpleNamespace(variant_data=variants)
    destination = tmp_path / "alleles"
    destination.mkdir()

    cli.vds_to_alleles(vds, "sample", destination)

    table = pq.read_table(destination / "sample.alleles.zstd.parquet")
    assert table.column("contig").to_pylist() == ["chr22"] * 4
    assert table.column("position").to_pylist() == [1, 1, 2, 2]
    assert table.column("alleles").to_pylist() == ["A", "C", "A", "C"]


def test_scaled_vortex_conversion_rewrites_stored_contigs(tmp_path, monkeypatch):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "contig": ["chr22", "chr22"],
                "position": [1, 2],
            }
        ),
        sample_source / "HG00187.reference.zstd.parquet",
        compression="zstd",
    )
    destination = tmp_path / "vortices"
    monkeypatch.chdir(Path(__file__).parents[1])

    cli.convert_parquets(source, destination, scale_factor=3)

    sample_destination = destination / "s=HG00187"
    files = sorted(sample_destination.glob("*.vortex"))
    assert [file.name for file in files] == [
        "HG00187.reference.chr20.vortex",
        "HG00187.reference.chr21.vortex",
        "HG00187.reference.vortex",
    ]
    assert not [path for path in sample_destination.iterdir() if path.is_dir()]
    contigs = {
        file.name: set(
            vortex.open(str(file))
            .to_arrow(["contig"])
            .read_all()
            .column("contig")
            .to_pylist()
        )
        for file in files
    }
    assert contigs == {
        "HG00187.reference.chr20.vortex": {"chr20"},
        "HG00187.reference.chr21.vortex": {"chr21"},
        "HG00187.reference.vortex": {"chr22"},
    }
