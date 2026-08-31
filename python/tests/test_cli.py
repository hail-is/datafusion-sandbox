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


def test_pack_loci_writes_packed_values_and_normalizes_schema(tmp_path):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    source_file = sample_source / "HG00187.reference.zstd.parquet"
    schema = pa.schema(
        [
            pa.field("contig", pa.string(), nullable=True),
            pa.field("position", pa.int32(), nullable=True),
            pa.field("alleles", pa.string(), nullable=True),
            pa.field("value", pa.int32(), nullable=True),
        ]
    )
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(["chr22", "chr22"]),
                pa.array([1, 2], type=pa.int32()),
                pa.array(["A", "C"]),
                pa.array([None, 3], type=pa.int32()),
            ],
            schema=schema,
        ),
        source_file,
        compression="zstd",
    )
    destination = tmp_path / "packed"

    cli.pack_loci(source, destination)

    normalized = pq.read_table(source_file)
    assert not normalized.schema.field("contig").nullable
    assert not normalized.schema.field("position").nullable
    assert not normalized.schema.field("alleles").nullable
    assert normalized.schema.field("value").nullable

    packed_file = destination / "s=HG00187/HG00187.reference.chr22.zstd.parquet"
    packed = pq.read_table(packed_file)
    assert packed.column_names == ["locus", "alleles", "value"]
    assert packed.column("locus").to_pylist() == [
        (22 << 32) | 1,
        (22 << 32) | 2,
    ]
    assert not packed.schema.field("locus").nullable
    assert not packed.schema.field("alleles").nullable
    assert packed.schema.field("value").nullable


def test_pack_loci_rejects_a_non_numeric_contig_without_leaving_output(tmp_path):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    pq.write_table(
        pa.table({"contig": ["chrX"], "position": pa.array([1], type=pa.int32())}),
        sample_source / "HG00187.reference.zstd.parquet",
    )
    destination = tmp_path / "packed"

    with pytest.raises(ValueError, match="chrX"):
        cli.pack_loci(source, destination)

    assert not list(destination.rglob("*.parquet"))


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


def test_scaled_conversion_persists_padded_parquets_before_vortex(tmp_path, monkeypatch):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    pq.write_table(
        pa.table({"contig": ["chr22"], "position": pa.array([1], type=pa.int32())}),
        sample_source / "HG00187.reference.zstd.parquet",
    )
    destinations = []
    monkeypatch.setattr(
        cli,
        "parquet_to_vortex",
        lambda _source, destination, _compact: destinations.append(destination.name),
    )

    cli.convert_parquets(source, tmp_path / "vortices", scale_factor=14)

    scaled = sample_source / "HG00187.reference.chr09.zstd.parquet"
    assert scaled.is_file()
    assert pq.read_table(scaled).column("contig").to_pylist() == ["chr09"]
    assert "HG00187.reference.chr09.vortex" in destinations


def test_setup_builds_both_locus_representations_for_both_datasets(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "download", lambda: calls.append(("download",)))
    monkeypatch.setattr(
        cli,
        "convert_gvcfs",
        lambda source, destination: calls.append(("gvcfs", source, destination)),
    )
    monkeypatch.setattr(
        cli,
        "convert_vdss",
        lambda source, destination, alleles: calls.append(
            ("vdss", source, destination, alleles)
        ),
    )
    monkeypatch.setattr(
        cli,
        "scale_parquets",
        lambda path, factor: calls.append(("scale", path, factor)),
    )
    monkeypatch.setattr(
        cli,
        "pack_loci",
        lambda source, destination: calls.append(("pack", source, destination)),
    )
    monkeypatch.setattr(
        cli,
        "convert_parquets",
        lambda source, destination: calls.append(("vortex", source, destination)),
    )

    cli.setup()

    assert calls == [
        ("download",),
        ("gvcfs", Path("gvcfs_chr22"), Path("vdss_chr22")),
        (
            "vdss",
            Path("vdss_chr22"),
            Path("parquets_chr22"),
            Path("parquets_alleles_chr22"),
        ),
        ("scale", Path("parquets_chr22"), 1),
        ("scale", Path("parquets_alleles_chr22"), 1),
        ("pack", Path("parquets_chr22"), Path("parquets_packed_chr22")),
        (
            "pack",
            Path("parquets_alleles_chr22"),
            Path("parquets_alleles_packed_chr22"),
        ),
        ("vortex", Path("parquets_chr22"), Path("vortices_chr22")),
        (
            "vortex",
            Path("parquets_alleles_chr22"),
            Path("vortices_alleles_chr22"),
        ),
        ("vortex", Path("parquets_packed_chr22"), Path("vortices_packed_chr22")),
        (
            "vortex",
            Path("parquets_alleles_packed_chr22"),
            Path("vortices_alleles_packed_chr22"),
        ),
    ]
