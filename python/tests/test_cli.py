import os
from pathlib import Path
from types import SimpleNamespace

import hail as hl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner
import vortex

from hailtools import cli


@pytest.fixture(scope="module")
def hail_context(tmp_path_factory):
    hl.init(
        backend="spark",
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


def write_reference_table(path, sample, loci):
    """Write a per-sample reference Hail Table shaped like the production ones.

    `loci` holds `(contig, position, ploidy)` triples, with a `None` contig for a
    missing locus. The table is keyed by locus and split into three partitions,
    so a contig with three or more rows spans a partition boundary.
    """
    table = hl.Table.parallelize(
        [
            {
                "locus": (
                    None
                    if contig is None
                    else hl.Locus(contig, position, reference_genome="GRCh38")
                ),
                "DP": position + 1,
                "GQ": position + 2,
                "LGT": hl.Call([0] * ploidy),
                "LEN": position + 3,
            }
            for contig, position, ploidy in loci
        ],
        schema=hl.tstruct(
            locus=hl.tlocus("GRCh38"),
            DP=hl.tint32,
            GQ=hl.tint32,
            LGT=hl.tcall,
            LEN=hl.tint32,
        ),
        key="locus",
        n_partitions=3,
    )
    table = table.annotate_globals(ref_block_max_length=10, s=sample)
    table.write(str(path))


def test_convert_hts_writes_one_parquet_per_contig_for_the_global_sample(
    tmp_path, hail_context
):
    source = tmp_path / "hts"
    write_reference_table(
        source / "table-file-name.ht",
        "SAMPLE-7.0",
        [("chr8", 40, 1), ("chr1", 30, 2), ("chr1", 10, 2), ("chr8", 20, 1), ("chr1", 20, 2)],
    )
    destination = tmp_path / "parquets"

    result = CliRunner().invoke(cli.app, ["convert-hts", str(source), str(destination)])

    assert result.exit_code == 0, result.output
    assert [p.name for p in destination.iterdir()] == ["s=SAMPLE-7.0"]
    sample_dir = destination / "s=SAMPLE-7.0"
    assert sorted(p.name for p in sample_dir.iterdir()) == [
        "SAMPLE-7.0.reference.chr01.zstd.parquet",
        "SAMPLE-7.0.reference.chr08.zstd.parquet",
    ]
    chr01 = pq.read_table(sample_dir / "SAMPLE-7.0.reference.chr01.zstd.parquet")
    assert chr01.schema.remove_metadata() == pa.schema([
        pa.field("contig", pa.string(), nullable=False),
        pa.field("position", pa.int32(), nullable=False),
        ("DP", pa.int32()),
        ("GQ", pa.int32()),
        ("LEN", pa.int32()),
        ("ploidy", pa.int32()),
    ])
    assert chr01.to_pydict() == {
        "contig": ["chr01"] * 3,
        "position": [10, 20, 30],
        "DP": [11, 21, 31],
        "GQ": [12, 22, 32],
        "LEN": [13, 23, 33],
        "ploidy": [2, 2, 2],
    }
    chr08 = pq.read_table(sample_dir / "SAMPLE-7.0.reference.chr08.zstd.parquet")
    assert chr08.to_pydict() == {
        "contig": ["chr08"] * 2,
        "position": [20, 40],
        "DP": [21, 41],
        "GQ": [22, 42],
        "LEN": [23, 43],
        "ploidy": [1, 1],
    }


def test_convert_hts_limit_converts_the_first_tables_in_sorted_order(
    tmp_path, hail_context
):
    source = tmp_path / "hts"
    for name, sample in [("c.ht", "S1"), ("a.ht", "S2"), ("b.ht", "S3")]:
        write_reference_table(source / name, sample, [("chr1", 1, 2), ("chr1", 2, 2)])
    destination = tmp_path / "parquets"

    result = CliRunner().invoke(
        cli.app, ["convert-hts", str(source), str(destination), "--limit", "2"]
    )

    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in destination.iterdir()) == ["s=S2", "s=S3"]


def test_convert_hts_rejects_a_non_numeric_contig_without_leaving_output(
    tmp_path, hail_context
):
    source = tmp_path / "hts"
    write_reference_table(source / "sample.ht", "S1", [("chr1", 1, 2), ("chrX", 1, 1)])
    destination = tmp_path / "parquets"

    result = CliRunner().invoke(cli.app, ["convert-hts", str(source), str(destination)])

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "sample.ht" in str(result.exception)
    assert "chrX" in str(result.exception)
    assert list(destination.iterdir()) == []


def test_convert_hts_rejects_a_missing_locus_without_leaving_output(
    tmp_path, hail_context
):
    source = tmp_path / "hts"
    write_reference_table(source / "sample.ht", "S1", [("chr1", 1, 2), (None, 2, 2)])
    destination = tmp_path / "parquets"

    result = CliRunner().invoke(cli.app, ["convert-hts", str(source), str(destination)])

    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "sample.ht" in str(result.exception)
    assert "missing locus" in str(result.exception)
    assert list(destination.iterdir()) == []


def test_convert_hts_refuses_an_existing_sample_directory(tmp_path, hail_context):
    source = tmp_path / "hts"
    write_reference_table(source / "sample.ht", "S1", [("chr1", 1, 2)])
    earlier = tmp_path / "parquets" / "s=S1"
    earlier.mkdir(parents=True)
    (earlier / "earlier.parquet").write_bytes(b"earlier")

    result = CliRunner().invoke(
        cli.app, ["convert-hts", str(source), str(tmp_path / "parquets")]
    )

    assert result.exit_code != 0
    assert isinstance(result.exception, FileExistsError)
    assert [p.name for p in earlier.iterdir()] == ["earlier.parquet"]


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
        pa.table({"contig": ["chr22"], "position": pa.array([1], type=pa.int32())}),
        sample_source / "HG00187.a.reference.zstd.parquet",
    )
    pq.write_table(
        pa.table({"contig": ["chrX"], "position": pa.array([1], type=pa.int32())}),
        sample_source / "HG00187.b.reference.zstd.parquet",
    )
    destination = tmp_path / "packed"

    with pytest.raises(ValueError, match="chrX"):
        cli.pack_loci(source, destination)

    assert not list(destination.rglob("*.parquet"))


def test_pack_loci_accumulates_one_contig_across_batches(tmp_path):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    source_file = sample_source / "HG00187.reference.zstd.parquet"
    pq.write_table(
        pa.table(
            {
                "contig": ["chr22", "chr22"],
                "position": pa.array([1, 2], type=pa.int32()),
            }
        ),
        source_file,
        row_group_size=1,
    )
    assert pq.ParquetFile(source_file).metadata.num_row_groups == 2
    destination = tmp_path / "packed"

    cli.pack_loci(source, destination)

    packed = pq.read_table(
        destination / "s=HG00187/HG00187.reference.chr22.zstd.parquet"
    )
    assert packed.column("locus").to_pylist() == [
        (22 << 32) | 1,
        (22 << 32) | 2,
    ]


def test_pack_loci_rejects_multiple_contigs_and_names_them(tmp_path):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "contig": ["chr22", "chr21"],
                "position": pa.array([1, 2], type=pa.int32()),
            }
        ),
        sample_source / "HG00187.reference.zstd.parquet",
        row_group_size=1,
    )
    destination = tmp_path / "packed"

    with pytest.raises(ValueError, match=r"chr21.*chr22"):
        cli.pack_loci(source, destination)

    assert not list(destination.rglob("*.parquet"))


def test_pack_loci_does_not_rewrite_an_already_normalized_source(tmp_path):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    source_file = sample_source / "HG00187.reference.zstd.parquet"
    schema = pa.schema(
        [
            pa.field("contig", pa.string(), nullable=False),
            pa.field("position", pa.int32(), nullable=False),
        ]
    )
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array(["chr22"]), pa.array([1], type=pa.int32())],
            schema=schema,
        ),
        source_file,
    )
    destination = tmp_path / "packed"
    cli.pack_loci(source, destination)
    unchanged_timestamp = 1_000_000_000
    os.utime(source_file, ns=(unchanged_timestamp, unchanged_timestamp))

    cli.pack_loci(source, destination)

    assert source_file.stat().st_mtime_ns == unchanged_timestamp


def test_scale_parquets_rewrites_stored_contigs_before_conversion(tmp_path, monkeypatch):
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
        row_group_size=1,
    )
    destination = tmp_path / "vortices"
    monkeypatch.chdir(Path(__file__).parents[1])

    cli.scale_parquets(source, scale_factor=3)
    cli.convert_parquets(source, destination)

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


def test_scale_parquets_persists_padded_files_before_conversion(tmp_path, monkeypatch):
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

    cli.scale_parquets(source, scale_factor=14)
    cli.convert_parquets(source, tmp_path / "vortices")

    scaled = sample_source / "HG00187.reference.chr09.zstd.parquet"
    assert scaled.is_file()
    assert pq.read_table(scaled).column("contig").to_pylist() == ["chr09"]
    assert "HG00187.reference.chr09.vortex" in destinations


def test_convert_parquets_accepts_packed_loci(tmp_path, monkeypatch):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    source_file = sample_source / "HG00187.reference.chr22.zstd.parquet"
    pq.write_table(
        pa.table({"locus": pa.array([(22 << 32) | 1], type=pa.int64())}),
        source_file,
    )
    converted = []
    monkeypatch.setattr(
        cli,
        "parquet_to_vortex",
        lambda parquet, _destination, _compact: converted.append(parquet),
    )

    cli.convert_parquets(source, tmp_path / "vortices")

    assert converted == [source_file]


def test_scale_parquets_rejects_contig_names_that_cannot_stay_fixed_width(tmp_path):
    source = tmp_path / "parquets"
    sample_source = source / "s=HG00187"
    sample_source.mkdir(parents=True)
    pq.write_table(
        pa.table({"contig": ["chr22"], "position": pa.array([1], type=pa.int32())}),
        sample_source / "HG00187.reference.zstd.parquet",
    )

    with pytest.raises(ValueError, match="contig naming"):
        cli.scale_parquets(source, scale_factor=24)

    assert [path.name for path in sample_source.glob("*.parquet")] == [
        "HG00187.reference.zstd.parquet"
    ]


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
        lambda path, factor=10: calls.append(("scale", path, factor)),
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
        ("scale", Path("parquets_chr22"), 10),
        ("scale", Path("parquets_alleles_chr22"), 10),
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


def test_probe_viewer_writes_the_page_where_it_is_told(tmp_path):
    metrics = tmp_path / "metrics"
    (metrics / "runs").mkdir(parents=True)
    pq.write_table(
        pa.table({
            "run_id": ["shadow-union-j1"],
            "formulation": ["union"],
            "threads": pa.array([1], type=pa.uint64()),
            "action": ["shadow"],
            "steady_state_throughput": [100.0],
            "would_stop_ns": pa.array([None], type=pa.uint64()),
        }),
        metrics / "runs" / "shadow-union-j1.parquet",
    )
    page = tmp_path / "page.html"

    result = CliRunner().invoke(cli.app, ["probe-viewer", str(metrics), "-o", str(page)])

    assert result.exit_code == 0, result.output
    assert "shadow-union-j1" in page.read_text()
    assert str(page) in result.output


def test_probe_viewer_says_why_it_writes_no_page(tmp_path):
    result = CliRunner().invoke(cli.app, ["probe-viewer", str(tmp_path)])

    assert result.exit_code == 1
    assert "no run records" in result.output
    assert not (tmp_path / "probe-viewer.html").exists()


def test_sweep_report_prints_a_row_per_cell_and_runner_shape(tmp_path):
    (tmp_path / "runs").mkdir()
    (tmp_path / "runners").mkdir()
    pq.write_table(
        pa.table({
            "run_id": ["b50-c4-standard-2-tpc2-r1"],
            "threads": pa.array([1], type=pa.uint64()),
            "input_tables": pa.array([50], type=pa.uint64()),
            "rows_written": pa.array([1_000_000], type=pa.uint64()),
            "run_ns": pa.array([1_000_000_000], type=pa.uint64()),
        }),
        tmp_path / "runs" / "b50-c4-standard-2-tpc2-r1.parquet",
    )
    (tmp_path / "runners" / "c4-standard-2-tpc2-r1.json").write_text(
        '{"runner": "c4-standard-2-tpc2-r1", "machine_type": "c4-standard-2", "threads_per_core": "2", "repetition": "1", "cell_order": ["b50"],'
        ' "cells": [{"cell": "b50", "run_id": "b50-c4-standard-2-tpc2-r1", "exit_status": 0, "seconds": 1}],'
        ' "status": "done"}'
    )

    result = CliRunner().invoke(cli.app, ["sweep-report", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "| b50 | c4-standard-2 | 2 | 1 | 3.91 ±0.0% |" in result.output


def test_sweep_report_says_why_it_reports_nothing(tmp_path):
    result = CliRunner().invoke(cli.app, ["sweep-report", str(tmp_path)])

    assert result.exit_code == 1
    assert "no runner records" in result.output
