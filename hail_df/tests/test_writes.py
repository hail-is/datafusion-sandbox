"""The `Write` and `MeasuredWrite` Actions through the real backend: what they leave at the output
path and under the metrics directory, each read back through what a later reader has."""

import hail_df
import pyarrow.parquet as pq
import pytest
from conftest import ROWS, SAMPLE_SETS, plan_sections, write_mixed_dataset
from hail_df import (
    BackendError,
    Collect,
    Explain,
    ExplainAnalyze,
    FormulationRecord,
    InvalidAction,
    MeasuredWrite,
    MeasuredWritten,
    OutputLayout,
    PlanText,
    RowsWritten,
    Write,
    formulations,
)
from hail_df.datafusion import col

SAMPLES = sum(len(sample_set) for sample_set in SAMPLE_SETS)
GROUPED = FormulationRecord("grouped-merge", groups=2)


@pytest.fixture
def ctx():
    return hail_df.context()


def write_of(ordered, path, input_format, annotate_samples=False, compression=None) -> Write:
    return Write(
        str(path), input_format, compression, ordered.ordering, ordered.layout, annotate_samples
    )


def sort_keys(dataset) -> list:
    return [
        col(column).sort(ascending, nulls_first)
        for column, ascending, nulls_first in dataset.ordering
    ]


def run_record(metrics, run_id) -> dict:
    """The run record of `run_id` under `metrics`, as a reader of the directory gets it."""
    table = pq.read_table(metrics / "runs" / f"{run_id}.parquet")
    # A nanosecond timestamp has no Python form without pandas; this suite checks no start time.
    records = table.drop_columns(["started_at"]).to_pylist()
    assert len(records) == 1
    return records[0]


def test_a_write_writes_every_row_in_its_ordering_to_one_file(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    path = tmp_path / "out" / f"g.{input_format}"

    outcome = backend.start(
        ctx, ordered.frame, write_of(ordered, path, input_format, annotate_samples=True)
    ).wait()

    assert outcome == RowsWritten(len(ROWS) * SAMPLES)
    written = backend.open_dataset(str(path.parent), input_format, "locus,alleles")
    assert [table.name for table in written.input_tables] == ["g"]
    rows = backend.start(
        ctx, formulations.union(ctx, written).frame, Collect(written.ordering)
    ).wait()
    expected = backend.start(ctx, ordered.frame, Collect(ordered.ordering)).wait()
    assert rows.rows.select(["contig", "position", "alleles"]).equals(
        expected.rows.select(["contig", "position", "alleles"])
    )


def test_a_write_annotating_samples_covers_exactly_the_input_tables_the_plan_reads(
    backend, dataset, ctx, input_format, tmp_path
):
    frame = dataset.read(ctx, ["g0", "s=HG02230"]).sort(*sort_keys(dataset))
    path = tmp_path / "out" / f"g.{input_format}"

    backend.start(
        ctx,
        frame,
        Write(str(path), input_format, None, dataset.ordering, OutputLayout.SINGLE_FILE, True),
    ).wait()

    written = backend.open_dataset(str(path.parent), input_format, "locus,alleles")
    assert [table.sample_set for table in written.input_tables] == [
        ("HG00308", "HG00592", "HG02230")
    ]


def test_a_write_without_annotating_samples_writes_no_annotation_table(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    path = tmp_path / "out" / f"g.{input_format}"

    backend.start(ctx, ordered.frame, write_of(ordered, path, input_format)).wait()

    assert [entry.name for entry in path.parent.iterdir()] == [path.name]


def test_a_failed_data_write_writes_no_annotation_table(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    # A directory at the output path, which the file can't replace once it is written.
    path = tmp_path / "out" / f"g.{input_format}"
    path.mkdir(parents=True)

    with pytest.raises(BackendError):
        backend.start(
            ctx, ordered.frame, write_of(ordered, path, input_format, annotate_samples=True)
        ).wait()

    assert not (path.parent / f"g.samples.{input_format}").exists()


def test_a_file_per_partition_write_raises_invalid_action(backend, dataset, ctx, tmp_path):
    ordered = formulations.union(ctx, dataset)
    write = Write(
        str(tmp_path / "out"),
        dataset.input_format,
        None,
        ordered.ordering,
        OutputLayout.FILE_PER_PARTITION,
    )

    with pytest.raises(InvalidAction, match="per partition"):
        backend.start(ctx, ordered.frame, write)
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize(
    ("name", "output_format", "compression", "layout", "problem"),
    [
        ("g.parquet", "csv", None, OutputLayout.SINGLE_FILE, "csv"),
        ("g.parquet", "parquet", "brotli", OutputLayout.SINGLE_FILE, "brotli"),
        ("g", "parquet", None, OutputLayout.SINGLE_FILE, "no extension"),
        ("g.vortex", "parquet", None, OutputLayout.SINGLE_FILE, "contradicts"),
        ("g.samples.parquet", "parquet", None, OutputLayout.SINGLE_FILE, "annotation"),
        (".g.parquet", "parquet", None, OutputLayout.SINGLE_FILE, "hidden"),
        ("g.parquet", "parquet", None, "one big file", "one big file"),
    ],
)
def test_a_write_that_cannot_mean_anything_raises_invalid_action(
    backend, dataset, ctx, tmp_path, name, output_format, compression, layout, problem
):
    ordered = formulations.union(ctx, dataset)
    write = Write(str(tmp_path / name), output_format, compression, ordered.ordering, layout)

    with pytest.raises(InvalidAction, match=problem):
        backend.start(ctx, ordered.frame, write)


def test_a_compression_mode_reaches_the_file_and_the_run_record(backend, ctx, tmp_path):
    dataset = backend.open_dataset(
        str(write_mixed_dataset(tmp_path, "parquet")), "parquet", "locus,alleles"
    )
    ordered = formulations.union(ctx, dataset)
    path = tmp_path / "out" / "g.parquet"
    metrics = tmp_path / "metrics"

    run = backend.start(
        ctx,
        ordered.frame,
        MeasuredWrite(
            write_of(ordered, path, "parquet", compression="gzip(3)"),
            str(metrics),
            ordered.formulation,
        ),
    )
    run.wait()

    codec = pq.read_metadata(path).row_group(0).column(0).compression
    assert codec == "GZIP"
    assert run_record(metrics, run.run_id)["compression"] == "gzip(3)"


def test_explaining_a_write_renders_its_sink_and_writes_nothing(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    path = tmp_path / "out" / f"g.{input_format}"
    write = write_of(ordered, path, input_format, annotate_samples=True)

    outcome = backend.start(ctx, ordered.frame, Explain(ordered.ordering, write)).wait()

    assert isinstance(outcome, PlanText)
    physical_plan = plan_sections(outcome.text)["physical_plan"]
    assert "DataSinkExec" in physical_plan
    assert "OrderingAssertionExec" in physical_plan
    assert "SortExec" not in physical_plan
    assert not (tmp_path / "out").exists()


def test_explain_analyzing_a_write_performs_it_and_renders_its_metrics(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    path = tmp_path / "out" / f"g.{input_format}"
    write = write_of(ordered, path, input_format, annotate_samples=True)

    outcome = backend.start(ctx, ordered.frame, ExplainAnalyze(ordered.ordering, write)).wait()

    assert "DataSinkExec" in plan_sections(outcome.text)["Plan with Metrics"]
    assert path.is_file()
    assert (path.parent / f"g.samples.{input_format}").is_file()


@pytest.mark.parametrize("action", [Explain, ExplainAnalyze])
def test_an_explain_of_a_write_holding_another_ordering_raises_invalid_action(
    backend, dataset, ctx, tmp_path, action
):
    ordered = formulations.union(ctx, dataset)
    write = write_of(ordered, tmp_path / f"g.{dataset.input_format}", dataset.input_format)

    with pytest.raises(InvalidAction, match="ordering|holds its rows"):
        backend.start(ctx, ordered.frame, action(ordered.ordering[:1], write))


def test_a_measured_write_records_the_dataset_from_its_scans_and_the_formulation_it_was_given(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    frame = dataset.read(ctx, ["g0", "s=HG02230"]).sort(*sort_keys(dataset))
    path = tmp_path / "out" / f"g.{input_format}"
    metrics = tmp_path / "metrics"

    run = backend.start(
        ctx,
        frame,
        MeasuredWrite(
            write_of(ordered, path, input_format, annotate_samples=True), str(metrics), GROUPED
        ),
        settings={"hail_df.threads": "2"},
        run_id="run-a",
    )
    outcome = run.wait()

    assert isinstance(outcome, MeasuredWritten)
    assert outcome.rows_written == len(ROWS) * 3
    # The run metrics table has a column for every metric these plans report, so nothing is named.
    assert outcome.unrecorded_metrics == ()
    record = run_record(metrics, "run-a")
    expected = {
        "run_id": "run-a",
        "status": "completed",
        "formulation": "grouped-merge",
        "groups": 2,
        "split_points": None,
        "dataset_path": dataset.path,
        "input_format": input_format,
        "row_ordering": "locus,alleles",
        "input_tables": 2,
        "samples": 3,
        "output_path": str(path),
        "output_format": input_format,
        "threads": 2,
        "rows_written": len(ROWS) * 3,
    }
    assert {column: record[column] for column in expected} == expected
    assert record["execute_ns"] is not None
    assert (metrics / "metrics" / "run-a.parquet").is_file()


def test_a_measured_write_names_the_metrics_it_could_not_record(
    backend, dataset, ctx, input_format, tmp_path
):
    # A hash join reports metrics the run metrics table has no column for.
    keys = ["contig", "position", "alleles"]
    left = dataset.read(ctx, ["s=HG02230"]).select(*keys)
    right = dataset.read(ctx, ["s=NA18534"]).select(*[col(key).alias(f"r_{key}") for key in keys])
    frame = (
        left.join(right, left_on=keys, right_on=[f"r_{key}" for key in keys], how="inner")
        .select(*keys)
        .sort(*sort_keys(dataset))
    )
    write = Write(
        str(tmp_path / "out" / f"g.{input_format}"),
        input_format,
        None,
        dataset.ordering,
        OutputLayout.SINGLE_FILE,
    )

    outcome = backend.start(
        ctx, frame, MeasuredWrite(write, str(tmp_path / "metrics"), GROUPED)
    ).wait()

    assert isinstance(outcome.unrecorded_metrics, tuple)
    assert {"build_time", "join_time"} <= set(outcome.unrecorded_metrics)


def test_a_measured_write_records_every_field_of_the_formulation_record_it_was_given(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    metrics = tmp_path / "metrics"
    formulation = FormulationRecord("interval-merge", groups=3, split_points="chr01:100,chr02:5")

    backend.start(
        ctx,
        ordered.frame,
        MeasuredWrite(
            write_of(ordered, tmp_path / "out" / f"g.{input_format}", input_format),
            str(metrics),
            formulation,
        ),
        run_id="run-a",
    ).wait()

    record = run_record(metrics, "run-a")
    assert record["formulation"] == "interval-merge"
    assert record["groups"] == 3
    assert record["split_points"] == "chr01:100,chr02:5"


def test_a_measured_write_whose_data_write_fails_records_failed_and_raises_backend_error(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    path = tmp_path / "out" / f"g.{input_format}"
    path.mkdir(parents=True)
    metrics = tmp_path / "metrics"
    action = MeasuredWrite(write_of(ordered, path, input_format), str(metrics), ordered.formulation)

    with pytest.raises(BackendError) as raised:
        backend.start(ctx, ordered.frame, action, run_id="run-a").wait()

    assert not isinstance(raised.value, InvalidAction)
    record = run_record(metrics, "run-a")
    assert record["status"] == "failed"
    assert record["dataset_path"] == dataset.path
    assert record["input_tables"] == 3
    assert record["rows_written"] is None
    assert record["execute_ns"] is None
    assert not (metrics / "metrics" / "run-a.parquet").exists()


def test_a_measured_write_of_a_plan_over_two_datasets_raises_invalid_action(
    backend, dataset, ctx, input_format, tmp_path
):
    other_path = write_mixed_dataset(tmp_path / "other", input_format)
    other = backend.open_dataset(str(other_path), input_format, "locus,alleles")
    frame = dataset.read(ctx, ["g0"]).union(other.read(ctx, ["g0"]))
    ordered = formulations.union(ctx, dataset)
    metrics = tmp_path / "metrics"
    write = write_of(ordered, tmp_path / "out" / f"g.{input_format}", input_format)

    with pytest.raises(InvalidAction, match="one dataset"):
        backend.start(ctx, frame, MeasuredWrite(write, str(metrics), ordered.formulation))

    assert not metrics.exists()
    assert not (tmp_path / "out").exists()


def test_a_write_annotating_samples_over_two_datasets_covers_both(
    backend, dataset, ctx, input_format, tmp_path
):
    other_path = tmp_path / "other"
    other_path = write_mixed_dataset(other_path, input_format)
    other = backend.open_dataset(str(other_path), input_format, "locus,alleles")
    frame = (
        dataset.read(ctx, ["g0"])
        .union(other.read(ctx, ["g0", "s=NA18534"]))
        .sort(*sort_keys(dataset))
    )
    path = tmp_path / "out" / f"g.{input_format}"

    backend.start(
        ctx,
        frame,
        Write(str(path), input_format, None, dataset.ordering, OutputLayout.SINGLE_FILE, True),
    ).wait()

    written = backend.open_dataset(str(path.parent), input_format, "locus,alleles")
    assert written.input_tables[0].sample_set == ("HG00308", "HG00592", "NA18534")


def test_a_measured_write_under_a_recorded_run_id_fails_and_replaces_nothing(
    backend, dataset, ctx, input_format, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    metrics = tmp_path / "metrics"
    action = MeasuredWrite(
        write_of(ordered, tmp_path / "out" / f"g.{input_format}", input_format),
        str(metrics),
        ordered.formulation,
    )
    backend.start(ctx, ordered.frame, action, run_id="run-a").wait()
    first = run_record(metrics, "run-a")

    with pytest.raises(BackendError, match="run-a"):
        backend.start(ctx, ordered.frame, action, run_id="run-a").wait()

    assert run_record(metrics, "run-a") == first


@pytest.mark.parametrize("groups", [-1, 2**64])
def test_a_formulation_record_no_run_record_can_hold_raises_invalid_action(
    backend, dataset, ctx, tmp_path, groups
):
    ordered = formulations.union(ctx, dataset)
    action = MeasuredWrite(
        write_of(ordered, tmp_path / f"g.{dataset.input_format}", dataset.input_format),
        str(tmp_path / "metrics"),
        FormulationRecord("grouped-merge", groups=groups),
    )

    with pytest.raises(InvalidAction, match="groups"):
        backend.start(ctx, ordered.frame, action)


def test_a_measured_write_given_something_that_is_no_formulation_record_raises_type_error(
    backend, dataset, ctx, tmp_path
):
    ordered = formulations.union(ctx, dataset)
    action = MeasuredWrite(
        write_of(ordered, tmp_path / f"g.{dataset.input_format}", dataset.input_format),
        str(tmp_path / "metrics"),
        "union",
    )

    with pytest.raises(TypeError, match="FormulationRecord"):
        backend.start(ctx, ordered.frame, action)
