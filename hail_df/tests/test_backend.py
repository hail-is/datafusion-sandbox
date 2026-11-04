import itertools
import os
import sys
import threading
import time
from dataclasses import FrozenInstanceError

import hail_df
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from conftest import INPUT_TABLES, SAMPLE_SETS, plan_sections, write_large_dataset
from datafusion import col, functions, lit, udf
from hail_df import (
    BackendError,
    Collect,
    Explain,
    ExplainAnalyze,
    InvalidAction,
    OrderingAssertionFailed,
    PlanText,
    Rows,
    SortColumn,
    formulations,
)

LOCUS_THEN_ALLELES = [
    SortColumn("contig", True, False),
    SortColumn("position", True, False),
    SortColumn("alleles", True, False),
]


def test_opening_a_dataset_lists_its_input_tables_with_their_sample_sets(dataset):
    assert [table.name for table in dataset.input_tables] == INPUT_TABLES
    assert [table.sample_set for table in dataset.input_tables] == SAMPLE_SETS


def test_each_input_table_s_scan_description_names_its_dataset_and_itself(
    dataset, mixed_dataset, input_format
):
    assert [fields(table.scan_description) for table in dataset.input_tables] == [
        (str(mixed_dataset), input_format, "locus,alleles", name) for name in INPUT_TABLES
    ]


def test_an_opened_dataset_states_its_stored_ordering_as_plain_sort_columns(dataset):
    assert list(dataset.ordering) == LOCUS_THEN_ALLELES
    assert dataset.ordering[0] == ("contig", True, False)


def test_an_opened_dataset_has_the_schema_of_its_rows_with_their_sample(dataset):
    # The fixture's columns, non-null, with strings read as string views.
    assert dataset.schema == pa.schema(
        [
            pa.field("contig", pa.string_view(), nullable=False),
            pa.field("position", pa.int32(), nullable=False),
            pa.field("alleles", pa.string_view(), nullable=False),
            pa.field("s", pa.string_view(), nullable=False),
        ]
    )


def test_reading_every_input_table_gives_a_frame_with_the_dataset_s_schema(dataset):
    ctx = hail_df.context()

    assert dataset.read(ctx).schema() == dataset.schema


def test_reading_some_input_tables_gives_a_frame_with_the_dataset_s_schema(dataset):
    ctx = hail_df.context()

    for names in [["s=NA18534"], ["s=NA18534", "g0"]]:
        assert dataset.read(ctx, names).schema() == dataset.schema


def test_a_frame_over_the_scans_refuses_to_scan_in_python(dataset):
    ctx = hail_df.context()
    frame = dataset.read(ctx, ["g0", "s=HG02230"])

    with pytest.raises(Exception, match="scan description"):
        frame.collect()


def test_plan_bytes_of_a_frame_read_back_as_the_scan_descriptions_it_reads(backend, dataset):
    ctx = hail_df.context()
    keys = [col(c).sort(ascending=a, nulls_first=n) for c, a, n in dataset.ordering]
    for names in [["s=NA18534"], ["s=NA18534", "g0"], None]:
        frame = dataset.read(ctx, names).sort(*keys)

        plan_bytes = frame.logical_plan().to_bytes(ctx)

        read = names if names is not None else INPUT_TABLES
        tables = {table.name: table for table in dataset.input_tables}
        assert backend.scan_descriptions(plan_bytes) == [
            tables[name].scan_description for name in read
        ]


@pytest.mark.parametrize(
    ("names", "message"),
    [
        ([], "at least one"),
        (["g0", "g0"], "repeat"),
        (["g0", "g9"], "g9"),
    ],
)
def test_reading_input_tables_the_dataset_does_not_hold_once_each_fails(dataset, names, message):
    with pytest.raises(ValueError, match=message):
        dataset.read(hail_df.context(), names)


# Disk paths, as every `hail_df` dataset fixture is: see conftest.py.
def test_opening_a_dataset_that_is_not_there_raises_backend_error(backend, tmp_path):
    with pytest.raises(BackendError):
        backend.open_dataset(str(tmp_path / "missing"), "parquet", "locus")


@pytest.mark.parametrize(
    ("input_format", "row_ordering", "message"),
    [("csv", "locus", "'csv'"), ("parquet", "alleles", "'alleles'")],
)
def test_opening_a_dataset_in_an_unknown_format_or_ordering_raises_backend_error(
    backend, tmp_path, input_format, row_ordering, message
):
    with pytest.raises(BackendError, match=message):
        backend.open_dataset(str(tmp_path), input_format, row_ordering)


def test_bytes_that_are_no_plan_raise_backend_error(backend):
    with pytest.raises(BackendError):
        backend.scan_descriptions(b"not a plan")


def fields(description):
    return (
        description.dataset_path,
        description.input_format,
        description.row_ordering,
        description.input_table,
    )


def union_frame(ctx, dataset):
    return formulations.union(ctx, dataset).frame


def test_a_run_from_plan_bytes_has_the_same_outcome_as_a_run_from_their_frame(backend, dataset):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)
    action = Collect(dataset.ordering)

    from_frame = backend.start(ctx, frame, action).wait()
    from_bytes = backend.start(ctx, frame.logical_plan().to_bytes(ctx), action).wait()

    assert from_frame.rows.num_rows > 0
    assert from_bytes == from_frame


def test_a_collect_s_rows_arrive_as_arrow_data_with_the_schema_of_the_frame(backend, dataset):
    ctx = hail_df.context()

    outcome = backend.start(ctx, union_frame(ctx, dataset), Collect(dataset.ordering)).wait()

    assert isinstance(outcome, Rows)
    assert isinstance(outcome.rows, pa.Table)
    assert outcome.rows.schema == dataset.schema


def test_a_collect_of_no_rows_still_has_their_schema(backend, dataset):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset).filter(col("position") < lit(0))

    rows = backend.start(ctx, frame, Collect(dataset.ordering)).wait().rows

    assert rows.num_rows == 0
    assert rows.schema == dataset.schema


def test_a_collect_is_a_frozen_value(dataset):
    assert Collect(dataset.ordering) == Collect(dataset.ordering)
    with pytest.raises(FrozenInstanceError):
        Collect(dataset.ordering).ordering = ()


def test_a_run_keeps_the_id_it_was_given_and_otherwise_generates_a_distinct_one(backend, dataset):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)
    action = Collect(dataset.ordering)

    assert backend.start(ctx, frame, action, run_id="mine").run_id == "mine"
    assert backend.start(ctx, frame, action).run_id != backend.start(ctx, frame, action).run_id


def test_waiting_again_hands_back_the_same_outcome(backend, dataset):
    ctx = hail_df.context()
    run = backend.start(ctx, union_frame(ctx, dataset), Collect(dataset.ordering))

    first = run.wait()

    assert first.rows.num_rows > 0
    assert run.wait() == first


def test_another_python_thread_keeps_running_while_wait_blocks(backend, tmp_path):
    # On disk, as every `hail_df` dataset fixture is: see conftest.py. Enough rows that the run far
    # outlasts the time this thread takes to reach `wait` and to take the rows from it.
    rows_per_table = 2_000_000
    path = write_large_dataset(tmp_path / "large", rows_per_table)
    dataset = backend.open_dataset(str(path), "parquet", "locus,alleles")
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)

    ticks = []
    done = threading.Event()

    def tick():
        while not done.wait(0.001):
            ticks.append(time.perf_counter())

    ticker = threading.Thread(target=tick)
    ticker.start()
    try:
        run = backend.start(ctx, frame, Collect(dataset.ordering))
        waiting = time.perf_counter()
        outcome = run.wait()
        waited = time.perf_counter()
    finally:
        done.set()
        ticker.join()

    assert outcome.rows.num_rows == 2 * rows_per_table
    # A wait that held the interpreter would stop the ticker for nearly all of it.
    during = [waiting, *(tick for tick in ticks if waiting < tick < waited), waited]
    longest_stop = max(later - earlier for earlier, later in itertools.pairwise(during))
    assert longest_stop < (waited - waiting) / 2


def test_a_frame_carrying_an_inlined_python_udf_raises_invalid_action(backend, dataset):
    ctx = hail_df.context()
    doubled = udf(lambda a: pc.multiply(a, 2), [pa.int32()], pa.int32(), "immutable", "doubled")
    frame = union_frame(ctx, dataset).with_column("doubled", doubled(col("position")))

    with pytest.raises(InvalidAction, match="Python function"):
        backend.start(ctx, frame, Collect(dataset.ordering))


def test_plan_bytes_with_a_mismatched_wire_version_raise_invalid_action(backend, dataset):
    ctx = hail_df.context()
    plan_bytes = union_frame(ctx, dataset).logical_plan().to_bytes(ctx)
    mismatched = plan_bytes.replace(b"DFPYCHN\x01", b"DFPYCHN\x02")
    assert mismatched != plan_bytes

    with pytest.raises(InvalidAction, match="DFPYCHN version 2"):
        backend.start(ctx, mismatched, Collect(dataset.ordering))
    with pytest.raises(InvalidAction, match="DFPYCHN version 2"):
        backend.scan_descriptions(mismatched)


@pytest.mark.parametrize(
    "key", ["hail_df.typo", "threads", "datafusion.execution.target_partitions"]
)
def test_a_settings_key_outside_the_three_the_backend_takes_raises_invalid_action(
    backend, dataset, key
):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)

    with pytest.raises(InvalidAction, match=key):
        backend.start(ctx, frame, Collect(dataset.ordering), settings={key: "1"})


@pytest.mark.parametrize(
    "key", ["hail_df.threads", "hail_df.target_partitions", "hail_df.repartition_file_min_size"]
)
def test_a_setting_that_is_not_a_count_raises_invalid_action(backend, dataset, key):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)

    with pytest.raises(InvalidAction, match=key):
        backend.start(ctx, frame, Collect(dataset.ordering), settings={key: "many"})


def test_target_partitions_sets_the_session_s(backend, dataset):
    # An aggregate over the sorted scans shows it, unlike the formulations, whose merge tree no
    # setting changes: it repartitions by hash into the target partitions.
    ctx = hail_df.context()
    by_contig = col("contig").sort(ascending=True, nulls_first=False)
    frame = (
        dataset.read(ctx)
        .aggregate([col("contig")], [functions.count(col("position")).alias("rows")])
        .sort(by_contig)
    )
    action = Explain([SortColumn("contig", True, False)])

    # Two counts, so that at most one is the machine's default.
    for target_partitions in [3, 7]:
        settings = {"hail_df.target_partitions": str(target_partitions)}
        text = backend.start(ctx, frame, action, settings=settings).wait().text

        assert f"Hash([contig@0], {target_partitions})" in plan_sections(text)["physical_plan"]


def test_a_run_s_session_keeps_the_shared_defaults_under_any_settings(backend, dataset):
    # ADR 0003's shared session pushes filters down so that a filtered Parquet plan has a filtered
    # Vortex plan's shape, which a session of DataFusion's own defaults doesn't: it keeps a
    # `FilterExec` over Parquet.
    ctx = hail_df.context()
    frame = dataset.read(ctx, ["g0"]).filter(col("position") > lit(0))

    for settings in [None, {"hail_df.threads": "2", "hail_df.target_partitions": "2"}]:
        text = backend.start(ctx, frame, Explain(dataset.ordering), settings=settings).wait().text

        assert "FilterExec" not in plan_sections(text)["physical_plan"]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="counts threads through /proc")
def test_the_threads_setting_sets_the_thread_count_of_the_run_s_runtimes(backend, dataset):
    # More threads than any machine's default, so that only the setting can account for them: the
    # run builds an IO and a CPU runtime of that many, each running for the whole run. This reads
    # /proc because no interface states how many threads a run has until a measured write records
    # them; the Rust suite checks the same count at the backend's object-store seam.
    threads = 64
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)
    before = len(os.listdir("/proc/self/task"))
    peak = [before]
    done = threading.Event()

    def sample():
        while not done.is_set():
            peak[0] = max(peak[0], len(os.listdir("/proc/self/task")))

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        settings = {"hail_df.threads": str(threads)}
        backend.start(ctx, frame, Collect(dataset.ordering), settings=settings).wait()
    finally:
        done.set()
        sampler.join()

    assert peak[0] - before >= 2 * threads


def test_explain_renders_the_frontend_logical_and_physical_plans_without_running(backend, dataset):
    ctx = hail_df.context()

    outcome = backend.start(ctx, union_frame(ctx, dataset), Explain(dataset.ordering)).wait()

    assert isinstance(outcome, PlanText)
    sections = plan_sections(outcome.text)
    assert list(sections) == ["frontend_plan", "logical_plan", "physical_plan"]
    assert "Sort: " in sections["frontend_plan"]
    assert "OrderingAssertion" in sections["logical_plan"]
    assert "OrderingAssertionExec" in sections["physical_plan"]
    assert "output_rows=" not in outcome.text


def test_explain_analyze_runs_the_plan_and_shows_per_operator_metrics(backend, dataset):
    ctx = hail_df.context()

    outcome = backend.start(ctx, union_frame(ctx, dataset), ExplainAnalyze(dataset.ordering)).wait()

    assert isinstance(outcome, PlanText)
    sections = plan_sections(outcome.text)
    assert list(sections) == [
        "frontend_plan",
        "logical_plan",
        "physical_plan",
        "Plan with Metrics",
    ]
    # The fixture's eight rows, for each of its four samples.
    metrics = sections["Plan with Metrics"]
    assert "output_rows=32" in metrics
    assert "elapsed_compute=" in metrics


def test_the_frontend_plan_is_the_frame_as_written_unoptimized(backend, dataset):
    ctx = hail_df.context()
    # The optimizer would push this filter beneath the sort, into the scans.
    frame = union_frame(ctx, dataset).filter(col("position") < lit(0))

    outcome = backend.start(ctx, frame, Explain(dataset.ordering)).wait()

    frontend = plan_sections(outcome.text)["frontend_plan"].splitlines()
    assert frontend[0].startswith("Filter: ")
    assert frontend[1].strip().startswith("Sort: ")
    assert all("Filter" not in line for line in frontend[1:])


@pytest.mark.parametrize("action", [Explain, ExplainAnalyze])
def test_an_explain_failing_raises_backend_error_rather_than_rendering_the_failure(
    backend, dataset, action
):
    ctx = hail_df.context()
    run = backend.start(
        ctx, union_frame(ctx, dataset), action([SortColumn("alleles", True, False)])
    )

    with pytest.raises(BackendError, match="ordering_assertion_failed"):
        run.wait()


@pytest.mark.parametrize("action", [Explain, ExplainAnalyze])
def test_an_explain_with_no_ordering_raises_invalid_action(backend, dataset, action):
    ctx = hail_df.context()

    with pytest.raises(InvalidAction, match="ordering"):
        backend.start(ctx, union_frame(ctx, dataset), action(()))


@pytest.mark.parametrize("action", [Explain, ExplainAnalyze])
def test_an_explain_given_something_that_is_no_write_raises_type_error(backend, dataset, action):
    ctx = hail_df.context()

    with pytest.raises(TypeError, match="write must be a Write"):
        backend.start(ctx, union_frame(ctx, dataset), action(dataset.ordering, "a write"))


def test_a_setting_whose_value_does_not_parse_raises_invalid_action(backend, dataset):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)
    settings = {"hail_df.threads": "many"}

    with pytest.raises(InvalidAction, match="hail_df.threads"):
        backend.start(ctx, frame, Collect(dataset.ordering), settings=settings)


@pytest.mark.parametrize("settings", [{"hail_df.threads": 1}, {1: "1"}])
def test_a_setting_that_is_not_a_string_raises_type_error(backend, dataset, settings):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)

    with pytest.raises(TypeError):
        backend.start(ctx, frame, Collect(dataset.ordering), settings=settings)


def test_a_setting_the_backend_takes_leaves_the_rows_a_plan_collects(backend, dataset):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)
    action = Collect(dataset.ordering)
    settings = {"hail_df.target_partitions": "1"}

    assert backend.start(ctx, frame, action, settings=settings).wait().rows.num_rows == (
        backend.start(ctx, frame, action).wait().rows.num_rows
    )


def test_a_collect_with_no_ordering_raises_invalid_action(backend, dataset):
    ctx = hail_df.context()

    with pytest.raises(InvalidAction, match="ordering"):
        backend.start(ctx, union_frame(ctx, dataset), Collect(()))


def test_invalid_action_is_a_backend_error():
    assert issubclass(InvalidAction, BackendError)


def test_a_run_failing_its_root_assertion_raises_ordering_assertion_failed_from_wait(
    backend, dataset
):
    ctx = hail_df.context()
    # Rows sorted by locus then alleles aren't in alleles order, unless the backend inserts a sort.
    action = Collect([SortColumn("alleles", True, False)])

    run = backend.start(ctx, union_frame(ctx, dataset), action)

    with pytest.raises(OrderingAssertionFailed, match="ordering_assertion_failed") as raised:
        run.wait()
    assert isinstance(raised.value, BackendError)
    assert not isinstance(raised.value, InvalidAction)


def test_starting_bytes_that_are_no_plan_raises_backend_error_not_invalid_action(backend):
    with pytest.raises(BackendError) as raised:
        backend.start(hail_df.context(), b"not a plan", Collect(LOCUS_THEN_ALLELES))
    assert not isinstance(raised.value, InvalidAction)


def test_starting_something_that_is_no_action_raises_type_error(backend, dataset):
    ctx = hail_df.context()

    with pytest.raises(TypeError, match="Collect, Explain, ExplainAnalyze, Write or MeasuredWrite"):
        backend.start(ctx, union_frame(ctx, dataset), "collect")
