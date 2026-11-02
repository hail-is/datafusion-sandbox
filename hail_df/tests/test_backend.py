import itertools
import threading
import time
from dataclasses import FrozenInstanceError

import hail_df
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from conftest import INPUT_TABLES, ROW_FIELDS, SAMPLE_SETS, write_table
from datafusion import col, lit, udf
from hail_df import BackendError, Collect, InvalidAction, Rows, SortColumn, formulations

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
    rows = pa.table(
        [
            pa.repeat("chr01", rows_per_table),
            pa.array(range(1, rows_per_table + 1), pa.int32()),
            pa.repeat("A,G", rows_per_table),
        ],
        schema=pa.schema(ROW_FIELDS),
    )
    write_table(rows, tmp_path / "large" / "s=HG02230" / "a.parquet", "parquet")
    write_table(rows, tmp_path / "large" / "s=NA18534" / "a.parquet", "parquet")
    dataset = backend.open_dataset(str(tmp_path / "large"), "parquet", "locus,alleles")
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


def test_an_unknown_setting_raises_invalid_action(backend, dataset):
    ctx = hail_df.context()
    frame = union_frame(ctx, dataset)

    with pytest.raises(InvalidAction, match="hail_df.typo"):
        backend.start(ctx, frame, Collect(dataset.ordering), settings={"hail_df.typo": "1"})


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


def test_a_run_failing_raises_backend_error_from_wait(backend, dataset):
    ctx = hail_df.context()
    # Rows sorted by locus then alleles aren't in alleles order, unless the backend inserts a sort.
    action = Collect([SortColumn("alleles", True, False)])

    run = backend.start(ctx, union_frame(ctx, dataset), action)

    with pytest.raises(BackendError, match="ordering_assertion_failed") as raised:
        run.wait()
    assert not isinstance(raised.value, InvalidAction)


def test_starting_bytes_that_are_no_plan_raises_backend_error_not_invalid_action(backend):
    with pytest.raises(BackendError) as raised:
        backend.start(hail_df.context(), b"not a plan", Collect(LOCUS_THEN_ALLELES))
    assert not isinstance(raised.value, InvalidAction)


def test_starting_something_that_is_no_action_raises_type_error(backend, dataset):
    ctx = hail_df.context()

    with pytest.raises(TypeError, match="Collect"):
        backend.start(ctx, union_frame(ctx, dataset), "collect")
