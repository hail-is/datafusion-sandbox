import hail_df
import pyarrow as pa
import pytest
from conftest import INPUT_TABLES, SAMPLE_SETS
from datafusion import col
from hail_df import Backend, BackendError, SortColumn

LOCUS_THEN_ALLELES = [
    SortColumn("contig", True, False),
    SortColumn("position", True, False),
    SortColumn("alleles", True, False),
]


@pytest.fixture(scope="module")
def backend() -> Backend:
    return Backend()


@pytest.fixture
def dataset(backend, mixed_dataset, input_format):
    return backend.open_dataset(str(mixed_dataset), input_format, "locus,alleles")


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
